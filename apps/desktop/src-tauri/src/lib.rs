//! trackerstream desktop backend. The libp2p/IPFS data plane now runs as an external Go
//! sidecar (`tsnode`, see `sidecar`/`rpc`); this backend spawns it, drives it over the local
//! kubo-compatible RPC, and keeps the pure logic in Rust: audio reassembly/streaming (`ipfs`),
//! the catalog SQLite VFS (`catalog`), and signed-IPNS verification (`ipns`). The frontend asks
//! the backend to resolve a root CID and gets back reassembled module bytes to play.

pub mod catalog;
pub mod ipfs;
pub mod ipns;
pub mod rpc;
pub mod sidecar;

use cid::Cid;
use rpc::NodeRpc;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tauri::ipc::{Channel, Response};
use tauri::{Manager, State};

/// Backend node state: the RPC handle to the sidecar + our PeerId. The `Sidecar` is held in
/// managed state too (separately) so the child lives as long as the app.
struct NodeState {
    rpc: NodeRpc,
    peer_id: String,
}

/// In-flight (or finished) v2 streams, keyed by root CID string.
#[derive(Default)]
struct Streams(Mutex<HashMap<String, Arc<ipfs::StreamState>>>);

/// Roots this client holds in its blockstore: `root CID -> fully complete?`. Net-new
/// bookkeeping persisted to `held_roots.json` (the blockstore has no root index). The Go node
/// advertises held roots over the presence topic / DHT; this just records what we've fetched.
pub(crate) struct HeldRoots {
    map: Mutex<HashMap<String, bool>>,
    path: Option<std::path::PathBuf>,
}

impl HeldRoots {
    fn load(dir: Option<&std::path::Path>) -> Self {
        let path = dir.map(|d| d.join("held_roots.json"));
        let map = path
            .as_ref()
            .and_then(|p| std::fs::read(p).ok())
            .and_then(|b| serde_json::from_slice::<HashMap<String, bool>>(&b).ok())
            .unwrap_or_default();
        HeldRoots { map: Mutex::new(map), path }
    }

    /// Record a root we now hold. `complete=false` marks/keeps it partial; a later
    /// `complete=true` upgrades it. Never downgrades a complete root.
    fn mark(&self, root: &str, complete: bool) {
        {
            let mut m = self.map.lock().unwrap();
            let entry = m.entry(root.to_string()).or_insert(false);
            *entry = *entry || complete;
        }
        self.persist();
    }

    fn persist(&self) {
        let Some(path) = &self.path else { return };
        let snapshot = self.map.lock().unwrap().clone();
        if let Ok(bytes) = serde_json::to_vec(&snapshot) {
            std::fs::write(path, bytes).ok();
        }
    }
}

/// Verified IPNS records cached to `ipns_cache.json` (name -> base64 record). Lets the client
/// resolve the catalog name through a short box/sidecar outage with zero network: a cached
/// record is re-verified (signature + EOL) on every read, so a stale/expired/forged entry fails
/// and falls through to a fresh `routing/get`.
pub(crate) struct IpnsCache {
    map: Mutex<HashMap<String, String>>,
    path: Option<std::path::PathBuf>,
}

impl IpnsCache {
    fn load(dir: Option<&std::path::Path>) -> Self {
        let path = dir.map(|d| d.join("ipns_cache.json"));
        let map = path
            .as_ref()
            .and_then(|p| std::fs::read(p).ok())
            .and_then(|b| serde_json::from_slice::<HashMap<String, String>>(&b).ok())
            .unwrap_or_default();
        IpnsCache { map: Mutex::new(map), path }
    }

    fn get(&self, name: &str) -> Option<String> {
        self.map.lock().unwrap().get(name).cloned()
    }

    fn put(&self, name: &str, record_b64: &str) {
        self.map.lock().unwrap().insert(name.to_string(), record_b64.to_string());
        self.persist();
    }

    fn persist(&self) {
        let Some(path) = &self.path else { return };
        let snapshot = self.map.lock().unwrap().clone();
        if let Ok(bytes) = serde_json::to_vec(&snapshot) {
            std::fs::write(path, bytes).ok();
        }
    }
}

fn stream_for(streams: &State<'_, Streams>, root: &str) -> Result<Arc<ipfs::StreamState>, String> {
    streams
        .0
        .lock()
        .unwrap()
        .get(root)
        .cloned()
        .ok_or_else(|| format!("no stream for {root}"))
}

#[derive(Serialize)]
struct NodeInfo {
    peer_id: String,
    listening: Vec<String>,
    /// AutoNAT verdict from the Go node: `Some(true)` public, `Some(false)` private, `None`
    /// undecided (drives the peers-pane reachability badge).
    reachable: Option<bool>,
}

#[tauri::command]
async fn node_info(state: State<'_, NodeState>) -> Result<NodeInfo, String> {
    let id = state.rpc.id().await.map_err(|e| e.to_string())?;
    let reachable = match state.rpc.node_status().await.map(|s| s.reachability) {
        Ok(r) if r == "public" => Some(true),
        Ok(r) if r == "private" => Some(false),
        _ => None,
    };
    Ok(NodeInfo { peer_id: state.peer_id.clone(), listening: id.addresses, reachable })
}

#[derive(Serialize)]
struct PeerEntry {
    id: String,
    down: u64,
    up: u64,
    connected: bool,
    /// "master" (the always-on seed) or "other". Per-peer warm tagging moved into the Go node.
    role: &'static str,
}

/// Relay-hop telemetry for the peers pane (preserves the frontend's field names; sourced from
/// the Go node's `node/status`). DCUtR-upgrade counting is not yet surfaced by the node.
#[derive(Serialize, Default)]
struct RelayCounts {
    direct: u64,
    relayed_peer: u64,
    relayed_master: u64,
    dcutr_upgrades: u64,
}

#[derive(Serialize)]
struct PeerStats {
    connected: usize,
    peers: Vec<PeerEntry>,
    relay: RelayCounts,
}

#[tauri::command]
async fn peer_stats(state: State<'_, NodeState>) -> Result<PeerStats, String> {
    let master = ipfs::master_peer_id();
    let conns = state.rpc.swarm_peers().await.map_err(|e| e.to_string())?;
    let connected: std::collections::HashSet<String> = conns.iter().map(|p| p.peer.clone()).collect();
    let bw = state.rpc.bandwidth_by_peer().await.unwrap_or_default();
    let status = state.rpc.node_status().await.ok();

    // Union of connected peers and every peer that has transferred bytes (so a peer that did
    // up/down then dropped stays, grayed, until it reconnects).
    let mut ids: std::collections::HashSet<String> = bw.keys().cloned().collect();
    ids.extend(connected.iter().cloned());
    let peers = ids
        .into_iter()
        .map(|id| {
            let (down, up) = bw.get(&id).copied().unwrap_or((0, 0));
            let role = if id == master { "master" } else { "other" };
            PeerEntry { connected: connected.contains(&id), id, down, up, role }
        })
        .collect();
    let relay = status
        .map(|s| RelayCounts {
            direct: s.relay_stats.direct,
            relayed_peer: s.relay_stats.peer_relay,
            relayed_master: s.relay_stats.master_relay,
            dcutr_upgrades: 0,
        })
        .unwrap_or_default();
    Ok(PeerStats { connected: connected.len(), peers, relay })
}

#[derive(Serialize)]
struct PeerDetail {
    id: String,
    connected: bool,
    role: &'static str,
    warm_reason: Vec<String>,
    down: u64,
    up: u64,
    addrs: Vec<String>,
    relayed: bool,
    transport: String,
    agent: Option<String>,
    protocols: Vec<String>,
    observed_addr: Option<String>,
    rtt_ms: Option<f64>,
}

/// (all-relayed?, transport label) from the live connection addrs.
fn classify_transport(addrs: &[String]) -> (bool, String) {
    if addrs.is_empty() {
        return (false, "unknown".into());
    }
    let direct: Vec<&String> = addrs.iter().filter(|a| !a.contains("/p2p-circuit")).collect();
    match direct.first() {
        Some(a) if a.contains("/quic") => (false, "quic".into()),
        Some(a) if a.contains("/tcp") => (false, "tcp".into()),
        Some(_) => (false, "direct".into()),
        None => (true, "relay".into()),
    }
}

#[tauri::command]
async fn peer_detail(peer_id: String, state: State<'_, NodeState>) -> Result<PeerDetail, String> {
    let master = ipfs::master_peer_id();
    let conns = state.rpc.swarm_peers().await.map_err(|e| e.to_string())?;
    let addrs: Vec<String> =
        conns.iter().filter(|p| p.peer == peer_id).map(|p| p.addr.clone()).collect();
    let connected = !addrs.is_empty();
    let (down, up) =
        state.rpc.bandwidth_by_peer().await.unwrap_or_default().get(&peer_id).copied().unwrap_or((0, 0));
    let role = if peer_id == master { "master" } else { "other" };
    let (relayed, transport) = classify_transport(&addrs);
    // identify / ping detail (agent, protocols, observed addr, RTT) is not exposed over the
    // current RPC subset — the peers-pane detail degrades gracefully to None/empty.
    Ok(PeerDetail {
        id: peer_id,
        connected,
        role,
        warm_reason: vec![],
        down,
        up,
        addrs,
        relayed,
        transport,
        agent: None,
        protocols: vec![],
        observed_addr: None,
        rtt_ms: None,
    })
}

#[tauri::command]
async fn connect_peer(addr: String, state: State<'_, NodeState>) -> Result<(), String> {
    state.rpc.swarm_connect(&addr).await.map_err(|e| format!("connect {addr} failed: {e}"))
}

/// Pin a persistent connection to the master (called once at app mount with the config
/// bootstrap addrs). The Go node keepalives the link itself; here we kick the dials so the
/// swarm forms immediately rather than lazily per-play.
#[tauri::command]
async fn keepalive_master(addrs: Vec<String>, state: State<'_, NodeState>) -> Result<(), String> {
    let mut last_err = None;
    for a in &addrs {
        if let Err(e) = state.rpc.swarm_connect(a).await {
            last_err = Some(e);
        } else {
            return Ok(()); // one good dial is enough; the node holds it open
        }
    }
    match last_err {
        Some(e) => Err(format!("keepalive master failed: {e}")),
        None => Ok(()),
    }
}

/// Queue-driven pre-connection hint. Holder discovery + warm-set management now live in the Go
/// node (custom DHT providers + presence topic), so this validates the CID and is otherwise a
/// no-op — the node warms holders for roots it sees demand for.
#[tauri::command]
async fn warm_root(root: String) -> Result<(), String> {
    let _: Cid = root.parse().map_err(|e| format!("bad CID {root}: {e}"))?;
    Ok(())
}

/// Resolve an IPNS name to its current CID, verifying the signed record locally (the node is an
/// untrusted cache). `routing/get` first (freshest — gossipsub/DHT-backed in the node), then the
/// re-verified on-disk cache as the box-down fallback.
async fn resolve_ipns_name(
    name: &str,
    cache: &IpnsCache,
    rpc: &NodeRpc,
) -> Result<Cid, String> {
    // A cold start needs a few seconds for the sidecar to dial the master and the custom DHT to
    // answer, so retry `routing/get` briefly before giving up — otherwise the very first query
    // (getFormats on mount) flips "catalog offline" before the swarm has even formed. A
    // previously-cached, still-valid record short-circuits the wait (box-down fallback), so only
    // a true first run pays the retry latency.
    let mut last = String::new();
    for attempt in 0..6u32 {
        match rpc.routing_get(name).await {
            Ok(record) => match ipns::verify_b64(name, &record) {
                Ok(cid) => {
                    cache.put(name, &record);
                    return Ok(cid);
                }
                Err(e) => last = format!("record failed verification: {e}"),
            },
            Err(e) => last = e.to_string(),
        }
        if let Some(b64) = cache.get(name) {
            if let Ok(cid) = ipns::verify_b64(name, &b64) {
                return Ok(cid);
            }
        }
        if attempt < 5 {
            tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        }
    }
    Err(format!("resolve_ipns {name}: routing/get failed after retries ({last})"))
}

/// Answer a catalog query by lazily reading the IPNS-published catalog DB over the Bitswap-
/// backed SQLite VFS. Resolves `name` (the catalog's `CATALOG_IPNS_KEY`) to the current DB CID,
/// then runs the query touching only the pages it needs.
#[tauri::command]
async fn catalog_query(
    name: String,
    req: catalog::CatalogReq,
    cache: State<'_, Arc<IpnsCache>>,
    state: State<'_, NodeState>,
) -> Result<serde_json::Value, String> {
    let cid = resolve_ipns_name(&name, &cache, &state.rpc).await?;
    catalog::run_query(state.rpc.clone(), cid, req).await
}

/// Resolve a module root CID to its exact bytes, 100% from CID blocks over the sidecar.
#[tauri::command]
async fn fetch_module(
    root: String,
    state: State<'_, NodeState>,
    held: State<'_, Arc<HeldRoots>>,
) -> Result<Response, String> {
    let cid: Cid = root.parse().map_err(|e| format!("bad CID {root}: {e}"))?;
    held.mark(&root, false);
    let bytes = ipfs::reassemble(&state.rpc, cid)
        .await
        .map_err(|e| format!("fetch_module {root} failed: {e}"))?;
    held.mark(&root, true);
    // Advertise the track root now that we hold it — peers can fetch the whole track from us
    // (best-effort; a providing failure must not fail playback).
    let _ = state.rpc.provide_track_root(&root).await;
    Ok(Response::new(bytes))
}

/// Begin a v2 stream: returns immediately, then ticks `on_event` with control events
/// (Skeleton{plan}, Sample{index,frames}, Complete).
#[tauri::command]
async fn start_stream(
    root: String,
    on_event: Channel<ipfs::StreamEvent>,
    state: State<'_, NodeState>,
    streams: State<'_, Streams>,
    held: State<'_, Arc<HeldRoots>>,
) -> Result<(), String> {
    let cid: Cid = root.parse().map_err(|e| format!("bad CID {root}: {e}"))?;
    let rpc = state.rpc.clone();
    let st = Arc::new(ipfs::StreamState::default());
    streams.0.lock().unwrap().insert(root.clone(), st.clone());
    held.mark(&root, false);
    let held = held.inner().clone();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ipfs::StreamEvent>();
    tauri::async_runtime::spawn(async move {
        while let Some(ev) = rx.recv().await {
            let _ = on_event.send(ev);
        }
    });
    let etx = tx.clone();
    tauri::async_runtime::spawn(async move {
        match ipfs::stream_v2(&rpc, cid, st, tx).await {
            Ok(()) => {
                held.mark(&root, true);
                // Advertise the track root now that we hold the stream (best-effort).
                let _ = rpc.provide_track_root(&root).await;
            }
            Err(e) => {
                let _ = etx.send(ipfs::StreamEvent::Error { message: e.to_string() });
            }
        }
    });
    Ok(())
}

#[tauri::command]
async fn get_skeleton(root: String, streams: State<'_, Streams>) -> Result<Response, String> {
    let st = stream_for(&streams, &root)?;
    let data = st.skeleton.lock().unwrap().clone();
    Ok(Response::new(data))
}

#[tauri::command]
async fn get_sample(root: String, index: u32, streams: State<'_, Streams>) -> Result<Response, String> {
    let st = stream_for(&streams, &root)?;
    let data = st
        .samples
        .lock()
        .unwrap()
        .get(&index)
        .cloned()
        .ok_or_else(|| format!("sample {index} not ready for {root}"))?;
    Ok(Response::new(data))
}

#[tauri::command]
fn debug_log(line: String) {
    eprintln!("[UIDBG] {line}");
}

#[tauri::command]
fn set_playhead(root: String, order: u32, streams: State<'_, Streams>) -> Result<(), String> {
    let st = stream_for(&streams, &root)?;
    st.playhead.store(order, std::sync::atomic::Ordering::Relaxed);
    Ok(())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        // Deep links: trackerstream://share/<code> (E2).
        .plugin(tauri_plugin_deep_link::init())
        .setup(|app| {
            #[cfg(any(target_os = "linux", windows))]
            {
                use tauri_plugin_deep_link::DeepLinkExt;
                let _ = app.deep_link().register_all();
            }
            // App data dir holds the sidecar's repo (blockstore + identity) and our caches.
            let dir = app.path().app_data_dir().ok();
            let repo = dir.clone().map(|d| d.join("tsnode")).unwrap_or_else(|| std::env::temp_dir().join("tsnode"));
            let held = Arc::new(HeldRoots::load(dir.as_deref()));
            let ipns_cache = Arc::new(IpnsCache::load(dir.as_deref()));

            // Spawn the tsnode sidecar and wait for its RPC. We hand it the master bootstrap
            // multiaddrs at startup (TS_BOOTSTRAP override, else the packages/config mirror in
            // ipfs::default_bootstrap) so it dials + keepalives the master itself — the catalog
            // resolve no longer races the frontend's keepalive_master.
            let bin = sidecar::locate_binary().map_err(|e| e.to_string())?;
            let bootstrap = ipfs::default_bootstrap();
            let sc = tauri::async_runtime::block_on(sidecar::Sidecar::spawn(&bin, &repo, &bootstrap))
                .map_err(|e| format!("tsnode sidecar failed to start: {e}"))?;
            let rpc = sc.rpc();
            let peer_id = tauri::async_runtime::block_on(rpc.id())
                .map_err(|e| format!("tsnode id: {e}"))?
                .id;
            eprintln!("[tsnode] sidecar up, peer={peer_id}, rpc={}", sc.rpc_addr());

            app.manage(NodeState { rpc, peer_id });
            app.manage(sc); // keep the child alive for the app's lifetime
            app.manage(Streams::default());
            app.manage(held);
            app.manage(ipns_cache);
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            node_info,
            peer_stats,
            peer_detail,
            connect_peer,
            keepalive_master,
            warm_root,
            catalog_query,
            fetch_module,
            start_stream,
            get_skeleton,
            get_sample,
            set_playhead,
            debug_log
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
