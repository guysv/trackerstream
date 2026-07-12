//! trackerstream desktop backend. The libp2p/IPFS data plane now runs as an external Go
//! sidecar (`tsnode`, see `sidecar`/`rpc`); this backend spawns it, drives it over the local
//! kubo-compatible RPC, and keeps the pure logic in Rust: audio reassembly/streaming (`ipfs`),
//! the catalog SQLite VFS (`catalog`), and signed-IPNS verification (`ipns`). The frontend asks
//! the backend to resolve a root CID and gets back reassembled module bytes to play.

pub mod catalog;
pub mod ipfs;
pub mod ipns;
pub mod link;
#[cfg(target_os = "macos")]
pub mod mediakeys_macos;
pub mod playlists;
pub mod rpc;
pub mod safename;
pub mod sidecar;
pub mod trackers;

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

/// Load a JSON map from a durable state file, distinguishing "absent" (first run — silent) from
/// a read/parse failure (corruption or disk fault — logged, then treated as empty so the app still
/// starts). Without the log, a corrupt state file was indistinguishable from a clean first run.
fn load_state_map<T: serde::de::DeserializeOwned + Default>(path: &std::path::Path, what: &str) -> T {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|e| {
            log::warn!("state: {what} parse failed ({e}); starting empty");
            T::default()
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => T::default(),
        Err(e) => {
            log::warn!("state: {what} read failed: {e}");
            T::default()
        }
    }
}

/// Write a value to a durable state file, logging (rather than silently dropping) a serialize or
/// disk-write failure — so a full/read-only disk that's losing held-root / IPNS state is visible.
fn write_state<T: serde::Serialize>(path: &std::path::Path, value: &T, what: &str) {
    match serde_json::to_vec(value) {
        Ok(bytes) => {
            if let Err(e) = std::fs::write(path, bytes) {
                log::warn!("state: {what} write failed: {e}");
            }
        }
        Err(e) => log::warn!("state: {what} serialize failed: {e}"),
    }
}

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
        let map = path.as_ref().map(|p| load_state_map(p, "held_roots")).unwrap_or_default();
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
        write_state(path, &snapshot, "held_roots");
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
        let map = path.as_ref().map(|p| load_state_map(p, "ipns_cache")).unwrap_or_default();
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
        write_state(path, &snapshot, "ipns_cache");
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
    /// Our own AutoNAT verdict: `Some(true)` public, `Some(false)` private, `None`
    /// undecided. Drives the peers-pane reachability badge (UPnP/relay/DCUtR outcome).
    reachable: Option<bool>,
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
    let reachable = match status.as_ref().map(|s| s.reachability.as_str()) {
        Some("public") => Some(true),
        Some("private") => Some(false),
        _ => None,
    };
    let relay = status
        .map(|s| RelayCounts {
            direct: s.relay_stats.direct,
            relayed_peer: s.relay_stats.peer_relay,
            relayed_master: s.relay_stats.master_relay,
            dcutr_upgrades: 0,
        })
        .unwrap_or_default();
    Ok(PeerStats { connected: connected.len(), peers, relay, reachable })
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
    // Surface non-seed catalog sources in the background so page reads can pull from peers, not
    // just the seed; the query proceeds immediately (seed stays the fallback).
    {
        let rpc = state.rpc.clone();
        let root = cid.to_string();
        tauri::async_runtime::spawn(async move {
            let _ = rpc.dial_providers(&root).await;
        });
    }
    catalog::run_query(state.rpc.clone(), cid, req).await
}

/// Streaming search: resolves the catalog and emits each hit on `on_row` as SQLite steps to it
/// (its pages arrive over the VFS), so the UI renders results progressively instead of after the
/// whole page. Returns the total row count when the stream finishes. Shares the cancel/dial-
/// providers plumbing with `catalog_query`.
#[tauri::command]
async fn catalog_search_stream(
    name: String,
    q: String,
    limit: Option<i64>,
    after: Option<i64>,
    names: Option<bool>,
    on_row: Channel<serde_json::Value>,
    cache: State<'_, Arc<IpnsCache>>,
    state: State<'_, NodeState>,
) -> Result<usize, String> {
    let cid = resolve_ipns_name(&name, &cache, &state.rpc).await?;
    {
        let rpc = state.rpc.clone();
        let root = cid.to_string();
        tauri::async_runtime::spawn(async move {
            let _ = rpc.dial_providers(&root).await;
        });
    }
    catalog::run_search_stream(state.rpc.clone(), cid, q, limit.unwrap_or(60), after, names.unwrap_or(false), move |row| {
        let _ = on_row.send(row);
    })
    .await
}

/// Cancel any in-flight catalog query — its VFS reads abort at the next page boundary and the
/// in-flight Bitswap cats are dropped (tsnode aborts them). Called by the search box when a new
/// query supersedes the last (or the box is cleared) so a stale search stops pulling pages.
#[tauri::command]
fn catalog_cancel() {
    catalog::cancel_inflight();
}

/// Prewarm the catalog page cache (schema + FTS upper tree) so the first search after the box
/// opens descends from warm pages instead of paying cold schema/FTS-root round-trips. Called
/// fire-and-forget from the search page on mount; best-effort.
#[tauri::command]
async fn catalog_warm(
    name: String,
    cache: State<'_, Arc<IpnsCache>>,
    state: State<'_, NodeState>,
) -> Result<(), String> {
    let cid = resolve_ipns_name(&name, &cache, &state.rpc).await?;
    {
        let rpc = state.rpc.clone();
        let root = cid.to_string();
        tauri::async_runtime::spawn(async move {
            let _ = rpc.dial_providers(&root).await;
        });
    }
    catalog::warm(state.rpc.clone(), cid).await
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
    // Surface non-seed providers in the background so bitswap can pull from peers, not just the
    // seed — the fetch proceeds immediately (seed stays the fallback).
    {
        let rpc = state.rpc.clone();
        let root = root.clone();
        tauri::async_runtime::spawn(async move {
            let _ = rpc.dial_providers(&root).await;
        });
    }
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
    // Surface non-seed providers in the background (peers, incl. same-LAN) so the stream pulls
    // from them rather than only the seed; streaming starts immediately regardless.
    {
        let rpc = rpc.clone();
        let root = root.clone();
        tauri::async_runtime::spawn(async move {
            let _ = rpc.dial_providers(&root).await;
        });
    }

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

/// Publish the current track + play state to the OS "Now Playing" widget so the system routes
/// hardware media keys and headphone transport buttons back to us (macOS only; a no-op elsewhere,
/// where the frontend's global-shortcut path handles the keys). Called from the frontend on every
/// track change, play/pause and seek (src/lib/mediaKeys.ts).
///
/// `has_next`/`has_prev` gate the widget's skip buttons, so the OS greys out what we can't service.
#[tauri::command]
#[allow(clippy::too_many_arguments)]
fn update_now_playing(
    app: tauri::AppHandle,
    title: String,
    artist: String,
    playing: bool,
    duration: f64,
    elapsed: f64,
    has_next: bool,
    has_prev: bool,
) {
    #[cfg(target_os = "macos")]
    mediakeys_macos::update_now_playing(
        &app, title, artist, playing, duration, elapsed, has_next, has_prev,
    );
    #[cfg(not(target_os = "macos"))]
    let _ = (app, title, artist, playing, duration, elapsed, has_next, has_prev);
}

/// Give the OS "Now Playing" slot back while we have nothing loaded, so an idle trackerstream
/// doesn't sit in Control Center holding the media keys hostage from the app the user is actually
/// listening to (macOS only; a no-op elsewhere). Called from the frontend whenever no track is
/// loaded, and on quit (below).
#[tauri::command]
fn clear_now_playing(app: tauri::AppHandle) {
    #[cfg(target_os = "macos")]
    mediakeys_macos::clear_now_playing(&app);
    #[cfg(not(target_os = "macos"))]
    let _ = app;
}

/// Reveal the app's log directory (where tauri-plugin-log writes `trackerstream.log`), so a
/// user can grab the file for a bug report without running the app from a terminal.
#[tauri::command]
fn open_logs_dir(app: tauri::AppHandle) -> Result<(), String> {
    let dir = app.path().app_log_dir().map_err(|e| e.to_string())?;
    tauri_plugin_opener::OpenerExt::opener(&app)
        .open_path(dir.to_string_lossy(), None::<&str>)
        .map_err(|e| e.to_string())
}

/// Result of a "download + open with" so the UI can report where the file landed and whether the
/// external tracker actually launched (the download half succeeds independently of the launch).
#[derive(Serialize)]
struct DownloadResult {
    path: String,
    launched: bool,
    launch_error: Option<String>,
}

/// Nudge the macOS Dock's Downloads stack to register + bounce for a freshly-saved file — the same
/// `com.apple.DownloadFileFinished` distributed notification Safari/Chrome post when a download
/// completes. Best-effort: we shell out to JXA (`osascript -l JavaScript`) so we don't have to pull
/// in an Objective-C dependency, and pass the path via env var so no shell/AppleScript quoting can
/// break on odd filenames. Any failure is swallowed — the file is already in Downloads regardless.
#[cfg(target_os = "macos")]
fn notify_downloads_stack(path: &str) {
    const JS: &str = "ObjC.import('Foundation');\
        var p = $.NSProcessInfo.processInfo.environment.objectForKey('TS_DL_PATH');\
        if (p) $.NSDistributedNotificationCenter.defaultCenter\
            .postNotificationNameObjectUserInfoDeliverImmediately('com.apple.DownloadFileFinished', p, $(), true);";
    let _ = std::process::Command::new("osascript")
        .args(["-l", "JavaScript", "-e", JS])
        .env("TS_DL_PATH", path)
        .spawn();
}

#[cfg(not(target_os = "macos"))]
fn notify_downloads_stack(_path: &str) {}

/// Reassemble a module's BYTE-EXACT original from its root CID (v3/v4 streaming root or v1/flat
/// root — dispatched on manifest version), verify it against the catalog `md5`, write it to the OS
/// Downloads dir, and open it with an external tracker (schismtracker / milkytracker). The desktop
/// "Open with" path. `open_with` is an opaque tracker ID from `list_installed_trackers`
/// ("schismtracker" | "milkytracker" | "openmpt"), NOT a path — the backend resolves it against the
/// static registry, because `open` executes that value as a program (see `trackers::resolve_id`).
/// None opens with the OS default. The file is saved BEFORE launching, so a missing tracker still
/// leaves the rebuilt module in Downloads (launched=false + launch_error). See REBUILD.md.
/// Detect which external trackers are actually installed, so the "Open with" menu offers only
/// launchable ones (resolved lazily when the user hovers the submenu). Pure filesystem probe — no
/// process is spawned. Each result carries the resolved `open_with` `target` to hand straight back
/// to `download_and_open`. See `trackers`.
#[tauri::command]
fn list_installed_trackers() -> Vec<trackers::TrackerInfo> {
    trackers::installed()
}

#[tauri::command]
async fn download_and_open(
    app: tauri::AppHandle,
    root: String,
    md5: String,
    filename: String,
    open_with: Option<String>,
    state: State<'_, NodeState>,
    held: State<'_, Arc<HeldRoots>>,
) -> Result<DownloadResult, String> {
    let cid: Cid = root.parse().map_err(|e| format!("bad CID {root}: {e}"))?;
    held.mark(&root, false);
    // Surface non-seed providers in the background so bitswap can pull the blocks from peers.
    {
        let rpc = state.rpc.clone();
        let root = root.clone();
        tauri::async_runtime::spawn(async move {
            let _ = rpc.dial_providers(&root).await;
        });
    }
    // Byte-exact reconstruction (v3/v4 -> reassemble_v3, FLAC-decoding v4 leaves; v1/flat ->
    // reassemble). Every block is CID-verified; the DAG is byte-exact by construction (REBUILD.md /
    // repack test/v3-roundtrip + v4-roundtrip).
    let bytes = ipfs::reassemble_any(&state.rpc, cid)
        .await
        .map_err(|e| format!("reassemble {root} failed: {e}"))?;
    held.mark(&root, true);

    // MD5 parity gate: guards against a wrong/mismatched root. The catalog md5 is the raw-file
    // md5 (lowercase hex). Empty md5 (older catalog row) -> skip.
    if !md5.is_empty() {
        let got = format!("{:x}", md5::compute(&bytes));
        if got != md5.to_lowercase() {
            return Err(format!("md5 mismatch for {filename}: expected {md5}, rebuilt {got}"));
        }
    }

    // Write to the OS Downloads dir. The catalog filename is ingested verbatim from the archive and
    // the bytes are whatever the DAG held, so the name is sanitized (single component, no ADS
    // selector, no reserved device name, module extension enforced) before it can name a file —
    // otherwise a `song.exe` row would drop a runnable executable here. See `safename`.
    let dir = app.path().download_dir().map_err(|e| format!("no Downloads dir: {e}"))?;
    let base = safename::safe_download_name(&filename, &root);
    let out = dir.join(&base);
    std::fs::write(&out, &bytes).map_err(|e| format!("write {}: {e}", out.display()))?;
    let path = out.to_string_lossy().into_owned();

    // Tag it as internet-sourced (Windows Mark-of-the-Web) so Defender/SmartScreen still interpose
    // if anything ever slips past the extension allowlist. Best-effort. No-op off Windows.
    mark_of_the_web(&out);

    // Register the file with the macOS Dock Downloads stack (bounce + stack entry) now that the
    // bytes are on disk — matches the OS "download finished" affordance. No-op off macOS.
    notify_downloads_stack(&path);

    // Resolve the tracker id to a launch target IN THE BACKEND. `open` executes this string as a
    // program, so it must come from our static registry — never from the webview. An unknown or
    // uninstalled id fails closed (no launch, no OS-default fallback); the file is already saved.
    let with = match open_with.as_deref().filter(|s| !s.is_empty()) {
        Some(id) => match trackers::resolve_id(id) {
            Some(target) => Some(target),
            None => {
                log::warn!("open-with: refusing unknown tracker id {id:?}");
                let launch_error = Some(format!("unknown or uninstalled tracker: {id}"));
                return Ok(DownloadResult { path, launched: false, launch_error });
            }
        },
        None => None,
    };

    // Launch the external tracker on the saved file. Download already succeeded, so a launch
    // failure is reported (launched=false) but not fatal.
    match tauri_plugin_opener::OpenerExt::opener(&app).open_path(&path, with) {
        Ok(()) => Ok(DownloadResult { path, launched: true, launch_error: None }),
        Err(e) => Ok(DownloadResult { path, launched: false, launch_error: Some(e.to_string()) }),
    }
}

/// Windows Mark-of-the-Web: browsers tag downloaded files with a `Zone.Identifier` alternate data
/// stream so SmartScreen/Defender gate them and Office opens them in Protected View. We write bytes
/// pulled off a P2P network, so we owe the OS the same signal. Best-effort: the stream needs NTFS
/// (a FAT/exFAT Downloads dir just won't take it), and failing to tag must never fail the download.
///
/// Not done on macOS: `com.apple.quarantine` only gates executables/bundles, so it buys nothing for
/// a `.mod` while making some apps nag "downloaded from the internet" on every "Open with".
#[cfg(target_os = "windows")]
fn mark_of_the_web(path: &std::path::Path) {
    // ZoneId=3 is URLZONE_INTERNET.
    let stream = format!("{}:Zone.Identifier", path.display());
    if let Err(e) = std::fs::write(&stream, "[ZoneTransfer]\r\nZoneId=3\r\n") {
        log::warn!("mark-of-the-web: {} ({e})", path.display());
    }
}

#[cfg(not(target_os = "windows"))]
fn mark_of_the_web(_path: &std::path::Path) {}

#[tauri::command]
fn set_playhead(root: String, order: u32, streams: State<'_, Streams>) -> Result<(), String> {
    let st = stream_for(&streams, &root)?;
    st.playhead.store(order, std::sync::atomic::Ordering::Relaxed);
    Ok(())
}

// ---- playlists (PLAYLISTS.md) — thin wrappers over playlists::Playlists ----

#[tauri::command]
fn playlist_search(
    q: String,
    pl: State<'_, Arc<playlists::Playlists>>,
) -> Result<Vec<playlists::PlaylistMeta>, String> {
    pl.search(&q).map_err(|e| e.to_string())
}

#[tauri::command]
fn playlist_list(
    scope: Option<String>,
    pl: State<'_, Arc<playlists::Playlists>>,
) -> Result<Vec<playlists::PlaylistMeta>, String> {
    pl.list(scope.as_deref().unwrap_or("all")).map_err(|e| e.to_string())
}

/// Add/remove a foreign playlist to/from the library (the "holder" tier — backed,
/// never evicted, re-announced). Pushes the disclosure-set manifest right away so
/// peer-list answers and beacons reflect the change without waiting a sync tick.
#[tauri::command]
async fn playlist_hold(
    name: String,
    held: bool,
    pl: State<'_, Arc<playlists::Playlists>>,
) -> Result<(), String> {
    pl.set_held(&name, held).map_err(|e| e.to_string())?;
    pl.push_manifest().await;
    Ok(())
}

#[tauri::command]
fn playlist_get(
    name: String,
    pl: State<'_, Arc<playlists::Playlists>>,
) -> Result<Option<playlists::PlaylistDetail>, String> {
    pl.get(&name).map_err(|e| e.to_string())
}

#[tauri::command]
async fn playlist_create(
    title: String,
    tracks: Vec<(String, String, String)>,
    pl: State<'_, Arc<playlists::Playlists>>,
) -> Result<playlists::PlaylistMeta, String> {
    pl.create(title, tracks).await.map_err(|e| e.to_string())
}

#[tauri::command]
async fn playlist_update(
    name: String,
    title: String,
    tracks: Vec<(String, String, String)>,
    pl: State<'_, Arc<playlists::Playlists>>,
) -> Result<(), String> {
    pl.update(&name, title, tracks).await.map_err(|e| e.to_string())
}

#[tauri::command]
async fn playlist_delete(name: String, pl: State<'_, Arc<playlists::Playlists>>) -> Result<(), String> {
    pl.delete(&name).await.map_err(|e| e.to_string())?;
    pl.push_manifest().await;
    Ok(())
}

/// The explicit "Share" action — the ONLY path that makes a playlist public.
#[tauri::command]
async fn playlist_publish(name: String, pl: State<'_, Arc<playlists::Playlists>>) -> Result<(), String> {
    pl.publish(&name).await.map_err(|e| e.to_string())?;
    pl.push_manifest().await;
    Ok(())
}

/// "Make private again" — best-effort tombstone retract, then keep the playlist local
/// and editable (published=0). Distinct from delete, which also drops the data.
#[tauri::command]
async fn playlist_unpublish(name: String, pl: State<'_, Arc<playlists::Playlists>>) -> Result<(), String> {
    pl.unpublish(&name).await.map_err(|e| e.to_string())?;
    pl.push_manifest().await;
    Ok(())
}

#[tauri::command]
fn playlist_played(name: String, pl: State<'_, Arc<playlists::Playlists>>) -> Result<(), String> {
    pl.mark_played(&name).map_err(|e| e.to_string())
}

/// Pin the playlist the player is sourced from (None when playback moves elsewhere):
/// pinned rows can't be evicted, decayed, or tombstone-deleted out from under playback.
#[tauri::command]
fn playlist_pin(name: Option<String>, pl: State<'_, Arc<playlists::Playlists>>) {
    pl.pin_playing(name);
}

#[tauri::command]
fn playlist_sync_status(pl: State<'_, Arc<playlists::Playlists>>) -> Result<playlists::SyncStatus, String> {
    pl.status().map_err(|e| e.to_string())
}

// ---- liked tracks (the private, per-client "Liked Tracks" playlist — Spotify-style) ----

/// Toggle a track's membership in "Liked Tracks" (the ♥ button). Creates the private
/// liked playlist on first use; returns `true` if the track is now liked. Never
/// published — private by construction.
#[tauri::command]
async fn playlist_like_toggle(
    track: (String, String, String),
    pl: State<'_, Arc<playlists::Playlists>>,
) -> Result<bool, String> {
    pl.like_toggle(track).await.map_err(|e| e.to_string())
}

/// The track md5s currently liked — the set the heart buttons read (empty if none yet).
#[tauri::command]
fn playlist_liked_ids(pl: State<'_, Arc<playlists::Playlists>>) -> Result<Vec<String>, String> {
    pl.liked_ids().map_err(|e| e.to_string())
}

/// The liked playlist's name (creating it if needed) — for the UI to open it.
#[tauri::command]
async fn playlist_liked_name(pl: State<'_, Arc<playlists::Playlists>>) -> Result<String, String> {
    pl.ensure_liked().await.map_err(|e| e.to_string())
}

// ---- playlist-list peer protocol (PLAYLISTS.md §10, shipped) ----

/// One disclosed playlist of a peer, joined against the local DB for the
/// "in your library / you have this" markers.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PeerPlaylistEntry {
    name: String,
    seq: u64,
    title: String,
    have: bool,
    held: bool,
    mine: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PeerPlaylists {
    supported: bool,
    playlists: Vec<PeerPlaylistEntry>,
}

/// Ask one connected peer for its disclosure set (deliberate 1:1 pull — the peer card's
/// "playlists" section). supported=false = old build or the seed, a normal answer.
#[tauri::command]
async fn peer_playlists(
    peer_id: String,
    state: State<'_, NodeState>,
    pl: State<'_, Arc<playlists::Playlists>>,
) -> Result<PeerPlaylists, String> {
    let (entries, _reann, supported) =
        state.rpc.playlist_peer_list(&peer_id, &[]).await.map_err(|e| e.to_string())?;
    let playlists = entries
        .into_iter()
        .map(|e| {
            let (mine, held, have) = pl.local_flags(&e.name);
            PeerPlaylistEntry { name: e.name, seq: e.seq, title: e.title, have, held, mine }
        })
        .collect();
    Ok(PeerPlaylists { supported, playlists })
}

/// Request a targeted re-announce of one playlist from a peer that holds it (the peer
/// card's "get" button; also the name-only deep-link resolver). The doc then arrives
/// through the normal gossip → sync path. Returns how many entries were re-announced.
#[tauri::command]
async fn playlist_request(
    peer_id: String,
    name: String,
    state: State<'_, NodeState>,
) -> Result<u64, String> {
    let (_, reann, _) =
        state.rpc.playlist_peer_list(&peer_id, &[name]).await.map_err(|e| e.to_string())?;
    Ok(reann)
}

// ---- deep links (PLAYLISTS.md §10, shipped) ----

/// Handle an incoming trackerstream:// or https://trackerstream.xyz/p/ link: verify the
/// fragment payload through the normal ingest path, or chase a name-only link via
/// gossip. Returns {name, status: "ready" | "pending"}.
#[tauri::command]
async fn playlist_ingest_link(
    url: String,
    pl: State<'_, Arc<playlists::Playlists>>,
) -> Result<playlists::LinkStatus, String> {
    pl.ingest_link(&url).await.map_err(|e| e.to_string())
}

/// Build the shareable HTTPS link for a stored playlist (requires a live record).
#[tauri::command]
fn playlist_copy_link(name: String, pl: State<'_, Arc<playlists::Playlists>>) -> Result<String, String> {
    pl.copy_link(&name).map_err(|e| e.to_string())
}

/// Name-only links still waiting on gossip (drives the "syncing…" placeholder).
#[tauri::command]
fn playlist_pending(pl: State<'_, Arc<playlists::Playlists>>) -> Vec<String> {
    pl.pending_names()
}

/// Hold-beacon backer counts for the given names (discover ranking, "backed by ~N").
#[tauri::command]
async fn playlist_backers(
    names: Vec<String>,
    state: State<'_, NodeState>,
) -> Result<HashMap<String, u64>, String> {
    state.rpc.playlist_backers(&names).await.map_err(|e| e.to_string())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
/// Parse a RUST_LOG-style `TS_LOG` value into a global level plus per-target overrides.
/// A bare level token sets the default (`debug`); a `topic=level` pair raises one target
/// (`playlist=debug`). Order-independent; the last bare level wins. Unparsable tokens are
/// ignored so a malformed `TS_LOG` can never panic startup. Default when unset: info.
fn parse_ts_log(raw: Option<&str>) -> (log::LevelFilter, Vec<(String, log::LevelFilter)>) {
    let mut level = log::LevelFilter::Info;
    let mut overrides = Vec::new();
    let Some(raw) = raw else { return (level, overrides) };
    for tok in raw.split(',').map(str::trim).filter(|t| !t.is_empty()) {
        match tok.split_once('=') {
            // Bare level → the global default (last one wins).
            None => {
                if let Ok(l) = tok.parse::<log::LevelFilter>() {
                    level = l;
                }
            }
            // topic=level → raise/lower a single target.
            Some((target, lvl)) => {
                if let Ok(l) = lvl.trim().parse::<log::LevelFilter>() {
                    overrides.push((ts_log_target(target.trim()), l));
                }
            }
        }
    }
    (level, overrides)
}

/// Map a friendly topic alias to its real log target; unknown names pass through verbatim
/// so full module paths (`desktop_lib::playlists`) still work. Territories: `playlist` the
/// playlist subsystem (`playlists.rs`), `stream` the streaming path (`ipfs.rs`), `webview`
/// the frontend (`debug.ts`); and three carved out of the sidecar stream by
/// `sidecar::classify_tsnode` — `dial` the connection-failure chatter, `nat` NAT-traversal
/// signal (reachability / hole punch / relay), `tsnode` everything else. `reqwest` is the
/// HTTP client's own logging.
fn ts_log_target(alias: &str) -> String {
    match alias {
        "playlist" | "playlists" | "sync" => "playlist",
        "tsnode" | "node" => "tsnode",
        "dial" => "dial",
        "nat" | "holepunch" | "hp" => "nat",
        "webview" | "ui" | "frontend" => "webview",
        "catalog" => "desktop_lib::catalog",
        "stream" => "stream",
        "reqwest" | "net" | "http" => "reqwest",
        other => other,
    }
    .to_string()
}

pub fn run() {
    // The client log hub: everything routed through the `log` facade (backend, frontend via
    // the plugin's JS API, tsnode sidecar output via sidecar.rs) lands in one rotating file
    // in the OS app-log dir (macOS: ~/Library/Logs/xyz.trackerstream/) plus stdout for dev.
    // TS_LOG is RUST_LOG-style so one topic can be traced without the global firehose: a
    // bare level sets the default (`TS_LOG=debug`), and `topic=level` pairs raise a single
    // target (`TS_LOG=info,playlist=debug` times a share landing; see `parse_ts_log` for
    // the topic aliases). Default is info everywhere; a runtime knob, no rebuild needed.
    let (level, overrides) = parse_ts_log(std::env::var("TS_LOG").ok().as_deref());
    let mut log_builder = tauri_plugin_log::Builder::new().level(level);
    for (target, lvl) in overrides {
        log_builder = log_builder.level_for(target, lvl);
    }
    let log_plugin = log_builder
        .targets([
            tauri_plugin_log::Target::new(tauri_plugin_log::TargetKind::Stdout),
            tauri_plugin_log::Target::new(tauri_plugin_log::TargetKind::LogDir {
                file_name: Some("trackerstream".into()),
            }),
        ])
        .max_file_size(2_000_000)
        .rotation_strategy(tauri_plugin_log::RotationStrategy::KeepAll)
        .build();
    let builder = tauri::Builder::default()
        // Single-instance MUST be the first plugin registered (Tauri docs): on
        // Win/Linux a deep-link click launches a second process, whose argv URL the
        // "deep-link" feature forwards into onOpenUrl on THIS instance — registered
        // any later, that URL is lost. The callback just surfaces the window.
        .plugin(tauri_plugin_single_instance::init(|app, _argv, _cwd| {
            if let Some(w) = app.get_webview_window("main") {
                let _ = w.unminimize();
                let _ = w.set_focus();
            }
        }))
        .plugin(log_plugin)
        .plugin(tauri_plugin_opener::init())
        // Deep links: trackerstream://share/<code> (E2).
        .plugin(tauri_plugin_deep_link::init());

    // Media keys on Windows/Linux: the plugin is only the register/unregister bridge; the frontend
    // (src/lib/mediaKeys.ts) grabs MediaPlayPause/Next/Previous — lazily, once something is
    // actually playing — and routes each press into the JS playback controls where all playback
    // state lives. macOS can't capture the media keys this way at all (Carbon hotkeys don't see
    // NSSystemDefined events) and uses the native MediaPlayer bridge instead, so the plugin isn't
    // compiled in there — see mediakeys_macos.rs and the platform-scoped capability.
    #[cfg(not(target_os = "macos"))]
    let builder = builder.plugin(tauri_plugin_global_shortcut::Builder::new().build());

    builder
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
            log::info!("sidecar up, peer={peer_id}, rpc={}", sc.rpc_addr());

            // Playlists: the durable local store + sync/announce engine (PLAYLISTS.md).
            let pl = Arc::new(
                playlists::Playlists::open(dir.as_deref(), rpc.clone())
                    .map_err(|e| format!("playlists store: {e}"))?,
            );
            tauri::async_runtime::spawn(playlists::run_loops(pl.clone(), app.handle().clone()));

            app.manage(NodeState { rpc, peer_id });
            app.manage(sc); // keep the child alive for the app's lifetime
            app.manage(Streams::default());
            app.manage(held);
            app.manage(ipns_cache);
            app.manage(pl);

            // macOS: install the MediaPlayer remote-command handlers (hardware media keys +
            // headset buttons). They stay disabled, and we publish no Now Playing entry, until
            // the user actually plays something — an idle app must not hold the media keys.
            // Must run on the main thread; the setup hook is. Win/Linux use global shortcuts.
            #[cfg(target_os = "macos")]
            mediakeys_macos::init(app.handle());

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
            catalog_search_stream,
            catalog_cancel,
            catalog_warm,
            fetch_module,
            start_stream,
            get_skeleton,
            get_sample,
            set_playhead,
            update_now_playing,
            clear_now_playing,
            open_logs_dir,
            list_installed_trackers,
            download_and_open,
            playlist_search,
            playlist_list,
            playlist_get,
            playlist_hold,
            playlist_create,
            playlist_update,
            playlist_delete,
            playlist_publish,
            playlist_unpublish,
            playlist_played,
            playlist_pin,
            playlist_sync_status,
            playlist_like_toggle,
            playlist_liked_ids,
            playlist_liked_name,
            peer_playlists,
            playlist_request,
            playlist_ingest_link,
            playlist_copy_link,
            playlist_pending,
            playlist_backers
        ])
        .build(tauri::generate_context!())
        .expect("error while building tauri application")
        .run(|_app, _event| {
            // Hand the OS "Now Playing" slot back on quit instead of leaving a ghost entry behind
            // that still claims the media keys. Exit runs on the main thread, and the event loop
            // is already winding down, so a run_on_main_thread hop would never be delivered.
            #[cfg(target_os = "macos")]
            if matches!(_event, tauri::RunEvent::Exit) {
                mediakeys_macos::clear_now_playing_blocking();
            }
        });
}
