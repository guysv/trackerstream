//! trackerstream desktop backend. Hosts the in-process IPFS data-plane node
//! (see `ipfs`) and exposes it to the Svelte frontend as Tauri commands. The
//! frontend never fetches module files over HTTP — it asks the embedded node to
//! resolve a root CID and gets back the reassembled module bytes to play.

pub mod ipfs;
pub mod ipns;
pub mod pinglog;
pub mod tracker;

use cid::Cid;
use rust_ipfs::{Ipfs, PeerId};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tauri::ipc::{Channel, Response};
use tauri::{Manager, State};

/// A duration with ±25% random jitter. Fixed retry intervals make every client fire in
/// lockstep — a synchronized thundering herd against the master rcmgr during a
/// correlated outage. Used by the announce / roster / master-reconnect loops. See
/// P2P-NEXT-STEPS Phase 0.
pub(crate) fn jittered(base: Duration) -> Duration {
    base.mul_f64(rand::random::<f64>() * 0.5 + 0.75) // 0.75 .. 1.25
}

/// Embedded node handle (started once at setup; `Ipfs` is a cheap clonable,
/// thread-safe handle, unlike the non-Send builder it came from).
struct IpfsState {
    ipfs: Ipfs,
    peer_id: String,
}

/// In-flight (or finished) v2 streams, keyed by root CID string. Each holds the
/// assembled skeleton + decoded sample PCM as it arrives + the live playhead.
#[derive(Default)]
struct Streams(Mutex<HashMap<String, Arc<ipfs::StreamState>>>);

/// Roots this client holds in its blockstore: `root CID -> fully complete?`.
/// Net-new bookkeeping — the blockstore has no root index and nothing pins, so we
/// record a root as partial the moment we begin holding it and upgrade it to
/// complete when the whole DAG is in hand. The announce loop (peer-assist tracker)
/// publishes these as the roots this client can serve. Persisted to
/// `held_roots.json` so it survives restarts — safe because there is no blockstore
/// GC, so a `complete` root stays complete.
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

    pub(crate) fn roots(&self) -> Vec<String> {
        self.map.lock().unwrap().keys().cloned().collect()
    }

    fn persist(&self) {
        let Some(path) = &self.path else { return };
        let snapshot = self.map.lock().unwrap().clone();
        if let Ok(bytes) = serde_json::to_vec(&snapshot) {
            std::fs::write(path, bytes).ok();
        }
    }
}

/// Last-known online roster, persisted to `roster_cache.json` next to the blockstore.
/// Written through on every non-empty roster pull; read once on startup so a restart
/// — even during a tracker/box outage — immediately re-dials last session's peers
/// instead of waiting on a (possibly dead) tracker. On a normal restart it also warms
/// the set a full roster round-trip sooner. Stale addrs are harmless (Noise binds the
/// connection to the PeerId, so a wrong addr is a failed dial, never a wrong peer) and
/// a fresh roster tick supersedes the cache within ~30s. This is the on-disk seed of
/// PEX (P2P-NEXT-STEPS Phase 1): the durable form of the address book PEX will gossip.
pub(crate) struct RosterCache {
    path: Option<std::path::PathBuf>,
}

impl RosterCache {
    fn load(dir: Option<&std::path::Path>) -> Self {
        RosterCache { path: dir.map(|d| d.join("roster_cache.json")) }
    }

    /// The cached roster — empty on a missing/corrupt file or no data dir.
    fn read(&self) -> Vec<tracker::PeerRef> {
        self.path
            .as_ref()
            .and_then(|p| std::fs::read(p).ok())
            .and_then(|b| serde_json::from_slice::<Vec<tracker::PeerRef>>(&b).ok())
            .unwrap_or_default()
    }

    /// Write-through the latest roster (best-effort; errors swallowed like HeldRoots).
    fn persist(&self, roster: &[tracker::PeerRef]) {
        let Some(path) = &self.path else { return };
        if let Ok(bytes) = serde_json::to_vec(roster) {
            std::fs::write(path, bytes).ok();
        }
    }
}

/// Verified IPNS records cached to `ipns_cache.json` (name -> base64 record). Lets the
/// thin client resolve the catalog name through a short tracker/box outage with zero
/// network: the cached record is re-verified (signature + EOL) on every read, so a
/// stale/expired/forged entry simply fails and falls through to the tracker. Spans an
/// outage only as far as the record's EOL — the master should publish IPNS with a
/// generous validity window (~24–48h). See P2P-NEXT-STEPS Phase 0.
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

    /// Cache a freshly-verified record (write-through, like HeldRoots::mark).
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

/// Cap on warm (peer-assist) connections. The master is never counted here, so it
/// is structurally exempt from eviction.
const WARM_CAP: usize = 24;

/// The bounded set of warm peer-assist connections. Each warmed holder is tagged
/// with the root(s) that caused warming + a last-used stamp for LRU eviction. This
/// is the client's connection budget for offload — candidates come only from the
/// tracker (never a DHT crawl), so churn is bounded. See PEER-ASSIST.md §2.3.
#[derive(Default)]
struct WarmSet(Mutex<HashMap<PeerId, WarmEntry>>);

struct WarmEntry {
    roots: HashSet<String>,
    last_used: Instant,
}

impl WarmSet {
    fn record(&self, pid: PeerId, root: &str) {
        let mut m = self.0.lock().unwrap();
        let e = m.entry(pid).or_insert_with(|| WarmEntry {
            roots: HashSet::new(),
            last_used: Instant::now(),
        });
        e.roots.insert(root.to_string());
        e.last_used = Instant::now();
    }

    /// Currently-warm peers — for the peers-pane `role` tag (B6 telemetry).
    fn members(&self) -> HashSet<PeerId> {
        self.0.lock().unwrap().keys().copied().collect()
    }

    /// Why this peer is warm: the root CID(s) that pulled it in, or "roster".
    /// Empty if it isn't in the warm set. For the peer-detail view.
    fn reason(&self, pid: &PeerId) -> Vec<String> {
        self.0
            .lock()
            .unwrap()
            .get(pid)
            .map(|e| e.roots.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Remove + return the LRU peers above `WARM_CAP` so the caller can disconnect
    /// them. Just-warmed holders have the newest stamp, so they're never victims.
    fn take_overflow(&self) -> Vec<PeerId> {
        let mut m = self.0.lock().unwrap();
        if m.len() <= WARM_CAP {
            return vec![];
        }
        let overflow = m.len() - WARM_CAP;
        let mut by_age: Vec<(PeerId, Instant)> =
            m.iter().map(|(k, v)| (*k, v.last_used)).collect();
        by_age.sort_by_key(|(_, t)| *t); // oldest first
        let victims: Vec<PeerId> = by_age.into_iter().take(overflow).map(|(k, _)| k).collect();
        for v in &victims {
            m.remove(v);
        }
        victims
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
}

#[tauri::command]
async fn node_info(state: State<'_, IpfsState>) -> Result<NodeInfo, String> {
    let listening = state
        .ipfs
        .listening_addresses()
        .await
        .map(|addrs| addrs.iter().map(|a| a.to_string()).collect())
        .unwrap_or_default();
    Ok(NodeInfo {
        peer_id: state.peer_id.clone(),
        listening,
    })
}

#[derive(Serialize)]
struct PeerEntry {
    /// Peer id (frontend labels the master via config MASTER_PEER_ID).
    id: String,
    /// Cumulative Bitswap block bytes exchanged with this peer (real wire bytes
    /// from the rust-ipfs patch). Persist across disconnects.
    down: u64,
    up: u64,
    /// Whether the peer is connected right now. False -> the UI grays the row but
    /// keeps it (and its totals); a later reconnect continues from the same totals.
    connected: bool,
    /// Offload role: "master" (the always-on seed), "warm" (a peer-assist holder we
    /// pre-connected), or "other". Lets the peers pane prove offload: a "warm" peer
    /// accruing `down` while the master stays flat = bytes came from a peer, not the
    /// master. See PEER-ASSIST.md §B6.
    role: &'static str,
}

#[derive(Serialize)]
struct PeerStats {
    /// Count of currently-connected peers (the `peers · N` toggle).
    connected: usize,
    /// Connected peers UNION every peer that has ever transferred — so peers that
    /// did up/down then dropped remain (grayed) until they reconnect.
    peers: Vec<PeerEntry>,
}

/// Snapshot for the peers pane. Polled ~1/s by the frontend, which derives per-peer
/// up/down speed from the byte-counter deltas.
#[tauri::command]
async fn peer_stats(
    state: State<'_, IpfsState>,
    warm: State<'_, Arc<WarmSet>>,
) -> Result<PeerStats, String> {
    let connected: HashSet<PeerId> = state
        .ipfs
        .connected()
        .await
        .unwrap_or_default()
        .into_iter()
        .collect();
    let bw = ipfs::peer_bandwidth();
    let master = ipfs::master_peer_id();
    let warm_members = warm.members();
    let mut ids: HashSet<PeerId> = bw.keys().copied().collect();
    ids.extend(connected.iter().copied());
    let peers = ids
        .into_iter()
        .map(|p| {
            let (down, up) = bw.get(&p).copied().unwrap_or((0, 0));
            let role = if master == Some(p) {
                "master"
            } else if warm_members.contains(&p) {
                "warm"
            } else {
                "other"
            };
            PeerEntry { id: p.to_string(), down, up, connected: connected.contains(&p), role }
        })
        .collect();
    Ok(PeerStats { connected: connected.len(), peers })
}

/// Rich, on-demand per-peer info for the peers-pane detail view. Heavier than
/// peer_stats (live connection addrs + an identify lookup), so it's fetched only
/// while a peer is selected, not in the 1 Hz list poll. All fields are sourced
/// from the local node (Bucket A) — no tracker call.
#[derive(Serialize)]
struct PeerDetail {
    id: String,
    connected: bool,
    role: &'static str,
    /// Root CID(s) that warmed this peer, or "roster"; empty if not warm.
    warm_reason: Vec<String>,
    down: u64,
    up: u64,
    /// Live connected multiaddr(s) for this peer (the actual endpoints).
    addrs: Vec<String>,
    /// True when every live connection is over the master's circuit relay — i.e.
    /// bytes still flow through the master (a non-offload). The §5 relay trap, surfaced.
    relayed: bool,
    /// "quic" | "tcp" | "relay" | "direct" | "unknown".
    transport: String,
    /// identify: peer's user-agent (e.g. trackerstream vs kubo), protocols, and the
    /// address it observes us at.
    agent: Option<String>,
    protocols: Vec<String>,
    observed_addr: Option<String>,
    /// Latest ping RTT in milliseconds, if one has been observed yet.
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
        None => (true, "relay".into()), // every connection is /p2p-circuit
    }
}

#[tauri::command]
async fn peer_detail(
    peer_id: String,
    state: State<'_, IpfsState>,
    warm: State<'_, Arc<WarmSet>>,
) -> Result<PeerDetail, String> {
    let pid: PeerId = peer_id.parse().map_err(|e| format!("bad peer id {peer_id}: {e}"))?;
    let connected = state.ipfs.is_connected(pid).await.unwrap_or(false);
    let (down, up) = ipfs::peer_bandwidth().get(&pid).copied().unwrap_or((0, 0));
    let role = if ipfs::master_peer_id() == Some(pid) {
        "master"
    } else if warm.members().contains(&pid) {
        "warm"
    } else {
        "other"
    };
    let warm_reason = warm.reason(&pid);
    let addrs: Vec<String> = state
        .ipfs
        .addrs()
        .await
        .unwrap_or_default()
        .into_iter()
        .find(|(p, _)| *p == pid)
        .map(|(_, a)| a.iter().map(|m| m.to_string()).collect())
        .unwrap_or_default();
    let (relayed, transport) = classify_transport(&addrs);
    // identify can stall for a peer mid-handshake; cap it so the command stays snappy.
    let (agent, protocols, observed_addr) = match tokio::time::timeout(
        std::time::Duration::from_secs(3),
        state.ipfs.identity(Some(pid)),
    )
    .await
    {
        Ok(Ok(info)) => (
            Some(info.agent_version),
            info.protocols.iter().map(|p| p.to_string()).collect(),
            info.observed_addr.map(|a| a.to_string()),
        ),
        _ => (None, vec![], None),
    };
    let rtt_ms = pinglog::ping_rtt(&peer_id).map(|d| d.as_secs_f64() * 1000.0);
    Ok(PeerDetail {
        id: peer_id,
        connected,
        role,
        warm_reason,
        down,
        up,
        addrs,
        relayed,
        transport,
        agent,
        protocols,
        observed_addr,
        rtt_ms,
    })
}

#[tauri::command]
async fn connect_peer(addr: String, state: State<'_, IpfsState>) -> Result<(), String> {
    let ipfs = state.ipfs.clone();
    ipfs::connect(&ipfs, &addr)
        .await
        .map_err(|e| format!("connect {addr} failed: {e}"))
}

/// Pin a persistent, auto-reconnecting connection to the master (called once at
/// app mount with the config bootstrap addrs). Without it the master link is
/// dialed lazily per-play and pruned when idle — see ipfs::keepalive_master.
#[tauri::command]
async fn keepalive_master(addrs: Vec<String>, state: State<'_, IpfsState>) -> Result<(), String> {
    ipfs::keepalive_master(&state.ipfs, &addrs)
        .await
        .map_err(|e| format!("keepalive master failed: {e}"))
}

/// Queue-driven pre-connection: when a root enters the player's queue the frontend
/// calls this, BEFORE playback reaches it. We ask the tracker who holds the root
/// and warm-connect those holders so the swarm is pre-formed — by the time Bitswap
/// wants the blocks, they broadcast to already-connected peers and the master is
/// bypassed (no query-then-dial latency on the critical path). Best-effort: on any
/// tracker/dial failure we simply fall back to the master. See PEER-ASSIST.md §2.4.
/// Dial + keepalive a batch of tracker peers into the warm set, then evict LRU
/// overflow. Skips ourselves and the master (the master is kept alive separately by
/// keepalive_master). Shared by queue-driven pre-connection (warm_root) and the
/// roster backbone loop — both need the same dial/record/evict, and keepalive on
/// BOTH ends is what stops a holder dropping us when a download finishes.
async fn warm_into_set(
    ipfs: &Ipfs,
    warm: &WarmSet,
    peers: Vec<tracker::PeerRef>,
    tag: &str,
    self_peer: &str,
) {
    let master = ipfs::master_peer_id();
    for h in peers {
        if h.peer_id == self_peer {
            continue;
        }
        let Ok(pid) = h.peer_id.parse::<PeerId>() else { continue };
        if Some(pid) == master {
            continue;
        }
        match ipfs::warm_connect(ipfs, &h.peer_id, &h.addrs).await {
            Ok(()) => warm.record(pid, tag),
            Err(e) => eprintln!("[warm] {} failed: {e}", h.peer_id),
        }
    }
    for victim in warm.take_overflow() {
        let _ = ipfs.disconnect(victim).await;
        let _ = ipfs.remove_peer(victim).await;
    }
}

#[tauri::command]
async fn warm_root(
    root: String,
    state: State<'_, IpfsState>,
    warm: State<'_, Arc<WarmSet>>,
) -> Result<(), String> {
    // Validate the CID but don't fetch — this only pre-connects.
    let _: Cid = root.parse().map_err(|e| format!("bad CID {root}: {e}"))?;
    let holders = tracker::query_peers(&state.peer_id, &root).await;
    warm_into_set(&state.ipfs, warm.inner(), holders, &root, &state.peer_id).await;
    Ok(())
}

/// Resolve an IPNS name to the CID it currently points at, verifying the signed
/// record locally (PEER-ASSIST.md §9). Seam for the future catalog→IPNS migration:
/// not yet wired into any flow, but lets the thin client resolve `/ipns/<name>`
/// without a DHT once the catalog is published as IPNS.
#[tauri::command]
async fn resolve_ipns(name: String, cache: State<'_, IpnsCache>) -> Result<String, String> {
    // 1. Local cache — re-verify (signature + EOL); a stale/expired/forged entry fails
    //    here and falls through. Zero network when valid: survives a short outage.
    if let Some(b64) = cache.get(&name) {
        if let Ok(cid) = ipns::verify_b64(&name, &b64) {
            return Ok(cid.to_string());
        }
    }
    // 2. Tracker fetch; cache the verified record for the next outage.
    let (b64, cid) = ipns::fetch_record(&name)
        .await
        .map_err(|e| format!("resolve_ipns {name} failed: {e}"))?;
    cache.put(&name, &b64);
    Ok(cid.to_string())
}

/// Resolve a module root CID to its exact bytes, sourced 100% from CID blocks
/// over libp2p. Returns raw bytes (Tauri delivers them to JS as an ArrayBuffer).
#[tauri::command]
async fn fetch_module(
    root: String,
    state: State<'_, IpfsState>,
    held: State<'_, Arc<HeldRoots>>,
) -> Result<Response, String> {
    let cid: Cid = root.parse().map_err(|e| format!("bad CID {root}: {e}"))?;
    let ipfs = state.ipfs.clone();
    held.mark(&root, false);
    let bytes = ipfs::reassemble(&ipfs, cid)
        .await
        .map_err(|e| format!("fetch_module {root} failed: {e}"))?;
    held.mark(&root, true); // whole DAG reassembled -> complete holder
    Ok(Response::new(bytes))
}

/// Begin a v2 stream: returns immediately, then ticks `on_event` with control
/// events (Skeleton{plan}, Sample{index,frames}, Complete). The frontend pulls
/// the binary skeleton / sample PCM via get_skeleton / get_sample and feeds them
/// to the immortal-instance worklet (init + provideSample).
#[tauri::command]
async fn start_stream(
    root: String,
    on_event: Channel<ipfs::StreamEvent>,
    state: State<'_, IpfsState>,
    streams: State<'_, Streams>,
    held: State<'_, Arc<HeldRoots>>,
) -> Result<(), String> {
    let cid: Cid = root.parse().map_err(|e| format!("bad CID {root}: {e}"))?;
    let ipfs = state.ipfs.clone();
    let st = Arc::new(ipfs::StreamState::default());
    streams.0.lock().unwrap().insert(root.clone(), st.clone());
    held.mark(&root, false); // holding (partial) the moment streaming starts
    let held = held.inner().clone();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ipfs::StreamEvent>();
    tauri::async_runtime::spawn(async move {
        while let Some(ev) = rx.recv().await {
            let _ = on_event.send(ev);
        }
    });
    let etx = tx.clone();
    tauri::async_runtime::spawn(async move {
        match ipfs::stream_v2(&ipfs, cid, st, tx).await {
            // A completed stream drains the whole DAG -> this client is a full
            // holder. A user-skip before completion leaves the root partial.
            Ok(()) => held.mark(&root, true),
            Err(e) => {
                let _ = etx.send(ipfs::StreamEvent::Error { message: e.to_string() });
            }
        }
    });
    Ok(())
}

/// The assembled skeleton bytes for a stream (delivered to JS as an ArrayBuffer,
/// then init'd as the immortal instance). Ready once the Skeleton event fired.
#[tauri::command]
async fn get_skeleton(root: String, streams: State<'_, Streams>) -> Result<Response, String> {
    let st = stream_for(&streams, &root)?;
    let data = st.skeleton.lock().unwrap().clone();
    Ok(Response::new(data))
}

/// One streamed sample's decoded PCM bytes. Ready once its Sample event fired.
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

/// Frontend debug bridge: the webview console isn't visible in the dev terminal,
/// so the UI streaming-state tracer (lib/debug.ts) forwards transitions here and
/// we print them to stderr alongside the RUST_LOG tracing. Gated on the frontend
/// (DEBUG flag) so it's a no-op in normal runs.
#[tauri::command]
fn debug_log(line: String) {
    eprintln!("[UIDBG] {line}");
}

/// Update the live playhead order so the prefetch scheduler reprioritizes around
/// it (closed-loop; also how a seek reseeds the fetch queue).
#[tauri::command]
fn set_playhead(root: String, order: u32, streams: State<'_, Streams>) -> Result<(), String> {
    let st = stream_for(&streams, &root)?;
    st.playhead.store(order, std::sync::atomic::Ordering::Relaxed);
    Ok(())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    // RUST_LOG-driven tracing (rust-ipfs/libp2p/bitswap + our own spans), plus a
    // dedicated layer that captures connexa's ping-RTT log line into pinglog's map
    // for the peers pane. The ping layer has its OWN filter so it works even when
    // RUST_LOG is unset (release runs stay otherwise quiet — fmt is off by default).
    {
        use tracing_subscriber::prelude::*;
        let fmt_layer = tracing_subscriber::fmt::layer().with_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                // Silence connexa's swarm-task ERROR spam: routine outgoing-dial failures
                // (stale roster peers re-dialed on startup, unreachable NAT'd peers) are
                // logged at ERROR but aren't actionable — our code already treats a failed
                // dial as expected. This is a specific-target directive, so it overrides a
                // global RUST_LOG level (e.g. `info`) by specificity; an explicit
                // `RUST_LOG=connexa::task::swarm=…` still customizes it. Does NOT affect the
                // separate connexa::task::ping capture (pinglog runs as its own layer).
                .add_directive(
                    "connexa::task::swarm=off".parse().expect("valid tracing directive"),
                ),
        );
        let ping_layer = pinglog::PingLayer
            .with_filter(tracing_subscriber::EnvFilter::new(pinglog::PING_DIRECTIVE));
        let _ = tracing_subscriber::registry()
            .with(fmt_layer)
            .with(ping_layer)
            .try_init();
    }
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        // Deep links: trackerstream://share/<code> (E2). The frontend handles the
        // URL via @tauri-apps/plugin-deep-link's onOpenUrl/getCurrent.
        .plugin(tauri_plugin_deep_link::init())
        .setup(|app| {
            // On Linux/Windows the scheme must be registered at runtime (macOS
            // registers it from the bundle Info.plist). Harmless if already set.
            #[cfg(any(target_os = "linux", windows))]
            {
                use tauri_plugin_deep_link::DeepLinkExt;
                let _ = app.deep_link().register_all();
            }
            // Persistent blockstore under the app data dir = the client CID cache.
            let dir = app.path().app_data_dir().ok().map(|d| d.join("ipfs"));
            // Held-roots index lives next to the blockstore (load before `dir` moves).
            let held = Arc::new(HeldRoots::load(dir.as_deref()));
            // Roster cache sits beside it — also load before `dir` moves into start().
            let roster_cache = RosterCache::load(dir.as_deref());
            // IPNS record cache — same directory, load before `dir` moves.
            let ipns_cache = IpnsCache::load(dir.as_deref());
            // Start the (non-Send) builder once here, off the command path.
            let node =
                tauri::async_runtime::block_on(ipfs::start(dir)).map_err(|e| e.to_string())?;
            // Clone handles + identity for the background loops before the node
            // moves into managed state.
            let announce_ipfs = node.ipfs.clone();
            let roster_ipfs = node.ipfs.clone();
            let announce_pid = node.peer_id.parse::<PeerId>().ok();
            let self_peer = node.peer_id.clone();
            let warm = Arc::new(WarmSet::default());
            app.manage(IpfsState {
                ipfs: node.ipfs,
                peer_id: node.peer_id,
            });
            app.manage(Streams::default());
            app.manage(held.clone());
            app.manage(warm.clone());
            app.manage(ipns_cache);
            // Peer-assist tracker: announce presence + held roots every ~30s (also
            // the presence heartbeat). Skipped only if our PeerId failed to parse.
            if let Some(pid) = announce_pid {
                tracker::spawn_announce_loop(announce_ipfs, pid, held);
            }
            // Roster backbone: periodically warm a bounded set of ONLINE peers
            // (content-independent), so warm connections are symmetric — both ends
            // keepalive, so a holder no longer drops us when a download finishes —
            // and the presence backbone forms. Bounded + LRU-evicted by WARM_CAP.
            tauri::async_runtime::spawn(async move {
                // Startup: re-dial last session's peers immediately, before the first
                // roster pull. During a tracker/box outage this is the ONLY source of
                // peers; on a normal restart it just warms the set ~30s sooner. Bounded
                // by WARM_CAP; the first live roster tick supersedes it.
                let cached = roster_cache.read();
                if !cached.is_empty() {
                    warm_into_set(&roster_ipfs, &warm, cached, "restore", &self_peer).await;
                }
                // Pull immediately, then every ~30s with jitter so clients don't poll the
                // tracker (and re-dial the master) in lockstep after a correlated outage.
                loop {
                    let roster = tracker::query_roster(&self_peer).await;
                    if !roster.is_empty() {
                        // Write-through so the next restart has a fresh peer list.
                        roster_cache.persist(&roster);
                        warm_into_set(&roster_ipfs, &warm, roster, "roster", &self_peer).await;
                    }
                    tokio::time::sleep(jittered(Duration::from_secs(30))).await;
                }
            });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            node_info,
            peer_stats,
            peer_detail,
            connect_peer,
            keepalive_master,
            warm_root,
            resolve_ipns,
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
