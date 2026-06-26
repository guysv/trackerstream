//! trackerstream desktop backend. Hosts the in-process IPFS data-plane node
//! (see `ipfs`) and exposes it to the Svelte frontend as Tauri commands. The
//! frontend never fetches module files over HTTP — it asks the embedded node to
//! resolve a root CID and gets back the reassembled module bytes to play.

pub mod ipfs;
pub mod ipns;
pub mod tracker;

use cid::Cid;
use rust_ipfs::{Ipfs, PeerId};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tauri::ipc::{Channel, Response};
use tauri::{Manager, State};

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
#[tauri::command]
async fn warm_root(
    root: String,
    state: State<'_, IpfsState>,
    warm: State<'_, Arc<WarmSet>>,
) -> Result<(), String> {
    // Validate the CID but don't fetch — this only pre-connects.
    let _: Cid = root.parse().map_err(|e| format!("bad CID {root}: {e}"))?;
    let holders = tracker::query_peers(&state.peer_id, &root).await;
    let ipfs = state.ipfs.clone();
    for h in holders {
        match ipfs::warm_connect(&ipfs, &h.peer_id, &h.addrs).await {
            Ok(()) => {
                if let Ok(pid) = h.peer_id.parse::<PeerId>() {
                    warm.record(pid, &root);
                }
            }
            Err(e) => eprintln!("[warm] {} failed: {e}", h.peer_id),
        }
    }
    // Enforce the warm-set cap (LRU eviction) — disconnect + drop keepalive.
    for victim in warm.take_overflow() {
        let _ = ipfs.disconnect(victim).await;
        let _ = ipfs.remove_peer(victim).await;
    }
    Ok(())
}

/// Resolve an IPNS name to the CID it currently points at, verifying the signed
/// record locally (PEER-ASSIST.md §9). Seam for the future catalog→IPNS migration:
/// not yet wired into any flow, but lets the thin client resolve `/ipns/<name>`
/// without a DHT once the catalog is published as IPNS.
#[tauri::command]
async fn resolve_ipns(name: String) -> Result<String, String> {
    ipns::resolve_ipns(&name)
        .await
        .map(|cid| cid.to_string())
        .map_err(|e| format!("resolve_ipns {name} failed: {e}"))
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
    // RUST_LOG-driven tracing (rust-ipfs/libp2p/bitswap + our own spans). No-op
    // unless RUST_LOG is set, so release runs stay quiet.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
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
            // Start the (non-Send) builder once here, off the command path.
            let node =
                tauri::async_runtime::block_on(ipfs::start(dir)).map_err(|e| e.to_string())?;
            // Clone the handle + identity for the background announce loop before
            // the node moves into managed state.
            let announce_ipfs = node.ipfs.clone();
            let announce_pid = node.peer_id.parse::<PeerId>().ok();
            app.manage(IpfsState {
                ipfs: node.ipfs,
                peer_id: node.peer_id,
            });
            app.manage(Streams::default());
            app.manage(held.clone());
            app.manage(Arc::new(WarmSet::default()));
            // Peer-assist tracker: announce presence + held roots every ~30s (also
            // the presence heartbeat). Skipped only if our PeerId failed to parse.
            if let Some(pid) = announce_pid {
                tracker::spawn_announce_loop(announce_ipfs, pid, held);
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            node_info,
            peer_stats,
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
