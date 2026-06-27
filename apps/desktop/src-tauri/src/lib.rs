//! trackerstream desktop backend. Hosts the in-process IPFS data-plane node
//! (see `ipfs`) and exposes it to the Svelte frontend as Tauri commands. The
//! frontend never fetches module files over HTTP — it asks the embedded node to
//! resolve a root CID and gets back the reassembled module bytes to play.

pub mod ipfs;
pub mod ipns;
pub mod peer;
pub mod pinglog;
pub mod tracker;

use cid::Cid;
use futures::stream::StreamExt;
use rust_ipfs::{ConnectionEvent, Ipfs, Multiaddr, PeerId, Protocol};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
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

/// Durable peer address book — the live generalization of Phase 0's roster cache, which
/// it replaces. Persisted to `address_book.json` next to the blockstore. Fed from THREE
/// sources — the tracker roster, PEX gossip (`peer::pex_pull`), and learned holders — and
/// consumed two ways: startup warm-restore, and opportunistic book-dialing when the
/// tracker is unreachable but the warm set has free slots. Stale addrs are harmless (Noise
/// binds the connection to the PeerId, so a wrong addr is a failed dial, never a wrong
/// peer). This is the on-disk + in-RAM form of the PEX address book (P2P-NEXT-STEPS
/// Phase 1). Cap is well above WARM_CAP: most known peers are gossiped but never dialed.
pub(crate) struct AddressBook {
    map: Mutex<HashMap<String, BookEntry>>,
    path: Option<std::path::PathBuf>,
}

#[derive(Clone, Serialize, Deserialize)]
struct BookEntry {
    addrs: Vec<String>,
    /// Has at least one non-relay addr → directly dialable. Direct peers are kept
    /// preferentially (sampled first, evicted last).
    #[serde(default)]
    direct: bool,
    /// Unix secs of last sighting — drives freshness ordering + cap eviction.
    #[serde(default)]
    last_seen: u64,
}

const BOOK_CAP: usize = 256;

fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

fn is_direct(addrs: &[String]) -> bool {
    addrs.iter().any(|a| !a.contains("/p2p-circuit"))
}

impl AddressBook {
    fn load(dir: Option<&std::path::Path>) -> Self {
        let path = dir.map(|d| d.join("address_book.json"));
        let mut map: HashMap<String, BookEntry> = path
            .as_ref()
            .and_then(|p| std::fs::read(p).ok())
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        // One-time migration: with no book yet but a Phase 0 roster cache present, seed
        // from it so the first launch after the upgrade keeps last session's peers. The
        // book owns the data thereafter; roster_cache.json becomes vestigial.
        if map.is_empty() {
            if let Some(seed) = dir
                .and_then(|d| std::fs::read(d.join("roster_cache.json")).ok())
                .and_then(|b| serde_json::from_slice::<Vec<tracker::PeerRef>>(&b).ok())
            {
                let now = unix_now();
                for p in seed.into_iter().filter(|p| !p.addrs.is_empty()) {
                    let direct = is_direct(&p.addrs);
                    map.insert(p.peer_id, BookEntry { addrs: p.addrs, direct, last_seen: now });
                }
            }
        }
        AddressBook { map: Mutex::new(map), path }
    }

    /// Merge peers in: dedup by peerId, union addrs, refresh `direct` + `last_seen`; then
    /// cap (evict relay-only/oldest first, never a direct peer before a relay one) and
    /// write through. Best-effort, like HeldRoots.
    fn merge(&self, peers: &[tracker::PeerRef]) {
        if peers.is_empty() {
            return;
        }
        let now = unix_now();
        {
            let mut m = self.map.lock().unwrap();
            for p in peers {
                if p.addrs.is_empty() {
                    continue;
                }
                let direct = is_direct(&p.addrs);
                let e = m
                    .entry(p.peer_id.clone())
                    .or_insert_with(|| BookEntry { addrs: vec![], direct: false, last_seen: now });
                for a in &p.addrs {
                    if !e.addrs.contains(a) {
                        e.addrs.push(a.clone());
                    }
                }
                e.direct |= direct;
                e.last_seen = now;
            }
            if m.len() > BOOK_CAP {
                // Order worst-first by (direct, last_seen): relay-only + oldest lead.
                let mut ranked: Vec<(String, bool, u64)> =
                    m.iter().map(|(k, v)| (k.clone(), v.direct, v.last_seen)).collect();
                ranked.sort_by(|a, b| (a.1, a.2).cmp(&(b.1, b.2)));
                for (k, _, _) in ranked.into_iter().take(m.len() - BOOK_CAP) {
                    m.remove(&k);
                }
            }
        }
        self.persist();
    }

    /// Up to `want` peers as `PeerRef`s, DIRECT-dialable first then most-recently-seen,
    /// excluding `self_peer`. Serves `Pex` and feeds startup/opportunistic book-dialing.
    fn sample(&self, want: usize, self_peer: &str) -> Vec<tracker::PeerRef> {
        let m = self.map.lock().unwrap();
        let mut ranked: Vec<(&String, &BookEntry)> =
            m.iter().filter(|(k, _)| k.as_str() != self_peer).collect();
        ranked.sort_by(|a, b| (b.1.direct, b.1.last_seen).cmp(&(a.1.direct, a.1.last_seen)));
        ranked
            .into_iter()
            .take(want)
            .map(|(k, v)| tracker::PeerRef { peer_id: k.clone(), addrs: v.addrs.clone() })
            .collect()
    }

    /// Number of known peers (for tests; the opportunistic-dial gate keys off the warm
    /// set size, not the book).
    #[cfg(test)]
    fn len(&self) -> usize {
        self.map.lock().unwrap().len()
    }

    fn persist(&self) {
        let Some(path) = &self.path else { return };
        let snapshot = self.map.lock().unwrap().clone();
        if let Ok(bytes) = serde_json::to_vec(&snapshot) {
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

    /// Currently-warm peers — for the peers-pane `role` tag (B6 telemetry) and as the
    /// pull target set for peer.rs (warm ∩ connected = never dial a stranger).
    pub(crate) fn members(&self) -> HashSet<PeerId> {
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

/// Number of consecutive agreeing AutoNAT samples required to flip the debounced verdict.
const REACH_HYSTERESIS: i32 = 3;

/// Debounced public-reachability verdict from AutoNAT — the R2 eligibility gate. AutoNAT
/// flips on individual probe results, so we require `REACH_HYSTERESIS` consecutive agreeing
/// samples before changing the published verdict; this keeps the advertised `willingToRelay`
/// fact (Tier-1 PEX) and the self-reservation loop (M3) from churning on one noisy probe.
/// `public()` is read by the announce loop, relay-fact gossip, and self-reservation.
/// `nat_public()` collapses Private/Unknown together, so a freshly-booted node reads as
/// "not public" until AutoNAT confirms — undecided is surfaced as `None`.
#[derive(Default)]
pub(crate) struct Reachability(Mutex<ReachState>);

#[derive(Default)]
struct ReachState {
    /// Debounced verdict: `Some(true)` public, `Some(false)` private, `None` undecided.
    public: Option<bool>,
    /// Signed run length of consecutive agreeing samples (+ public, − not-public).
    run: i32,
}

impl Reachability {
    /// Feed one AutoNAT sample (`is_public`); extend or reset the run in its direction and
    /// flip the debounced verdict once the run reaches the hysteresis threshold. Returns
    /// the (possibly updated) verdict.
    fn observe(&self, is_public: bool) -> Option<bool> {
        let mut s = self.0.lock().unwrap();
        s.run = if is_public {
            if s.run > 0 {
                s.run + 1
            } else {
                1
            }
        } else if s.run < 0 {
            s.run - 1
        } else {
            -1
        };
        if s.run >= REACH_HYSTERESIS {
            s.public = Some(true);
        } else if s.run <= -REACH_HYSTERESIS {
            s.public = Some(false);
        }
        s.public
    }

    /// Current debounced verdict (`None` until AutoNAT produces a decisive run).
    pub(crate) fn public(&self) -> Option<bool> {
        self.0.lock().unwrap().public
    }
}

/// R2 relay facts learned from warm peers via `peer::relay_pull` (volatile — NOT persisted;
/// refreshed every PEX tick). Records which connected peers advertise `willing && reachable`,
/// i.e. the relays a NAT'd node can self-reserve on (M3). `can_reach` is per-query (M4) and
/// not stored here. Capped implicitly by the warm set it's fed from.
#[derive(Default)]
pub(crate) struct RelayView(Mutex<HashMap<PeerId, RelayFacts>>);

#[derive(Clone, Copy)]
struct RelayFacts {
    reachable: bool,
    willing: bool,
}

impl RelayView {
    /// Fold in a batch of relay replies (from `relay_pull(target=None)`). A peer that drops
    /// lingers here harmlessly — the M3 picker intersects with live `connected()` anyway.
    fn update(&self, replies: &[peer::RelayReply]) {
        let mut m = self.0.lock().unwrap();
        for r in replies {
            m.insert(r.peer, RelayFacts { reachable: r.reachable, willing: r.willing });
        }
    }

    /// Peers we believe are usable relays (willing + reachable). The M3 self-reservation loop
    /// picks from these; it intersects with `ipfs.connected()` at the call site.
    pub(crate) fn willing_relays(&self) -> Vec<PeerId> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, f)| f.willing && f.reachable)
            .map(|(p, _)| *p)
            .collect()
    }
}

/// R2/M5 instrumentation: cumulative connection telemetry, so the peers pane can SHOW whether
/// relayed traffic flows through PEERS (the offload win) vs the MASTER. Fed from
/// `connection_events`. This is a visibility dashboard, not a flip-off gate — we keep the
/// master's clamped coordination relay regardless (roadmap §R2 / [[master-relay-dual-role]]).
#[derive(Default)]
pub(crate) struct RelayStats(Mutex<RelayCounts>);

#[derive(Default, Clone, Copy, Serialize)]
pub(crate) struct RelayCounts {
    /// Direct connections established (non-circuit endpoints).
    direct: u64,
    /// Relayed connections established through a PEER relay (the R2 win).
    relayed_peer: u64,
    /// Relayed connections established through the MASTER's relay (what peer relays shrink).
    relayed_master: u64,
    /// Circuit→direct transitions to the same peer — inferred DCUtR hole-punch upgrades.
    dcutr_upgrades: u64,
}

impl RelayStats {
    fn snapshot(&self) -> RelayCounts {
        *self.0.lock().unwrap()
    }
    fn record_relay(&self, via_master: bool) {
        let mut c = self.0.lock().unwrap();
        if via_master {
            c.relayed_master += 1;
        } else {
            c.relayed_peer += 1;
        }
    }
    fn record_direct(&self) {
        self.0.lock().unwrap().direct += 1;
    }
    fn record_upgrade(&self) {
        self.0.lock().unwrap().dcutr_upgrades += 1;
    }
}

/// The relay-hop PeerId of a circuit multiaddr (the `/p2p/<hop>` immediately before
/// `/p2p-circuit`), or `None` for a direct address. Lets us split relayed connections into
/// peer-relayed vs master-relayed.
fn relay_hop(addr: &Multiaddr) -> Option<PeerId> {
    let mut last_p2p = None;
    for p in addr.iter() {
        match p {
            Protocol::P2p(id) => last_p2p = Some(id),
            Protocol::P2pCircuit => return last_p2p,
            _ => {}
        }
    }
    None
}

/// Consume `connection_events`, classifying each established connection for `RelayStats`.
/// DCUtR upgrades are inferred: a peer first seen on a circuit endpoint, later reached
/// directly, counts as one hole-punch upgrade.
fn spawn_relay_stats_loop(ipfs: Ipfs, stats: Arc<RelayStats>, master: Option<PeerId>) {
    tauri::async_runtime::spawn(async move {
        let mut stream = match ipfs.connection_events().await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[relay-stats] connection_events failed: {e}");
                return;
            }
        };
        let mut seen_relayed: HashSet<PeerId> = HashSet::new();
        while let Some(ev) = stream.next().await {
            let ConnectionEvent::ConnectionEstablished { peer_id, endpoint, .. } = ev else {
                continue;
            };
            let addr = endpoint.get_remote_address();
            match relay_hop(addr) {
                Some(hop) => {
                    stats.record_relay(Some(hop) == master);
                    seen_relayed.insert(peer_id);
                }
                None => {
                    stats.record_direct();
                    if seen_relayed.remove(&peer_id) {
                        stats.record_upgrade(); // circuit → direct = inferred hole-punch
                    }
                }
            }
        }
    });
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
    /// Debounced AutoNAT verdict (R2): `Some(true)` publicly reachable, `Some(false)`
    /// private/NAT'd, `None` undecided. Drives relay eligibility + the peers-pane badge.
    reachable: Option<bool>,
}

#[tauri::command]
async fn node_info(
    state: State<'_, IpfsState>,
    reach: State<'_, Arc<Reachability>>,
) -> Result<NodeInfo, String> {
    let listening = state
        .ipfs
        .listening_addresses()
        .await
        .map(|addrs| addrs.iter().map(|a| a.to_string()).collect())
        .unwrap_or_default();
    Ok(NodeInfo {
        peer_id: state.peer_id.clone(),
        listening,
        reachable: reach.public(),
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
    /// R2/M5 cumulative relay telemetry: direct vs peer-relayed vs master-relayed
    /// connections (+ inferred DCUtR upgrades). Shows whether offload rides peers, not the box.
    relay: RelayCounts,
}

/// Snapshot for the peers pane. Polled ~1/s by the frontend, which derives per-peer
/// up/down speed from the byte-counter deltas.
#[tauri::command]
async fn peer_stats(
    state: State<'_, IpfsState>,
    warm: State<'_, Arc<WarmSet>>,
    relay_stats: State<'_, Arc<RelayStats>>,
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
    Ok(PeerStats { connected: connected.len(), peers, relay: relay_stats.snapshot() })
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
            // R2/M4 residual fallback: a direct dial failed and we lack this holder's circuit
            // addr — ask warm peers if any can relay us to it, and if so dial through that one.
            Err(e) => match relay_fallback(ipfs, warm, &h.peer_id, pid).await {
                Ok(()) => warm.record(pid, tag),
                Err(e2) => eprintln!("[warm] {} failed (direct + relay): {e} / {e2}", h.peer_id),
            },
        }
    }
    for victim in warm.take_overflow() {
        let _ = ipfs.disconnect(victim).await;
        let _ = ipfs.remove_peer(victim).await;
    }
}

/// R2/M4: after a direct dial to `holder` fails, ask warm peers whether any can relay us to it
/// (`Relay{target}` → `can_reach`), and dial through the first that says yes. Thin residual
/// path — most NAT'd holders are reached directly via their own announced circuit addr (M3).
/// Errs if no warm peer can reach the holder or the relayed dial fails.
async fn relay_fallback(ipfs: &Ipfs, warm: &WarmSet, holder: &str, holder_pid: PeerId) -> Result<(), String> {
    let via = peer::relay_pull(ipfs, warm, Some(holder))
        .await
        .into_iter()
        .find(|r| r.can_reach)
        .map(|r| r.peer)
        .ok_or_else(|| "no warm peer can relay to it".to_string())?;
    ipfs::dial_via_relay(ipfs, via, holder_pid)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn warm_root(
    root: String,
    state: State<'_, IpfsState>,
    warm: State<'_, Arc<WarmSet>>,
    book: State<'_, Arc<AddressBook>>,
) -> Result<(), String> {
    // Validate the CID but don't fetch — this only pre-connects.
    let _: Cid = root.parse().map_err(|e| format!("bad CID {root}: {e}"))?;
    let mut holders = tracker::query_peers(&state.peer_id, &root).await;
    if holders.is_empty() {
        // Tracker had nothing (or is down): ask warm peers who holds it (Phase 1).
        holders = peer::peers_pull(&state.ipfs, warm.inner(), &root).await;
    }
    if !holders.is_empty() {
        book.merge(&holders); // learned holders enrich the book for PEX + future restores
    }
    warm_into_set(&state.ipfs, warm.inner(), holders, &root, &state.peer_id).await;
    Ok(())
}

/// Resolve an IPNS name to the CID it currently points at, verifying the signed
/// record locally (PEER-ASSIST.md §9). Seam for the future catalog→IPNS migration:
/// not yet wired into any flow, but lets the thin client resolve `/ipns/<name>`
/// without a DHT once the catalog is published as IPNS.
#[tauri::command]
async fn resolve_ipns(
    name: String,
    cache: State<'_, Arc<IpnsCache>>,
    state: State<'_, IpfsState>,
    warm: State<'_, Arc<WarmSet>>,
) -> Result<String, String> {
    // 1. Local cache — re-verify (signature + EOL); a stale/expired/forged entry fails
    //    here and falls through. Zero network when valid: survives a short outage.
    if let Some(b64) = cache.get(&name) {
        if let Ok(cid) = ipns::verify_b64(&name, &b64) {
            return Ok(cid.to_string());
        }
    }
    // 2. Tracker fetch; cache the verified record for the next outage.
    if let Ok((b64, cid)) = ipns::fetch_record(&name).await {
        cache.put(&name, &b64);
        return Ok(cid.to_string());
    }
    // 3. Peer-pull (box-down path, Phase 1): ask warm peers, verify each, newest sequence
    //    wins. We cache + serve what we resolve, so records spread peer-to-peer (pull-based
    //    IPNS gossip — no DHT, no gossipsub mesh).
    if let Some((b64, cid)) = peer::ipns_pull(&state.ipfs, warm.inner(), &name).await {
        cache.put(&name, &b64);
        return Ok(cid.to_string());
    }
    Err(format!("resolve_ipns {name}: tracker + peers all failed"))
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
            // Durable peer address book sits beside it — also load before `dir` moves.
            let book = Arc::new(AddressBook::load(dir.as_deref()));
            // IPNS record cache — same directory, load before `dir` moves. Arc so the
            // serve loop and the resolve_ipns command share it.
            let ipns_cache = Arc::new(IpnsCache::load(dir.as_deref()));
            // R2: read the SAME relay opt-in `ipfs::start` uses to enable the relay server, so
            // the advertised `willing` fact agrees with whether we actually run one. Read before
            // `dir` moves into start().
            let relay_willing = ipfs::read_relay_policy(dir.as_deref());
            // Start the (non-Send) builder once here, off the command path.
            let node =
                tauri::async_runtime::block_on(ipfs::start(dir)).map_err(|e| e.to_string())?;
            // Clone handles + identity for the background loops before the node
            // moves into managed state.
            let announce_ipfs = node.ipfs.clone();
            let roster_ipfs = node.ipfs.clone();
            let pex_ipfs = node.ipfs.clone();
            let serve_ipfs = node.ipfs.clone();
            let announce_pid = node.peer_id.parse::<PeerId>().ok();
            let self_peer = node.peer_id.clone();
            let serve_self = node.peer_id.clone();
            let warm = Arc::new(WarmSet::default());
            // Debounced AutoNAT reachability (R2 eligibility gate). Polled by its own loop.
            let reach = Arc::new(Reachability::default());
            let reach_ipfs = node.ipfs.clone();
            let reach_for_loop = reach.clone();
            // R2 relay view (volatile): which warm peers are usable relays. Fed by the PEX
            // loop's relay_pull; read by M3 self-reservation.
            let relay_view = Arc::new(RelayView::default());
            let relay_for_pex = relay_view.clone();
            let relay_for_resv = relay_view.clone();
            let resv_ipfs = node.ipfs.clone();
            let reach_for_resv = reach.clone();
            // R2/M5 relay telemetry, fed from connection_events.
            let relay_stats = Arc::new(RelayStats::default());
            let stats_ipfs = node.ipfs.clone();
            let stats_master = ipfs::master_peer_id();
            // Loops/serve share the Arcs; clone before `held` moves into the announce loop.
            let held_for_serve = held.clone();
            let book_for_serve = book.clone();
            let book_for_pex = book.clone();
            let book_for_roster = book.clone();
            let warm_for_pex = warm.clone();
            let warm_for_serve = warm.clone();
            let reach_for_serve = reach.clone();
            let ipns_for_serve = ipns_cache.clone();
            app.manage(IpfsState {
                ipfs: node.ipfs,
                peer_id: node.peer_id,
            });
            app.manage(Streams::default());
            app.manage(held.clone());
            app.manage(warm.clone());
            app.manage(ipns_cache);
            app.manage(book.clone());
            app.manage(reach.clone());
            app.manage(relay_view.clone());
            app.manage(relay_stats.clone());
            // R2/M5: classify every established connection (direct / peer-relay / master-relay,
            // + inferred DCUtR upgrades) for the peers-pane offload dashboard.
            spawn_relay_stats_loop(stats_ipfs, relay_stats, stats_master);
            // Peer-assist tracker: announce presence + held roots every ~30s (also
            // the presence heartbeat). Skipped only if our PeerId failed to parse.
            if let Some(pid) = announce_pid {
                tracker::spawn_announce_loop(announce_ipfs, pid, held);
            }
            // Phase-1 peer control plane: answer inbound Pex/Peers/Ipns requests from our
            // local caches so warm peers can serve each other when the box is down.
            peer::spawn_serve_loop(peer::ServeCtx {
                ipfs: serve_ipfs,
                held: held_for_serve,
                ipns: ipns_for_serve,
                book: book_for_serve,
                self_peer: serve_self,
                warm: warm_for_serve,
                reach: reach_for_serve,
                relay_willing,
            });
            // Reachability poll (R2 eligibility gate): sample AutoNAT's verdict every ~30s
            // and feed the debounced `Reachability`. AutoNAT needs a probe round first, so
            // the early samples read "not public"; the hysteresis + jitter keep the verdict
            // from flapping. No behaviour change yet — M1+ read this to gate relay duties.
            tauri::async_runtime::spawn(async move {
                loop {
                    let is_public = reach_ipfs.nat_public().await.unwrap_or(false);
                    reach_for_loop.observe(is_public);
                    tokio::time::sleep(jittered(Duration::from_secs(30))).await;
                }
            });
            // R2/M3 self-reservation (the main reachability win): when we are NOT publicly
            // reachable, hold a circuit reservation on a couple of willing+reachable relays so
            // peers can dial us through them (the circuit addr flows into announce on its own).
            // When we become public, tear them down — a public node needs no relay. The relays
            // come from the RelayView (refreshed by the PEX-tick relay_pull). ~45s cadence.
            tauri::async_runtime::spawn(async move {
                const TARGET: usize = 2;
                let mut held: HashMap<PeerId, rust_ipfs::ListenerId> = HashMap::new();
                loop {
                    tokio::time::sleep(jittered(Duration::from_secs(45))).await;
                    let public = reach_for_resv.public() == Some(true);
                    let connected: HashSet<PeerId> =
                        resv_ipfs.connected().await.unwrap_or_default().into_iter().collect();
                    // Drop reservations we no longer want: we became public, or the relay left.
                    let stale: Vec<PeerId> = held
                        .keys()
                        .copied()
                        .filter(|r| public || !connected.contains(r))
                        .collect();
                    for r in stale {
                        if let Some(id) = held.remove(&r) {
                            ipfs::drop_reservation(&resv_ipfs, id).await;
                        }
                    }
                    if public {
                        continue;
                    }
                    // Top up to TARGET reservations from willing+reachable, connected relays.
                    let mut candidates: Vec<PeerId> = relay_for_resv
                        .willing_relays()
                        .into_iter()
                        .filter(|r| connected.contains(r) && !held.contains_key(r))
                        .collect();
                    while held.len() < TARGET {
                        let Some(relay) = candidates.pop() else { break };
                        if let Some(id) = ipfs::reserve_on_relay(&resv_ipfs, relay).await {
                            held.insert(relay, id);
                        }
                    }
                }
            });
            // PEX gossip: periodically pull peers from the warm set and fold them into the
            // address book (the durable PEX store). Sleep FIRST — the warm set is empty at
            // boot; the roster/restore path populates it. Jittered so clients don't gossip
            // in lockstep. This is what sustains membership knowledge through a box outage.
            tauri::async_runtime::spawn(async move {
                loop {
                    tokio::time::sleep(jittered(Duration::from_secs(60))).await;
                    let learned = peer::pex_pull(&pex_ipfs, &warm_for_pex, 32).await;
                    if !learned.is_empty() {
                        book_for_pex.merge(&learned);
                    }
                    // R2: on the same tick, refresh which warm peers are usable relays (node
                    // facts only — target=None). Feeds the M3 self-reservation picker.
                    let relays = peer::relay_pull(&pex_ipfs, &warm_for_pex, None).await;
                    if !relays.is_empty() {
                        relay_for_pex.update(&relays);
                    }
                }
            });
            // Roster backbone: periodically warm a bounded set of ONLINE peers
            // (content-independent), so warm connections are symmetric — both ends
            // keepalive, so a holder no longer drops us when a download finishes —
            // and the presence backbone forms. Bounded + LRU-evicted by WARM_CAP.
            tauri::async_runtime::spawn(async move {
                // Startup: re-dial last session's peers (from the address book) immediately,
                // before the first roster pull. During a tracker/box outage this is the ONLY
                // source of peers; on a normal restart it just warms the set ~30s sooner.
                // Bounded by WARM_CAP; the first live roster tick supersedes it.
                let restore = book_for_roster.sample(WARM_CAP, &self_peer);
                if !restore.is_empty() {
                    warm_into_set(&roster_ipfs, &warm, restore, "restore", &self_peer).await;
                }
                // Pull immediately, then every ~30s with jitter so clients don't poll the
                // tracker (and re-dial the master) in lockstep after a correlated outage.
                loop {
                    let roster = tracker::query_roster(&self_peer).await;
                    if !roster.is_empty() {
                        // Feed the book so the next restart + PEX have a fresh peer list.
                        book_for_roster.merge(&roster);
                        warm_into_set(&roster_ipfs, &warm, roster, "roster", &self_peer).await;
                    } else if warm.members().len() < WARM_CAP {
                        // Empty roster = tracker unreachable. With free warm slots, dial
                        // from the book to sustain membership through the outage. (PEX keeps
                        // the book itself fresh from whoever we're still connected to.)
                        let dialable = book_for_roster.sample(WARM_CAP, &self_peer);
                        if !dialable.is_empty() {
                            warm_into_set(&roster_ipfs, &warm, dialable, "book", &self_peer).await;
                        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use tracker::PeerRef;

    fn pref(id: &str, addrs: &[&str]) -> PeerRef {
        PeerRef { peer_id: id.into(), addrs: addrs.iter().map(|s| s.to_string()).collect() }
    }

    // AddressBook with no path: pure in-memory, no disk I/O — exercises merge/sample/cap.
    fn mem_book() -> AddressBook {
        AddressBook { map: Mutex::new(HashMap::new()), path: None }
    }

    #[test]
    fn book_merge_dedups_and_unions_addrs() {
        let book = mem_book();
        book.merge(&[pref("12D3KooWa", &["/ip4/1.1.1.1/tcp/4001/p2p/12D3KooWa"])]);
        // Same peer, a new addr — addrs union, not duplicate, one entry.
        book.merge(&[pref(
            "12D3KooWa",
            &[
                "/ip4/1.1.1.1/tcp/4001/p2p/12D3KooWa",
                "/ip4/2.2.2.2/udp/4001/quic-v1/p2p/12D3KooWa",
            ],
        )]);
        assert_eq!(book.len(), 1);
        let s = book.sample(10, "self");
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].addrs.len(), 2, "addrs should union, not duplicate");
    }

    #[test]
    fn book_sample_is_direct_first_and_excludes_self() {
        let book = mem_book();
        book.merge(&[
            pref("relayonly", &["/ip4/9.9.9.9/tcp/4001/p2p-circuit/p2p/relayonly"]),
            pref("direct", &["/ip4/1.1.1.1/tcp/4001/p2p/direct"]),
            pref("self", &["/ip4/3.3.3.3/tcp/4001/p2p/self"]),
        ]);
        let s = book.sample(10, "self");
        assert!(!s.iter().any(|p| p.peer_id == "self"), "self must be excluded");
        assert_eq!(s[0].peer_id, "direct", "direct-dialable peer must sort first");
    }

    #[test]
    fn book_cap_evicts_relay_only_and_oldest_first() {
        let book = mem_book();
        // One direct peer, then flood with > BOOK_CAP relay-only peers. The direct peer
        // must survive (never evicted before a relay-only one).
        book.merge(&[pref("keepDirect", &["/ip4/1.1.1.1/tcp/4001/p2p/keepDirect"])]);
        let flood: Vec<PeerRef> = (0..BOOK_CAP + 50)
            .map(|i| pref(&format!("r{i}"), &[&format!("/ip4/9.9.9.9/tcp/4001/p2p-circuit/p2p/r{i}")]))
            .collect();
        book.merge(&flood);
        assert!(book.len() <= BOOK_CAP, "book must be capped");
        assert!(
            book.sample(BOOK_CAP, "x").iter().any(|p| p.peer_id == "keepDirect"),
            "direct peer must outlive relay-only peers under cap pressure",
        );
    }

    #[test]
    fn book_seed_migrates_from_roster_cache() {
        // load() with only roster_cache.json present should seed the book from it.
        let tmp = std::env::temp_dir().join(format!("ts-book-seed-{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();
        let roster = vec![pref("12D3KooWseed", &["/ip4/4.4.4.4/tcp/4001/p2p/12D3KooWseed"])];
        std::fs::write(tmp.join("roster_cache.json"), serde_json::to_vec(&roster).unwrap()).unwrap();
        let book = AddressBook::load(Some(&tmp));
        assert_eq!(book.len(), 1);
        assert_eq!(book.sample(10, "x")[0].peer_id, "12D3KooWseed");
        std::fs::remove_dir_all(&tmp).ok();
    }

    // HeldRoots with no path: in-memory, exercises the Peers serve arm.
    fn mem_held(root: Option<&str>) -> HeldRoots {
        let mut map = HashMap::new();
        if let Some(r) = root {
            map.insert(r.to_string(), true);
        }
        HeldRoots { map: Mutex::new(map), path: None }
    }

    #[test]
    fn peers_response_self_reports_only_when_held() {
        let held = mem_held(Some("bafyROOT"));
        let me = pref("12D3KooWme", &["/ip4/5.5.5.5/tcp/4001/p2p/12D3KooWme"]);
        // Holds it + we have a self ref → answer with ourselves.
        let peer::Resp::Peers { peers } =
            peer::peers_response(&held, "bafyROOT", Some(me.clone()))
        else {
            panic!("expected Resp::Peers");
        };
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].peer_id, "12D3KooWme");
        // Don't hold it → empty, even with a self ref.
        let peer::Resp::Peers { peers } =
            peer::peers_response(&held, "bafyOTHER", Some(me))
        else {
            panic!("expected Resp::Peers");
        };
        assert!(peers.is_empty());
    }

    #[test]
    fn reachability_debounces_with_hysteresis() {
        let r = Reachability::default();
        // Undecided until a full agreeing run lands.
        assert_eq!(r.public(), None);
        assert_eq!(r.observe(true), None); // run 1
        assert_eq!(r.observe(true), None); // run 2
        assert_eq!(r.observe(true), Some(true)); // run 3 → public
        // A single noisy private sample doesn't flip the verdict...
        assert_eq!(r.observe(false), Some(true));
        // ...but three consecutive do.
        assert_eq!(r.observe(false), Some(true)); // run -2
        assert_eq!(r.observe(false), Some(false)); // run -3 → private
        // And a public sample mid-run resets the counter (no premature flip back).
        assert_eq!(r.observe(true), Some(false)); // run 1
        assert_eq!(r.observe(true), Some(false)); // run 2
        assert_eq!(r.observe(true), Some(true)); // run 3 → public again
    }

    #[test]
    fn relay_hop_extracts_the_hop_and_stats_count() {
        let relay: PeerId = "12D3KooWGv1nSsDQ4JnzoNZ1ayGZmAnkM7RpbyGPnixH9xNsXFeV".parse().unwrap();
        let target: PeerId = "12D3KooWM1LwoXBwBkyZbKZDDTauWyWtGx8WZt1jGXW5vYiRQnxM".parse().unwrap();
        // A direct address has no relay hop.
        let direct: Multiaddr = "/ip4/1.2.3.4/tcp/4001".parse().unwrap();
        assert_eq!(relay_hop(&direct), None);
        // A circuit address yields the `/p2p/<hop>` immediately before `/p2p-circuit`.
        let circuit: Multiaddr =
            format!("/ip4/1.2.3.4/tcp/4001/p2p/{relay}/p2p-circuit/p2p/{target}")
                .parse()
                .unwrap();
        assert_eq!(relay_hop(&circuit), Some(relay));

        // Counters: peer-relay vs master-relay split + the DCUtR upgrade tally.
        let stats = RelayStats::default();
        stats.record_relay(false); // a peer relay (the R2 win)
        stats.record_relay(true); // the master relay
        stats.record_direct();
        stats.record_upgrade();
        let s = stats.snapshot();
        assert_eq!(
            (s.direct, s.relayed_peer, s.relayed_master, s.dcutr_upgrades),
            (1, 1, 1, 1),
        );
    }
}
