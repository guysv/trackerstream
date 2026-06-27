//! In-process IPFS data-plane node (rust-ipfs) embedded in the Tauri backend —
//! NO sidecar / second process. Resolves a module's root CID, fetches its DAG
//! blocks over libp2p/Bitswap (each verified against its CID by `Block::new`),
//! and reassembles the EXACT original module bytes (mirrors
//! packages/repack/src/dag.ts). Interops with the kubo master (same CIDv1 /
//! sha2-256 / raw + dag-cbor block scheme).

use anyhow::{anyhow, Result};
use cid::Cid;
use futures::stream::{FuturesOrdered, FuturesUnordered, StreamExt};
use rust_ipfs::builder::IpfsBuilder;
use rust_ipfs::p2p::RelayConfig;
use rust_ipfs::{
    AddPeerOpt, Ipfs, Keypair, ListenerId, Multiaddr, PeerId, Protocol, RequestResponseConfig,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

const FETCH_TIMEOUT: Duration = Duration::from_secs(60);
const FETCH_CONCURRENCY: usize = 32;

/// Per-peer cumulative `(down, up)` Bitswap block bytes since process start, read
/// by the peers pane via `peer_stats`. Sourced from our rust-ipfs patch
/// (patches/0001-bitswap-peer-attribution.patch), which counts real block
/// payloads at the wire — inbound `put_block` (down) + outbound block response
/// (up) — keyed by peer. The map never evicts a peer, so a peer that disconnects
/// keeps its totals (the UI grays it) and continues from them on reconnect.
pub fn peer_bandwidth() -> HashMap<PeerId, (u64, u64)> {
    rust_ipfs::bandwidth_snapshot()
}

// The fra1 master (mirror of packages/config MASTER_PEER_ID). Every block fetch
// names it as the Bitswap provider, so we ask the always-on master directly
// instead of doing DHT provider discovery across the public swarm — the master
// holds the whole archive and we bootstrap-connect to it, so this is both faster
// and reliable. (Client-to-client discovery via the DHT is a deferred follow-up.)
const MASTER_PEER_ID: &str = "12D3KooWGb7eHYgZnMFfADEDeS5xDEwEVQKPTGozsKanpDf9XvzL";

fn master_provider() -> Option<PeerId> {
    // TS_PROVIDER overrides the master identity (e.g. point the headless transport
    // soak at a local kubo). Defaults to the always-on master. Used by
    // keepalive_master to hold the master connection open.
    if let Ok(s) = std::env::var("TS_PROVIDER") {
        return s.parse().ok();
    }
    MASTER_PEER_ID.parse().ok()
}

/// The master's PeerId (TS_PROVIDER override or the hardcoded default) — used by
/// the peers pane to tag the master row vs warm-set holders.
pub fn master_peer_id() -> Option<PeerId> {
    master_provider()
}

/// Bitswap provider hint for block fetches — set ONLY when TS_PROVIDER is
/// explicitly configured (the headless soak harness, which dials a local kubo it
/// doesn't otherwise keepalive-connect to). In normal operation this is None so
/// the want-list BROADCASTS to every connected peer (the master, held open by
/// keepalive_master, AND any warm-set holders). That broadcast is what offloads
/// the master — peers answer, the master backfills. See PEER-ASSIST.md §4.
fn explicit_provider_override() -> Option<PeerId> {
    std::env::var("TS_PROVIDER").ok().and_then(|s| s.parse().ok())
}

// --- v1 manifest (byte-exact reassembly) — kept only for the v1 fallback path:
// a not-yet-re-baked or flat (mo3) root is reassembled WHOLE and handed to the
// worklet as a full-load skeleton. New roots are v2 (see ManifestV2 below).
#[derive(Debug, Deserialize)]
struct SampleEntry {
    offset: u64,
    length: u64,
    #[serde(rename = "pcmRoot")]
    pcm_root: Cid,
}

#[derive(Debug, Deserialize)]
struct Manifest {
    #[serde(rename = "originalLength")]
    original_length: u64,
    #[serde(rename = "skeletonChunks")]
    skeleton_chunks: Vec<Cid>,
    samples: Vec<SampleEntry>,
}

#[derive(Debug, Deserialize)]
struct PcmRoot {
    chunks: Vec<Cid>,
    #[allow(dead_code)]
    length: u64,
}

// --- v2 manifest (immortal-instance streaming; STREAMING-PARITY-V2-SCHEMA.md) ---

#[derive(Debug, Deserialize)]
struct SampleV2 {
    index: u32, // 1-based libopenmpt slot (== provide_sample arg)
    frames: u32,
    chunks: Vec<Cid>, // decoded native-layout PCM leaves
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct CheckpointV2 {
    order: u32,
    samples: Vec<u32>, // resident slot indices
}

// Cumulative seconds at the start of each valid play order — the time<->order map
// for seek-by-seconds (consumed by the seek benchmark; the fence/prefetch only need
// `checkpoints`). dag-cbor canonically encodes whole-number floats as ints, so
// `seconds: 0` arrives as a CBOR integer that a plain `f64` field rejects — the
// crash that fec83fa fixed by dropping the field. We instead accept int OR float
// via a lenient visitor, so re-modeling the field can't reintroduce that crash.
#[derive(Debug, Deserialize, Serialize, Clone)]
struct OrderSec {
    order: u32,
    #[serde(deserialize_with = "de_f64_lenient")]
    seconds: f64,
}

fn de_f64_lenient<'de, D: serde::Deserializer<'de>>(d: D) -> Result<f64, D::Error> {
    struct V;
    impl serde::de::Visitor<'_> for V {
        type Value = f64;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a number (CBOR int or float)")
        }
        fn visit_f64<E>(self, v: f64) -> Result<f64, E> { Ok(v) }
        fn visit_i64<E>(self, v: i64) -> Result<f64, E> { Ok(v as f64) }
        fn visit_u64<E>(self, v: u64) -> Result<f64, E> { Ok(v as f64) }
        fn visit_i128<E>(self, v: i128) -> Result<f64, E> { Ok(v as f64) }
        fn visit_u128<E>(self, v: u128) -> Result<f64, E> { Ok(v as f64) }
    }
    d.deserialize_any(V)
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct PlanV2 {
    #[serde(default)]
    checkpoints: Vec<CheckpointV2>,
    #[serde(default, rename = "orderSeconds")]
    order_seconds: Vec<OrderSec>,
}

impl PlanV2 {
    /// Sample slots that must be resident before playback may proceed at `order` —
    /// the Rust port of `Fence.requiredAt(order)` (apps/desktop/src/lib/audio/fence.ts):
    /// the floor checkpoint at `order` UNION the next one (a render quantum can cross
    /// one checkpoint boundary mid-buffer). Empty plan / single checkpoint degrade
    /// gracefully. The fence (and the TTFP/seek benchmarks) gate on exactly this set.
    pub fn required_at(&self, order: u32) -> Vec<u32> {
        let mut cps: Vec<&CheckpointV2> = self.checkpoints.iter().collect();
        cps.sort_by_key(|c| c.order);
        // floorIdx: greatest index with checkpoint order <= `order`, else -1.
        // unionRange(floor, floor+1), clamped (floor=-1 => cp[0] only).
        let floor: isize = cps.iter().rposition(|c| c.order <= order).map_or(-1, |i| i as isize);
        let lo = floor.max(0) as usize;
        let hi = ((floor + 1).max(0) as usize).min(cps.len().saturating_sub(1));
        let mut set: std::collections::BTreeSet<u32> = std::collections::BTreeSet::new();
        if !cps.is_empty() {
            for k in lo..=hi {
                set.extend(cps[k].samples.iter().copied());
            }
        }
        set.into_iter().collect()
    }

    /// requiredAt(0) — the "enough to start playing" set the TTFP benchmark waits on.
    pub fn required_at_zero(&self) -> Vec<u32> {
        self.required_at(0)
    }

    /// Map a wall-clock position (seconds) to the play order to seek to, via the baked
    /// cumulative `orderSeconds` map: the latest order whose start time is <= `secs`.
    /// Empty map (v1/flat root, or a v2 root with no time map) -> 0. This is the
    /// bake-time approximation of where libopenmpt's seek-by-seconds lands.
    pub fn order_for_seconds(&self, secs: f64) -> u32 {
        let mut best = 0u32;
        let mut best_t = f64::NEG_INFINITY;
        for os in &self.order_seconds {
            if os.seconds <= secs && os.seconds >= best_t {
                best_t = os.seconds;
                best = os.order;
            }
        }
        best
    }

    /// Number of checkpoints in the plan (0 for a v1/flat root with no streaming).
    pub fn checkpoint_count(&self) -> usize {
        self.checkpoints.len()
    }

    /// Whether the plan carries a time<->order map (false for v1/flat roots).
    pub fn has_order_map(&self) -> bool {
        !self.order_seconds.is_empty()
    }
}

#[derive(Debug, Deserialize)]
struct IndexV2 {
    samples: Vec<SampleV2>,
    #[serde(default)]
    plan: PlanV2,
}

#[derive(Debug, Deserialize)]
struct ManifestV2 {
    v: u8,
    #[serde(rename = "skeletonChunks")]
    skeleton_chunks: Vec<Cid>,
    // Run-length recipe [nContentChunks, zeroBytes, ...] interleaving the structure
    // content chunks above with synthesized zero runs (orphaned compressed bytes,
    // zeroed PCM, appended decoded-length tail) that are never transferred. Empty
    // for older manifests -> skeleton is a plain concat of skeleton_chunks.
    #[serde(default, rename = "skeletonLayout")]
    skeleton_layout: Vec<u64>,
    // Inline index, OR a pointer to a spilled index block (large modules).
    #[serde(default)]
    index: Option<IndexV2>,
    #[serde(default, rename = "indexRoot")]
    index_root: Option<Cid>,
}

/// The embedded node plus its identity.
pub struct Node {
    pub ipfs: Ipfs,
    pub peer_id: String,
}

/// Start an in-process libp2p/IPFS node. TCP + QUIC transports; relay client +
/// DCUtR for NAT hole punching; AutoNAT for reachability. When `data_dir` is
/// given the blockstore is fs-backed and PERSISTENT — that store IS the client's
/// CID block cache (cross-module reuse + survives restarts; Phase 3).
pub async fn start(data_dir: Option<PathBuf>) -> Result<Node> {
    // Persist the identity alongside the blockstore so the PeerId is STABLE across
    // restarts. The blockstore survives restarts but a fresh keypair every launch
    // would churn the PeerId — orphaning our tracker presence row and the per-peer
    // byte counters. With `data_dir == None` (headless/test) we stay ephemeral.
    let keypair = match &data_dir {
        Some(dir) => {
            std::fs::create_dir_all(dir).ok();
            load_or_create_keypair(&dir.join("identity.key"))?
        }
        None => Keypair::generate_ed25519(),
    };
    let peer_id = keypair.public().to_peer_id().to_string();
    // STABLE inbound port (P2P-NEXT-STEPS). We bind TCP, and that port leaks into every
    // address we announce / a peer persists about us (the LAN/observed-direct cases — see
    // tracker::announce_addrs). A fresh ephemeral port every launch makes all of those
    // stale the moment WE restart, so a peer that remembered us (Phase 0 reconnect /
    // Phase 1 PEX) can't re-dial us directly. Reuse last session's port when it's still
    // free; fall back to ephemeral if it's taken (a second instance, or some other
    // process grabbed it) — never fail startup over it. Best-effort: it hardens
    // direct-dialable entries, but NAT remapping / IP changes still need a tracker/PEX
    // refresh (Noise binds the connection to the PeerId, so a stale addr is only ever a
    // wasted dial). Ephemeral (data_dir == None, headless/test) keeps port 0.
    let port_file = data_dir.as_ref().map(|d| d.join("listen_port"));
    let tcp_port = pick_listen_port(port_file.as_deref());
    let listen_addr = format!("/ip4/0.0.0.0/tcp/{tcp_port}");
    // R2: opt-in, default-OFF circuit-relay-v2 server. We decide server PRESENCE once here
    // (the builder is one-shot); willingness is advertised separately at runtime, so an
    // idle clamped server is ~free. A NAT'd or opted-out node simply doesn't construct it.
    let relay_willing = read_relay_policy(data_dir.as_deref());
    // The builder is generic over a custom NetworkBehaviour; we don't need one,
    // so pin it to libp2p's no-op dummy behaviour (ToSwarm = Infallible).
    // A LIGHTWEIGHT client node — deliberately NOT a DHT server. with_default()
    // would enable Kademlia, making the node crawl + hold connections to hundreds
    // of public peers (huge churn, and it starves the master connection). We only
    // need to talk to the master, so enable just identify (for relay/autonat/dcutr),
    // bitswap (block transfer), and ping (keep-alive) — no kademlia, no pubsub.
    let mut builder = IpfsBuilder::with_keypair(&keypair)?
        .with_identify(Default::default())
        .with_bitswap()
        .with_ping(Default::default())
        .enable_tcp()
        .enable_quic()
        // Enable the DNS transport. NOTE: connexa 0.4.1's builder composes this
        // over a *dummy* transport (the real TCP/QUIC sit outside it), so a /dns*
        // dial through it fails with "Unsupported resolved address" — it does NOT
        // actually make /dns4//dns6 dialing work. We instead resolve the host in
        // `connect()` (resolve_dns_multiaddr) and dial the literal IP, which is
        // what delivers dial-by-hostname (IP change needs no client rebuild). This
        // call is kept harmless/in-place pending an upstream connexa fix.
        .enable_dns()
        .add_listening_addr(listen_addr.parse()?)
        .with_relay(true) // relay client + DCUtR hole punching
        .with_autonat()
        // libp2p defaults idle_connection_timeout to 0 — a connection with no
        // keepalive reason closes the instant it goes idle. For peer-assist that
        // means a holder drops the requester the moment a download finishes (we
        // only keepalive from the *requesting* side). Hold idle connections ~60s
        // so they survive between blocks/tracks and long enough for the roster
        // warm loop to make the link symmetric (both ends keepalive). See lib.rs.
        .set_idle_connection_timeout(60)
        .fd_limit(rust_ipfs::FDLimit::Max)
        // Phase-1 peer control plane (P2P-NEXT-STEPS): a single request_response
        // protocol carrying CBOR Pex/Peers/Ipns queries (see peer.rs). This is the
        // built-in connexa request_response — NOT the custom-behaviour slot below,
        // which stays the no-op dummy. Responses are tiny (≤64 PeerRefs), so clamp
        // the 2 MiB default down to 256 KiB as defense-in-depth.
        .with_request_response(vec![
            RequestResponseConfig::new(crate::peer::PROTOCOL).set_max_response_size(256 * 1024),
        ])
        .with_custom_behaviour(|_| Ok(rust_ipfs::swarm::dummy::Behaviour));
    // R2: stand up the clamped relay server only when the user opted in. The clamp mirrors
    // the master's handshake-only posture — enough to broker a DCUtR hole-punch, never
    // enough to carry a stream (see relay_server_config). Inert until a peer reserves on us,
    // and peers only learn to via the willingToRelay PEX fact (M2), gated on reachability.
    if relay_willing {
        builder = builder.with_relay_server(relay_server_config());
    }
    if let Some(dir) = data_dir {
        std::fs::create_dir_all(&dir).ok();
        builder = builder.set_path(dir);
    }
    let ipfs: Ipfs = builder.start().await?;
    // Persist the port we ACTUALLY bound, so next launch reuses it. Covers both paths:
    // we reused last session's port, or fell back to ephemeral and now pin whatever the
    // OS handed us. Best-effort — a write failure just means an ephemeral port next time.
    if let Some(pf) = &port_file {
        if let Some(p) = bound_tcp_port(&ipfs).await {
            std::fs::write(pf, p.to_string()).ok();
        }
    }
    Ok(Node { ipfs, peer_id })
}

/// R2 relay-server policy: opt-in, default OFF. Reads `relay_policy.json` (`{"willing":bool}`)
/// from the data dir. Absent/unreadable/no-dir → `false`. Toggling on takes effect on the
/// next launch (the builder is one-shot); toggling off is immediate via the advertised fact.
/// `pub(crate)` so lib.rs reads the SAME value to advertise `willing` (server presence here
/// must agree with the gossiped fact).
pub(crate) fn read_relay_policy(dir: Option<&std::path::Path>) -> bool {
    #[derive(serde::Deserialize)]
    struct RelayPolicy {
        #[serde(default)]
        willing: bool,
    }
    dir.map(|d| d.join("relay_policy.json"))
        .and_then(|p| std::fs::read(p).ok())
        .and_then(|b| serde_json::from_slice::<RelayPolicy>(&b).ok())
        .map(|p| p.willing)
        .unwrap_or(false)
}

/// The clamped circuit-relay-v2 server config (R2). Mirrors the master's handshake-only
/// posture (`deploy/install.sh`): 128 KiB / 30s per circuit is enough to broker a DCUtR
/// hole-punch but never enough to carry an audio stream — so a willing client relays
/// *coordination*, not bulk bytes. Footprint kept small (defaults are master-scale 128/16):
/// a desktop client volunteers a handful of slots, not a public-relay's worth.
fn relay_server_config() -> RelayConfig {
    RelayConfig {
        max_circuit_bytes: 1 << 17, // 128 KiB — matches the master's ConnectionDataLimit
        max_circuit_duration: Duration::from_secs(30), // matches ConnectionDurationLimit
        max_reservations: 32,
        max_reservations_per_peer: 2,
        max_circuits: 8,
        max_circuits_per_peer: 2,
        ..Default::default() // keep the default PerPeer/PerIp rate limiters
    }
}

/// R2/M3: reserve a circuit-relay slot on `relay` (a willing, publicly-reachable peer we're
/// already connected to) so peers can dial us through it. Builds the circuit listen addr from
/// the relay's live direct address and adds it as a listener — which (per the vendored-relay
/// findings) BLOCKS until the relay grants the reservation, so we timeout-guard it and never
/// await it unbounded. Returns the `ListenerId` on success (track it to tear the reservation
/// down later). Best-effort: `None` on no usable relay addr / error / timeout. The granted
/// circuit addr then flows into `announce_addrs` automatically (it returns external addrs).
pub(crate) async fn reserve_on_relay(ipfs: &Ipfs, relay: PeerId) -> Option<ListenerId> {
    // The relay's live direct (non-circuit) address — the transport peers route through.
    let direct = ipfs
        .addrs()
        .await
        .ok()?
        .into_iter()
        .find(|(p, _)| *p == relay)?
        .1
        .into_iter()
        .find(|a| !a.to_string().contains("p2p-circuit"))?;
    // Build "/<relay-transport>/p2p/<relay>/p2p-circuit".
    let mut circuit = direct;
    if !matches!(circuit.iter().last(), Some(Protocol::P2p(_))) {
        circuit.push(Protocol::P2p(relay));
    }
    circuit.push(Protocol::P2pCircuit);
    match tokio::time::timeout(Duration::from_secs(20), ipfs.add_listening_address(circuit)).await {
        Ok(Ok(id)) => Some(id),
        Ok(Err(e)) => {
            eprintln!("[relay] reserve on {relay} failed: {e}");
            None
        }
        Err(_) => {
            eprintln!("[relay] reserve on {relay} timed out (no grant)");
            None
        }
    }
}

/// Tear down a self-reservation (we became publicly reachable, or the relay disconnected).
pub(crate) async fn drop_reservation(ipfs: &Ipfs, id: ListenerId) {
    let _ = ipfs.remove_listening_address(id).await;
}

/// R2/M4: dial `target` THROUGH relay `via` — a warm peer that reported it can reach the
/// target (`can_reach`). Builds "/<via-transport>/p2p/<via>/p2p-circuit/p2p/<target>" from the
/// relay's live direct address and connects. This is the residual fallback for when a holder's
/// own announced circuit addr (M3) isn't known but a common warm peer can bridge us. Err if we
/// have no usable relay addr or the relayed dial fails.
pub(crate) async fn dial_via_relay(ipfs: &Ipfs, via: PeerId, target: PeerId) -> Result<()> {
    let direct = ipfs
        .addrs()
        .await?
        .into_iter()
        .find(|(p, _)| *p == via)
        .and_then(|(_, addrs)| addrs.into_iter().find(|a| !a.to_string().contains("p2p-circuit")))
        .ok_or_else(|| anyhow!("no direct addr for relay {via}"))?;
    let mut circuit = direct;
    if !matches!(circuit.iter().last(), Some(Protocol::P2p(_))) {
        circuit.push(Protocol::P2p(via));
    }
    circuit.push(Protocol::P2pCircuit);
    circuit.push(Protocol::P2p(target));
    connect(ipfs, &circuit.to_string()).await
}

/// Choose the inbound TCP port: reuse the persisted one if it's still bindable right now,
/// else 0 (let the OS pick). Probing with a throwaway bind is a tiny TOCTOU race — if it
/// loses (port grabbed between probe and libp2p's bind), the next launch self-heals by
/// persisting whatever we actually got. `None`/unreadable/0 → ephemeral.
fn pick_listen_port(port_file: Option<&std::path::Path>) -> u16 {
    let Some(pf) = port_file else { return 0 };
    let Some(p) = std::fs::read_to_string(pf)
        .ok()
        .and_then(|s| s.trim().parse::<u16>().ok())
        .filter(|&p| p != 0)
    else {
        return 0;
    };
    // Free iff we can bind it this instant; release immediately and hand the number to
    // the builder.
    match std::net::TcpListener::bind(("0.0.0.0", p)) {
        Ok(l) => {
            drop(l);
            p
        }
        Err(_) => 0,
    }
}

/// The TCP port the live node actually bound (after `start`), read from its listen addrs.
/// All listen addrs share the one bound port, so the first TCP hop is authoritative.
async fn bound_tcp_port(ipfs: &Ipfs) -> Option<u16> {
    ipfs.listening_addresses().await.ok()?.into_iter().find_map(|m| {
        m.iter().find_map(|p| match p {
            Protocol::Tcp(port) => Some(port),
            _ => None,
        })
    })
}

/// Load the persisted ed25519 keypair from `path`, or mint + persist a new one.
/// The file is the libp2p protobuf encoding (private key material), so it is
/// written 0600 on unix. A corrupt/unreadable file is replaced (a fresh identity
/// is better than refusing to start). Deleting this file is the intended "new
/// identity" reset — the next start mints a fresh keypair.
fn load_or_create_keypair(path: &std::path::Path) -> Result<Keypair> {
    if let Ok(bytes) = std::fs::read(path) {
        match Keypair::from_protobuf_encoding(&bytes) {
            Ok(kp) => return Ok(kp),
            Err(e) => eprintln!("[identity] {} unreadable ({e}); minting fresh", path.display()),
        }
    }
    let keypair = Keypair::generate_ed25519();
    let bytes = keypair.to_protobuf_encoding()?;
    std::fs::write(path, &bytes)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).ok();
    }
    Ok(keypair)
}

/// Resolve a `/dns4|/dns6|/dnsaddr/<host>/…` multiaddr to a literal
/// `/ip4|/ip6/<addr>/…` one, preserving every other protocol hop (tcp/udp/quic/p2p).
///
/// WHY: connexa 0.4.1 (the transport builder under rust-ipfs 0.15) composes its
/// DNS transport over a *dummy* transport, while the real TCP/QUIC transports are
/// added OUTSIDE that DNS layer. So a `/dns*` dial resolves the host and then
/// hands the resolved address to the dummy → `Unsupported resolved address`, and
/// the dial never connects (the bug is still present in connexa HEAD; only the
/// wasm path was reworked). We resolve the host OURSELVES and dial the literal IP.
/// This is what makes dial-by-hostname (`trackerstream.xyz`) actually connect —
/// the durability guarantee in packages/config: a master IP change needs no
/// client rebuild. Returns None when there's no `/dns*` hop (already literal) or
/// nothing resolves in the requested address family.
async fn resolve_dns_multiaddr(ma: &Multiaddr) -> Option<Multiaddr> {
    let mut host: Option<String> = None;
    let (mut want_v4, mut want_v6) = (true, true);
    for p in ma.iter() {
        match p {
            Protocol::Dns4(h) => {
                host = Some(h.into_owned());
                want_v6 = false; // /dns4 -> A records only
            }
            Protocol::Dns6(h) => {
                host = Some(h.into_owned());
                want_v4 = false; // /dns6 -> AAAA records only
            }
            Protocol::Dnsaddr(h) => host = Some(h.into_owned()),
            _ => {}
        }
    }
    let host = host?;
    // Port is irrelevant for the A/AAAA lookup; 0 is fine.
    let ip = tokio::net::lookup_host((host.as_str(), 0))
        .await
        .ok()?
        .map(|sa| sa.ip())
        .find(|ip| match ip {
            IpAddr::V4(_) => want_v4,
            IpAddr::V6(_) => want_v6,
        })?;
    let resolved: Multiaddr = ma
        .iter()
        .map(|p| match p {
            Protocol::Dns4(_) | Protocol::Dns6(_) | Protocol::Dnsaddr(_) => match ip {
                IpAddr::V4(a) => Protocol::Ip4(a),
                IpAddr::V6(a) => Protocol::Ip6(a),
            },
            other => other,
        })
        .collect();
    Some(resolved)
}

/// Dial a peer by multiaddr (must include `/p2p/<peer-id>`). `/dns*` hosts are
/// resolved to a literal IP first (see `resolve_dns_multiaddr`).
pub async fn connect(ipfs: &Ipfs, addr: &str) -> Result<()> {
    let ma: Multiaddr = addr.parse()?;
    let dial = match resolve_dns_multiaddr(&ma).await {
        Some(resolved) => {
            eprintln!("[connect] resolved {addr} -> {resolved}");
            resolved
        }
        None => ma,
    };
    let r = ipfs.connect(dial).await;
    match &r {
        Ok(_) => eprintln!("[connect] OK {addr}"),
        Err(e) => eprintln!("[connect] FAIL {addr}: {e}"),
    }
    r?;
    Ok(())
}

/// Dial the master and HOLD the connection open — keepalive + auto-reconnect —
/// instead of the lazy one-shot dials playback makes. libp2p prunes idle
/// connections, so without this the master is only in the swarm while a fetch is
/// actually in flight: it pops into the peers pane for an uncached track, then
/// drops. `add_peer().keepalive()` keeps it from being pruned. `addrs` are the
/// config BOOTSTRAP_MULTIADDRS; `/dns*` ones are resolved here for the same
/// reason `connect()` resolves them (connexa can't dial `/dns*` itself).
///
/// We DON'T use AddPeerOpt's built-in `.reconnect()`: it re-dials on a FIXED interval
/// with no jitter (its 2nd arg is max-attempts, not interval — see vendor/rust-ipfs
/// addressbook.rs), so when the master returns after a correlated outage every client
/// re-dials it in lockstep — a thundering herd against its rcmgr. Instead we spawn our
/// own re-dial loop with jitter + capped exponential backoff. See P2P-NEXT-STEPS Phase 0.
pub async fn keepalive_master(ipfs: &Ipfs, addrs: &[String]) -> Result<()> {
    let master = master_provider().ok_or_else(|| anyhow!("no master peer id"))?;
    let mut dialable: Vec<Multiaddr> = Vec::new();
    for a in addrs {
        let Ok(ma) = a.parse::<Multiaddr>() else { continue };
        // resolve_dns_multiaddr -> None for already-literal addrs; keep them as-is.
        dialable.push(resolve_dns_multiaddr(&ma).await.unwrap_or(ma));
    }
    if dialable.is_empty() {
        return Err(anyhow!("no dialable master addresses"));
    }
    // Register the addrs + pin keepalive so the live connection isn't pruned (no
    // built-in reconnect). set_addresses puts the addrs in libp2p's addressbook, which
    // the by-peer-id dial below relies on.
    let opt = AddPeerOpt::with_peer_id(master)
        .set_addresses(dialable)
        .set_dial(true)
        .keepalive();
    ipfs.add_peer(opt).await?;
    eprintln!("[keepalive] master pinned (keepalive); custom reconnect loop armed");

    // Custom re-dial loop: poll the connection; while it's down, dial and back off
    // exponentially (capped, jittered); reset to base once connected. Spawned so the
    // command returns immediately. Ipfs is a cheap clonable handle.
    //
    // Dial BY PEER-ID (not per-addr): libp2p then uses its addressbook + happy-eyeballs
    // to form ONE managed connection and applies PeerCondition::DisconnectedAndNotDialing
    // (won't pile up dials). Dialing each addr explicitly instead opened a redundant
    // TCP *and* QUIC link to the master, which it reset — a ping-failure storm.
    let ipfs = ipfs.clone();
    const BASE: Duration = Duration::from_secs(5);
    const MAX: Duration = Duration::from_secs(300);
    tauri::async_runtime::spawn(async move {
        let mut backoff = BASE;
        loop {
            if ipfs.is_connected(master).await.unwrap_or(false) {
                backoff = BASE;
            } else {
                let _ = ipfs.connect(master).await;
                backoff = (backoff * 2).min(MAX);
            }
            tokio::time::sleep(crate::jittered(backoff)).await;
        }
    });
    Ok(())
}

/// Warm-connect a peer-assist holder (from the tracker): dial + keepalive so the
/// connection isn't pruned before playback wants it. Unlike the master we do NOT
/// `.reconnect()` — a dropped warm holder should fall out of the set rather than
/// reconnect-storm (the master is the durable link; warm holders are opportunistic).
/// `/dns*` hosts are resolved to a literal IP first (connexa can't dial `/dns*`).
pub async fn warm_connect(ipfs: &Ipfs, peer_id: &str, addrs: &[String]) -> Result<()> {
    let pid: PeerId = peer_id
        .parse()
        .map_err(|e| anyhow!("bad warm peer id {peer_id}: {e}"))?;
    let mut dialable: Vec<Multiaddr> = Vec::new();
    for a in addrs {
        let Ok(ma) = a.parse::<Multiaddr>() else { continue };
        dialable.push(resolve_dns_multiaddr(&ma).await.unwrap_or(ma));
    }
    if dialable.is_empty() {
        return Err(anyhow!("no dialable addrs for {peer_id}"));
    }
    let opt = AddPeerOpt::with_peer_id(pid)
        .set_addresses(dialable)
        .set_dial(true)
        .keepalive();
    ipfs.add_peer(opt).await?;
    Ok(())
}

/// Fetch one block (Bitswap when not local); `Block::new` verifies cid == hash.
async fn fetch_bytes(ipfs: &Ipfs, cid: Cid) -> Result<Vec<u8>> {
    let mut req = ipfs.get_block(cid).set_local(false).timeout(FETCH_TIMEOUT);
    // DO NOT unconditionally re-add `.provider(master)` here. A non-empty provider
    // list makes Bitswap TARGET only those peers and REPLACES the broadcast to
    // connected peers (verified in vendor/rust-ipfs/src/p2p/bitswap.rs) — so
    // pinning the master as provider silently routes every want to the master
    // alone and kills peer offload, with no error. We set a hint ONLY for the
    // explicit TS_PROVIDER soak override; otherwise the want broadcasts to all
    // connected peers (master + warm-set holders). See PEER-ASSIST.md §4.
    if let Some(p) = explicit_provider_override() {
        req = req.provider(p);
    }
    let block = req.await?;
    Ok(block.data().to_vec())
}

// Named so every queued future has the SAME type (FuturesUnordered requires it).
async fn fetch_pair(ipfs: &Ipfs, c: Cid) -> (Cid, Result<Vec<u8>>) {
    (c, fetch_bytes(ipfs, c).await)
}

async fn fetch_many(ipfs: &Ipfs, cids: &[Cid]) -> Result<HashMap<Cid, Vec<u8>>> {
    let mut tasks = FuturesUnordered::new();
    let mut iter = cids.iter().copied();
    for _ in 0..FETCH_CONCURRENCY {
        if let Some(c) = iter.next() {
            tasks.push(fetch_pair(ipfs, c));
        }
    }
    let mut out = HashMap::with_capacity(cids.len());
    while let Some((cid, res)) = tasks.next().await {
        out.insert(cid, res?);
        if let Some(c) = iter.next() {
            tasks.push(fetch_pair(ipfs, c));
        }
    }
    Ok(out)
}

/// Resolve a module root CID -> exact original module bytes, 100% from CID
/// blocks over libp2p. Two index levels (manifest, pcm-roots) then a concurrent
/// leaf fetch, then splice skeleton + sample PCM back into the original layout.
pub async fn reassemble(ipfs: &Ipfs, root: Cid) -> Result<Vec<u8>> {
    let manifest_bytes = fetch_bytes(ipfs, root).await?;
    let manifest: Manifest = serde_ipld_dagcbor::from_slice(&manifest_bytes)?;

    // pcm-roots (one per sample), fetched concurrently, kept in sample order.
    let mut pcm_root_futs = FuturesOrdered::new();
    for s in &manifest.samples {
        let cid = s.pcm_root;
        pcm_root_futs.push_back(async move { fetch_bytes(ipfs, cid).await });
    }
    let mut pcm_roots: Vec<PcmRoot> = Vec::with_capacity(manifest.samples.len());
    while let Some(res) = pcm_root_futs.next().await {
        pcm_roots.push(serde_ipld_dagcbor::from_slice(&res?)?);
    }

    // Every leaf chunk CID (skeleton + all sample chunks), fetched concurrently.
    let mut leaf_cids: Vec<Cid> = manifest.skeleton_chunks.clone();
    for pr in &pcm_roots {
        leaf_cids.extend_from_slice(&pr.chunks);
    }
    let leaves = fetch_many(ipfs, &leaf_cids).await?;
    let get = |c: &Cid| -> Result<&Vec<u8>> {
        leaves.get(c).ok_or_else(|| anyhow!("missing leaf {c}"))
    };

    // Reconstruct the skeleton stream, then splice into the original layout.
    let mut skeleton: Vec<u8> = Vec::new();
    for c in &manifest.skeleton_chunks {
        skeleton.extend_from_slice(get(c)?);
    }

    let total = manifest.original_length as usize;
    let mut out = vec![0u8; total];
    let mut skel_cursor = 0usize;
    let mut prev_end = 0usize;
    for (s, pr) in manifest.samples.iter().zip(pcm_roots.iter()) {
        let off = s.offset as usize;
        let gap = off - prev_end;
        out[prev_end..off].copy_from_slice(&skeleton[skel_cursor..skel_cursor + gap]);
        skel_cursor += gap;
        let mut w = off;
        for c in &pr.chunks {
            let b = get(c)?;
            out[w..w + b.len()].copy_from_slice(b);
            w += b.len();
        }
        prev_end = off + s.length as usize;
    }
    out[prev_end..].copy_from_slice(&skeleton[skel_cursor..]);

    Ok(out)
}

// --- v2 streaming: immortal instance + provide_sample + playhead prefetch ------

/// Per-stream shared state: the assembled skeleton, decoded sample PCM as it
/// arrives (pulled by the frontend per index), and the live playhead order the
/// prefetch scheduler prioritizes around.
pub struct StreamState {
    pub skeleton: Mutex<Vec<u8>>,
    pub samples: Mutex<HashMap<u32, Vec<u8>>>,
    pub playhead: AtomicU32,
}

impl Default for StreamState {
    fn default() -> Self {
        Self {
            skeleton: Mutex::new(Vec::new()),
            samples: Mutex::new(HashMap::new()),
            playhead: AtomicU32::new(0),
        }
    }
}

/// Control events to the frontend during a v2 stream (binary skeleton / sample
/// PCM are pulled separately via get_skeleton / get_sample to stay zero-copy).
#[derive(Serialize, Clone)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum StreamEvent {
    /// Skeleton assembled; here is the plan (the worklet fence's source of truth)
    /// and the total streamed-sample count (for a progress indicator).
    Skeleton { plan: PlanV2, samples: u32 },
    /// One sample's decoded PCM is resident and ready to pull + provide.
    Sample { index: u32, frames: u32 },
    Complete,
    Error { message: String },
}

/// Fetch + concatenate an ordered chunk list (skeleton or one sample's PCM).
async fn assemble(ipfs: &Ipfs, chunks: &[Cid]) -> Result<Vec<u8>> {
    let map = fetch_many(ipfs, chunks).await?;
    let mut out = Vec::new();
    for c in chunks {
        out.extend_from_slice(map.get(c).ok_or_else(|| anyhow!("missing chunk {c}"))?);
    }
    Ok(out)
}

/// Reconstruct the normalized skeleton from its structure content chunks (already
/// fetched into `blocks`) plus the zero-fill `layout` recipe [nContentChunks,
/// zeroBytes, ...]: emit each content chunk, then a run of zero bytes, repeating.
/// An empty layout means a plain concat (older manifests with no zero-fill).
fn assemble_skeleton(chunks: &[Cid], layout: &[u64], blocks: &HashMap<Cid, Vec<u8>>) -> Result<Vec<u8>> {
    let get = |c: &Cid| -> Result<&Vec<u8>> {
        blocks.get(c).ok_or_else(|| anyhow!("missing skeleton chunk {c}"))
    };
    let mut out = Vec::new();
    if layout.is_empty() {
        for c in chunks {
            out.extend_from_slice(get(c)?);
        }
        return Ok(out);
    }
    let mut ci = 0usize;
    for pair in layout.chunks(2) {
        let nc = pair[0] as usize;
        let z = pair.get(1).copied().unwrap_or(0) as usize;
        for _ in 0..nc {
            let c = chunks.get(ci).ok_or_else(|| anyhow!("skeleton layout overruns chunk list"))?;
            out.extend_from_slice(get(c)?);
            ci += 1;
        }
        out.resize(out.len() + z, 0); // synthesized zero run — never transferred
    }
    Ok(out)
}

/// Prefetch priority for a sample: forward distance (orders) from the playhead to
/// the nearest checkpoint that needs it; already-passed samples sort after all
/// upcoming ones (still fetched, for backward seek); samples in no checkpoint last.
fn need_key(orders: Option<&Vec<u32>>, ph: u32) -> u64 {
    match orders {
        Some(os) if !os.is_empty() => {
            if let Some(fwd) = os.iter().copied().filter(|&o| o >= ph).min() {
                (fwd - ph) as u64
            } else {
                let last = os.iter().copied().max().unwrap_or(0);
                1_000_000_000 + ph.saturating_sub(last) as u64
            }
        }
        _ => u64::MAX - 1,
    }
}

/// Stream a v2 root onto the immortal-instance protocol: assemble + emit the
/// skeleton + plan, then fetch each sample's decoded PCM in playhead-priority
/// order, emitting a Sample event per arrival. A v1 (un-re-baked / flat) root is
/// reassembled WHOLE and emitted as a full-load skeleton with an empty plan, so
/// the v2-only worklet plays it without a v1 code path.
pub async fn stream_v2(
    ipfs: &Ipfs,
    root: Cid,
    state: Arc<StreamState>,
    events: mpsc::UnboundedSender<StreamEvent>,
) -> Result<()> {
    let manifest: ManifestV2 = serde_ipld_dagcbor::from_slice(&fetch_bytes(ipfs, root).await?)?;

    if manifest.v != 2 {
        eprintln!("[stream] {root}: v1 root -> full reassemble (no streaming)");
        let bytes = reassemble(ipfs, root).await?;
        *state.skeleton.lock().unwrap() = bytes;
        let _ = events.send(StreamEvent::Skeleton { plan: PlanV2::default(), samples: 0 });
        let _ = events.send(StreamEvent::Complete);
        return Ok(());
    }

    let index = match manifest.index {
        Some(ix) => ix,
        None => {
            let ir = manifest
                .index_root
                .ok_or_else(|| anyhow!("v2 manifest missing index + indexRoot"))?;
            serde_ipld_dagcbor::from_slice(&fetch_bytes(ipfs, ir).await?)?
        }
    };
    let IndexV2 { samples, mut plan } = index;

    // Guard against manifests whose plan references slots that are not actually
    // streamed (samples[]). The bake's no-regression re-bake once demoted every
    // compressed slot to resident-in-skeleton without pruning the checkpoints, so a
    // fully-compressed module baked to zero streamed samples yet a non-empty plan —
    // the fence then waits forever for samples that never arrive (buffering stuck at
    // 100%) even though the skeleton already holds the full audio. Drop checkpoints
    // referencing absent slots (and any emptied checkpoint); an empty plan tells the
    // fence there is nothing to gate, so the all-resident skeleton plays at once. The
    // bake now prunes too, but this keeps already-baked corpus roots playable.
    let streamed_idx: std::collections::HashSet<u32> = samples.iter().map(|s| s.index).collect();
    let dropped = plan.checkpoints.len();
    plan.checkpoints.retain_mut(|c| {
        c.samples.retain(|s| streamed_idx.contains(s));
        !c.samples.is_empty()
    });
    let dropped = dropped - plan.checkpoints.len();

    eprintln!(
        "[stream] {root}: v2 — {} streamed samples, {} checkpoints{}",
        samples.len(),
        plan.checkpoints.len(),
        if dropped > 0 { format!(" ({dropped} checkpoint(s) pruned — referenced un-streamed slots)") } else { String::new() }
    );

    // Per-sample checkpoint orders, for playhead-priority prefetch.
    let mut orders_for: HashMap<u32, Vec<u32>> = HashMap::new();
    for cp in &plan.checkpoints {
        for &s in &cp.samples {
            orders_for.entry(s).or_default().push(cp.order);
        }
    }

    // H1 — single-batch order-0 prefetch. The cold-TTFP fence needs the skeleton AND
    // every required-at-0 sample. The old path fetched the skeleton, then each order-0
    // sample in its own serial round-trip (N+1 cold Bitswap batches) — the dominant
    // cost in the compressed-IT streaming experiment (sequential `r` round-trips, not
    // bytes). Every leaf CID is known up-front from the index, so we issue ONE want-list
    // over skeleton chunks ∪ all required-at-0 sample leaves: the order-0 PCM pipelines
    // alongside the skeleton on the same cold session instead of paying a fresh handshake
    // each. This is the lever that lets streaming beat full-load TTFP.
    let required0: std::collections::HashSet<u32> = plan.required_at_zero().into_iter().collect();
    let mut warm_cids: Vec<Cid> = manifest.skeleton_chunks.clone();
    for s in &samples {
        if required0.contains(&s.index) {
            warm_cids.extend_from_slice(&s.chunks);
        }
    }
    let warm = fetch_many(ipfs, &warm_cids).await?;

    // Skeleton from the warm batch (a valid module; create_from_memory => all-pending).
    // Interleave the structure content chunks with synthesized zero runs per the
    // layout recipe — the zeros were never transferred (the bulk of a compressed
    // IT's skeleton is zero), so this is where streaming claws back its byte win.
    let skel = assemble_skeleton(&manifest.skeleton_chunks, &manifest.skeleton_layout, &warm)?;
    *state.skeleton.lock().unwrap() = skel;
    let _ = events.send(StreamEvent::Skeleton { plan: plan.clone(), samples: samples.len() as u32 });

    // Required-at-0 samples are already in hand from the warm batch — concatenate and
    // emit them immediately, opening the fence in a single cold round-trip. Emitting
    // them before any non-required sample preserves the playhead-priority invariant the
    // bench relies on (every required-at-0 slot resident before the first non-required).
    let mut delivered: std::collections::HashSet<u32> = std::collections::HashSet::new();
    for s in &samples {
        if !required0.contains(&s.index) {
            continue;
        }
        let mut pcm = Vec::new();
        for c in &s.chunks {
            pcm.extend_from_slice(warm.get(c).ok_or_else(|| anyhow!("missing sample chunk {c}"))?);
        }
        state.samples.lock().unwrap().insert(s.index, pcm);
        let _ = events.send(StreamEvent::Sample { index: s.index, frames: s.frames });
        delivered.insert(s.index);
    }

    // Remaining (post-fence) samples: dynamic playhead-distance order (re-evaluated every
    // step, so a seek that moves the playhead re-prioritizes the queue). Their leaves were
    // not in the warm batch, so they fetch cold here — but the fence is already open.
    let mut remaining: Vec<usize> =
        (0..samples.len()).filter(|&i| !delivered.contains(&samples[i].index)).collect();
    while !remaining.is_empty() {
        let ph = state.playhead.load(Ordering::Relaxed);
        let mut best = 0usize;
        let mut best_key = u64::MAX;
        for (ri, &si) in remaining.iter().enumerate() {
            let key = need_key(orders_for.get(&samples[si].index), ph);
            if key < best_key {
                best_key = key;
                best = ri;
            }
        }
        let si = remaining.swap_remove(best);
        let s = &samples[si];
        let pcm = assemble(ipfs, &s.chunks).await?;
        state.samples.lock().unwrap().insert(s.index, pcm);
        let _ = events.send(StreamEvent::Sample { index: s.index, frames: s.frames });
    }

    let _ = events.send(StreamEvent::Complete);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pick_listen_port_reuses_a_free_port_and_falls_back_when_taken() {
        let tmp = std::env::temp_dir().join(format!("ts-port-{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();
        let pf = tmp.join("listen_port");

        // No file → ephemeral.
        assert_eq!(pick_listen_port(Some(&pf)), 0);
        // No port file path at all (headless) → ephemeral.
        assert_eq!(pick_listen_port(None), 0);

        // Record a port the OS just confirmed is free, then reuse it.
        let probe = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let free = probe.local_addr().unwrap().port();
        drop(probe);
        std::fs::write(&pf, free.to_string()).unwrap();
        assert_eq!(pick_listen_port(Some(&pf)), free, "a free persisted port is reused");

        // Occupy it → pick must fall back to ephemeral rather than collide.
        let occupy = std::net::TcpListener::bind(("0.0.0.0", free)).unwrap();
        assert_eq!(pick_listen_port(Some(&pf)), 0, "a taken persisted port falls back to 0");
        drop(occupy);

        std::fs::remove_dir_all(&tmp).ok();
    }

    // localhost resolves offline + deterministically, so this exercises the
    // /dns* -> /ip* rewrite (and the hop-preservation) without external DNS.
    #[tokio::test]
    async fn dns4_rewrites_to_ip4_preserving_hops() {
        let ma: Multiaddr =
            "/dns4/localhost/tcp/4001/p2p/12D3KooWGb7eHYgZnMFfADEDeS5xDEwEVQKPTGozsKanpDf9XvzL"
                .parse()
                .unwrap();
        let out = resolve_dns_multiaddr(&ma).await.expect("should resolve");
        let s = out.to_string();
        assert!(s.starts_with("/ip4/127.0.0.1/tcp/4001/p2p/"), "got {s}");
    }

    // A literal multiaddr has no /dns* hop -> nothing to resolve.
    #[tokio::test]
    async fn literal_multiaddr_is_left_alone() {
        let ma: Multiaddr = "/ip4/5.75.131.145/tcp/4001".parse().unwrap();
        assert!(resolve_dns_multiaddr(&ma).await.is_none());
    }

    fn plan(cps: &[(u32, &[u32])]) -> PlanV2 {
        PlanV2 {
            checkpoints: cps
                .iter()
                .map(|(order, s)| CheckpointV2 { order: *order, samples: s.to_vec() })
                .collect(),
            order_seconds: vec![],
        }
    }

    // required_at_zero must equal Fence.requiredAt(0): floor checkpoint at order 0
    // UNION the next one. Mirrors fence.spec semantics so the TTFP benchmark gates
    // on exactly the bytes the worklet fence gates on.
    #[test]
    fn required_at_zero_is_floor_union_next() {
        // Typical: first checkpoint is order 0 -> cp[0] ∪ cp[1].
        assert_eq!(plan(&[(0, &[3, 1]), (4, &[5]), (8, &[9])]).required_at_zero(), vec![1, 3, 5]);
        // Single checkpoint at order 0 -> just it (no next to union).
        assert_eq!(plan(&[(0, &[2, 7])]).required_at_zero(), vec![2, 7]);
        // No order-0 checkpoint (degenerate) -> floor = -1 -> cp[0] only, not cp[1].
        assert_eq!(plan(&[(2, &[4]), (5, &[6])]).required_at_zero(), vec![4]);
        // Empty plan -> nothing to gate.
        assert!(plan(&[]).required_at_zero().is_empty());
        // Dedup across the union (same slot in both checkpoints).
        assert_eq!(plan(&[(0, &[1, 2]), (3, &[2, 8])]).required_at_zero(), vec![1, 2, 8]);
    }

    // required_at(order) generalizes the fence to any seek target: floor∪next.
    #[test]
    fn required_at_arbitrary_order() {
        let p = plan(&[(0, &[1]), (4, &[5]), (8, &[9]), (12, &[13])]);
        assert_eq!(p.required_at(0), vec![1, 5]); // floor(0) ∪ next(4)
        assert_eq!(p.required_at(4), vec![5, 9]); // floor(4) ∪ next(8)
        assert_eq!(p.required_at(5), vec![5, 9]); // between -> floor(4)
        assert_eq!(p.required_at(8), vec![9, 13]); // floor(8) ∪ next(12)
        assert_eq!(p.required_at(12), vec![13]); // last -> no next
        assert_eq!(p.required_at(100), vec![13]); // beyond end -> floor=last
    }

    // orderSeconds must survive a dag-cbor round-trip (whole-number floats encode
    // as CBOR ints — the fec83fa crash) and map seconds -> seek order correctly.
    #[test]
    fn order_seconds_tolerates_cbor_ints_and_maps() {
        let p = PlanV2 {
            checkpoints: vec![],
            order_seconds: vec![
                OrderSec { order: 0, seconds: 0.0 },   // encodes as CBOR int 0
                OrderSec { order: 4, seconds: 1.5 },   // stays a float
                OrderSec { order: 8, seconds: 3.0 },   // encodes as CBOR int 3
            ],
        };
        let bytes = serde_ipld_dagcbor::to_vec(&p).unwrap();
        let back: PlanV2 = serde_ipld_dagcbor::from_slice(&bytes).unwrap();
        assert_eq!(back.order_seconds.len(), 3);
        assert!(back.has_order_map());
        assert_eq!(back.order_for_seconds(0.0), 0);
        assert_eq!(back.order_for_seconds(1.4), 0);
        assert_eq!(back.order_for_seconds(1.5), 4);
        assert_eq!(back.order_for_seconds(2.9), 4);
        assert_eq!(back.order_for_seconds(3.0), 8);
        assert_eq!(back.order_for_seconds(999.0), 8); // clamp to last
        // 70%-style mapping on a 10s "track": 0.70*10 = 7.0 -> last order <= 7.0
        assert_eq!(back.order_for_seconds(0.70 * 10.0), 8);
    }

    fn raw_block(data: &[u8]) -> rust_ipfs::Block {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(data);
        // 0x12 = sha2-256, 0x55 = raw codec.
        let mh = cid::multihash::Multihash::<64>::wrap(0x12, &digest).unwrap();
        let cid = Cid::new_v1(0x55, mh);
        rust_ipfs::Block::new(cid, data.to_vec()).unwrap()
    }

    // The CORE offload claim (B5 / PEER-ASSIST.md §4): with the master-provider hint
    // removed, a block fetch BROADCASTS its want to every CONNECTED peer. Two
    // ephemeral nodes, NO master: A holds a block, B merely connects to A (no
    // provider hint, no DHT) and fetches it — proving "connect-is-enough."
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn block_fetches_from_connected_peer_without_provider_hint() {
        let a = start(None).await.unwrap();
        let b = start(None).await.unwrap();

        let payload = b"trackerstream peer offload test".to_vec();
        let block = raw_block(&payload);
        let cid = *block.cid();
        a.ipfs.put_block(&block).await.unwrap();

        // Dial A from B over loopback using A's actual TCP listen port.
        let port = a
            .ipfs
            .listening_addresses()
            .await
            .unwrap()
            .iter()
            .find_map(|m| {
                m.iter().find_map(|p| match p {
                    Protocol::Tcp(port) => Some(port),
                    _ => None,
                })
            })
            .expect("A should have a TCP listener");
        let addr = format!("/ip4/127.0.0.1/tcp/{port}/p2p/{}", a.peer_id);
        connect(&b.ipfs, &addr).await.unwrap();

        // Fetch on B: network-only (set_local false), NO `.provider()` hint. If this
        // resolves, the want reached A purely via the connected-peer broadcast.
        let got = tokio::time::timeout(
            Duration::from_secs(20),
            b.ipfs.get_block(cid).set_local(false),
        )
        .await
        .expect("offload fetch timed out (want did not reach the connected peer)")
        .unwrap();
        assert_eq!(got.data(), payload.as_slice());
    }

    // R2/M1 ship-gate — retires vendoring-risk #1: a willing node (A) stands up the clamped
    // relay server, and a second node (B) reserves a circuit slot on it, ending up with a live
    // `/p2p/<A>/p2p-circuit/p2p/<B>` listen addr. Proves connexa 0.4.1 composes the relay-client
    // transport for a circuit listen addr. Three findings baked in:
    //   * Reserve via `add_listening_address(.../p2p-circuit)` (resolves on NewListenAddr), NOT
    //     `enable_relay` — the vendored enable_relay's completion channel is never drained.
    //   * A relay must have a CONFIRMED EXTERNAL ADDRESS or the voucher is empty and the client
    //     fails with NoAddressesInReservation. In prod the eligibility gate guarantees this
    //     (willingToRelay ⟹ publiclyReachable ⟹ has external addrs); here we inject one.
    //   * Exercises the real relay_policy.json opt-in read path.
    // Slow (two real nodes + a reservation handshake), so #[ignore]'d:
    //   cargo test --lib relay_reservation_yields_a_circuit_addr -- --ignored --nocapture
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "slow (~10s, spins up two real libp2p nodes + a relay reservation)"]
    async fn relay_reservation_yields_a_circuit_addr() {
        // A: willing relay server (policy opt-in written to a temp data dir).
        let dir = std::env::temp_dir().join(format!("ts_m1_relay_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("relay_policy.json"), br#"{"willing":true}"#).unwrap();
        assert!(read_relay_policy(Some(&dir)), "policy read should be willing");
        let a = start(Some(dir.clone())).await.unwrap();
        let b = start(None).await.unwrap();

        let port = a
            .ipfs
            .listening_addresses()
            .await
            .unwrap()
            .iter()
            .find_map(|m| {
                m.iter().find_map(|p| match p {
                    Protocol::Tcp(port) => Some(port),
                    _ => None,
                })
            })
            .expect("A should have a TCP listener");
        let a_addr: Multiaddr = format!("/ip4/127.0.0.1/tcp/{port}/p2p/{}", a.peer_id)
            .parse()
            .unwrap();
        // A relay can only issue a USABLE reservation if it has a confirmed external address
        // to put in the voucher (else the client gets NoAddressesInReservation and the circuit
        // listener dies). In production this is guaranteed by the eligibility gate — a node
        // advertises willingToRelay only when publiclyReachable, and a public node HAS external
        // addrs. AutoNAT can't confirm one on loopback, so we inject it to simulate a public
        // relay. This is the key R2 invariant: willingToRelay ⟹ has an external address.
        a.ipfs
            .add_external_address(format!("/ip4/127.0.0.1/tcp/{port}").parse().unwrap())
            .await
            .expect("add_external_address on the relay");

        // B dials A directly first (so identify exchanges A's relay-HOP protocol).
        connect(&b.ipfs, &a_addr.to_string()).await.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Reserve a circuit slot ON A via connexa's listen_on path (the enable_relay API is
        // incomplete in the vendored crate — its completion channel is never drained). This
        // resolves on NewListenAddr, i.e. once A grants a usable reservation. Timeout-guarded
        // so a failed grant surfaces instead of hanging.
        let circuit: Multiaddr = format!("/ip4/127.0.0.1/tcp/{port}/p2p/{}/p2p-circuit", a.peer_id)
            .parse()
            .unwrap();
        match tokio::time::timeout(Duration::from_secs(20), b.ipfs.add_listening_address(circuit))
            .await
        {
            Ok(Ok(id)) => println!("OK: reservation granted, circuit listener {id:?}"),
            Ok(Err(e)) => panic!("add_listening_address(circuit) errored: {e}"),
            Err(_) => panic!("add_listening_address(circuit) hung: reservation never granted"),
        }
        let listen = b.ipfs.listening_addresses().await.unwrap_or_default();
        assert!(
            listen.iter().any(|a| a.to_string().contains("p2p-circuit")),
            "B should expose a /p2p-circuit listen addr: {listen:?}",
        );
        println!("OK: B reserved a relay slot on A. circuit listen addrs = {listen:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // R2/M3 ship-gate proof — the genuine reachability win, BOX DOWN: a NAT'd holder (B) is
    // reached by a third node (C) PURELY through a peer relay (A), using only B's announced
    // circuit addr. C is never given B's direct addr, so success means the relay path works.
    // Mirrors the production self-reservation: B reserves on A via `reserve_on_relay`, the
    // circuit addr appears in B's listen set, and C dials it. Run:
    //   cargo test --lib nat_peer_reachable_through_peer_relay -- --ignored --nocapture
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "slow (~6s, spins up three real libp2p nodes + a relay reservation)"]
    async fn nat_peer_reachable_through_peer_relay() {
        // A: willing, publicly-reachable relay (policy opt-in + injected external addr so the
        // reservation voucher is populated — see the M1 test).
        let dir = std::env::temp_dir().join(format!("ts_m3_relay_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("relay_policy.json"), br#"{"willing":true}"#).unwrap();
        let a = start(Some(dir.clone())).await.unwrap();
        let b = start(None).await.unwrap(); // NAT'd holder
        let c = start(None).await.unwrap(); // a peer that wants to reach B
        let a_pid: PeerId = a.peer_id.parse().unwrap();
        let b_pid: PeerId = b.peer_id.parse().unwrap();

        let port = a
            .ipfs
            .listening_addresses()
            .await
            .unwrap()
            .iter()
            .find_map(|m| m.iter().find_map(|p| match p {
                Protocol::Tcp(port) => Some(port),
                _ => None,
            }))
            .expect("A TCP listener");
        a.ipfs
            .add_external_address(format!("/ip4/127.0.0.1/tcp/{port}").parse().unwrap())
            .await
            .unwrap();

        // B connects to A and self-reserves via the SAME path production uses.
        connect(&b.ipfs, &format!("/ip4/127.0.0.1/tcp/{port}/p2p/{}", a.peer_id))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let _lid = reserve_on_relay(&b.ipfs, a_pid)
            .await
            .expect("B should reserve a circuit slot on A");

        // B's announced circuit addr (full, ending in /p2p/<B>) — the ONLY way we hand C to B.
        let b_circuit = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let addrs = b.ipfs.listening_addresses().await.unwrap_or_default();
                if let Some(c) = addrs.into_iter().find(|a| a.to_string().contains("p2p-circuit")) {
                    break c;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .expect("B should expose a circuit listen addr");
        let b_dialable = with_p2p_suffix(b_circuit, b_pid);

        // C dials B THROUGH A (C never saw B's direct addr). Success = a live C↔B connection.
        connect(&c.ipfs, &b_dialable.to_string())
            .await
            .expect("C should reach NAT'd B through the peer relay A");
        let reached = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if c.ipfs.is_connected(b_pid).await.unwrap_or(false) {
                    break true;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .unwrap_or(false);
        assert!(reached, "C must be connected to B via the relay");
        println!("OK: NAT'd B reached by C purely through peer relay A ({b_dialable})");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(test)]
    fn with_p2p_suffix(mut ma: Multiaddr, peer: PeerId) -> Multiaddr {
        if !matches!(ma.iter().last(), Some(Protocol::P2p(_))) {
            ma.push(Protocol::P2p(peer));
        }
        ma
    }

    // R2/M4 — the residual fallback: B reaches NAT'd holder Q WITHOUT Q's circuit addr, by
    // asking warm peers whether any can relay it to Q (`Relay{target}` → can_reach) and dialing
    // through the one that says yes (W). Exercises both M4 building blocks — `relay_pull(Some)`
    // discovery + `dial_via_relay`. Run:
    //   cargo test --lib relay_fallback_reaches_holder_via_can_reach_peer -- --ignored --nocapture
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "slow (~6s, three real libp2p nodes + a relay reservation)"]
    async fn relay_fallback_reaches_holder_via_can_reach_peer() {
        use crate::peer::{relay_pull, spawn_serve_loop, ServeCtx};

        // W: willing, publicly-reachable relay.
        let dir = std::env::temp_dir().join(format!("ts_m4_relay_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("relay_policy.json"), br#"{"willing":true}"#).unwrap();
        let w = start(Some(dir.clone())).await.unwrap();
        let q = start(None).await.unwrap(); // NAT'd holder
        let b = start(None).await.unwrap(); // wants to reach Q
        let w_pid: PeerId = w.peer_id.parse().unwrap();
        let q_pid: PeerId = q.peer_id.parse().unwrap();
        let b_pid: PeerId = b.peer_id.parse().unwrap();

        let port = w
            .ipfs
            .listening_addresses()
            .await
            .unwrap()
            .iter()
            .find_map(|m| m.iter().find_map(|p| match p {
                Protocol::Tcp(port) => Some(port),
                _ => None,
            }))
            .expect("W TCP listener");
        let w_addr = format!("/ip4/127.0.0.1/tcp/{port}/p2p/{}", w.peer_id);
        w.ipfs
            .add_external_address(format!("/ip4/127.0.0.1/tcp/{port}").parse().unwrap())
            .await
            .unwrap();

        // W's serve loop: willing + public, with B and Q warm so it answers their relay probes.
        let w_warm = Arc::new(crate::WarmSet::default());
        let w_reach = Arc::new(crate::Reachability::default());
        for _ in 0..3 {
            w_reach.observe(true);
        }
        spawn_serve_loop(ServeCtx {
            ipfs: w.ipfs.clone(),
            held: Arc::new(crate::HeldRoots::load(None)),
            ipns: Arc::new(crate::IpnsCache::load(None)),
            book: Arc::new(crate::AddressBook::load(None)),
            self_peer: w.peer_id.clone(),
            warm: w_warm.clone(),
            reach: w_reach,
            relay_willing: true,
        });

        // Q connects to W and reserves, so W↔Q is live and Q is dialable through W.
        connect(&q.ipfs, &w_addr).await.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        reserve_on_relay(&q.ipfs, w_pid).await.expect("Q reserves on W");
        w_warm.record(q_pid, "test");

        // B connects to W and warms it (so B will probe W).
        connect(&b.ipfs, &w_addr).await.unwrap();
        w_warm.record(b_pid, "test");
        let b_warm = crate::WarmSet::default();
        b_warm.record(w_pid, "test");

        // B asks its warm peers whether any can relay it to Q → W answers can_reach.
        let via = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let replies = relay_pull(&b.ipfs, &b_warm, Some(&q.peer_id)).await;
                if let Some(r) = replies.into_iter().find(|r| r.can_reach) {
                    break r.peer;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
        .await
        .expect("a warm peer should report it can relay to Q");
        assert_eq!(via, w_pid, "W is the relay that can reach Q");

        // B dials Q through W → reaches NAT'd Q (B never had Q's circuit addr).
        dial_via_relay(&b.ipfs, via, q_pid)
            .await
            .expect("relayed dial to Q via W");
        let reached = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if b.ipfs.is_connected(q_pid).await.unwrap_or(false) {
                    break true;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .unwrap_or(false);
        assert!(reached, "B must reach Q through the can_reach relay W");
        println!("OK: M4 fallback reached NAT'd Q via the can_reach peer W");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // The REAL brittleness guard for ping capture: two live nodes ping each other
    // and we assert pinglog's map gets populated — i.e. connexa STILL logs ping
    // RTTs in the shape our tracing layer parses. If connexa changes its log
    // wording/level (or ping stops firing), this fails. Slow (~libp2p ping interval,
    // ~15s) and installs a process-global subscriber, so it's #[ignore]'d:
    //   cargo test --lib ping_capture_from_real_nodes -- --ignored --nocapture
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "slow (~15s, waits for a real libp2p ping) + sets a global tracing subscriber"]
    async fn ping_capture_from_real_nodes() {
        use tracing_subscriber::prelude::*;
        // Cross-thread (swarm task) events need a GLOBAL default subscriber.
        let sub = tracing_subscriber::registry().with(
            crate::pinglog::PingLayer
                .with_filter(tracing_subscriber::EnvFilter::new(crate::pinglog::PING_DIRECTIVE)),
        );
        let _ = tracing::subscriber::set_global_default(sub);

        let a = start(None).await.unwrap();
        let b = start(None).await.unwrap();
        let port = a
            .ipfs
            .listening_addresses()
            .await
            .unwrap()
            .iter()
            .find_map(|m| {
                m.iter().find_map(|p| match p {
                    Protocol::Tcp(port) => Some(port),
                    _ => None,
                })
            })
            .expect("A should have a TCP listener");
        connect(&b.ipfs, &format!("/ip4/127.0.0.1/tcp/{port}/p2p/{}", a.peer_id))
            .await
            .unwrap();

        // Poll until a ping RTT is captured (either direction populates the map).
        let waited = tokio::time::timeout(Duration::from_secs(40), async {
            loop {
                if crate::pinglog::ping_rtt(&a.peer_id).is_some()
                    || crate::pinglog::ping_rtt(&b.peer_id).is_some()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        })
        .await;
        assert!(
            waited.is_ok(),
            "no ping RTT captured in 40s — connexa's ping log format likely changed \
             (pinglog parse broke), or ping didn't fire"
        );
    }
}
