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
use rust_ipfs::{Ipfs, Keypair, Multiaddr, PeerId, Protocol};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

const FETCH_TIMEOUT: Duration = Duration::from_secs(60);
const FETCH_CONCURRENCY: usize = 32;

// The fra1 master (mirror of packages/config MASTER_PEER_ID). Every block fetch
// names it as the Bitswap provider, so we ask the always-on master directly
// instead of doing DHT provider discovery across the public swarm — the master
// holds the whole archive and we bootstrap-connect to it, so this is both faster
// and reliable. (Client-to-client discovery via the DHT is a deferred follow-up.)
const MASTER_PEER_ID: &str = "12D3KooWGb7eHYgZnMFfADEDeS5xDEwEVQKPTGozsKanpDf9XvzL";

fn master_provider() -> Option<PeerId> {
    MASTER_PEER_ID.parse().ok()
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

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct PlanV2 {
    #[serde(default, rename = "orderSeconds")]
    order_seconds: Vec<OrderSeconds>,
    #[serde(default)]
    checkpoints: Vec<CheckpointV2>,
}

#[derive(Debug, Deserialize, Clone, Serialize)]
struct OrderSeconds {
    order: u32,
    seconds: f64,
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
    let keypair = Keypair::generate_ed25519();
    let peer_id = keypair.public().to_peer_id().to_string();
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
        .add_listening_addr("/ip4/0.0.0.0/tcp/0".parse()?)
        .with_relay(true) // relay client + DCUtR hole punching
        .with_autonat()
        .fd_limit(rust_ipfs::FDLimit::Max)
        .with_custom_behaviour(|_| Ok(rust_ipfs::swarm::dummy::Behaviour));
    if let Some(dir) = data_dir {
        std::fs::create_dir_all(&dir).ok();
        builder = builder.set_path(dir);
    }
    let ipfs: Ipfs = builder.start().await?;
    Ok(Node { ipfs, peer_id })
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

/// Fetch one block (Bitswap when not local); `Block::new` verifies cid == hash.
async fn fetch_bytes(ipfs: &Ipfs, cid: Cid) -> Result<Vec<u8>> {
    let mut req = ipfs.get_block(cid).set_local(false).timeout(FETCH_TIMEOUT);
    // Ask the master directly (Bitswap dials it) rather than DHT-discovering
    // providers — skips the flaky public-swarm crawl.
    if let Some(p) = master_provider() {
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

use std::sync::atomic::{AtomicU32, Ordering};

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
    let IndexV2 { samples, plan } = index;
    eprintln!(
        "[stream] {root}: v2 — {} streamed samples, {} checkpoints",
        samples.len(),
        plan.checkpoints.len()
    );

    // Skeleton first (a valid module; create_from_memory => all-pending samples).
    let skel = assemble(ipfs, &manifest.skeleton_chunks).await?;
    *state.skeleton.lock().unwrap() = skel;
    let _ = events.send(StreamEvent::Skeleton { plan: plan.clone(), samples: samples.len() as u32 });

    // Per-sample checkpoint orders, for playhead-priority prefetch.
    let mut orders_for: HashMap<u32, Vec<u32>> = HashMap::new();
    for cp in &plan.checkpoints {
        for &s in &cp.samples {
            orders_for.entry(s).or_default().push(cp.order);
        }
    }

    // Fetch each sample in dynamic playhead-distance order (re-evaluated every step,
    // so a seek that moves the playhead re-prioritizes the remaining queue).
    let mut remaining: Vec<usize> = (0..samples.len()).collect();
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
}
