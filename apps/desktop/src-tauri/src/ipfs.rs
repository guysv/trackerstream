//! Data-plane module logic, now driven over the tsnode sidecar's RPC (`crate::rpc::NodeRpc`)
//! instead of an in-process rust-ipfs node. The libp2p/Bitswap node is the external Go
//! process (`crate::sidecar`); this module keeps the pure, valuable logic — fetch each DAG
//! block over the RPC (`block/get`, CID-verified), and reassemble the EXACT original module
//! bytes (v1 whole-file and v2 streaming). Mirrors packages/repack/src/dag.ts; interops with
//! the same CIDv1 / sha2-256 / raw + dag-cbor block scheme as the master.

use anyhow::{anyhow, Result};
use cid::Cid;
use crate::rpc::NodeRpc;
use futures::stream::{FuturesOrdered, FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

const FETCH_CONCURRENCY: usize = 32;

// The master's stable swarm identity (mirror of packages/config MASTER_PEER_ID) — used by the
// peers pane to tag the master row vs warm-set holders. The Go node bootstraps to it; block
// fetches broadcast their want to every connected peer (master + warm holders), so no provider
// hint is needed here.
const MASTER_PEER_ID: &str = "12D3KooWGb7eHYgZnMFfADEDeS5xDEwEVQKPTGozsKanpDf9XvzL";

/// Master host/IP (mirror of packages/config MASTER_HOST / MASTER_IPV4) used to build the
/// sidecar's default bootstrap list.
const MASTER_HOST: &str = "trackerstream.xyz";
const MASTER_IPV4: &str = "5.75.131.145";
// Non-default swarm port (mirror of packages/config LIBP2P_SWARM_PORT). Off :4001 so
// public-IPFS scanners that cached the master's old IP:4001 no longer reach it.
const MASTER_SWARM_PORT: u16 = 5478;

/// The master's PeerId string (TS_PROVIDER override or the hardcoded default), for tagging the
/// master row in the peers pane.
pub fn master_peer_id() -> String {
    std::env::var("TS_PROVIDER").unwrap_or_else(|_| MASTER_PEER_ID.to_string())
}

/// The comma-separated bootstrap multiaddr list the sidecar dials at startup so it connects to
/// the master (and thus can resolve the catalog over the custom DHT) without waiting on the
/// frontend's `keepalive_master`. `TS_BOOTSTRAP` overrides; otherwise we build the same /dns +
/// /ip4 set as packages/config BOOTSTRAP_MULTIADDRS. The Go node keepalives the link itself.
pub fn default_bootstrap() -> String {
    if let Ok(b) = std::env::var("TS_BOOTSTRAP") {
        if !b.is_empty() {
            return b;
        }
    }
    let pid = master_peer_id();
    [
        format!("/dns4/{MASTER_HOST}/udp/{MASTER_SWARM_PORT}/quic-v1/p2p/{pid}"),
        format!("/dns4/{MASTER_HOST}/tcp/{MASTER_SWARM_PORT}/p2p/{pid}"),
        format!("/ip4/{MASTER_IPV4}/udp/{MASTER_SWARM_PORT}/quic-v1/p2p/{pid}"),
        format!("/ip4/{MASTER_IPV4}/tcp/{MASTER_SWARM_PORT}/p2p/{pid}"),
    ]
    .join(",")
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


/// Fetch one block over the sidecar RPC (`block/get`), verifying cid == sha2-256(bytes) as
/// defense in depth (Bitswap already content-addresses, but the reassembly path re-checks).
async fn fetch_bytes(rpc: &NodeRpc, cid: Cid) -> Result<Vec<u8>> {
    let bytes = rpc.block_get(&cid.to_string()).await?;
    verify_cid(&cid, &bytes)?;
    Ok(bytes)
}

/// Re-derive the CID from the bytes and compare — rejects a block whose payload doesn't match
/// its address (the Go node verifies too; this is the in-Rust trust boundary on reassembly).
fn verify_cid(cid: &Cid, bytes: &[u8]) -> Result<()> {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    let mh = cid::multihash::Multihash::<64>::wrap(0x12, &digest)
        .map_err(|e| anyhow!("multihash: {e}"))?;
    let got = Cid::new_v1(cid.codec(), mh);
    if &got != cid {
        return Err(anyhow!("block {cid} failed CID verification (got {got})"));
    }
    Ok(())
}

// Named so every queued future has the SAME type (FuturesUnordered requires it).
async fn fetch_pair(rpc: &NodeRpc, c: Cid) -> (Cid, Result<Vec<u8>>) {
    (c, fetch_bytes(rpc, c).await)
}

async fn fetch_many(rpc: &NodeRpc, cids: &[Cid]) -> Result<HashMap<Cid, Vec<u8>>> {
    let mut tasks = FuturesUnordered::new();
    let mut iter = cids.iter().copied();
    for _ in 0..FETCH_CONCURRENCY {
        if let Some(c) = iter.next() {
            tasks.push(fetch_pair(rpc, c));
        }
    }
    let mut out = HashMap::with_capacity(cids.len());
    while let Some((cid, res)) = tasks.next().await {
        out.insert(cid, res?);
        if let Some(c) = iter.next() {
            tasks.push(fetch_pair(rpc, c));
        }
    }
    Ok(out)
}


/// Resolve a module root CID -> exact original module bytes, 100% from CID
/// blocks over libp2p. Two index levels (manifest, pcm-roots) then a concurrent
/// leaf fetch, then splice skeleton + sample PCM back into the original layout.
pub async fn reassemble(rpc: &NodeRpc, root: Cid) -> Result<Vec<u8>> {
    let manifest_bytes = fetch_bytes(rpc, root).await?;
    let manifest: Manifest = serde_ipld_dagcbor::from_slice(&manifest_bytes)?;

    // pcm-roots (one per sample), fetched concurrently, kept in sample order.
    let mut pcm_root_futs = FuturesOrdered::new();
    for s in &manifest.samples {
        let cid = s.pcm_root;
        pcm_root_futs.push_back(async move { fetch_bytes(rpc, cid).await });
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
    let leaves = fetch_many(rpc, &leaf_cids).await?;
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
async fn assemble(rpc: &NodeRpc, chunks: &[Cid]) -> Result<Vec<u8>> {
    let map = fetch_many(rpc, chunks).await?;
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
    rpc: &NodeRpc,
    root: Cid,
    state: Arc<StreamState>,
    events: mpsc::UnboundedSender<StreamEvent>,
) -> Result<()> {
    let manifest: ManifestV2 = serde_ipld_dagcbor::from_slice(&fetch_bytes(rpc, root).await?)?;

    if manifest.v != 2 {
        eprintln!("[stream] {root}: v1 root -> full reassemble (no streaming)");
        let bytes = reassemble(rpc, root).await?;
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
            serde_ipld_dagcbor::from_slice(&fetch_bytes(rpc, ir).await?)?
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
    let warm = fetch_many(rpc, &warm_cids).await?;

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
        let pcm = assemble(rpc, &s.chunks).await?;
        state.samples.lock().unwrap().insert(s.index, pcm);
        let _ = events.send(StreamEvent::Sample { index: s.index, frames: s.frames });
    }

    let _ = events.send(StreamEvent::Complete);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(back.order_for_seconds(0.70 * 10.0), 8);
    }
}
