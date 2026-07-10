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
    // `v` is read separately via manifest_version() for the streaming dispatch (a v2 body only
    // reaches this full decode after the version check), so it's intentionally not a field here
    // — serde ignores the unknown `v` key.
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

// --- v3 manifest: v2 streaming + BYTE-EXACT reassembly from the SAME blocks (REBUILD.md) ---
// v3 streams exactly like v2 (parsed as ManifestV2 for playback — serde ignores the extra
// fields). For DOWNLOAD it carries, per streamed sample, the on-disk file `offset` + an `enc`
// transform tag, plus `originalLength`, so the original file is reconstructed by assembling the
// skeleton then writing enc(decodedPCM) back at each sample's offset. The decoded native PCM v2
// stores is a deterministic, invertible transform of the on-disk bytes, so no bytes are dup'd.

#[derive(Debug, Deserialize)]
struct SampleV3 {
    #[allow(dead_code)]
    index: u32,
    #[allow(dead_code)]
    frames: u32,
    channels: u32, // native interleave 1|2
    #[serde(rename = "bitDepth")]
    bit_depth: u32, // 8|16
    chunks: Vec<Cid>, // decoded native-layout PCM leaves (same blocks v2 streams)
    offset: u64, // on-disk byte offset in the original file
    enc: u32, // ENC_* bitmask: enc(decoded) == on-disk bytes
}

#[derive(Debug, Deserialize)]
struct IndexV3 {
    samples: Vec<SampleV3>,
}

#[derive(Debug, Deserialize)]
struct ManifestV3 {
    #[serde(rename = "originalLength")]
    original_length: u64,
    #[serde(rename = "skeletonChunks")]
    skeleton_chunks: Vec<Cid>,
    #[serde(default, rename = "skeletonLayout")]
    skeleton_layout: Vec<u64>,
    #[serde(default)]
    index: Option<IndexV3>,
    #[serde(default, rename = "indexRoot")]
    index_root: Option<Cid>,
}

const ENC_SIGN: u32 = 0x1;
const ENC_DELTA: u32 = 0x2;
const ENC_DEINT: u32 = 0x4;

/// Turn decoded native PCM into on-disk bytes under `enc` (DEINT -> SIGN -> DELTA). MUST stay in
/// lockstep with `applyEnc` in packages/repack/src/dag.ts (the bake byte-verified the tag).
fn apply_enc(enc: u32, d: &[u8], bit_depth: u32, channels: u32) -> Vec<u8> {
    let bps = if bit_depth == 16 { 2usize } else { 1usize };
    let mut cur = d.to_vec();

    if enc & ENC_DEINT != 0 && channels == 2 && cur.len() % (bps * 2) == 0 {
        let frames = cur.len() / (bps * 2);
        let mut out = vec![0u8; cur.len()];
        for f in 0..frames {
            for c in 0..2 {
                for b in 0..bps {
                    out[(c * frames + f) * bps + b] = cur[(f * 2 + c) * bps + b];
                }
            }
        }
        cur = out;
    }
    if enc & ENC_SIGN != 0 {
        if bps == 1 {
            for x in cur.iter_mut() {
                *x ^= 0x80;
            }
        } else {
            let mut i = 1;
            while i < cur.len() {
                cur[i] ^= 0x80;
                i += 2;
            }
        }
    }
    if enc & ENC_DELTA != 0 {
        let nch = if enc & ENC_DEINT != 0 && channels == 2 { 2 } else { 1 };
        let ch_len = cur.len() / nch;
        let mut out = vec![0u8; cur.len()];
        for c in 0..nch {
            let base = c * ch_len;
            if bps == 1 {
                let mut p: u8 = 0;
                for i in 0..ch_len {
                    let v = cur[base + i];
                    out[base + i] = v.wrapping_sub(p);
                    p = v;
                }
            } else {
                let mut p: u16 = 0;
                let mut i = 0;
                while i + 1 < ch_len {
                    let v = (cur[base + i] as u16) | ((cur[base + i + 1] as u16) << 8);
                    let dd = v.wrapping_sub(p);
                    out[base + i] = dd as u8;
                    out[base + i + 1] = (dd >> 8) as u8;
                    p = v;
                    i += 2;
                }
            }
        }
        cur = out;
    }
    cur
}

/// Reassemble the BYTE-EXACT original from a v3 root: assemble the skeleton (streamed regions are
/// zero there), then write enc(decodedPCM) into each sample's file offset. Resident samples already
/// sit in the skeleton. Every block CID is verified on fetch (fetch_bytes / fetch_many).
pub async fn reassemble_v3(rpc: &NodeRpc, root: Cid) -> Result<Vec<u8>> {
    let manifest_bytes = fetch_bytes(rpc, root).await?;
    let manifest: ManifestV3 = serde_ipld_dagcbor::from_slice(&manifest_bytes)?;
    let index = match manifest.index {
        Some(ix) => ix,
        None => {
            let ir = manifest.index_root.ok_or_else(|| anyhow!("v3 manifest missing index + indexRoot"))?;
            serde_ipld_dagcbor::from_slice(&fetch_bytes(rpc, ir).await?)?
        }
    };

    // One concurrent fetch of every block (skeleton structure + all sample PCM leaves).
    let mut all: Vec<Cid> = manifest.skeleton_chunks.clone();
    for s in &index.samples {
        all.extend_from_slice(&s.chunks);
    }
    let blocks = fetch_many(rpc, &all).await?;

    let mut out = assemble_skeleton(&manifest.skeleton_chunks, &manifest.skeleton_layout, &blocks)?;
    if out.len() as u64 != manifest.original_length {
        return Err(anyhow!("v3 skeleton length {} != originalLength {}", out.len(), manifest.original_length));
    }
    for s in &index.samples {
        let mut pcm = Vec::new();
        for c in &s.chunks {
            pcm.extend_from_slice(blocks.get(c).ok_or_else(|| anyhow!("missing sample chunk {c}"))?);
        }
        let enc = apply_enc(s.enc, &pcm, s.bit_depth, s.channels);
        let off = s.offset as usize;
        if off + enc.len() > out.len() {
            return Err(anyhow!("v3 sample at {off} (+{}) overruns {}", enc.len(), out.len()));
        }
        out[off..off + enc.len()].copy_from_slice(&enc);
    }
    Ok(out)
}

/// Reassemble the byte-exact original from ANY reassemble-able root, dispatching on manifest
/// version: v3 (streaming + reassembly), v0/v1 (whole-file byte-exact). A v2 (streaming-only)
/// root has no byte-exact path; a newer version is unsupported. Used by the "Open with" download.
pub async fn reassemble_any(rpc: &NodeRpc, root: Cid) -> Result<Vec<u8>> {
    let manifest_bytes = fetch_bytes(rpc, root).await?;
    match manifest_version(&manifest_bytes) {
        3 => reassemble_v3(rpc, root).await,
        0 | 1 => reassemble(rpc, root).await,
        2 => Err(anyhow!("v2 (streaming-only) root has no byte-exact reassembly; re-bake to v3")),
        other => Err(anyhow!("unsupported manifest version {other}; please update trackerstream")),
    }
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

/// Read just the manifest version from its dag-cbor body, for the streaming dispatch. A body
/// with no `v` field (pre-versioning v1 manifests) — or one that doesn't decode at all — reads
/// as 0, which routes to the v1 reassembly path (which then verifies/errors on its own terms).
/// Keeping this separate from the full ManifestV2 decode is what lets an unknown-newer version
/// be recognised and degraded instead of silently mis-parsed as v1.
fn manifest_version(bytes: &[u8]) -> u8 {
    #[derive(Deserialize)]
    struct ManifestHeader {
        #[serde(default)]
        v: u8,
    }
    serde_ipld_dagcbor::from_slice::<ManifestHeader>(bytes)
        .map(|h| h.v)
        .unwrap_or(0)
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
    let manifest_bytes = fetch_bytes(rpc, root).await?;

    // Version dispatch on a MINIMAL header first (wire-version hardening). The old code parsed
    // the body as ManifestV2 and treated "v != 2" as v1 — so a FUTURE v3 root fell into the v1
    // reassembly and silently mis-decoded. Instead read just `v`, then route explicitly: an
    // unknown-newer version DEGRADES to a clean error rather than running the v1 path on a body
    // it doesn't understand.
    let ver = manifest_version(&manifest_bytes);
    match ver {
        // v3 streams identically to v2 — it only ADDS reassembly fields (offset/enc/originalLength)
        // that the ManifestV2 decode below ignores. So both route through the v2 streaming path.
        2 | 3 => {} // fall through to the v2 streaming path below
        0 | 1 => {
            log::info!(target: "stream", "{root}: v{ver} root -> full reassemble (no streaming)");
            let bytes = reassemble(rpc, root).await?;
            *state.skeleton.lock().unwrap() = bytes;
            let _ = events.send(StreamEvent::Skeleton { plan: PlanV2::default(), samples: 0 });
            let _ = events.send(StreamEvent::Complete);
            return Ok(());
        }
        other => {
            // A manifest from a newer bake than this client understands. Do NOT run the v1
            // reassembly on it (that mis-decodes); surface a clean error the UI can turn into
            // "unsupported, please update".
            return Err(anyhow!("unsupported manifest version {other}; please update trackerstream"));
        }
    }

    let manifest: ManifestV2 = serde_ipld_dagcbor::from_slice(&manifest_bytes)?;

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

    log::info!(
        target: "stream",
        "{root}: v2 — {} streamed samples, {} checkpoints{}",
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

    // apply_enc must byte-match packages/repack/src/dag.ts applyEnc (the bake picks + byte-verifies
    // the tag there; the client reverses it here). Vectors generated from the TS impl over the same
    // 16-byte input across every enc mask (0..8) x bitDepth {8,16} x channels {1,2}.
    #[test]
    fn apply_enc_matches_ts() {
        let input: [u8; 16] = [11, 48, 85, 122, 159, 196, 233, 14, 51, 88, 125, 162, 199, 236, 17, 54];
        let cases: &[(u32, u32, u32, &[u8])] = &[
            (0, 8, 1, &[11, 48, 85, 122, 159, 196, 233, 14, 51, 88, 125, 162, 199, 236, 17, 54]),
            (1, 8, 1, &[139, 176, 213, 250, 31, 68, 105, 142, 179, 216, 253, 34, 71, 108, 145, 182]),
            (2, 8, 1, &[11, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37]),
            (3, 8, 1, &[139, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37]),
            (4, 8, 1, &[11, 48, 85, 122, 159, 196, 233, 14, 51, 88, 125, 162, 199, 236, 17, 54]),
            (5, 8, 1, &[139, 176, 213, 250, 31, 68, 105, 142, 179, 216, 253, 34, 71, 108, 145, 182]),
            (6, 8, 1, &[11, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37]),
            (7, 8, 1, &[139, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37]),
            (0, 8, 2, &[11, 48, 85, 122, 159, 196, 233, 14, 51, 88, 125, 162, 199, 236, 17, 54]),
            (1, 8, 2, &[139, 176, 213, 250, 31, 68, 105, 142, 179, 216, 253, 34, 71, 108, 145, 182]),
            (2, 8, 2, &[11, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37]),
            (3, 8, 2, &[139, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37]),
            (4, 8, 2, &[11, 85, 159, 233, 51, 125, 199, 17, 48, 122, 196, 14, 88, 162, 236, 54]),
            (5, 8, 2, &[139, 213, 31, 105, 179, 253, 71, 145, 176, 250, 68, 142, 216, 34, 108, 182]),
            (6, 8, 2, &[11, 74, 74, 74, 74, 74, 74, 74, 48, 74, 74, 74, 74, 74, 74, 74]),
            (7, 8, 2, &[139, 74, 74, 74, 74, 74, 74, 74, 176, 74, 74, 74, 74, 74, 74, 74]),
            (0, 16, 1, &[11, 48, 85, 122, 159, 196, 233, 14, 51, 88, 125, 162, 199, 236, 17, 54]),
            (1, 16, 1, &[11, 176, 85, 250, 159, 68, 233, 142, 51, 216, 125, 34, 199, 108, 17, 182]),
            (2, 16, 1, &[11, 48, 74, 74, 74, 74, 74, 74, 74, 73, 74, 74, 74, 74, 74, 73]),
            (3, 16, 1, &[11, 176, 74, 74, 74, 74, 74, 74, 74, 73, 74, 74, 74, 74, 74, 73]),
            (4, 16, 1, &[11, 48, 85, 122, 159, 196, 233, 14, 51, 88, 125, 162, 199, 236, 17, 54]),
            (5, 16, 1, &[11, 176, 85, 250, 159, 68, 233, 142, 51, 216, 125, 34, 199, 108, 17, 182]),
            (6, 16, 1, &[11, 48, 74, 74, 74, 74, 74, 74, 74, 73, 74, 74, 74, 74, 74, 73]),
            (7, 16, 1, &[11, 176, 74, 74, 74, 74, 74, 74, 74, 73, 74, 74, 74, 74, 74, 73]),
            (0, 16, 2, &[11, 48, 85, 122, 159, 196, 233, 14, 51, 88, 125, 162, 199, 236, 17, 54]),
            (1, 16, 2, &[11, 176, 85, 250, 159, 68, 233, 142, 51, 216, 125, 34, 199, 108, 17, 182]),
            (2, 16, 2, &[11, 48, 74, 74, 74, 74, 74, 74, 74, 73, 74, 74, 74, 74, 74, 73]),
            (3, 16, 2, &[11, 176, 74, 74, 74, 74, 74, 74, 74, 73, 74, 74, 74, 74, 74, 73]),
            (4, 16, 2, &[11, 48, 159, 196, 51, 88, 199, 236, 85, 122, 233, 14, 125, 162, 17, 54]),
            (5, 16, 2, &[11, 176, 159, 68, 51, 216, 199, 108, 85, 250, 233, 142, 125, 34, 17, 182]),
            (6, 16, 2, &[11, 48, 148, 148, 148, 147, 148, 148, 85, 122, 148, 148, 148, 147, 148, 147]),
            (7, 16, 2, &[11, 176, 148, 148, 148, 147, 148, 148, 85, 250, 148, 148, 148, 147, 148, 147]),
        ];
        for &(enc, bd, ch, expected) in cases {
            let got = apply_enc(enc, &input, bd, ch);
            assert_eq!(got, expected, "apply_enc(enc={enc}, bd={bd}, ch={ch}) mismatch vs TS");
        }
    }

    // The streaming dispatch reads the manifest version from a minimal header: v1/v2 route to
    // their paths, a NEWER version is recognised (so stream_v2 degrades to a clean error rather
    // than mis-running the v1 reassembly), and an absent/undecodable `v` reads as v1.
    #[test]
    fn manifest_version_dispatch() {
        use std::collections::BTreeMap;
        let with_v = |v: u8| {
            let mut m: BTreeMap<String, u8> = BTreeMap::new();
            m.insert("v".into(), v);
            serde_ipld_dagcbor::to_vec(&m).unwrap()
        };
        assert_eq!(manifest_version(&with_v(1)), 1);
        assert_eq!(manifest_version(&with_v(2)), 2);
        assert_eq!(manifest_version(&with_v(3)), 3); // v3 -> streams via the v2 path (+ reassembly)
        assert_eq!(manifest_version(&with_v(4)), 4); // newer -> caller emits "please update"
        // A pre-versioning v1 manifest with no `v` field reads as 0 -> v1 reassembly path.
        let mut no_v: BTreeMap<String, u64> = BTreeMap::new();
        no_v.insert("originalLength".into(), 42);
        assert_eq!(manifest_version(&serde_ipld_dagcbor::to_vec(&no_v).unwrap()), 0);
        // Undecodable bytes read as 0 (routed to reassembly, which errors on its own terms).
        assert_eq!(manifest_version(b"\xff not cbor"), 0);
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
        assert_eq!(back.order_for_seconds(0.70 * 10.0), 8);
    }
}
