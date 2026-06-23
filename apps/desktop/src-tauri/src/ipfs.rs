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
use rust_ipfs::{Ipfs, Keypair, Multiaddr};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

const FETCH_TIMEOUT: Duration = Duration::from_secs(60);
const FETCH_CONCURRENCY: usize = 32;

/// The growing partial module buffer for a progressive (streaming) fetch.
#[derive(Default)]
pub struct StreamBuffer {
    pub data: Vec<u8>,
    pub version: u32,
    pub complete: bool,
}

/// Progress tick emitted to the frontend during a streaming fetch.
#[derive(Clone, Copy)]
pub struct Progress {
    pub version: u32,
    pub pct: f32,
    pub playable: bool,
    pub complete: bool,
}

#[derive(Debug, Deserialize)]
struct CdcConfig {
    #[allow(dead_code)]
    min: u64,
    #[allow(dead_code)]
    avg: u64,
    #[allow(dead_code)]
    max: u64,
}

#[derive(Debug, Deserialize)]
struct SampleEntry {
    offset: u64,
    length: u64,
    #[serde(rename = "pcmRoot")]
    pcm_root: Cid,
}

#[derive(Debug, Deserialize)]
struct Manifest {
    #[allow(dead_code)]
    v: u8,
    #[allow(dead_code)]
    format: String,
    #[serde(rename = "originalLength")]
    original_length: u64,
    #[allow(dead_code)]
    cdc: CdcConfig,
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
    let mut builder = IpfsBuilder::with_keypair(&keypair)?
        .with_default()
        .enable_tcp()
        .enable_quic()
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

/// Dial a peer by multiaddr (must include `/p2p/<peer-id>`).
pub async fn connect(ipfs: &Ipfs, addr: &str) -> Result<()> {
    let ma: Multiaddr = addr.parse()?;
    ipfs.connect(ma).await?;
    Ok(())
}

/// Fetch one block (Bitswap when not local); `Block::new` verifies cid == hash.
async fn fetch_bytes(ipfs: &Ipfs, cid: Cid) -> Result<Vec<u8>> {
    let block = ipfs
        .get_block(cid)
        .set_local(false)
        .timeout(FETCH_TIMEOUT)
        .await?;
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

/// Splice the skeleton stream into the buffer's non-sample gaps.
fn write_skeleton(out: &mut [u8], manifest: &Manifest, skel: &[u8]) {
    let mut skel_cursor = 0usize;
    let mut prev_end = 0usize;
    for s in &manifest.samples {
        let off = s.offset as usize;
        let gap = off - prev_end;
        out[prev_end..off].copy_from_slice(&skel[skel_cursor..skel_cursor + gap]);
        skel_cursor += gap;
        prev_end = off + s.length as usize;
    }
    out[prev_end..].copy_from_slice(&skel[skel_cursor..]);
}

/// Progressive reassembly: write the skeleton first (a valid module that plays
/// with not-yet-arrived samples as silence — lab/FINDINGS.md), then fill sample
/// PCM in playback-ish order, bumping `buf.version` + emitting progress so the
/// client can recreate-on-grow and start playing well before the full DAG lands.
pub async fn stream_reassemble(
    ipfs: &Ipfs,
    root: Cid,
    buf: Arc<Mutex<StreamBuffer>>,
    progress: mpsc::UnboundedSender<Progress>,
) -> Result<()> {
    let manifest: Manifest = serde_ipld_dagcbor::from_slice(&fetch_bytes(ipfs, root).await?)?;
    let total = manifest.original_length as usize;
    {
        let mut b = buf.lock().unwrap();
        b.data = vec![0u8; total];
        b.version = 0;
        b.complete = false;
    }
    let sample_total: usize = manifest.samples.iter().map(|s| s.length as usize).sum();

    // Skeleton (headers/orders/patterns) -> valid, playable module.
    let skel_map = fetch_many(ipfs, &manifest.skeleton_chunks).await?;
    let mut skel: Vec<u8> = Vec::new();
    for c in &manifest.skeleton_chunks {
        skel.extend_from_slice(skel_map.get(c).ok_or_else(|| anyhow!("missing skel {c}"))?);
    }
    let v = {
        let mut b = buf.lock().unwrap();
        write_skeleton(&mut b.data, &manifest, &skel);
        b.version += 1;
        b.version
    };
    let _ = progress.send(Progress { version: v, pct: 5.0, playable: true, complete: false });

    // Sample PCM, in file order (first patterns generally hit low samples first).
    let mut done = 0usize;
    for s in &manifest.samples {
        let pcm_root: PcmRoot =
            serde_ipld_dagcbor::from_slice(&fetch_bytes(ipfs, s.pcm_root).await?)?;
        let chunks = fetch_many(ipfs, &pcm_root.chunks).await?;
        let v = {
            let mut b = buf.lock().unwrap();
            let mut w = s.offset as usize;
            for c in &pcm_root.chunks {
                let bytes = chunks.get(c).ok_or_else(|| anyhow!("missing chunk {c}"))?;
                b.data[w..w + bytes.len()].copy_from_slice(bytes);
                w += bytes.len();
            }
            b.version += 1;
            b.version
        };
        done += s.length as usize;
        let pct = if sample_total > 0 {
            5.0 + 95.0 * (done as f32 / sample_total as f32)
        } else {
            100.0
        };
        let _ = progress.send(Progress { version: v, pct, playable: true, complete: false });
    }

    let v = {
        let mut b = buf.lock().unwrap();
        b.complete = true;
        b.version += 1;
        b.version
    };
    let _ = progress.send(Progress { version: v, pct: 100.0, playable: true, complete: true });
    Ok(())
}
