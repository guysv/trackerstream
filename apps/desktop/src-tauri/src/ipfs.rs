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
use serde::Deserialize;
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
    // Seek-support tables (MVP-FOLLOWUP B1/B2/B3); absent on older/flat manifests.
    #[serde(default)]
    segment0: Vec<u64>,
    #[serde(default)]
    seek: Option<SeekTable>,
}

// Per-order resident sample set (indices into samples[]) for a cold seek. We
// ignore the manifest's `orderSeconds` here (the seek bar's time<->order map is
// a UI concern); Rust seeks by order index, so only the checkpoints are needed.
#[derive(Debug, Deserialize)]
struct Checkpoint {
    order: u32,
    samples: Vec<u64>,
}

#[derive(Debug, Deserialize, Default)]
struct SeekTable {
    #[serde(default)]
    checkpoints: Vec<Checkpoint>,
}

#[derive(Debug, Deserialize)]
struct PcmRoot {
    chunks: Vec<Cid>,
    #[allow(dead_code)]
    length: u64,
}

/// Fetch order for a streaming reassembly: segment0 (the first pattern's audible
/// samples, B2) first, then by checkpoint appearance (earliest order a sample is
/// resident — a static playback-order proxy, B3), then any remaining samples in
/// file order. Returns indices into `manifest.samples`. Falls back to plain file
/// order when there are no tables (older/flat manifests).
fn stream_fetch_order(manifest: &Manifest) -> Vec<usize> {
    let n = manifest.samples.len();
    let mut order: Vec<usize> = Vec::with_capacity(n);
    let mut seen = vec![false; n];
    let push = |order: &mut Vec<usize>, seen: &mut Vec<bool>, i: usize| {
        if i < seen.len() && !seen[i] {
            seen[i] = true;
            order.push(i);
        }
    };
    for &i in &manifest.segment0 {
        push(&mut order, &mut seen, i as usize);
    }
    if let Some(seek) = &manifest.seek {
        for cp in &seek.checkpoints {
            for &i in &cp.samples {
                push(&mut order, &mut seen, i as usize);
            }
        }
    }
    for i in 0..n {
        push(&mut order, &mut seen, i);
    }
    order
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
    eprintln!("[stream] {root}: fetching manifest…");
    let manifest: Manifest = serde_ipld_dagcbor::from_slice(&fetch_bytes(ipfs, root).await?)?;
    let total = manifest.original_length as usize;
    eprintln!(
        "[stream] {root}: manifest ok — {} samples, {} skeleton chunks, {} bytes",
        manifest.samples.len(),
        manifest.skeleton_chunks.len(),
        total
    );
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
    // The first played pattern's audible sample set (B2). Playback must NOT start
    // until every one of these is resident — otherwise the opening renders with
    // them as silence (the chopped-opening bug) and they only "fill in" over the
    // later reload ticks. The skeleton alone is NOT playable when we have a
    // first-pattern set to wait for. When the manifest carries no segment0
    // (flat/older manifests, or non-IT formats seek.ts doesn't cover) we can't
    // know the opening set, so we keep the old behaviour: playable at skeleton.
    let seg0: std::collections::HashSet<usize> = manifest
        .segment0
        .iter()
        .map(|&i| i as usize)
        .filter(|&i| i < manifest.samples.len())
        .collect();
    let seg0_count = seg0.len();

    let v = {
        let mut b = buf.lock().unwrap();
        write_skeleton(&mut b.data, &manifest, &skel);
        b.version += 1;
        b.version
    };
    let _ = progress.send(Progress {
        version: v,
        pct: 5.0,
        playable: seg0_count == 0, // nothing to wait for -> playable now
        complete: false,
    });

    // Sample PCM in seek-table fetch order: segment0 (first pattern's audible
    // samples, B2) first, then a static playback-order proxy (B3), then the rest.
    // Falls back to file order when the manifest carries no tables. Because the
    // segment0 indices are at the FRONT of this order, `seg0_done` reaches
    // `seg0_count` exactly when the opening is fully resident — the moment we can
    // start playback without a chopped opening.
    let fetch_order = stream_fetch_order(&manifest);
    eprintln!("[stream] {root}: skeleton done, fetching {} samples", fetch_order.len());
    let mut done = 0usize;
    let mut seg0_done = 0usize;
    for (n, &si) in fetch_order.iter().enumerate() {
        let s = &manifest.samples[si];
        eprintln!(
            "[stream] {root}: sample {}/{} (idx {si}, pcmRoot {})…",
            n + 1,
            fetch_order.len(),
            s.pcm_root
        );
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
        if seg0.contains(&si) {
            seg0_done += 1;
        }
        let pct = if sample_total > 0 {
            5.0 + 95.0 * (done as f32 / sample_total as f32)
        } else {
            100.0
        };
        // Playable once the whole first-pattern set is resident (or there was
        // none to wait for). Before that the client shows "buffering".
        let playable = seg0_done >= seg0_count;
        let _ = progress.send(Progress { version: v, pct, playable, complete: false });
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

/// Cold seek (B1): resolve a module to a partial buffer playable at `target_order`,
/// fetching ONLY the resident sample set for the nearest checkpoint <= target_order
/// (plus the skeleton) instead of streaming the whole DAG — lab-measured ~3x faster
/// time-to-playback (lab/SEEK.md). The result is a valid module: the skeleton makes
/// it parse, the resident samples make the seek target audible, everything else is
/// silent until normal streaming fills it. The frontend loads this and calls
/// set_position(order). Falls back to a full reassemble when the module carries no
/// seek table (older/flat manifests).
pub async fn seek_module(ipfs: &Ipfs, root: Cid, target_order: u32) -> Result<Vec<u8>> {
    let manifest: Manifest = serde_ipld_dagcbor::from_slice(&fetch_bytes(ipfs, root).await?)?;
    let checkpoints = match &manifest.seek {
        Some(s) if !s.checkpoints.is_empty() => &s.checkpoints,
        _ => return reassemble(ipfs, root).await, // no table -> full fetch
    };
    // Nearest checkpoint at or before the target (else the earliest checkpoint).
    let cp = checkpoints
        .iter()
        .filter(|c| c.order <= target_order)
        .max_by_key(|c| c.order)
        .unwrap_or(&checkpoints[0]);

    // Skeleton -> a valid, parseable module.
    let skel_map = fetch_many(ipfs, &manifest.skeleton_chunks).await?;
    let mut skel: Vec<u8> = Vec::new();
    for c in &manifest.skeleton_chunks {
        skel.extend_from_slice(skel_map.get(c).ok_or_else(|| anyhow!("missing skel {c}"))?);
    }
    let total = manifest.original_length as usize;
    let mut out = vec![0u8; total];
    write_skeleton(&mut out, &manifest, &skel);

    // Only the samples resident at the seek target.
    for &si in &cp.samples {
        let si = si as usize;
        if si >= manifest.samples.len() {
            continue;
        }
        let s = &manifest.samples[si];
        let pcm_root: PcmRoot =
            serde_ipld_dagcbor::from_slice(&fetch_bytes(ipfs, s.pcm_root).await?)?;
        let chunks = fetch_many(ipfs, &pcm_root.chunks).await?;
        let mut w = s.offset as usize;
        for c in &pcm_root.chunks {
            let bytes = chunks.get(c).ok_or_else(|| anyhow!("missing chunk {c}"))?;
            out[w..w + bytes.len()].copy_from_slice(bytes);
            w += bytes.len();
        }
    }
    Ok(out)
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
