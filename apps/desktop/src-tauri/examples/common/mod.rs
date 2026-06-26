//! Shared harness for the streaming benchmarks (`bench` = TTFP, `seek` = cold seek).
//! Holds the bits both examples need so flags + output stay identical between them:
//! arg parsing (`--jobs`, `--ttfp-only`, `--seek`), the resilient master dial, the
//! bounded-concurrency sweep runner, and the CSV/JSON + summary writer. The per-track
//! measurement (what order the fence gates on) lives in each example.
#![allow(dead_code)] // each example uses a subset of this module.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::stream::StreamExt;
use serde::{Deserialize, Serialize};

pub const MASTER: &str =
    "/dns4/trackerstream.xyz/tcp/4001/p2p/12D3KooWGb7eHYgZnMFfADEDeS5xDEwEVQKPTGozsKanpDf9XvzL";
pub const PER_TRACK_TIMEOUT: Duration = Duration::from_secs(180);

const DEFAULT_INPUT: &str = "/private/tmp/claude-501/-Users-guysviry-git-trackerstream/ebff10c7-d4c9-4bcf-a745-a658852dfd59/scratchpad/bench-set.json";

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Track {
    pub filename: String,
    pub format: String,
    pub duration: f64,
    pub root_cid: String,
    pub id: u64,
}

/// One measured track. Superset of what either benchmark needs; the unused fields
/// stay at their defaults (e.g. `seek_ms`/`target_*` are 0 for TTFP).
#[derive(Serialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct Row {
    pub id: u64,
    pub filename: String,
    pub format: String,
    pub duration: f64,
    pub root_cid: String,
    pub checkpoints: usize,
    pub has_order_map: bool,
    pub samples_declared: u32,
    /// the play order the fence gates on (0 for TTFP; the 70%-order for seek)
    pub target_order: u32,
    pub target_seconds: f64,
    /// distinct sample slots required resident at `target_order`
    pub required: usize,
    /// of those, how many were actually streamed (rest ride in the skeleton)
    pub required_streamed: usize,
    pub skeleton_bytes: usize,
    pub required_bytes: usize,
    pub total_sample_bytes: usize,
    pub connect_ms: u128,
    pub skeleton_ms: u128,
    /// THE headline metric, measured from stream start: skeleton + required resident.
    pub ready_ms: u128,
    /// for seek only: time from the seek being issued (skeleton already resident).
    pub seek_ms: u128,
    /// 0 when not measured (e.g. --ttfp-only aborts before Complete).
    pub complete_ms: u128,
    pub status: String,
}

impl Row {
    pub fn new(t: &Track) -> Row {
        Row {
            id: t.id,
            filename: t.filename.clone(),
            format: t.format.clone(),
            duration: t.duration,
            root_cid: t.root_cid.clone(),
            status: "ok".into(),
            ..Default::default()
        }
    }
    /// Progress line printed as each track lands (self-contained — safe interleaved).
    fn line(&self) -> String {
        let mut s = format!(
            "ready={:>6}ms skel={:>5}ms(+{:>2}smp/{:>3}KB)",
            self.ready_ms, self.skeleton_ms, self.required_streamed, self.required_bytes / 1024
        );
        if self.seek_ms > 0 || self.target_order > 0 {
            s.push_str(&format!(" seek={:>6}ms@ord{}", self.seek_ms, self.target_order));
        }
        if self.complete_ms > 0 {
            s.push_str(&format!(" complete={:>6}ms", self.complete_ms));
        }
        s.push_str(&format!(" [{}]", self.status));
        s
    }
}

pub struct Args {
    pub input: String,
    pub out_dir: String,
    pub jobs: usize,
    /// Abort each stream once the fence opens, instead of running to Complete.
    pub ttfp_only: bool,
    /// Seek target as a fraction of duration (seek benchmark only). Default 0.70.
    pub seek_fraction: f64,
    /// Run the whole sweep on ONE embedded node (one master connection) instead of a
    /// fresh node per track. Eliminates the per-IP dial storm that fails high --jobs
    /// against prod (the master accepts all conns; the failure is client-side — 8
    /// connexa nodes per process race on dial/negotiation). Caveat: a shared blockstore
    /// means all-zero skeleton chunks dedup by CID across tracks, so skeleton_ms is
    /// slightly optimistic on later tracks (sample cold-fetch stays accurate).
    pub shared_node: bool,
}

/// Parse `<input.json> [out-dir] [--jobs N|-j N] [--ttfp-only] [--seek F] [--shared-node]`.
pub fn parse_args(default_out: &str) -> Args {
    let mut positional: Vec<String> = Vec::new();
    let (mut jobs, mut ttfp_only, mut seek_fraction, mut shared_node) = (1usize, false, 0.70f64, false);
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--jobs" | "-j" => jobs = it.next().and_then(|v| v.parse().ok()).unwrap_or(1).max(1),
            s if s.starts_with("--jobs=") => jobs = s[7..].parse().unwrap_or(1).max(1),
            "--ttfp-only" => ttfp_only = true,
            "--seek" => seek_fraction = it.next().and_then(|v| v.parse().ok()).unwrap_or(0.70),
            s if s.starts_with("--seek=") => seek_fraction = s[7..].parse().unwrap_or(0.70),
            "--shared-node" => shared_node = true,
            _ => positional.push(a),
        }
    }
    Args {
        input: positional.first().cloned().unwrap_or_else(|| DEFAULT_INPUT.into()),
        out_dir: positional.get(1).cloned().unwrap_or_else(|| default_out.into()),
        jobs,
        ttfp_only,
        seek_fraction,
        shared_node,
    }
}

/// Start one embedded node and connect it to the master (for `--shared-node`). The
/// returned Node must be kept alive for the whole sweep (its `ipfs` is cloned per
/// stream). Connecting once avoids the per-track dial storm entirely.
pub async fn shared_master_node() -> anyhow::Result<rust_ipfs::Ipfs> {
    let node = desktop_lib::ipfs::start(None).await?;
    connect_with_retry(&node.ipfs, 0).await?;
    eprintln!("[bench] shared node {} connected to master", node.peer_id);
    // Intentionally leak the Node so its ipfs stays alive for the process lifetime
    // (the sweep runs to completion then the process exits — no cleanup needed).
    let ipfs = node.ipfs.clone();
    std::mem::forget(node);
    Ok(ipfs)
}

/// Dial the master, retrying transient failures. With jobs>1 the fresh nodes dial
/// near-simultaneously; the master's go-libp2p Resource Manager sheds bursts of new
/// inbound connections from one IP ("Connection reset by peer" / "negotiation
/// failed"). A few staggered backoff retries recover what's recoverable. NOT counted
/// in the metric (the real player holds a persistent master peer; this is setup).
pub async fn connect_with_retry(ipfs: &rust_ipfs::Ipfs, seq: u64) -> anyhow::Result<()> {
    tokio::time::sleep(Duration::from_millis((seq % 8) * 120)).await; // spread the herd
    let mut last = None;
    for attempt in 0..6u64 {
        match desktop_lib::ipfs::connect(ipfs, MASTER).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                last = Some(e);
                tokio::time::sleep(Duration::from_millis(250 + attempt * 400)).await;
            }
        }
    }
    Err(last.unwrap_or_else(|| anyhow::anyhow!("connect failed")))
}

/// Run `per` over every track at `args.jobs` concurrency, printing each as it lands.
/// `per` builds the measurement future for one track (borrowing it for the run).
pub async fn run_sweep<'a, F>(tracks: &'a [Track], args: &Args, per: F) -> Vec<Row>
where
    // No Send bound: buffer_unordered polls these futures in-task (no tokio::spawn),
    // and ipfs::start's builder holds non-Send closures. The per-track stream_v2 IS
    // spawned, but on a cloned Ipfs (Send) inside the future, which is fine.
    F: Fn(&'a Track, usize) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<Row>> + 'a>>,
{
    let total = tracks.len();
    let done = AtomicUsize::new(0);
    if args.jobs > 1 {
        eprintln!(
            "[bench] WARNING: jobs>1 runs {} cold streams concurrently. They contend for your\n\
             [bench]          downlink + the master's Bitswap serving, so ABSOLUTE timings inflate.\n\
             [bench]          Use for fast RELATIVE ranking; run jobs=1 for true single-user latency.",
            args.jobs
        );
    }
    futures::stream::iter(tracks.iter().enumerate())
        .map(|(seq, t)| {
            let done = &done;
            let fut = per(t, seq);
            async move {
                let r = fut.await;
                let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                match &r {
                    Ok(row) => eprintln!("[{n:>3}/{total}] {:<38.38} {}", row.filename, row.line()),
                    Err(e) => eprintln!("[{n:>3}/{total}] {:<38.38} ERR {e}", t.filename),
                }
                r.ok()
            }
        })
        .buffer_unordered(args.jobs)
        .filter_map(|r| async move { r })
        .collect()
        .await
}

/// Sort by the headline metric, write `<out>/{label}.csv` + `.json`, print a table.
pub fn finish(mut rows: Vec<Row>, out_dir: &str, label: &str) -> anyhow::Result<()> {
    rows.sort_by(|a, b| b.ready_ms.cmp(&a.ready_ms));

    let mut csv = String::from(
        "ready_ms,seek_ms,skeleton_ms,complete_ms,connect_ms,format,duration_s,target_order,target_seconds,checkpoints,has_order_map,samples_declared,required,required_streamed,required_bytes,skeleton_bytes,total_sample_bytes,status,filename,root_cid\n",
    );
    for r in &rows {
        csv.push_str(&format!(
            "{},{},{},{},{},{},{:.1},{},{:.2},{},{},{},{},{},{},{},{},{},{},{}\n",
            r.ready_ms, r.seek_ms, r.skeleton_ms, r.complete_ms, r.connect_ms, r.format, r.duration,
            r.target_order, r.target_seconds, r.checkpoints, r.has_order_map, r.samples_declared,
            r.required, r.required_streamed, r.required_bytes, r.skeleton_bytes, r.total_sample_bytes,
            r.status, r.filename, r.root_cid
        ));
    }
    std::fs::write(format!("{out_dir}/{label}.csv"), &csv)?;
    std::fs::write(format!("{out_dir}/{label}.json"), serde_json::to_vec_pretty(&rows)?)?;

    println!("\n=== {label} (slowest first) — top 20 ===");
    println!(
        "{:>7} {:>7} {:>7}  {:<5} {:>4} {:>5} {:>5} {:>6}  {}",
        "ready", "seek", "skel", "fmt", "cp", "req", "reqKB", "ord", "file"
    );
    for r in rows.iter().take(20) {
        println!(
            "{:>5}ms {:>5}ms {:>5}ms  {:<5} {:>4} {:>5} {:>5} {:>6}  {}",
            r.ready_ms, r.seek_ms, r.skeleton_ms, r.format, r.checkpoints,
            r.required_streamed, r.required_bytes / 1024, r.target_order, r.filename
        );
    }

    let ok: Vec<&Row> = rows.iter().filter(|r| r.status == "ok").collect();
    let mut ms: Vec<u128> = ok.iter().map(|r| r.ready_ms).collect();
    ms.sort_unstable();
    let pct = |p: f64| ms.get(((ms.len() as f64 * p) as usize).min(ms.len().saturating_sub(1))).copied().unwrap_or(0);
    println!(
        "\n[bench] {} ok / {} total | {label} p50={}ms p90={}ms p95={}ms max={}ms",
        ok.len(), rows.len(), pct(0.50), pct(0.90), pct(0.95), ms.last().copied().unwrap_or(0)
    );
    println!("[bench] wrote {out_dir}/{label}.csv and {label}.json");
    Ok(())
}
