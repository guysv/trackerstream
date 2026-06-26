//! TTFP (time-to-first-playable) benchmark for the v2 streaming path.
//!
//! Measures, per track, the wall-clock from the start of `stream_v2` until the
//! client fence (apps/desktop/src/lib/audio/fence.ts) would open playback at
//! order 0 — i.e. skeleton resident AND every sample in `PlanV2::required_at_zero()`
//! (the floor∪next checkpoint union at order 0) arrived over real Bitswap. That is
//! exactly "enough of the track to start playing it." Each track runs on a FRESH
//! in-memory node (cold/uncached); the node is connected before the timer starts, so
//! the metric excludes the dial (the real player holds a persistent master peer).
//!
//!   cargo run --release --example bench -- <bench-set.json> [out-dir] [--jobs N] [--ttfp-only]
//!
//! --jobs N    run N tracks concurrently (faster RELATIVE ranking; inflates absolute ms).
//! --ttfp-only abort each stream the moment the fence opens, instead of running to
//!             Complete — roughly halves wall-clock on long tracks, no master stress,
//!             no measurement change (drops only the `complete_ms` column).
use std::sync::Arc;
use std::time::Instant;

use cid::Cid;
use desktop_lib::ipfs::{self, StreamEvent, StreamState};

mod common;
use common::{connect_with_retry, finish, parse_args, run_sweep, shared_master_node, Args, Row, Track, PER_TRACK_TIMEOUT};

/// Run one cold-cache stream and measure TTFP. `seq` only staggers the dial. When
/// `shared` is Some, reuse that already-connected node (no per-track dial); else spin
/// a fresh node + connect (the true-cold default).
async fn bench_one(
    t: &Track,
    seq: usize,
    ttfp_only: bool,
    shared: Option<rust_ipfs::Ipfs>,
) -> anyhow::Result<Row> {
    let root: Cid = t.root_cid.parse()?;
    let mut row = Row::new(t);

    // _own holds the fresh Node alive for the stream; None in shared mode.
    let (ipfs, _own) = match shared {
        Some(ip) => (ip, None),
        None => {
            let node = ipfs::start(None).await?;
            let cs = Instant::now();
            connect_with_retry(&node.ipfs, seq as u64).await?;
            row.connect_ms = cs.elapsed().as_millis();
            let ipfs = node.ipfs.clone();
            (ipfs, Some(node))
        }
    };

    let state = Arc::new(StreamState::default());
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<StreamEvent>();
    let ev_ipfs = ipfs.clone();
    let st = state.clone();
    let t0 = Instant::now();
    let handle = tokio::spawn(async move { ipfs::stream_v2(&ev_ipfs, root, st, tx).await });

    // Required-at-0 slots still awaited. Drains as they stream in; slots that ride in
    // the skeleton never arrive (already resident) — handled at the boundary below.
    let mut required: std::collections::HashSet<u32> = std::collections::HashSet::new();
    let mut skeleton_ms = 0u128;
    let mut last_required_ms = 0u128; // latest arrival among streamed required slots

    let drive = async {
        while let Some(ev) = rx.recv().await {
            match ev {
                StreamEvent::Skeleton { plan, samples } => {
                    skeleton_ms = t0.elapsed().as_millis();
                    row.skeleton_ms = skeleton_ms;
                    row.skeleton_bytes = state.skeleton.lock().unwrap().len();
                    row.samples_declared = samples;
                    row.checkpoints = plan.checkpoint_count();
                    row.has_order_map = plan.has_order_map();
                    required = plan.required_at_zero().into_iter().collect();
                    row.required = required.len();
                    // Nothing to fetch (v1/flat, or all required ride in the skeleton):
                    // the fence opens the instant the skeleton is resident.
                    if ttfp_only && required.is_empty() {
                        break;
                    }
                }
                StreamEvent::Sample { index, .. } => {
                    let bytes = state.samples.lock().unwrap().get(&index).map(|v| v.len()).unwrap_or(0);
                    row.total_sample_bytes += bytes;
                    if required.remove(&index) {
                        last_required_ms = t0.elapsed().as_millis();
                        row.required_streamed += 1;
                        row.required_bytes += bytes;
                        // All streamed required slots in hand (any leftover ride in the
                        // skeleton) -> fence is open.
                        if ttfp_only && required.is_empty() {
                            break;
                        }
                    } else if ttfp_only {
                        // Prefetch is playhead-priority: required-at-0 slots (need_key
                        // 0 for cp[0], cp[1].order for cp[1]) are ALL fetched strictly
                        // before any non-required slot (need_key >= cp[2].order). So the
                        // first non-required arrival proves every streamed required slot
                        // is already resident — the fence is open. (Leftover required
                        // slots, if any, are skeleton-resident.)
                        break;
                    }
                }
                StreamEvent::Complete => {
                    row.complete_ms = t0.elapsed().as_millis();
                    break;
                }
                StreamEvent::Error { message } => {
                    row.status = format!("stream-error: {message}");
                    break;
                }
            }
        }
    };

    if tokio::time::timeout(PER_TRACK_TIMEOUT, drive).await.is_err() {
        row.status = "timeout".into();
    }
    handle.abort();

    // TTFP: skeleton present AND every required-at-0 *streamed* slot resident. Slots
    // that ride in the skeleton are resident at skeleton time, covered by the max.
    row.ready_ms = skeleton_ms.max(last_required_ms);
    Ok(row)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let args: Args = parse_args(
        "/private/tmp/claude-501/-Users-guysviry-git-trackerstream/ebff10c7-d4c9-4bcf-a745-a658852dfd59/scratchpad",
    );
    let tracks: Vec<Track> = serde_json::from_slice(&std::fs::read(&args.input)?)?;
    eprintln!(
        "[bench] {} tracks | jobs={} {}{} | from {}",
        tracks.len(), args.jobs, if args.ttfp_only { "ttfp-only" } else { "full" },
        if args.shared_node { " shared-node" } else { "" }, args.input
    );

    let ttfp_only = args.ttfp_only;
    let shared = if args.shared_node { Some(shared_master_node().await?) } else { None };
    let rows = run_sweep(&tracks, &args, move |t, seq| {
        Box::pin(bench_one(t, seq, ttfp_only, shared.clone()))
    })
    .await;
    finish(rows, &args.out_dir, "ttfp")?;
    Ok(())
}
