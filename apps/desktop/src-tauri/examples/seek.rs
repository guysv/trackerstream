//! Cold-seek benchmark for the v2 streaming path: how long until a seek to F% into
//! a track (default 70%) becomes audible over Bitswap, from a cold cache.
//!
//! Mirrors the fence at a non-zero order. On a FRESH node we run `stream_v2`, wait
//! for the skeleton, then issue the seek — set the playhead to the target order so
//! the existing playhead-priority prefetch re-targets the 70% region — and measure
//! until `PlanV2::required_at(targetOrder)` (floor∪next checkpoint union) is resident.
//! targetOrder = `order_for_seconds(F * duration)`, the bake-time time<->order map.
//!
//!   cargo run --release --example seek -- <bench-set.json> [out-dir] [--jobs N] [--seek 0.70] [--ttfp-only]
//!
//! Reports two times: `seek_ms` (from the seek being issued — the skeleton is already
//! resident, so this is pure seek cost) and `ready_ms` (from stream start, incl.
//! skeleton). Seeking immediately on skeleton keeps the target region genuinely cold.
//!
//! NOTE on a real effect this surfaces: for a target between checkpoints, the
//! still-audible held-sample slots live in the floor checkpoint (order < target), and
//! `need_key` ranks those as "passed" -> LOW prefetch priority. So the fence can wait
//! on slots the prefetcher fetches last. That inflated wait is real seek latency, so
//! we measure the full required set (no early "first non-required" shortcut).
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use cid::Cid;
use desktop_lib::ipfs::{self, StreamEvent, StreamState};

mod common;
use common::{connect_with_retry, finish, parse_args, run_sweep, shared_master_node, Args, Row, Track, PER_TRACK_TIMEOUT};

/// Run one cold seek to `fraction` of the track and measure time-to-seek-playable.
/// When `shared` is Some, reuse that already-connected node (no per-track dial).
async fn seek_one(
    t: &Track,
    seq: usize,
    fraction: f64,
    ttfp_only: bool,
    shared: Option<rust_ipfs::Ipfs>,
) -> anyhow::Result<Row> {
    let root: Cid = t.root_cid.parse()?;
    let mut row = Row::new(t);

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

    let target_seconds = fraction * t.duration;

    // Required slots at the seek target still awaited (drains as they stream in).
    let mut required: std::collections::HashSet<u32> = std::collections::HashSet::new();
    let mut skeleton_ms = 0u128;
    let mut last_required_ms = 0u128; // latest arrival among streamed required slots
    let mut seek_start: Option<Instant> = None;
    let mut captured = false; // ready/seek time recorded when required first drains

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

                    let target_order = plan.order_for_seconds(target_seconds);
                    row.target_order = target_order;
                    row.target_seconds = target_seconds;
                    required = plan.required_at(target_order).into_iter().collect();
                    row.required = required.len();

                    // Issue the seek: reseed the prefetch to the target, start the timer.
                    state.playhead.store(target_order, Ordering::Relaxed);
                    seek_start = Some(Instant::now());

                    // Target region already fully resident in the skeleton (v1/flat, or
                    // every required slot rides in the skeleton) -> fence opens at once.
                    if required.is_empty() {
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
                        if required.is_empty() && !captured {
                            // Every streamed required slot resident -> fence open.
                            captured = true;
                            row.ready_ms = last_required_ms.max(skeleton_ms);
                            row.seek_ms = seek_start.map(|s| s.elapsed().as_millis()).unwrap_or(0);
                            if ttfp_only {
                                break; // skip the rest of the track
                            }
                        }
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

    // Not captured during the loop: either the target rode entirely in the skeleton
    // (no streamed required), or some required slots are skeleton-resident so the set
    // never fully drained. Either way the fence opened at the last streamed-required
    // arrival (or at skeleton time if none), all resident by Complete.
    if !captured {
        row.ready_ms = skeleton_ms.max(last_required_ms);
        row.seek_ms = row.ready_ms.saturating_sub(skeleton_ms);
    }
    Ok(row)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let args: Args = parse_args(
        "/private/tmp/claude-501/-Users-guysviry-git-trackerstream/ebff10c7-d4c9-4bcf-a745-a658852dfd59/scratchpad",
    );
    let tracks: Vec<Track> = serde_json::from_slice(&std::fs::read(&args.input)?)?;
    eprintln!(
        "[bench] {} tracks | jobs={} seek={:.0}% {}{} | from {}",
        tracks.len(), args.jobs, args.seek_fraction * 100.0,
        if args.ttfp_only { "ttfp-only" } else { "full" },
        if args.shared_node { " shared-node" } else { "" }, args.input
    );

    let (frac, ttfp_only) = (args.seek_fraction, args.ttfp_only);
    let shared = if args.shared_node { Some(shared_master_node().await?) } else { None };
    let rows = run_sweep(&tracks, &args, move |t, seq| {
        Box::pin(seek_one(t, seq, frac, ttfp_only, shared.clone()))
    })
    .await;
    finish(rows, &args.out_dir, "seek")?;
    Ok(())
}
