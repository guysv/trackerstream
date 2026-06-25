//! Headless transport soak for the v2 streaming path: start the embedded
//! rust-ipfs node, connect to a peer holding a v2 root's blocks (a local kubo, or
//! the master), run `stream_v2`, and assert the skeleton + every streamed sample
//! arrive over real Bitswap. This exercises the actual desktop fetch path
//! (`fetch_bytes`/`fetch_many` + the v2 manifest parse + prefetch) without a
//! WebView — the part `cargo check` can't prove.
//!
//!   TS_PROVIDER=<peer-id> cargo run --example soak -- <root-cid> <peer-multiaddr>
//!
//! e.g. against a local online kubo:
//!   TS_PROVIDER=12D3Koo... cargo run --example soak -- \
//!     bafyrei... /ip4/127.0.0.1/tcp/4555/p2p/12D3Koo...
use std::sync::Arc;
use std::time::Instant;

use cid::Cid;
use desktop_lib::ipfs::{self, StreamEvent, StreamState};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let root: Cid = args.next().expect("usage: soak <root-cid> <peer-multiaddr>").parse()?;
    let peer = args.next().expect("usage: soak <root-cid> <peer-multiaddr>");

    let node = ipfs::start(None).await?;
    eprintln!("[soak] embedded node {}", node.peer_id);
    ipfs::connect(&node.ipfs, &peer).await?;

    let state = Arc::new(StreamState::default());
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<StreamEvent>();
    let ipfs = node.ipfs.clone();
    let st = state.clone();
    let t0 = Instant::now();
    let handle = tokio::spawn(async move { ipfs::stream_v2(&ipfs, root, st, tx).await });

    let mut skeleton_len = 0usize;
    let mut declared = 0u32;
    let mut got = 0u32;
    let mut failed = false;
    while let Some(ev) = rx.recv().await {
        match ev {
            StreamEvent::Skeleton { samples, .. } => {
                skeleton_len = state.skeleton.lock().unwrap().len();
                declared = samples;
                eprintln!("[soak] skeleton {skeleton_len} bytes, {declared} samples declared ({:?})", t0.elapsed());
            }
            StreamEvent::Sample { index, frames } => {
                got += 1;
                let bytes = state.samples.lock().unwrap().get(&index).map(|v| v.len()).unwrap_or(0);
                if bytes == 0 {
                    eprintln!("[soak] FAIL sample {index} reported but not resident");
                    failed = true;
                }
                eprintln!("[soak]   sample {index} ({frames} frames, {bytes} bytes) [{got}/{declared}]");
            }
            StreamEvent::Complete => {
                eprintln!("[soak] complete in {:?}", t0.elapsed());
                break;
            }
            StreamEvent::Error { message } => {
                eprintln!("[soak] FAIL stream error: {message}");
                failed = true;
                break;
            }
        }
    }
    handle.await??;

    let ok = !failed && skeleton_len > 0 && got == declared;
    println!(
        "{}  skeleton={skeleton_len}B samples={got}/{declared}",
        if ok { "SOAK OK" } else { "SOAK FAIL" }
    );
    std::process::exit(if ok { 0 } else { 1 });
}
