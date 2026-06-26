//! Validate per-peer Bitswap byte accounting (the rust-ipfs patch): connect to the
//! master, stream a real v2 module (skeleton + all samples) so many blocks are
//! pulled, then assert `peer_bandwidth()` attributes the downloaded bytes to the
//! master peer. Down should be > 0 on the master; up stays 0 (we don't serve).
//! Defaults target prod; override MASTER_ADDR / ROOT_CID via env.
//!
//!   cargo run --example peer-bw-check
use desktop_lib::ipfs;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let master = std::env::var("MASTER_ADDR").unwrap_or_else(|_| {
        "/ip4/5.75.131.145/tcp/4001/p2p/12D3KooWGb7eHYgZnMFfADEDeS5xDEwEVQKPTGozsKanpDf9XvzL".into()
    });
    let root: cid::Cid = std::env::var("ROOT_CID")
        .unwrap_or_else(|_| "bafyreibc7al6brbv7kmpb6qel6nmjxano6i7q3wdsrcbbte5nzt4ycjqre".into())
        .parse()?;

    let node = ipfs::start(None).await?;
    println!("client peer: {}", node.peer_id);
    ipfs::connect(&node.ipfs, &master).await?;
    println!("connected: {master}");

    let st = Arc::new(ipfs::StreamState::default());
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ipfs::StreamEvent>();
    let drain = tokio::spawn(async move {
        let mut samples = 0u32;
        while let Some(ev) = rx.recv().await {
            match ev {
                ipfs::StreamEvent::Sample { .. } => samples += 1,
                ipfs::StreamEvent::Complete => break,
                ipfs::StreamEvent::Error { message } => {
                    eprintln!("stream error: {message}");
                    break;
                }
                _ => {}
            }
        }
        samples
    });
    ipfs::stream_v2(&node.ipfs, root, st, tx).await?;
    let samples = drain.await?;
    println!("streamed {samples} samples");

    let bw = ipfs::peer_bandwidth();
    println!("--- peer_bandwidth() ---");
    let (mut total_down, mut total_up) = (0u64, 0u64);
    for (peer, (down, up)) in &bw {
        println!("  {peer}  down={down}  up={up}");
        total_down += down;
        total_up += up;
    }

    let master_peer: rust_ipfs::PeerId =
        "12D3KooWGb7eHYgZnMFfADEDeS5xDEwEVQKPTGozsKanpDf9XvzL".parse()?;
    let (md, mu) = bw.get(&master_peer).copied().unwrap_or((0, 0));
    assert!(md > 0, "FAIL: master peer has no down bytes recorded");
    assert!(mu == 0, "unexpected: recorded up bytes to master ({mu})");
    println!(
        "OK: per-peer DOWN attributed to master = {md} bytes (up={mu}); \
         totals down={total_down} up={total_up} across {} peer(s)",
        bw.len()
    );
    Ok(())
}
