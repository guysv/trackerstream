//! Headless check of progressive (streaming) reassembly: connect to a master,
//! stream a module, and confirm it becomes PLAYABLE on the first (skeleton)
//! blocks well before the full DAG arrives, and that the final streamed buffer
//! is byte-identical to a full reassembly.
//!
//!   MASTER_ADDR=… ROOT_CID=… cargo run --example stream-check
use desktop_lib::ipfs;
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let master = std::env::var("MASTER_ADDR").expect("MASTER_ADDR");
    let root: cid::Cid = std::env::var("ROOT_CID").expect("ROOT_CID").parse()?;

    let node = ipfs::start(None).await?;
    ipfs::connect(&node.ipfs, &master).await?;

    let buf = Arc::new(Mutex::new(ipfs::StreamBuffer::default()));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ipfs::Progress>();

    let t0 = Instant::now();
    let logger = tokio::spawn(async move {
        let mut first_playable_ms: Option<u128> = None;
        let mut ticks = 0u32;
        while let Some(p) = rx.recv().await {
            ticks += 1;
            if p.playable && first_playable_ms.is_none() {
                first_playable_ms = Some(t0.elapsed().as_millis());
            }
        }
        (first_playable_ms, ticks)
    });

    ipfs::stream_reassemble(&node.ipfs, root, buf.clone(), tx).await?;
    let total_ms = t0.elapsed().as_millis();
    let (first_playable_ms, ticks) = logger.await?;

    let streamed = buf.lock().unwrap().data.clone();
    let complete = buf.lock().unwrap().complete;
    let full = ipfs::reassemble(&node.ipfs, root).await?; // cached -> instant
    let exact = streamed == full;

    let ok = complete && exact && first_playable_ms.is_some();
    println!(
        "{} streamed {} bytes: playable@{}ms, complete@{}ms, {} progress ticks, final {}",
        if ok { "OK  " } else { "FAIL" },
        streamed.len(),
        first_playable_ms.map(|m| m.to_string()).unwrap_or("never".into()),
        total_ms,
        ticks,
        if exact { "BYTE-EXACT vs full reassembly" } else { "DIFFERS" },
    );
    std::process::exit(if ok { 0 } else { 1 });
}
