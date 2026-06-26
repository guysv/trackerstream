//! Validate per-peer UP *and* DOWN attribution end-to-end. Two in-process nodes:
//!   A: pulls a module from the master (A's cache fills).
//!   B: connects to A and pulls the SAME module FROM A (TS_PROVIDER=A).
//! The byte counters are process-global but keyed by PEER, so the directions
//! separate cleanly: B's receipts land under A's id (down), A's serves land under
//! B's id (up). Asserts all three legs: down<-master, down<-A, up->B.
use desktop_lib::ipfs;
use std::sync::Arc;

async fn stream_all(ipfs: &rust_ipfs::Ipfs, root: cid::Cid) -> anyhow::Result<()> {
    let st = Arc::new(ipfs::StreamState::default());
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ipfs::StreamEvent>();
    let drain = tokio::spawn(async move {
        while let Some(ev) = rx.recv().await {
            if matches!(ev, ipfs::StreamEvent::Complete | ipfs::StreamEvent::Error { .. }) {
                break;
            }
        }
    });
    ipfs::stream_v2(ipfs, root, st, tx).await?;
    drain.await?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let master = std::env::var("MASTER_ADDR").unwrap_or_else(|_| {
        "/ip4/5.75.131.145/tcp/4001/p2p/12D3KooWGb7eHYgZnMFfADEDeS5xDEwEVQKPTGozsKanpDf9XvzL".into()
    });
    let root: cid::Cid = std::env::var("ROOT_CID")
        .unwrap_or_else(|_| "bafyreibc7al6brbv7kmpb6qel6nmjxano6i7q3wdsrcbbte5nzt4ycjqre".into())
        .parse()?;
    let master_peer: rust_ipfs::PeerId =
        "12D3KooWGb7eHYgZnMFfADEDeS5xDEwEVQKPTGozsKanpDf9XvzL".parse()?;

    // A: fill cache from the master.
    let a = ipfs::start(None).await?;
    ipfs::connect(&a.ipfs, &master).await?;
    stream_all(&a.ipfs, root).await?;
    println!("A ({}) cached the module from master", a.peer_id);

    // B: pull the SAME module FROM A (not the master).
    let b = ipfs::start(None).await?;
    let a_addr = a
        .ipfs
        .listening_addresses()
        .await?
        .into_iter()
        .map(|m| m.to_string())
        .find(|m| m.contains("127.0.0.1"))
        .map(|m| format!("{m}/p2p/{}", a.peer_id))
        .expect("A loopback listen addr");
    ipfs::connect(&b.ipfs, &a_addr).await?;
    std::env::set_var("TS_PROVIDER", &a.peer_id); // B's Bitswap asks A
    stream_all(&b.ipfs, root).await?;
    println!("B ({}) pulled the module from A", b.peer_id);

    let a_peer: rust_ipfs::PeerId = a.peer_id.parse()?;
    let b_peer: rust_ipfs::PeerId = b.peer_id.parse()?;
    let bw = ipfs::peer_bandwidth();
    println!("--- peer_bandwidth() ---");
    for (p, (d, u)) in &bw {
        let tag = if *p == a_peer {
            "A"
        } else if *p == b_peer {
            "B"
        } else if *p == master_peer {
            "master"
        } else {
            "?"
        };
        println!("  [{tag:>6}] {p} down={d} up={u}");
    }

    let (md, _) = bw.get(&master_peer).copied().unwrap_or((0, 0));
    let (ad, _) = bw.get(&a_peer).copied().unwrap_or((0, 0)); // B downloaded from A
    let (_, bu) = bw.get(&b_peer).copied().unwrap_or((0, 0)); // A served to B
    assert!(md > 0, "FAIL: no DOWN recorded from master");
    assert!(ad > 0, "FAIL: B did not record DOWN from peer A");
    assert!(bu > 0, "FAIL: A did not record UP served to peer B");
    println!("OK: DOWN<-master={md}, DOWN<-peerA={ad}, UP->peerB={bu} — per-peer up+down reliable");
    Ok(())
}
