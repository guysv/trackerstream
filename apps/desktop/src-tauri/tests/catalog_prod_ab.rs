//! Prod before/after: measure a fresh Bitswap client against the LIVE master for two catalog
//! CIDs (OLD_CID = pre-change, NEW_CID = the detail=none rebake — both pinned on the master),
//! cold, per term: full-search waves/bytes/ms + the FTS-only wave decomposition. This is the
//! real-WAN "where did we get" for the rebake. Gated on BENCH_PROD=1 (hits the live network).
//!
//!   TS_NODE_BIN=/tmp/tsnode BENCH_PROD=1 OLD_CID=bafy... NEW_CID=bafy... \
//!     cargo test -p desktop --test catalog_prod_ab -- --nocapture

use cid::Cid;
use desktop_lib::catalog::{
    clear_page_cache, fetch_waves, fetched_bytes, reset_fetch_counters, reset_fetched_bytes,
    run_fts_only, run_search_stream,
};
use desktop_lib::rpc::NodeRpc;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const MASTER_PEER_ID: &str = "12D3KooWGb7eHYgZnMFfADEDeS5xDEwEVQKPTGozsKanpDf9XvzL";
const MASTER_IPV4: &str = "5.75.131.145";
const SWARM_PORT: u16 = 5478;

fn bin() -> Option<PathBuf> { std::env::var("TS_NODE_BIN").ok().map(PathBuf::from).filter(|p| p.exists()) }
fn free_port() -> u16 { std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port() }
fn tempdir() -> PathBuf {
    let d = std::env::temp_dir().join(format!("ts-prodab-{}-{}", std::process::id(), free_port()));
    std::fs::create_dir_all(&d).unwrap(); d
}
fn master_boot() -> String {
    format!("/ip4/{ip}/udp/{p}/quic-v1/p2p/{id},/ip4/{ip}/tcp/{p}/p2p/{id}",
        ip = MASTER_IPV4, p = SWARM_PORT, id = MASTER_PEER_ID)
}
async fn spawn_client(bin: &Path, repo: &Path, port: u16, boot: &str) -> Child {
    let mut cmd = Command::new(bin);
    cmd.args(["--role", "client"]).arg("--repo").arg(repo).args(["--swarm-port", "0"])
        .arg("--rpc").arg(format!("127.0.0.1:{port}")).arg("--bootstrap").arg(boot)
        .stdout(Stdio::null()).stderr(Stdio::null());
    let child = cmd.spawn().expect("spawn tsnode");
    let rpc = NodeRpc::new(&format!("127.0.0.1:{port}"));
    for _ in 0..50 { if rpc.id().await.is_ok() { return child; } tokio::time::sleep(Duration::from_millis(200)).await; }
    panic!("client RPC never came up");
}

#[tokio::test(flavor = "multi_thread")]
async fn prod_ab() {
    let Some(bin) = bin() else { eprintln!("SKIP: set TS_NODE_BIN"); return; };
    if std::env::var("BENCH_PROD").ok().as_deref() != Some("1") {
        eprintln!("SKIP prod_ab: set BENCH_PROD=1 (hits the LIVE master)"); return;
    }
    let old_cid = std::env::var("OLD_CID").ok();
    let new_cid = std::env::var("NEW_CID").ok();
    let cids: Vec<(String, Cid)> = [("OLD", old_cid), ("NEW", new_cid)].into_iter()
        .filter_map(|(l, c)| c.and_then(|c| c.parse().ok().map(|cid| (l.to_string(), cid)))).collect();
    if cids.is_empty() { eprintln!("SKIP prod_ab: set OLD_CID and/or NEW_CID"); return; }

    let boot = master_boot();
    let terms = ["strings", "jungle", "the", "piano"];

    // One fresh client, connected to the master. (Fresh blockstore; page cache cleared per op so
    // waves/bytes are cold-plan. A cold WAN client re-fetches each CID's blocks from the master.)
    let repo = tempdir(); let port = free_port();
    let mut cli = spawn_client(&bin, &repo, port, &boot).await;
    let rpc = NodeRpc::new(&format!("127.0.0.1:{port}"));
    for addr in boot.split(',') { let _ = rpc.swarm_connect(addr).await; }
    tokio::time::sleep(Duration::from_secs(3)).await; // let the master link settle

    eprintln!("\n=== prod A/B (live master, cold client) ===");
    eprintln!("{:<4} {:<8} {:>6} {:>7} {:>7} {:>8} {:>8}  {:>8}",
        "cid", "term", "hits", "full_w", "fts_w", "gathr_w", "full_MB", "full_ms");
    for (label, cid) in &cids {
        for term in &terms {
            clear_page_cache(); reset_fetched_bytes(); reset_fetch_counters();
            let t0 = Instant::now();
            let hits = run_search_stream(rpc.clone(), *cid, term.to_string(), 60, None, |_| {}).await.unwrap_or(0);
            let ms = t0.elapsed().as_millis();
            let (fw, fb) = (fetch_waves(), fetched_bytes());
            clear_page_cache(); reset_fetch_counters(); reset_fetched_bytes();
            let _ = run_fts_only(rpc.clone(), *cid, term.to_string(), 60).await;
            let ftsw = fetch_waves();
            eprintln!("{:<4} {:<8} {:>6} {:>7} {:>7} {:>8} {:>8.2} {:>8}",
                label, term, hits, fw, ftsw, fw.saturating_sub(ftsw), fb as f64 / 1e6, ms);
        }
    }
    cli.kill().ok(); let _ = cli.wait(); std::fs::remove_dir_all(&repo).ok();
}
