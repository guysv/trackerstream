//! A1 (concurrent gather) A/B: serial search (run_search_stream, one connection, the shipped
//! path) vs parallel search (run_search_parallel, FTS-only rowids then a concurrent per-hit
//! gather). Fresh COLD client per measurement (empty blockstore + cleared page/size caches) so
//! each is a true cold search. Waves stay ~equal (same work); the win is wall-clock (ms), as the
//! scattered gather's dependent page-fetches overlap. Also asserts identical result sets.
//! Runs on F3.db (the shipped detail=none config) unless CAND is set.
//!
//!   TS_NODE_BIN=/tmp/tsnode CAND_DIR=/path/to/cand [CAND=F3] \
//!     cargo test -p desktop --test catalog_a1_bench -- --nocapture

use cid::Cid;
use desktop_lib::catalog::{
    clear_page_cache, clear_size_cache, fetch_waves, fetched_bytes, reset_fetch_counters,
    reset_fetched_bytes, run_search_parallel, run_search_stream,
};
use desktop_lib::rpc::NodeRpc;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn bin() -> Option<PathBuf> { std::env::var("TS_NODE_BIN").ok().map(PathBuf::from).filter(|p| p.exists()) }
fn cand_dir() -> Option<PathBuf> { std::env::var("CAND_DIR").ok().map(PathBuf::from).filter(|p| p.is_dir()) }
fn free_port() -> u16 { std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port() }
fn tempdir() -> PathBuf {
    let d = std::env::temp_dir().join(format!("ts-a1-{}-{}", std::process::id(), free_port()));
    std::fs::create_dir_all(&d).unwrap(); d
}
async fn spawn_node(bin: &Path, role: &str, repo: &Path, port: u16, boot: &str) -> Child {
    let mut cmd = Command::new(bin);
    cmd.args(["--role", role]).arg("--repo").arg(repo).args(["--swarm-port", "0"])
        .arg("--rpc").arg(format!("127.0.0.1:{port}")).stdout(Stdio::null()).stderr(Stdio::null());
    if !boot.is_empty() { cmd.arg("--bootstrap").arg(boot); }
    let child = cmd.spawn().expect("spawn tsnode");
    let rpc = NodeRpc::new(&format!("127.0.0.1:{port}"));
    for _ in 0..50 { if rpc.id().await.is_ok() { return child; } tokio::time::sleep(Duration::from_millis(200)).await; }
    panic!("tsnode {role} RPC never came up");
}
async fn publish(port: u16, path: &Path) -> Cid {
    let data = std::fs::read(path).unwrap();
    let text = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/api/v0/add?chunker=size-16384&pin=true"))
        .body(data).send().await.unwrap().text().await.unwrap();
    #[derive(serde::Deserialize)] struct Add { #[serde(rename = "Hash")] hash: String }
    let a: Add = serde_json::from_str(text.lines().last().unwrap()).unwrap();
    a.hash.parse().unwrap()
}

/// Fresh cold client, run `f`, kill it. Guarantees an empty blockstore for a true cold search.
async fn cold<F, Fut, T>(bin: &Path, boot: &str, f: F) -> T
where F: FnOnce(NodeRpc) -> Fut, Fut: std::future::Future<Output = T> {
    let repo = tempdir(); let port = free_port();
    let mut cli = spawn_node(bin, "client", &repo, port, boot).await;
    let rpc = NodeRpc::new(&format!("127.0.0.1:{port}"));
    for addr in boot.split(',') { let _ = rpc.swarm_connect(addr).await; }
    tokio::time::sleep(Duration::from_secs(3)).await; // let the (possibly WAN) master link settle
    clear_page_cache(); clear_size_cache(); reset_fetched_bytes(); reset_fetch_counters();
    let out = f(rpc).await;
    cli.kill().ok(); let _ = cli.wait(); std::fs::remove_dir_all(&repo).ok();
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn a1_ab() {
    let (Some(bin), Some(dir)) = (bin(), cand_dir()) else {
        eprintln!("SKIP catalog_a1_bench: set TS_NODE_BIN and CAND_DIR"); return;
    };
    let cand = std::env::var("CAND").unwrap_or_else(|_| "F3".into());
    let path = dir.join(format!("{cand}.db"));
    if !path.exists() { eprintln!("SKIP: {} missing", path.display()); return; }

    // PROD mode: bootstrap to the live master and search the live NEW_CID (real WAN RTT — where
    // A1's gather-parallelism actually pays). Otherwise spin a local server + publish the candidate.
    let prod = std::env::var("BENCH_PROD").ok().as_deref() == Some("1");
    let (mut srv, boot, cid): (Option<Child>, String, Cid) = if prod {
        let boot = "/ip4/5.75.131.145/udp/5478/quic-v1/p2p/12D3KooWGb7eHYgZnMFfADEDeS5xDEwEVQKPTGozsKanpDf9XvzL,/ip4/5.75.131.145/tcp/5478/p2p/12D3KooWGb7eHYgZnMFfADEDeS5xDEwEVQKPTGozsKanpDf9XvzL".to_string();
        let cid = std::env::var("NEW_CID").expect("NEW_CID for prod mode").parse().unwrap();
        eprintln!("PROD mode: live master, cid {cid}");
        (None, boot, cid)
    } else {
        let srv_repo = tempdir(); let srv_port = free_port();
        let srv = spawn_node(&bin, "server", &srv_repo, srv_port, "").await;
        let srv_id = NodeRpc::new(&format!("127.0.0.1:{srv_port}")).id().await.unwrap();
        let boot = srv_id.addresses.iter().find(|a| a.contains("/127.0.0.1/"))
            .map(|a| format!("{a}/p2p/{}", srv_id.id)).unwrap();
        let cid = publish(srv_port, &path).await;
        (Some(srv), boot, cid)
    };

    let terms = ["strings", "jungle", "the", "piano"];
    eprintln!("\n=== A1 serial vs parallel gather ({cand}, cold client each) ===");
    eprintln!("{:<8} {:>6} | {:>7} {:>8} {:>8} | {:>7} {:>8} {:>8} | {:>7} {:>6}",
        "term", "hits", "ser_w", "ser_MB", "ser_ms", "par_w", "par_MB", "par_ms", "speedup", "parity");
    for term in &terms {
        // serial (shipped path)
        let sids = Arc::new(Mutex::new(Vec::<i64>::new()));
        let s2 = sids.clone();
        let (sw, sb, sms) = cold(&bin, &boot, |rpc| async move {
            let t0 = Instant::now();
            run_search_stream(rpc, cid, term.to_string(), 60, None, move |row| {
                if let Some(id) = row.get("id").and_then(|v| v.as_i64()) { s2.lock().unwrap().push(id); }
            }).await.ok();
            (fetch_waves(), fetched_bytes(), t0.elapsed().as_millis())
        }).await;
        // parallel (A1)
        let (pids, pw, pb, pms) = cold(&bin, &boot, |rpc| async move {
            let t0 = Instant::now();
            let res = run_search_parallel(rpc, cid, term.to_string(), 60, None).await.unwrap_or_default();
            let mut ids: Vec<i64> = res.get("results").and_then(|r| r.as_array()).map(|a|
                a.iter().filter_map(|x| x.get("id").and_then(|v| v.as_i64())).collect()).unwrap_or_default();
            ids.sort_unstable();
            (ids, fetch_waves(), fetched_bytes(), t0.elapsed().as_millis())
        }).await;
        let mut sids_v = sids.lock().unwrap().clone(); sids_v.sort_unstable();
        let parity = if sids_v == pids { "ok" } else { "FAIL" };
        let speedup = if pms > 0 { sms as f64 / pms as f64 } else { 0.0 };
        eprintln!("{:<8} {:>6} | {:>7} {:>8.2} {:>8} | {:>7} {:>8.2} {:>8} | {:>6.2}x {:>6}",
            term, pids.len(), sw, sb as f64/1e6, sms, pw, pb as f64/1e6, pms, speedup, parity);
    }
    if let Some(mut srv) = srv { srv.kill().ok(); let _ = srv.wait(); }
}
