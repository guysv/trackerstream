//! Gather decomposition: for a term, how many of a search's serialized fetch-waves are the
//! FTS walk vs the scattered per-hit row gather (FTS rowid -> JOIN modules)? The gather is the
//! part a concurrent implementation (A1) would collapse from N serial waves to ~ceil(N/16).
//! Measures full-search vs FTS-only, cold, on baseline (B) + the detail=none winner (F3).
//!
//!   TS_NODE_BIN=/tmp/tsnode CAND_DIR=/path/to/cand \
//!     cargo test -p desktop --test catalog_gather_ab -- --nocapture

use cid::Cid;
use desktop_lib::catalog::{
    clear_page_cache, fetch_waves, fetched_bytes, reset_fetch_counters, reset_fetched_bytes,
    run_fts_only, run_search_stream,
};
use desktop_lib::rpc::NodeRpc;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn bin() -> Option<PathBuf> { std::env::var("TS_NODE_BIN").ok().map(PathBuf::from).filter(|p| p.exists()) }
fn cand_dir() -> Option<PathBuf> { std::env::var("CAND_DIR").ok().map(PathBuf::from).filter(|p| p.is_dir()) }
fn free_port() -> u16 { std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port() }
fn tempdir() -> PathBuf {
    let d = std::env::temp_dir().join(format!("ts-ab-{}-{}", std::process::id(), free_port()));
    std::fs::create_dir_all(&d).unwrap(); d
}
async fn spawn_node(bin: &Path, role: &str, repo: &Path, rpc_port: u16, boot: &str) -> Child {
    let mut cmd = Command::new(bin);
    cmd.args(["--role", role]).arg("--repo").arg(repo).args(["--swarm-port", "0"])
        .arg("--rpc").arg(format!("127.0.0.1:{rpc_port}")).stdout(Stdio::null()).stderr(Stdio::null());
    if !boot.is_empty() { cmd.arg("--bootstrap").arg(boot); }
    let child = cmd.spawn().expect("spawn tsnode");
    let rpc = NodeRpc::new(&format!("127.0.0.1:{rpc_port}"));
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

#[tokio::test(flavor = "multi_thread")]
async fn gather_ab() {
    let (Some(bin), Some(dir)) = (bin(), cand_dir()) else {
        eprintln!("SKIP catalog_gather_ab: set TS_NODE_BIN and CAND_DIR"); return;
    };
    let srv_repo = tempdir(); let srv_port = free_port();
    let mut srv = spawn_node(&bin, "server", &srv_repo, srv_port, "").await;
    let srv_rpc = NodeRpc::new(&format!("127.0.0.1:{srv_port}"));
    let srv_id = srv_rpc.id().await.unwrap();
    let boot = srv_id.addresses.iter().find(|a| a.contains("/127.0.0.1/"))
        .map(|a| format!("{a}/p2p/{}", srv_id.id)).unwrap();

    let terms = ["strings", "jungle", "the", "piano"];
    eprintln!("\n=== gather decomposition (waves): full search vs FTS-only, cold ===");
    eprintln!("{:<5} {:<9} {:>6} {:>9} {:>8} {:>9} {:>8}  {:>10}",
        "cand", "term", "hits", "full_w", "fts_w", "gather_w", "full_MB", "A1_proj_w");

    for name in ["B", "F3"] {
        let path = dir.join(format!("{name}.db"));
        if !path.exists() { continue; }
        let cid = publish(srv_port, &path).await;
        let cli_repo = tempdir(); let cli_port = free_port();
        let mut cli = spawn_node(&bin, "client", &cli_repo, cli_port, &boot).await;
        let rpc = NodeRpc::new(&format!("127.0.0.1:{cli_port}"));
        rpc.swarm_connect(&boot).await.ok();

        for term in &terms {
            // full search (JOIN gather)
            clear_page_cache(); reset_fetched_bytes(); reset_fetch_counters();
            let t0 = Instant::now();
            let hits = run_search_stream(rpc.clone(), cid, term.to_string(), 60, None, |_| {}).await.unwrap_or(0);
            let full_ms = t0.elapsed().as_millis();
            let (full_w, full_b) = (fetch_waves(), fetched_bytes());
            // FTS-only (no gather)
            clear_page_cache(); reset_fetch_counters(); reset_fetched_bytes();
            let _ = run_fts_only(rpc.clone(), cid, term.to_string(), 60).await.unwrap_or(0);
            let fts_w = fetch_waves();
            let gather_w = full_w.saturating_sub(fts_w);
            // A1 projection: gather collapses to ceil(gather_w / 16) parallel waves.
            let a1 = fts_w + gather_w.div_ceil(16);
            eprintln!("{:<5} {:<9} {:>6} {:>9} {:>8} {:>9} {:>8.2}  {:>10} ({}ms full)",
                name, term, hits, full_w, fts_w, gather_w, full_b as f64 / 1e6, a1, full_ms);
        }
        cli.kill().ok(); let _ = cli.wait(); std::fs::remove_dir_all(&cli_repo).ok();
    }
    srv.kill().ok(); let _ = srv.wait(); std::fs::remove_dir_all(&srv_repo).ok();
}
