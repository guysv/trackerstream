//! FTS-walk sharing probe (#2). On ONE client, run FTS-only queries for several terms; clear the
//! page cache ONLY before the first. If the later (warm) terms cost far fewer waves than the cold
//! first, most of the FTS-walk is SHARED dictionary structure — which a shipped/prewarmed
//! dictionary preamble could collapse. If they stay high, the cost is per-term posting reads and
//! a preamble won't help. Runs on F3.db (shipped detail=none) unless CAND is set.
//!
//!   TS_NODE_BIN=/tmp/tsnode CAND_DIR=/path/to/cand \
//!     cargo test -p desktop --test catalog_ftswalk -- --nocapture

use cid::Cid;
use desktop_lib::catalog::{clear_page_cache, clear_size_cache, fetch_waves, fetched_bytes,
    reset_fetch_counters, reset_fetched_bytes, run_fts_only};
use desktop_lib::rpc::NodeRpc;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

fn bin() -> Option<PathBuf> { std::env::var("TS_NODE_BIN").ok().map(PathBuf::from).filter(|p| p.exists()) }
fn cand_dir() -> Option<PathBuf> { std::env::var("CAND_DIR").ok().map(PathBuf::from).filter(|p| p.is_dir()) }
fn free_port() -> u16 { std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port() }
fn tempdir() -> PathBuf {
    let d = std::env::temp_dir().join(format!("ts-fw-{}-{}", std::process::id(), free_port()));
    std::fs::create_dir_all(&d).unwrap(); d
}
async fn spawn(bin: &Path, role: &str, repo: &Path, port: u16, boot: &str) -> Child {
    let mut cmd = Command::new(bin);
    cmd.args(["--role", role]).arg("--repo").arg(repo).args(["--swarm-port","0"])
        .arg("--rpc").arg(format!("127.0.0.1:{port}")).stdout(Stdio::null()).stderr(Stdio::null());
    if !boot.is_empty() { cmd.arg("--bootstrap").arg(boot); }
    let c = cmd.spawn().unwrap();
    let rpc = NodeRpc::new(&format!("127.0.0.1:{port}"));
    for _ in 0..50 { if rpc.id().await.is_ok() { return c; } tokio::time::sleep(Duration::from_millis(200)).await; }
    panic!("no rpc");
}
async fn publish(port: u16, path: &Path) -> Cid {
    let data = std::fs::read(path).unwrap();
    let t = reqwest::Client::new().post(format!("http://127.0.0.1:{port}/api/v0/add?chunker=size-16384&pin=true"))
        .body(data).send().await.unwrap().text().await.unwrap();
    #[derive(serde::Deserialize)] struct A { #[serde(rename="Hash")] hash: String }
    serde_json::from_str::<A>(t.lines().last().unwrap()).unwrap().hash.parse().unwrap()
}

#[tokio::test(flavor="multi_thread")]
async fn ftswalk() {
    let (Some(bin), Some(dir)) = (bin(), cand_dir()) else { eprintln!("SKIP: TS_NODE_BIN + CAND_DIR"); return; };
    let path = dir.join(format!("{}.db", std::env::var("CAND").unwrap_or_else(|_|"F3".into())));
    if !path.exists() { eprintln!("SKIP: {} missing", path.display()); return; }
    let srepo = tempdir(); let sport = free_port();
    let mut srv = spawn(&bin,"server",&srepo,sport,"").await;
    let sid = NodeRpc::new(&format!("127.0.0.1:{sport}")).id().await.unwrap();
    let boot = sid.addresses.iter().find(|a| a.contains("/127.0.0.1/")).map(|a| format!("{a}/p2p/{}", sid.id)).unwrap();
    let cid = publish(sport, &path).await;

    let crepo = tempdir(); let cport = free_port();
    let mut cli = spawn(&bin,"client",&crepo,cport,&boot).await;
    let rpc = NodeRpc::new(&format!("127.0.0.1:{cport}"));
    rpc.swarm_connect(&boot).await.ok();

    // First term COLD (clear caches); the rest WARM (shared dictionary already cached).
    let terms = ["strings","piano","guitar","techno","acid","chip"];
    eprintln!("\n=== FTS-walk sharing (FTS-only, first cold, rest warm) ===");
    eprintln!("{:<8} {:>6} {:>8} {:>8}", "term", "waves", "MB", "state");
    for (i, t) in terms.iter().enumerate() {
        if i == 0 { clear_page_cache(); clear_size_cache(); }
        reset_fetch_counters(); reset_fetched_bytes();
        let _ = run_fts_only(rpc.clone(), cid, t.to_string(), 60).await;
        eprintln!("{:<8} {:>6} {:>8.2} {:>8}", t, fetch_waves(), fetched_bytes() as f64/1e6,
            if i==0 {"cold"} else {"warm"});
    }
    cli.kill().ok(); let _=cli.wait(); srv.kill().ok(); let _=srv.wait();
    std::fs::remove_dir_all(&crepo).ok(); std::fs::remove_dir_all(&srepo).ok();
}
