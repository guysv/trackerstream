//! TSZCAT (per-page-zstd) end-to-end A/B. Spawns a server tsnode, runs the Node bake script to
//! publish BOTH the raw SQLite catalog and a zstd manifest of it, then drives a realistic session
//! from a fresh client against each and reports bytes/waves. The zstd path exercises the new VFS
//! manifest branch (block/get + decompress per page). A parity gate asserts identical search
//! results, so compression can't silently change what the user sees.
//!
//!   TS_NODE_BIN=/tmp/tsnode CAND_DIR=/path/to/cand BAKE=/path/to/bake_zstd.mjs \
//!     cargo test -p desktop --test catalog_zstd_bench -- --nocapture

use cid::Cid;
use desktop_lib::catalog::{clear_page_cache, clear_size_cache, fetch_waves, fetched_bytes,
    reset_fetch_counters, reset_fetched_bytes, run_query, run_search_stream, CatalogReq};
use desktop_lib::rpc::NodeRpc;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn env_path(k: &str) -> Option<PathBuf> { std::env::var(k).ok().map(PathBuf::from).filter(|p| p.exists()) }
fn free_port() -> u16 { std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port() }
fn tempdir() -> PathBuf {
    let d = std::env::temp_dir().join(format!("ts-z-{}-{}", std::process::id(), free_port()));
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
    panic!("no rpc {role}");
}

async fn session(bin: &Path, boot: &str, cid: Cid) -> (u64, u64, u128, Vec<i64>) {
    // Fresh cold client; run browse + 3 searches + detail; return total bytes, waves, ms, and the
    // "strings" result ids (parity key). Page/size/manifest caches cleared for a true cold client.
    let repo = tempdir(); let port = free_port();
    let mut cli = spawn(bin, "client", &repo, port, boot).await;
    let rpc = NodeRpc::new(&format!("127.0.0.1:{port}"));
    rpc.swarm_connect(boot).await.ok();
    let (mut bytes, mut waves, mut ms) = (0u64, 0u64, 0u128);
    let mut sig = Vec::new();
    // Manifest + size are SESSION-level (fetched once per catalog, then cached) — clear them once
    // at session start, not per op, so the 0.61 MB manifest is charged once like real life. Only
    // the page cache is cleared per op (cold-page measurement, same convention for raw and zstd).
    clear_size_cache();
    // browse
    for (i, term) in ["", "jungle", "strings", "the"].iter().enumerate() {
        clear_page_cache(); reset_fetched_bytes(); reset_fetch_counters();
        let t0 = Instant::now();
        if i == 0 {
            let _ = run_query(rpc.clone(), cid, CatalogReq::List { format: None, sort: None, limit: Some(100), offset: Some(0) }).await;
        } else {
            let ids = Arc::new(Mutex::new(Vec::new())); let i2 = ids.clone();
            run_search_stream(rpc.clone(), cid, term.to_string(), 60, None, move |row| {
                if let Some(id) = row.get("id").and_then(|v| v.as_i64()) { i2.lock().unwrap().push(id); }
            }).await.ok();
            if *term == "strings" { sig = ids.lock().unwrap().clone(); sig.sort_unstable(); }
        }
        bytes += fetched_bytes(); waves += fetch_waves(); ms += t0.elapsed().as_millis();
    }
    // detail
    clear_page_cache(); reset_fetched_bytes(); reset_fetch_counters();
    let t0 = Instant::now();
    let _ = run_query(rpc.clone(), cid, CatalogReq::Get { id: 85024 }).await;
    bytes += fetched_bytes(); waves += fetch_waves(); ms += t0.elapsed().as_millis();
    cli.kill().ok(); let _ = cli.wait(); std::fs::remove_dir_all(&repo).ok();
    (bytes, waves, ms, sig)
}

#[tokio::test(flavor="multi_thread")]
async fn zstd_ab() {
    let (Some(bin), Some(dir), Some(bake)) = (env_path("TS_NODE_BIN"), env_path("CAND_DIR"), env_path("BAKE")) else {
        eprintln!("SKIP catalog_zstd_bench: set TS_NODE_BIN + CAND_DIR + BAKE"); return;
    };
    let db = dir.join("F3.db");
    if !db.exists() { eprintln!("SKIP: {} missing", db.display()); return; }

    let srepo = tempdir(); let sport = free_port();
    let mut srv = spawn(&bin, "server", &srepo, sport, "").await;
    let sid = NodeRpc::new(&format!("127.0.0.1:{sport}")).id().await.unwrap();
    let boot = sid.addresses.iter().find(|a| a.contains("/127.0.0.1/")).map(|a| format!("{a}/p2p/{}", sid.id)).unwrap();

    // Bake: publish raw + zstd manifest to the server. (node strips the .ts import types.)
    let out = Command::new("node").arg("--experimental-strip-types").arg(&bake)
        .arg(format!("http://127.0.0.1:{sport}")).arg(&db).arg(tempdir().join("manifest.bin"))
        .output().expect("run bake");
    let stdout = String::from_utf8_lossy(&out.stdout);
    eprintln!("bake stderr: {}", String::from_utf8_lossy(&out.stderr).trim());
    if !out.status.success() { panic!("bake failed: {stdout}"); }
    let get = |k: &str| stdout.lines().find_map(|l| l.strip_prefix(k)).map(|s| s.trim().parse::<Cid>().unwrap());
    let raw_cid = get("RAW_CID=").expect("RAW_CID");
    let zstd_cid = get("ZSTD_CID=").expect("ZSTD_CID");
    eprintln!("raw  {raw_cid}\nzstd {zstd_cid}");

    let (rb, rw, rms, rsig) = session(&bin, &boot, raw_cid).await;
    let (zb, zw, zms, zsig) = session(&bin, &boot, zstd_cid).await;

    eprintln!("\n=== TSZCAT session A/B (cold client, browse+3 searches+detail) ===");
    eprintln!("{:<6} {:>10} {:>8} {:>8}", "fmt", "bytes(MB)", "waves", "ms");
    eprintln!("{:<6} {:>10.2} {:>8} {:>8}", "raw", rb as f64/1e6, rw, rms);
    eprintln!("{:<6} {:>10.2} {:>8} {:>8}", "zstd", zb as f64/1e6, zw, zms);
    eprintln!("traffic reduction: {:.2}x   parity: {}", rb as f64 / zb.max(1) as f64,
        if rsig == zsig && !rsig.is_empty() { "ok" } else { "FAIL" });

    srv.kill().ok(); let _ = srv.wait(); std::fs::remove_dir_all(&srepo).ok();
    assert_eq!(rsig, zsig, "zstd search results must match raw");
}
