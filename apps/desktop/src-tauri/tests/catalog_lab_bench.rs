//! Catalog packing LAB. Measures, per candidate DB and per session-op, what a fresh Bitswap
//! client pulls: bytes, blocks, **fetch-waves** (serialized round-trips ≈ cold latency), cat
//! calls, and wall-clock. Publishes every candidate under `CAND_DIR` to one local server tsnode
//! (chunker size-16384, so a >16 KB SQLite page spans N leaves = the client CHUNK), then drives
//! a realistic session from a fresh client per candidate:
//!   browse100 · search-title · search-instr · search-broad · detail · page2 · TYPE-AHEAD
//! The type-ahead op emulates a user typing a word key-by-key with the real debounce+cancel
//! (each keystroke cancels the previous in-flight query), measuring the cumulative cost of
//! typing a word cold. A per-term result-parity gate vs the baseline candidate (`B`) enforces
//! "don't ruin the search" — instrument/comment coverage must be identical.
//!
//! Gated on TS_NODE_BIN + CAND_DIR (skips/passes if absent, so `cargo test` stays green):
//!   TS_NODE_BIN=/tmp/tsnode  CAND_DIR=/path/to/cand  \
//!     cargo test -p desktop --test catalog_lab_bench -- --nocapture

use cid::Cid;
use desktop_lib::catalog::{
    cancel_inflight, cat_calls, clear_page_cache, fetch_waves, fetched_bytes, reset_fetch_counters,
    reset_fetched_bytes, run_query, run_search_stream, CatalogReq,
};
use desktop_lib::rpc::NodeRpc;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// ------------------------------------------------------------------ helpers (node spawn)
fn bin() -> Option<PathBuf> {
    std::env::var("TS_NODE_BIN").ok().map(PathBuf::from).filter(|p| p.exists())
}
fn cand_dir() -> Option<PathBuf> {
    std::env::var("CAND_DIR").ok().map(PathBuf::from).filter(|p| p.is_dir())
}
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}
fn tempdir() -> PathBuf {
    let d = std::env::temp_dir().join(format!("ts-lab-{}-{}", std::process::id(), free_port()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

async fn spawn_node(bin: &Path, role: &str, repo: &Path, rpc_port: u16, bootstrap: &str) -> Child {
    let mut cmd = Command::new(bin);
    cmd.args(["--role", role])
        .arg("--repo").arg(repo)
        .args(["--swarm-port", "0"])
        .arg("--rpc").arg(format!("127.0.0.1:{rpc_port}"))
        .stdout(Stdio::null()).stderr(Stdio::null());
    if !bootstrap.is_empty() {
        cmd.arg("--bootstrap").arg(bootstrap);
    }
    let child = cmd.spawn().expect("spawn tsnode");
    let rpc = NodeRpc::new(&format!("127.0.0.1:{rpc_port}"));
    for _ in 0..50 {
        if rpc.id().await.is_ok() {
            return child;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("tsnode {role} RPC never came up");
}

async fn publish(port: u16, path: &Path) -> Cid {
    // Same layout ingest uses: size-16384 chunker, dag-pb leaves, pinned. Page size varies per
    // candidate but the leaf/chunk stays 16 KB (= the client's CHUNK) so we isolate page_size.
    let data = std::fs::read(path).expect("read candidate");
    let text = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/api/v0/add?chunker=size-16384&pin=true"))
        .body(data).send().await.expect("add").text().await.unwrap();
    #[derive(serde::Deserialize)]
    struct Add { #[serde(rename = "Hash")] hash: String }
    let add: Add = serde_json::from_str(text.lines().last().unwrap()).expect("add json");
    add.hash.parse().expect("root cid")
}

// ------------------------------------------------------------------ metric plumbing
#[derive(Clone, Default)]
struct M { results: usize, bytes: u64, blocks: u64, waves: u64, cats: u64, ms: u128 }

fn snap_reset() {
    clear_page_cache();
    reset_fetched_bytes();
    reset_fetch_counters();
}
fn snap_read(results: usize, ms: u128) -> M {
    let bytes = fetched_bytes();
    M { results, bytes, blocks: (bytes + 16383) / 16384, waves: fetch_waves(), cats: cat_calls(), ms }
}

/// Streaming search returning the sorted matched rowids (for the parity gate) + its metric.
async fn search(rpc: &NodeRpc, cid: Cid, q: &str, limit: i64, after: Option<i64>) -> (Vec<i64>, M) {
    snap_reset();
    let ids = Arc::new(Mutex::new(Vec::<i64>::new()));
    let ids2 = ids.clone();
    let t0 = Instant::now();
    let n = run_search_stream(rpc.clone(), cid, q.to_string(), limit, after, move |row| {
        if let Some(id) = row.get("id").and_then(|v| v.as_i64()) {
            ids2.lock().unwrap().push(id);
        }
    }).await.unwrap_or(0);
    let m = snap_read(n, t0.elapsed().as_millis());
    let mut v = ids.lock().unwrap().clone();
    v.sort_unstable();
    (v, m)
}

/// Emulate typing `word` key-by-key (2-char gated, like the shipped client): each keystroke
/// cancels the previous in-flight query and fires a new one; intermediate keystrokes are
/// interrupted after a debounce, the final settles to completion. Shared page cache across the
/// burst (real behavior — later keystrokes reuse earlier pages). Returns the CUMULATIVE cost.
async fn typeahead(rpc: &NodeRpc, cid: Cid, word: &str, settle_limit: i64) -> M {
    snap_reset();
    let t0 = Instant::now();
    let prefixes: Vec<String> = (2..=word.len()).map(|n| word[..n].to_string()).collect();
    let last = prefixes.len().saturating_sub(1);
    for (i, p) in prefixes.iter().enumerate() {
        cancel_inflight(); // a new keystroke supersedes the previous query's in-flight fetch
        if i == last {
            let _ = run_search_stream(rpc.clone(), cid, p.clone(), settle_limit, None, |_| {}).await;
        } else {
            let (rpc2, p2) = (rpc.clone(), p.clone());
            tokio::spawn(async move {
                let _ = run_search_stream(rpc2, cid, p2, 30, None, |_| {}).await;
            });
            tokio::time::sleep(Duration::from_millis(90)).await; // inter-keystroke debounce
        }
    }
    tokio::time::sleep(Duration::from_millis(150)).await; // let any cancelled straggler settle
    snap_read(0, t0.elapsed().as_millis())
}

async fn browse_or_get(rpc: &NodeRpc, cid: Cid, req: CatalogReq) -> M {
    snap_reset();
    let t0 = Instant::now();
    let res = run_query(rpc.clone(), cid, req).await;
    let n = res.ok().and_then(|v| v.get("results").and_then(|r| r.as_array()).map(|a| a.len()))
        .unwrap_or(0);
    snap_read(n, t0.elapsed().as_millis())
}

const INSTR_TERM: &str = "strings"; // 6896 hits, 6869 in instruments/comment — the bio/info path
const DETAIL_ID: i64 = 85024;

#[tokio::test(flavor = "multi_thread")]
async fn catalog_lab() {
    let (Some(bin), Some(dir)) = (bin(), cand_dir()) else {
        eprintln!("SKIP catalog_lab: set TS_NODE_BIN and CAND_DIR");
        return;
    };
    // Candidate files: B first (baseline / parity reference), then the rest sorted.
    let mut cands: Vec<(String, PathBuf)> = std::fs::read_dir(&dir).unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "db").unwrap_or(false))
        .map(|p| (p.file_stem().unwrap().to_string_lossy().to_string(), p))
        .collect();
    cands.sort_by_key(|(n, _)| (n != "B", n.clone()));

    let srv_repo = tempdir();
    let srv_port = free_port();
    let mut srv = spawn_node(&bin, "server", &srv_repo, srv_port, "").await;
    let srv_rpc = NodeRpc::new(&format!("127.0.0.1:{srv_port}"));
    let srv_id = srv_rpc.id().await.unwrap();
    let boot = srv_id.addresses.iter().find(|a| a.contains("/127.0.0.1/"))
        .map(|a| format!("{a}/p2p/{}", srv_id.id)).expect("server loopback addr");

    // Publish every candidate up front; record its root CID + on-disk size.
    let mut roots: Vec<(String, Cid, u64)> = Vec::new();
    for (name, path) in &cands {
        let cid = publish(srv_port, path).await;
        let sz = std::fs::metadata(path).unwrap().len();
        eprintln!("published {name:<5} {:>6.1} MB  {cid}", sz as f64 / 1e6);
        roots.push((name.clone(), cid, sz));
    }

    // Parity reference: baseline's sorted rowids per search term.
    let mut baseline: BTreeMap<&str, Vec<i64>> = BTreeMap::new();
    let mut rows: Vec<(String, u64, BTreeMap<&'static str, M>)> = Vec::new();

    for (name, cid, sz) in &roots {
        // Fresh client per candidate: cold-ish blockstore. (Byte/wave counters are cold-plan
        // regardless — snap_reset clears the Rust page cache before every op.)
        let cli_repo = tempdir();
        let cli_port = free_port();
        let mut cli = spawn_node(&bin, "client", &cli_repo, cli_port, &boot).await;
        let cli_rpc = NodeRpc::new(&format!("127.0.0.1:{cli_port}"));
        cli_rpc.swarm_connect(&boot).await.ok();

        let mut m: BTreeMap<&str, M> = BTreeMap::new();

        m.insert("browse100", browse_or_get(&cli_rpc, *cid, CatalogReq::List {
            format: None, sort: None, limit: Some(100), offset: Some(0) }).await);

        let (title_ids, tm) = search(&cli_rpc, *cid, "jungle", 60, None).await;
        m.insert("search-title", tm);
        let (instr_ids, im) = search(&cli_rpc, *cid, INSTR_TERM, 60, None).await;
        m.insert("search-instr", im);
        let (broad_ids, bm) = search(&cli_rpc, *cid, "the", 60, None).await;
        m.insert("search-broad", bm);

        m.insert("detail", browse_or_get(&cli_rpc, *cid, CatalogReq::Get { id: DETAIL_ID }).await);

        let after = title_ids.last().copied();
        let (_p2, p2m) = search(&cli_rpc, *cid, "jungle", 60, after).await;
        m.insert("page2", p2m);

        m.insert("typeahead", typeahead(&cli_rpc, *cid, INSTR_TERM, 60).await);

        // Parity gate.
        if name == "B" {
            baseline.insert("search-title", title_ids);
            baseline.insert("search-instr", instr_ids);
            baseline.insert("search-broad", broad_ids);
        } else {
            for (k, got) in [("search-title", &title_ids), ("search-instr", &instr_ids), ("search-broad", &broad_ids)] {
                let base = baseline.get(k).unwrap();
                if base != got {
                    eprintln!("  !! PARITY FAIL {name} {k}: baseline {} rows vs {} rows", base.len(), got.len());
                }
            }
        }

        rows.push((name.clone(), *sz, m));
        cli.kill().ok();
        let _ = cli.wait();
        std::fs::remove_dir_all(&cli_repo).ok();
    }

    // ---- report ----
    let ops = ["browse100", "search-title", "search-instr", "search-broad", "detail", "page2", "typeahead"];
    for metric in ["bytes(MB)", "waves", "blocks", "ms"] {
        eprintln!("\n=== {metric} ===");
        eprint!("{:<6} {:>7}", "cand", "size");
        for op in &ops { eprint!(" {:>13}", op); }
        eprintln!();
        for (name, sz, m) in &rows {
            eprint!("{:<6} {:>6.0}", name, *sz as f64 / 1e6);
            for op in &ops {
                let v = m.get(*op).cloned().unwrap_or_default();
                match metric {
                    "bytes(MB)" => eprint!(" {:>13.2}", v.bytes as f64 / 1e6),
                    "waves" => eprint!(" {:>13}", v.waves),
                    "blocks" => eprint!(" {:>13}", v.blocks),
                    _ => eprint!(" {:>13}", v.ms),
                }
            }
            eprintln!();
        }
    }
    // Cold-session totals (sum of the cold ops = a realistic first-visit cost).
    eprintln!("\n=== cold session totals (all ops) ===");
    eprintln!("{:<6} {:>8} {:>8} {:>8} {:>8}", "cand", "MB", "waves", "blocks", "ms");
    for (name, _sz, m) in &rows {
        let (mut b, mut w, mut bl, mut ms) = (0u64, 0u64, 0u64, 0u128);
        for op in &ops { let v = m.get(*op).cloned().unwrap_or_default(); b += v.bytes; w += v.waves; bl += v.blocks; ms += v.ms; }
        eprintln!("{:<6} {:>8.2} {:>8} {:>8} {:>8}", name, b as f64 / 1e6, w, bl, ms);
    }

    srv.kill().ok();
    let _ = srv.wait();
    std::fs::remove_dir_all(&srv_repo).ok();
}
