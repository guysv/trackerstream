//! Benchmark: how many bytes does a fresh client pull over the catalog Bitswap-VFS to
//! answer a keyword search? Publishes a REAL catalog snapshot to a local server tsnode,
//! then drives `catalog::run_query` from a freshly-bootstrapped client and reports, per
//! term, the bytes fetched (= 16 KB pages the query plan touched), result count, and
//! wall-clock. This is the harness behind the "'jungle' pulls ~23 MB" observation and the
//! regression guard that search stays on the flat rowid path (no bm25 scan; see `catalog.rs`).
//!
//! Bytes/pages are the headline metric: they come from `FETCHED_BYTES`, which counts what each
//! query's SQLite plan actually fetched over the VFS. The in-process page cache (`PAGE_CACHE`) is
//! shared across queries, so each loop calls `clear_page_cache()` first to measure true cold
//! per-term cost — otherwise terms after the first would show the (real, but different) warm cost
//! of reusing the schema + FTS top-of-tree. Wall-clock also warms across terms (the client caches
//! blocks), so read ms as a lower bound, not cold TTFB.
//!
//! Gated on two env vars; skips (passes) if either is absent, so plain `cargo test` stays green:
//!   TS_NODE_BIN=/path/to/tsnode        (go -C node build -o /tmp/tsnode ./cmd/tsnode)
//!   CATALOG_SNAPSHOT=/path/to/catalog.sqlite.snapshot
//! Get a snapshot from any catalog DB:  sqlite3 catalog.db "VACUUM INTO 'catalog.snapshot'"
//! (or scp the prod one). Optional: BENCH_TERMS="jungle,techno,acid" overrides the term list.
//!
//! Run:  TS_NODE_BIN=/tmp/tsnode CATALOG_SNAPSHOT=/tmp/catalog.snapshot \
//!         cargo test -p desktop --test catalog_search_bench -- --nocapture

use cid::Cid;
use desktop_lib::catalog::{
    cancel_inflight, clear_page_cache, fetched_bytes, reset_fetched_bytes, resolve_ipns_cid,
    run_query, CatalogReq,
};
use desktop_lib::rpc::NodeRpc;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Prod master + catalog IPNS identity (source of truth: packages/config/index.js). A fresh
/// `--role client` node joins the private swarm with just these bootstrap addrs. Override the
/// bootstrap with BENCH_BOOTSTRAP="/ip4/.../p2p/..." if the master address changes.
const MASTER_PEER_ID: &str = "12D3KooWGb7eHYgZnMFfADEDeS5xDEwEVQKPTGozsKanpDf9XvzL";
const MASTER_IPV4: &str = "5.75.131.145";
const SWARM_PORT: u16 = 5478;
const CATALOG_IPNS_KEY: &str = "12D3KooWDb53qFZvANj5kDCr3riMhT2HJG32i5xqFKhvBtzh7wPC";

fn bin() -> Option<PathBuf> {
    std::env::var("TS_NODE_BIN").ok().map(PathBuf::from).filter(|p| p.exists())
}
fn snapshot() -> Option<PathBuf> {
    std::env::var("CATALOG_SNAPSHOT").ok().map(PathBuf::from).filter(|p| p.exists())
}

/// Common keywords that stress the broad-match path, plus a couple of selective ones and a
/// two-term query. Override with BENCH_TERMS="a,b,c".
fn terms() -> Vec<String> {
    if let Ok(csv) = std::env::var("BENCH_TERMS") {
        return csv.split(',').map(|t| t.trim().to_string()).filter(|t| !t.is_empty()).collect();
    }
    ["jungle", "techno", "acid", "trance", "chip", "intro", "remix", "dream", "space", "star"]
        .iter().map(|s| s.to_string()).collect()
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

async fn spawn_node(bin: &Path, role: &str, repo: &Path, rpc_port: u16, bootstrap: &str) -> Child {
    let mut cmd = Command::new(bin);
    cmd.args(["--role", role])
        .arg("--repo").arg(repo)
        .args(["--swarm-port", "0"])
        .arg("--rpc").arg(format!("127.0.0.1:{rpc_port}"))
        .stdout(Stdio::null())
        .stderr(Stdio::null());
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

#[tokio::test(flavor = "multi_thread")]
async fn catalog_search_fetch_bench() {
    let (Some(bin), Some(snap)) = (bin(), snapshot()) else {
        eprintln!("SKIP catalog_search_bench: set TS_NODE_BIN and CATALOG_SNAPSHOT to run it");
        return;
    };
    let db_bytes = std::fs::metadata(&snap).unwrap().len();

    let srv_repo = tempdir();
    let cli_repo = tempdir();
    let srv_port = free_port();
    let cli_port = free_port();

    // Server holds the catalog; client fetches its pages over Bitswap (the fresh-client path).
    let mut srv = spawn_node(&bin, "server", &srv_repo, srv_port, "").await;
    let srv_rpc = NodeRpc::new(&format!("127.0.0.1:{srv_port}"));
    let srv_id = srv_rpc.id().await.unwrap();

    // Publish the snapshot with the SAME layout ingest uses: size-16384 chunker, dag-pb
    // leaves (raw-leaves defaults false), pinned. Raw body add (the node accepts it).
    let data = std::fs::read(&snap).expect("read snapshot");
    let add_text = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{srv_port}/api/v0/add?chunker=size-16384&pin=true"))
        .body(data)
        .send().await.expect("add").text().await.unwrap();
    #[derive(serde::Deserialize)]
    struct Add { #[serde(rename = "Hash")] hash: String }
    let add: Add = serde_json::from_str(add_text.lines().last().unwrap()).expect("add json");
    let root: Cid = add.hash.parse().expect("catalog root cid");

    // Fresh client, bootstrapped + connected to the server (empty blockstore).
    let boot = srv_id.addresses.iter().find(|a| a.contains("/127.0.0.1/"))
        .map(|a| format!("{a}/p2p/{}", srv_id.id)).expect("server loopback addr");
    let mut cli = spawn_node(&bin, "client", &cli_repo, cli_port, &boot).await;
    let cli_rpc = NodeRpc::new(&format!("127.0.0.1:{cli_port}"));
    cli_rpc.swarm_connect(&boot).await.expect("client connects to server");

    eprintln!("\n=== catalog search fetch benchmark ===");
    eprintln!("catalog: {} ({:.1} MB, root {})", snap.display(), db_bytes as f64 / 1e6, root);
    eprintln!("{:<16} {:>8} {:>10} {:>8} {:>9}  {:>6}", "term", "results", "pulled", "pages", "% of db", "ms");
    eprintln!("{}", "-".repeat(64));

    let mut total_pulled = 0u64;
    for term in terms() {
        clear_page_cache(); // cold per term: don't let the shared cache warm across terms
        reset_fetched_bytes();
        let t0 = Instant::now();
        let res = run_query(cli_rpc.clone(), root, CatalogReq::Search { q: term.clone(), limit: Some(60), after: None })
            .await.expect("run_query");
        let ms = t0.elapsed().as_millis();
        let pulled = fetched_bytes();
        total_pulled += pulled;
        let results = res.get("results").and_then(|r| r.as_array()).map(|a| a.len()).unwrap_or(0);
        let pages = (pulled + 16383) / 16384;
        eprintln!(
            "{:<16} {:>8} {:>9.2}M {:>8} {:>8.1}%  {:>6}",
            term, results, pulled as f64 / 1e6, pages, 100.0 * pulled as f64 / db_bytes as f64, ms,
        );
    }
    eprintln!("{}", "-".repeat(64));
    eprintln!("total pulled across {} terms: {:.2} MB ({:.1}x the {:.1} MB db)\n",
        terms().len(), total_pulled as f64 / 1e6, total_pulled as f64 / db_bytes as f64, db_bytes as f64 / 1e6);

    srv.kill().ok();
    cli.kill().ok();
    let _ = (srv.wait(), cli.wait());
    cleanup(&[srv_repo, cli_repo]);
}

// ---------------------------------------------------------------------------------
// Prod variant: fresh client bootstrapped to the REAL master. Measures bytes AND
// "time to results" (cold IPNS resolve + real Bitswap round-trips), not a local mock.
// Gated on BENCH_PROD=1 (hits the live network) + TS_NODE_BIN. Set BENCH_FRESH_EACH=1
// to re-spawn a cold client per term (true cold TTR for every keyword; slower).
// ---------------------------------------------------------------------------------

fn master_bootstrap() -> String {
    std::env::var("BENCH_BOOTSTRAP").unwrap_or_else(|_| {
        format!(
            "/ip4/{ip}/udp/{p}/quic-v1/p2p/{id},/ip4/{ip}/tcp/{p}/p2p/{id}",
            ip = MASTER_IPV4, p = SWARM_PORT, id = MASTER_PEER_ID,
        )
    })
}

/// Spawn a fresh client bootstrapped to the master, wait until the master peer is up,
/// and resolve the catalog CID (cold, with retry). Returns (client, rpc, cid, resolve_ms).
async fn fresh_prod_client(bin: &Path, boot: &str) -> (Child, NodeRpc, Cid, u128) {
    let port = free_port();
    let repo = tempdir();
    let child = spawn_node(bin, "client", &repo, port, boot).await;
    let rpc = NodeRpc::new(&format!("127.0.0.1:{port}"));
    for addr in boot.split(',') {
        let _ = rpc.swarm_connect(addr).await; // first that dials wins; others are fallbacks
    }
    // Wait for the master link, then resolve the catalog IPNS name (cold DHT/gossipsub).
    let t0 = Instant::now();
    let mut cid = None;
    for _ in 0..40 {
        if let Ok(c) = resolve_ipns_cid(&rpc, CATALOG_IPNS_KEY).await {
            cid = Some(c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(1500)).await;
    }
    let resolve_ms = t0.elapsed().as_millis();
    let cid = cid.unwrap_or_else(|| panic!("could not resolve catalog IPNS from prod in time"));
    // leave repo to be cleaned by the caller via kill; store path on the child? we just leak the
    // tempdir dir name into TMP — cleaned by the OS. (Kept simple; benchmark, not a fixture.)
    let _ = repo;
    (child, rpc, cid, resolve_ms)
}

fn run_one(rt_bytes: u64, term: &str, results: usize, ms: u128) {
    let pages = (rt_bytes + 16383) / 16384;
    eprintln!(
        "{:<16} {:>8} {:>9.2}M {:>8} {:>7}",
        term, results, rt_bytes as f64 / 1e6, pages, ms,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn catalog_search_prod_bench() {
    let Some(bin) = bin() else {
        eprintln!("SKIP catalog_search_prod_bench: set TS_NODE_BIN to run it");
        return;
    };
    if std::env::var("BENCH_PROD").ok().as_deref() != Some("1") {
        eprintln!("SKIP catalog_search_prod_bench: set BENCH_PROD=1 (hits the LIVE prod network)");
        return;
    }
    let boot = master_bootstrap();
    let fresh_each = std::env::var("BENCH_FRESH_EACH").ok().as_deref() == Some("1");
    let terms = terms();

    eprintln!("\n=== catalog search PROD benchmark (live master) ===");
    eprintln!("bootstrap: {boot}");
    eprintln!("mode: {}", if fresh_each { "fresh cold client per term" } else { "one client, first term cold / rest warm" });

    if fresh_each {
        eprintln!("{:<16} {:>8} {:>10} {:>8} {:>7} {:>10}", "term", "results", "pulled", "pages", "q_ms", "resolve_ms");
        eprintln!("{}", "-".repeat(66));
        for term in &terms {
            let (mut child, rpc, cid, resolve_ms) = fresh_prod_client(&bin, &boot).await;
            clear_page_cache(); // in-process cache is shared across terms even with a fresh client
            reset_fetched_bytes();
            let t0 = Instant::now();
            let res = run_query(rpc.clone(), cid, CatalogReq::Search { q: term.clone(), limit: Some(60), after: None })
                .await.expect("run_query");
            let ms = t0.elapsed().as_millis();
            let results = res.get("results").and_then(|r| r.as_array()).map(|a| a.len()).unwrap_or(0);
            let pulled = fetched_bytes();
            let pages = (pulled + 16383) / 16384;
            eprintln!("{:<16} {:>8} {:>9.2}M {:>8} {:>7} {:>10}",
                term, results, pulled as f64 / 1e6, pages, ms, resolve_ms);
            child.kill().ok();
            let _ = child.wait();
        }
    } else {
        let (mut child, rpc, cid, resolve_ms) = fresh_prod_client(&bin, &boot).await;
        eprintln!("catalog cid: {cid}");
        eprintln!("cold IPNS resolve: {resolve_ms} ms");
        eprintln!("{:<16} {:>8} {:>10} {:>8} {:>7}", "term", "results", "pulled", "pages", "ms");
        eprintln!("{}", "-".repeat(56));
        for term in &terms {
            clear_page_cache(); // cold per term (one client, but the page cache would carry over)
            reset_fetched_bytes();
            let t0 = Instant::now();
            let res = run_query(rpc.clone(), cid, CatalogReq::Search { q: term.clone(), limit: Some(60), after: None })
                .await.expect("run_query");
            let ms = t0.elapsed().as_millis();
            let results = res.get("results").and_then(|r| r.as_array()).map(|a| a.len()).unwrap_or(0);
            run_one(fetched_bytes(), term, results, ms);
        }
        child.kill().ok();
        let _ = child.wait();
    }
    eprintln!();
}

/// Proves cancellation reaches the fetch loop: start a broad query, cancel it mid-flight, and
/// assert it (a) errors out and (b) stops pulling pages (tsnode aborts the in-flight Bitswap).
#[tokio::test(flavor = "multi_thread")]
async fn catalog_cancel_prod() {
    let Some(bin) = bin() else {
        eprintln!("SKIP catalog_cancel_prod: set TS_NODE_BIN");
        return;
    };
    if std::env::var("BENCH_PROD").ok().as_deref() != Some("1") {
        eprintln!("SKIP catalog_cancel_prod: set BENCH_PROD=1 (hits the LIVE prod network)");
        return;
    }
    let boot = master_bootstrap();
    let (mut child, rpc, cid, _) = fresh_prod_client(&bin, &boot).await;
    clear_page_cache(); // start cold: a warm cache could serve the query before we can cancel it
    reset_fetched_bytes();

    // Broad term, big limit → a long fetch we can interrupt part-way.
    let h = tokio::spawn({
        let rpc = rpc.clone();
        async move { run_query(rpc, cid, CatalogReq::Search { q: "the".into(), limit: Some(300), after: None }).await }
    });
    tokio::time::sleep(Duration::from_millis(700)).await;
    let at_cancel = fetched_bytes();
    cancel_inflight();
    let res = h.await.expect("join");
    tokio::time::sleep(Duration::from_millis(1000)).await; // let any stragglers land
    let after = fetched_bytes();

    eprintln!("\n=== cancel test ===");
    eprintln!("query result: {}", if res.is_err() { "Err (aborted)" } else { "Ok (completed before cancel)" });
    eprintln!("bytes at cancel: {:.2} MB", at_cancel as f64 / 1e6);
    eprintln!("bytes 1s after cancel: {:.2} MB (delta {:.0} KB)\n",
        after as f64 / 1e6, (after - at_cancel) as f64 / 1e3);
    assert!(res.is_err(), "cancelled query should return an error, not complete");
    assert!((after - at_cancel) < 2 * 1024 * 1024,
        "fetching should stop within a batch of the cancel (< 2 MB), got {} KB", (after - at_cancel) / 1024);

    child.kill().ok();
    let _ = child.wait();
}

fn tempdir() -> PathBuf {
    let d = std::env::temp_dir().join(format!("ts-bench-{}-{}", std::process::id(), free_port()));
    std::fs::create_dir_all(&d).unwrap();
    d
}
fn cleanup(dirs: &[PathBuf]) {
    for d in dirs {
        std::fs::remove_dir_all(d).ok();
    }
}
