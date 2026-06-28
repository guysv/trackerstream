//! Client-seam integration: the desktop backend's RPC client (`rpc::NodeRpc`) + sidecar
//! manager (`sidecar::Sidecar`) driving REAL tsnode binaries — the mirror of the server-seam
//! proof, but for the client data plane (block fetch, ranged cat, status, IPNS resolve).
//!
//! Requires the tsnode binary; set `TS_NODE_BIN=/path/to/tsnode` (e.g. `go -C node build -o
//! /tmp/tsnode ./cmd/tsnode`). Skips (passes) when the binary is absent so a plain `cargo
//! test` without it stays green.
use desktop_lib::rpc::NodeRpc;
use desktop_lib::sidecar::Sidecar;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

fn bin() -> Option<PathBuf> {
    std::env::var("TS_NODE_BIN").ok().map(PathBuf::from).filter(|p| p.exists())
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// Spawn a tsnode with an explicit role/repo/rpc port and wait for its RPC.
async fn spawn_node(bin: &PathBuf, role: &str, repo: &std::path::Path, rpc_port: u16, bootstrap: &str) -> Child {
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

/// Raw POST helper (block/put + add accept a raw body when not multipart).
async fn raw_post(url: &str, body: Vec<u8>) -> reqwest::Response {
    reqwest::Client::new().post(url).body(body).send().await.expect("post")
}

#[tokio::test]
async fn client_data_plane_over_two_nodes() {
    let Some(bin) = bin() else {
        eprintln!("SKIP: set TS_NODE_BIN to run the client-seam integration");
        return;
    };
    let srv_repo = tempdir();
    let cli_repo = tempdir();
    let srv_port = free_port();
    let cli_port = free_port();

    let mut srv = spawn_node(&bin, "server", &srv_repo, srv_port, "").await;
    let srv_rpc = NodeRpc::new(&format!("127.0.0.1:{srv_port}"));
    let srv_id = srv_rpc.id().await.unwrap();

    // Server stores a block (raw-body block/put) and a multi-chunk file (raw-body add).
    let block = b"client-seam: a block fetched over Bitswap via the RPC".to_vec();
    #[derive(serde::Deserialize)]
    struct Put { #[serde(rename = "Key")] key: String }
    let put: Put = raw_post(&format!("http://127.0.0.1:{srv_port}/api/v0/block/put?cid-codec=raw"), block.clone())
        .await.json().await.unwrap();

    let file: Vec<u8> = (0..80_000u32).map(|i| (i % 251) as u8).collect();
    #[derive(serde::Deserialize)]
    struct Add { #[serde(rename = "Hash")] hash: String }
    let add_text = raw_post(&format!("http://127.0.0.1:{srv_port}/api/v0/add?chunker=size-16384&pin=true"), file.clone())
        .await.text().await.unwrap();
    let add: Add = serde_json::from_str(add_text.lines().last().unwrap()).unwrap();

    // Client node, bootstrapped to the server.
    let boot = srv_id.addresses.iter().find(|a| a.contains("/127.0.0.1/"))
        .map(|a| format!("{a}/p2p/{}", srv_id.id)).expect("server loopback addr");
    let mut cli = spawn_node(&bin, "client", &cli_repo, cli_port, &boot).await;
    let cli_rpc = NodeRpc::new(&format!("127.0.0.1:{cli_port}"));
    cli_rpc.swarm_connect(&boot).await.expect("client connects to server");

    // block/get over Bitswap from the connected server.
    let got = cli_rpc.block_get(&put.key).await.expect("block_get");
    assert_eq!(got, block, "block bytes round-trip over the client RPC");

    // cat range out of the published file (the catalog VFS read path).
    let slice = cli_rpc.cat(&add.hash, 16384, 100).await.expect("cat range");
    assert_eq!(slice, &file[16384..16484], "ranged cat returns the exact slice");

    // node/status + swarm/peers reflect the live link.
    let status = cli_rpc.node_status().await.expect("node_status");
    assert!(status.peers >= 1, "client should see at least the server peer");
    assert!(cli_rpc.swarm_peers().await.unwrap().iter().any(|p| p.peer == srv_id.id));

    srv.kill().ok();
    cli.kill().ok();
    let _ = (srv.wait(), cli.wait());
    cleanup(&[srv_repo, cli_repo]);
}

#[tokio::test]
async fn sidecar_lifecycle_spawns_and_health_checks() {
    let Some(bin) = bin() else {
        eprintln!("SKIP: set TS_NODE_BIN to run the sidecar-lifecycle test");
        return;
    };
    let repo = tempdir();
    let mut sc = Sidecar::spawn(&bin, &repo, "").await.expect("sidecar spawns + becomes healthy");
    // The managed RPC answers id.
    let id = sc.rpc().id().await.expect("sidecar rpc id");
    assert!(id.id.starts_with("12D3KooW"), "got a libp2p PeerId");
    assert!(sc.rpc_addr().starts_with("127.0.0.1:"));
    sc.shutdown();
    cleanup(&[repo]);
}

// --- tiny temp-dir helpers (avoid pulling a tempfile dep) ---
fn tempdir() -> PathBuf {
    let d = std::env::temp_dir().join(format!("ts-it-{}-{}", std::process::id(), free_port()));
    std::fs::create_dir_all(&d).unwrap();
    d
}
fn cleanup(dirs: &[PathBuf]) {
    for d in dirs {
        std::fs::remove_dir_all(d).ok();
    }
}
