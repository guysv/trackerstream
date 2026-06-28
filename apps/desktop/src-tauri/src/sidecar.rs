//! Lifecycle for the bundled `tsnode` sidecar: pick a free RPC port, spawn the binary with the
//! app's data dir as its repo, health-check the RPC, and shut it down on exit. This reverses the
//! old "no second process" design (`ipfs.rs` header) — the libp2p node is now an external Go
//! process the backend drives over `rpc.rs`.
//!
//! In a packaged app the binary is the Tauri externalBin (resolved next to the main executable
//! as `tsnode`); in dev it is found via `TS_NODE_BIN` or `tsnode` on PATH. Spawning is done with
//! a plain `std::process::Command` so this module is testable without the Tauri runtime.
use crate::rpc::NodeRpc;
use anyhow::{anyhow, Context, Result};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

/// A running tsnode child. Dropping it kills the process (best-effort), so the node dies with
/// the app.
pub struct Sidecar {
    child: Child,
    rpc_addr: String,
    rpc: NodeRpc,
}

impl Sidecar {
    /// The RPC client for the spawned node.
    pub fn rpc(&self) -> NodeRpc {
        self.rpc.clone()
    }

    /// The loopback RPC address the node is serving (e.g. "127.0.0.1:53111").
    pub fn rpc_addr(&self) -> &str {
        &self.rpc_addr
    }

    /// Spawn `tsnode --role client` with `repo` as its data dir, on a free loopback RPC port,
    /// then wait (up to ~10s) for the RPC to answer `id`. `bootstrap` is an optional
    /// comma-separated bootstrap multiaddr list (the master); empty uses tsnode's built-in.
    pub async fn spawn(bin: &Path, repo: &Path, bootstrap: &str) -> Result<Self> {
        std::fs::create_dir_all(repo).with_context(|| format!("create repo {}", repo.display()))?;
        // A sidecar orphaned by an abnormally-killed app (panic / SIGKILL, so Drop never ran)
        // keeps the datastore LOCK held, so the next launch dies with "datastore: resource
        // temporarily unavailable". Best-effort reap any stale tsnode still bound to THIS repo
        // before we spawn — the repo path is unique to this app's data dir, so it only ever
        // matches our own leftover child.
        reap_stale_for_repo(repo);
        let port = free_loopback_port()?;
        let rpc_addr = format!("127.0.0.1:{port}");

        let mut cmd = Command::new(bin);
        cmd.arg("--role")
            .arg("client")
            .arg("--repo")
            .arg(repo)
            .arg("--swarm-port")
            .arg("0")
            .arg("--rpc")
            .arg(&rpc_addr)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        if !bootstrap.is_empty() {
            cmd.arg("--bootstrap").arg(bootstrap);
        }
        let child = cmd
            .spawn()
            .with_context(|| format!("spawn tsnode at {}", bin.display()))?;

        let rpc = NodeRpc::new(&rpc_addr);
        let mut sc = Sidecar {
            child,
            rpc_addr,
            rpc,
        };
        sc.await_healthy(Duration::from_secs(10)).await?;
        Ok(sc)
    }

    /// Poll `id` until the RPC answers or `deadline` elapses (or the child dies).
    async fn await_healthy(&mut self, deadline: Duration) -> Result<()> {
        let start = std::time::Instant::now();
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Err(anyhow!("tsnode exited during startup: {status}"));
            }
            if self.rpc.id().await.is_ok() {
                return Ok(());
            }
            if start.elapsed() > deadline {
                let _ = self.child.kill();
                return Err(anyhow!("tsnode RPC did not become healthy within {deadline:?}"));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Stop the node (SIGKILL via std). Called on app shutdown; also runs on Drop.
    pub fn shutdown(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Sidecar {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Best-effort: terminate any leftover tsnode process still bound to `repo` (an orphan from a
/// previous run whose Drop never fired). Matches on the unique repo path (the `--repo` argv
/// value), so it can't hit an unrelated process, and runs BEFORE we spawn so it never touches
/// our own new child. Unix uses `pkill -f`; on other platforms this is a no-op (the next spawn
/// surfaces the lock error instead). The brief sleep lets the OS release the datastore lock.
fn reap_stale_for_repo(repo: &Path) {
    #[cfg(unix)]
    {
        let path = repo.to_string_lossy();
        let killed = Command::new("pkill")
            .arg("-f")
            .arg(path.as_ref())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if killed {
            std::thread::sleep(Duration::from_millis(300));
        }
    }
    #[cfg(not(unix))]
    let _ = repo;
}

/// Bind :0 to learn a currently-free loopback port, then release it for the child to claim. A
/// tiny TOCTOU window exists, but on loopback for a just-launched app it is not a practical
/// concern (and far safer than hard-coding kubo's 5001, which may collide with a real kubo).
fn free_loopback_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").context("bind ephemeral port")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

/// Locate the tsnode binary: `TS_NODE_BIN` override, else `tsnode` beside the current executable
/// (the packaged externalBin location), else `tsnode` on PATH (dev).
pub fn locate_binary() -> Result<PathBuf> {
    if let Ok(p) = std::env::var("TS_NODE_BIN") {
        let p = PathBuf::from(p);
        if p.exists() {
            return Ok(p);
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let name = if cfg!(windows) { "tsnode.exe" } else { "tsnode" };
            let cand = dir.join(name);
            if cand.exists() {
                return Ok(cand);
            }
        }
    }
    Ok(PathBuf::from("tsnode")) // PATH fallback
}
