//! Local RPC client for the tsnode sidecar (the Go go-libp2p+boxo node bundled with the app).
//!
//! The desktop backend no longer embeds an in-process libp2p node; instead it spawns `tsnode`
//! (see `sidecar.rs`) and drives it over its kubo-compatible `/api/v0/` HTTP RPC on loopback —
//! the same contract the server ingest uses (`packages/repack/src/kubo.ts`). This module is the
//! thin typed client for the subset the client needs: block/cat fetch (data plane), swarm
//! connect/peers (warm-set forming), IPNS resolve (`routing/get`), and the trackerstream
//! extensions (`bandwidth/by-peer`, `node/status`, `warm`). The pure local logic — audio
//! reassembly, the catalog SQLite VFS, IPNS signature verify — stays in Rust and rides on top.
use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;

/// A handle to the local tsnode RPC. Cheap to clone (wraps a reqwest client + base URL).
#[derive(Clone)]
pub struct NodeRpc {
    http: reqwest::Client,
    base: String, // e.g. "http://127.0.0.1:5099"
}

/// Identity of the local node (`/api/v0/id`).
#[derive(Debug, Deserialize)]
pub struct NodeId {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "Addresses", default)]
    pub addresses: Vec<String>,
}

/// A live swarm connection (`/api/v0/swarm/peers`).
#[derive(Debug, Deserialize)]
pub struct SwarmPeer {
    #[serde(rename = "Peer")]
    pub peer: String,
    #[serde(rename = "Addr", default)]
    pub addr: String,
}

/// Relay-hop breakdown for the peers pane (`node/status`).
#[derive(Debug, Default, Deserialize, Clone)]
pub struct RelayStats {
    #[serde(default)]
    pub direct: u64,
    #[serde(default)]
    pub peer_relay: u64,
    #[serde(default)]
    pub master_relay: u64,
}

/// Consolidated node status (`/api/v0/node/status`) — reachability + relay + counts.
#[derive(Debug, Default, Deserialize)]
pub struct NodeStatus {
    #[serde(rename = "Reachability", default)]
    pub reachability: String,
    #[serde(rename = "RelayStats", default)]
    pub relay_stats: RelayStats,
    #[serde(rename = "Peers", default)]
    pub peers: u64,
    #[serde(rename = "CatalogPeers", default)]
    pub catalog_peers: u64,
    #[serde(rename = "TotalIn", default)]
    pub total_in: u64,
    #[serde(rename = "TotalOut", default)]
    pub total_out: u64,
    #[serde(rename = "Pins", default)]
    pub pins: u64,
}

/// Per-peer Bitswap byte attribution (`bandwidth/by-peer`) — the peers-pane down/up totals
/// (the rust-ipfs patch-0001 `bandwidth_snapshot` dissolved into go-libp2p's BandwidthCounter).
#[derive(Debug, Deserialize)]
struct PeerBytes {
    #[serde(rename = "TotalIn", default)]
    total_in: u64,
    #[serde(rename = "TotalOut", default)]
    total_out: u64,
}

impl NodeRpc {
    /// Build a client for the RPC at `base` (e.g. "127.0.0.1:5099", with or without scheme).
    pub fn new(base: &str) -> Self {
        let base = if base.starts_with("http") {
            base.trim_end_matches('/').to_string()
        } else {
            format!("http://{}", base.trim_end_matches('/'))
        };
        // No global timeout: block/cat fetches over Bitswap can legitimately take many seconds.
        // Per-call timeouts are applied where they matter (id health checks, cat).
        let http = reqwest::Client::builder()
            .build()
            .expect("reqwest client");
        Self { http, base }
    }

    fn url(&self, path: &str) -> String {
        format!("{}/api/v0/{}", self.base, path)
    }

    /// `id` — the local PeerId + listen addresses. Also the RPC health probe.
    pub async fn id(&self) -> Result<NodeId> {
        let resp = self
            .http
            .post(self.url("id"))
            .timeout(Duration::from_secs(5))
            .send()
            .await?
            .error_for_status()?;
        Ok(resp.json().await?)
    }

    /// `block/get?arg=<cid>` — the raw block bytes. The caller verifies the CID (Bitswap already
    /// content-addresses, but we re-hash on the reassembly path as defense in depth).
    pub async fn block_get(&self, cid: &str) -> Result<Vec<u8>> {
        let resp = self
            .http
            .post(self.url(&format!("block/get?arg={cid}")))
            .send()
            .await?
            .error_for_status()
            .with_context(|| format!("block/get {cid}"))?;
        Ok(resp.bytes().await?.to_vec())
    }

    /// `cat?arg=<cid>&offset&length` — a byte range of a UnixFS file, fetching only the leaves
    /// the range covers. The catalog SQLite VFS rides this for lazy page reads.
    pub async fn cat(&self, cid: &str, offset: u64, length: u64) -> Result<Vec<u8>> {
        let resp = self
            .http
            .post(self.url(&format!("cat?arg={cid}&offset={offset}&length={length}")))
            .timeout(Duration::from_secs(30))
            .send()
            .await?
            .error_for_status()
            .with_context(|| format!("cat {cid} [{offset},+{length})"))?;
        Ok(resp.bytes().await?.to_vec())
    }

    /// `provide/track-root?arg=<cid>` — advertise a track manifest root this node now holds, so
    /// peers can discover us as a provider of the whole track (bitswap pulls the interior DAG).
    /// Best-effort: providing failures are non-fatal to playback, so callers may ignore the error.
    pub async fn provide_track_root(&self, cid: &str) -> Result<()> {
        self.http
            .post(self.url(&format!("provide/track-root?arg={cid}")))
            .send()
            .await?
            .error_for_status()
            .with_context(|| format!("provide/track-root {cid}"))?;
        Ok(())
    }

    /// `dial-providers?arg=<cid>` — find the providers of a CID on the DHT and connect to the
    /// non-seed ones, so the ensuing bitswap fetch pulls from peers (incl. same-LAN) rather than
    /// only the seed. Fire this at the start of a track fetch; it's best-effort and the fetch
    /// proceeds regardless (the seed remains the fallback).
    pub async fn dial_providers(&self, cid: &str) -> Result<()> {
        self.http
            .post(self.url(&format!("dial-providers?arg={cid}")))
            .send()
            .await?
            .error_for_status()
            .with_context(|| format!("dial-providers {cid}"))?;
        Ok(())
    }

    /// `swarm/connect?arg=<multiaddr>` — dial a peer (warm-set forming, master keepalive).
    pub async fn swarm_connect(&self, multiaddr: &str) -> Result<()> {
        self.http
            .post(self.url(&format!("swarm/connect?arg={}", urlencode(multiaddr))))
            .timeout(Duration::from_secs(30))
            .send()
            .await?
            .error_for_status()
            .with_context(|| format!("swarm/connect {multiaddr}"))?;
        Ok(())
    }

    /// `swarm/disconnect?arg=<multiaddr>` — drop a peer (warm-set eviction).
    pub async fn swarm_disconnect(&self, multiaddr: &str) -> Result<()> {
        self.http
            .post(self.url(&format!("swarm/disconnect?arg={}", urlencode(multiaddr))))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    /// `swarm/peers` — currently connected peers.
    pub async fn swarm_peers(&self) -> Result<Vec<SwarmPeer>> {
        #[derive(Deserialize)]
        struct Resp {
            #[serde(rename = "Peers", default)]
            peers: Option<Vec<SwarmPeer>>,
        }
        let resp: Resp = self
            .http
            .post(self.url("swarm/peers"))
            .timeout(Duration::from_secs(10))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(resp.peers.unwrap_or_default())
    }

    /// `warm?arg=<peer>` — mark a peer keepalive-worthy (the Go node redials it on drop).
    pub async fn warm(&self, peer: &str) -> Result<()> {
        self.http
            .post(self.url(&format!("warm?arg={peer}")))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    /// `routing/get?arg=/ipns/<name>` — the latest signed IPNS record (base64), resolved by the
    /// Go node from its cache / gossipsub topic / custom DHT. The CALLER verifies the signature
    /// (`ipns::verify_b64`) — the node is an untrusted cache, exactly like the tracker was.
    pub async fn routing_get(&self, name: &str) -> Result<String> {
        #[derive(Deserialize)]
        struct Line {
            #[serde(rename = "Type", default)]
            kind: i64,
            #[serde(rename = "Extra", default)]
            extra: String,
        }
        let text = self
            .http
            .post(self.url(&format!("routing/get?arg=/ipns/{name}")))
            .timeout(Duration::from_secs(8))
            .send()
            .await?
            .error_for_status()
            .with_context(|| format!("routing/get {name}"))?
            .text()
            .await?;
        // Newline-delimited JSON; the signed record is the first Type==5 (Value) line's Extra.
        for line in text.lines() {
            if let Ok(l) = serde_json::from_str::<Line>(line) {
                if l.kind == 5 && !l.extra.is_empty() {
                    return Ok(l.extra);
                }
            }
        }
        Err(anyhow!("routing/get {name}: no Value record"))
    }

    /// `node/status` — reachability verdict + relay-hop breakdown + peer/byte counts.
    pub async fn node_status(&self) -> Result<NodeStatus> {
        let resp = self
            .http
            .post(self.url("node/status"))
            .timeout(Duration::from_secs(5))
            .send()
            .await?
            .error_for_status()?;
        Ok(resp.json().await?)
    }

    /// `bandwidth/by-peer` — per-peer cumulative Bitswap `(down, up)` byte totals.
    pub async fn bandwidth_by_peer(&self) -> Result<HashMap<String, (u64, u64)>> {
        #[derive(Deserialize)]
        struct Resp {
            #[serde(rename = "ByPeer", default)]
            by_peer: HashMap<String, PeerBytes>,
        }
        let resp: Resp = self
            .http
            .post(self.url("bandwidth/by-peer"))
            .timeout(Duration::from_secs(5))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(resp
            .by_peer
            .into_iter()
            .map(|(p, b)| (p, (b.total_in, b.total_out)))
            .collect())
    }
}

/// Minimal percent-encoding for multiaddr query args (`/`, `+` and the like). reqwest does not
/// encode query values placed directly in the path, so we encode the few bytes that matter.
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}
