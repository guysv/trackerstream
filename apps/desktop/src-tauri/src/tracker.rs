//! Peer-assist tracker CLIENT: announces our presence + held roots to the
//! coordination server (apps/server) and queries it for holders of a given root.
//! This is the CONTROL plane only — it decides WHO to connect to. The data plane
//! (Bitswap over the warm set) is unchanged. See PEER-ASSIST.md §2.2.

use crate::HeldRoots;
use rust_ipfs::{Ipfs, Multiaddr, PeerId, Protocol};
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_API_BASE: &str = "https://trackerstream.xyz";
const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(30);

/// Tracker base URL. `TS_API_BASE` overrides it (local dev / 2-client test against
/// a local server, e.g. http://127.0.0.1:8080).
pub(crate) fn api_base() -> String {
    std::env::var("TS_API_BASE").unwrap_or_else(|_| DEFAULT_API_BASE.to_string())
}

#[derive(Serialize)]
struct AnnouncePayload {
    #[serde(rename = "peerId")]
    peer_id: String,
    addrs: Vec<String>,
    #[serde(rename = "heldRoots")]
    held_roots: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PeerRef {
    #[serde(rename = "peerId")]
    pub peer_id: String,
    pub addrs: Vec<String>,
}

#[derive(Deserialize)]
struct PeersResponse {
    peers: Vec<PeerRef>,
}

/// Best-effort primary LAN IP (no packets sent — connecting a UDP socket just
/// selects the egress interface so we can read its local address).
fn local_lan_ip() -> Option<IpAddr> {
    let sock = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    sock.connect("8.8.8.8:80").ok()?;
    sock.local_addr().ok().map(|sa| sa.ip())
}

/// Rewrite an unspecified bind addr (0.0.0.0 / ::) to the LAN IP; drop it if there
/// is no usable LAN IP. Concrete addrs (loopback / LAN / public) pass through.
fn rewrite_unspecified(ma: Multiaddr, lan: Option<IpAddr>) -> Option<Multiaddr> {
    let mut out = Multiaddr::empty();
    for p in ma.iter() {
        match p {
            Protocol::Ip4(a) if a.is_unspecified() => match lan {
                Some(IpAddr::V4(l)) => out.push(Protocol::Ip4(l)),
                _ => return None,
            },
            Protocol::Ip6(a) if a.is_unspecified() => match lan {
                Some(IpAddr::V6(l)) => out.push(Protocol::Ip6(l)),
                _ => return None,
            },
            other => out.push(other),
        }
    }
    Some(out)
}

/// Ensure the addr ends in `/p2p/<peerId>` so a holder can be dialed directly.
fn with_p2p(mut ma: Multiaddr, peer_id: &PeerId) -> Multiaddr {
    if !matches!(ma.iter().last(), Some(Protocol::P2p(_))) {
        ma.push(Protocol::P2p(*peer_id));
    }
    ma
}

/// Our dialable addrs for the announce: AutoNAT/identify-confirmed external addrs
/// (incl. relay/circuit) when present, else listening addrs with 0.0.0.0 rewritten
/// to the LAN IP so the local 2-client test is dialable. Each carries `/p2p/<id>`.
async fn announce_addrs(ipfs: &Ipfs, peer_id: &PeerId) -> Vec<String> {
    let external = ipfs.external_addresses().await.unwrap_or_default();
    let base: Vec<Multiaddr> = if !external.is_empty() {
        external
    } else {
        let lan = local_lan_ip();
        ipfs
            .listening_addresses()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter_map(|a| rewrite_unspecified(a, lan))
            .collect()
    };
    base.into_iter()
        .map(|a| with_p2p(a, peer_id).to_string())
        .collect()
}

/// POST one announce (presence + held roots). Doubles as the presence heartbeat.
async fn announce_once(client: &reqwest::Client, ipfs: &Ipfs, peer_id: &PeerId, held: &HeldRoots) {
    let payload = AnnouncePayload {
        peer_id: peer_id.to_string(),
        addrs: announce_addrs(ipfs, peer_id).await,
        held_roots: held.roots(),
    };
    let url = format!("{}/announce", api_base());
    match client.post(&url).json(&payload).send().await {
        Ok(r) if r.status().is_success() => {}
        Ok(r) => eprintln!("[announce] {url} -> {}", r.status()),
        Err(e) => eprintln!("[announce] {url} failed: {e}"),
    }
}

/// Spawn the announce/heartbeat loop. `tokio::time::interval` fires immediately on
/// the first tick, so we register as soon as the node is up, then every ~30s.
pub(crate) fn spawn_announce_loop(ipfs: Ipfs, peer_id: PeerId, held: Arc<HeldRoots>) {
    tauri::async_runtime::spawn(async move {
        let client = reqwest::Client::new();
        let mut tick = tokio::time::interval(ANNOUNCE_INTERVAL);
        loop {
            tick.tick().await;
            announce_once(&client, &ipfs, &peer_id, &held).await;
        }
    });
}

/// Ask the tracker which peers hold `root` (excluding ourselves). Best-effort:
/// returns empty on any error so callers degrade to the master fallback.
pub(crate) async fn query_peers(self_peer_id: &str, root: &str) -> Vec<PeerRef> {
    let url = format!("{}/peers?root={root}&self={self_peer_id}", api_base());
    match reqwest::Client::new().get(&url).send().await {
        Ok(r) => match r.json::<PeersResponse>().await {
            Ok(pr) => pr.peers,
            Err(e) => {
                eprintln!("[peers] decode {url}: {e}");
                vec![]
            }
        },
        Err(e) => {
            eprintln!("[peers] {url} failed: {e}");
            vec![]
        }
    }
}
