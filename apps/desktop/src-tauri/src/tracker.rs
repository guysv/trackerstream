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
/// Ceiling for the announce backoff when the tracker is unreachable.
const ANNOUNCE_BACKOFF_MAX: Duration = Duration::from_secs(300);

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

// Serialize too: the address book (lib.rs AddressBook) persists peer entries to disk so
// a restart can re-dial last session's peers, and PEX (peer.rs) ships `PeerRef`s over the
// wire. The `peerId` rename keeps the on-disk JSON and the CBOR wire shape identical to
// the tracker wire shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct PeerRef {
    #[serde(rename = "peerId")]
    pub peer_id: String,
    pub addrs: Vec<String>,
}

#[derive(Deserialize)]
struct PeersResponse {
    peers: Vec<PeerRef>,
}

#[derive(Deserialize)]
struct RosterResponse {
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
pub(crate) async fn announce_addrs(ipfs: &Ipfs, peer_id: &PeerId) -> Vec<String> {
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
/// Returns whether it succeeded, so the loop can back off when the tracker is down.
async fn announce_once(
    client: &reqwest::Client,
    ipfs: &Ipfs,
    peer_id: &PeerId,
    held: &HeldRoots,
) -> bool {
    let payload = AnnouncePayload {
        peer_id: peer_id.to_string(),
        addrs: announce_addrs(ipfs, peer_id).await,
        held_roots: held.roots(),
    };
    let url = format!("{}/announce", api_base());
    match client.post(&url).json(&payload).send().await {
        Ok(r) if r.status().is_success() => true,
        Ok(r) => {
            eprintln!("[announce] {url} -> {}", r.status());
            false
        }
        Err(e) => {
            eprintln!("[announce] {url} failed: {e}");
            false
        }
    }
}

/// Spawn the announce/heartbeat loop. Announces immediately (register as soon as the
/// node is up), then sleeps ~30s with jitter between announces. On failure it backs off
/// exponentially (capped) so a dead tracker isn't hammered and clients don't all re-dial
/// the returning tracker in lockstep. See P2P-NEXT-STEPS Phase 0.
pub(crate) fn spawn_announce_loop(ipfs: Ipfs, peer_id: PeerId, held: Arc<HeldRoots>) {
    tauri::async_runtime::spawn(async move {
        let client = reqwest::Client::new();
        let mut backoff = ANNOUNCE_INTERVAL;
        loop {
            if announce_once(&client, &ipfs, &peer_id, &held).await {
                backoff = ANNOUNCE_INTERVAL;
            } else {
                backoff = (backoff * 2).min(ANNOUNCE_BACKOFF_MAX);
            }
            tokio::time::sleep(crate::jittered(backoff)).await;
        }
    });
}

/// Ask the tracker for the online roster (presence backbone). Excludes ourselves
/// client-side. Best-effort: empty on any error. Used to warm a bounded set of
/// peers regardless of content, so connections are symmetric (both ends keepalive)
/// and persist past a download — see PEER-ASSIST.md §2.3 (presence floor).
pub(crate) async fn query_roster(self_peer_id: &str) -> Vec<PeerRef> {
    let url = format!("{}/roster", api_base());
    let peers = match reqwest::Client::new().get(&url).send().await {
        Ok(r) => match r.json::<RosterResponse>().await {
            Ok(rr) => rr.peers,
            Err(e) => {
                eprintln!("[roster] decode {url}: {e}");
                vec![]
            }
        },
        Err(e) => {
            eprintln!("[roster] {url} failed: {e}");
            vec![]
        }
    };
    peers.into_iter().filter(|p| p.peer_id != self_peer_id).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // The roster cache persists Vec<PeerRef> as JSON and reads it back on restart, so
    // PeerRef must Serialize<->Deserialize symmetrically — and the on-disk key must stay
    // `peerId` (matching the tracker wire shape), not the Rust field `peer_id`.
    #[test]
    fn peer_ref_json_round_trips_with_peerid_key() {
        let peers = vec![PeerRef {
            peer_id: "12D3KooWtest".into(),
            addrs: vec!["/ip4/1.2.3.4/tcp/4001/p2p/12D3KooWtest".into()],
        }];
        let json = serde_json::to_string(&peers).unwrap();
        assert!(json.contains("\"peerId\""), "on-disk key must be peerId: {json}");
        assert!(!json.contains("peer_id"), "must not leak the Rust field name: {json}");
        let back: Vec<PeerRef> = serde_json::from_str(&json).unwrap();
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].peer_id, "12D3KooWtest");
        assert_eq!(back[0].addrs, peers[0].addrs);
    }
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
