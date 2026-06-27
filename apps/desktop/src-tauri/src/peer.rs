//! Phase-1 peer control plane (P2P-NEXT-STEPS): a single `request_response` protocol,
//! `/trackerstream/peer/1.0.0`, that lets clients answer each other's control-plane
//! queries WITHOUT the central box. Three queries, each reusing a data shape we already
//! ship to/from the tracker:
//!
//!   * `Pex`   — gossip known peers (PEX): sustains swarm membership through a box outage.
//!   * `Peers` — holder discovery, peer-served: who holds root R.
//!   * `Ipns`  — pull-based IPNS gossip: a peer serves the signed record it resolved,
//!               the puller re-verifies (signature + EOL + newest sequence wins).
//!
//! This rides the BUILT-IN connexa request_response (enabled on the builder in
//! `ipfs::start`), not the no-op custom-behaviour slot. The wire codec is raw bytes, so
//! payloads are CBOR (dag-cbor, like the rest of the data plane). Queries only ever go to
//! peers we are ALREADY connected to (the warm set ∩ live connections) — we never dial a
//! stranger here, preserving the no-crawl posture (PEER-ASSIST G1).

use crate::tracker::PeerRef;
use cid::Cid;
use futures::stream::StreamExt;
use rust_ipfs::{Ipfs, PeerId};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;

/// The single request_response protocol id. Registered on the builder in `ipfs::start`.
pub const PROTOCOL: &str = "/trackerstream/peer/1.0.0";

/// Cap on peers returned/requested in a single `Pex` exchange — keeps responses tiny.
const PEX_CAP: usize = 64;

/// Control-plane request. `root`/`name` are Strings (CID / PeerId text) to match every
/// existing shape (`PeerRef`, `HeldRoots` keys, IPNS names) — zero re-encoding.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum Req {
    /// Send me up to `want` peers you know (PEX gossip).
    Pex { want: u8 },
    /// Who holds this root CID? (peer-served holder discovery)
    Peers { root: String },
    /// Give me the latest signed IPNS record you hold for this name.
    Ipns { name: String },
    /// R2 relay probe. `target = None` → just your node facts (reachable/willing), to
    /// populate the asker's RelayView. `target = Some(Q)` → ALSO answer whether you can
    /// relay me to Q right now (you're willing AND hold a live connection to Q). We reveal
    /// connectivity only to the single Q the asker already discovered — never a neighbor dump.
    Relay { target: Option<String> },
}

/// Control-plane response, paired with `Req`. `record` is the base64 signed record —
/// exactly what `IpnsCache` stores and `ipns::verify_b64*` takes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum Resp {
    Pex { peers: Vec<PeerRef> },
    Peers { peers: Vec<PeerRef> },
    Ipns { record: Option<String> },
    /// R2 relay facts. `reachable`/`willing` are node-global (public AutoNAT verdict + the
    /// opt-in relay policy). `can_reach` answers the `Relay{target}` probe: true iff we're
    /// willing AND connected to that target. All `false` is also the refusal we send a
    /// non-warm requester (we reveal nothing to peers outside our warm set).
    Relay { reachable: bool, willing: bool, can_reach: bool },
}

/// CBOR-encode a request/response for the wire (dag-cbor, like the data plane).
pub(crate) fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>, String> {
    serde_ipld_dagcbor::to_vec(v).map_err(|e| format!("cbor encode: {e}"))
}

/// CBOR-decode a wire payload. A decode error is non-fatal everywhere it's used (we
/// degrade to an empty response / skip the peer), never a panic.
pub(crate) fn decode<'a, T: Deserialize<'a>>(b: &'a [u8]) -> Result<T, String> {
    serde_ipld_dagcbor::from_slice(b).map_err(|e| format!("cbor decode: {e}"))
}

// ---------------------------------------------------------------------------------------
// Serve side — answer inbound requests from the local caches.
// ---------------------------------------------------------------------------------------

/// Read-only dependencies the serve loop hands each request handler.
pub(crate) struct ServeCtx {
    pub ipfs: Ipfs,
    pub held: Arc<crate::HeldRoots>,
    pub ipns: Arc<crate::IpnsCache>,
    pub book: Arc<crate::AddressBook>,
    pub self_peer: String,
    /// R2: our warm set (the `Relay` arm answers only warm requesters), our debounced
    /// AutoNAT reachability, and the static opt-in relay policy (advertised `willing` =
    /// `relay_willing && reachable`).
    pub warm: Arc<crate::WarmSet>,
    pub reach: Arc<crate::Reachability>,
    pub relay_willing: bool,
}

/// Build a `PeerRef` for OURSELVES from our currently-dialable addrs — reuses the exact
/// addr logic the announce path uses (`tracker::announce_addrs`), so a peer can dial us
/// back the same way the tracker would hand us out. `None` if we have no dialable addr.
async fn self_peer_ref(ipfs: &Ipfs, self_peer: &str) -> Option<PeerRef> {
    let pid: PeerId = self_peer.parse().ok()?;
    let addrs = crate::tracker::announce_addrs(ipfs, &pid).await;
    if addrs.is_empty() {
        return None;
    }
    Some(PeerRef { peer_id: self_peer.to_string(), addrs })
}

/// The `Peers{root}` answer, factored pure for testing: we self-report as a holder iff we
/// actually hold `root`. `self_ref` is our PeerRef (the serve loop fetches it lazily, only
/// when we hold the root — `None` otherwise). Reaching OTHER holders is PEX's job.
pub(crate) fn peers_response(held: &crate::HeldRoots, root: &str, self_ref: Option<PeerRef>) -> Resp {
    let peers = match self_ref {
        Some(me) if held.roots().iter().any(|r| r == root) => vec![me],
        _ => vec![],
    };
    Resp::Peers { peers }
}

/// Decode + dispatch one inbound request to a response. Best-effort: garbage decodes to an
/// empty `Pex` response (we still reply, so the requester's future resolves promptly rather
/// than hitting the 30s timeout). `requester` is the asking peer (for the `Relay` warm guard).
async fn respond(ctx: &ServeCtx, requester: PeerId, bytes: &[u8]) -> Resp {
    match decode::<Req>(bytes) {
        Ok(Req::Pex { want }) => Resp::Pex {
            peers: ctx.book.sample((want as usize).min(PEX_CAP), &ctx.self_peer),
        },
        Ok(Req::Peers { root }) => {
            // Only fetch our addrs (a swarm round-trip) when we actually hold the root.
            let self_ref = if ctx.held.roots().iter().any(|r| *r == root) {
                self_peer_ref(&ctx.ipfs, &ctx.self_peer).await
            } else {
                None
            };
            peers_response(&ctx.held, &root, self_ref)
        }
        // We only ever `put` verified records into the cache, so the local store is
        // trusted; the PULLER re-verifies (verify_b64_seq), so no need to verify on serve.
        Ok(Req::Ipns { name }) => Resp::Ipns { record: ctx.ipns.get(&name) },
        Ok(Req::Relay { target }) => relay_response(ctx, requester, target.as_deref()).await,
        Err(_) => Resp::Pex { peers: vec![] },
    }
}

/// The `Relay` answer. We refuse (all-false) any requester outside our warm set — relay facts
/// + connectivity are only for peers we're already assisting, bounding who can probe us.
/// `reachable` is our debounced AutoNAT verdict; `willing = relay_willing && reachable`
/// (an unreachable node can't issue a usable reservation, M1). `can_reach` is true only when
/// we're willing AND hold a live connection to `target` — revealing connectivity to that one
/// peer, never a neighbor list. The connectivity round-trip runs only when it could matter.
async fn relay_response(ctx: &ServeCtx, requester: PeerId, target: Option<&str>) -> Resp {
    let is_warm = ctx.warm.members().contains(&requester);
    let reachable = ctx.reach.public().unwrap_or(false);
    let willing = ctx.relay_willing && reachable;
    // Only probe connectivity when the answer could be `true`: warm + willing + a named target.
    let target_connected = match (is_warm && willing, target.and_then(|t| t.parse::<PeerId>().ok())) {
        (true, Some(t)) => Some(ctx.ipfs.connected().await.unwrap_or_default().contains(&t)),
        _ => None,
    };
    relay_decision(is_warm, reachable, ctx.relay_willing, target_connected)
}

/// Pure `Relay` decision, factored for testing. `target_connected` is `Some(bool)` when a
/// target was named (and we got far enough to check it), `None` otherwise. A non-warm
/// requester learns nothing (all false); `willing` requires both the opt-in policy and
/// reachability; `can_reach` additionally requires a live connection to the target.
pub(crate) fn relay_decision(
    is_warm: bool,
    reachable: bool,
    relay_willing: bool,
    target_connected: Option<bool>,
) -> Resp {
    if !is_warm {
        return Resp::Relay { reachable: false, willing: false, can_reach: false };
    }
    let willing = relay_willing && reachable;
    let can_reach = willing && target_connected.unwrap_or(false);
    Resp::Relay { reachable, willing, can_reach }
}

/// Spawn the serve loop: subscribe to inbound `/trackerstream/peer/1.0.0` requests and
/// answer each. Per-request handlers are SPAWNED (a slow handler or a held Mutex must not
/// stall the single inbound stream); handlers are tiny and bounded. If the subscription
/// itself fails the loop logs and exits — the node still functions as a pure client.
pub(crate) fn spawn_serve_loop(ctx: ServeCtx) {
    tauri::async_runtime::spawn(async move {
        let mut stream = match ctx.ipfs.requests_subscribe(PROTOCOL).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[peer] requests_subscribe({PROTOCOL}) failed: {e}");
                return;
            }
        };
        let ctx = Arc::new(ctx);
        while let Some((peer, id, bytes)) = stream.next().await {
            let ctx = ctx.clone();
            tauri::async_runtime::spawn(async move {
                let resp = respond(&ctx, peer, bytes.as_ref()).await;
                match encode(&resp) {
                    Ok(payload) => {
                        if let Err(e) = ctx.ipfs.send_response(peer, id, (PROTOCOL, payload)).await {
                            eprintln!("[peer] send_response to {peer}: {e}");
                        }
                    }
                    Err(e) => eprintln!("[peer] encode response: {e}"),
                }
            });
        }
    });
}

// ---------------------------------------------------------------------------------------
// Client side — pull from warm peers. Never dials: targets are warm ∩ already-connected.
// ---------------------------------------------------------------------------------------

/// Candidate peers for a pull: warm-set members we ALREADY hold a live connection to,
/// capped at `limit`. This is the never-dial-a-stranger invariant — we only `send_request`
/// over existing connections; the only new-connection path is book-dialing in lib.rs.
async fn warm_targets(ipfs: &Ipfs, warm: &crate::WarmSet, limit: usize) -> Vec<PeerId> {
    let connected: HashSet<PeerId> =
        ipfs.connected().await.unwrap_or_default().into_iter().collect();
    warm.members()
        .into_iter()
        .filter(|p| connected.contains(p))
        .take(limit)
        .collect()
}

/// PEX: ask warm peers for the peers they know, merge their `PeerRef`s. The caller folds
/// the result into the `AddressBook`. Best-effort — dead peers (io::Error in the fan-out)
/// and undecodable responses are skipped.
pub(crate) async fn pex_pull(ipfs: &Ipfs, warm: &crate::WarmSet, want: u8) -> Vec<PeerRef> {
    let targets = warm_targets(ipfs, warm, crate::WARM_CAP).await;
    if targets.is_empty() {
        return vec![];
    }
    let Ok(req) = encode(&Req::Pex { want }) else { return vec![] };
    let mut stream = match ipfs.send_requests(targets, (PROTOCOL, req)).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[pex] send_requests: {e}");
            return vec![];
        }
    };
    let mut out = Vec::new();
    while let Some((_peer, res)) = stream.next().await {
        if let Ok(bytes) = res {
            if let Ok(Resp::Pex { peers }) = decode::<Resp>(&bytes) {
                out.extend(peers);
            }
        }
    }
    out
}

/// IPNS peer-pull: ask a handful of warm peers for `name`'s signed record, verify each
/// (signature + EOL), and return the NEWEST by sequence. `None` if nobody serves a valid
/// record. The caller caches + serves it (pull-based gossip).
pub(crate) async fn ipns_pull(
    ipfs: &Ipfs,
    warm: &crate::WarmSet,
    name: &str,
) -> Option<(String, Cid)> {
    let targets = warm_targets(ipfs, warm, 8).await;
    if targets.is_empty() {
        return None;
    }
    let req = encode(&Req::Ipns { name: name.to_string() }).ok()?;
    let mut stream = ipfs.send_requests(targets, (PROTOCOL, req)).await.ok()?;
    let mut best: Option<(u64, String, Cid)> = None;
    while let Some((_peer, res)) = stream.next().await {
        let Ok(bytes) = res else { continue };
        let Ok(Resp::Ipns { record: Some(b64) }) = decode::<Resp>(&bytes) else { continue };
        if let Ok((cid, seq)) = crate::ipns::verify_b64_seq(name, &b64) {
            if best.as_ref().map_or(true, |(s, _, _)| seq > *s) {
                best = Some((seq, b64, cid));
            }
        }
    }
    best.map(|(_, b64, cid)| (b64, cid))
}

/// Holder discovery, peer-served: ask warm peers who holds `root`, collect the
/// (self-reported) holders. Best-effort. Used as the tracker fallback in `warm_root`.
pub(crate) async fn peers_pull(ipfs: &Ipfs, warm: &crate::WarmSet, root: &str) -> Vec<PeerRef> {
    let targets = warm_targets(ipfs, warm, crate::WARM_CAP).await;
    if targets.is_empty() {
        return vec![];
    }
    let Ok(req) = encode(&Req::Peers { root: root.to_string() }) else { return vec![] };
    let mut stream = match ipfs.send_requests(targets, (PROTOCOL, req)).await {
        Ok(s) => s,
        Err(_) => return vec![],
    };
    let mut out = Vec::new();
    while let Some((_peer, res)) = stream.next().await {
        if let Ok(bytes) = res {
            if let Ok(Resp::Peers { peers }) = decode::<Resp>(&bytes) {
                out.extend(peers);
            }
        }
    }
    out
}

/// A peer's reply to a `Relay` probe (R2).
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct RelayReply {
    pub peer: PeerId,
    pub reachable: bool,
    pub willing: bool,
    pub can_reach: bool,
}

/// R2 relay probe over the warm set. `target = None` gathers each peer's node facts to refresh
/// the `RelayView` (which willing+reachable relays we could reserve on, M3). `target = Some(Q)`
/// additionally asks each whether it can relay us to Q *right now* (the M4 dial-fail fallback).
/// Best-effort, warm ∩ connected only — never dials a stranger.
pub(crate) async fn relay_pull(
    ipfs: &Ipfs,
    warm: &crate::WarmSet,
    target: Option<&str>,
) -> Vec<RelayReply> {
    let targets = warm_targets(ipfs, warm, crate::WARM_CAP).await;
    if targets.is_empty() {
        return vec![];
    }
    let Ok(req) = encode(&Req::Relay { target: target.map(|s| s.to_string()) }) else {
        return vec![];
    };
    let mut stream = match ipfs.send_requests(targets, (PROTOCOL, req)).await {
        Ok(s) => s,
        Err(_) => return vec![],
    };
    let mut out = Vec::new();
    while let Some((peer, res)) = stream.next().await {
        if let Ok(bytes) = res {
            if let Ok(Resp::Relay { reachable, willing, can_reach }) = decode::<Resp>(&bytes) {
                out.push(RelayReply { peer, reachable, willing, can_reach });
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(id: &str) -> PeerRef {
        PeerRef { peer_id: id.into(), addrs: vec![format!("/ip4/1.2.3.4/tcp/4001/p2p/{id}")] }
    }

    #[test]
    fn req_round_trips_every_variant() {
        for r in [
            Req::Pex { want: 32 },
            Req::Peers { root: "bafyroot".into() },
            Req::Ipns { name: "12D3KooWname".into() },
            Req::Relay { target: None },
            Req::Relay { target: Some("12D3KooWtarget".into()) },
        ] {
            let back: Req = decode(&encode(&r).unwrap()).unwrap();
            assert_eq!(back, r);
        }
    }

    #[test]
    fn resp_round_trips_including_empty_and_none() {
        for r in [
            Resp::Pex { peers: vec![peer("12D3KooWa"), peer("12D3KooWb")] },
            Resp::Peers { peers: vec![] },
            Resp::Ipns { record: Some("AAEC".into()) },
            Resp::Ipns { record: None },
            Resp::Relay { reachable: true, willing: true, can_reach: true },
            Resp::Relay { reachable: false, willing: false, can_reach: false },
        ] {
            let back: Resp = decode(&encode(&r).unwrap()).unwrap();
            assert_eq!(back, r);
        }
    }

    #[test]
    fn relay_decision_covers_warm_reach_willing_target() {
        let facts = |r: Resp| match r {
            Resp::Relay { reachable, willing, can_reach } => (reachable, willing, can_reach),
            _ => panic!("expected Resp::Relay"),
        };
        // Non-warm requester learns nothing, even when we're a willing reachable relay
        // connected to the target.
        assert_eq!(facts(relay_decision(false, true, true, Some(true))), (false, false, false));
        // Warm + reachable + willing-policy + connected to target → full yes.
        assert_eq!(facts(relay_decision(true, true, true, Some(true))), (true, true, true));
        // Warm + reachable + willing, but NOT connected to the target → can't reach it.
        assert_eq!(facts(relay_decision(true, true, true, Some(false))), (true, true, false));
        // Not publicly reachable → can't issue a usable reservation, so never willing/can_reach
        // (even with the policy on and a connection to the target).
        assert_eq!(facts(relay_decision(true, false, true, Some(true))), (false, false, false));
        // Reachable but opted OUT of relaying → reachable fact true, but not willing.
        assert_eq!(facts(relay_decision(true, true, false, Some(true))), (true, false, false));
        // Facts-only probe (no target) → no can_reach, willing reflects policy && reachable.
        assert_eq!(facts(relay_decision(true, true, true, None)), (true, true, false));
    }

    #[test]
    fn decode_rejects_garbage() {
        // The serve loop maps a decode error to an empty response; assert it IS an error.
        assert!(decode::<Req>(&[0xff, 0x00, 0x13, 0x37]).is_err());
    }

    /// Phase-1 ship-gate proof, BOX DOWN: two in-process nodes, no tracker. B asks A —
    /// over `/trackerstream/peer/1.0.0` — for an IPNS record, a root's holders, and PEX,
    /// and gets correct answers purely peer-to-peer. Run:
    ///   cargo test --lib peer_to_peer_queries_survive_a_dead_box -- --ignored --nocapture
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "slow (~5s, spins up two real libp2p nodes)"]
    async fn peer_to_peer_queries_survive_a_dead_box() {
        use base64::Engine;
        use rust_ipfs::{Keypair, PeerId};
        use std::time::Duration;

        const CID: &str = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi";
        let root = CID; // A holds this root
        let gossiped = "12D3KooWGossipedPeerXXXXXXXXXXXXXXXXXXXXXXXXXXXX"; // A's book has this

        // A signed, unexpired IPNS record for name N (N == the signing key's PeerId).
        let kp = Keypair::generate_ed25519();
        let name = kp.public().to_peer_id().to_string();
        let record_bytes = rust_ipns::Record::new(&kp, CID.as_bytes(), chrono::Duration::seconds(3600), 1, 0)
            .unwrap()
            .encode()
            .unwrap();
        let record_b64 = base64::engine::general_purpose::STANDARD.encode(&record_bytes);

        // --- Node A: holds root R, caches name N, knows one gossipable peer ---
        let a = crate::ipfs::start(None).await.unwrap();
        let a_peer: PeerId = a.peer_id.parse().unwrap();
        let held = Arc::new(crate::HeldRoots::load(None));
        held.mark(root, true);
        let ipns = Arc::new(crate::IpnsCache::load(None));
        ipns.put(&name, &record_b64);
        let book = Arc::new(crate::AddressBook::load(None));
        book.merge(&[PeerRef {
            peer_id: gossiped.into(),
            addrs: vec![format!("/ip4/7.7.7.7/tcp/4001/p2p/{gossiped}")],
        }]);
        spawn_serve_loop(ServeCtx {
            ipfs: a.ipfs.clone(),
            held,
            ipns,
            book,
            self_peer: a.peer_id.clone(),
            warm: Arc::new(crate::WarmSet::default()),
            reach: Arc::new(crate::Reachability::default()),
            relay_willing: false,
        });

        // --- Node B: connect to A directly (no tracker anywhere), warm A in ---
        let b = crate::ipfs::start(None).await.unwrap();
        let port = a
            .ipfs
            .listening_addresses()
            .await
            .unwrap()
            .iter()
            .find_map(|m| m.iter().find_map(|p| match p {
                rust_ipfs::Protocol::Tcp(port) => Some(port),
                _ => None,
            }))
            .expect("A TCP listener");
        crate::ipfs::connect(&b.ipfs, &format!("/ip4/127.0.0.1/tcp/{port}/p2p/{}", a.peer_id))
            .await
            .unwrap();
        let warm = crate::WarmSet::default();
        warm.record(a_peer, "test");

        // Wait until B<->A is live and A's serve loop has subscribed (retry the pull).
        let pulled = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if let Some(hit) = ipns_pull(&b.ipfs, &warm, &name).await {
                    break hit;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
        .await
        .expect("IPNS peer-pull should resolve from A within 15s");
        assert_eq!(pulled.1.to_string(), CID, "peer-pulled record points at the right CID");

        // Holder discovery: B asks who holds R → A self-reports.
        let holders = peers_pull(&b.ipfs, &warm, root).await;
        assert!(
            holders.iter().any(|p| p.peer_id == a.peer_id),
            "A should self-report as a holder of root R: {holders:?}",
        );

        // PEX: B asks A for peers → gets A's gossiped peer (A excludes itself).
        let learned = pex_pull(&b.ipfs, &warm, 32).await;
        assert!(
            learned.iter().any(|p| p.peer_id == gossiped),
            "PEX should surface A's gossiped peer: {learned:?}",
        );

        // Negatives bound the no-hang claim: unheld root / uncached name return empty/None.
        assert!(peers_pull(&b.ipfs, &warm, "bafyNOTHELD").await.is_empty());
        assert!(ipns_pull(&b.ipfs, &warm, "12D3KooWunknownnamenobodyhas").await.is_none());

        println!("OK: IPNS + holder-discovery + PEX all answered peer-to-peer with no box");
    }

    /// R2/M2: the `Relay` probe over the wire. B pulls A's relay facts and asserts the warm
    /// guard (A refuses until B is in A's warm set), the reachability+policy gating of
    /// `willing`, and that `can_reach` reflects A's live connection to a named target. Run:
    ///   cargo test --lib relay_probe_respects_warm_guard -- --ignored --nocapture
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "slow (~3s, spins up two real libp2p nodes)"]
    async fn relay_probe_respects_warm_guard() {
        use rust_ipfs::{PeerId, Protocol};
        use std::time::Duration;

        // A: a willing, publicly-reachable relay (reachability injected past the hysteresis).
        let a = crate::ipfs::start(None).await.unwrap();
        let b = crate::ipfs::start(None).await.unwrap();
        let a_peer: PeerId = a.peer_id.parse().unwrap();
        let b_peer: PeerId = b.peer_id.parse().unwrap();

        let a_warm = Arc::new(crate::WarmSet::default());
        let a_reach = Arc::new(crate::Reachability::default());
        for _ in 0..3 {
            a_reach.observe(true); // flip A's debounced verdict to public
        }
        spawn_serve_loop(ServeCtx {
            ipfs: a.ipfs.clone(),
            held: Arc::new(crate::HeldRoots::load(None)),
            ipns: Arc::new(crate::IpnsCache::load(None)),
            book: Arc::new(crate::AddressBook::load(None)),
            self_peer: a.peer_id.clone(),
            warm: a_warm.clone(),
            reach: a_reach,
            relay_willing: true,
        });

        // B connects to A and warms it (so B will probe A).
        let port = a
            .ipfs
            .listening_addresses()
            .await
            .unwrap()
            .iter()
            .find_map(|m| m.iter().find_map(|p| match p {
                Protocol::Tcp(port) => Some(port),
                _ => None,
            }))
            .expect("A TCP listener");
        crate::ipfs::connect(&b.ipfs, &format!("/ip4/127.0.0.1/tcp/{port}/p2p/{}", a.peer_id))
            .await
            .unwrap();
        let warm = crate::WarmSet::default();
        warm.record(a_peer, "test");

        // Case 1 — B is NOT in A's warm set: A refuses (all-false), even though A is a willing
        // reachable relay. Retry until the serve loop is subscribed + the link is live.
        let refused = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let r = relay_pull(&b.ipfs, &warm, None).await;
                if let Some(reply) = r.iter().find(|r| r.peer == a_peer) {
                    break *reply;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
        .await
        .expect("A should answer the relay probe within 15s");
        assert_eq!(
            (refused.reachable, refused.willing, refused.can_reach),
            (false, false, false),
            "non-warm requester must learn nothing",
        );

        // Case 2 — add B to A's warm set: A now shares facts. willing = policy && reachable.
        a_warm.record(b_peer, "test");
        let facts = relay_pull(&b.ipfs, &warm, None).await;
        let a_facts = facts.iter().find(|r| r.peer == a_peer).expect("A reply");
        assert!(a_facts.reachable && a_facts.willing, "warm + reachable + willing: {a_facts:?}");
        assert!(!a_facts.can_reach, "facts-only probe carries no can_reach");

        // Case 3 — targeted probe: A is connected to B, so 'can you reach B?' is true.
        let targeted = relay_pull(&b.ipfs, &warm, Some(&b.peer_id)).await;
        let t = targeted.iter().find(|r| r.peer == a_peer).expect("A reply");
        assert!(t.can_reach, "A is connected to the named target, so can_reach: {t:?}");

        println!("OK: relay probe respects warm guard, reachability gating, and can_reach");
    }
}
