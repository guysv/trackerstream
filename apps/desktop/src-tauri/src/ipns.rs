//! Signed-IPNS verification — the trust anchor of master-independent catalog resolution. The
//! tsnode sidecar resolves `/ipns/<name>` (gossipsub catalog topic + custom DHT) and hands back
//! a SIGNED record; this module verifies the signature LOCALLY against the name's public key, so
//! the node (like the old tracker) is an untrusted cache — no peer or node can forge a newer
//! record. The Go node SIGNS byte-compatibly (boxo `ipns`); this Rust path VERIFIES (rust_ipns).

use anyhow::{anyhow, Result};
use base64::Engine;
use cid::Cid;
use libp2p_identity::PeerId;
use std::time::{SystemTime, UNIX_EPOCH};

/// Decode a base64-encoded signed record and verify it (signature + EOL), returning the
/// CID it points at. Used by lib.rs's resolve (fresh from `routing/get`) and the on-disk IPNS
/// cache — a cached entry is re-verified on every read, so a stale/expired/forged entry fails
/// here and the caller falls through. No network — pure decode + verify.
pub(crate) fn verify_b64(name: &str, b64: &str) -> Result<Cid> {
    verify_b64_seq(name, b64).map(|(cid, _)| cid)
}

/// Like `verify_b64`, but also returns the record SEQUENCE number. Peer-pull
/// (`peer::ipns_pull`, P2P-NEXT-STEPS Phase 1) fans an `Ipns` request out to several warm
/// peers and must pick the *newest* validly-signed record; the IPNS sequence number is
/// that total order. `verify_b64` drops the seq for callers that only need the CID. No
/// network — pure decode + verify.
pub(crate) fn verify_b64_seq(name: &str, b64: &str) -> Result<(Cid, u64)> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b64.trim())
        .map_err(|e| anyhow!("ipns record not base64: {e}"))?;
    verify_record_seq(name, &bytes)
}

/// The trust core, factored out for testing: decode a signed IPNS record, verify
/// its signature against `name`'s public key, reject it if expired, and return the
/// CID it points at. Pure + synchronous — no network. The value is stored as the
/// bare root CID string (what the master's publish hook must write). Production callers
/// go through `verify_b64`/`verify_b64_seq`; the tests exercise this core directly.
#[cfg(test)]
fn verify_record(name: &str, bytes: &[u8]) -> Result<Cid> {
    verify_record_seq(name, bytes).map(|(cid, _)| cid)
}

/// `verify_record` plus the record sequence (newest-wins ordering for peer-pull).
fn verify_record_seq(name: &str, bytes: &[u8]) -> Result<(Cid, u64)> {
    let peer_id: PeerId = name
        .parse()
        .map_err(|e| anyhow!("bad ipns name {name}: {e}"))?;
    let record = rust_ipns::Record::decode(bytes).map_err(|e| anyhow!("decode ipns record: {e}"))?;
    // Signature check: derives the pubkey from the PeerId and verifies signatureV2.
    record
        .verify(peer_id)
        .map_err(|e| anyhow!("ipns signature invalid for {name}: {e}"))?;
    // Freshness: verify() does NOT enforce EOL, so reject expired records here to
    // bound downgrade/withholding (a stale-but-validly-signed record).
    if let Ok(validity) = record.validity() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        if validity.timestamp() < now {
            return Err(anyhow!("ipns record for {name} expired at {validity}"));
        }
    }
    let cid = record.value().map_err(|e| anyhow!("ipns value: {e}"))?;
    Ok((cid, record.sequence()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use libp2p_identity::Keypair;

    const CID: &str = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi";

    fn signed(kp: &Keypair, value: &str, ttl_secs: i64) -> Vec<u8> {
        rust_ipns::Record::new(kp, value.as_bytes(), Duration::seconds(ttl_secs), 1, 0)
            .unwrap()
            .encode()
            .unwrap()
    }

    #[test]
    fn verifies_and_extracts_cid() {
        let kp = Keypair::generate_ed25519();
        let name = kp.public().to_peer_id().to_string();
        let cid = verify_record(&name, &signed(&kp, CID, 3600)).unwrap();
        assert_eq!(cid.to_string(), CID);
    }

    #[test]
    fn rejects_record_signed_by_a_different_key() {
        // A forged record (signed by an impostor) must not verify against our name.
        let ours = Keypair::generate_ed25519();
        let impostor = Keypair::generate_ed25519();
        let name = ours.public().to_peer_id().to_string();
        assert!(verify_record(&name, &signed(&impostor, CID, 3600)).is_err());
    }

    #[test]
    fn rejects_expired_record() {
        // A record whose EOL is in the past must be rejected (downgrade guard).
        let kp = Keypair::generate_ed25519();
        let name = kp.public().to_peer_id().to_string();
        assert!(verify_record(&name, &signed(&kp, CID, -10)).is_err());
    }

    fn b64(bytes: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    fn signed_seq(kp: &Keypair, value: &str, ttl_secs: i64, seq: u64) -> Vec<u8> {
        rust_ipns::Record::new(kp, value.as_bytes(), Duration::seconds(ttl_secs), seq, 0)
            .unwrap()
            .encode()
            .unwrap()
    }

    #[test]
    fn verify_b64_seq_returns_the_record_sequence() {
        // The sequence drives newest-wins in peer-pull, so it must round-trip.
        let kp = Keypair::generate_ed25519();
        let name = kp.public().to_peer_id().to_string();
        let (cid, seq) = verify_b64_seq(&name, &b64(&signed_seq(&kp, CID, 3600, 7))).unwrap();
        assert_eq!(cid.to_string(), CID);
        assert_eq!(seq, 7);
    }

    #[test]
    fn newest_sequence_wins_in_a_pull_fold() {
        // Mirrors ipns_pull's fold: among several validly-signed records for the same
        // name, the highest sequence must be selected.
        let kp = Keypair::generate_ed25519();
        let name = kp.public().to_peer_id().to_string();
        let records = [
            b64(&signed_seq(&kp, CID, 3600, 1)),
            b64(&signed_seq(&kp, CID, 3600, 5)),
            b64(&signed_seq(&kp, CID, 3600, 3)),
        ];
        let mut best: Option<(u64, Cid)> = None;
        for r in &records {
            if let Ok((cid, seq)) = verify_b64_seq(&name, r) {
                if best.as_ref().map_or(true, |(s, _)| seq > *s) {
                    best = Some((seq, cid));
                }
            }
        }
        assert_eq!(best.unwrap().0, 5);
    }

    #[test]
    fn verify_b64_round_trips_a_cached_record() {
        // The IPNS cache stores the base64 record; on read it's decoded + re-verified
        // via verify_b64. A valid (signed, unexpired) cached entry must resolve.
        let kp = Keypair::generate_ed25519();
        let name = kp.public().to_peer_id().to_string();
        let cid = verify_b64(&name, &b64(&signed(&kp, CID, 3600))).unwrap();
        assert_eq!(cid.to_string(), CID);
    }

    #[test]
    fn verify_b64_rejects_expired_cache_entry() {
        // An expired cached record must fail verify_b64 so the caller falls through to
        // the tracker rather than serving a stale name.
        let kp = Keypair::generate_ed25519();
        let name = kp.public().to_peer_id().to_string();
        assert!(verify_b64(&name, &b64(&signed(&kp, CID, -10))).is_err());
    }

    // CROSS-STACK GATE: a record signed by the Go node (tsnode, boxo `ipns.NewRecord`) must
    // verify under this Rust `verify_b64` — the load-bearing interop of the Go-everywhere
    // rewrite. The fixture is produced by `node`'s TestDumpRustFixture (deterministic 0x42
    // seed, value `/ipfs/<fixtureCID>`, EOL 2125 so it never expires). Regenerate with
    // `TS_DUMP_FIXTURE=1 go -C node test -run TestDumpRustFixture -v` if the format changes.
    #[test]
    fn verifies_a_record_signed_by_the_go_node() {
        const GO_NAME: &str = "12D3KooWC4T1AXU2s2YBgGJ2FeaYVtsKoHZWJeubnWe9SnuSE7Zb";
        const GO_RECORD_B64: &str = "CkEvaXBmcy9iYWZrcmVpZ2gyYWtpc2NhaWxkY3FhYnN5ZzNkZnI2Y2h1M2ZncHJlZ2l5bXNjazdlN2FxYTRzNTJ6eRJACHH9JpgtFOgtn5mbBFsCtOQHhDp5lMg5SW1qJCyTbDl8/XLcEnKvGhqk9JWjkb5YDqwwhX6AY7Ir9PAZN/yoBRgAIhQyMTI1LTAxLTAxVDAwOjAwOjAwWigBMIDwksvdCEJAnQAyRdz8Du1E/D49Thql3BeHc/wCGAUvLFS08wYojNG+/mOmn1g/OxLA7xZEd/zt5Osg4xhtMqhKAI8/kr/PDUqNAaVjVFRMGwAAAEXZZLgAZVZhbHVlWEEvaXBmcy9iYWZrcmVpZ2gyYWtpc2NhaWxkY3FhYnN5ZzNkZnI2Y2h1M2ZncHJlZ2l5bXNjazdlN2FxYTRzNTJ6eWhTZXF1ZW5jZQFoVmFsaWRpdHlUMjEyNS0wMS0wMVQwMDowMDowMFpsVmFsaWRpdHlUeXBlAA==";
        const FIXTURE_CID: &str = "bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy";
        let cid = verify_b64(GO_NAME, GO_RECORD_B64)
            .expect("Go-signed IPNS record must verify under the Rust path");
        assert_eq!(cid.to_string(), FIXTURE_CID);
    }
}
