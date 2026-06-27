//! IPNS resolution for the thin client (PEER-ASSIST.md §9 / C2). The node runs no
//! DHT and no pubsub, so it can't resolve `/ipns/<name>` the usual way. Instead it
//! fetches the latest SIGNED record from our tracker server (`GET /ipns/<name>`)
//! and verifies the signature LOCALLY against the name's public key — the server
//! is an untrusted cache, the signature is the trust anchor, so no peer (or the
//! server) can forge a newer record. This is the naming half of master-independent
//! catalog recovery; it stays inert until the catalog is published as IPNS.

use anyhow::{anyhow, Result};
use base64::Engine;
use cid::Cid;
use rust_ipfs::PeerId;
use serde::Deserialize;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Deserialize)]
struct IpnsResponse {
    record: String, // base64 of the signed IPNS record protobuf
}

/// Fetch the latest signed record for `name` from the tracker, verify it (signature +
/// EOL), and return BOTH the raw base64 record (so the caller can cache it) and the CID
/// it points at. `name` is the publisher PeerId (the form our master publishes under).
/// Errors on a missing/forged/expired record so callers can fall back to the master.
pub(crate) async fn fetch_record(name: &str) -> Result<(String, Cid)> {
    let url = format!("{}/ipns/{name}", crate::tracker::api_base());
    let resp: IpnsResponse = reqwest::Client::new()
        .get(&url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let b64 = resp.record.trim().to_string();
    let cid = verify_b64(name, &b64)?;
    Ok((b64, cid))
}

/// Decode a base64-encoded signed record and verify it (signature + EOL), returning the
/// CID it points at. Shared by `fetch_record` (fresh from the tracker) and the on-disk
/// IPNS cache (lib.rs IpnsCache) — a cached entry is re-verified on every read, so a
/// stale/expired/forged cache entry fails here and the caller falls through to the
/// tracker. No network — pure decode + verify.
pub(crate) fn verify_b64(name: &str, b64: &str) -> Result<Cid> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b64.trim())
        .map_err(|e| anyhow!("ipns record not base64: {e}"))?;
    verify_record(name, &bytes)
}

/// The trust core, factored out for testing: decode a signed IPNS record, verify
/// its signature against `name`'s public key, reject it if expired, and return the
/// CID it points at. Pure + synchronous — no network. The value is stored as the
/// bare root CID string (what the master's publish hook must write).
fn verify_record(name: &str, bytes: &[u8]) -> Result<Cid> {
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
    record.value().map_err(|e| anyhow!("ipns value: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use rust_ipfs::Keypair;

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
}
