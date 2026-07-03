//! Playlist deep links (PLAYLISTS.md §10, shipped): the URL fragment carries the SAME
//! self-certifying `{name, record, doc}` envelope as gossip — uvarint-length-prefixed
//! name ++ record ++ doc, byte-compatible with the Go node's `encodePlaylistMsg` — so a
//! link verifies through the exact `ingest_wire` path a gossiped playlist does. The
//! fragment never reaches the web server (fragments aren't sent in HTTP requests), and
//! the web tier stores nothing: the link IS a third transport (gossip = push,
//! playlist-list = pull, link = out-of-band), all with identical trust.

use anyhow::{anyhow, bail, Result};
use base64::engine::general_purpose::{STANDARD as B64, URL_SAFE_NO_PAD as B64URL};
use base64::Engine;

/// Envelope caps — mirror the Go decoder (`node/playlist.go` readFrame bounds).
const NAME_MAX: usize = 128;
const RECORD_MAX: usize = 10 << 10; // boxo ipns.MaxRecordSize
const DOC_MAX: usize = 1 << 20;

/// Links longer than this fall back to name-only (no fragment): chat apps and browsers
/// start truncating URLs in the low tens of KB, and a truncated payload is worse than a
/// gossip-resolved one. ~8000 chars ≈ a 100+ track playlist.
pub const URL_CHAR_MAX: usize = 8000;

pub const WEB_BASE: &str = "https://trackerstream.xyz/p/";
pub const SCHEME_BASE: &str = "trackerstream://playlist/";

fn put_uvarint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

fn read_uvarint(b: &[u8]) -> Option<(u64, usize)> {
    let mut v: u64 = 0;
    for (i, &byte) in b.iter().enumerate().take(10) {
        v |= u64::from(byte & 0x7f) << (7 * i);
        if byte & 0x80 == 0 {
            return Some((v, i + 1));
        }
    }
    None
}

fn read_frame(b: &[u8], max: usize) -> Result<(&[u8], &[u8])> {
    let (len, n) = read_uvarint(b).ok_or_else(|| anyhow!("malformed envelope frame"))?;
    let len = usize::try_from(len).map_err(|_| anyhow!("malformed envelope frame"))?;
    if len > max || b.len() - n < len {
        bail!("envelope frame out of bounds ({len} > {max})");
    }
    Ok((&b[n..n + len], &b[n + len..]))
}

/// Frame `{name, record, doc}` exactly like the Go node's `encodePlaylistMsg`.
pub fn encode_envelope(name: &str, record: &[u8], doc: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(name.len() + record.len() + doc.len() + 15);
    for part in [name.as_bytes(), record, doc] {
        put_uvarint(&mut buf, part.len() as u64);
        buf.extend_from_slice(part);
    }
    buf
}

/// Decode + bounds-check an envelope. Rejects trailing bytes (like the Go decoder).
pub fn decode_envelope(b: &[u8]) -> Result<(String, Vec<u8>, Vec<u8>)> {
    let (name, rest) = read_frame(b, NAME_MAX)?;
    let (record, rest) = read_frame(rest, RECORD_MAX)?;
    let (doc, rest) = read_frame(rest, DOC_MAX)?;
    if !rest.is_empty() {
        bail!("trailing bytes in envelope");
    }
    let name = std::str::from_utf8(name).map_err(|_| anyhow!("envelope name not utf-8"))?;
    Ok((name.to_string(), record.to_vec(), doc.to_vec()))
}

/// Plausible playlist name (a base58 PeerId): the only thing a hostile link can put in
/// the URL path, so shape-check it before it goes anywhere near the DB or a request.
pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= NAME_MAX
        && name.bytes().all(|b| b.is_ascii_alphanumeric())
}

/// Build the shareable HTTPS link for a playlist from its stored row. `record_b64` is
/// the DB's base64 signed record; `doc_json` the raw doc. Oversized payloads degrade to
/// a name-only link (resolved via gossip + a targeted `want` on the other end).
pub fn build_link(name: &str, record_b64: &str, doc_json: &str) -> Result<String> {
    let record = B64
        .decode(record_b64.trim())
        .map_err(|e| anyhow!("record not base64: {e}"))?;
    let frag = B64URL.encode(encode_envelope(name, &record, doc_json.as_bytes()));
    let url = format!("{WEB_BASE}{name}#{frag}");
    if url.len() > URL_CHAR_MAX {
        return Ok(format!("{WEB_BASE}{name}"));
    }
    Ok(url)
}

/// Parse an incoming link — either form:
///   trackerstream://playlist/<name>[#frag]
///   https://trackerstream.xyz/p/<name>[#frag]
/// Returns the name and the decoded envelope bytes (None for a name-only link).
pub fn parse_link(url: &str) -> Result<(String, Option<Vec<u8>>)> {
    let url = url.trim();
    let rest = url
        .strip_prefix(SCHEME_BASE)
        .or_else(|| url.strip_prefix(WEB_BASE))
        .ok_or_else(|| anyhow!("not a playlist link: {url}"))?;
    let (name, frag) = match rest.split_once('#') {
        Some((n, f)) => (n, Some(f)),
        None => (rest, None),
    };
    let name = name.trim_end_matches('/');
    if !valid_name(name) {
        bail!("bad playlist name in link");
    }
    let envelope = match frag.filter(|f| !f.is_empty()) {
        Some(f) => Some(
            B64URL
                .decode(f)
                .map_err(|e| anyhow!("link fragment not base64url: {e}"))?,
        ),
        None => None,
    };
    Ok((name.to_string(), envelope))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_roundtrip() {
        let enc = encode_envelope("12D3KooWName", b"record-bytes", b"{\"v\":1}");
        let (n, r, d) = decode_envelope(&enc).unwrap();
        assert_eq!((n.as_str(), r.as_slice(), d.as_slice()), ("12D3KooWName", &b"record-bytes"[..], &b"{\"v\":1}"[..]));
    }

    #[test]
    fn envelope_rejects_trailing_and_oversize() {
        let mut enc = encode_envelope("n", b"r", b"d");
        enc.push(0);
        assert!(decode_envelope(&enc).is_err());

        let big = vec![0u8; DOC_MAX + 1];
        let enc = encode_envelope("n", b"r", &big);
        assert!(decode_envelope(&enc).is_err());
    }

    #[test]
    fn link_roundtrip_both_forms() {
        let url = build_link("12D3KooWName", &B64.encode(b"rec"), "{\"v\":1}").unwrap();
        assert!(url.starts_with(WEB_BASE));
        let (name, env) = parse_link(&url).unwrap();
        assert_eq!(name, "12D3KooWName");
        let (n2, r2, d2) = decode_envelope(&env.unwrap()).unwrap();
        assert_eq!((n2.as_str(), r2.as_slice(), d2.as_slice()), ("12D3KooWName", &b"rec"[..], &b"{\"v\":1}"[..]));

        let (name, env) = parse_link("trackerstream://playlist/12D3KooWName").unwrap();
        assert_eq!(name, "12D3KooWName");
        assert!(env.is_none());
    }

    #[test]
    fn oversized_doc_degrades_to_name_only() {
        let fat = format!("{{\"v\":1,\"t\":\"{}\"}}", "x".repeat(20_000));
        let url = build_link("12D3KooWName", &B64.encode(b"rec"), &fat).unwrap();
        assert_eq!(url, format!("{WEB_BASE}12D3KooWName"));
    }

    #[test]
    fn hostile_names_rejected() {
        for u in [
            "trackerstream://playlist/../../etc",
            "trackerstream://playlist/",
            "https://trackerstream.xyz/p/<script>",
            "https://evil.example/p/12D3KooWName",
        ] {
            assert!(parse_link(u).is_err(), "{u} must be rejected");
        }
    }
}
