// Playlist deep links — a TS port of apps/desktop/src-tauri/src/link.rs.
//
// The URL FRAGMENT carries the same self-certifying {name, record, doc} envelope gossip does, so a
// link verifies through the exact same path a gossiped playlist does. The fragment never reaches the
// web server (fragments aren't sent in HTTP requests) and the web tier stores nothing: the link is
// simply a third transport (gossip = push, playlist-list = pull, link = out-of-band), with identical
// trust. Note it frames name/record/doc WITHOUT the gossip envelope's leading version byte.
import { DOC_MAX } from "./doc.ts";

const NAME_MAX = 128;
const RECORD_MAX = 10 << 10;
/** Chat apps and browsers start truncating URLs in the low tens of KB, and a truncated payload is
 *  worse than a gossip-resolved one — so an oversized link degrades to name-only. */
export const URL_CHAR_MAX = 8000;

export const WEB_BASE = "https://trackerstream.xyz/p/";
export const SCHEME_BASE = "trackerstream://playlist/";

const b64url = (b: Uint8Array): string =>
  btoa(String.fromCharCode(...b)).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");

const unb64url = (s: string): Uint8Array => {
  const pad = s.replace(/-/g, "+").replace(/_/g, "/");
  return Uint8Array.from(atob(pad + "=".repeat((4 - (pad.length % 4)) % 4)), (c) => c.charCodeAt(0));
};

function putUvarint(out: number[], v: number): void {
  while (v >= 0x80) {
    out.push((v & 0x7f) | 0x80);
    v >>>= 7;
  }
  out.push(v);
}

export function encodeEnvelope(name: string, record: Uint8Array, doc: Uint8Array): Uint8Array {
  const out: number[] = [];
  for (const part of [new TextEncoder().encode(name), record, doc]) {
    putUvarint(out, part.length);
    out.push(...part);
  }
  return new Uint8Array(out);
}

export function decodeEnvelope(b: Uint8Array): { name: string; record: Uint8Array; doc: Uint8Array } {
  let at = 0;
  const frame = (max: number): Uint8Array => {
    let v = 0;
    let s = 0;
    for (;;) {
      if (at >= b.length) throw new Error("malformed envelope frame");
      const c = b[at++];
      v |= (c & 0x7f) << s;
      if (c < 0x80) break;
      s += 7;
    }
    if (v > max || b.length - at < v) throw new Error(`envelope frame out of bounds (${v} > ${max})`);
    const part = b.subarray(at, at + v);
    at += v;
    return part;
  };
  const name = new TextDecoder().decode(frame(NAME_MAX));
  const record = frame(RECORD_MAX);
  const doc = frame(DOC_MAX);
  if (at !== b.length) throw new Error("trailing bytes in envelope");
  return { name, record, doc };
}

/** A plausible playlist name (a base58 PeerId). The only thing a hostile link can put in the URL
 *  path, so shape-check it before it goes anywhere near the DB. */
export const validName = (n: string): boolean =>
  n.length > 0 && n.length <= NAME_MAX && /^[A-Za-z0-9]+$/.test(n);

/** Shareable HTTPS link. Oversized payloads degrade to a name-only link, which the other end
 *  resolves via gossip + a targeted want. */
export function buildLink(name: string, recordB64: string, docJson: string): string {
  const record = Uint8Array.from(atob(recordB64.trim()), (c) => c.charCodeAt(0));
  const frag = b64url(encodeEnvelope(name, record, new TextEncoder().encode(docJson)));
  const url = `${WEB_BASE}${name}#${frag}`;
  return url.length > URL_CHAR_MAX ? `${WEB_BASE}${name}` : url;
}

/** Parse `trackerstream://playlist/<name>[#frag]` or `https://trackerstream.xyz/p/<name>[#frag]`.
 *  A name-only link (no fragment) is valid — it pends until gossip delivers the doc. */
export function parseLink(url: string): { name: string; envelope: Uint8Array | null } {
  const u = url.trim();
  const rest = u.startsWith(SCHEME_BASE)
    ? u.slice(SCHEME_BASE.length)
    : u.startsWith(WEB_BASE)
      ? u.slice(WEB_BASE.length)
      : null;
  if (rest === null) throw new Error(`not a playlist link: ${url}`);
  const hash = rest.indexOf("#");
  const rawName = (hash === -1 ? rest : rest.slice(0, hash)).replace(/\/+$/, "");
  const frag = hash === -1 ? "" : rest.slice(hash + 1);
  if (!validName(rawName)) throw new Error("bad playlist name in link");
  return { name: rawName, envelope: frag ? unb64url(frag) : null };
}
