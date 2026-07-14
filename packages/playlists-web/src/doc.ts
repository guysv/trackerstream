// The playlist document — the thing that actually travels the mesh.
//
// A port of the document half of apps/desktop/src-tauri/src/playlists.rs. The wire shape is the
// contract between the desktop and this client, so the constants and the validation rules are
// copied, not reinterpreted.
import { sha256 } from "multiformats/hashes/sha2";
import { CID } from "multiformats/cid";

export const DOC_VERSION = 2; // v2 = md5-keyed tracks (v1 was rowid-keyed, and is gone)
export const DOC_MAX = 1 << 20;
export const TRACKS_MAX = 20_000;
export const TITLE_MAX = 300;
export const FIELD_MAX = 512;
export const MD5_MAX = 64;
/** IPNS record lifetime. Records are re-signed at seq+1 when they come within RENEW_MARGIN of it. */
export const LIFETIME_MS = 168 * 3600 * 1000;
export const RENEW_MARGIN_SECS = 24 * 3600;

/** `[md5, module name, song title]` — denormalized so a playlist renders with zero catalog lookups.
 *  Keyed by the module's CONTENT md5, never the catalog rowid: the rowid is reassigned on every full
 *  re-ingest, and playlists keyed by it broke on every rebake. */
export type TrackRef = [md5: string, modName: string, title: string];

/** `{"v":2,"t":"title","ts":[[md5,mod,title],...]}`. A tombstone is `{"v":2,"del":true}`. */
export interface PlaylistDoc {
  v: number;
  t?: string;
  del?: boolean;
  ts?: TrackRef[];
}

/**
 * Parse + schema-validate UNTRUSTED doc bytes.
 *
 * FORWARD-TOLERANT, deliberately: a NEWER doc (v > DOC_VERSION) is ACCEPTED and read through the
 * fields we understand. Unknown fields are ignored, and the caller stores the ORIGINAL bytes — so a
 * future additive field survives storage and re-announce untouched. Only a genuinely older, dropped
 * schema (v < DOC_VERSION) is rejected.
 *
 * This is what stops a reject-and-poison flag-day: an old client must DEGRADE past a v:3 doc, never
 * permanently cache it as rejected. Only bump `v` for a truly INCOMPATIBLE change.
 */
export function validateDoc(bytes: Uint8Array): PlaylistDoc {
  if (bytes.length === 0 || bytes.length > DOC_MAX) {
    throw new Error(`doc size ${bytes.length} out of bounds`);
  }
  const doc = JSON.parse(new TextDecoder().decode(bytes)) as PlaylistDoc;
  if (typeof doc.v !== "number" || doc.v < DOC_VERSION) {
    throw new Error(`obsolete doc version ${doc.v}`);
  }
  if (doc.del) return doc; // a tombstone carries nothing else
  if ((doc.t ?? "").length > TITLE_MAX) throw new Error("title too long");
  const ts = doc.ts ?? [];
  if (ts.length > TRACKS_MAX) throw new Error(`too many tracks (${ts.length})`);
  for (const t of ts) {
    if (!t[0] || t[0].length > MD5_MAX) throw new Error("track md5 key out of bounds");
    if ((t[1] ?? "").length > FIELD_MAX || (t[2] ?? "").length > FIELD_MAX) {
      throw new Error("track field too long");
    }
  }
  return doc;
}

/** The doc's integrity anchor: raw codec (0x55) + sha2-256 — exactly what the Go node signs into
 *  the IPNS record's value. Anything else fails closed. */
export async function docCid(doc: Uint8Array): Promise<CID> {
  return CID.createV1(0x55, await sha256.digest(doc));
}

/** Serialize a doc for the wire. The bytes we emit here are the bytes we hash and the bytes we
 *  store — so there is no canonical-JSON problem: field order is ours to choose, as long as we
 *  never re-serialize a doc that came from someone else. */
export function encodeDoc(doc: PlaylistDoc): Uint8Array {
  const out: Record<string, unknown> = { v: doc.v };
  if (doc.del) out.del = true;
  else {
    if (doc.t) out.t = doc.t;
    if (doc.ts?.length) out.ts = doc.ts;
  }
  return new TextEncoder().encode(JSON.stringify(out));
}
