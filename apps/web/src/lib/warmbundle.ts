// Catalog "warm bundle": pre-load the catalog's hot FTS pages into the blockstore over plain HTTP,
// so a FRESH profile's first search doesn't pay ~26 serialized Bitswap round-trips over the WAN.
//
// WHY HTTP AND NOT THE MESH: a cold catalog search is dependent-read bound (SQLite pointer-chases the
// FTS b-tree one 16 KB page at a time), and the browser's single webrtc-direct connection to the
// master COLLAPSES under concurrency — so warming over Bitswap in the background contends with the
// real query and makes it worse (measured 8s -> 35s). Fetching the same pages as ONE static HTTP GET
// from the edge sidesteps the mesh connection entirely: no contention, one round-trip, and the blocks
// land in the same persistent blockstore the query reads from.
//
// THE RACE, AND WHY THE MANIFEST LISTS CIDs: the blob load and the user's first search start at nearly
// the same instant. If getBlock just fired cold Bitswap fetches, the search would miss the still-
// downloading bundle entirely. So a catalog page fetch WAITS for the bundle — but ONLY for a CID the
// bundle actually contains (from the manifest's cid list). Pages the bundle doesn't carry (e.g. the
// home page's meta rows) never wait, so nothing else regresses.
//
// SAFETY: every block is content-addressed. We DERIVE each page's CID from its bytes (raw-codec CIDv1
// sha256) and store under that — a tampered or corrupt bundle can only produce blocks under CIDs the
// catalog never references, never a forged page under a real CID. So the bundle is untrusted input.
// The manifest's `root` is only a freshness hint (skip a useless load for a superseded catalog).
import type { Helia } from "helia";
import { CID } from "multiformats/cid";
import { sha256 } from "multiformats/hashes/sha2";

const MAGIC = "TSWARM1\n"; // 8 bytes
const RAW_CODEC = 0x55;

interface WarmManifest {
  root: string; // catalog root CID this bundle was generated for
  url: string; // bundle blob URL (relative to the manifest or absolute)
  cids: string[]; // the page CIDs the bundle carries — lets getBlock wait only for covered pages
}

export interface WarmBundle {
  /** True if `cid` is one of the pages this bundle carries. */
  has(cid: CID): boolean;
  /** Resolves once the blob has been fetched and its pages written to the blockstore (or on failure). */
  ready: Promise<void>;
}

/**
 * Fetch the warm-bundle MANIFEST for `catalogRoot` (small, fast) and kick the blob download in the
 * background. Returns a handle whose `has()` is usable immediately and whose `ready` resolves when the
 * pages are resident — or null if there's no matching bundle (no manifest, wrong root, malformed).
 * Never throws. The manifest await is a tiny JSON GET; the big blob loads off the boot path.
 */
export async function startWarmBundle(helia: Helia, catalogRoot: string, manifestUrl: string): Promise<WarmBundle | null> {
  let man: WarmManifest;
  try {
    const res = await fetch(manifestUrl, { cache: "no-store" });
    if (!res.ok) return null;
    man = (await res.json()) as WarmManifest;
  } catch {
    return null;
  }
  if (!man?.root || !man?.url || !Array.isArray(man.cids)) return null;
  if (man.root !== catalogRoot) return null; // bundle is for a superseded catalog — skip

  const dbg = Boolean((import.meta as unknown as { env?: { DEV?: boolean } }).env?.DEV);
  const set = new Set(man.cids);
  const bundleUrl = new URL(man.url, new URL(manifestUrl, location.href)).href;
  const ready = loadBlob(helia, bundleUrl)
    .then((n) => {
      if (dbg) console.info(`[warmbundle] loaded ${n}/${set.size} catalog pages`);
    })
    .catch(() => {});
  return {
    has: (cid) => set.has(cid.toString()),
    ready,
  };
}

/** Fetch + parse the blob and write every page to the blockstore under its derived (verified) CID.
 *  Returns how many pages were written. */
async function loadBlob(helia: Helia, bundleUrl: string): Promise<number> {
  const res = await fetch(bundleUrl);
  if (!res.ok) return 0;
  const buf = new Uint8Array(await res.arrayBuffer());
  if (buf.length < 12 || new TextDecoder().decode(buf.subarray(0, 8)) !== MAGIC) return 0;
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  let off = 8;
  const count = dv.getUint32(off, true);
  off += 4;
  let n = 0;
  for (let i = 0; i < count && off + 4 <= buf.length; i++) {
    const len = dv.getUint32(off, true);
    off += 4;
    if (off + len > buf.length) break; // truncated — keep what we have
    const bytes = buf.slice(off, off + len); // copy: a subarray view detaches oddly through IDB
    off += len;
    const cid = CID.createV1(RAW_CODEC, await sha256.digest(bytes));
    await helia.blockstore.put(cid, bytes);
    n++;
  }
  return n;
}
