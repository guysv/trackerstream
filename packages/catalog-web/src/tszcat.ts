// TSZCAT — the per-page-zstd catalog format the master actually publishes.
//
// The published root is NOT a SQLite file. It is a manifest:
//
//   [magic "TSZCAT1\n" 8B][page_size u32 LE][page_count u32 LE][page_count x 36-byte CIDv1]
//
// ...and every 16KB SQLite page is its OWN raw (0x55) block, zstd-compressed at level 19. That is
// the whole reason the browser catalog is tractable: a page read is a plain Bitswap block fetch by
// CID. No UnixFS reader, no ranged reads, no DAG walking. The manifest itself is the only UnixFS
// file in the path, and it is a cached whole-file read (~650KB for the current corpus).
//
// Written by apps/server/src/ingest.ts (publishZstdCatalog); read by
// apps/desktop/src-tauri/src/catalog.rs (read_zstd/ensure_pages) and by this package.
import { CID } from "multiformats/cid";

const MAGIC = "TSZCAT1\n";
const CID_LEN = 36; // CIDv1 + raw codec + sha2-256

export interface TszcatManifest {
  pageSize: number;
  pageCount: number;
  pages: CID[];
}

/** True if these bytes are a TSZCAT manifest rather than a raw SQLite file. The client detects the
 *  format from the root's magic — which is why a single IPNS name serves both, and why the empty
 *  CATALOG_Z_IPNS_KEY in packages/config does NOT mean zstd is unpublished (that two-key design
 *  was hard-cut; see the comment in ingest.ts). */
export function isTszcat(bytes: Uint8Array): boolean {
  if (bytes.length < 8) return false;
  for (let i = 0; i < 8; i++) if (bytes[i] !== MAGIC.charCodeAt(i)) return false;
  return true;
}

export function parseTszcat(bytes: Uint8Array): TszcatManifest {
  if (!isTszcat(bytes)) {
    const head = new TextDecoder().decode(bytes.subarray(0, 16));
    throw new Error(`not a TSZCAT manifest (root begins ${JSON.stringify(head)})`);
  }
  const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const pageSize = dv.getUint32(8, true);
  const pageCount = dv.getUint32(12, true);
  const need = 16 + pageCount * CID_LEN;
  if (bytes.length < need) {
    throw new Error(`TSZCAT manifest truncated: ${bytes.length}B, need ${need}B for ${pageCount} pages`);
  }
  const pages: CID[] = new Array(pageCount);
  for (let i = 0; i < pageCount; i++) {
    const off = 16 + i * CID_LEN;
    pages[i] = CID.decode(bytes.subarray(off, off + CID_LEN));
  }
  return { pageSize, pageCount, pages };
}
