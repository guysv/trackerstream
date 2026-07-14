// Byte-exact original rebuild, version-dispatched — the browser half of "Download".
//
// The manifest version decides the path (v1/flat, v3, v4). An unknown-NEWER version must fail
// cleanly rather than be mis-parsed as v1: a re-baked corpus must never make an old client emit
// corrupt bytes and call them the original.
import { reassemble, reassembleV3, reassembleV4 } from "@trackerstream/repack/dag";
import * as dagCbor from "@ipld/dag-cbor";
import type { CID } from "multiformats/cid";

type Getter = (cid: CID) => Promise<Uint8Array>;

export async function reassembleAny(root: CID, get: Getter): Promise<Uint8Array> {
  const v = Number(dagCbor.decode<{ v?: number }>(await get(root)).v ?? 1);
  if (v <= 1) return (await reassemble(root, get, { verify: true })).bytes;
  if (v === 3) return (await reassembleV3(root, get, { verify: true })).bytes;
  if (v === 4) return (await reassembleV4(root, get, { verify: true })).bytes;
  throw new Error(`rebuild: manifest v${v} is newer than this client understands`);
}
