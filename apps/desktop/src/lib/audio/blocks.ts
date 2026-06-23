// Fixed-size content-block round-trip — a stand-in for the P2P fetch path used
// to confirm in the app shell that a module reassembled from blocks (not the
// original file) plays bit-identically (Phase 0 exit criterion). Phase 1+ swaps
// this for real CID block fetch + verify; the reassembly contract is the same:
// concatenate the blocks back in order to recover the exact module bytes.

export interface Block {
  index: number;
  bytes: Uint8Array;
}

/** Split a module buffer into fixed-size blocks (last block may be short). */
export function splitIntoBlocks(data: Uint8Array, blockSize: number): Block[] {
  const blocks: Block[] = [];
  for (let off = 0, i = 0; off < data.length; off += blockSize, i++) {
    blocks.push({ index: i, bytes: data.subarray(off, Math.min(off + blockSize, data.length)) });
  }
  return blocks;
}

/** Reassemble blocks (any arrival order) back into the exact original buffer. */
export function reassembleFromBlocks(data: Uint8Array, blockSize: number): ArrayBuffer {
  const blocks = splitIntoBlocks(data, blockSize);
  // Simulate out-of-order arrival, then reorder by index — the reassembly must
  // not depend on fetch order.
  const shuffled = [...blocks].reverse();
  const out = new Uint8Array(data.length);
  for (const b of shuffled) out.set(b.bytes, b.index * blockSize);
  return out.buffer;
}
