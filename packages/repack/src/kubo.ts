// Minimal kubo RPC client (the data-plane node implementation, driven over the
// kubo HTTP RPC API). The master node and — via a Tauri sidecar — the desktop
// client both run kubo; this is how trackerstream puts/gets/pins blocks and
// manages the swarm. We compute CIDs ourselves (multiformats) and block-put with
// matching codec + sha2-256 so kubo stores them under the exact same CID,
// keeping the DAG self-verifying end to end.

import { readFile } from "node:fs/promises";
import { basename } from "node:path";
import { CID } from "multiformats/cid";
import type { Block, BlockGetter } from "./dag.ts";

const CODEC_NAME: Record<number, string> = { 0x55: "raw", 0x71: "dag-cbor" };

export class KuboRpc {
  private base: string;
  constructor(base: string) {
    this.base = base;
  }

  private async post(path: string, body?: BodyInit): Promise<Response> {
    const res = await fetch(`${this.base}/api/v0/${path}`, { method: "POST", body });
    if (!res.ok) throw new Error(`kubo ${path} -> ${res.status} ${await res.text()}`);
    return res;
  }

  async id(): Promise<{ ID: string; Addresses: string[] }> {
    return (await this.post("id")).json();
  }

  /** block put with an explicit codec + sha2-256 so the CID matches ours. */
  async blockPut(bytes: Uint8Array, codec: number): Promise<CID> {
    const name = CODEC_NAME[codec];
    if (!name) throw new Error(`unsupported codec 0x${codec.toString(16)}`);
    const form = new FormData();
    form.append("data", new Blob([bytes as BlobPart]), "block");
    const res = await this.post(
      `block/put?cid-codec=${name}&mhtype=sha2-256&mhlen=32&pin=false`,
      form,
    );
    const { Key } = (await res.json()) as { Key: string };
    return CID.parse(Key);
  }

  /** Batched block put — one HTTP round-trip AND one leveldb fsync (server-side
   *  PutMany) for the whole slice, vs one of each per block. Body is a raw framed
   *  stream of [uint32-BE codec][uint32-BE len][bytes] entries. Returns CIDs in order. */
  async blockPutMany(entries: { bytes: Uint8Array; codec: number }[]): Promise<CID[]> {
    let total = 0;
    for (const e of entries) total += 8 + e.bytes.length;
    const buf = new Uint8Array(total);
    const dv = new DataView(buf.buffer);
    let off = 0;
    for (const e of entries) {
      dv.setUint32(off, e.codec);
      dv.setUint32(off + 4, e.bytes.length);
      buf.set(e.bytes, off + 8);
      off += 8 + e.bytes.length;
    }
    const res = await this.post("block/put-many", buf as BodyInit);
    const { Keys } = (await res.json()) as { Keys: string[] };
    return Keys.map((k) => CID.parse(k));
  }

  async blockGet(cid: CID): Promise<Uint8Array> {
    const res = await this.post(`block/get?arg=${cid.toString()}`);
    return new Uint8Array(await res.arrayBuffer());
  }

  /** Add a whole file as a UnixFS DAG (the catalog publish path, R1). For stable
   *  cross-rebake block reuse the catalog is added page-aligned + raw-leaves so each
   *  SQLite page = one fixed block (lab: 92–96% reuse on an incremental rebake).
   *  Returns the root CID; pins it on the master by default (master-as-seeder). */
  async addFile(
    path: string,
    opts: { chunker?: string; rawLeaves?: boolean; cidVersion?: number; pin?: boolean } = {},
  ): Promise<CID> {
    const chunker = opts.chunker ?? "size-262144";
    const rawLeaves = opts.rawLeaves ?? false;
    const cidVersion = opts.cidVersion ?? 0;
    const pin = opts.pin ?? true;
    const bytes = await readFile(path);
    const form = new FormData();
    form.append("file", new Blob([bytes as BlobPart]), basename(path));
    const res = await this.post(
      `add?chunker=${chunker}&raw-leaves=${rawLeaves}&cid-version=${cidVersion}&pin=${pin}`,
      form,
    );
    // kubo streams newline-delimited JSON; the final object carries the root Hash.
    const last = (await res.text()).trim().split("\n").filter(Boolean).pop();
    if (!last) throw new Error("kubo add: empty response");
    const { Hash } = JSON.parse(last) as { Hash: string };
    return CID.parse(Hash);
  }

  /** Ensure a named ed25519 IPNS key exists; returns its PeerId in base58 (`12D3…`)
   *  form — the form the desktop client parses (`PeerId::from_str`) and verifies
   *  records against. Idempotent: an existing key is looked up, not recreated. */
  async keyGen(name: string, type = "ed25519"): Promise<string> {
    try {
      const res = await this.post(`key/gen?arg=${encodeURIComponent(name)}&type=${type}&ipns-base=b58mh`);
      const { Id } = (await res.json()) as { Id: string };
      return Id;
    } catch (e) {
      if (String(e).includes("already exists")) return this.keyId(name);
      throw e;
    }
  }

  /** PeerId (base58) of an existing named key. */
  async keyId(name: string): Promise<string> {
    const { Keys } = (await (await this.post("key/list?ipns-base=b58mh")).json()) as {
      Keys: { Name: string; Id: string }[] | null;
    };
    const k = (Keys ?? []).find((x) => x.Name === name);
    if (!k) throw new Error(`kubo key/list: no key named ${name}`);
    return k.Id;
  }

  /** Sign + publish an IPNS record (bumps the sequence) pointing `key` at `cid`.
   *  `lifetime` is the record validity window the client enforces as the EOL. */
  async namePublish(cid: CID | string, opts: { key?: string; lifetime?: string } = {}): Promise<void> {
    const key = opts.key ?? "self";
    const lifetime = opts.lifetime ?? "48h";
    await this.post(
      `name/publish?arg=/ipfs/${cid.toString()}&key=${encodeURIComponent(key)}&lifetime=${lifetime}&allow-offline=true`,
    );
  }

  /** Fetch the latest SIGNED IPNS record protobuf for a name, base64 (standard) —
   *  exactly the form the tracker's IpnsStore holds and the client's `verify_b64`
   *  decodes. kubo returns the value as base64 in a Type-5 (Value) routing event.
   *  NOTE: kubo gates routing/get to ONLINE mode (it 500s on an --offline daemon);
   *  the master daemon is always online so this returns its own record from local. */
  async routingGet(name: string): Promise<string> {
    const arg = name.startsWith("/ipns/") ? name : `/ipns/${name}`;
    const res = await this.post(`routing/get?arg=${encodeURIComponent(arg)}`);
    for (const line of (await res.text()).trim().split("\n")) {
      if (!line.trim()) continue;
      const msg = JSON.parse(line) as { Type?: number; Extra?: string };
      if (msg.Type === 5 && msg.Extra) return msg.Extra; // Type 5 = Value; Extra = base64(record)
    }
    throw new Error(`kubo routing/get ${arg}: no Value in response`);
  }

  async pinAdd(cid: CID, recursive = true): Promise<void> {
    await this.post(`pin/add?arg=${cid.toString()}&recursive=${recursive}`);
  }

  /** Remove a recursive pin (used to drop a superseded root after a re-bake;
   *  shared leaf blocks stay pinned under the new root). Tolerates "not pinned". */
  async pinRm(cid: CID | string, recursive = true): Promise<void> {
    try {
      await this.post(`pin/rm?arg=${cid.toString()}&recursive=${recursive}`);
    } catch (e) {
      if (!String(e).includes("not pinned")) throw e; // already gone -> fine
    }
  }

  async swarmConnect(addr: string): Promise<void> {
    await this.post(`swarm/connect?arg=${encodeURIComponent(addr)}`);
  }

  async swarmPeers(): Promise<string[]> {
    const { Peers } = (await (await this.post("swarm/peers")).json()) as {
      Peers: { Addr: string; Peer: string }[] | null;
    };
    return (Peers ?? []).map((p) => `${p.Addr}/p2p/${p.Peer}`);
  }

  /** A BlockGetter backed by this node (fetches via Bitswap when not local). */
  getter(): BlockGetter {
    return (cid) => this.blockGet(cid);
  }
}

/**
 * Put every block of a built DAG into the node and recursively pin the root.
 *
 * All of a module's blocks go up in ONE batched `block/put-many` request, which
 * the node commits in a single leveldb batch — one HTTP round-trip and one fsync
 * for the whole DAG, versus one of each per block. A big module is hundreds of
 * leaf blocks; the per-block path made each a separate RPC + fsync, the dominant
 * bulk-ingest floor (worst on the master's high-latency network volume). We still
 * `pin=false` on the puts then a single recursive pin of the root, keeping the DAG
 * self-verifying. (Ops half of the win: Provide.Strategy=roots on the master so a
 * per-block DHT provide doesn't dominate — clients Bitswap-fetch all blocks from
 * the always-on master they bootstrap to, so only roots need provider records.)
 */
export async function loadDagToKubo(
  rpc: KuboRpc,
  blocks: Block[],
  root: CID,
  concurrency = 16,
): Promise<{ put: number; mismatched: string[] }> {
  const mismatched: string[] = [];
  // One batched put per (sub-)DAG: a single HTTP round-trip and a single leveldb
  // fsync for the whole slice. Cap the sub-batch so a pathological DAG (max ~424
  // blocks observed) can't build an unbounded body; `concurrency` reused as the cap.
  const cap = Math.max(1, concurrency * 64);
  for (let i = 0; i < blocks.length; i += cap) {
    const slice = blocks.slice(i, i + cap);
    const got = await rpc.blockPutMany(slice.map((b) => ({ bytes: b.bytes, codec: b.cid.code })));
    for (let j = 0; j < slice.length; j++) {
      if (got[j]?.toString() !== slice[j].cid.toString())
        mismatched.push(`${slice[j].cid} != ${got[j]}`);
    }
  }
  await rpc.pinAdd(root, true);
  return { put: blocks.length, mismatched };
}
