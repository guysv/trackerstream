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

/** Run `fn` over `items` with a bounded concurrency pool (order-independent). */
async function mapPool<T>(items: T[], concurrency: number, fn: (t: T) => Promise<void>): Promise<void> {
  let i = 0;
  const worker = async () => {
    while (i < items.length) await fn(items[i++]);
  };
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, worker));
}

/**
 * Put every block of a built DAG into kubo and recursively pin the root.
 *
 * Block puts run CONCURRENTLY (bounded pool) — the previous serial loop was a
 * major ingest bottleneck (MVP-FOLLOWUP A2): a big module is hundreds of leaf
 * blocks, each a separate kubo RPC round-trip. `pin=false` on each put then a
 * single recursive pin of the root keeps the DAG self-verifying. (The other half
 * of the A2 fix is ops: Provide.Strategy=roots on the master so a per-block DHT
 * provide doesn't dominate — clients Bitswap-fetch all blocks from the always-on
 * master they bootstrap to, so only roots need provider records.)
 */
export async function loadDagToKubo(
  rpc: KuboRpc,
  blocks: Block[],
  root: CID,
  concurrency = 16,
): Promise<{ put: number; mismatched: string[] }> {
  const mismatched: string[] = [];
  await mapPool(blocks, concurrency, async (b) => {
    const got = await rpc.blockPut(b.bytes, b.cid.code);
    if (got.toString() !== b.cid.toString()) mismatched.push(`${b.cid} != ${got}`);
  });
  await rpc.pinAdd(root, true);
  return { put: blocks.length, mismatched };
}
