// Minimal kubo RPC client (the data-plane node implementation, driven over the
// kubo HTTP RPC API). The master node and — via a Tauri sidecar — the desktop
// client both run kubo; this is how trackerstream puts/gets/pins blocks and
// manages the swarm. We compute CIDs ourselves (multiformats) and block-put with
// matching codec + sha2-256 so kubo stores them under the exact same CID,
// keeping the DAG self-verifying end to end.

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

  async pinAdd(cid: CID, recursive = true): Promise<void> {
    await this.post(`pin/add?arg=${cid.toString()}&recursive=${recursive}`);
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

/** Put every block of a built DAG into kubo and recursively pin the root. */
export async function loadDagToKubo(
  rpc: KuboRpc,
  blocks: Block[],
  root: CID,
): Promise<{ put: number; mismatched: string[] }> {
  const mismatched: string[] = [];
  for (const b of blocks) {
    const got = await rpc.blockPut(b.bytes, b.cid.code);
    if (got.toString() !== b.cid.toString()) mismatched.push(`${b.cid} != ${got}`);
  }
  await rpc.pinAdd(root, true);
  return { put: blocks.length, mismatched };
}
