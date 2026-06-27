// IPNS record cache (PEER-ASSIST.md §9 / C1). Serves the master's latest SIGNED
// IPNS record so thin clients (no DHT, no pubsub) can resolve /ipns/<key> ->
// /ipfs/<cid> and verify the signature THEMSELVES. The server is a dumb cache — it
// never validates; the signature on the record is the trust anchor, so an
// untrusted cache is safe. Records are held as base64 (the wire protobuf bytes).
//
// Persisted to a JSON file (write-through on set, loaded on construct) so a record
// published by the ingest pipeline survives an API-server restart — without it a
// box bounce would 404 /ipns/<catalogKey> until the next rebake republished.
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

export class IpnsStore {
  private records = new Map<string, string>(); // ipns name -> base64 signed record
  private path?: string;

  /** @param path optional JSON file to persist records to (omit for in-memory). */
  constructor(path?: string) {
    this.path = path;
    if (path) this.load();
  }

  private load(): void {
    try {
      const obj = JSON.parse(readFileSync(this.path!, "utf8")) as Record<string, string>;
      for (const [k, v] of Object.entries(obj)) this.records.set(k, v);
    } catch {
      /* missing/corrupt file -> start empty (rebuilt on the next publish) */
    }
  }

  private persist(): void {
    if (!this.path) return;
    try {
      mkdirSync(dirname(this.path), { recursive: true });
      writeFileSync(this.path, JSON.stringify(Object.fromEntries(this.records)));
    } catch (e) {
      console.error(`IpnsStore persist (${this.path}) failed: ${e}`);
    }
  }

  get(key: string): string | undefined {
    return this.records.get(key);
  }

  /** Publish/replace a key's record (called by the master's republish hook). */
  set(key: string, recordB64: string): void {
    this.records.set(key, recordB64);
    this.persist();
  }

  count(): number {
    return this.records.size;
  }
}
