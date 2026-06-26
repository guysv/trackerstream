// IPNS record cache (PEER-ASSIST.md §9 / C1). Serves the master's latest SIGNED
// IPNS record so thin clients (no DHT, no pubsub) can resolve /ipns/<key> ->
// /ipfs/<cid> and verify the signature THEMSELVES. The server is a dumb cache — it
// never validates; the signature on the record is the trust anchor, so an
// untrusted cache is safe. Records are held as base64 (the wire protobuf bytes).
// Inert until the catalog migrates to IPNS.
export class IpnsStore {
  private records = new Map<string, string>(); // ipns name -> base64 signed record

  get(key: string): string | undefined {
    return this.records.get(key);
  }

  /** Publish/replace a key's record (called by the master's republish hook). */
  set(key: string, recordB64: string): void {
    this.records.set(key, recordB64);
  }

  count(): number {
    return this.records.size;
  }
}
