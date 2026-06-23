# @trackerstream/repack

Module ↔ **content-addressed CID-DAG** repack: the data-plane core. Productizes the
lab tooling ([`lab/CID.md`](../../lab/CID.md)) into the pipeline that the server ingest
(Phase 2) and the client fetch path (Phase 3) share. Pure TypeScript, run natively by
Node ≥ 23 (no build step).

## The DAG (`src/dag.ts`)

```
manifest (dag-cbor, root, ~1–4 KB)
  format, originalLength, cdc params
  skeletonChunks: [CID]   — CDC chunks of everything that is NOT sample PCM
                            (headers/orders/patterns); module-unique
  samples: [{ offset, length, pcmRoot: CID }]
pcm-root (dag-cbor, per sample): { chunks: [CID], length }
chunk (raw leaf): sample-PCM or skeleton bytes  ← SHARED across modules (dedup)
```

- **Sample PCM** is FastCDC-chunked (`src/cdc.ts`); identical chunks share a CID and
  are stored/served once (measured ~41% sub-sample dedup corpus-wide).
- The **skeleton** is the carved-out non-sample remainder — 5.7 % of a big IT, 12–19 %
  of small modules (matches the labs). It's module-unique and also chunked.
- **Reassembly rebuilds the EXACT original bytes**, so playback is bit-identical
  (Phase 0 proved byte-exact ⇒ bit-identical render).
- **Self-verifying:** every fetched block is re-hashed against its CID
  (`reassemble({verify:true})`); a tampered peer-served block is rejected.

CIDs are computed with `multiformats` (CIDv1, sha2-256, raw `0x55` / dag-cbor `0x71`)
so a `kubo block put` with the matching codec stores them under the identical CID.

## Parsers (`src/parse.ts`)

MOD / IT / S3M / XM sample-region locators (ported from `lab/cid-dedup.mjs`), returning
byte regions so the builder can carve skeleton vs sample PCM. **Static, never guessed** —
unparseable files return `null`.

## kubo transport (`src/kubo.ts`)

Minimal kubo RPC client (block put/get, pin, swarm). `loadDagToKubo` puts every block +
recursively pins the root; `KuboRpc.getter()` is a `BlockGetter` that fetches via Bitswap
(local cache when warm, peer otherwise).

## Tests

```
pnpm test            # in-memory build -> reassemble -> byte-exact + tamper detection
bash test/swarm.sh   # TWO-node kubo Bitswap swarm: B fetches a DAG from A, verified
```

### Phase 1 swarm result (localhost, `beyond_the_network.it`, 4.4 MB, 311 blocks)

| fetch | path | time | bytes |
|---|---|---|---|
| **cold** | B ← A over Bitswap, concurrent prefetch, every block CID-verified | **1.4 s** | byte-exact |
| **warm** | B local cache | 31 ms | byte-exact |

100 % of the module sourced from CID blocks over libp2p — no HTTP file fetch. (Serial
per-block fetch was 12 s; concurrent prefetch — the shape of the Phase 3 fetch plan — is
~8.6× faster.) Localhost proves the *mechanism*; real cold-vs-peer network numbers need
the droplet + a second machine (Phase 1 cross-NAT task).
