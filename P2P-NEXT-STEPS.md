# trackerstream — P2P next steps (roadmap)

**North star.** The single box (now one `tsnode` Go binary — go-libp2p + boxo — that
both seeds and bootstraps) becomes a **seeder + bootstrap, not a dependency.** Resilience comes from the
swarm, not from infrastructure redundancy — because *one box is a feature*
(copyleft, one-command deploy, cheapest possible to run). The target is **zero
perceived downtime**: if the box reboots — or disappears entirely — the swarm
self-heals and keeps serving everything it collectively holds and can reach.

This supersedes nothing in [`PEER-ASSIST.md`](PEER-ASSIST.md); it sequences the
work *after* it. PEER-ASSIST built the tracker + warm-set + IPNS readiness; this
doc is how that hardens into a swarm that outlives its origin.

**The backend was transitional — and now it's one binary we own, run on both ends.** Historically
*three* stacks were in play: the **master kubo** (Go seeder + libp2p bootstrap), the **`apps/server`
Node service** (catalog HTTP API + tracker + IPNS dumb-store — *scaffolding*), and a *third* on the
desktop — an embedded rust-ipfs/connexa node. **R4 collapsed all of it into a single custom Go binary,
`tsnode` (go-libp2p + boxo), run as BOTH the box master (`--role server`) and the Tauri client sidecar
(`--role client`)** — see the rewritten R4 below. kubo is removed (disabled warm-standby for rollback),
rust-ipfs is removed, and the Node service is **fully dissolved**: **R1 ✅** removed the catalog API
(clients query the IPNS-published DB directly), the tracker routes were deleted at cutover, and the
**HTTP API server was retired entirely** — catalog naming + distribution are now pure libp2p (a custom
private DHT + an IPNS-over-gossipsub topic, both native because we own both ends). Membership +
holder-discovery moved from Phase-1 PEX/`Peers` into the custom DHT routing table + presence topic;
what's left for the product track is the **presence / interest table** and the **blind mailbox** (P5),
on the same primitives. End state, now real: the box runs *only* `tsnode` (+ an ingest CLI timer).
Design accordingly — **the control plane is libp2p; do not reintroduce a server dependency.**

Status legend: ▶️ do-now · 🟡 core · 🟢 demand-driven / later · ⚪ deferred.

---

## 0. The honest bound on "zero downtime"

Say it out loud so we don't over-promise. With both central services gone, the
swarm can serve content **iff some reachable live peer holds it.** That decomposes
into three independent capabilities, and a DHT only buys the first:

1. **Discovery** — find *who* holds X (tracker today; PEX/structured discovery when
   the box is down).
2. **Reachability** — actually *connect* to that holder, NAT and all (the master's
   relay today; **peer-provided relay** when it's down). ← the hard one.
3. **Transfer** — bitswap the bytes. Single-hop only: you fetch from a peer you are
   **directly connected to**. There is no multi-hop block routing in IPFS — "the
   swarm collectively has everything" does *not* mean you can reach it. Discovery's
   whole job is to make the holder your direct neighbor.

The irreducible gap: **truly cold content** (held only by the master, never streamed
by anyone) is unfetchable while the box is down. No discovery/relay fixes a block
that exists in only one place. The only lever is **collaborative pinning** (§R3) —
pushing hot/at-risk roots onto clients as replicas so "some live peer holds it"
stays true. So the achievable target is precise:

> **Zero perceived downtime for the catalog + anything the swarm has warmed,
> bounded below by how aggressively clients pin.**

---

## 1. The dependency spine

```
Phase 0  (free SPOF relief)                    ✅ shipped
   │
Phase 1  (request_response protocol) ── the keystone; everything peer-served rides it  ✅ shipped
   │
   ├── Track R (resilience):  R2 reachability ✅ (independent, client-only)
   │                          R1 catalog→IPNS ✅ ─→ R4 Go everywhere: one tsnode binary ✅ (dissolves kubo AND rust-ipfs)
   │                          R3 fan-archive-across-peers (R1-gated, independent of R4)
   │
   └── Track P (product):     P4 identity ─→ P4.5 user-feeds ─→ P5 friends ─→ P6 social
                                   │
                              (Browser tier gated on R2 + P4)
```

The two tracks are **independent** — Phase 1 and Track R operate at the
device/PeerId networking level; identity (P4) sits *above* PeerIds and assumes
nothing from R. They share primitives (signed self-certifying records,
request_response, gossipsub, the warm-set) but not a critical path, so they
parallelize.

---

## Phase 0 — Free wins ✅ SHIPPED

> **Shipped** (commit `525eec4`). All four landed: verified-IPNS cache, reconnect
> last-session peers, jitter+backoff on the loops, and the master relay clamped to
> handshake-only (128 KiB / 30s — deployed + live on prod). The roster cache was later
> absorbed into Phase 1's `AddressBook`.

No new protocols, no behaviour changes. Days of work, immediate SPOF relief.

- **Persist last-known state.** Cache the verified IPNS record locally until its
  EOL (resolution then needs zero network through a short outage; have the master
  publish with a generous validity window, ~24–48h). Reuses `ipns.rs:verify_record`.
- **Reconnect last-session peers on startup.** Persist the last-known peer list
  (the roster the warm loop already pulls) and **re-dial it on every launch** — not
  only during an outage. Tracker-down is just the special case where the cached list
  is the *only* source; even with the tracker up, dialing cached peers immediately
  beats waiting a full roster round-trip for the warm set to reform, so it's a free
  faster-first-byte win on every restart. **Dial bounded + opportunistic** (cap at
  `WARM_CAP`, keepalive but **no `.reconnect()`**, like `warm_connect` today): cached
  addrs go stale fast (dynamic IPs, NAT rebinding), so a fresh roster tick supersedes
  them within ~30s and a stale entry is just a wasted dial — Noise binds the
  connection to the PeerId, so it can never be a *wrong* peer (same posture as the
  tracker addrs). This persistence machinery is the **seed of PEX (Phase 1)**: the
  on-disk peer list is the durable form of the address book PEX maintains and
  gossips — Phase 1 generalizes "last roster" into a live, peer-gossiped book that
  survives even a mid-session box death, and persists *that* across restarts.
- **Jitter + backoff** on the announce loop (`tracker.rs:147`, fixed 30s) and the
  master reconnect (`ipfs.rs:386`, fixed 5s). Fixed intervals = synchronized
  thundering herd against the rcmgr during a correlated outage. Cheap fix.
- **Clamp the master relay to handshake-only.** circuit-relay-v2 reservations carry
  per-reservation data + duration limits. Clamp them so the master can facilitate a
  DCUtR hole-punch but **cannot carry bulk data** — this kills the §5 "relay-bytes
  trap" (offload bytes secretly flowing through the master's relay) *immediately*
  and with zero reachability risk, and exposes any fake-offload loudly.

**Ship gate:** a short box reboot survives for warm content + cached names; the
master stops secretly relaying audio. Nothing structural has changed yet.

---

## Phase 1 — The keystone: a `request_response` protocol ✅ SHIPPED

> **Shipped** — commits `3cf3f5f` (Phase 1) + `e86f236` (stable listen port). New module
> `peer.rs`, protocol `/trackerstream/peer/1.0.0`. **Client-only — no server deploy**: the
> master stays a seeder, it doesn't speak the protocol. Validated by 26 unit tests + a
> 2-node box-down integration test (`peer_to_peer_queries_survive_a_dead_box`): with no
> tracker, **membership, naming, and holder-discovery all answered peer-to-peer.**
>
> **What shipped, and where it diverged from the sketch below:**
> - **No custom `NetworkBehaviour`.** The plan was to drop a `request_response` behaviour
>   into the dummy custom-behaviour slot — but that slot is constrained to
>   `ToSwarm = Infallible`, which a real RR behaviour doesn't satisfy. The vendored
>   rust-ipfs/connexa stack *already* exposes request_response as a built-in
>   (`builder.with_request_response(...)` + `Ipfs::{send_request, send_requests,
>   requests_subscribe, send_response}`), so we enabled THAT and left the dummy slot
>   untouched. The codec is raw bytes, so we CBOR-encode ourselves (`serde_ipld_dagcbor`).
> - **Wire types are `String`, not `Cid`/`PeerId`/`Vec<u8>`** — to match every existing
>   shape (`PeerRef`, `HeldRoots` keys, the base64 `IpnsCache` value). `Resp::Ipns.record`
>   is the base64 string `verify_b64` already takes → threads through the cache with zero
>   re-encoding.
> - **`AddressBook` absorbed Phase 0's `RosterCache`** (one source of truth): the durable
>   PEX store (`address_book.json`), seeded once from the old `roster_cache.json`, fed by
>   roster + PEX + learned holders, direct-first sampling, 256-cap.
> - **All three legs live**: PEX (membership; opportunistic book-dialing fills free warm
>   slots when the tracker is unreachable), `Peers{root}` (served from `HeldRoots`;
>   `warm_root` falls back to `peers_pull`), IPNS peer-pull (`resolve_ipns`: local →
>   tracker → peers, newest-by-sequence). Pulls target only warm ∩ connected peers — never
>   dial a stranger, no G1 crawl.
> - **Stable listen port** (`e86f236`): we persist + reuse the bound TCP port, so the
>   addresses peers persist/gossip about us survive *our* restart. The old ephemeral
>   `tcp/0` made every direct entry stale on relaunch, silently defeating reconnect + PEX.
>   Best-effort — NAT remap / IP change still need a tracker/PEX refresh.
>
> **Residuals (not blockers):**
> - **Doorbell deferred** (the `{name, seq}` push). Pull-on-resolve already spreads records
>   epidemically; add it only if freshness latency proves to matter.
> - **IPNS leg is wired but dormant in prod.** `resolve_ipns` isn't invoked by the frontend
>   yet, and nothing publishes the catalog as IPNS (the server `IpnsStore` is *"inert until
>   the catalog migrates to IPNS"*; no master republish hook). The cache/pull chain stays
>   dormant until the **catalog→IPNS migration** lands server-side (publish with ~24–48h
>   validity) — now specified as **R1** below. PEX + `Peers` go live the moment clients update.
> - **Rollout pending:** rebuild + push to the two desktop clients (carries Phase 0 + 1).

_Original design sketch (kept as rationale of record; see **Shipped** above for what
actually changed):_

The builder is generic over a custom `NetworkBehaviour`; today it's pinned to the
no-op dummy (`ipfs.rs:251`). **That slot is the integration point.** Drop in a single
`libp2p::request_response` behaviour (CBOR codec) under `/trackerstream/peer/1.0.0`.
It rides the existing swarm — inherits relay/DCUtR/NAT traversal, and **never dials
a stranger** (only peers already known via tracker/PEX), so it does *not* reopen the
G1 crawl. Keeps QUIC (no pnet).

One protocol, three queries — each reusing data shapes we already have:

```rust
enum Req  { Pex { want: u8 }, Peers { root: Cid }, Ipns { name: PeerId } }
enum Resp { Pex  { peers: Vec<PeerRef> },           // tracker's exact PeerRef shape
            Peers { peers: Vec<PeerRef> },
            Ipns  { record: Option<Vec<u8>> } }      // verify_record's exact input
```

Build on it:

- **PEX** — periodic `Pex` pulls over the warm set; merge into an address book
  (dedup by peerId, prefer direct-dialable, cap). When the tracker is unreachable
  and the warm set has free slots, **dial from the book** instead. *This sustains
  membership through a box outage.* Self-reported addrs are fine (same posture as
  the tracker; Noise binds the connection to the PeerId, so worst case is a wasted
  dial, never impersonation).
- **Peer-pull for IPNS + a seq-bump doorbell.** Resolution order: local cache → tracker
  → **peer-pull** (`Ipns` to several warm peers, verify each via `verify_record`,
  newest-wins by sequence). Every client caches what it resolved and serves it, so
  records spread epidemically — **pull-based gossip without a gossipsub mesh.** Add
  a tiny push *notification* (`{name, seq}` only — a doorbell, not the record) over
  the same channel for seconds-not-minutes freshness. Pull wins here because the
  catalog changes ~once per rebake; gossipsub's standing mesh cost isn't worth it
  for a near-static value (see "Settled: pubsub" below).
- **`Peers{root}`** — holder discovery, peer-served, so it survives the box.

**Ship gate:** kill the box in the two-node test → **membership, naming, and
holder-discovery all survive in the swarm** for warm content. The box is now a
seeder, not a dependency, for everything except *reachability* (R2) and *cold
backfill* (R3). Highest-leverage phase: one behaviour unlocks all three.

---

## Track R — Resilience

### R1 — Catalog → IPNS: the catalog as a pinned, self-certifying artifact ✅ SHIPPED

> **Shipped** (commit `ee6ee11`, branch `feat/r1-catalog-ipns`; deployed to prod). The catalog
> is now a master-signed IPNS-pointed SQLite artifact that clients **query lazily over a
> Bitswap-backed `sqlite-vfs`**, fetching only the pages a query touches — and the `/catalog`
> HTTP routes are **hard-cut**. This lit up the entire dormant Phase-1 IPNS plane
> (verify → cache → peer-pull) end to end.
>
> **What shipped, milestone by milestone (all four met their gates):**
> - **R1.0 — publisher + signing (server/master).** Schema tuned for index-only access
>   (`page_size=16384`, covering indexes for the three browse orders, a `meta` count/format
>   table, a bm25 low-selectivity guard); snapshot via checkpoint + `copyFileSync` (no VACUUM);
>   new `KuboRpc.addFile`/`keyGen`/`namePublish`/`routingGet`; persisted `IpnsStore`; the
>   ingest hook publishes (seq++) and `POST /ipns`es the signed record. `CATALOG_IPNS_KEY` is in
>   `packages/config`. Validated against real kubo 0.42 — `ipfs name inspect --verify` reports
>   `Valid: true`, the record survives an API restart.
> - **R1.1 — lazy-query VFS (the headline).** Vendor patch `UnixfsCat::range` (`patches/0003`);
>   `rusqlite`(bundled)+`sqlite-vfs`; `catalog.rs` — a read-only Bitswap VFS with a page cache,
>   **concurrent prefetch** (`buffer_unordered`), and a sync→async bridge (`spawn_blocking` +
>   `Handle::block_on`); the 4 queries ported to Rust (same SQL); `catalog_query` Tauri command;
>   frontend `invoke()` cutover + a stale-result guard. *Gate met:* the box-down test
>   (`catalog_queries_lazily_over_bitswap_from_a_peer`) — node B, holding **none** of the DAG,
>   answers search/browse/detail/formats over Bitswap from A, fetching **~1% of the DB** for a
>   detail lookup. (Folded the old R1.2 — blocks-from-a-peer — into this test.)
> - **R1.3 — chunk-stability + prefetch + tuning.** Page-aligned **dag-pb** leaves (16 KB) —
>   *not* `--raw-leaves` (the vendored rust-unixfs visitor can't walk raw `bafkrei…` leaves; this
>   was the "catalog offline" bug). One-time prod conversion to `page_size=16384`. Concurrent
>   prefetch + bounded bm25 probe cut a broad search **41 s → ~8 s cold, ~80 ms warm**.
> - **R1.4 — flip default + hard-cut.** `catalog.ts` calls `invoke("catalog_query", …)` only; the
>   `/search`, `/modules`, `/module/:id`, `/formats` routes and dead query methods are deleted and
>   deployed. The box now serves only publish + tracker + IPNS.
>
> **~~R1.2~~ DROPPED** (folded into R1.1). True box-down catalog *completeness* needs replication
> (R3), not a milestone here; the realistic model is master-as-seeder + opportunistic peer cache.
>
> **Residuals / spin-offs — all closed by R4 (Go everywhere):**
> - **Per-search resolve round-trip** (each query resolved `CATALOG_IPNS_KEY` before opening the
>   VFS). The Phase-1 doorbell wanted to push the record on an app-topic; the two-stack interop
>   that blocked it (connexa↔kubo gossipsub never proven, the kubo-plugin CBOR route byte-identical
>   but a daemon fork) **evaporated** once both ends are the same Go binary. R4 ships
>   **IPNS-over-gossipsub** on a custom catalog topic — clients hold the record push-style, **zero**
>   per-query resolve.
> - **Signing went through kubo** (`key/gen` + `name/publish`). R4 moved it in-process to boxo
>   `ipns` (byte-compatible with the client's `verify_b64`, which stays the trust anchor).

**The idea (confirmed direction): publish the catalog *as a SQLite file* on IPFS, point a
master-signed IPNS record at it, let clients pin + query it locally.** Today the catalog is a
`node:sqlite` DB (`modules` table + FTS5, `catalog.ts`) the server queries on every
`/search`, `/modules`, `/module/:id` and returns thin `SearchHit`s over HTTP (`api.ts`). That
makes the box a hard dependency for *finding* anything — even though the audio bytes already
flow peer-to-peer. R1 turns the catalog into just-another-content-addressed-artifact riding
the plane we already built:

```
master: snapshot DB → `ipfs add` (CDC chunker) → catalogCID → name publish (seq++) → POST /ipns
client: resolve_ipns (cache → tracker → peer-pull) → Bitswap-assemble the DB → pin it → query locally (rusqlite + FTS5)
```

Box-down, the pointer still resolves (cached record within EOL, or peer-pull) **and** the DB
is fetchable (peers who pinned it serve its blocks). That closed loop — *resolve the name from
a peer, assemble the bytes from a peer, answer the query locally* — is the "missing link" the
rest of the roadmap keeps referencing.

**Why SQLite-as-a-blob and not a CBOR/DAG catalog.** We already bake audio into CBOR DAGs; the
catalog *could* be one too. **Don't** — the whole value of shipping the SQLite file is that
**FTS5 + SQL ride inside it for free**. A DAG catalog throws away bm25 search and structured
filter/sort/paginate, forcing us to rebuild a query/index layer client-side. Keep SQLite;
solve the one real problem it creates (next).

**Don't sync the whole DB to query it — fetch only the pages a query touches.** This is the
realization that reshapes R1 (cf. phiresky's [`sql.js-httpvfs`](https://phiresky.github.io/blog/2021/hosting-sqlite-databases-on-github-pages/),
which queries a 600 MB+ SQLite DB on a *static host* by fetching only the pages a query reads
over HTTP **range** requests). SQLite reads in fixed-size **pages** and walks **B-tree
indexes**, so any one query touches O(log n) + result pages — a handful, not the file. The
IPFS analog is exact: a UnixFS file *is* a DAG of chunks, and Bitswap fetches a byte range by
fetching just the leaf blocks it covers. Back `rusqlite` with a **custom VFS** (the
`sqlite-vfs` crate registers one in Rust) whose `read(offset,len)` maps to leaf block(s) →
`ipfs` block-get, with a local page cache. A search then pulls a few KB of index + row blocks,
**not the catalog.** This is the headline of R1 and it dissolves the
whole-DB-download problem for the common case.

**So R1 has three access modes, and they compose:**
1. **Lazy query (default, no pin).** Most users never hold the whole catalog — they resolve
   the IPNS pointer, then query over the Bitswap-backed VFS, fetching only touched pages. Tiny
   footprint; offline-capable for any block a peer holds.
2. **Full pin (R3, opt-in).** Durable replicas hold *every* page → serve the cold tail +
   guarantee completeness box-down. **This is why R3 still matters:** lazy query covers hot
   paths, full pins cover cold rows no peer happens to hold. Incremental update of a full copy
   is the chunk-stability concern below.
3. **Server HTTP (transitional).** The fast path while the box is up — and precisely the
   dependency R1 dissolves (see *"the Node backend is transitional"* up top).

**What chunking still has to get right (refined — not "re-download everything," but cache
locality).** Lazy query removes the bulk-download cost, but two things still ride on
**page↔block stability across versions:**
- **Align the UnixFS leaf size to the SQLite `page_size`** (small fixed chunks, ~16–64 KB —
  *not* the 256 KB default) so one page read → one block fetch, minimal over-fetch (the same
  tuning phiresky does with chunk size).
- **Keep unchanged pages byte-stable across rebakes** so their block CIDs don't churn: publish
  the **WAL-checkpointed file as-is, never `VACUUM`d** (vacuum reorders pages → every block CID
  changes). This earns *two* wins at once — (a) full-pinners (mode 2) fetch a small diff on a
  rebake, and (b) the hot index pages **every** querier touches stay **cached across the swarm**
  version-to-version instead of being re-fetched from the master each rebake. Content-defined
  chunking (which `repack`'s `buildDagV2` already does for samples) is the stronger version if
  fixed page-aligned chunks churn too much. **Measure the cross-version block reuse + per-query
  page count on a real rebake (R1.3)** before committing; format/id-range **sharded DBs** are
  the fallback if page churn wins.

**New hard part — Bitswap latency per page.** HTTP range (phiresky) rides one multiplexed
connection; Bitswap is a per-block want/have round-trip, so a query touching 30 pages serially
could stall. Mitigate with a generous local page cache (the blockstore already caches),
**read-ahead / block prefetch** (issue a wantlist for the predicted page set), and the natural
swarm-wide caching of exactly the hot index pages everyone touches. Acceptable for a
near-static catalog; measure, then prefetch if it bites.

**Publishing + signing (the server/master half that's missing).** Nothing holds a master
private key today — `MASTER_PEER_ID` is config-only, and `IpnsStore` is in-memory and never
written (`ipns.ts`). R1 adds the publisher:
- The **master kubo already holds keys and already pins the corpus** (ingest's
  `loadDagToKubo` recursively pins every root). Give it a dedicated **catalog key**; on each
  ingest/rebake, snapshot the DB → `ipfs add` (CDC chunker) → `ipfs name publish --key=catalog`
  (bumps the IPNS sequence, ~24–48h validity per Phase 0). This yields a **standard IPNS record
  the client already verifies** — `verify_record` derives the pubkey from the name = the key's
  PeerId. Wire the publish into `runIngest` after `updateRoot`/insert so it fires exactly
  "once per rebake."
- A small **republish hook** reads the signed record back (`ipfs routing get /ipns/<key>`) and
  `POST /ipns`es it to the tracker dumb-store — the caller the `ipns.ts:14` comment promises.
  **Persist `IpnsStore`** (today in-memory) so it survives a tracker restart.
- The published **name = the catalog key's PeerId**; ship it in `packages/config` beside
  `MASTER_PEER_ID` so the client knows what to resolve.

**Pinning + R3 composition.** R1 makes the catalog the **first thing R3 pins** ("catalog
first, hottest root"). Peers that pin it become catalog-block providers, so even box-down a
client assembles the *latest* catalog (within the record EOL) from peers — discovery survives
without the box. R1 ships the publish/resolve/local-query loop; R3 layers the opt-in disk
budget + at-risk-root pinning on top.

**Milestones.**
- **R1.0 — publisher + signing.** Catalog key on the master; `runIngest` snapshots
  (page-aligned chunker, no-vacuum) → `ipfs add` → `name publish` → republish hook
  `POST /ipns`; persist `IpnsStore`. *Gate:* `GET /ipns/<catalogKey>` returns a record the
  client's `verify_record` accepts, and the CID matches the added DB.
- **R1.1 — lazy-query VFS (the headline).** `rusqlite` over a Bitswap-backed `sqlite-vfs`:
  resolve `<catalogKey>` → open the remote DB → answer `/search` (FTS5) + list + detail by
  fetching only touched pages. Port the 5 query handlers from TS (`catalog.ts`) to Rust.
  *Gate:* box-up, a client answers a search pulling ≪ the whole DB (instrument bytes fetched).
- **R1.2 — box-down lazy query.** Same path, blocks sourced from peers — hot index pages from
  swarm cache, anything else from full-pinners. *Gate:* with the box down, a popular search
  returns correct results assembled purely from peers.
- **R1.3 — chunk-stability proof.** Page-aligned leaves + no-vacuum (CDC if needed); measure
  cross-version block reuse and per-query page count over a real incremental rebake. *Gate:* a
  +N-module rebake reuses ~all prior blocks; a typical query touches a handful of pages.
- **R1.4 — full-pin mode + flip the default.** Opt-in full-DB pin (feeds R3's "catalog first");
  optionally make lazy-VFS the frontend default and demote HTTP to bootstrap/fallback. *Gate:*
  search works with the box never contacted (lazy for hot, full-pin for cold).

**Hard parts / risks.**
- **Per-query latency** (Bitswap round-trips) and **chunk stability** (cache locality across
  rebakes) — both covered above; settle empirically in R1.1/R1.3, sharding is the fallback.
- **Snapshot consistency:** publish a checkpointed, WAL-free single file; never publish a DB
  mid-write — `.backup` into a temp path and `ipfs add` *that*.
- **Staleness window:** box-down, a client may query a catalog up to one record-EOL old.
  Acceptable (the catalog is near-static); the Phase-1 doorbell (deferred) tightens it if
  freshness ever bites.
- **Trust:** the record signature is the only trust anchor — the tracker/peer serving it stays
  untrusted (already the design). The catalog key's compromise = a poisoned catalog; treat it
  like the release-signing key.

**Ship gate:** with the box down, a client resolves the catalog name from a peer and answers a
search **locally over Bitswap-fetched pages — pulling ≪ the catalog** — so discovery survives
the box end to end, and an incremental rebake leaves the hot index pages cached swarm-wide.

### R2 — Reachability: peer-provided relay ✅ SHIPPED (client-only)

> **Shipped (M0–M5, uncommitted).** NAT'd peers reach each other through peer-provided
> relays with no master — proven by `nat_peer_reachable_through_peer_relay` (C reaches a
> NAT'd holder B purely via peer relay A's circuit addr). Client-only: no server change; the
> master keeps its clamped coordination relay (see the dual-role note below). 29 unit + 6
> integration tests green.
>
> **What shipped, milestone by milestone:**
> - **M0 — eligibility gate.** Vendor patch `Ipfs::nat_public()` (AutoNAT verdict as a bool,
>   no libp2p-type leak) + a debounced `Reachability` (3-consecutive hysteresis) + a ~30s
>   poll loop; surfaced as `node_info.reachable`.
> - **M1 — relay server.** `IpfsBuilder::with_relay_server(clamped 128KiB/30s)` gated on
>   `relay_policy.json` (**opt-in, default off**). Findings: reserve via
>   `add_listening_address(.../p2p-circuit)` — the vendored `enable_relay` is incomplete
>   (its completion channel is never drained); a relay must have a confirmed external address
>   or the voucher is empty (`NoAddressesInReservation`) — guaranteed by the M0 gate.
> - **M2 — relay wire (refined).** `Req::Relay{target}` / `Resp::Relay{reachable,willing,
>   can_reach}` — a **targeted `can_reach` query**, not a neighbor-dump (avoids the
>   social-graph leak AND the privacy-vs-function contradiction). Warm-only serve guard,
>   `relay_pull`, volatile `RelayView`.
> - **M3 — self-reservation (the main win).** A NAT'd node holds a circuit reservation on
>   1–2 willing+reachable relays (`reserve_on_relay`, timeout-guarded); the circuit addr
>   flows into announce automatically; public nodes hold none.
> - **M4 — residual fallback.** On a failed direct dial with no known circuit addr, ask warm
>   peers `Relay{target}` and dial through a `can_reach` one (`dial_via_relay`).
> - **M5 — instrumentation.** `RelayStats` from `connection_events`: direct vs peer-relay vs
>   master-relay split + inferred DCUtR upgrades, surfaced in `peer_stats`. A visibility
>   dashboard, **not** a flip-off gate — the master keeps its coordination relay.
>
> **Residuals:** rollout (rebuild + push to the desktop clients); community public relays if
> the real NAT-heavy population shows thin peer-relay supply (measure via M5).

A perfect DHT that says "peer Y holds X" is useless if Y is NAT'd and your only
relay was the master. This leg is what makes both-boxes-dead *fetchable*, not just
discoverable — and it's the genuinely hard part.

_Original design notes (kept as rationale of record; see **Shipped** above for what
actually landed):_

- **Client AutoNAT (both roles).** A client must know it's publicly reachable
  before it can volunteer as a relay — AutoNAT is the **eligibility gate**. Serve
  AutoNAT too, so the "am I reachable?" question survives the master.
- **Peer-provided relays.** Publicly-reachable clients run circuit-relay-v2 (opt-in,
  capacity-bounded, not on metered links) + DCUtR upgrades.
- **Relay selection is a *relation*, not a flag.** Relay *ability* is an edge on the
  live connection graph, not a node attribute. The rule:

  > **X can relay P↔Q ⟺ P and Q each hold a live, usable connection to X.**

  `publiclyReachable` is just the degenerate case where that's true for *everyone*.
  So advertise node-global facts — `publiclyReachable` (AutoNAT-gated) +
  `willingToRelay` (policy/capacity) — and gossip each peer's **live neighbor /
  active-reservation set** in PEX. Relay candidates for a failed pair = the
  **intersection** of live neighbor sets, filtered by willingness — computed
  locally, per-pair. A NAT'd peer *can* relay for two neighbors it already
  hole-punched to (relay-v2 HOP runs over existing connections); public peers
  dominate the intersection automatically when present. One mechanism, both cases.
  **Do not ship a global `canRelay` boolean** — it conflates a relation with a
  property.
- **Demote the master relay — but never conflate its two functions.** "Relay" hides
  two separate roles, and only one is expensive:
  1. **Bulk byte transport** (circuit-v2 HOP carrying the *stream*). The costly part.
     Phase 0 already clamped it to near-nothing (128 KiB / 30s). This is the only role
     R2 ever considers flipping.
  2. **Hole-punch coordination** (the DCUtR rendezvous): two NAT'd peers need a shared
     connection to exchange observed addresses + sync the simultaneous-open. That's a
     *reservation* carrying a few control bytes — and the Phase-0 clamp was deliberately
     sized to keep exactly this alive while killing #1. Plus **AutoNAT** (the
     reachability eligibility gate) and **identify** (`observed_addr`, DCUtR's raw input)
     — which were never "relay" and stay on **unconditionally**.

  So **do not fully disable `Swarm.RelayService`.** The honest boundary: a node offering
  *zero* reservation capacity stops being a hole-punch coordinator, which strands the one
  case peer relays can't always cover — **two symmetric-NAT peers with no shared
  connection** (the DCUtR punch needs a common rendezvous; if no peer relay sits in the
  intersection, the pair stays unreachable). The clamped handshake-only reservation is a
  cheap permanent **coordination floor**; keep it. Instrument **peer-relay coverage +
  DCUtR upgrade rate** to confirm peers carry the *bulk* path, not to justify removing the
  master's coordination role. If peer supply is thin, the robust fix for a NAT-heavy swarm
  is still **a couple of community-run public relays** (stateless, opt-in, copy-catable —
  the relay analog of federated trackers; the "dedicated relay infra" deferred in MVP §F).

**Ship gate:** NAT'd peers reach each other through peer relays without the master;
the peers pane shows B↔A traffic, not B↔master. The offload is finally *genuine*.

### R4 — Go everywhere: one `tsnode` binary (dissolve kubo AND rust-ipfs) ✅ SHIPPED

> **Shipped (phases A–D), branch `feat/go-tsnode`; cutover live on prod 2026-06-27/28.** The box and
> the desktop client now run the **same custom Go binary, `tsnode`** (go-libp2p host + boxo data layer),
> as `--role server` and `--role client`. kubo is removed (kept disabled as a one-command warm-standby
> rollback), the embedded rust-ipfs/connexa node is removed, and the Node service — tracker **and** the
> HTTP API — is fully retired. Catalog naming + distribution are pure libp2p: a **custom private DHT**
> (`/trackerstream/kad/1.0.0`) + an **IPNS-over-gossipsub** catalog topic, both native because we own
> both ends.

**Why this replaced "R4 = Rust master."** The original R4 tried to port the *master* onto the clients'
rust-ipfs/connexa stack. That stack proved too immature for the **load-bearing seeder** — we'd already
vendor-patched it three times — so R4-in-Rust was **aborted** and its branch discarded. The pivot
(user-directed): instead of porting the master *down* onto the fragile client stack, **assemble one
binary we own from boxo + go-libp2p and run it on *both* ends.** boxo is the Go data layer kubo itself
is built from, so the seeder/blockstore is battle-tested at scale **by construction** — the single
load-bearing unknown that gated the Rust plan ("Bitswap-at-scale") simply **dissolves**.

**What the pivot buys (vs. both the old kubo+rust split and the aborted Rust-master plan).**
- **The three rust-ipfs vendor patches dissolve into native Go APIs.** Per-peer Bitswap attribution
  (patch 0001) → `metrics.BandwidthCounter`; AutoNAT verdict (patch 0002) → `EvtLocalReachabilityChanged`;
  ranged UnixFS reads (patch 0003) → boxo's seekable reader / `cat?offset&length`. Because we *assemble*
  our own binary importing boxo as a library, **there is no vendored-fork-patch workflow left at all** —
  every customization is our own code calling public APIs.
- **Custom DHT, native and private.** `dht.ProtocolPrefix("/trackerstream")` → `/trackerstream/kad/1.0.0`.
  Routing tables admit only peers that speak the custom protocol (gated via identify), so public IPFS
  nodes (`/ipfs/kad`) can connect inbound but are **never** added to the table — trackerstream-only **by
  construction**, no crawl, no G1, and **QUIC is kept** (a routing-layer protocol string, not pnet's
  transport-layer XOR). This is the "native custom-kad" the old R4.4 wanted, now just *how the node is
  built*. Corollary: **PEX is app-peers-only for free** — peer exchange reads the DHT routing table
  (trackerstream nodes only) + the presence topic (trackerstream subscribers only); the public randos in
  the libp2p peerstore are never exchanged.
- **IPNS-over-gossipsub kills the per-search resolve round-trip.** The master signs in-process (boxo
  `ipns.NewRecord`, byte-compatible with the Rust `verify_b64` that **stays** the trust anchor) and
  pushes the record on a custom catalog topic; subscribers hold it push-style and resolve with **zero**
  DHT round-trip. The custom DHT is the box-down fallback resolve. (This realizes the Phase-1 doorbell
  natively.)
- **One self-introspectable codebase**, with a kubo-compatible `/api/v0` RPC so the ingest (`KuboRpc`)
  and the Rust client port across a narrow seam.

**Phases (all shipped + verified).**
- **A — tsnode core** (`node/`, own go.mod). Host assembly (tcp+quic, identify, ping), custom-prefix
  kad-DHT, gossipsub (catalog + presence topics), circuit-relay v2 (client + clamped server),
  AutoRelay/autonat/dcutr, BandwidthCounter; boxo bitswap (client+server), persistent
  blockstore/datastore, unixfs add + ranged cat, ipns sign/verify; the trackerstream control plane
  (warm set, presence, held-roots, relay selection, peerstore, IPNS cache); kubo-compatible RPC. *Gate
  (Go tests):* custom-DHT providers + isolation from `/ipfs/kad`, gossipsub delivery, ranged cat,
  block put/get CID-parity, a tsnode-signed IPNS record verifies under the Rust `verify_b64`.
- **B — server master cutover.** Replaced kubo with `tsnode --role server` on the box; imported the two
  ed25519 identities (swarm + catalog key) so already-shipped clients keep resolving; ingest publishes
  in-process (DHT + gossipsub); **deleted the Node tracker**. Zero-downtime alt-port re-ingest (1453
  modules, root CIDs md5-identical) then port-swap; kubo kept disabled as warm-standby rollback.
- **C — client sidecar.** Bundled `tsnode` as a Tauri `externalBin`; the Rust backend spawns it,
  health-checks the RPC, kills it on exit, and drives every call (block fetch, ranged catalog reads,
  dial/warm, peers pane) over the local kubo-compatible RPC. **Deleted** `peer.rs`/`pinglog.rs`/
  `tracker.rs`, the `vendor/` + `patches/` trees, and the rust-ipfs deps; `ipns.rs` verify stays as the
  trust anchor.
- **D — custom DHT + IPNS topics end-to-end.** Clients subscribe to the catalog topic (zero per-search
  resolve); custom-DHT provider discovery for cold roots box-down.

**Post-cutover hardening (this session).** Raised rcmgr/connmgr limits for the inbound swarm; persisted
the DHT record store (`dht.Datastore`) so IPNS/provider records survive restart; gave the client sidecar
a built-in default bootstrap (it had been spawning with none → "catalog offline") + a cold-start resolve
retry; auto-reap a sidecar orphaned by an abnormally-killed app (stale datastore lock). **Retired the
HTTP API server entirely** on prod — catalog resolve is pure libp2p, metrics repointed to tsnode
`node/status`, Caddy reduced to a static 200 for the cert/uptime probe.

**Two load-bearing bugs found + fixed (Go regression tests in `node/`).**
- **Never pass `dht.BootstrapPeers` to `dht.New`** — it dials during `libp2p.New`, *before* `bitswap.New`
  registers its connection notifiee, so Bitswap never learns the peer is connected and every `block/get`
  hangs (broadcast-want reaches nobody). Connect to bootstrap explicitly *after* Bitswap is up.
- **`readNamedFile` (RPC block/put/add) must dispatch on Content-Type** before the multipart parser,
  which otherwise drains a raw body and stores an empty (un-servable) block. Real kubo clients use
  multipart so prod was unaffected; raw-body callers hit it.

**Residuals.** The desktop release/codesign+notarize pipeline is greenfield (no desktop CI yet — the one
genuinely new surface; the binary is ~30 MB stripped, one triple per install via `externalBin`). AutoNAT
hysteresis on the Go side (reachability flaps) and decay of stale public-swarm connections are minor
follow-ups. R3 (cold-tail replication) is unchanged and independent.

**How it connects to the rest of the R leg.**
- **Consumes R1** byte-for-byte: same published artifact + client VFS; it swapped the *publisher*
  (kubo → in-process boxo `ipns`) and the *delivery* of the pointer (per-search resolve → gossipsub push).
- **Absorbed the old R4.4 (custom-kad).** Native + private by construction (above) — not a milestone,
  just how tsnode's DHT is configured. Routing and replication were always separate concerns; the old
  kubo constraint was the only thing that bundled kad into R3.
- **Doesn't touch R2.** Reachability re-implemented on the Go node (circuit-relay v2 + AutoRelay + DCUtR);
  the master keeps its clamped coordination relay.
- **Realizes the Phase-1 doorbell** as the catalog gossipsub topic — push-native, not pull-on-resolve.

**Ship gate (met).** A client searches with **zero per-query name resolution** (record pushed over the
catalog topic, signed in-process by boxo `ipns`); and the box runs as a **single Go binary** that seeds,
signs, announces, and coordinates relay, with kubo removed and box-reboot recovery intact.


### R3 — Fanning the archive across peers ⏸️ ON HOLD (gated on R1 ✅, but deferred — see below)

> **ON HOLD (2026-06-29).** Not a sequencing/dependency block — a cost-benefit one. The master
> now runs as a **private overlay on `:5478` with ~0 peers** (we moved off the public `:4001` and
> shed the public-IPFS swarm on purpose). R3 spends bandwidth + peer disk to *distribute the cold
> corpus across the swarm* — but with no swarm to distribute to, that machinery costs more than it
> buys: replicas would land nowhere, and the placement/PEX/re-balance churn runs against an empty
> roster. **Replication only pays once there's a real peer population to hold the replicas.**
> Revisit R3 when concurrent client count is non-trivial (a handful of always-on peers, not zero).
> Until then the master-pinned blockstore (GC-disabled, durable) + backups are the availability
> floor, and that's adequate at this scale. Everything below is the design we'll pick up *then*.

> **Prerequisite: R1 (done); independent of R4 (done).** R3 is now scoped down to one question:
> **how do we spread the actual archive — the cold module corpus, ~100× the catalog — across
> peers so it survives a dead box?** The structured-*discovery* half (custom-kad) was absorbed into
> **R4** and shipped — the `tsnode` master speaks `/trackerstream/kad/1.0.0` natively, so routing
> exists (routing ≠ replication; the old kubo constraint is the only thing that ever bundled them).
> What's left here is purely the **data-distribution policy**: who holds which bytes, and how much.
> With R4 shipped, R3 *was* the next resilience do-now — now **on hold** (see the banner above):
> 0-peer overlay means replication has nowhere to land.

This is the one leg that addresses the **irreducible gap** (§0): truly *cold* content — held
only by the master, never streamed by anyone — is unfetchable while the box is down. No discovery
or relay fixes a block that exists in one place. The only lever is **replication**, and R3 is the
decision of *how to place* those replicas. The catalog (R1) is already covered — lazy VFS + its
hot pages swarm-cache, and it's tiny; **R3 is about the archive bytes**, which are big, cold, and
mostly held only by the master today.

**The design space (this is the decision R3 makes):**
- **Opportunistic / interest-driven** — peers pin what they (or their friends) actually play,
  plus a voluntary disk budget of at-risk roots. Simple, zero-coordination, naturally weights hot
  content — but leaves the **cold tail** (rarely-played modules) with replication factor ~1
  (master only), so box-down they're still gone. Good floor, no guarantee.
- **Deterministic sharding (HRW / rendezvous)** — `hash(root)` → the K peers responsible for
  holding it, computed over the PEX-gossiped roster. Gives an *even, predictable* spread with a
  target replication factor across the **whole** corpus, cold tail included — at the cost of
  peers holding bytes they never play, and re-balancing churn as the roster changes. This is the
  "fan the entire archive across the swarm" model; it pairs naturally with R4's custom kad (same
  HRW responsibility function can drive both placement and lookup).
- **Hybrid (likely answer)** — opportunistic for hot content (free, already happening via the
  warm-set), deterministic K-replication only for the **at-risk cold tail** the presence table
  flags as under-replicated. Spend coordination only where opportunism leaves a hole.

**What R3 ships regardless of the model chosen:** a live **replication-factor** signal (from the
presence table — who holds what, how many copies) with an **alert when an at-risk root drops
below a floor**, an **opt-in disk budget** per client, and the **placement policy** (one of the
above) that decides what each client pins. **Catalog-first** stays the seed rule (hottest,
self-certifying root from R1).

**Ship gate:** the archive's at-risk cold tail has a measured replication factor above its floor
across live peers — so box-down, the corpus (not just the hot/warm slice) is still fetchable from
the swarm; the chosen placement policy holds the factor without manual intervention.

---

## Track P — Product (identity → friends → social)

### P4 — Identity foundation 🟡 (do before ANY identity-bearing feature)

Multi-device is a day-one requirement, and retrofitting an identity layer *after*
shipping device-as-identity is an ugly migration (every record references the wrong
key level). So get the data model right before there's data. Pattern ≈ Matrix
cross-signing.

- **Identity key `Ik`** = the account / "you", long-lived, **backed up via recovery
  phrase**. Friends bond to `Ik`, never to a device.
- **Device keys** keep their own libp2p PeerIds (never share one keypair across
  devices — that's networking chaos). `Ik` signs a **device certificate** per
  device: `sign_Ik(devicePubkey, name, notAfter)`. Present it in announce/handshake;
  presence aggregates device → identity ("A is online" = any authorized device
  present).
- **QR enrollment ceremony.** New device displays QR `{ devicePeerId,
  dialableAddrs[], oneTimeNonce }`; the `Ik`-holding device scans, then **signs a
  device cert and sends it + the `Ik` *public* key + profile/friend state — NOT the
  `Ik` private key.** Copying `Ik` collapses the whole split and **destroys
  revocation** (a device holding `Ik` can't be revoked; you'd have to rotate the
  identity and re-friend everyone). Signing a cert keeps `Ik` scarce and makes lost
  devices a one-line revoke. Noise binds the transfer to the PeerId (MITM-safe); the
  nonce prevents replay/unsolicited push; optional SAS string for belt-and-braces.
  Make **owner** (holds `Ik`, can enroll/revoke, *not* cleanly revocable) vs
  **member** (cert only, fully revocable) an explicit choice; default to member.
- **Publish an x25519 encryption subkey** in the signed profile. `Ik` is an ed25519
  *signing* key — it can't *receive* encrypted content. This one field is agony to
  retrofit and blocks every friend-scoped private feature; add it now.

**Ship gate:** multi-device works; identity survives device swaps; the social data
model is in place. Forward-compatible — friends pin `Ik`, so hardening *how* `Ik` is
stored later (cold root + delegated signing) breaks nothing.

### P4.5 — User feeds: per-device IPNS, CRDT-merged 🟡 (the user-data plane for P5/P6)

Once there's an identity, peers publish their own feeds — **profile, playlists, friend
set, now-playing history** — that others read. The naive shape (one IPNS name per user)
is **impossible under the P4 key rule**, and the fix forces a CRDT.

**The crux: an IPNS name is 1:1 with a keypair.** Verification derives the pubkey *from
the name* and checks the signature (`ipns.rs:verify_record`); there is no cert-chain
delegation in IPNS. So whoever holds the name's private key can bump it — and only they.

- One feed under `Ik` → only **owner** devices (which hold `Ik`) can write; delegated
  member devices can't. And you must **not** hand `Ik` to a device (kills revocation, P4).
- One shared "feed key" F → same problem moved to F; revoking one device means rotating
  F → republishing under a **new name** → every friend has to re-discover you.

There is no safe way to let a delegated device bump a *shared* name. So:

**Each device publishes its OWN feed under its device key; the user's state is the CRDT
merge of all authorized device feeds.** A delegated device signs *its* feed freely (no
`Ik`), is fully revocable, and readers merge. Two tiers mirror the key hierarchy:

| Tier | Content | Writer | Frequency |
|---|---|---|---|
| **Roster** | authorized device set (`Ik`-signed certs) | **owner only** | rare (enroll/revoke) |
| **Data** | profile, playlists, friends, history | **any authorized device** (device key) | frequent |

A reader resolves the `Ik`-signed **roster** → resolves **each device feed** → **merges**,
gating each device's ops on "was it authorized at write time?" Frequent writes never touch
`Ik`; `Ik` only signs the rarely-changing membership. The stable public handle stays
**`Ik`** (friends bond to it); the roster is the `Ik`→devices indirection.

**Reuses the Phase 1 plane almost wholesale.** Our `Ipns` record (`verify_b64_seq` +
newest-by-sequence + peer-pull) is *already* a signed, versioned, peer-gossipable mutable
pointer. A device feed = that, with the name being a **device PeerId**, verification
*also* checking the `Ik`→device cert (+ revocation), and the pointer targeting a
**content-addressed CRDT snapshot** fetched over Bitswap. The merge sits *above* the
existing transport — net-new work is the CRDT data model + the **cert-gated merge**.

**v1 shape (recommended).** **State-based** CRDT snapshots per device (idempotent merge,
no causal-delivery machinery; reuses IPNS-pointer + Bitswap exactly):

- *Profile fields*: LWW-register per field, ordered by a **hybrid logical clock** (never
  wall-clock — skew/backdating), device-id tiebreak.
- *Playlists / friends / blocks*: **OR-set** of entries (the friend-set OR-set P5 already
  names); block list wants remove-resistant semantics.
- Each op carries **(device-id, HLC)**; merge is **gated on cert validity at op time**.

**Hard parts (the design lives here):**

- **Revocation vs. history.** Revoking a device cuts its *new* writes (gate on the cert
  validity window); the user re-asserts state from a trusted device to stomp anything bad.
  Don't blindly drop a revoked device's *past* ops — it can resurrect deletions.
- **Revocation propagation in degraded mode.** Lean on **short-`notAfter` device certs**
  (owner re-issues) → revocation = stop renewing → auto-expiry, eventually-consistent
  without pushing a revoke to every reader. Explicit `Ik`-signed revocation records
  (newest-wins) are the fast path on top. Trade-off: owner must reappear periodically.
- **Tombstone GC** (OR-sets accumulate removes) — compact at snapshot time with a rule
  that can't drop a concurrent unseen add. ⚪ defer, but don't design into a corner.
- **Encryption.** Friend-scoped feeds are sealed to the friend set (the P4 x25519 subkey);
  merge happens on plaintext post-decrypt. Keep public vs friend-scoped as separate
  sub-feeds, not one mixed blob.

⚪ **Defer:** sequence-CRDT playlists (concurrent reorder of the *same* list is rare for a
single user's own devices — OR-set/LWW is plenty for v1), op-logs (vs snapshots), GC.

**Library landscape (Rust).** There is **no production-grade turnkey "libp2p + CRDT store"
in Rust** (Go has `go-ds-crdt`; Rust has no maintained equivalent). The pattern is
**bring-your-own-transport + a CRDT lib** — which fits us, since we already own the
transport:

- *Transport-agnostic CRDT libs* (pair with our plane): **`automerge`** (JSON-doc,
  multi-actor, history + compaction — best "document" fit), **`yrs`** (Yjs; best
  lists/text), **`loro`** (rich types incl. movable list — good for reorderable
  playlists), **`crdts`** (low-level OR-set/LWW toolkit if hand-rolling the composite).
- *Full-stack alternatives on their OWN network* (not rust-libp2p): **`iroh`+`iroh-docs`**
  (multi-writer signed KV, per-author keys — conceptually ideal but iroh's QUIC stack),
  **`p2panda`** (per-author append-only logs + materialization), **`willow-rs`** (per-
  author subspaces; maturity unverified). Adopting one means adopting its stack.
- **Decision:** pair a transport-agnostic CRDT (lean `automerge` or `loro`) with the
  Phase 1 plane; do **not** bolt on a parallel replication stack. Critically, **none** of
  these do the **authorization layer** (whose ops count, cert windows, revocation) — that
  cert-gated merge is ours, and it's the actually-novel part.

**Ship gate:** a second device edits a playlist; both devices + a friend converge on the
merged state with the box down; revoking a device drops its future writes. Naming + data
both peer-served (rides Phase 1).

### P5 — Friends 🟡

- **Two relations.** `friend` (mutual, consented, symmetric — gates DM, rich
  presence, the β-floor) and `follow` (asymmetric, no consent — gates the DJ feed).
  Distinct consent semantics; don't collapse them. Records are signed statements
  between `Ik`s, persisted in `friends.json`.
- **Request protocol** over request_response + the **tracker blind-mailbox**
  (requests encrypted to `Ik_B`; the tracker is a dumb store, blind to contents and
  graph) for offline delivery. **Structural anti-spam:** you need someone's OOB
  friend-code to request them → no enumeration, no cold-spam, for free.
- **Verified vs unverified** trust levels (fingerprint confirmed in person via
  QR/SAS, à la Signal). Show a badge; matters once friends gate sensitive features.
- **Self-sync across your own devices.** Model the friend set (and block list) as an
  **OR-set CRDT** gossiped among your device group (encrypted to your devices), so
  friend-on-phone / unfriend-on-laptop merges deterministically.
- **Web-of-trust introductions** (A vouches B to C) — anticipate the signed-record
  shape, ⚪ defer the feature.

### P6 — Social on the graph 🟡/🟢

- **Friend presence — peer-to-peer, encrypted; the tracker stays blind.** Rich
  "now-playing" must NOT go through the tracker (it would have to learn your graph +
  activity). Push it directly to friends — and since **friends occupy the β
  presence-floor warm slots (PEER-ASSIST §2.3), you're already connected to them** —
  or to a friend-scoped topic encrypted to the friend set. Tracker holds only coarse
  online/offline. This finally gives the β-term a constituency.
- **DM** — sealed-box to the friend's x25519 subkey (v1: static-key sealed-box, no
  forward secrecy; ⚪ defer the double-ratchet).
- **Per-track chat — gossipsub** (the *second* behaviour, and the justified pubsub
  use case: many writers, real-time, ephemeral). Topic = the track's root CID (a
  content-addressed room id, no registry); the mesh bootstraps from the tracker's
  **Interest table** + PEX + warm set (no DHT discovery). StrictSign authenticates
  senders by device key → attributed to identity via the cert chain. Subscribe only
  to the playing/queued track; unsubscribe on move (mesh cost ∝ active listening).
  Subscriber set = free "who's listening now" presence. **Honest hard parts:** no
  central moderation (mitigate with peer-scoring + per-identity block, accept you
  can't delete content) and no scrollback (ephemeral, like eMule — or a separate
  persistence design later).
- **Shared queue** — start with the **DJ/follow model** (one writer broadcasts queue
  + track to followers; trivial over pubsub). ⚪ Defer the collaborative
  (sequence-CRDT) and synchronized-playhead variants.
- **Friend-gated relay** — prefer relaying for friends / friends-of-friends; the
  web-of-trust throttles the R2 relay-abuse problem without a central authority.

---

## Browser tier ⚪/🟢 (gated on R2 + P4)

Browsers **can't listen** — no sockets; reachability is rented from a relay.
Transports are dial-out (WSS, WebTransport, WebRTC-direct) plus **browser↔browser
WebRTC**, which is *relay-brokered*: common relay → reservation → relayed signaling
channel → SDP/ICE/STUN hole-punch → direct datachannel. Consequences:

- Browsers can't be relays and **can't connect to each other without a public
  signaling relay** — so they deepen, not relieve, the dependency on
  publicly-reachable nodes. A browser tier makes R2's community relays a **hard
  requirement**, not a nice-to-have.
- Our nodes aren't browser-reachable today (TCP+QUIC only). The **master (kubo)** is
  the easy first entrypoint via **WebRTC-direct** (certhash, no CA cert). Making rust
  desktop peers browser-dialable is more work.

Great for zero-install reach; weakest tier for both-boxes-dead. Slots in after R2
(relay substrate) and P4 (identity for social).

---

## Recommended order

```
Phase 0 ─→ Phase 1 ─→ ┌─ R2 ✅ ─→ R1 ✅ (catalog→IPNS) ─→ R4 ✅ (Go everywhere: one tsnode binary; kubo + rust-ipfs dissolved)
                      │                                   └─ R3 (fan archive across peers; R1-gated, independent of R4) ⏸️ ON HOLD (0-peer overlay)
                      └─ P4  (in parallel: cheap, non-retrofittable, unblocks P5/P6) ─→ P4.5 ─→ P5 ─→ P6
Browser: whenever reach matters, after R2 + P4.
```

**R4 is shipped** — the box and client are now one `tsnode` (go-libp2p + boxo) binary; kubo and
rust-ipfs are both gone, the Node tracker + HTTP API are fully dissolved, and the custom DHT +
IPNS-over-gossipsub that the old plan chased are native (no two-stack interop, no vendor patches,
no Bitswap-at-scale unknown — boxo *is* what kubo is built from). The next resilience step *would*
be **R3** — fanning the cold archive across peers (the one remaining lever on the §0 irreducible
gap) — but it's **on hold (2026-06-29)**: the master now runs a private `:5478` overlay with ~0
peers, so replication has nowhere to land and would cost more than it buys. Revisit once there's a
real peer population. Until then the master-pinned, GC-disabled blockstore + backups are the
availability floor. **Run P4** as the actual do-now — small, non-retrofittable, unblocks the product
track, no shared critical path. The live follow-ups on R4 itself are operational, not architectural:
the desktop release/codesign pipeline, AutoNAT hysteresis, public-connection decay (now resolved by
the port move — public scanners shed, conns dropped ~2100→0).

---

## Settled design decisions (don't relitigate)

- **Tracker over DHT/pubsub for discovery.** Coordination is cheap, bytes are
  expensive; the tracker gives **presence** a content-DHT can't, keeps the client
  thin (no G1 crawl), and stays additive (master keeps its public role). The DHT's
  only edge — guaranteed reach to rare content with no central index — is a
  degraded-mode-only slice covered by tracker (up) + PEX (down), at far lower cost.
- **pnet is out; custom-kad is the private-DHT option — now SHIPPED, native.** pnet's
  transport-XOR is fundamentally incompatible with QUIC and not patchable; it also
  costs the master its public role. Custom-kad-protocol-id keeps QUIC and the seeder
  model. **Realized in R4 (not a milestone, just how tsnode is built):** the master
  speaks `/trackerstream/kad/1.0.0` as a first-class routing node and routing tables admit
  only custom-protocol speakers, so the overlay is trackerstream-only by construction — no
  kubo fork, no clients-only overlay. Routing and replication were always separate concerns;
  the old kubo constraint was the only thing that bundled kad into R3.
- **Pubsub: doorbell-push for IPNS, gossipsub-mesh for chat.** Still settled: don't run a
  heavy push *mesh* for a once-per-rebake record. **Refined by R1, realized by R4:** the
  per-search *resolve* round-trip turned out to be the real cost — on the read side, not the
  write side — so the Phase-1 **doorbell** (a low-rate record push on an app-specific topic,
  bumped only on rebake) is the right tool, and **R4 made it native**: the `tsnode` master signs
  in-process (boxo `ipns`) and publishes to a custom catalog topic; clients hold the record
  push-style and never resolve per query. That is *not* the heavy many-writer mesh — that one is
  still reserved for the first frequently-changing multi-writer object, i.e. **per-track chat
  (P6)**, not the catalog. Same record/identity primitives; transport chosen by workload.
- **Relay ability is relational** (live common neighbors), not a node flag.
- **Sign device certs; never copy `Ik`.** Preserves revocation.
- **User data is per-device CRDT feeds, merged — not a shared writable name.** An IPNS
  name is 1:1 with a key and you never copy `Ik`, so each device owns a feed under its own
  key; readers merge (cert-gated). Use a transport-agnostic CRDT (`automerge`/`loro`) over
  the Phase 1 plane — not a parallel replication stack (iroh/p2panda/willow). The
  cert-gated merge is ours. See P4.5.
- **Rich presence is friend-to-friend encrypted; the tracker stays blind** to graph
  and activity.
- **Bitswap is single-hop.** Discovery exists to make the holder a direct neighbor;
  there is no multi-hop block routing to lean on.

## Open questions

1. Relay supply in a NAT-heavy population — measure before flipping the master relay
   off; community public relays may be required, not optional.
2. Forward secrecy for DM — static sealed-box now; double-ratchet when it matters.
3. Multi-device key hygiene — owner devices hold `Ik` (larger compromise surface);
   when to move to a cold root + delegated per-device signing keys (Matrix 3-tier).
4. Chat moderation + persistence — accept ephemeral/unmoderated for v1, or design a
   pinned-log + reputation layer?
5. Self-sync transport for the friend OR-set — same request_response channel, or a
   dedicated device-group topic?
6. User feeds (P4.5): must they stay live with **no owner device present**? Short-`notAfter`
   certs (owner must reappear to renew) vs. explicit revocation records (more machinery,
   owner-optional).
7. User feeds (P4.5): playlist concurrency — do two devices ever edit the **same** playlist
   simultaneously (→ sequence-CRDT now), or is it different-device-different-time
   (→ OR-set/LWW is enough for v1)?
8. ~~Catalog→IPNS (R1): chunk stability across rebakes vs. needing shards?~~ **Answered (R1.3):**
   page-aligned **dag-pb** leaves (16 KB, `page_size=16384`, no VACUUM) keep unchanged pages
   byte-stable; a query touches a handful of pages. (`--raw-leaves` is *out* — the vendored
   rust-unixfs visitor can't walk raw leaves.) No shards needed at current scale.
9. ~~Catalog→IPNS (R1): is Bitswap per-page latency low enough to make the lazy VFS the frontend
   default?~~ **Answered (R1.4):** yes — with concurrent prefetch + page cache the lazy VFS is the
   default and the HTTP `/catalog` routes are hard-cut. Broad search ~8 s cold / ~80 ms warm.
10. ~~**R4 (the gate):** does connexa's Bitswap-serving + blockstore hold up at ~100× the corpus,
    i.e. can a Rust master replace kubo as the **seeder**?~~ **Moot — the Go-everywhere pivot
    dissolved it.** R4 didn't port the seeder onto connexa; it built `tsnode` on **boxo**, the
    exact data layer kubo is built from, so seeder/blockstore scale is battle-tested by
    construction. The "Rust master at scale" unknown no longer exists.
11. **R4 containment (still worth a periodic empirical check):** the custom DHT prefix keeps the
    routing table trackerstream-only by construction, but confirm no subsystem dials outward on
    its own. Current observation post-cutover: public IPFS nodes *connect inbound* to the
    well-known PeerId/port (they're in the libp2p peerstore) but are **excluded from the DHT
    routing table** and never exchanged via PEX/presence. For *true* network privacy (not just
    routing isolation) a PNET/new-identity pass is the lever — deferred; public inbound
    connections are currently left to decay.
