# trackerstream — peer-assist plane (design + plan)

**Goal.** Offload the master node by letting clients fetch cached blocks from
**each other**, and lay the connection-management backbone for future
peer-to-peer interactions (presence, social) — without rejoining the public DHT
(the G1-crawl failure mode) and without touching the master's public-IPFS /
Filecoin compatibility.

This supersedes the abstract "B1–B4" framing in `MVP-FOLLOWUP.md §G`. The
mechanism we land is **B3 (tracker / rendezvous), reframed presence-first**, with
bitswap unchanged as the data plane.

Status legend: ▶️ ship-first · 🟡 needed for full offload · 🟢 hardening / later ·
⚪ explicitly out of scope.

---

## 1. Where we are today (and the three gaps)

The pieces for peer-assist mostly exist; they're just not wired to each other.

**Already true (the substrate):**

- **The client blockstore IS a persistent cache.** `start(data_dir)` is
  fs-backed at `app_data_dir()/ipfs` (`ipfs.rs:218-219`, `lib.rs:227`). Every
  block fetched stays — no GC, no eviction, no size cap, survives restarts,
  cross-module dedup falls out of content addressing. **A client that streamed a
  module genuinely holds those blocks on disk.**
- **Bitswap is BitTorrent's piece exchange.** Wantlists = have/want bitfields,
  block responses = piece transfer, partial seeders work natively. The hard part
  is already built — and the client runs the bitswap **server** too, so it can
  serve what it holds.
- **NAT traversal substrate is live.** Master kubo runs **circuit-relay v2** +
  **coturn STUN/TURN** (`install.sh:72`); the client has relay-client + DCUtR +
  AutoNAT on (`ipfs.rs:245-246`). NAT'd holders are reachable and relayed conns
  can be upgraded to direct.
- **A persistent master link exists.** `keepalive_master()` (`ipfs.rs:338-358`)
  holds the master connection open with reconnect — this is also our natural
  tracker/heartbeat transport.
- **Per-peer accounting + a peers pane** already exist (`45289b3`:
  `peer_stats`, `peer_bandwidth`, `PeersPanel.svelte`) — our offload telemetry is
  already on screen.

**The three gaps (all in scope):**

1. **No discovery.** The client node is deliberately thin — identify + bitswap +
   ping, **no kademlia, no pubsub** (`ipfs.rs:225-229`). It only knows the
   master. Clients never learn that other clients exist, so B has no path to A's
   cache.
2. **The fetch path is hard-pinned to the master.** `fetch_bytes()` does
   `get_block(cid).provider(master_provider())` (`ipfs.rs:362-371`). **Even if B
   were connected to a holder, the request is addressed to the master by
   design.** This is explicitly in scope — see §4.
3. **No connection policy.** Nothing decides *which* peers to connect to or
   *when*. There is no warm-set, no roster.

---

## 2. Architecture — one control plane, unchanged data plane

### 2.1 Data plane: bitswap over a warm peer set; master demoted to fallback seed

No new data protocol. Bitswap already **broadcasts wantlists to every connected
peer**, so the entire offload reduces to: *be connected to a holder before you
fetch.* The master stops being the default provider and becomes the
seed-of-last-resort that backfills whatever the swarm can't.

### 2.2 Control plane: a tracker on the master, presence-first

A coordination service in the existing public HTTPS server (`apps/server`,
`createApi` at `api.ts:36`, already fronted by Caddy). It holds **two tables** —
presence as the foundation, content-interest as an index into it:

```
Presence  (the backbone):     peerId → { addrs[] (incl. relay/circuit), digest, lastSeen, online }
Interest  (the offload view):  rootCID → Set<peerId>     // derived from each peer's announced roots
```

Centralizing *coordination* is the correct BitTorrent split, not a compromise:
the expensive thing we're offloading is **shipping module bytes** (that moves to
the swarm); the cheap thing (tiny JSON announces/queries) stays on the
always-on box. It also avoids the public-DHT crawl entirely.

Presence-first is the key call: a pure per-torrent tracker (`rootCID → peers`
only) has no concept of "peer is online" independent of a download. By putting
presence underneath, **the same service that offloads the master today becomes
the peer-directory the social layer needs tomorrow** — social features become new
*queries*, not new infrastructure.

**Endpoints (BitTorrent-shaped):**

| Endpoint | Purpose |
|---|---|
| `POST /announce` | register presence + dialable addrs + held-roots; doubles as heartbeat |
| `GET /peers?root=X` | exact holders of X (bounded random subset, ~50) — queue pre-connect + miss fallback |
| `GET /candidates` | roster slice with per-peer **roots digest** — for warm-set affinity scoring |
| `GET /roster` | pure presence (who's online) — for the social backbone (🟢 later) |

All four are views over the same two tables.

### 2.3 The client's heart: a scored warm-set manager

The client maintains a **bounded** set of warm connections (target ~20–50, with
scored eviction like a conn-manager — *not* a DHT crawl, so churn is controlled
because every candidate comes from the tracker, never from crawling). This single
component serves all goals at once. Each roster candidate is scored:

```
score(peer) = α · content_affinity    // might have what I'll want   (predictive offload)
            + β · presence_value       // worth a link with zero overlap (social backbone)
            + γ · reachability         // direct-dialable > relay-only
```

"Connect to peers that **might** have the mutual download" and "connect to peers
**without** any mutual download" are the α-term and the β-term of one function. A
**presence floor** (reserved β budget) guarantees the social backbone is never
starved by a heavy download session.

### 2.4 The "might have it" predictor — active, not reactive

We do **not** wait for a cache miss. Two mechanisms, in priority order:

1. **▶️ Queue-driven pre-connection (highest value).** When a root enters B's
   *queue* — not when it plays — B calls `GET /peers?root=X` and warms those
   connections immediately. By the time playback reaches it, the swarm is
   pre-formed and the master is bypassed **with no query-then-dial latency on the
   critical path.** This is the active-connect behavior, driven by the strongest
   possible signal: what B is about to play.
2. **🟡 Affinity-driven membership (general case).** `GET /candidates` returns a
   compact **roots digest** per online peer (a Bloom filter / set sketch — the
   cross-module analog of BitTorrent's per-torrent bitfield). B scores overlap
   between each digest and its own interest model (queue + recently played +
   browsing context) and warms high-overlap peers *before* any specific request —
   "this peer's cache looks like my future."

Exact-CID `GET /peers?root=X` also serves as the **miss fallback**: if B wants X
and no warm peer has it, query + dial on demand. The warm set should make that
rare.

---

## 3. When peers connect (the trigger model)

| Trigger | Source | Effect |
|---|---|---|
| Startup | client boots | register in roster, warm the presence floor |
| Root enters queue | player | pre-connect to that root's holders (▶️) |
| Affinity refresh | timer + queue/play events | re-score `/candidates`, adjust warm set |
| Cache miss | fetch with no warm holder | on-demand `/peers?root=X` + dial (fallback) |

Membership is continuous and proactive, not per-fetch-lazy. The warm-set manager
is event- **and** timer-driven.

---

## 4. The master-provider change (explicitly in scope)

Today `fetch_bytes()` (`ipfs.rs:362-371`) hard-pins `.provider(master)` on every
block. This is **gap #2** and must change:

- **Demote, don't delete.** Keep the master as *one* provider / fallback so a
  block no warm peer holds still resolves. But stop making it the default — let
  the connected swarm answer first via normal wantlist broadcast, master
  backfills the gaps.
- **Connect-is-enough.** Because bitswap broadcasts wantlists to all connected
  peers, simply having warmed a holder before the fetch is sufficient; we likely
  don't need explicit per-peer `.provider()` hints for swarm peers — to verify in
  Phase 2.
- **Partial seeders just work.** A v2-pruned streaming client that only cached
  *streamed slots* answers the blocks it has; the master covers the rest. (Tie to
  the `complete: false` announce flag so B can prefer full seeders.)

Both `reassemble()` (full-load, `ipfs.rs:399`) and the v2 streaming path
(`ipfs.rs:554-571`) go through `fetch_bytes`, so this one change covers both.

---

## 5. The relay-bytes trap (the one way this silently fails to offload)

If a NAT'd holder A can't be DCUtR-upgraded to a **direct** connection, B↔A
bitswap traffic falls back through the master's **circuit relay** — i.e. the audio
bytes *still flow through the master*, just as relay instead of bitswap. That is a
non-offload dressed up as an offload.

Mitigations (Phase 2):

- Measure DCUtR upgrade success in the 2-client test before trusting the offload.
- **Score reachability** (γ): prefer direct-dialable holders; treat relay-only
  holders as low-value for *content* (still fine for presence).
- Cap or avoid pushing large block volume over relayed connections; if a holder
  stays relay-only, prefer the master directly (same path, fewer hops).

---

## 6. Build order

**Phase 1 — Tracker MVP (server) ▶️**
- Presence + Interest tables + `POST /announce`, `GET /peers` in `apps/server`.
- Client announces held roots over the existing keepalive connection; heartbeat
  to stay in the roster.

**Phase 2 — Offload path (client) ▶️🟡**
- Warm-set manager (bounded, scored) with **queue-driven pre-connection**.
- **Demote the master provider hint** in `fetch_bytes` (§4); verify bitswap pulls
  from the warmed peer (2-client test: peers pane should show B↔A traffic, not
  B↔master).
- Relay-bytes guardrail (§5): confirm DCUtR upgrades; score reachability.
- *This phase alone delivers most of the offload + the active-connect behavior.*

**Phase 3 — Predictive membership 🟡**
- `GET /candidates` + per-peer roots digest; affinity scoring into the warm set.
- `complete` flag so partial seeders are deprioritized vs full holders.

**Phase 4 — Social backbone 🟢**
- Presence floor (β) + `GET /roster`; exercise with a first non-content
  interaction to prove the backbone holds with zero content overlap.

**Deferred ⚪/🟢**
- Signed peer records / libp2p-rendezvous (replaces self-reported addrs, kills
  spoofing) — required before any social *identity* rides on presence.
- Tit-for-tat / choking — only matters once the master isn't a free always-on
  seed.
- Decentralizing the tracker (gossip-of-haves) — only if the central tracker
  ever becomes the bottleneck. *Same path later carries signed IPNS records (§9)
  for master-down catalog resolution.*
- **IPNS record distribution** (`GET /ipns/<key>` serving the master's latest
  signed record) — small add to the tracker; ship alongside the catalog→IPNS
  migration so resolution stops being master-pinned (§9).

---

## 7. Open decisions

1. **Warm-set budget:** hard slot split (e.g. 30 affinity + 10 presence) vs. a
   single blended score with a presence floor. *Lean: blended + floor* — simpler,
   and the floor guarantees the social backbone never starves.
2. **Interest-model source:** queue + recent plays (cheap, ship now) vs.
   eventually a recommendation signal. Architecture-neutral — it's just whatever
   feeds the affinity score.
3. **Trust:** self-reported addrs are fine for a public-domain-music MVP. Signed
   records are the real fix and must precede social-identity features.
4. **Tracker host:** the TS HTTP server (simplest, already public, BitTorrent
   trackers are literally HTTP) vs. a libp2p-native rendezvous behaviour on the
   master (signed records for free, but needs the `with_custom_behaviour` hook).
   *Lean: HTTP server first, rendezvous as the clean-up upgrade.*

---

## 8. What does NOT change

- The **master** stays a vanilla public kubo node (DHT server, provider=roots,
  Filecoin/public-pinning compatible). It is never enrolled in the client
  overlay — the overlay is purely client↔client, addressed via the tracker. This
  is what kept Option A (pnet) off the table and what makes this additive.
- The **client node** stays thin (no kademlia, no pubsub). Discovery comes from
  the tracker, not from crawling — so the G1 failure mode cannot recur.
- The **data format** (manifests, v2 DAGs, blockstore) is untouched.

---

## 9. Mutable objects — the IPNS catalog (forward compatibility)

Future plan: the module catalog (today HTTP-served JSON from `apps/server`)
becomes an **IPNS object** — a mutable `/ipns/<masterKey> → /ipfs/<catalogCID>`
pointer the master republishes when the corpus changes. Question: does the
peer-assist plane help peers recover the catalog (and other master-pinned IPNS
objects) without the master? **Yes for the bytes immediately; yes for the name
with one small addition.** IPNS is two separate things and peer-assist treats
them differently — so split it:

**Content (the CID's DAG): yes, for free.** Once the catalog is a
content-addressed DAG, its root is *just another root*. Clients cache its
blocks, announce it in `POST /announce`, and serve it over bitswap exactly like
module data — no new infra. In fact the catalog is the **hottest** root in the
system (everyone fetches it), so peer-assist is *most* valuable here. The
presence/interest table even yields a live **replication count** for the catalog
(how many online peers hold it) — a health signal we don't have today.

**Naming (name → current CID): not for free — but this is its right home.** IPNS
*resolution* normally rides the DHT or IPNS-pubsub, both of which the thin client
deliberately disables. So resolution is, like the block fetch (gap #2),
implicitly **master-pinned today**. The fix is small and lands on infra we're
already building:

- **Serve the signed record off the tracker.** `GET /ipns/<key>` returns the
  master's latest **signed** IPNS record (tiny — same shape as an announce). The
  client verifies the signature itself, so the tracker is a *cache, not a trusted
  authority*. Resolution becomes master-independent **at the tracker** — and the
  tracker is cheap and replicable.
- **Self-certifying ⇒ safe to gossip.** IPNS records are signed by the publisher
  key and carry a sequence number + validity window, so newest-wins is
  well-defined and **no peer can forge a newer catalog** (it can't sign). That
  makes the **presence backbone the natural substrate to gossip records
  peer-to-peer** later (the deferred "gossip-of-haves" path, now also carrying
  IPNS records) — true master-down resolution. Residual risk is only
  *downgrade/withholding* (serving a stale-but-valid record), bounded by short
  record EOL + cross-checking the master/tracker when reachable — **never
  forgery.**

**Trust note — better than presence.** Catalog distribution has *stronger* trust
properties than the peer-presence layer: presence relies on self-reported addrs
(§7.3, needs signed records eventually), whereas IPNS records are self-certifying
out of the box. So mutable-object recovery is safe to ship over an untrusted
tracker and untrusted peers from day one — it does **not** wait on the signed-
peer-records work.

**Collaborative pinning (bonus).** Because the catalog is small, hot, and
universally wanted, clients can voluntarily **pin** it (not just opportunistically
cache), turning the swarm into N replicas of the master's pinned set — a
poor-man's collaborative-pinning / Filecoin-lite for the objects that matter
most. The presence table gives the live replication factor, so the master can
alert if it ever drops below a floor.

**Net:** the bytes are recoverable peer-to-peer *immediately* (catalog = just
another root); the *name* becomes master-independent with one small addition
(`GET /ipns/<key>` serving the signed record) and fully decentralizes later via
the same presence-gossip path. Same two tables, same backbone — **mutable objects
are a new query, not new infrastructure**, exactly like the social layer.
