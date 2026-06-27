# trackerstream — P2P next steps (roadmap)

**North star.** The single box (master kubo + `apps/server` tracker, same machine)
becomes a **seeder + bootstrap, not a dependency.** Resilience comes from the
swarm, not from infrastructure redundancy — because *one box is a feature*
(copyleft, one-command deploy, cheapest possible to run). The target is **zero
perceived downtime**: if the box reboots — or disappears entirely — the swarm
self-heals and keeps serving everything it collectively holds and can reach.

This supersedes nothing in [`PEER-ASSIST.md`](PEER-ASSIST.md); it sequences the
work *after* it. PEER-ASSIST built the tracker + warm-set + IPNS readiness; this
doc is how that hardens into a swarm that outlives its origin.

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
   ├── Track R (resilience):  R2 reachability ─→ R3 floor/scale        ◀ YOU ARE HERE (the fork)
   │
   └── Track P (product):     P4 identity ─→ P5 friends ─→ P6 social   ◀ run alongside R2
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
>   dormant until that **catalog→IPNS migration** lands server-side (publish with ~24–48h
>   validity). PEX + `Peers` go live the moment clients update.
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

### R2 — Reachability: peer-provided relay 🟡 (the hard, long-pole leg)

A perfect DHT that says "peer Y holds X" is useless if Y is NAT'd and your only
relay was the master. This leg is what makes both-boxes-dead *fetchable*, not just
discoverable — and it's the genuinely hard part.

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
- **Demote the master relay, gate full-off on telemetry.** Phase 0 already clamped
  it to handshake-only. Now instrument **peer-relay coverage + DCUtR upgrade rate**;
  flip the master relay fully off only when the data shows coverage holds across the
  real (likely NAT-heavy) population. If supply is thin, keep it clamped longer —
  the robust fix for a NAT-heavy swarm is **a couple of community-run public
  relays** (stateless, opt-in, copy-catable — the relay analog of federated
  trackers; the "dedicated relay infra" deferred in MVP §F).

**Ship gate:** NAT'd peers reach each other through peer relays without the master;
the peers pane shows B↔A traffic, not B↔master. The offload is finally *genuine*.

### R3 — Cold-content floor + structured discovery 🟢 (demand-driven)

- **Collaborative pinning.** Voluntarily pin hot/at-risk roots — **catalog first**
  (hottest root, self-certifying) — onto clients as replicas. Sets the floor on what
  "zero downtime" can mean (PEER-ASSIST §9). The presence table yields a live
  replication factor; alert if it drops below a floor.
- **Structured discovery, only if scale demands it.** PEX is neighborhood-bounded:
  great for popular roots, can miss a *rare-but-existing* root outside your horizon
  during a box outage. If/when that bites:
  - **First:** rendezvous-hashing over the PEX-gossiped roster (hash root → K
    responsible peers who index its holders) — a one-hop "DHT-lite" reusing
    membership you already have. Right for thousands of nodes.
  - **Only at millions:** a **custom-kad protocol id** (`/trackerstream/kad/1.0.0`)
    for O(log n) routing. Note: this is the private-DHT option that **keeps QUIC**
    (it's a routing-layer protocol string, not pnet's transport-layer XOR — pnet
    and QUIC are fundamentally incompatible and not patchable). The master stays a
    seeder, so it needn't speak the custom protocol; no kubo fork needed. Clients'
    routing tables hold only `/trackerstream/kad` speakers → trackerstream-only by
    construction, no crawl, no G1.

**Ship gate:** catalog + hot content survive even cold; rare-content discovery has a
structured fallback when the swarm is large enough to need one.

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
Phase 0 ─→ Phase 1 ─→ ┌─ R2  (start first: hardest, long pole, north-star gate) ─→ R3
                      └─ P4  (in parallel: cheap, non-retrofittable, unblocks P5/P6) ─→ P5 ─→ P6
Browser: whenever reach matters, after R2 + P4.
```

**The one genuine fork is yours:** after Phase 1, lead with **R2 (resilience)** or
**P4→P5 (product)**? Lean: start R2 because it's the hardest and the north star
depends on it — but run **P4 alongside** since it's small, non-retrofittable, and
unblocks the entire product track. That way neither blocks, and you never ship a
feature that forces an identity migration.

---

## Settled design decisions (don't relitigate)

- **Tracker over DHT/pubsub for discovery.** Coordination is cheap, bytes are
  expensive; the tracker gives **presence** a content-DHT can't, keeps the client
  thin (no G1 crawl), and stays additive (master keeps its public role). The DHT's
  only edge — guaranteed reach to rare content with no central index — is a
  degraded-mode-only slice covered by tracker (up) + PEX (down), at far lower cost.
- **pnet is out; custom-kad is the private-DHT option if ever needed.** pnet's
  transport-XOR is fundamentally incompatible with QUIC and not patchable; it also
  costs the master its public role. Custom-kad-protocol-id keeps QUIC and the seeder
  model.
- **Pubsub: pull + doorbell for IPNS, gossipsub for chat.** Push-mesh is wrong for a
  once-per-rebake record (standing cost > rare poll) and right for many-writer
  real-time chat. Same record/identity primitives, transport chosen by workload. The
  trigger to add gossipsub is the first frequently-changing multi-writer object —
  which is per-track chat, not the catalog.
- **Relay ability is relational** (live common neighbors), not a node flag.
- **Sign device certs; never copy `Ik`.** Preserves revocation.
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
