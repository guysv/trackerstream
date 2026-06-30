# trackerstream — validated block-forwarding (design)

**Goal.** Let publicly-reachable peers carry bulk for NAT'd peers **without running an
open relay.** Replace the donor's circuit-relay bulk-byte path with a **public,
content-addressed forwarding protocol** that can only ever move verifiable blocks —
never an opaque tunnel — while keeping the clamped stock relay for DCUtR coordination.

This supersedes the bulk-byte half of [`P2P-NEXT-STEPS.md`](P2P-NEXT-STEPS.md) **R2**.
R2's reachability + dual-role-relay framing stands; this doc replaces *how the bytes
move once two NAT'd peers can't go direct.*

## The hole we're closing

The Go rewrite (R4) reached for the stock primitive: reachable clients run
`EnableRelayService(WithInfiniteLimits())`. That is the **standard** circuit-relay hop
(`/libp2p/circuit/relay/0.2.0/hop`) — open to *any* libp2p peer (our custom DHT prefix
gates content routing, not the relay), forwarding **end-to-end-encrypted transport** the
donor can neither see nor validate, with bytes unbounded (any finite `RelayLimit` flags
the conn `Limited` and bitswap won't traverse it — so "bounded bulk relay" is not a thing
circuit relay can express). Net: a user who just wanted to seed a music network becomes a
free, opaque, unbounded proxy for the libp2p internet. That is the property we reject.

## The decision

Two relays, cleanly split — the same dual-role boundary R2 drew, made real for bulk:

1. **Clamped stock relay — kept, open, coordination-only.** `EnableRelayService()` (default
   128KiB/2min). Bytes stay `Limited` → bitswap refuses it *by design*; it exists solely as
   the always-reachable **DCUtR rendezvous** and as the thing AutoRelay reserves against.
   No ACL — an open clamped relay is not abusable (it can't carry bulk).
2. **`/trackerstream/fwd/1.0.0` — new, public, the bulk path.** A donor fetches a **CID** on
   a caller's behalf and streams back the **hash-verified block**. Content-addressed
   store-and-forward, not a tunnel.

### Why it's safe without gating

Safety is **structural, not an allowlist.** A caller can only ever receive bytes `B` where
`CID(B)` equals the CID it asked for — there is no way to make the donor carry an arbitrary
live stream, a non-bitswap protocol, or anything it can't name and verify. So *who* calls
it is irrelevant: **the protocol is public** (no ACL, no peer identity check — that whole
"is this an app peer" problem disappears).

Honest residual: content-addressing does not make arbitrary-data-*moving* impossible — two
colluding peers could chunk a payload into raw blocks and shuttle it through a donor. But
that's degraded from an open relay in every way that matters: store-and-forward of
size-capped chunks (no live socket, no arbitrary protocol), the donor sees every CID (can
rate-limit, log, cache-dedupe), and it's bounded by a **per-peer rate cap** — the
compensating control that scope-restriction would otherwise have provided. The abuse case
collapses from "open proxy for the internet" to "two consenting peers store-and-forward
blocks through a middle peer" — i.e. what a CDN does.

## Architecture

```
Holder H (NAT, has blocks)                         Fetcher F (NAT, wants CID)
   │ AutoRelay-with-peer-source:                     │ direct bitswap to connected peers
   │ stock HOP RESERVE on reachable donor R          │ misses (holder not connected to F)
   ▼ (reservation keeps H↔R conn ALIVE)              ▼
Donor R (public)  ◀───── /trackerstream/fwd Fetch{cids} ───────┘
   • clamped stock relay (open) — DCUtR rendezvous + AutoRelay target
   • fwd server:  for each cid → blockservice.GetBlock(cid)   ← rate-capped, timeout-bound
        - local blockstore hit → serve
        - else bitswap broadcasts WANT to connected peers (incl. reservation-kept H)
        - bitswap verifies CID==hash → cache (short TTL) → stream block to F
```

The keystone: **bitswap already routes wants to every connected peer.** Once H's reservation
keeps the H↔R connection alive, R's ordinary `GetBlock` reaches H with no new routing, no
holder registry, no "who holds what" bookkeeping. The forwarder is, in essence, **transitive
`GetBlock` exposed over a stream, rate-capped and cached.**

**Who forwards: public client donors only — never the master.** The seed serves its *own* pinned
content via bitswap (clients connect and pull directly), but it does not register the forwarding
handler and never proxies peer-to-peer user traffic — consistent with "the master is a seeder +
bootstrap, not a dependency." The handler is registered on clients and gated on AutoNAT-confirmed
public reachability, so only public client donors carry forwarding (the same gate as the
infinite-limit relay activating); the master is never a donor and never advertises the rendezvous.

## Riding HOP

We can't share the stock relay's reservation *object* (the `rsvp` table is unexported; only a
fork shares it) — but we don't need to. We ride the reservation *flow*: `EnableAutoRelayWith
PeerSource` makes a NAT'd holder send a **stock HOP RESERVE** to reachable donors (not just the
master), and that reservation's kept-alive connection is exactly what the forwarder's bitswap
rides. One reservation, two beneficiaries (the holder's own circuit addr + the donor's reach to
it), zero forked code, **zero ACL**.

## Wire protocol — `/trackerstream/fwd/1.0.0`

Length-delimited, mirroring the Phase-1 `request_response` style:

- **`Fetch { cids: [Cid] }`** — caller → donor. A batch of wanted CIDs (the caller's
  outstanding bitswap wants).
- Response stream, one per cid:
  - **`Block { cid, data }`** — verified block bytes.
  - **`Miss { cid }`** — donor couldn't source it within the timeout.
  - **`Busy {}`** — rate cap exceeded; caller backs off.

Donor handler:
1. **Rate cap** — per-peer token bucket (bytes/s + reqs/s, keyed by the stream's remote
   peer ID, *no allowlist*) + a global concurrent-transitive-fetch semaphore. Over → `Busy`.
2. **`blockservice.GetBlock(ctx, cid)`** with a short timeout — local hit, else bitswap over
   connected peers. Bitswap enforces `CID == hash(block)`.
3. Hit → **cache** in a bounded, short-TTL store (NOT pinned → GC reaps it) → `Block`.
   Miss/timeout → `Miss`.

Caller (fetcher) side: the fallback is not bolted onto individual RPC handlers — it wraps the
**bitswap exchange** itself (`fwdExchange`), so *every* blockservice fetch gains it transparently:
single (`block/get`), batch, and **session** fetches. That last one matters: the catalog DAG walk
fetches via `merkledag` `GetMany` → a blockservice session → `exchange.NewSession`, which bypasses
`BlockService.GetBlock` — so a per-handler or BlockService-level wrap would miss the catalog. The
exchange is the one chokepoint that covers track blocks *and* catalog pages with no per-call wiring.
The donor's own transitive fetch passes a no-forward context so it stays on raw bitswap (the
anti-amplification invariant). Never blocks playback.

## Forwarding vs. direct hole-punching (the prioritization)

Forwarding is a **bridge, not a destination** — a direct fetcher→provider connection (formed by
DCUtR after `DialProviders` dials the provider's circuit addr) is always preferred: one hop, no
donor load. The goals are (1) **hole-punching takes over as soon as it succeeds**, and (2) **don't
pay any latency up front** — a streaming app cares most about TTFB, so the root block should come via
forwarding *immediately* while the hole-punch forms in the background.

Two facts make the *data* takeover automatic and lag-free: DCUtR upgrades the Limited circuit to a
direct connection as soon as it can, and bitswap always prefers the fastest responder — so the
instant C↔provider is direct, bitswap pulls in one hop and wins. The only thing left to control is
**whether to also bother a donor**, and that is gated on the querying node's own connection state
(no timer, no EWMA):

The assisted exchange runs bitswap and forwarding **in parallel with zero delay**, except the
forwarding arm is **suppressed** when `forwardingSuppressed()` is true:

> suppress ⟺ a discovered content provider is directly `Connected` **and** none is still relay-only
> (`Limited`).

- **Provider relay-only (hole-punch pending) or none known yet (initial window)** → not suppressed →
  forward immediately (grace 0ms). The root block is served via a donor at once.
- **Provider directly Connected** → suppressed → bitswap pulls direct in one hop; donors untouched.
  Because the check reads live `Connectedness`, the flip happens the instant DCUtR lands — zero lag.

Providers are learned from `DialProviders` (the DHT providers of the root). A `Limited`-only provider
keeps forwarding on; `NotConnected` (stale) providers are ignored. Note the suppression keys on
*content providers*, not "any direct peer" — otherwise the donor itself (a direct connection) would
wrongly look like a direct path and suppress forwarding.

## Locked decisions

- **Any CID.** No catalog scoping — safety is content-addressing + rate cap, not provenance.
- **Plain bitswap for R→holder.** No dedicated holder stream; ride the connected-peer set.
- **Short-TTL block cache on the donor.** Dedupe popular forwards; reaped by GC, never pinned.
- **Per-peer rate cap.** The DoS/abuse backstop, identity-free.
- **Public protocol — no ACL.** The relay-era gate is gone.

## Rides vs. new

| Piece | Approach |
|---|---|
| Holder→donor reservation / keepalive | **Ride** stock HOP via `EnableAutoRelayWithPeerSource` |
| Donor reach to NAT'd holder | **Ride** the reservation-kept connection + plain bitswap |
| Clamped coordination relay | **Keep** `EnableRelayService()` (open, unabusable) |
| F→R fetch request | **New** `/trackerstream/fwd/1.0.0` (transitive GetBlock) |
| R→holder fetch | **Reuse** `blockservice.GetBlock` — no new wire |
| Rate cap + short-TTL cache | **New** (small) |
| Donor discovery (peer source) | **New infra** — DHT rendezvous of reachable peers |

## Open question — donor discovery

The one genuinely new dependency: `EnableAutoRelayWithPeerSource` needs a stream of reachable
donor candidates. Reuse the content-providing pattern just shipped — reachable peers re-provide
a well-known rendezvous key on the custom DHT; the peer source pulls from it. (With UPnP now
moving clients into the reachable bucket, donor supply should be non-trivial.) Until donors exist,
forwarding is simply inactive and fetches fall back to the seed serving its own content directly.

## Ship gate

A NAT'd fetcher pulls a block that originated on a **NAT'd holder, through a third public donor,
over `/trackerstream/fwd`** — peers pane shows the bytes as peer offload, not master traffic —
with **no circuit relay carrying bulk** and **no ACL anywhere**. Donor's forwarded-byte rate
stays under its per-peer cap under a synthetic flood.

## Tests

- Three-node: NAT'd holder + public donor + NAT'd fetcher → fetcher gets the block via fwd,
  donor's circuit relay carries zero bulk bytes.
- `Fetch` for a CID no connected peer has → `Miss` within timeout (no hang).
- Rate cap: flood a donor → `Busy` once the bucket drains; legit caller unaffected.
- Cache: second fetch of the same CID served from the donor's cache (no re-bitswap), reaped
  after TTL.
- Forwarded block verifies `CID == hash` (a corrupt holder can't poison the fetcher).
