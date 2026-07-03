# Playlists over IPNS — v2 (inline gossip)

User-created playlists published under per-client IPNS names, with the playlist document
carried **inline in the gossip message** next to its signed IPNS record. No bitswap, no
blockstore, no pinning for playlists: the client's local SQLite is the only durable
store; the Go node is a validating relay with small bounded caches; the seed forwards
gossip and saves records only, never documents.

Trust model unchanged from the catalog: the Go sidecar is an untrusted cache; Rust
verifies every record (rust_ipns) and every document (content hash against the record's
CID) itself.

## Roles at a glance

| Actor | Does | Does NOT |
|---|---|---|
| Author client | signs records under its own IPNS keys; gossips `{record, doc}` together | — |
| Seed/master | subscribes to the playlist topic (mesh hub → forwards), keeps a **bounded LRU of records only** | store playlist documents; touch the DHT for playlists |
| Syncing client | verifies record + doc hash, stores the doc in `playlists.db`, re-announces held playlists (suppressed), evicts LRU past a byte budget | trust the sidecar; fetch anything over bitswap |

## 1. Naming and document format

**One IPNS name per playlist.** Keypair in the node keystore, key name
`playlist-<uuid>` (`key/gen` exists: `node/rpc.go:41`). The base58 PeerId of the key is
the playlist's global id. Per-playlist names give independent newest-seq-wins update
streams (`node/ipns.go:57-69`) and tombstone-able deletion.

**Compact document.** User content is potentially unbounded, so the format is arrays,
not objects — roughly 40–60 bytes per track:

```json
{"v":1,"t":"chiptune bangers","ts":[[12345,"aurora.it","Hymn to Aurora"], ...]}
```

Per track: `[catalogId, moduleName, songTitle]` — title and module name are denormalized
so rendering a playlist needs **zero catalog lookups**; the catalog id resolves to a CID
via the normal catalog `get` path **only at play time**. A 100-track playlist ≈ 5 KB;
1 MiB ≈ ~20k tracks.

**Tombstone:** `{"v":1,"del":true}` published at `seq+1`. IPNS has no delete; the
record dies at EOL. On receipt, SEEN-tier copies drop; LIBRARY copies (held/mine) are
preserved **dormant** — we don't delete playlists the user chose to keep. The kept row's
record is cleared (it can never be re-announced, honoring the deletion) and its seq pins
at the tombstone's (replayed older records can't resurrect it); a genuine author
republish at `seq+1` revives it in place.

**Hard cap: 1 MiB of document bytes.** Anything larger is adversarial by definition —
dropped in the topic validator, never stored, never forwarded.

**Self-certification.** The record's value is `/ipfs/<cid>` where `cid` is computed over
the raw doc bytes (raw codec, sha-256). A receiver verifies: record signature against the
name (rust_ipns / `ipns.ValidateWithName`) → `hash(doc) == cid` → doc is authentic. The
doc never needs to be fetchable over bitswap; the CID is an integrity anchor, not an
address. (If a fetch path is ever wanted later, docs >256 KiB would need chunking — noted,
not built.)

**Record lifetime: 168h (7 days)** — an author must come online weekly to keep the
*record* alive; holders keep re-announcing the last signed record until EOL. Catalog
stays at 48h.

**Sequence numbers** are caller-managed (`node/ipns.go:79`). Rust owns the per-playlist
counter in `playlists.db`. **Seq recovery:** before republishing an existing playlist,
resolve your own name (`routing/get`, answered from the node's gossip-warmed buffer) and
use `max(local, buffer) + 1`. With per-device keys the local counter is normally
authoritative; the buffer arm covers a re-imported key whose history has re-announced.
A never-published playlist skips the lookup (fresh key — nothing to recover).

## 2. Gossip layer (Go node)

New topic in `node/config.go`:

```go
PlaylistTopic = "/trackerstream/playlist/1.0.0"
```

**Generalize `node/pubsub.go` to N topics** (today hardcodes one join/subscribe path,
`pubsub.go:38,54-79`). The catalog topic keeps its JSON `catalogMsg`; the playlist topic
uses a **binary envelope** `{name, record, doc}` (varint-framed) — the envelope is
Go↔Go only, and JSON's base64 would add +33% to every hop for fat docs.

**Message size:** `pubsub.WithMaxMessageSize(2 << 20)` so a max-size doc + record +
framing fits (go-libp2p-pubsub default is 1 MiB, `pubsub.go:29`).

Packet-level note: there is **no fragmentation cliff** to engineer around — gossipsub
rides libp2p streams (QUIC forbids IP fragmentation and segments to ~1200-byte packets
with per-packet recovery; TCP segments to MSS). Cost of a message is *linear*: flood
degree (D≈6–12) × duplicate factor (2–4×) × size, and per-hop latency is
store-and-forward (full receive + validate before re-forward). Compactness pays linearly;
single-packet-sized docs buy nothing special. go-libp2p-pubsub v0.16 has gossipsub v1.2
**IDONTWANT**, which suppresses duplicate delivery of large messages — the protocol's own
mitigation for the occasional fat playlist.

**Topic validator — fully self-certifying at first hop.** `RegisterTopicValidator` on the
playlist topic: envelope decodes; doc ≤ 1 MiB; record validates under the name
(`ipns.ValidateWithName`); EOL not passed; `hash(doc) == record CID`. Because the doc
travels with the record, the validator can verify *everything* — invalid or forged
messages die at the first hop and are never forwarded (unlike the catalog path, where
validation happens after forwarding, `node/node.go:259`). Per-peer rate limiting slots in
here later if needed.

**Stores — bounded everywhere:**

- **Seed:** records-only LRU (default 8192 × ~200 B ≈ 2 MB). Docs pass through in
  transit (forwarding) but are never stored — the "seed forwards but doesn't save"
  contract, structurally.
- **Client node:** a byte-bounded ingest buffer of `{record, doc}` pairs (default
  16 MiB, LRU) — a relay buffer for Rust to drain, not a store of record;
  `playlists.db` is the durable copy.
- **Catalog record store gets a cap too** (small LRU, 512): today it is an unbounded
  map (`node/ipns.go:20-25`) fed by an untrusted topic — any peer can mint keys and push
  validly-signed records for made-up names, and every node caches them forever.
  Pre-existing wart, fixed while the bounded-store code is being written anyway.

**Re-announce with suppression (discovery for late joiners).** Gossip is ephemeral, so
holders re-announce. The problem without suppression: every holder re-announcing every
held playlist each cycle costs `holders × playlists × doc size` per cycle — grows with
network size even when nothing changes, and now each redundant announce carries a whole
doc. Suppression = "don't repeat what you recently heard": each node tracks last-seen
time per playlist (at validator/ingest), and on its jittered ~15-minute cycle re-announces
only playlists that went a full cycle unseen. Whoever fires first announces; everyone
else hears it and skips. Steady state ≈ one announce per playlist per cycle
network-wide; a late joiner learns the working set within one cycle. Records past EOL are
dropped, not re-announced.

- Clients re-announce `{record, doc}` from `playlists.db` (Rust-driven, §4/§5 —
  durable data lives exactly once).
- The seed re-announces records-only? No — a record without its doc is unusable (there
  is no fetch path), so the seed does not re-announce at all. It forwards live traffic;
  holders do the re-announcing.

**No DHT — pubsub+seq only (v1).** Playlists never touch the DHT: no record puts, no
provider records, nothing. Distribution, late-joiner discovery, and seq recovery all
ride the gossip topic and its suppressed re-announce cycle; `routing/get` on a playlist
name answers from the node's gossip buffer. (The catalog keeps its DHT path unchanged.)
Consequence, accepted: a playlist's record exists nowhere but in gossip and holders'
`playlists.db` — if every holder is offline, the playlist is simply gone until one
returns. That is the v1 contract, stated plainly.

## 3. What is deliberately NOT built (vs. v1 of this design)

Inline docs delete the entire playlist storage layer: no playlist blockstore/leveldb, no
second blockservice, no `playlist/cat`, no `playlist/rm`, no `KindPlaylist` pins, no
orphan sweep, no dial-providers step, no node-side budget eviction. The 50 MB budget
becomes a local `SUM(doc_bytes)` in SQLite with local eviction.

**Phase 0 stands on its own — fwd donor-cache leak fix.** Unrelated to playlists but part
of this plan: today `fwdServe` misses its LRU and calls `n.GetBlock`
(`node/fwd.go:175`), and boxo's blockservice unconditionally persists exchange-fetched
blocks (v0.41.0 `blockservice/blockservice.go:277`) — every donor transitive fetch leaks
permanently into the main leveldb, defeating the LRU's "bounded, GC-disabled" intent
(`node/fwd.go:34`). Fix: wrap the bounded/TTL LRU as a `Blockstore`; give the donor path
`fwdServ := blockservice.New(tiered{read: main→fwd, write: fwd}, bswap)` over the **raw**
bitswap exchange (donor recursion becomes structurally impossible; `ctxNoForward` stays
but is no longer load-bearing there); serve view `main ∪ fwd` into `bitswap.New`
(`node/node.go:206`); collapse `cacheGet`/`cachePut`.

## 4. Node RPC additions

| Endpoint | Purpose |
|---|---|
| `playlist/publish?key=<name>&seq=<n>&lifetime=168h` (doc bytes in body) | node computes `cid = raw-sha256(doc)`, signs the record (value `/ipfs/<cid>`), stores it, gossips the binary `{name, record, doc}` envelope (no DHT). Returns `{Name, Seq, Record}`. |
| `playlist/records?since=<v>` | drain the ingest buffer as ndjson `{Name, Seq, Record(b64), Doc(b64)}` with a monotonic version counter — cheap no-change poll. |
| `playlist/announce` (body: `{record, doc}` entries) | Rust-driven re-announce of held playlists, taken verbatim (no re-signing); node applies last-seen suppression before publishing. |
| `playlist/manifest` (body: `[{Name, Seq, Title}]`) | Rust posts the **disclosure set** (held + published-mine; §9 №15-16) — what playlist-list answers with and what beacons hash. Full replacement, idempotent; a change kicks an early beacon (floor 10 min). |
| `playlist/peer-list?peer=<id>&want=<a,b>` | dial a peer's `/trackerstream/playlist-list/1.0.0` stream: its disclosure set as `{name, seq, title}`, plus targeted re-announce of the `want` names (suppression bypassed, 30s/name cooldown). `Supported:false` = old build / the seed. |
| `playlist/backers` (body: `{Names:[...]}`) | windowed (24h) distinct-holder counts from the beacon topic, keyed by name (the node hashes; Rust never sees the hash scheme). |

Existing `routing/get` serves seq recovery. Poll (every ~20s), not push: the Rust↔Go
boundary is strictly HTTP today; SSE is a clean later upgrade.

## 5. Rust sync engine (`src-tauri/src/playlists.rs`)

**Local store:** `playlists.db` (SQLite, `app_data_dir`, next to `ipns_cache.json`) —
the *only* durable playlist store anywhere.

- `playlists(name PK, title, doc_json, seq, size_bytes, is_mine, published, key_name,
  last_update_at, last_played_at)`
- `playlists_fts` — FTS5 over title + track titles/module names (patterns from
  `src-tauri/src/catalog.rs:375-396`)
- `rejected(name, seq)` — failed-validation pairs, never re-processed.

**Sync loop** (~20s poll + a kick when the Playlists view opens):

1. `GET playlist/records?since=<v>`.
2. Per entry: `ipns::verify_b64_seq(name, record)` (`src-tauri/src/ipns.rs:17-31`);
   skip if `seq <= stored` or in `rejected`.
3. Verify `sha256(doc) == record value CID`; doc ≤ 1 MiB; parse; schema-validate
   (`v==1`, types, track-count cap ~20k, clamp string lengths). Reject → `rejected`.
4. Tombstone → delete local row. Else upsert row + FTS.
5. **Budget:** when `SUM(size_bytes)` of non-mine rows exceeds `TS_PLAYLIST_BUDGET`
   (default **50 MB**), evict least-recently-updated non-mine rows until under. Own
   playlists never evicted. Pure local DB ops.

**Re-announce:** jittered ~15-minute cycle, `playlist/announce` with all held
`{record, doc}` pairs (node suppresses those recently seen on the topic). Skip records
past EOL; for own playlists, EOL approaching → re-sign at same content, `seq+1`.

**Publish path — explicit, not automatic.** Playlists are **local-only by default**;
publishing is a deliberate "Share" action (auto-publishing would broadcast listening
habits — privacy by default). On share / edit of a shared playlist:

1. serialize compact doc;
2. first publish: `key/gen` name `playlist-<uuid>`;
3. seq recovery: `routing/get` own name → `seq = max(local, network) + 1`;
4. `playlist/publish` (doc in body, key, seq, lifetime=168h);
5. update local row (`published=1`, seq).

Delete of a published playlist publishes the tombstone first, then removes local state.

**Tauri commands** (register in `src-tauri/src/lib.rs:537-551`):
`playlist_search(q)` / `playlist_list(sort)` / `playlist_get(name)` — local SQLite;
`playlist_create(title, tracks)` / `playlist_update(name, doc)` / `playlist_delete(name)`
/ `playlist_publish(name)`; `playlist_sync_status()` — counts, bytes, budget.

## 6. UI

**Bottom "Playlists" toggle.** The bottom bar is `NowPlaying` (`+page.svelte:165`, 64px
grid at `NowPlaying.svelte:94-107`); add the toggle to its grid (left cluster), styled
like the header `.rtoggles` buttons (`+page.svelte:137-144,217-225`).

**View switch = the existing enum pattern** (`rightView` at `+page.svelte:22`):

```svelte
let mainView = $state<"tracks" | "playlists">("tracks");
```

wraps the center region (`+page.svelte:148-162`): `tracks` → current
`ResultsTable`/`DetailPanel`; `playlists` → new components. The header search input
dispatches to catalog search or local playlist search by `mainView` — search-in-place
replaces track search/view.

**Components** (`src/lib/components/`):

- `PlaylistsView.svelte` — local playlists (title, track count, updated, mine/published
  badges), filtered by header query via `playlist_search`; rows modeled on
  `QueuePanel.svelte`.
- `PlaylistDetail.svelte` — tracks of the selected playlist; ▶ plays via existing
  `playList(items, index)` (`src/lib/player.svelte.ts:94`) — denormalized rows flow into
  the player unchanged; track CIDs resolve via catalog `get` at play time. `is_mine`:
  reorder/remove, **Share** (confirm dialog: playlist becomes public), published badge.
  Not mine: read-only + "duplicate to my playlists".
- `playlists.svelte.ts` store wrapping the Tauri commands.

**Creation affordances:** "＋ playlist" in `PlaylistsView`; "add to playlist" in
`DetailPanel.svelte:33-35`; "save queue as playlist" in `QueuePanel.svelte`.

**Untrusted text:** peer content — Svelte's default escaping only (no `{@html}`), clamp
rendered lengths.

## 7. Abuse and failure notes

- **Impersonation:** impossible — record verifies against the name's key at the
  validator, at ingest, and again in Rust; doc verifies against the record's CID.
- **Spam (unlimited fresh keys):** validator kills malformed/oversized at first hop; seed
  LRU 8k records; client ingest buffer 16 MiB; 50 MB local budget with LRU eviction.
  Next lever if needed: per-peer rate limiting in the validator.
- **Fat-message flood:** 1 MiB cap × flood degree is real but linear; IDONTWANT bounds
  duplicates; suppression bounds steady-state; human-rate updates make it negligible in
  practice.
- **Availability (accepted):** playlists are gossip-only — records AND docs. A playlist
  whose holders are all offline is unrecoverable until one returns. Popularity =
  durability — the honest P2P contract, stated sharply.
- **Multi-device / collaboration (out of scope):** keys live in one keystore; concurrent
  same-name edits race newest-seq-wins wholesale. The `v` field keeps the doc evolvable
  (CRDT ordering keys or an op log would be `v:2`).
- **Phase 5–7 surfaces:** playlist-list requests are per-peer rate-limited (0.1/s,
  burst 4) with bounded response/request frames; `want` re-gossip is bounded by a
  30s/name cooldown and only ever serves manifest names (the node is not a re-gossip
  oracle for its whole buffer). Beacons: format violations die at the first hop
  (Reject), repeats within 2 min per origin are Ignored, counts saturate at 512 origins
  per hash, and Sybil inflation is accepted-until-identity (§10) — beacons only rank
  discover, they gate nothing. Deep links re-anchor on the same Rust verifier as
  gossip; a crafted fragment is exactly as powerless as a crafted gossip message.

## 8. Execution plan

**Phase 0 — fwd leak fix (no playlist code).**
`node/`: union/tiered blockstore wrappers; fwd LRU as `Blockstore`; `fwdServ` over raw
bitswap; collapse `fwdServe`; serve view `main ∪ fwd`.
*Verify:* existing fwd tests; donor transitive fetch leaves the main blockstore
untouched; recursion test still holds.

**Phase 1 — node gossip + RPC.**
`node/`: multi-topic pubsub refactor (catalog JSON kept; playlist binary envelope);
`PlaylistTopic`; `WithMaxMessageSize(2MiB)`; topic validator (full self-certification);
seed records-only LRU + client ingest buffer + catalog store cap; last-seen tracking +
announce suppression; RPC `playlist/publish`, `playlist/records`, `playlist/announce`.
*Verify:* extend `phased_test.go` — A publishes `{record, doc}`, B's buffer holds it,
seed holds the record but no doc bytes; invalid doc-hash message never propagates past
the first hop; late-joiner C gets the working set via B's re-announce; suppression keeps
announces ~1/playlist/cycle.

**Phase 2 — Rust engine + publish path.**
`src-tauri/`: `playlists.rs` (poll, verify record+hash, schema validation, budget,
tombstones, seq recovery, re-announce feed); `playlists.db` + FTS5; Tauri commands.
*Verify:* headless against two local nodes — sync, update, tombstone, budget eviction
(own survive), rejected memory, db-loss + keystore-intact republish lands with higher
seq.

**Phase 3 — UI.**
`src/lib/`: `mainView` toggle + bottom-bar button; `PlaylistsView` / `PlaylistDetail` /
`playlists.svelte.ts`; header search dispatch; create / add-to-playlist /
save-queue-as-playlist; Share confirm flow; duplicate-to-mine.
*Verify:* two desktop instances against a local seed — create, share on one, appears and
plays on the other.

**Phase 4 — polish.**
Re-announce tuning; `playlist_sync_status` footer. (Per-peer rate limiting: shipped.
Deep links: moved to §10 future work — web redirect + fragment payload design.)

**Phase 5 — playlist-list peer protocol (§10 → shipped).**
`node/playlistlist.go`: `/trackerstream/playlist-list/1.0.0` (msgio, one JSON frame each
way, ≤256 entries), served from the Rust-posted manifest (`playlist/manifest`), clients
only; `want` = targeted re-announce, suppression bypassed with a 30s/name cooldown.
`playlists.rs`: `manifest()` + hash-gated `push_manifest()` on every sync tick and after
share/hold/delete. Peer card gains a "playlists" section (fetch-on-open pull + "get").
*Verify:* `node/playlistlist_test.go` — B gets exactly A's manifest; the seed reads as
unsupported; `want` re-gossips a suppressed playlist into B's buffer, repeat hits the
cooldown; non-manifest names never served. Rust: disclosure-set query includes held +
published-mine only.

**Phase 6 — deep links (§10 → shipped).**
`link.rs`: fragment = b64url of the **verbatim gossip envelope** (uvarint name ++ record
++ doc, byte-compatible with `encodePlaylistMsg`); >8000-char URLs degrade to name-only.
`ingest_link` re-anchors on `ingest_wire` (path-name vs envelope-name mismatch = reject);
name-only links pend (in-memory, 15 min) + fire `want` at ≤8 connected peers. "copy
link" on any row with a live record; `trackerstream://playlist/<name>#<frag>` handled
(single-instance plugin forwards Win/Linux second launches). Web tier: ONE static page
(`deploy/site/p.html`, Caddy `handle /p/*`) that hands off client-side — the fragment
never reaches the server.
*Verify:* Rust — copy_link→ingest_link roundtrip on a fresh store (lands seen-tier),
tampered fragment rejected + remembered, name mismatch rejected, oversized doc →
name-only URL, pending clears when the row arrives via sync.

**Phase 7 — hold beacons (§10 → shipped).**
`node/beacon.go`: `/trackerstream/playlist-beacon/1.0.0`, hourly ±15min jittered
`[0x01][uvarint N][N × sha256(name)[:8]]` (N ≤ 4096) of the manifest; every node counts
hash → distinct origins over 24h (LRU 16384 × ≤512 origins, saturating); validator =
first-hop rate → format Reject → 2-min per-origin gap; manifest change kicks early
(floor 10 min). `playlist/backers` feeds the UI: discover ranks by backing, "backed by
~N holders" in the detail pane, "rare" badge on held rows with ≤1 backer.
*Verify:* `node/beacon_test.go` — codec bounds; two clients converge on count 2 for a
shared name / 1 for a solo one on both sides; repeats Ignored; window decay; origin-set
saturation; malformed dies at the local validator.

## 9. Decisions log

1. **Docs travel inline in the gossip message** `{name, record, doc}`; the record's CID
   is an integrity anchor, not an address. Deletes the entire playlist
   blockstore/bitswap layer (§3).
2. Fwd-cache retirement is Phase 0 of this plan.
3. Compact array format, ~40–60 B/track; denormalized `[id, moduleName, songTitle]`;
   tracks fetched only at play time.
4. **1 MiB doc cap = adversarial drop**, enforced at the topic validator before
   forwarding. Pubsub max message raised to 2 MiB. Binary envelope on the playlist topic
   (base64 would add +33%/hop).
5. No packet-size micro-optimization: gossipsub is stream-based (QUIC/TCP), no
   fragmentation cliff; cost is linear in size. IDONTWANT (pubsub v0.16) bounds
   duplicate delivery of fat messages.
6. Deletion via tombstone `{"v":1,"del":true}` at `seq+1`; record dies at EOL (168h).
7. **Publish is explicit** ("Share"); playlists local-only by default — privacy.
8. Re-announce with last-seen suppression; holders (clients) re-announce `{record,doc}`
   from `playlists.db`; the **seed never re-announces** (a record without its doc is
   useless — no fetch path exists).
9. Seq recovery from the network (`max(local, network)+1`) before every publish.
10. Seed stores records only (bounded LRU); client node keeps only a bounded ingest
    buffer; `playlists.db` is the single durable store. Catalog record store capped
    while at it (pre-existing unbounded-map wart).
11. Record stores in-memory on all roles; a restarted client re-learns the working set
    within one re-announce cycle.
12. **Playlists are pubsub+seq ONLY — zero DHT** (user call, 2026-07-02, reverting the
    initial best-effort record put). No record puts, no provider records, no holder
    re-puts. `routing/get` on playlist names answers from the gossip buffer; a
    never-published playlist publishes seq 1 without any lookup.
13. **Play-time pin (user call 2026-07-03).** The playlist the queue is currently
    sourced from is pinned in Rust (in-memory, single slot): exempt from budget
    eviction and seen-tier decay, and a mid-play tombstone keeps the row dormant
    (library-style) instead of deleting it. NOT the held tier — nothing is backed or
    re-announced (playing must not silently become a public backing act). The pin
    releases when the queue's source changes or clears; normal lifecycle resumes on
    the next decay/eviction pass. Dormant rows otherwise linger in discover only until
    the next decay tick (≤ one announce cycle) — accepted, badged.
14. **Holder tier ("add to library", user call 2026-07-02).** Three tiers in
    `playlists.db`: *mine* (signing key, editable, never evicted), *held* (foreign,
    deliberately added — never evicted, follows the author's updates, read-only), and
    *seen* (gossip brought it in — the discover pool, budget-evicted). **Re-announce is
    library-only** (mine + held): backing is deliberate. **Decay:** only the author can
    re-sign a record, so once no library re-announces a playlist its record ages to EOL
    (≤168h) and every unbacked copy purges itself — network-wide decay within one record
    lifetime. Held rows survive local decay even with an expired record (the user chose
    to keep the data; it just can't propagate until the author returns). UI: library
    default + discover tab; "add to library" ≠ "duplicate to mine" (fork) ≠ delete.
15. **No "back silently", ever (user call 2026-07-03,** superseding the earlier §10
    note that floated a per-playlist opt-out). Holding or publishing a public playlist
    is inherently a public act; the privacy path is forking to a private (unpublished)
    copy. The **disclosure set** — held + published-mine rows with a live, announceable
    record — is used identically by playlist-list answers and hold beacons. Private
    playlists and the seen tier are never disclosed, structurally.
16. **`playlist/manifest` RPC.** Rust posts the disclosure set `{name, seq, title}` on
    startup and whenever the library changes (hash-gated); the node serves playlist-list
    responses and generates beacons from this in-memory manifest. The node stays a
    stateless relay — nothing durable added.
17. **Deep-link fragment is the verbatim gossip envelope** (uvarint name ++ record ++
    doc, b64url), verified by the same `ingest_wire` path as gossip; the web tier is one
    static handoff page under Caddy (`/p/*`) and the fragment never reaches the server.
    No browser app. Oversized docs degrade to a name-only link resolved by gossip + a
    targeted `want`.
18. **`want` re-announce bypasses suppression** (30s per-name cooldown): the asker
    demonstrably missed the last announce — the pull-triggered push for late joiners and
    name-only links, still pure pubsub+seq (zero-DHT unchanged).

## 10. Future (documented, deliberately not built)

The three near/mid-term items that used to live here — the **playlist-list peer
protocol**, **deep links**, and **hold beacons** — shipped as Phases 5–7 (§8; design
calls in §9 №15-18). What remains is the long-term work. Notable scope trims from the
original sketches, decided at build time (2026-07-03):

- No "back silently" toggle, ever (§9 №15) — backing is public, period.
- No browser app edition: the `/p/` URL is served by a single static handoff page; the
  web client remains future work. If it ever lands, `/p/<name>` is its natural entry.
- No og:/`?t=` rich chat previews — a preview requires telling the server about the
  playlist, and the server seeing nothing is the point.

### Identity + friends tiers — Sybil resistance (long-term)

A cross-device **identity layer** (one identity, many device keys) with a **friends
layer** on top, giving three peer trust tiers — me / friends / everyone — mirroring the
playlist tiers (mine / held / seen), one level up.

- Identity fixes the deliberately-scoped-out v1 seams: multi-device edits of one
  playlist key (today: newest-seq-wins race, single-keystore), and keystore portability.
- Friends become a *trusted discovery channel*: friend attestations (hold beacons,
  playlist-list answers) get a trust multiplier; stranger counts get discounted or
  capped. Per-identity rate limits replace per-connection nuisance-bounds — actual
  Sybil resistance for the discovery channel, in the same feeds built earlier.
- Until then, per-peer rate limiting (§ validator) is explicitly a nuisance bound, and
  that is fine (user decision, 2026-07-03).
