# trackerstream

Mod Archive dataset — a Spotify-like client for browsing, streaming, and sharing tracker music from [Mod Archive](https://modarchive.org/) and related sources. Desktop-first, with a lighter mobile experience planned later.

## Vision

Tracker modules are a deep catalog of community-made music, but discovery and playback still feel stuck in the 1990s: file downloads, scattered forums, and no shared listening context. **trackerstream** aims to make that catalog feel as approachable as a modern streaming app—while keeping the aesthetic and spirit of classic tracker culture.

## Platform strategy

| Phase | Target | Notes |
|-------|--------|-------|
| **Now** | PC (desktop) | Full UI, keyboard shortcuts, rich browsing |
| **Later** | Mobile | Full peer (fetch + re-serve); stream, queue, social—trimmed chrome |

## MVP

### Impulse Tracker–like UI

The desktop client should feel like opening a well-organized tracker, not a generic web player:

- Dense, information-rich layouts (pattern/list views, module metadata, instrument/sample hints where available)
- Keyboard-first navigation where it makes sense
- Retro palette and typography inspired by Impulse Tracker—functional first, nostalgic second
- Fast search and queueing across modules, artists, and collections
- Client-side playback of the major tracker formats (see [Playback architecture](#playback-architecture))

### Social integration

Music is better with context. Social features focus on lightweight sharing and presence, not building a full social network:

- **Share playlists peer-to-peer** — public by choice, private by default (shipped; see [Architecture](#architecture))
- Public and private playlists tied to Mod Archive (and compatible) content
- *(Planned)* Follow friends or curators and see what they are playing — today only anonymous content-popularity signals exist, not social presence or accounts

## Playback architecture

Playback is **client-side**. Tracker modules are not audio files—they are instructions plus samples that must be *synthesized*. Rendering them in the client keeps transfers tiny (modules are kilobytes, not megabytes) and is what makes the content-addressing ideas below (manifest-first loading, CID dedup, segment prefetch) meaningful in the first place.

### Engine

- **[libopenmpt](https://lib.openmpt.org/libopenmpt/)** is the playback engine, chosen for the widest format coverage available (MOD, XM, S3M, IT, MPTM, plus dozens of legacy/exotic formats) and reference-grade accuracy—it is the engine inside OpenMPT, which the tracker community treats as ground truth.
- Compiled to **WebAssembly via a custom Emscripten build**, so we control render quality rather than inheriting someone else's web defaults.
- Optional **libmpg123** (LGPL) and **libvorbis** (BSD) are linked in for **MO3** support and IT/XM modules with compressed (MP3/OGG) samples.

### Audio thread, not the UI thread

libopenmpt runs in an **AudioWorklet** (a dedicated audio thread) that *pulls* PCM on demand, rather than synthesizing on the main thread.

This is a deliberate departure from prior art. The reference Mod Archive web player uses libopenmpt via `chiptune2.js`, which synthesizes on the main thread, and warns: *"Complex modules may put your web browser under heavy load and cause audio drop-outs. We recommend you use a dedicated standalone player for optimal listening."* That is a limitation of main-thread playback, **not** of libopenmpt—the engine is reference-accurate. Running it off-thread lets our dense, keyboard-driven UI stay busy without starving audio, so trackerstream can be smoother than the reference site using the same engine.

### Render quality

- High-quality settings tuned to match desktop OpenMPT: **sinc interpolation**, 48 kHz output, volume ramping—not the throttled-for-CPU web defaults.
- Position model is **seek by order:row / seconds**, not byte offset. The engine resynthesizes from a position; there is no PCM scrub. This maps naturally onto the streaming fetch plan, where seeks follow jumps in the order list.

*Off-MVP:* for a specific format where bit-exact fidelity to the original tracker/hardware matters more than breadth (e.g. Amiga ProTracker `.mod` via a dedicated player), a format-specific engine could be added later. This is gold-plating, not architecture.

## Architecture

trackerstream is a **hybrid**: a peer-to-peer **data plane** anchored by a single always-on master, over a deliberately thin **control plane**. Module bytes, the catalog, *and* user playlists all travel as content-addressed, self-verifying blocks over libp2p; the only centralized pieces are one **master node** (the availability + discovery floor) and an **offline ingest pipeline** that bakes and publishes the catalog. There are **no user accounts and no client-facing HTTP APIs**—identity is a local libp2p keypair.

(An earlier draft split a decentralized *delivery* plane under a server-owned catalog/playlist/accounts control plane over HTTP; the [content-addressing labs](MVP.md) showed ~30–41% byte-exact sample dedup across the archive, which made decentralizing delivery worth it. Since then the **catalog and playlists have also moved onto the P2P plane**, and the client and master consolidated onto one Go node.)

### One node, two roles — `tsnode`

A single Go binary (`node/`, built on **go-libp2p + boxo**) is the entire network layer, run in two roles:

- **`--role server`** — the **master**: pins the whole corpus (the guaranteed CID provider / availability floor when no peer holds the content), bootstraps a private-overlay Kademlia DHT (`/trackerstream/kad/1.0.0`), acts as a **circuit-relay-v2** relay and **DCUtR** hole-punch coordinator, validates and relays the playlist gossip topic, and **publishes + serves the catalog's IPNS record**. Runs on a Hetzner box under systemd.
- **`--role client`** — bundled into the desktop app as a **Tauri sidecar binary**; the Rust backend spawns it and drives it over a loopback RPC. It does all client-side libp2p: **Bitswap fetch and re-serve**, DHT lookups, IPNS resolution, playlist pubsub, and relay/hole-punching.

This "Go everywhere" design replaced the earlier kubo-master + in-process-rust-ipfs-client split; kubo is kept installed but stopped as a rollback standby.

### Desktop client (Tauri)

- **Svelte UI** on a **Rust backend**. The backend spawns the `tsnode` sidecar, drives it over RPC, and **verifies everything the sidecar returns**—IPNS records (via `rust-ipns`) and every block against its CID—treating the node as an untrusted cache.
- Synthesizes audio locally with libopenmpt in an AudioWorklet (see [Playback architecture](#playback-architecture)); the network only ever hands over CID blocks.
- Runs the **streaming fetch plan** (progressive, playback-order block fetch + prefetch) and the **playlist sync engine**.
- Owns all UI: browsing, dense IT-style views, the local play **queue**, keyboard navigation, transport, and now-playing display.

### Catalog — published, not served

The catalog is a **baked SQLite database published on IPFS under the master's signed IPNS name**. There is no catalog/search HTTP API.

- Ingest bakes a **SQLite + FTS5** catalog (one row per module → its root CID, plus a content md5 join key), **page-aligned** so each 16 KB SQLite page is one stable UnixFS block, and publishes it under the `catalog` IPNS name.
- The client resolves that name to the current DB CID and **queries the database locally over a Bitswap-backed SQLite VFS**: page reads are served as IPFS blocks, so a search or lookup fetches only the handful of index/result pages it touches (covering indexes + FTS5), never the whole file—with a page cache, read-ahead, and cancellation. (phiresky's `sql.js-httpvfs` idea, over Bitswap instead of HTTP ranges.)
- Results carry each module's root CID; the bytes are then fetched over the data plane.

### Playlists — gossiped, not stored server-side

User playlists are **signed IPNS documents gossiped inline over a pubsub topic**. The client's local SQLite (`playlists.db`) is the only durable store anywhere; the master is a bounded, validating relay that never stores the documents.

- A playlist is a compact signed doc carried next to its IPNS record in the gossip message—no Bitswap, no pinning. Every receiver verifies the record signature and that the doc hashes to the record's CID before trusting it.
- Tracks are referenced by their **content md5**—stable across catalog re-bakes—and resolved to a current CID through the catalog only at play time.
- Publishing is an explicit "share"; playlists are **private (local-only) by default**.

### Content-addressed repack

Tracker modules can be several megabytes, so the server stores each as a **content-addressed block DAG** rather than a single file—which makes playback start fast *and* lets identical samples dedupe and travel peer-to-peer:

- A small **manifest** (root) ships first: order list, pattern metadata, instrument table, per-sample `pcm-root` CIDs, and baked **seek tables** (timing map + per-checkpoint resident sets).
- **Sample PCM is content-defined-chunked (FastCDC)** into blocks addressed by **CID**. Identical chunks across modules share a CID—stored once on the master, cached once per client. The chunk is simultaneously the **dedup unit**, the **partial-fetch granule**, and the **seek resident-set unit**.
- The client's **fetch plan** resolves the blocks for **segment 0** (the first pattern's resident chunks) first, then prefetches later blocks by configurable **lookahead** (N patterns / T seconds) in playback order, sourcing from peers + master.
- **Seek/loop** reuse cached blocks; a cold seek fetches only the target's resident-set chunk CIDs, not the whole DAG.

### Discovery & NAT traversal

- **Discovery / popularity:** beyond the DHT, nodes exchange lightweight **gossip beacons** (hashed digests of the playlists they hold) so the client can rank and surface popular playlists—no server, no leaked identity.
- **NAT traversal:** circuit-relay v2 + **DCUtR** hole-punching (coordinated by the master) are the primary path; a **coturn STUN/TURN** endpoint is kept as a symmetric-NAT fallback.

Self-verifying CIDs mean peer-served content can't be tampered with; only public archive modules travel P2P.

## Future ideas

Out of scope for now, but the architecture above is designed to accommodate them:

### Deeper decentralization

The data plane already puts module delivery, the catalog, and playlists on libp2p with a custom Kademlia DHT (see [Architecture](#architecture)). Beyond it:

- **Funded / incentivized public pinning** beyond the single master node—community seeding so availability doesn't rest on one origin.
- **Dedicated, scaled relay infrastructure** for clients on symmetric NATs (the primary relay is the master today).

### Other

- **Social layer** — user accounts, follows, and "now playing" presence (today only anonymous content-popularity beacons exist).
- Mobile client—a **full peer** like desktop, with trimmed UI chrome.
- Format-specific playback engines for bit-exact fidelity (e.g. Amiga ProTracker `.mod`).

## Project status

The stack has been rewritten **"Go everywhere"**: one `tsnode` binary (go-libp2p + boxo) is both the master and the desktop client's bundled sidecar, replacing the previous kubo master + in-process rust-ipfs client. The **catalog and playlists have moved onto the P2P plane**, and the server is now an offline ingest pipeline only. The master runs on a Hetzner box (systemd: `tsnode` + a nightly ingest bake + backups/metrics); the desktop client streams modules 100 % from CID blocks over libp2p and queries the catalog over the Bitswap SQLite VFS. User accounts / social graph are not yet built. Monorepo layout:

- `node/` — the **`tsnode`** Go node (go-libp2p + boxo): custom DHT, IPNS-over-gossipsub, Bitswap, circuit relay + DCUtR, the playlist pubsub relay + hold beacons, and a kubo-compatible RPC. Run as the master (`--role server`) and as the desktop sidecar (`--role client`).
- `packages/wasm` — custom Emscripten **libopenmpt** build (MO3 + compressed samples)
- `packages/repack` — module ↔ **CID-DAG** repack (parsers, FastCDC, DAG build/reassemble, seek tables, RPC client)
- `packages/config` — single source of truth for the master's addresses + the catalog IPNS name, shared by server and client
- `apps/server` — offline **ingest pipeline**: repack → pin on the master → bake the **SQLite + FTS5** catalog → publish it over IPNS (no live HTTP service)
- `apps/desktop` — **Tauri + Svelte** client: Rust backend + bundled **`tsnode`** sidecar + an AudioWorklet engine
- `deploy/` — provisioning, the tsnode build/cutover tooling, systemd units, Caddy/TLS, and backup/restore/verify ops

**Stack (decided):** UI **Svelte/SvelteKit**; network **one Go `tsnode` binary** (go-libp2p + boxo) for both the master and the client sidecar, driven from the Rust backend over RPC (revising the earlier kubo-master + rust-ipfs-client pick); catalog and playlists **content-addressed over libp2p** (no client-facing HTTP); ingest a **Node.js** pipeline over **SQLite + FTS5**; identity a **local libp2p keypair** (user accounts deferred). The overall shape is a **hybrid**—a P2P data plane anchored by a single master, over a thin control plane (see [Architecture](#architecture)). The playback engine (libopenmpt → WASM in an AudioWorklet) is described under [Playback architecture](#playback-architecture).

## License

Copyright © 2026 guysv

**trackerstream** is licensed under the [GNU Affero General Public License v3.0 or later](LICENSE) (SPDX: `AGPL-3.0-or-later`).

Tracker modules streamed or referenced through the app remain under their respective authors’ terms; Mod Archive attribution applies separately from this software license.

### Dependency compatibility

Bundled dependencies must be compatible with AGPL-3.0. The playback stack is: libopenmpt (BSD-3-Clause) and its optional codecs libmpg123 (LGPL) and libvorbis (BSD). Permissive (BSD/MIT/zlib) and LGPL licenses compose cleanly into an AGPL work; proprietary module libraries (e.g. BASS, FMOD) are excluded.
