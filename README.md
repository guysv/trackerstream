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

Music is better with context. MVP social features focus on lightweight sharing and presence, not building a full social network:

- Share tracks, playlists, and listening sessions
- Follow friends or curators and see what they are playing
- Public and private playlists tied to Mod Archive (and compatible) content

Exact providers (OAuth, ActivityPub, custom accounts, etc.) are TBD as the stack takes shape.

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

trackerstream is a **hybrid**: a **decentralized data plane** over a **centralized control plane**. The *bytes of modules* travel peer-to-peer as content-addressed blocks (clients fetch **and re-serve** them); everything user-facing and private—catalog, search, accounts, playlists, social, presence—stays server-owned. (An earlier draft was "client–server, not decentralized"; the [content-addressing labs](MVP.md) showed ~30–41% byte-exact sample dedup across the archive, which made decentralizing *delivery* worth it. Only delivery is decentralized.)

### Central server

- Owns the canonical **catalog**: extracted metadata, the search/browse APIs, and the mapping from each module to its **root CID**.
- Runs the **repack → content-addressed DAG pipeline** on ingest (see below), and **pins the archive modules on a master IPFS node**—the guaranteed availability floor when no peer holds the content.
- Acts as a **Kademlia DHT bootstrap node and guaranteed CID provider** for every archive module, and runs **STUN** (with a circuit-relay fallback) for NAT traversal — so peers discover each other and content via the DHT while rare modules stay reachable through the master.
- Owns user **accounts**, **playlists**, the **social graph** (follows), **presence** ("now playing"), and **share links**—all over HTTP, never on the P2P plane.

### Client (Tauri desktop)

- Synthesizes audio locally with libopenmpt in an AudioWorklet (see [Playback architecture](#playback-architecture)).
- Runs a **libp2p/IPFS node**: resolves a module's **root CID**, fetches its DAG blocks from peers + master, **verifies each block against its CID**, and **re-serves** cached blocks to other peers.
- Derives a **fetch plan** from the manifest and streams blocks progressively in playback order.
- **Caches** fetched blocks (keyed by CID) for seek/loop reuse, re-listens, and **cross-module reuse** (a later track reuses an earlier one's shared chunks).
- Owns all UI: browsing, dense IT-style views, the (ephemeral, local) play **queue**, keyboard navigation, transport, and now-playing display.

Playlists and the queue are plain ordered lists of module references (catalog id + root CID), held on the server / locally—never gossiped over P2P.

### Content-addressed repack (MVP)

Tracker modules can be several megabytes, so the server stores each as a **content-addressed block DAG** rather than a single file—which makes playback start fast *and* lets identical samples dedupe and travel peer-to-peer:

- A small **manifest** (root) ships first: order list, pattern metadata, instrument table, per-sample `pcm-root` CIDs, and baked **seek tables** (timing map + per-checkpoint resident sets).
- **Sample PCM is content-defined-chunked (FastCDC)** into blocks addressed by **CID**. Identical chunks across modules share a CID—stored once on the master, cached once per client. The chunk is simultaneously the **dedup unit**, the **partial-fetch granule**, and the **seek resident-set unit**.
- The client's **fetch plan** resolves the blocks for **segment 0** (the first pattern's resident chunks) first, then prefetches later blocks by configurable **lookahead** (N patterns / T seconds) in playback order, sourcing from peers + master.
- **Seek/loop** reuse cached blocks; a cold seek fetches only the target's resident-set chunk CIDs, not the whole DAG.

Self-verifying CIDs mean peer-served content can't be tampered; only public archive modules travel P2P.

## Future ideas

Out of MVP scope, but the architecture above is designed to accommodate them:

### Deeper decentralization

The MVP already puts module delivery on IPFS/libp2p with a Kademlia DHT (see [Architecture](#architecture)). Beyond it:

- **Funded / incentivized public pinning** beyond the single master node—community seeding so availability doesn't rest on one origin.
- **Dedicated, scaled relay infrastructure** for clients on symmetric NATs (the MVP ships only a minimal relay fallback).

### Other

- Mobile client—a **full peer** like desktop, with trimmed UI chrome.
- Format-specific playback engines for bit-exact fidelity (e.g. Amiga ProTracker `.mod`).

## Project status

**Early stage.** Repository bootstrap—product spec and README only. Implementation details (API shape, Mod Archive integration) will be documented here as they land.

**Stack:** desktop client built on **Tauri** (small footprint; the Rust backend handles native playback glue, the block cache, and the libp2p/IPFS node). Overall shape is a **hybrid**—decentralized data plane, centralized control plane (see [Architecture](#architecture)). The playback engine (libopenmpt → WASM in an AudioWorklet) is described under [Playback architecture](#playback-architecture). Other choices (UI framework, server stack, social providers, P2P stack) are still open.

## License

Copyright © 2026 guysv

**trackerstream** is licensed under the [GNU Affero General Public License v3.0 or later](LICENSE) (SPDX: `AGPL-3.0-or-later`).

Tracker modules streamed or referenced through the app remain under their respective authors’ terms; Mod Archive attribution applies separately from this software license.

### Dependency compatibility

Bundled dependencies must be compatible with AGPL-3.0. The playback stack is: libopenmpt (BSD-3-Clause) and its optional codecs libmpg123 (LGPL) and libvorbis (BSD). Permissive (BSD/MIT/zlib) and LGPL licenses compose cleanly into an AGPL work; proprietary module libraries (e.g. BASS, FMOD) are excluded.
