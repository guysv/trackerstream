# trackerstream

Mod Archive dataset — a Spotify-like client for browsing, streaming, and sharing tracker music from [Mod Archive](https://modarchive.org/) and related sources. Desktop-first, with a lighter mobile experience planned later.

## Vision

Tracker modules are a deep catalog of community-made music, but discovery and playback still feel stuck in the 1990s: file downloads, scattered forums, and no shared listening context. **trackerstream** aims to make that catalog feel as approachable as a modern streaming app—while keeping the aesthetic and spirit of classic tracker culture.

## Platform strategy

| Phase | Target | Notes |
|-------|--------|-------|
| **Now** | PC (desktop) | Full UI, keyboard shortcuts, rich browsing |
| **Later** | Mobile (lite) | Stream, queue, and social—trimmed chrome |

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

trackerstream is a **client–server** application, not a decentralized one. Full decentralization was considered and dropped as not worth the cost for now; the IPFS sharing layer under [Future ideas](#future-ideas) is a later addition, not the foundation.

### Central server

- Owns the canonical **catalog**: module files, extracted metadata, and the search/browse APIs.
- Runs the **repack pipeline** on ingest (see below).
- **Streams** modules to clients—manifest first, then segments on demand.
- Owns user **accounts**, **playlists**, the **social graph** (follows), **presence** ("now playing"), and **share links**.

### Client (Tauri desktop)

- Synthesizes audio locally with libopenmpt in an AudioWorklet (see [Playback architecture](#playback-architecture)).
- Derives a **fetch plan** from a module's manifest and streams segments progressively.
- **Caches** fetched segments for seek/loop reuse and re-listens.
- Owns all UI: browsing, dense IT-style views, the (ephemeral, local) play **queue**, keyboard navigation, transport, and now-playing display.

Playlists and the queue are plain ordered lists of module references—no content addressing involved.

### Streaming repack (MVP)

Tracker modules can be several megabytes, so to start playback quickly the server stores them in a repacked, streaming-friendly format rather than shipping the whole file up front:

- A small **manifest** (order list, pattern metadata, instrument table) ships first.
- Instruments are grouped into **segments** ordered by when they are first needed—walking pattern→instrument dependencies in **playback order** (the order list), not raw pattern index. Segment 0 is enough to begin playback; later segments prefetch in the background (HLS-like, but keyed on tracker dependencies).
- **Lookahead** is configurable (N patterns or T seconds ahead): required-now vs prefetch vs on-demand.
- **Seek/loop** reuse already-fetched segments; the fetch plan follows jumps in the order list, not sequential pattern files.
- Optional: inline tiny one-shot samples in the manifest to avoid extra round trips.

This is purely a buffering optimization and does **not** require content addressing—instruments are referenced by an opaque id within the module's manifest.

## Future ideas

Out of MVP scope, but the architecture above is designed to accommodate them:

### IPFS sharing layer

- Mirror or reference module assets on IPFS for decentralized availability, reducing reliance on a single origin.
- Fund a dedicated **pinning + STUN** service so playback and peer discovery stay reliable.
- **CID-native modules:** make the repack's opaque instrument ids resolve to **content IDs (CIDs)**, so identical samples across modules dedupe at the content layer (better cache hit rates, less storage). Because the MVP repack already separates instruments behind ids, this is a delivery-backend swap, not a redesign.

### Other

- Mobile-lite client.
- Format-specific playback engines for bit-exact fidelity (e.g. Amiga ProTracker `.mod`).

## Project status

**Early stage.** Repository bootstrap—product spec and README only. Implementation details (API shape, Mod Archive integration) will be documented here as they land.

**Stack:** desktop client built on **Tauri** (small footprint; the Rust backend handles native playback glue, the segment cache, and a future IPFS layer). Overall shape is **client–server** (see [Architecture](#architecture)). The playback engine (libopenmpt → WASM in an AudioWorklet) is described under [Playback architecture](#playback-architecture). Other choices (UI framework, server stack, social providers) are still open.

## License

Copyright © 2026 guysv

**trackerstream** is licensed under the [GNU Affero General Public License v3.0 or later](LICENSE) (SPDX: `AGPL-3.0-or-later`).

Tracker modules streamed or referenced through the app remain under their respective authors’ terms; Mod Archive attribution applies separately from this software license.

### Dependency compatibility

Bundled dependencies must be compatible with AGPL-3.0. The playback stack is: libopenmpt (BSD-3-Clause) and its optional codecs libmpg123 (LGPL) and libvorbis (BSD). Permissive (BSD/MIT/zlib) and LGPL licenses compose cleanly into an AGPL work; proprietary module libraries (e.g. BASS, FMOD) are excluded.
