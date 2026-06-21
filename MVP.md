# trackerstream — MVP, sliced to phases

This document breaks the [MVP](README.md#mvp) into ordered, shippable phases. Each phase has a **goal**, **scope**, **exit criteria** (how we know it's done), and **risks/notes**.

Ordering principle: **de-risk the hardest unknowns first** (off-thread WASM playback, then streaming repack), then build outward to catalog, UI, and social. Each phase should leave `main` in a runnable state.

Design constraints carried through every phase (from the [spec](README.md)):
- **Client–server**, not decentralized. The [central server](README.md#central-server) owns the catalog, repack pipeline, streaming delivery, and account/social state; the [client](README.md#client-tauri-desktop) owns playback, the fetch plan, caching, and all UI.
- **Client-side playback** via libopenmpt in an AudioWorklet — never block audio on the UI thread.
- **Streaming repack** (manifest + needed-instruments-first segments) is in scope — see Phase 2. Instruments are referenced by an opaque id; no content addressing. The core viability of this approach has already been proven in a WASM lab — see [`lab/FINDINGS.md`](lab/FINDINGS.md).
- **Desktop-first** (Tauri). Mobile-lite is a later, separate effort.
- **AGPL-compatible dependencies only** (see [License](README.md#license)).
- IPFS / CID dedup is **out of MVP** — a later swap of the delivery backend, not the foundation.

The full local Mod Archive torrent is available offline, so it can seed the server and every phase can be developed and tested without a public network.

---

## Phase 0 — Playback spike (de-risk)

**Goal:** Prove the core loop: a tracker module plays, glitch-free, off the main thread.

**Scope**
- Tauri app shell (single window, dev build).
- Custom Emscripten build of libopenmpt → WASM (with libmpg123 + libvorbis linked for MO3/compressed samples).
- AudioWorklet that pulls PCM from libopenmpt and outputs via Web Audio.
- Hardcoded: load one `.it` from disk, Play/Pause, print `order:row` + per-channel activity.
- Render params set to desktop quality (sinc interpolation, 48 kHz, volume ramping).
- **Probe the repack feasibility question** (see Phase 2 risk): can the engine start playback from a partial set of instruments, or does it require the complete module? This answer shapes Phase 2.

**Exit criteria**
- A complex `.it` plays cleanly while the UI thread is artificially busy (no drop-outs — the failure mode the reference Mod Archive player warns about).
- Format smoke test: one each of MOD / XM / S3M / IT / MPTM plays.
- One MO3 and one module with a compressed sample play (codecs wired correctly).
- A written note on whether progressive/partial playback is feasible with our engine.

**Risks/notes**
- libopenmpt WASM build config is the main unknown — settle Emscripten flags + codec linkage here.
- Decide how WASM/worklet assets are bundled in Tauri.

---

## Phase 1 — Central server: catalog & search

**Goal:** Stand up the server that owns the canonical catalog, seeded from the local archive.

**Scope**
- Ingest: walk the archive and extract metadata per module via libopenmpt (title, format, duration, channel count, instrument/sample names + text, subsong count).
- Catalog store + full-text search over title / filename / instrument text / comments — mirroring Mod Archive's own search axes.
- HTTP API consumed by the client: search, filter by format, paginate, fetch module detail.

**Exit criteria**
- Full archive ingested; ingest is incremental/re-runnable.
- Sub-second search across the corpus on common queries.
- Module detail returns everything the UI and player need before any module bytes are fetched.

**Risks/notes**
- Ingesting the whole torrent is the scale test — keep it streaming/batched.
- Server stack is chosen here (still open per README).

---

## Phase 2 — Streaming repack & progressive playback

**Goal:** Play modules streamed from the server, starting before the whole file arrives. This is the ambitious core of the MVP.

**Scope**
- **Repack pipeline (server):** on ingest, transform each module into a manifest (order list, pattern metadata, instrument table) plus instrument **segments** ordered by first-needed-in-playback-order (pattern→instrument deps walked along the order list). Segment 0 = enough to start.
- **Delivery (server):** serve the manifest first, then segments on request.
- **Fetch plan (client):** read the manifest, request segment 0 to begin playback, prefetch later segments by configurable lookahead (N patterns / T seconds). Seek/loop follow jumps in the order list and reuse cached segments.
- **Segment cache (client):** keep fetched segments for seek/loop and re-listens.

**Exit criteria**
- A multi-megabyte module starts playing from segment 0 well before full download.
- No stalls during continuous playback under normal network conditions; lookahead keeps ahead of the playhead.
- Seeking into an already-cached region is instant; seeking into an un-fetched region fetches the right segment, not the whole file.

**Risks/notes**
- **Key risk — already de-risked in the lab ([`lab/FINDINGS.md`](lab/FINDINGS.md)).** Stock libopenmpt loads a *complete* module, so "start from segment 0" needs the client to assemble a playable module that grows as instruments arrive. The lab proved this is viable on **stock** libopenmpt — no engine fork: `create_from_memory` tolerates truncated buffers (succeeds from ~1% of the file, missing samples play as silence), recreate-on-arrival is cheap (parse ≤25 ms), and a repack that front-loads the **first pattern's** samples (found by *static* pattern decode) reproduces the opening **bit-exactly** while cutting time-to-first-pattern ~2.3–3.1× on the large ITs that hurt most. The approach (a) above is the path; no fallback needed.
- **Methodology note for the repacker:** detect needed samples by **static pattern decode**, not audio diffing — libopenmpt rendering is non-deterministic for modules using IT random vol/pan variation, which silently corrupts audio-based detection.
- Instruments are referenced by opaque id — deliberately the seam where the future IPFS layer substitutes CIDs.

---

## Phase 3 — Browsing UI (Impulse Tracker–style)

**Goal:** A dense, keyboard-first interface for finding and inspecting modules.

**Scope**
- Retro palette + monospace typography; functional-first IT aesthetic.
- Dense list/table views (search results, latest, random, by format) with keyboard navigation, backed by the Phase 1 API.
- Module detail view: metadata, instrument/sample list, comments.
- Wire "play this" from any list/detail into the streaming player.

**Exit criteria**
- Browse → search → select → play works entirely from the keyboard.
- Lists stay responsive over the full corpus (virtualized rendering).
- Audio never stutters while scrolling/searching (validates the off-thread architecture under real UI load).

**Risks/notes**
- UI framework choice lives here (still open per README).
- Lock in look-and-feel tokens early so later phases inherit them.

---

## Phase 4 — Player UX

**Goal:** A real transport and a tracker-authentic now-playing experience.

**Scope**
- Full transport: play/pause, seek (by `order:row` and by seconds), next/previous, auto-advance.
- Now-playing display: pattern/row position, per-channel VU/activity, instrument/sample table, subsong selector.
- Gapless / auto-next handoff between modules.
- Persisted playback settings (interpolation, volume).

**Exit criteria**
- Seeking and looping reuse engine + segment-cache state correctly (no full reload on seek).
- Subsongs selectable; multi-subsong modules handled.
- Continuous listening across a queue with no gaps or drop-outs.

---

## Phase 5 — Queue & playlists

**Goal:** Organize listening beyond a single track.

**Scope**
- Play queue: an ephemeral, ordered list — enqueue, reorder, clear, "play next."
- Playlists: create, edit, reorder; public/private flag (storage only — sharing lands in Phase 6). Stored server-side, referencing modules by their catalog id.

**Exit criteria**
- Queue persists locally across restarts; playlists persist on the server.
- Playlists load and play correctly on a fresh client.

---

## Phase 6 — Social (lite)

**Goal:** Lightweight sharing and presence — not a social network.

**Scope**
- Accounts/identity (provider TBD — OAuth / ActivityPub / custom, per README).
- Share a track / playlist / listening session via link.
- Follow friends or curators; see what they're playing (presence).
- Public vs private playlists honored across sharing.

**Exit criteria**
- A shared link opens the right module/playlist on another client.
- Following a user surfaces their current/recent listening.

**Risks/notes**
- Scope tightly to sharing + presence. Provider decision (OAuth/ActivityPub/custom) made at the start of this phase.

---

## Phase 7 — Polish & packaging

**Goal:** Ship a coherent desktop app.

**Scope**
- Aesthetic pass (palette, typography, density) across all views.
- Performance pass: ingest, search, streaming, render, memory under the full corpus.
- Keyboard shortcut map + discoverability.
- Tauri packaging/distribution for desktop targets; AGPL compliance (source offer, dependency notices).

**Exit criteria**
- Installable desktop build that boots to a usable, themed app on a clean machine.
- No audio drop-outs under sustained real-world use.
- License/attribution notices present (AGPL + per-module author terms + Mod Archive attribution).

---

## Explicitly out of MVP

Deferred, but the architecture is designed to accommodate them (see [Future ideas](README.md#future-ideas)):
- IPFS sharing layer; funded pinning + STUN service.
- CID-native modules (opaque instrument ids → CIDs for cross-module dedup).
- Mobile-lite client.
- Format-specific playback engines for bit-exact fidelity (e.g. ProTracker `.mod`).
