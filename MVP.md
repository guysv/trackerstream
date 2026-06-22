# trackerstream — MVP, sliced to phases

This document breaks the [MVP](README.md#mvp) into ordered, shippable phases. Each phase has a **goal**, **scope**, **exit criteria** (how we know it's done), and **risks/notes**.

Ordering principle: **de-risk the hardest unknowns first** — off-thread WASM playback, then **peer-to-peer block transport**, then content-addressed streaming repack — before building outward to catalog, UI, and social. Each phase should leave `main` in a runnable state.

Design constraints carried through every phase (from the [spec](README.md)):
- **Hybrid architecture — decentralized data plane, centralized control plane.**
  - *Data plane (module/sample bytes) is peer-to-peer:* each module is a **content-addressed block DAG** (chunk-level CIDs) that clients fetch **and re-serve** to each other over libp2p/IPFS.
  - *Control plane is the [central server](README.md#central-server):* it owns catalog/search, accounts, playlists, social graph and presence; runs an **IPFS master node that pins only the archive modules** (the guaranteed seed/availability floor); and runs a **STUN server** for NAT traversal + peer discovery.
  - This **revises** the earlier "client–server, not decentralized" stance — only the *delivery* of module bytes is decentralized; everything user-facing and private stays server-owned.
- **Client-side playback** via libopenmpt in an AudioWorklet — never block audio on the UI thread.
- **Content-addressed repack** is the core: modules are decomposed into a manifest + **chunked sample blocks** addressed by CID. Identical chunks dedupe across the whole corpus — **measured ~30% whole-sample / ~41% chunk-level byte-exact** ([`lab/CID.md`](lab/CID.md)). The CDC chunk is simultaneously the **dedup unit**, the **partial-fetch granule**, and the **seek resident-set unit** — one chunking scheme serves all three.
- **Self-verifying integrity:** every block is checked against its CID, so peer-served content can't be tampered. Only **public archive modules** travel P2P; private playlists/accounts/social stay server-side over HTTP.
- **Desktop-first** (Tauri). Mobile is a later, separate effort — but a **full peer** like desktop, not a trimmed non-peering client. Every client peers.
- **AGPL-compatible dependencies only** (see [License](README.md#license)) — including the P2P stack (kubo / rust-libp2p / Helia are MIT/Apache, which compose into an AGPL work).

The full local Mod Archive torrent is available offline (52 GB on disk as **~40.9k zip files containing 122,272 modules** — mod 67.7k / xm 36.0k / it 9.9k / s3m 8.6k, already parsed in the labs from `~/tmp/modarchive`), so it can seed the master node and every phase can be developed and tested without a public network. **Getting it onto the droplet is an explicit MVP step:** the corpus is pushed to `/srv/trackerstream/archive/` with [`deploy/sync-archive.sh`](deploy/sync-archive.sh) (rsync over SSH — resumable, incremental, verified), which the Phase 2 ingest then reads.

**Server deployment target (single master node — provisioned).** The control plane ships as a **deployable artifact** (a `.deb`, or a versioned tarball + install script) installed on a **DigitalOcean droplet running Debian 13 (trixie) x64**, region **fra1** — Basic Regular, **4 vCPU / 8 GB RAM** (~$48/mo; Premium AMD/Intel is ~$64/mo and only buys faster one-time ingest — not worth it for a single master node) — with a **250 GB Block Storage Volume (ext4, `noatime,discard`)** mounted as the data dir. The box is reachable as `ssh trackerstream-server` and is base-provisioned: build toolchain (`build-essential`, `git`, `pkg-config`, `libssl-dev`, `sqlite3`/`libsqlite3-dev`), a **4 GB swapfile** (`vm.swappiness=10`), the volume persisted in `/etc/fstab` by UUID (`nofail`), a `trackerstream` **system user**, and a stable data root at **`/srv/trackerstream`** → the volume, laid out as `archive/` (raw modules, ~52 GB), `ipfs/` (pin store, ~40–60 GB unique chunks + datastore overhead), `catalog/` (SQLite + FTS5 DB, a few GB), and `artifacts/` (deploy artifacts). Services run **natively under systemd** — no container runtime: the **HTTP API**, the **master IPFS/libp2p node** (Kademlia DHT bootstrap + always-on provider for every archive CID), and the **STUN/relay** endpoint. *Still pending (deferred to the relevant phase):* the app toolchain (**Node.js + kubo**, per the Phase 1 stack pick — install pending) and the **firewall** (`ufw` is installed but not enabled; lock to SSH + the libp2p/STUN ports once those are fixed). This sizes the MVP's single master node; horizontal scaling, managed infra, and multi-origin pinning are out of MVP.

**No domain yet — clients hard-code the server addresses (MVP).** Until a domain + TLS cert exist, the desktop client ships with the master node's literal addresses baked in as **a single configurable constant** (one config module / file, *not* literals scattered across the codebase) — currently **IPv4 `165.227.155.138`** / **IPv6 `2a03:b0c0:3:f0:0:2:959c:b000`** (the fra1 droplet). The **HTTP API base URL**, the libp2p **bootstrap/DHT multiaddrs**, and the **STUN** endpoint all reference these directly (no DNS resolution). This is a deliberate MVP shortcut that pins availability to one IP: keep the indirection clean so a hostname can replace the literals later without touching call sites. To survive a droplet rebuild without re-shipping every client, the IP should later be moved to a **DigitalOcean Reserved IP**; adding a domain + TLS + DNS-based discovery is deferred (see Phase 8 / out-of-MVP).

---

## Phase 0 — Playback spike (de-risk)

**Goal:** Prove the core loop: a tracker module plays, glitch-free, off the main thread — and that the engine can play a module **reassembled from content-addressed blocks**.

**Scope**
- Tauri app shell (single window, dev build).
- Custom Emscripten build of libopenmpt → WASM (with libmpg123 + libvorbis linked for MO3/compressed samples).
- AudioWorklet that pulls PCM from libopenmpt and outputs via Web Audio.
- Hardcoded: load one `.it` from disk, Play/Pause, print `order:row` + per-channel activity.
- Render params set to desktop quality (sinc interpolation, 48 kHz, volume ramping).
- **Confirm block reassembly:** assemble a playable module buffer from separated/partial blocks and `create_from_memory` it (already shown viable on stock libopenmpt in [`lab/FINDINGS.md`](lab/FINDINGS.md) — confirm it in the app shell, since the whole P2P plan depends on it).

**Exit criteria**
- A complex `.it` plays cleanly while the UI thread is artificially busy (no drop-outs — the failure mode the reference Mod Archive player warns about).
- Format smoke test: one each of MOD / XM / S3M / IT / MPTM plays.
- One MO3 and one module with a compressed sample play (codecs wired correctly).
- A module reassembled from blocks (not the original file) plays bit-identically to the original.

**Risks/notes**
- libopenmpt WASM build config is the main unknown — settle Emscripten flags + codec linkage here.
- Decide how WASM/worklet assets are bundled in Tauri.

---

## Phase 1 — P2P block transport spike (de-risk)

**Goal:** Prove a client can play a module sourced **entirely from CID blocks over libp2p**, and that a second peer accelerates/serves it through NAT.

**Scope**
- **P2P stack (decided): a bundled `kubo` node + a Node API driving it over kubo RPC.** The master node runs kubo; the desktop client bundles a kubo node as a Tauri sidecar (alternatives weighed and dropped: Rust libp2p sidecar, Helia in the webview). Server and client share the same node implementation. *Still to settle here:* the block/codec format (raw blocks + IPLD links, or UnixFS).
- **Settle the chunk/block size.** IPFS's default 256 KB block vs the labs' ~2 KB CDC chunks vs the partial-fetch granule pull in different directions: smaller blocks → more dedup + finer partial fetch, but more round-trips and Bitswap wantlist overhead. Choose a size (likely a CDC target in the low-tens-of-KB) that balances all three; lock it for Phase 2.
- Master node pins a handful of **pre-chunked module DAGs** (build them with the lab tooling). Client resolves a root CID → fetches blocks (Bitswap) → verifies each against its CID → reassembles → plays.
- Two clients on **different networks** swarm: stand up the server **STUN** endpoint, hole-punch, and have one peer serve blocks to the other. Measure **cold (master-only)** vs **warm (peer-assisted)** fetch.

**Exit criteria**
- A module plays sourced 100% from CID blocks over libp2p (no HTTP file fetch); every block verified against its CID.
- A second peer (behind NAT, via STUN) serves blocks to a first; the master node still guarantees availability when no peer has the content.
- A measured number for cold vs peer-assisted block fetch time on a real module.

**Risks/notes**
- **NAT traversal is the top risk.** STUN hole-punching fails on symmetric NATs — note a **relay/TURN fallback** as the contingency, and measure the success rate on consumer networks.
- **Peer discovery via Kademlia DHT** (libp2p content routing — provider records keyed by CID). The master node doubles as a **DHT bootstrap peer and always-on provider** for every archive CID, so discovery never rests solely on DHT propagation and rare content is always reachable. Validate DHT provide/find-providers latency in this spike.
- P2P stack maturity in a Tauri/desktop context; AGPL compatibility of the chosen stack (verify licenses here).

---

## Phase 2 — Central server: catalog, CID-DAG repack, master pin & STUN

**Goal:** Stand up the server that owns the canonical catalog and **produces + seeds** the content-addressed module DAGs.

**Scope**
- **Corpus sync to the droplet:** push the offline corpus (~52 GB) from the workstation to `/srv/trackerstream/archive/` with [`deploy/sync-archive.sh`](deploy/sync-archive.sh) (rsync over SSH). Resumable + incremental, so re-runs only ship changed/new modules and a dropped link can be restarted — this is the input-side counterpart to the re-runnable ingest below. Verifies file count + size after transfer.
- **Ingest + repack→DAG pipeline:** walk the archive (the corpus is ~40.9k zip files — extract each contained module, ~122k total); per module extract metadata (title, format, duration, channels, instrument/sample text, subsong count) **and** build the content-addressed DAG: parse → **CDC-chunk** the sample PCM → content-address → assemble the DAG (manifest root + pattern/instrument blocks + per-sample `pcm-root` + chunk leaves) → bake **seek-support tables** (timing map + per-checkpoint resident sets, referencing **chunk CIDs** — [`lab/SEEK.md`](lab/SEEK.md)) → **pin on the master IPFS node** → record the **root CID** + metadata in the catalog. Dedup is realized automatically: identical chunks share a CID and are stored once.
- **Catalog store + full-text search:** **SQLite + FTS5** (single-file DB on the block volume) over title / filename / instrument text / comments (mirroring Mod Archive's axes). Search results carry the module's **root CID**.
- **DHT bootstrap + STUN/relay:** the master node serves as the **Kademlia DHT bootstrap peer and guaranteed CID provider**; STUN (with a circuit-relay fallback for symmetric NATs) for hole-punching.
- **Deployable server artifact:** package the API, master IPFS/libp2p node, and STUN/relay as a **`.deb` (or tarball + install script)** with **systemd units** and a sample config (data dir on the mounted volume, ports, bootstrap addrs). A documented procedure takes a **base-provisioned Debian 13 droplet (`/srv/trackerstream` data root) → install artifact → start services → sync the corpus (`deploy/sync-archive.sh`) → ingest the archive**. Config and secrets live outside the artifact; ingest is a separate, re-runnable systemd unit/timer.

**Exit criteria**
- Full archive ingested, chunked, pinned; the unique chunk set is ~59% of raw sample bytes (the measured dedup), and ingest is incremental/re-runnable.
- Sub-second search across the corpus; results resolve to root CIDs.
- A client takes a search hit's root CID and fetches its full DAG from the master (and any peers), reassembles, and plays.
- **The server deploys from the artifact to the DigitalOcean droplet** (Debian 13, `/srv/trackerstream` on the mounted volume): services come up under systemd, ingest populates the pin store + SQLite catalog, and the search/fetch path above works against the deployed instance (not just a dev box).

**Risks/notes**
- Chunk/block size from Phase 1 is locked here — it determines pin-store size and partial-fetch granularity.
- Ingesting + chunking the whole torrent is the scale test — keep it streaming/batched (the lab already parses the full corpus in ~9 min).
- **Methodology (from labs):** detect needed samples by **static pattern decode**, not audio diffing — libopenmpt rendering is non-deterministic for IT random vol/pan, which corrupts audio-based detection (neutralizing it is only for deterministic *analysis*, never for delivered bytes).
- **Server stack (decided):** Debian 13 droplet + block volume, native **systemd** services, **SQLite + FTS5** catalog, `.deb`/tarball artifact. (README still lists this as open — reconcile it there.) The API/runtime is **Node.js driving kubo over kubo RPC** (decided with the Phase 1 stack pick) — node and API share one toolchain.

---

## Phase 3 — Streaming & progressive playback (over P2P)

**Goal:** Play modules streamed as CID blocks, starting before the whole DAG arrives. This is the ambitious core of the MVP.

**Scope**
- **Fetch plan (client):** read the manifest, resolve + fetch the blocks for **segment 0** (the first pattern's resident sample chunks) first, then prefetch later blocks by configurable lookahead (N patterns / T seconds) **in playback order**, sourcing from peers + master. Seek/loop follow jumps in the order list and reuse cached blocks.
- **Block cache (client), keyed by CID:** cross-module reuse becomes **cache hits** — a later track in a session reuses an earlier one's chunks (the warm-cache payoff of dedup). Cache survives for seek/loop and re-listens.

**Exit criteria**
- A multi-megabyte module starts playing from its first blocks well before the full DAG downloads.
- No stalls during continuous playback under normal conditions; lookahead keeps ahead of the playhead.
- Blocks are shared across modules: playing a second track in a session demonstrably reuses cached chunks from the first.
- Seeking into an already-cached region is instant; seeking into an un-fetched region fetches the target's resident-set chunk CIDs, not the whole DAG.

**Risks/notes**
- **First-block latency for rare modules:** content nobody else holds is served only by the master node — i.e. worst case ≈ today's single-origin fetch. P2P is a **bandwidth-offload + resilience** layer for popular content, not a cold-start speedup for rare content.
- The repack/seek wins reduce **bytes needed** (time-to-first-pattern ~2.3–3.1×, cold seek ~3×, [`lab/FINDINGS.md`](lab/FINDINGS.md) / [`lab/SEEK.md`](lab/SEEK.md)) — those help regardless of transport.

---

## Phase 4 — Browsing UI (Impulse Tracker–style)

**Goal:** A dense, keyboard-first interface for finding and inspecting modules.

**Scope**
- Retro palette + monospace typography; functional-first IT aesthetic.
- Dense list/table views (search results, latest, random, by format) with keyboard navigation, backed by the Phase 2 API.
- Module detail view: metadata, instrument/sample list, comments.
- Wire "play this" from any list/detail into the streaming player (resolves the root CID).

**Exit criteria**
- Browse → search → select → play works entirely from the keyboard.
- Lists stay responsive over the full corpus (virtualized rendering).
- Audio never stutters while scrolling/searching (validates the off-thread architecture under real UI load).

**Risks/notes**
- UI framework choice lives here (still open per README).
- Lock in look-and-feel tokens early so later phases inherit them.

---

## Phase 5 — Player UX

**Goal:** A real transport and a tracker-authentic now-playing experience.

**Scope**
- Full transport: play/pause, seek (by `order:row` and by seconds), next/previous, auto-advance.
- **Cold-seek into an unbuffered region** uses the packer's per-checkpoint **resident sample set** + **timing map** (baked in Phase 2): fetch only the target's resident-set **chunk CIDs**, `set_position`, play; lookahead streams the rest. Validated ~3× faster time-to-playback than a naive prefix (up to ~7× on late seeks), bit-exact, with stock libopenmpt — [`lab/SEEK.md`](lab/SEEK.md).
- Now-playing display: pattern/row position, per-channel VU/activity, instrument/sample table, subsong selector.
- Gapless / auto-next handoff between modules.
- Persisted playback settings (interpolation, volume).

**Exit criteria**
- Seeking and looping reuse engine + block cache correctly (no full reload on seek).
- A cold seek into an un-fetched region starts playing by fetching the target's resident-set blocks, not the whole DAG.
- Subsongs selectable; multi-subsong modules handled.
- Continuous listening across a queue with no gaps or drop-outs.

---

## Phase 6 — Queue & playlists

**Goal:** Organize listening beyond a single track.

**Scope**
- Play queue: an ephemeral, ordered list — enqueue, reorder, clear, "play next."
- Playlists: create, edit, reorder; public/private flag (storage only — sharing lands in Phase 7). Stored server-side, referencing modules by **catalog id + root CID**. (Playlists reference archive modules, which the master already pins — no extra pinning needed.)

**Exit criteria**
- Queue persists locally across restarts; playlists persist on the server.
- Playlists load and play correctly on a fresh client (resolve root CIDs, fetch DAGs P2P).

---

## Phase 7 — Social (lite)

**Goal:** Lightweight sharing and presence — not a social network.

**Scope**
- Accounts/identity (provider TBD — OAuth / ActivityPub / custom, per README).
- Share a track / playlist / listening session via link (a share resolves to catalog id + root CID).
- Follow friends or curators; see what they're playing (presence).
- Public vs private playlists honored across sharing.

**Exit criteria**
- A shared link opens the right module/playlist on another client (and fetches it P2P).
- Following a user surfaces their current/recent listening.

**Risks/notes**
- Scope tightly to sharing + presence. Provider decision (OAuth/ActivityPub/custom) made at the start of this phase.
- Social/account data is **never** on the P2P plane — it stays in the server DB over HTTP.

---

## Phase 8 — Polish & packaging

**Goal:** Ship a coherent desktop app.

**Scope**
- Aesthetic pass (palette, typography, density) across all views.
- Performance pass: ingest, search, streaming, render, memory, **and swarm behavior** under the full corpus.
- **P2P hardening:** NAT-traversal success rate, relay/TURN fallback where STUN is insufficient, sane behavior when offline / no peers (master fallback).
- **Server ops hardening (DigitalOcean):** firewall/`ufw` rules (HTTP API + libp2p/STUN ports only), **TLS for the HTTP API** (reverse proxy or built-in), **backups** of the SQLite catalog + IPFS pin store (volume snapshots), basic health/metrics + log rotation, and a **clean reinstall/restore drill** from the artifact + a backup. Confirm the running pinset matches the catalog (no orphaned/missing CIDs).
- Keyboard shortcut map + discoverability.
- Tauri packaging/distribution for desktop targets; AGPL compliance (source offer, dependency notices — **including the P2P stack**).

**Exit criteria**
- Installable desktop build that boots to a usable, themed app on a clean machine.
- **Server reproducibly deploys to a fresh Debian 13 droplet from the artifact + documented steps (re-running the base provisioning), and survives a restore-from-backup drill** with catalog and pinset intact.
- No audio drop-outs under sustained real-world use; playback works peer-assisted and master-only.
- License/attribution notices present (AGPL + per-module author terms + Mod Archive attribution).

---

## Explicitly out of MVP

Deferred, but the architecture is designed to accommodate them (see [Future ideas](README.md#future-ideas)):
- **Funded/incentivized public pinning** beyond the single master node; community seeding so availability doesn't rest on one origin.
- **Dedicated, scaled relay infrastructure** (the MVP ships a minimal relay fallback for symmetric NATs).
- Mobile client — a **full peer** like desktop, with trimmed UI chrome.
- Format-specific playback engines for bit-exact fidelity (e.g. ProTracker `.mod`).
