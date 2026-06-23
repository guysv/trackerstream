# trackerstream — MVP follow-up / deferred work

All 8 phases of [MVP.md](MVP.md) are implemented, and the control plane is
**deployed live** on the fra1 droplet. This document tracks everything that was
**deferred, simplified, or shipped as a functional-but-not-optimal version** —
so nothing falls through the cracks.

Status legend: 🔴 blocks full real-world use · 🟡 proven in labs, not yet wired ·
🟢 nice-to-have / hardening · ⚪ explicitly out of MVP (from the spec).

---

## A. Needed to actually use the deployed app end-to-end

### 🔴 A1. TLS for the HTTP API (catalog is unreachable from the packaged app)
**Symptom:** the packaged desktop app shows **"catalog offline."** The API is up
and reachable, but the WKWebView runs at a secure `tauri://` origin and macOS App
Transport Security **blocks the cleartext `http://165.227.155.138:8080` fetch**.
**Why deferred:** MVP.md ships with *no domain* (clients hard-code the IP), and
real TLS (Let's Encrypt) needs a hostname.
**Current state:** [`deploy/Caddyfile`](deploy/Caddyfile) is domain-ready (auto-HTTPS
once `TS_DOMAIN` points at the droplet). The **P2P data plane is unaffected** (it
runs through the embedded rust-ipfs node in Rust, not the webview).
**To do:** point a hostname at the droplet → run Caddy → set `API_BASE_URL` to
`https://<host>` in [`packages/config`](packages/config/index.js). No client code
change needed; the webview `fetch` to `https://` is allowed.
**Stopgap (if you want it working before TLS):** route catalog/social HTTP through
Rust via `@tauri-apps/plugin-http` (requests originate in Rust, bypassing ATS/CORS).
We deliberately chose to wait for TLS instead.

### 🔴 A2. Full-corpus sync + ingest (only a slice is live)
**Current state:** ~1,376 modules ingested (a 480 MB / 37-zip slice). The pipeline
is proven; the full **52 GB / 122,272-module** ingest is a long job.
**Blocker to fix first — ingest throughput.** The droplet ingests **~1.3 modules/s**
(vs ~24/s locally) because `Provide.Strategy=all` triggers a **DHT provide per
pinned block**; at that rate 122k modules ≈ 26 h. Batch/defer the provides:
- set `Provide.Strategy=roots` (or disable provide) during bulk ingest, then a
  single `ipfs provide --all` / reprovide afterwards, **or**
- parallelize `block put` (the ingest currently pins each module serially).
**Then:** `deploy/sync-archive.sh` (full corpus) → `systemctl start trackerstream-ingest`
(incremental, re-runnable). **Verify** the measured **~59% unique-chunk dedup**
(MVP.md Phase 2 exit) with `ipfs repo stat` vs raw sample bytes, and
`deploy/verify-pinset.sh`.

### 🔴 A3. Physical cross-NAT / peer-assisted test (Phase 1 exit, deferred by request)
STUN (coturn) + circuit-relay v2 + AutoNAT + client DCUtR are **built and running**.
Not yet measured on real networks:
- NAT-traversal **success rate** on consumer networks (symmetric-NAT → relay/TURN
  fallback).
- **Cold (master-only) vs warm (peer-assisted)** fetch time on two machines on
  *different* networks (the localhost swarm only proved the mechanism: cold 1.4 s /
  warm 31 ms for a 4.5 MB module).
- DHT **provide / find-providers latency** validation (Phase 1 risk note).

---

## B. Proven-in-lab optimizations not yet wired (functional fallback shipped)

These all *work* today via the progressive-streaming path; the lab-proven byte
optimizations on top are not baked into the manifest yet. The missing piece is the
**seek-support tables** (Phase 2 scope item that was deferred): a baked **timing
map** + **per-checkpoint resident sample sets** referencing chunk CIDs
([`lab/SEEK.md`](lab/SEEK.md)).

### 🟡 B1. Cold-seek resident-set fetch (Phase 5)
**Shipped:** seek works (streaming buffer + `set_position`); a seek into an
un-fetched region plays as the stream fills.
**Deferred:** fetching **only the target's resident-set chunk CIDs** instead of
streaming the whole DAG — lab-measured **~3× faster** time-to-playback (up to ~7×
on late seeks), bit-exact. Needs the per-checkpoint resident sets baked at ingest
(port `lab/seek-pack.mjs` + `lab/seek-partial.mjs` into `packages/repack`, add a
`seek` table to the manifest, add a Rust `seek_module(root, order)` that fetches
just those chunks).

### 🟡 B2. Segment-0 = first-pattern resident chunks (Phase 3)
**Shipped:** skeleton-first progressive start (valid module, silent samples fill in
order) — playable well before the full DAG (measured 565 ms vs 5.7 s).
**Deferred:** prioritizing the **first pattern's** sample chunks specifically —
lab-measured **2.3–3.1×** faster to a *correct* (audible) opening
([`lab/FINDINGS.md`](lab/FINDINGS.md)). Needs static first-pattern decode
(port `lab/it-static.mjs`) baked as a `segment0` hint in the manifest; the Rust
streamer then fetches those samples first.

### 🟡 B3. Playback-order lookahead prefetch (Phase 3)
**Shipped:** samples streamed in file order with concurrent leaf prefetch.
**Deferred:** true **playback-order** prefetch with a *configurable* lookahead
window (N patterns / T seconds) following the order list, and seek/loop following
jumps in that order. Ties to B1/B2 (needs the timing map).

### 🟡 B4. Warm-cache cross-module transfer benchmark (Phase 3 / CID.md)
The cache exists (persistent rust-ipfs blockstore → shared chunks are local hits by
construction). Not yet **benchmarked**: a warm-cache simulation over realistic
listening sessions (same artist / scene / playlist) to quantify the per-session
transfer savings that compound with dedup ([`lab/CID.md`](lab/CID.md) "What this
is and isn't").

---

## C. Coverage gaps

### 🟡 C1. MPTM / MO3 sample-level dedup
The CID-DAG parsers cover **MOD / IT / S3M / XM** (~97% of the corpus). MPTM and MO3
(140 + 628 in the corpus) currently ingest as **flat DAGs** (whole-file CDC, no
sample separation) — they're fetchable + byte-exact + playable, but get **no
cross-module sample dedup** and **no seek/segment-0 tables**. Add MPTM/MO3
sample-region parsers to [`packages/repack/src/parse.ts`](packages/repack/src/parse.ts).

### 🟢 C2. Bundled test fixtures / CI
Tests (`packages/wasm`, `packages/repack`, `apps/desktop`) depend on local fixtures
(`~/tmp/somemods`, `~/tmp/modarchive`) and a local kubo. Bundle a tiny licensed
fixture set + wire a CI pipeline so the oracles (smoke, reassembly, roundtrip,
tamper, worklet, swarm/interop) run on every change.

---

## D. Production / ops hardening

### 🔴 D1. Reserved IP
The client hard-codes the droplet IP in [`packages/config`](packages/config/index.js).
A droplet rebuild changes the IP and bricks every shipped client. Move to a
**DigitalOcean Reserved IP** (MVP.md §"No domain yet") before any real distribution.

### 🟢 D2. Code signing + notarization + auto-update
The packaged `trackerstream_0.1.0_aarch64.dmg` is **unsigned** (Gatekeeper will warn).
Add an Apple Developer signing identity + notarization in `tauri.conf.json`, and a
Tauri updater. Also: Windows/Linux build targets.

### 🟢 D3. Automated backups + metrics + log rotation
- Backups are manual ([`deploy/backup.sh`](deploy/backup.sh) + volume snapshot note).
  Schedule them (systemd timer + `doctl` volume snapshots).
- `/healthz` exposes basic counts; no real metrics/alerting (Prometheus, uptime).
- Logs go to journald (default rotation); set explicit retention if needed.

### 🟢 D4. TURN/relay load + symmetric-NAT validation
coturn is configured with a static TURN credential but the relay path for symmetric
NATs isn't load-tested (ties to A3). Rotate the TURN secret management out of the
plaintext config too.

---

## E. Account / social extensions

### 🟢 E1. OAuth / ActivityPub providers
Only **custom email accounts** (scrypt) are implemented (the MVP default). The spec
leaves OAuth / ActivityPub open ([`apps/server/src/social.ts`](apps/server/src/social.ts)).

### 🟢 E2. Share-link UX
Shares resolve to catalog id + root CID and open via a pasted **code**. A real
deep-link/custom-protocol (`trackerstream://share/<code>`) handler and richer
playlist-share + listening-session-share UX are follow-ups.

---

## F. Explicitly out of MVP (from the spec — listed for completeness)

⚪ These are designed-for but intentionally not built ([MVP.md](MVP.md#explicitly-out-of-mvp),
[README Future ideas](README.md#future-ideas)):

- **Funded / incentivized public pinning** beyond the single master node;
  community seeding so availability doesn't rest on one origin.
- **Dedicated, scaled relay infrastructure** (MVP ships only a minimal fallback).
- **Mobile client** — a full peer like desktop, with trimmed UI chrome.
- **Format-specific playback engines** for bit-exact fidelity (e.g. ProTracker `.mod`).
- **Horizontal scaling / managed infra / multi-origin pinning** (MVP sizes a single
  master node).
- **Domain + DNS-based peer/service discovery** (beyond the Reserved IP step in D1).

---

## Quick triage if picking this back up

1. **D1 Reserved IP** + **A1 TLS** (domain → Caddy) — unblocks the packaged app and
   makes the deployment durable.
2. **A2** ingest throughput fix → full-corpus ingest (gets the real catalog + the
   ~59% dedup number).
3. **B1/B2 seek + segment-0 tables** — the biggest UX wins, already proven in labs.
4. **A3** physical NAT test — validates the P2P offload story end-to-end.
