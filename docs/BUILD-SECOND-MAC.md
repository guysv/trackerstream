# Building trackerstream desktop on a second Mac (cross-NAT / STUN test)

This is the runbook for **MVP-FOLLOWUP A3**: build the desktop client on a second
machine on a *different physical network* and validate NAT traversal (STUN /
circuit-relay v2 / AutoNAT / DCUtR hole punching) and cold-vs-warm peer-assisted
fetch. The control plane is already live at `https://trackerstream.xyz` and the
master libp2p node is the DHT bootstrap + always-on provider.

The repo vendors the prebuilt `packages/wasm/dist/libopenmpt.js`, so you do **not**
need Emscripten/emsdk — only Rust, Node, and pnpm.

## 1. Prerequisites (Apple Silicon Mac)

```bash
# Xcode command-line tools (clang/linker for the Rust + Tauri build)
xcode-select --install

# Rust (stable)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"

# Node 24 + pnpm (via nvm, or brew install node@24)
#   brew install pnpm   # or: corepack enable && corepack prepare pnpm@11.8.0 --activate
node -v   # must be >= 23 (TS type-stripping + --experimental-sqlite are used)
pnpm -v
```

## 2. Clone + install

```bash
git clone -b lab/cid-blocks git@github.com:guysv/trackerstream.git
cd trackerstream
pnpm install
```

## 3. Run the app

For a quick interactive run (recommended for the NAT test — fast, live logs):

```bash
pnpm --filter @trackerstream/desktop tauri dev
```

The first run compiles the embedded `rust-ipfs` node and its deps — **expect
several minutes**. Subsequent runs are incremental. To instead produce a
double-clickable bundle:

```bash
pnpm --filter @trackerstream/desktop tauri build
# -> apps/desktop/src-tauri/target/release/bundle/dmg/trackerstream_0.1.0_aarch64.dmg
```

Gatekeeper will warn (the build is unsigned — see D2): right-click → Open, or
`xattr -dr com.apple.quarantine <app>`.

The app loads its catalog from `https://trackerstream.xyz` (HTTPS — fixes the old
"catalog offline" ATS block) and bootstraps the data plane to the master over
`/dns4/trackerstream.xyz/tcp/4001/...` (see `packages/config/index.js`).

## 4. What the embedded node does (so you know what to look for)

On startup the in-process node (`apps/desktop/src-tauri/src/ipfs.rs`) enables:
- **TCP + QUIC** transports, ephemeral listen ports;
- **relay client + DCUtR** (circuit-relay v2 reservation, then hole-punch to a
  direct connection);
- **AutoNAT** (reachability detection);
- a **persistent fs blockstore** under the app data dir = the client's CID cache.

The frontend dials the master bootstrap (`connect_peer`) then resolves a module
root CID over Bitswap (`fetch_module` / `start_stream`).

## 5. The cross-NAT test protocol

Run the app on **Mac A** (network 1) and **Mac B** (network 2 — e.g. a phone
hotspot, so the two are on genuinely different NATs).

1. **Bootstrap reachability** — on both, confirm the catalog loads (browse/search
   work) and playback works at all. That alone proves the HTTPS control plane +
   master-served Bitswap path across your NAT.
2. **Cold fetch (master-only)** — on Mac A, pick a module and time how long until
   it plays (cold cache, sourced from the master). Note the size.
3. **Warm/peer-assisted fetch** — on Mac B, fetch the **same** module. If
   peer-discovery + hole-punching work, B can pull blocks from A (who now holds +
   may provide them) instead of only the master. Compare time-to-playback and
   watch the logs for a DCUtR upgrade / a `/p2p-circuit/` relayed address being
   replaced by a direct one.
4. **Symmetric-NAT fallback** — repeat with one Mac behind a symmetric NAT (some
   carrier hotspots). If a direct hole-punch fails, the connection should fall
   back to the **relay** (and TURN via coturn on `trackerstream.xyz:3478`).

### Reading the connection state

`tauri dev` prints the embedded node's libp2p logs to the terminal. Useful
signals: `identify` observed-address reports, `autonat` Public/Private verdicts,
`relay` reservation accepted, `dcutr` hole-punch attempt/success, and Bitswap
session peers. To raise verbosity, launch with:

```bash
RUST_LOG=info,libp2p_dcutr=debug,libp2p_relay=debug,libp2p_autonat=debug \
  pnpm --filter @trackerstream/desktop tauri dev
```

> Known gap (honest): peer-**assisted** transfer requires the two clients to
> discover each other as providers of the root CID. The relay/DCUtR/AutoNAT
> mechanism is built and running; whether the embedded node announces the
> content it caches to the DHT is exactly what this test is meant to establish
> (MVP-FOLLOWUP A3). If B always sources from the master, that's the finding —
> client-side providing is the follow-up. Master-served fetch across both NATs
> should work regardless.

## 6. Troubleshooting

- **"catalog offline"** — should not happen now (HTTPS). If it does, check
  `curl https://trackerstream.xyz/healthz` from that Mac.
- **No peers / can't connect to master** — verify outbound UDP/TCP 4001 isn't
  blocked by the local network; QUIC (UDP) is tried first.
- **Build fails on `libopenmpt`** — the prebuilt JS is vendored; if you deleted
  it, rebuild with `pnpm wasm:build` (needs emsdk) or restore it from git.
- **Slow first build** — normal; the embedded IPFS stack is large. Use
  `tauri dev` (debug) rather than `tauri build` (release) while testing.
