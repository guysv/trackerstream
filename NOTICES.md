# Notices & attribution

**trackerstream** is licensed under **AGPL-3.0-or-later** (see [LICENSE](LICENSE)).
Per the AGPL, the complete corresponding source is this repository; network users
are offered the source via the app's About screen / this repo URL.

All bundled dependencies are AGPL-compatible (permissive or LGPL, which compose
into an AGPL work). Proprietary module libraries (BASS, FMOD) are excluded.

## Playback

| Component | License | Notes |
|---|---|---|
| libopenmpt | BSD-3-Clause | custom Emscripten build (`packages/wasm`) |
| minimp3 | CC0 / public domain | MP3 sample decode (bundled in libopenmpt) |
| stb_vorbis | public domain (MIT alt) | OGG/Vorbis sample decode (bundled) |

## Data plane (P2P)

| Component | License |
|---|---|
| kubo / go-libp2p (master node) | MIT / Apache-2.0 |
| rust-ipfs / rust-libp2p (client, embedded) | MIT / Apache-2.0 |
| multiformats, @ipld/dag-cbor | MIT / Apache-2.0 |
| coturn (STUN/TURN) | BSD-3-Clause |

## App / build

| Component | License |
|---|---|
| Tauri | MIT / Apache-2.0 |
| Svelte / SvelteKit, Vite, esbuild | MIT |
| Node.js (`node:sqlite` = SQLite, public domain) | MIT / public domain |
| yauzl | MIT |

## Content

Tracker modules streamed or referenced through the app remain under their
respective authors' terms. **Mod Archive** attribution applies separately from
this software license; module bytes are not redistributed in this repository.
