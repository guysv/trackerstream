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

### Social integration

Music is better with context. MVP social features focus on lightweight sharing and presence, not building a full social network:

- Share tracks, playlists, and listening sessions
- Follow friends or curators and see what they are playing
- Public and private playlists tied to Mod Archive (and compatible) content

Exact providers (OAuth, ActivityPub, custom accounts, etc.) are TBD as the stack takes shape.

## Future ideas

These are intentionally out of MVP scope but inform architecture choices today:

### IPFS content layer

- Mirror or reference module assets on IPFS for decentralized availability
- Fund a dedicated **pinning + STUN** service so playback and peer discovery stay reliable without relying on a single origin

### CID-native modules

- Repack tracker modules so embedded/cloned instruments resolve to **content IDs (CIDs)**
- Identical samples across uploads dedupe at the content layer, improving cache hit rates for clients and reducers for storage

#### Streaming-friendly repack

CIDs are content hashes—they do not imply fetch order. The repack format can: a small **manifest** (order list, pattern metadata, instrument table) ships first; clients derive a **fetch plan** from pattern→instrument dependencies walked in **playback order** (order list), not raw pattern index.

- **Segments** group instrument CIDs needed for upcoming patterns; segment 0 is enough to start playback, later segments prefetch in the background (HLS-like, but keyed on tracker deps)
- **Dedup** still applies: same sample → same CID, fetch once and reuse across patterns and modules
- **Lookahead** is configurable (N patterns or T seconds ahead); tiers for required-now vs prefetch vs on-demand
- **Seek/loop** reuse cached CIDs; fetch plan follows jumps in the order list, not sequential pattern files

Optional: inline tiny samples in the manifest to avoid extra round trips for short one-shot hits.

Designing the desktop client with stable content addressing and manifest-first loading in mind (even before IPFS lands) will make this migration easier.

## Project status

**Early stage.** Repository bootstrap—product spec and README only. Implementation details (stack, API shape, Mod Archive integration) will be documented here as they land.

## License

Copyright © 2026 guysv

**trackerstream** is licensed under the [GNU Affero General Public License v3.0 or later](LICENSE) (SPDX: `AGPL-3.0-or-later`).

Tracker modules streamed or referenced through the app remain under their respective authors’ terms; Mod Archive attribution applies separately from this software license.
