# Releasing the trackerstream desktop client (signing, notarization, updates)

Covers MVP-FOLLOWUP **D2**: code signing + notarization + auto-update + the
Windows/Linux build targets. The default build is **unsigned** (Gatekeeper warns;
right-click → Open to run). Everything below is opt-in and env-driven — the
unsigned `tauri build` keeps working unchanged.

The bundle is already configured for it: hardened-runtime `entitlements.plist`
(JIT/network — needed by the libopenmpt WASM + embedded rust-ipfs node), macOS
`minimumSystemVersion` 11.0, and `bundle.targets: "all"` (see `tauri.conf.json`).

## macOS — sign + notarize (needs an Apple Developer account)

Tauri reads signing/notarization from the environment at `tauri build` time; no
secrets live in the repo.

```bash
# Developer ID Application cert (from your Apple Developer account), in the login keychain:
export APPLE_SIGNING_IDENTITY="Developer ID Application: Your Name (TEAMID)"

# Notarization — either an App Store Connect API key (preferred) …
export APPLE_API_ISSUER="<issuer-uuid>"
export APPLE_API_KEY="<key-id>"
export APPLE_API_KEY_PATH="/path/to/AuthKey_<key-id>.p8"
# … or an app-specific password:
#   export APPLE_ID="you@example.com"
#   export APPLE_PASSWORD="<app-specific-password>"
#   export APPLE_TEAM_ID="TEAMID"

pnpm --filter @trackerstream/desktop tauri build
# Tauri signs with the hardened runtime + entitlements.plist, then notarizes and
# staples the .dmg/.app. Verify:
spctl -a -vv "apps/desktop/src-tauri/target/release/bundle/macos/trackerstream.app"
```

> The entitlements enable `allow-jit` + `allow-unsigned-executable-memory` (WASM)
> and network client/server (the P2P node). Don't remove them or playback / the
> embedded node will fail under the hardened runtime.

## Auto-update (Tauri updater)

1. Add the plugin (Rust + JS) and generate a signing keypair:
   ```bash
   pnpm --filter @trackerstream/desktop add @tauri-apps/plugin-updater
   cargo add tauri-plugin-updater --manifest-path apps/desktop/src-tauri/Cargo.toml
   pnpm --filter @trackerstream/desktop tauri signer generate -w ~/.tauri/trackerstream.key
   # keep the PRIVATE key secret (CI secret TAURI_SIGNING_PRIVATE_KEY); the PUBLIC key goes in config
   ```
2. In `tauri.conf.json` add (uncomment-ready):
   ```jsonc
   "plugins": {
     "updater": {
       "endpoints": ["https://trackerstream.xyz/updates/{{target}}-{{arch}}/{{current_version}}"],
       "pubkey": "<public key from signer generate>"
     }
   },
   "bundle": { "createUpdaterArtifacts": true }
   ```
   Register `tauri_plugin_updater::Builder::new().build()` in `lib.rs` and call the
   JS `check()` on startup. Host the signed `latest.json` + artifacts behind Caddy
   (a `/updates/` route on the droplet, or GitHub Releases).
3. Sign at build time with `TAURI_SIGNING_PRIVATE_KEY` (+ password) in the env.

This is deferred (no update server stood up yet) but the path is the above; it
composes with the macOS signing/notarization step.

## Windows + Linux targets

`bundle.targets` is `"all"`, so building on each OS produces its native bundles:

- **Windows** (build on Windows): `pnpm --filter @trackerstream/desktop tauri build`
  → `.msi` + NSIS `.exe`. Sign with `signtool` / set `WINDOWS_CERTIFICATE*` env, or
  use Azure Trusted Signing. Needs the WebView2 runtime (bootstrapped by the NSIS
  installer by default).
- **Linux** (build on Linux): same command → `.deb` + `.AppImage` (+ `.rpm` with
  the rpm tooling installed). Deep links additionally need the
  `tauri-plugin-single-instance` plugin on Linux/Windows to forward the URL to a
  running instance (macOS delivers it natively).

Cross-compiling desktop bundles is not supported by Tauri — build each target on
its own OS (a CI matrix: macos-latest / windows-latest / ubuntu-latest).

## CI hint

Extend `.github/workflows/ci.yml` (or a separate `release.yml`) with a
`tauri-apps/tauri-action` matrix over the three runners, injecting the signing
secrets per-OS. Gate it on tags (`v*`) so PR CI stays fast and unsigned.
