#!/usr/bin/env node
// Lock-aware wrapper around `tauri dev --no-watch`.
//
// Why this exists: `tauri dev`'s own watcher recompiles on every file save, so an
// agent editing Rust/Go mid-change thrashes the app up and down. Tauri's watcher
// can't be paused live (`.taurignore` is read once at startup), so instead we run
// Tauri with `--no-watch` and drive rebuilds ourselves — gated by a lock file.
//
//   • normal use: edit Rust/Go → one coalesced rebuild (same DX as `tauri dev`).
//   • locked use: `touch .dev-lock` → changes are deferred; `rm .dev-lock` → one
//     clean rebuild with the finished code. Meant for an agent doing multi-file
//     surgery who doesn't want half-written WIP compiled.
//
// Frontend (Svelte/Vite) edits are untouched: Vite HMR lives inside the tauri
// child and only rust/go changes ever restart it.
//
// Run from apps/desktop:  node scripts/dev-locked.mjs   (or: pnpm dev:locked)

import { spawn } from 'node:child_process'
import { watch, existsSync } from 'node:fs'
import { join, dirname, resolve, extname } from 'node:path'
import { fileURLToPath } from 'node:url'

const DESKTOP = resolve(dirname(fileURLToPath(import.meta.url)), '..') // apps/desktop
const REPO = resolve(DESKTOP, '..', '..')
const LOCK = join(DESKTOP, '.dev-lock')

// Debounce so a burst of saves (or a multi-file edit) collapses into one rebuild.
const DEBOUNCE_MS = 400

// Paths we react to. Directories are watched recursively; files individually.
const WATCH_DIRS = [
  { path: join(DESKTOP, 'src-tauri', 'src'), exts: ['.rs'] },
  { path: join(REPO, 'node'), exts: ['.go', '.mod', '.sum'] },
]
const WATCH_FILES = [
  join(DESKTOP, 'src-tauri', 'Cargo.toml'),
  join(DESKTOP, 'src-tauri', 'build.rs'),
]

const log = (msg) => console.log(`\x1b[35m[dev-locked]\x1b[0m ${msg}`)

// ── tauri child lifecycle ────────────────────────────────────────────────────
let child = null
let restarting = false

function startTauri() {
  // detached so we can signal the whole process group (pnpm → tauri → cargo → app).
  child = spawn('pnpm', ['tauri', 'dev', '--no-watch'], {
    cwd: DESKTOP,
    stdio: 'inherit',
    detached: true,
    // We already watch rust+go here and drive our own restarts; tell the sidecar
    // watcher (started by `pnpm dev` via beforeDevCommand) to stand down so a go
    // change isn't rebuilt twice.
    env: { ...process.env, TS_DEV_LOCKED: '1' },
  })
  child.on('exit', (code, signal) => {
    if (restarting) return // expected kill during a rebuild
    log(`tauri exited (code=${code} signal=${signal}); shutting down wrapper`)
    process.exit(code ?? 0)
  })
}

function killTauri() {
  return new Promise((done) => {
    if (!child || child.exitCode !== null) return done()
    const c = child
    c.once('exit', () => done())
    try {
      process.kill(-c.pid, 'SIGTERM') // negative pid → whole group
    } catch {
      done()
      return
    }
    // hard-kill if it doesn't go down promptly
    setTimeout(() => {
      try { process.kill(-c.pid, 'SIGKILL') } catch {}
    }, 4000).unref()
  })
}

async function rebuild(reason) {
  if (restarting) return
  restarting = true
  log(`🔁 rebuilding — ${reason}`)
  await killTauri()
  startTauri()
  restarting = false
}

// ── lock + change bookkeeping ────────────────────────────────────────────────
let pending = null // reason string for a change deferred while locked
let debounceTimer = null

const isLocked = () => existsSync(LOCK)

function onSourceChange(what) {
  if (debounceTimer) clearTimeout(debounceTimer)
  debounceTimer = setTimeout(() => {
    debounceTimer = null
    if (isLocked()) {
      pending = what
      log(`🔒 held (tree locked): ${what}`)
    } else {
      rebuild(what)
    }
  }, DEBOUNCE_MS)
}

function onLockRemoved() {
  if (pending) {
    const what = pending
    pending = null
    log('🔓 unlocked with pending changes')
    rebuild(what)
  } else {
    log('🔓 unlocked (no changes)')
  }
}

// ── watchers ─────────────────────────────────────────────────────────────────
function relevant(exts, filename) {
  if (!filename) return true // some fs events omit the name; be conservative
  const name = String(filename)
  if (name.includes('/.') || name.includes('~')) return false // dot/backup churn
  return exts.includes(extname(name))
}

for (const { path, exts } of WATCH_DIRS) {
  if (!existsSync(path)) { log(`skip (missing): ${path}`); continue }
  watch(path, { recursive: true }, (_ev, filename) => {
    if (relevant(exts, filename)) onSourceChange(`${path.split('/').pop()}/${filename}`)
  })
}

for (const file of WATCH_FILES) {
  if (!existsSync(file)) continue
  watch(file, () => onSourceChange(file.split('/').pop()))
}

// Watch the lock file by watching its directory (survives create/delete cycles).
let wasLocked = isLocked()
watch(DESKTOP, (_ev, filename) => {
  if (filename !== '.dev-lock') return
  const now = isLocked()
  if (now === wasLocked) return
  wasLocked = now
  if (now) log('🔒 tree locked — deferring rebuilds')
  else onLockRemoved()
})

// ── go ───────────────────────────────────────────────────────────────────────
process.on('SIGINT', async () => { restarting = true; await killTauri(); process.exit(0) })
process.on('SIGTERM', async () => { restarting = true; await killTauri(); process.exit(0) })

log(`watching rust+go, gated on ${LOCK.replace(REPO + '/', '')}`)
if (wasLocked) log('🔒 starting with tree already locked')
startTauri()
