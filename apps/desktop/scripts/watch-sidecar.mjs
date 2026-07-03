#!/usr/bin/env node
// Rebuild the Go `tsnode` sidecar when node/ changes — the piece `tauri dev` misses.
//
// Tauri's dev watcher only watches the Rust crate, so a Go-only edit (or a git-sync
// that lands new node/ code) never rebuilds the sidecar: the app just keeps re-spawning
// the stale binary. This watcher closes that gap. On a node/ change it runs
// prepare-sidecar.sh (rebuilds binaries/tsnode-<triple> AND refreshes the dev-run copy
// at target/<profile>/tsnode), then bumps src-tauri/src/main.rs so Tauri's own watcher
// restarts the app and picks up the fresh node.
//
// Skipped when TS_DEV_LOCKED is set: dev-locked.mjs already watches rust+go and drives
// its own restarts, so running here too would only double-trigger.
//
// Started automatically by scripts/dev.mjs (i.e. by every `tauri dev`); not run alone.

import { spawnSync } from 'node:child_process'
import { watch, existsSync, utimesSync } from 'node:fs'
import { join, dirname, resolve, extname } from 'node:path'
import { fileURLToPath } from 'node:url'

const DESKTOP = resolve(dirname(fileURLToPath(import.meta.url)), '..') // apps/desktop
const REPO = resolve(DESKTOP, '..', '..')
const NODE_DIR = join(REPO, 'node')
const MAIN_RS = join(DESKTOP, 'src-tauri', 'src', 'main.rs')
const PREPARE = join(DESKTOP, 'src-tauri', 'scripts', 'prepare-sidecar.sh')

const DEBOUNCE_MS = 400
const EXTS = ['.go', '.mod', '.sum']

const log = (msg) => console.log(`\x1b[36m[sidecar]\x1b[0m ${msg}`)

if (process.env.TS_DEV_LOCKED) {
  log('dev:locked owns rust+go rebuilds — sidecar watcher idle')
  process.exit(0)
}
if (!existsSync(NODE_DIR)) {
  log(`no node/ at ${NODE_DIR} — sidecar watcher idle`)
  process.exit(0)
}

let building = false
let queued = false

function rebuild(reason) {
  if (building) {
    queued = true // coalesce changes that land mid-build into one follow-up
    return
  }
  building = true
  log(`rebuilding tsnode — ${reason}`)
  // prepare-sidecar.sh is idempotent (stamp-guarded) and prints its own progress.
  const r = spawnSync('bash', [PREPARE], { cwd: DESKTOP, stdio: 'inherit' })
  building = false
  if (r.status !== 0) {
    log(`build failed (exit ${r.status}) — app left on the previous node`)
  } else if (existsSync(MAIN_RS)) {
    // Nudge Tauri's Rust watcher so it restarts the app onto the fresh sidecar.
    const now = new Date()
    try {
      utimesSync(MAIN_RS, now, now)
      log('rebuilt — restarting app to pick up the new node')
    } catch {
      log('rebuilt — could not touch main.rs; restart the app to load the new node')
    }
  }
  if (queued) {
    queued = false
    rebuild('coalesced changes during build')
  }
}

let timer = null
function onChange(what) {
  if (timer) clearTimeout(timer)
  timer = setTimeout(() => {
    timer = null
    rebuild(what)
  }, DEBOUNCE_MS)
}

function relevant(filename) {
  if (!filename) return true // some fs events omit the name; be conservative
  const name = String(filename)
  if (name.includes('/.') || name.startsWith('.') || name.includes('~')) return false
  return EXTS.includes(extname(name))
}

watch(NODE_DIR, { recursive: true }, (_ev, filename) => {
  if (relevant(filename)) onChange(`node/${filename}`)
})

log(`watching ${NODE_DIR} for go changes`)
