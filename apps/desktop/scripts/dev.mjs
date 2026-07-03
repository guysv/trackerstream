#!/usr/bin/env node
// Dev launcher: run Vite AND the Go-sidecar watcher together.
//
// This is `pnpm dev`, which is also Tauri's `beforeDevCommand` — so `pnpm tauri dev`
// now rebuilds BOTH the frontend (Vite HMR) and the native node (watch-sidecar.mjs) on
// change. Tauri's own watcher can't see node/, which is why a Go edit otherwise never
// rebuilt the sidecar. The initial sidecar build already happened in `predev`
// (prepare:sidecar); the watcher only reacts to live changes from here on.
//
// Under dev:locked the watcher self-exits (TS_DEV_LOCKED) since dev-locked.mjs already
// watches rust+go and drives its own restarts.

import { spawn } from 'node:child_process'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const DESKTOP = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const children = []

function start(cmd, args) {
  const c = spawn(cmd, args, { cwd: DESKTOP, stdio: 'inherit' })
  children.push(c)
  c.on('exit', (code, signal) => {
    // Vite going down means the dev session is over — tear everything down so Tauri's
    // beforeDevCommand doesn't linger. The watcher exiting on its own (locked) is fine.
    if (c === vite) shutdown(code ?? (signal ? 1 : 0))
  })
  return c
}

let shuttingDown = false
function shutdown(code) {
  if (shuttingDown) return
  shuttingDown = true
  for (const c of children) {
    if (c.exitCode === null) {
      try { c.kill('SIGTERM') } catch {}
    }
  }
  process.exit(code)
}

const vite = start('vite', ['dev'])
// The sidecar watcher no-ops immediately when TS_DEV_LOCKED is set.
start('node', [join(DESKTOP, 'scripts', 'watch-sidecar.mjs')])

process.on('SIGINT', () => shutdown(0))
process.on('SIGTERM', () => shutdown(0))
