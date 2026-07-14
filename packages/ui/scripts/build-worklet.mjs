// Pre-bundle the AudioWorkletProcessor into a single self-contained CLASSIC
// script (WASM inlined, no imports) so audioWorklet.addModule() works in any
// WebView regardless of ES-module-worklet support.
//
// The worklet lives in @trackerstream/ui but must be SERVED from each shell's static dir (the
// worklet is fetched by URL, and ModPlayer loads it from "/player.worklet.js"), so the consumer
// passes its own output path:
//
//   node ../../packages/ui/scripts/build-worklet.mjs static/player.worklet.js
//
// Run by the predev/prebuild hooks; re-run when the worklet or the WASM build changes.
import { build } from "esbuild";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, "..");
// Relative to the CALLER's cwd, so each app drops it in its own static/.
const outfile = resolve(process.cwd(), process.argv[2] ?? "static/player.worklet.js");

// Stub Node built-ins: libopenmpt's runtime references them only inside its
// dead ENVIRONMENT_IS_NODE branch (false in a worklet), but esbuild still needs
// to resolve them at bundle time.
const stubNodeBuiltins = {
  name: "stub-node-builtins",
  setup(b) {
    const builtins = /^(module|fs|path|crypto|url|os|util|worker_threads|node:.*)$/;
    b.onResolve({ filter: builtins }, (args) => ({ path: args.path, namespace: "node-stub" }));
    b.onLoad({ filter: /.*/, namespace: "node-stub" }, () => ({
      contents: "export default {}; export const createRequire = () => () => ({});",
      loader: "js",
    }));
  },
};

await build({
  entryPoints: [resolve(root, "src/audio/player.worklet.ts")],
  outfile,
  bundle: true,
  format: "iife",
  platform: "browser",
  target: ["safari15", "chrome100"],
  minify: true,
  sourcemap: false,
  legalComments: "none",
  define: { "import.meta.url": '""' },
  plugins: [stubNodeBuiltins],
  logLevel: "info",
});

console.log(`built ${outfile}`);
