// Pre-bundle the AudioWorkletProcessor into a single self-contained CLASSIC
// script (WASM inlined, no imports) so audioWorklet.addModule() works in any
// WebView regardless of ES-module-worklet support. Output -> static/.
//
// Run by the predev/prebuild npm hooks; re-run when the worklet or the WASM
// build changes.
import { build } from "esbuild";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, "..");

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
  entryPoints: [resolve(root, "src/lib/audio/player.worklet.ts")],
  outfile: resolve(root, "static/player.worklet.js"),
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

console.log("built static/player.worklet.js");
