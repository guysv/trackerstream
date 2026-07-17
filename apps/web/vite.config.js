import { defineConfig } from "vite";
import { sveltekit } from "@sveltejs/kit/vite";

export default defineConfig(({ command }) => ({
  plugins: [sveltekit()],
  optimizeDeps: {
    // wa-sqlite ships its own .mjs/.wasm pair; esbuild pre-bundling breaks the wasm locate.
    exclude: ["@journeyapps/wa-sqlite"],
  },
  worker: { format: "es" },
  // Dev server only: proxy /bootstrap.json to prod so the browser fetches it SAME-ORIGIN. In
  // production the app is served from trackerstream.xyz (same origin as the endpoint); on localhost a
  // direct fetch to https://trackerstream.xyz/bootstrap.json is a cross-origin request the browser
  // blocks, so boot fails. Set VITE_BOOTSTRAP_URL=/bootstrap.json to route through this. No effect on
  // the static build (`command === "build"`).
  ...(command === "serve" && {
    server: {
      // The catalog runs in a Web Worker that lives in a sibling workspace package
      // (packages/catalog-web/src/worker.ts). Vite's dev server refuses to serve files outside the
      // app root by default (server.fs.allow), so the worker fetch fails (net::ERR_FAILED) and every
      // catalog query hangs. Allow the monorepo root so cross-package source (worker, ui, etc.) is
      // dev-servable. No effect on the bundled production build.
      fs: { allow: ["../.."] },
      proxy: {
        "/bootstrap.json": { target: "https://trackerstream.xyz", changeOrigin: true, secure: true },
        "/catalog-warm.json": { target: "https://trackerstream.xyz", changeOrigin: true, secure: true },
        "/catalog-warm.bin": { target: "https://trackerstream.xyz", changeOrigin: true, secure: true },
        // The warm bundle is served by prod Caddy (static, per-bake). Proxy it same-origin in dev so
        // a locally-run web client pre-loads the hot catalog pages exactly as a deployed one would.
      },
    },
  }),
}));
