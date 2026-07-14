import { defineConfig } from "vite";
import { sveltekit } from "@sveltejs/kit/vite";

export default defineConfig({
  plugins: [sveltekit()],
  optimizeDeps: {
    // wa-sqlite ships its own .mjs/.wasm pair; esbuild pre-bundling breaks the wasm locate.
    exclude: ["@journeyapps/wa-sqlite"],
  },
  worker: { format: "es" },
});
