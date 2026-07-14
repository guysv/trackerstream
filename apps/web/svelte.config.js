// Same shape as the desktop: adapter-static with an index.html fallback = SPA mode. The desktop
// picked it because Tauri has no Node server; the web build wants it because Caddy serves this as
// plain static files off the master.
import adapter from "@sveltejs/adapter-static";
import { vitePreprocess } from "@sveltejs/vite-plugin-svelte";

/** @type {import('@sveltejs/kit').Config} */
export default {
  preprocess: vitePreprocess(),
  kit: { adapter: adapter({ fallback: "index.html" }) },
};
