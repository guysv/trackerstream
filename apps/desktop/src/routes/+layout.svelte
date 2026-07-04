<script lang="ts">
  import "$lib/theme.css";
  import { onMount, tick, untrack } from "svelte";
  import { goto } from "$app/navigation";
  import { page } from "$app/stores";
  import NavRail from "$lib/components/NavRail.svelte";
  import ContextMenu from "$lib/components/ContextMenu.svelte";
  import NowPlaying from "$lib/components/NowPlaying.svelte";
  import QueuePanel from "$lib/components/QueuePanel.svelte";
  import PeersPanel from "$lib/components/PeersPanel.svelte";
  import DetailPanel from "$lib/components/DetailPanel.svelte";
  import { playNext, playPrev, player, nowPlaying, queue } from "$lib/player.svelte";
  import { peers, startPeerPolling } from "$lib/peers.svelte";
  import { initDeepLinks } from "$lib/deeplink";
  import { plIngestLink, plBump } from "$lib/playlists.svelte";
  import { dbg } from "$lib/debug";
  import { keepaliveMaster } from "$lib/p2p";
  import { BOOTSTRAP_MULTIADDRS } from "@trackerstream/config";
  import { ui } from "$lib/ui.svelte";

  let { children } = $props();

  // Suppress the webview's native context menu app-wide (Spotify-like: blank areas show
  // nothing). Element handlers that open our menu stopPropagation, so this only fires for
  // unhandled targets. Kept enabled in production; left on in dev so "Inspect Element" works.
  function suppressNativeMenu(e: MouseEvent) {
    if (!import.meta.env.DEV) e.preventDefault();
  }

  // Search box ⇄ /search?q. Local text is the box value; typing navigates. Publish the
  // element to the ui store so the global "/" shortcut can focus it from anywhere.
  let searchEl: HTMLInputElement | undefined = $state();
  $effect(() => {
    ui.searchEl = searchEl;
  });
  let qInput = $state("");
  // Sync the box from the URL when arriving on /search via link / back / forward. Only
  // depends on the URL — qInput is read via untrack so typing (which changes qInput before
  // the async goto commits the new URL) doesn't re-fire this and clobber the keystroke.
  $effect(() => {
    if ($page.url.pathname === "/search") {
      const urlq = $page.url.searchParams.get("q") ?? "";
      untrack(() => {
        if (urlq !== qInput) qInput = urlq;
      });
    }
  });

  function onSearchInput() {
    const q = qInput.trim();
    const onSearch = $page.url.pathname === "/search";
    if (onSearch && q === ($page.url.searchParams.get("q") ?? "").trim()) return;
    // First transition from another route pushes one history entry into /search;
    // subsequent keystrokes replace it so history isn't polluted keystroke-by-keystroke.
    void goto(q ? "/search?q=" + encodeURIComponent(q) : "/search", {
      keepFocus: true,
      noScroll: true,
      replaceState: onSearch,
    });
  }

  const SHORTCUTS: Array<[string, string]> = [
    ["/", "focus search"],
    ["Space", "play / pause"],
    ["↑ / ↓", "move selection"],
    ["PgUp / PgDn", "page selection"],
    ["Enter", "play selected"],
    ["← / →", "seek ∓5 s (seek bar)"],
    ["?", "this help"],
    ["Esc", "close"],
  ];

  function globalKeys(e: KeyboardEvent) {
    // Any focused text-entry element owns its keystrokes — otherwise Space, "/" and "?"
    // get swallowed while typing (e.g. renaming a playlist), not just in the search box.
    const el = document.activeElement as HTMLElement | null;
    const inField =
      el === ui.searchEl ||
      el?.tagName === "INPUT" ||
      el?.tagName === "TEXTAREA" ||
      el?.isContentEditable === true;
    if (e.key === "Escape") {
      ui.showHelp = false;
      if (inField) ui.searchEl?.blur();
    } else if (e.key === "?" && !inField) {
      e.preventDefault();
      ui.showHelp = !ui.showHelp;
    } else if (e.key === "/" && !inField) {
      e.preventDefault();
      ui.searchEl?.focus();
    } else if (e.key === " " && !inField) {
      e.preventDefault();
      if (player.info) player.toggle();
    }
  }

  onMount(() => {
    player.init().then(() => player.loadSettings());
    // Deep links: an incoming trackerstream:// (or /p/ handoff) URL is verified + ingested
    // in Rust, then we navigate to its playlist route — a not-yet-synced name lands on the
    // route's "syncing…" placeholder until gossip delivers.
    void initDeepLinks(async (url) => {
      try {
        const st = await plIngestLink(url);
        plBump();
        await tick(); // ensure the router is mounted before the first navigation
        await goto("/playlists/" + encodeURIComponent(st.name));
      } catch (e) {
        dbg("deep link rejected", { url, error: String(e) });
      }
    });
    // Hold a persistent master connection from startup (not lazily per-play), so the peers
    // pane reflects reality and uncached playback skips the re-dial.
    void keepaliveMaster(BOOTSTRAP_MULTIADDRS);
    const stopPeers = startPeerPolling();
    window.addEventListener("keydown", globalKeys);
    return () => {
      window.removeEventListener("keydown", globalKeys);
      stopPeers();
    };
  });
</script>

<svelte:window oncontextmenu={suppressNativeMenu} />
<ContextMenu />

<div class="app">
  <header>
    <input
      bind:this={searchEl}
      bind:value={qInput}
      oninput={onSearchInput}
      class="search"
      placeholder="search title / file / instruments / comments / playlists    (press /)"
      spellcheck="false"
    />
    <span class="status">{ui.status}</span>
    <div class="rtoggles">
      <button
        class="rtoggle peers"
        class:on={ui.right === "peers"}
        onclick={() => (ui.right = ui.right === "peers" ? "detail" : "peers")}
      >
        peers · {peers.connected}
      </button>
      <button
        class="rtoggle"
        class:on={ui.right === "queue"}
        onclick={() => (ui.right = ui.right === "queue" ? "detail" : "queue")}
      >
        queue · {queue.items.length}
      </button>
    </div>
    <span class="engine">{player.ready ? "engine ●" : "engine ○"}</span>
  </header>

  <main>
    <NavRail />
    <section class="center">
      {@render children()}
    </section>
    <aside class="detail">
      {#if ui.right === "queue"}
        <QueuePanel />
      {:else if ui.right === "peers"}
        <PeersPanel />
      {:else}
        <DetailPanel id={ui.inspectorTrackId} />
      {/if}
    </aside>
  </main>

  {#if nowPlaying.error}<div class="toast">{nowPlaying.error}</div>{/if}
  <NowPlaying onnext={playNext} onprev={playPrev} />

  {#if ui.showHelp}
    <div class="help-bg" onclick={() => (ui.showHelp = false)} role="presentation">
      <div class="help" role="dialog" aria-label="keyboard shortcuts">
        <div class="help-title">keyboard shortcuts</div>
        {#each SHORTCUTS as [k, d]}
          <div class="help-row"><kbd>{k}</kbd><span>{d}</span></div>
        {/each}
        <div class="about">
          trackerstream · AGPL-3.0-or-later · source: github.com/guysv/trackerstream<br />
          module bytes © their authors · Mod Archive attribution applies
        </div>
      </div>
    </div>
  {/if}
</div>

<style>
  .app {
    display: grid;
    grid-template-rows: auto 1fr auto;
    height: 100vh;
    overflow: hidden;
  }
  header {
    display: flex;
    align-items: center;
    gap: 1rem;
    padding: 0.6rem 1rem;
    background: var(--panel);
    border-bottom: 1px solid var(--border);
  }
  .search {
    flex: 1;
    max-width: 640px;
  }
  .status {
    color: var(--dim);
    min-width: 90px;
  }
  .rtoggles {
    margin-left: auto;
    display: flex;
    gap: 0.4rem;
  }
  .rtoggle {
    font-size: 12px;
  }
  .rtoggle.on {
    border-color: var(--amber);
    color: var(--amber);
  }
  /* peers pane uses cyan as its accent (matches PeersPanel header) */
  .rtoggle.peers.on {
    border-color: var(--cyan);
    color: var(--cyan);
  }
  .engine {
    color: var(--dim);
    font-size: 11px;
  }
  main {
    display: grid;
    grid-template-columns: 240px 1fr 320px;
    min-height: 0;
    overflow: hidden;
  }
  .center {
    display: flex;
    flex-direction: column;
    min-width: 0;
    min-height: 0;
    overflow: hidden;
    border-right: 1px solid var(--border);
  }
  .detail {
    min-height: 0;
    overflow: hidden;
    background: var(--panel-2);
  }
  .toast {
    position: absolute;
    bottom: 72px;
    left: 50%;
    transform: translateX(-50%);
    background: var(--hot);
    color: #1a1b26;
    padding: 0.4rem 0.8rem;
    border-radius: 4px;
    font-size: 12px;
  }
  .help-bg {
    position: fixed;
    inset: 0;
    background: rgba(0, 0, 0, 0.6);
    display: grid;
    place-items: center;
    z-index: 10;
  }
  .help {
    background: var(--panel);
    border: 1px solid var(--border-hi);
    border-radius: 8px;
    padding: 1.2rem 1.5rem;
    min-width: 320px;
  }
  .help-title {
    color: var(--accent);
    text-transform: uppercase;
    font-size: 12px;
    letter-spacing: 0.06em;
    margin-bottom: 0.8rem;
  }
  .help-row {
    display: flex;
    justify-content: space-between;
    gap: 2rem;
    padding: 0.25rem 0;
  }
  kbd {
    background: var(--bg);
    border: 1px solid var(--border-hi);
    border-radius: 4px;
    padding: 0.05rem 0.4rem;
    color: var(--cyan);
  }
  .help-row span {
    color: var(--dim);
  }
  .about {
    margin-top: 1rem;
    padding-top: 0.8rem;
    border-top: 1px solid var(--border);
    color: var(--dim);
    font-size: 10px;
    line-height: 1.5;
  }
</style>
