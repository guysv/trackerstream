<script lang="ts">
  import "../theme.css";
  import { onMount, tick, untrack } from "svelte";
  import { goto } from "$app/navigation";
  import { page } from "$app/stores";
  import NavRail from "../components/NavRail.svelte";
  import ContextMenu from "../components/ContextMenu.svelte";
  import NowPlaying from "../components/NowPlaying.svelte";
  import QueuePanel from "../components/QueuePanel.svelte";
  import PeersPanel from "../components/PeersPanel.svelte";
  import DetailPanel from "../components/DetailPanel.svelte";
  import { playNext, playPrev, player, nowPlaying, queue, refreshQueueRoots } from "../player.svelte";
  import { peers, startPeerPolling } from "../peers.svelte";
  import { initDeepLinks } from "../deeplink";
  import { initMediaKeys, syncMediaSession } from "../mediaKeys";
  import { plIngestLink, plBump, initPlaylists } from "../playlists.svelte";
  import { dbg } from "../debug";
  import { toasts, dismiss } from "../toast.svelte";
  import { keepaliveMaster } from "../p2p";
  import { BOOTSTRAP_MULTIADDRS } from "@trackerstream/config";
  import { ui } from "../ui.svelte";

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

  // Keep the OS Now Playing widget in step with what's loaded and whether it's playing. The
  // reads inside syncMediaSession (nowPlaying.hit, player.playing) make this effect re-run on
  // every track change and play/pause.
  $effect(() => {
    syncMediaSession();
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

  // "names only" search mode: restrict matching to title/filename (vs. also matching the
  // instrument + comment text — great for discovery, noisy when fetching a known module).
  // Carried in the URL alongside ?q so it persists across reload/back-forward and the search
  // route re-runs when it flips. Both the typing path and the toggle preserve the other's param.
  const namesOnly = $derived($page.url.searchParams.get("names") === "1");

  function searchUrl(q: string, names: boolean): string {
    const params = new URLSearchParams();
    if (q) params.set("q", q);
    if (names) params.set("names", "1");
    const qs = params.toString();
    return qs ? "/search?" + qs : "/search";
  }

  function onSearchInput() {
    const q = qInput.trim();
    const onSearch = $page.url.pathname === "/search";
    if (onSearch && q === ($page.url.searchParams.get("q") ?? "").trim()) return;
    // First transition from another route pushes one history entry into /search;
    // subsequent keystrokes replace it so history isn't polluted keystroke-by-keystroke.
    void goto(searchUrl(q, namesOnly), {
      keepFocus: true,
      noScroll: true,
      replaceState: onSearch,
    });
  }

  function toggleNames() {
    // Flip the mode, keep the current box text, and keep focus in the box so the "/"-then-type
    // flow isn't interrupted. Replace history when already on /search (like a keystroke).
    void goto(searchUrl(qInput.trim(), !namesOnly), {
      keepFocus: true,
      noScroll: true,
      replaceState: $page.url.pathname === "/search",
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
    // Must run after the shell's setClient(): the playlist store's live signals talk to the node.
    const stopPlaylists = initPlaylists();
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
    // Re-resolve the persisted queue's CIDs by md5 — a corpus rebake since it was saved
    // would otherwise leave the restored "up next" list pointing at orphaned CIDs. Retries
    // internally while the node/catalog warms; no-op for a fresh (this-session) queue.
    void refreshQueueRoots();
    const stopPeers = startPeerPolling();
    window.addEventListener("keydown", globalKeys);
    // Hardware media keys + headphone transport → playback controls (macOS via the native
    // MediaPlayer bridge, Win/Linux via global shortcuts). syncMediaSession (the $effect above)
    // keeps the OS Now Playing widget in step.
    const stopMediaKeys = initMediaKeys();
    return () => {
      stopPlaylists();
      window.removeEventListener("keydown", globalKeys);
      stopPeers();
      stopMediaKeys();
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
      placeholder={namesOnly
        ? "search title / file    (names only · press /)"
        : "search title / file / instruments / comments / playlists    (press /)"}
      spellcheck="false"
    />
    <button
      class="rtoggle names"
      class:on={namesOnly}
      onclick={toggleNames}
      title={namesOnly
        ? "names only: matching title / filename. Click to also match instruments + comments."
        : "matching title / file / instruments / comments. Click to restrict to names only."}
    >
      names only
    </button>
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
        <DetailPanel id={ui.inspectorTrackId} md5={ui.inspectorTrackMd5} />
      {/if}
    </aside>
  </main>

  {#if nowPlaying.error}<div class="toast">{nowPlaying.error}</div>{/if}
  {#if toasts.length}
    <div class="toasts" role="alert" aria-live="polite">
      {#each toasts as t (t.id)}
        <button
          type="button"
          class="toast toast-{t.kind}"
          onclick={() => dismiss(t.id)}
          title="dismiss"
        >
          {t.message}
        </button>
      {/each}
    </div>
  {/if}
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
  /* General toast stack (non-playback notices). Sits above the now-playing bar; newest on top,
     each click-to-dismiss. Positioned so it doesn't collide with the nowPlaying.error toast. */
  .toasts {
    position: absolute;
    bottom: 96px;
    left: 50%;
    transform: translateX(-50%);
    display: flex;
    flex-direction: column-reverse;
    gap: 6px;
    align-items: center;
    z-index: 20;
    max-width: 80%;
  }
  .toasts .toast {
    position: static;
    transform: none;
    cursor: pointer;
    max-width: 100%;
    text-align: center;
    border: none;
    font: inherit;
    font-size: 12px;
  }
  .toast-info {
    background: var(--accent);
    color: #1a1b26;
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
