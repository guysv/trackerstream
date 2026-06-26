<script lang="ts">
  import { onMount } from "svelte";
  import Sidebar from "$lib/components/Sidebar.svelte";
  import ResultsTable from "$lib/components/ResultsTable.svelte";
  import DetailPanel from "$lib/components/DetailPanel.svelte";
  import NowPlaying from "$lib/components/NowPlaying.svelte";
  import QueuePanel from "$lib/components/QueuePanel.svelte";
  import {
    search,
    listModules,
    getFormats,
    type ModuleHit,
    type FormatCount,
  } from "$lib/catalog";
  import { playList, playNext, playPrev, player, nowPlaying, queue } from "$lib/player.svelte";
  import { initDeepLinks } from "$lib/deeplink";

  let rightView = $state<"detail" | "queue">("detail");

  function play(h: ModuleHit) {
    playList(
      rows,
      rows.findIndex((r) => r.id === h.id),
    );
  }

  let query = $state("");
  let format = $state<string | null>(null);
  let sort = $state<"latest" | "random" | "title">("latest");
  let rows = $state<ModuleHit[]>([]);
  let selectedId = $state<number | null>(null);
  let formats = $state<FormatCount[]>([]);
  let total = $state(0);
  let loading = $state(false);
  let apiError = $state(false);
  let showHelp = $state(false);
  let searchEl: HTMLInputElement | undefined = $state();

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
    const inField = document.activeElement === searchEl;
    if (e.key === "Escape") {
      showHelp = false;
      if (inField) searchEl?.blur();
    } else if (e.key === "?" && !inField) {
      e.preventDefault();
      showHelp = !showHelp;
    } else if (e.key === "/" && !inField) {
      e.preventDefault();
      searchEl?.focus();
    } else if (e.key === " " && !inField) {
      e.preventDefault();
      if (player.info) player.toggle();
    }
  }

  onMount(() => {
    player.init().then(() => player.loadSettings());
    getFormats()
      .then((f) => ((formats = f.formats), (total = f.total)))
      .catch(() => (apiError = true));
    // E2: register the trackerstream:// scheme (handlers land with P2P sharing).
    void initDeepLinks();
    window.addEventListener("keydown", globalKeys);
    return () => window.removeEventListener("keydown", globalKeys);
  });

  let timer: ReturnType<typeof setTimeout>;
  $effect(() => {
    const q = query.trim();
    const fmt = format;
    const s = sort;
    clearTimeout(timer);
    loading = true;
    timer = setTimeout(
      async () => {
        try {
          rows = q
            ? await search(q, 200)
            : await listModules({ format: fmt ?? undefined, sort: s, limit: 300 });
          apiError = false;
          if (!rows.some((x) => x.id === selectedId)) selectedId = rows[0]?.id ?? null;
        } catch {
          apiError = true;
        } finally {
          loading = false;
        }
      },
      q ? 180 : 0,
    );
  });
</script>

<div class="app">
  <header>
    <input
      bind:this={searchEl}
      bind:value={query}
      class="search"
      placeholder="search title / file / instruments / comments    (press /)"
      spellcheck="false"
    />
    <span class="status">
      {#if apiError}<span class="err">catalog offline</span>
      {:else if loading}loading…
      {:else}{rows.length} result{rows.length === 1 ? "" : "s"}{/if}
    </span>
    <button class="qtoggle" class:on={rightView === "queue"} onclick={() => (rightView = rightView === "queue" ? "detail" : "queue")}>
      queue · {queue.items.length}
    </button>
    <span class="engine">{player.ready ? "engine ●" : "engine ○"}</span>
  </header>

  <main>
    <Sidebar {formats} {total} bind:format bind:sort />
    <section class="results">
      <ResultsTable {rows} bind:selectedId onplay={play} />
    </section>
    <aside class="detail">
      {#if rightView === "queue"}
        <QueuePanel />
      {:else}
        <DetailPanel id={selectedId} onplay={play} />
      {/if}
    </aside>
  </main>

  {#if nowPlaying.error}<div class="toast">{nowPlaying.error}</div>{/if}
  <NowPlaying onnext={playNext} onprev={playPrev} />

  {#if showHelp}
    <div class="help-bg" onclick={() => (showHelp = false)} role="presentation">
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
  .err {
    color: var(--hot);
  }
  .qtoggle {
    margin-left: auto;
    font-size: 12px;
  }
  .qtoggle.on {
    border-color: var(--amber);
    color: var(--amber);
  }
  .engine {
    color: var(--dim);
    font-size: 11px;
  }
  main {
    display: grid;
    grid-template-columns: 180px 1fr 320px;
    min-height: 0;
    overflow: hidden;
  }
  .results {
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
