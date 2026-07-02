<script lang="ts">
  import {
    plSearch,
    plList,
    plCreate,
    plGet,
    plState,
    plBump,
    playPlaylist,
    type PlaylistMeta,
  } from "$lib/playlists.svelte";

  let {
    query,
    selectedName = $bindable(null),
  }: { query: string; selectedName: string | null } = $props();

  let rows = $state<PlaylistMeta[]>([]);
  let error = $state<string | null>(null);
  // Library = mine + held (what you back); discover = the seen tier gossip brought in.
  // A non-empty search spans everything regardless of the toggle.
  let tab = $state<"library" | "discover">("library");

  $effect(() => {
    const q = query.trim();
    const t = tab;
    void plState.version; // re-query after any mutation + the sync tick
    (q ? plSearch(q) : plList(t === "library" ? "library" : "seen"))
      .then((r) => {
        rows = r;
        error = null;
        if (!rows.some((p) => p.name === selectedName)) selectedName = rows[0]?.name ?? null;
      })
      .catch((e) => (error = String(e)));
  });

  async function createNew() {
    try {
      const meta = await plCreate("new playlist", []);
      plBump();
      selectedName = meta.name;
    } catch (e) {
      error = `create failed: ${e}`;
    }
  }

  async function play(p: PlaylistMeta) {
    const d = await plGet(p.name);
    if (d) await playPlaylist(d).catch((e) => (error = String(e)));
  }

  const fmtAge = (secs: number) => {
    const d = Math.floor((Date.now() / 1000 - secs) / 86400);
    return d <= 0 ? "today" : d === 1 ? "1 day" : `${d} days`;
  };
</script>

<div class="plists">
  <div class="phead">
    <span class="tabs">
      <button class="tab" class:on={tab === "library"} onclick={() => (tab = "library")}>library</button>
      <button class="tab" class:on={tab === "discover"} onclick={() => (tab = "discover")}>discover</button>
      <span class="count">· {rows.length}</span>
    </span>
    <button onclick={createNew}>＋ new</button>
  </div>
  <div class="plist">
    {#each rows as p (p.name)}
      <div
        class="prow"
        class:sel={p.name === selectedName}
        onclick={() => (selectedName = p.name)}
        ondblclick={() => play(p)}
        role="button"
        tabindex="-1"
      >
        <span class="title">{p.title || "(untitled)"}</span>
        <span class="badges">
          {#if p.isMine}<span class="badge mine">mine</span>{/if}
          {#if p.held}<span class="badge held">held</span>{/if}
          {#if p.dormant}<span class="badge dormant" title="record expired — author absent; fork to keep it shareable">dormant</span>{/if}
          {#if p.published}<span class="badge pub">shared</span>{/if}
        </span>
        <span class="count">{p.tracks} trk</span>
        <span class="age">{fmtAge(p.lastUpdateAt)}</span>
      </div>
    {/each}
    {#if error}
      <div class="empty err">{error}</div>
    {:else if !rows.length}
      <div class="empty">
        {#if query.trim()}no playlists match{:else if tab === "discover"}
          nothing seen from the network yet{:else}
          library empty — ＋ new, save the queue as one, or add from discover{/if}
      </div>
    {/if}
  </div>
</div>

<style>
  .plists {
    height: 100%;
    display: flex;
    flex-direction: column;
    min-width: 0;
  }
  .phead {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0.6rem 0.8rem;
    color: var(--violet);
    border-bottom: 1px solid var(--border);
    text-transform: uppercase;
    font-size: 11px;
  }
  .plist {
    flex: 1;
    overflow-y: auto;
  }
  .prow {
    display: grid;
    grid-template-columns: 1fr auto auto auto;
    gap: 0.8rem;
    align-items: center;
    padding: 0.3rem 0.8rem;
    height: 28px;
    cursor: default;
  }
  .prow:hover {
    background: var(--row-hover);
  }
  .prow.sel {
    background: var(--row-sel);
  }
  .prow.sel .title {
    color: var(--accent);
  }
  .title {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .badges {
    display: flex;
    gap: 0.3rem;
  }
  .badge {
    font-size: 10px;
    border: 1px solid var(--border-hi);
    border-radius: 3px;
    padding: 0 0.3rem;
    color: var(--dim);
  }
  .badge.mine {
    color: var(--amber);
    border-color: var(--amber);
  }
  .badge.held {
    color: var(--violet);
    border-color: var(--violet);
  }
  .badge.dormant {
    color: var(--hot);
    border-color: var(--hot);
  }
  .tabs {
    display: flex;
    align-items: center;
    gap: 0.3rem;
  }
  .tab {
    font-size: 11px;
    text-transform: uppercase;
    padding: 0.15rem 0.5rem;
  }
  .tab.on {
    border-color: var(--violet);
    color: var(--violet);
  }
  .count {
    color: var(--dim);
    text-transform: none;
  }
  .badge.pub {
    color: var(--cyan);
    border-color: var(--cyan);
  }
  .count,
  .age {
    color: var(--dim);
    font-size: 11px;
  }
  .empty {
    padding: 2rem;
    text-align: center;
    color: var(--dim);
  }
  .empty.err {
    color: var(--hot);
    word-break: break-word;
  }
</style>
