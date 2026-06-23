<script lang="ts">
  import type { FormatCount, PlaylistSummary } from "$lib/catalog";

  let {
    formats,
    total,
    playlists = [],
    format = $bindable(null),
    sort = $bindable("latest"),
    onPlaylist,
  }: {
    formats: FormatCount[];
    total: number;
    playlists?: PlaylistSummary[];
    format?: string | null;
    sort?: "latest" | "random" | "title";
    onPlaylist?: (id: number) => void;
  } = $props();

  const sorts: Array<"latest" | "random" | "title"> = ["latest", "random", "title"];
</script>

<nav class="sidebar">
  <div class="brand">tracker<span>stream</span></div>

  <div class="group">browse</div>
  <button class="nav" class:active={format === null} onclick={() => (format = null)}>
    <span>all</span><span class="count">{total}</span>
  </button>
  {#each formats as f}
    <button class="nav" class:active={format === f.format} onclick={() => (format = f.format)}>
      <span class="fmt-{f.format}">{f.format}</span><span class="count">{f.count}</span>
    </button>
  {/each}

  <div class="group">sort</div>
  <div class="sorts">
    {#each sorts as s}
      <button class="chip" class:active={sort === s} onclick={() => (sort = s)}>{s}</button>
    {/each}
  </div>

  {#if playlists.length}
    <div class="group">playlists</div>
    {#each playlists as p}
      <button class="nav" onclick={() => onPlaylist?.(p.id)}>
        <span class="pl">{p.isPublic ? "" : "🔒"}{p.name}</span><span class="count">{p.count}</span>
      </button>
    {/each}
  {/if}
</nav>

<style>
  .sidebar {
    width: 180px;
    background: var(--panel);
    border-right: 1px solid var(--border);
    padding: 0.8rem 0.6rem;
    min-height: 0;
    overflow-y: auto;
  }
  .brand {
    color: var(--accent);
    font-size: 15px;
    letter-spacing: 0.04em;
    padding: 0 0.3rem 0.8rem;
  }
  .brand span {
    color: var(--fg);
  }
  .group {
    color: var(--dim);
    text-transform: uppercase;
    font-size: 10px;
    letter-spacing: 0.08em;
    margin: 0.8rem 0.3rem 0.3rem;
  }
  .nav {
    display: flex;
    justify-content: space-between;
    width: 100%;
    background: none;
    border: none;
    border-radius: 4px;
    padding: 0.3rem 0.4rem;
    text-transform: uppercase;
    font-size: 12px;
  }
  .nav:hover {
    background: var(--row-hover);
  }
  .nav.active {
    background: var(--row-sel);
  }
  .count {
    color: var(--dim);
  }
  .pl {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    text-transform: none;
  }
  .sorts {
    display: flex;
    flex-wrap: wrap;
    gap: 0.3rem;
    padding: 0 0.3rem;
  }
  .chip {
    background: var(--bg);
    padding: 0.2rem 0.5rem;
    font-size: 11px;
  }
  .chip.active {
    border-color: var(--accent);
    color: var(--accent);
  }
  .fmt-it {
    color: var(--accent);
  }
  .fmt-xm {
    color: var(--blue);
  }
  .fmt-mod {
    color: var(--amber);
  }
  .fmt-s3m {
    color: var(--violet);
  }
</style>
