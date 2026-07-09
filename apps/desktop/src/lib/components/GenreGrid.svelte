<script lang="ts">
  import type { GenreCount } from "$lib/catalog";

  // The home "Browse by genre" directory. Reads meta.genre_counts (via getGenres) — no scan.
  // Shows the most-populous genres by default with an expander for the full 77-genre list.
  let {
    genres,
    onpick,
  }: {
    genres: GenreCount[];
    onpick: (genreid: number, label: string) => void;
  } = $props();

  const COLLAPSED = 12;
  let expanded = $state(false);
  const shown = $derived(expanded ? genres : genres.slice(0, COLLAPSED));
</script>

{#if genres.length > 0}
  <section class="genres">
    <div class="head">
      <h2>Browse by genre</h2>
      {#if genres.length > COLLAPSED}
        <button class="more" onclick={() => (expanded = !expanded)}>
          {expanded ? "show less" : `all ${genres.length}`}
        </button>
      {/if}
    </div>
    <div class="grid">
      {#each shown as g (g.genreid)}
        <button class="tile" onclick={() => onpick(g.genreid, g.genre)} title={g.genre}>
          <span class="label">{g.genre}</span>
          <span class="count">{g.count.toLocaleString()}</span>
        </button>
      {/each}
    </div>
  </section>
{/if}

<style>
  .genres {
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
  }
  .head {
    display: flex;
    align-items: baseline;
    gap: 0.6rem;
  }
  h2 {
    font-size: 13px;
    font-weight: 600;
    margin: 0;
    color: var(--fg);
  }
  .more {
    background: none;
    border: none;
    color: var(--accent);
    font-size: 11px;
    cursor: pointer;
    padding: 0;
  }
  .grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(150px, 1fr));
    gap: 0.4rem;
  }
  .tile {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.5rem;
    padding: 0.5rem 0.7rem;
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 6px;
    cursor: pointer;
    text-align: left;
  }
  .tile:hover {
    background: var(--row-hover);
    border-color: var(--accent);
  }
  .label {
    font-size: 12px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .count {
    flex: 0 0 auto;
    font-size: 11px;
    color: var(--dim);
  }
</style>
