<script lang="ts">
  import type { GenreCount } from "$lib/catalog";

  // The home "Browse by genre" directory. Reads meta.genre_counts (via getGenres) — no scan.
  // Shows the most-populous genres by default with an expander for the full 77-genre list.
  // `naCount` is the un-genred tail (genreid IS NULL) — most of the corpus. It's rendered as a
  // trailing "n/a" tile (always last, regardless of the count sort / collapse) that browses that
  // tail via `onpickNa`. Hidden when 0.
  let {
    genres,
    naCount = 0,
    onpick,
    onpickNa,
  }: {
    genres: GenreCount[];
    naCount?: number;
    onpick: (genreid: number, label: string) => void;
    onpickNa: () => void;
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
      {#if naCount > 0}
        <button class="tile" onclick={onpickNa} title="No genre">
          <span class="label">n/a</span>
          <span class="count">{naCount.toLocaleString()}</span>
        </button>
      {/if}
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
    grid-template-columns: repeat(2, 1fr);
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
