<script lang="ts">
  import type { GenreCount } from "$lib/catalog";

  // The home "Browse by genre" directory. Reads meta.genre_counts (via getGenres) — no scan.
  // Sorted by name by default (toggle flips to size/populous-first). The full genre list is
  // always shown.
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

  // Genres arrive from the backend sorted by size (count desc). Name is the default
  // view here; "size" flips back to the populous-first order. The n/a tile is pinned
  // last by the markup below, so it's unaffected by either sort.
  let sortBy = $state<"name" | "size">("name");
  const shown = $derived(
    sortBy === "name"
      ? [...genres].sort((a, b) => a.genre.localeCompare(b.genre))
      : [...genres].sort((a, b) => b.count - a.count),
  );
</script>

{#if genres.length > 0}
  <section class="genres">
    <div class="head">
      <h2>Browse by genre</h2>
      <span class="sep" aria-hidden="true">·</span>
      <button
        class="more"
        onclick={() => (sortBy = sortBy === "name" ? "size" : "name")}
        title="Toggle sort order"
      >
        {sortBy === "name" ? "by name" : "by count"}
      </button>
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
  .sep {
    color: var(--dim);
    font-size: 11px;
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
