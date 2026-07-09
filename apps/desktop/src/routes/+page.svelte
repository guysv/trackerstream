<script lang="ts">
  import { onMount } from "svelte";
  import ResultsTable from "$lib/components/ResultsTable.svelte";
  import Rail from "$lib/components/Rail.svelte";
  import GenreGrid from "$lib/components/GenreGrid.svelte";
  import {
    listModules,
    getFormats,
    getGenres,
    type ModuleHit,
    type FormatCount,
    type GenreCount,
  } from "$lib/catalog";
  import { playList } from "$lib/player.svelte";
  import { ui } from "$lib/ui.svelte";

  // Home is an "Explore" landing (rails + genre/format directories); picking a facet drops
  // into the results table. `browsing` is the mode flag — set by any pick (incl. "all", which
  // browses the whole corpus with no facet), cleared by the "‹ Explore" back button.
  let browsing = $state(false);
  let genre = $state<number | null>(null);
  let genreLabel = $state("");
  let format = $state<string | null>(null);
  let sort = $state<"latest" | "random" | "title">("latest");

  let rows = $state<ModuleHit[]>([]);
  let formats = $state<FormatCount[]>([]);
  let genres = $state<GenreCount[]>([]);
  let total = $state(0);
  // Landing rails. Fresh = latest ingested; surprise = random. (TMA-charts rails land here later.)
  let fresh = $state<ModuleHit[]>([]);
  let surprise = $state<ModuleHit[]>([]);

  const facetLabel = $derived(
    genre !== null ? genreLabel : format !== null ? format.toUpperCase() : "All modules",
  );
  const facetCount = $derived(
    genre !== null
      ? (genres.find((g) => g.genreid === genre)?.count ?? 0)
      : format !== null
        ? (formats.find((f) => f.format === format)?.count ?? 0)
        : total,
  );

  const sorts: Array<"latest" | "random" | "title"> = ["latest", "random", "title"];

  // The right-panel inspector follows the row selection on this route.
  let selectedId = $state<number | null>(ui.inspectorTrackId);
  let scrollTop = $state(0);
  $effect(() => {
    ui.inspectorTrackId = selectedId;
    ui.inspectorTrackMd5 = null; // this route inspects by rowid; drop any playlist md5
  });

  // Restore the whole view (facet + selection + scroll) across back/forward navigation.
  export const snapshot = {
    capture: () => ({ browsing, genre, genreLabel, format, sort, selectedId, scrollTop }),
    restore: (v: {
      browsing: boolean;
      genre: number | null;
      genreLabel: string;
      format: string | null;
      sort: "latest" | "random" | "title";
      selectedId: number | null;
      scrollTop: number;
    }) => {
      browsing = v.browsing;
      genre = v.genre;
      genreLabel = v.genreLabel;
      format = v.format;
      sort = v.sort;
      selectedId = v.selectedId;
      scrollTop = v.scrollTop;
    },
  };

  function pickGenre(id: number, label: string) {
    format = null;
    genreLabel = label;
    genre = id;
    browsing = true;
  }
  function pickFormat(fmt: string) {
    genre = null;
    format = fmt;
    browsing = true;
  }
  function browseAll() {
    genre = null;
    format = null;
    browsing = true;
  }
  function backToExplore() {
    browsing = false;
    genre = null;
    format = null;
  }

  function play(h: ModuleHit) {
    playList(
      rows,
      rows.findIndex((r) => r.id === h.id),
    );
  }
  function playFrom(list: ModuleHit[], h: ModuleHit) {
    playList(
      list,
      list.findIndex((r) => r.id === h.id),
    );
  }

  onMount(() => {
    getFormats()
      .then((f) => ((formats = f.formats), (total = f.total)))
      .catch(() => (ui.status = "catalog offline"));
    getGenres()
      .then((g) => (genres = g.genres))
      .catch(() => {});
    listModules({ sort: "latest", limit: 18 })
      .then((r) => (fresh = r))
      .catch(() => {});
    listModules({ sort: "random", limit: 18 })
      .then((r) => (surprise = r))
      .catch(() => {});
  });

  let timer: ReturnType<typeof setTimeout>;
  let reqSeq = 0;
  $effect(() => {
    const b = browsing;
    const g = genre;
    const fmt = format;
    const s = sort;
    if (!b) return; // landing mode: no results query
    clearTimeout(timer);
    ui.status = "loading…";
    timer = setTimeout(async () => {
      // Stale-result guard: catalog queries run over the P2P VFS and a cold one can take
      // seconds, so two can be in flight — only the latest commits.
      const myReq = ++reqSeq;
      try {
        const result = await listModules({
          genre: g ?? undefined,
          format: fmt ?? undefined,
          sort: s,
          limit: 300,
        });
        if (myReq !== reqSeq) return;
        rows = result;
        if (!rows.some((x) => x.id === selectedId)) selectedId = rows[0]?.id ?? null;
        ui.status = `${rows.length} result${rows.length === 1 ? "" : "s"}`;
      } catch {
        if (myReq === reqSeq) ui.status = "catalog offline";
      }
    }, 0);
  });
</script>

{#if !browsing}
  <div class="explore">
    <Rail title="Fresh" rows={fresh} onplay={(h) => playFrom(fresh, h)} />
    <Rail title="Surprise me" rows={surprise} onplay={(h) => playFrom(surprise, h)} />
    <GenreGrid {genres} onpick={pickGenre} />
    <section class="formats">
      <h2>Browse by format</h2>
      <div class="chips">
        <button class="chip" onclick={browseAll}>
          all <span class="count">{total.toLocaleString()}</span>
        </button>
        {#each formats as f (f.format)}
          <button class="chip" onclick={() => pickFormat(f.format)}>
            <span class="fmt-{f.format}">{f.format}</span>
            <span class="count">{f.count.toLocaleString()}</span>
          </button>
        {/each}
      </div>
    </section>
  </div>
{:else}
  <div class="results-head">
    <button class="back" onclick={backToExplore}>‹ Explore</button>
    <span class="facet">{facetLabel} <span class="count">{facetCount.toLocaleString()}</span></span>
    <div class="sorts">
      {#each sorts as s}
        <button class="chip" class:active={sort === s} onclick={() => (sort = s)}>{s}</button>
      {/each}
    </div>
  </div>
  <div class="results">
    <ResultsTable
      {rows}
      bind:selectedId
      bind:scrollTop
      onplay={play}
      onselect={() => (ui.right = "detail")}
    />
  </div>
{/if}

<style>
  .explore {
    flex: 1;
    display: flex;
    flex-direction: column;
    gap: 1.2rem;
    padding: 1rem;
    overflow-y: auto;
    min-height: 0;
  }
  .formats {
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
  }
  .formats h2 {
    font-size: 13px;
    font-weight: 600;
    margin: 0;
    color: var(--fg);
  }
  .chips {
    display: flex;
    gap: 0.3rem;
    flex-wrap: wrap;
  }
  .results-head {
    display: flex;
    align-items: center;
    gap: 0.8rem;
    padding: 0.5rem 0.8rem;
    border-bottom: 1px solid var(--border);
  }
  .back {
    background: none;
    border: 1px solid transparent;
    border-radius: 4px;
    padding: 0.2rem 0.5rem;
    font-size: 12px;
    color: var(--accent);
    cursor: pointer;
  }
  .back:hover {
    background: var(--row-hover);
  }
  .facet {
    font-size: 13px;
    font-weight: 600;
  }
  .count {
    color: var(--dim);
    font-weight: 400;
  }
  .sorts {
    display: flex;
    gap: 0.3rem;
    margin-left: auto;
  }
  .chip {
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 0.2rem 0.5rem;
    font-size: 11px;
    cursor: pointer;
  }
  .chip:hover {
    background: var(--row-hover);
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
  .results {
    flex: 1;
    display: flex;
    flex-direction: column;
    min-height: 0;
    overflow: hidden;
  }
</style>
