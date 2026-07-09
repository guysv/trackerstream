<script lang="ts">
  import type { ModuleHit } from "$lib/catalog";
  import { fmtTime } from "$lib/format";

  // Generic horizontal card rail: a title + a strip of module cards. The home landing
  // uses it for "Fresh"/"Surprise" now; the same component hosts TMA-charts rails later
  // (Top Favourites / Highest Rated / …) — a rail is just {title, rows}, so charts slot
  // in with no structural change. Clicking a card plays from that rail's ordering.
  let {
    title,
    rows,
    onplay,
  }: {
    title: string;
    rows: ModuleHit[];
    onplay: (h: ModuleHit) => void;
  } = $props();
</script>

<section class="rail">
  <h2>{title}</h2>
  <div class="track">
    {#each rows as h (h.id)}
      <button class="card" onclick={() => onplay(h)} title={h.title || h.filename}>
        <span class="fmt fmt-{h.format}">{h.format}</span>
        <span class="t">{h.title || h.filename}</span>
        <span class="d">{fmtTime(h.duration)}</span>
      </button>
    {/each}
    {#if rows.length === 0}
      {#each Array(6) as _}
        <div class="card skel" aria-hidden="true"></div>
      {/each}
    {/if}
  </div>
</section>

<style>
  .rail {
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
  }
  h2 {
    font-size: 13px;
    font-weight: 600;
    margin: 0;
    color: var(--fg);
  }
  .track {
    display: flex;
    gap: 0.5rem;
    overflow-x: auto;
    padding-bottom: 0.3rem;
    scrollbar-width: thin;
  }
  .card {
    flex: 0 0 auto;
    width: 150px;
    height: 74px;
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 0.2rem;
    padding: 0.5rem 0.6rem;
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 6px;
    text-align: left;
    cursor: pointer;
  }
  .card:hover {
    background: var(--row-hover);
    border-color: var(--accent);
  }
  .card .t {
    font-size: 12px;
    line-height: 1.25;
    overflow: hidden;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    line-clamp: 2;
    -webkit-box-orient: vertical;
  }
  .card .fmt {
    font-size: 10px;
    text-transform: uppercase;
    color: var(--dim);
  }
  .card .d {
    margin-top: auto;
    font-size: 10px;
    color: var(--dim);
  }
  .skel {
    cursor: default;
    background: var(--bg);
    opacity: 0.5;
  }
  .skel:hover {
    background: var(--bg);
    border-color: var(--border);
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
