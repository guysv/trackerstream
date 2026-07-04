<script lang="ts">
  import type { FormatCount } from "$lib/catalog";

  let {
    formats,
    total,
    format = $bindable(null),
    sort = $bindable("latest"),
  }: {
    formats: FormatCount[];
    total: number;
    format?: string | null;
    sort?: "latest" | "random" | "title";
  } = $props();

  const sorts: Array<"latest" | "random" | "title"> = ["latest", "random", "title"];
</script>

<!-- Browse-view filter strip: the format/sort controls that used to be the left rail,
     relocated to a header now that the left column is navigation + library. -->
<div class="filters">
  <div class="formats">
    <button class="nav" class:active={format === null} onclick={() => (format = null)}>
      all <span class="count">{total}</span>
    </button>
    {#each formats as f}
      <button class="nav" class:active={format === f.format} onclick={() => (format = f.format)}>
        <span class="fmt-{f.format}">{f.format}</span> <span class="count">{f.count}</span>
      </button>
    {/each}
  </div>
  <div class="sorts">
    {#each sorts as s}
      <button class="chip" class:active={sort === s} onclick={() => (sort = s)}>{s}</button>
    {/each}
  </div>
</div>

<style>
  .filters {
    display: flex;
    align-items: center;
    gap: 1rem;
    padding: 0.5rem 0.8rem;
    border-bottom: 1px solid var(--border);
    overflow-x: auto;
  }
  .formats {
    display: flex;
    gap: 0.3rem;
    flex-wrap: wrap;
  }
  .nav {
    background: none;
    border: 1px solid transparent;
    border-radius: 4px;
    padding: 0.2rem 0.5rem;
    text-transform: uppercase;
    font-size: 12px;
    white-space: nowrap;
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
  .sorts {
    display: flex;
    gap: 0.3rem;
    margin-left: auto;
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
