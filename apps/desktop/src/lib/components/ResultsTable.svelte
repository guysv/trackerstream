<script lang="ts">
  import type { ModuleHit } from "$lib/catalog";
  import { fmtTime } from "$lib/format";

  let {
    rows,
    selectedId = $bindable(null),
    onplay,
  }: {
    rows: ModuleHit[];
    selectedId?: number | null;
    onplay: (h: ModuleHit) => void;
  } = $props();

  const ROW_H = 24;
  let scrollEl: HTMLDivElement | undefined = $state();
  let scrollTop = $state(0);
  let viewH = $state(400);

  const start = $derived(Math.max(0, Math.floor(scrollTop / ROW_H) - 6));
  const count = $derived(Math.ceil(viewH / ROW_H) + 12);
  const visible = $derived(rows.slice(start, start + count));

  function move(delta: number) {
    const idx = rows.findIndex((r) => r.id === selectedId);
    const next = Math.min(rows.length - 1, Math.max(0, (idx < 0 ? -1 : idx) + delta));
    const row = rows[next];
    if (!row || !scrollEl) return;
    selectedId = row.id;
    const y = next * ROW_H;
    if (y < scrollEl.scrollTop) scrollEl.scrollTop = y;
    else if (y + ROW_H > scrollEl.scrollTop + viewH) scrollEl.scrollTop = y + ROW_H - viewH;
  }

  function onKey(e: KeyboardEvent) {
    const page = Math.max(1, Math.floor(viewH / ROW_H) - 1);
    if (e.key === "ArrowDown") (e.preventDefault(), move(1));
    else if (e.key === "ArrowUp") (e.preventDefault(), move(-1));
    else if (e.key === "PageDown") (e.preventDefault(), move(page));
    else if (e.key === "PageUp") (e.preventDefault(), move(-page));
    else if (e.key === "Home") (e.preventDefault(), move(-rows.length));
    else if (e.key === "End") (e.preventDefault(), move(rows.length));
    else if (e.key === "Enter") {
      const h = rows.find((r) => r.id === selectedId);
      if (h) onplay(h);
    }
  }
</script>

<div class="head">
  <span class="c-title">title</span>
  <span class="c-file">file</span>
  <span class="c-fmt">fmt</span>
  <span class="c-ch">ch</span>
  <span class="c-time">time</span>
</div>
<div
  class="vlist"
  bind:this={scrollEl}
  bind:clientHeight={viewH}
  onscroll={() => (scrollTop = scrollEl!.scrollTop)}
  onkeydown={onKey}
  tabindex="0"
  role="listbox"
  aria-label="modules"
>
  <div class="spacer" style="height:{rows.length * ROW_H}px">
    {#each visible as row, i (row.id)}
      <div
        class="row"
        class:sel={row.id === selectedId}
        style="top:{(start + i) * ROW_H}px;height:{ROW_H}px"
        role="option"
        aria-selected={row.id === selectedId}
        tabindex="-1"
        onclick={() => (selectedId = row.id)}
        ondblclick={() => onplay(row)}
      >
        <span class="c-title">{row.title || row.filename}</span>
        <span class="c-file">{row.filename}</span>
        <span class="c-fmt fmt-{row.format}">{row.format}</span>
        <span class="c-ch">{row.channels}</span>
        <span class="c-time">{fmtTime(row.duration)}</span>
      </div>
    {/each}
  </div>
  {#if rows.length === 0}
    <div class="empty">no modules</div>
  {/if}
</div>

<style>
  .head,
  .row {
    display: grid;
    grid-template-columns: 1fr 1fr 48px 36px 56px;
    gap: 0.5rem;
    align-items: center;
    padding: 0 0.7rem;
    white-space: nowrap;
  }
  .head {
    height: 26px;
    color: var(--dim);
    border-bottom: 1px solid var(--border);
    text-transform: uppercase;
    font-size: 11px;
    letter-spacing: 0.05em;
  }
  .vlist {
    position: relative;
    flex: 1;
    overflow-y: auto;
    outline: none;
  }
  .vlist:focus-within .row.sel {
    background: var(--row-sel);
  }
  .spacer {
    position: relative;
  }
  .row {
    position: absolute;
    left: 0;
    right: 0;
    cursor: default;
  }
  .row:hover {
    background: var(--row-hover);
  }
  .row.sel {
    background: var(--row-sel);
  }
  .c-title {
    overflow: hidden;
    text-overflow: ellipsis;
    color: var(--fg);
  }
  .c-file {
    overflow: hidden;
    text-overflow: ellipsis;
    color: var(--dim);
  }
  .c-ch,
  .c-time {
    text-align: right;
    color: var(--dim);
  }
  .c-fmt {
    text-transform: uppercase;
    font-size: 11px;
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
  .empty {
    padding: 2rem;
    color: var(--dim);
    text-align: center;
  }
</style>
