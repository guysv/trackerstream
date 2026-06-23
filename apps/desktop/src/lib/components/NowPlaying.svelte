<script lang="ts">
  import { player, nowPlaying } from "$lib/player.svelte";
  import { fmtTime } from "$lib/format";

  let { onnext, onprev }: { onnext?: () => void; onprev?: () => void } = $props();

  const info = $derived(player.info);
  const pos = $derived(player.pos);

  function seek(e: MouseEvent) {
    const dur = info?.durationSeconds ?? 0;
    if (!dur) return;
    const el = e.currentTarget as HTMLDivElement;
    const frac = (e.clientX - el.getBoundingClientRect().left) / el.clientWidth;
    player.seekSeconds(Math.max(0, Math.min(1, frac)) * dur);
  }

  function seekKey(e: KeyboardEvent) {
    const cur = pos?.seconds ?? 0;
    if (e.key === "ArrowRight") player.seekSeconds(cur + 5);
    else if (e.key === "ArrowLeft") player.seekSeconds(Math.max(0, cur - 5));
  }
</script>

<div class="bar">
  <div class="transport">
    <button onclick={() => onprev?.()} disabled={!onprev} title="previous">⏮</button>
    <button class="pp" onclick={() => player.toggle()} disabled={!info}>
      {player.playing ? "⏸" : "▶"}
    </button>
    <button onclick={() => onnext?.()} disabled={!onnext} title="next">⏭</button>
  </div>

  <div class="center">
    <div class="track">
      <span class="name">{nowPlaying.hit?.title || nowPlaying.hit?.filename || "—"}</span>
      {#if nowPlaying.streaming}<span class="streaming">streaming {nowPlaying.pct}%</span>{/if}
      {#if pos}<span class="pos">ord {String(pos.order).padStart(2, "0")}:{String(pos.row).padStart(2, "0")}</span>{/if}
    </div>
    <div class="seekrow">
      <span class="t">{fmtTime(pos?.seconds ?? 0)}</span>
      <div class="seek" onclick={seek} onkeydown={seekKey} role="slider" aria-label="seek" tabindex="0" aria-valuenow={pos?.seconds ?? 0}>
        {#if nowPlaying.streaming}
          <div class="buf" style="width:{nowPlaying.pct}%"></div>
        {/if}
        <div
          class="played"
          style="width:{info?.durationSeconds ? ((pos?.seconds ?? 0) / info.durationSeconds) * 100 : 0}%"
        ></div>
      </div>
      <span class="t">{fmtTime(info?.durationSeconds ?? 0)}</span>
    </div>
  </div>

  <div class="controls">
    {#if info && info.numSubsongs > 1}
      <select
        title="subsong"
        onchange={(e) => player.selectSubsong(+(e.currentTarget as HTMLSelectElement).value)}
      >
        {#each Array(info.numSubsongs) as _, i}
          <option value={i}>sub {i + 1}</option>
        {/each}
      </select>
    {/if}
    <input
      class="vol"
      type="range"
      min="0"
      max="1"
      step="0.01"
      value={player.volume}
      oninput={(e) => player.setVolume(+(e.currentTarget as HTMLInputElement).value)}
      title="volume"
    />
  </div>

  <div class="vu" aria-hidden="true">
    {#each pos?.vu ?? [] as v}
      <div class="ch"><div class="lvl" style="height:{Math.min(100, v * 100)}%"></div></div>
    {/each}
  </div>
</div>

<style>
  .bar {
    position: relative;
    z-index: 5;
    display: grid;
    grid-template-columns: auto 1fr auto 140px;
    gap: 1rem;
    align-items: center;
    height: 64px;
    padding: 0 1rem;
    /* Opaque — must fully occlude any content above it (was showing through). */
    background: var(--panel);
    border-top: 1px solid var(--border-hi);
    box-shadow: 0 -10px 24px rgba(0, 0, 0, 0.45);
  }
  .controls {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }
  .vol {
    width: 80px;
  }
  .transport {
    display: flex;
    gap: 0.3rem;
  }
  .transport button {
    width: 36px;
  }
  .pp {
    color: var(--accent);
    border-color: var(--accent);
  }
  .center {
    min-width: 0;
  }
  .track {
    display: flex;
    gap: 0.8rem;
    align-items: baseline;
    margin-bottom: 0.3rem;
  }
  .name {
    color: var(--fg);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .streaming {
    color: var(--cyan);
    font-size: 11px;
  }
  .pos {
    color: var(--accent);
    font-size: 11px;
    margin-left: auto;
  }
  .seekrow {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }
  .t {
    color: var(--dim);
    font-size: 11px;
    width: 38px;
  }
  .t:last-child {
    text-align: right;
  }
  .seek {
    position: relative;
    flex: 1;
    height: 8px;
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 4px;
    cursor: pointer;
    overflow: hidden;
  }
  .buf {
    position: absolute;
    inset: 0 auto 0 0;
    background: var(--border-hi);
  }
  .played {
    position: absolute;
    inset: 0 auto 0 0;
    background: linear-gradient(90deg, var(--accent), var(--cyan));
  }
  .vu {
    display: flex;
    gap: 1px;
    align-items: flex-end;
    height: 40px;
  }
  .ch {
    flex: 1;
    height: 100%;
    display: flex;
    align-items: flex-end;
    min-width: 2px;
  }
  .lvl {
    width: 100%;
    background: linear-gradient(var(--accent), var(--amber), var(--hot));
    transition: height 0.05s linear;
  }
</style>
