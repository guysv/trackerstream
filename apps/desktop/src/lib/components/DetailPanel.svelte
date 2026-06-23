<script lang="ts">
  import { getModule, type ModuleDetail, type ModuleHit } from "$lib/catalog";
  import { fmtTime, fmtBytes } from "$lib/format";
  import { enqueue } from "$lib/player.svelte";
  import { shareModule } from "$lib/social.svelte";

  let { id, onplay }: { id: number | null; onplay: (h: ModuleHit) => void } = $props();
  let shareCode = $state("");

  async function doShare() {
    if (!detail) return;
    try {
      shareCode = await shareModule(detail);
      await navigator.clipboard?.writeText(shareCode).catch(() => {});
    } catch {
      shareCode = "share failed";
    }
  }

  let detail = $state<ModuleDetail | null>(null);

  $effect(() => {
    const cur = id;
    detail = null;
    if (cur == null) return;
    getModule(cur)
      .then((d) => {
        if (id === cur) detail = d;
      })
      .catch(() => {});
  });

  const instruments = $derived(
    (detail?.instruments ?? "").split(/\s+/).filter(Boolean).slice(0, 200),
  );
</script>

<div class="detail">
  {#if !detail}
    <div class="placeholder">select a module</div>
  {:else}
    <div class="title">{detail.title || detail.filename}</div>
    <div class="file">{detail.filename}</div>
    <div class="actions">
      <button class="play" onclick={() => onplay(detail!)}>▶ play</button>
      <button onclick={() => enqueue(detail!)}>+ queue</button>
      <button onclick={() => enqueue(detail!, true)}>play next</button>
      <button onclick={doShare} title="create a share code">share</button>
    </div>
    {#if shareCode}<div class="cid">share code: <b>{shareCode}</b> (copied)</div>{/if}

    <dl class="meta">
      <dt>format</dt><dd class="fmt-{detail.format}">{detail.format.toUpperCase()}</dd>
      <dt>length</dt><dd>{fmtTime(detail.duration)}</dd>
      <dt>channels</dt><dd>{detail.channels}</dd>
      <dt>samples</dt><dd>{detail.numSamples}</dd>
      <dt>instruments</dt><dd>{detail.numInstruments}</dd>
      <dt>subsongs</dt><dd>{detail.numSubsongs}</dd>
      <dt>size</dt><dd>{fmtBytes(detail.sizeBytes)}</dd>
    </dl>

    <div class="cid" title={detail.rootCid}>CID {detail.rootCid.slice(0, 20)}…</div>

    {#if instruments.length}
      <div class="section">instruments / samples</div>
      <ol class="inst">
        {#each instruments as name, i}
          <li><span class="idx">{String(i + 1).padStart(2, "0")}</span> {name}</li>
        {/each}
      </ol>
    {/if}

    {#if detail.comment}
      <div class="section">comment</div>
      <pre class="comment">{detail.comment}</pre>
    {/if}
  {/if}
</div>

<style>
  .detail {
    height: 100%;
    overflow-y: auto;
    padding: 1rem;
  }
  .placeholder {
    color: var(--dim);
    padding-top: 2rem;
    text-align: center;
  }
  .title {
    color: var(--violet);
    font-size: 15px;
  }
  .file {
    color: var(--dim);
    margin-bottom: 0.7rem;
  }
  .actions {
    display: flex;
    gap: 0.4rem;
    margin-bottom: 1rem;
  }
  .play {
    color: var(--accent);
    border-color: var(--accent);
  }
  dl.meta {
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 0.2rem 0.8rem;
    margin: 0 0 0.8rem;
  }
  dt {
    color: var(--dim);
  }
  dd {
    margin: 0;
  }
  .cid {
    color: var(--cyan);
    font-size: 11px;
    margin-bottom: 0.8rem;
    word-break: break-all;
  }
  .section {
    color: var(--amber);
    text-transform: uppercase;
    font-size: 11px;
    letter-spacing: 0.05em;
    border-bottom: 1px solid var(--border);
    padding-bottom: 0.2rem;
    margin: 0.8rem 0 0.4rem;
  }
  ol.inst {
    margin: 0;
    padding: 0;
    list-style: none;
  }
  ol.inst li {
    color: var(--fg);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .idx {
    color: var(--dim);
  }
  pre.comment {
    white-space: pre-wrap;
    word-break: break-word;
    color: var(--dim);
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 0.5rem;
    margin: 0;
    max-height: 220px;
    overflow-y: auto;
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
