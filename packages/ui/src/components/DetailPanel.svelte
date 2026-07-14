<script lang="ts">
  import { getModule, getModuleByMd5, type ModuleDetail } from "../catalog";
  import { fmtTime, fmtBytes } from "../format";
  import { enqueue, playList } from "../player.svelte";
  import {
    plList,
    plCreate,
    addTrackTo,
    hitTuple,
    plBump,
    toggleLike,
    isLiked,
    type PlaylistMeta,
  } from "../playlists.svelte";
  import { openContextMenu } from "../contextmenu.svelte";
  import { trackMenuItems } from "../menus";
  import { logError, logWarn } from "../debug";

  // Two ways in: `id` (search/browse rows — a live catalog rowid) or `md5` (playlist
  // rows, which persist only the stable content hash). Prefer id; fall back to md5.
  let { id, md5 = null }: { id: number | null; md5?: string | null } = $props();

  let detail = $state<ModuleDetail | null>(null);
  let plMenu = $state(false);
  let myLists = $state<PlaylistMeta[]>([]);

  $effect(() => {
    const curId = id;
    const curMd5 = md5;
    detail = null;
    plMenu = false;
    const load = curId != null ? getModule(curId) : curMd5 ? getModuleByMd5(curMd5) : null;
    if (!load) return;
    load
      .then((d) => {
        // Guard against a racing prop change resolving out of order.
        if (id === curId && md5 === curMd5 && d) detail = d;
      })
      .catch((e) => logWarn("detail:load", e, { id: curId, md5: curMd5 })); // panel just stays empty
  });

  async function openPlMenu() {
    if (!plMenu) myLists = (await plList().catch((e) => (logError("detail:plList", e), []))).filter((p) => p.isMine);
    plMenu = !plMenu;
  }

  async function addTo(name: string) {
    if (detail) await addTrackTo(name, detail);
    plMenu = false;
  }

  async function addToNew() {
    if (!detail) return;
    await plCreate(detail.title || detail.filename, [hitTuple(detail)]);
    plBump();
    plMenu = false;
  }

  const instruments = $derived(
    (detail?.instruments ?? "").split(/\s+/).filter(Boolean).slice(0, 200),
  );
</script>

<div class="detail">
  {#if !detail}
    <div class="placeholder">select a module</div>
  {:else}
    <!-- svelte-ignore a11y_no_static_element_interactions -->
    <div class="title" oncontextmenu={(e) => detail && openContextMenu(e, () => trackMenuItems(detail!))}>
      {detail.title || detail.filename}
    </div>
    <div class="file">{detail.filename}</div>
    <div class="actions">
      <button class="play" onclick={() => detail && playList([detail], 0)}>▶ play</button>
      <button
        class="like"
        class:on={isLiked(detail.md5)}
        title={isLiked(detail.md5) ? "remove from Liked Tracks" : "add to Liked Tracks"}
        onclick={() => detail && toggleLike(detail)}
      >
        {isLiked(detail.md5) ? "♥" : "♡"}
      </button>
      <button onclick={() => enqueue(detail!)}>+ queue</button>
      <button onclick={() => enqueue(detail!, true)}>play next</button>
      <span class="plwrap">
        <button onclick={openPlMenu}>+ list</button>
        {#if plMenu}
          <div class="plmenu">
            {#each myLists as p (p.name)}
              <button class="plitem" onclick={() => addTo(p.name)}>{p.title || "(untitled)"}</button>
            {/each}
            <button class="plitem new" onclick={addToNew}>＋ new playlist</button>
          </div>
        {/if}
      </span>
    </div>

    <dl class="meta">
      <dt>format</dt><dd class="fmt-{detail.format}">{detail.format.toUpperCase()}</dd>
      <dt>genre</dt><dd>{detail.genre || "n/a"}</dd>
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
  .like.on {
    color: var(--hot);
    border-color: var(--hot);
  }
  .plwrap {
    position: relative;
  }
  .plmenu {
    position: absolute;
    top: 100%;
    right: 0;
    z-index: 6;
    display: flex;
    flex-direction: column;
    min-width: 160px;
    max-height: 220px;
    overflow-y: auto;
    background: var(--panel);
    border: 1px solid var(--border-hi);
    border-radius: 4px;
    padding: 0.2rem;
  }
  .plitem {
    text-align: left;
    background: none;
    border: none;
    padding: 0.25rem 0.5rem;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .plitem:hover {
    background: var(--row-hover);
  }
  .plitem.new {
    color: var(--violet);
    border-top: 1px solid var(--border);
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
