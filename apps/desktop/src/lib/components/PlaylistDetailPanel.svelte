<script lang="ts">
  import {
    plGet,
    plUpdate,
    plDelete,
    plPublish,
    plHold,
    plState,
    plBump,
    playPlaylist,
    duplicatePlaylist,
    itemTuples,
    type PlaylistDetail,
  } from "$lib/playlists.svelte";

  let { name }: { name: string | null } = $props();

  let detail = $state<PlaylistDetail | null>(null);
  let confirmShare = $state(false);
  let confirmDelete = $state(false);
  let busy = $state(false);
  let error = $state<string | null>(null);
  let loadedFor: string | null = null; // last name a fetch completed for (not reactive)

  $effect(() => {
    const cur = name;
    void plState.version; // re-fetch on mutations AND the periodic sync tick
    if (!cur) {
      detail = null;
      loadedFor = null;
      return;
    }
    if (cur !== loadedFor) {
      // Switching playlists: clear immediately. A same-name refresh (the 10s sync
      // tick) keeps the current detail on screen and swaps in place — no flicker.
      detail = null;
      confirmShare = false;
      confirmDelete = false;
      error = null;
    }
    plGet(cur)
      .then((d) => {
        if (name === cur) {
          detail = d;
          loadedFor = cur;
        }
      })
      .catch((e) => (error = String(e)));
  });

  async function removeTrack(i: number) {
    if (!detail?.isMine) return;
    const tuples = itemTuples(detail.items);
    tuples.splice(i, 1);
    await plUpdate(detail.name, detail.title, tuples);
    plBump();
  }

  async function share() {
    if (!detail) return;
    busy = true;
    error = null;
    try {
      await plPublish(detail.name);
      plBump();
    } catch (e) {
      error = `share failed: ${e}`;
    } finally {
      busy = false;
      confirmShare = false;
    }
  }

  async function del() {
    if (!detail) return;
    busy = true;
    error = null;
    try {
      await plDelete(detail.name);
      plBump();
    } catch (e) {
      error = `delete failed: ${e}`;
    } finally {
      busy = false;
      confirmDelete = false;
    }
  }
</script>

<div class="pdetail">
  {#if error}
    <div class="error">{error}</div>
  {/if}
  {#if !detail}
    <div class="placeholder">select a playlist</div>
  {:else}
    <div class="title">{detail.title || "(untitled)"}</div>
    <div class="sub">
      {detail.items.length} tracks
      {#if detail.isMine}· mine{/if}
      {#if detail.held}· <span class="heldtxt">in library</span>{/if}
      {#if detail.dormant}· <span class="dormant">dormant</span>{/if}
      {#if detail.published}· <span class="pub">shared</span>{/if}
    </div>

    {#if detail.dormant}
      <div class="dnote">
        {#if detail.tombstoned}
          the author deleted this playlist (tombstone received). your kept copy stays
          playable but can't be shared onward — unless the author republishes, in which
          case it revives here automatically.
        {:else}
          this playlist's record has expired — its author hasn't re-signed it in over a
          week, so nobody (including you) can share it onward. your copy stays playable.
        {/if}
        <b>duplicate to mine</b> forks it under your key to make it shareable again.
      </div>
    {/if}

    <div class="actions">
      <button
        class="play"
        onclick={() => detail && playPlaylist(detail).catch((e) => (error = String(e)))}
        disabled={!detail.items.length}
      >
        ▶ play
      </button>
      {#if detail.isMine}
        {#if !confirmShare}
          <button onclick={() => (confirmShare = true)} disabled={busy}>
            {detail.published ? "re-share" : "share"}
          </button>
        {/if}
        {#if !confirmDelete}
          <button onclick={() => (confirmDelete = true)} disabled={busy}>delete</button>
        {/if}
      {:else}
        <!-- Holder tier: "in library" = backed (re-announced, never evicted), still
             following the author's updates. Duplicate = fork under your own key. -->
        <button
          class:held={detail.held}
          onclick={async () => {
            if (!detail) return;
            await plHold(detail.name, !detail.held).catch((e) => (error = String(e)));
            plBump();
          }}
        >
          {detail.held ? "✓ in library" : "＋ add to library"}
        </button>
        <button onclick={() => detail && duplicatePlaylist(detail)}>duplicate to mine</button>
        {#if !detail.held}
          <button onclick={del} disabled={busy}>remove</button>
        {/if}
      {/if}
    </div>

    {#if confirmShare}
      <div class="confirm">
        sharing publishes this playlist to <b>everyone</b> on the network. it can be
        tombstoned later, but copies may persist.
        <div class="cbtns">
          <button class="go" onclick={share} disabled={busy}>share it</button>
          <button onclick={() => (confirmShare = false)}>cancel</button>
        </div>
      </div>
    {/if}
    {#if confirmDelete}
      <div class="confirm">
        {#if detail.published}delete publishes a tombstone (syncers drop it), then removes it locally.
        {:else}delete removes this playlist.{/if}
        <div class="cbtns">
          <button class="go" onclick={del} disabled={busy}>delete</button>
          <button onclick={() => (confirmDelete = false)}>cancel</button>
        </div>
      </div>
    {/if}

    <div class="tlist">
      {#each detail.items as t, i (t.id + "-" + i)}
        <div class="trow" ondblclick={() => detail && playPlaylist(detail, i)} role="button" tabindex="-1">
          <span class="num">{i + 1}</span>
          <span class="tname" title={t.modName}>{t.title || t.modName}</span>
          {#if detail.isMine}
            <button class="rm" onclick={() => removeTrack(i)} title="remove">✕</button>
          {/if}
        </div>
      {/each}
      {#if !detail.items.length}<div class="empty">no tracks — add from a module's detail pane</div>{/if}
    </div>
  {/if}
</div>

<style>
  .pdetail {
    height: 100%;
    overflow-y: auto;
    padding: 1rem;
    display: flex;
    flex-direction: column;
  }
  .placeholder {
    color: var(--dim);
    padding-top: 2rem;
    text-align: center;
  }
  .error {
    color: var(--hot);
    font-size: 11px;
    margin-bottom: 0.6rem;
    word-break: break-word;
  }
  .title {
    color: var(--violet);
    font-size: 15px;
  }
  .sub {
    color: var(--dim);
    margin-bottom: 0.7rem;
    font-size: 11px;
  }
  .pub {
    color: var(--cyan);
  }
  .heldtxt {
    color: var(--violet);
  }
  .dormant {
    color: var(--hot);
  }
  .dnote {
    border: 1px solid var(--hot);
    border-radius: 4px;
    padding: 0.6rem;
    font-size: 11px;
    color: var(--dim);
    margin-bottom: 0.8rem;
  }
  button.held {
    border-color: var(--violet);
    color: var(--violet);
  }
  .actions {
    display: flex;
    gap: 0.4rem;
    margin-bottom: 0.8rem;
    flex-wrap: wrap;
  }
  .play {
    color: var(--accent);
    border-color: var(--accent);
  }
  .confirm {
    border: 1px solid var(--amber);
    border-radius: 4px;
    padding: 0.6rem;
    font-size: 11px;
    color: var(--dim);
    margin-bottom: 0.8rem;
  }
  .cbtns {
    display: flex;
    gap: 0.4rem;
    margin-top: 0.5rem;
  }
  .go {
    color: var(--amber);
    border-color: var(--amber);
  }
  .tlist {
    flex: 1;
  }
  .trow {
    display: grid;
    grid-template-columns: 28px 1fr auto;
    gap: 0.4rem;
    align-items: center;
    padding: 0.2rem 0;
    height: 24px;
    cursor: default; /* rows are dblclick targets, not links (matches QueuePanel) */
  }
  .trow:hover {
    background: var(--row-hover);
  }
  .num {
    color: var(--dim);
  }
  .tname {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .rm {
    padding: 0 0.3rem;
    background: none;
    border: none;
    color: var(--dim);
    opacity: 0;
  }
  .trow:hover .rm {
    opacity: 1;
  }
  .rm:hover {
    color: var(--hot);
  }
  .empty {
    padding: 1.5rem 0;
    text-align: center;
    color: var(--dim);
    font-size: 11px;
  }
</style>
