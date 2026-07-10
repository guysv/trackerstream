<script lang="ts">
  import {
    plGet,
    plUpdate,
    plDelete,
    plPublish,
    plUnpublish,
    plHold,
    plCopyLink,
    plPending,
    plBackers,
    plState,
    plBump,
    playPlaylist,
    duplicatePlaylist,
    itemTuples,
    refreshLiked,
    type PlaylistDetail,
  } from "$lib/playlists.svelte";
  import { getModuleByMd5, type ModuleHit } from "$lib/catalog";
  import { cachedHit } from "$lib/cidCache";
  import { fmtTime } from "$lib/format";
  import { ui } from "$lib/ui.svelte";

  let { name }: { name: string | null } = $props();

  let detail = $state<PlaylistDetail | null>(null);
  let confirmShare = $state(false);
  let confirmUnshare = $state(false);
  let confirmDelete = $state(false);
  let busy = $state(false);
  let error = $state<string | null>(null);
  let pending = $state(false); // selected name is a name-only deep link awaiting gossip
  let linkCopied = $state(false);
  let editingTitle = $state(false); // title shown as an editable input
  let titleDraft = $state("");
  let loadedFor: string | null = null; // last name a fetch completed for (not reactive)

  // Full catalog metadata per track md5 (format/channels/duration/filename), so rows can show
  // the same columns as the main results table — playlist items themselves carry only
  // md5/modName/title. Seeded synchronously from the local last-known-good cache (cidCache),
  // then filled for anything uncached from the catalog in the background (same resolve path
  // playPlaylist uses). Keyed by md5 so duplicate rows share one lookup.
  let meta = $state<Record<string, ModuleHit>>({});
  let selIdx = $state(-1); // keyboard cursor into detail.items (-1 = nothing selected)
  let listEl: HTMLDivElement | undefined = $state();

  $effect(() => {
    const cur = name;
    void plState.version; // re-fetch on mutations AND the periodic sync tick
    if (!cur) {
      detail = null;
      loadedFor = null;
      pending = false;
      return;
    }
    if (cur !== loadedFor) {
      // Switching playlists: clear immediately. A same-name refresh (the 10s sync
      // tick) keeps the current detail on screen and swaps in place — no flicker.
      detail = null;
      confirmShare = false;
      confirmUnshare = false;
      confirmDelete = false;
      editingTitle = false;
      error = null;
      linkCopied = false;
      selIdx = -1;
    }
    plGet(cur)
      .then(async (d) => {
        if (name !== cur) return;
        detail = d;
        loadedFor = cur;
        // No local row: a name-only deep link may still be syncing from the network.
        pending = d === null && (await plPending().catch((): string[] => [])).includes(cur);
        backerCount = d
          ? ((await plBackers([cur]).catch((): Record<string, number> => ({})))[cur] ?? 0)
          : 0;
      })
      .catch((e) => (error = String(e)));
  });

  // Hold-beacon holder count (24h window; 0 = none heard yet — beacons are hourly).
  let backerCount = $state(0);

  const fmtAge = (secs: number) => {
    const d = Math.floor((Date.now() / 1000 - secs) / 86400);
    return d <= 0 ? "today" : d === 1 ? "1 day" : `${d} days`;
  };

  // Resolve each track's full metadata for the parity columns. Seed from the local cache so
  // known tracks render complete rows immediately, then fetch only the genuinely-uncached md5s
  // from the catalog (serially, to avoid a burst of VFS queries). Re-runs when `detail` swaps,
  // but the cache seed means the periodic sync tick refetches nothing already resolved.
  $effect(() => {
    const d = detail;
    if (!d) {
      meta = {};
      return;
    }
    const seed: Record<string, ModuleHit> = {};
    for (const it of d.items) {
      const c = cachedHit(it.md5);
      if (c) seed[it.md5] = c;
    }
    meta = seed;
    const missing = [...new Set(d.items.map((i) => i.md5))].filter((m) => !seed[m]);
    if (!missing.length) return;
    let cancelled = false;
    void (async () => {
      for (const md5 of missing) {
        if (cancelled || detail !== d) return;
        try {
          const h = await getModuleByMd5(md5);
          if (cancelled || detail !== d) return;
          meta = { ...meta, [md5]: h };
        } catch {
          /* leave the row without format/time — resolution needs the catalog online */
        }
      }
    })();
    return () => (cancelled = true);
  });

  // Keyboard navigation over the track list, mirroring the results table: Arrow/Page/Home/End
  // move a cursor (which also drives the right-hand detail pane, like a click), Enter plays.
  function select(i: number, scroll = true) {
    const items = detail?.items ?? [];
    if (!items.length) return;
    const n = Math.min(items.length - 1, Math.max(0, i));
    selIdx = n;
    ui.inspectorTrackMd5 = items[n].md5;
    ui.inspectorTrackId = null;
    ui.right = "detail";
    if (scroll) (listEl?.children[n] as HTMLElement | undefined)?.scrollIntoView({ block: "nearest" });
  }

  function onKey(e: KeyboardEvent) {
    const items = detail?.items ?? [];
    if (!items.length) return;
    const cur = selIdx < 0 ? 0 : selIdx;
    if (e.key === "ArrowDown") (e.preventDefault(), select(selIdx < 0 ? 0 : cur + 1));
    else if (e.key === "ArrowUp") (e.preventDefault(), select(selIdx < 0 ? 0 : cur - 1));
    else if (e.key === "PageDown") (e.preventDefault(), select(cur + 10));
    else if (e.key === "PageUp") (e.preventDefault(), select(cur - 10));
    else if (e.key === "Home") (e.preventDefault(), select(0));
    else if (e.key === "End") (e.preventDefault(), select(items.length - 1));
    else if (e.key === "Enter" && selIdx >= 0 && detail) {
      e.preventDefault();
      playPlaylist(detail, selIdx).catch((err) => (error = String(err)));
    }
  }

  async function copyLink() {
    if (!detail) return;
    try {
      await navigator.clipboard.writeText(await plCopyLink(detail.name));
      linkCopied = true;
      setTimeout(() => (linkCopied = false), 2000);
    } catch (e) {
      error = String(e);
    }
  }

  async function removeTrack(i: number) {
    if (!detail?.isMine) return;
    const tuples = itemTuples(detail.items);
    tuples.splice(i, 1);
    await plUpdate(detail.name, detail.title, tuples);
    if (detail.liked) void refreshLiked(); // keep the ♥ set in sync when un-liking here
    plBump();
  }

  // Rename reuses the generic update path (rewrites the doc's title, republishing at
  // seq+1 if shared); only `title` changes — `name` is the stable IPNS identity.
  function startRename() {
    if (!detail?.isMine || detail.liked) return; // Liked Tracks title is fixed
    titleDraft = detail.title;
    editingTitle = true;
  }

  function cancelRename() {
    editingTitle = false;
  }

  async function commitRename() {
    if (!detail || !editingTitle) return; // guard the double-fire from Enter → blur
    editingTitle = false;
    const next = titleDraft.trim();
    if (!next || next === detail.title) return; // no-op on empty or unchanged
    try {
      await plUpdate(detail.name, next, itemTuples(detail.items));
      plBump();
    } catch (e) {
      error = `rename failed: ${e}`;
    }
  }

  // Focus + select the input the moment it mounts, so the title is ready to overtype.
  function focusSelect(node: HTMLInputElement) {
    node.focus();
    node.select();
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

  async function unshare() {
    if (!detail) return;
    busy = true;
    error = null;
    try {
      await plUnpublish(detail.name);
      plBump();
    } catch (e) {
      error = `make private failed: ${e}`;
    } finally {
      busy = false;
      confirmUnshare = false;
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
    {#if pending}
      <div class="placeholder">
        syncing playlist from the network…<br />
        <span class="pendsub">
          a holder needs to be online — this usually lands within a few minutes. if it
          never does, the link's holder may be offline.
        </span>
      </div>
    {:else}
      <div class="placeholder">select a playlist</div>
    {/if}
  {:else}
    {#if editingTitle}
      <input
        class="title titleedit"
        bind:value={titleDraft}
        maxlength="200"
        autocomplete="off"
        autocorrect="off"
        autocapitalize="off"
        spellcheck="false"
        onkeydown={(e) => {
          if (e.key === "Enter") commitRename();
          else if (e.key === "Escape") cancelRename();
        }}
        onblur={commitRename}
        use:focusSelect
      />
    {:else if detail.isMine && !detail.liked}
      <div
        class="title renamable"
        role="button"
        tabindex="0"
        title="click to rename"
        onclick={startRename}
        onkeydown={(e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            startRename();
          }
        }}
      >
        {detail.title || "(untitled)"}
      </div>
    {:else}
      <div class="title">{detail.title || "(untitled)"}</div>
    {/if}
    <div class="sub">
      <span class="metatxt">{detail.items.length} tracks · {fmtAge(detail.lastUpdateAt)}</span>
      {#if detail.isMine && !detail.liked}<span class="badge mine">mine</span>{/if}
      {#if detail.held}<span class="badge held">held</span>{/if}
      {#if detail.dormant}<span class="badge dormant" title="record expired — author absent; fork to keep it shareable">dormant</span>{/if}
      {#if detail.published}<span class="badge pub">shared</span>{/if}
      {#if backerCount > 1}
        <span class="badge backers" title="distinct holders heard on the network (24h)">~{backerCount}</span>
      {/if}
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
      {#if detail.isMine && detail.liked}
        <!-- "Liked Tracks" is the private per-client playlist: never shared, never
             deleted from the UI. Only play/edit affordances apply. -->
        <span class="likednote">private · your Liked Tracks</span>
      {:else if detail.isMine}
        {#if !editingTitle}
          <button onclick={startRename} disabled={busy}>rename</button>
        {/if}
        {#if !confirmShare}
          <button onclick={() => (confirmShare = true)} disabled={busy}>
            {detail.published ? "re-share" : "share"}
          </button>
        {/if}
        {#if detail.published && !detail.dormant}
          <button onclick={copyLink}>{linkCopied ? "✓ copied" : "copy link"}</button>
        {/if}
        {#if detail.published && !detail.dormant && !confirmUnshare}
          <button onclick={() => (confirmUnshare = true)} disabled={busy}>make private</button>
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
        {#if !detail.dormant}
          <button onclick={copyLink}>{linkCopied ? "✓ copied" : "copy link"}</button>
        {/if}
        {#if !detail.held}
          <button onclick={del} disabled={busy}>remove</button>
        {/if}
      {/if}
    </div>

    {#if confirmShare}
      <div class="confirm">
        sharing publishes this playlist to <b>everyone</b> on the network. you can make it
        private again later, but copies may persist.
        <div class="cbtns">
          <button class="go" onclick={share} disabled={busy}>share it</button>
          <button onclick={() => (confirmShare = false)}>cancel</button>
        </div>
      </div>
    {/if}
    {#if confirmUnshare}
      <div class="confirm">
        making this private again publishes a tombstone asking the network to drop the
        shared copy, then keeps it here local + editable. best-effort: copies others
        already saved, forks, or offline nodes may persist (any straggler record expires
        within ~7 days).
        <div class="cbtns">
          <button class="go" onclick={unshare} disabled={busy}>make private</button>
          <button onclick={() => (confirmUnshare = false)}>cancel</button>
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

    {#if detail.items.length}
      <div class="thead">
        <span class="num">#</span>
        <span class="c-title">title</span>
        <span class="c-file">file</span>
        <span class="c-fmt">fmt</span>
        <span class="c-ch">ch</span>
        <span class="c-time">time</span>
        <span class="c-rm"></span>
      </div>
    {/if}
    <div
      class="tlist"
      bind:this={listEl}
      onkeydown={onKey}
      tabindex="0"
      role="listbox"
      aria-label="playlist tracks"
    >
      {#each detail.items as t, i (t.md5 + "-" + i)}
        {@const h = meta[t.md5]}
        <div
          class="trow"
          class:sel={selIdx === i}
          onclick={() => select(i, false)}
          ondblclick={() => detail && playPlaylist(detail, i)}
          role="option"
          aria-selected={selIdx === i}
          tabindex="-1"
        >
          <span class="num">{i + 1}</span>
          <span class="c-title" title={h?.filename ?? t.modName}>{h?.title || t.title || t.modName}</span>
          <span class="c-file">{h?.filename ?? t.modName}</span>
          <span class="c-fmt fmt-{h?.format ?? ''}">{h?.format ?? ''}</span>
          <span class="c-ch">{h?.channels ?? ''}</span>
          <span class="c-time">{h ? fmtTime(h.duration) : ''}</span>
          {#if detail.isMine}
            <button class="rm" onclick={(e) => { e.stopPropagation(); removeTrack(i); }} title="remove">✕</button>
          {:else}
            <span class="c-rm"></span>
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
    overflow: hidden; /* the track list scrolls on its own (below); the header/meta stay pinned */
    padding: 1rem;
    display: flex;
    flex-direction: column;
  }
  .placeholder {
    color: var(--dim);
    padding-top: 2rem;
    text-align: center;
  }
  .pendsub {
    font-size: 11px;
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
  .renamable {
    cursor: text;
    border-radius: 3px;
    padding: 0 2px;
    margin: 0 -2px; /* keep text baseline aligned despite the padding */
  }
  .renamable:hover {
    background: var(--row-hover);
  }
  .titleedit {
    font-family: inherit;
    background: var(--row-hover);
    border: 1px solid var(--violet);
    border-radius: 3px;
    padding: 0 2px;
    margin: 0 -3px; /* offset border+padding so the text doesn't shift on edit */
    width: 100%;
    outline: none;
  }
  .sub {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 0.4rem;
    color: var(--dim);
    margin-bottom: 0.7rem;
    font-size: 11px;
  }
  .metatxt {
    color: var(--dim);
  }
  .badge {
    font-size: 10px;
    border: 1px solid var(--border-hi);
    border-radius: 3px;
    padding: 0 0.3rem;
    color: var(--dim);
  }
  .badge.mine {
    color: var(--amber);
    border-color: var(--amber);
  }
  .badge.held {
    color: var(--violet);
    border-color: var(--violet);
  }
  .badge.dormant {
    color: var(--hot);
    border-color: var(--hot);
  }
  .badge.pub {
    color: var(--cyan);
    border-color: var(--cyan);
  }
  .badge.backers {
    color: var(--green, #7dcfa0);
    border-color: var(--green, #7dcfa0);
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
  .likednote {
    align-self: center;
    color: var(--dim);
    font-size: 11px;
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
  /* The list is its own scroll area (like ResultsTable's .vlist) with the header pinned above
     it as a non-scrolling sibling — so scrolling/arrow-nav never slides a row under the header. */
  .tlist {
    flex: 1;
    min-height: 0;
    overflow-y: auto;
    outline: none;
  }
  /* Same column grid as the main results table (ResultsTable.svelte) plus a leading track
     number and a trailing remove slot, so the playlist reads as the same list widget. */
  .thead,
  .trow {
    display: grid;
    grid-template-columns: 28px 1fr 1fr 48px 36px 56px 20px;
    gap: 0.4rem;
    align-items: center;
  }
  .thead {
    height: 24px;
    color: var(--dim);
    border-bottom: 1px solid var(--border);
    text-transform: uppercase;
    font-size: 11px;
    letter-spacing: 0.05em;
  }
  .trow {
    padding: 0.2rem 0;
    height: 24px;
    white-space: nowrap;
    cursor: default; /* rows are dblclick targets, not links (matches QueuePanel) */
  }
  .trow:hover {
    background: var(--row-hover);
  }
  .trow.sel {
    background: var(--row-sel);
  }
  .num {
    color: var(--dim);
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
