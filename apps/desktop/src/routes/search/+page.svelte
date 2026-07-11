<script lang="ts">
  import { onMount } from "svelte";
  import { goto } from "$app/navigation";
  import { page } from "$app/stores";
  import ResultsTable from "$lib/components/ResultsTable.svelte";
  import { searchStream, cancelSearch, warmCatalog, type ModuleHit } from "$lib/catalog";
  import { plSearch, type PlaylistMeta } from "$lib/playlists.svelte";
  import { playList } from "$lib/player.svelte";
  import { openContextMenu } from "$lib/contextmenu.svelte";
  import { playlistMenuItems } from "$lib/menus";
  import { ui } from "$lib/ui.svelte";
  import { logError } from "$lib/debug";

  const q = $derived(($page.url.searchParams.get("q") ?? "").trim());
  // "names only" toggle (set in the header): restrict the match to title/filename via the
  // names_fts index instead of also matching instrument + comment text. Carried in the URL like
  // `q`, so it survives reload/back-forward and toggling it re-runs the search effect below.
  const namesOnly = $derived($page.url.searchParams.get("names") === "1");

  let rows = $state<ModuleHit[]>([]);
  let plrows = $state<PlaylistMeta[]>([]);

  // Infinite scroll: fetch tracks a chunk at a time. ResultsTable calls loadMore() only once
  // the bottom-most loaded row enters its render window, so we never pull rows the user hasn't
  // scrolled to — the initial fetch fills roughly one viewport, the rest arrives on demand.
  const PAGE = 40;
  let done = $state(false); // the current query has no more track pages
  let fetching = $state(false); // a search or loadMore is in flight (gates re-entry + loadMore)

  // Library = what you own or back; discover = the seen tier gossip surfaced. Split so
  // discover playlists sit in their own section under tracks, not mixed with your library.
  const libRows = $derived(plrows.filter((p) => p.isMine || p.held));
  const discoverRows = $derived(plrows.filter((p) => !p.isMine && !p.held));

  let selectedId = $state<number | null>(null);
  $effect(() => {
    ui.inspectorTrackId = selectedId;
    ui.inspectorTrackMd5 = null; // this route inspects by rowid; drop any playlist md5
  });

  // Warm the catalog page cache (schema + FTS upper tree) before the first keystroke, so the
  // opening search descends from warm pages instead of paying the cold schema/FTS-root fetches.
  onMount(() => {
    void warmCatalog();
  });

  function play(h: ModuleHit) {
    playList(
      rows,
      rows.findIndex((r) => r.id === h.id),
    );
  }

  let timer: ReturnType<typeof setTimeout>;
  let reqSeq = 0;
  $effect(() => {
    const query = q;
    // Capture the toggle as a dependency so flipping it re-runs the search with the same query.
    const names = namesOnly;
    clearTimeout(timer);
    // Supersede the last op: invalidate its result (reqSeq) AND abort its in-flight fetch so
    // tsnode stops pulling pages for a search we've typed past (or cleared).
    reqSeq++;
    void cancelSearch();
    // New query supersedes any pagination state from the previous one.
    done = false;
    // Skip 1-char queries: a single-char prefix (`"a"*`) matches almost everything and scans a
    // huge slice of the FTS dictionary for a useless result. The `prefix='2 3'` index makes 2–3
    // char prefixes cheap, so start searching at 2.
    if (query.length < 2) {
      rows = [];
      plrows = [];
      fetching = false;
      ui.status = query.length === 1 ? "keep typing…" : "";
      return;
    }
    // Hold off loadMore across the debounce + initial stream (cleared in the timeout's finally).
    fetching = true;
    ui.status = "searching…";
    timer = setTimeout(async () => {
      // Same stale-result guard as browse: keep only the latest in-flight query.
      const myReq = ++reqSeq;
      // Stream hits in as their pages arrive. Keep the previous results on screen until the
      // first new hit lands (no empty flash), then replace; append the rest.
      let firstRow = true;
      const onRow = (hit: ModuleHit) => {
        if (myReq !== reqSeq) return;
        if (firstRow) {
          rows = [hit];
          selectedId = hit.id;
          firstRow = false;
        } else {
          rows.push(hit);
        }
        ui.status = `${rows.length}${rows.length >= PAGE ? "+" : ""} track${rows.length === 1 ? "" : "s"}…`;
      };
      try {
        const [count, pl] = await Promise.all([searchStream(query, PAGE, undefined, onRow, names), plSearch(query)]);
        if (myReq !== reqSeq) return;
        if (count === 0) {
          rows = []; // query matched nothing → clear the stale rows we kept on screen
          selectedId = null;
        }
        plrows = pl;
        done = count < PAGE; // a short first page means there's no more
        ui.status = `${count}${done ? "" : "+"} track${count === 1 ? "" : "s"} · ${pl.length} playlist${pl.length === 1 ? "" : "s"}`;
      } catch (e) {
        // The header status is the user-facing surface (a toast per keystroke would spam);
        // but the real error — an FTS syntax error, a node outage, etc. — reaches the log.
        logError("search", e, { q: query, names });
        if (myReq === reqSeq) ui.status = "search offline";
      } finally {
        if (myReq === reqSeq) fetching = false;
      }
    }, 250);
  });

  // Fetch the next page and append it, using the last row's id as the keyset cursor. Called
  // by ResultsTable as the viewport nears the end; guarded so scroll spam can't double-load,
  // and dropped if the query changed mid-fetch (reqSeq).
  async function loadMore() {
    if (fetching || done || rows.length === 0) return;
    fetching = true;
    const myReq = reqSeq;
    const cursor = rows[rows.length - 1].id;
    try {
      // Stream-append the next chunk; each hit shows the moment its pages arrive.
      const count = await searchStream(q, PAGE, cursor, (hit) => {
        if (myReq === reqSeq) rows.push(hit);
      }, namesOnly);
      if (myReq !== reqSeq) return; // superseded by a newer query
      if (count < PAGE) done = true;
      ui.status = `${rows.length}${done ? "" : "+"} tracks · ${plrows.length} playlist${plrows.length === 1 ? "" : "s"}`;
    } catch (e) {
      // Transient — leave `done` false so a later scroll retries; log so a persistent
      // paging failure (vs a one-off blip) is visible rather than an infinite silent retry.
      logError("search:loadMore", e, { q, cursor });
    } finally {
      if (myReq === reqSeq) fetching = false;
    }
  }
</script>

{#snippet plrow(p: PlaylistMeta)}
  <a
    class="prow"
    href="/playlists/{encodeURIComponent(p.name)}"
    oncontextmenu={(e) => openContextMenu(e, () => playlistMenuItems(p))}
  >
    <span class="ptitle">
      {#if p.liked}<span class="lheart">♥</span> {/if}{p.title || "(untitled)"}
    </span>
    <span class="pmeta">{p.tracks} trk</span>
  </a>
{/snippet}

{#snippet discover()}
  {#if discoverRows.length}
    <div class="section flow">discover playlists</div>
    {#each discoverRows as p (p.name)}{@render plrow(p)}{/each}
  {/if}
{/snippet}

{#if !q}
  <div class="empty">type in the search box to find tracks and playlists</div>
{:else}
  {#if libRows.length}
    <div class="section">library playlists</div>
    <div class="plist">
      {#each libRows as p (p.name)}{@render plrow(p)}{/each}
    </div>
  {/if}
  <div class="section">tracks</div>
  <div class="results">
    <ResultsTable
      {rows}
      bind:selectedId
      onplay={play}
      onselect={() => (ui.right = "detail")}
      onendreached={loadMore}
      busy={fetching}
      footer={discover}
    />
  </div>
{/if}

<style>
  .empty {
    padding: 2rem;
    text-align: center;
    color: var(--dim);
  }
  .section {
    color: var(--amber);
    text-transform: uppercase;
    font-size: 11px;
    letter-spacing: 0.05em;
    padding: 0.5rem 0.8rem 0.3rem;
    border-bottom: 1px solid var(--border);
  }
  /* Discover header flows inside the tracks scroll area, under the last row. */
  .section.flow {
    margin-top: 0.5rem;
    border-top: 1px solid var(--border);
  }
  .plist {
    max-height: 30%;
    overflow-y: auto;
    flex: none;
  }
  .prow {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 0.8rem;
    padding: 0.3rem 0.8rem;
    height: 28px;
    text-decoration: none;
    color: var(--fg);
  }
  .prow:hover {
    background: var(--row-hover);
  }
  .ptitle {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .lheart {
    color: var(--hot);
  }
  .pmeta {
    color: var(--dim);
    font-size: 11px;
  }
  /* The tracks table takes the remaining height and scrolls internally. */
  .results {
    flex: 1;
    display: flex;
    flex-direction: column;
    min-height: 0;
    overflow: hidden;
  }
</style>
