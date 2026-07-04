<script lang="ts">
  import { goto } from "$app/navigation";
  import { page } from "$app/stores";
  import ResultsTable from "$lib/components/ResultsTable.svelte";
  import { search, type ModuleHit } from "$lib/catalog";
  import { plSearch, type PlaylistMeta } from "$lib/playlists.svelte";
  import { playList } from "$lib/player.svelte";
  import { openContextMenu } from "$lib/contextmenu.svelte";
  import { playlistMenuItems } from "$lib/menus";
  import { ui } from "$lib/ui.svelte";

  const q = $derived(($page.url.searchParams.get("q") ?? "").trim());

  let rows = $state<ModuleHit[]>([]);
  let plrows = $state<PlaylistMeta[]>([]);

  // Library = what you own or back; discover = the seen tier gossip surfaced. Split so
  // discover playlists sit in their own section under tracks, not mixed with your library.
  const libRows = $derived(plrows.filter((p) => p.isMine || p.held));
  const discoverRows = $derived(plrows.filter((p) => !p.isMine && !p.held));

  let selectedId = $state<number | null>(null);
  $effect(() => {
    ui.inspectorTrackId = selectedId;
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
    clearTimeout(timer);
    if (!query) {
      rows = [];
      plrows = [];
      ui.status = "";
      return;
    }
    ui.status = "searching…";
    timer = setTimeout(async () => {
      // Same stale-result guard as browse: keep only the latest in-flight query.
      const myReq = ++reqSeq;
      try {
        const [tr, pl] = await Promise.all([search(query, 200), plSearch(query)]);
        if (myReq !== reqSeq) return;
        rows = tr;
        plrows = pl;
        if (!rows.some((x) => x.id === selectedId)) selectedId = rows[0]?.id ?? null;
        ui.status = `${tr.length} track${tr.length === 1 ? "" : "s"} · ${pl.length} playlist${pl.length === 1 ? "" : "s"}`;
      } catch {
        if (myReq === reqSeq) ui.status = "search offline";
      }
    }, 180);
  });
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
