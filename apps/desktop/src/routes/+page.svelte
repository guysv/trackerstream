<script lang="ts">
  import { onMount } from "svelte";
  import LibraryFilters from "$lib/components/LibraryFilters.svelte";
  import ResultsTable from "$lib/components/ResultsTable.svelte";
  import { listModules, getFormats, type ModuleHit, type FormatCount } from "$lib/catalog";
  import { playList } from "$lib/player.svelte";
  import { ui } from "$lib/ui.svelte";

  let format = $state<string | null>(null);
  let sort = $state<"latest" | "random" | "title">("latest");
  let rows = $state<ModuleHit[]>([]);
  let formats = $state<FormatCount[]>([]);
  let total = $state(0);

  // The right-panel inspector follows the row selection on this route.
  let selectedId = $state<number | null>(ui.inspectorTrackId);
  let scrollTop = $state(0);
  $effect(() => {
    ui.inspectorTrackId = selectedId;
  });

  // Restore selection + scroll when returning to this route via back/forward. Scroll is
  // best-effort — rows re-fetch on remount, so it lands only if the list is already tall
  // enough; selection survives reliably (the query effect keeps a still-present id).
  export const snapshot = {
    capture: () => ({ selectedId, scrollTop }),
    restore: (v: { selectedId: number | null; scrollTop: number }) => {
      selectedId = v.selectedId;
      scrollTop = v.scrollTop;
    },
  };

  function play(h: ModuleHit) {
    playList(
      rows,
      rows.findIndex((r) => r.id === h.id),
    );
  }

  onMount(() => {
    getFormats()
      .then((f) => ((formats = f.formats), (total = f.total)))
      .catch(() => (ui.status = "catalog offline"));
  });

  let timer: ReturnType<typeof setTimeout>;
  let reqSeq = 0;
  $effect(() => {
    const fmt = format;
    const s = sort;
    clearTimeout(timer);
    ui.status = "loading…";
    timer = setTimeout(async () => {
      // Stale-result guard: catalog queries run over the P2P VFS and a cold one can take
      // seconds, so two can be in flight — only the latest commits.
      const myReq = ++reqSeq;
      try {
        const result = await listModules({ format: fmt ?? undefined, sort: s, limit: 300 });
        if (myReq !== reqSeq) return;
        rows = result;
        if (!rows.some((x) => x.id === selectedId)) selectedId = rows[0]?.id ?? null;
        ui.status = `${rows.length} result${rows.length === 1 ? "" : "s"}`;
      } catch {
        if (myReq === reqSeq) ui.status = "catalog offline";
      }
    }, 0);
  });
</script>

<LibraryFilters {formats} {total} bind:format bind:sort />
<div class="results">
  <ResultsTable {rows} bind:selectedId bind:scrollTop onplay={play} onselect={() => (ui.right = "detail")} />
</div>

<style>
  .results {
    flex: 1;
    display: flex;
    flex-direction: column;
    min-height: 0;
    overflow: hidden;
  }
</style>
