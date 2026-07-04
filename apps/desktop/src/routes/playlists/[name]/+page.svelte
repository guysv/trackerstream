<script lang="ts">
  import { page } from "$app/stores";
  import PlaylistDetailPanel from "$lib/components/PlaylistDetailPanel.svelte";
  import { ui } from "$lib/ui.svelte";

  // SvelteKit URL-decodes params; the name is the playlist's stable IPNS identity.
  const name = $derived($page.params.name ?? null);
  $effect(() => {
    ui.status = ""; // this route has no header status of its own
  });
</script>

<PlaylistDetailPanel {name} />
