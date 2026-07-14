<script lang="ts">
  // The desktop shell: install the Tauri NodeClient, then mount the shared app.
  // Everything the user actually sees lives in @trackerstream/ui and is byte-identical to the web
  // build — this file is the entire difference between the two.
  import { AppShell, setClient } from "@trackerstream/ui";
  import { TauriClient } from "$lib/client/tauri";
  import "@trackerstream/ui/theme.css";

  // Before mount, and before any store touches client(): AppShell's onMount wires the playlist
  // signals, peer polling and media keys, all of which need a node.
  setClient(new TauriClient());

  let { children } = $props();
</script>

<AppShell {children} />
