<script lang="ts">
  // The web shell: boot a js-libp2p node, install it as the NodeClient, then mount the SAME app the
  // desktop runs. This file plus lib/client/web.ts is the entire difference between the two.
  import { AppShell, setClient } from "@trackerstream/ui";
  import { WebClient } from "$lib/client/web";
  import "@trackerstream/ui/theme.css";

  let { children } = $props();

  // The node has to exist before any store touches client(), so the app doesn't mount until it does.
  // Booting is a real network round-trip (fetch /bootstrap.json -> dial the seed over webrtc-direct
  // -> resolve the catalog's IPNS name), so it gets an honest progress line rather than a blank page.
  let error = $state<string | null>(null);
  const boot = WebClient.create()
    .then((c) => setClient(c))
    .catch((e) => (error = e instanceof Error ? e.message : String(e)));
</script>

{#await boot}
  <p class="boot">connecting to the swarm…</p>
{:then}
  {#if error}
    <p class="boot err">could not join the swarm: {error}</p>
  {:else}
    <AppShell {children} />
  {/if}
{/await}

<style>
  .boot {
    display: grid;
    place-items: center;
    height: 100vh;
    margin: 0;
    color: var(--fg-dim, #888);
    font: 13px/1.4 system-ui, sans-serif;
  }
  .err {
    color: var(--err, #e66);
  }
</style>
