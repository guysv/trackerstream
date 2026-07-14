<script lang="ts">
  import { goto } from "$app/navigation";
  import { page } from "$app/stores";
  import PlaylistsView from "../components/PlaylistsView.svelte";

  // URL is the source of truth for which playlist is open — derive the highlight from it.
  const openName = $derived(
    $page.url.pathname.startsWith("/playlists/")
      ? decodeURIComponent($page.url.pathname.slice("/playlists/".length))
      : null,
  );
  const isHome = $derived($page.url.pathname === "/");
  const isSearch = $derived($page.url.pathname === "/search");

  const open = (name: string) => goto("/playlists/" + encodeURIComponent(name));
</script>

<nav class="rail">
  <div class="brand">tracker<span>stream</span></div>
  <div class="links">
    <a href="/" class="link" class:active={isHome}>home</a>
    <a href="/search" class="link" class:active={isSearch}>search</a>
  </div>
  <div class="lib">
    <PlaylistsView selectedName={openName} onopen={open} />
  </div>
</nav>

<style>
  .rail {
    display: flex;
    flex-direction: column;
    min-height: 0;
    background: var(--panel);
    border-right: 1px solid var(--border);
  }
  .brand {
    color: var(--accent);
    font-size: 15px;
    letter-spacing: 0.04em;
    padding: 0.8rem 0.9rem 0.6rem;
  }
  .brand span {
    color: var(--fg);
  }
  .links {
    display: flex;
    flex-direction: column;
    padding: 0 0.6rem 0.4rem;
    border-bottom: 1px solid var(--border);
  }
  .link {
    color: var(--dim);
    text-transform: uppercase;
    font-size: 12px;
    letter-spacing: 0.05em;
    text-decoration: none;
    padding: 0.3rem 0.4rem;
    border-radius: 4px;
  }
  .link:hover {
    background: var(--row-hover);
    color: var(--fg);
  }
  .link.active {
    background: var(--row-sel);
    color: var(--accent);
  }
  /* PlaylistsView fills the remaining height and scrolls internally. */
  .lib {
    flex: 1;
    min-height: 0;
    display: flex;
    flex-direction: column;
  }
</style>
