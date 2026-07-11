<script lang="ts">
  import { menu, closeContextMenu, type MenuItem } from "$lib/contextmenu.svelte";
  import { logError } from "$lib/debug";

  let el: HTMLDivElement | undefined = $state();

  // Re-derive items every render so an open menu reflects live state (see the store's
  // reactivity contract). A stable identity for `menu.open` means "same menu instance".
  const items = $derived(menu.open ? menu.open.build() : []);

  // Submenu state, reset whenever a NEW menu opens (not on every reactive re-derive).
  let lastOpen: unknown = null;
  let openSub = $state<number | null>(null);
  let subItems = $state<MenuItem[]>([]);
  let subLoading = $state(false);
  let subToken = 0;
  $effect(() => {
    if (menu.open !== lastOpen) {
      lastOpen = menu.open;
      openSub = null;
      subItems = [];
    }
  });

  async function hoverItem(i: number, it: MenuItem) {
    if (it.kind !== "submenu") {
      openSub = null;
      return;
    }
    openSub = i;
    const token = ++subToken;
    if (typeof it.items === "function") {
      subLoading = true;
      subItems = [];
      try {
        const resolved = await it.items();
        if (token === subToken) subItems = resolved;
      } catch (e) {
        logError("contextmenu:submenu", e, { label: it.label });
        if (token === subToken) subItems = [];
      } finally {
        if (token === subToken) subLoading = false;
      }
    } else {
      subItems = it.items;
      subLoading = false;
    }
  }

  async function run(it: MenuItem) {
    if (it.kind !== "action" || it.disabled) return;
    closeContextMenu();
    await it.onSelect();
  }

  // Clamp the menu into the viewport once it has a measured size.
  $effect(() => {
    if (!el || !menu.open) return;
    const r = el.getBoundingClientRect();
    const vw = window.innerWidth;
    const vh = window.innerHeight;
    let x = menu.open.x;
    let y = menu.open.y;
    if (x + r.width > vw - 4) x = Math.max(4, vw - r.width - 4);
    if (y + r.height > vh - 4) y = Math.max(4, vh - r.height - 4);
    el.style.left = `${x}px`;
    el.style.top = `${y}px`;
  });

  // Close on any scroll (capturing, so inner scrollers count too) while open.
  $effect(() => {
    if (!menu.open) return;
    const onScroll = () => closeContextMenu();
    window.addEventListener("scroll", onScroll, true);
    return () => window.removeEventListener("scroll", onScroll, true);
  });

  function onWinDown(e: MouseEvent) {
    if (menu.open && el && !el.contains(e.target as Node)) closeContextMenu();
  }
  function onWinKey(e: KeyboardEvent) {
    if (menu.open && e.key === "Escape") {
      e.stopPropagation();
      closeContextMenu();
    }
  }
</script>

<svelte:window onmousedown={onWinDown} onkeydown={onWinKey} />

{#if menu.open}
  <div class="menu" bind:this={el} style="left:{menu.open.x}px;top:{menu.open.y}px" role="menu">
    {#each items as it, i}
      {#if it.kind === "separator"}
        <div class="sep"></div>
      {:else if it.kind === "header"}
        <div class="mheader">{it.label}</div>
      {:else if it.kind === "submenu"}
        <!-- svelte-ignore a11y_no_static_element_interactions -->
        <div
          class="mitem sub"
          class:open={openSub === i}
          role="menuitem"
          tabindex="-1"
          onmouseenter={() => hoverItem(i, it)}
        >
          {#if it.icon}<span class="ico">{it.icon}</span>{/if}
          <span class="lbl">{it.label}</span>
          <span class="arrow">▸</span>
          {#if openSub === i}
            <div class="menu submenu" role="menu">
              {#if subLoading}
                <div class="mitem loading">…</div>
              {:else}
                {#each subItems as sit}
                  {#if sit.kind === "separator"}
                    <div class="sep"></div>
                  {:else if sit.kind === "action"}
                    <button
                      class="mitem"
                      class:danger={sit.danger}
                      disabled={sit.disabled}
                      onclick={() => run(sit)}
                    >
                      {#if sit.icon}<span class="ico">{sit.icon}</span>{/if}
                      <span class="lbl">{sit.label}</span>
                    </button>
                  {/if}
                {/each}
              {/if}
            </div>
          {/if}
        </div>
      {:else}
        <button
          class="mitem"
          class:danger={it.danger}
          disabled={it.disabled}
          onmouseenter={() => (openSub = null)}
          onclick={() => run(it)}
        >
          {#if it.icon}<span class="ico">{it.icon}</span>{/if}
          <span class="lbl">{it.label}</span>
        </button>
      {/if}
    {/each}
  </div>
{/if}

<style>
  .menu {
    position: fixed;
    z-index: 40;
    min-width: 180px;
    max-width: 280px;
    padding: 0.25rem;
    background: var(--panel);
    border: 1px solid var(--border-hi);
    border-radius: 6px;
    box-shadow: 0 8px 24px rgba(0, 0, 0, 0.4);
    display: flex;
    flex-direction: column;
  }
  .submenu {
    position: absolute;
    left: 100%;
    top: -0.25rem;
    margin-left: 2px;
  }
  .mitem {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    width: 100%;
    text-align: left;
    background: none;
    border: none;
    border-radius: 4px;
    padding: 0.3rem 0.5rem;
    color: var(--fg);
    white-space: nowrap;
    position: relative;
  }
  .mitem:hover:not(:disabled),
  .mitem.open {
    background: var(--row-hover);
  }
  .mitem:disabled {
    color: var(--dim);
    opacity: 0.5;
  }
  .mitem.danger {
    color: var(--hot);
  }
  .mitem.loading {
    color: var(--dim);
  }
  .ico {
    width: 1em;
    text-align: center;
    color: var(--dim);
  }
  .lbl {
    overflow: hidden;
    text-overflow: ellipsis;
    flex: 1;
  }
  .arrow {
    color: var(--dim);
    font-size: 10px;
  }
  .mheader {
    color: var(--dim);
    text-transform: uppercase;
    font-size: 10px;
    letter-spacing: 0.06em;
    padding: 0.3rem 0.5rem 0.15rem;
  }
  .sep {
    height: 1px;
    background: var(--border);
    margin: 0.25rem 0.3rem;
  }
</style>
