<script lang="ts">
  import { onMount } from "svelte";
  import {
    auth,
    login,
    register,
    logout,
    followUser,
    getFollowing,
    resolveShare,
    type Presence,
  } from "$lib/social.svelte";
  import { playList } from "$lib/player.svelte";
  import { fmtTime } from "$lib/format";
  import type { ModuleHit } from "$lib/catalog";

  let email = $state("");
  let password = $state("");
  let mode = $state<"login" | "register">("login");
  let err = $state("");
  let followEmail = $state("");
  let friends = $state<Presence[]>([]);
  let shareCode = $state("");

  async function submit() {
    err = "";
    try {
      if (mode === "login") await login(email.trim(), password);
      else await register(email.trim(), password);
      password = "";
      await refresh();
    } catch (e) {
      err = String((e as Error).message ?? e);
    }
  }

  async function refresh() {
    if (!auth.token) return;
    try {
      friends = await getFollowing();
    } catch {
      /* offline */
    }
  }

  async function doFollow() {
    if (!followEmail.trim()) return;
    try {
      await followUser(followEmail.trim());
      followEmail = "";
      await refresh();
    } catch (e) {
      err = String((e as Error).message ?? e);
    }
  }

  function playPresence(p: Presence) {
    if (!p.rootCid) return;
    const hit: ModuleHit = {
      id: p.moduleId ?? -1,
      filename: p.title ?? "shared",
      format: "",
      title: p.title ?? "",
      duration: 0,
      channels: 0,
      rootCid: p.rootCid,
    };
    playList([hit], 0);
  }

  async function openShare() {
    if (!shareCode.trim()) return;
    try {
      const s = await resolveShare(shareCode.trim());
      if (s.resolved?.rootCid) playList([s.resolved], 0);
      shareCode = "";
    } catch (e) {
      err = String((e as Error).message ?? e);
    }
  }

  onMount(() => {
    void refresh();
    const t = setInterval(refresh, 15000);
    return () => clearInterval(t);
  });
</script>

<div class="social">
  {#if !auth.token}
    <div class="head">account</div>
    <form onsubmit={(e) => (e.preventDefault(), submit())}>
      <input bind:value={email} placeholder="email" autocomplete="email" />
      <input bind:value={password} type="password" placeholder="password" />
      <button type="submit">{mode}</button>
    </form>
    <button class="link" onclick={() => (mode = mode === "login" ? "register" : "login")}>
      {mode === "login" ? "create account" : "have an account? log in"}
    </button>
    {#if err}<div class="err">{err}</div>{/if}
  {:else}
    <div class="head">
      <span>{auth.email}</span>
      <button class="sm" onclick={() => (logout(), (friends = []))}>log out</button>
    </div>

    <div class="sub">follow</div>
    <form class="row" onsubmit={(e) => (e.preventDefault(), doFollow())}>
      <input bind:value={followEmail} placeholder="friend's email" />
      <button type="submit">+</button>
    </form>

    <div class="sub">open share code</div>
    <form class="row" onsubmit={(e) => (e.preventDefault(), openShare())}>
      <input bind:value={shareCode} placeholder="paste code" />
      <button type="submit">play</button>
    </form>

    <div class="sub">following · now playing</div>
    <div class="friends">
      {#each friends as f}
        <div class="friend">
          <div class="fmeta">
            <div class="fe">{f.email}</div>
            <div class="ft">{f.title ?? "—"}</div>
          </div>
          <button class="sm" disabled={!f.rootCid} onclick={() => playPresence(f)}>▶</button>
        </div>
      {/each}
      {#if !friends.length}<div class="empty">not following anyone</div>{/if}
    </div>
    {#if err}<div class="err">{err}</div>{/if}
  {/if}
</div>

<style>
  .social {
    height: 100%;
    overflow-y: auto;
    padding: 1rem;
  }
  .head {
    color: var(--cyan);
    text-transform: uppercase;
    font-size: 11px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    border-bottom: 1px solid var(--border);
    padding-bottom: 0.3rem;
    margin-bottom: 0.6rem;
  }
  .sub {
    color: var(--amber);
    text-transform: uppercase;
    font-size: 10px;
    margin: 0.9rem 0 0.3rem;
  }
  form {
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
  }
  form.row {
    flex-direction: row;
  }
  form.row input {
    flex: 1;
  }
  .link {
    background: none;
    border: none;
    color: var(--blue);
    padding: 0.4rem 0;
    font-size: 11px;
  }
  .sm {
    padding: 0.1rem 0.4rem;
    font-size: 11px;
  }
  .friend {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0.35rem 0;
    border-bottom: 1px solid var(--border);
  }
  .fe {
    color: var(--fg);
  }
  .ft {
    color: var(--dim);
    font-size: 11px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 220px;
  }
  .empty {
    color: var(--dim);
    padding: 0.6rem 0;
  }
  .err {
    color: var(--hot);
    margin-top: 0.6rem;
    font-size: 11px;
  }
</style>
