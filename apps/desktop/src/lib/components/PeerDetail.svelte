<script lang="ts">
  import { onDestroy } from "svelte";
  import { peers, speedHistory, connectedSince, clearSelection } from "$lib/peers.svelte";
  import { peerDetail, type PeerDetail } from "$lib/p2p";
  import { fmtBytes } from "$lib/format";

  let { peerId }: { peerId: string } = $props();

  // Fast fields (bandwidth/speed/role) come from the 1 Hz list poll; the rich
  // fields (transport, identity, rtt) are fetched on a lighter cadence.
  let detail = $state<PeerDetail | null>(null);
  const row = $derived(peers.rows.find((r) => r.id === peerId));

  async function refresh(): Promise<void> {
    try {
      detail = await peerDetail(peerId);
    } catch {
      /* keep the last snapshot */
    }
  }
  void refresh();
  const iv = setInterval(refresh, 2500);
  onDestroy(() => clearInterval(iv));

  const rate = (n: number): string => `${fmtBytes(n)}/s`;
  const short = (id: string): string =>
    id.length > 18 ? `${id.slice(0, 9)}…${id.slice(-9)}` : id;

  const sharePct = $derived(
    peers.totalDown > 0 && row ? Math.round((row.down / peers.totalDown) * 100) : 0,
  );

  function sinceLabel(): string {
    const t = connectedSince(peerId);
    if (!t) return "—";
    const s = Math.max(0, Math.floor((Date.now() - t) / 1000));
    if (s < 60) return `${s}s`;
    const m = Math.floor(s / 60);
    if (m < 60) return `${m}m ${s % 60}s`;
    return `${Math.floor(m / 60)}h ${m % 60}m`;
  }

  // Sparkline polyline points from the rolling download-speed history.
  function spark(): string {
    const h = speedHistory(peerId);
    if (h.length < 2) return "";
    const max = Math.max(1, ...h);
    const w = 160;
    const ht = 30;
    return h.map((v, i) => `${(i / (h.length - 1)) * w},${ht - (v / max) * ht}`).join(" ");
  }

  async function copyId(): Promise<void> {
    try {
      await navigator.clipboard.writeText(peerId);
    } catch {
      /* clipboard unavailable */
    }
  }
</script>

<div class="detail">
  <div class="dhead">
    <button class="back" onclick={clearSelection} title="back to peers">←</button>
    <span class="role role-{detail?.role ?? row?.role ?? 'other'}">
      {detail?.role ?? row?.role ?? "peer"}
    </span>
    <button class="pid" onclick={copyId} title="click to copy full id">{short(peerId)} ⧉</button>
  </div>

  {#if row && !row.connected}
    <div class="banner">disconnected — showing last-known stats</div>
  {/if}

  <div class="body">
    <!-- Transport: the offload-truth row -->
    <div class="block">
      <span class="lbl">transport</span>
      {#if detail}
        {#if detail.relayed}
          <span class="big warn">⚠ relayed via master</span>
          <span class="sub warn">bytes routed through the master relay — not a true offload</span>
        {:else}
          <span class="big ok">direct · {detail.transport}</span>
        {/if}
        {#if detail.addrs.length}
          <span class="sub mono">{detail.addrs[0]}</span>
        {/if}
      {:else}
        <span class="big">…</span>
      {/if}
    </div>

    <!-- Contribution -->
    <div class="block">
      <span class="lbl">contribution</span>
      <div class="kvs">
        <span>↓ {rate(row?.speedDown ?? 0)}</span>
        <span>↑ {rate(row?.speedUp ?? 0)}</span>
      </div>
      <div class="kvs sub">
        <span>{fmtBytes(row?.down ?? 0)} down total</span>
        {#if detail?.role !== "master"}<span>{sharePct}% of your downloads</span>{/if}
      </div>
      {#if spark()}
        <svg class="spark" viewBox="0 0 160 30" preserveAspectRatio="none">
          <polyline points={spark()} fill="none" stroke="currentColor" stroke-width="1.2" />
        </svg>
      {/if}
    </div>

    <!-- Connection facts -->
    <div class="grid">
      <div><span class="lbl">rtt</span><span>{detail?.rtt_ms != null ? `${detail.rtt_ms.toFixed(0)} ms` : "—"}</span></div>
      <div><span class="lbl">connected</span><span>{row?.connected ? sinceLabel() : "no"}</span></div>
      <div>
        <span class="lbl">why</span>
        <span>{detail?.warm_reason?.length ? detail.warm_reason.join(", ") : detail?.role === "master" ? "origin seed" : "—"}</span>
      </div>
      <div><span class="lbl">client</span><span class="mono">{detail?.agent ?? "—"}</span></div>
    </div>

    {#if detail?.protocols?.length}
      <div class="block">
        <span class="lbl">protocols</span>
        <span class="sub mono">{detail.protocols.join("  ")}</span>
      </div>
    {/if}
  </div>
</div>

<style>
  .detail {
    height: 100%;
    display: flex;
    flex-direction: column;
    font-size: 12px;
  }
  .dhead {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.6rem 0.8rem;
    border-bottom: 1px solid var(--border);
  }
  .back {
    background: none;
    border: 1px solid var(--border);
    color: var(--fg);
    border-radius: 4px;
    cursor: pointer;
    padding: 0 0.4rem;
    line-height: 1.6;
  }
  .pid {
    margin-left: auto;
    background: none;
    border: none;
    color: var(--dim);
    cursor: pointer;
    font-family: var(--mono, monospace);
  }
  .role {
    text-transform: uppercase;
    font-size: 10px;
    padding: 0.1rem 0.35rem;
    border-radius: 3px;
    border: 1px solid currentColor;
  }
  .role-master {
    color: var(--amber);
  }
  .role-warm {
    color: var(--green, #7dcfa0);
  }
  .role-other {
    color: var(--dim);
  }
  .banner {
    padding: 0.4rem 0.8rem;
    background: color-mix(in srgb, var(--amber) 15%, transparent);
    color: var(--amber);
    font-size: 11px;
  }
  .body {
    overflow-y: auto;
    padding: 0.8rem;
    display: flex;
    flex-direction: column;
    gap: 0.9rem;
  }
  .block {
    display: flex;
    flex-direction: column;
    gap: 0.2rem;
  }
  .lbl {
    color: var(--dim);
    text-transform: uppercase;
    font-size: 10px;
  }
  .big {
    font-size: 15px;
  }
  .ok {
    color: var(--green, #7dcfa0);
  }
  .warn {
    color: var(--amber);
  }
  .sub {
    font-size: 11px;
    color: var(--dim);
  }
  .mono {
    font-family: var(--mono, monospace);
    word-break: break-all;
  }
  .kvs {
    display: flex;
    gap: 1.2rem;
    font-variant-numeric: tabular-nums;
  }
  .spark {
    width: 100%;
    height: 30px;
    color: var(--green, #7dcfa0);
    margin-top: 0.2rem;
  }
  .grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.7rem;
  }
  .grid > div {
    display: flex;
    flex-direction: column;
    gap: 0.15rem;
  }
</style>
