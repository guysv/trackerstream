<script lang="ts">
  import { peers, selection, selectPeer } from "$lib/peers.svelte";
  import { fmtBytes } from "$lib/format";
  import PeerDetail from "./PeerDetail.svelte";

  const rate = (n: number): string => `${fmtBytes(n)}/s`;
  const short = (id: string): string => (id.length > 16 ? `${id.slice(0, 6)}…${id.slice(-6)}` : id);

  // AutoNAT reachability badge: public = directly dialable (UPnP mapped or natively
  // reachable); private = behind NAT (relay/DCUtR only); undecided while AutoNAT probes.
  const reachLabel = (r: boolean | null): string =>
    r === true ? "public" : r === false ? "private" : "checking…";
  const reachTitle = (r: boolean | null): string =>
    r === true
      ? "directly reachable — other peers can dial in (UPnP/port-map or native public IP)"
      : r === false
        ? "behind NAT — reachable only via relay + DCUtR hole-punching"
        : "AutoNAT is still determining reachability";

  function rowKey(e: KeyboardEvent, id: string): void {
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      selectPeer(id);
    }
  }
</script>

{#if selection.id}
  <PeerDetail peerId={selection.id} />
{:else}
<div class="peers">
  <div class="phead">
    <span>peers · {peers.connected} connected</span>
    <span class="spacer"></span>
    {#if peers.offloadDown > 0}
      <span class="offload" title="cumulative download from non-master peers (offload)">
        offload {fmtBytes(peers.offloadDown)}
      </span>
    {/if}
    <span
      class="reach"
      class:public={peers.reachable === true}
      class:private={peers.reachable === false}
      title={reachTitle(peers.reachable)}
    >
      <span class="rdot"></span>{reachLabel(peers.reachable)}
    </span>
  </div>

  <div class="bw">
    <div class="bwcol">
      <span class="dir">↓ down</span>
      <span class="rate">{rate(peers.speedDown)}</span>
      <span class="total">{fmtBytes(peers.totalDown)}</span>
    </div>
    <div class="bwcol">
      <span class="dir">↑ up</span>
      <span class="rate">{rate(peers.speedUp)}</span>
      <span class="total">{fmtBytes(peers.totalUp)}</span>
    </div>
  </div>

  <div class="plist">
    {#each peers.rows as p (p.id)}
      <div
        class="prow"
        class:off={!p.connected}
        role="button"
        tabindex="0"
        onclick={() => selectPeer(p.id)}
        onkeydown={(e) => rowKey(e, p.id)}
        title="view peer details"
      >
        <span
          class="dot"
          class:master={p.role === "master"}
          class:warm={p.role === "warm"}
          class:on={p.connected}
        ></span>
        <span class="pid">
          {short(p.id)}
          {#if p.role === "master"}<span class="tag">master</span>{/if}
          {#if p.role === "warm"}<span class="tag warm">warm</span>{/if}
        </span>
        <span class="pbw" title="download from this peer">↓ {rate(p.speedDown)}<small>{fmtBytes(p.down)}</small></span>
        <span class="pbw" title="upload to this peer">↑ {rate(p.speedUp)}<small>{fmtBytes(p.up)}</small></span>
      </div>
    {/each}
    {#if !peers.rows.length}<div class="empty">no peers yet</div>{/if}
  </div>
</div>
{/if}

<style>
  .peers {
    height: 100%;
    display: flex;
    flex-direction: column;
  }
  .phead {
    padding: 0.6rem 0.8rem;
    color: var(--cyan);
    border-bottom: 1px solid var(--border);
    text-transform: uppercase;
    font-size: 11px;
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    gap: 0.5rem;
  }
  .spacer {
    flex: 1;
  }
  .offload {
    color: var(--green, #7dcfa0);
    font-size: 10px;
  }
  .reach {
    display: inline-flex;
    align-items: center;
    gap: 0.3rem;
    font-size: 10px;
    color: var(--dim);
    text-transform: lowercase;
  }
  .reach.public {
    color: var(--green, #7dcfa0);
  }
  .reach.private {
    color: var(--amber);
  }
  .rdot {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: currentColor;
    flex: none;
  }
  .bw {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.8rem;
    padding: 0.8rem;
    border-bottom: 1px solid var(--border);
  }
  .bwcol {
    display: flex;
    flex-direction: column;
    gap: 0.15rem;
  }
  .dir {
    color: var(--dim);
    font-size: 11px;
    text-transform: uppercase;
  }
  .rate {
    color: var(--fg);
    font-size: 18px;
    font-variant-numeric: tabular-nums;
  }
  .total {
    color: var(--dim);
    font-size: 11px;
  }
  .plist {
    flex: 1;
    overflow-y: auto;
  }
  .prow {
    display: grid;
    grid-template-columns: auto 1fr auto auto;
    align-items: center;
    gap: 0.5rem;
    padding: 0.35rem 0.8rem;
    font-size: 12px;
    border-bottom: 1px solid color-mix(in srgb, var(--border) 50%, transparent);
    cursor: pointer;
    text-align: left;
  }
  .prow:hover {
    background: color-mix(in srgb, var(--fg) 6%, transparent);
  }
  .prow:focus-visible {
    outline: 1px solid var(--cyan);
    outline-offset: -1px;
  }
  .prow.off {
    opacity: 0.4;
  }
  .dot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: var(--dim);
    flex: none;
  }
  .dot.on {
    background: var(--green, #7dcfa0);
  }
  .dot.master {
    background: var(--amber);
  }
  .dot.warm {
    background: var(--green, #7dcfa0);
  }
  .pid {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-family: var(--mono, monospace);
    color: var(--fg);
  }
  .tag {
    color: var(--amber);
    font-size: 10px;
    text-transform: uppercase;
    margin-left: 0.3rem;
  }
  .tag.warm {
    color: var(--green, #7dcfa0);
  }
  .pbw {
    display: flex;
    flex-direction: column;
    align-items: flex-end;
    color: var(--dim);
    font-variant-numeric: tabular-nums;
    min-width: 64px;
    line-height: 1.2;
  }
  .pbw small {
    font-size: 10px;
    opacity: 0.7;
  }
  .empty {
    padding: 2rem;
    text-align: center;
    color: var(--dim);
  }
</style>
