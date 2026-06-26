<script lang="ts">
  import { queue, playList, removeFromQueue, moveInQueue, clearQueue } from "$lib/player.svelte";
  import { fmtTime } from "$lib/format";
</script>

<div class="queue">
  <div class="qhead">
    <span>queue · {queue.items.length}</span>
    <button onclick={clearQueue} disabled={!queue.items.length}>clear</button>
  </div>

  <div class="qlist">
    {#each queue.items as item, i (item.id + "-" + i)}
      <div class="qrow" class:cur={i === queue.index}>
        <span class="num">{i + 1}</span>
        <span class="name" ondblclick={() => playList(queue.items, i)} role="button" tabindex="-1"
          >{item.title || item.filename}</span
        >
        <span class="time">{fmtTime(item.duration)}</span>
        <span class="ops">
          <button onclick={() => moveInQueue(i, -1)} title="up">↑</button>
          <button onclick={() => moveInQueue(i, 1)} title="down">↓</button>
          <button onclick={() => removeFromQueue(i)} title="remove">✕</button>
        </span>
      </div>
    {/each}
    {#if !queue.items.length}<div class="empty">queue empty</div>{/if}
  </div>
</div>

<style>
  .queue {
    height: 100%;
    display: flex;
    flex-direction: column;
  }
  .qhead {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0.6rem 0.8rem;
    color: var(--amber);
    border-bottom: 1px solid var(--border);
    text-transform: uppercase;
    font-size: 11px;
  }
  .qlist {
    flex: 1;
    overflow-y: auto;
  }
  .qrow {
    display: grid;
    grid-template-columns: 28px 1fr auto auto;
    gap: 0.4rem;
    align-items: center;
    padding: 0.25rem 0.8rem;
    height: 26px;
  }
  .qrow:hover {
    background: var(--row-hover);
  }
  .qrow.cur {
    background: var(--row-sel);
  }
  .qrow.cur .name {
    color: var(--accent);
  }
  .num {
    color: var(--dim);
  }
  .name {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    cursor: default;
  }
  .time {
    color: var(--dim);
  }
  .ops {
    display: flex;
    gap: 2px;
    opacity: 0;
  }
  .qrow:hover .ops {
    opacity: 1;
  }
  .ops button {
    padding: 0 0.3rem;
    background: none;
    border: none;
    color: var(--dim);
  }
  .ops button:hover {
    color: var(--fg);
  }
  .empty {
    padding: 2rem;
    text-align: center;
    color: var(--dim);
  }
</style>
