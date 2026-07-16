// Per-peer bandwidth accounting for the browser node — the js-libp2p equivalent of the desktop's
// go-libp2p metrics.BandwidthCounter (node/node.go: libp2p.BandwidthReporter). The desktop's peers
// pane shows real up/down per peer; without this the web client reports zeros (see web.ts peers()).
//
// js-libp2p has no per-peer bandwidth reporter built in — the stock metrics packages
// (@libp2p/simple-metrics, @libp2p/prometheus-metrics) only aggregate global/by-direction totals.
// But the core ALREADY calls two byte hooks on whatever `metrics` component you supply:
//   - upgrader.js -> metrics.trackMultiaddrConnection(maConn)   (raw per-connection)
//   - connection.js -> metrics.trackProtocolStream(stream, conn) (per protocol stream, WITH the conn)
// The tab passes NO metrics component today, so both are no-ops. We supply one and count bytes in
// trackProtocolStream, keyed by connection.remotePeer — ALL-PROTOCOL, matching the go counter's
// semantics (it tallies every stream, not just bitswap). This is the officially-pluggable seam, not
// a libp2p fork.
//
// WHY trackProtocolStream, not trackMultiaddrConnection: WebRTC carries bytes over datachannel
// STREAMS, not a single maConn source/sink, so the per-connection hook would miss most of a browser's
// traffic. The per-stream hook fires for every datachannel and hands us the connection (-> peer id),
// so it captures webrtc-direct, relayed, and browser<->browser alike. Bytes counted here are at the
// protocol-payload level (post-mux, post-decrypt) — the same level go's Reporter logs at.
import type { Connection, Metrics, Stream } from "@libp2p/interface";

export interface PeerBytes {
  down: number; // bytes RECEIVED from the peer (stream.source)
  up: number; // bytes SENT to the peer (stream.sink)
}

/** Iterate a stream's source/sink chunks, tallying each chunk's length before passing it through
 *  untouched. `for await` handles both sync and async iterables (a sink is handed either). Chunks are
 *  Uint8Array or Uint8ArrayList; both expose `.byteLength`. */
async function* meter(src: AsyncIterable<{ byteLength: number }> | Iterable<{ byteLength: number }>, onBytes: (n: number) => void): AsyncGenerator<{ byteLength: number }> {
  for await (const chunk of src) {
    onBytes(chunk.byteLength);
    yield chunk;
  }
}

/** A no-op stat object covering every Metric/Counter/Histogram/Summary (+ *Group) method libp2p
 *  components call on the metrics component. We only care about the two byte hooks; everything else
 *  (identify/dht/gossipsub registering counters) must not throw, but we record nothing. */
const NOOP: Record<string, (...a: unknown[]) => unknown> = new Proxy(
  {},
  { get: () => () => {} },
);

/**
 * The metrics component: implements just enough of `Metrics` to receive the byte hooks, and keeps a
 * per-peer running total. Held by node.ts (so web.ts can read it) and handed to createLibp2p via a
 * `metrics: () => tracker` factory.
 */
export class BandwidthTracker {
  private byPeer = new Map<string, PeerBytes>();

  /** Cumulative up/down for one peer since the tab loaded, or zeros if we've seen no stream from it. */
  get(peerId: string): PeerBytes {
    return this.byPeer.get(peerId) ?? { down: 0, up: 0 };
  }

  /** Sum across all peers (the node-wide totals, were we to surface them). */
  totals(): PeerBytes {
    let down = 0;
    let up = 0;
    for (const v of this.byPeer.values()) {
      down += v.down;
      up += v.up;
    }
    return { down, up };
  }

  private ensure(peerId: string): PeerBytes {
    let rec = this.byPeer.get(peerId);
    if (!rec) {
      rec = { down: 0, up: 0 };
      this.byPeer.set(peerId, rec);
    }
    return rec;
  }

  // ---- the byte hooks (the whole point) ----

  /** Called by connection.js for every negotiated protocol stream, AFTER source/sink are set and
   *  BEFORE the handler/caller consumes them (so our wrap lands first). Wrap both directions to tally
   *  bytes against the connection's remote peer. All protocols count. */
  trackProtocolStream(stream: Stream, connection: Connection): void {
    const rec = this.ensure(connection.remotePeer.toString());
    // source = bytes we READ from the remote -> download.
    const origSource = stream.source as AsyncIterable<{ byteLength: number }>;
    stream.source = meter(origSource, (n) => (rec.down += n)) as typeof stream.source;
    // sink = bytes we WRITE to the remote -> upload. The consumer hands its outgoing source to sink;
    // wrap THAT source so we count on the way out, then defer to the real sink.
    const origSink = stream.sink.bind(stream);
    stream.sink = (source) => origSink(meter(source, (n) => (rec.up += n)) as typeof source);
  }

  /** We attribute at the protocol-stream level (see file header — webrtc bytes never touch a single
   *  maConn), so the raw per-connection hook is intentionally a no-op. */
  trackMultiaddrConnection(): void {}

  // ---- register*/trace: no-op stubs so other components' metric calls don't throw ----
  registerMetric = () => NOOP;
  registerMetricGroup = () => NOOP;
  registerCounter = () => NOOP;
  registerCounterGroup = () => NOOP;
  registerHistogram = () => NOOP;
  registerHistogramGroup = () => NOOP;
  registerSummary = () => NOOP;
  registerSummaryGroup = () => NOOP;
  traceFunction = <F>(_name: string, fn: F): F => fn;
  createTrace = (): unknown => undefined;
}

/** libp2p's `metrics` option is a `(components) => Metrics` factory. Our tracker needs no components,
 *  so bind the caller's instance. Cast once here: the register/trace stubs satisfy Metrics
 *  structurally but not its exact overload types, and forcing those would drown the real logic. */
export const bandwidthMetrics = (tracker: BandwidthTracker) => (): Metrics => tracker as unknown as Metrics;
