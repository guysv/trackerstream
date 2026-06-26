// Control-plane HTTP API: catalog search/browse. Results carry the root CID,
// which the client resolves over libp2p (the data plane). Plain node:http. All
// endpoints are public reads; user-generated data (playlists, social) is being
// rebuilt on the P2P plane and no longer lives here. CORS-open.
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { Catalog } from "./catalog.ts";
import { Tracker } from "./tracker.ts";
import { IpnsStore } from "./ipns.ts";

type Handler = (
  req: IncomingMessage,
  res: ServerResponse,
  ctx: { url: URL; body: unknown; catalog: Catalog; tracker: Tracker; ipns: IpnsStore },
) => void | Promise<void>;

// Hand-validate an announce body (no validation lib; raw node:http).
function parseAnnounce(
  body: unknown,
): { peerId: string; addrs: string[]; heldRoots: string[] } | null {
  if (!body || typeof body !== "object") return null;
  const b = body as Record<string, unknown>;
  const strArr = (v: unknown): string[] =>
    Array.isArray(v) ? v.filter((x): x is string => typeof x === "string") : [];
  if (typeof b.peerId !== "string" || !b.peerId) return null;
  return { peerId: b.peerId, addrs: strArr(b.addrs), heldRoots: strArr(b.heldRoots) };
}

function readBody(req: IncomingMessage): Promise<unknown> {
  return new Promise((resolve) => {
    const chunks: Buffer[] = [];
    req.on("data", (c) => chunks.push(c as Buffer));
    req.on("end", () => {
      const s = Buffer.concat(chunks).toString("utf8");
      try {
        resolve(s ? JSON.parse(s) : {});
      } catch {
        resolve({});
      }
    });
    req.on("error", () => resolve({}));
  });
}

const send = (res: ServerResponse, code: number, body: unknown) => {
  res.statusCode = code;
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify(body));
};

// Optional shared secret for the IPNS publish hook. When TS_IPNS_TOKEN is set,
// POST /ipns requires `Authorization: Bearer <token>`; unset = open (dev only).
const IPNS_TOKEN = process.env.TS_IPNS_TOKEN;

export function createApi(catalog: Catalog, tracker: Tracker, ipns: IpnsStore): Server {
  const routes: Array<{ method: string; pattern: RegExp; handler: Handler }> = [
    {
      method: "GET",
      pattern: /^\/healthz$/,
      handler: (_q, res) =>
        send(res, 200, {
          ok: true,
          modules: catalog.count(),
          tracker: tracker.stats(),
          uptimeSeconds: Math.round(process.uptime()),
        }),
    },
    {
      method: "GET",
      pattern: /^\/search$/,
      handler: (_q, res, { url }) => {
        const q = url.searchParams.get("q") ?? "";
        const limit = Math.min(200, Math.max(1, +(url.searchParams.get("limit") ?? 50)));
        send(res, 200, { query: q, results: catalog.search(q, limit) });
      },
    },
    {
      method: "GET",
      pattern: /^\/modules$/,
      handler: (_q, res, { url }) => {
        const format = url.searchParams.get("format") ?? undefined;
        const sort = (url.searchParams.get("sort") ?? "latest") as "latest" | "random" | "title";
        const limit = +(url.searchParams.get("limit") ?? 100);
        const offset = +(url.searchParams.get("offset") ?? 0);
        send(res, 200, { results: catalog.list({ format, sort, limit, offset }) });
      },
    },
    {
      method: "GET",
      pattern: /^\/formats$/,
      handler: (_q, res) => send(res, 200, { formats: catalog.formatCounts(), total: catalog.count() }),
    },
    {
      method: "GET",
      pattern: /^\/module\/(\d+)$/,
      handler: (_q, res, { url }) => {
        const id = +url.pathname.split("/")[2];
        const hit = catalog.get(id);
        hit ? send(res, 200, hit) : send(res, 404, { error: "not found" });
      },
    },
    // --- Peer-assist tracker (PEER-ASSIST.md §2.2) ---
    {
      method: "POST",
      pattern: /^\/announce$/,
      handler: (_q, res, { body, tracker }) => {
        const a = parseAnnounce(body);
        if (!a) return send(res, 400, { error: "bad announce" });
        tracker.announce(a.peerId, a.addrs, a.heldRoots);
        send(res, 200, { ok: true });
      },
    },
    {
      method: "GET",
      pattern: /^\/peers$/,
      handler: (_q, res, { url, tracker }) => {
        const root = url.searchParams.get("root") ?? "";
        if (!root) return send(res, 400, { error: "missing root" });
        const limit = Math.min(50, Math.max(1, +(url.searchParams.get("limit") ?? 50)));
        const self = url.searchParams.get("self") ?? undefined;
        send(res, 200, { root, peers: tracker.peers(root, limit, self) });
      },
    },
    {
      method: "GET",
      pattern: /^\/roster$/,
      handler: (_q, res, { tracker }) => send(res, 200, { peers: tracker.roster() }),
    },
    // --- IPNS record cache (PEER-ASSIST.md §9; inert until catalog migrates) ---
    {
      method: "GET",
      pattern: /^\/ipns\/([^/]+)$/,
      handler: (_q, res, { url, ipns }) => {
        const key = decodeURIComponent(url.pathname.split("/")[2]);
        const record = ipns.get(key);
        record ? send(res, 200, { key, record }) : send(res, 404, { error: "no record" });
      },
    },
    {
      method: "POST",
      pattern: /^\/ipns$/,
      handler: (req, res, { body, ipns }) => {
        if (IPNS_TOKEN && req.headers.authorization !== `Bearer ${IPNS_TOKEN}`) {
          return send(res, 401, { error: "unauthorized" });
        }
        const b = (body ?? {}) as Record<string, unknown>;
        if (typeof b.key !== "string" || typeof b.record !== "string") {
          return send(res, 400, { error: "key + record (base64) required" });
        }
        ipns.set(b.key, b.record);
        send(res, 200, { ok: true });
      },
    },
  ];

  return createServer(async (req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type,Authorization");
    if (req.method === "OPTIONS") return send(res, 204, {});

    const url = new URL(req.url ?? "/", "http://localhost");
    const route = routes.find((r) => r.method === req.method && r.pattern.test(url.pathname));
    if (!route) return send(res, 404, { error: "no such route" });
    try {
      const body = req.method === "POST" || req.method === "PUT" ? await readBody(req) : undefined;
      await route.handler(req, res, { url, body, catalog, tracker, ipns });
    } catch (e) {
      send(res, 500, { error: String(e) });
    }
  });
}
