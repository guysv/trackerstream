// Control-plane HTTP API: catalog search/browse. Results carry the root CID,
// which the client resolves over libp2p (the data plane). Plain node:http. All
// endpoints are public reads; user-generated data (playlists, social) is being
// rebuilt on the P2P plane and no longer lives here. CORS-open.
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { Catalog } from "./catalog.ts";

type Handler = (
  req: IncomingMessage,
  res: ServerResponse,
  ctx: { url: URL; body: unknown; catalog: Catalog },
) => void | Promise<void>;

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

export function createApi(catalog: Catalog): Server {
  const routes: Array<{ method: string; pattern: RegExp; handler: Handler }> = [
    {
      method: "GET",
      pattern: /^\/healthz$/,
      handler: (_q, res) =>
        send(res, 200, {
          ok: true,
          modules: catalog.count(),
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
      await route.handler(req, res, { url, body, catalog });
    } catch (e) {
      send(res, 500, { error: String(e) });
    }
  });
}
