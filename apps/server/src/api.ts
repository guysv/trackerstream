// Control-plane HTTP API: catalog search/browse + playlists. Results carry the
// root CID, which the client resolves over libp2p (the data plane). Plain
// node:http. Read endpoints are public; playlists are storage-only here (auth +
// ownership land in Phase 7). CORS-open.
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { Catalog } from "./catalog.ts";
import { Social, type User } from "./social.ts";
import { handleCallback, isProvider, resolveApHandle, startUrl } from "./oauth.ts";

type Handler = (
  req: IncomingMessage,
  res: ServerResponse,
  ctx: { url: URL; body: unknown; catalog: Catalog; social: Social; user: User | null },
) => void | Promise<void>;

const requireAuth = (res: ServerResponse, user: User | null): user is User => {
  if (!user) {
    send(res, 401, { error: "auth required" });
    return false;
  }
  return true;
};

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

export function createApi(catalog: Catalog, social: Social): Server {
  const routes: Array<{ method: string; pattern: RegExp; handler: Handler }> = [
    {
      method: "GET",
      pattern: /^\/healthz$/,
      handler: (_q, res) =>
        send(res, 200, {
          ok: true,
          modules: catalog.count(),
          playlists: catalog.playlistCount(),
          users: social.userCount(),
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
    {
      method: "GET",
      pattern: /^\/playlists$/,
      handler: (_q, res, { user }) =>
        send(res, 200, { playlists: catalog.listPlaylists(user ? String(user.id) : undefined) }),
    },
    {
      method: "POST",
      pattern: /^\/playlists$/,
      handler: (_q, res, { body, user }) => {
        const b = (body ?? {}) as { name?: string; isPublic?: boolean; items?: number[] };
        if (!b.name) return send(res, 400, { error: "name required" });
        const id = catalog.createPlaylist(b.name, !!b.isPublic, user ? String(user.id) : null);
        if (Array.isArray(b.items)) catalog.setPlaylistItems(id, b.items);
        send(res, 201, catalog.getPlaylist(id));
      },
    },
    {
      method: "GET",
      pattern: /^\/playlist\/(\d+)$/,
      handler: (_q, res, { url }) => {
        const pl = catalog.getPlaylist(+url.pathname.split("/")[2]);
        pl ? send(res, 200, pl) : send(res, 404, { error: "not found" });
      },
    },
    {
      method: "PUT",
      pattern: /^\/playlist\/(\d+)$/,
      handler: (_q, res, { url, body }) => {
        const id = +url.pathname.split("/")[2];
        const b = (body ?? {}) as { name?: string; isPublic?: boolean; items?: number[] };
        if (b.name !== undefined || b.isPublic !== undefined)
          catalog.updatePlaylistMeta(id, b.name, b.isPublic);
        if (Array.isArray(b.items)) catalog.setPlaylistItems(id, b.items);
        const pl = catalog.getPlaylist(id);
        pl ? send(res, 200, pl) : send(res, 404, { error: "not found" });
      },
    },
    {
      method: "DELETE",
      pattern: /^\/playlist\/(\d+)$/,
      handler: (_q, res, { url }) => {
        catalog.deletePlaylist(+url.pathname.split("/")[2]);
        send(res, 200, { ok: true });
      },
    },

    // --- auth ---
    {
      method: "POST",
      pattern: /^\/auth\/register$/,
      handler: async (_q, res, { body }) => {
        const b = (body ?? {}) as { email?: string; password?: string; apHandle?: string };
        if (!b.email || !b.password) return send(res, 400, { error: "email + password required" });
        // Optional ActivityPub handle: webfinger-verify before accepting (alias only).
        let apHandle: string | null = null;
        if (b.apHandle) {
          const r = await resolveApHandle(b.apHandle);
          if (!r) return send(res, 400, { error: "apHandle not found / unverifiable" });
          apHandle = r.handle;
        }
        try {
          const out = social.register(b.email, b.password);
          if (apHandle) social.setApHandle(out.user.id, apHandle);
          send(res, 201, out);
        } catch {
          send(res, 409, { error: "email already registered" });
        }
      },
    },
    {
      method: "POST",
      pattern: /^\/auth\/login$/,
      handler: (_q, res, { body }) => {
        const b = (body ?? {}) as { email?: string; password?: string };
        const r = b.email && b.password ? social.login(b.email, b.password) : null;
        r ? send(res, 200, r) : send(res, 401, { error: "bad credentials" });
      },
    },
    {
      method: "GET",
      pattern: /^\/me$/,
      handler: (_q, res, { user }) => (requireAuth(res, user) ? send(res, 200, user) : undefined),
    },

    // --- OAuth (authorization-code) ---
    // Pluggable providers (github, google, generic oidc). No-op (501) when the
    // provider's env vars are unset, so the default build needs no config.
    {
      method: "GET",
      pattern: /^\/auth\/oauth\/([\w-]+)\/start$/,
      handler: (_q, res, { url }) => {
        const provider = url.pathname.split("/")[3];
        if (!isProvider(provider)) return send(res, 404, { error: "no such provider" });
        const dest = startUrl(provider);
        if (!dest) return send(res, 501, { error: "oauth not configured" });
        // Browsers follow the 302 to the provider; API/JSON callers can read Location.
        res.statusCode = 302;
        res.setHeader("Location", dest);
        res.end();
      },
    },
    {
      method: "GET",
      pattern: /^\/auth\/oauth\/([\w-]+)\/callback$/,
      handler: async (_q, res, { url, social }) => {
        const provider = url.pathname.split("/")[3];
        if (!isProvider(provider)) return send(res, 404, { error: "no such provider" });
        const code = url.searchParams.get("code");
        const state = url.searchParams.get("state");
        if (!code || !state) return send(res, 400, { error: "code + state required" });
        try {
          const r = await handleCallback(social, provider, code, state);
          r ? send(res, 200, r) : send(res, 501, { error: "oauth not configured" });
        } catch (e) {
          send(res, 401, { error: String((e as Error).message ?? e) });
        }
      },
    },

    // --- ActivityPub handle (identity alias; webfinger-verified, not federation) ---
    {
      method: "POST",
      pattern: /^\/ap\/verify$/,
      handler: async (_q, res, { body, user }) => {
        if (!requireAuth(res, user)) return;
        const b = (body ?? {}) as { handle?: string };
        if (!b.handle) return send(res, 400, { error: "handle required" });
        const r = await resolveApHandle(b.handle);
        if (!r) return send(res, 404, { error: "handle not found / unverifiable" });
        social.setApHandle(user.id, r.handle);
        send(res, 200, { ok: true, handle: r.handle, actor: r.actor });
      },
    },

    // --- follow + presence ---
    {
      method: "POST",
      pattern: /^\/follow$/,
      handler: (_q, res, { body, user }) => {
        if (!requireAuth(res, user)) return;
        const b = (body ?? {}) as { email?: string };
        const target = b.email ? social.findUser(b.email) : null;
        if (!target) return send(res, 404, { error: "no such user" });
        social.follow(user.id, target.id);
        send(res, 200, { ok: true, following: target });
      },
    },
    {
      method: "DELETE",
      pattern: /^\/follow\/(\d+)$/,
      handler: (_q, res, { url, user }) => {
        if (!requireAuth(res, user)) return;
        social.unfollow(user.id, +url.pathname.split("/")[2]);
        send(res, 200, { ok: true });
      },
    },
    {
      method: "GET",
      pattern: /^\/following$/,
      handler: (_q, res, { user }) =>
        requireAuth(res, user) ? send(res, 200, { following: social.following(user.id) }) : undefined,
    },
    {
      method: "POST",
      pattern: /^\/presence$/,
      handler: (_q, res, { body, user }) => {
        if (!requireAuth(res, user)) return;
        const b = (body ?? {}) as { moduleId?: number; rootCid?: string; title?: string };
        social.setPresence(user.id, b.moduleId ?? null, b.rootCid ?? null, b.title ?? null);
        send(res, 200, { ok: true });
      },
    },

    // --- share links ---
    {
      method: "POST",
      pattern: /^\/share$/,
      handler: (_q, res, { body }) => {
        const b = (body ?? {}) as { kind?: "module" | "playlist"; refId?: number; rootCid?: string };
        if (!b.kind || !b.refId) return send(res, 400, { error: "kind + refId required" });
        send(res, 201, { code: social.createShare(b.kind, b.refId, b.rootCid ?? null) });
      },
    },
    {
      method: "GET",
      pattern: /^\/share\/([\w-]+)$/,
      handler: (_q, res, { url }) => {
        const code = url.pathname.split("/")[2];
        const s = social.resolveShare(code);
        if (!s) return send(res, 404, { error: "no such share" });
        const resolved =
          s.kind === "module" ? catalog.get(s.refId) : catalog.getPlaylist(s.refId);
        send(res, 200, { kind: s.kind, ...s, resolved });
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
      const token = (req.headers.authorization ?? "").replace(/^Bearer\s+/i, "") || null;
      const user = social.userForToken(token);
      const body = req.method === "POST" || req.method === "PUT" ? await readBody(req) : undefined;
      await route.handler(req, res, { url, body, catalog, social, user });
    } catch (e) {
      send(res, 500, { error: String(e) });
    }
  });
}
