// Pluggable OAuth (authorization-code flow) + an ActivityPub-handle identity alias.
//
// Scope (be honest): this is *login federation*, not social federation. We let a
// user prove control of a GitHub/Google/OIDC account and link it to a local
// trackerstream account, returning the SAME { token, user } shape as email login.
// Full ActivityPub federation (an actor, inbox/outbox, signed activities, follow
// fan-out) is OUT of scope; what we ship is a webfinger-verified handle *alias*
// (see resolveApHandle + the design TODO at the bottom).
//
// No new deps: only built-in `fetch`. The whole feature is a no-op (routes return
// 501 "oauth not configured") for any provider whose env vars are unset, so the
// existing build keeps running with zero config.
import { randomBytes } from "node:crypto";
import type { Social, User } from "./social.ts";

// --- provider registry ---

export interface OAuthProvider {
  // OAuth endpoints.
  authorizeUrl: string;
  tokenUrl: string;
  scope: string;
  // Reads credentials from env; returns null when unconfigured (-> 501).
  config(): { clientId: string; clientSecret: string; redirectUri: string } | null;
  // Exchange the provider access token for a normalized { id, email }.
  profile(accessToken: string): Promise<{ id: string; email: string }>;
}

const env = (k: string): string => process.env[k] ?? "";

/** Default redirect: <PUBLIC_BASE_URL>/auth/oauth/<provider>/callback. */
const redirectFor = (provider: string): string =>
  `${env("PUBLIC_BASE_URL") || "http://localhost:8080"}/auth/oauth/${provider}/callback`;

// GitHub. Profile email may be private, so fall back to the /user/emails endpoint
// and pick the primary, verified address.
const github: OAuthProvider = {
  authorizeUrl: "https://github.com/login/oauth/authorize",
  tokenUrl: "https://github.com/login/oauth/access_token",
  scope: "read:user user:email",
  config() {
    const clientId = env("GITHUB_OAUTH_CLIENT_ID");
    const clientSecret = env("GITHUB_OAUTH_CLIENT_SECRET");
    if (!clientId || !clientSecret) return null;
    return { clientId, clientSecret, redirectUri: env("GITHUB_OAUTH_REDIRECT_URI") || redirectFor("github") };
  },
  async profile(accessToken) {
    const headers = { Authorization: `Bearer ${accessToken}`, "User-Agent": "trackerstream", Accept: "application/vnd.github+json" };
    const u = (await (await fetch("https://api.github.com/user", { headers })).json()) as { id: number; email: string | null };
    let email = u.email;
    if (!email) {
      const emails = (await (await fetch("https://api.github.com/user/emails", { headers })).json()) as Array<{ email: string; primary: boolean; verified: boolean }>;
      email = (emails.find((e) => e.primary && e.verified) ?? emails.find((e) => e.verified))?.email ?? null;
    }
    if (!email) throw new Error("github: no verified email available");
    return { id: String(u.id), email };
  },
};

// Google (OIDC). Wired but only active once GOOGLE_OAUTH_CLIENT_ID/SECRET are set —
// demonstrates that a second provider is a config away, not a code change.
const google: OAuthProvider = {
  authorizeUrl: "https://accounts.google.com/o/oauth2/v2/auth",
  tokenUrl: "https://oauth2.googleapis.com/token",
  scope: "openid email profile",
  config() {
    const clientId = env("GOOGLE_OAUTH_CLIENT_ID");
    const clientSecret = env("GOOGLE_OAUTH_CLIENT_SECRET");
    if (!clientId || !clientSecret) return null;
    return { clientId, clientSecret, redirectUri: env("GOOGLE_OAUTH_REDIRECT_URI") || redirectFor("google") };
  },
  async profile(accessToken) {
    const u = (await (await fetch("https://openidconnect.googleapis.com/v1/userinfo", {
      headers: { Authorization: `Bearer ${accessToken}` },
    })).json()) as { sub: string; email: string; email_verified?: boolean };
    if (!u.email) throw new Error("google: no email in userinfo");
    return { id: u.sub, email: u.email };
  },
};

// Generic OIDC: a single env-driven provider so self-hosted IdPs (Keycloak, Authentik,
// Auth0...) work without new code. Set OIDC_AUTHORIZE_URL/OIDC_TOKEN_URL/OIDC_USERINFO_URL
// + OIDC_OAUTH_CLIENT_ID/SECRET.
const oidc: OAuthProvider = {
  get authorizeUrl() {
    return env("OIDC_AUTHORIZE_URL");
  },
  get tokenUrl() {
    return env("OIDC_TOKEN_URL");
  },
  scope: env("OIDC_SCOPE") || "openid email profile",
  config() {
    const clientId = env("OIDC_OAUTH_CLIENT_ID");
    const clientSecret = env("OIDC_OAUTH_CLIENT_SECRET");
    if (!clientId || !clientSecret || !env("OIDC_AUTHORIZE_URL") || !env("OIDC_TOKEN_URL") || !env("OIDC_USERINFO_URL"))
      return null;
    return { clientId, clientSecret, redirectUri: env("OIDC_OAUTH_REDIRECT_URI") || redirectFor("oidc") };
  },
  async profile(accessToken) {
    const u = (await (await fetch(env("OIDC_USERINFO_URL"), {
      headers: { Authorization: `Bearer ${accessToken}` },
    })).json()) as { sub: string; email: string };
    if (!u.email) throw new Error("oidc: no email in userinfo");
    return { id: u.sub, email: u.email };
  },
};

const PROVIDERS: Record<string, OAuthProvider> = { github, google, oidc };

// --- CSRF state (short TTL, in-memory) ---
// A single-process master node, so a Map is sufficient; restart simply invalidates
// any in-flight logins. { provider } is bound to the state so a callback can't be
// replayed against a different provider.
const STATE_TTL_MS = 10 * 60 * 1000;
const states = new Map<string, { provider: string; expires: number }>();

function newState(provider: string): string {
  const s = randomBytes(16).toString("hex");
  states.set(s, { provider, expires: Date.now() + STATE_TTL_MS });
  return s;
}
function takeState(state: string, provider: string): boolean {
  const e = states.get(state);
  states.delete(state); // single-use
  return !!e && e.provider === provider && e.expires > Date.now();
}
// Opportunistic GC so the map can't grow unbounded under abandoned logins.
function gcStates(): void {
  if (states.size < 256) return;
  const now = Date.now();
  for (const [k, v] of states) if (v.expires <= now) states.delete(k);
}

// --- flow ---

/** Build the provider authorize URL (or null when unconfigured -> caller 501s). */
export function startUrl(provider: string): string | null {
  const p = PROVIDERS[provider];
  const cfg = p?.config();
  if (!p || !cfg) return null;
  gcStates();
  const state = newState(provider);
  const q = new URLSearchParams({
    client_id: cfg.clientId,
    redirect_uri: cfg.redirectUri,
    scope: p.scope,
    state,
    response_type: "code",
  });
  return `${p.authorizeUrl}?${q.toString()}`;
}

/**
 * Handle the provider redirect-back: validate state, exchange code -> access token,
 * fetch the profile, and find-or-create-or-link the local user. Returns the same
 * { token, user } shape as email login, or null when the provider is unconfigured.
 * Throws on a genuine error (bad state, token exchange failure).
 */
export async function handleCallback(
  social: Social,
  provider: string,
  code: string,
  state: string,
): Promise<{ token: string; user: User } | null> {
  const p = PROVIDERS[provider];
  const cfg = p?.config();
  if (!p || !cfg) return null;
  if (!takeState(state, provider)) throw new Error("invalid or expired state");

  // Token exchange. GitHub honors Accept: application/json; OIDC/Google use the
  // standard JSON token response too.
  const tokenRes = await fetch(p.tokenUrl, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded", Accept: "application/json" },
    body: new URLSearchParams({
      client_id: cfg.clientId,
      client_secret: cfg.clientSecret,
      code,
      redirect_uri: cfg.redirectUri,
      grant_type: "authorization_code",
    }).toString(),
  });
  const tok = (await tokenRes.json()) as { access_token?: string; error?: string };
  if (!tok.access_token) throw new Error(`token exchange failed: ${tok.error ?? tokenRes.status}`);

  const prof = await p.profile(tok.access_token);
  const user = social.findOrCreateOAuthUser(provider, prof.id, prof.email);
  return { token: social.mintToken(user.id), user };
}

// --- ActivityPub handle (identity alias, NOT federation) ---
//
// We accept "@user@instance" (or "user@instance") at registration as a verifiable
// alias. Verification = a webfinger lookup that proves the actor exists; we then
// store the canonical handle on the user. We do NOT fetch/store the actor object,
// follow it, or expose an inbox.
//
// TODO (full federation, deliberately deferred — see MVP-FOLLOWUP E1):
//   1. webfinger:  GET https://<instance>/.well-known/webfinger?resource=acct:user@instance
//                  -> JRD with links[rel="self", type="application/activity+json"].href = actor URL  (done below).
//   2. actor fetch: GET that href (Accept: application/activity+json) -> { id, inbox, outbox,
//                   publicKey, preferredUsername }.  (NOT done — would let us display/verify the profile.)
//   3. to *be* an actor (receive follows): host /.well-known/webfinger + an actor doc +
//      an inbox that verifies HTTP Signatures, persist followers, and deliver signed
//      Create/Announce activities to follower inboxes.  This is the large piece left out.

const HANDLE_RE = /^@?([A-Za-z0-9_.-]+)@([A-Za-z0-9.-]+)$/;

/** Webfinger-resolve a handle to its actor URL, proving it exists. null = not found/invalid. */
export async function resolveApHandle(handle: string): Promise<{ handle: string; actor: string } | null> {
  const m = HANDLE_RE.exec(handle.trim());
  if (!m) return null;
  const [, user, instance] = m;
  const acct = `acct:${user}@${instance}`;
  try {
    const res = await fetch(`https://${instance}/.well-known/webfinger?resource=${encodeURIComponent(acct)}`, {
      headers: { Accept: "application/jrd+json, application/json" },
    });
    if (!res.ok) return null;
    const jrd = (await res.json()) as { links?: Array<{ rel: string; type?: string; href?: string }> };
    const self = jrd.links?.find((l) => l.rel === "self" && l.type === "application/activity+json" && l.href);
    if (!self?.href) return null;
    return { handle: `@${user}@${instance}`, actor: self.href };
  } catch {
    return null; // network/DNS error -> treat as unverifiable
  }
}

export const isProvider = (name: string): boolean => name in PROVIDERS;
