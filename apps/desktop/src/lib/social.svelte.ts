// Social (lite) client — accounts, follow, presence, share. All over HTTP
// (control plane). The auth token is held locally; presence is pushed when a
// track starts. Never touches the P2P plane.
import { API_BASE_URL } from "@trackerstream/config";
import type { ModuleHit } from "./catalog";

interface Auth {
  token: string | null;
  email: string | null;
}

export const auth = $state<Auth>(loadAuth());

function loadAuth(): Auth {
  try {
    const a = JSON.parse(localStorage.getItem("ts.auth") ?? "");
    if (a && typeof a.token === "string") return a;
  } catch {
    /* none */
  }
  return { token: null, email: null };
}
function saveAuth() {
  try {
    localStorage.setItem("ts.auth", JSON.stringify(auth));
  } catch {
    /* headless */
  }
}

async function call<T>(path: string, method = "GET", body?: unknown): Promise<T> {
  const res = await fetch(`${API_BASE_URL}${path}`, {
    method,
    headers: {
      "Content-Type": "application/json",
      ...(auth.token ? { Authorization: `Bearer ${auth.token}` } : {}),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!res.ok) throw new Error((await res.json().catch(() => ({})))?.error ?? `${path}: ${res.status}`);
  return res.json() as Promise<T>;
}

export async function register(email: string, password: string): Promise<void> {
  const r = await call<{ token: string; user: { email: string } }>("/auth/register", "POST", { email, password });
  auth.token = r.token;
  auth.email = r.user.email;
  saveAuth();
}
export async function login(email: string, password: string): Promise<void> {
  const r = await call<{ token: string; user: { email: string } }>("/auth/login", "POST", { email, password });
  auth.token = r.token;
  auth.email = r.user.email;
  saveAuth();
}
export function logout(): void {
  auth.token = null;
  auth.email = null;
  saveAuth();
}

export const followUser = (email: string) => call("/follow", "POST", { email });

export interface Presence {
  userId: number;
  email: string;
  moduleId: number | null;
  rootCid: string | null;
  title: string | null;
  updatedAt: number;
}
export const getFollowing = (): Promise<Presence[]> =>
  call<{ following: Presence[] }>("/following").then((r) => r.following);

export function pushPresence(hit: ModuleHit): void {
  if (!auth.token) return;
  void call("/presence", "POST", { moduleId: hit.id, rootCid: hit.rootCid, title: hit.title }).catch(() => {});
}

export const shareModule = (hit: ModuleHit): Promise<string> =>
  call<{ code: string }>("/share", "POST", { kind: "module", refId: hit.id, rootCid: hit.rootCid }).then(
    (r) => r.code,
  );

export const resolveShare = (
  code: string,
): Promise<{ kind: string; rootCid: string | null; resolved: ModuleHit }> => call(`/share/${code}`);
