// Catalog/search client — the control plane (HTTP). Module *bytes* never come
// from here; search/browse results carry a root CID the data plane resolves P2P.
import { API_BASE_URL } from "@trackerstream/config";

export interface ModuleHit {
  id: number;
  filename: string;
  format: string;
  title: string;
  duration: number;
  channels: number;
  rootCid: string;
}

export interface ModuleDetail extends ModuleHit {
  numSamples: number;
  numInstruments: number;
  numSubsongs: number;
  sizeBytes: number;
  instruments: string;
  comment: string;
}

export interface FormatCount {
  format: string;
  count: number;
}

async function api<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE_URL}${path}`);
  if (!res.ok) throw new Error(`catalog ${path}: ${res.status}`);
  return res.json() as Promise<T>;
}

export const search = (q: string, limit = 60): Promise<ModuleHit[]> =>
  api<{ results: ModuleHit[] }>(`/search?q=${encodeURIComponent(q)}&limit=${limit}`).then(
    (r) => r.results,
  );

export const listModules = (opts: {
  format?: string;
  sort?: "latest" | "random" | "title";
  limit?: number;
  offset?: number;
}): Promise<ModuleHit[]> => {
  const p = new URLSearchParams();
  if (opts.format) p.set("format", opts.format);
  p.set("sort", opts.sort ?? "latest");
  p.set("limit", String(opts.limit ?? 100));
  p.set("offset", String(opts.offset ?? 0));
  return api<{ results: ModuleHit[] }>(`/modules?${p}`).then((r) => r.results);
};

export const getModule = (id: number): Promise<ModuleDetail> => api<ModuleDetail>(`/module/${id}`);

export const getFormats = (): Promise<{ formats: FormatCount[]; total: number }> =>
  api(`/formats`);
