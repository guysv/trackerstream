// Walk the offline Mod Archive corpus and yield each module's bytes with a stable
// `source` key for incremental ingest. Two zip layouts are accepted: the 2007
// snapshot's double-zipped buckets (outer prefix-zip -> per-module `name.ext.zip`
// -> module file) and the yearly-additions dumps (a per-module `name.ext.zip` with
// the module file directly inside, no inner zip). Streaming + sequential so memory
// stays bounded over the full ~60 GB / ~170k-module archive.
import { readdirSync } from "node:fs";
import { join, relative } from "node:path";
import yauzl from "yauzl";

export interface CorpusModule {
  source: string; // stable idempotency key: outerRel!innerName!moduleName
  name: string; // module filename
  bytes: Buffer;
}

function openZip(input: string | Buffer): Promise<yauzl.ZipFile> {
  return new Promise((res, rej) => {
    const cb = (e: Error | null, zf?: yauzl.ZipFile) => (e ? rej(e) : res(zf!));
    if (Buffer.isBuffer(input)) yauzl.fromBuffer(input, { lazyEntries: true }, cb);
    else yauzl.open(input, { lazyEntries: true }, cb);
  });
}

function readEntry(zf: yauzl.ZipFile, entry: yauzl.Entry): Promise<Buffer> {
  return new Promise((res, rej) => {
    zf.openReadStream(entry, (e, rs) => {
      if (e || !rs) return rej(e ?? new Error("no stream"));
      const chunks: Buffer[] = [];
      rs.on("data", (d) => chunks.push(d as Buffer));
      rs.on("end", () => res(Buffer.concat(chunks)));
      rs.on("error", rej);
    });
  });
}

function walkZip(zf: yauzl.ZipFile, onEntry: (e: yauzl.Entry) => Promise<void>): Promise<void> {
  return new Promise((resolve, reject) => {
    zf.on("entry", async (entry: yauzl.Entry) => {
      try {
        await onEntry(entry);
        zf.readEntry();
      } catch (e) {
        reject(e);
      }
    });
    zf.on("end", resolve);
    zf.on("error", reject);
    zf.readEntry();
  });
}

function listOuterZips(root: string): string[] {
  const out: string[] = [];
  (function rec(d: string) {
    for (const e of readdirSync(d, { withFileTypes: true })) {
      if (e.name.startsWith(".")) continue;
      const p = join(d, e.name);
      if (e.isDirectory()) rec(p);
      else if (e.name.toLowerCase().endsWith(".zip")) out.push(p);
    }
  })(root);
  return out.sort();
}

// Sidecar/metadata files that ride alongside a module inside its inner zip (the
// Mod Archive snapshot pairs each `name.ext` with a `name.ext.info` text file).
// These are not modules — skip them so they never enter the ingest/stream path.
const SIDECAR = /\.(info|txt|nfo|diz|readme|md|doc|jpg|jpeg|png|gif)$/i;

/** Corpus I/O timers for bake profiling (PROFILE=1). `listMs` is the ONE-TIME recursive tree
 *  scan (fixed cost, amortized over the whole corpus); `openMs`+`readMs` are the per-module
 *  zip open + entry decompress (scale with module count). Kept separate so extrapolation to the
 *  full corpus doesn't multiply the one-time scan. */
export const corpusStats = { listMs: 0, openMs: 0, readMs: 0 };

export interface WalkOpts {
  formats?: string[]; // lowercase extensions to include (default: all)
  limit?: number; // stop after N modules (0 = no limit)
  /** Subset re-bake: restrict the walk to exactly these `source` keys. Outer zips that
   *  contain no wanted source are skipped unopened; non-matching entries inside opened
   *  zips are skipped before their bytes are read. (Used for a targeted v4 re-bake.) */
  sources?: Set<string>;
}

/** Call `cb` for every module in the corpus, sequentially. Returns the count. */
export async function forEachModule(
  root: string,
  cb: (m: CorpusModule) => Promise<void>,
  opts: WalkOpts = {},
): Promise<number> {
  const formats = opts.formats?.map((f) => f.toLowerCase());
  const limit = opts.limit ?? 0;
  // Subset re-bake allowlist: the wanted `source` keys, plus the set of outer-zip rel
  // paths they live under (source = `${outerRel}!...`) so we skip opening zips entirely
  // when none of their modules are wanted.
  const sources = opts.sources;
  const neededOuter = sources ? new Set([...sources].map((s) => s.slice(0, s.indexOf("!")))) : null;
  // Ops shard knob: SHARD="i/N" processes only the outer zips whose path hashes to
  // shard i of N, letting N worker processes bake disjoint slices of the corpus into
  // the same catalog in parallel. Unset (or N<=1) => process everything, so the
  // normal timer-driven ingest is unaffected.
  const [shardIdx, shardN] = (() => {
    const m = /^(\d+)\/(\d+)$/.exec(process.env.SHARD ?? "");
    return m ? [Number(m[1]), Number(m[2])] : [0, 1];
  })();
  const inShard = (rel: string): boolean => {
    if (shardN <= 1) return true;
    let h = 2166136261 >>> 0; // FNV-1a (32-bit)
    for (let i = 0; i < rel.length; i++) {
      h ^= rel.charCodeAt(i);
      h = Math.imul(h, 16777619) >>> 0;
    }
    return h % shardN === shardIdx;
  };
  let count = 0;
  const _tl = Date.now();
  const outerZips = listOuterZips(root);
  corpusStats.listMs += Date.now() - _tl;
  for (const oz of outerZips) {
    const outerRel = relative(root, oz);
    if (!inShard(outerRel)) continue;
    if (neededOuter && !neededOuter.has(outerRel)) continue; // subset: no wanted module here
    let zf: yauzl.ZipFile;
    const _to = Date.now();
    try {
      zf = await openZip(oz);
    } catch {
      continue;
    }
    corpusStats.openMs += Date.now() - _to;
    await walkZip(zf, async (entry) => {
      const nm = entry.fileName;
      if (nm.endsWith("/")) return; // directory entry
      if (nm.toLowerCase().endsWith(".zip")) {
        // Two-level layout: outer prefix-zip -> inner `name.ext.zip` -> module
        // file (the 2007 snapshot's bucket zips + our local-upload batches).
        const ext = nm.slice(0, -4).split(".").pop()?.toLowerCase();
        if (formats && (!ext || !formats.includes(ext))) return;
        let _t = Date.now();
        const innerBuf = await readEntry(zf, entry);
        let izf: yauzl.ZipFile;
        try {
          izf = await openZip(innerBuf);
        } catch {
          return;
        }
        corpusStats.readMs += Date.now() - _t;
        await walkZip(izf, async (me) => {
          if (me.fileName.endsWith("/")) return;
          if (SIDECAR.test(me.fileName)) return; // skip .info etc. — not a module
          if (sources && !sources.has(`${outerRel}!${nm}!${me.fileName}`)) return; // subset gate
          _t = Date.now();
          const bytes = await readEntry(izf, me);
          corpusStats.readMs += Date.now() - _t;
          await cb({ source: `${outerRel}!${nm}!${me.fileName}`, name: me.fileName, bytes });
          count++;
        });
      } else {
        // Single-level layout: the outer zip *is* a per-module `name.ext.zip` and
        // holds the module file directly, with no inner zip (the yearly Mod Archive
        // additions dumps). Empty middle key segment keeps these from ever
        // colliding with a two-level `outerRel!innerZip!module` key.
        if (SIDECAR.test(nm)) return; // skip .info etc. — not a module
        const ext = nm.split(".").pop()?.toLowerCase();
        if (formats && (!ext || !formats.includes(ext))) return;
        if (sources && !sources.has(`${outerRel}!!${nm}`)) return; // subset gate
        const _t = Date.now();
        const bytes = await readEntry(zf, entry);
        corpusStats.readMs += Date.now() - _t;
        await cb({ source: `${outerRel}!!${nm}`, name: nm, bytes });
        count++;
      }
    });
    if (limit && count >= limit) break;
  }
  return count;
}
