// Walk the offline Mod Archive corpus (double-zipped: outer prefix-zip ->
// per-module `name.ext.zip` -> module file) and yield each module's bytes with a
// stable `source` key for incremental ingest. Streaming + sequential so memory
// stays bounded over the full 52 GB / ~122k-module archive.
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

export interface WalkOpts {
  formats?: string[]; // lowercase extensions to include (default: all)
  limit?: number; // stop after N modules (0 = no limit)
}

/** Call `cb` for every module in the corpus, sequentially. Returns the count. */
export async function forEachModule(
  root: string,
  cb: (m: CorpusModule) => Promise<void>,
  opts: WalkOpts = {},
): Promise<number> {
  const formats = opts.formats?.map((f) => f.toLowerCase());
  const limit = opts.limit ?? 0;
  let count = 0;
  for (const oz of listOuterZips(root)) {
    const outerRel = relative(root, oz);
    let zf: yauzl.ZipFile;
    try {
      zf = await openZip(oz);
    } catch {
      continue;
    }
    await walkZip(zf, async (entry) => {
      const nm = entry.fileName;
      if (!nm.toLowerCase().endsWith(".zip")) return; // inner must be a module zip
      const ext = nm.slice(0, -4).split(".").pop()?.toLowerCase();
      if (formats && (!ext || !formats.includes(ext))) return;
      const innerBuf = await readEntry(zf, entry);
      let izf: yauzl.ZipFile;
      try {
        izf = await openZip(innerBuf);
      } catch {
        return;
      }
      await walkZip(izf, async (me) => {
        if (me.fileName.endsWith("/")) return;
        if (SIDECAR.test(me.fileName)) return; // skip .info etc. — not a module
        const bytes = await readEntry(izf, me);
        await cb({ source: `${outerRel}!${nm}!${me.fileName}`, name: me.fileName, bytes });
        count++;
      });
    });
    if (limit && count >= limit) break;
  }
  return count;
}
