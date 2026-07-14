// A read-only wa-sqlite VFS whose pages come from Bitswap.
//
// This is the JS counterpart of catalog.rs's IpfsVfs — and it is *simpler* than the Rust, because
// wa-sqlite's VFS may be genuinely async. The Rust has to bridge SQLite's synchronous read_exact_at
// onto an async fetch with spawn_blocking + block_on; here jRead can just await. That bridge was
// the ugliest part of the Rust and it simply does not exist in this port.
//
// NOTE the wa-sqlite build matters: stock wa-sqlite ships WITHOUT FTS5, and the catalog is nothing
// without it. We use @journeyapps/wa-sqlite (PowerSync's fork, same FacadeVFS API), whose build
// includes it.
import { FacadeVFS } from "@journeyapps/wa-sqlite/src/FacadeVFS.js";
import * as SQLite from "@journeyapps/wa-sqlite";
import { CancelledError, PageScheduler } from "./scheduler.ts";
import type { TszcatManifest } from "./tszcat.ts";

export class CatalogVFS extends FacadeVFS {
  private man!: TszcatManifest;
  private sched!: PageScheduler;

  static async create(name: string, module: unknown, man: TszcatManifest, sched: PageScheduler) {
    const vfs = new CatalogVFS(name, module);
    vfs.man = man;
    vfs.sched = sched;
    await vfs.isReady();
    return vfs;
  }

  private get size(): number {
    return this.man.pageSize * this.man.pageCount;
  }

  jOpen(_filename: string | null, _fileId: number, _flags: number, pOutFlags: DataView): number {
    pOutFlags.setInt32(0, 0, true);
    return SQLite.SQLITE_OK;
  }

  jClose(_fileId: number): number {
    return SQLite.SQLITE_OK;
  }

  /** Map a byte range onto TSZCAT pages and hand SQLite the bytes. Everything interesting (which
   *  pages to *also* fetch, how many at once, and when to give up because the user typed again)
   *  lives in the scheduler. */
  async jRead(_fileId: number, pData: Uint8Array, iOffset: number): Promise<number> {
    const { pageSize } = this.man;
    const end = iOffset + pData.length;
    if (iOffset >= this.size) {
      pData.fill(0);
      return SQLite.SQLITE_IOERR_SHORT_READ;
    }
    const first = Math.floor(iOffset / pageSize);
    const last = Math.floor((Math.min(end, this.size) - 1) / pageSize);
    try {
      await this.sched.ensure(first, last);
    } catch (e) {
      // A superseded query is not an I/O failure; it is the point. SQLITE_IOERR unwinds the
      // statement cleanly and the caller discards the results.
      if (e instanceof CancelledError) return SQLite.SQLITE_IOERR;
      throw e;
    }

    let written = 0;
    while (written < pData.length) {
      const abs = iOffset + written;
      if (abs >= this.size) break;
      const idx = Math.floor(abs / pageSize);
      const within = abs % pageSize;
      const page = this.sched.page(idx);
      const n = Math.min(pData.length - written, pageSize - within);
      pData.set(page.subarray(within, within + n), written);
      written += n;
    }
    if (written < pData.length) {
      pData.fill(0, written); // SQLite requires the tail be zeroed on a short read
      return SQLite.SQLITE_IOERR_SHORT_READ;
    }
    return SQLite.SQLITE_OK;
  }

  jFileSize(_fileId: number, pSize: DataView): number {
    pSize.setBigInt64(0, BigInt(this.size), true);
    return SQLite.SQLITE_OK;
  }

  // Content-addressed and read-only: locking and journals are meaningless here.
  jLock(_fileId: number, _lock: number): number {
    return SQLite.SQLITE_OK;
  }
  jUnlock(_fileId: number, _lock: number): number {
    return SQLite.SQLITE_OK;
  }
  jAccess(_name: string, _flags: number, pResOut: DataView): number {
    // The published snapshot is VACUUM INTO'd, so it is in rollback-journal (DELETE) mode with no
    // sidecar. Answering "no" to -journal/-wal probes is what stops SQLite hunting for files that
    // the single published root does not carry.
    pResOut.setInt32(0, 0, true);
    return SQLite.SQLITE_OK;
  }
  jSectorSize(_fileId: number): number {
    return this.man.pageSize;
  }
  jDeviceCharacteristics(_fileId: number): number {
    return 0x00002000; // SQLITE_IOCAP_IMMUTABLE
  }
}
