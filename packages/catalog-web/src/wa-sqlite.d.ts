// @journeyapps/wa-sqlite ships its VFS base classes as plain JS under src/, with no declarations.
// They are consumed by subclassing (see vfs.ts), so a structural stub is all TypeScript needs.
declare module "@journeyapps/wa-sqlite/src/FacadeVFS.js" {
  export class FacadeVFS {
    constructor(name: string, module: unknown);
    readonly name: string;
    isReady(): Promise<void>;
  }
}
declare module "@journeyapps/wa-sqlite/src/examples/IDBBatchAtomicVFS.js" {
  export class IDBBatchAtomicVFS {
    static create(name: string, module: unknown): Promise<IDBBatchAtomicVFS>;
    close(): Promise<void>;
  }
}
declare module "@journeyapps/wa-sqlite/dist/wa-sqlite-async.mjs" {
  const factory: (opts?: { wasmBinary?: BufferSource }) => Promise<unknown>;
  export default factory;
}
