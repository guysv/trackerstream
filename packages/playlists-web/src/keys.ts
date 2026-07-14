// Playlist identities.
//
// A playlist's NAME is its key's PeerId. The key IS the identity: lose it and that playlist can
// never be updated, republished, or tombstoned by anyone, ever. On desktop the Go keystore owns
// these; a browser has only evictable storage, so this file carries the mitigations.
import { generateKeyPair, privateKeyFromProtobuf, privateKeyToProtobuf } from "@libp2p/crypto/keys";
import { peerIdFromPrivateKey } from "@libp2p/peer-id";
import { get as idbGet, set as idbSet, del as idbDel } from "idb-keyval";

/** The key type as @libp2p/crypto actually produces it.
 *
 *  Deliberately inferred, not imported: the libp2p ecosystem is mid-migration and @libp2p/crypto is
 *  built against @libp2p/interface@3 while libp2p@2's own types use interface@2. Both copies are in
 *  the tree. The shapes are identical — it is a nominal clash, not a runtime one — so we infer from
 *  the function we call and cast only where the two worlds meet. */
export type PlaylistKey = Awaited<ReturnType<typeof generateKeyPair>>;

const KEY_PREFIX = "ts:playlist-key:";

/** Ask the browser to make our storage persistent BEFORE the first playlist exists.
 *
 *  Without this, the browser may evict IndexedDB under storage pressure — and every playlist key the
 *  user owns is gone, irrecoverably. This is the single most destructive failure mode in the web
 *  client, and it is silent. Call it before create(). */
export async function requestPersistence(): Promise<boolean> {
  if (!navigator.storage?.persist) return false;
  if (await navigator.storage.persisted?.()) return true;
  return navigator.storage.persist();
}

/** Mint a new playlist identity. */
export async function createKey(): Promise<{ name: string; key: PlaylistKey }> {
  const key = await generateKeyPair("Ed25519");
  const name = peerIdFromPrivateKey(key as never).toString();
  // Stored in the SAME protobuf encoding node/keystore.go writes — which is what makes the
  // export/import below double as desktop<->web playlist portability.
  await idbSet(KEY_PREFIX + name, privateKeyToProtobuf(key));
  return { name, key };
}

export async function loadKey(name: string): Promise<PlaylistKey | null> {
  const raw = await idbGet<Uint8Array>(KEY_PREFIX + name);
  return raw ? (privateKeyFromProtobuf(raw) as PlaylistKey) : null;
}

export async function deleteKey(name: string): Promise<void> {
  await idbDel(KEY_PREFIX + name);
}

/** Export every playlist key as a portable bundle.
 *
 *  Not a nicety: browser storage is evictable, and this is the ONLY recovery path. Because the
 *  encoding matches the Go keystore's, the same bundle also moves a playlist between the web client
 *  and the desktop — which is the honest answer to "I made this on my laptop, why can't I edit it
 *  here?". Asserted by node/ipns_jsinterop_test.go: a JS-exported key unmarshals in Go to the SAME
 *  PeerId, i.e. the same playlist. */
export async function exportKeys(names: string[]): Promise<string> {
  const keys: Record<string, string> = {};
  for (const n of names) {
    const raw = await idbGet<Uint8Array>(KEY_PREFIX + n);
    if (raw) keys[n] = btoa(String.fromCharCode(...raw));
  }
  return JSON.stringify({ v: 1, keys }, null, 2);
}

/** Import a bundle. Returns the names now owned. */
export async function importKeys(bundle: string): Promise<string[]> {
  const parsed = JSON.parse(bundle) as { v: number; keys: Record<string, string> };
  if (parsed.v !== 1) throw new Error(`unknown key bundle version ${parsed.v}`);
  const names: string[] = [];
  for (const [name, b64] of Object.entries(parsed.keys)) {
    const raw = Uint8Array.from(atob(b64), (c) => c.charCodeAt(0));
    // Verify the key really is this name before trusting the label — a mislabelled bundle would
    // otherwise sign records under a name we don't own, which every peer would then reject.
    const key = privateKeyFromProtobuf(raw);
    const derived = peerIdFromPrivateKey(key as never).toString();
    if (derived !== name) throw new Error(`key bundle: ${name} does not match its key (${derived})`);
    await idbSet(KEY_PREFIX + name, raw);
    names.push(name);
  }
  return names;
}
