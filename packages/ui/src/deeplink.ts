// E2: custom-protocol / deep-link plumbing. The app registers the
// `trackerstream://` scheme (see src-tauri/tauri.conf.json + the deep-link
// plugin); this wires up the OS-level link path so clicking a trackerstream://
// link opens the app (launching it if needed).
//
// The share-code handler that used to live here was removed with the central
// social/playlist plane. The scheme + listener are kept intentionally: the P2P
// rebuild will resolve trackerstream:// links onto IPNS-named playlists/modules.
import { maybeClient } from "./client/index.ts";

/** Called for each incoming trackerstream:// URL. */
export type DeepLinkHandler = (url: string) => void | Promise<void>;

/**
 * Register deep-link handling and process any cold-start launch URL (the link that launched the
 * app). The shell decides what a "deep link" IS: the desktop registers the OS `trackerstream://`
 * scheme; the web shell feeds it the /p/<name> route it was loaded on. Safe to call
 * unconditionally — a shell with no deep links installs nothing.
 */
export async function initDeepLinks(onUrl: DeepLinkHandler = () => {}): Promise<void> {
  maybeClient()?.platform.onDeepLink((u) => void onUrl(u));
}
