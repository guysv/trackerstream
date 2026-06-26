// E2: custom-protocol / deep-link plumbing. The app registers the
// `trackerstream://` scheme (see src-tauri/tauri.conf.json + the deep-link
// plugin); this wires up the OS-level link path so clicking a trackerstream://
// link opens the app (launching it if needed).
//
// The share-code handler that used to live here was removed with the central
// social/playlist plane. The scheme + listener are kept intentionally: the P2P
// rebuild will resolve trackerstream:// links onto IPNS-named playlists/modules.
import { onOpenUrl, getCurrent } from "@tauri-apps/plugin-deep-link";

/** Called for each incoming trackerstream:// URL. */
export type DeepLinkHandler = (url: string) => void | Promise<void>;

/**
 * Register deep-link handling and process any cold-start launch URL (the link
 * that launched the app). No-op (caught) when the deep-link plugin isn't present
 * — e.g. a plain web/SSR preview — so this is safe to call unconditionally.
 */
export async function initDeepLinks(onUrl: DeepLinkHandler = () => {}): Promise<void> {
  try {
    const launched = await getCurrent();
    if (launched) for (const u of launched) await onUrl(u);
    await onOpenUrl((urls) => {
      for (const u of urls) void onUrl(u);
    });
  } catch {
    /* deep-link plugin unavailable in this runtime */
  }
}
