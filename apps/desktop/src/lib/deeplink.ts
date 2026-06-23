// E2: custom-protocol / deep-link handling. The app registers the
// `trackerstream://` scheme (see src-tauri/tauri.conf.json + the deep-link
// plugin); this resolves an incoming share link and acts on it — e.g.
// `trackerstream://share/<code>` plays the shared module.
//
// Shares still also resolve via a pasted code (social.resolveShare); this just
// adds the OS-level link path: clicking a trackerstream:// link opens the app
// (launching it if needed) straight onto the shared content.
import { onOpenUrl, getCurrent } from "@tauri-apps/plugin-deep-link";
import { resolveShare } from "./social.svelte";
import { playModule } from "./player.svelte";

const SHARE_RE = /^trackerstream:\/\/share\/([\w-]+)/i;

/** Called for each open. Override hooks let the page also surface playlists. */
export interface DeepLinkHandlers {
  onPlaylist?: (refId: number, rootCid: string | null) => void;
}

async function handleUrl(url: string, h: DeepLinkHandlers): Promise<void> {
  const m = SHARE_RE.exec(url.trim());
  if (!m) return;
  try {
    const r = await resolveShare(m[1]);
    if (r.kind === "module" && r.resolved) {
      await playModule(r.resolved);
    } else if (r.kind === "playlist") {
      h.onPlaylist?.((r as unknown as { refId: number }).refId, r.rootCid);
    }
  } catch {
    /* invalid / expired share code — ignore (paste-code path still available) */
  }
}

/**
 * Register deep-link handling and process any cold-start launch URL (the link
 * that launched the app). No-op (caught) when the deep-link plugin isn't present
 * — e.g. a plain web/SSR preview — so this is safe to call unconditionally.
 */
export async function initDeepLinks(handlers: DeepLinkHandlers = {}): Promise<void> {
  try {
    const launched = await getCurrent();
    if (launched) for (const u of launched) await handleUrl(u, handlers);
    await onOpenUrl((urls) => {
      for (const u of urls) void handleUrl(u, handlers);
    });
  } catch {
    /* deep-link plugin unavailable in this runtime */
  }
}
