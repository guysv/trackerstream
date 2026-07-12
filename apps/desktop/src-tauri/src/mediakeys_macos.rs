//! macOS media-key + headphone/AirPods transport, via Apple's MediaPlayer framework.
//!
//! Why native (and not tauri-plugin-global-shortcut / navigator.mediaSession):
//!   - The hardware media keys and headset transport buttons are delivered as `NSSystemDefined`
//!     events, which Carbon global hotkeys (what the global-shortcut plugin registers) cannot
//!     grab — so that path silently does nothing on macOS.
//!   - Playback here is Web Audio with no `<audio>`/`<video>` element, so `navigator.mediaSession`
//!     never activates either.
//! The only mechanism that receives Play/Pause/Next/Prev from BOTH the keyboard and headphones is
//! to register `MPRemoteCommandCenter` handlers and publish `MPNowPlayingInfoCenter` — which also
//! gives us the system "Now Playing" widget for free.
//!
//! Each command handler just emits a Tauri event ([`EVENT`]) to the frontend, where all playback
//! state lives (src/lib/mediaKeys.ts routes it into the same debounced dispatcher as the
//! Win/Linux global-shortcut path).

use block2::RcBlock;
use objc2::rc::Retained;
use objc2::runtime::AnyObject;
use objc2_foundation::{NSDictionary, NSNumber, NSString};
use objc2_media_player::{
    MPMediaItemPropertyArtist, MPMediaItemPropertyTitle, MPNowPlayingInfoCenter,
    MPNowPlayingInfoPropertyPlaybackRate, MPNowPlayingPlaybackState, MPRemoteCommandCenter,
    MPRemoteCommandEvent, MPRemoteCommandHandlerStatus,
};
use std::ptr::NonNull;
use tauri::{AppHandle, Emitter};

/// Event emitted to the webview on each remote command. Payload is one of
/// `"playpause"`, `"next"`, `"prev"` — see `src/lib/mediaKeys.ts`.
pub const EVENT: &str = "media-remote-command";

/// Register the remote-command handlers and publish an initial Now Playing entry so the system
/// routes media keys + headset commands to us. Must run on the main thread (the Tauri `setup`
/// hook does).
pub fn init(app: &AppHandle) {
    // SAFETY: all calls are on the main thread (setup hook); the objc2 bindings are `unsafe`
    // only because they cross the FFI boundary. Each handler block is copied+retained by the
    // command (ObjC `addTargetWithHandler:` copies the block), so it outlives this function.
    unsafe {
        let center = MPRemoteCommandCenter::sharedCommandCenter();

        // One handler per command; play/pause/toggle all collapse to a single "playpause"
        // action (with playbackState reported below, the OS only sends "play" when paused and
        // "pause" when playing, so a toggle on the JS side always lands correctly).
        let handler = |app: AppHandle, action: &'static str| {
            RcBlock::new(move |_evt: NonNull<MPRemoteCommandEvent>| {
                let _ = app.emit(EVENT, action);
                MPRemoteCommandHandlerStatus::Success
            })
        };

        for (cmd, action) in [
            (center.togglePlayPauseCommand(), "playpause"),
            (center.playCommand(), "playpause"),
            (center.pauseCommand(), "playpause"),
            (center.nextTrackCommand(), "next"),
            (center.previousTrackCommand(), "prev"),
        ] {
            cmd.setEnabled(true);
            // The returned target token is only needed to later removeTarget:; the command keeps
            // its own reference to the handler, so we can drop it — the handler stays live.
            let _ = cmd.addTargetWithHandler(&handler(app.clone(), action));
        }
    }

    // Publish a placeholder so we register as the active Now Playing app from startup; the
    // frontend overwrites it with the real track on first play.
    update_now_playing(app, "trackerstream".into(), String::new(), false);
}

/// Push the current track + play state to the system Now Playing widget. Safe to call from any
/// thread — it hops to the main thread before touching the framework.
pub fn update_now_playing(app: &AppHandle, title: String, artist: String, playing: bool) {
    let _ = app.run_on_main_thread(move || {
        // SAFETY: on the main thread; all objects are created and released within this closure.
        unsafe {
            let center = MPNowPlayingInfoCenter::defaultCenter();
            let title_s = NSString::from_str(&title);
            let artist_s = NSString::from_str(if artist.is_empty() { "tracker module" } else { &artist });
            let rate = NSNumber::new_f64(if playing { 1.0 } else { 0.0 });

            let keys: [&NSString; 3] = [
                MPMediaItemPropertyTitle,
                MPMediaItemPropertyArtist,
                MPNowPlayingInfoPropertyPlaybackRate,
            ];
            let objects: [&AnyObject; 3] = [&*title_s, &*artist_s, &*rate];
            let info: Retained<NSDictionary<NSString, AnyObject>> =
                NSDictionary::from_slices(&keys, &objects);

            center.setNowPlayingInfo(Some(&info));
            center.setPlaybackState(if playing {
                MPNowPlayingPlaybackState::Playing
            } else {
                MPNowPlayingPlaybackState::Paused
            });
        }
    });
}
