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
//! # Sharing the Now Playing slot with other players
//!
//! macOS routes the media keys to whichever app most recently published a Now Playing entry in a
//! *playing* state, and an app becomes a candidate the moment it enables a remote command. That
//! arbitration only works if we take part honestly:
//!
//!   - handlers are registered at startup but every command is left DISABLED, and no Now Playing
//!     entry is published, until the user actually plays something. A freshly-launched
//!     trackerstream is therefore invisible to Control Center and does not take the media keys
//!     away from whatever the user is really listening to;
//!   - while a track is loaded we report Playing/Paused truthfully, so the moment another app
//!     starts playing, the system hands the keys over to it;
//!   - [`clear_now_playing`] withdraws the entry and disables the commands again, handing the slot
//!     back. We call it on quit rather than leaving a ghost entry holding onto it;
//!   - next/prev are enabled only when they can actually do something, so the OS greys the buttons
//!     out instead of sending us commands we would drop on the floor.
//!
//! Each command handler just emits a Tauri event ([`EVENT`]) to the frontend, where all playback
//! state lives (src/lib/mediaKeys.ts routes it into the same dispatcher as the Win/Linux
//! global-shortcut path).

use block2::RcBlock;
use objc2::rc::Retained;
use objc2::runtime::AnyObject;
use objc2_foundation::{NSDictionary, NSNumber, NSString};
use objc2_media_player::{
    MPMediaItemPropertyArtist, MPMediaItemPropertyPlaybackDuration, MPMediaItemPropertyTitle,
    MPNowPlayingInfoCenter, MPNowPlayingInfoPropertyElapsedPlaybackTime,
    MPNowPlayingInfoPropertyPlaybackRate, MPNowPlayingPlaybackState, MPRemoteCommandCenter,
    MPRemoteCommandEvent, MPRemoteCommandHandlerStatus,
};
use std::ptr::NonNull;
use tauri::{AppHandle, Emitter};

/// Event emitted to the webview on each remote command. Payload is one of `"play"`, `"pause"`,
/// `"playpause"`, `"next"`, `"prev"` — see `src/lib/mediaKeys.ts`.
///
/// `play` and `pause` stay distinct from the `playpause` toggle on purpose. The system tells us
/// which one it wants; collapsing all three into a toggle inverts the command whenever the OS's
/// idea of our play state has drifted from ours (a `play` arriving while we already believe we're
/// playing would *pause* us). The frontend applies each one idempotently instead.
pub const EVENT: &str = "media-remote-command";

/// Register the remote-command handlers — disabled, and publishing nothing, until a track is
/// actually loaded (see [`update_now_playing`]). Must run on the main thread (the Tauri `setup`
/// hook does).
pub fn init(app: &AppHandle) {
    // SAFETY: all calls are on the main thread (setup hook); the objc2 bindings are `unsafe`
    // only because they cross the FFI boundary. Each handler block is copied+retained by the
    // command (ObjC `addTargetWithHandler:` copies the block), so it outlives this function.
    unsafe {
        let center = MPRemoteCommandCenter::sharedCommandCenter();

        let handler = |app: AppHandle, action: &'static str| {
            RcBlock::new(move |_evt: NonNull<MPRemoteCommandEvent>| {
                let _ = app.emit(EVENT, action);
                MPRemoteCommandHandlerStatus::Success
            })
        };

        for (cmd, action) in [
            (center.togglePlayPauseCommand(), "playpause"),
            (center.playCommand(), "play"),
            (center.pauseCommand(), "pause"),
            (center.nextTrackCommand(), "next"),
            (center.previousTrackCommand(), "prev"),
        ] {
            // Enabling a command is what puts us in the system's media rotation, so enabling these
            // at startup is precisely how an idle app ends up stealing the keys from the one the
            // user is actually listening to. Stay out of it until we have something to control.
            cmd.setEnabled(false);
            // The returned target token is only needed to later removeTarget:; the command keeps
            // its own reference to the handler, so we can drop it — the handler stays live.
            let _ = cmd.addTargetWithHandler(&handler(app.clone(), action));
        }
    }
}

/// Publish the current track + play state to the system Now Playing widget, and enable exactly
/// those commands that can currently do something. This is also what makes the OS route media keys
/// and headphone buttons to us, so it must fire on every track change and play/pause.
///
/// `duration`/`elapsed` are seconds. The OS drives the scrubber by extrapolating from `elapsed`
/// and the playback rate we hand it, so re-publishing on discrete changes (track, play/pause,
/// seek) is enough — no per-frame updates needed.
///
/// Safe to call from any thread — it hops to the main thread before touching the framework.
#[allow(clippy::too_many_arguments)]
pub fn update_now_playing(
    app: &AppHandle,
    title: String,
    artist: String,
    playing: bool,
    duration: f64,
    elapsed: f64,
    has_next: bool,
    has_prev: bool,
) {
    let _ = app.run_on_main_thread(move || {
        // SAFETY: on the main thread; all objects are created and released within this closure.
        unsafe {
            let commands = MPRemoteCommandCenter::sharedCommandCenter();
            commands.togglePlayPauseCommand().setEnabled(true);
            commands.playCommand().setEnabled(true);
            commands.pauseCommand().setEnabled(true);
            // Grey the buttons out rather than accept a command we would silently drop.
            commands.nextTrackCommand().setEnabled(has_next);
            commands.previousTrackCommand().setEnabled(has_prev);

            let center = MPNowPlayingInfoCenter::defaultCenter();
            let title_s = NSString::from_str(&title);
            let artist_s =
                NSString::from_str(if artist.is_empty() { "tracker module" } else { &artist });
            let rate = NSNumber::new_f64(if playing { 1.0 } else { 0.0 });
            let duration_n = NSNumber::new_f64(duration.max(0.0));
            let elapsed_n = NSNumber::new_f64(elapsed.max(0.0));

            let keys: [&NSString; 5] = [
                MPMediaItemPropertyTitle,
                MPMediaItemPropertyArtist,
                MPMediaItemPropertyPlaybackDuration,
                MPNowPlayingInfoPropertyElapsedPlaybackTime,
                MPNowPlayingInfoPropertyPlaybackRate,
            ];
            let objects: [&AnyObject; 5] =
                [&*title_s, &*artist_s, &*duration_n, &*elapsed_n, &*rate];
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

/// Hand the Now Playing slot back: withdraw our entry, report Stopped, and disable every remote
/// command, so the system stops routing media keys to us and Control Center falls back to whatever
/// else is playing. Called while nothing is loaded, and on quit.
///
/// Safe to call from any thread — it hops to the main thread before touching the framework.
pub fn clear_now_playing(app: &AppHandle) {
    let _ = app.run_on_main_thread(clear_on_main_thread);
}

/// [`clear_now_playing`] for callers already on the main thread. Used at exit, where the event
/// loop is winding down and a `run_on_main_thread` hop would never be delivered.
pub fn clear_now_playing_blocking() {
    clear_on_main_thread();
}

fn clear_on_main_thread() {
    // SAFETY: on the main thread; all objects are created and released within this function.
    unsafe {
        let commands = MPRemoteCommandCenter::sharedCommandCenter();
        for cmd in [
            commands.togglePlayPauseCommand(),
            commands.playCommand(),
            commands.pauseCommand(),
            commands.nextTrackCommand(),
            commands.previousTrackCommand(),
        ] {
            cmd.setEnabled(false);
        }

        let center = MPNowPlayingInfoCenter::defaultCenter();
        center.setNowPlayingInfo(None);
        center.setPlaybackState(MPNowPlayingPlaybackState::Stopped);
    }
}
