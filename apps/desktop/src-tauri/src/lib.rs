//! trackerstream desktop backend. Hosts the in-process IPFS data-plane node
//! (see `ipfs`) and exposes it to the Svelte frontend as Tauri commands. The
//! frontend never fetches module files over HTTP — it asks the embedded node to
//! resolve a root CID and gets back the reassembled module bytes to play.

pub mod ipfs;

use cid::Cid;
use rust_ipfs::Ipfs;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tauri::ipc::{Channel, Response};
use tauri::{Manager, State};

/// Embedded node handle (started once at setup; `Ipfs` is a cheap clonable,
/// thread-safe handle, unlike the non-Send builder it came from).
struct IpfsState {
    ipfs: Ipfs,
    peer_id: String,
}

/// In-flight (or finished) v2 streams, keyed by root CID string. Each holds the
/// assembled skeleton + decoded sample PCM as it arrives + the live playhead.
#[derive(Default)]
struct Streams(Mutex<HashMap<String, Arc<ipfs::StreamState>>>);

fn stream_for(streams: &State<'_, Streams>, root: &str) -> Result<Arc<ipfs::StreamState>, String> {
    streams
        .0
        .lock()
        .unwrap()
        .get(root)
        .cloned()
        .ok_or_else(|| format!("no stream for {root}"))
}

#[derive(Serialize)]
struct NodeInfo {
    peer_id: String,
    listening: Vec<String>,
}

#[tauri::command]
async fn node_info(state: State<'_, IpfsState>) -> Result<NodeInfo, String> {
    let listening = state
        .ipfs
        .listening_addresses()
        .await
        .map(|addrs| addrs.iter().map(|a| a.to_string()).collect())
        .unwrap_or_default();
    Ok(NodeInfo {
        peer_id: state.peer_id.clone(),
        listening,
    })
}

#[tauri::command]
async fn connect_peer(addr: String, state: State<'_, IpfsState>) -> Result<(), String> {
    let ipfs = state.ipfs.clone();
    ipfs::connect(&ipfs, &addr)
        .await
        .map_err(|e| format!("connect {addr} failed: {e}"))
}

/// Resolve a module root CID to its exact bytes, sourced 100% from CID blocks
/// over libp2p. Returns raw bytes (Tauri delivers them to JS as an ArrayBuffer).
#[tauri::command]
async fn fetch_module(root: String, state: State<'_, IpfsState>) -> Result<Response, String> {
    let cid: Cid = root.parse().map_err(|e| format!("bad CID {root}: {e}"))?;
    let ipfs = state.ipfs.clone();
    let bytes = ipfs::reassemble(&ipfs, cid)
        .await
        .map_err(|e| format!("fetch_module {root} failed: {e}"))?;
    Ok(Response::new(bytes))
}

/// Begin a v2 stream: returns immediately, then ticks `on_event` with control
/// events (Skeleton{plan}, Sample{index,frames}, Complete). The frontend pulls
/// the binary skeleton / sample PCM via get_skeleton / get_sample and feeds them
/// to the immortal-instance worklet (init + provideSample).
#[tauri::command]
async fn start_stream(
    root: String,
    on_event: Channel<ipfs::StreamEvent>,
    state: State<'_, IpfsState>,
    streams: State<'_, Streams>,
) -> Result<(), String> {
    let cid: Cid = root.parse().map_err(|e| format!("bad CID {root}: {e}"))?;
    let ipfs = state.ipfs.clone();
    let st = Arc::new(ipfs::StreamState::default());
    streams.0.lock().unwrap().insert(root.clone(), st.clone());

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ipfs::StreamEvent>();
    tauri::async_runtime::spawn(async move {
        while let Some(ev) = rx.recv().await {
            let _ = on_event.send(ev);
        }
    });
    let etx = tx.clone();
    tauri::async_runtime::spawn(async move {
        if let Err(e) = ipfs::stream_v2(&ipfs, cid, st, tx).await {
            let _ = etx.send(ipfs::StreamEvent::Error { message: e.to_string() });
        }
    });
    Ok(())
}

/// The assembled skeleton bytes for a stream (delivered to JS as an ArrayBuffer,
/// then init'd as the immortal instance). Ready once the Skeleton event fired.
#[tauri::command]
async fn get_skeleton(root: String, streams: State<'_, Streams>) -> Result<Response, String> {
    let st = stream_for(&streams, &root)?;
    let data = st.skeleton.lock().unwrap().clone();
    Ok(Response::new(data))
}

/// One streamed sample's decoded PCM bytes. Ready once its Sample event fired.
#[tauri::command]
async fn get_sample(root: String, index: u32, streams: State<'_, Streams>) -> Result<Response, String> {
    let st = stream_for(&streams, &root)?;
    let data = st
        .samples
        .lock()
        .unwrap()
        .get(&index)
        .cloned()
        .ok_or_else(|| format!("sample {index} not ready for {root}"))?;
    Ok(Response::new(data))
}

/// Frontend debug bridge: the webview console isn't visible in the dev terminal,
/// so the UI streaming-state tracer (lib/debug.ts) forwards transitions here and
/// we print them to stderr alongside the RUST_LOG tracing. Gated on the frontend
/// (DEBUG flag) so it's a no-op in normal runs.
#[tauri::command]
fn debug_log(line: String) {
    eprintln!("[UIDBG] {line}");
}

/// Update the live playhead order so the prefetch scheduler reprioritizes around
/// it (closed-loop; also how a seek reseeds the fetch queue).
#[tauri::command]
fn set_playhead(root: String, order: u32, streams: State<'_, Streams>) -> Result<(), String> {
    let st = stream_for(&streams, &root)?;
    st.playhead.store(order, std::sync::atomic::Ordering::Relaxed);
    Ok(())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    // RUST_LOG-driven tracing (rust-ipfs/libp2p/bitswap + our own spans). No-op
    // unless RUST_LOG is set, so release runs stay quiet.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        // Deep links: trackerstream://share/<code> (E2). The frontend handles the
        // URL via @tauri-apps/plugin-deep-link's onOpenUrl/getCurrent.
        .plugin(tauri_plugin_deep_link::init())
        .setup(|app| {
            // On Linux/Windows the scheme must be registered at runtime (macOS
            // registers it from the bundle Info.plist). Harmless if already set.
            #[cfg(any(target_os = "linux", windows))]
            {
                use tauri_plugin_deep_link::DeepLinkExt;
                let _ = app.deep_link().register_all();
            }
            // Persistent blockstore under the app data dir = the client CID cache.
            let dir = app.path().app_data_dir().ok().map(|d| d.join("ipfs"));
            // Start the (non-Send) builder once here, off the command path.
            let node =
                tauri::async_runtime::block_on(ipfs::start(dir)).map_err(|e| e.to_string())?;
            app.manage(IpfsState {
                ipfs: node.ipfs,
                peer_id: node.peer_id,
            });
            app.manage(Streams::default());
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            node_info,
            connect_peer,
            fetch_module,
            start_stream,
            get_skeleton,
            get_sample,
            set_playhead,
            debug_log
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
