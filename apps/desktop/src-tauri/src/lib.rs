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

/// In-flight progressive (streaming) fetches, keyed by root CID string.
#[derive(Default)]
struct Streams(Mutex<HashMap<String, Arc<Mutex<ipfs::StreamBuffer>>>>);

#[derive(Serialize, Clone)]
struct StreamProgress {
    version: u32,
    pct: f32,
    playable: bool,
    complete: bool,
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

/// Begin a progressive (streaming) fetch: returns immediately, then ticks
/// `on_progress` as the partial module buffer grows. The frontend pulls the
/// current bytes with `get_stream_buffer` (recreate-on-grow in the worklet).
#[tauri::command]
async fn start_stream(
    root: String,
    on_progress: Channel<StreamProgress>,
    state: State<'_, IpfsState>,
    streams: State<'_, Streams>,
) -> Result<(), String> {
    let cid: Cid = root.parse().map_err(|e| format!("bad CID {root}: {e}"))?;
    let ipfs = state.ipfs.clone();
    let buf = Arc::new(Mutex::new(ipfs::StreamBuffer::default()));
    streams.0.lock().unwrap().insert(root.clone(), buf.clone());

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ipfs::Progress>();
    tauri::async_runtime::spawn(async move {
        while let Some(p) = rx.recv().await {
            let _ = on_progress.send(StreamProgress {
                version: p.version,
                pct: p.pct,
                playable: p.playable,
                complete: p.complete,
            });
        }
    });
    tauri::async_runtime::spawn(async move {
        let _ = ipfs::stream_reassemble(&ipfs, cid, buf, tx).await;
    });
    Ok(())
}

/// Cold seek (B1): resolve a module to a partial buffer that is playable at
/// `order` (skeleton + only that seek target's resident samples), far smaller
/// than the whole DAG. The frontend loads these bytes and calls set_position;
/// normal streaming then fills the remainder. Falls back to a full fetch when
/// the module has no seek table.
#[tauri::command]
async fn seek_module(root: String, order: u32, state: State<'_, IpfsState>) -> Result<Response, String> {
    let cid: Cid = root.parse().map_err(|e| format!("bad CID {root}: {e}"))?;
    let ipfs = state.ipfs.clone();
    let bytes = ipfs::seek_module(&ipfs, cid, order)
        .await
        .map_err(|e| format!("seek_module {root}@{order} failed: {e}"))?;
    Ok(Response::new(bytes))
}

/// Current bytes of an in-flight (or finished) streaming fetch.
#[tauri::command]
async fn get_stream_buffer(root: String, streams: State<'_, Streams>) -> Result<Response, String> {
    let buf = {
        let map = streams.0.lock().unwrap();
        map.get(&root).cloned().ok_or_else(|| format!("no stream for {root}"))?
    };
    let data = buf.lock().unwrap().data.clone();
    Ok(Response::new(data))
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
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
            seek_module,
            get_stream_buffer
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
