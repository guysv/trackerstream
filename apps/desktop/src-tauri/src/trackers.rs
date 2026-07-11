//! External-tracker detection for the "Open with" menu. We probe the system (filesystem only —
//! never spawning a process) for each tracker we know how to launch, and hand the UI just the
//! ones actually installed. Each is resolved to a per-platform launch `target` that
//! `tauri_plugin_opener::open_path`'s `with` argument understands (see `download_and_open`):
//!   - macOS: the `.app` bundle path (or a CLI binary on $PATH) — passed to `open <file> -a <target>`.
//!   - Linux: the executable path on $PATH — run directly as `<target> <file>`.
//!   - Windows: the full `.exe` path — ShellExecute's `lpFile` won't resolve a bare tracker name,
//!     and trackers rarely register an App Paths key or land on %PATH%.
//! Detection is deliberately conservative: a tracker only appears if we can point at a real file.

use serde::Serialize;
use std::path::{Path, PathBuf};

/// A tracker we know how to detect + launch. Static registry in `TRACKERS`. Some fields are only
/// read on one platform (`mac_apps`/`win_paths`), hence the blanket allow.
#[allow(dead_code)]
struct TrackerDef {
    /// Stable id (also the historical `open_with` value); handy in logs.
    id: &'static str,
    /// Menu label.
    label: &'static str,
    /// Executable basenames to look for on $PATH, most-preferred first. On Windows we also try
    /// each with a `.exe` suffix. (macOS Homebrew installs land here too.)
    bins: &'static [&'static str],
    /// macOS `.app` bundle names to look for under /Applications and ~/Applications, preferred first.
    mac_apps: &'static [&'static str],
    /// Windows install-relative `.exe` paths under the Program Files roots / LocalAppData\Programs,
    /// preferred first — covers apps that neither register an App Paths key nor land on %PATH%.
    win_paths: &'static [&'static str],
}

const TRACKERS: &[TrackerDef] = &[
    TrackerDef {
        id: "schismtracker",
        label: "SchismTracker",
        bins: &["schismtracker"],
        mac_apps: &["Schism Tracker", "SchismTracker"],
        win_paths: &["Schism Tracker\\schismtracker.exe", "schismtracker\\schismtracker.exe"],
    },
    TrackerDef {
        id: "milkytracker",
        label: "MilkyTracker",
        bins: &["milkytracker"],
        mac_apps: &["MilkyTracker"],
        win_paths: &["MilkyTracker\\milkytracker.exe"],
    },
    TrackerDef {
        id: "openmpt",
        label: "OpenMPT",
        // Both binaries ARE OpenMPT; prefer the modern `openmpt`, fall back to the legacy `mptrack`.
        bins: &["openmpt", "mptrack"],
        mac_apps: &[], // no native macOS build (Windows-only / Wine)
        win_paths: &["OpenMPT\\OpenMPT.exe", "OpenMPT\\mptrack.exe"],
    },
];

/// One installed tracker handed to the UI. `target` is the resolved `open_with` launch target.
#[derive(Serialize)]
pub struct TrackerInfo {
    pub id: String,
    pub label: String,
    pub target: String,
}

/// Detect installed trackers (filesystem probe only, no spawn). Returns the subset the UI should
/// offer, each with a resolved launch `target`, in registry order.
pub fn installed() -> Vec<TrackerInfo> {
    TRACKERS
        .iter()
        .filter_map(|def| {
            resolve(def).map(|target| TrackerInfo {
                id: def.id.to_string(),
                label: def.label.to_string(),
                target,
            })
        })
        .collect()
}

/// Probe $PATH for `name` (exact basename), returning the first executable match.
fn find_on_path(name: &str) -> Option<PathBuf> {
    let paths = std::env::var_os("PATH")?;
    std::env::split_paths(&paths)
        .map(|dir| dir.join(name))
        .find(|p| is_executable_file(p))
}

#[cfg(unix)]
fn is_executable_file(p: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(p)
        .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn is_executable_file(p: &Path) -> bool {
    p.is_file()
}

#[cfg(target_os = "macos")]
fn resolve(def: &TrackerDef) -> Option<String> {
    // Prefer a real .app bundle (the double-clickable install); fall back to a CLI binary on $PATH
    // (e.g. `brew install schismtracker`).
    let mut app_dirs = vec![PathBuf::from("/Applications")];
    if let Some(home) = std::env::var_os("HOME") {
        app_dirs.push(Path::new(&home).join("Applications"));
    }
    for name in def.mac_apps {
        for dir in &app_dirs {
            let app = dir.join(format!("{name}.app"));
            if app.is_dir() {
                return Some(app.to_string_lossy().into_owned());
            }
        }
    }
    def.bins
        .iter()
        .find_map(|bin| find_on_path(bin))
        .map(|p| p.to_string_lossy().into_owned())
}

#[cfg(target_os = "windows")]
fn resolve(def: &TrackerDef) -> Option<String> {
    // Program Files roots (native + 32-bit + explicit 64-bit) plus the per-user install location.
    let roots: Vec<PathBuf> = ["ProgramFiles", "ProgramFiles(x86)", "ProgramW6432"]
        .iter()
        .filter_map(|k| std::env::var_os(k).map(PathBuf::from))
        .chain(std::env::var_os("LOCALAPPDATA").map(|l| PathBuf::from(l).join("Programs")))
        .collect();
    for rel in def.win_paths {
        for root in &roots {
            let exe = root.join(rel);
            if exe.is_file() {
                return Some(exe.to_string_lossy().into_owned());
            }
        }
    }
    // Last resort: a bare exe on %PATH%.
    def.bins
        .iter()
        .find_map(|bin| find_on_path(&format!("{bin}.exe")))
        .map(|p| p.to_string_lossy().into_owned())
}

#[cfg(all(unix, not(target_os = "macos")))]
fn resolve(def: &TrackerDef) -> Option<String> {
    def.bins
        .iter()
        .find_map(|bin| find_on_path(bin))
        .map(|p| p.to_string_lossy().into_owned())
}
