//! Sanitizes a catalog-supplied filename into something safe to write into the user's Downloads
//! dir and hand to an external tracker.
//!
//! The threat: `modules.filename` is ingested VERBATIM from the archive (`ingest.ts`: `filename:
//! m.name`), and the bytes we write under it are whatever the DAG holds. A catalog entry named
//! `song.exe` carrying a PE payload would otherwise land as a runnable executable in Downloads —
//! and the OS-default launch path (`open_with = None`) dispatches on extension, so Windows would
//! execute it. Everything here is therefore DENY-BY-DEFAULT: a name only keeps its extension if
//! that extension is a known module format, otherwise a safe one is appended.
//!
//! Measured against the 2026-07-12 prod catalog (170,049 rows): every filename already ends in one
//! of mod/xm/it/s3m/mptm/mo3, so this rewrites nothing that exists today (1 leading-dash name, 0
//! colons, 0 separators). It is a guard against a future re-bake, a different source, or a
//! hostile catalog — not a fix for a live exploit.

/// Extensions we will let a downloaded module keep. The formats libopenmpt actually loads (a
/// superset of the six in the current corpus). Anything outside this set gets [`FALLBACK_EXT`]
/// appended rather than trusted — that is the whole point of the allowlist.
const MODULE_EXTS: &[&str] = &[
    "mod", "xm", "it", "s3m", "mptm", "mo3", "stk", "st26", "med", "tcb", "669", "amf", "ams",
    "c67", "dbm", "digi", "dmf", "dsm", "dsym", "dtm", "far", "gdm", "gt2", "ice", "imf", "ims",
    "itp", "j2b", "m15", "mdl", "mt2", "mtm", "mus", "nst", "okt", "plm", "psm", "pt36", "ptm",
    "sfx", "sfx2", "stm", "stp", "symmod", "ult", "umx", "wow", "xmf", "itz", "mdz", "s3z", "xmz",
];

/// Appended when the name's own extension isn't an allowlisted module format. We keep the original
/// name in front of it (`evil.exe` -> `evil.exe.mod`) so the file is still recognizable rather than
/// silently renamed into something that misrepresents it.
const FALLBACK_EXT: &str = "mod";

/// Windows device names. Reserved with OR without an extension (`CON`, `CON.mod`), case-insensitive.
const RESERVED: &[&str] = &[
    "con", "prn", "aux", "nul", "com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8",
    "com9", "com0", "lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9", "lpt0",
];

/// Cap the stem so stem+ext stays well inside every filesystem's per-component limit (255 bytes on
/// ext4/APFS/NTFS). Chars, not bytes: a multi-byte char costs <= 4 bytes, so 120 chars <= 480... we
/// truncate on a char boundary and re-check bytes below.
const MAX_STEM_CHARS: usize = 100;

/// Turn a catalog filename into a safe basename for the Downloads dir.
///
/// Guarantees, in order: no directory component survives (`/` and `\` BOTH, on every platform — a
/// backslash is a separator on Windows and a legal filename char on Unix, so a name crafted on one
/// must not become a path on the other); no NTFS alternate-data-stream selector (`:`); no control
/// or bidi-override characters (U+202E to spoof the visible extension); no trailing dots/spaces
/// (Win32 strips them, so `foo.exe. ` would otherwise land as `foo.exe` and defeat a check done on
/// the raw string); no Windows reserved device name; and an allowlisted module extension.
///
/// `fallback_stem` is used when nothing usable survives (e.g. the name was `..`); pass the root CID.
pub fn safe_download_name(raw: &str, fallback_stem: &str) -> String {
    // Last path segment, splitting on BOTH separators regardless of host platform.
    let base = raw.rsplit(['/', '\\']).next().unwrap_or("");

    // Drop control chars (C0/DEL) and the Unicode bidi/format overrides used to visually reverse an
    // extension in a file browser. Map the ADS selector to '_' rather than dropping it, so
    // `song.mod:evil.exe` becomes visibly mangled instead of silently truncating to `song.mod`.
    let cleaned: String = base
        .chars()
        .filter(|c| !c.is_control())
        .filter(|c| !matches!(c, '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}'))
        .map(|c| if c == ':' { '_' } else { c })
        .collect();

    // Win32 silently strips trailing dots and spaces off a component; do it ourselves so what we
    // validate is what the filesystem ends up with.
    let trimmed = cleaned.trim_end_matches(['.', ' ']).trim_start();

    // `.` / `..` / empty carry no name at all.
    let name = if trimmed.is_empty() || trimmed == "." || trimmed == ".." { "" } else { trimmed };

    let (stem, ext) = match name.rsplit_once('.') {
        // A leading-dot name (".hidden") is all stem, no extension.
        Some((s, e)) if !s.is_empty() => (s, e.to_ascii_lowercase()),
        _ => (name, String::new()),
    };

    let mut stem = if stem.is_empty() { fallback_stem } else { stem }.to_string();

    // Reserved device names are reserved even with an extension, so guard the STEM.
    if RESERVED.contains(&stem.to_ascii_lowercase().as_str()) {
        stem.insert(0, '_');
    }

    // Keep the extension only if it names a module format; otherwise the original name survives as
    // part of the stem and a safe extension goes on the end (`evil.exe` -> `evil.exe.mod`).
    let keep_ext = MODULE_EXTS.contains(&ext.as_str());
    if !keep_ext && !ext.is_empty() {
        stem.push('.');
        stem.push_str(&ext);
    }
    let ext = if keep_ext { ext } else { FALLBACK_EXT.to_string() };

    // Truncate the stem on a char boundary, then again by bytes, so the component fits everywhere.
    if stem.chars().count() > MAX_STEM_CHARS {
        stem = stem.chars().take(MAX_STEM_CHARS).collect();
    }
    while stem.len() + ext.len() + 1 > 200 {
        stem.pop();
    }
    // Truncation can re-expose a trailing dot/space.
    let stem = stem.trim_end_matches(['.', ' ']);
    let stem = if stem.is_empty() { fallback_stem } else { stem };

    format!("{stem}.{ext}")
}

#[cfg(test)]
mod tests {
    use super::*;

    const CID: &str = "bafyfallback";

    /// The hostile cases. Each is a real way to turn "write a file the user asked for" into "drop
    /// an executable" or "write somewhere else".
    #[test]
    fn neutralizes_hostile_names() {
        let cases = [
            // Executable/script extensions must never survive -> OS-default launch can't run them.
            ("song.exe", "song.exe.mod"),
            ("song.bat", "song.bat.mod"),
            ("song.lnk", "song.lnk.mod"),
            ("song.ps1", "song.ps1.mod"),
            ("song.scr", "song.scr.mod"),
            // NTFS alternate data stream: must not write to `song.mod`'s hidden stream.
            ("song.mod:evil.exe", "song.mod_evil.exe.mod"),
            // Path traversal / directory escape, both separators, on every platform.
            ("../../etc/passwd", "passwd.mod"),
            ("..\\..\\windows\\system32\\evil.exe", "evil.exe.mod"),
            ("/abs/path/song.xm", "song.xm"),
            // Win32 strips trailing dots+spaces: `foo.exe. ` would land as `foo.exe`.
            ("song.exe. ", "song.exe.mod"),
            ("song.exe...", "song.exe.mod"),
            // Windows reserved device names, with and without an extension.
            ("CON", "_CON.mod"),
            ("con.mod", "_con.mod"),
            ("LPT1.xm", "_LPT1.xm"),
            ("nul", "_nul.mod"),
            // Bidi override used to make `song.exe` render as `song.txt`-ish in a file browser.
            ("song\u{202e}txt.exe", "songtxt.exe.mod"),
            // Control characters.
            ("song\u{7}\n.mod", "song.mod"),
            // Degenerate names fall back to the CID stem.
            ("..", "bafyfallback.mod"),
            (".", "bafyfallback.mod"),
            ("", "bafyfallback.mod"),
            ("/", "bafyfallback.mod"),
        ];
        for (raw, want) in cases {
            assert_eq!(safe_download_name(raw, CID), want, "input {raw:?}");
        }
    }

    /// The whole corpus (170,049 rows as of 2026-07-12) is these six extensions — none may be
    /// touched, or every legitimate download gets renamed.
    #[test]
    fn preserves_real_corpus_names() {
        for name in [
            "aurora.mod",
            "second_reality.s3m",
            "chip.xm",
            "hymn.it",
            "track.mptm",
            "packed.mo3",
        ] {
            assert_eq!(safe_download_name(name, CID), name, "input {name:?}");
        }
        // Extension casing is normalized, the stem is left alone.
        assert_eq!(safe_download_name("AURORA.MOD", CID), "AURORA.mod");
        // Spaces and unicode in the stem are legal and must survive (the corpus has both).
        assert_eq!(safe_download_name("a song.mod", CID), "a song.mod");
        assert_eq!(safe_download_name("naïve.xm", CID), "naïve.xm");
        // The one leading-dash name in the corpus: legal on disk, and `open`'s `--` guard keeps the
        // tracker from parsing it as a flag. We must not mangle it.
        assert_eq!(safe_download_name("-dash.mod", CID), "-dash.mod");
    }

    #[test]
    fn caps_absurd_lengths() {
        let long = format!("{}.mod", "a".repeat(5000));
        let got = safe_download_name(&long, CID);
        assert!(got.len() <= 200, "len {}", got.len());
        assert!(got.ends_with(".mod"));
    }

    /// The output must be a bare filename — never a path — so `dir.join(name)` can't escape.
    #[test]
    fn output_is_always_a_single_component() {
        for raw in ["../../x.mod", "a/b/c.mod", "a\\b\\c.mod", "..", "song.mod:x", "/etc/x"] {
            let got = safe_download_name(raw, CID);
            assert!(!got.contains('/') && !got.contains('\\') && !got.contains(':'), "{raw:?} -> {got:?}");
            assert_eq!(std::path::Path::new(&got).components().count(), 1, "{raw:?} -> {got:?}");
        }
    }
}
