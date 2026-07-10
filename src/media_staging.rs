//! Host-side staging of attached media into cyan-media's confined root.
//!
//! cyan-media (correctly) refuses any input outside `CYAN_MEDIA_ROOT` — path
//! confinement is its security boundary and stays intact. The HOST therefore
//! owns the handoff: a user may attach a master from ANYWHERE (`~/sig.mp4`,
//! a Desktop drop, a mounted volume) and before a tool sees it the file is
//! staged — content-addressed, idempotent — into the root the plugin is
//! allowed to read. The same staged path feeds the Video-face player, so the
//! tool input and the preview can never disagree about where the media lives.
//!
//! This module is the ONE definition of "the media root" for the whole host:
//! resolution (`pipeline_executor`), plugin spawn env (`mcp_host`), conform
//! (`review_loop` / `conform_dispatch`) and the player FFI all go through it.
//! It retires the `CYAN_MEDIA_ROOT="$HOME"` manual-testing hack.

use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};

/// Directory (under the media root) that holds staged attachments.
pub const STAGED_DIR: &str = "attached";

/// The effective media root: `CYAN_MEDIA_ROOT` when set (the deploy/test
/// override), else the canonical per-user location `~/.cyan-phase3/media` —
/// the same root the e2e harness and the seeded fixtures already use, and a
/// path that survives app reboots and re-logins.
pub fn effective_media_root() -> PathBuf {
    if let Ok(r) = std::env::var("CYAN_MEDIA_ROOT")
        && !r.trim().is_empty()
    {
        return PathBuf::from(r.trim());
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    Path::new(&home).join(".cyan-phase3").join("media")
}

/// Is `path` already confined to `root`? Resolves symlinks on both sides so a
/// symlinked root (e.g. /tmp on macOS) can't produce false negatives.
pub fn is_within_root(path: &Path, root: &Path) -> bool {
    let (Ok(p), Ok(r)) = (path.canonicalize(), root.canonicalize()) else {
        return false;
    };
    p.starts_with(&r)
}

/// Stage a local file into the media root so the confined cyan-media plugin
/// (and the player) can read it. Content-addressed and idempotent:
///
/// - already inside the root → returned unchanged (no copy);
/// - else copied to `<root>/attached/<blake3-16>/<filename>`; a re-attach of
///   the same bytes lands on the SAME path (dedup by content, not by origin).
///
/// `display_name` names the staged file when given (final path component only —
/// sanitized). The app's upload store keeps board files by BARE HASH with no
/// extension; staging under the human name ("sig_source.mp4") keeps the
/// extension end-to-end, which the player and any extension-sniffing consumer
/// need (found live 2026-07-08: an extension-less staged master rendered the
/// Video face poster instead of media).
///
/// Errors (unreadable source, copy failure) surface as `Err` — the caller
/// decides whether to fall back to the raw path (letting the plugin produce
/// its own clear denial) or to fail the step.
pub fn stage_into_media_root(src: &Path, display_name: Option<&str>) -> Result<PathBuf> {
    let root = effective_media_root();
    if !src.is_file() {
        return Err(anyhow!("not a readable file: {}", src.display()));
    }
    if is_within_root(src, &root) {
        return Ok(src.to_path_buf());
    }

    let digest = hash_file_blake3(src)?;
    let sanitized = display_name
        .map(|n| Path::new(n).file_name().map(|f| f.to_os_string()))
        .unwrap_or_default();
    let name = match &sanitized {
        Some(n) if !n.is_empty() => n.clone(),
        _ => src
            .file_name()
            .ok_or_else(|| anyhow!("path has no filename: {}", src.display()))?
            .to_os_string(),
    };
    let name = name.as_os_str();
    let dest_dir = root.join(STAGED_DIR).join(&digest[..16]);
    std::fs::create_dir_all(&dest_dir)
        .with_context(|| format!("create staging dir {}", dest_dir.display()))?;
    let dest = dest_dir.join(name);

    let src_len = src.metadata()?.len();
    if let Ok(meta) = dest.metadata()
        && meta.is_file()
        && meta.len() == src_len
    {
        return Ok(dest); // same content hash + same size ⇒ already staged
    }

    // Copy via a temp name then rename, so a concurrent reader never sees a
    // half-written file at the content-addressed path.
    let tmp = dest_dir.join(format!(".{}.tmp-{}", digest, std::process::id()));
    std::fs::copy(src, &tmp)
        .with_context(|| format!("stage {} -> {}", src.display(), tmp.display()))?;
    std::fs::rename(&tmp, &dest)
        .with_context(|| format!("finalize staged file {}", dest.display()))?;
    Ok(dest)
}

/// Convenience for resolution call-sites: stage `path` and return it as a
/// string; on any staging error, log and fall back to the ORIGINAL path so
/// the plugin surfaces its own clear denial rather than us inventing one.
pub fn stage_local_media(path: &str) -> String {
    stage_local_media_named(path, None)
}

/// [`stage_local_media`] with the attachment's DISPLAY name (see
/// [`stage_into_media_root`] — keeps the human filename + extension).
pub fn stage_local_media_named(path: &str, display_name: Option<&str>) -> String {
    match stage_into_media_root(Path::new(path), display_name) {
        Ok(staged) => staged.display().to_string(),
        Err(e) => {
            tracing::warn!("media staging failed for {path}: {e:#} — passing through");
            path.to_string()
        }
    }
}

/// Streamed blake3 of a file's contents (hex). Blake3 matches the hash family
/// the file store already uses, so "same bytes = same identity" holds across
/// the attachment store and the staging area.
fn hash_file_blake3(path: &Path) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    let mut file = std::fs::File::open(path)
        .with_context(|| format!("open for hashing: {}", path.display()))?;
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

/// Process-wide lock for tests that mutate `CYAN_MEDIA_ROOT` — cargo runs test
/// threads in parallel and the env is process-global. Every test (in ANY
/// module) that touches the var must hold this.
#[cfg(test)]
pub(crate) static MEDIA_ROOT_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
mod tests {
    use super::*;

    use super::MEDIA_ROOT_ENV_LOCK as ENV_LOCK;

    fn with_root<T>(root: &Path, f: impl FnOnce() -> T) -> T {
        let _g = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev = std::env::var("CYAN_MEDIA_ROOT").ok();
        unsafe { std::env::set_var("CYAN_MEDIA_ROOT", root) };
        let out = f();
        match prev {
            Some(v) => unsafe { std::env::set_var("CYAN_MEDIA_ROOT", v) },
            None => unsafe { std::env::remove_var("CYAN_MEDIA_ROOT") },
        }
        out
    }

    #[test]
    fn stages_outside_file_content_addressed_and_idempotent() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let src = outside.path().join("sig_source.mp4");
        std::fs::write(&src, b"not really a video, but bytes are bytes").unwrap();

        with_root(root.path(), || {
            let staged = stage_into_media_root(&src, None).unwrap();
            assert!(is_within_root(&staged, root.path()), "staged into the root");
            assert_eq!(staged.file_name().unwrap(), "sig_source.mp4");
            assert_eq!(
                std::fs::read(&staged).unwrap(),
                std::fs::read(&src).unwrap()
            );
            // Idempotent: same bytes → same path, no duplicate entries.
            let again = stage_into_media_root(&src, None).unwrap();
            assert_eq!(staged, again);

            // Same bytes from a DIFFERENT origin dedup to the same staged file.
            let copy = outside.path().join("renamed-elsewhere.mp4");
            std::fs::copy(&src, &copy).unwrap();
            let staged_copy = stage_into_media_root(&copy, None).unwrap();
            assert_eq!(staged.parent(), staged_copy.parent(), "same content dir");
        });
    }

    #[test]
    fn inside_root_paths_pass_through_unchanged() {
        let root = tempfile::tempdir().unwrap();
        let inside = root.path().join("master").join("clip.mp4");
        std::fs::create_dir_all(inside.parent().unwrap()).unwrap();
        std::fs::write(&inside, b"already confined").unwrap();

        with_root(root.path(), || {
            let staged = stage_into_media_root(&inside, None).unwrap();
            assert_eq!(staged, inside, "no copy for an already-confined file");
        });
    }

    #[test]
    fn missing_file_is_an_error_and_stage_local_media_falls_back() {
        let root = tempfile::tempdir().unwrap();
        with_root(root.path(), || {
            assert!(stage_into_media_root(Path::new("/definitely/not/here.mp4"), None).is_err());
            // The string helper never panics the pipeline — it passes through.
            assert_eq!(
                stage_local_media("/definitely/not/here.mp4"),
                "/definitely/not/here.mp4"
            );
        });
    }

    #[test]
    fn default_root_is_the_canonical_phase3_location() {
        let _g = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let prev = std::env::var("CYAN_MEDIA_ROOT").ok();
        unsafe { std::env::remove_var("CYAN_MEDIA_ROOT") };
        let root = effective_media_root();
        if let Some(v) = prev {
            unsafe { std::env::set_var("CYAN_MEDIA_ROOT", v) }
        }
        assert!(root.ends_with(".cyan-phase3/media"), "got {}", root.display());
    }
}
