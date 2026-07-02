//! Small shared helpers.

use std::sync::{Mutex, MutexGuard};

/// Lock a `Mutex` without panicking if the lock was poisoned.
///
/// `mutex.lock().unwrap()` panics when another thread panicked while holding the
/// lock — and a panic across the FFI boundary crashes the iOS app. `lock_safe`
/// behaves identically to `.lock().unwrap()` on the happy (non-poisoned) path,
/// and on poison it recovers the guard via `into_inner()` instead of panicking.
///
/// Recovering is sound here: the values we lock (a SQLite `Connection`, the
/// per-group peer maps, the DM sender registry) are not left in a broken state by
/// an unrelated thread's panic, so continuing with the recovered guard preserves
/// the existing behavior while removing the crash path.
pub trait MutexExt<T: ?Sized> {
    fn lock_safe(&self) -> MutexGuard<'_, T>;
}

impl<T: ?Sized> MutexExt<T> for Mutex<T> {
    #[inline]
    fn lock_safe(&self) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Free disk space (bytes) on the filesystem holding `path` — the transfer
/// preflight oracle (a multi-GB download must be refused up front, not fail at
/// 95% with a full disk). `statvfs` via libc: std exposes no equivalent, and both
/// shipping targets (macOS dev hosts, iOS devices) are unix.
pub fn free_disk_space(path: &std::path::Path) -> anyhow::Result<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|e| anyhow::anyhow!("path has interior NUL: {e}"))?;
    // SAFETY: statvfs only reads the NUL-terminated path and writes the zeroed
    // out-struct we hand it; both live on this stack frame.
    let mut vfs: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), &mut vfs) };
    if rc != 0 {
        return Err(anyhow::anyhow!(
            "statvfs({}) failed: {}",
            path.display(),
            std::io::Error::last_os_error()
        ));
    }
    // f_bavail = blocks available to unprivileged callers (what we can actually use).
    Ok((vfs.f_bavail as u64).saturating_mul(vfs.f_frsize as u64))
}

/// This process's PEAK resident set size in BYTES (`getrusage`) — the RAM-flat
/// transfer oracle. Monotonic per process: compare a delta across a transfer, not
/// absolutes. macOS reports `ru_maxrss` in bytes; Linux in kilobytes.
pub fn peak_rss_bytes() -> u64 {
    // SAFETY: getrusage(RUSAGE_SELF) writes the zeroed out-struct on this frame.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    if rc != 0 {
        return 0;
    }
    let raw = usage.ru_maxrss.max(0) as u64;
    if cfg!(target_os = "macos") || cfg!(target_os = "ios") {
        raw
    } else {
        raw * 1024
    }
}
