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
