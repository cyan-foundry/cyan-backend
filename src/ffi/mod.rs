// src/ffi/mod.rs
//
// All FFI functions for Swift/C interop live here.
// This is the single source of truth for FFI - no duplicates in lib.rs.

mod scaffold;
mod core;

// Re-export scaffold utilities (used internally by FFI functions)

// Re-export all FFI functions from core
// These are the #[no_mangle] extern "C" functions called from Swift
