// Phase 0 build-fingerprint guardrail (FABLE_OVERNIGHT_PROMPT §0.3).
//
// Embeds the git short SHA + dirty flag of THIS checkout into the compiled
// engine as `CYAN_BUILD_COMMIT`, so a running app can prove which source it
// was built from (`cyan_build_commit()` FFI + the init log line). A stale
// binary can then be detected loudly instead of masquerading as fresh.
//
// `build_static_lib.sh` passes CYAN_BUILD_COMMIT explicitly (and cargo's
// rerun-if-env-changed makes a HEAD move force a re-run even when git-checkout
// mtimes would otherwise let cargo no-op). Building outside the script still
// works: we fall back to asking git directly, and "unknown" off-git.

use std::process::Command;

fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn main() {
    // The script's explicit stamp wins; a change to it forces a build.rs re-run.
    println!("cargo:rerun-if-env-changed=CYAN_BUILD_COMMIT");
    // Best-effort git triggers for builds outside the script (HEAD file changes
    // on branch switch; the index changes on commit/checkout).
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/index");

    let stamp = std::env::var("CYAN_BUILD_COMMIT")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| {
            let sha = git(&["rev-parse", "--short=9", "HEAD"]).unwrap_or_else(|| "unknown".into());
            let dirty = git(&["status", "--porcelain", "--untracked-files=no"])
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false);
            if dirty { format!("{sha}-dirty") } else { sha }
        });

    println!("cargo:rustc-env=CYAN_BUILD_COMMIT={stamp}");
}
