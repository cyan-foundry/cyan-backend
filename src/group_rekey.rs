//! Group re-key consumption — the device side of the "fired employee" cut
//! (IDENTITY_W16_W17_SPEC §C, W17).
//!
//! The model + transition rules live in cyan-identity (`GroupEpoch::genesis` /
//! `rekey`): each rotation bumps the epoch, drops any revoked member, and commits
//! to fresh key material a removed member never receives. The **scheduler** (the
//! ~7-day cycle) and the **deprovision** trigger live in the lens broker; the
//! backend is purely **receive-only** here — it consumes whatever rotation the
//! broker/mesh hands it and tracks the current epoch per group.
//!
//! [`GroupEpochStore`] is that consumer. It is additive and offline: it holds the
//! latest `GroupEpoch` we have seen for each group and applies an incoming one
//! only if it is **newer** (monotonic `epoch`), so a peer cannot replay a stale
//! epoch to slip a revoked member back into the roster. A device whose member is
//! revoked simply stops receiving new epochs that include it, so it cannot read
//! content created after the cut.

use std::collections::HashMap;

use cyan_identity::GroupEpoch;

/// The device's view of the current keying epoch for each group it follows.
///
/// Receive-only: [`GroupEpochStore::apply`] ingests a rotation the broker/mesh
/// produced (it never mints one). Monotonic per group — a lower-or-equal epoch is
/// ignored, so a revoked member can't be re-admitted by replaying an old epoch.
#[derive(Debug, Clone, Default)]
pub struct GroupEpochStore {
    epochs: HashMap<String, GroupEpoch>,
}

impl GroupEpochStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply an incoming `epoch` for its group. Accepted (and stored) only if we
    /// hold nothing for that group yet, or its `epoch` strictly supersedes what we
    /// hold. Returns `true` if it was applied, `false` if ignored as stale/replay.
    pub fn apply(&mut self, epoch: GroupEpoch) -> bool {
        match self.epochs.get(&epoch.group_id) {
            Some(current) if epoch.epoch <= current.epoch => false,
            _ => {
                self.epochs.insert(epoch.group_id.clone(), epoch);
                true
            }
        }
    }

    /// The current epoch for `group_id`, if any has been applied.
    pub fn current(&self, group_id: &str) -> Option<&GroupEpoch> {
        self.epochs.get(group_id)
    }

    /// The current epoch number for `group_id`, if known.
    pub fn epoch_of(&self, group_id: &str) -> Option<u64> {
        self.epochs.get(group_id).map(|e| e.epoch)
    }

    /// Whether `pubkey` is a member of `group_id`'s current epoch. A revoked
    /// member dropped by a re-key is no longer included, so this returns `false`
    /// once the post-revocation epoch has been applied — they are locked out of
    /// content created after the cut. Unknown group ⇒ `false`.
    pub fn includes(&self, group_id: &str, pubkey: &str) -> bool {
        self.epochs
            .get(group_id)
            .map(|e| e.includes(pubkey))
            .unwrap_or(false)
    }
}
