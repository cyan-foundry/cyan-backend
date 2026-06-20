// src/models/core.rs
//
// Core entity types - base types with no internal dependencies

use serde::{Deserialize, Serialize};

// ═══════════════════════════════════════════════════════════════════════════
// BASIC ENTITY TYPES
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Group {
    pub id: String,
    pub name: String,
    pub icon: String,
    pub color: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workspace {
    pub id: String,
    pub group_id: String,
    pub name: String,
    pub created_at: i64,
    /// ROUND8 §W3: a system workspace is auto-seeded per group and **non-deletable**
    /// (the per-group "Plugins" workspace). Defaults to `false` so older peers /
    /// persisted rows decode as ordinary workspaces.
    #[serde(default)]
    pub system: bool,
}