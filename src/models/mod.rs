// src/models/mod.rs
//
// Models module - data types for Cyan
//
// Structure:
//   core.rs     - Base entity types (Group, Workspace)
//   dto.rs      - Data transfer objects for storage/serialization
//   protocol.rs - Network protocol types (SnapshotFrame, FileTransferMsg)
//   commands.rs - Network commands
//   events.rs   - Network and Swift events

pub mod core;
pub mod dto;
pub mod protocol;
pub mod commands;
pub mod events;
pub mod node_config;

// Re-exports for convenience
pub use core::{Group, Workspace};
pub use dto::*;
pub use protocol::{FileTransferMsg, SnapshotFrame};
pub use node_config::{relay_mode_for, DiscoveryPolicy, NodeConfig, RelayPolicy};
