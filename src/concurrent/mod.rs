//! Run multiple coroutines concurrently
//!
//! This module provides joining operations for concurrent execution.

mod join;

// Re-export concurrent operations
pub use join::{Join, JoinEnvelope, JoinError, JoinId, JoinVec, join, join_vec};
