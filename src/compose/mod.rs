//! Combining coroutines together
//!
//! This module provides functions for chaining and transforming coroutines.

mod chain;
mod map;

// Re-export composition operations
pub use chain::{AndThen, Chain, and_then, chain};
pub use map::{MapInput, MapReturn, MapYield, map_input, map_return, map_yield};
