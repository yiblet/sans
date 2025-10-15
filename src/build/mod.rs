//! Building coroutines from scratch
//!
//! This module provides functions and types for creating new coroutines.

mod func;

// Re-export building blocks
pub use func::{FromFn, Once, Repeat, from_fn, once, repeat};
