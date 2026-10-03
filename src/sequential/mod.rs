//! Run an array of coroutines in order with [`many`].
//!
//! For two coroutines of different types, use [`Sans::chain`](crate::Sans::chain).

mod many;

// Re-export sequential operations
pub use many::{Many, many};
