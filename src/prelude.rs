//! Commonly used imports
//!
//! Use `use sans::prelude::*;` for quick access to the most common types and functions.

// Core types
pub use crate::{Sans, Step};

// Initialization types
pub use crate::yielded::Yielded;

// Most common constructors
pub use crate::func::{from_fn, once, repeat, try_from_fn};

// Composition
pub use crate::compose::chain;

// Running coroutines
pub use crate::handle::{handle, handle_async};

// Transformations
pub use crate::compose::{map_input, map_return, map_yield};

// Polling (universal adapter)
pub use crate::poll::poll;
