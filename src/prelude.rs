//! Common traits, builders, adapters, and runners: `use sans::prelude::*;`.

// Core types
pub use crate::{InitSans, Sans, Step};

// Most common constructors
pub use crate::build::{from_fn, init, init_from_fn, init_once, init_repeat, once, repeat, start};

// Composition
pub use crate::compose::chain;

// Transformations
pub use crate::compose::{map_input, map_return, map_yield, with_state};

// Explicit polling adapters
pub use crate::poll::{init_poll, poll};

// Execution
pub use crate::run::{
    handle, handle_async, handle_with_input, handle_with_input_async, try_handle, try_handle_async,
    try_handle_with_input, try_handle_with_input_async,
};
