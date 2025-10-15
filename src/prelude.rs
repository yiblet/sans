//! Commonly used imports
//!
//! Use `use sans::prelude::*;` for quick access to the most common types and functions.

// Core types
pub use crate::{Sans, Step};

// Builder API for initialization
pub use crate::init::{
    Build, ShortCircuit, ShortCircuitBuild, YieldBuild, YieldShortCircuitBuild, Yielded, build,
    shortcircuit, yielding,
};

// Most common constructors
pub use crate::build::{from_fn, once, repeat};

// Composition
pub use crate::compose::chain;

// Transformations
pub use crate::compose::{map_input, map_return, map_yield};

// Polling (universal adapter)
pub use crate::poll::poll;
