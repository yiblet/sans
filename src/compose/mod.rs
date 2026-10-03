//! Adapt a coroutine or connect it to the next one.
//!
//! | To… | Use |
//! | --- | --- |
//! | Convert inputs | [`map_input`] |
//! | Convert yielded outputs | [`map_yield`] |
//! | Convert the final result | [`map_return`] |
//! | Share state between input processing and completion | [`with_state`] |
//! | Pass the final result to another coroutine | [`chain`] |
//! | Create the next coroutine from the final result | [`and_then`] |
//!
//! For method syntax, see [`Sans`](crate::Sans) and [`InitSans`](crate::InitSans).

mod chain;
mod map;
pub(crate) mod sequence;
mod state;

// Re-export composition operations
pub use chain::{AndThen, Chain, and_then, chain, init_chain};
pub use map::{MapInput, MapReturn, MapYield, init_map_yield, map_input, map_return, map_yield};

pub use state::{WithState, with_state};
