//! Create coroutines from functions.
//!
//! | To | Use |
//! | --- | --- |
//! | Decide when to yield or complete | [`from_fn`] |
//! | Yield once, then return the next input | [`once`] |
//! | Yield for every input | [`repeat`] |
//! | Supply an initial output | [`init`] |
//! | Supply the first input | [`start`] |
//!
//! Combine an initial output with a function using [`init_from_fn`],
//! [`init_once`], or [`init_repeat`].

mod func;
mod init;
mod start;

// Re-export building blocks
pub use func::{FromFn, Once, Repeat, from_fn, once, repeat};
pub use init::{init, init_from_fn, init_once, init_repeat};
pub use start::start;
