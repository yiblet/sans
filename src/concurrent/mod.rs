//! Drive several coroutines independently through one polling interface.
//!
//! Use [`join`] for an array of continuations, or [`init_join`] for initializers.
//! The `_vec` variants accept a runtime-sized collection. All children must have
//! the same concrete type. Each step runs synchronously.
//!
//! Send [`Poll::Poll`](crate::poll::Poll::Poll) to check for outputs, or
//! [`Poll::Input`](crate::poll::Poll::Input) with a [`JoinEnvelope`] to supply input
//! to one child. Use [`JoinEnvelope::map`] to turn an output into a response while
//! preserving its child ID:
//!
//! ```
//! use sans::{Sans, build::repeat, concurrent::init_join, poll::{Poll, PollOutput}};
//!
//! fn increment(x: i32) -> i32 { x + 1 }
//! let mut joined = init_join([(10, repeat(increment)), (20, repeat(increment))]);
//! let PollOutput::Output(request) = joined.next(Poll::Poll).unwrap_yielded() else {
//!     panic!("expected an initial output");
//! };
//! let child = request.0;
//! let response = request.map(|output| output * 2);
//! let PollOutput::Output(output) = joined.next(Poll::Input(response)).unwrap_yielded() else {
//!     panic!("expected the child's next output");
//! };
//! assert_eq!(output.0, child);
//! ```
//!
//! Follow the [polling protocol](crate::poll) for `NeedsInput` and `NeedsPoll`.
//! See [`Join`] for completion and error behavior.

mod join;

// Re-export concurrent operations
pub use join::{
    Join, JoinEnvelope, JoinError, JoinId, JoinVec, init_join, init_join_vec, join, join_vec,
};
