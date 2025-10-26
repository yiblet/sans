//! Joining multiple coroutines for concurrent execution.
//!
//! This module provides the [`Join`] combinator for running multiple coroutines
//! concurrently, polling them for outputs and directing inputs to specific coroutines.

use crate::poll::{PollError, PollInput, PollOutput, Pollable, poll};
use crate::{Sans, Step};

/// Create a [`Join`] from an array of [`Sans`] coroutines.
///
/// Wraps each coroutine in a [`Pollable`] for concurrent execution. The resulting [`Join`]
/// can be polled to get outputs from any ready coroutine, or sent inputs directed to specific coroutines.
///
/// # Examples
///
/// ```
/// use sans::prelude::*;
/// use sans::poll::{PollInput, PollOutput};
/// use sans::concurrent::{join, JoinEnvelope};
///
/// fn add_one(x: i32) -> i32 { x + 1 }
/// let coro1 = repeat(add_one);
/// let coro2 = repeat(add_one);
///
/// let mut joined = join([coro1, coro2]);
///
/// // Send input to first coro
/// match joined.next(PollInput::Input(JoinEnvelope::new(0, 10))) {
///     Step::Yielded(PollOutput::Output(env)) => {
///         assert_eq!(*env.value(), 11);
///     }
///     _ => panic!("Expected output from coro 0"),
/// }
/// ```
pub fn join<const N: usize, I, S>(rest: [S; N]) -> Join<N, I, S>
where
    S: Sans<I>,
{
    Join {
        pollables: rest.map(|s| poll(s)),
        returns: std::array::from_fn(|_| None),
        last_index: 0,
        complete: 0,
    }
}

/// Create a [`JoinVec`] from a vector of [`Sans`] coroutines.
///
/// Like [`join`] but accepts a dynamic number of coroutines at runtime.
pub fn join_vec<I, S>(sans: Vec<S>) -> JoinVec<I, S>
where
    S: Sans<I>,
{
    let len = sans.len();
    JoinVec {
        pollables: sans.into_iter().map(|s| poll(s)).collect(),
        returns: (0..len).map(|_| None).collect(),
        last_index: 0,
        complete: 0,
    }
}

/// Runs multiple coroutines concurrently, allowing them to be polled and fed inputs independently.
///
/// `Join` coordinates execution of `N` coroutines, each wrapped in a [`Pollable`]. Inputs and outputs
/// are tagged with a [`JoinEnvelope`] containing the coroutine index.
///
/// When polled (`PollInput::Poll`), it uses round-robin scheduling to check each coroutine for available
/// output. Inputs (`PollInput::Input(JoinEnvelope(index, value))`) are routed to the specified coroutine.
///
/// The join completes when all coroutines complete, returning an array of their return values.
pub struct Join<const N: usize, I, S>
where
    S: Sans<I>,
{
    pollables: [Pollable<I, S>; N],
    returns: [Option<S::Return>; N],
    last_index: usize,
    complete: usize,
}

/// Vec-based version of [`Join`] for dynamic number of coroutines.
///
/// Like [`Join`] but uses a `Vec` to store coroutines, allowing the number to be determined at runtime.
pub struct JoinVec<I, S>
where
    S: Sans<I>,
{
    pollables: Vec<Pollable<I, S>>,
    returns: Vec<Option<S::Return>>,
    last_index: usize,
    complete: usize,
}

/// Errors that can occur during join execution.
#[derive(Debug)]
pub enum JoinError {
    /// A pollable coroutine failed with the given index and error.
    PollableFailed(JoinId, PollError),
}

impl std::fmt::Display for JoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinError::PollableFailed(id, err) => {
                write!(f, "pollable at index {} failed: {}", id.as_usize(), err)
            }
        }
    }
}

impl std::error::Error for JoinError {}

/// Identifier for a coroutine in a [`Join`] operation.
///
/// This is a type-safe wrapper around a coroutine index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JoinId(usize);

impl JoinId {
    pub(crate) fn new(index: usize) -> Self {
        JoinId(index)
    }

    pub(crate) fn as_usize(&self) -> usize {
        self.0
    }
}

/// Wraps values with a coroutine index for routing in [`Join`] operations.
///
/// The first field is the coroutine index, the second is the wrapped value.
///
/// Implements `Deref` to access the inner value conveniently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JoinEnvelope<T>(pub JoinId, pub T);

impl<T> JoinEnvelope<T> {
    /// Creates a new `JoinEnvelope` with the given index and value.
    pub fn new(index: usize, value: T) -> Self {
        JoinEnvelope(JoinId::new(index), value)
    }

    /// Returns a reference to the wrapped value.
    pub fn value(&self) -> &T {
        &self.1
    }

    pub fn map<U, F>(self, f: F) -> JoinEnvelope<U>
    where
        F: FnOnce(T) -> U,
    {
        JoinEnvelope(self.0, f(self.1))
    }
}

impl<T> std::ops::Deref for JoinEnvelope<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.1
    }
}

impl<const N: usize, I, S>
    Sans<PollInput<JoinEnvelope<I>>>
    for Join<N, I, S>
where
    S: Sans<I>,
{
    type Output = PollOutput<JoinEnvelope<I>, JoinEnvelope<S::Output>>;
    type Return = Result<[S::Return; N], JoinError>;

    fn next(
        &mut self,
        input: PollInput<JoinEnvelope<I>>,
    ) -> Step<Self::Output, Self::Return> {
        match input {
            PollInput::Poll => {
                // Round-robin through pollables looking for output
                for i in 0..N {
                    let idx = (self.last_index + 1 + i) % N;
                    if let Some(pollable) = self.pollables.get_mut(idx) {
                        match pollable.next(PollInput::Poll) {
                            Step::Yielded(PollOutput::Output(o)) => {
                                self.last_index = idx;
                                return Step::Yielded(PollOutput::Output(JoinEnvelope(
                                    JoinId::new(idx),
                                    o,
                                )));
                            }
                            Step::Yielded(PollOutput::NeedsInput) => continue,
                            Step::Yielded(PollOutput::Complete) => {
                                // This shouldn't happen - Complete is not yielded, it's in Step::Complete
                                continue;
                            }
                            Step::Yielded(PollOutput::NeedsPoll(_)) => {
                                // This shouldn't happen when polling
                                continue;
                            }
                            Step::Complete(Ok(r)) => {
                                // Store the return value
                                self.returns[idx] = Some(r);
                                self.complete += 1;

                                // Check if all are done
                                if self.complete == N {
                                    // Collect all returns
                                    let results: [S::Return; N] = std::array::from_fn(|i| {
                                        self.returns[i]
                                            .take()
                                            .expect("return value should be present")
                                    });
                                    return Step::Complete(Ok(results));
                                }
                                continue;
                            }
                            Step::Complete(Err(e)) => {
                                return Step::Complete(Err(JoinError::PollableFailed(
                                    JoinId::new(idx),
                                    e,
                                )));
                            }
                        }
                    }
                }

                // Check if all are complete
                if self.complete == N {
                    // Collect all returns
                    let results: [S::Return; N] = std::array::from_fn(|i| {
                        self.returns[i]
                            .take()
                            .expect("return value should be present")
                    });
                    return Step::Complete(Ok(results));
                }

                // All waiting for input
                Step::Yielded(PollOutput::NeedsInput)
            }

            PollInput::Input(JoinEnvelope(id, input)) => {
                let idx = id.as_usize();
                if let Some(pollable) = self.pollables.get_mut(idx) {
                    match pollable.next(PollInput::Input(input)) {
                        Step::Yielded(PollOutput::Output(o)) => {
                            Step::Yielded(PollOutput::Output(JoinEnvelope(id, o)))
                        }
                        Step::Yielded(PollOutput::NeedsPoll(i2)) => {
                            Step::Yielded(PollOutput::NeedsPoll(JoinEnvelope(id, i2)))
                        }
                        Step::Yielded(PollOutput::NeedsInput) => {
                            Step::Yielded(PollOutput::NeedsInput)
                        }
                        Step::Yielded(PollOutput::Complete) => {
                            // Shouldn't happen
                            Step::Yielded(PollOutput::NeedsInput)
                        }
                        Step::Complete(Ok(r)) => {
                            // Store the return value
                            self.returns[idx] = Some(r);
                            self.complete += 1;

                            if self.complete == N {
                                // All done - collect all returns
                                let results: [S::Return; N] = std::array::from_fn(|i| {
                                    self.returns[i]
                                        .take()
                                        .expect("return value should be present")
                                });
                                return Step::Complete(Ok(results));
                            }
                            Step::Yielded(PollOutput::NeedsInput)
                        }
                        Step::Complete(Err(e)) => {
                            Step::Complete(Err(JoinError::PollableFailed(id, e)))
                        }
                    }
                } else {
                    // This shouldn't happen - idx out of bounds
                    unreachable!("index {} out of bounds for pollables array", idx)
                }
            }
        }
    }
}

// Implement Sans for JoinVec
impl<I, S> Sans<PollInput<JoinEnvelope<I>>>
    for JoinVec<I, S>
where
    S: Sans<I>,
{
    type Output = PollOutput<JoinEnvelope<I>, JoinEnvelope<S::Output>>;
    type Return = Result<Vec<S::Return>, JoinError>;

    fn next(
        &mut self,
        input: PollInput<JoinEnvelope<I>>,
    ) -> Step<Self::Output, Self::Return> {
        let n = self.pollables.len();

        match input {
            PollInput::Poll => {
                // Round-robin through pollables looking for output
                for i in 0..n {
                    let idx = (self.last_index + 1 + i) % n;
                    if let Some(pollable) = self.pollables.get_mut(idx) {
                        match pollable.next(PollInput::Poll) {
                            Step::Yielded(PollOutput::Output(o)) => {
                                self.last_index = idx;
                                return Step::Yielded(PollOutput::Output(JoinEnvelope(
                                    JoinId::new(idx),
                                    o,
                                )));
                            }
                            Step::Yielded(PollOutput::NeedsInput) => continue,
                            Step::Yielded(PollOutput::Complete) => continue,
                            Step::Yielded(PollOutput::NeedsPoll(_)) => continue,
                            Step::Complete(Ok(r)) => {
                                // Store the return value
                                self.returns[idx] = Some(r);
                                self.complete += 1;

                                // Check if all are done
                                if self.complete == n {
                                    // Collect all returns
                                    let results: Vec<S::Return> = self
                                        .returns
                                        .iter_mut()
                                        .map(|opt| {
                                            opt.take().expect("return value should be present")
                                        })
                                        .collect();
                                    return Step::Complete(Ok(results));
                                }
                                continue;
                            }
                            Step::Complete(Err(e)) => {
                                return Step::Complete(Err(JoinError::PollableFailed(
                                    JoinId::new(idx),
                                    e,
                                )));
                            }
                        }
                    }
                }

                // Check if all are complete
                if self.complete == n {
                    // Collect all returns
                    let results: Vec<S::Return> = self
                        .returns
                        .iter_mut()
                        .map(|opt| opt.take().expect("return value should be present"))
                        .collect();
                    return Step::Complete(Ok(results));
                }

                // All waiting for input
                Step::Yielded(PollOutput::NeedsInput)
            }

            PollInput::Input(JoinEnvelope(id, input)) => {
                let idx = id.as_usize();
                if let Some(pollable) = self.pollables.get_mut(idx) {
                    match pollable.next(PollInput::Input(input)) {
                        Step::Yielded(PollOutput::Output(o)) => {
                            Step::Yielded(PollOutput::Output(JoinEnvelope(id, o)))
                        }
                        Step::Yielded(PollOutput::NeedsPoll(i2)) => {
                            Step::Yielded(PollOutput::NeedsPoll(JoinEnvelope(id, i2)))
                        }
                        Step::Yielded(PollOutput::NeedsInput) => {
                            Step::Yielded(PollOutput::NeedsInput)
                        }
                        Step::Yielded(PollOutput::Complete) => {
                            Step::Yielded(PollOutput::NeedsInput)
                        }
                        Step::Complete(Ok(r)) => {
                            // Store the return value
                            self.returns[idx] = Some(r);
                            self.complete += 1;

                            if self.complete == n {
                                // All done - collect all returns
                                let results: Vec<S::Return> = self
                                    .returns
                                    .iter_mut()
                                    .map(|opt| opt.take().expect("return value should be present"))
                                    .collect();
                                return Step::Complete(Ok(results));
                            }
                            Step::Yielded(PollOutput::NeedsInput)
                        }
                        Step::Complete(Err(e)) => {
                            Step::Complete(Err(JoinError::PollableFailed(id, e)))
                        }
                    }
                } else {
                    // This shouldn't happen - idx out of bounds
                    unreachable!("index {} out of bounds for pollables vec", idx)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::func::{once, repeat};

    #[test]
    fn test_join_two_sans_basic() {
        // Use the same function for both to have the same type
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let s1 = repeat(add_one);
        let s2 = repeat(add_one);
        let mut joined = join([s1, s2]);

        // Poll should indicate needs input
        match joined.next(PollInput::Poll) {
            Step::Yielded(PollOutput::NeedsInput) => {}
            other => panic!("Expected NeedsInput, got {:?}", other),
        }

        // Send input to first sans
        match joined.next(PollInput::Input(JoinEnvelope::new(0, 10))) {
            Step::Yielded(PollOutput::Output(JoinEnvelope(_, 11))) => {}
            other => panic!("Expected Output(JoinEnvelope(_, 11)), got {:?}", other),
        }

        // Send input to second sans
        match joined.next(PollInput::Input(JoinEnvelope::new(1, 5))) {
            Step::Yielded(PollOutput::Output(JoinEnvelope(_, 6))) => {}
            other => panic!("Expected Output(JoinEnvelope(_, 6)), got {:?}", other),
        }
    }

    #[test]
    fn test_join_round_robin_polling() {
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let s1 = repeat(add_one);
        let s2 = repeat(add_one);
        let mut joined = join([s1, s2]);

        // Send inputs to both - they produce outputs directly (repeat always yields)
        match joined.next(PollInput::Input(JoinEnvelope::new(0, 10))) {
            Step::Yielded(PollOutput::Output(JoinEnvelope(_, 11))) => {}
            other => panic!("Expected Output, got {:?}", other),
        }

        match joined.next(PollInput::Input(JoinEnvelope::new(1, 5))) {
            Step::Yielded(PollOutput::Output(JoinEnvelope(_, 6))) => {}
            other => panic!("Expected Output, got {:?}", other),
        }

        // Send more inputs
        match joined.next(PollInput::Input(JoinEnvelope::new(0, 20))) {
            Step::Yielded(PollOutput::Output(JoinEnvelope(_, 21))) => {}
            other => panic!("Expected Output, got {:?}", other),
        }
    }

    #[test]
    fn test_join_completion_single_sans() {
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let s1 = once(add_one);
        let mut joined = join([s1]);

        // Send input - once yields first
        match joined.next(PollInput::Input(JoinEnvelope::new(0, 10))) {
            Step::Yielded(PollOutput::Output(JoinEnvelope(_, 11))) => {}
            other => panic!("Expected Output, got {:?}", other),
        }

        // Send another input to complete
        match joined.next(PollInput::Input(JoinEnvelope::new(0, 99))) {
            Step::Complete(Ok([99])) => {}
            other => panic!("Expected Complete(Ok([99])), got {:?}", other),
        }
    }

    #[test]
    fn test_join_completion_multiple_sans() {
        fn process(x: i32) -> i32 {
            x + 1
        }
        let s1 = once(process);
        let s2 = once(process);
        let s3 = once(process);
        let mut joined = join([s1, s2, s3]);

        // Send inputs to all - they yield outputs first
        joined
            .next(PollInput::Input(JoinEnvelope::new(0, 10)))
            .expect_yielded("should yield");
        joined
            .next(PollInput::Input(JoinEnvelope::new(1, 5)))
            .expect_yielded("should yield");
        joined
            .next(PollInput::Input(JoinEnvelope::new(2, 20)))
            .expect_yielded("should yield");

        // Send second inputs to complete each
        joined
            .next(PollInput::Input(JoinEnvelope::new(0, 100)))
            .expect_yielded("should yield");
        joined
            .next(PollInput::Input(JoinEnvelope::new(1, 200)))
            .expect_yielded("should yield");

        // Final completion
        match joined.next(PollInput::Input(JoinEnvelope::new(2, 300))) {
            Step::Complete(Ok(results)) => {
                assert_eq!(results, [100, 200, 300]);
            }
            other => panic!("Expected Complete, got {:?}", other),
        }
    }

    #[test]
    fn test_join_out_of_order_completion() {
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let s1 = once(add_one);
        let s2 = once(add_one);
        let mut joined = join([s1, s2]);

        // Send inputs out of order - they yield first
        joined
            .next(PollInput::Input(JoinEnvelope::new(1, 5)))
            .expect_yielded("should yield");
        joined
            .next(PollInput::Input(JoinEnvelope::new(0, 10)))
            .expect_yielded("should yield");

        // Complete them
        joined
            .next(PollInput::Input(JoinEnvelope::new(1, 100)))
            .expect_yielded("should yield");

        match joined.next(PollInput::Input(JoinEnvelope::new(0, 200))) {
            Step::Complete(Ok(results)) => {
                assert_eq!(results, [200, 100]);
            }
            other => panic!("Expected Complete, got {:?}", other),
        }
    }

    #[test]
    fn test_join_interleaved_operations() {
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let s1 = repeat(add_one);
        let s2 = repeat(add_one);
        let mut joined = join([s1, s2]);

        // Interleave operations on both sans
        for i in 0..3 {
            joined
                .next(PollInput::Input(JoinEnvelope::new(0, i)))
                .expect_yielded("should yield");
            joined
                .next(PollInput::Input(JoinEnvelope::new(1, i)))
                .expect_yielded("should yield");
        }

        // Should still be running
        match joined.next(PollInput::Poll) {
            Step::Yielded(_) => {}
            other => panic!("Expected Yielded, got {:?}", other),
        }
    }

    #[test]
    fn test_join_continuous_operation() {
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let s1 = repeat(add_one);
        let mut joined = join([s1]);

        // Send inputs continuously
        for i in 1..=5 {
            match joined.next(PollInput::Input(JoinEnvelope::new(0, i))) {
                Step::Yielded(PollOutput::Output(JoinEnvelope(_, output))) => {
                    assert_eq!(output, i + 1);
                }
                other => panic!("Expected Output, got {:?}", other),
            }
        }
    }

    #[test]
    fn test_join_all_waiting() {
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let s1 = repeat(add_one);
        let s2 = repeat(add_one);
        let mut joined = join([s1, s2]);

        // Poll when all are waiting
        match joined.next(PollInput::Poll) {
            Step::Yielded(PollOutput::NeedsInput) => {}
            other => panic!("Expected NeedsInput, got {:?}", other),
        }
    }

    #[test]
    fn test_join_poll_after_all_complete() {
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let s1 = once(add_one);
        let s2 = once(add_one);
        let mut joined = join([s1, s2]);

        // First inputs yield outputs
        joined
            .next(PollInput::Input(JoinEnvelope::new(0, 10)))
            .expect_yielded("should yield");
        joined
            .next(PollInput::Input(JoinEnvelope::new(1, 5)))
            .expect_yielded("should yield");

        // Complete both
        joined
            .next(PollInput::Input(JoinEnvelope::new(0, 100)))
            .expect_yielded("should yield");

        // Last completion
        match joined.next(PollInput::Input(JoinEnvelope::new(1, 200))) {
            Step::Complete(Ok(results)) => {
                assert_eq!(results, [100, 200]);
            }
            other => panic!("Expected Complete, got {:?}", other),
        }
    }
}
