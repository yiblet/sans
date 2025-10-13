//! Joining multiple coroutines for concurrent execution.
//!
//! This module provides the [`Join`] combinator for running multiple coroutines
//! concurrently, polling them for outputs and directing inputs to specific coroutines.

use crate::poll::{Poll, PollError, PollOutput, Pollable, init_poll, poll};
use crate::{InitSans, Sans, Step};

/// Create a [`Join`] from an array of [`InitSans`] coroutines.
///
/// Each coroutine is wrapped in a [`Pollable`] using [`init_poll`], allowing them to be
/// polled concurrently. See [`Join`] for details on how concurrent execution works.
///
/// # Examples
///
/// ```
/// use sans::prelude::*;
/// use sans::poll::{Poll, PollOutput};
/// use sans::concurrent::{init_join, JoinEnvelope};
///
/// // Create two coroutines with initial outputs
/// fn add_one(x: i32) -> i32 { x + 1 }
/// let f = add_one as fn(i32) -> i32;
/// let coro1 = (100, repeat(f));
/// let coro2 = (200, repeat(f));
///
/// let mut joined = init_join([coro1, coro2]);
///
/// // Poll to get initial outputs
/// match joined.next(Poll::Poll) {
///     Step::Yielded(PollOutput::Output(env)) => {
///         assert!(*env.value() == 100 || *env.value() == 200);
///     }
///     _ => panic!("Expected output"),
/// }
/// ```
pub fn init_join<const N: usize, I, O, S, T>(rest: [T; N]) -> Join<N, S, O, S::Return>
where
    T: InitSans<I, O, Next = S>,
    S: Sans<I, O>,
{
    let pollables = rest.map(|init_sans| init_poll(init_sans));

    Join {
        pollables,
        returns: std::array::from_fn(|_| None),
        last_index: 0,
        complete: 0,
    }
}

/// Create a [`Join`] from an array of [`Sans`] coroutines.
///
/// Wraps each coroutine in a [`Pollable`] for concurrent execution. The resulting [`Join`]
/// can be polled to get outputs from any ready coroutine, or sent inputs directed to specific coroutines.
///
/// # Examples
///
/// ```
/// use sans::prelude::*;
/// use sans::poll::{Poll, PollOutput};
/// use sans::concurrent::{join, JoinEnvelope};
///
/// fn add_one(x: i32) -> i32 { x + 1 }
/// let coro1 = repeat(add_one);
/// let coro2 = repeat(add_one);
///
/// let mut joined = join([coro1, coro2]);
///
/// // Send input to first coro
/// match joined.next(Poll::Input(JoinEnvelope::new(0, 10))) {
///     Step::Yielded(PollOutput::Output(env)) => {
///         assert_eq!(*env.value(), 11);
///     }
///     _ => panic!("Expected output from coro 0"),
/// }
/// ```
pub fn join<const N: usize, I, O, S>(rest: [S; N]) -> Join<N, S, O, S::Return>
where
    S: Sans<I, O>,
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
pub fn join_vec<I, O, S>(sans: Vec<S>) -> JoinVec<S, O, S::Return>
where
    S: Sans<I, O>,
{
    let len = sans.len();
    JoinVec {
        pollables: sans.into_iter().map(|s| poll(s)).collect(),
        returns: (0..len).map(|_| None).collect(),
        last_index: 0,
        complete: 0,
    }
}

/// Create a [`JoinVec`] from a vector of [`InitSans`] coroutines.
///
/// Like [`init_join`] but accepts a dynamic number of coroutines at runtime.
pub fn init_join_vec<I, O, S, T>(inits: Vec<T>) -> JoinVec<S, O, S::Return>
where
    T: InitSans<I, O, Next = S>,
    S: Sans<I, O>,
{
    let len = inits.len();
    JoinVec {
        pollables: inits.into_iter().map(|init| init_poll(init)).collect(),
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
/// When polled (`Poll::Poll`), it uses round-robin scheduling to check each coroutine for available
/// output. Inputs (`Poll::Input(JoinEnvelope(index, value))`) are routed to the specified coroutine.
///
/// The join completes when all coroutines complete, returning an array of their return values.
pub struct Join<const N: usize, S, O, R> {
    pollables: [Pollable<S, O, R>; N],
    returns: [Option<R>; N],
    last_index: usize,
    complete: usize,
}

/// Vec-based version of [`Join`] for dynamic number of coroutines.
///
/// Like [`Join`] but uses a `Vec` to store coroutines, allowing the number to be determined at runtime.
pub struct JoinVec<S, O, R> {
    pollables: Vec<Pollable<S, O, R>>,
    returns: Vec<Option<R>>,
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

impl<const N: usize, I, O, S>
    Sans<Poll<JoinEnvelope<I>>, PollOutput<JoinEnvelope<I>, JoinEnvelope<O>>>
    for Join<N, S, O, S::Return>
where
    S: Sans<I, O>,
{
    type Return = Result<[S::Return; N], JoinError>;

    fn next(
        &mut self,
        input: Poll<JoinEnvelope<I>>,
    ) -> Step<PollOutput<JoinEnvelope<I>, JoinEnvelope<O>>, Self::Return> {
        match input {
            Poll::Poll => {
                // Round-robin through pollables looking for output
                for i in 0..N {
                    let idx = (self.last_index + 1 + i) % N;
                    if let Some(pollable) = self.pollables.get_mut(idx) {
                        match pollable.next(Poll::Poll) {
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

            Poll::Input(JoinEnvelope(id, input)) => {
                let idx = id.as_usize();
                if let Some(pollable) = self.pollables.get_mut(idx) {
                    match pollable.next(Poll::Input(input)) {
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

// also implement InitSans for Join
impl<const N: usize, I, O, S>
    InitSans<Poll<JoinEnvelope<I>>, PollOutput<JoinEnvelope<I>, JoinEnvelope<O>>>
    for Join<N, S, O, S::Return>
where
    S: Sans<I, O>,
{
    type Next = Self;

    fn init(
        mut self,
    ) -> Step<
        (PollOutput<JoinEnvelope<I>, JoinEnvelope<O>>, Self::Next),
        <Self::Next as Sans<Poll<JoinEnvelope<I>>, PollOutput<JoinEnvelope<I>, JoinEnvelope<O>>>>::Return,
    >{
        match self.next(Poll::Poll) {
            Step::Yielded(o) => Step::Yielded((o, self)),
            Step::Complete(r) => Step::Complete(r),
        }
    }
}

// Implement Sans for JoinVec
impl<I, O, S> Sans<Poll<JoinEnvelope<I>>, PollOutput<JoinEnvelope<I>, JoinEnvelope<O>>>
    for JoinVec<S, O, S::Return>
where
    S: Sans<I, O>,
{
    type Return = Result<Vec<S::Return>, JoinError>;

    fn next(
        &mut self,
        input: Poll<JoinEnvelope<I>>,
    ) -> Step<PollOutput<JoinEnvelope<I>, JoinEnvelope<O>>, Self::Return> {
        let n = self.pollables.len();

        match input {
            Poll::Poll => {
                // Round-robin through pollables looking for output
                for i in 0..n {
                    let idx = (self.last_index + 1 + i) % n;
                    if let Some(pollable) = self.pollables.get_mut(idx) {
                        match pollable.next(Poll::Poll) {
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

            Poll::Input(JoinEnvelope(id, input)) => {
                let idx = id.as_usize();
                if let Some(pollable) = self.pollables.get_mut(idx) {
                    match pollable.next(Poll::Input(input)) {
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

// Implement InitSans for JoinVec
impl<I, O, S> InitSans<Poll<JoinEnvelope<I>>, PollOutput<JoinEnvelope<I>, JoinEnvelope<O>>>
    for JoinVec<S, O, S::Return>
where
    S: Sans<I, O>,
{
    type Next = Self;

    fn init(
        mut self,
    ) -> Step<
        (PollOutput<JoinEnvelope<I>, JoinEnvelope<O>>, Self::Next),
        <Self::Next as Sans<Poll<JoinEnvelope<I>>, PollOutput<JoinEnvelope<I>, JoinEnvelope<O>>>>::Return,
    >{
        match self.next(Poll::Poll) {
            Step::Yielded(o) => Step::Yielded((o, self)),
            Step::Complete(r) => Step::Complete(r),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build::{once, repeat};

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
        match joined.next(Poll::Poll) {
            Step::Yielded(PollOutput::NeedsInput) => {}
            other => panic!("Expected NeedsInput, got {:?}", other),
        }

        // Send input to first sans
        match joined.next(Poll::Input(JoinEnvelope::new(0, 10))) {
            Step::Yielded(PollOutput::Output(JoinEnvelope(_, 11))) => {}
            other => panic!("Expected Output(JoinEnvelope(_, 11)), got {:?}", other),
        }

        // Send input to second sans
        match joined.next(Poll::Input(JoinEnvelope::new(1, 5))) {
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
        match joined.next(Poll::Input(JoinEnvelope::new(0, 10))) {
            Step::Yielded(PollOutput::Output(JoinEnvelope(_, 11))) => {}
            other => panic!("Expected Output, got {:?}", other),
        }

        match joined.next(Poll::Input(JoinEnvelope::new(1, 5))) {
            Step::Yielded(PollOutput::Output(JoinEnvelope(_, 6))) => {}
            other => panic!("Expected Output, got {:?}", other),
        }

        // Send more inputs
        match joined.next(Poll::Input(JoinEnvelope::new(0, 20))) {
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
        match joined.next(Poll::Input(JoinEnvelope::new(0, 10))) {
            Step::Yielded(PollOutput::Output(JoinEnvelope(_, 11))) => {}
            other => panic!("Expected Output, got {:?}", other),
        }

        // Send another input to complete
        match joined.next(Poll::Input(JoinEnvelope::new(0, 99))) {
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
            .next(Poll::Input(JoinEnvelope::new(0, 10)))
            .expect_yielded("should yield");
        joined
            .next(Poll::Input(JoinEnvelope::new(1, 5)))
            .expect_yielded("should yield");
        joined
            .next(Poll::Input(JoinEnvelope::new(2, 20)))
            .expect_yielded("should yield");

        // Send second inputs to complete each
        joined
            .next(Poll::Input(JoinEnvelope::new(0, 100)))
            .expect_yielded("should yield");
        joined
            .next(Poll::Input(JoinEnvelope::new(1, 200)))
            .expect_yielded("should yield");

        // Final completion
        match joined.next(Poll::Input(JoinEnvelope::new(2, 300))) {
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
            .next(Poll::Input(JoinEnvelope::new(1, 5)))
            .expect_yielded("should yield");
        joined
            .next(Poll::Input(JoinEnvelope::new(0, 10)))
            .expect_yielded("should yield");

        // Complete them
        joined
            .next(Poll::Input(JoinEnvelope::new(1, 100)))
            .expect_yielded("should yield");

        match joined.next(Poll::Input(JoinEnvelope::new(0, 200))) {
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
                .next(Poll::Input(JoinEnvelope::new(0, i)))
                .expect_yielded("should yield");
            joined
                .next(Poll::Input(JoinEnvelope::new(1, i)))
                .expect_yielded("should yield");
        }

        // Should still be running
        match joined.next(Poll::Poll) {
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
            match joined.next(Poll::Input(JoinEnvelope::new(0, i))) {
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
        match joined.next(Poll::Poll) {
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
            .next(Poll::Input(JoinEnvelope::new(0, 10)))
            .expect_yielded("should yield");
        joined
            .next(Poll::Input(JoinEnvelope::new(1, 5)))
            .expect_yielded("should yield");

        // Complete both
        joined
            .next(Poll::Input(JoinEnvelope::new(0, 100)))
            .expect_yielded("should yield");

        // Last completion
        match joined.next(Poll::Input(JoinEnvelope::new(1, 200))) {
            Step::Complete(Ok(results)) => {
                assert_eq!(results, [100, 200]);
            }
            other => panic!("Expected Complete, got {:?}", other),
        }
    }

    #[test]
    fn test_init_join_basic() {
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let init1 = (100, repeat(add_one));
        let init2 = (200, repeat(add_one));

        let mut joined = init_join([init1, init2]);

        // Should have initial outputs available
        match joined.next(Poll::Poll) {
            Step::Yielded(PollOutput::Output(JoinEnvelope(id, val))) => {
                let idx = id.as_usize();
                assert!(idx == 0 || idx == 1);
                assert!(val == 100 || val == 200);
            }
            other => panic!("Expected Output, got {:?}", other),
        }
    }

    #[test]
    fn test_join_empty_array() {
        use crate::build;
        // Need a concrete type for empty array
        #[allow(clippy::type_complexity)]
        let joined: Join<0, build::Repeat<fn(i32) -> i32>, i32, i32> = join([]);

        // Should immediately complete with empty array
        let mut joined = joined;
        match joined.next(Poll::Poll) {
            Step::Complete(Ok([])) => {}
            other => panic!("Expected Complete(Ok([])), got {:?}", other),
        }
    }

    // Tests for JoinVec
    #[test]
    fn test_join_vec_basic() {
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let sans = vec![repeat(add_one), repeat(add_one), repeat(add_one)];
        let mut joined = join_vec(sans);

        // Poll should indicate needs input
        match joined.next(Poll::Poll) {
            Step::Yielded(PollOutput::NeedsInput) => {}
            other => panic!("Expected NeedsInput, got {:?}", other),
        }

        // Send input to each sans
        for i in 0..3 {
            let input_val = (i * 10) as i32;
            match joined.next(Poll::Input(JoinEnvelope::new(i, input_val))) {
                Step::Yielded(PollOutput::Output(JoinEnvelope(id, val))) => {
                    assert_eq!(id.as_usize(), i);
                    assert_eq!(val, input_val + 1);
                }
                other => panic!("Expected Output, got {:?}", other),
            }
        }
    }

    #[test]
    fn test_join_vec_completion() {
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let sans = vec![once(add_one), once(add_one)];
        let mut joined = join_vec(sans);

        // First inputs yield
        joined
            .next(Poll::Input(JoinEnvelope::new(0, 10)))
            .expect_yielded("should yield");
        joined
            .next(Poll::Input(JoinEnvelope::new(1, 20)))
            .expect_yielded("should yield");

        // Complete both
        joined
            .next(Poll::Input(JoinEnvelope::new(0, 100)))
            .expect_yielded("should yield");

        // Final completion
        match joined.next(Poll::Input(JoinEnvelope::new(1, 200))) {
            Step::Complete(Ok(results)) => {
                assert_eq!(results, vec![100, 200]);
            }
            other => panic!("Expected Complete, got {:?}", other),
        }
    }

    #[test]
    fn test_join_vec_empty() {
        use crate::build;
        #[allow(clippy::type_complexity)]
        let sans: Vec<build::Repeat<fn(i32) -> i32>> = vec![];
        let mut joined = join_vec(sans);

        // Should immediately complete with empty vec
        match joined.next(Poll::Poll) {
            Step::Complete(Ok(results)) => {
                assert_eq!(results, Vec::<i32>::new());
            }
            other => panic!("Expected Complete(Ok([])), got {:?}", other),
        }
    }

    #[test]
    fn test_join_vec_dynamic_size() {
        fn add_one(x: i32) -> i32 {
            x + 1
        }

        // Test with different sizes
        for size in 1..=10 {
            let sans: Vec<_> = (0..size).map(|_| repeat(add_one)).collect();
            let mut joined = join_vec(sans);

            // Send input to all
            for i in 0..size {
                let input_val = (i * 5) as i32;
                match joined.next(Poll::Input(JoinEnvelope::new(i, input_val))) {
                    Step::Yielded(PollOutput::Output(JoinEnvelope(id, val))) => {
                        assert_eq!(id.as_usize(), i);
                        assert_eq!(val, input_val + 1);
                    }
                    other => panic!("Expected Output at index {}, got {:?}", i, other),
                }
            }
        }
    }

    #[test]
    fn test_init_join_vec_basic() {
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let inits = vec![
            (100, repeat(add_one)),
            (200, repeat(add_one)),
            (300, repeat(add_one)),
        ];
        let mut joined = init_join_vec(inits);

        // Should have initial outputs available
        let mut found = [false, false, false];
        for _ in 0..3 {
            match joined.next(Poll::Poll) {
                Step::Yielded(PollOutput::Output(JoinEnvelope(id, val))) => {
                    let idx = id.as_usize();
                    assert!(idx < 3);
                    found[idx] = true;
                    assert!(val == 100 || val == 200 || val == 300);
                }
                other => panic!("Expected Output, got {:?}", other),
            }
        }
        assert!(
            found.iter().all(|&x| x),
            "All initial outputs should be found"
        );
    }

    #[test]
    fn test_join_vec_interleaved() {
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let sans = vec![repeat(add_one), repeat(add_one)];
        let mut joined = join_vec(sans);

        // Interleave operations
        for round in 0..3 {
            for idx in 0..2 {
                let input_val = (round * 10 + idx) as i32;
                match joined.next(Poll::Input(JoinEnvelope::new(idx, input_val))) {
                    Step::Yielded(PollOutput::Output(JoinEnvelope(id, val))) => {
                        assert_eq!(id.as_usize(), idx);
                        assert_eq!(val, input_val + 1);
                    }
                    other => panic!("Expected Output, got {:?}", other),
                }
            }
        }
    }

    #[test]
    fn test_join_vec_large_collection() {
        fn multiply_two(x: i32) -> i32 {
            x * 2
        }
        let sans: Vec<_> = (0..100).map(|_| repeat(multiply_two)).collect();
        let mut joined = join_vec(sans);

        // Send input to first 10
        for i in 0..10 {
            let input_val = i as i32;
            match joined.next(Poll::Input(JoinEnvelope::new(i, input_val))) {
                Step::Yielded(PollOutput::Output(JoinEnvelope(id, val))) => {
                    assert_eq!(id.as_usize(), i);
                    assert_eq!(val, input_val * 2);
                }
                other => panic!("Expected Output, got {:?}", other),
            }
        }
    }
}
