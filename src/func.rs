//! Building coroutines from scratch
//!
//! This module provides functions and types for creating new coroutines.
//! The implementations below were previously spread across submodules and have
//! been consolidated here for easier navigation.

use crate::{Sans, step::Step};

/// Wraps a closure so it implements [`Sans`].
pub struct FromFn<F>(F);

impl<I, O, D, F> Sans<I> for FromFn<F>
where
    F: FnMut(I) -> Step<O, D>,
{
    type Output = O;
    type Return = D;

    fn next(&mut self, input: I) -> Step<Self::Output, Self::Return> {
        (self.0)(input)
    }
}

/// Create a coroutine from a closure returning [`Step`].
///
/// ```rust
/// use sans::prelude::*;
///
/// let mut toggle = from_fn(|x: bool| {
///     if x { Step::Yielded(!x) } else { Step::Complete(x) }
/// });
/// assert_eq!(toggle.next(true).unwrap_yielded(), false);
/// assert_eq!(toggle.next(false).unwrap_complete(), false);
/// ```
pub fn from_fn<F>(f: F) -> FromFn<F> {
    FromFn(f)
}

/// Wraps a fallible closure so it implements [`Sans`].
pub struct TryFromFn<F>(F);

impl<I, O, D, E, F> Sans<I> for TryFromFn<F>
where
    F: FnMut(I) -> Result<Step<O, D>, E>,
{
    type Output = O;
    type Return = Result<D, E>;

    fn next(&mut self, input: I) -> Step<Self::Output, Self::Return> {
        match (self.0)(input) {
            Ok(Step::Yielded(output)) => Step::Yielded(output),
            Ok(Step::Complete(done)) => Step::Complete(Ok(done)),
            Err(error) => Step::Complete(Err(error)),
        }
    }
}

/// Create a coroutine from a fallible closure returning [`Result<Step<O, D>, E>`].
///
/// ```rust
/// use sans::prelude::*;
///
/// let mut fallible_toggle = try_from_fn(|x: bool| {
///     if x {
///         Ok(Step::Yielded(!x))
///     } else if x == false {
///         Err("Error on false input")
///     } else {
///         Ok(Step::Complete(x))
///     }
/// });
///
/// match fallible_toggle.next(true) {
///     Step::Yielded(output) => println!("Yielded: {}", output),
///     Step::Complete(Ok(result)) => println!("Completed successfully: {}", result),
///     Step::Complete(Err(error)) => println!("Failed with error: {}", error),
/// }
/// ```
pub fn try_from_fn<F>(f: F) -> TryFromFn<F> {
    TryFromFn(f)
}

/// Applies a function to each input, yielding results indefinitely.
///
/// Never completes on its own - will continue processing until externally stopped.
pub struct Repeat<F>(F);

impl<I, O, F> Sans<I> for Repeat<F>
where
    F: FnMut(I) -> O,
{
    type Output = O;
    type Return = I;

    fn next(&mut self, input: I) -> Step<Self::Output, Self::Return> {
        Step::Yielded(self.0(input))
    }
}

/// Create a coroutine that applies a function indefinitely.
///
/// ```rust
/// use sans::prelude::*;
///
/// let mut doubler = repeat(|x: i32| x * 2);
/// assert_eq!(doubler.next(5).unwrap_yielded(), 10);
/// assert_eq!(doubler.next(3).unwrap_yielded(), 6);
/// // Continues forever...
/// ```
pub fn repeat<I, O, F: FnMut(I) -> O>(f: F) -> Repeat<F> {
    Repeat(f)
}

/// Applies a function once, then completes on subsequent calls.
///
/// First call yields the function result, subsequent calls return `Done(input)`.
pub struct Once<F>(Option<F>);

/// Create a coroutine that applies a function once.
///
/// ```rust
/// use sans::prelude::*;
///
/// let mut coro = once(|x: i32| x + 10);
/// assert_eq!(coro.next(5).unwrap_yielded(), 15);
/// assert_eq!(coro.next(3).unwrap_complete(), 3); // Done
/// ```
pub fn once<F>(f: F) -> Once<F> {
    Once(Some(f))
}

impl<I, O, F> Sans<I> for Once<F>
where
    F: FnOnce(I) -> O,
{
    type Output = O;
    type Return = I;

    fn next(&mut self, input: I) -> Step<Self::Output, Self::Return> {
        match self.0.take() {
            Some(f) => Step::Yielded(f(input)),
            None => Step::Complete(input),
        }
    }
}
