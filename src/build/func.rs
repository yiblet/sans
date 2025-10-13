//! Function-based coroutine builders.
//!
//! This module provides basic building blocks for creating coroutines from functions.

use crate::{Sans, step::Step};

pub struct FromFn<F>(F);

impl<I, O, D, F> Sans<I, O> for FromFn<F>
where
    F: FnMut(I) -> Step<O, D>,
{
    type Return = D;
    fn next(&mut self, input: I) -> Step<O, Self::Return> {
        (self.0)(input)
    }
}

/// Create a coroutine from a closure.
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

/// Applies a function to each input, yielding results indefinitely.
///
/// Never completes on its own - will continue processing until externally stopped.
pub struct Repeat<F>(F);

impl<I, O, F> Sans<I, O> for Repeat<F>
where
    F: FnMut(I) -> O,
{
    type Return = I;
    fn next(&mut self, input: I) -> Step<O, Self::Return> {
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

impl<I, O, F> Sans<I, O> for Once<F>
where
    F: FnOnce(I) -> O,
{
    type Return = I;
    fn next(&mut self, input: I) -> Step<O, Self::Return> {
        match self.0.take() {
            Some(f) => Step::Yielded(f(input)),
            None => Step::Complete(input),
        }
    }
}
