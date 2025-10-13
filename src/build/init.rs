//! Builders for coroutines with initial output.
//!
//! This module provides functions for creating [`InitSans`](crate::InitSans) coroutines
//! that yield an initial value before processing input.

use super::func::{FromFn, Once, Repeat};
use crate::{Sans, Step};

/// Create an `InitSans` by pairing an initial output with a continuation.
///
/// This is a convenience function for wrapping a `Sans` in the tuple form required
/// by APIs like [`and_then`](crate::Sans::and_then). Instead of manually writing
/// `(output, continuation)`, you can use `init(output, continuation)`.
///
/// # Examples
///
/// ```rust
/// use sans::prelude::*;
///
/// // Using init() with and_then
/// let mut coro = once(|x: i32| x * 2)
///     .and_then(|val| init(val * 10, repeat(move |x| x + val)));
///
/// assert_eq!(coro.next(5).unwrap_yielded(), 10);  // First coro: 5 * 2
/// assert_eq!(coro.next(7).unwrap_yielded(), 70);  // Second coro init: 7 * 10
/// assert_eq!(coro.next(3).unwrap_yielded(), 10);  // Second coro: 3 + 7
/// ```
pub fn init<I, O, S>(output: O, continuation: S) -> (O, S)
where
    S: Sans<I, O>,
{
    (output, continuation)
}

/// Yield an initial value, then apply a function once.
///
/// Combines immediate output with single function application.
pub fn init_once<I, O, F: FnOnce(I) -> O>(o: O, f: F) -> (O, Once<F>) {
    (o, super::func::once(f))
}

/// Yield an initial value, then apply a function indefinitely.
///
/// Useful for generators that need to emit a seed value before starting their loop.
pub fn init_repeat<I, O, F: FnMut(I) -> O>(o: O, f: F) -> (O, Repeat<F>) {
    (o, super::func::repeat(f))
}

/// Yield an initial value, then create a coroutine from a closure.
///
/// ```rust
/// use sans::prelude::*;
///
/// let mut counter = 0;
/// let (initial, mut coro) = init_from_fn(42, move |x: i32| {
///     counter += 1;
///     if counter < 3 { Step::Yielded(x * counter) } else { Step::Complete(x + counter) }
/// });
/// assert_eq!(initial, 42);
/// assert_eq!(coro.next(10).unwrap_yielded(), 10);
/// assert_eq!(coro.next(10).unwrap_yielded(), 20);
/// assert_eq!(coro.next(10).unwrap_complete(), 13);
/// ```
pub fn init_from_fn<I, O, D, F>(initial: O, f: F) -> (O, FromFn<F>)
where
    F: FnMut(I) -> Step<O, D>,
{
    (initial, super::func::from_fn(f))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InitSans, Sans};
    use std::mem::size_of_val;

    #[test]
    fn test_simple_addition() {
        let mut prev = 1;
        let fib = init_repeat(1, move |n: u128| {
            let next = prev + n;
            prev = next;
            next
        });

        let (_, mut next) = fib.init().unwrap_yielded();
        for i in 1..11 {
            let cur = next.next(1).unwrap_yielded();
            assert_eq!(i + 1, cur);
        }
    }

    #[test]
    fn test_simple_divider() {
        let mut start = 101323012313805546028676730784521326u128;
        let divider = init_repeat(start, |divisor: u128| {
            start /= divisor;
            start
        });

        assert_eq!(size_of_val(&divider), 32);
        let (mut cur, mut next) = divider.init().unwrap_yielded();
        for i in 2..20 {
            let next_cur = next.next(i).unwrap_yielded();
            assert_eq!(cur / i, next_cur);
            cur = next_cur;
        }
    }
}
