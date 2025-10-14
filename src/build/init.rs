//! Builders for coroutines with initial output.
//!
//! This module provides functions for creating [`InitSans`](crate::InitSans) coroutines
//! that yield an initial value before processing input.

use std::marker::PhantomData;

use super::func::{FromFn, Once, Repeat};
use crate::{InitSans, Sans, Step};

/// A wrapper that adds an initial output value to any `Sans` coroutine.
///
/// This struct allows you to create an `InitSans` from any existing `Sans` coroutine
/// by providing an initial value that will be yielded before the wrapped coroutine
/// begins processing input.
///
/// # Type Parameters
///
/// * `I` - The input type for the wrapped coroutine
/// * `O` - The output type (both for initial value and wrapped coroutine)
/// * `S` - The wrapped `Sans` coroutine type
pub struct Init<I, O, S: Sans<I, O>>(O, S, PhantomData<I>);

impl<I, O, S> InitSans<I, O> for Init<I, O, S>
where
    S: Sans<I, O>,
{
    type Next = S;
    type Return = S::Return;

    fn init(self) -> Step<(O, S), Self::Return> {
        Step::Yielded((self.0, self.1))
    }
}

/// Creates an `InitSans` coroutine from an initial output value and a `Sans` coroutine.
///
/// **Deprecated:** Use the builder API instead: `yielding(output).then(coro)`
///
/// This is a convenience function for constructing an `Init` wrapper that yields
/// the provided output value first, then continues with the given coroutine.
///
/// # Parameters
///
/// * `output` - The initial value to yield before processing any input
/// * `coro` - The `Sans` coroutine to wrap
///
/// # Returns
///
/// An `Init` wrapper that implements `InitSans`
///
/// # Examples
///
/// ```rust
/// use sans::prelude::*;
///
/// let coro = init(42, repeat(|x: i32| x + 1));
/// let (initial, mut cont) = coro.init().unwrap_yielded();
/// assert_eq!(initial, 42);
/// assert_eq!(cont.next(10).unwrap_yielded(), 11);
/// ```
#[deprecated(
    since = "0.2.0",
    note = "Use the builder API: yielding(output).then(coro)"
)]
pub fn init<I, O, S: Sans<I, O>>(output: O, coro: S) -> Init<I, O, S> {
    Init(output, coro, PhantomData)
}

/// Yield an initial value, then apply a function once.
///
/// **Deprecated:** Use the builder API instead: `yielding(o).then(once(f))`
///
/// Combines immediate output with single function application.
#[deprecated(
    since = "0.2.0",
    note = "Use the builder API: yielding(o).then(once(f))"
)]
pub fn init_once<I, O, F: FnOnce(I) -> O>(o: O, f: F) -> (O, Once<F>) {
    (o, super::func::once(f))
}

/// Yield an initial value, then apply a function indefinitely.
///
/// **Deprecated:** Use the builder API instead: `yielding(o).then(repeat(f))`
///
/// Useful for generators that need to emit a seed value before starting their loop.
#[deprecated(
    since = "0.2.0",
    note = "Use the builder API: yielding(o).then(repeat(f))"
)]
pub fn init_repeat<I, O, F: FnMut(I) -> O>(o: O, f: F) -> (O, Repeat<F>) {
    (o, super::func::repeat(f))
}

/// Yield an initial value, then create a coroutine from a closure.
///
/// **Deprecated:** Use the builder API instead: `yielding(initial).then(from_fn(f))`
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
#[deprecated(
    since = "0.2.0",
    note = "Use the builder API: yielding(initial).then(from_fn(f))"
)]
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
