//! Chaining coroutines sequentially.
//!
//! This module provides the [`Chain`] and [`AndThen`] combinators for running
//! coroutines one after another.

use crate::{Sans, step::Step};

/// A coroutine that runs one coroutine to completion, then uses its return value
/// to create and run a second coroutine.
///
/// This is similar to a monadic bind operation. The first coroutine runs until it
/// completes, then its return value is passed to a function that produces the
/// second coroutine as a `(O, S2)` tuple.
///
/// Created via the [`and_then`] function.
///
/// # Type Parameters
///
/// * `S1` - The type of the first coroutine
/// * `S2` - The type of the second coroutine
/// * `F` - The function type that creates the second coroutine from the first's return value
pub struct AndThen<S1, S2, F> {
    state: AndThenState<S1, S2, F>,
}

impl<I, L, R, F> Sans<I> for AndThen<L, R, F>
where
    L: Sans<I>,
    R: Sans<I, Output = L::Output>,
    F: FnOnce(L::Return) -> (L::Output, R),
{
    type Output = L::Output;
    type Return = R::Return;
    fn next(&mut self, input: I) -> Step<Self::Output, Self::Return> {
        self.state.next(input)
    }
}

enum AndThenState<S1, S2, F> {
    OnFirst(S1, Option<F>),
    OnSecond(S2),
}

impl<I, L, R, F> Sans<I> for AndThenState<L, R, F>
where
    L: Sans<I>,
    R: Sans<I, Output = L::Output>,
    F: FnOnce(L::Return) -> (L::Output, R),
{
    type Output = L::Output;
    type Return = R::Return;
    fn next(&mut self, input: I) -> Step<Self::Output, Self::Return> {
        match self {
            AndThenState::OnFirst(l, f) => match l.next(input) {
                Step::Yielded(o) => Step::Yielded(o),
                Step::Complete(a) => {
                    let (o, next_r) = f.take().expect("AndThen::can only be used once")(a);
                    *self = AndThenState::OnSecond(next_r);
                    Step::Yielded(o)
                }
            },
            AndThenState::OnSecond(r) => r.next(input),
        }
    }
}

/// Chains two coroutines where the second coroutine is created from the first coroutine's return value.
///
/// This is a monadic bind operation for coroutines. The first coroutine runs to completion,
/// then its return value is passed to a function `f` that creates the second coroutine.
///
/// **Important:** The function `f` must return a `(O, R)` tuple where `O` is the output to yield
/// and `R` is the continuation coroutine:
///
/// ```rust
/// use sans::prelude::*;
///
/// let mut coro = once(|x: i32| x * 2)
///     .and_then(|val| (val * 10, repeat(move |x| x + val)));
/// ```
///
/// # Arguments
///
/// * `l` - The first coroutine to run
/// * `f` - A function that takes the first coroutine's return value and produces a `(O, R)` tuple
///
/// # Returns
///
/// An [`AndThen`] continuation that runs both coroutines in sequence.
///
/// # Examples
///
/// ```
/// use sans::prelude::*;
/// use sans::compose::and_then;
///
/// // First coro yields once, then completes with a return value
/// // Second coro uses that return value to configure its behavior
/// let mut coro = and_then(
///     once(|x: i32| x * 2),  // yields x*2, returns next input
///     |return_val| (return_val * 10, repeat(move |y: i32| y + return_val)),
/// );
///
/// // First coro yields 5 * 2 = 10
/// assert_eq!(coro.next(5).unwrap_yielded(), 10);
///
/// // First coro completes with return value = 7
/// // Second coro initializes with (7*10, ...) and yields 70
/// assert_eq!(coro.next(7).unwrap_yielded(), 70);
///
/// // Second coro continues: 3 + 7 = 10
/// assert_eq!(coro.next(3).unwrap_yielded(), 10);
/// ```
pub fn and_then<I, L, R, F>(l: L, f: F) -> AndThen<L, R, F>
where
    L: Sans<I>,
    R: Sans<I, Output = L::Output>,
    F: FnOnce(L::Return) -> (L::Output, R),
{
    AndThen {
        state: AndThenState::OnFirst(l, Some(f)),
    }
}

/// Run the first coroutine to completion, then feed its result to the second.
///
/// The first coroutine's `Done` value becomes the input to the second coroutine.
/// Both coroutines must yield the same type.
pub fn chain<I, L, R>(l: L, r: R) -> Chain<L, R>
where
    L: Sans<I>,
    L::Return: Into<I>,
    R: Sans<I, Output = L::Output>,
{
    Chain(Some(l), r)
}

/// Chains two coroutines sequentially.
///
/// Created via `chain()` or `first_chain()`. The first coroutine is dropped from memory
/// once it completes to free resources.
pub struct Chain<S1, S2>(Option<S1>, S2);

impl<I, L, R> Sans<I> for Chain<L, R>
where
    L: Sans<I>,
    L::Return: Into<I>,
    R: Sans<I, Output = L::Output>,
{
    type Output = L::Output;
    type Return = R::Return;
    fn next(&mut self, input: I) -> Step<Self::Output, Self::Return> {
        match self.0 {
            Some(ref mut l) => match l.next(input) {
                Step::Yielded(o) => Step::Yielded(o),
                Step::Complete(a) => {
                    self.0 = None; // we drop the old coro when it's done
                    self.1.next(a.into())
                }
            },
            None => self.1.next(input),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::func::{once, repeat};

    #[test]
    fn test_chain_switches_to_second_coroutine_after_first_done() {
        let mut coro = chain(once(|val: u32| val + 1), repeat(|val: u32| val * 2));

        assert_eq!(coro.next(3).unwrap_yielded(), 4);
        assert_eq!(coro.next(4).unwrap_yielded(), 8);
        assert_eq!(coro.next(5).unwrap_yielded(), 10);
    }

    #[test]
    fn test_chain_propagates_done_from_second_coroutine() {
        let mut coro = chain(once(|val: u32| val + 1), once(|val: u32| val * 2));

        assert_eq!(coro.next(2).unwrap_yielded(), 3);
        assert_eq!(coro.next(3).unwrap_yielded(), 6);
        assert_eq!(coro.next(4).unwrap_complete(), 4);
    }

    #[test]
    fn test_and_then_basic() {
        // First coroutine yields once then completes with a computed value
        // and_then uses the RETURN value of first coroutine to create second coroutine
        let mut coro = and_then(
            once(|x: i32| x * 2), // yields x*2, then completes with next input
            |return_val| (return_val * 10, repeat(move |y: i32| y + return_val)),
        );

        // First: 5 * 2 = 10 (yielded)
        assert_eq!(coro.next(5).unwrap_yielded(), 10);
        // Second: once completes with return value = 7
        // and_then creates second coroutine with return_val=7
        // Second coroutine initializes: (7*10, ...) = (70, ...)
        // Yields 70
        assert_eq!(coro.next(7).unwrap_yielded(), 70);
        // Second coroutine continues: 3 + 7 = 10
        assert_eq!(coro.next(3).unwrap_yielded(), 10);
        // 5 + 7 = 12
        assert_eq!(coro.next(5).unwrap_yielded(), 12);
    }

    #[test]
    fn test_and_then_first_coro_yields_multiple() {
        // First coroutine yields twice before completing
        use crate::func::from_fn;
        let mut count = 0;
        let first = from_fn(move |x: i32| {
            count += 1;
            if count <= 2 {
                Step::Yielded(x * count)
            } else {
                Step::Complete(count)
            }
        });

        let mut coro = and_then(first, |final_count| {
            (final_count * 100, once(move |x: i32| x + final_count))
        });

        // First yields
        assert_eq!(coro.next(5).unwrap_yielded(), 5); // 5 * 1
        assert_eq!(coro.next(5).unwrap_yielded(), 10); // 5 * 2
        // Now first completes with count=3, second coroutine initializes with (300, ...)
        assert_eq!(coro.next(0).unwrap_yielded(), 300); // Initial yield 3 * 100
        // Second coroutine continues: 10 + 3 = 13
        assert_eq!(coro.next(10).unwrap_yielded(), 13); // 10 + 3
        // Second coroutine (once) completes
        assert_eq!(coro.next(20).unwrap_complete(), 20);
    }

    #[test]
    fn test_and_then_with_init_sans() {
        // Second coroutine has initial yield
        let mut coro = and_then(once(|x: i32| x + 1), |result| {
            (result * 10, repeat(move |y: i32| y + result))
        });

        // First coroutine: 5 + 1 = 6 (yielded)
        assert_eq!(coro.next(5).unwrap_yielded(), 6);
        // First coroutine completes with result = 8
        // Second coroutine initializes with (8 * 10, ...) = (80, ...)
        // Yields the initial value 80
        assert_eq!(coro.next(8).unwrap_yielded(), 80);
        // Now the repeat continues: y + result = 3 + 8 = 11
        assert_eq!(coro.next(3).unwrap_yielded(), 11);
    }

    #[test]
    fn test_and_then_completes_immediately() {
        // First coroutine completes on first input, second coroutine yields once then completes
        let mut coro = and_then(once(|x: i32| x * 2), |val| {
            (val + 100, once(move |x: i32| x + val))
        });

        // First: 5 * 2 = 10 (yielded)
        assert_eq!(coro.next(5).unwrap_yielded(), 10);
        // First completes with val = 12
        // Second initializes with (12 + 100, once(...)) = (112, ...)
        // Yields 112
        assert_eq!(coro.next(12).unwrap_yielded(), 112);
        // Second continues: 20 + 12 = 32
        assert_eq!(coro.next(20).unwrap_yielded(), 32);
        // Second completes with 7
        assert_eq!(coro.next(7).unwrap_complete(), 7);
    }
}
