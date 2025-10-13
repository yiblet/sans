//! Running multiple coroutines sequentially.
//!
//! This module provides the [`Many`] combinator for executing an array of coroutines
//! in sequence, passing each return value to the next coroutine.

use crate::{Sans, Step};

/// Run multiple coroutines sequentially, passing the return value of each to the next.
///
/// Each coroutine must have `Return = I` so the return value can become the input
/// to the next coroutine. All coroutines must have the same type, which typically means
/// using function pointers rather than closures.
///
/// # Examples
///
/// ```
/// use sans::prelude::*;
/// use sans::sequential::many;
///
/// fn add_ten(x: i32) -> i32 { x + 10 }
/// fn mul_two(x: i32) -> i32 { x * 2 }
///
/// let mut coro = many([
///     once(add_ten as fn(i32) -> i32),
///     once(mul_two as fn(i32) -> i32),
/// ]);
///
/// // First coro: 5 + 10 = 15
/// assert_eq!(coro.next(5).unwrap_yielded(), 15);
/// // First completes with 7, second starts: 7 * 2 = 14
/// assert_eq!(coro.next(7).unwrap_yielded(), 14);
/// // Second completes with 20
/// assert_eq!(coro.next(20).unwrap_complete(), 20);
/// ```
pub fn many<const N: usize, I, O, S>(rest: [S; N]) -> Many<N, S>
where
    S: Sans<I, O, Return = I>,
{
    Many {
        states: rest.map(|r| Some(r)),
        index: 0,
    }
}

/// Executes an array of coroutines sequentially, passing each return value to the next.
///
/// Created via [`many`]. Coroutines are executed in order until all complete.
pub struct Many<const N: usize, S> {
    states: [Option<S>; N],
    index: usize,
}

impl<const N: usize, I, O, S> Sans<I, O> for Many<N, S>
where
    S: Sans<I, O, Return = I>,
{
    type Return = S::Return;
    fn next(&mut self, mut input: I) -> Step<O, Self::Return> {
        loop {
            match self.states.get_mut(self.index) {
                Some(Some(s)) => match s.next(input) {
                    Step::Yielded(o) => return Step::Yielded(o),
                    Step::Complete(a) => {
                        self.index += 1;
                        input = a;
                    }
                },
                Some(None) => {
                    self.index += 1;
                }
                None => return Step::Complete(input),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build::once;

    #[test]
    fn test_many_empty_array() {
        // Empty array should immediately complete with input
        #[allow(clippy::type_complexity)]
        let mut coro: Many<0, crate::build::Once<fn(i32) -> i32>> = many([]);

        assert_eq!(coro.next(42).unwrap_complete(), 42);
    }

    #[test]
    fn test_many_single_coroutine() {
        // Single coroutine: yields once, completes with next input
        fn add_ten(x: i32) -> i32 {
            x + 10
        }
        let mut coro = many([once(add_ten)]);

        // First input: yields 5 + 10 = 15
        assert_eq!(coro.next(5).unwrap_yielded(), 15);
        // Second input: once completes with 20, many completes with 20
        assert_eq!(coro.next(20).unwrap_complete(), 20);
    }

    #[test]
    fn test_many_two_coroutines() {
        // Two coroutines chained: first yields, completes, then second yields, completes
        // Need to use function pointers to make types match
        fn add_ten(x: i32) -> i32 {
            x + 10
        }
        fn mul_two(x: i32) -> i32 {
            x * 2
        }
        let mut coro = many([
            once(add_ten as fn(i32) -> i32),
            once(mul_two as fn(i32) -> i32),
        ]);

        // First coroutine: 5 + 10 = 15 (yielded)
        assert_eq!(coro.next(5).unwrap_yielded(), 15);
        // First coroutine completes with 7, second coroutine starts with 7
        // Second coroutine: 7 * 2 = 14 (yielded)
        assert_eq!(coro.next(7).unwrap_yielded(), 14);
        // Second coroutine completes with 20, many completes with 20
        assert_eq!(coro.next(20).unwrap_complete(), 20);
    }

    #[test]
    fn test_many_three_coroutines() {
        // Three coroutines: add 10, multiply by 2, add 100
        fn add_ten(x: i32) -> i32 {
            x + 10
        }
        fn mul_two(x: i32) -> i32 {
            x * 2
        }
        fn add_hundred(x: i32) -> i32 {
            x + 100
        }
        let mut coro = many([
            once(add_ten as fn(i32) -> i32),
            once(mul_two as fn(i32) -> i32),
            once(add_hundred as fn(i32) -> i32),
        ]);

        // Coroutine 1: 5 + 10 = 15
        assert_eq!(coro.next(5).unwrap_yielded(), 15);
        // Coroutine 1 completes with 6, coroutine 2 starts: 6 * 2 = 12
        assert_eq!(coro.next(6).unwrap_yielded(), 12);
        // Coroutine 2 completes with 7, coroutine 3 starts: 7 + 100 = 107
        assert_eq!(coro.next(7).unwrap_yielded(), 107);
        // Coroutine 3 completes with 8, many completes with 8
        assert_eq!(coro.next(8).unwrap_complete(), 8);
    }

    #[test]
    fn test_many_passes_return_to_next_coro() {
        // Verify that the return value of one coroutine becomes input to next
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        fn mul_ten(x: i32) -> i32 {
            x * 10
        }
        let mut coro = many([
            once(add_one as fn(i32) -> i32),
            once(mul_ten as fn(i32) -> i32),
        ]);

        // Coroutine 1: yields 5 + 1 = 6
        assert_eq!(coro.next(5).unwrap_yielded(), 6);
        // Coroutine 1 completes with return=100, coroutine 2 receives 100
        // Coroutine 2: yields 100 * 10 = 1000
        assert_eq!(coro.next(100).unwrap_yielded(), 1000);
        // Coroutine 2 completes with 50
        assert_eq!(coro.next(50).unwrap_complete(), 50);
    }

    #[test]
    fn test_many_large_array() {
        // Test with 5 coroutines
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let f = add_one as fn(i32) -> i32;
        let mut coro = many([once(f), once(f), once(f), once(f), once(f)]);

        // Each coroutine yields x+1, then completes with next input
        assert_eq!(coro.next(0).unwrap_yielded(), 1); // 0+1
        assert_eq!(coro.next(10).unwrap_yielded(), 11); // 10+1
        assert_eq!(coro.next(20).unwrap_yielded(), 21); // 20+1
        assert_eq!(coro.next(30).unwrap_yielded(), 31); // 30+1
        assert_eq!(coro.next(40).unwrap_yielded(), 41); // 40+1
        assert_eq!(coro.next(50).unwrap_complete(), 50); // Final completion
    }
}
