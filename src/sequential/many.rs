//! Run an array of coroutines in order.

use crate::{Sans, Step};

/// Run an array of coroutines, passing each final result to the next as input.
///
/// All coroutines must have the same type and return their input type.
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
/// assert_eq!(coro.next(5).unwrap_yielded(), 15);
/// assert_eq!(coro.next(7).unwrap_yielded(), 14);
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

/// An array of coroutines connected by [`many`].
///
/// Each completed coroutine is dropped before the next begins. After all finish,
/// `next` returns [`Step::Complete`] with the supplied input.
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
        while let Some(slot) = self.states.get_mut(self.index) {
            let child = slot.as_mut().expect("the active coroutine is present");
            match child.next(input) {
                Step::Yielded(output) => return Step::Yielded(output),
                Step::Complete(result) => {
                    // Slots before index are empty; the active and later slots are present.
                    *slot = None;
                    self.index += 1;
                    input = result;
                }
            }
        }
        Step::Complete(input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build::once;
    use std::{cell::Cell, rc::Rc};

    struct TrackedChild {
        id: usize,
        yielded: bool,
        drops: Rc<Cell<usize>>,
    }

    impl Sans<i32, i32> for TrackedChild {
        type Return = i32;

        fn next(&mut self, input: i32) -> Step<i32, i32> {
            // Every earlier child must be released before this child is called.
            assert_eq!(self.drops.get(), self.id);
            if self.yielded {
                Step::Complete(input)
            } else {
                self.yielded = true;
                Step::Yielded(input)
            }
        }
    }

    impl Drop for TrackedChild {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    #[test]
    fn test_many_drops_completed_children_before_starting_next() {
        let drops = Rc::new(Cell::new(0));
        let mut coro = many(std::array::from_fn::<_, 2, _>(|id| TrackedChild {
            id,
            yielded: false,
            drops: Rc::clone(&drops),
        }));

        assert_eq!(coro.next(5).unwrap_yielded(), 5);
        assert_eq!(drops.get(), 0);
        assert_eq!(coro.next(7).unwrap_yielded(), 7);
        assert_eq!(drops.get(), 1);
        assert_eq!(coro.next(9).unwrap_complete(), 9);
        assert_eq!(drops.get(), 2);
        drop(coro);
        assert_eq!(drops.get(), 2);
    }

    #[test]
    fn test_many_drops_immediate_children_in_one_call() {
        let drops = Rc::new(Cell::new(0));
        let mut coro = many(std::array::from_fn::<_, 3, _>(|id| TrackedChild {
            id,
            yielded: true,
            drops: Rc::clone(&drops),
        }));

        assert_eq!(coro.next(42).unwrap_complete(), 42);
        assert_eq!(drops.get(), 3);
        assert_eq!(coro.next(99).unwrap_complete(), 99);
        assert_eq!(drops.get(), 3);
    }

    #[test]
    fn test_many_post_completion_returns_fresh_input() {
        let mut coro = many([once(|input: i32| input + 1)]);
        assert_eq!(coro.next(1).unwrap_yielded(), 2);
        assert_eq!(coro.next(3).unwrap_complete(), 3);
        assert_eq!(coro.next(4).unwrap_complete(), 4);
        assert_eq!(coro.next(5).unwrap_complete(), 5);
    }

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
