//! Connect coroutines in sequence.

use super::sequence::Sequence;
use crate::{InitSans, Sans, step::Step};

/// Create the next coroutine from the final result. See [`Sans::and_then`].
pub struct AndThen<S1, S2, F> {
    state: Sequence<S1, S2, F>,
}

impl<I, O, L, R, F> Sans<I, O> for AndThen<L, R::Next, F>
where
    L: Sans<I, O>,
    R: InitSans<I, O>,
    R::Next: Sans<I, O>,
    F: FnOnce(L::Return) -> R,
{
    type Return = <R::Next as Sans<I, O>>::Return;
    fn next(&mut self, input: I) -> Step<O, Self::Return> {
        self.state
            .next(input, |f, value| f(value).init(), |value| value)
    }
}

/// Create the next coroutine from the final result. See [`Sans::and_then`] for an example.
pub fn and_then<I, O, L, R, F>(l: L, f: F) -> AndThen<L, R::Next, F>
where
    L: Sans<I, O>,
    R: InitSans<I, O>,
    R::Next: Sans<I, O>,
    F: FnOnce(L::Return) -> R,
{
    AndThen {
        state: Sequence::OnFirst(l, Some(f)),
    }
}

/// Pass the first coroutine's final result to the second as its first input.
///
/// See [`Sans::chain`].
pub fn chain<I, O, L, R>(l: L, r: R) -> Chain<L, R>
where
    L: Sans<I, O, Return = I>,
    R: Sans<I, O>,
{
    Chain(Some(l), r)
}

/// The [`chain`] constructor for an [`InitSans`].
pub fn init_chain<I, O, L, R>(l: L, r: R) -> Chain<L, R>
where
    L: InitSans<I, O>,
    R: Sans<I, O>,
    L::Next: Sans<I, O, Return = I>,
{
    Chain(Some(l), r)
}

/// Two coroutines connected by [`chain`] or [`init_chain`].
///
/// The first coroutine is dropped when it completes.
pub struct Chain<S1, S2>(Option<S1>, S2);

impl<I, O, L, R> Sans<I, O> for Chain<L, R>
where
    L: Sans<I, O, Return = I>,
    R: Sans<I, O>,
{
    type Return = R::Return;
    fn next(&mut self, input: I) -> Step<O, Self::Return> {
        match self.0 {
            Some(ref mut l) => match l.next(input) {
                Step::Yielded(o) => Step::Yielded(o),
                Step::Complete(a) => {
                    self.0 = None; // we drop the old coro when it's done
                    self.1.next(a)
                }
            },
            None => self.1.next(input),
        }
    }
}

impl<I, O, L, R> InitSans<I, O> for Chain<L, R>
where
    L: InitSans<I, O>,
    R: Sans<I, O>,
    L::Next: Sans<I, O, Return = I>,
{
    type Next = either::Either<Chain<L::Next, R>, R>;

    fn init(mut self) -> Step<(O, Self::Next), R::Return> {
        match self.0.take().expect("Chain left side must be Some").init() {
            Step::Yielded((o, next)) => {
                Step::Yielded((o, either::Either::Left(Chain(Some(next), self.1))))
            }
            Step::Complete(d) => match self.1.next(d) {
                Step::Yielded(o) => Step::Yielded((o, either::Either::Right(self.1))),
                Step::Complete(r) => Step::Complete(r),
            },
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::build::{once, repeat};

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
        use crate::build::from_fn;
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
