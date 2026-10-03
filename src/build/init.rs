use super::func::{FromFn, Once, Repeat};
use crate::{Sans, Step};

/// Yield `output` on initialization, then continue with `coro`.
///
/// Returns the tuple `(output, coro)`, which implements [`InitSans`](crate::InitSans).
///
/// ```rust
/// use sans::prelude::*;
///
/// let coro = init(42, repeat(|x: i32| x + 1));
/// let (initial, mut cont) = coro.init().unwrap_yielded();
/// assert_eq!(initial, 42);
/// assert_eq!(cont.next(10).unwrap_yielded(), 11);
/// ```
pub fn init<I, O, S: Sans<I, O>>(output: O, coro: S) -> (O, S) {
    (output, coro)
}

/// Yield an initial output, then continue with [`once`](super::once).
pub fn init_once<I, O, F: FnOnce(I) -> O>(o: O, f: F) -> (O, Once<F>) {
    (o, super::func::once(f))
}

/// Yield an initial output, then continue with [`repeat`](super::repeat).
pub fn init_repeat<I, O, F: FnMut(I) -> O>(o: O, f: F) -> (O, Repeat<F>) {
    (o, super::func::repeat(f))
}

/// Yield an initial output, then continue with [`from_fn`](super::from_fn).
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
    fn test_init_returns_tuple_with_exact_continuation_type() {
        fn add_one(input: i32) -> i32 {
            input + 1
        }

        type Continuation = Repeat<fn(i32) -> i32>;
        let initializer: (i32, Continuation) =
            init(42, crate::build::repeat(add_one as fn(i32) -> i32));
        let (initial, mut continuation): (i32, Continuation) = initializer.init().unwrap_yielded();
        assert_eq!(initial, 42);
        assert_eq!(continuation.next(10).unwrap_yielded(), 11);
    }

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
