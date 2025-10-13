//! Transforming coroutine inputs, outputs, and return values.
//!
//! This module provides [`MapInput`], [`MapYield`], and [`MapReturn`] combinators
//! for adapting coroutines to different types.

use crate::{InitSans, Sans, step::Step};

/// Transforms input before passing it to the wrapped coroutine.
///
/// Useful for adapting between different input types or preprocessing data.
pub struct MapInput<S, F> {
    f: F,
    coro: S,
}

/// Create a coroutine that transforms input before passing it to the wrapped coroutine.
///
/// # Examples
///
/// ```
/// use sans::prelude::*;
///
/// let coro = repeat(|x: i32| x * 2);
/// let mut mapped = map_input(|s: &str| s.parse::<i32>().unwrap(), coro);
///
/// assert_eq!(mapped.next("5").unwrap_yielded(), 10);
/// ```
pub fn map_input<S, F>(f: F, coro: S) -> MapInput<S, F> {
    MapInput { f, coro }
}

/// Create a MapInput from an InitSans coroutine.
///
/// This is used when applying input transformation to a coroutine that yields immediately.
pub fn init_map_input<I1, I2, O, S, F>(f: F, coro: S) -> MapInput<S, F>
where
    S: InitSans<I2, O>,
    F: FnMut(I1) -> I2,
{
    MapInput { f, coro }
}

impl<I1, I2, O, S, F> Sans<I1, O> for MapInput<S, F>
where
    S: Sans<I2, O>,
    F: FnMut(I1) -> I2,
{
    type Return = S::Return;
    fn next(&mut self, input: I1) -> Step<O, Self::Return> {
        let i2 = (self.f)(input);
        self.coro.next(i2)
    }
}

impl<I1, I2, O, S, F> InitSans<I1, O> for MapInput<S, F>
where
    S: InitSans<I2, O>,
    F: FnMut(I1) -> I2,
{
    type Next = MapInput<S::Next, F>;
    type Return = S::Return;

    fn init(self) -> Step<(O, Self::Next), Self::Return> {
        match self.coro.init() {
            Step::Yielded((o, next)) => Step::Yielded((
                o,
                MapInput {
                    f: self.f,
                    coro: next,
                },
            )),
            Step::Complete(d) => Step::Complete(d),
        }
    }
}

/// Transforms yielded values from the wrapped coroutine.
///
/// Allows converting or formatting output without changing the underlying computation.
pub struct MapYield<S, F, I, O1> {
    f: F,
    coro: S,
    _phantom: std::marker::PhantomData<(I, O1)>,
}

/// Create a coroutine that transforms yielded values from the wrapped coroutine.
///
/// # Examples
///
/// ```
/// use sans::prelude::*;
///
/// let coro = repeat(|x: i32| x * 2);
/// let mut mapped = map_yield(|y: i32| y.to_string(), coro);
///
/// assert_eq!(mapped.next(5).unwrap_yielded(), "10");
/// ```
pub fn map_yield<I, O1, O2, S, F>(f: F, coro: S) -> MapYield<S, F, I, O1>
where
    S: Sans<I, O1>,
    F: FnMut(O1) -> O2,
{
    MapYield {
        f,
        coro,
        _phantom: std::marker::PhantomData,
    }
}

/// Create a MapYield from an InitSans coroutine.
///
/// This is used when applying yield transformation to a coroutine that yields immediately.
pub fn init_map_yield<I, O1, O2, S, F>(f: F, coro: S) -> MapYield<S, F, I, O1>
where
    S: InitSans<I, O1>,
    F: FnMut(O1) -> O2,
{
    MapYield {
        f,
        coro,
        _phantom: std::marker::PhantomData,
    }
}

impl<I, O1, O2, S, F> Sans<I, O2> for MapYield<S, F, I, O1>
where
    S: Sans<I, O1>,
    F: FnMut(O1) -> O2,
{
    type Return = S::Return;
    fn next(&mut self, input: I) -> Step<O2, Self::Return> {
        match self.coro.next(input) {
            Step::Yielded(o1) => Step::Yielded((self.f)(o1)),
            Step::Complete(a) => Step::Complete(a),
        }
    }
}

impl<I, O1, O2, S, F> InitSans<I, O2> for MapYield<S, F, I, O1>
where
    S: InitSans<I, O1>,
    F: FnMut(O1) -> O2,
{
    type Next = MapYield<S::Next, F, I, O1>;
    type Return = S::Return;

    fn init(self) -> Step<(O2, Self::Next), Self::Return> {
        match self.coro.init() {
            Step::Yielded((o1, next)) => {
                let mut f = self.f;
                let o2 = f(o1);
                Step::Yielded((
                    o2,
                    MapYield {
                        f,
                        coro: next,
                        _phantom: std::marker::PhantomData,
                    },
                ))
            }
            Step::Complete(d) => Step::Complete(d),
        }
    }
}

/// Transforms the final result from the wrapped coroutine.
///
/// Applied only when the computation completes, not to intermediate yields.
pub struct MapReturn<S, F> {
    f: F,
    coro: S,
}

/// Create a coroutine that transforms the final result from the wrapped coroutine.
///
/// # Examples
///
/// ```
/// use sans::prelude::*;
///
/// let coro = once(|x: i32| x + 5);
/// let mut mapped = map_return(|r: i32| r * 10, coro);
///
/// // Yield is not transformed
/// assert_eq!(mapped.next(10).unwrap_yielded(), 15);
/// // Return is transformed: 20 * 10 = 200
/// assert_eq!(mapped.next(20).unwrap_complete(), 200);
/// ```
pub fn map_return<S, F>(f: F, coro: S) -> MapReturn<S, F> {
    MapReturn { f, coro }
}

/// Create a MapReturn from an InitSans coroutine.
///
/// This is used when applying return transformation to a coroutine that yields immediately.
pub fn init_map_return<I, O, D1, D2, S, F>(f: F, coro: S) -> MapReturn<S, F>
where
    S: InitSans<I, O, Return = D1>,
    F: FnMut(D1) -> D2,
{
    MapReturn { f, coro }
}

impl<I, O, D1, D2, S, F> Sans<I, O> for MapReturn<S, F>
where
    S: Sans<I, O, Return = D1>,
    F: FnMut(D1) -> D2,
{
    type Return = D2;
    fn next(&mut self, input: I) -> Step<O, Self::Return> {
        match self.coro.next(input) {
            Step::Yielded(o) => Step::Yielded(o),
            Step::Complete(r1) => Step::Complete((self.f)(r1)),
        }
    }
}

impl<I, O, D1, D2, S, F> InitSans<I, O> for MapReturn<S, F>
where
    S: InitSans<I, O, Return = D1>,
    F: FnMut(D1) -> D2,
{
    type Next = MapReturn<S::Next, F>;
    type Return = D2;

    fn init(self) -> Step<(O, Self::Next), Self::Return> {
        match self.coro.init() {
            Step::Yielded((o, next)) => Step::Yielded((
                o,
                MapReturn {
                    f: self.f,
                    coro: next,
                },
            )),
            Step::Complete(d1) => {
                let mut f = self.f;
                Step::Complete(f(d1))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InitSans;
    use crate::build::repeat;

    #[test]
    fn test_map_input_and_map_yield_pipeline() {
        let mut total = 0_i64;
        let init_coro = (
            0_i64,
            repeat(move |delta: i64| {
                total += delta;
                total
            }),
        )
            .map_input(|cmd: &str| -> i64 {
                let mut parts = cmd.split_whitespace();
                let op = parts.next().expect("operation must exist");
                let amount: i64 = parts
                    .next()
                    .expect("amount must exist")
                    .parse()
                    .expect("amount must parse");
                match op {
                    "add" => amount,
                    "sub" => -amount,
                    _ => panic!("unsupported op: {op}"),
                }
            })
            .map_yield(|value: i64| format!("total={value}"));

        let (initial_total, mut coro) = init_coro.init().unwrap_yielded();

        assert_eq!("total=0", initial_total);
        assert_eq!("total=5", coro.next("add 5").unwrap_yielded());
        assert_eq!("total=2", coro.next("sub 3").unwrap_yielded());
        assert_eq!("total=7", coro.next("add 5").unwrap_yielded());
    }

    #[test]
    fn test_map_input_basic() {
        use crate::build::repeat;
        let coro = repeat(|x: i32| x * 2);
        let mut mapped = map_input(|s: &str| s.parse::<i32>().unwrap(), coro);

        assert_eq!(mapped.next("5").unwrap_yielded(), 10);
        assert_eq!(mapped.next("7").unwrap_yielded(), 14);
        assert_eq!(mapped.next("10").unwrap_yielded(), 20);
    }

    #[test]
    fn test_map_input_with_once() {
        use crate::build::once;
        let coro = once(|x: i32| x + 100);
        let mut mapped = map_input(|s: String| s.len() as i32, coro);

        // First input: "hello".len() = 5, yields 5 + 100 = 105
        assert_eq!(mapped.next("hello".to_string()).unwrap_yielded(), 105);
        // Second input: "world".len() = 5, completes with 5
        assert_eq!(mapped.next("world".to_string()).unwrap_complete(), 5);
    }

    #[test]
    fn test_map_input_preserves_return() {
        use crate::build::once;
        let coro = once(|x: i32| x * 2);
        let mut mapped = map_input(|x: i32| x + 1, coro);

        // Input 5 -> 6, yields 12
        mapped.next(5).unwrap_yielded();
        // Input 10 -> 11, completes with 11
        assert_eq!(mapped.next(10).unwrap_complete(), 11);
    }

    #[test]
    fn test_map_yield_basic() {
        use crate::build::repeat;
        let coro = repeat(|x: i32| x * 2);
        let mut mapped = map_yield(|y: i32| y.to_string(), coro);

        assert_eq!(mapped.next(5).unwrap_yielded(), "10");
        assert_eq!(mapped.next(7).unwrap_yielded(), "14");
        assert_eq!(mapped.next(100).unwrap_yielded(), "200");
    }

    #[test]
    fn test_map_yield_with_once() {
        use crate::build::once;
        let coro = once(|x: i32| x + 10);
        let mut mapped = map_yield(|y: i32| format!("result={}", y), coro);

        assert_eq!(mapped.next(5).unwrap_yielded(), "result=15");
        assert_eq!(mapped.next(20).unwrap_complete(), 20);
    }

    #[test]
    fn test_map_yield_preserves_return() {
        use crate::build::once;
        let coro = once(|x: i32| x * 2);
        let mut mapped = map_yield(|y: i32| y as f64, coro);

        // Yield is transformed to f64
        assert_eq!(mapped.next(5).unwrap_yielded(), 10.0);
        // Return is NOT transformed (still i32)
        assert_eq!(mapped.next(7).unwrap_complete(), 7);
    }

    #[test]
    fn test_map_return_basic() {
        use crate::build::once;
        let coro = once(|x: i32| x + 5);
        let mut mapped = map_return(|r: i32| r * 10, coro);

        // Yield is not transformed
        assert_eq!(mapped.next(10).unwrap_yielded(), 15);
        // Return is transformed: 20 * 10 = 200
        assert_eq!(mapped.next(20).unwrap_complete(), 200);
    }

    #[test]
    fn test_map_return_with_repeat() {
        use crate::build::repeat;
        // repeat never completes, so this just demonstrates the type change
        let coro = repeat(|x: i32| x + 1);
        let _mapped = map_return(|r: i32| r.to_string(), coro);
        // We can't test completion, but we can verify it compiles with transformed return type
    }

    #[test]
    fn test_map_return_yield_passthrough() {
        use crate::build::once;
        let coro = once(|x: i32| x * 2);
        let mut mapped = map_return(|r: i32| format!("done:{}", r), coro);

        // First: yields 5 * 2 = 10
        assert_eq!(mapped.next(5).unwrap_yielded(), 10);
        // Second: once completes with 7, return is transformed
        assert_eq!(mapped.next(7).unwrap_complete(), "done:7");
    }

    #[test]
    fn test_map_return_type_conversion() {
        use crate::build::once;
        let coro = once(|x: i32| x + 1);
        let mut mapped = map_return(|r: i32| (r as f64, r * 2), coro);

        mapped.next(5).unwrap_yielded(); // 6
        // Return is transformed to tuple
        assert_eq!(mapped.next(10).unwrap_complete(), (10.0, 20));
    }

    #[test]
    fn test_all_three_maps_combined() {
        use crate::build::once;
        // Input: &str -> parse to i32
        // Yield: i32 -> format as string
        // Return: i32 -> convert to f64
        let coro = once(|x: i32| x * 2);
        let mut mapped = map_return(
            |r: i32| r as f64,
            map_yield(
                |y: i32| format!("yielded:{}", y),
                map_input(|s: &str| s.parse::<i32>().unwrap(), coro),
            ),
        );

        // Input "5" -> 5, yields 10 -> "yielded:10"
        assert_eq!(mapped.next("5").unwrap_yielded(), "yielded:10");
        // Input "7" -> 7, completes with 7 -> 7.0
        assert_eq!(mapped.next("7").unwrap_complete(), 7.0);
    }

    #[test]
    fn test_map_input_multiple_transformations() {
        use crate::build::repeat;
        let coro = repeat(|x: i32| x + 1);
        // Double map_input: String -> usize (len) -> i32
        let mut mapped = map_input(|s: String| s.len(), map_input(|n: usize| n as i32, coro));

        // Input "hello" -> len=5 -> 5 + 1 = 6
        assert_eq!(mapped.next("hello".to_string()).unwrap_yielded(), 6);
        // Input "a" -> len=1 -> 1 + 1 = 2
        assert_eq!(mapped.next("a".to_string()).unwrap_yielded(), 2);
    }
}
