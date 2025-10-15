//! Result combinators for error handling in coroutines.
//!
//! This module provides adapters and extension traits for working with [`Result`] types
//! in coroutine pipelines, enabling composable error handling patterns.
//!
//! # Core Combinators
//!
//! - [`short_circuit`] - Short-circuits on the first `Err` in yielded values
//! - [`ok_chain`] - Chains to another coroutine only if the first returns `Ok`
//! - [`flatten`] - Flattens nested `Result<Result<T, E>, E>` types
//!
//! # Extension Traits
//!
//! The [`TrySans`] trait provides method syntax for these combinators,
//! enabling fluent error handling chains.
//!
//! # Examples
//!
//! ```
//! use sans::prelude::*;
//! use sans::result::{short_circuit, TrySans};
//!
//! // Short-circuit on errors
//! let coro = repeat(|x: i32| {
//!     if x < 0 { Err("negative") } else { Ok(x * 2) }
//! });
//! let mut sc = short_circuit(coro);
//!
//! assert_eq!(sc.next(5).unwrap_yielded(), 10);
//! assert_eq!(sc.next(-1).unwrap_complete(), Err("negative"));
//! ```
use crate::{Sans, step::Step};

/// Short-circuits on the first `Err` in a yielded `Result`.
///
/// Converts `Sans<I, Result<O, E>, Return = P>` to `Sans<I, O, Return = Result<P, E>>`.
/// If any yield is `Err(e)`, immediately completes with `Err(e)`.
/// Otherwise yields unwrapped `Ok` values and completes with `Ok(P)`.
pub struct ShortCircuit<S, E> {
    coro: S,
    _phantom: std::marker::PhantomData<E>,
}

/// Create a coroutine that short-circuits on the first yielded `Err`.
///
/// # Examples
///
/// ```
/// use sans::prelude::*;
/// use sans::result::short_circuit;
///
/// let coro = repeat(|x: i32| {
///     if x < 0 { Err("negative") } else { Ok(x * 2) }
/// });
/// let mut sc = short_circuit(coro);
///
/// assert_eq!(sc.next(5).unwrap_yielded(), 10);
/// assert_eq!(sc.next(-1).unwrap_complete(), Err("negative"));
/// ```
pub fn short_circuit<S, E>(coro: S) -> ShortCircuit<S, E> {
    ShortCircuit {
        coro,
        _phantom: std::marker::PhantomData,
    }
}

impl<I, O, E, S> Sans<I, O> for ShortCircuit<S, E>
where
    S: Sans<I, Result<O, E>>,
{
    type Return = Result<S::Return, E>;

    fn next(&mut self, input: I) -> Step<O, Self::Return> {
        match self.coro.next(input) {
            Step::Yielded(Ok(o)) => Step::Yielded(o),
            Step::Yielded(Err(e)) => Step::Complete(Err(e)),
            Step::Complete(p) => Step::Complete(Ok(p)),
        }
    }
}

/// Chains to another coroutine only if the first returns `Ok`.
///
/// Converts the Ok value to the input type for the next coroutine.
pub struct OkChain<S, R> {
    coro: Option<S>,
    next: R,
}

/// Create a coroutine that chains to another coroutine on `Ok`.
///
/// # Examples
///
/// ```
/// use sans::prelude::*;
/// use sans::result::ok_chain;
/// use sans::build::from_fn;
/// use sans::Step;
///
/// let mut called = false;
/// let first = from_fn(move |x: i32| -> Step<i32, Result<i32, String>> {
///     if !called {
///         called = true;
///         Step::Yielded(x * 2)
///     } else if x > 0 {
///         Step::Complete(Ok(x))
///     } else {
///         Step::Complete(Err("non-positive".to_string()))
///     }
/// });
/// let second = repeat(|x: i32| x + 1);
///
/// let mut chained = ok_chain(first, second);
///
/// // First input: 5 -> yields 10
/// assert_eq!(chained.next(5).unwrap_yielded(), 10);
/// // Second input: completes with Ok(3), chains with 3
/// assert_eq!(chained.next(3).unwrap_yielded(), 4);
/// // Now in second coro
/// assert_eq!(chained.next(10).unwrap_yielded(), 11);
/// ```
pub fn ok_chain<I, O, E, S, R>(coro: S, next: R) -> OkChain<S, R>
where
    S: Sans<I, O, Return = Result<I, E>>,
    R: Sans<I, O>,
{
    OkChain {
        coro: Some(coro),
        next,
    }
}

impl<I, O, E, S, R> Sans<I, O> for OkChain<S, R>
where
    S: Sans<I, O, Return = Result<I, E>>,
    R: Sans<I, O>,
{
    type Return = Result<R::Return, E>;

    fn next(&mut self, input: I) -> Step<O, Self::Return> {
        if self.coro.is_none() {
            return self.next.next(input).map_complete(Ok);
        }

        let mut coro = self.coro.take().expect("OkChain coro already consumed");
        match coro.next(input) {
            Step::Yielded(o) => {
                self.coro = Some(coro);
                Step::Yielded(o)
            }
            Step::Complete(Err(e)) => Step::Complete(Err(e)),
            Step::Complete(Ok(i)) => match self.next.next(i) {
                Step::Yielded(o) => Step::Yielded(o),
                Step::Complete(ret) => Step::Complete(Ok(ret)),
            },
        }
    }
}

/// Flattens nested `Result` types in the return value.
///
/// Converts `Result<Result<T, E>, E>` to `Result<T, E>`.
pub struct Flatten<S> {
    coro: S,
}

/// Create a coroutine that flattens nested `Result` types.
///
/// # Examples
///
/// ```
/// use sans::prelude::*;
/// use sans::result::flatten;
/// use sans::build::from_fn;
/// use sans::Step;
///
/// let mut called = false;
/// let coro = from_fn(move |x: i32| -> Step<Result<Result<i32, String>, String>, Result<Result<i32, String>, String>> {
///     if !called {
///         called = true;
///         if x > 0 {
///             if x < 100 { Step::Yielded(Ok(Ok(x * 2))) } else { Step::Yielded(Ok(Err("too large".to_string()))) }
///         } else {
///             Step::Yielded(Err("non-positive".to_string()))
///         }
///     } else {
///         Step::Complete(Ok(Ok(x)))
///     }
/// });
///
/// let mut flattened = flatten(coro);
///
/// assert_eq!(flattened.next(5).unwrap_yielded(), Ok(Ok(10)));
/// // Second call completes with flattened result
/// assert_eq!(flattened.next(10).unwrap_complete(), Ok(10));
/// ```
pub fn flatten<S>(coro: S) -> Flatten<S> {
    Flatten { coro }
}

impl<I, O, T, E, S> Sans<I, O> for Flatten<S>
where
    S: Sans<I, O, Return = Result<Result<T, E>, E>>,
{
    type Return = Result<T, E>;

    fn next(&mut self, input: I) -> Step<O, Self::Return> {
        match self.coro.next(input) {
            Step::Yielded(o) => Step::Yielded(o),
            Step::Complete(Ok(Ok(t))) => Step::Complete(Ok(t)),
            Step::Complete(Ok(Err(e))) => Step::Complete(Err(e)),
            Step::Complete(Err(e)) => Step::Complete(Err(e)),
        }
    }
}

/// Extension trait for `Sans` that provides result combinator methods.
///
/// This trait is automatically implemented for all types that implement `Sans`.
pub trait TrySans<I, O>: Sized {
    /// Chains to another coroutine only if the first returns `Ok`.
    fn ok_chain<E, R>(self, next: R) -> OkChain<Self, R>
    where
        Self: Sans<I, O, Return = Result<I, E>>,
        R: Sans<I, O>,
    {
        ok_chain(self, next)
    }

    /// Flattens nested `Result` types in the return value.
    fn flatten<T, E>(self) -> Flatten<Self>
    where
        Self: Sans<I, O, Return = Result<Result<T, E>, E>>,
    {
        flatten(self)
    }
}

impl<I, O, S> TrySans<I, O> for S where S: Sans<I, O> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build::{once, repeat};

    #[test]
    fn test_short_circuit_propagates_ok_yields() {
        let coro = repeat(|x: i32| if x < 0 { Err("negative") } else { Ok(x * 2) });
        let mut sc = short_circuit(coro);

        assert_eq!(sc.next(5).unwrap_yielded(), 10);
        assert_eq!(sc.next(3).unwrap_yielded(), 6);
        assert_eq!(sc.next(10).unwrap_yielded(), 20);
    }

    #[test]
    fn test_short_circuit_stops_on_err() {
        let coro = repeat(|x: i32| if x < 0 { Err("negative") } else { Ok(x * 2) });
        let mut sc = short_circuit(coro);

        assert_eq!(sc.next(5).unwrap_yielded(), 10);
        assert_eq!(sc.next(-1).unwrap_complete(), Err("negative"));
    }

    #[test]
    fn test_short_circuit_completes_with_ok() {
        let coro = once(|x: i32| if x < 0 { Err("negative") } else { Ok(x * 2) });
        let mut sc = short_circuit(coro);

        assert_eq!(sc.next(5).unwrap_yielded(), 10);
        assert_eq!(sc.next(3).unwrap_complete(), Ok(3));
    }

    #[test]
    fn test_ok_chain_propagates_err() {
        use crate::build::from_fn;
        let mut called = false;
        let first = from_fn(move |x: i32| {
            if !called {
                called = true;
                Step::Yielded(x * 2)
            } else if x > 0 {
                Step::Complete(Ok(x))
            } else {
                Step::Complete(Err("non-positive".to_string()))
            }
        });
        let second = repeat(|x: i32| x + 1);

        let mut chained = ok_chain(first, second);

        assert_eq!(chained.next(5).unwrap_yielded(), 10);
        assert_eq!(
            chained.next(-5).unwrap_complete(),
            Err("non-positive".to_string())
        );
    }

    #[test]
    fn test_ok_chain_chains_on_ok() {
        use crate::build::from_fn;
        let mut called = false;
        let first = from_fn(move |x: i32| -> Step<i32, Result<i32, String>> {
            if !called {
                called = true;
                Step::Yielded(x * 2)
            } else {
                Step::Complete(Ok(x))
            }
        });
        let second = repeat(|x: i32| x + 1);

        let mut chained = ok_chain(first, second);

        assert_eq!(chained.next(5).unwrap_yielded(), 10);
        assert_eq!(chained.next(3).unwrap_yielded(), 4);
        assert_eq!(chained.next(10).unwrap_yielded(), 11);
    }

    #[test]
    fn test_flatten_outer_err() {
        use crate::build::from_fn;
        let mut called = false;
        let coro = from_fn(move |x: i32| {
            if !called {
                called = true;
                Step::Yielded(x * 2)
            } else if x > 0 {
                Step::Complete(Ok(Ok(x)))
            } else {
                Step::Complete(Err("outer error".to_string()))
            }
        });

        let mut flattened = flatten(coro);

        assert_eq!(flattened.next(5).unwrap_yielded(), 10);
        assert_eq!(
            flattened.next(-5).unwrap_complete(),
            Err("outer error".to_string())
        );
    }

    #[test]
    fn test_flatten_inner_err() {
        use crate::build::from_fn;
        let mut called = false;
        let coro = from_fn(move |x: i32| {
            if !called {
                called = true;
                Step::Yielded(x * 2)
            } else if x > 0 {
                if x < 100 {
                    Step::Complete(Ok(Ok(x)))
                } else {
                    Step::Complete(Ok(Err("inner error".to_string())))
                }
            } else {
                Step::Complete(Err("outer error".to_string()))
            }
        });

        let mut flattened = flatten(coro);

        assert_eq!(flattened.next(5).unwrap_yielded(), 10);
        assert_eq!(
            flattened.next(150).unwrap_complete(),
            Err("inner error".to_string())
        );
    }

    #[test]
    fn test_flatten_both_ok() {
        use crate::build::from_fn;
        let mut called = false;
        let coro = from_fn(move |x: i32| {
            if !called {
                called = true;
                Step::Yielded(x * 2)
            } else if x > 0 {
                if x < 100 {
                    Step::Complete(Ok(Ok(x)))
                } else {
                    Step::Complete(Ok(Err("too large".to_string())))
                }
            } else {
                Step::Complete(Err("non-positive".to_string()))
            }
        });

        let mut flattened = flatten(coro);

        assert_eq!(flattened.next(5).unwrap_yielded(), 10);
        assert_eq!(flattened.next(10).unwrap_complete(), Ok(10));
    }

}
