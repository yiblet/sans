//! Result combinators for error handling in coroutines.
//!
//! This module provides adapters and extension traits for working with [`Result`] types
//! in coroutine pipelines, enabling composable error handling patterns.
//!
//! # Core Combinators
//!
//! - [`short_circuit`] - Short-circuits on the first `Err` in yielded values
//! - [`ok_map`] - Maps `Ok` return values through a function that produces an [`InitSans`]
//! - [`ok_and_then`] - Chains through a function that produces a fallible [`InitSans`]
//! - [`ok_chain`] - Chains to another coroutine only if the first returns `Ok`
//! - [`flatten`] - Flattens nested `Result<Result<T, E>, E>` types
//!
//! # Extension Traits
//!
//! The [`TrySans`] and [`TryInitSans`] traits provide method syntax for these combinators,
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
use crate::{
    InitSans, Sans,
    init::{ShortCircuit as InitShortCircuit, Yielded},
    step::Step,
};

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

/// Create a ShortCircuit from an InitSans coroutine.
///
/// This is used when applying short-circuit to a coroutine that yields immediately.
pub fn init_short_circuit<I, O, E, S>(coro: S) -> ShortCircuit<S, E>
where
    S: InitSans<I, Result<O, E>>,
{
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

impl<I, O, E, S> InitSans<I, O> for ShortCircuit<S, E>
where
    S: InitSans<I, Result<O, E>>,
{
    type Next = ShortCircuit<S::Next, E>;
    type Return = Result<S::Return, E>;

    fn init(self) -> Step<(O, Self::Next), Self::Return> {
        // Convert the result to InitShortCircuit for consistent handling
        let sc: InitShortCircuit<Yielded<Result<O, E>, S::Next>, S::Return> =
            self.coro.init().into();
        match sc {
            InitShortCircuit::Pending(Yielded(Ok(o), next)) => Step::Yielded((
                o,
                ShortCircuit {
                    coro: next,
                    _phantom: std::marker::PhantomData,
                },
            )),
            InitShortCircuit::Pending(Yielded(Err(e), _)) => Step::Complete(Err(e)),
            InitShortCircuit::Complete(p) => Step::Complete(Ok(p)),
        }
    }
}

/// Maps `Ok` values through a function that produces an `InitSans`.
///
/// If the coroutine completes with `Err`, propagates the error.
/// If it completes with `Ok(p)`, calls `f(p)` and wraps the result in `Ok`.
pub struct OkMap<S, T, F> {
    state: OkMapState<S, T, F>,
}

enum OkMapState<S, T, F> {
    OnFirst(S, Option<F>),
    OnSecond(T),
}

/// Create a coroutine that maps `Ok` return values through a function.
///
/// # Examples
///
/// ```
/// use sans::prelude::*;
/// use sans::result::ok_map;
/// use sans::build::from_fn;
/// use sans::Step;
///
/// let mut called = false;
/// let coro = from_fn(move |x: i32| -> Step<i32, Result<i32, String>> {
///     if !called {
///         called = true;
///         Step::Yielded(x * 2)
///     } else if x > 0 {
///         Step::Complete(Ok(x))
///     } else {
///         Step::Complete(Err("non-positive".to_string()))
///     }
/// });
///
/// let mut mapped = ok_map(coro, |val| init(val, repeat(move |x: i32| x + val)));
///
/// // First input: 5 -> yields 10
/// assert_eq!(mapped.next(5).unwrap_yielded(), 10);
/// // Second input: completes with Ok(3), starts second coro with val=3
/// assert_eq!(mapped.next(3).unwrap_yielded(), 3);
/// // Third input: second coro continues 7 + 3 = 10
/// assert_eq!(mapped.next(7).unwrap_yielded(), 10);
/// ```
pub fn ok_map<I, O, P, E, S, T, F>(coro: S, f: F) -> OkMap<S, T::Next, F>
where
    S: Sans<I, O, Return = Result<P, E>>,
    T: InitSans<I, O>,
    F: FnOnce(P) -> T,
{
    OkMap {
        state: OkMapState::OnFirst(coro, Some(f)),
    }
}

/// Create an OkMap from an InitSans coroutine.
///
/// This is used when applying ok_map to a coroutine that yields immediately.
pub fn init_ok_map<I, O, P, E, S, T, F>(coro: S, f: F) -> OkMap<S, T::Next, F>
where
    S: InitSans<I, O, Return = Result<P, E>>,
    T: InitSans<I, O>,
    F: FnOnce(P) -> T,
{
    OkMap {
        state: OkMapState::OnFirst(coro, Some(f)),
    }
}

impl<I, O, P, E, S, T, F> Sans<I, O> for OkMap<S, T::Next, F>
where
    S: Sans<I, O, Return = Result<P, E>>,
    T: InitSans<I, O>,
    F: FnOnce(P) -> T,
{
    type Return = Result<T::Return, E>;

    fn next(&mut self, input: I) -> Step<O, Self::Return> {
        match &mut self.state {
            OkMapState::OnSecond(next) => next.next(input).map_complete(Ok),
            OkMapState::OnFirst(coro, f) => match coro.next(input) {
                Step::Yielded(o) => Step::Yielded(o),
                Step::Complete(Err(e)) => Step::Complete(Err(e)),
                Step::Complete(Ok(p)) => {
                    let f = f.take().expect("ok_map can only be used once");
                    let init_sans = f(p);
                    // Convert the result to InitShortCircuit for consistent handling
                    let sc: InitShortCircuit<Yielded<O, T::Next>, T::Return> =
                        init_sans.init().into();
                    match sc {
                        InitShortCircuit::Pending(Yielded(o, next)) => {
                            *self = OkMap {
                                state: OkMapState::OnSecond(next),
                            };
                            Step::Yielded(o)
                        }
                        InitShortCircuit::Complete(ret) => Step::Complete(Ok(ret)),
                    }
                }
            },
        }
    }
}

impl<I, O, P, E, S, T, F> InitSans<I, O> for OkMap<S, T::Next, F>
where
    S: InitSans<I, O, Return = Result<P, E>>,
    T: InitSans<I, O>,
    F: FnOnce(P) -> T,
{
    type Next = OkMap<S::Next, T::Next, F>;
    type Return = Result<T::Return, E>;

    fn init(self) -> Step<(O, Self::Next), Self::Return> {
        let OkMapState::OnFirst(coro, f) = self.state else {
            unreachable!("OkMap::init called on OnSecond state")
        };

        // Convert the result to InitShortCircuit for consistent handling
        let sc: InitShortCircuit<Yielded<O, S::Next>, Result<P, E>> = coro.init().into();
        match sc {
            InitShortCircuit::Pending(Yielded(o, next)) => Step::Yielded((
                o,
                OkMap {
                    state: OkMapState::OnFirst(next, f),
                },
            )),
            InitShortCircuit::Complete(Err(e)) => Step::Complete(Err(e)),
            InitShortCircuit::Complete(Ok(p)) => {
                let f = f.expect("f should be available");
                let init_sans = f(p);
                // Convert the second init result too
                let sc2: InitShortCircuit<Yielded<O, T::Next>, T::Return> = init_sans.init().into();
                match sc2 {
                    InitShortCircuit::Pending(Yielded(o, next)) => Step::Yielded((
                        o,
                        OkMap {
                            state: OkMapState::OnSecond(next),
                        },
                    )),
                    InitShortCircuit::Complete(ret) => Step::Complete(Ok(ret)),
                }
            }
        }
    }
}

/// Chains through a function that produces an `InitSans` with a `Result` return type.
///
/// If the coroutine completes with `Err`, propagates the error without calling `f`.
/// If it completes with `Ok(p)`, calls `f(p)` which itself can return `Result`.
pub struct OkAndThen<S, T, F> {
    state: OkAndThenState<S, T, F>,
}

enum OkAndThenState<S, T, F> {
    OnFirst(S, Option<F>),
    OnSecond(T),
}

/// Create a coroutine that chains `Ok` values through a fallible function.
///
/// # Examples
///
/// ```
/// use sans::prelude::*;
/// use sans::result::ok_and_then;
/// use sans::build::from_fn;
/// use sans::Step;
///
/// let mut first_called = false;
/// let coro = from_fn(move |x: i32| -> Step<Result<i32, String>, Result<i32, String>> {
///     if !first_called {
///         first_called = true;
///         Step::Yielded(Ok(x * 2))
///     } else if x > 0 {
///         Step::Complete(Ok(x))
///     } else {
///         Step::Complete(Err("non-positive".to_string()))
///     }
/// });
///
/// let mut chained = ok_and_then(coro, move |val| {
///     let mut second_called = false;
///     init(Ok(val), from_fn(move |x: i32| -> Step<Result<i32, String>, Result<i32, String>> {
///         if !second_called {
///             second_called = true;
///             if x > 100 { Step::Yielded(Err("too large".to_string())) } else { Step::Yielded(Ok(x + val)) }
///         } else {
///             Step::Complete(Ok(x))
///         }
///     }))
/// });
///
/// // First input: 5 -> yields Ok(10)
/// assert_eq!(chained.next(5).unwrap_yielded(), Ok(10));
/// // Second input: completes with Ok(3), starts second coro with val=3, yields Ok(3)
/// assert_eq!(chained.next(3).unwrap_yielded(), Ok(3));
/// // Third input: 7 -> yields Ok(7+3) = Ok(10)
/// assert_eq!(chained.next(7).unwrap_yielded(), Ok(10));
/// ```
pub fn ok_and_then<I, O, P, Q, E, S, T, F>(coro: S, f: F) -> OkAndThen<S, T::Next, F>
where
    S: Sans<I, O, Return = Result<P, E>>,
    T: InitSans<I, O, Return = Result<Q, E>>,
    F: FnOnce(P) -> T,
{
    OkAndThen {
        state: OkAndThenState::OnFirst(coro, Some(f)),
    }
}

/// Create an OkAndThen from an InitSans coroutine.
///
/// This is used when applying ok_and_then to a coroutine that yields immediately.
pub fn init_ok_and_then<I, O, P, Q, E, S, T, F>(coro: S, f: F) -> OkAndThen<S, T::Next, F>
where
    S: InitSans<I, O, Return = Result<P, E>>,
    T: InitSans<I, O>,
    F: FnOnce(P) -> T,
{
    OkAndThen {
        state: OkAndThenState::OnFirst(coro, Some(f)),
    }
}

impl<I, O, P, Q, E, S, T, F> Sans<I, O> for OkAndThen<S, T::Next, F>
where
    S: Sans<I, O, Return = Result<P, E>>,
    T: InitSans<I, O, Return = Result<Q, E>>,
    F: FnOnce(P) -> T,
{
    type Return = Result<Q, E>;

    fn next(&mut self, input: I) -> Step<O, Self::Return> {
        match &mut self.state {
            OkAndThenState::OnSecond(next) => next.next(input),
            OkAndThenState::OnFirst(coro, f) => match coro.next(input) {
                Step::Yielded(o) => Step::Yielded(o),
                Step::Complete(Err(e)) => Step::Complete(Err(e)),
                Step::Complete(Ok(p)) => {
                    let f = f.take().expect("ok_and_then can only be used once");
                    let init_sans = f(p);
                    // Convert the result to InitShortCircuit for consistent handling
                    let sc: InitShortCircuit<Yielded<O, T::Next>, Result<Q, E>> =
                        init_sans.init().into();
                    match sc {
                        InitShortCircuit::Pending(Yielded(o, next)) => {
                            *self = OkAndThen {
                                state: OkAndThenState::OnSecond(next),
                            };
                            Step::Yielded(o)
                        }
                        InitShortCircuit::Complete(ret) => Step::Complete(ret),
                    }
                }
            },
        }
    }
}

impl<I, O, P, Q, E, S, T, F> InitSans<I, O> for OkAndThen<S, T::Next, F>
where
    S: InitSans<I, O, Return = Result<P, E>>,
    T: InitSans<I, O, Return = Result<Q, E>>,
    F: FnOnce(P) -> T,
{
    type Next = OkAndThen<S::Next, T::Next, F>;
    type Return = Result<Q, E>;

    fn init(self) -> Step<(O, Self::Next), Self::Return> {
        let OkAndThenState::OnFirst(coro, f) = self.state else {
            unreachable!("OkAndThen::init called on OnSecond state")
        };

        // Convert the result to InitShortCircuit for consistent handling
        let sc: InitShortCircuit<Yielded<O, S::Next>, Result<P, E>> = coro.init().into();
        match sc {
            InitShortCircuit::Pending(Yielded(o, next)) => Step::Yielded((
                o,
                OkAndThen {
                    state: OkAndThenState::OnFirst(next, f),
                },
            )),
            InitShortCircuit::Complete(Err(e)) => Step::Complete(Err(e)),
            InitShortCircuit::Complete(Ok(p)) => {
                let f = f.expect("f should be available");
                let init_sans = f(p);
                // Convert the second init result too
                let sc2: InitShortCircuit<Yielded<O, T::Next>, Result<Q, E>> =
                    init_sans.init().into();
                match sc2 {
                    InitShortCircuit::Pending(Yielded(o, next)) => Step::Yielded((
                        o,
                        OkAndThen {
                            state: OkAndThenState::OnSecond(next),
                        },
                    )),
                    InitShortCircuit::Complete(ret) => Step::Complete(ret),
                }
            }
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

/// Create an OkChain from an InitSans coroutine.
///
/// This is used when applying ok_chain to a coroutine that yields immediately.
pub fn init_ok_chain<I, O, E, S, R>(coro: S, next: R) -> OkChain<S, R>
where
    S: InitSans<I, O, Return = Result<I, E>>,
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

impl<I, O, E, S, R> InitSans<I, O> for OkChain<S, R>
where
    S: InitSans<I, O, Return = Result<I, E>>,
    R: Sans<I, O>,
{
    type Next = OkChain<S::Next, R>;
    type Return = Result<R::Return, E>;

    fn init(mut self) -> Step<(O, Self::Next), Self::Return> {
        // Convert the result to InitShortCircuit for consistent handling
        let sc: InitShortCircuit<Yielded<O, S::Next>, Result<I, E>> = self
            .coro
            .take()
            .expect("OkChain coro must be Some")
            .init()
            .into();
        match sc {
            InitShortCircuit::Pending(Yielded(o, next)) => Step::Yielded((
                o,
                OkChain {
                    coro: Some(next),
                    next: self.next,
                },
            )),
            InitShortCircuit::Complete(Err(e)) => Step::Complete(Err(e)),
            InitShortCircuit::Complete(Ok(i)) => match self.next.next(i) {
                Step::Yielded(o) => Step::Yielded((
                    o,
                    OkChain {
                        coro: None,
                        next: self.next,
                    },
                )),
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

/// Create a Flatten from an InitSans coroutine.
///
/// This is used when applying flatten to a coroutine that yields immediately.
pub fn init_flatten<I, O, T, E, S>(coro: S) -> Flatten<S>
where
    S: InitSans<I, O, Return = Result<Result<T, E>, E>>,
{
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

impl<I, O, T, E, S> InitSans<I, O> for Flatten<S>
where
    S: InitSans<I, O, Return = Result<Result<T, E>, E>>,
{
    type Next = Flatten<S::Next>;
    type Return = Result<T, E>;

    fn init(self) -> Step<(O, Self::Next), Self::Return> {
        // Convert the result to InitShortCircuit for consistent handling
        let sc: InitShortCircuit<Yielded<O, S::Next>, Result<Result<T, E>, E>> =
            self.coro.init().into();
        match sc {
            InitShortCircuit::Pending(Yielded(o, next)) => {
                Step::Yielded((o, Flatten { coro: next }))
            }
            InitShortCircuit::Complete(Ok(Ok(t))) => Step::Complete(Ok(t)),
            InitShortCircuit::Complete(Ok(Err(e))) => Step::Complete(Err(e)),
            InitShortCircuit::Complete(Err(e)) => Step::Complete(Err(e)),
        }
    }
}

/// Extension trait for `Sans` that provides result combinator methods.
///
/// This trait is automatically implemented for all types that implement `Sans`.
pub trait TrySans<I, O>: Sized {
    /// Maps `Ok` return values through a function that produces an `InitSans`.
    fn ok_map<P, E, T, F>(self, f: F) -> OkMap<Self, T::Next, F>
    where
        Self: Sans<I, O, Return = Result<P, E>>,
        T: InitSans<I, O>,
        T::Next: Sans<I, O>,
        F: FnOnce(P) -> T,
    {
        ok_map(self, f)
    }

    /// Chains through a function that produces an `InitSans` with a `Result` return type.
    fn ok_and_then<P, Q, E, T, F>(self, f: F) -> OkAndThen<Self, T::Next, F>
    where
        Self: Sans<I, O, Return = Result<P, E>>,
        T: InitSans<I, O, Return = Result<Q, E>>,
        F: FnOnce(P) -> T,
    {
        ok_and_then(self, f)
    }

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

/// Extension trait for `InitSans` that provides result combinator methods.
///
/// This trait is automatically implemented for all types that implement `InitSans`.
pub trait TryInitSans<I, O>: InitSans<I, O> + Sized {
    /// Maps `Ok` return values through a function that produces an `InitSans`.
    fn ok_map<P, E, T, F>(self, f: F) -> OkMap<Self, T::Next, F>
    where
        Self: InitSans<I, O, Return = Result<P, E>>,
        T: InitSans<I, O>,
        F: FnOnce(P) -> T,
    {
        init_ok_map(self, f)
    }

    /// Chains through a function that produces an `InitSans` with a `Result` return type.
    fn ok_and_then<P, Q, E, T, F>(self, f: F) -> OkAndThen<Self, T::Next, F>
    where
        Self: InitSans<I, O, Return = Result<P, E>>,
        T: InitSans<I, O, Return = Result<Q, E>>,
        F: FnOnce(P) -> T,
    {
        init_ok_and_then::<I, O, P, Q, E, Self, T, F>(self, f)
    }

    /// Chains to another coroutine only if the first returns `Ok`.
    fn ok_chain<E, R>(self, next: R) -> OkChain<Self, R>
    where
        Self: InitSans<I, O, Return = Result<I, E>>,
        R: Sans<I, O>,
    {
        init_ok_chain(self, next)
    }

    /// Flattens nested `Result` types in the return value.
    fn flatten<T, E>(self) -> Flatten<Self>
    where
        Self: InitSans<I, O, Return = Result<Result<T, E>, E>>,
    {
        init_flatten(self)
    }
}

impl<I, O, S> TryInitSans<I, O> for S where S: InitSans<I, O> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build::init;
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
    fn test_ok_map_propagates_err() {
        use crate::build::from_fn;
        let mut called = false;
        let coro = from_fn(move |x: i32| {
            if !called {
                called = true;
                Step::Yielded(x * 2)
            } else if x > 0 {
                Step::Complete(Ok(x))
            } else {
                Step::Complete(Err("non-positive".to_string()))
            }
        });

        let mut mapped = ok_map(coro, |val| init(val, repeat(move |x: i32| x + val)));

        assert_eq!(mapped.next(5).unwrap_yielded(), 10);
        assert_eq!(
            mapped.next(-5).unwrap_complete(),
            Err("non-positive".to_string())
        );
    }

    #[test]
    fn test_ok_map_chains_on_ok() {
        use crate::build::from_fn;
        let mut called = false;
        let coro = from_fn(move |x: i32| -> Step<i32, Result<i32, String>> {
            if !called {
                called = true;
                Step::Yielded(x * 2)
            } else {
                Step::Complete(Ok(x))
            }
        });

        let mut mapped = ok_map(coro, |val| init(val, repeat(move |x: i32| x + val)));

        assert_eq!(mapped.next(5).unwrap_yielded(), 10);
        assert_eq!(mapped.next(3).unwrap_yielded(), 3); // initial value from init
        assert_eq!(mapped.next(7).unwrap_yielded(), 10); // 3 + 7
    }

    #[test]
    fn test_ok_and_then_propagates_err_from_first() {
        use crate::build::from_fn;
        let mut called = false;
        let coro = from_fn(
            move |x: i32| -> Step<Result<i32, String>, Result<i32, String>> {
                if !called {
                    called = true;
                    Step::Yielded(Ok(x * 2))
                } else if x > 0 {
                    Step::Complete(Ok(x))
                } else {
                    Step::Complete(Err("non-positive".to_string()))
                }
            },
        );

        let mut chained = ok_and_then(coro, |val| {
            init(
                Ok(val),
                from_fn(
                    move |x: i32| -> Step<Result<i32, String>, Result<i32, String>> {
                        Step::Yielded(Ok(x + val))
                    },
                ),
            )
        });

        assert_eq!(chained.next(5).unwrap_yielded(), Ok(10));
        assert_eq!(
            chained.next(-5).unwrap_complete(),
            Err("non-positive".to_string())
        );
    }

    #[test]
    fn test_ok_and_then_chains_and_propagates_err_from_second() {
        use crate::build::from_fn;
        let mut first_called = false;
        let coro = from_fn(
            move |x: i32| -> Step<Result<i32, String>, Result<i32, String>> {
                if !first_called {
                    first_called = true;
                    Step::Yielded(Ok(x * 2))
                } else {
                    Step::Complete(Ok(x))
                }
            },
        );

        let mut chained = ok_and_then(coro, |val| {
            init(
                Ok(val),
                from_fn(
                    move |x: i32| -> Step<Result<i32, String>, Result<i32, String>> {
                        if x > 100 {
                            Step::Yielded(Err("too large".to_string()))
                        } else {
                            Step::Yielded(Ok(x + val))
                        }
                    },
                ),
            )
        });

        assert_eq!(chained.next(5).unwrap_yielded(), Ok(10));
        assert_eq!(chained.next(3).unwrap_yielded(), Ok(3));
        assert_eq!(chained.next(50).unwrap_yielded(), Ok(53));
        assert_eq!(
            chained.next(200).unwrap_yielded(),
            Err("too large".to_string())
        );
    }

    #[test]
    fn test_ok_and_then_both_ok() {
        use crate::build::from_fn;
        let mut first_called = false;
        let coro = from_fn(
            move |x: i32| -> Step<Result<i32, String>, Result<i32, String>> {
                if !first_called {
                    first_called = true;
                    Step::Yielded(Ok(x * 2))
                } else {
                    Step::Complete(Ok(x))
                }
            },
        );

        let mut chained = ok_and_then(coro, |val| {
            let mut second_called = false;
            init(
                Ok(val),
                from_fn(
                    move |x: i32| -> Step<Result<i32, String>, Result<i32, String>> {
                        if !second_called {
                            second_called = true;
                            Step::Yielded(Ok(x + val))
                        } else {
                            Step::Complete(Ok(x))
                        }
                    },
                ),
            )
        });

        assert_eq!(chained.next(5).unwrap_yielded(), Ok(10));
        assert_eq!(chained.next(3).unwrap_yielded(), Ok(3));
        assert_eq!(chained.next(7).unwrap_yielded(), Ok(10));
        assert_eq!(chained.next(5).unwrap_complete(), Ok(5));
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

    // InitSans tests

    #[test]
    fn test_short_circuit_init_yields_ok() {
        let init_coro = init(
            Ok(42),
            repeat(|x: i32| if x < 0 { Err("negative") } else { Ok(x * 2) }),
        );
        let sc = short_circuit(init_coro);

        let (initial, mut coro) = sc.init().unwrap_yielded();
        assert_eq!(initial, 42);
        assert_eq!(coro.next(5).unwrap_yielded(), 10);
        assert_eq!(coro.next(3).unwrap_yielded(), 6);
    }

    #[test]
    fn test_short_circuit_init_yields_err() {
        let init_coro = init(Err("initial error"), repeat(|x: i32| Ok(x * 2)));
        let sc = short_circuit(init_coro);

        assert_eq!(sc.init().unwrap_complete(), Err("initial error"));
    }

    #[test]
    fn test_short_circuit_init_completes_immediately() {
        use crate::build::from_fn;
        let coro = from_fn(|_: i32| Step::Complete::<Result<i32, &str>, i32>(100));
        let init_coro = (Ok(42), coro);
        let sc = short_circuit(init_coro);

        let (initial, mut coro) = sc.init().unwrap_yielded();
        assert_eq!(initial, 42);
        assert_eq!(coro.next(0).unwrap_complete(), Ok(100));
    }

    #[test]
    fn test_ok_map_init_first_yields() {
        use crate::build::from_fn;
        let mut called = false;
        let first_coro = from_fn(move |x: i32| -> Step<i32, Result<i32, &str>> {
            if !called {
                called = true;
                Step::Yielded(x * 2)
            } else {
                Step::Complete(Ok(x))
            }
        });
        let init_first = (10, first_coro);
        let mapped = OkMap {
            state: OkMapState::OnFirst(
                init_first,
                Some(|val| init(val, repeat(move |x: i32| x + val))),
            ),
        };

        let (initial, mut coro) = mapped.init().unwrap_yielded();
        assert_eq!(initial, 10);

        // First coroutine continues and yields 5*2=10
        assert_eq!(coro.next(5).unwrap_yielded(), 10);

        // First completes with Ok(0), second coroutine starts with val=0
        assert_eq!(coro.next(0).unwrap_yielded(), 0);

        // Second coroutine continues: 3 + 0
        assert_eq!(coro.next(3).unwrap_yielded(), 3);
    }

    #[test]
    fn test_ok_map_init_first_completes_ok_second_yields() {
        use crate::build::from_fn;
        let first = (
            10,
            from_fn(|_: i32| Step::Complete::<i32, Result<i32, &str>>(Ok(20))),
        );
        let mapped = OkMap {
            state: OkMapState::OnFirst(
                first,
                Some(|val| init(val * 2, repeat(move |x: i32| x + val))),
            ),
        };

        let (initial, mut coro) = mapped.init().unwrap_yielded();
        assert_eq!(initial, 10);

        // First completes with Ok(20), second coroutine inits with (40, ...)
        assert_eq!(coro.next(0).unwrap_yielded(), 40);

        // Second coroutine continues: 5 + 20
        assert_eq!(coro.next(5).unwrap_yielded(), 25);
    }

    #[test]
    fn test_ok_map_init_first_completes_err() {
        use crate::build::from_fn;
        let first = (
            10,
            from_fn(|_: i32| Step::Complete::<i32, Result<i32, &str>>(Err("error"))),
        );
        let mapped = OkMap {
            state: OkMapState::OnFirst(first, Some(|val| init(val, repeat(move |x: i32| x + val)))),
        };

        let (initial, mut coro) = mapped.init().unwrap_yielded();
        assert_eq!(initial, 10);

        assert_eq!(coro.next(0).unwrap_complete(), Err("error"));
    }

    #[test]
    fn test_ok_and_then_init_first_yields() {
        use crate::build::from_fn;
        let mut called = false;
        let first_coro = from_fn(
            move |x: i32| -> Step<Result<i32, &str>, Result<i32, &str>> {
                if !called {
                    called = true;
                    Step::Yielded(Ok(x * 2))
                } else {
                    Step::Complete(Ok(x))
                }
            },
        );
        let first = (Ok(10), first_coro);
        let chained = OkAndThen {
            state: OkAndThenState::OnFirst(
                first,
                Some(|val| {
                    init(
                        Ok(val),
                        from_fn(
                            move |x: i32| -> Step<Result<i32, &str>, Result<i32, &str>> {
                                Step::Yielded(Ok(x + val))
                            },
                        ),
                    )
                }),
            ),
        };

        let (initial, mut coro) = chained.init().unwrap_yielded();
        assert_eq!(initial, Ok(10));

        // First coroutine continues and yields 5*2=10
        assert_eq!(coro.next(5).unwrap_yielded(), Ok(10));

        // First completes with Ok(0), second coroutine starts with Ok(0)
        assert_eq!(coro.next(0).unwrap_yielded(), Ok(0));

        // Second coroutine continues: 3 + 0
        assert_eq!(coro.next(3).unwrap_yielded(), Ok(3));
    }

    #[test]
    fn test_ok_and_then_init_first_completes_ok_second_yields() {
        use crate::build::from_fn;
        let first = (
            Ok(10),
            from_fn(|_: i32| Step::Complete::<Result<i32, &str>, Result<i32, &str>>(Ok(20))),
        );
        let chained = OkAndThen {
            state: OkAndThenState::OnFirst(
                first,
                Some(|val| {
                    init(
                        Ok(val * 2),
                        from_fn(
                            move |x: i32| -> Step<Result<i32, &str>, Result<i32, &str>> {
                                Step::Yielded(Ok(x + val))
                            },
                        ),
                    )
                }),
            ),
        };

        let (initial, mut coro) = chained.init().unwrap_yielded();
        assert_eq!(initial, Ok(10));

        // First completes with Ok(20), second inits with Ok(40)
        assert_eq!(coro.next(0).unwrap_yielded(), Ok(40));

        // Second coroutine continues: 5 + 20
        assert_eq!(coro.next(5).unwrap_yielded(), Ok(25));
    }

    #[test]
    fn test_ok_and_then_init_first_completes_err() {
        use crate::build::from_fn;
        let first = (
            Ok(10),
            from_fn(|_: i32| Step::Complete::<Result<i32, &str>, Result<i32, &str>>(Err("error"))),
        );
        let chained = OkAndThen {
            state: OkAndThenState::OnFirst(
                first,
                Some(|val| {
                    init(
                        Ok(val),
                        from_fn(
                            move |x: i32| -> Step<Result<i32, &str>, Result<i32, &str>> {
                                Step::Yielded(Ok(x + val))
                            },
                        ),
                    )
                }),
            ),
        };

        let (initial, mut coro) = chained.init().unwrap_yielded();
        assert_eq!(initial, Ok(10));

        assert_eq!(coro.next(0).unwrap_complete(), Err("error"));
    }

    #[test]
    fn test_ok_chain_init_first_yields() {
        use crate::build::from_fn;
        let mut called = false;
        let first_coro = from_fn(move |x: i32| -> Step<i32, Result<i32, &str>> {
            if !called {
                called = true;
                Step::Yielded(x * 2)
            } else {
                Step::Complete(Ok(x))
            }
        });
        let first = (10, first_coro);
        let second = repeat(|x: i32| x + 1);
        let chained = OkChain {
            coro: Some(first),
            next: second,
        };

        let (initial, mut coro) = chained.init().unwrap_yielded();
        assert_eq!(initial, 10);

        // First coroutine continues and yields 5*2=10
        assert_eq!(coro.next(5).unwrap_yielded(), 10);

        // First completes with Ok(0), second coroutine starts with 0
        assert_eq!(coro.next(0).unwrap_yielded(), 1);
    }

    #[test]
    fn test_ok_chain_init_first_completes_ok_second_yields() {
        use crate::build::from_fn;
        let first = (
            10,
            from_fn(|_: i32| Step::Complete::<i32, Result<i32, &str>>(Ok(20))),
        );
        let second = repeat(|x: i32| x + 1);
        let chained = OkChain {
            coro: Some(first),
            next: second,
        };

        let (initial, mut coro) = chained.init().unwrap_yielded();
        assert_eq!(initial, 10);

        // First completes with Ok(20), second starts with 20
        assert_eq!(coro.next(0).unwrap_yielded(), 21);
        assert_eq!(coro.next(50).unwrap_yielded(), 51);
    }

    #[test]
    fn test_ok_chain_init_first_completes_err() {
        use crate::build::from_fn;
        let first = (
            10,
            from_fn(|_: i32| Step::Complete::<i32, Result<i32, &str>>(Err("error"))),
        );
        let second = repeat(|x: i32| x + 1);
        let chained = OkChain {
            coro: Some(first),
            next: second,
        };

        let (initial, mut coro) = chained.init().unwrap_yielded();
        assert_eq!(initial, 10);

        assert_eq!(coro.next(0).unwrap_complete(), Err("error"));
    }

    #[test]
    fn test_ok_chain_init_both_complete_immediately() {
        use crate::build::from_fn;
        let first = (
            10,
            from_fn(|_: i32| Step::Complete::<i32, Result<i32, &str>>(Ok(20))),
        );
        let second = from_fn(|x: i32| Step::Complete(x * 2));
        let chained = OkChain {
            coro: Some(first),
            next: second,
        };

        let (initial, mut coro) = chained.init().unwrap_yielded();
        assert_eq!(initial, 10);

        // First completes with Ok(20), second completes with 40
        assert_eq!(coro.next(0).unwrap_complete(), Ok(40));
    }

    #[test]
    fn test_flatten_init_yields_outer_err() {
        use crate::build::from_fn;
        let coro = from_fn(|_: i32| {
            Step::Complete::<Result<Result<i32, &str>, &str>, Result<Result<i32, &str>, &str>>(
                Err::<Result<i32, &str>, &str>("outer error"),
            )
        });
        let mut flat = flatten(coro);

        assert_eq!(flat.next(0).unwrap_complete(), Err("outer error"));
    }

    #[test]
    fn test_flatten_init_yields_inner_err() {
        use crate::build::from_fn;
        let coro = from_fn(|_: i32| {
            Step::Complete::<Result<Result<i32, &str>, &str>, Result<Result<i32, &str>, &str>>(Ok(
                Err::<i32, &str>("inner error"),
            ))
        });
        let mut flat = flatten(coro);

        assert_eq!(flat.next(0).unwrap_complete(), Err("inner error"));
    }

    #[test]
    fn test_flatten_init_completes_ok_ok() {
        use crate::build::from_fn;
        let coro = from_fn(|_: i32| {
            Step::Complete::<Result<Result<i32, &str>, &str>, Result<Result<i32, &str>, &str>>(Ok(
                Ok::<i32, &str>(100),
            ))
        });
        let init_coro = (Ok(Ok::<i32, &str>(42)), coro);
        let flat = flatten(init_coro);

        let (initial, mut coro) = flat.init().unwrap_yielded();
        assert_eq!(initial, Ok(Ok(42)));
        assert_eq!(coro.next(0).unwrap_complete(), Ok(100));
    }

    #[test]
    fn test_flatten_init_completes_ok_err() {
        use crate::build::from_fn;
        let coro = from_fn(|_: i32| {
            Step::Complete::<Result<Result<i32, &str>, &str>, Result<Result<i32, &str>, &str>>(Ok(
                Err::<i32, &str>("inner error"),
            ))
        });
        let init_coro = (Ok(Ok::<i32, &str>(42)), coro);
        let flat = flatten(init_coro);

        let (initial, mut coro) = flat.init().unwrap_yielded();
        assert_eq!(initial, Ok(Ok(42)));
        assert_eq!(coro.next(0).unwrap_complete(), Err("inner error"));
    }

    #[test]
    fn test_flatten_init_completes_err() {
        use crate::build::from_fn;
        let coro = from_fn(|_: i32| {
            Step::Complete::<Result<Result<i32, &str>, &str>, Result<Result<i32, &str>, &str>>(
                Err::<Result<i32, &str>, &str>("outer error"),
            )
        });
        let init_coro = (Ok::<Result<i32, &str>, &str>(Ok::<i32, &str>(42)), coro);
        let flat = flatten(init_coro);

        let (initial, mut coro) = flat.init().unwrap_yielded();
        assert_eq!(initial, Ok(Ok(42)));
        assert_eq!(coro.next(0).unwrap_complete(), Err("outer error"));
    }

    // Extension trait tests

    #[test]
    fn test_try_sans_ok_map() {
        use crate::build::from_fn;
        use crate::result::TrySans;

        let mut called = false;
        let coro = from_fn(move |x: i32| -> Step<i32, Result<i32, &str>> {
            if !called {
                called = true;
                Step::Yielded(x * 2)
            } else {
                Step::Complete(Ok(x))
            }
        });

        let mut mapped = coro.ok_map(|val| init(val, repeat(move |x: i32| x + val)));

        assert_eq!(mapped.next(5).unwrap_yielded(), 10);
        assert_eq!(mapped.next(3).unwrap_yielded(), 3);
        assert_eq!(mapped.next(7).unwrap_yielded(), 10);
    }

    #[test]
    fn test_try_sans_ok_and_then() {
        use crate::build::from_fn;
        use crate::result::TrySans;

        let mut called = false;
        let coro = from_fn(
            move |x: i32| -> Step<Result<i32, &str>, Result<i32, &str>> {
                if !called {
                    called = true;
                    Step::Yielded(Ok(x * 2))
                } else {
                    Step::Complete(Ok(x))
                }
            },
        );

        let mut chained = coro.ok_and_then(|val| {
            let mut inner_called = false;
            init(
                Ok(val),
                from_fn(
                    move |x: i32| -> Step<Result<i32, &str>, Result<i32, &str>> {
                        if !inner_called {
                            inner_called = true;
                            Step::Yielded(Ok(x + val))
                        } else {
                            Step::Complete(Ok(x))
                        }
                    },
                ),
            )
        });

        assert_eq!(chained.next(5).unwrap_yielded(), Ok(10));
        assert_eq!(chained.next(3).unwrap_yielded(), Ok(3));
        assert_eq!(chained.next(7).unwrap_yielded(), Ok(10));
    }

    #[test]
    fn test_try_sans_ok_chain() {
        use crate::build::from_fn;
        use crate::result::TrySans;

        let mut called = false;
        let first = from_fn(move |x: i32| -> Step<i32, Result<i32, &str>> {
            if !called {
                called = true;
                Step::Yielded(x * 2)
            } else {
                Step::Complete(Ok(x))
            }
        });
        let second = repeat(|x: i32| x + 1);

        let mut chained = first.ok_chain(second);

        assert_eq!(chained.next(5).unwrap_yielded(), 10);
        assert_eq!(chained.next(3).unwrap_yielded(), 4);
        assert_eq!(chained.next(10).unwrap_yielded(), 11);
    }

    #[test]
    fn test_try_sans_flatten() {
        use crate::build::from_fn;
        use crate::result::TrySans;

        let mut called = false;
        #[allow(clippy::type_complexity)]
        let coro = from_fn(move |x: i32| -> Step<
            Result<Result<i32, &str>, &str>,
            Result<Result<i32, &str>, &str>,
        > {
            if !called {
                called = true;
                Step::Yielded(Ok(Ok(x * 2)))
            } else {
                Step::Complete(Ok(Ok(x)))
            }
        });

        let mut flattened = coro.flatten();

        assert_eq!(flattened.next(5).unwrap_yielded(), Ok(Ok(10)));
        assert_eq!(flattened.next(10).unwrap_complete(), Ok(10));
    }

    #[test]
    fn test_try_init_sans_ok_map() {
        use crate::build::from_fn;
        use crate::result::TryInitSans;

        let mut called = false;
        let first_coro = from_fn(move |x: i32| -> Step<i32, Result<i32, &str>> {
            if !called {
                called = true;
                Step::Yielded(x * 2)
            } else {
                Step::Complete(Ok(x))
            }
        });
        let init_first = (10, first_coro);
        let mapped = init_first.ok_map(|val| init(val, repeat(move |x: i32| x + val)));

        let (initial, mut coro) = mapped.init().unwrap_yielded();
        assert_eq!(initial, 10);

        assert_eq!(coro.next(5).unwrap_yielded(), 10);
        assert_eq!(coro.next(0).unwrap_yielded(), 0);
        assert_eq!(coro.next(3).unwrap_yielded(), 3);
    }

    #[test]
    fn test_try_init_sans_flatten() {
        use crate::build::from_fn;
        use crate::result::TryInitSans;

        let coro = from_fn(|_: i32| {
            Step::Complete::<Result<Result<i32, &str>, &str>, Result<Result<i32, &str>, &str>>(Ok(
                Ok::<i32, &str>(100),
            ))
        });
        let init_coro = (Ok(Ok::<i32, &str>(42)), coro);
        let flat = init_coro.flatten();

        let (initial, mut coro) = flat.init().unwrap_yielded();
        assert_eq!(initial, Ok(Ok(42)));
        assert_eq!(coro.next(0).unwrap_complete(), Ok(100));
    }
}
