//! Coroutines with initial output.
//!
//! This module provides types and builders for coroutines that can produce
//! output immediately upon initialization, before receiving any input.
//!
//! # Builder API
//!
//! The recommended way to create initialization results is through the builder API:
//!
//! - [`yielding(output).then(sans)`](yielding) - Creates a [`Yielded<O, S>`] that yields initial output before continuing
//! - [`shortcircuit().then(sans)`](shortcircuit) or [`shortcircuit().returning(done)`](shortcircuit) - Creates a [`ShortCircuit<S, R>`] that may complete early
//! - [`build().then(sans)`](build) - Wraps a [`Sans`] without initial output
//!
//! # Types
//!
//! - [`Yielded<O, S>`] - Result of initialization that yields output before continuing with coroutine `S`
//! - [`ShortCircuit<S, R>`] - Result that may be `Pending(S)` or `Complete(R)`, allowing early completion
//!
//! # Examples
//!
//! ```rust
//! use sans::prelude::*;
//!
//! // Create a coroutine with initial output using the builder API
//! let Yielded(initial, mut cont) = yielding(42).then(repeat(|x: i32| x + 1));
//! assert_eq!(initial, 42);
//! assert_eq!(cont.next(10).unwrap_yielded(), 11);
//! ```
//!
//! # Legacy API
//!
//! The [`InitSans`] trait is deprecated. Use the builder API and concrete types instead.

use crate::{
    build::{once, repeat, Once, Repeat},
    compose::{
        init_chain, init_map_input, init_map_return, init_map_yield, AndThen, Chain, MapInput,
        MapReturn, MapYield,
    },
    iter::InitSansIter,
    Sans, Step,
};

/// Result of initializing a coroutine that must yield before continuing.
#[derive(Debug, Clone, Copy)]
pub struct Yielded<O, S>(pub O, pub S);

impl<O, S> Yielded<O, S> {
    /// Splits the yielded pair into its components.
    ///
    /// Returns a tuple of `(output, continuation)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use sans::prelude::*;
    ///
    /// let yielded = yielding(42).then(repeat(|x: i32| x + 1));
    /// let (output, cont) = yielded.split();
    /// assert_eq!(output, 42);
    /// ```
    pub fn split(self) -> (O, S) {
        (self.0, self.1)
    }

    /// Converts from `&Yielded<O, S>` to `Yielded<&O, &S>`.
    ///
    /// Useful for inspecting the yielded value and continuation without consuming them.
    pub fn as_ref(&self) -> Yielded<&O, &S> {
        Yielded(&self.0, &self.1)
    }

    /// Converts from `&mut Yielded<O, S>` to `Yielded<&mut O, &mut S>`.
    ///
    /// Useful for mutating the yielded value or continuation in place.
    pub fn as_mut(&mut self) -> Yielded<&mut O, &mut S> {
        Yielded(&mut self.0, &mut self.1)
    }

    /// Maps the continuation stored inside this value.
    ///
    /// This transforms the continuation coroutine while preserving the initial output.
    ///
    /// # Examples
    ///
    /// ```
    /// use sans::prelude::*;
    ///
    /// let yielded = yielding(10).then(once(|x: i32| x + 1));
    /// let mapped = yielded.map_next(|sans| sans.map_yield(|x| x * 2));
    /// ```
    pub fn map_next<F, T>(self, f: F) -> Yielded<O, T>
    where
        F: FnOnce(S) -> T,
    {
        let (output, next) = self.split();
        Yielded(output, f(next))
    }

    /// Transforms coroutine inputs before they reach the continuation.
    ///
    /// This allows you to preprocess or convert input values before the coroutine processes them.
    pub fn map_input<I1, I2, F>(self, f: F) -> Yielded<O, MapInput<S, F>>
    where
        S: Sans<I2, O>,
        F: FnMut(I1) -> I2,
    {
        let (output, next) = self.split();
        Yielded(output, next.map_input(f))
    }

    /// Transforms yielded values produced by the continuation.
    ///
    /// This applies the transformation to both the initial output and all future yields from the continuation.
    pub fn map_yield<I, O2, F>(self, mut f: F) -> Yielded<O2, MapYield<S, F, I, O>>
    where
        S: Sans<I, O>,
        F: FnMut(O) -> O2,
    {
        let (output, next) = self.split();
        let mapped_output = f(output);
        Yielded(mapped_output, next.map_yield(f))
    }

    /// Transforms the return value produced when the continuation completes.
    ///
    /// This doesn't affect yielded values, only the final return value.
    pub fn map_return<I, D2, F>(self, f: F) -> Yielded<O, MapReturn<S, F>>
    where
        S: Sans<I, O>,
        F: FnMut(S::Return) -> D2,
    {
        let (output, next) = self.split();
        Yielded(output, next.map_return(f))
    }

    /// Chains the continuation with another coroutine.
    ///
    /// When the first coroutine completes, its return value is passed as input to the second coroutine.
    pub fn chain<I, R>(self, r: R) -> Yielded<O, Chain<S, R>>
    where
        S: Sans<I, O, Return = I>,
        R: Sans<I, O>,
    {
        let (output, next) = self.split();
        Yielded(output, next.chain(r))
    }

    /// Chains the continuation with a function that produces a `Yielded` result.
    ///
    /// This allows chaining based on the first coroutine's return value.
    pub fn and_then<I, T, F>(self, f: F) -> Yielded<O, AndThen<S, T, F>>
    where
        S: Sans<I, O>,
        T: Sans<I, O>,
        F: FnOnce(S::Return) -> Yielded<O, T>,
    {
        let (output, next) = self.split();
        Yielded(output, next.and_then(f))
    }
}

impl<O, S> From<(O, S)> for Yielded<O, S> {
    fn from(value: (O, S)) -> Self {
        Yielded(value.0, value.1)
    }
}

impl<O, S> From<Yielded<O, S>> for (O, S) {
    fn from(value: Yielded<O, S>) -> Self {
        value.split()
    }
}

/// Initialization result that may already be complete.
#[derive(Debug, Clone, Copy)]
pub enum ShortCircuit<S, R> {
    Pending(S),
    Complete(R),
}

impl<S, R> ShortCircuit<S, R> {
    /// Returns `true` if this is a `Pending` variant.
    ///
    /// # Examples
    ///
    /// ```
    /// use sans::prelude::*;
    ///
    /// let pending: ShortCircuit<i32, ()> = ShortCircuit::Pending(42);
    /// assert!(pending.is_pending());
    /// ```
    pub fn is_pending(&self) -> bool {
        matches!(self, ShortCircuit::Pending(_))
    }

    /// Returns `true` if this is a `Complete` variant.
    ///
    /// # Examples
    ///
    /// ```
    /// use sans::prelude::*;
    ///
    /// let complete: ShortCircuit<i32, ()> = ShortCircuit::Complete(());
    /// assert!(complete.is_complete());
    /// ```
    pub fn is_complete(&self) -> bool {
        matches!(self, ShortCircuit::Complete(_))
    }

    /// Returns the pending continuation, panicking if this is `Complete`.
    ///
    /// # Panics
    ///
    /// Panics if called on a `Complete` variant.
    ///
    /// # Examples
    ///
    /// ```
    /// use sans::prelude::*;
    ///
    /// let pending: ShortCircuit<i32, ()> = ShortCircuit::Pending(42);
    /// assert_eq!(pending.unwrap_pending(), 42);
    /// ```
    pub fn unwrap_pending(self) -> S {
        match self {
            ShortCircuit::Pending(s) => s,
            ShortCircuit::Complete(_) => panic!("called `unwrap_pending()` on a `Complete` value"),
        }
    }

    /// Returns the completion value, panicking if this is `Pending`.
    ///
    /// # Panics
    ///
    /// Panics if called on a `Pending` variant.
    ///
    /// # Examples
    ///
    /// ```
    /// use sans::prelude::*;
    ///
    /// let complete: ShortCircuit<i32, ()> = ShortCircuit::Complete(());
    /// assert_eq!(complete.unwrap_complete(), ());
    /// ```
    pub fn unwrap_complete(self) -> R {
        match self {
            ShortCircuit::Pending(_) => panic!("called `unwrap_complete()` on a `Pending` value"),
            ShortCircuit::Complete(r) => r,
        }
    }

    /// Converts from `&ShortCircuit<S, R>` to `ShortCircuit<&S, &R>`.
    ///
    /// Useful for inspecting values without consuming the enum.
    pub fn as_ref(&self) -> ShortCircuit<&S, &R> {
        match self {
            ShortCircuit::Pending(s) => ShortCircuit::Pending(s),
            ShortCircuit::Complete(r) => ShortCircuit::Complete(r),
        }
    }

    /// Converts from `&mut ShortCircuit<S, R>` to `ShortCircuit<&mut S, &mut R>`.
    ///
    /// Useful for mutating values in place.
    pub fn as_mut(&mut self) -> ShortCircuit<&mut S, &mut R> {
        match self {
            ShortCircuit::Pending(s) => ShortCircuit::Pending(s),
            ShortCircuit::Complete(r) => ShortCircuit::Complete(r),
        }
    }
    /// Maps the pending continuation, leaving `Complete` unchanged.
    ///
    /// # Examples
    ///
    /// ```
    /// use sans::prelude::*;
    ///
    /// let pending: ShortCircuit<i32, ()> = ShortCircuit::Pending(42);
    /// let mapped = pending.map_pending(|x| x * 2);
    /// assert_eq!(mapped.unwrap_pending(), 84);
    /// ```
    pub fn map_pending<F, T>(self, f: F) -> ShortCircuit<T, R>
    where
        F: FnOnce(S) -> T,
    {
        match self {
            ShortCircuit::Pending(s) => ShortCircuit::Pending(f(s)),
            ShortCircuit::Complete(r) => ShortCircuit::Complete(r),
        }
    }

    /// Flat maps the pending continuation, leaving `Complete` unchanged.
    ///
    /// # Examples
    ///
    /// ```
    /// use sans::prelude::*;
    ///
    /// let pending: ShortCircuit<i32, ()> = ShortCircuit::Pending(42);
    /// let flat_mapped = pending.flat_map_pending(|x| {
    ///     if x > 0 {
    ///         ShortCircuit::Pending(x * 2)
    ///     } else {
    ///         ShortCircuit::Complete(())
    ///     }
    /// });
    /// assert_eq!(flat_mapped.unwrap_pending(), 84);
    /// ```
    pub fn flat_map_pending<F, T>(self, f: F) -> ShortCircuit<T, R>
    where
        F: FnOnce(S) -> ShortCircuit<T, R>,
    {
        match self {
            ShortCircuit::Pending(s) => f(s),
            ShortCircuit::Complete(r) => ShortCircuit::Complete(r),
        }
    }

    /// Maps the completion value, leaving `Pending` unchanged.
    ///
    /// # Examples
    ///
    /// ```
    /// use sans::prelude::*;
    ///
    /// let complete: ShortCircuit<i32, i32> = ShortCircuit::Complete(42);
    /// let mapped = complete.map_complete(|x| x * 2);
    /// assert_eq!(mapped.unwrap_complete(), 84);
    /// ```
    pub fn map_complete<F, R2>(self, f: F) -> ShortCircuit<S, R2>
    where
        F: FnOnce(R) -> R2,
    {
        match self {
            ShortCircuit::Pending(s) => ShortCircuit::Pending(s),
            ShortCircuit::Complete(r) => ShortCircuit::Complete(f(r)),
        }
    }

    /// Converts into a `Result`, using `Ok` for `Pending` and `Err` for `Complete`.
    ///
    /// This is useful when you want to treat early completion as an error condition.
    pub fn into_result(self) -> Result<S, R> {
        match self {
            ShortCircuit::Pending(s) => Ok(s),
            ShortCircuit::Complete(r) => Err(r),
        }
    }

    /// Transform inputs to the pending continuation.
    pub fn map_input<I1, I2, O, F>(self, f: F) -> ShortCircuit<MapInput<S, F>, R>
    where
        S: Sans<I2, O>,
        F: FnMut(I1) -> I2,
    {
        self.map_pending(|s| s.map_input(f))
    }

    /// Transform yields from the pending continuation.
    pub fn map_yield<I, O1, O2, F>(self, f: F) -> ShortCircuit<MapYield<S, F, I, O1>, R>
    where
        S: Sans<I, O1>,
        F: FnMut(O1) -> O2,
    {
        self.map_pending(|s| s.map_yield(f))
    }

    /// Transform the completion value.
    pub fn map_return<I, O, F, R2>(self, mut f: F) -> ShortCircuit<MapReturn<S, F>, R2>
    where
        F: FnMut(R) -> R2,
        S: Sans<I, O, Return = R>,
    {
        match self {
            ShortCircuit::Pending(s) => ShortCircuit::Pending(s.map_return(f)),
            ShortCircuit::Complete(r) => ShortCircuit::Complete(f(r)),
        }
    }

    /// Chain the pending continuation with another coroutine.
    pub fn chain<I, O, R2>(self, r: R2) -> ShortCircuit<Chain<S, R2>, R>
    where
        S: Sans<I, O, Return = I>,
        R2: Sans<I, O>,
    {
        self.map_pending(|s| s.chain(r))
    }

    /// Chain the pending continuation with a function that produces a `Yielded` result.
    pub fn and_then<I, O, T, F>(self, f: F) -> ShortCircuit<AndThen<S, T, F>, R>
    where
        S: Sans<I, O, Return = R>,
        T: Sans<I, O, Return = R>,
        F: FnOnce(S::Return) -> Yielded<O, T>,
    {
        self.map_pending(|s| s.and_then(f))
    }
}

impl<O, S, R> From<Step<(O, S), R>> for ShortCircuit<Yielded<O, S>, R> {
    fn from(step: Step<(O, S), R>) -> Self {
        match step {
            Step::Yielded((o, s)) => ShortCircuit::Pending(Yielded(o, s)),
            Step::Complete(r) => ShortCircuit::Complete(r),
        }
    }
}

impl<S, R> From<Step<S, R>> for ShortCircuit<S, R> {
    fn from(step: Step<S, R>) -> Self {
        match step {
            Step::Yielded(s) => ShortCircuit::Pending(s),
            Step::Complete(r) => ShortCircuit::Complete(r),
        }
    }
}

impl<O, S, R> From<(O, S)> for ShortCircuit<Yielded<O, S>, R> {
    fn from(value: (O, S)) -> Self {
        ShortCircuit::Pending(Yielded(value.0, value.1))
    }
}

/// Builder entry point for constructing initialization states.
///
/// Use this when you want to wrap a coroutine without yielding an initial value.
/// You can then call `.yielding()` or `.shortcircuit()` on the result, or `.then(sans)` to wrap directly.
///
/// # Examples
///
/// ```
/// use sans::prelude::*;
///
/// // Wrap a coroutine directly
/// let coro = build().then(repeat(|x: i32| x + 1));
/// ```
pub fn build() -> Build {
    Build
}

/// Creates a builder that yields an initial value.
///
/// This is the most common way to start building an initialization result. Call `.then(sans)`
/// to attach a continuation coroutine.
///
/// # Examples
///
/// ```
/// use sans::prelude::*;
///
/// // Yield 42 and then count up from there
/// let Yielded(initial, mut cont) = yielding(42).then(repeat(|x: i32| x + 1));
/// assert_eq!(initial, 42);
/// assert_eq!(cont.next(10).unwrap_yielded(), 11);
/// ```
pub fn yielding<O>(output: O) -> YieldBuild<O> {
    YieldBuild { output }
}

/// Creates a builder that may short-circuit during initialization.
///
/// Use this when initialization might complete immediately without yielding a continuation.
/// Call `.then(sans)` to provide a pending continuation, or `.returning(value)` to complete immediately.
///
/// # Examples
///
/// ```
/// use sans::prelude::*;
///
/// // Create a pending short-circuit (type annotation provides R)
/// let pending: ShortCircuit<_, ()> = shortcircuit().then(repeat(|x: i32| x + 1));
/// assert!(pending.is_pending());
///
/// // Create a complete short-circuit
/// let complete: ShortCircuit<(), i32> = shortcircuit().returning(42);
/// assert!(complete.is_complete());
/// ```
pub fn shortcircuit() -> ShortCircuitBuild {
    ShortCircuitBuild
}

/// Builder state before any initialization behaviour is chosen.
#[derive(Debug, Default, Clone, Copy)]
pub struct Build;

impl Build {
    pub fn yielding<O>(self, output: O) -> YieldBuild<O> {
        YieldBuild { output }
    }

    pub fn shortcircuit(self) -> ShortCircuitBuild {
        ShortCircuitBuild
    }

    pub fn then<I, O, S>(self, sans: S) -> S
    where
        S: Sans<I, O>,
    {
        sans
    }
}

/// Builder state representing an initial yield with guaranteed continuation.
#[derive(Debug, Clone, Copy)]
pub struct YieldBuild<O> {
    output: O,
}

impl<O> YieldBuild<O> {
    pub fn then<I, S>(self, sans: S) -> Yielded<O, S>
    where
        S: Sans<I, O>,
    {
        Yielded(self.output, sans)
    }

    pub fn shortcircuit(self) -> YieldShortCircuitBuild<O> {
        YieldShortCircuitBuild {
            output: self.output,
        }
    }
}

/// Builder state representing a potential short-circuit without initial yield.
#[derive(Debug, Default, Clone, Copy)]
pub struct ShortCircuitBuild;

impl ShortCircuitBuild {
    pub fn then<I, O, S, R>(self, sans: S) -> ShortCircuit<S, R>
    where
        S: Sans<I, O>,
    {
        ShortCircuit::Pending(sans)
    }

    pub fn returning<S, R>(self, done: R) -> ShortCircuit<S, R> {
        ShortCircuit::Complete(done)
    }

    pub fn yielding<O>(self, output: O) -> YieldShortCircuitBuild<O> {
        YieldShortCircuitBuild { output }
    }
}

/// Builder state representing an initial yield that may short-circuit.
#[derive(Debug, Clone, Copy)]
pub struct YieldShortCircuitBuild<O> {
    output: O,
}

impl<O> YieldShortCircuitBuild<O> {
    pub fn then<I, S, R>(self, sans: S) -> ShortCircuit<Yielded<O, S>, R>
    where
        S: Sans<I, O>,
    {
        ShortCircuit::Pending(Yielded(self.output, sans))
    }

    pub fn returning<S, R>(self, done: R) -> ShortCircuit<Yielded<O, S>, R> {
        ShortCircuit::Complete(done)
    }
}

/// Computations that yield an initial value before processing input.
///
/// **Deprecated:** Use the builder API instead: `yielding(output).then(sans)` returns `Yielded<O, S>`,
/// or use `ShortCircuit<Yielded<O, S>, R>` for fallible initialization.
///
/// Unlike `Sans`, `InitSans` coroutines can produce output immediately, making them ideal
/// for pipeline initialization or generators with seed values.
///
/// ```rust
/// use sans::prelude::*;
///
/// let coro = init_once(42, |x: i32| x + 1);
/// let (initial, mut cont) = coro.init().unwrap_yielded();
/// assert_eq!(initial, 42);
/// ```
#[deprecated(
    since = "0.2.0",
    note = "Use the builder API: yielding(output).then(sans) or ShortCircuit<Yielded<O, S>, R>"
)]
pub trait InitSans<I, O> {
    type Return;
    type Next: Sans<I, O, Return = Self::Return>;

    /// Execute the first coroutine.
    ///
    /// Returns `Yield((yield_value, continuation))` for normal execution,
    /// or `Done(done_value)` if the computation completes immediately.
    #[allow(clippy::type_complexity)]
    fn init(self) -> Step<(O, Self::Next), Self::Return>;

    /// Chain with a coroutine.
    fn chain<R>(self, r: R) -> Chain<Self, R>
    where
        Self: Sized + InitSans<I, O, Return = I>,
        R: Sans<I, O>,
    {
        init_chain(self, r)
    }

    /// Chain with a function that executes once.
    fn chain_once<F>(self, f: F) -> Chain<Self, Once<F>>
    where
        Self: Sized + InitSans<I, O, Return = I>,
        F: FnOnce(Self::Return) -> O,
    {
        self.chain(once(f))
    }

    /// Chain with a function that repeats indefinitely.
    fn chain_repeat<F>(self, f: F) -> Chain<Self, Repeat<F>>
    where
        Self: Sized + InitSans<I, O, Return = I>,
        F: FnMut(Self::Return) -> O,
    {
        self.chain(repeat(f))
    }

    /// Transform inputs before they reach the underlying coroutine.
    fn map_input<I2, F>(self, f: F) -> MapInput<Self, F>
    where
        Self: Sized,
        F: FnMut(I2) -> I,
    {
        init_map_input(f, self)
    }

    /// Transform yielded values before returning them.
    fn map_yield<O2, F>(self, f: F) -> MapYield<Self, F, I, O>
    where
        Self: Sized,
        F: FnMut(O) -> O2,
    {
        init_map_yield(f, self)
    }

    /// Transform the final result when completing.
    fn map_done<D2, F>(self, f: F) -> MapReturn<Self, F>
    where
        Self: Sized,
        F: FnMut(Self::Return) -> D2,
    {
        init_map_return(f, self)
    }

    /// Convert to an iterator.
    fn into_iter(self) -> InitSansIter<O, Self>
    where
        Self: Sized + InitSans<(), O>,
    {
        InitSansIter::new(self)
    }
}

impl<I, O, S> InitSans<I, O> for (O, S)
where
    S: Sans<I, O>,
{
    type Next = S;
    type Return = S::Return;

    fn init(self) -> Step<(O, S), Self::Return> {
        Step::Yielded(self)
    }
}

impl<I, O, S> InitSans<I, O> for Step<(O, S), S::Return>
where
    S: Sans<I, O>,
{
    type Next = S;
    type Return = S::Return;

    fn init(self) -> Step<(O, S), Self::Return> {
        self
    }
}

impl<I, O, C> InitSans<I, O> for Option<C>
where
    C: InitSans<I, O>,
{
    type Next = Option<C::Next>;
    type Return = Option<C::Return>;

    fn init(self) -> Step<(O, Self::Next), Self::Return> {
        match self {
            Some(c) => match c.init() {
                Step::Yielded((o, next)) => Step::Yielded((o, Some(next))),
                Step::Complete(d) => Step::Complete(Some(d)),
            },
            None => Step::Complete(None),
        }
    }
}

impl<I, O, L, R> InitSans<I, O> for either::Either<L, R>
where
    L: InitSans<I, O>,
    R: InitSans<I, O, Return = L::Return>,
{
    type Next = either::Either<L::Next, R::Next>;
    type Return = L::Return;

    fn init(self) -> Step<(O, Self::Next), Self::Return> {
        match self {
            either::Either::Left(l) => match l.init() {
                Step::Yielded((o, next_l)) => Step::Yielded((o, either::Either::Left(next_l))),
                Step::Complete(resume) => Step::Complete(resume),
            },
            either::Either::Right(r) => match r.init() {
                Step::Yielded((o, next_r)) => Step::Yielded((o, either::Either::Right(next_r))),
                Step::Complete(resume) => Step::Complete(resume),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build::{init_once, init_repeat, once, repeat, Repeat};

    fn add_three(value: i32) -> i32 {
        value + 3
    }

    fn plus_one_fn(value: i32) -> i32 {
        value + 1
    }

    #[derive(Clone, Copy, Debug)]
    struct ImmediateFirstDone;

    impl Sans<&'static str, &'static str> for ImmediateFirstDone {
        type Return = &'static str;

        fn next(&mut self, input: &'static str) -> Step<&'static str, Self::Return> {
            Step::Complete(input)
        }
    }

    impl InitSans<&'static str, &'static str> for ImmediateFirstDone {
        type Next = Self;
        type Return = &'static str;

        fn init(self) -> Step<(&'static str, Self::Next), Self::Return> {
            Step::Complete("left-done")
        }
    }

    #[test]
    fn test_chain_once_into_repeat() {
        let initializer = init_once(10_u32, |input: u32| input + 5);
        let mut multiplier = 2_u32;
        let repeater = repeat(move |input: u32| {
            let output = input * multiplier;
            multiplier += 1;
            output
        });

        let (first_yield, mut coro) = initializer.chain(repeater).init().unwrap_yielded();
        assert_eq!(10, first_yield);
        assert_eq!(13, coro.next(8).unwrap_yielded());
        assert_eq!(16, coro.next(8).unwrap_yielded());
        assert_eq!(24, coro.next(8).unwrap_yielded());
        assert_eq!(32, coro.next(8).unwrap_yielded());
    }

    #[test]
    fn test_map_input_and_map_yield_pipeline() {
        let mut total = 0_i64;
        let (initial_total, mut coro) = init_repeat(0_i64, move |delta: i64| {
            total += delta;
            total
        })
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
        .map_yield(|value: i64| format!("total={value}"))
        .init()
        .unwrap_yielded();

        assert_eq!("total=0", initial_total);
        assert_eq!("total=5", coro.next("add 5").unwrap_yielded());
        assert_eq!("total=2", coro.next("sub 3").unwrap_yielded());
        assert_eq!("total=7", coro.next("add 5").unwrap_yielded());
    }

    #[test]
    fn test_chain_and_map_done_resume_flow() {
        use crate::build::once;
        let initializer = init_once(42_u32, |input: u32| input + 1);
        let finisher = once(|input: u32| input * 3);

        let first = initializer.chain(finisher);
        let (first_value, mut coro) = first
            .map_yield(|resume: u32| (resume + 7) as i32)
            .map_done(|done: u32| done as i32 * 3)
            .init()
            .unwrap_yielded();

        assert_eq!(49, first_value);
        assert_eq!(18, coro.next(10).unwrap_yielded());
        assert_eq!(37, coro.next(10).unwrap_yielded());
        assert_eq!(30i32, coro.next(10).unwrap_complete());
    }

    #[test]
    fn test_either_first_right_branch_selected() {
        #[allow(clippy::type_complexity)]
        let coro: either::Either<
            (i32, Repeat<fn(i32) -> i32>),
            (i32, Repeat<fn(i32) -> i32>),
        > = either::Either::Right(init_repeat(2_i32, add_three));

        let (first_value, mut next_coro) = coro.init().unwrap_yielded();
        assert_eq!(2, first_value);
        assert_eq!(5, next_coro.next(2).unwrap_yielded());
        assert_eq!(6, next_coro.next(3).unwrap_yielded());
    }

    #[test]
    fn test_either_first_left_done_returns_resume() {
        let coro: either::Either<ImmediateFirstDone, ImmediateFirstDone> =
            either::Either::Left(ImmediateFirstDone);

        let resume = coro.init().unwrap_complete();
        assert_eq!("left-done", resume);
    }

    #[test]
    fn test_first_ext_map_input_yield_done() {
        use crate::build::once;
        let initializer = init_once(5_u32, |input: u32| input + 2);
        let finisher = once(|value: u32| value * 2);

        let (first_value, mut rest) = initializer
            .chain(finisher)
            .map_input(|text: &str| text.parse::<u32>().expect("number"))
            .map_yield(|value: u32| format!("value={value}"))
            .map_done(|resume: u32| format!("done={resume}"))
            .init()
            .unwrap_yielded();
        assert_eq!("value=5", first_value);
        assert_eq!("value=9", rest.next("7").unwrap_yielded());
        assert_eq!("value=16", rest.next("8").unwrap_yielded());
        assert_eq!("done=9", rest.next("9").unwrap_complete());
    }

    #[test]
    fn yielded_round_trip_and_maps() {
        let yielded: Yielded<_, _> = (5_i32, repeat(|x: i32| x + 1)).into();
        let (initial, mut cont) = yielded.into();
        assert_eq!(5, initial);
        assert_eq!(2, cont.next(1).unwrap_yielded());

        let mapped_input = yielding(7)
            .then(repeat(|x: i32| x + 2))
            .map_input(|text: &str| text.parse::<i32>().unwrap());
        let (initial, mut cont) = mapped_input.into();
        assert_eq!(7, initial);
        assert_eq!(9, cont.next("7").unwrap_yielded());

        let mapped_yield = yielding(3)
            .then(repeat(|x: i32| x * 2))
            .map_yield(|value| value + 1);
        let (initial, mut cont) = mapped_yield.into();
        assert_eq!(4, initial);
        assert_eq!(7, cont.next(3).unwrap_yielded());

        let mapped_return = yielding(0)
            .then(once(|value: i32| value))
            .map_return::<i32, _, _>(|ret| ret + 5);
        let (initial, mut cont) = mapped_return.into();
        assert_eq!(0, initial);
        assert_eq!(10, cont.next(10).unwrap_yielded());
        assert_eq!(16, cont.next(11).unwrap_complete());
    }

    #[test]
    fn yielded_chain_and_then() {
        let chained = yielding(2)
            .then(once(|x: i32| x + 1))
            .chain(repeat(|x: i32| x * 2));
        let (initial, mut cont) = chained.into();
        assert_eq!(2, initial);
        assert_eq!(4, cont.next(3).unwrap_yielded());
        assert_eq!(8, cont.next(4).unwrap_yielded());

        let appended = yielding(1)
            .then(once(|x: i32| x + 1))
            .and_then(|value| yielding(value * 2).then(repeat(move |input: i32| input + value)));
        let (initial, mut cont) = appended.into();
        assert_eq!(1, initial);
        assert_eq!(3, cont.next(2).unwrap_yielded());
    }

    #[test]
    fn shortcircuit_conversions_and_maps() {
        let step_pending: Step<(i32, Repeat<fn(i32) -> i32>), &str> =
            Step::Yielded((4, repeat(plus_one_fn as fn(i32) -> i32)));
        let mut pending: ShortCircuit<Yielded<_, _>, _> = step_pending.into();
        match &mut pending {
            ShortCircuit::Pending(Yielded(output, sans)) => {
                assert_eq!(4, *output);
                assert_eq!(6, sans.next(5).unwrap_yielded());
            }
            ShortCircuit::Complete(_) => panic!("expected pending"),
        }

        let completed: ShortCircuit<Yielded<i32, Repeat<fn(i32) -> i32>>, _> =
            Step::<(i32, Repeat<fn(i32) -> i32>), &str>::Complete("done").into();
        assert!(matches!(completed, ShortCircuit::Complete("done")));

        let mapped = ShortCircuit::Pending(repeat(|x: i32| x + 1))
            .map_input(|text: &str| text.parse::<i32>().unwrap())
            .map_yield(|value| value * 3)
            .map_return(|msg: i32| format!("{msg}!!!"));
        match mapped {
            ShortCircuit::Pending(mut sans) => {
                assert_eq!(18, sans.next("5").unwrap_yielded());
            }
            ShortCircuit::Complete(msg) => panic!("unexpected completion: {msg}"),
        }
    }

    #[test]
    fn builder_state_transitions() {
        let mut plain = build().then(once(|x: i32| x + 1));
        assert_eq!(4, plain.next(3).unwrap_yielded());
        assert_eq!(5, plain.next(5).unwrap_complete());

        let Yielded(initial, mut cont) = yielding(10).then(once(|x: i32| x + 2));
        assert_eq!(10, initial);
        assert_eq!(5, cont.next(3).unwrap_yielded());
        assert_eq!(4, cont.next(4).unwrap_complete());

        let sc: ShortCircuit<_, &str> = shortcircuit().then(once(|x: i32| x + 1));
        match sc {
            ShortCircuit::Pending(mut sans) => {
                assert_eq!(6, sans.next(5).unwrap_yielded());
                assert_eq!(7, sans.next(7).unwrap_complete());
            }
            ShortCircuit::Complete(_) => panic!("expected pending"),
        }

        let sc_with_yield: ShortCircuit<Yielded<_, _>, &'static str> =
            yielding(3).shortcircuit().then(once(|x: i32| x + 1));
        match sc_with_yield {
            ShortCircuit::Pending(Yielded(output, mut sans)) => {
                assert_eq!(3, output);
                assert_eq!(2, sans.next(1).unwrap_yielded());
                assert_eq!(8, sans.next(8).unwrap_complete());
            }
            ShortCircuit::Complete(_) => panic!("expected pending"),
        }

        let sc_complete: ShortCircuit<(), &str> = shortcircuit().returning("done");
        assert!(matches!(sc_complete, ShortCircuit::Complete("done")));
    }
}
