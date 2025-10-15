//! Core trait for stateful coroutines.
//!
//! This module defines the [`Sans`] trait, the fundamental building block for
//! coroutine-based programming in this library. A [`Sans`] represents a stateful
//! computation that can process input values and yield intermediate results.
//!
//! # The Sans Trait
//!
//! [`Sans<I, O>`] represents a computation that:
//! - Takes input of type `I`
//! - Yields intermediate values of type `O`
//! - Eventually completes with a final value of type `Return`
//!
//! # Examples
//!
//! ```rust
//! use sans::prelude::*;
//!
//! // Create a coroutine that processes one input then completes
//! let mut coro = once(|x: i32| x * 2);
//! assert_eq!(coro.next(5).unwrap_yielded(), 10);
//! assert_eq!(coro.next(3).unwrap_complete(), 3);
//! ```

use std::{
    cell::RefCell,
    fmt,
    rc::Rc,
    sync::{Arc, Mutex},
};

use crate::{
    build::{Once, Repeat, once, repeat},
    compose::{AndThen, Chain, MapInput, MapReturn, MapYield, and_then, chain},
    init::{ShortCircuit, Yielded},
    iter::SansIter,
    step::Step,
};

/// Core trait for stateful computations that process input and yield intermediate values.
///
/// Each call to `next()` either yields an intermediate result or signals completion.
/// This allows building composable, resumable computations.
///
/// ```rust
/// use sans::prelude::*;
///
/// let mut coro = once(|x: i32| x * 2);
/// assert_eq!(coro.next(5).unwrap_yielded(), 10);
/// assert_eq!(coro.next(3).unwrap_complete(), 3); // return
/// ```
pub trait Sans<I, O> {
    /// Type of final result when computation completes
    type Return;

    /// Process input, returning `Yield` to continue or `Return` to complete.
    fn next(&mut self, input: I) -> Step<O, Self::Return>;

    /// Chain with a coroutine created from this coroutine's return value.
    ///
    /// The function `f` receives the return value and must produce a `Yielded<O, T>`.
    /// Use the builder API to construct the result:
    ///
    /// ```rust
    /// use sans::prelude::*;
    ///
    /// let mut coro = once(|x: i32| x * 2)
    ///     .and_then(|val| yielding(val).then(repeat(move |x| x + val)));
    /// ```
    fn and_then<T, F>(self, f: F) -> AndThen<Self, T, F>
    where
        Self: Sized + Sans<I, O, Return = I>,
        T: Sans<I, O>,
        F: FnOnce(Self::Return) -> Yielded<O, T>,
    {
        and_then(self, f)
    }

    fn boxed(self) -> Box<dyn Sans<I, O, Return = Self::Return>>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }

    /// Chain with another coroutine.
    fn chain<R>(self, r: R) -> Chain<Self, R>
    where
        Self: Sized + Sans<I, O, Return = I>,
        R: Sans<I, O>,
    {
        chain(self, r)
    }

    /// Chain with a function that executes once.
    fn chain_once<F>(self, f: F) -> Chain<Self, Once<F>>
    where
        Self: Sized + Sans<I, O, Return = I>,
        F: FnOnce(Self::Return) -> O,
    {
        chain(self, once(f))
    }

    /// Chain with a function that repeats indefinitely.
    fn chain_repeat<F>(self, f: F) -> Chain<Self, Repeat<F>>
    where
        Self: Sized + Sans<I, O, Return = I>,
        F: FnMut(Self::Return) -> O,
    {
        chain(self, repeat(f))
    }

    /// Transform inputs before they reach this coroutine.
    fn map_input<I2, F>(self, f: F) -> MapInput<Self, F>
    where
        Self: Sized,
        F: FnMut(I2) -> I,
    {
        crate::compose::map_input(f, self)
    }

    /// Transform yielded values before returning them.
    fn map_yield<O2, F>(self, f: F) -> MapYield<Self, F, I, O>
    where
        Self: Sized,
        F: FnMut(O) -> O2,
    {
        crate::compose::map_yield(f, self)
    }

    /// Transform the final result when completing.
    fn map_return<D2, F>(self, f: F) -> MapReturn<Self, F>
    where
        Self: Sized,
        F: FnMut(Self::Return) -> D2,
    {
        crate::compose::map_return(f, self)
    }

    /// Convert to an iterator.
    fn into_iter(self) -> SansIter<O, Self>
    where
        Self: Sized + Sans<(), O>,
    {
        SansIter::new(self)
    }
}

#[derive(Debug)]
pub struct PoisonError;

impl fmt::Display for PoisonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "lock was poisoned")
    }
}

impl std::error::Error for PoisonError {}

impl<I, O, C> Sans<I, O> for Arc<Mutex<C>>
where
    C: Sans<I, O>,
{
    type Return = Result<C::Return, PoisonError>;
    fn next(&mut self, input: I) -> Step<O, Self::Return> {
        match self.lock().map_err(|_| PoisonError) {
            Ok(mut f) => f.next(input).map_complete(Ok),
            Err(e) => Step::Complete(Err(e)),
        }
    }
}

impl<I, O, C> Sans<I, O> for Rc<RefCell<C>>
where
    C: Sans<I, O>,
{
    type Return = C::Return;
    fn next(&mut self, input: I) -> Step<O, Self::Return> {
        let mut v = self.as_ref().borrow_mut();
        v.next(input)
    }
}

impl<I, O, C> Sans<I, O> for Option<C>
where
    C: Sans<I, O>,
{
    type Return = Option<C::Return>;
    fn next(&mut self, input: I) -> Step<O, Self::Return> {
        match self {
            Some(c) => c.next(input).map_complete(Some),
            None => Step::Complete(None),
        }
    }
}

impl<I, O, L, R> Sans<I, O> for either::Either<L, R>
where
    L: Sans<I, O>,
    R: Sans<I, O, Return = L::Return>,
{
    type Return = L::Return;
    fn next(&mut self, input: I) -> Step<O, Self::Return> {
        match self {
            either::Either::Left(l) => l.next(input),
            either::Either::Right(r) => r.next(input),
        }
    }
}

impl<I, O, D> Sans<I, O> for Box<dyn Sans<I, O, Return = D>> {
    type Return = D;

    fn next(&mut self, input: I) -> Step<O, Self::Return> {
        (**self).next(input)
    }
}

impl<I, O, D> Sans<I, O> for &'_ mut dyn Sans<I, O, Return = D> {
    type Return = D;

    fn next(&mut self, input: I) -> Step<O, Self::Return> {
        (*self).next(input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chain_switches_to_second_coroutine_after_first_done() {
        let mut coro = once(|val: u32| val + 1).chain(repeat(|val: u32| val * 2));

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
    fn test_map_done_applies_after_chain_completion() {
        let mut coro = chain(once(|val: u32| val + 1), once(|val: u32| val * 2))
            .map_return(|done: u32| format!("resume={done}"));

        assert_eq!(coro.next(5).unwrap_yielded(), 6);
        assert_eq!(coro.next(6).unwrap_yielded(), 12);
        assert_eq!(coro.next(7).unwrap_complete(), "resume=7".to_string());
    }
}
