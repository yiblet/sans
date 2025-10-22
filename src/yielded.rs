//! Coroutines with initial output.
//!
//! This module provides the [`Yielded<O, S>`] type for coroutines that produce
//! output immediately upon initialization, before receiving any input.
//!
//! # Examples
//!
//! ```rust
//! use sans::prelude::*;
//!
//! // Create a coroutine with initial output
//! let Yielded(initial, mut cont) = Yielded(42, repeat(|x: i32| x + 1));
//! assert_eq!(initial, 42);
//! assert_eq!(cont.next(10).unwrap_yielded(), 11);
//! ```

use crate::{
    Sans,
    compose::{AndThen, Chain, MapInput, MapReturn, MapYield},
    iter::SansIter,
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
    /// let yielded = Yielded(42, repeat(|x: i32| x + 1));
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
    /// let yielded = Yielded(10, once(|x: i32| x + 1));
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

    /// Chains the continuation with a function that produces a `(O, T)` tuple.
    ///
    /// This allows chaining based on the first coroutine's return value.
    pub fn and_then<I, T, F>(self, f: F) -> Yielded<O, AndThen<S, T, F>>
    where
        S: Sans<I, O>,
        T: Sans<I, O>,
        F: FnOnce(S::Return) -> (O, T),
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

impl<O, S> IntoIterator for Yielded<O, S>
where
    S: Sans<(), O>,
{
    type Item = O;
    type IntoIter = SansIter<O, S>;

    fn into_iter(self) -> Self::IntoIter {
        SansIter::from_yielded(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::func::{once, repeat};

    #[test]
    fn yielded_round_trip_and_maps() {
        let yielded: Yielded<_, _> = (5_i32, repeat(|x: i32| x + 1)).into();
        let (initial, mut cont) = yielded.into();
        assert_eq!(5, initial);
        assert_eq!(2, cont.next(1).unwrap_yielded());

        let mapped_input =
            Yielded(7, repeat(|x: i32| x + 2)).map_input(|text: &str| text.parse::<i32>().unwrap());
        let (initial, mut cont) = mapped_input.into();
        assert_eq!(7, initial);
        assert_eq!(9, cont.next("7").unwrap_yielded());

        let mapped_yield = Yielded(3, repeat(|x: i32| x * 2)).map_yield(|value| value + 1);
        let (initial, mut cont) = mapped_yield.into();
        assert_eq!(4, initial);
        assert_eq!(7, cont.next(3).unwrap_yielded());

        let mapped_return =
            Yielded(0, once(|value: i32| value)).map_return::<i32, _, _>(|ret| ret + 5);
        let (initial, mut cont) = mapped_return.into();
        assert_eq!(0, initial);
        assert_eq!(10, cont.next(10).unwrap_yielded());
        assert_eq!(16, cont.next(11).unwrap_complete());
    }

    #[test]
    fn yielded_chain_and_then() {
        let chained = Yielded(2, once(|x: i32| x + 1)).chain(repeat(|x: i32| x * 2));
        let (initial, mut cont) = chained.into();
        assert_eq!(2, initial);
        assert_eq!(4, cont.next(3).unwrap_yielded());
        assert_eq!(8, cont.next(4).unwrap_yielded());

        let appended = Yielded(1, once(|x: i32| x + 1))
            .and_then(|value| (value * 2, repeat(move |input: i32| input + value)));
        let (initial, mut cont) = appended.into();
        assert_eq!(1, initial);
        assert_eq!(3, cont.next(2).unwrap_yielded());
    }
}
