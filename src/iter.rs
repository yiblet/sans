//! Iterator adapters for Sans coroutines with unit input.
//!
//! This module provides iterator adapters for [`Sans<(), O>`],
//! allowing you to iterate over yielded values and access the final return value.
//!
//! # Examples
//!
//! Basic usage with [`Sans`]:
//! ```rust
//! use sans::prelude::*;
//!
//! let mut iter = repeat(|()| 42).into_iter();
//! // Take 3 values
//! let values: Vec<_> = (&mut iter).take(3).collect();
//! assert_eq!(values, vec![42, 42, 42]);
//! // Iterator never completes for repeat, so no return value
//! ```

use crate::{
    Sans, Step,
    init::{ShortCircuit, Yielded},
};

/// Iterator adapter for [`Sans<(), O>`].
///
/// Repeatedly calls `next(())` on the wrapped coroutine and yields values
/// until the coroutine completes.
///
/// Both `SansIter` and `&mut SansIter` implement `Iterator`, allowing you to
/// iterate without consuming the wrapper, so you can later access the return value.
pub struct SansIter<O, S>
where
    S: Sans<(), O>,
{
    state: SansIterState<O, S>,
}

enum SansIterState<O, S>
where
    S: Sans<(), O>,
{
    Yielded(O, S),
    Active(S),
    Complete(S::Return),
    Invalid,
}

impl<O, S> SansIterState<O, S>
where
    S: Sans<(), O>,
{
    fn take(&mut self) -> Self {
        std::mem::replace(self, SansIterState::Invalid)
    }
}

impl<O, S> SansIter<O, S>
where
    S: Sans<(), O>,
{
    /// Create a new iterator from a Sans coroutine.
    pub fn new(sans: S) -> Self {
        Self {
            state: SansIterState::Active(sans),
        }
    }

    pub fn from_yielded(yielded: Yielded<O, S>) -> Self {
        Self {
            state: SansIterState::Yielded(yielded.0, yielded.1),
        }
    }

    pub fn from_return(ret: S::Return) -> Self {
        Self {
            state: SansIterState::Complete(ret),
        }
    }

    pub fn from_short_circuit(short_circuit: ShortCircuit<S, S::Return>) -> Self {
        match short_circuit {
            ShortCircuit::Pending(s) => Self::new(s),
            ShortCircuit::Complete(ret) => Self::from_return(ret),
        }
    }

    pub fn from_short_circuit_yielded(
        short_circuit: ShortCircuit<Yielded<O, S>, S::Return>,
    ) -> Self {
        match short_circuit {
            ShortCircuit::Pending(Yielded(output, s)) => Self::from_yielded(Yielded(output, s)),
            ShortCircuit::Complete(ret) => Self::from_return(ret),
        }
    }

    /// Check if the iterator has completed.
    pub fn is_complete(&self) -> bool {
        matches!(self.state, SansIterState::Complete(_))
    }

    /// Consume the iterator and return the final value if complete.
    ///
    /// Returns `None` if the iterator hasn't completed yet.
    pub fn into_return(self) -> Option<S::Return> {
        match self.state {
            SansIterState::Complete(ret) => Some(ret),
            _ => None,
        }
    }

    /// Get a reference to the return value if complete.
    pub fn return_value(&self) -> Option<&S::Return> {
        match &self.state {
            SansIterState::Complete(ret) => Some(ret),
            _ => None,
        }
    }
}

impl<O, S> Iterator for SansIter<O, S>
where
    S: Sans<(), O>,
{
    type Item = O;

    fn next(&mut self) -> Option<Self::Item> {
        let state = self.state.take();
        match state {
            SansIterState::Yielded(output, sans) => {
                self.state = SansIterState::Active(sans);
                Some(output)
            }
            SansIterState::Active(mut sans) => match sans.next(()) {
                Step::Yielded(output) => {
                    self.state = SansIterState::Active(sans);
                    Some(output)
                }
                Step::Complete(ret) => {
                    self.state = SansIterState::Complete(ret);
                    None
                }
            },
            SansIterState::Complete(ret) => {
                self.state = SansIterState::Complete(ret);
                None
            }
            SansIterState::Invalid => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build::{once, repeat};

    #[test]
    fn test_sans_iter_once() {
        let mut iter = once(|()| 42).into_iter();
        assert_eq!(iter.next(), Some(42));
        assert_eq!(iter.next(), None);
        assert!(iter.is_complete());
        assert_eq!(iter.into_return(), Some(()));
    }

    #[test]
    fn test_sans_iter_repeat_with_mut_ref() {
        let mut iter = repeat(|()| 42).into_iter();
        let values: Vec<_> = (&mut iter).take(5).collect();
        assert_eq!(values, vec![42, 42, 42, 42, 42]);
        // Repeat never completes
        assert!(!iter.is_complete());
    }

    #[test]
    fn test_return_value_reference() {
        let mut iter = once(|()| 42).into_iter();
        assert_eq!(iter.return_value(), None);
        let _ = (&mut iter).collect::<Vec<_>>();
        assert_eq!(iter.return_value(), Some(&()));
    }
}
