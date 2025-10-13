//! Iterator adapters for Sans coroutines with unit input.
//!
//! This module provides iterator adapters for [`Sans<(), O>`] and [`InitSans<(), O>`],
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
//!
//! With [`InitSans`]:
//! ```rust
//! use sans::prelude::*;
//!
//! let mut iter = init_once(10, |()| 20).into_iter();
//! let values: Vec<_> = iter.by_ref().collect();
//! assert_eq!(values, vec![10, 20]);
//! assert_eq!(iter.into_return(), Some(()));
//! ```

use crate::{InitSans, Sans, Step};

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

/// Iterator adapter for [`InitSans<(), O>`].
///
/// Calls `init()` on the wrapped coroutine to get the initial value and continuation,
/// then repeatedly calls `next(())` on the continuation.
///
/// Both `InitSansIter` and `&mut InitSansIter` implement `Iterator`.
pub struct InitSansIter<O, S>
where
    S: InitSans<(), O>,
{
    state: InitSansIterState<O, S>,
}

enum InitSansIterState<O, S>
where
    S: InitSans<(), O>,
{
    Uninit(S),
    Active { first: Option<O>, sans: S::Next },
    Complete(S::Return),
    Invalid,
}

impl<O, S> InitSansIterState<O, S>
where
    S: InitSans<(), O>,
{
    fn take(&mut self) -> Self {
        std::mem::replace(self, InitSansIterState::Invalid)
    }
}

impl<O, S> InitSansIter<O, S>
where
    S: InitSans<(), O>,
{
    /// Create a new iterator from an InitSans coroutine.
    pub fn new(init_sans: S) -> Self {
        Self {
            state: InitSansIterState::Uninit(init_sans),
        }
    }

    /// Check if the iterator has completed.
    pub fn is_complete(&self) -> bool {
        matches!(self.state, InitSansIterState::Complete(_))
    }

    /// Consume the iterator and return the final value if complete.
    ///
    /// Returns `None` if the iterator hasn't completed yet.
    pub fn into_return(self) -> Option<S::Return> {
        match self.state {
            InitSansIterState::Complete(ret) => Some(ret),
            _ => None,
        }
    }

    /// Get a reference to the return value if complete.
    pub fn return_value(&self) -> Option<&S::Return> {
        match &self.state {
            InitSansIterState::Complete(ret) => Some(ret),
            _ => None,
        }
    }
}

impl<O, S> Iterator for InitSansIter<O, S>
where
    S: InitSans<(), O>,
{
    type Item = O;

    fn next(&mut self) -> Option<Self::Item> {
        let state = self.state.take();
        match state {
            InitSansIterState::Uninit(init_sans) => match init_sans.init() {
                Step::Yielded((first, sans)) => {
                    self.state = InitSansIterState::Active {
                        first: Some(first),
                        sans,
                    };
                    self.next()
                }
                Step::Complete(ret) => {
                    self.state = InitSansIterState::Complete(ret);
                    None
                }
            },
            InitSansIterState::Active { first, mut sans } => {
                if let Some(first_value) = first {
                    self.state = InitSansIterState::Active { first: None, sans };
                    return Some(first_value);
                }

                match sans.next(()) {
                    Step::Yielded(output) => {
                        self.state = InitSansIterState::Active { first: None, sans };
                        Some(output)
                    }
                    Step::Complete(ret) => {
                        self.state = InitSansIterState::Complete(ret);
                        None
                    }
                }
            }
            InitSansIterState::Complete(ret) => {
                self.state = InitSansIterState::Complete(ret);
                None
            }
            InitSansIterState::Invalid => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build::{init_once, init_repeat, once, repeat};

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
    fn test_init_sans_iter_once() {
        let mut iter = init_once(10, |()| 20).into_iter();
        assert_eq!(iter.next(), Some(10));
        assert_eq!(iter.next(), Some(20));
        assert_eq!(iter.next(), None);
        assert!(iter.is_complete());
        assert_eq!(iter.into_return(), Some(()));
    }

    #[test]
    fn test_init_sans_iter_repeat_with_mut_ref() {
        let mut iter = init_repeat(100, |()| 42).into_iter();
        assert_eq!(iter.next(), Some(100));
        let values: Vec<_> = (&mut iter).take(5).collect();
        assert_eq!(values, vec![42, 42, 42, 42, 42]);
    }

    #[test]
    fn test_for_loop_with_mut_ref() {
        let mut iter = init_once(1, |()| 2).into_iter();
        let mut values = Vec::new();
        for value in &mut iter {
            values.push(value);
        }
        assert_eq!(values, vec![1, 2]);
        assert_eq!(iter.into_return(), Some(()));
    }

    #[test]
    fn test_return_value_reference() {
        let mut iter = once(|()| 42).into_iter();
        assert_eq!(iter.return_value(), None);
        let _ = (&mut iter).collect::<Vec<_>>();
        assert_eq!(iter.return_value(), Some(&()));
    }
}
