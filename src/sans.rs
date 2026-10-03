use std::{
    cell::RefCell,
    fmt,
    rc::Rc,
    sync::{Arc, Mutex},
};

use crate::{
    InitSans,
    build::{Once, Repeat, once, repeat},
    compose::{AndThen, Chain, MapInput, MapReturn, MapYield, WithState, and_then, chain},
    iter::SansIter,
    step::Step,
};

/// A coroutine that accepts input and yields an output or a final result.
///
/// Call [`next`](Self::next) to supply input and advance one step.
/// Stop after [`Step::Complete`]; further calls may panic.
/// To start without caller input, use [`InitSans`].
///
/// ```rust
/// use sans::prelude::*;
///
/// let mut coro = once(|x: i32| x * 2);
/// assert_eq!(coro.next(5).unwrap_yielded(), 10);
/// assert_eq!(coro.next(3).unwrap_complete(), 3);
/// ```
pub trait Sans<I, O> {
    /// The final result type.
    type Return;

    /// Process input, returning [`Step::Yielded`] to continue or [`Step::Complete`] to finish.
    fn next(&mut self, input: I) -> Step<O, Self::Return>;

    /// Create the next coroutine from this coroutine's final result.
    ///
    /// ```rust
    /// use sans::prelude::*;
    ///
    /// let mut coro = once(|x: i32| x * 2)
    ///     .and_then(|result| init(result, once(move |x| x + result)));
    /// assert_eq!(coro.next(5).unwrap_yielded(), 10);
    /// assert_eq!(coro.next(3).unwrap_yielded(), 3); // Start the next coroutine.
    /// assert_eq!(coro.next(4).unwrap_yielded(), 7);
    /// assert_eq!(coro.next(9).unwrap_complete(), 9);
    /// ```
    fn and_then<T, F>(self, f: F) -> AndThen<Self, T::Next, F>
    where
        Self: Sized,
        T: InitSans<I, O>,
        F: FnOnce(Self::Return) -> T,
    {
        and_then(self, f)
    }

    /// Box this coroutine as a `dyn Sans`.
    fn boxed(self) -> Box<dyn Sans<I, O, Return = Self::Return>>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }

    /// Pass the final result to the next coroutine as its first input.
    ///
    /// Use [`and_then`](Self::and_then) to create that coroutine from the result.
    fn chain<R>(self, r: R) -> Chain<Self, R>
    where
        Self: Sized + Sans<I, O, Return = I>,
        R: Sans<I, O>,
    {
        chain(self, r)
    }

    /// Pass the final result to a function that yields once.
    fn chain_once<F>(self, f: F) -> Chain<Self, Once<F>>
    where
        Self: Sized + Sans<I, O, Return = I>,
        F: FnOnce(Self::Return) -> O,
    {
        chain(self, once(f))
    }

    /// Continue with a function that yields for every input.
    fn chain_repeat<F>(self, f: F) -> Chain<Self, Repeat<F>>
    where
        Self: Sized + Sans<I, O, Return = I>,
        F: FnMut(Self::Return) -> O,
    {
        chain(self, repeat(f))
    }

    /// Convert inputs before passing them to the coroutine.
    ///
    /// See [`map_input`](crate::compose::map_input) for an example.
    fn map_input<I2, F>(self, f: F) -> MapInput<Self, F>
    where
        Self: Sized,
        F: FnMut(I2) -> I,
    {
        crate::compose::map_input(f, self)
    }

    /// Share state between input processing and completion.
    ///
    /// See [`with_state`](crate::compose::with_state) for an example.
    fn with_state<State, I2, R, Input, Finish>(
        self,
        state: State,
        input: Input,
        finish: Finish,
    ) -> WithState<Self, State, Input, Finish>
    where
        Self: Sized,
        Input: FnMut(&mut State, I2) -> I,
        Finish: FnOnce(State, Self::Return) -> R,
    {
        crate::compose::with_state(self, state, input, finish)
    }

    /// Convert yielded outputs.
    ///
    /// See [`map_yield`](crate::compose::map_yield) for an example.
    fn map_yield<O2, F>(self, f: F) -> MapYield<Self, F, I, O>
    where
        Self: Sized,
        F: FnMut(O) -> O2,
    {
        crate::compose::map_yield(f, self)
    }

    /// Convert the final result.
    ///
    /// See [`map_return`](crate::compose::map_return) for an example.
    fn map_return<D2, F>(self, f: F) -> MapReturn<Self, F>
    where
        Self: Sized,
        F: FnMut(Self::Return) -> D2,
    {
        crate::compose::map_return(f, self)
    }

    /// Iterate over outputs, supplying `()` as each input.
    fn into_iter(self) -> SansIter<O, Self>
    where
        Self: Sized + Sans<(), O>,
    {
        SansIter::new(self)
    }
}

/// The mutex around a shared coroutine was poisoned.
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
    fn test_and_then_accepts_return_different_from_input() {
        let first = crate::build::from_fn(|input: i32| -> Step<i32, String> {
            Step::Complete(format!("return={input}"))
        });
        let mut coro = first.and_then(|returned: String| {
            assert_eq!(returned, "return=7");
            crate::build::init(
                42,
                crate::build::from_fn(move |input: i32| -> Step<i32, String> {
                    Step::Complete(format!("{returned}, next={input}"))
                }),
            )
        });

        assert_eq!(coro.next(7).unwrap_yielded(), 42);
        assert_eq!(coro.next(9).unwrap_complete(), "return=7, next=9");
    }

    #[test]
    fn test_and_then_second_initializer_completes_immediately() {
        let first = crate::build::from_fn(|input: i32| -> Step<i32, String> {
            Step::Complete(format!("return={input}"))
        });
        let mut coro = first.and_then(|returned: String| {
            type Continuation = Box<dyn Sans<i32, i32, Return = String>>;
            let complete: Step<(i32, Continuation), String> =
                Step::Complete(format!("finished: {returned}"));
            complete
        });

        assert_eq!(coro.next(7).unwrap_complete(), "finished: return=7");
    }

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
