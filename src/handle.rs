//! Running coroutines to completion
//!
//! Functions and types for driving coroutines to completion.
//!
//! This module provides both synchronous and asynchronous execution functions,
//! plus utilities for working with coroutines that need initial input.

use crate::sans::Sans;
use crate::step::Step;
use crate::yielded::Yielded;
use std::future::Future;

/// Drives a coroutine to completion with synchronous responses.
///
/// This is a convenience function that creates a [`Handler`] and immediately
/// uses it to drive the given coroutine to completion.
///
/// # Parameters
///
/// - `coro`: The coroutine to drive to completion
/// - `input`: The initial input value for the coroutine
/// - `responder`: A function that responds to each output with the next input
///
/// # Returns
///
/// The final return value of the coroutine
///
/// # Examples
///
/// ```
/// use sans::prelude::*;
/// use sans::handle::handle;
///
/// let coro = once(|x: i32| x * 2);
/// let result = handle(coro, 5, |output| output + 1);
/// assert_eq!(result, 11);
/// ```
pub fn handle<C, I, F>(coro: C, input: I, responder: F) -> C::Return
where
    C: Sans<I>,
    F: FnMut(C::Output) -> I,
{
    Handler::new(responder).handle(coro, input)
}

/// Drives a coroutine to completion with asynchronous responses.
///
/// This is a convenience function that creates a [`HandlerAsync`] and immediately
/// uses it to drive the given coroutine to completion asynchronously.
///
/// # Parameters
///
/// - `coro`: The coroutine to drive to completion
/// - `input`: The initial input value for the coroutine
/// - `responder`: An async function that responds to each output with the next input
///
/// # Returns
///
/// A future that resolves to the final return value of the coroutine
///
/// # Examples
///
/// ```
/// use sans::prelude::*;
/// use sans::handle::handle_async;
/// use std::future::ready;
///
/// # async fn example() {
/// let coro = once(|x: i32| x * 2);
/// let result = handle_async(coro, 5, |output| ready(output + 1)).await;
/// assert_eq!(result, 11);
/// # }
/// ```
pub async fn handle_async<C, I, F, Fut>(coro: C, input: I, responder: F) -> C::Return
where
    C: Sans<I>,
    F: FnMut(C::Output) -> Fut,
    Fut: Future<Output = I>,
{
    HandlerAsync::new(responder).handle(coro, input).await
}

/// Synchronous handler for driving coroutines to completion.
///
/// A [`Handler`] wraps a function that responds to coroutine outputs synchronously.
/// It provides methods for handling different initialization types: plain coroutines
/// and yielded initializers.
///
/// # Type Parameters
///
/// - `F`: A function type that responds to outputs. Can be:
///   - `FnMut(O) -> I` for infallible handling
///   - `FnMut(O) -> Result<I, E>` for fallible handling
///
/// # Examples
///
/// ```
/// use sans::prelude::*;
/// use sans::handle::Handler;
///
/// // Create a handler that responds with the same value
/// let handler = Handler::new(|x: i32| x + 1);
///
/// // Drive a simple coroutine
/// let coro = once(|x: i32| x * 2);
/// let result = handler.handle(coro, 5);
/// assert_eq!(result, 11);
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Handler<F> {
    func: F,
}

impl<F> Handler<F> {
    /// Creates a new synchronous handler.
    ///
    /// # Examples
    ///
    /// ```
    /// use sans::handle::Handler;
    ///
    /// let handler = Handler::new(|x: i32| x + 1);
    /// ```
    pub fn new(func: F) -> Self {
        Handler { func }
    }
}

impl<F> Handler<F> {
    /// Drives a coroutine to completion with synchronous responses.
    ///
    /// # Examples
    ///
    /// ```
    /// use sans::prelude::*;
    /// use sans::handle::Handler;
    ///
    /// let handler = Handler::new(|x: i32| x + 1);
    /// let coro = once(|x: i32| x * 2);
    /// let result = handler.handle(coro, 5);
    /// assert_eq!(result, 11);
    /// ```
    pub fn handle<C, I>(mut self, mut coro: C, mut input: I) -> C::Return
    where
        F: FnMut(C::Output) -> I,
        C: Sans<I>,
    {
        loop {
            match coro.next(input) {
                Step::Yielded(output) => {
                    input = (self.func)(output);
                }
                Step::Complete(done) => return done,
            }
        }
    }

    /// Drives a yielded initialization to completion.
    ///
    /// Handles coroutines that have an initial output value.
    ///
    /// # Examples
    ///
    /// ```
    /// use sans::prelude::*;
    /// use sans::handle::Handler;
    ///
    /// let handler = Handler::new(|x: i32| x);
    /// let yielded = Yielded(10, once(|x: i32| x * 2));
    /// let result = handler.handle_yielded(yielded);
    /// assert_eq!(result, 20);
    /// ```
    pub fn handle_yielded<S, I>(mut self, yielded: Yielded<S::Output, S>) -> S::Return
    where
        F: FnMut(S::Output) -> I,
        S: Sans<I>,
    {
        let (initial_output, coro) = yielded.split();
        let initial_input = (self.func)(initial_output);
        self.handle(coro, initial_input)
    }
}

impl<F> Handler<F> {
    /// Drives a coroutine to completion with fallible responses.
    ///
    /// If the responder returns an error, the coroutine stops and returns the error.
    ///
    /// # Examples
    ///
    /// ```
    /// use sans::prelude::*;
    /// use sans::handle::Handler;
    ///
    /// let handler = Handler::new(|x: i32| {
    ///     if x > 100 { Err("too large") } else { Ok(x + 1) }
    /// });
    ///
    /// let coro = once(|x: i32| x * 2);
    /// let result: Result<i32, _> = handler.handle_result(coro, 5);
    /// assert_eq!(result, Ok(11));
    /// ```
    pub fn handle_result<C, I, E>(mut self, mut coro: C, mut input: I) -> Result<C::Return, E>
    where
        F: FnMut(C::Output) -> Result<I, E>,
        C: Sans<I>,
    {
        loop {
            match coro.next(input) {
                Step::Yielded(output) => {
                    input = (self.func)(output)?;
                }
                Step::Complete(done) => return Ok(done),
            }
        }
    }

    /// Drives a yielded initialization to completion with fallible responses.
    ///
    /// # Examples
    ///
    /// ```
    /// use sans::prelude::*;
    /// use sans::handle::Handler;
    ///
    /// let handler = Handler::new(|x: i32| {
    ///     if x > 100 { Err("too large") } else { Ok(x) }
    /// });
    ///
    /// let yielded = Yielded(10, once(|x: i32| x * 2));
    /// let result: Result<i32, _> = handler.handle_yielded_result(yielded);
    /// assert_eq!(result, Ok(20));
    /// ```
    pub fn handle_yielded_result<S, I, E>(
        mut self,
        yielded: Yielded<S::Output, S>,
    ) -> Result<S::Return, E>
    where
        F: FnMut(S::Output) -> Result<I, E>,
        S: Sans<I>,
    {
        let (initial_output, coro) = yielded.split();
        let initial_input = (self.func)(initial_output)?;
        self.handle_result(coro, initial_input)
    }
}

/// Asynchronous handler for driving coroutines to completion.
///
/// A [`HandlerAsync`] wraps an async function that responds to coroutine outputs.
/// It provides methods for handling different initialization types: plain coroutines
/// and yielded initializers.
///
/// # Type Parameters
///
/// - `F`: A function type that responds to outputs asynchronously. Can be:
///   - `FnMut(O) -> Fut` where `Fut: Future<Output = I>` for infallible handling
///   - `FnMut(O) -> Fut` where `Fut: Future<Output = Result<I, E>>` for fallible handling
///
/// # Examples
///
/// ```
/// use sans::prelude::*;
/// use sans::handle::HandlerAsync;
/// use std::future::ready;
///
/// # async fn example() {
/// // Create an async handler
/// let handler = HandlerAsync::new(|x: i32| ready(x + 1));
///
/// // Drive a coroutine asynchronously
/// let coro = once(|x: i32| x * 2);
/// let result = handler.handle(coro, 5).await;
/// assert_eq!(result, 6);
/// # }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct HandlerAsync<F> {
    func: F,
}

impl<F> HandlerAsync<F> {
    /// Creates a new asynchronous handler.
    ///
    /// # Examples
    ///
    /// ```
    /// use sans::handle::HandlerAsync;
    /// use std::future::ready;
    ///
    /// let handler = HandlerAsync::new(|x: i32| ready(x + 1));
    /// ```
    pub fn new(func: F) -> Self {
        HandlerAsync { func }
    }
}

impl<F> HandlerAsync<F> {
    /// Drives a coroutine to completion with asynchronous responses.
    ///
    /// # Examples
    ///
    /// ```
    /// use sans::prelude::*;
    /// use sans::handle::HandlerAsync;
    /// use std::future::ready;
    ///
    /// # async fn example() {
    /// let handler = HandlerAsync::new(|x: i32| ready(x + 1));
    /// let coro = once(|x: i32| x * 2);
    /// let result = handler.handle(coro, 5).await;
    /// assert_eq!(result, 6);
    /// # }
    /// ```
    pub async fn handle<C, I, Fut>(mut self, mut coro: C, mut input: I) -> C::Return
    where
        F: FnMut(C::Output) -> Fut,
        Fut: Future<Output = I>,
        C: Sans<I>,
    {
        loop {
            match coro.next(input) {
                Step::Yielded(output) => {
                    input = (self.func)(output).await;
                }
                Step::Complete(done) => return done,
            }
        }
    }

    /// Drives a yielded initialization to completion asynchronously.
    ///
    /// # Examples
    ///
    /// ```
    /// use sans::prelude::*;
    /// use sans::handle::HandlerAsync;
    /// use std::future::ready;
    ///
    /// # async fn example() {
    /// let handler = HandlerAsync::new(|x: i32| ready(x));
    /// let yielded = Yielded(10, once(|x: i32| x * 2));
    /// let result = handler.handle_yielded(yielded).await;
    /// assert_eq!(result, 10);
    /// # }
    /// ```
    pub async fn handle_yielded<S, I, Fut>(mut self, yielded: Yielded<S::Output, S>) -> S::Return
    where
        F: FnMut(S::Output) -> Fut,
        Fut: Future<Output = I>,
        S: Sans<I>,
    {
        let (initial_output, coro) = yielded.split();
        let initial_input = (self.func)(initial_output).await;
        self.handle(coro, initial_input).await
    }
}

impl<F> HandlerAsync<F> {
    /// Drives a coroutine to completion with fallible asynchronous responses.
    ///
    /// # Examples
    ///
    /// ```
    /// use sans::prelude::*;
    /// use sans::handle::HandlerAsync;
    /// use std::future::ready;
    ///
    /// # async fn example() {
    /// let handler = HandlerAsync::new(|x: i32| {
    ///     ready(if x > 100 { Err("too large") } else { Ok(x + 1) })
    /// });
    ///
    /// let coro = once(|x: i32| x * 2);
    /// let result: Result<i32, _> = handler.handle_result(coro, 5).await;
    /// assert_eq!(result, Ok(6));
    /// # }
    /// ```
    pub async fn handle_result<C, I, E, Fut>(
        mut self,
        mut coro: C,
        mut input: I,
    ) -> Result<C::Return, E>
    where
        F: FnMut(C::Output) -> Fut,
        Fut: Future<Output = Result<I, E>>,
        C: Sans<I>,
    {
        loop {
            match coro.next(input) {
                Step::Yielded(output) => {
                    input = (self.func)(output).await?;
                }
                Step::Complete(done) => return Ok(done),
            }
        }
    }

    /// Drives a yielded initialization to completion with fallible asynchronous responses.
    ///
    /// # Examples
    ///
    /// ```
    /// use sans::prelude::*;
    /// use sans::handle::HandlerAsync;
    /// use std::future::ready;
    ///
    /// # async fn example() {
    /// let handler = HandlerAsync::new(|x: i32| {
    ///     ready(if x > 100 { Err("too large") } else { Ok(x) })
    /// });
    ///
    /// let yielded = Yielded(10, once(|x: i32| x * 2));
    /// let result: Result<i32, _> = handler.handle_yielded_result(yielded).await;
    /// assert_eq!(result, Ok(10));
    /// # }
    /// ```
    pub async fn handle_yielded_result<S, I, E, Fut>(
        mut self,
        yielded: Yielded<S::Output, S>,
    ) -> Result<S::Return, E>
    where
        F: FnMut(S::Output) -> Fut,
        Fut: Future<Output = Result<I, E>>,
        S: Sans<I>,
    {
        let (initial_output, coro) = yielded.split();
        let initial_input = (self.func)(initial_output).await?;
        self.handle_result(coro, initial_input).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{compose::chain, func::once};
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::future::{Future, ready};
    use std::rc::Rc;
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    fn block_on<F: Future>(future: F) -> F::Output {
        struct Noop;
        impl Wake for Noop {
            fn wake(self: Arc<Self>) {}
        }

        let waker = Waker::from(Arc::new(Noop));
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(future);

        loop {
            match Future::poll(future.as_mut(), &mut context) {
                Poll::Ready(value) => return value,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    #[test]
    fn test_handle_cont_sync() {
        let coro = chain(once(|val: u32| val + 1), once(|val: u32| val * 3));
        let yields = Rc::new(RefCell::new(Vec::new()));
        let responses = Rc::new(RefCell::new(VecDeque::from(vec![5_u32, 7])));

        let done = handle(coro, 1_u32, {
            let yields = Rc::clone(&yields);
            let responses = Rc::clone(&responses);
            move |value| {
                yields.borrow_mut().push(value);
                responses
                    .borrow_mut()
                    .pop_front()
                    .expect("response must exist")
            }
        });

        assert_eq!(done, 7);
        assert_eq!(&*yields.borrow(), &[2, 15]);
    }

    #[test]
    fn test_handle_cont_async() {
        let coro = chain(once(|val: u32| val + 1), once(|val: u32| val * 3));
        let yields = Rc::new(RefCell::new(Vec::new()));
        let responses = Rc::new(RefCell::new(VecDeque::from(vec![5_u32, 7])));

        let done = block_on(handle_async(coro, 1_u32, {
            let yields = Rc::clone(&yields);
            let responses = Rc::clone(&responses);
            move |value| {
                yields.borrow_mut().push(value);
                let next = responses
                    .borrow_mut()
                    .pop_front()
                    .expect("response must exist");
                ready(next)
            }
        }));

        assert_eq!(done, 7);
        assert_eq!(&*yields.borrow(), &[2, 15]);
    }

    #[test]
    fn test_handler_basic() {
        use crate::func::once;

        let handler = Handler::new(|x: u32| x + 1);
        let coro = once(|x: u32| x * 2);
        let result = handler.handle(coro, 5);
        assert_eq!(result, 11);
    }

    #[test]
    fn test_handler_yielded() {
        use crate::func::once;
        use crate::yielded::Yielded;

        let handler = Handler::new(|x: u32| x);
        let yielded = Yielded(10, once(|x: u32| x * 2));
        let result = handler.handle_yielded(yielded);
        assert_eq!(result, 20);
    }

    #[test]
    fn test_handler_result_ok() {
        use crate::func::once;

        let handler = Handler::new(|x: u32| if x > 100 { Err("too large") } else { Ok(x + 1) });

        let coro = once(|x: u32| x * 2);
        let result: Result<u32, _> = handler.handle_result(coro, 5);
        assert_eq!(result, Ok(11));
    }

    #[test]
    fn test_handler_result_err() {
        use crate::func::repeat;

        let handler = Handler::new(|x: u32| if x > 100 { Err("too large") } else { Ok(x + 1) });

        let coro = repeat(|x: u32| x * 2);
        let result: Result<u32, _> = handler.handle_result(coro, 60);
        assert_eq!(result, Err("too large"));
    }

    #[test]
    fn test_handler_yielded_result_ok() {
        use crate::func::once;
        use crate::yielded::Yielded;

        let handler = Handler::new(|x: u32| if x > 100 { Err("too large") } else { Ok(x) });

        let yielded = Yielded(10, once(|x: u32| x * 2));
        let result: Result<u32, _> = handler.handle_yielded_result(yielded);
        assert_eq!(result, Ok(20));
    }

    #[test]
    fn test_handler_yielded_result_err() {
        use crate::func::once;
        use crate::yielded::Yielded;

        let handler = Handler::new(|x: u32| if x > 100 { Err("too large") } else { Ok(x) });

        let yielded = Yielded(150, once(|x: u32| x * 2));
        let result: Result<u32, _> = handler.handle_yielded_result(yielded);
        assert_eq!(result, Err("too large"));
    }

    #[test]
    fn test_handler_async_basic() {
        use crate::func::once;

        let handler = HandlerAsync::new(|x: u32| ready(x + 1));
        let coro = once(|x: u32| x * 2);
        let result = block_on(handler.handle(coro, 5));
        assert_eq!(result, 11);
    }

    #[test]
    fn test_handler_async_yielded() {
        use crate::func::once;
        use crate::yielded::Yielded;

        let handler = HandlerAsync::new(|x: u32| ready(x));
        let yielded = Yielded(10, once(|x: u32| x * 2));
        let result = block_on(handler.handle_yielded(yielded));
        assert_eq!(result, 20);
    }

    #[test]
    fn test_handler_async_result_ok() {
        use crate::func::once;

        let handler =
            HandlerAsync::new(|x: u32| ready(if x > 100 { Err("too large") } else { Ok(x + 1) }));

        let coro = once(|x: u32| x * 2);
        let result: Result<u32, _> = block_on(handler.handle_result(coro, 5));
        assert_eq!(result, Ok(11));
    }

    #[test]
    fn test_handler_async_result_err() {
        use crate::func::repeat;

        let handler =
            HandlerAsync::new(|x: u32| ready(if x > 100 { Err("too large") } else { Ok(x + 1) }));

        let coro = repeat(|x: u32| x * 2);
        let result: Result<u32, _> = block_on(handler.handle_result(coro, 60));
        assert_eq!(result, Err("too large"));
    }

    #[test]
    fn test_handler_async_yielded_result_ok() {
        use crate::func::once;
        use crate::yielded::Yielded;

        let handler =
            HandlerAsync::new(|x: u32| ready(if x > 100 { Err("too large") } else { Ok(x) }));

        let yielded = Yielded(10, once(|x: u32| x * 2));
        let result: Result<u32, _> = block_on(handler.handle_yielded_result(yielded));
        assert_eq!(result, Ok(20));
    }

    #[test]
    fn test_handler_async_yielded_result_err() {
        use crate::func::once;
        use crate::yielded::Yielded;

        let handler =
            HandlerAsync::new(|x: u32| ready(if x > 100 { Err("too large") } else { Ok(x) }));

        let yielded = Yielded(150, once(|x: u32| x * 2));
        let result: Result<u32, _> = block_on(handler.handle_yielded_result(yielded));
        assert_eq!(result, Err("too large"));
    }
}
