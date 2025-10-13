//! Running coroutines to completion
//!
//! Functions for driving coroutines to completion.
//!
//! This module provides both synchronous and asynchronous execution functions,
//! plus utilities for working with coroutines that need initial input.

use crate::init::InitSans;
use crate::sans::Sans;
use crate::step::Step;
use std::future::Future;

/// Drive an `InitSans` coroutine to completion with synchronous responses.
///
/// Executes the initial `InitSans` coroutine and continues driving the resulting
/// coroutine until completion, calling the responder for each yield.
pub fn handle_init_sync<S, I, O, R>(coro: S, mut responder: R) -> S::Return
where
    S: InitSans<I, O>,
    R: FnMut(O) -> I,
{
    match coro.init() {
        Step::Yielded((output, mut next_coro)) => {
            let mut input = responder(output);
            loop {
                match next_coro.next(input) {
                    Step::Yielded(output) => {
                        input = responder(output);
                    }
                    Step::Complete(done) => return done,
                }
            }
        }
        Step::Complete(done) => done,
    }
}

/// Drive a coroutine to completion with synchronous responses.
///
/// Takes an existing coroutine with initial input and drives it to completion.
pub fn handle_cont_sync<C, I, O, R>(mut coro: C, mut input: I, mut responder: R) -> C::Return
where
    C: Sans<I, O>,
    R: FnMut(O) -> I,
{
    loop {
        match coro.next(input) {
            Step::Yielded(output) => {
                input = responder(output);
            }
            Step::Complete(done) => return done,
        }
    }
}

/// Async version of `handle_cont_sync`.
///
/// The responder function returns a future that produces the next input.
pub async fn handle_cont_async<C, I, O, R, Fut>(
    mut coro: C,
    mut input: I,
    mut responder: R,
) -> C::Return
where
    C: Sans<I, O>,
    R: FnMut(O) -> Fut,
    Fut: Future<Output = I>,
{
    loop {
        match coro.next(input) {
            Step::Yielded(output) => {
                input = responder(output).await;
            }
            Step::Complete(done) => return done,
        }
    }
}

/// Async version of `handle_init_sync`.
///
/// The responder function returns a future that produces the next input.
pub async fn handle_init_async<S, I, O, R, Fut>(coro: S, mut responder: R) -> S::Return
where
    S: InitSans<I, O>,
    R: FnMut(O) -> Fut,
    Fut: Future<Output = I>,
{
    match coro.init() {
        Step::Yielded((output, mut next_coro)) => {
            let mut input = responder(output).await;
            loop {
                match next_coro.next(input) {
                    Step::Yielded(output) => {
                        input = responder(output).await;
                    }
                    Step::Complete(done) => return done,
                }
            }
        }
        Step::Complete(done) => done,
    }
}

/// Main function for executing coroutine pipelines.
///
/// This is the most commonly used function - a shorthand for `handle_init_sync`.
///
/// ```rust
/// use sans::prelude::*;
///
/// let pipeline = init_once(10, |x: i32| x * 2).chain(once(|x: i32| x + 1));
/// let result = handle(pipeline, |output| output + 5);
/// ```
pub fn handle<S, I, O, R>(coro: S, responder: R) -> S::Return
where
    S: InitSans<I, O>,
    R: FnMut(O) -> I,
{
    handle_init_sync(coro, responder)
}

/// Async version of `handle`.
///
/// Works with responder functions that return futures.
pub async fn handle_async<S, I, O, R, Fut>(coro: S, responder: R) -> S::Return
where
    S: InitSans<I, O>,
    R: FnMut(O) -> Fut,
    Fut: Future<Output = I>,
{
    handle_init_async(coro, responder).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        build::{init_once, once},
        compose::chain,
    };
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

        let done = handle_cont_sync(coro, 1_u32, {
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

        let done = block_on(handle_cont_async(coro, 1_u32, {
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
    fn test_handle_init_sync() {
        let initializer = init_once(10_u32, |input: u32| input + 2);
        let finisher = once(|value: u32| value * 3);
        let coro = initializer.chain(finisher);

        let yields = Rc::new(RefCell::new(Vec::new()));
        let responses = Rc::new(RefCell::new(VecDeque::from(vec![5_u32, 6, 7])));

        let done = handle_init_sync(coro, {
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
        assert_eq!(&*yields.borrow(), &[10, 7, 18]);
    }

    #[test]
    fn test_handle_init_async() {
        let initializer = init_once(10_u32, |input: u32| input + 2);
        let finisher = once(|value: u32| value * 3);
        let coro = initializer.chain(finisher);

        let yields = Rc::new(RefCell::new(Vec::new()));
        let responses = Rc::new(RefCell::new(VecDeque::from(vec![5_u32, 6, 7])));

        let done = block_on(handle_init_async(coro, {
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
        assert_eq!(&*yields.borrow(), &[10, 7, 18]);
    }

    #[test]
    fn test_handle_shortcut_for_init() {
        let initializer = init_once(2_u32, |input: u32| input + 1);
        let finisher = once(|value: u32| value * 2);
        let done = handle(initializer.chain(finisher), |value: u32| value + 1);
        assert_eq!(done, 11);
    }

    #[test]
    fn test_handle_shortcut_for_cont_with_input_helper() {
        let coro = once(|n: u32| n + 2);
        let done = handle((1_u32, coro), |value: u32| value + 1);
        assert_eq!(done, 5);
    }

    #[test]
    fn test_handle_async_shortcut_for_cont_with_input_helper() {
        let coro = once(|n: u32| n + 2);
        let done = block_on(handle_async((1_u32, coro), |value: u32| ready(value + 1)));
        assert_eq!(done, 5);
    }
}
