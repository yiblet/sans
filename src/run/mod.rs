//! Running coroutines to completion
//!
//! Functions for driving coroutines to completion.
//!
//! This module provides both synchronous and asynchronous execution functions,
//! plus utilities for working with coroutines that need initial input.

use crate::sans::Sans;
use crate::step::Step;
use std::future::Future;

/// Drive a coroutine to completion with synchronous responses.
///
/// Takes an existing coroutine with initial input and drives it to completion.
pub fn handle<C, I, O, R>(mut coro: C, mut input: I, mut responder: R) -> C::Return
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

/// Async version of [handle].
///
/// The responder function returns a future that produces the next input.
pub async fn handle_async<C, I, O, R, Fut>(mut coro: C, mut input: I, mut responder: R) -> C::Return
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{build::once, compose::chain};
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
}
