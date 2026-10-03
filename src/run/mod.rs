//! Run a coroutine to completion with a responder.
//!
//! The responder turns each yielded output into the next input. A coroutine that
//! completes without yielding does not call it.
//!
//! | Start with | Sync responder | Async responder |
//! | --- | --- | --- |
//! | Initializer | [`handle`] | [`handle_async`] |
//! | Continuation and first input | [`handle_with_input`] | [`handle_with_input_async`] |
//! | Initializer, responses can fail | [`try_handle`] | [`try_handle_async`] |
//! | Continuation and first input, responses can fail | [`try_handle_with_input`] | [`try_handle_with_input_async`] |
//!
//! The `try_` runners return the first core or responder error. Both use the same
//! error type; convert errors before running if needed. A responder error drops
//! the owned coroutine without resuming it.
//!
//! Async runners await responses; coroutine steps remain synchronous. Dropping
//! the runner future drops its owned coroutine and active response future.

use crate::init::InitSans;
use crate::sans::Sans;
use crate::step::Step;
use std::future::Future;

/// Run an initializer with synchronous responses.
///
/// See the [runner guide](crate::run) for the shared behavior.
///
/// ```
/// use sans::prelude::*;
///
/// let pipeline = init_once(10, |x: i32| x * 2);
/// assert_eq!(handle(pipeline, |output| output + 5), 35);
/// ```
pub fn handle<S, I, O, R>(coro: S, mut responder: R) -> <S::Next as Sans<I, O>>::Return
where
    S: InitSans<I, O>,
    R: FnMut(O) -> I,
{
    match coro.init() {
        Step::Yielded((output, next_coro)) => {
            let input = responder(output);
            handle_with_input(next_coro, input, responder)
        }
        Step::Complete(done) => done,
    }
}

/// Run a continuation with synchronous responses, starting with `input`.
///
/// ```
/// use sans::prelude::*;
///
/// assert_eq!(handle_with_input(once(|x: i32| x * 2), 10, |output| output + 5), 25);
/// ```
pub fn handle_with_input<C, I, O, R>(mut coro: C, mut input: I, mut responder: R) -> C::Return
where
    C: Sans<I, O>,
    R: FnMut(O) -> I,
{
    loop {
        match coro.next(input) {
            Step::Yielded(output) => input = responder(output),
            Step::Complete(done) => return done,
        }
    }
}

/// Run an initializer, awaiting each response.
///
/// Async version of [`handle`]. See the [runner guide](crate::run) for cancellation behavior.
pub async fn handle_async<S, I, O, R, Fut>(
    coro: S,
    mut responder: R,
) -> <S::Next as Sans<I, O>>::Return
where
    S: InitSans<I, O>,
    R: FnMut(O) -> Fut,
    Fut: Future<Output = I>,
{
    match coro.init() {
        Step::Yielded((output, next_coro)) => {
            let input = responder(output).await;
            handle_with_input_async(next_coro, input, responder).await
        }
        Step::Complete(done) => done,
    }
}

/// Run a continuation from `input`, awaiting each response.
///
/// Async version of [`handle_with_input`]. See the [runner guide](crate::run).
pub async fn handle_with_input_async<C, I, O, R, Fut>(
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
            Step::Yielded(output) => input = responder(output).await,
            Step::Complete(done) => return done,
        }
    }
}

/// Run an initializer, stopping on a core or responder error.
///
/// Both errors use the same type. A responder error drops the coroutine without
/// resuming it. See the [runner guide](crate::run).
///
/// ```
/// use sans::{InitSans, build::init_once, run::try_handle};
///
/// let pipeline = init_once(10, |x: i32| x * 2).map_return(Ok::<_, &str>);
/// let result = try_handle(pipeline, |output| Ok(output + 5));
/// assert_eq!(result, Ok(35));
/// ```
pub fn try_handle<S, I, O, T, E, R>(coro: S, mut responder: R) -> Result<T, E>
where
    S: InitSans<I, O>,
    S::Next: Sans<I, O, Return = Result<T, E>>,
    R: FnMut(O) -> Result<I, E>,
{
    match coro.init() {
        Step::Yielded((output, next_coro)) => {
            let input = responder(output)?;
            try_handle_with_input(next_coro, input, responder)
        }
        Step::Complete(done) => done,
    }
}

/// Run a continuation from `input`, stopping on a core or responder error.
///
/// Uses the same error behavior as [`try_handle`].
pub fn try_handle_with_input<C, I, O, T, E, R>(
    mut coro: C,
    mut input: I,
    mut responder: R,
) -> Result<T, E>
where
    C: Sans<I, O, Return = Result<T, E>>,
    R: FnMut(O) -> Result<I, E>,
{
    loop {
        match coro.next(input) {
            Step::Yielded(output) => input = responder(output)?,
            Step::Complete(done) => return done,
        }
    }
}

/// Run an initializer with fallible async responses.
///
/// Async version of [`try_handle`]. See the [runner guide](crate::run) for cancellation behavior.
pub async fn try_handle_async<S, I, O, T, E, R, Fut>(coro: S, mut responder: R) -> Result<T, E>
where
    S: InitSans<I, O>,
    S::Next: Sans<I, O, Return = Result<T, E>>,
    R: FnMut(O) -> Fut,
    Fut: Future<Output = Result<I, E>>,
{
    match coro.init() {
        Step::Yielded((output, next_coro)) => {
            let input = responder(output).await?;
            try_handle_with_input_async(next_coro, input, responder).await
        }
        Step::Complete(done) => done,
    }
}

/// Run a continuation from `input` with fallible async responses.
///
/// Async version of [`try_handle_with_input`]. See the [runner guide](crate::run).
pub async fn try_handle_with_input_async<C, I, O, T, E, R, Fut>(
    mut coro: C,
    mut input: I,
    mut responder: R,
) -> Result<T, E>
where
    C: Sans<I, O, Return = Result<T, E>>,
    R: FnMut(O) -> Fut,
    Fut: Future<Output = Result<I, E>>,
{
    loop {
        match coro.next(input) {
            Step::Yielded(output) => input = responder(output).await?,
            Step::Complete(done) => return done,
        }
    }
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
    use std::task::{Context, Poll, Waker};

    fn block_on<F: Future>(future: F) -> F::Output {
        let mut context = Context::from_waker(Waker::noop());
        let mut future = Box::pin(future);

        loop {
            match Future::poll(future.as_mut(), &mut context) {
                Poll::Ready(value) => return value,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    struct CompleteOnInit;

    impl InitSans<u32, u32> for CompleteOnInit {
        type Next = crate::build::Once<fn(u32) -> u32>;

        fn init(self) -> Step<(u32, Self::Next), u32> {
            Step::Complete(42)
        }
    }

    #[test]
    fn test_handle_immediate_completion_skips_responder() {
        assert_eq!(
            handle(CompleteOnInit, |_| panic!("responder must not run")),
            42
        );
    }

    #[test]
    fn test_handle_async_immediate_completion_skips_responder() {
        assert_eq!(
            block_on(handle_async(
                CompleteOnInit,
                |_| -> std::future::Ready<u32> { panic!("responder must not run") }
            )),
            42
        );
    }

    #[test]
    fn test_handle_with_input() {
        let coro = chain(once(|val: u32| val + 1), once(|val: u32| val * 3));
        let yields = Rc::new(RefCell::new(Vec::new()));
        let responses = Rc::new(RefCell::new(VecDeque::from(vec![5_u32, 7])));

        let done = handle_with_input(coro, 1_u32, {
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
    fn test_handle_with_input_async() {
        let coro = chain(once(|val: u32| val + 1), once(|val: u32| val * 3));
        let yields = Rc::new(RefCell::new(Vec::new()));
        let responses = Rc::new(RefCell::new(VecDeque::from(vec![5_u32, 7])));

        let done = block_on(handle_with_input_async(coro, 1_u32, {
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
    fn test_handle() {
        let initializer = init_once(10_u32, |input: u32| input + 2);
        let finisher = once(|value: u32| value * 3);
        let coro = initializer.chain(finisher);

        let yields = Rc::new(RefCell::new(Vec::new()));
        let responses = Rc::new(RefCell::new(VecDeque::from(vec![5_u32, 6, 7])));

        let done = handle(coro, {
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
    fn test_handle_async() {
        let initializer = init_once(10_u32, |input: u32| input + 2);
        let finisher = once(|value: u32| value * 3);
        let coro = initializer.chain(finisher);

        let yields = Rc::new(RefCell::new(Vec::new()));
        let responses = Rc::new(RefCell::new(VecDeque::from(vec![5_u32, 6, 7])));

        let done = block_on(handle_async(coro, {
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
    fn test_handle_pipeline() {
        let initializer = init_once(2_u32, |input: u32| input + 1);
        let finisher = once(|value: u32| value * 2);
        let done = handle(initializer.chain(finisher), |value: u32| value + 1);
        assert_eq!(done, 11);
    }

    #[test]
    fn test_handle_with_initial_output_tuple() {
        let coro = once(|n: u32| n + 2);
        let done = handle((1_u32, coro), |value: u32| value + 1);
        assert_eq!(done, 5);
    }

    #[test]
    fn test_handle_async_with_initial_output_tuple() {
        let coro = once(|n: u32| n + 2);
        let done = block_on(handle_async((1_u32, coro), |value: u32| ready(value + 1)));
        assert_eq!(done, 5);
    }

    struct TrackedCore {
        steps: Rc<std::cell::Cell<usize>>,
        drops: Rc<std::cell::Cell<usize>>,
        fail: bool,
    }

    impl Sans<usize, usize> for TrackedCore {
        type Return = Result<usize, &'static str>;

        fn next(&mut self, input: usize) -> Step<usize, Self::Return> {
            self.steps.set(self.steps.get() + 1);
            if input < 3 {
                Step::Yielded(input)
            } else {
                Step::Complete(if self.fail { Err("core") } else { Ok(input) })
            }
        }
    }

    impl Drop for TrackedCore {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    fn tracked_core(
        fail: bool,
    ) -> (
        TrackedCore,
        Rc<std::cell::Cell<usize>>,
        Rc<std::cell::Cell<usize>>,
    ) {
        let steps = Rc::new(std::cell::Cell::new(0));
        let drops = Rc::new(std::cell::Cell::new(0));
        let core = TrackedCore {
            steps: Rc::clone(&steps),
            drops: Rc::clone(&drops),
            fail,
        };
        (core, steps, drops)
    }

    fn respond(output: usize, fail_at: Option<usize>) -> Result<usize, &'static str> {
        if fail_at == Some(output) {
            Err("responder")
        } else {
            Ok(output + 1)
        }
    }

    #[test]
    fn fallible_initializers_preserve_success_and_errors_and_drop_the_core() {
        // Initial failure must never step the core; a later failure must stop
        // after the last yielded output. Core failures retain their own value.
        for (fail_at, core_fails, expected, expected_steps) in [
            (Some(0), false, Err("responder"), 0),
            (Some(2), false, Err("responder"), 2),
            (None, true, Err("core"), 3),
            (None, false, Ok(3), 3),
        ] {
            let (core, steps, drops) = tracked_core(core_fails);
            assert_eq!(
                try_handle((0, core), |output| respond(output, fail_at)),
                expected
            );
            assert_eq!(steps.get(), expected_steps);
            assert_eq!(drops.get(), 1);

            let (core, steps, drops) = tracked_core(core_fails);
            assert_eq!(
                block_on(try_handle_async((0, core), |output| ready(respond(
                    output, fail_at
                )))),
                expected
            );
            assert_eq!(steps.get(), expected_steps);
            assert_eq!(drops.get(), 1);
        }
    }

    #[test]
    fn fallible_continuations_preserve_success_and_errors_and_drop_the_core() {
        for (fail_at, core_fails, expected, expected_steps) in [
            (Some(1), false, Err("responder"), 1),
            (Some(2), false, Err("responder"), 2),
            (None, true, Err("core"), 3),
            (None, false, Ok(3), 3),
        ] {
            let (core, steps, drops) = tracked_core(core_fails);
            assert_eq!(
                try_handle_with_input(core, 1, |output| respond(output, fail_at)),
                expected
            );
            assert_eq!(steps.get(), expected_steps);
            assert_eq!(drops.get(), 1);

            let (core, steps, drops) = tracked_core(core_fails);
            assert_eq!(
                block_on(try_handle_with_input_async(core, 1, |output| ready(
                    respond(output, fail_at)
                ))),
                expected
            );
            assert_eq!(steps.get(), expected_steps);
            assert_eq!(drops.get(), 1);
        }
    }

    #[test]
    fn fallible_immediate_completion_skips_responders_in_both_phases() {
        for expected in [Ok(42), Err("core")] {
            let initialized: Step<(usize, TrackedCore), _> = Step::Complete(expected);
            assert_eq!(
                try_handle(initialized, |_| panic!("no response needed")),
                expected
            );
            let initialized: Step<(usize, TrackedCore), _> = Step::Complete(expected);
            assert_eq!(
                block_on(try_handle_async(
                    initialized,
                    |_| -> std::future::Ready<Result<usize, &'static str>> {
                        panic!("no response needed")
                    }
                )),
                expected
            );

            let (core, steps, drops) = tracked_core(expected.is_err());
            assert_eq!(
                try_handle_with_input(core, 42, |_| panic!("no response needed")),
                expected
            );
            assert_eq!(steps.get(), 1);
            assert_eq!(drops.get(), 1);
            let (core, steps, drops) = tracked_core(expected.is_err());
            assert_eq!(
                block_on(try_handle_with_input_async(
                    core,
                    42,
                    |_| -> std::future::Ready<Result<usize, &'static str>> {
                        panic!("no response needed")
                    }
                )),
                expected
            );
            assert_eq!(steps.get(), 1);
            assert_eq!(drops.get(), 1);
        }
    }

    struct PendingResponse(Rc<std::cell::Cell<usize>>);

    impl Future for PendingResponse {
        type Output = Result<usize, &'static str>;

        fn poll(self: std::pin::Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for PendingResponse {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }

    #[test]
    fn cancelling_fallible_async_runners_drops_core_and_pending_response() {
        let mut context = Context::from_waker(Waker::noop());
        let (core, steps, drops) = tracked_core(false);
        let response_drops = Rc::new(std::cell::Cell::new(0));
        let mut runner = Box::pin(try_handle_async((0, core), |_| {
            PendingResponse(Rc::clone(&response_drops))
        }));
        assert!(runner.as_mut().poll(&mut context).is_pending());
        assert_eq!(steps.get(), 0);
        assert_eq!(drops.get(), 0);
        drop(runner);
        assert_eq!(steps.get(), 0);
        assert_eq!(drops.get(), 1);
        assert_eq!(response_drops.get(), 1);

        let (core, steps, drops) = tracked_core(false);
        let response_drops = Rc::new(std::cell::Cell::new(0));
        let mut runner = Box::pin(try_handle_with_input_async(core, 1, |_| {
            PendingResponse(Rc::clone(&response_drops))
        }));
        assert!(runner.as_mut().poll(&mut context).is_pending());
        assert_eq!(steps.get(), 1);
        assert_eq!(drops.get(), 0);
        drop(runner);
        assert_eq!(steps.get(), 1);
        assert_eq!(drops.get(), 1);
        assert_eq!(response_drops.get(), 1);
    }

    #[test]
    fn fallible_runners_accept_borrowed_cores_and_responders() {
        let mut core_steps = 0;
        let mut responses = 0;
        let core = crate::build::from_fn(|input: usize| {
            core_steps += 1;
            if input < 2 {
                Step::Yielded(input)
            } else {
                Step::Complete(Ok::<_, &'static str>(input))
            }
        });
        let result = block_on(try_handle_async((0, core), |output| {
            responses += 1;
            ready(Ok(output + 1))
        }));
        assert_eq!(result, Ok(2));
        assert_eq!(core_steps, 2);
        assert_eq!(responses, 2);
    }
}
