//! Separate output checks from input delivery.
//!
//! Wrap a continuation with [`poll`], or an initializer with [`init_poll`]. Send
//! [`Poll::Poll`] to retrieve buffered output and [`Poll::Input`] to supply input.
//! These operations are synchronous; this is not `Future` polling.
//!
//! [`PollOutput::NeedsInput`] asks for input. [`PollOutput::NeedsPoll`] returns an
//! unconsumed input: poll for the buffered output or completion before retrying it.
//! [`Step::Complete`] ends the run; later calls return [`PollError::AlreadyComplete`].
//!
//! Both adapters implement [`Sans`]. Use [`start`](crate::build::start) with
//! [`Poll::Poll`] when a runner needs an initializer.
use crate::{InitSans, Sans, Step};

enum PollState<S, O, R> {
    Sans(S),
    SansOutput(O, S),
    Return(R),
    Completed,
}

impl<S, O, R> PollState<S, O, R> {
    fn take_output(&mut self) -> Option<O> {
        match self {
            PollState::SansOutput(_, _) => {
                let s = std::mem::replace(self, PollState::Completed);
                match s {
                    PollState::SansOutput(o, s) => {
                        *self = PollState::Sans(s);
                        Some(o)
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn take_return(&mut self) -> Option<R> {
        match self {
            PollState::Return(_) => {
                let r = std::mem::replace(self, PollState::Completed);
                match r {
                    PollState::Return(r) => Some(r),
                    _ => None,
                }
            }
            _ => None,
        }
    }
}

/// A coroutine with separate output checks and input delivery.
///
/// Construct with [`poll`] or [`init_poll`]. See the [polling guide](crate::poll)
/// for the protocol.
pub struct Pollable<S, O, R> {
    state: PollState<S, O, R>,
}

/// An output check or input delivery for a polling coroutine.
pub enum Poll<I> {
    /// Check if there's output available without providing input.
    Poll,
    /// Provide an input value to the coroutine.
    Input(I),
}

/// Output from a [`Pollable`] coroutine.
#[derive(Debug)]
pub enum PollOutput<I, O> {
    /// Coroutine produced an output value.
    Output(O),
    /// Coroutine needs input before it can produce output.
    NeedsInput,
    /// Returns unconsumed input; poll for buffered output or completion first.
    NeedsPoll(I),
}

/// Errors from [`Pollable`] operations.
#[derive(Debug)]
pub enum PollError {
    /// Attempted to poll or provide input after coroutine completed.
    AlreadyComplete,
}

impl std::fmt::Display for PollError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PollError::AlreadyComplete => write!(f, "already complete"),
        }
    }
}

impl std::error::Error for PollError {}

/// Wrap a continuation for polling and input delivery.
///
/// The first poll returns [`PollOutput::NeedsInput`]. See the
/// [polling guide](crate::poll) for the protocol.
///
/// ```
/// use sans::{Sans, build::repeat, poll::{poll, Poll, PollOutput}};
///
/// let mut coro = poll(repeat(|x: i32| x + 1));
/// assert!(matches!(coro.next(Poll::Poll).unwrap_yielded(), PollOutput::NeedsInput));
/// assert!(matches!(coro.next(Poll::Input(5)).unwrap_yielded(), PollOutput::Output(6)));
/// ```
pub fn poll<I, S, O, R>(coro: S) -> Pollable<S, O, R>
where
    S: Sans<I, O>,
{
    Pollable {
        state: PollState::Sans(coro),
    }
}

/// Initialize a coroutine now and buffer its first output or completion.
///
/// The first [`Poll::Poll`] retrieves the buffered value. Unlike
/// [`start`](crate::build::start), this constructor runs initialization immediately.
/// See the [polling guide](crate::poll) for the protocol.
pub fn init_poll<I, S, O, T>(init: T) -> Pollable<S, O, S::Return>
where
    S: Sans<I, O>,
    T: InitSans<I, O, Next = S>,
{
    match init.init() {
        Step::Yielded((o, s)) => Pollable {
            state: PollState::SansOutput(o, s),
        },
        Step::Complete(r) => Pollable {
            state: PollState::Return(r),
        },
    }
}

impl<I, O, S> Sans<Poll<I>, PollOutput<I, O>> for Pollable<S, O, S::Return>
where
    S: Sans<I, O>,
{
    type Return = Result<S::Return, PollError>;

    fn next(&mut self, input: Poll<I>) -> Step<PollOutput<I, O>, Self::Return> {
        match (&mut self.state, input) {
            (PollState::Sans(_), Poll::Poll) => Step::Yielded(PollOutput::NeedsInput),
            (PollState::Sans(s), Poll::Input(i)) => match s.next(i) {
                Step::Yielded(o) => Step::Yielded(PollOutput::Output(o)),
                Step::Complete(a) => {
                    self.state = PollState::Completed;
                    Step::Complete(Ok(a))
                }
            },
            (PollState::SansOutput(_, _), Poll::Poll) => {
                let o = self.state.take_output().unwrap(); // SAFETY: we know it's in the sans output state
                Step::Yielded(PollOutput::Output(o))
            }
            (PollState::SansOutput(_, _), Poll::Input(i2)) => {
                Step::Yielded(PollOutput::NeedsPoll(i2))
            }
            (PollState::Return(_), Poll::Poll) => {
                let r = self.state.take_return().unwrap(); // SAFETY: we know it's in the return state
                Step::Complete(Ok(r))
            }
            (PollState::Return(_), Poll::Input(i2)) => Step::Yielded(PollOutput::NeedsPoll(i2)),
            (PollState::Completed, _) => Step::Complete(Err(PollError::AlreadyComplete)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build::{once, repeat};

    #[test]
    fn test_poll_basic_needs_input() {
        let coro = repeat(|x: i32| x + 1);
        let mut pollable = poll(coro);

        // Initially, polling should indicate needs input
        match pollable.next(Poll::Poll) {
            Step::Yielded(PollOutput::NeedsInput) => {}
            _ => panic!("Expected NeedsInput"),
        }
    }

    #[test]
    fn test_poll_input_yields_output() {
        let coro = repeat(|x: i32| x + 1);
        let mut pollable = poll(coro);

        // Send input
        match pollable.next(Poll::Input(5)) {
            Step::Yielded(PollOutput::Output(6)) => {}
            _ => panic!("Expected Output(6)"),
        }
    }

    #[test]
    fn test_poll_sequence_poll_input_poll() {
        let coro = repeat(|x: i32| x * 2);
        let mut pollable = poll(coro);

        // Poll -> NeedsInput
        match pollable.next(Poll::Poll) {
            Step::Yielded(PollOutput::NeedsInput) => {}
            _ => panic!("Expected NeedsInput"),
        }

        // Input -> Output
        match pollable.next(Poll::Input(10)) {
            Step::Yielded(PollOutput::Output(20)) => {}
            _ => panic!("Expected Output(20)"),
        }

        // Poll again -> NeedsInput
        match pollable.next(Poll::Poll) {
            Step::Yielded(PollOutput::NeedsInput) => {}
            _ => panic!("Expected NeedsInput"),
        }
    }

    #[test]
    fn test_poll_completion() {
        let coro = once(|x: i32| x + 10);
        let mut pollable = poll(coro);

        // Input yields output first
        match pollable.next(Poll::Input(5)) {
            Step::Yielded(PollOutput::Output(15)) => {}
            other => panic!("Expected Output(15), got {:?}", other),
        }

        // Then send another input to complete (once completes on second input)
        match pollable.next(Poll::Input(99)) {
            Step::Complete(Ok(99)) => {} // once returns the final input value
            other => panic!("Expected Complete(Ok(99)), got {:?}", other),
        }
    }

    #[test]
    fn test_poll_already_complete_error() {
        let coro = once(|x: i32| x + 1);
        let mut pollable = poll(coro);

        // Send input, get output
        pollable.next(Poll::Input(5)).expect_yielded("should yield");

        // Send another input to complete
        let _ = pollable
            .next(Poll::Input(10))
            .expect_complete("should complete");

        // Try to poll after completion
        match pollable.next(Poll::Poll) {
            Step::Complete(Err(PollError::AlreadyComplete)) => {}
            _ => panic!("Expected AlreadyComplete error"),
        }

        // Try to send input after completion
        match pollable.next(Poll::Input(20)) {
            Step::Complete(Err(PollError::AlreadyComplete)) => {}
            _ => panic!("Expected AlreadyComplete error"),
        }
    }

    #[test]
    fn test_poll_needs_poll_with_init() {
        // Use init_poll with a tuple to create SansOutput state
        let init = (100, repeat(|x: i32| x * 3));
        let mut pollable = init_poll(init);

        // Send input while in SansOutput state (before polling) - should get NeedsPoll
        match pollable.next(Poll::Input(5)) {
            Step::Yielded(PollOutput::NeedsPoll(5)) => {}
            other => panic!("Expected NeedsPoll(5), got {:?}", other),
        }

        // Now poll to get the initial output
        match pollable.next(Poll::Poll) {
            Step::Yielded(PollOutput::Output(100)) => {}
            other => panic!("Expected Output(100), got {:?}", other),
        }
    }

    #[test]
    fn test_init_poll_with_tuple_init() {
        use crate::build::repeat;

        let init = (100, repeat(|x: i32| x + 1));
        let mut pollable = init_poll(init);

        // First poll should yield the initial output
        match pollable.next(Poll::Poll) {
            Step::Yielded(PollOutput::Output(100)) => {}
            other => panic!("Expected Output(100), got {:?}", other),
        }

        // Now it should need input
        match pollable.next(Poll::Poll) {
            Step::Yielded(PollOutput::NeedsInput) => {}
            other => panic!("Expected NeedsInput, got {:?}", other),
        }

        // Send input
        match pollable.next(Poll::Input(5)) {
            Step::Yielded(PollOutput::Output(6)) => {}
            other => panic!("Expected Output(6), got {:?}", other),
        }
    }

    #[test]
    fn test_pollable_multiple_inputs_outputs() {
        let coro = repeat(|x: i32| x * 2);
        let mut pollable = poll(coro);

        for i in 1..=5 {
            // Poll
            assert!(matches!(
                pollable.next(Poll::Poll),
                Step::Yielded(PollOutput::NeedsInput)
            ));

            // Input
            match pollable.next(Poll::Input(i)) {
                Step::Yielded(PollOutput::Output(o)) => assert_eq!(o, i * 2),
                other => panic!("Expected Output({}), got {:?}", i * 2, other),
            }
        }
    }

    #[test]
    fn test_poll_output_then_complete() {
        let coro = once(|x: i32| x + 1);
        let mut pollable = poll(coro);

        // Send input which yields output first
        match pollable.next(Poll::Input(10)) {
            Step::Yielded(PollOutput::Output(11)) => {}
            other => panic!("Expected Output(11), got {:?}", other),
        }

        // Poll indicates needs input
        match pollable.next(Poll::Poll) {
            Step::Yielded(PollOutput::NeedsInput) => {}
            other => panic!("Expected NeedsInput, got {:?}", other),
        }

        // Send another input to complete
        match pollable.next(Poll::Input(20)) {
            Step::Complete(Ok(20)) => {}
            other => panic!("Expected Complete(Ok(20)), got {:?}", other),
        }
    }
}
