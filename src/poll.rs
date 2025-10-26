//! Polling for [`Sans`]
//!
//! This module provides an adapter for polling [`Sans`] coroutines.
use crate::{Sans, Step};

/// A coroutine wrapper that allows polling for outputs and asynchronously providing inputs.
///
/// Created via [`poll`]. Wraps a [`Sans`] coroutine to enable explicit
/// control over when inputs are provided and outputs are retrieved.
pub enum Pollable<I, S>
where
    S: Sans<I>,
{
    Yielded(S::Output, S),
    Return(S::Return),
    Sans(S),
    Completed,
}

/// Input type for [`Pollable`] coroutines.
///
/// Either polls for available output or provides an input value.
pub enum PollInput<I> {
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
    /// Coroutine completed (should not occur in Yielded, only in Complete).
    Complete,
    /// Coroutine needs input before it can produce output.
    NeedsInput,
    /// Input was provided but coroutine wasn't ready for it; poll first.
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

/// Wrap a [`Sans`] coroutine in a [`Pollable`] for explicit input/output control.
///
/// The resulting [`Pollable`] can be polled with [`PollInput::Poll`] to check for available
/// output, or sent inputs with [`PollInput::Input`].
///
/// # Examples
///
/// ```
/// use sans::prelude::*;
/// use sans::poll::{PollInput, PollOutput};
///
/// let coro = repeat(|x: i32| x + 1);
/// let mut pollable = poll(coro);
///
/// // Poll first - coro needs input
/// match pollable.next(PollInput::Poll) {
///     Step::Yielded(PollOutput::NeedsInput) => {}
///     _ => panic!("Expected NeedsInput"),
/// }
///
/// // Provide input
/// match pollable.next(PollInput::Input(5)) {
///     Step::Yielded(PollOutput::Output(6)) => {}
///     _ => panic!("Expected Output(6)"),
/// }
/// ```
pub fn poll<I, S>(coro: S) -> Pollable<I, S>
where
    S: Sans<I>,
{
    Pollable::Sans(coro)
}

impl<I, S> Sans<PollInput<I>> for Pollable<I, S>
where
    S: Sans<I>,
{
    type Output = PollOutput<I, S::Output>;
    type Return = Result<S::Return, PollError>;

    fn next(
        &mut self,
        input: PollInput<I>,
    ) -> Step<Self::Output, <Self as Sans<PollInput<I>>>::Return> {
        match self {
            Pollable::Sans(s) => match input {
                PollInput::Poll => Step::Yielded(PollOutput::NeedsInput),
                PollInput::Input(i) => match s.next(i) {
                    Step::Yielded(o) => Step::Yielded(PollOutput::Output(o)),
                    Step::Complete(r) => {
                        *self = Pollable::Completed;
                        Step::Complete(Ok(r))
                    }
                },
            },
            Pollable::Yielded(_, _) => match input {
                PollInput::Poll => {
                    // Move output out and transition to Sans state
                    let output = std::mem::replace(self, Pollable::Completed);
                    if let Pollable::Yielded(o, s) = output {
                        *self = Pollable::Sans(s);
                        Step::Yielded(PollOutput::Output(o))
                    } else {
                        unreachable!()
                    }
                }
                PollInput::Input(i) => Step::Yielded(PollOutput::NeedsPoll(i)),
            },
            Pollable::Return(_) => {
                let output = std::mem::replace(self, Pollable::Completed);
                if let Pollable::Return(r) = output {
                    Step::Complete(Ok(r))
                } else {
                    unreachable!()
                }
            }
            Pollable::Completed => Step::Complete(Err(PollError::AlreadyComplete)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::func::{once, repeat};

    #[test]
    fn test_poll_basic_needs_input() {
        let coro = repeat(|x: i32| x + 1);
        let mut pollable = poll(coro);

        // Initially, polling should indicate needs input
        match pollable.next(PollInput::Poll) {
            Step::Yielded(PollOutput::NeedsInput) => {}
            _ => panic!("Expected NeedsInput"),
        }
    }

    #[test]
    fn test_poll_input_yields_output() {
        let coro = repeat(|x: i32| x + 1);
        let mut pollable = poll(coro);

        // Send input
        match pollable.next(PollInput::Input(5)) {
            Step::Yielded(PollOutput::Output(6)) => {}
            _ => panic!("Expected Output(6)"),
        }
    }

    #[test]
    fn test_poll_sequence_poll_input_poll() {
        let coro = repeat(|x: i32| x * 2);
        let mut pollable = poll(coro);

        // Poll -> NeedsInput
        match pollable.next(PollInput::Poll) {
            Step::Yielded(PollOutput::NeedsInput) => {}
            _ => panic!("Expected NeedsInput"),
        }

        // Input -> Output
        match pollable.next(PollInput::Input(10)) {
            Step::Yielded(PollOutput::Output(20)) => {}
            _ => panic!("Expected Output(20)"),
        }

        // Poll again -> NeedsInput
        match pollable.next(PollInput::Poll) {
            Step::Yielded(PollOutput::NeedsInput) => {}
            _ => panic!("Expected NeedsInput"),
        }
    }

    #[test]
    fn test_poll_completion() {
        let coro = once(|x: i32| x + 10);
        let mut pollable = poll(coro);

        // Input yields output first
        match pollable.next(PollInput::Input(5)) {
            Step::Yielded(PollOutput::Output(15)) => {}
            other => panic!("Expected Output(15), got {:?}", other),
        }

        // Then send another input to complete (once completes on second input)
        match pollable.next(PollInput::Input(99)) {
            Step::Complete(Ok(99)) => {} // once returns the final input value
            other => panic!("Expected Complete(Ok(99)), got {:?}", other),
        }
    }

    #[test]
    fn test_poll_already_complete_error() {
        let coro = once(|x: i32| x + 1);
        let mut pollable = poll(coro);

        // Send input, get output
        pollable
            .next(PollInput::Input(5))
            .expect_yielded("should yield");

        // Send another input to complete
        let _ = pollable
            .next(PollInput::Input(10))
            .expect_complete("should complete");

        // Try to poll after completion
        match pollable.next(PollInput::Poll) {
            Step::Complete(Err(PollError::AlreadyComplete)) => {}
            _ => panic!("Expected AlreadyComplete error"),
        }

        // Try to send input after completion
        match pollable.next(PollInput::Input(20)) {
            Step::Complete(Err(PollError::AlreadyComplete)) => {}
            _ => panic!("Expected AlreadyComplete error"),
        }
    }

    #[test]
    fn test_pollable_multiple_inputs_outputs() {
        let coro = repeat(|x: i32| x * 2);
        let mut pollable = poll(coro);

        for i in 1..=5 {
            // Poll
            assert!(matches!(
                pollable.next(PollInput::Poll),
                Step::Yielded(PollOutput::NeedsInput)
            ));

            // Input
            match pollable.next(PollInput::Input(i)) {
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
        match pollable.next(PollInput::Input(10)) {
            Step::Yielded(PollOutput::Output(11)) => {}
            other => panic!("Expected Output(11), got {:?}", other),
        }

        // Poll indicates needs input
        match pollable.next(PollInput::Poll) {
            Step::Yielded(PollOutput::NeedsInput) => {}
            other => panic!("Expected NeedsInput, got {:?}", other),
        }

        // Send another input to complete
        match pollable.next(PollInput::Input(20)) {
            Step::Complete(Ok(20)) => {}
            other => panic!("Expected Complete(Ok(20)), got {:?}", other),
        }
    }
}
