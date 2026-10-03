//! Private state machine shared by sequential adapters.
use crate::{InitSans, Sans, Step};

pub(crate) enum Sequence<S, T, F> {
    OnFirst(S, Option<F>),
    OnSecond(T),
}

impl<S, T, F> Sequence<S, T, F> {
    /// The adapter supplies only return conversion and second-stage creation.
    pub(crate) fn next<I, O, D>(
        &mut self,
        input: I,
        start: impl FnOnce(F, S::Return) -> Step<(O, T), D>,
        finish: impl FnOnce(T::Return) -> D,
    ) -> Step<O, D>
    where
        S: Sans<I, O>,
        T: Sans<I, O>,
    {
        match self {
            Self::OnFirst(first, factory) => match first.next(input) {
                Step::Yielded(output) => Step::Yielded(output),
                Step::Complete(value) => {
                    let factory = factory
                        .take()
                        .expect("sequencing factory can only be used once");
                    match start(factory, value) {
                        Step::Yielded((output, next)) => {
                            *self = Self::OnSecond(next);
                            Step::Yielded(output)
                        }
                        Step::Complete(done) => Step::Complete(done),
                    }
                }
            },
            Self::OnSecond(second) => second.next(input).map_complete(finish),
        }
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn init<I, O, D>(
        self,
        start: impl FnOnce(F, <S::Next as Sans<I, O>>::Return) -> Step<(O, T), D>,
    ) -> Step<(O, Sequence<S::Next, T, F>), D>
    where
        S: InitSans<I, O>,
    {
        let Self::OnFirst(first, factory) = self else {
            unreachable!("initializer must be in its first stage")
        };
        match first.init() {
            Step::Yielded((output, next)) => {
                Step::Yielded((output, Sequence::OnFirst(next, factory)))
            }
            Step::Complete(value) => {
                let factory = factory.expect("sequencing factory must be available");
                start(factory, value)
                    .map_yielded(|(output, next)| (output, Sequence::OnSecond(next)))
            }
        }
    }
}
