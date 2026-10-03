//! Owned state shared by input conversion and completion.

use crate::{InitSans, Sans, Step};

/// Share state between input processing and completion. See [`with_state`].
pub struct WithState<C, State, Input, Finish> {
    coro: C,
    input: Input,
    completion: Option<(State, Finish)>,
}

/// Share state between input processing and completion.
///
/// The input callback borrows the state mutably. On completion, `finish` receives
/// ownership of the state and the final result. Dropping the coroutine drops the
/// state without calling `finish`.
///
/// Supports both [`Sans`] and [`InitSans`]. Use [`Sans::map_yield`] or
/// [`InitSans::map_yield`] to convert outputs.
///
/// ```
/// use sans::prelude::*;
///
/// let mut counted = once(|x: i32| x * 2).with_state(
///     0,
///     |count, input| { *count += 1; input },
///     |count, result| (count, result),
/// );
///
/// assert_eq!(counted.next(3).unwrap_yielded(), 6);
/// assert_eq!(counted.next(5).unwrap_complete(), (2, 5));
/// ```
pub fn with_state<C, State, Input, Finish>(
    coro: C,
    state: State,
    input: Input,
    finish: Finish,
) -> WithState<C, State, Input, Finish> {
    WithState {
        coro,
        input,
        completion: Some((state, finish)),
    }
}

impl<I, InnerInput, O, R, C, State, Input, Finish> Sans<I, O> for WithState<C, State, Input, Finish>
where
    C: Sans<InnerInput, O>,
    Input: FnMut(&mut State, I) -> InnerInput,
    Finish: FnOnce(State, C::Return) -> R,
{
    type Return = R;

    fn next(&mut self, input: I) -> Step<O, R> {
        let (state, _) = self
            .completion
            .as_mut()
            .expect("WithState cannot resume after completion");
        let inner_input = (self.input)(state, input);
        match self.coro.next(inner_input) {
            Step::Yielded(output) => Step::Yielded(output),
            Step::Complete(result) => {
                let (state, finish) = self.completion.take().unwrap();
                Step::Complete(finish(state, result))
            }
        }
    }
}

impl<I, InnerInput, O, R, C, State, Input, Finish> InitSans<I, O>
    for WithState<C, State, Input, Finish>
where
    C: InitSans<InnerInput, O>,
    Input: FnMut(&mut State, I) -> InnerInput,
    Finish: FnOnce(State, <C::Next as Sans<InnerInput, O>>::Return) -> R,
{
    type Next = WithState<C::Next, State, Input, Finish>;

    fn init(self) -> Step<(O, Self::Next), R> {
        match self.coro.init() {
            Step::Yielded((output, next)) => Step::Yielded((
                output,
                WithState {
                    coro: next,
                    input: self.input,
                    completion: self.completion,
                },
            )),
            Step::Complete(result) => {
                let (state, finish) = self.completion.unwrap();
                Step::Complete(finish(state, result))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, collections::HashMap, rc::Rc};

    use super::*;
    use crate::build::{from_fn, init, once};

    #[test]
    fn initializer_shares_owned_cache_and_maps_all_yields() {
        let mut count = 0;
        let core = from_fn(move |id: usize| -> Step<usize, Vec<usize>> {
            count += 1;
            if count == 2 {
                Step::Complete(vec![1, 2])
            } else {
                Step::Yielded(id + 1)
            }
        });
        // Moving this String out of the closure makes finish genuinely FnOnce.
        let label = String::from("documents");
        let wrapped = init(1, core)
            .with_state(
                HashMap::new(),
                |cache, (id, document): (usize, String)| {
                    cache.insert(id, document);
                    id
                },
                move |mut cache, ids| {
                    let documents: Vec<_> = ids
                        .into_iter()
                        .map(|id| cache.remove(&id).unwrap())
                        .collect();
                    (label, documents)
                },
            )
            .map_yield(|id| format!("fetch {id}"));

        let (initial, mut continuation) = wrapped.init().unwrap_yielded();
        assert_eq!(initial, "fetch 1");
        assert_eq!(
            continuation
                .next((1, String::from("first")))
                .unwrap_yielded(),
            "fetch 2"
        );
        assert_eq!(
            continuation
                .next((2, String::from("second")))
                .unwrap_complete(),
            (
                String::from("documents"),
                vec![String::from("first"), String::from("second")]
            )
        );
    }

    #[test]
    fn immediate_init_finishes_without_converting_input() {
        let converted = Cell::new(0);
        let finished = Cell::new(0);
        type Next = crate::build::Once<fn(usize) -> usize>;
        let core: Step<(usize, Next), usize> = Step::Complete(7);
        let owned = String::from("cached");
        let wrapped = core.with_state(
            owned,
            |_, text: &str| {
                converted.set(converted.get() + 1);
                text.len()
            },
            |state, result| {
                finished.set(finished.get() + 1);
                (state, result)
            },
        );

        assert_eq!(
            wrapped.init().unwrap_complete(),
            (String::from("cached"), 7)
        );
        assert_eq!(converted.get(), 0);
        assert_eq!(finished.get(), 1);
    }

    #[test]
    fn continuation_can_borrow_state_and_finish_capture() {
        let prefix = String::from("total");
        let mut inputs = Vec::new();
        let core = from_fn(|value: usize| -> Step<(), usize> { Step::Complete(value * 2) });
        {
            let mut wrapped = core.with_state(
                &mut inputs,
                |seen, text: &str| {
                    seen.push(text.to_owned());
                    text.len()
                },
                |seen, result| (prefix.as_str(), seen.len(), result),
            );
            assert_eq!(wrapped.next("abc").unwrap_complete(), ("total", 1, 6));
        }
        assert_eq!(inputs, vec!["abc"]);
    }

    #[test]
    fn free_constructor_supports_initializer_and_conditional_send() {
        fn assert_send<T: Send>(_: &T) {}
        let wrapped = with_state(
            init(3, once(|value: usize| value + 1)),
            Vec::new(),
            |seen: &mut Vec<usize>, value: usize| {
                seen.push(value);
                value
            },
            |seen: Vec<usize>, result| (seen, result),
        );
        assert_send(&wrapped);
        let (initial, mut continuation) = wrapped.init().unwrap_yielded();
        assert_eq!(initial, 3);
        assert_send(&continuation);
        assert_eq!(continuation.next(4).unwrap_yielded(), 5);
        assert_eq!(continuation.next(6).unwrap_complete(), (vec![4, 6], 6));
    }

    #[test]
    fn protocol_values_do_not_need_to_be_send() {
        fn assert_send<T: Send>(_: &T) {}
        fn complete(value: Rc<usize>) -> Step<Rc<usize>, Rc<usize>> {
            Step::Complete(value)
        }
        let mut wrapped = from_fn(complete as fn(Rc<usize>) -> Step<Rc<usize>, Rc<usize>>)
            .with_state((), |_, value: Rc<usize>| value, |(), result| result);
        assert_send(&wrapped);
        assert_eq!(*wrapped.next(Rc::new(9)).unwrap_complete(), 9);
    }

    #[test]
    fn dropping_suspended_adapter_releases_state_without_finishing() {
        struct DropState<'a>(&'a Cell<usize>);
        impl Drop for DropState<'_> {
            fn drop(&mut self) {
                self.0.set(self.0.get() + 1);
            }
        }
        let dropped = Cell::new(0);
        let finished = Cell::new(0);
        let wrapped = init(1, once(|value: usize| value)).with_state(
            DropState(&dropped),
            |_, value: usize| value,
            |state, _| {
                finished.set(finished.get() + 1);
                drop(state);
            },
        );
        let (_, continuation) = wrapped.init().unwrap_yielded();
        drop(continuation);
        assert_eq!(dropped.get(), 1);
        assert_eq!(finished.get(), 0);
    }

    #[test]
    #[should_panic(expected = "WithState cannot resume after completion")]
    fn completion_prevents_reentering_core_or_callbacks() {
        let core = from_fn(|_: ()| -> Step<(), ()> { Step::Complete(()) });
        let mut wrapped = core.with_state((), |_, ()| (), |(), ()| ());
        wrapped.next(()).unwrap_complete();
        wrapped.next(());
    }
}
