use crate::{InitSans, Sans, Step};

struct Start<C, I> {
    coro: C,
    first_input: I,
}

/// Supply the first input when [`InitSans::init`] runs.
///
/// That step can yield or complete. Use [`init`](super::init) to supply an output instead.
///
/// ```
/// use sans::prelude::*;
///
/// let initializer = start(once(|x: i32| x * 2), 5);
/// let (first, mut next) = initializer.init().unwrap_yielded();
/// assert_eq!(first, 10);
/// assert_eq!(next.next(7).unwrap_complete(), 7);
/// ```
pub fn start<I, O, C>(coro: C, first_input: I) -> impl InitSans<I, O, Next = C>
where
    C: Sans<I, O>,
{
    Start { coro, first_input }
}

impl<I, O, C> InitSans<I, O> for Start<C, I>
where
    C: Sans<I, O>,
{
    type Next = C;

    fn init(mut self) -> Step<(O, C), C::Return> {
        match self.coro.next(self.first_input) {
            Step::Yielded(output) => Step::Yielded((output, self.coro)),
            Step::Complete(result) => Step::Complete(result),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::concurrent::{JoinEnvelope, join, join_vec};
    use crate::poll::{Poll, PollOutput};
    use crate::prelude::*;
    use std::cell::Cell;

    fn exact_next<I, O, C: Sans<I, O>>(initializer: impl InitSans<I, O, Next = C>) -> C {
        initializer.init().unwrap_yielded().1
    }

    #[test]
    fn startup_is_lazy_and_preserves_borrowed_continuation() {
        let calls = Cell::new(0);
        let text = String::from("borrowed");
        let coro = repeat(|input: &str| {
            calls.set(calls.get() + 1);
            input.len() + text.len()
        });
        let initializer = start(coro, text.as_str());
        assert_eq!(calls.get(), 0);
        let mut next = exact_next(initializer);
        assert_eq!(calls.get(), 1);
        assert_eq!(next.next("a").unwrap_yielded(), 9);
    }

    #[test]
    fn startup_preserves_send_and_yields_original_next() {
        fn assert_send(_: &impl Send) {}
        let initializer = start(once(|x: i32| x * 2), 5);
        assert_send(&initializer);
        // A non-Send output is not stored by startup and does not restrict Send.
        let output_is_not_send = start(repeat(|x: i32| std::rc::Rc::new(x)), 5);
        assert_send(&output_is_not_send);
        let (first, mut next) = initializer.init().unwrap_yielded();
        assert_eq!(first, 10);
        assert_eq!(next.next(7).unwrap_complete(), 7);
    }

    #[test]
    fn initialized_return_mapping_covers_immediate_and_later_completion() {
        let immediate = start(from_fn(|x: i32| Step::<i32, _>::Complete(x)), 3)
            .map_return(|x| format!("done={x}"));
        assert_eq!(immediate.init().unwrap_complete(), "done=3");
        let initializer = init(10, once(|x: i32| x * 2))
            .map_input(|text: &str| text.parse::<i32>().unwrap())
            .map_yield(|value| format!("value={value}"))
            .map_return(|value| format!("done={value}"));
        let (first, mut next) = initializer.init().unwrap_yielded();
        assert_eq!(first, "value=10");
        assert_eq!(next.next("5").unwrap_yielded(), "value=10");
        assert_eq!(next.next("7").unwrap_complete(), "done=7");
    }

    #[test]
    fn polling_and_join_mapping_are_unambiguous_with_prelude() {
        let mut polling = poll(repeat(|x: i32| x))
            .map_input(|x| x)
            .map_yield(|x| x)
            .map_return(|x| x);
        assert!(matches!(
            polling.next(Poll::Input(5)),
            Step::Yielded(PollOutput::Output(5))
        ));
        let mut array = join([once(|x: i32| x)])
            .map_input(|x| x)
            .map_yield(|x| x)
            .map_return(|x| x);
        assert!(matches!(
            array.next(Poll::Input(JoinEnvelope::new(0, 5))),
            Step::Yielded(PollOutput::Output(_))
        ));
        let mut vector = join_vec(vec![once(|x: i32| x)])
            .map_input(|x| x)
            .map_yield(|x| x)
            .map_return(|x| x);
        assert!(matches!(
            vector.next(Poll::Input(JoinEnvelope::new(0, 5))),
            Step::Yielded(PollOutput::Output(_))
        ));
    }

    #[test]
    fn start_join_runs_through_handle_with_ordered_returns() {
        let children =
            std::array::from_fn::<_, 2, _>(|_| from_fn(|x: i32| Step::<(), _>::Complete(x)));
        let mut index = 0;
        let result = handle(start(join(children), Poll::Poll), |output| {
            assert!(matches!(output, PollOutput::NeedsInput));
            let input = Poll::Input(JoinEnvelope::new(index, 10 + index as i32));
            index += 1;
            input
        });
        assert_eq!(result.unwrap(), [10, 11]);
    }

    #[test]
    fn free_mapping_constructors_support_both_phases() {
        let initializer = map_return(
            |x: i32| x.to_string(),
            map_input(
                |s: &str| s.parse::<i32>().unwrap(),
                init(1, once(|x: i32| x)),
            ),
        );
        let (_, mut next) = initializer.init().unwrap_yielded();
        assert_eq!(next.next("2").unwrap_yielded(), 2);
        assert_eq!(next.next("3").unwrap_complete(), "3");
    }
}
