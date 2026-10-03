use crate::{
    Sans, Step,
    build::{Once, Repeat, once, repeat},
    compose::{
        Chain, MapInput, MapReturn, MapYield, WithState, init_chain, init_map_yield, map_input,
        map_return,
    },
    iter::InitSansIter,
};

/// Start a coroutine without caller input.
///
/// Initialization yields an output and a continuation, or completes immediately.
/// The continuation is a [`Sans`] coroutine; resume it with [`Sans::next`].
///
/// ```rust
/// use sans::prelude::*;
///
/// let coro = init_once(42, |x: i32| x + 1);
/// let (initial, mut next) = coro.init().unwrap_yielded();
/// assert_eq!(initial, 42);
/// assert_eq!(next.next(10).unwrap_yielded(), 11);
/// ```
pub trait InitSans<I, O> {
    /// The coroutine to resume after the initial output.
    type Next: Sans<I, O>;

    /// Yield the initial `(output, continuation)` pair, or complete immediately.
    #[allow(clippy::type_complexity)]
    fn init(self) -> Step<(O, Self::Next), <Self::Next as Sans<I, O>>::Return>;

    /// Pass the final result to the next coroutine as its first input.
    fn chain<R>(self, r: R) -> Chain<Self, R>
    where
        Self: Sized,
        Self::Next: Sans<I, O, Return = I>,
        R: Sans<I, O>,
    {
        init_chain(self, r)
    }

    /// Pass the final result to a function that yields once.
    fn chain_once<F>(self, f: F) -> Chain<Self, Once<F>>
    where
        Self: Sized,
        Self::Next: Sans<I, O, Return = I>,
        F: FnOnce(<Self::Next as Sans<I, O>>::Return) -> O,
    {
        self.chain(once(f))
    }

    /// Continue with a function that yields for every input.
    fn chain_repeat<F>(self, f: F) -> Chain<Self, Repeat<F>>
    where
        Self: Sized,
        Self::Next: Sans<I, O, Return = I>,
        F: FnMut(<Self::Next as Sans<I, O>>::Return) -> O,
    {
        self.chain(repeat(f))
    }

    /// Convert inputs before passing them to the coroutine.
    ///
    /// See [`map_input`](crate::compose::map_input) for an example.
    fn map_input<I2, F>(self, f: F) -> MapInput<Self, F>
    where
        Self: Sized,
        F: FnMut(I2) -> I,
    {
        map_input(f, self)
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
        Finish: FnOnce(State, <Self::Next as Sans<I, O>>::Return) -> R,
    {
        crate::compose::with_state(self, state, input, finish)
    }

    /// Convert the initial output and each later output.
    ///
    /// See [`map_yield`](crate::compose::map_yield) for an example.
    fn map_yield<O2, F>(self, f: F) -> MapYield<Self, F, I, O>
    where
        Self: Sized,
        F: FnMut(O) -> O2,
    {
        init_map_yield(f, self)
    }

    /// Convert the final result, whether initialization or a later step completes.
    ///
    /// See [`map_return`](crate::compose::map_return) for an example.
    fn map_return<D2, F>(self, f: F) -> MapReturn<Self, F>
    where
        Self: Sized,
        F: FnMut(<Self::Next as Sans<I, O>>::Return) -> D2,
    {
        map_return(f, self)
    }

    /// Iterate over outputs, supplying `()` as each input.
    fn into_iter(self) -> InitSansIter<O, Self>
    where
        Self: Sized + InitSans<(), O>,
    {
        InitSansIter::new(self)
    }
}

impl<I, O, S> InitSans<I, O> for (O, S)
where
    S: Sans<I, O>,
{
    type Next = S;
    fn init(self) -> Step<(O, S), S::Return> {
        Step::Yielded(self)
    }
}

impl<I, O, S> InitSans<I, O> for Step<(O, S), S::Return>
where
    S: Sans<I, O>,
{
    type Next = S;
    fn init(self) -> Step<(O, S), S::Return> {
        self
    }
}

impl<I, O, C> InitSans<I, O> for Option<C>
where
    C: InitSans<I, O>,
{
    type Next = Option<C::Next>;

    fn init(self) -> Step<(O, Self::Next), <Self::Next as Sans<I, O>>::Return> {
        match self {
            Some(c) => match c.init() {
                Step::Yielded((o, next)) => Step::Yielded((o, Some(next))),
                Step::Complete(d) => Step::Complete(Some(d)),
            },
            None => Step::Complete(None),
        }
    }
}

impl<I, O, L, R> InitSans<I, O> for either::Either<L, R>
where
    L: InitSans<I, O>,
    R: InitSans<I, O>,
    R::Next: Sans<I, O, Return = <L::Next as Sans<I, O>>::Return>,
{
    type Next = either::Either<L::Next, R::Next>;
    fn init(self) -> Step<(O, Self::Next), <Self::Next as Sans<I, O>>::Return> {
        match self {
            either::Either::Left(l) => match l.init() {
                Step::Yielded((o, next_l)) => Step::Yielded((o, either::Either::Left(next_l))),
                Step::Complete(resume) => Step::Complete(resume),
            },
            either::Either::Right(r) => match r.init() {
                Step::Yielded((o, next_r)) => Step::Yielded((o, either::Either::Right(next_r))),
                Step::Complete(resume) => Step::Complete(resume),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build::{init_once, init_repeat, repeat};

    fn add_three(value: i32) -> i32 {
        value + 3
    }

    #[derive(Clone, Copy, Debug)]
    struct ImmediateFirstDone;

    impl Sans<&'static str, &'static str> for ImmediateFirstDone {
        type Return = &'static str;

        fn next(&mut self, input: &'static str) -> Step<&'static str, Self::Return> {
            Step::Complete(input)
        }
    }

    impl InitSans<&'static str, &'static str> for ImmediateFirstDone {
        type Next = Self;

        fn init(
            self,
        ) -> Step<
            (&'static str, Self::Next),
            <Self::Next as Sans<&'static str, &'static str>>::Return,
        > {
            Step::Complete("left-done")
        }
    }

    #[test]
    fn test_chain_once_into_repeat() {
        let initializer = init_once(10_u32, |input: u32| input + 5);
        let mut multiplier = 2_u32;
        let repeater = repeat(move |input: u32| {
            let output = input * multiplier;
            multiplier += 1;
            output
        });

        let (first_yield, mut coro) = initializer.chain(repeater).init().unwrap_yielded();
        assert_eq!(10, first_yield);
        assert_eq!(13, coro.next(8).unwrap_yielded());
        assert_eq!(16, coro.next(8).unwrap_yielded());
        assert_eq!(24, coro.next(8).unwrap_yielded());
        assert_eq!(32, coro.next(8).unwrap_yielded());
    }

    #[test]
    fn test_map_input_and_map_yield_pipeline() {
        let mut total = 0_i64;
        let (initial_total, mut coro) = init_repeat(0_i64, move |delta: i64| {
            total += delta;
            total
        })
        .map_input(|cmd: &str| -> i64 {
            let mut parts = cmd.split_whitespace();
            let op = parts.next().expect("operation must exist");
            let amount: i64 = parts
                .next()
                .expect("amount must exist")
                .parse()
                .expect("amount must parse");
            match op {
                "add" => amount,
                "sub" => -amount,
                _ => panic!("unsupported op: {op}"),
            }
        })
        .map_yield(|value: i64| format!("total={value}"))
        .init()
        .unwrap_yielded();

        assert_eq!("total=0", initial_total);
        assert_eq!("total=5", coro.next("add 5").unwrap_yielded());
        assert_eq!("total=2", coro.next("sub 3").unwrap_yielded());
        assert_eq!("total=7", coro.next("add 5").unwrap_yielded());
    }

    #[test]
    fn test_chain_and_map_return_resume_flow() {
        use crate::build::once;
        let initializer = init_once(42_u32, |input: u32| input + 1);
        let finisher = once(|input: u32| input * 3);

        let first = initializer.chain(finisher);
        let (first_value, mut coro) = first
            .map_yield(|resume: u32| (resume + 7) as i32)
            .map_return(|done: u32| done as i32 * 3)
            .init()
            .unwrap_yielded();

        assert_eq!(49, first_value);
        assert_eq!(18, coro.next(10).unwrap_yielded());
        assert_eq!(37, coro.next(10).unwrap_yielded());
        assert_eq!(30i32, coro.next(10).unwrap_complete());
    }

    #[test]
    fn test_either_first_right_branch_selected() {
        #[allow(clippy::type_complexity)]
        let coro: either::Either<
            (i32, Repeat<fn(i32) -> i32>),
            (i32, Repeat<fn(i32) -> i32>),
        > = either::Either::Right(init_repeat(2_i32, add_three));

        let (first_value, mut next_coro) = coro.init().unwrap_yielded();
        assert_eq!(2, first_value);
        assert_eq!(5, next_coro.next(2).unwrap_yielded());
        assert_eq!(6, next_coro.next(3).unwrap_yielded());
    }

    #[test]
    fn test_either_first_left_done_returns_resume() {
        let coro: either::Either<ImmediateFirstDone, ImmediateFirstDone> =
            either::Either::Left(ImmediateFirstDone);

        let resume = coro.init().unwrap_complete();
        assert_eq!("left-done", resume);
    }

    #[test]
    fn test_first_ext_map_input_yield_done() {
        use crate::build::once;
        let initializer = init_once(5_u32, |input: u32| input + 2);
        let finisher = once(|value: u32| value * 2);

        let (first_value, mut rest) = initializer
            .chain(finisher)
            .map_input(|text: &str| text.parse::<u32>().expect("number"))
            .map_yield(|value: u32| format!("value={value}"))
            .map_return(|resume: u32| format!("done={resume}"))
            .init()
            .unwrap_yielded();
        assert_eq!("value=5", first_value);
        assert_eq!("value=9", rest.next("7").unwrap_yielded());
        assert_eq!("value=16", rest.next("8").unwrap_yielded());
        assert_eq!("done=9", rest.next("9").unwrap_complete());
    }
}
