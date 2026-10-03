//! Handle errors in coroutine pipelines.
//!
//! | To… | Use |
//! | --- | --- |
//! | Stop on a yielded `Err` | [`short_circuit`] |
//! | Create an infallible coroutine after an `Ok` result | [`ok_then`] |
//! | Create a fallible coroutine after an `Ok` result | [`ok_and_then`] |
//! | Pass an `Ok` result to another coroutine | [`ok_chain`] |
//! | Flatten a nested final `Result` | [`flatten`] |
//!
//! Import [`TrySans`] or [`TryInitSans`] to use the final-result operations as methods.
use crate::compose::sequence::Sequence;
use crate::{InitSans, Sans, step::Step};

/// Stop on a yielded `Err`. See [`short_circuit`].
pub struct ShortCircuit<S, E> {
    coro: S,
    _phantom: std::marker::PhantomData<E>,
}

/// Yield unwrapped `Ok` outputs and complete on the first yielded `Err`.
///
/// Normal completion wraps the final result in `Ok`.
///
/// ```
/// use sans::prelude::*;
/// use sans::result::short_circuit;
///
/// let mut coro = short_circuit(repeat(|text: &str| text.parse::<i32>()));
/// assert_eq!(coro.next("5").unwrap_yielded(), 5);
/// assert!(coro.next("invalid").unwrap_complete().is_err());
/// ```
pub fn short_circuit<S, E>(coro: S) -> ShortCircuit<S, E> {
    ShortCircuit {
        coro,
        _phantom: std::marker::PhantomData,
    }
}

/// The [`short_circuit`] constructor for an [`InitSans`].
pub fn init_short_circuit<I, O, E, S>(coro: S) -> ShortCircuit<S, E>
where
    S: InitSans<I, Result<O, E>>,
{
    ShortCircuit {
        coro,
        _phantom: std::marker::PhantomData,
    }
}

impl<I, O, E, S> Sans<I, O> for ShortCircuit<S, E>
where
    S: Sans<I, Result<O, E>>,
{
    type Return = Result<S::Return, E>;

    fn next(&mut self, input: I) -> Step<O, Self::Return> {
        match self.coro.next(input) {
            Step::Yielded(Ok(o)) => Step::Yielded(o),
            Step::Yielded(Err(e)) => Step::Complete(Err(e)),
            Step::Complete(p) => Step::Complete(Ok(p)),
        }
    }
}

impl<I, O, E, S> InitSans<I, O> for ShortCircuit<S, E>
where
    S: InitSans<I, Result<O, E>>,
{
    type Next = ShortCircuit<S::Next, E>;

    fn init(self) -> Step<(O, Self::Next), Result<<S::Next as Sans<I, Result<O, E>>>::Return, E>> {
        match self.coro.init() {
            Step::Yielded((Ok(o), next)) => Step::Yielded((
                o,
                ShortCircuit {
                    coro: next,
                    _phantom: std::marker::PhantomData,
                },
            )),
            Step::Yielded((Err(e), _)) => Step::Complete(Err(e)),
            Step::Complete(p) => Step::Complete(Ok(p)),
        }
    }
}

/// Run an infallible coroutine after an `Ok` result. See [`ok_then`].
pub struct OkMap<S, T, F> {
    state: Sequence<S, T, F>,
}

/// Use an `Ok` final result to create the next coroutine, then wrap its result in `Ok`.
///
/// An `Err` stops the chain. Use [`ok_and_then`] if the next coroutine can fail.
///
/// ```
/// use sans::prelude::*;
/// use sans::result::TrySans;
///
/// let mut coro = once(|x: i32| x * 2)
///     .map_return(Ok::<_, &str>)
///     .ok_then(|value| init(value, once(move |x| x + value)));
///
/// assert_eq!(coro.next(5).unwrap_yielded(), 10);
/// assert_eq!(coro.next(3).unwrap_yielded(), 3);
/// assert_eq!(coro.next(7).unwrap_yielded(), 10);
/// assert_eq!(coro.next(9).unwrap_complete(), Ok(9));
/// ```
pub fn ok_then<I, O, P, E, S, T, F>(coro: S, f: F) -> OkMap<S, T::Next, F>
where
    S: Sans<I, O, Return = Result<P, E>>,
    T: InitSans<I, O>,
    T::Next: Sans<I, O>,
    F: FnOnce(P) -> T,
{
    OkMap {
        state: Sequence::OnFirst(coro, Some(f)),
    }
}

/// The [`ok_then`] constructor for an [`InitSans`].
pub fn init_ok_then<I, O, P, E, S, T, F>(coro: S, f: F) -> OkMap<S, T::Next, F>
where
    S: InitSans<I, O>,
    S::Next: Sans<I, O, Return = Result<P, E>>,
    T: InitSans<I, O>,
    T::Next: Sans<I, O>,
    F: FnOnce(P) -> T,
{
    OkMap {
        state: Sequence::OnFirst(coro, Some(f)),
    }
}

impl<I, O, P, E, S, T, F> Sans<I, O> for OkMap<S, T::Next, F>
where
    S: Sans<I, O, Return = Result<P, E>>,
    T: InitSans<I, O>,
    T::Next: Sans<I, O>,
    F: FnOnce(P) -> T,
{
    type Return = Result<<T::Next as Sans<I, O>>::Return, E>;

    fn next(&mut self, input: I) -> Step<O, Self::Return> {
        self.state.next(
            input,
            |f, value| match value {
                Ok(value) => f(value).init().map_complete(Ok),
                Err(error) => Step::Complete(Err(error)),
            },
            Ok,
        )
    }
}

impl<I, O, P, E, S, T, F> InitSans<I, O> for OkMap<S, T::Next, F>
where
    S: InitSans<I, O>,
    S::Next: Sans<I, O, Return = Result<P, E>>,
    T: InitSans<I, O>,
    T::Next: Sans<I, O>,
    F: FnOnce(P) -> T,
{
    type Next = OkMap<S::Next, T::Next, F>;

    fn init(self) -> Step<(O, Self::Next), Result<<T::Next as Sans<I, O>>::Return, E>> {
        self.state
            .init(|f, value| match value {
                Ok(value) => f(value).init().map_complete(Ok),
                Err(error) => Step::Complete(Err(error)),
            })
            .map_yielded(|(output, state)| (output, OkMap { state }))
    }
}

/// Run a fallible coroutine after an `Ok` result. See [`ok_and_then`].
pub struct OkAndThen<S, T, F> {
    state: Sequence<S, T, F>,
}

/// Use an `Ok` final result to create the next fallible coroutine.
///
/// An `Err` final result from either coroutine stops the chain.
///
/// ```
/// use sans::prelude::*;
/// use sans::result::TrySans;
///
/// let mut coro = once(|x: i32| x * 2)
///     .map_return(Ok::<_, &str>)
///     .ok_and_then(|value| init(value, once(|x| x).map_return(|x| {
///         if x > 0 { Ok(x) } else { Err("non-positive") }
///     })));
///
/// assert_eq!(coro.next(5).unwrap_yielded(), 10);
/// assert_eq!(coro.next(3).unwrap_yielded(), 3);
/// assert_eq!(coro.next(7).unwrap_yielded(), 7);
/// assert_eq!(coro.next(0).unwrap_complete(), Err("non-positive"));
/// ```
pub fn ok_and_then<I, O, P, Q, E, S, T, F>(coro: S, f: F) -> OkAndThen<S, T::Next, F>
where
    S: Sans<I, O, Return = Result<P, E>>,
    T: InitSans<I, O>,
    T::Next: Sans<I, O, Return = Result<Q, E>>,
    F: FnOnce(P) -> T,
{
    OkAndThen {
        state: Sequence::OnFirst(coro, Some(f)),
    }
}

/// The [`ok_and_then`] constructor for an [`InitSans`].
pub fn init_ok_and_then<I, O, P, Q, E, S, T, F>(coro: S, f: F) -> OkAndThen<S, T::Next, F>
where
    S: InitSans<I, O>,
    S::Next: Sans<I, O, Return = Result<P, E>>,
    T: InitSans<I, O>,
    T::Next: Sans<I, O, Return = Result<Q, E>>,
    F: FnOnce(P) -> T,
{
    OkAndThen {
        state: Sequence::OnFirst(coro, Some(f)),
    }
}

impl<I, O, P, Q, E, S, T, F> Sans<I, O> for OkAndThen<S, T::Next, F>
where
    S: Sans<I, O, Return = Result<P, E>>,
    T: InitSans<I, O>,
    T::Next: Sans<I, O, Return = Result<Q, E>>,
    F: FnOnce(P) -> T,
{
    type Return = Result<Q, E>;

    fn next(&mut self, input: I) -> Step<O, Self::Return> {
        self.state.next(
            input,
            |f, value| match value {
                Ok(value) => f(value).init().map_complete(|value| value),
                Err(error) => Step::Complete(Err(error)),
            },
            |value| value,
        )
    }
}

impl<I, O, P, Q, E, S, T, F> InitSans<I, O> for OkAndThen<S, T::Next, F>
where
    S: InitSans<I, O>,
    S::Next: Sans<I, O, Return = Result<P, E>>,
    T: InitSans<I, O>,
    T::Next: Sans<I, O, Return = Result<Q, E>>,
    F: FnOnce(P) -> T,
{
    type Next = OkAndThen<S::Next, T::Next, F>;

    fn init(self) -> Step<(O, Self::Next), Result<Q, E>> {
        self.state
            .init(|f, value| match value {
                Ok(value) => f(value).init().map_complete(|value| value),
                Err(error) => Step::Complete(Err(error)),
            })
            .map_yielded(|(output, state)| (output, OkAndThen { state }))
    }
}

/// Pass an `Ok` result to the next coroutine. See [`ok_chain`].
pub struct OkChain<S, R> {
    coro: Option<S>,
    next: R,
}

/// Pass an `Ok` final result to the next coroutine as its first input.
///
/// An `Err` stops the chain. The second coroutine's final result is wrapped in `Ok`.
///
/// ```
/// use sans::prelude::*;
/// use sans::result::TrySans;
///
/// let mut coro = once(|x: i32| x * 2)
///     .map_return(Ok::<_, &str>)
///     .ok_chain(once(|x| x + 1));
///
/// assert_eq!(coro.next(5).unwrap_yielded(), 10);
/// assert_eq!(coro.next(3).unwrap_yielded(), 4);
/// assert_eq!(coro.next(9).unwrap_complete(), Ok(9));
/// ```
pub fn ok_chain<I, O, E, S, R>(coro: S, next: R) -> OkChain<S, R>
where
    S: Sans<I, O, Return = Result<I, E>>,
    R: Sans<I, O>,
{
    OkChain {
        coro: Some(coro),
        next,
    }
}

/// The [`ok_chain`] constructor for an [`InitSans`].
pub fn init_ok_chain<I, O, E, S, R>(coro: S, next: R) -> OkChain<S, R>
where
    S: InitSans<I, O>,
    S::Next: Sans<I, O, Return = Result<I, E>>,
    R: Sans<I, O>,
{
    OkChain {
        coro: Some(coro),
        next,
    }
}

impl<I, O, E, S, R> Sans<I, O> for OkChain<S, R>
where
    S: Sans<I, O, Return = Result<I, E>>,
    R: Sans<I, O>,
{
    type Return = Result<R::Return, E>;

    fn next(&mut self, input: I) -> Step<O, Self::Return> {
        if self.coro.is_none() {
            return self.next.next(input).map_complete(Ok);
        }

        let mut coro = self.coro.take().expect("OkChain coro already consumed");
        match coro.next(input) {
            Step::Yielded(o) => {
                self.coro = Some(coro);
                Step::Yielded(o)
            }
            Step::Complete(Err(e)) => Step::Complete(Err(e)),
            Step::Complete(Ok(i)) => match self.next.next(i) {
                Step::Yielded(o) => Step::Yielded(o),
                Step::Complete(ret) => Step::Complete(Ok(ret)),
            },
        }
    }
}

impl<I, O, E, S, R> InitSans<I, O> for OkChain<S, R>
where
    S: InitSans<I, O>,
    S::Next: Sans<I, O, Return = Result<I, E>>,
    R: Sans<I, O>,
{
    type Next = OkChain<S::Next, R>;

    fn init(mut self) -> Step<(O, Self::Next), Result<R::Return, E>> {
        match self.coro.take().expect("OkChain coro must be Some").init() {
            Step::Yielded((o, next)) => Step::Yielded((
                o,
                OkChain {
                    coro: Some(next),
                    next: self.next,
                },
            )),
            Step::Complete(Err(e)) => Step::Complete(Err(e)),
            Step::Complete(Ok(i)) => match self.next.next(i) {
                Step::Yielded(o) => Step::Yielded((
                    o,
                    OkChain {
                        coro: None,
                        next: self.next,
                    },
                )),
                Step::Complete(ret) => Step::Complete(Ok(ret)),
            },
        }
    }
}

/// Flatten a nested final `Result`. See [`flatten`].
pub struct Flatten<S> {
    coro: S,
}

/// Flatten the final `Result<Result<T, E>, E>` into `Result<T, E>`.
///
/// Yielded outputs stay unchanged.
///
/// ```
/// use sans::prelude::*;
/// use sans::result::TrySans;
///
/// let mut coro = once(|x: i32| x * 2)
///     .map_return(|x| Ok::<_, &str>(Ok(x)))
///     .flatten();
///
/// assert_eq!(coro.next(5).unwrap_yielded(), 10);
/// assert_eq!(coro.next(3).unwrap_complete(), Ok(3));
/// ```
pub fn flatten<S>(coro: S) -> Flatten<S> {
    Flatten { coro }
}

/// The [`flatten`] constructor for an [`InitSans`].
pub fn init_flatten<I, O, T, E, S>(coro: S) -> Flatten<S>
where
    S: InitSans<I, O>,
    S::Next: Sans<I, O, Return = Result<Result<T, E>, E>>,
{
    Flatten { coro }
}

impl<I, O, T, E, S> Sans<I, O> for Flatten<S>
where
    S: Sans<I, O, Return = Result<Result<T, E>, E>>,
{
    type Return = Result<T, E>;

    fn next(&mut self, input: I) -> Step<O, Self::Return> {
        match self.coro.next(input) {
            Step::Yielded(o) => Step::Yielded(o),
            Step::Complete(Ok(Ok(t))) => Step::Complete(Ok(t)),
            Step::Complete(Ok(Err(e))) => Step::Complete(Err(e)),
            Step::Complete(Err(e)) => Step::Complete(Err(e)),
        }
    }
}

impl<I, O, T, E, S> InitSans<I, O> for Flatten<S>
where
    S: InitSans<I, O>,
    S::Next: Sans<I, O, Return = Result<Result<T, E>, E>>,
{
    type Next = Flatten<S::Next>;

    fn init(self) -> Step<(O, Self::Next), Result<T, E>> {
        match self.coro.init() {
            Step::Yielded((o, next)) => Step::Yielded((o, Flatten { coro: next })),
            Step::Complete(Ok(Ok(t))) => Step::Complete(Ok(t)),
            Step::Complete(Ok(Err(e))) => Step::Complete(Err(e)),
            Step::Complete(Err(e)) => Step::Complete(Err(e)),
        }
    }
}

/// Final-result methods for [`Sans`]. Import this trait to use them.
pub trait TrySans<I, O>: Sized {
    /// Create the next coroutine from an `Ok` result. See [`ok_then`].
    fn ok_then<P, E, T, F>(self, f: F) -> OkMap<Self, T::Next, F>
    where
        Self: Sans<I, O, Return = Result<P, E>>,
        T: InitSans<I, O>,
        T::Next: Sans<I, O>,
        F: FnOnce(P) -> T,
    {
        ok_then(self, f)
    }

    /// Create the next fallible coroutine from an `Ok` result. See [`ok_and_then`].
    fn ok_and_then<P, Q, E, T, F>(self, f: F) -> OkAndThen<Self, T::Next, F>
    where
        Self: Sans<I, O, Return = Result<P, E>>,
        T: InitSans<I, O>,
        T::Next: Sans<I, O, Return = Result<Q, E>>,
        F: FnOnce(P) -> T,
    {
        ok_and_then(self, f)
    }

    /// Pass an `Ok` result to the next coroutine. See [`ok_chain`].
    fn ok_chain<E, R>(self, next: R) -> OkChain<Self, R>
    where
        Self: Sans<I, O, Return = Result<I, E>>,
        R: Sans<I, O>,
    {
        ok_chain(self, next)
    }

    /// Flatten a nested final `Result`. See [`flatten`].
    fn flatten<T, E>(self) -> Flatten<Self>
    where
        Self: Sans<I, O, Return = Result<Result<T, E>, E>>,
    {
        flatten(self)
    }
}

impl<I, O, S> TrySans<I, O> for S where S: Sans<I, O> {}

/// Final-result methods for [`InitSans`]. Import this trait to use them.
pub trait TryInitSans<I, O>: InitSans<I, O> + Sized {
    /// Create the next coroutine from an `Ok` result. See [`ok_then`].
    fn ok_then<P, E, T, F>(self, f: F) -> OkMap<Self, T::Next, F>
    where
        Self: InitSans<I, O>,
        Self::Next: Sans<I, O, Return = Result<P, E>>,
        T: InitSans<I, O>,
        T::Next: Sans<I, O>,
        F: FnOnce(P) -> T,
    {
        init_ok_then(self, f)
    }

    /// Create the next fallible coroutine from an `Ok` result. See [`ok_and_then`].
    fn ok_and_then<P, Q, E, T, F>(self, f: F) -> OkAndThen<Self, T::Next, F>
    where
        Self: InitSans<I, O>,
        Self::Next: Sans<I, O, Return = Result<P, E>>,
        T: InitSans<I, O>,
        T::Next: Sans<I, O, Return = Result<Q, E>>,
        F: FnOnce(P) -> T,
    {
        init_ok_and_then(self, f)
    }

    /// Pass an `Ok` result to the next coroutine. See [`ok_chain`].
    fn ok_chain<E, R>(self, next: R) -> OkChain<Self, R>
    where
        Self: InitSans<I, O>,
        Self::Next: Sans<I, O, Return = Result<I, E>>,
        R: Sans<I, O>,
    {
        init_ok_chain(self, next)
    }

    /// Flatten a nested final `Result`. See [`flatten`].
    fn flatten<T, E>(self) -> Flatten<Self>
    where
        Self: InitSans<I, O>,
        Self::Next: Sans<I, O, Return = Result<Result<T, E>, E>>,
    {
        init_flatten(self)
    }
}

impl<I, O, S> TryInitSans<I, O> for S where S: InitSans<I, O> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Sans;
    use crate::build::init;
    use crate::build::{once, repeat};

    // A fully typed immediate initializer exercises paths that tuple seeds cannot.
    struct Immediate<D>(D);

    impl<D> InitSans<i32, i32> for Immediate<D> {
        type Next = crate::build::FromFn<fn(i32) -> Step<i32, D>>;

        fn init(self) -> Step<(i32, Self::Next), D> {
            Step::Complete(self.0)
        }
    }

    #[test]
    fn first_error_skips_factory_in_both_phases() {
        use crate::build::from_fn;
        use std::cell::Cell;
        let calls = Cell::new(0);
        let factory = |_| {
            calls.set(calls.get() + 1);
            Immediate(42)
        };
        let mut continuation = ok_then(
            from_fn(|_: i32| Step::<i32, Result<i32, &str>>::Complete(Err("first"))),
            factory,
        );
        assert_eq!(continuation.next(0), Step::Complete(Err("first")));
        assert_eq!(calls.get(), 0);
        let initializer = init_ok_then(Immediate(Err::<i32, _>("initial")), factory);
        assert_eq!(initializer.init().unwrap_complete(), Err("initial"));
        assert_eq!(calls.get(), 0);

        let fallible_factory = |_| {
            calls.set(calls.get() + 1);
            Immediate(Ok::<_, &str>(42))
        };
        let mut continuation = ok_and_then(
            from_fn(|_: i32| Step::<i32, Result<i32, &str>>::Complete(Err("first"))),
            fallible_factory,
        );
        assert_eq!(continuation.next(0), Step::Complete(Err("first")));
        let initializer = init_ok_and_then(Immediate(Err::<i32, _>("initial")), fallible_factory);
        assert_eq!(initializer.init().unwrap_complete(), Err("initial"));
        assert_eq!(calls.get(), 0);
    }

    #[test]
    fn second_initializer_immediate_completion_converts_only_infallible_return() {
        use crate::build::from_fn;
        let mut infallible = ok_then(
            from_fn(|x: i32| Step::<i32, Result<i32, &str>>::Complete(Ok(x))),
            |value| Immediate(value + 1),
        );
        assert_eq!(infallible.next(3), Step::Complete(Ok(4)));
        let mut fallible = ok_and_then(
            from_fn(|x: i32| Step::<i32, Result<i32, &str>>::Complete(Ok(x))),
            |_| Immediate(Err::<i32, _>("second")),
        );
        assert_eq!(fallible.next(3), Step::Complete(Err("second")));
        assert_eq!(
            init_ok_then(Immediate(Ok::<_, &str>(3)), |value| Immediate(value + 1))
                .init()
                .unwrap_complete(),
            Ok(4),
        );
        assert_eq!(
            init_ok_and_then(Immediate(Ok::<_, &str>(3)), |_| Immediate(Err::<i32, _>(
                "second"
            )))
            .init()
            .unwrap_complete(),
            Err("second"),
        );
    }

    #[test]
    fn initially_complete_first_installs_second_continuation() {
        use crate::build::from_fn;
        let initializer = init_ok_then(Immediate(Ok::<_, &str>(3)), |value| {
            init(value, once(move |x: i32| x + value))
        });
        let (initial, mut next) = initializer.init().unwrap_yielded();
        assert_eq!(initial, 3);
        assert_eq!(next.next(5), Step::Yielded(8));
        assert_eq!(next.next(9), Step::Complete(Ok(9)));

        let initializer = init_ok_and_then(Immediate(Ok::<_, &str>(3)), |value| {
            init(
                value,
                from_fn(|_: i32| Step::<i32, Result<i32, &str>>::Complete(Err("second"))),
            )
        });
        let (initial, mut next) = initializer.init().unwrap_yielded();
        assert_eq!(initial, 3);
        assert_eq!(next.next(5), Step::Complete(Err("second")));
    }

    #[test]
    fn factory_is_consumed_once_and_all_yields_keep_order() {
        use crate::build::from_fn;
        use std::cell::Cell;
        let calls = Cell::new(0);
        let owned = String::from("done");
        let first = once(|x: i32| x).map_return(Ok::<_, &str>);
        let mut next = ok_then(first, |value| {
            calls.set(calls.get() + 1);
            // Moving this String out requires FnOnce rather than FnMut.
            let owned = owned;
            let mut remaining = 2;
            init(
                value * 10,
                from_fn(move |x: i32| {
                    remaining -= 1;
                    if remaining > 0 {
                        Step::Yielded(x + value)
                    } else {
                        Step::Complete(owned.len())
                    }
                }),
            )
        });
        assert_eq!(next.next(1), Step::Yielded(1));
        assert_eq!(next.next(2), Step::Yielded(20));
        assert_eq!(next.next(3), Step::Yielded(5));
        assert_eq!(next.next(4), Step::Complete(Ok(4)));
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn plain_sequence_accepts_arbitrary_return_and_immediate_second() {
        use crate::build::from_fn;
        let mut next = crate::compose::and_then(
            from_fn(|_: i32| Step::<i32, String>::Complete(String::from("done"))),
            |value| Immediate(value.len()),
        );
        assert_eq!(next.next(0), Step::Complete(4));
    }

    #[test]
    fn named_adapters_support_exact_storage_types() {
        use crate::build::{FromFn, from_fn};
        type Plain = FromFn<fn(i32) -> Step<i32, i32>>;
        type Fallible = FromFn<fn(i32) -> Step<i32, Result<i32, &'static str>>>;
        type PlainFactory = fn(i32) -> (i32, Plain);
        type FallibleFactory = fn(i32) -> (i32, Fallible);
        fn plain(x: i32) -> Step<i32, i32> {
            Step::Complete(x)
        }
        fn fallible(x: i32) -> Step<i32, Result<i32, &'static str>> {
            Step::Complete(Ok(x))
        }
        fn plain_factory(x: i32) -> (i32, Plain) {
            init(x, from_fn(plain as fn(_) -> _))
        }
        fn fallible_factory(x: i32) -> (i32, Fallible) {
            init(x, from_fn(fallible as fn(_) -> _))
        }
        let mut plain_sequence: crate::compose::AndThen<Plain, Plain, PlainFactory> =
            crate::compose::and_then(from_fn(plain as fn(_) -> _), plain_factory as PlainFactory);
        let mut infallible_sequence: OkMap<Fallible, Plain, PlainFactory> = ok_then(
            from_fn(fallible as fn(_) -> _),
            plain_factory as PlainFactory,
        );
        let mut fallible_sequence: OkAndThen<Fallible, Fallible, FallibleFactory> = ok_and_then(
            from_fn(fallible as fn(_) -> _),
            fallible_factory as FallibleFactory,
        );
        assert_eq!(plain_sequence.next(3), Step::Yielded(3));
        assert_eq!(infallible_sequence.next(3), Step::Yielded(3));
        assert_eq!(fallible_sequence.next(3), Step::Yielded(3));
        assert_eq!(plain_sequence.next(4), Step::Complete(4));
        assert_eq!(infallible_sequence.next(4), Step::Complete(Ok(4)));
        assert_eq!(fallible_sequence.next(4), Step::Complete(Ok(4)));
    }

    #[test]
    fn named_adapters_preserve_borrowed_captures_and_send() {
        use crate::build::from_fn;
        fn assert_send<T: Send>(_: &T) {}
        let offset = 5;
        let borrowed = &offset;
        let first = from_fn(|x: i32| Step::<i32, Result<i32, &str>>::Complete(Ok(x)));
        let mut infallible: OkMap<_, _, _> = ok_then(first, move |value| {
            init(value + borrowed, once(move |x: i32| x + borrowed))
        });
        assert_send(&infallible);
        assert_eq!(infallible.next(3), Step::Yielded(8));
        assert_eq!(infallible.next(4), Step::Yielded(9));
        assert_eq!(infallible.next(2), Step::Complete(Ok(2)));
        let first = from_fn(|x: i32| Step::<i32, Result<i32, &str>>::Complete(Ok(x)));
        let mut fallible: OkAndThen<_, _, _> = ok_and_then(first, move |value| {
            init(
                value + borrowed,
                from_fn(move |x: i32| Step::<i32, Result<i32, &str>>::Complete(Ok(x + borrowed))),
            )
        });
        assert_send(&fallible);
        assert_eq!(fallible.next(3), Step::Yielded(8));
        assert_eq!(fallible.next(4), Step::Complete(Ok(9)));
        let mut plain: crate::compose::AndThen<_, _, _> = crate::compose::and_then(
            from_fn(|x: i32| Step::<i32, i32>::Complete(x)),
            move |value| init(value + borrowed, once(move |x: i32| x + borrowed)),
        );
        assert_send(&plain);
        assert_eq!(plain.next(3), Step::Yielded(8));
        assert_eq!(plain.next(4), Step::Yielded(9));
        assert_eq!(plain.next(2), Step::Complete(2));
    }

    #[test]
    fn test_short_circuit_propagates_ok_yields() {
        let coro = repeat(|x: i32| if x < 0 { Err("negative") } else { Ok(x * 2) });
        let mut sc = short_circuit(coro);

        assert_eq!(sc.next(5).unwrap_yielded(), 10);
        assert_eq!(sc.next(3).unwrap_yielded(), 6);
        assert_eq!(sc.next(10).unwrap_yielded(), 20);
    }

    #[test]
    fn test_short_circuit_stops_on_err() {
        let coro = repeat(|x: i32| if x < 0 { Err("negative") } else { Ok(x * 2) });
        let mut sc = short_circuit(coro);

        assert_eq!(sc.next(5).unwrap_yielded(), 10);
        assert_eq!(sc.next(-1).unwrap_complete(), Err("negative"));
    }

    #[test]
    fn test_short_circuit_completes_with_ok() {
        let coro = once(|x: i32| if x < 0 { Err("negative") } else { Ok(x * 2) });
        let mut sc = short_circuit(coro);

        assert_eq!(sc.next(5).unwrap_yielded(), 10);
        assert_eq!(sc.next(3).unwrap_complete(), Ok(3));
    }

    #[test]
    fn test_ok_then_propagates_err() {
        use crate::build::from_fn;
        let mut called = false;
        let coro = from_fn(move |x: i32| {
            if !called {
                called = true;
                Step::Yielded(x * 2)
            } else if x > 0 {
                Step::Complete(Ok(x))
            } else {
                Step::Complete(Err("non-positive".to_string()))
            }
        });

        let mut mapped = ok_then(coro, |val| init(val, repeat(move |x: i32| x + val)));

        assert_eq!(mapped.next(5).unwrap_yielded(), 10);
        assert_eq!(
            mapped.next(-5).unwrap_complete(),
            Err("non-positive".to_string())
        );
    }

    #[test]
    fn test_ok_then_chains_on_ok() {
        use crate::build::from_fn;
        let mut called = false;
        let coro = from_fn(move |x: i32| -> Step<i32, Result<i32, String>> {
            if !called {
                called = true;
                Step::Yielded(x * 2)
            } else {
                Step::Complete(Ok(x))
            }
        });

        let mut mapped = ok_then(coro, |val| init(val, repeat(move |x: i32| x + val)));

        assert_eq!(mapped.next(5).unwrap_yielded(), 10);
        assert_eq!(mapped.next(3).unwrap_yielded(), 3); // initial value from init
        assert_eq!(mapped.next(7).unwrap_yielded(), 10); // 3 + 7
    }

    #[test]
    fn test_ok_and_then_propagates_err_from_first() {
        use crate::build::from_fn;
        let mut called = false;
        let coro = from_fn(
            move |x: i32| -> Step<Result<i32, String>, Result<i32, String>> {
                if !called {
                    called = true;
                    Step::Yielded(Ok(x * 2))
                } else if x > 0 {
                    Step::Complete(Ok(x))
                } else {
                    Step::Complete(Err("non-positive".to_string()))
                }
            },
        );

        let mut chained = ok_and_then(coro, |val| {
            init(
                Ok(val),
                from_fn(
                    move |x: i32| -> Step<Result<i32, String>, Result<i32, String>> {
                        Step::Yielded(Ok(x + val))
                    },
                ),
            )
        });

        assert_eq!(chained.next(5).unwrap_yielded(), Ok(10));
        assert_eq!(
            chained.next(-5).unwrap_complete(),
            Err("non-positive".to_string())
        );
    }

    #[test]
    fn test_ok_and_then_chains_and_propagates_err_from_second() {
        use crate::build::from_fn;
        let mut first_called = false;
        let coro = from_fn(
            move |x: i32| -> Step<Result<i32, String>, Result<i32, String>> {
                if !first_called {
                    first_called = true;
                    Step::Yielded(Ok(x * 2))
                } else {
                    Step::Complete(Ok(x))
                }
            },
        );

        let mut chained = ok_and_then(coro, |val| {
            init(
                Ok(val),
                from_fn(
                    move |x: i32| -> Step<Result<i32, String>, Result<i32, String>> {
                        if x > 100 {
                            Step::Yielded(Err("too large".to_string()))
                        } else {
                            Step::Yielded(Ok(x + val))
                        }
                    },
                ),
            )
        });

        assert_eq!(chained.next(5).unwrap_yielded(), Ok(10));
        assert_eq!(chained.next(3).unwrap_yielded(), Ok(3));
        assert_eq!(chained.next(50).unwrap_yielded(), Ok(53));
        assert_eq!(
            chained.next(200).unwrap_yielded(),
            Err("too large".to_string())
        );
    }

    #[test]
    fn test_ok_and_then_both_ok() {
        use crate::build::from_fn;
        let mut first_called = false;
        let coro = from_fn(
            move |x: i32| -> Step<Result<i32, String>, Result<i32, String>> {
                if !first_called {
                    first_called = true;
                    Step::Yielded(Ok(x * 2))
                } else {
                    Step::Complete(Ok(x))
                }
            },
        );

        let mut chained = ok_and_then(coro, |val| {
            let mut second_called = false;
            init(
                Ok(val),
                from_fn(
                    move |x: i32| -> Step<Result<i32, String>, Result<i32, String>> {
                        if !second_called {
                            second_called = true;
                            Step::Yielded(Ok(x + val))
                        } else {
                            Step::Complete(Ok(x))
                        }
                    },
                ),
            )
        });

        assert_eq!(chained.next(5).unwrap_yielded(), Ok(10));
        assert_eq!(chained.next(3).unwrap_yielded(), Ok(3));
        assert_eq!(chained.next(7).unwrap_yielded(), Ok(10));
        assert_eq!(chained.next(5).unwrap_complete(), Ok(5));
    }

    #[test]
    fn test_ok_chain_propagates_err() {
        use crate::build::from_fn;
        let mut called = false;
        let first = from_fn(move |x: i32| {
            if !called {
                called = true;
                Step::Yielded(x * 2)
            } else if x > 0 {
                Step::Complete(Ok(x))
            } else {
                Step::Complete(Err("non-positive".to_string()))
            }
        });
        let second = repeat(|x: i32| x + 1);

        let mut chained = ok_chain(first, second);

        assert_eq!(chained.next(5).unwrap_yielded(), 10);
        assert_eq!(
            chained.next(-5).unwrap_complete(),
            Err("non-positive".to_string())
        );
    }

    #[test]
    fn test_ok_chain_chains_on_ok() {
        use crate::build::from_fn;
        let mut called = false;
        let first = from_fn(move |x: i32| -> Step<i32, Result<i32, String>> {
            if !called {
                called = true;
                Step::Yielded(x * 2)
            } else {
                Step::Complete(Ok(x))
            }
        });
        let second = repeat(|x: i32| x + 1);

        let mut chained = ok_chain(first, second);

        assert_eq!(chained.next(5).unwrap_yielded(), 10);
        assert_eq!(chained.next(3).unwrap_yielded(), 4);
        assert_eq!(chained.next(10).unwrap_yielded(), 11);
    }

    #[test]
    fn test_flatten_outer_err() {
        use crate::build::from_fn;
        let mut called = false;
        let coro = from_fn(move |x: i32| {
            if !called {
                called = true;
                Step::Yielded(x * 2)
            } else if x > 0 {
                Step::Complete(Ok(Ok(x)))
            } else {
                Step::Complete(Err("outer error".to_string()))
            }
        });

        let mut flattened = flatten(coro);

        assert_eq!(flattened.next(5).unwrap_yielded(), 10);
        assert_eq!(
            flattened.next(-5).unwrap_complete(),
            Err("outer error".to_string())
        );
    }

    #[test]
    fn test_flatten_inner_err() {
        use crate::build::from_fn;
        let mut called = false;
        let coro = from_fn(move |x: i32| {
            if !called {
                called = true;
                Step::Yielded(x * 2)
            } else if x > 0 {
                if x < 100 {
                    Step::Complete(Ok(Ok(x)))
                } else {
                    Step::Complete(Ok(Err("inner error".to_string())))
                }
            } else {
                Step::Complete(Err("outer error".to_string()))
            }
        });

        let mut flattened = flatten(coro);

        assert_eq!(flattened.next(5).unwrap_yielded(), 10);
        assert_eq!(
            flattened.next(150).unwrap_complete(),
            Err("inner error".to_string())
        );
    }

    #[test]
    fn test_flatten_both_ok() {
        use crate::build::from_fn;
        let mut called = false;
        let coro = from_fn(move |x: i32| {
            if !called {
                called = true;
                Step::Yielded(x * 2)
            } else if x > 0 {
                if x < 100 {
                    Step::Complete(Ok(Ok(x)))
                } else {
                    Step::Complete(Ok(Err("too large".to_string())))
                }
            } else {
                Step::Complete(Err("non-positive".to_string()))
            }
        });

        let mut flattened = flatten(coro);

        assert_eq!(flattened.next(5).unwrap_yielded(), 10);
        assert_eq!(flattened.next(10).unwrap_complete(), Ok(10));
    }

    // InitSans tests

    #[test]
    fn test_short_circuit_init_yields_ok() {
        let init_coro = init(
            Ok(42),
            repeat(|x: i32| if x < 0 { Err("negative") } else { Ok(x * 2) }),
        );
        let sc = short_circuit(init_coro);

        let (initial, mut coro) = sc.init().unwrap_yielded();
        assert_eq!(initial, 42);
        assert_eq!(coro.next(5).unwrap_yielded(), 10);
        assert_eq!(coro.next(3).unwrap_yielded(), 6);
    }

    #[test]
    fn test_short_circuit_init_yields_err() {
        let init_coro = init(Err("initial error"), repeat(|x: i32| Ok(x * 2)));
        let sc = short_circuit(init_coro);

        assert_eq!(sc.init().unwrap_complete(), Err("initial error"));
    }

    #[test]
    fn test_short_circuit_init_completes_immediately() {
        use crate::build::from_fn;
        let coro = from_fn(|_: i32| Step::Complete::<Result<i32, &str>, i32>(100));
        let init_coro = (Ok(42), coro);
        let sc = short_circuit(init_coro);

        let (initial, mut coro) = sc.init().unwrap_yielded();
        assert_eq!(initial, 42);
        assert_eq!(coro.next(0).unwrap_complete(), Ok(100));
    }

    #[test]
    fn test_ok_then_init_first_yields() {
        use crate::build::from_fn;
        let mut called = false;
        let first_coro = from_fn(move |x: i32| -> Step<i32, Result<i32, &str>> {
            if !called {
                called = true;
                Step::Yielded(x * 2)
            } else {
                Step::Complete(Ok(x))
            }
        });
        let init_first = (10, first_coro);
        let mapped = OkMap {
            state: Sequence::OnFirst(
                init_first,
                Some(|val| init(val, repeat(move |x: i32| x + val))),
            ),
        };

        let (initial, mut coro) = mapped.init().unwrap_yielded();
        assert_eq!(initial, 10);

        // First coroutine continues and yields 5*2=10
        assert_eq!(coro.next(5).unwrap_yielded(), 10);

        // First completes with Ok(0), second coroutine starts with val=0
        assert_eq!(coro.next(0).unwrap_yielded(), 0);

        // Second coroutine continues: 3 + 0
        assert_eq!(coro.next(3).unwrap_yielded(), 3);
    }

    #[test]
    fn test_ok_then_init_first_completes_ok_second_yields() {
        use crate::build::from_fn;
        let first = (
            10,
            from_fn(|_: i32| Step::Complete::<i32, Result<i32, &str>>(Ok(20))),
        );
        let mapped = OkMap {
            state: Sequence::OnFirst(
                first,
                Some(|val| init(val * 2, repeat(move |x: i32| x + val))),
            ),
        };

        let (initial, mut coro) = mapped.init().unwrap_yielded();
        assert_eq!(initial, 10);

        // First completes with Ok(20), second coroutine inits with (40, ...)
        assert_eq!(coro.next(0).unwrap_yielded(), 40);

        // Second coroutine continues: 5 + 20
        assert_eq!(coro.next(5).unwrap_yielded(), 25);
    }

    #[test]
    fn test_ok_then_init_first_completes_err() {
        use crate::build::from_fn;
        let first = (
            10,
            from_fn(|_: i32| Step::Complete::<i32, Result<i32, &str>>(Err("error"))),
        );
        let mapped = OkMap {
            state: Sequence::OnFirst(first, Some(|val| init(val, repeat(move |x: i32| x + val)))),
        };

        let (initial, mut coro) = mapped.init().unwrap_yielded();
        assert_eq!(initial, 10);

        assert_eq!(coro.next(0).unwrap_complete(), Err("error"));
    }

    #[test]
    fn test_ok_and_then_init_first_yields() {
        use crate::build::from_fn;
        let mut called = false;
        let first_coro = from_fn(
            move |x: i32| -> Step<Result<i32, &str>, Result<i32, &str>> {
                if !called {
                    called = true;
                    Step::Yielded(Ok(x * 2))
                } else {
                    Step::Complete(Ok(x))
                }
            },
        );
        let first = (Ok(10), first_coro);
        let chained = OkAndThen {
            state: Sequence::OnFirst(
                first,
                Some(|val| {
                    init(
                        Ok(val),
                        from_fn(
                            move |x: i32| -> Step<Result<i32, &str>, Result<i32, &str>> {
                                Step::Yielded(Ok(x + val))
                            },
                        ),
                    )
                }),
            ),
        };

        let (initial, mut coro) = chained.init().unwrap_yielded();
        assert_eq!(initial, Ok(10));

        // First coroutine continues and yields 5*2=10
        assert_eq!(coro.next(5).unwrap_yielded(), Ok(10));

        // First completes with Ok(0), second coroutine starts with Ok(0)
        assert_eq!(coro.next(0).unwrap_yielded(), Ok(0));

        // Second coroutine continues: 3 + 0
        assert_eq!(coro.next(3).unwrap_yielded(), Ok(3));
    }

    #[test]
    fn test_ok_and_then_init_first_completes_ok_second_yields() {
        use crate::build::from_fn;
        let first = (
            Ok(10),
            from_fn(|_: i32| Step::Complete::<Result<i32, &str>, Result<i32, &str>>(Ok(20))),
        );
        let chained = OkAndThen {
            state: Sequence::OnFirst(
                first,
                Some(|val| {
                    init(
                        Ok(val * 2),
                        from_fn(
                            move |x: i32| -> Step<Result<i32, &str>, Result<i32, &str>> {
                                Step::Yielded(Ok(x + val))
                            },
                        ),
                    )
                }),
            ),
        };

        let (initial, mut coro) = chained.init().unwrap_yielded();
        assert_eq!(initial, Ok(10));

        // First completes with Ok(20), second inits with Ok(40)
        assert_eq!(coro.next(0).unwrap_yielded(), Ok(40));

        // Second coroutine continues: 5 + 20
        assert_eq!(coro.next(5).unwrap_yielded(), Ok(25));
    }

    #[test]
    fn test_ok_and_then_init_first_completes_err() {
        use crate::build::from_fn;
        let first = (
            Ok(10),
            from_fn(|_: i32| Step::Complete::<Result<i32, &str>, Result<i32, &str>>(Err("error"))),
        );
        let chained = OkAndThen {
            state: Sequence::OnFirst(
                first,
                Some(|val| {
                    init(
                        Ok(val),
                        from_fn(
                            move |x: i32| -> Step<Result<i32, &str>, Result<i32, &str>> {
                                Step::Yielded(Ok(x + val))
                            },
                        ),
                    )
                }),
            ),
        };

        let (initial, mut coro) = chained.init().unwrap_yielded();
        assert_eq!(initial, Ok(10));

        assert_eq!(coro.next(0).unwrap_complete(), Err("error"));
    }

    #[test]
    fn test_ok_chain_init_first_yields() {
        use crate::build::from_fn;
        let mut called = false;
        let first_coro = from_fn(move |x: i32| -> Step<i32, Result<i32, &str>> {
            if !called {
                called = true;
                Step::Yielded(x * 2)
            } else {
                Step::Complete(Ok(x))
            }
        });
        let first = (10, first_coro);
        let second = repeat(|x: i32| x + 1);
        let chained = OkChain {
            coro: Some(first),
            next: second,
        };

        let (initial, mut coro) = chained.init().unwrap_yielded();
        assert_eq!(initial, 10);

        // First coroutine continues and yields 5*2=10
        assert_eq!(coro.next(5).unwrap_yielded(), 10);

        // First completes with Ok(0), second coroutine starts with 0
        assert_eq!(coro.next(0).unwrap_yielded(), 1);
    }

    #[test]
    fn test_ok_chain_init_first_completes_ok_second_yields() {
        use crate::build::from_fn;
        let first = (
            10,
            from_fn(|_: i32| Step::Complete::<i32, Result<i32, &str>>(Ok(20))),
        );
        let second = repeat(|x: i32| x + 1);
        let chained = OkChain {
            coro: Some(first),
            next: second,
        };

        let (initial, mut coro) = chained.init().unwrap_yielded();
        assert_eq!(initial, 10);

        // First completes with Ok(20), second starts with 20
        assert_eq!(coro.next(0).unwrap_yielded(), 21);
        assert_eq!(coro.next(50).unwrap_yielded(), 51);
    }

    #[test]
    fn test_ok_chain_init_first_completes_err() {
        use crate::build::from_fn;
        let first = (
            10,
            from_fn(|_: i32| Step::Complete::<i32, Result<i32, &str>>(Err("error"))),
        );
        let second = repeat(|x: i32| x + 1);
        let chained = OkChain {
            coro: Some(first),
            next: second,
        };

        let (initial, mut coro) = chained.init().unwrap_yielded();
        assert_eq!(initial, 10);

        assert_eq!(coro.next(0).unwrap_complete(), Err("error"));
    }

    #[test]
    fn test_ok_chain_init_both_complete_immediately() {
        use crate::build::from_fn;
        let first = (
            10,
            from_fn(|_: i32| Step::Complete::<i32, Result<i32, &str>>(Ok(20))),
        );
        let second = from_fn(|x: i32| Step::Complete(x * 2));
        let chained = OkChain {
            coro: Some(first),
            next: second,
        };

        let (initial, mut coro) = chained.init().unwrap_yielded();
        assert_eq!(initial, 10);

        // First completes with Ok(20), second completes with 40
        assert_eq!(coro.next(0).unwrap_complete(), Ok(40));
    }

    #[test]
    fn test_flatten_init_yields_outer_err() {
        use crate::build::from_fn;
        let coro = from_fn(|_: i32| {
            Step::Complete::<Result<Result<i32, &str>, &str>, Result<Result<i32, &str>, &str>>(
                Err::<Result<i32, &str>, &str>("outer error"),
            )
        });
        let mut flat = flatten(coro);

        assert_eq!(flat.next(0).unwrap_complete(), Err("outer error"));
    }

    #[test]
    fn test_flatten_init_yields_inner_err() {
        use crate::build::from_fn;
        let coro = from_fn(|_: i32| {
            Step::Complete::<Result<Result<i32, &str>, &str>, Result<Result<i32, &str>, &str>>(Ok(
                Err::<i32, &str>("inner error"),
            ))
        });
        let mut flat = flatten(coro);

        assert_eq!(flat.next(0).unwrap_complete(), Err("inner error"));
    }

    #[test]
    fn test_flatten_init_completes_ok_ok() {
        use crate::build::from_fn;
        let coro = from_fn(|_: i32| {
            Step::Complete::<Result<Result<i32, &str>, &str>, Result<Result<i32, &str>, &str>>(Ok(
                Ok::<i32, &str>(100),
            ))
        });
        let init_coro = (Ok(Ok::<i32, &str>(42)), coro);
        let flat = flatten(init_coro);

        let (initial, mut coro) = flat.init().unwrap_yielded();
        assert_eq!(initial, Ok(Ok(42)));
        assert_eq!(coro.next(0).unwrap_complete(), Ok(100));
    }

    #[test]
    fn test_flatten_init_completes_ok_err() {
        use crate::build::from_fn;
        let coro = from_fn(|_: i32| {
            Step::Complete::<Result<Result<i32, &str>, &str>, Result<Result<i32, &str>, &str>>(Ok(
                Err::<i32, &str>("inner error"),
            ))
        });
        let init_coro = (Ok(Ok::<i32, &str>(42)), coro);
        let flat = flatten(init_coro);

        let (initial, mut coro) = flat.init().unwrap_yielded();
        assert_eq!(initial, Ok(Ok(42)));
        assert_eq!(coro.next(0).unwrap_complete(), Err("inner error"));
    }

    #[test]
    fn test_flatten_init_completes_err() {
        use crate::build::from_fn;
        let coro = from_fn(|_: i32| {
            Step::Complete::<Result<Result<i32, &str>, &str>, Result<Result<i32, &str>, &str>>(
                Err::<Result<i32, &str>, &str>("outer error"),
            )
        });
        let init_coro = (Ok::<Result<i32, &str>, &str>(Ok::<i32, &str>(42)), coro);
        let flat = flatten(init_coro);

        let (initial, mut coro) = flat.init().unwrap_yielded();
        assert_eq!(initial, Ok(Ok(42)));
        assert_eq!(coro.next(0).unwrap_complete(), Err("outer error"));
    }

    // Extension trait tests

    #[test]
    fn test_try_sans_ok_then() {
        use crate::build::from_fn;
        use crate::result::TrySans;

        let mut called = false;
        let coro = from_fn(move |x: i32| -> Step<i32, Result<i32, &str>> {
            if !called {
                called = true;
                Step::Yielded(x * 2)
            } else {
                Step::Complete(Ok(x))
            }
        });

        let mut mapped = coro.ok_then(|val| init(val, repeat(move |x: i32| x + val)));

        assert_eq!(mapped.next(5).unwrap_yielded(), 10);
        assert_eq!(mapped.next(3).unwrap_yielded(), 3);
        assert_eq!(mapped.next(7).unwrap_yielded(), 10);
    }

    #[test]
    fn test_try_sans_ok_and_then() {
        use crate::build::from_fn;
        use crate::result::TrySans;

        let mut called = false;
        let coro = from_fn(
            move |x: i32| -> Step<Result<i32, &str>, Result<i32, &str>> {
                if !called {
                    called = true;
                    Step::Yielded(Ok(x * 2))
                } else {
                    Step::Complete(Ok(x))
                }
            },
        );

        let mut chained = coro.ok_and_then(|val| {
            let mut inner_called = false;
            init(
                Ok(val),
                from_fn(
                    move |x: i32| -> Step<Result<i32, &str>, Result<i32, &str>> {
                        if !inner_called {
                            inner_called = true;
                            Step::Yielded(Ok(x + val))
                        } else {
                            Step::Complete(Ok(x))
                        }
                    },
                ),
            )
        });

        assert_eq!(chained.next(5).unwrap_yielded(), Ok(10));
        assert_eq!(chained.next(3).unwrap_yielded(), Ok(3));
        assert_eq!(chained.next(7).unwrap_yielded(), Ok(10));
    }

    #[test]
    fn test_try_sans_ok_chain() {
        use crate::build::from_fn;
        use crate::result::TrySans;

        let mut called = false;
        let first = from_fn(move |x: i32| -> Step<i32, Result<i32, &str>> {
            if !called {
                called = true;
                Step::Yielded(x * 2)
            } else {
                Step::Complete(Ok(x))
            }
        });
        let second = repeat(|x: i32| x + 1);

        let mut chained = first.ok_chain(second);

        assert_eq!(chained.next(5).unwrap_yielded(), 10);
        assert_eq!(chained.next(3).unwrap_yielded(), 4);
        assert_eq!(chained.next(10).unwrap_yielded(), 11);
    }

    #[test]
    fn test_try_sans_flatten() {
        use crate::build::from_fn;
        use crate::result::TrySans;

        let mut called = false;
        #[allow(clippy::type_complexity)]
        let coro = from_fn(move |x: i32| -> Step<
            Result<Result<i32, &str>, &str>,
            Result<Result<i32, &str>, &str>,
        > {
            if !called {
                called = true;
                Step::Yielded(Ok(Ok(x * 2)))
            } else {
                Step::Complete(Ok(Ok(x)))
            }
        });

        let mut flattened = coro.flatten();

        assert_eq!(flattened.next(5).unwrap_yielded(), Ok(Ok(10)));
        assert_eq!(flattened.next(10).unwrap_complete(), Ok(10));
    }

    #[test]
    fn test_try_init_sans_ok_then() {
        use crate::build::from_fn;
        use crate::result::TryInitSans;

        let mut called = false;
        let first_coro = from_fn(move |x: i32| -> Step<i32, Result<i32, &str>> {
            if !called {
                called = true;
                Step::Yielded(x * 2)
            } else {
                Step::Complete(Ok(x))
            }
        });
        let init_first = (10, first_coro);
        let mapped = init_first.ok_then(|val| init(val, repeat(move |x: i32| x + val)));

        let (initial, mut coro) = mapped.init().unwrap_yielded();
        assert_eq!(initial, 10);

        assert_eq!(coro.next(5).unwrap_yielded(), 10);
        assert_eq!(coro.next(0).unwrap_yielded(), 0);
        assert_eq!(coro.next(3).unwrap_yielded(), 3);
    }

    #[test]
    fn test_try_init_sans_flatten() {
        use crate::build::from_fn;
        use crate::result::TryInitSans;

        let coro = from_fn(|_: i32| {
            Step::Complete::<Result<Result<i32, &str>, &str>, Result<Result<i32, &str>, &str>>(Ok(
                Ok::<i32, &str>(100),
            ))
        });
        let init_coro = (Ok(Ok::<i32, &str>(42)), coro);
        let flat = init_coro.flatten();

        let (initial, mut coro) = flat.init().unwrap_yielded();
        assert_eq!(initial, Ok(Ok(42)));
        assert_eq!(coro.next(0).unwrap_complete(), Ok(100));
    }
}
