use crate::poll::{Poll, PollError, PollOutput, Pollable, init_poll, poll};
use crate::{InitSans, Sans, Step};

/// Initialize an array of coroutines and join their polling interfaces.
///
/// Initialization runs here; initial outputs and completions are buffered until
/// polled. See the [routing example](crate::concurrent).
pub fn init_join<const N: usize, I, O, S, T>(rest: [T; N]) -> Join<N, S, O, S::Return>
where
    T: InitSans<I, O, Next = S>,
    S: Sans<I, O>,
{
    let pollables = rest.map(|init_sans| init_poll(init_sans));

    Join {
        pollables,
        returns: std::array::from_fn(|_| None),
        scheduler: Scheduler::default(),
    }
}

/// Join an array of continuations, routing inputs and outputs by child ID.
///
/// Use [`init_join`] for initializers. See the [routing example](crate::concurrent)
/// and [`Join`] for completion behavior.
pub fn join<const N: usize, I, O, S>(rest: [S; N]) -> Join<N, S, O, S::Return>
where
    S: Sans<I, O>,
{
    Join {
        pollables: rest.map(|s| poll(s)),
        returns: std::array::from_fn(|_| None),
        scheduler: Scheduler::default(),
    }
}

/// Like [`join`], with a runtime-sized vector of continuations.
pub fn join_vec<I, O, S>(sans: Vec<S>) -> JoinVec<S, O, S::Return>
where
    S: Sans<I, O>,
{
    let len = sans.len();
    JoinVec {
        pollables: sans.into_iter().map(|s| poll(s)).collect(),
        returns: (0..len).map(|_| None).collect(),
        scheduler: Scheduler::default(),
    }
}

/// Like [`init_join`], with a runtime-sized vector of initializers.
pub fn init_join_vec<I, O, S, T>(inits: Vec<T>) -> JoinVec<S, O, S::Return>
where
    T: InitSans<I, O, Next = S>,
    S: Sans<I, O>,
{
    let len = inits.len();
    JoinVec {
        pollables: inits.into_iter().map(|init| init_poll(init)).collect(),
        returns: (0..len).map(|_| None).collect(),
        scheduler: Scheduler::default(),
    }
}

/// An array of coroutines driven through one polling interface.
///
/// Construct with [`join`] or [`init_join`]. Polls check unfinished children in
/// round-robin order; inputs go to the child named by their [`JoinEnvelope`].
///
/// The join completes after every child completes, returning values in array order.
/// A child's `Err` return is stored like any other value. Only a [`JoinError`]
/// ends the join early: an invalid child ID or input sent to a completed child.
/// After the whole join completes or fails, later calls return
/// [`JoinError::AlreadyComplete`].
pub struct Join<const N: usize, S, O, R> {
    pollables: [Pollable<S, O, R>; N],
    returns: [Option<R>; N],
    scheduler: Scheduler,
}

/// A runtime-sized version of [`Join`] with the same polling and error behavior.
///
/// Construct with [`join_vec`] or [`init_join_vec`]. Returns child values in vector order.
pub struct JoinVec<S, O, R> {
    pollables: Vec<Pollable<S, O, R>>,
    returns: Vec<Option<R>>,
    scheduler: Scheduler,
}

// Array and vector joins share scheduling, but keep their own storage and return types.
#[derive(Default)]
struct Scheduler {
    last_index: usize,
    complete: usize,
    closed: bool,
}

impl Scheduler {
    fn next<I, O, S>(
        &mut self,
        pollables: &mut [Pollable<S, O, S::Return>],
        returns: &mut [Option<S::Return>],
        input: Poll<JoinEnvelope<I>>,
    ) -> Step<PollOutput<JoinEnvelope<I>, JoinEnvelope<O>>, Result<(), JoinError>>
    where
        S: Sans<I, O>,
    {
        if self.closed {
            return Step::Complete(Err(JoinError::AlreadyComplete));
        }
        let result = match input {
            Poll::Poll => {
                let len = pollables.len();
                let mut idx = self.last_index;
                for _ in 0..len {
                    idx = if idx + 1 == len { 0 } else { idx + 1 };
                    if returns[idx].is_some() {
                        continue;
                    }
                    match pollables[idx].next(Poll::Poll) {
                        Step::Yielded(PollOutput::Output(output)) => {
                            self.last_index = idx;
                            return Step::Yielded(PollOutput::Output(JoinEnvelope::new(
                                idx, output,
                            )));
                        }
                        Step::Yielded(PollOutput::NeedsInput | PollOutput::NeedsPoll(_)) => {}
                        Step::Complete(Ok(value)) => {
                            returns[idx] = Some(value);
                            self.complete += 1;
                        }
                        Step::Complete(Err(error)) => {
                            self.closed = true;
                            return Step::Complete(Err(JoinError::PollableFailed(
                                JoinId::new(idx),
                                error,
                            )));
                        }
                    }
                }
                if self.complete == len {
                    Step::Complete(Ok(()))
                } else {
                    Step::Yielded(PollOutput::NeedsInput)
                }
            }
            Poll::Input(JoinEnvelope(id, input)) => {
                let idx = id.as_usize();
                match pollables.get_mut(idx) {
                    None => Step::Complete(Err(JoinError::InvalidIndex(id))),
                    Some(pollable) => match pollable.next(Poll::Input(input)) {
                        Step::Yielded(PollOutput::Output(output)) => {
                            Step::Yielded(PollOutput::Output(JoinEnvelope(id, output)))
                        }
                        Step::Yielded(PollOutput::NeedsPoll(input)) => {
                            Step::Yielded(PollOutput::NeedsPoll(JoinEnvelope(id, input)))
                        }
                        Step::Yielded(PollOutput::NeedsInput) => {
                            Step::Yielded(PollOutput::NeedsInput)
                        }
                        Step::Complete(Ok(value)) => {
                            returns[idx] = Some(value);
                            self.complete += 1;
                            if self.complete == pollables.len() {
                                Step::Complete(Ok(()))
                            } else {
                                Step::Yielded(PollOutput::NeedsInput)
                            }
                        }
                        Step::Complete(Err(error)) => {
                            Step::Complete(Err(JoinError::PollableFailed(id, error)))
                        }
                    },
                }
            }
        };
        if matches!(result, Step::Complete(_)) {
            self.closed = true;
        }
        result
    }
}

/// A join protocol error, separate from child return values.
#[derive(Debug)]
pub enum JoinError {
    /// A child rejected an operation, such as input after completion.
    PollableFailed(JoinId, PollError),
    /// The requested coroutine index is outside the join.
    InvalidIndex(JoinId),
    /// The join already returned a completion or error.
    AlreadyComplete,
}

impl std::fmt::Display for JoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinError::InvalidIndex(id) => write!(f, "invalid join index {}", id.as_usize()),
            JoinError::AlreadyComplete => write!(f, "join already complete"),
            JoinError::PollableFailed(id, err) => {
                write!(f, "pollable at index {} failed: {}", id.as_usize(), err)
            }
        }
    }
}

impl std::error::Error for JoinError {}

/// A child identifier carried by [`JoinEnvelope`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JoinId(usize);

impl JoinId {
    pub(crate) fn new(index: usize) -> Self {
        JoinId(index)
    }

    pub(crate) fn as_usize(&self) -> usize {
        self.0
    }
}

/// A child ID and value used to route join inputs and outputs.
///
/// Use [`map`](Self::map) to build a response for the same child.
/// [`value`](Self::value) and `Deref` borrow the inner value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JoinEnvelope<T>(pub JoinId, pub T);

impl<T> JoinEnvelope<T> {
    /// Creates a new `JoinEnvelope` with the given index and value.
    pub fn new(index: usize, value: T) -> Self {
        JoinEnvelope(JoinId::new(index), value)
    }

    /// Returns a reference to the wrapped value.
    pub fn value(&self) -> &T {
        &self.1
    }

    /// Transform the value while preserving its child ID.
    ///
    /// See the [routing example](crate::concurrent).
    pub fn map<U, F>(self, f: F) -> JoinEnvelope<U>
    where
        F: FnOnce(T) -> U,
    {
        JoinEnvelope(self.0, f(self.1))
    }
}

impl<T> std::ops::Deref for JoinEnvelope<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.1
    }
}

impl<const N: usize, I, O, S>
    Sans<Poll<JoinEnvelope<I>>, PollOutput<JoinEnvelope<I>, JoinEnvelope<O>>>
    for Join<N, S, O, S::Return>
where
    S: Sans<I, O>,
{
    type Return = Result<[S::Return; N], JoinError>;

    fn next(
        &mut self,
        input: Poll<JoinEnvelope<I>>,
    ) -> Step<PollOutput<JoinEnvelope<I>, JoinEnvelope<O>>, Self::Return> {
        match self
            .scheduler
            .next(&mut self.pollables, &mut self.returns, input)
        {
            Step::Yielded(output) => Step::Yielded(output),
            Step::Complete(Ok(())) => Step::Complete(Ok(std::array::from_fn(|i| {
                self.returns[i]
                    .take()
                    .expect("completed child has a return value")
            }))),
            Step::Complete(Err(error)) => Step::Complete(Err(error)),
        }
    }
}

// Implement Sans for JoinVec
impl<I, O, S> Sans<Poll<JoinEnvelope<I>>, PollOutput<JoinEnvelope<I>, JoinEnvelope<O>>>
    for JoinVec<S, O, S::Return>
where
    S: Sans<I, O>,
{
    type Return = Result<Vec<S::Return>, JoinError>;

    fn next(
        &mut self,
        input: Poll<JoinEnvelope<I>>,
    ) -> Step<PollOutput<JoinEnvelope<I>, JoinEnvelope<O>>, Self::Return> {
        match self
            .scheduler
            .next(&mut self.pollables, &mut self.returns, input)
        {
            Step::Yielded(output) => Step::Yielded(output),
            Step::Complete(Ok(())) => Step::Complete(Ok(self
                .returns
                .iter_mut()
                .map(|value| value.take().expect("completed child has a return value"))
                .collect())),
            Step::Complete(Err(error)) => Step::Complete(Err(error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build::{once, repeat};

    // Identical lifecycle scenarios exercise both storage implementations.
    struct Worker;

    impl Sans<i32, i32> for Worker {
        type Return = i32;
        fn next(&mut self, input: i32) -> Step<i32, i32> {
            Step::Complete(input)
        }
    }

    enum Start {
        Output(i32),
        Complete(i32),
    }

    impl InitSans<i32, i32> for Start {
        type Next = Worker;
        fn init(self) -> Step<(i32, Worker), i32> {
            match self {
                Self::Output(value) => Step::Yielded((value, Worker)),
                Self::Complete(value) => Step::Complete(value),
            }
        }
    }

    fn assert_closed<G, R>(joined: &mut G)
    where
        G: Sans<
                Poll<JoinEnvelope<i32>>,
                PollOutput<JoinEnvelope<i32>, JoinEnvelope<i32>>,
                Return = Result<R, JoinError>,
            >,
        R: std::fmt::Debug,
    {
        for input in [Poll::Poll, Poll::Input(JoinEnvelope::new(usize::MAX, 5))] {
            assert!(matches!(
                joined.next(input),
                Step::Complete(Err(JoinError::AlreadyComplete))
            ));
        }
    }

    fn routed_completion<G, R>(mut joined: G)
    where
        G: Sans<
                Poll<JoinEnvelope<i32>>,
                PollOutput<JoinEnvelope<i32>, JoinEnvelope<i32>>,
                Return = Result<R, JoinError>,
            >,
        R: AsRef<[i32]> + std::fmt::Debug,
    {
        assert!(matches!(
            joined.next(Poll::Input(JoinEnvelope::new(1, 20))),
            Step::Yielded(PollOutput::NeedsInput)
        ));
        // A completed child must never be polled again, including consecutive scans.
        for _ in 0..3 {
            assert!(matches!(
                joined.next(Poll::Poll),
                Step::Yielded(PollOutput::NeedsInput)
            ));
        }
        let result = joined
            .next(Poll::Input(JoinEnvelope::new(0, 10)))
            .expect_complete("all finished")
            .unwrap();
        assert_eq!(result.as_ref(), [10, 20]);
        assert_closed(&mut joined);
    }

    #[test]
    fn routed_completion_skips_finished_children_and_preserves_order() {
        routed_completion(join([Worker, Worker]));
        routed_completion(join_vec(vec![Worker, Worker]));
    }

    fn initializer_completion<G, R>(mut joined: G)
    where
        G: Sans<
                Poll<JoinEnvelope<i32>>,
                PollOutput<JoinEnvelope<i32>, JoinEnvelope<i32>>,
                Return = Result<R, JoinError>,
            >,
        R: AsRef<[i32]> + std::fmt::Debug,
    {
        // Pending initial output blocks input, preserving the supplied value.
        assert!(
            matches!(joined.next(Poll::Input(JoinEnvelope::new(0, 7))), Step::Yielded(PollOutput::NeedsPoll(JoinEnvelope(id, 7))) if id.as_usize() == 0)
        );
        assert!(
            matches!(joined.next(Poll::Poll), Step::Yielded(PollOutput::Output(JoinEnvelope(id, 100))) if id.as_usize() == 0)
        );
        assert!(matches!(
            joined.next(Poll::Poll),
            Step::Yielded(PollOutput::NeedsInput)
        ));
        let result = joined
            .next(Poll::Input(JoinEnvelope::new(0, 10)))
            .expect_complete("initializer and routed child finished")
            .unwrap();
        assert_eq!(result.as_ref(), [10, 20]);
        assert_closed(&mut joined);
    }

    #[test]
    fn initializer_completion_skips_finished_children() {
        initializer_completion(init_join([Start::Output(100), Start::Complete(20)]));
        initializer_completion(init_join_vec(vec![Start::Output(100), Start::Complete(20)]));
    }

    fn all_initially_complete<G, R>(mut joined: G)
    where
        G: Sans<
                Poll<JoinEnvelope<i32>>,
                PollOutput<JoinEnvelope<i32>, JoinEnvelope<i32>>,
                Return = Result<R, JoinError>,
            >,
        R: AsRef<[i32]> + std::fmt::Debug,
    {
        let result = joined
            .next(Poll::Poll)
            .expect_complete("all initially finished")
            .unwrap();
        assert_eq!(result.as_ref(), [10, 20]);
        assert_closed(&mut joined);
    }

    #[test]
    fn all_initializer_completions_preserve_order() {
        all_initially_complete(init_join([Start::Complete(10), Start::Complete(20)]));
        all_initially_complete(init_join_vec(vec![
            Start::Complete(10),
            Start::Complete(20),
        ]));
    }

    fn invalid_index<G, R>(mut joined: G, index: usize)
    where
        G: Sans<
                Poll<JoinEnvelope<i32>>,
                PollOutput<JoinEnvelope<i32>, JoinEnvelope<i32>>,
                Return = Result<R, JoinError>,
            >,
        R: std::fmt::Debug,
    {
        assert!(
            matches!(joined.next(Poll::Input(JoinEnvelope::new(index, 0))), Step::Complete(Err(JoinError::InvalidIndex(id))) if id.as_usize() == index)
        );
        assert_closed(&mut joined);
    }

    #[test]
    fn invalid_indices_and_empty_inputs_close_join() {
        for index in [1, usize::MAX] {
            invalid_index(join([Worker]), index);
            invalid_index(join_vec(vec![Worker]), index);
        }
        for index in [0, usize::MAX] {
            invalid_index(join::<0, i32, i32, Worker>([]), index);
            invalid_index(join_vec::<i32, i32, Worker>(vec![]), index);
        }
    }

    fn empty_poll<G, R>(mut joined: G)
    where
        G: Sans<
                Poll<JoinEnvelope<i32>>,
                PollOutput<JoinEnvelope<i32>, JoinEnvelope<i32>>,
                Return = Result<R, JoinError>,
            >,
        R: AsRef<[i32]> + std::fmt::Debug,
    {
        let result = joined
            .next(Poll::Poll)
            .expect_complete("empty join finished")
            .unwrap();
        assert!(result.as_ref().is_empty());
        assert_closed(&mut joined);
    }

    #[test]
    fn empty_poll_closes_join() {
        empty_poll(join::<0, i32, i32, Worker>([]));
        empty_poll(join_vec::<i32, i32, Worker>(vec![]));
    }

    fn completed_child_input<G, R>(mut joined: G)
    where
        G: Sans<
                Poll<JoinEnvelope<i32>>,
                PollOutput<JoinEnvelope<i32>, JoinEnvelope<i32>>,
                Return = Result<R, JoinError>,
            >,
        R: std::fmt::Debug,
    {
        joined
            .next(Poll::Input(JoinEnvelope::new(0, 10)))
            .expect_yielded("one child remains");
        assert!(
            matches!(joined.next(Poll::Input(JoinEnvelope::new(0, 99))), Step::Complete(Err(JoinError::PollableFailed(id, PollError::AlreadyComplete))) if id.as_usize() == 0)
        );
        assert_closed(&mut joined);
    }

    #[test]
    fn completed_child_input_reports_error_and_closes_join() {
        completed_child_input(join([Worker, Worker]));
        completed_child_input(join_vec(vec![Worker, Worker]));
    }

    fn initial_output_order<G, R>(mut joined: G)
    where
        G: Sans<
                Poll<JoinEnvelope<i32>>,
                PollOutput<JoinEnvelope<i32>, JoinEnvelope<i32>>,
                Return = Result<R, JoinError>,
            >,
        R: std::fmt::Debug,
    {
        for expected in [1, 2, 0] {
            assert!(
                matches!(joined.next(Poll::Poll), Step::Yielded(PollOutput::Output(JoinEnvelope(id, value))) if id.as_usize() == expected && value == expected as i32)
            );
        }
        assert!(matches!(
            joined.next(Poll::Poll),
            Step::Yielded(PollOutput::NeedsInput)
        ));
    }

    #[test]
    fn initial_outputs_keep_existing_round_robin_order() {
        initial_output_order(init_join([
            Start::Output(0),
            Start::Output(1),
            Start::Output(2),
        ]));
        initial_output_order(init_join_vec(vec![
            Start::Output(0),
            Start::Output(1),
            Start::Output(2),
        ]));
    }

    #[test]
    fn errors_have_display_messages() {
        assert_eq!(
            JoinError::InvalidIndex(JoinId::new(5)).to_string(),
            "invalid join index 5"
        );
        assert_eq!(
            JoinError::AlreadyComplete.to_string(),
            "join already complete"
        );
        assert_eq!(
            JoinError::PollableFailed(JoinId::new(2), PollError::AlreadyComplete).to_string(),
            "pollable at index 2 failed: already complete"
        );
    }

    #[test]
    fn test_join_two_sans_basic() {
        // Use the same function for both to have the same type
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let s1 = repeat(add_one);
        let s2 = repeat(add_one);
        let mut joined = join([s1, s2]);

        // Poll should indicate needs input
        match joined.next(Poll::Poll) {
            Step::Yielded(PollOutput::NeedsInput) => {}
            other => panic!("Expected NeedsInput, got {:?}", other),
        }

        // Send input to first sans
        match joined.next(Poll::Input(JoinEnvelope::new(0, 10))) {
            Step::Yielded(PollOutput::Output(JoinEnvelope(_, 11))) => {}
            other => panic!("Expected Output(JoinEnvelope(_, 11)), got {:?}", other),
        }

        // Send input to second sans
        match joined.next(Poll::Input(JoinEnvelope::new(1, 5))) {
            Step::Yielded(PollOutput::Output(JoinEnvelope(_, 6))) => {}
            other => panic!("Expected Output(JoinEnvelope(_, 6)), got {:?}", other),
        }
    }

    #[test]
    fn test_join_round_robin_polling() {
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let s1 = repeat(add_one);
        let s2 = repeat(add_one);
        let mut joined = join([s1, s2]);

        // Send inputs to both - they produce outputs directly (repeat always yields)
        match joined.next(Poll::Input(JoinEnvelope::new(0, 10))) {
            Step::Yielded(PollOutput::Output(JoinEnvelope(_, 11))) => {}
            other => panic!("Expected Output, got {:?}", other),
        }

        match joined.next(Poll::Input(JoinEnvelope::new(1, 5))) {
            Step::Yielded(PollOutput::Output(JoinEnvelope(_, 6))) => {}
            other => panic!("Expected Output, got {:?}", other),
        }

        // Send more inputs
        match joined.next(Poll::Input(JoinEnvelope::new(0, 20))) {
            Step::Yielded(PollOutput::Output(JoinEnvelope(_, 21))) => {}
            other => panic!("Expected Output, got {:?}", other),
        }
    }

    #[test]
    fn test_join_completion_single_sans() {
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let s1 = once(add_one);
        let mut joined = join([s1]);

        // Send input - once yields first
        match joined.next(Poll::Input(JoinEnvelope::new(0, 10))) {
            Step::Yielded(PollOutput::Output(JoinEnvelope(_, 11))) => {}
            other => panic!("Expected Output, got {:?}", other),
        }

        // Send another input to complete
        match joined.next(Poll::Input(JoinEnvelope::new(0, 99))) {
            Step::Complete(Ok([99])) => {}
            other => panic!("Expected Complete(Ok([99])), got {:?}", other),
        }
    }

    #[test]
    fn test_join_completion_multiple_sans() {
        fn process(x: i32) -> i32 {
            x + 1
        }
        let s1 = once(process);
        let s2 = once(process);
        let s3 = once(process);
        let mut joined = join([s1, s2, s3]);

        // Send inputs to all - they yield outputs first
        joined
            .next(Poll::Input(JoinEnvelope::new(0, 10)))
            .expect_yielded("should yield");
        joined
            .next(Poll::Input(JoinEnvelope::new(1, 5)))
            .expect_yielded("should yield");
        joined
            .next(Poll::Input(JoinEnvelope::new(2, 20)))
            .expect_yielded("should yield");

        // Send second inputs to complete each
        joined
            .next(Poll::Input(JoinEnvelope::new(0, 100)))
            .expect_yielded("should yield");
        joined
            .next(Poll::Input(JoinEnvelope::new(1, 200)))
            .expect_yielded("should yield");

        // Final completion
        match joined.next(Poll::Input(JoinEnvelope::new(2, 300))) {
            Step::Complete(Ok(results)) => {
                assert_eq!(results, [100, 200, 300]);
            }
            other => panic!("Expected Complete, got {:?}", other),
        }
    }

    #[test]
    fn test_join_out_of_order_completion() {
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let s1 = once(add_one);
        let s2 = once(add_one);
        let mut joined = join([s1, s2]);

        // Send inputs out of order - they yield first
        joined
            .next(Poll::Input(JoinEnvelope::new(1, 5)))
            .expect_yielded("should yield");
        joined
            .next(Poll::Input(JoinEnvelope::new(0, 10)))
            .expect_yielded("should yield");

        // Complete them
        joined
            .next(Poll::Input(JoinEnvelope::new(1, 100)))
            .expect_yielded("should yield");

        match joined.next(Poll::Input(JoinEnvelope::new(0, 200))) {
            Step::Complete(Ok(results)) => {
                assert_eq!(results, [200, 100]);
            }
            other => panic!("Expected Complete, got {:?}", other),
        }
    }

    #[test]
    fn test_join_interleaved_operations() {
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let s1 = repeat(add_one);
        let s2 = repeat(add_one);
        let mut joined = join([s1, s2]);

        // Interleave operations on both sans
        for i in 0..3 {
            joined
                .next(Poll::Input(JoinEnvelope::new(0, i)))
                .expect_yielded("should yield");
            joined
                .next(Poll::Input(JoinEnvelope::new(1, i)))
                .expect_yielded("should yield");
        }

        // Should still be running
        match joined.next(Poll::Poll) {
            Step::Yielded(_) => {}
            other => panic!("Expected Yielded, got {:?}", other),
        }
    }

    #[test]
    fn test_join_continuous_operation() {
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let s1 = repeat(add_one);
        let mut joined = join([s1]);

        // Send inputs continuously
        for i in 1..=5 {
            match joined.next(Poll::Input(JoinEnvelope::new(0, i))) {
                Step::Yielded(PollOutput::Output(JoinEnvelope(_, output))) => {
                    assert_eq!(output, i + 1);
                }
                other => panic!("Expected Output, got {:?}", other),
            }
        }
    }

    #[test]
    fn test_join_all_waiting() {
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let s1 = repeat(add_one);
        let s2 = repeat(add_one);
        let mut joined = join([s1, s2]);

        // Poll when all are waiting
        match joined.next(Poll::Poll) {
            Step::Yielded(PollOutput::NeedsInput) => {}
            other => panic!("Expected NeedsInput, got {:?}", other),
        }
    }

    #[test]
    fn test_join_poll_after_all_complete() {
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let s1 = once(add_one);
        let s2 = once(add_one);
        let mut joined = join([s1, s2]);

        // First inputs yield outputs
        joined
            .next(Poll::Input(JoinEnvelope::new(0, 10)))
            .expect_yielded("should yield");
        joined
            .next(Poll::Input(JoinEnvelope::new(1, 5)))
            .expect_yielded("should yield");

        // Complete both
        joined
            .next(Poll::Input(JoinEnvelope::new(0, 100)))
            .expect_yielded("should yield");

        // Last completion
        match joined.next(Poll::Input(JoinEnvelope::new(1, 200))) {
            Step::Complete(Ok(results)) => {
                assert_eq!(results, [100, 200]);
            }
            other => panic!("Expected Complete, got {:?}", other),
        }
    }

    #[test]
    fn test_init_join_basic() {
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let init1 = (100, repeat(add_one));
        let init2 = (200, repeat(add_one));

        let mut joined = init_join([init1, init2]);

        // Should have initial outputs available
        match joined.next(Poll::Poll) {
            Step::Yielded(PollOutput::Output(JoinEnvelope(id, val))) => {
                let idx = id.as_usize();
                assert!(idx == 0 || idx == 1);
                assert!(val == 100 || val == 200);
            }
            other => panic!("Expected Output, got {:?}", other),
        }
    }

    #[test]
    fn test_join_empty_array() {
        use crate::build;
        // Need a concrete type for empty array
        #[allow(clippy::type_complexity)]
        let joined: Join<0, build::Repeat<fn(i32) -> i32>, i32, i32> = join([]);

        // Should immediately complete with empty array
        let mut joined = joined;
        match joined.next(Poll::Poll) {
            Step::Complete(Ok([])) => {}
            other => panic!("Expected Complete(Ok([])), got {:?}", other),
        }
    }

    // Tests for JoinVec
    #[test]
    fn test_join_vec_basic() {
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let sans = vec![repeat(add_one), repeat(add_one), repeat(add_one)];
        let mut joined = join_vec(sans);

        // Poll should indicate needs input
        match joined.next(Poll::Poll) {
            Step::Yielded(PollOutput::NeedsInput) => {}
            other => panic!("Expected NeedsInput, got {:?}", other),
        }

        // Send input to each sans
        for i in 0..3 {
            let input_val = (i * 10) as i32;
            match joined.next(Poll::Input(JoinEnvelope::new(i, input_val))) {
                Step::Yielded(PollOutput::Output(JoinEnvelope(id, val))) => {
                    assert_eq!(id.as_usize(), i);
                    assert_eq!(val, input_val + 1);
                }
                other => panic!("Expected Output, got {:?}", other),
            }
        }
    }

    #[test]
    fn test_join_vec_completion() {
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let sans = vec![once(add_one), once(add_one)];
        let mut joined = join_vec(sans);

        // First inputs yield
        joined
            .next(Poll::Input(JoinEnvelope::new(0, 10)))
            .expect_yielded("should yield");
        joined
            .next(Poll::Input(JoinEnvelope::new(1, 20)))
            .expect_yielded("should yield");

        // Complete both
        joined
            .next(Poll::Input(JoinEnvelope::new(0, 100)))
            .expect_yielded("should yield");

        // Final completion
        match joined.next(Poll::Input(JoinEnvelope::new(1, 200))) {
            Step::Complete(Ok(results)) => {
                assert_eq!(results, vec![100, 200]);
            }
            other => panic!("Expected Complete, got {:?}", other),
        }
    }

    #[test]
    fn test_join_vec_empty() {
        use crate::build;
        #[allow(clippy::type_complexity)]
        let sans: Vec<build::Repeat<fn(i32) -> i32>> = vec![];
        let mut joined = join_vec(sans);

        // Should immediately complete with empty vec
        match joined.next(Poll::Poll) {
            Step::Complete(Ok(results)) => {
                assert_eq!(results, Vec::<i32>::new());
            }
            other => panic!("Expected Complete(Ok([])), got {:?}", other),
        }
    }

    #[test]
    fn test_join_vec_dynamic_size() {
        fn add_one(x: i32) -> i32 {
            x + 1
        }

        // Test with different sizes
        for size in 1..=10 {
            let sans: Vec<_> = (0..size).map(|_| repeat(add_one)).collect();
            let mut joined = join_vec(sans);

            // Send input to all
            for i in 0..size {
                let input_val = (i * 5) as i32;
                match joined.next(Poll::Input(JoinEnvelope::new(i, input_val))) {
                    Step::Yielded(PollOutput::Output(JoinEnvelope(id, val))) => {
                        assert_eq!(id.as_usize(), i);
                        assert_eq!(val, input_val + 1);
                    }
                    other => panic!("Expected Output at index {}, got {:?}", i, other),
                }
            }
        }
    }

    #[test]
    fn test_init_join_vec_basic() {
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let inits = vec![
            (100, repeat(add_one)),
            (200, repeat(add_one)),
            (300, repeat(add_one)),
        ];
        let mut joined = init_join_vec(inits);

        // Should have initial outputs available
        let mut found = [false, false, false];
        for _ in 0..3 {
            match joined.next(Poll::Poll) {
                Step::Yielded(PollOutput::Output(JoinEnvelope(id, val))) => {
                    let idx = id.as_usize();
                    assert!(idx < 3);
                    found[idx] = true;
                    assert!(val == 100 || val == 200 || val == 300);
                }
                other => panic!("Expected Output, got {:?}", other),
            }
        }
        assert!(
            found.iter().all(|&x| x),
            "All initial outputs should be found"
        );
    }

    #[test]
    fn test_join_vec_interleaved() {
        fn add_one(x: i32) -> i32 {
            x + 1
        }
        let sans = vec![repeat(add_one), repeat(add_one)];
        let mut joined = join_vec(sans);

        // Interleave operations
        for round in 0..3 {
            for idx in 0..2 {
                let input_val = (round * 10 + idx) as i32;
                match joined.next(Poll::Input(JoinEnvelope::new(idx, input_val))) {
                    Step::Yielded(PollOutput::Output(JoinEnvelope(id, val))) => {
                        assert_eq!(id.as_usize(), idx);
                        assert_eq!(val, input_val + 1);
                    }
                    other => panic!("Expected Output, got {:?}", other),
                }
            }
        }
    }

    #[test]
    fn test_join_vec_large_collection() {
        fn multiply_two(x: i32) -> i32 {
            x * 2
        }
        let sans: Vec<_> = (0..100).map(|_| repeat(multiply_two)).collect();
        let mut joined = join_vec(sans);

        // Send input to first 10
        for i in 0..10 {
            let input_val = i as i32;
            match joined.next(Poll::Input(JoinEnvelope::new(i, input_val))) {
                Step::Yielded(PollOutput::Output(JoinEnvelope(id, val))) => {
                    assert_eq!(id.as_usize(), i);
                    assert_eq!(val, input_val * 2);
                }
                other => panic!("Expected Output, got {:?}", other),
            }
        }
    }
}
