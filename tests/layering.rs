//! Verify the public owned-state and fallible-runner APIs together.

use sans::prelude::*;
use std::cell::Cell;
use std::collections::HashMap;
use std::future::Future;
use std::task::{Context, Waker};

type Response = (usize, String);
type Collected = Result<Vec<String>, &'static str>;

fn collector(
    finished: &Cell<usize>,
    core_error: bool,
) -> impl InitSans<Response, usize, Next: Sans<Response, usize, Return = Collected>> + '_ {
    let resolver = from_fn(
        move |id: usize| -> Step<usize, Result<Vec<usize>, &'static str>> {
            if id == 1 {
                Step::Yielded(2)
            } else if core_error {
                Step::Complete(Err("resolver failed"))
            } else {
                Step::Complete(Ok(vec![1, 2]))
            }
        },
    );
    init(1, resolver).with_state(
        HashMap::new(),
        |documents, (id, text): Response| {
            documents.insert(id, text);
            id
        },
        |mut documents, result| {
            finished.set(finished.get() + 1);
            result?
                .into_iter()
                .map(|id| documents.remove(&id).ok_or("missing document"))
                .collect()
        },
    )
}

#[test]
fn collector_finishes_on_core_completion_and_preserves_core_errors() {
    let finished = Cell::new(0);
    assert_eq!(
        try_handle(collector(&finished, false), |id| Ok((id, id.to_string()))),
        Ok(vec![String::from("1"), String::from("2")])
    );
    assert_eq!(finished.get(), 1);
    assert_eq!(
        try_handle(collector(&finished, true), |id| Ok((id, id.to_string()))),
        Err("resolver failed")
    );
    assert_eq!(finished.get(), 2);
}

#[test]
fn responder_failure_after_a_cached_document_does_not_finish_collector() {
    let finished = Cell::new(0);
    assert_eq!(
        try_handle(collector(&finished, false), |id| match id {
            1 => Ok((id, String::from("first"))),
            _ => Err("fetch failed"),
        }),
        Err("fetch failed")
    );
    assert_eq!(finished.get(), 0);
}

#[test]
fn async_collector_success_and_cancellation_follow_the_same_finish_rules() {
    let finished = Cell::new(0);
    let mut context = Context::from_waker(Waker::noop());
    let mut run = Box::pin(try_handle_async(collector(&finished, false), |id| {
        std::future::ready(Ok((id, id.to_string())))
    }));
    assert_eq!(
        run.as_mut().poll(&mut context),
        std::task::Poll::Ready(Ok(vec![String::from("1"), String::from("2")]))
    );
    drop(run);
    assert_eq!(finished.get(), 1);

    let mut run = Box::pin(try_handle_async(collector(&finished, false), |_| {
        std::future::pending::<Result<Response, &'static str>>()
    }));
    assert!(run.as_mut().poll(&mut context).is_pending());
    drop(run);
    assert_eq!(finished.get(), 1);
}
