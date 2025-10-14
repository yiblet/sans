//! Integration test for the init builder API.
//!
//! This test demonstrates the full workflow of the builder API,
//! composing `init::yielding().then(...)`, `.and_then`, `poll::init_poll`,
//! and `run::handle` exclusively through the new builder API.

use sans::init::{ShortCircuit, yielding};
use sans::poll::{Poll, PollOutput, init_poll};
use sans::prelude::*;

#[test]
fn init_builder_pipeline() {
    // Build a pipeline using only the builder API
    // Stage 1: yield initial value, then multiply by 2 (repeat takes last input as return)
    let Yielded(initial, stage1) = yielding(10).then(from_fn(|x: i32| {
        if x < 3 {
            Step::Yielded(x * 2)
        } else {
            Step::Complete(x)
        }
    }));

    assert_eq!(initial, 10);

    // Stage 2: use and_then to create a dependent continuation
    let mut pipeline = stage1.and_then(|result| {
        // result is the completion value from stage1
        // Create a new initialization that depends on that result
        ShortCircuit::Pending(yielding(result * 10).then(repeat(move |x: i32| x + result)))
    });

    // Drive the pipeline
    // First, process through stage1
    assert_eq!(pipeline.next(1).unwrap_yielded(), 2); // 1 * 2
    assert_eq!(pipeline.next(2).unwrap_yielded(), 4); // 2 * 2

    // Stage1 completes with x=3, and_then creates stage2 with (3*10, ...) = (30, ...)
    // Stage2 yields 30
    assert_eq!(pipeline.next(3).unwrap_yielded(), 30);

    // Stage2 continues: x + result = 5 + 3 = 8
    assert_eq!(pipeline.next(5).unwrap_yielded(), 8);

    // Continue with more values
    assert_eq!(pipeline.next(10).unwrap_yielded(), 13); // 10 + 3
}

#[test]
fn init_builder_shortcircuit_pipeline() {
    // Test with shortcircuit that may complete early
    use sans::init::shortcircuit;

    // Create a pipeline that might short-circuit
    let maybe_pipeline = shortcircuit::<i32, i32, &'static str>().then(once(|x: i32| x * 2));

    match maybe_pipeline {
        ShortCircuit::Pending(mut sans) => {
            // Drive it normally
            assert_eq!(sans.next(5).unwrap_yielded(), 10);
            assert_eq!(sans.next(3).unwrap_complete(), 3);
        }
        ShortCircuit::Complete(_) => {
            panic!("Expected pending, got complete");
        }
    }

    // Test immediate completion
    let completed = shortcircuit::<i32, i32, &'static str>().returning::<()>("done early");

    match completed {
        ShortCircuit::Complete(msg) => {
            assert_eq!(msg, "done early");
        }
        ShortCircuit::Pending(_) => {
            panic!("Expected complete, got pending");
        }
    }
}

#[test]
fn init_builder_with_poll() {
    // Test the builder API with init_poll
    // Note: init_poll still uses the InitSans trait for compatibility,
    // but we can pass a tuple which implements InitSans
    let init_result = yielding(1).then(repeat(|x: i32| x + 1));
    let (initial, sans) = init_result.into(); // Convert Yielded to tuple

    assert_eq!(initial, 1);

    // Wrap in a pollable using the tuple (which implements InitSans)
    let mut pollable = init_poll((initial, sans));

    // Poll it
    match pollable.next(Poll::Poll) {
        Step::Yielded(PollOutput::Output(output)) => {
            assert_eq!(output, 1);
        }
        _ => panic!("Expected output"),
    }

    // Continue polling
    match pollable.next(Poll::Input(5)) {
        Step::Yielded(PollOutput::Output(output)) => {
            assert_eq!(output, 6); // 5 + 1
        }
        _ => panic!("Expected output"),
    }
}

#[test]
fn init_builder_complex_composition() {
    // Complex example: multi-stage pipeline with transformations

    // Stage 1: Initialize with a counter
    let counter_init = yielding(0).then(from_fn(|x: i32| {
        if x < 5 {
            Step::Yielded(x + 1)
        } else {
            Step::Complete(x)
        }
    }));

    // Stage 2: Transform the yields
    let transformed = counter_init.map_yield(|value| value * 10);

    // Stage 3: Transform the return
    let with_return = transformed.map_return(|ret| format!("final: {}", ret));

    // Extract and run
    let Yielded(initial, mut pipeline) = with_return;
    assert_eq!(initial, 0); // Initial value is 0

    assert_eq!(pipeline.next(0).unwrap_yielded(), 10); // (0 + 1) * 10
    assert_eq!(pipeline.next(1).unwrap_yielded(), 20); // (1 + 1) * 10
    assert_eq!(pipeline.next(2).unwrap_yielded(), 30); // (2 + 1) * 10
    assert_eq!(pipeline.next(3).unwrap_yielded(), 40); // (3 + 1) * 10
    assert_eq!(pipeline.next(4).unwrap_yielded(), 50); // (4 + 1) * 10
    assert_eq!(pipeline.next(5).unwrap_complete(), "final: 5"); // Complete with formatted return
}

#[test]
fn init_builder_chain_and_then() {
    // Test chaining and and_then together

    // First coroutine: yields once, completes with a value
    let first = once(|x: i32| x * 2);

    // Use and_then to create a second coroutine based on the return value
    let composed = first.and_then(|return_val| {
        // return_val is the completion value from first
        // Create a new initialization that uses this value
        ShortCircuit::Pending(yielding(return_val * 10).then(repeat(move |y: i32| y + return_val)))
    });

    // Extract the composed pipeline
    let mut pipeline = composed;

    // First coroutine: yields 5 * 2 = 10
    assert_eq!(pipeline.next(5).unwrap_yielded(), 10);

    // First coroutine completes with return_val = 7
    // Second coroutine initializes with (7 * 10, ...)
    // Yields 70
    assert_eq!(pipeline.next(7).unwrap_yielded(), 70);

    // Second coroutine continues: y + return_val = 3 + 7 = 10
    assert_eq!(pipeline.next(3).unwrap_yielded(), 10);

    // Continue with more inputs
    assert_eq!(pipeline.next(5).unwrap_yielded(), 12); // 5 + 7
}
