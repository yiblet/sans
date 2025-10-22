#![forbid(unsafe_code)]
//! # Sans: Composable Coroutine-Based Programming
//!
//! Build composable computations that can yield intermediate values and be driven
//! to completion by external input.
//!
//! ## Core Traits
//!
//! - **[`Sans<I, O>`]**: Stateful computations that process input and yield values
//!
//! ## Core Types
//!
//! - **[`Yielded<O, S>`](yielded::Yielded)**: Result of initialization that yields output before continuing
//! - **[`Step<Y, D>`]**: Result of a single coroutine step - either `Yielded(Y)` or `Complete(D)`
//!
//! ## Key Features
//!
//! - **Composable**: Chain coroutines together with `.chain()` and `.and_then()`
//! - **Transformable**: Use `.map_input()`, `.map_yield()`, `.map_return()`
//! - **Initial Output**: Produce values immediately with `Yielded`
//! - **Async Support**: Both sync and async execution with `handle()` and `handle_async()`
//!
//! ## Example
//!
//! ```
//! use sans::prelude::*;
//! use sans::handle::handle;
//!
//! // Build a pipeline with initial output
//! let init_result = Yielded(10, once(|x: i32| x + 1)  // Yields 10 initially, adds 1 to input
//!     .chain(once(|x: i32| x * 2)));                  // Multiplies by 2, then completes
//!
//! // Convert to tuple for compatibility with handle
//! let (initial, pipeline) = init_result.into();
//! assert_eq!(initial, 10);
//!
//! // Drive the pipeline with responses to each yield
//! let result = handle(pipeline, initial, |output| {
//!     // Respond to yields
//!     if output < 100 { output } else { 0 }
//! });
//! assert!(result >= 0); // Just verify it completes
//! ```
//!
//! ## Module Organization
//!
//! This library is organized by capability:
//!
//! - **[`build`]** - Creating new coroutines
//! - **[`yielded`]** - Initialization types for coroutines with initial output
//! - **[`compose`]** - Chaining and transforming coroutines
//! - **[`result`]** - Result combinators for error handling in coroutines
//! - **[`poll`]** - Universal polling adapter for bridging APIs
//! - **[`concurrent`]** - Running multiple coroutines concurrently
//! - **[`sequential`]** - Running coroutines one after another
//! - **[`run`]** - Executing coroutine pipelines
//! - **[`iter`]** - Iterator adapters for [`Sans<(), O>`]
//! - **[`prelude`]** - Common imports for quick start
//!
//! ## Common Functions
//!
//! **Building Coroutines:**
//! - [`once(f)`](build::once) - Apply function once, then complete
//! - [`repeat(f)`](build::repeat) - Apply function repeatedly
//! - [`from_fn(f)`](build::from_fn) - Create coroutine from closure returning `Step`
//! - [`chain(a, b)`](compose::chain) - Run coroutine `a` to completion, then run coroutine `b`
//!
//! **Initialization:**
//! - [`Yielded(value, sans)`](yielded::Yielded) - Create initialization with output
//!
//! **Execution:**
//! - [`handle(coroutine, responder)`](handle::handle) - Drive computation with sync responses
//! - [`handle_async(coroutine, responder)`](handle::handle_async) - Drive computation with async responses

// Core modules (essential types)
mod sans;
mod step;
pub mod yielded;

// Capability modules
pub mod build;
pub mod compose;
pub mod concurrent;
pub mod handle;
pub mod iter;
pub mod poll;
pub mod result;
pub mod sequential;

// Convenience
pub mod prelude;

// Re-export essential types at root
pub use sans::{PoisonError, Sans};
pub use step::Step;
