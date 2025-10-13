#![forbid(unsafe_code)]
//! # Sans: Composable Coroutine-Based Programming
//!
//! Build composable computations that can yield intermediate values and be driven
//! to completion by external input.
//!
//! ## Core Traits
//!
//! - **[`Sans<I, O>`]**: Stateful computations that process input and yield values
//! - **[`InitSans<I, O>`]**: Computations that provide initial output before processing input
//!
//! ## Key Features
//!
//! - **Composable**: Chain coroutines together with `.chain()`
//! - **Transformable**: Use `.map_input()`, `.map_yield()`, `.map_done()`
//! - **Async Support**: Both sync and async execution with `handle()` and `handle_async()`
//!
//! ## Example
//!
//! ```
//! use sans::prelude::*;
//!
//! // Build a pipeline that yields initial value, processes input, then finishes
//! let pipeline = init_once(10, |x: i32| x * 2)  // Yields 10, then multiplies input by 2
//!     .chain(once(|x: i32| x + 1));              // Adds 1 to input, then completes
//!
//! // Drive the pipeline with responses to each yield
//! let result = handle(pipeline, |output| output + 5);
//! ```
//!
//! ## Module Organization
//!
//! This library is organized by capability:
//!
//! - **[`build`]** - Creating new coroutines
//! - **[`compose`]** - Chaining and transforming coroutines
//! - **[`result`]** - Result combinators for error handling in coroutines
//! - **[`poll`]** - Universal adapter implementing both [`Sans`] and [`InitSans`] for bridging APIs
//! - **[`concurrent`]** - Running multiple coroutines concurrently
//! - **[`sequential`]** - Running coroutines one after another
//! - **[`run`]** - Executing coroutine pipelines
//! - **[`iter`]** - Iterator adapters for [`Sans<(), O>`] and [`InitSans<(), O>`]
//! - **[`prelude`]** - Common imports for quick start
//!
//! ## Common Functions
//!
//! **Building Coroutines:**
//! - [`once(f)`](build::once) - Apply function once, then complete
//! - [`repeat(f)`](build::repeat) - Apply function repeatedly
//! - [`init(value, coroutine)`](build::init) - Wrap a `Sans` with an initial output (for `and_then`)
//! - [`init_once(value, f)`](build::init_once) - Yield initial value, then apply function once
//! - [`chain(a, b)`](compose::chain) - Run coroutine `a` to completion, then run coroutine `b`
//!
//! **Execution:**
//! - [`handle(coroutine, responder)`](run::handle) - Drive computation with sync responses
//! - [`handle_async(coroutine, responder)`](run::handle_async) - Drive computation with async responses

// Core modules (essential types)
mod init;
mod sans;
mod step;

// Capability modules
pub mod build;
pub mod compose;
pub mod concurrent;
pub mod iter;
pub mod poll;
pub mod result;
pub mod run;
pub mod sequential;

// Convenience
pub mod prelude;

// Re-export essential types at root
pub use init::InitSans;
pub use sans::{PoisonError, Sans};
pub use step::Step;
