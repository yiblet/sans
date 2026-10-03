# sans, composable coroutine-based programming

[![LICENSE](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/yiblet/sans/blob/master/LICENSE)
[![Build Status](https://github.com/yiblet/sans/actions/workflows/ci.yml/badge.svg)](https://github.com/yiblet/sans/actions/workflows/ci.yml)
[![crates.io Version](https://img.shields.io/crates/v/sans.svg)](https://crates.io/crates/sans)
[![Minimum rustc version](https://img.shields.io/badge/rustc-1.85.0+-lightgray.svg)](#installation)

sans is a Rust library for composable, resumable computations. Build a pipeline
from small coroutines, then drive it one input at a time.

A coroutine keeps its state between inputs. Each step either yields an output
and pauses, or completes with a final result. The caller decides when to resume
the coroutine and supplies any I/O it needs. Adapters connect these steps and
transform their inputs, outputs, or return values.

<!-- toc -->

- [Example](#example)
- [Why sans?](#why-sans)
- [Installation](#installation)
- [Documentation](#documentation)

<!-- tocstop -->

## Example

This calculator keeps a running total across commands. `map_input` converts
each command to a numeric change, and `map_yield` formats the updated total.
Initialization produces the first output and a coroutine ready for input.

```rust
use sans::prelude::*;

let mut total = 0_i64;
let calculator = init_repeat(0_i64, move |delta: i64| {
    total += delta;
    total
})
.map_input(|cmd: &str| -> i64 {
    let mut parts = cmd.split_whitespace();
    let op = parts.next().expect("operation");
    let amount: i64 = parts.next().expect("amount").parse().expect("number");
    match op {
        "add" => amount,
        "sub" => -amount,
        _ => panic!("unknown operation"),
    }
})
.map_yield(|value: i64| format!("total={value}"));

let (initial, mut coro) = calculator.init().unwrap_yielded();
assert_eq!(initial, "total=0");
assert_eq!(coro.next("add 5").unwrap_yielded(), "total=5");
assert_eq!(coro.next("sub 3").unwrap_yielded(), "total=2");
assert_eq!(coro.next("add 10").unwrap_yielded(), "total=12");
```

The parser accepts commands such as `add 5` and `sub 3`; invalid commands panic.

For a complete pipeline, `chain` passes one coroutine's return value to the next
coroutine, while `map_return` transforms the final result. `handle` drives the
pipeline with a responder that turns each yielded output into the next input.
Here, the responder returns each output unchanged:

```rust
use sans::prelude::*;

let pipeline = init_once(10, |x: i32| x * 2)
    .map_yield(|x| x + 5)
    .chain(once(|x: i32| x * 3))
    .map_return(|x| format!("Result: {x}"));

let result = handle(pipeline, |output| output);
assert_eq!(result, "Result: 105");
```

When one layer needs a cache for both input conversion and completion, use
`with_state`. The input callback borrows the cache; the finish callback takes
ownership of it. This collector stores fetched documents while its inner core
tracks which document to request next:

```rust
use sans::prelude::*;
use std::collections::HashMap;

let resolver = from_fn(|id: usize| -> Step<usize, Result<Vec<usize>, &'static str>> {
    if id == 1 {
        Step::Yielded(2)
    } else {
        Step::Complete(Ok(vec![1, 2]))
    }
});
let collector = init(1, resolver).with_state(
    HashMap::new(),
    |documents, (id, text): (usize, String)| {
        documents.insert(id, text);
        id
    },
    |mut documents, result| {
        result?.into_iter()
            .map(|id| documents.remove(&id).ok_or("missing document"))
            .collect::<Result<Vec<_>, _>>()
    },
);

let documents = try_handle(collector, |id| {
    match id {
        1 => Ok((id, String::from("first"))),
        2 => Ok((id, String::from("second"))),
        _ => Err("fetch failed"),
    }
});
assert_eq!(documents, Ok(vec![String::from("first"), String::from("second")]));
```

`try_handle` stops on a core or responder error; both use the same error type.
Use `try_handle_async` for an async responder. The finish callback runs only when
the core completes; use `Drop` for cleanup when execution stops early.

## Why sans?

sans fits computations that need to preserve state while returning control to
their caller: interactive protocols, incremental computation, and data pipelines.
Keep the computation separate from the code that supplies input or performs I/O,
so the same coroutine can run with different responders.

This separation supports a pure core with an effectful harness around it. Keep
state and transitions in the coroutine, and let the harness handle I/O and other
effects. The same core can then run in synchronous or async contexts: `handle`
uses a synchronous responder, while `handle_async` awaits its responses.

- **Composition:** Build larger computations from small coroutines and familiar
  adapters such as `chain`, `map_input`, and `map_yield`. Use `with_state` when
  input conversion and completion need the same owned state.
- **Type safety:** Rust checks the input, output, and return types as you connect
  stages, including adapters that change those types.
- **Explicit control:** Resume a coroutine directly or use a runner to drive it.
  A final result ends the run; stop calling the coroutine after completion.
- **Concurrency:** Coordinate multiple coroutines with `join` and explicit polls.
  Your caller controls how their inputs and outputs reach external systems.
- **Safe Rust:** The library uses `#![forbid(unsafe_code)]`.

## Installation

Add sans to your `Cargo.toml`:

```toml
[dependencies]
sans = "0.1.0-alpha.4"
```

The prelude provides the core traits, common builders, adapters, and runners:

```rust
use sans::prelude::*;
```

**Requirements:** Rust 1.85 or later.

## Documentation

See the [API reference](https://docs.rs/sans) for types, methods, and examples.
Run `cargo doc --open` for documentation that matches your current checkout.

Start with [`Sans`](https://docs.rs/sans/latest/sans/trait.Sans.html),
[`InitSans`](https://docs.rs/sans/latest/sans/trait.InitSans.html), and
[`Step`](https://docs.rs/sans/latest/sans/enum.Step.html), then choose a module:

- [`build`](https://docs.rs/sans/latest/sans/build/): Create coroutines.
- [`compose`](https://docs.rs/sans/latest/sans/compose/): Connect coroutines and convert values.
- [`result`](https://docs.rs/sans/latest/sans/result/): Compose fallible coroutines.
- [`run`](https://docs.rs/sans/latest/sans/run/): Drive a coroutine with a responder.
- [`iter`](https://docs.rs/sans/latest/sans/iter/): Iterate over outputs.
- [`sequential`](https://docs.rs/sans/latest/sans/sequential/): Run a collection in sequence.
- [`poll`](https://docs.rs/sans/latest/sans/poll/) and
  [`concurrent`](https://docs.rs/sans/latest/sans/concurrent/): Poll and coordinate coroutines.

---

**License:** See [LICENSE](https://github.com/yiblet/sans/blob/master/LICENSE).
