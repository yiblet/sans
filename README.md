# sans, composable coroutine-based programming

[![LICENSE](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Build Status](https://github.com/yiblet/sans/actions/workflows/ci.yml/badge.svg)](https://github.com/yiblet/sans/actions/workflows/ci.yml)
[![crates.io Version](https://img.shields.io/crates/v/sans.svg)](https://crates.io/crates/sans)
[![Minimum rustc version](https://img.shields.io/badge/rustc-1.85.0+-lightgray.svg)](#rust-version-requirements-msrv)

sans is a coroutine combinators library for building composable, resumable computations in Rust. Build pipelines that yield, resume, and compose beautifully.

**What are coroutine combinators?** Instead of writing complex state machines with explicit state tracking, you compose small, focused functions into pipelines. Each coroutine can yield intermediate results, maintain state, and pass control to the next coroutine. The result is code that's easier to write, test, and reuse - all with compile-time type safety and zero-cost abstractions.

<!-- toc -->

- [Example](#example)
- [Why sans?](#why-sans)
- [Installation](#installation)
- [Documentation](#documentation)

<!-- tocstop -->

## Example

Interactive calculator that maintains state across inputs:

```rust
use sans::prelude::*;

// Build a stateful calculator that accumulates results
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
.map_yield(|value: i64| format!("total={}", value));

// Execute the pipeline
let (initial, mut coro) = calculator.init().unwrap_yielded();
println!("{}", initial);  // "total=0"

println!("{}", coro.next("add 5").unwrap_yielded());   // "total=5"
println!("{}", coro.next("sub 3").unwrap_yielded());   // "total=2"
println!("{}", coro.next("add 10").unwrap_yielded());  // "total=12"
```

Chained pipeline with transformation:

```rust
use sans::prelude::*;

let pipeline = init_once(10, |x: i32| x * 2)
    .map_yield(|x| x + 5)
    .chain(once(|x: i32| x * 3))
    .map_return(|x| format!("Result: {}", x));

let result = handle(pipeline, |output| {
    println!("Step: {}", output);
    output  // Pass through
});

println!("{}", result);  // "Result: 45"
```

## Why sans?

Build stateful, resumable pipelines for:

- **Interactive Protocols** - REPLs, network protocols, multi-step wizards
- **Streaming Pipelines** - Data processing, stream transformations, ETL
- **State Machines** - Protocol implementations, workflow engines, game AI
- **Incremental Computation** - Pausable async workflows, cooperative multitasking

**Key benefits:**
- **Composable** - Build complex pipelines from simple, reusable coroutines
- **Type-safe** - Compiler ensures coroutines compose correctly
- **Zero-cost** - No allocations in core combinators, stack-based state machines
- **Safe** - `#![forbid(unsafe_code)]`, no manual state management bugs
- **Concurrent** - Run multiple coroutines concurrently with `join`

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
sans = "0.1.0-alpha.2"
```

```rust
use sans::prelude::*;
```

**Requirements:** Rust 1.85+

## Documentation

**[Full API Documentation](https://docs.rs/sans)** - Complete reference for all types, traits, and functions

**Key modules:**
- `sans::build` - Create coroutines (`once`, `repeat`, `init_once`)
- `sans::compose` - Combine coroutines (`chain`, `map_input`, `map_yield`)
- `sans::concurrent` - Concurrent execution (`join`, `poll`)
- `sans::run` - Execute pipelines (`handle`, `handle_async`)

---

**License:** See [LICENSE](LICENSE) | **Contributing:** TBD
