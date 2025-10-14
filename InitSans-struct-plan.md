# InitSans Struct Transition — Plan

## Operating Requirements

## PLANNING

1. **CONFIRM INTENT BEFORE YOU START.** If you are actively building this plan file now, you MUST explicitly confirm with the user that you should proceed.
2. **SURFACE QUESTIONS AFTER DRAFTING.** When you finish the draft, you MUST list questions/concerns and point reviewers to the exact places to look.

## EXECUTION

1. **UPDATE PHASES WITH PROGRESS CONTINUOUSLY.** As you begin or complete a phase, you MUST update the plan with what changed and which tests were added.
2. **ALWAYS TEST AND VERIFY COMPLETION.** Always test and verify completion of a phase before proceeding to the next section.
3. **CHECK TESTS AT PHASE START.** At the start of a new phase, check tests to ensure everything started by working.
4. **CREATE COMMIT PER PHASE.** Create a commit per phase at completion. use semantic commit message conventions for the message.

- Goal: Replace the `InitSans` trait with a fluent, type-state initialization builder (`init::build()`, `.yielding()`, `.shortcircuit()`, `.then()`, `.returning()`) so coroutine startup is expressed via concrete values (`Yielded`, `ShortCircuit`, `Sans`) instead of trait objects.
- Success Criteria (as a test): Integration: `tests::init_builder_shortcircuit_pipeline` composes `Sans::and_then`, `init::yielding().shortcircuit().then(...)`, and `poll::init_poll` exclusively through the new builder API (no `InitSans` trait usage) and passes.
- Non-Goals: Altering `Sans` trait semantics or the `Step` enum; revisiting concurrent scheduler ergonomics; introducing new async runtimes beyond existing support.
- Constraints: Keep the abstraction zero-cost (no extra heap allocations relative to the tuple/step approach); preserve ergonomic call sites via `Into` conversions or lightweight builder helpers; constrain public API churn to a single breaking-change release.

## Discovery
- Relevant Code Map: `src/init.rs` hosts the current trait plus blanket impls; `src/build/init.rs` exports helper functions (`init`, `init_once`, `init_repeat`, `init_from_fn`) that manufacture trait implementors; `src/compose/chain.rs` / `map.rs`, `src/result.rs`, `src/concurrent/join.rs`, `src/poll.rs`, `src/run/mod.rs`, and `src/iter.rs` all call `.init()` or expect `InitSans`; `src/prelude.rs` re-exports those helpers.
- Existing Interfaces: `InitSans<I, O>` exposes `type Next`, `type Return`, and `fn init(self) -> Step<(O, Next), Return>`; tuples `(O, S)` and `Step<(O, S), R>` implement it; callers supply closures returning `InitSans` to methods like `Sans::and_then`.
- Prior Examples/Patterns: `Step<Y, D>` demonstrates how zero-cost enums provide ergonomic combinators; the library already embraces newtypes (`MapInput`, `AndThen`) to encode type-state; the `build` module shows how constructor helpers can wrap raw tuples for ergonomics.
- Areas of Uncertainty: Inferring the input type `I` for `init::build()` chains without relying on trait associated types; ensuring the new `ShortCircuit` and `Yielded` values slot into adapters like `poll::init_poll` without extra indirection; migration of external code that demands `InitSans + 'static`; reworking iterator patterns that store `Option<InitSans>`.
- Sketch (directional view of the target flow):
  - Introduce concrete results: `Yielded<O, S>` (initial output + continuation) and `ShortCircuit<S, R>` (pending continuation or completed return).
  - Provide fluent builders: `init::build()`, `.yielding(o)`, `.shortcircuit()`, `.then(sans)`, `.returning(r)`, with intermediate builder types (`Build`, `YieldBuild`, `ShortCircuitBuild`, `YieldShortCircuitBuild`) enforcing valid transitions.
  - Update constructors (`init`, `init_once`, etc.) to return appropriate builder end-states while still accepting legacy tuples via `From`.
  - Adapt consumers (`Sans::and_then`, `poll::init_poll`, iterators, result/composition modules) to accept `Yielded`/`ShortCircuit` outputs instead of invoking `InitSans::init`.
  - Remove the `InitSans` trait, re-export the builder API and result types, and refresh documentation to teach the fluent pattern.

## Build Out
### Phase 1 — Builder Foundations
Status: Planned

Context: introduce concrete wrappers alongside the existing trait so downstream call sites keep compiling while we validate the shape.

A) Feature Slice
- Introduce the fluent builders (`Build`, `YieldBuild`, `ShortCircuitBuild`, `YieldShortCircuitBuild`) plus result structs (`Yielded<O, S>`, `ShortCircuit<S, R>`), establishing them as the new source of truth while the legacy trait remains temporarily for migration.
- Document how each builder state composes into one of the four final result shapes (`S`, `Yielded`, `ShortCircuit<S, R>`, `ShortCircuit<Yielded<O, S>, R>`).

B) Detailed Design 
- Create `pub struct Yielded<O, S>(pub O, pub S);` with helpers `fn split(self) -> (O, S)` and continuation transforms `fn map_next<F>`, plus ergonomic combinators mirroring `Sans`:
  - `fn map_input<I2, F>(self, f: F) -> Yielded<O, MapInput<S, F>>`
  - `fn map_yield<O2, F>(self, f: F) -> Yielded<O2, MapYield<S, F, _, O>>`
  - `fn map_return<D2, F>(self, f: F) -> Yielded<O, MapReturn<S, F>>`
  - `fn and_then<T, F>(self, f: F) -> ShortCircuit<Yielded<O, T::Pending>, T::Return>` where `F` matches the new builder semantics
  - `fn chain<R>(self, r: R) -> Yielded<O, Chain<S, R>>`
- Create `pub enum ShortCircuit<S, R> { Pending(S), Complete(R) }` with combinators `map_pending`, `map_complete`, `into_result`, plus analogs of the Sans helpers:
  - `fn map_input<I2, F>(self, f: F) -> ShortCircuit<MapInput<S, F>, R>`
  - `fn map_yield<O2, F>(self, f: F) -> ShortCircuit<MapYield<S, F, _, _>, R>`
  - `fn map_return<R2, F>(self, f: F) -> ShortCircuit<S, R2>`
  - `fn and_then<T, F>(self, f: F) -> ShortCircuit<Yielded<_, _>, _>` that applies when the pending branch exists
  - `fn chain<Rc>(self, r: Rc) -> ShortCircuit<Chain<S, Rc>, Rc::Return>`
- Define builder states:
  - `pub struct Build<I, O>(PhantomData<(I, O)>)`
  - `pub struct YieldBuild<I, O> { output: O, marker: PhantomData<I> }`
  - `pub struct ShortCircuitBuild<I, O, R>(PhantomData<(I, O, R)>)`
  - `pub struct YieldShortCircuitBuild<I, O, R> { output: O, marker: PhantomData<(I, R)> }`
- Evaluate whether these states require both `I` and `O` markers or if we can minimize generics (e.g., carry only `I` on `Build` and infer `O` from subsequent methods) without hurting ergonomics.
- Implement fluent methods:
  - `init::build::<I, O>() -> Build<I, O>`
  - `Build::then(self, sans: S) -> S`
  - `Build::yielding(self, output: O) -> YieldBuild<I, O>`
  - `Build::shortcircuit<R>(self) -> ShortCircuitBuild<I, O, R>`
  - `YieldBuild::then(self, sans: S) -> Yielded<O, S>`
  - `YieldBuild::shortcircuit<R>(self) -> YieldShortCircuitBuild<I, O, R>`
  - `ShortCircuitBuild::then(self, sans: S) -> ShortCircuit<S, R>`
  - `ShortCircuitBuild::yielding(self, output: O) -> YieldShortCircuitBuild<I, O, R>`
  - `YieldShortCircuitBuild::then(self, sans: S) -> ShortCircuit<Yielded<O, S>, R>`
  - `ShortCircuitBuild::returning(self, done: R) -> ShortCircuit<S, R>`
  - `YieldShortCircuitBuild::returning(self, done: R) -> ShortCircuit<Yielded<O, S>, R>`
- Provide `From` impls so existing `(O, S)`/`Step<(O, S), R>` convert into `Yielded`/`ShortCircuit<Yielded<_>, R>` for backwards compatibility while call sites migrate.
- Pseudocode (shape of the new types):
  ```rust
  struct Yielded<O, S>(O, S);
  enum ShortCircuit<S, R> { Pending(S), Complete(R) }
  struct Build<I, O>(PhantomData<(I, O)>);
  struct YieldBuild<I, O> { output: O, marker: PhantomData<I> }
  struct ShortCircuitBuild<I, O, R>(PhantomData<(I, O, R)>);
  struct YieldShortCircuitBuild<I, O, R> { output: O, marker: PhantomData<(I, R)> }

  impl<I, O> Build<I, O> {
      fn yielding(self, output: O) -> YieldBuild<I, O> { YieldBuild { output, marker: PhantomData } }
      fn shortcircuit<R>(self) -> ShortCircuitBuild<I, O, R> { ShortCircuitBuild(PhantomData) }
      fn then<S>(self, sans: S) -> S where S: Sans<I, O> { sans }
  }
  ```
- Example Usage (constructing a seed coroutine):
  ```rust
  fn seed_counter() -> Yielded<i32, CounterSans> {
      init::yielding(0).then(CounterSans::new())
  }

  fn seed_chain() -> Yielded<i32, Chain<CounterSans, Repeat<_>>> {
      init::yielding(0)
          .then(CounterSans::new())
          .chain(repeat(|x| x + 1))
  }
  ```

C) Testing Plan (Unit)
- Add `init::tests::yielded_round_trip` ensuring `(O, S)` converts to `Yielded` and back (via `From` impls).
- Add `init::tests::shortcircuit_pending_complete` covering both enum variants and all `map_*`/`and_then`/`chain` helpers.
- Add `init::tests::yielded_maps_and_chain` verifying the mapping combinators and chaining behavior on `Yielded`.
- Add `init::tests::builder_state_transitions` asserting fluent chains produce the expected terminal types (`S`, `Yielded`, `ShortCircuit` variants).

### Phase 2 — API Migration
Status: Planned

Context: shift all internal APIs to speak the struct language while allowing external callers to continue passing tuples or steps via `Into`.

A) Feature Slice
- Switch core combinators and adapters (`Sans::and_then`, `poll::init_poll`, iterator/result/concurrent modules, `run::handle*`) to consume/produce `Yielded` and `ShortCircuit` end-states, eliminating legacy helper functions instead of maintaining shims.
- Highlight conversion points so reviewers can trace where legacy tuples (`(O, S)`) or `Step` values become the new structs and confirm no allocations are introduced.

B) Detailed Design 
- Update `Sans::and_then` so the closure must return a `Yielded<O, S>` produced via the fluent builder, simplifying control flow and removing trait indirection.
- Delete `build::init`, `init_once`, `init_repeat`, and `init_from_fn`, rewriting their call sites to use fluent-builder chains directly.
- Refactor modules currently bound to `InitSans` (`poll::init_poll`, `iter::InitSansIter`, `result` combinators, `concurrent::join`, `run::handle*`) to destructure `Yielded`/`ShortCircuit` directly instead of calling `InitSans::init`, adjusting internal state machines accordingly.
- Provide compatibility conversions:
  - `(O, S) -> Yielded<O, S>`
  - `Step<(O, S), R> -> ShortCircuit<Yielded<O, S>, R>`
  - `Step<S, R> -> ShortCircuit<S, R>`
- Update `prelude` exports to surface `init::build`, `Yielded`, and `ShortCircuit`.
- Pseudocode (how `and_then` consumes the new outputs):
  ```rust
  impl<I, O, C> Sans<I, O> for MySans<C> {
      fn and_then<F>(self, f: F) -> AndThen<Self, _>
      where F: FnOnce(Return) -> Yielded<O, _>
      {
          let Yielded(output, mut next) = f(self.finish_value());
          emit_initial(output);
          next
      }
  }
  ```
- Example Usage (caller ergonomics stay familiar):
  ```rust
  // Closures return fluent builder results directly.
  let pipeline = repeat(|x: i32| x + 1).and_then(|last| {
      init::yielding(last * 2)
          .then(repeat(move |input: i32| input + last))
  });

  let mut driver = poll::init_poll(pipeline);
  assert_eq!(driver.next(Poll::Poll).unwrap_yielded(), PollOutput::Output(2));
  ```

C) Testing Plan (Unit)
- Extend `compose::tests::and_then_accepts_builder_returns` to cover closures returning builder outputs, raw tuples, and `Step`, asserting identical behavior.
- Add `poll::tests::init_poll_handles_shortcircuit_yielded` ensuring `init_poll` correctly handles `ShortCircuit<Yielded<_>, _>`.
- Update iterator/result combinator tests to cover closures returning `ShortCircuit::Complete(...)`, confirming immediate completion.

### Phase 3 — Trait Removal & Documentation
Status: Planned

Context: once the struct-backed code paths are proven, remove the trait, clean up docs, and present the struct as the canonical initialization abstraction.

A) Feature Slice
- Remove the `InitSans` trait, finalize naming around `Yielded`/`ShortCircuit`, update documentation, and smooth public API by replacing trait bounds with builder/end-state types while deciding which legacy helper names (`init`, `init_once`, etc.) remain as aliases vs. retirement.
- Capture a changelog entry outline so release notes call out the trait removal and renamed exports.

B) Detailed Design 
- Delete `pub trait InitSans` from `src/init.rs`, migrate helper methods (`map_input`, `map_yield`, `map_return`, `into_iter`) onto inherent impls for the new builder/end-state types.
- Provide deprecation notices or adapter type aliases (e.g., `pub type InitSans<I, O, S, R> = ShortCircuit<Yielded<O, S>, R>`) to ease migration, and ensure doc examples showcase the fluent builder rather than legacy `init_*` helpers.
- Audit `README.md`, crate docs (`lib.rs`), and module docs (`poll`, `result`, `compose`) to align narrative with the builder workflow, replacing trait terminology with `Yielded`/`ShortCircuit`.
- Update `prelude` and `pub use` sites to expose `init::build`, `Yielded`, `ShortCircuit`, and builder helpers.
- Re-check public docs for references to blanket trait impl behavior and replace with guidance on `Into<ShortCircuit<_>>` conversions.

C) Testing Plan (Integration)
- Add `tests/init_builder_pipeline.rs::init_builder_pipeline` that composes `init::yielding().then(...)`, `.and_then`, `poll::init_poll`, and `run::handle`, ensuring the user-facing API works via the builder flow.
- Run crate-wide doctests (`cargo test --doc`) to confirm documentation examples compile against the new API.

## Questions & Review Pointers
- Legacy builders (`build::init`, `init_once`, `init_repeat`, `init_from_fn`) will be removed outright; confirm there are no downstream crates depending on them before deletion.
- Clarify expectations for how caller code specifies the input type `I` when starting a chain (turbofish on `init::build()` vs inference)—see Discovery (Areas of Uncertainty) and Phase 1 (Detailed Design builder definitions).
- Update `init.rs`, `README.md`, crate docs, and module docs to replace “call `.init()` or `init_*` helpers” guidance with the fluent builder narrative.
