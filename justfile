# Minimum supported Rust version used in CI workflows
MSRV := "1.85.0"

default:
    @just --list

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

clippy:
    cargo clippy --all-targets --all-features -- -D warnings

test *args='':
    cargo test {{args}}

test-no-default-features:
    cargo test --no-default-features

test-all-features:
    cargo test --all-features

test-msrv-default:
    cargo +{{MSRV}} test

test-msrv-no-default-features:
    cargo +{{MSRV}} test --no-default-features

test-msrv-all-features:
    cargo +{{MSRV}} test --all-features

test-default-matrix:
    @just test
    @just test-no-default-features
    @just test-all-features

test-matrix:
    @just test-default-matrix
    @just test-msrv-default
    @just test-msrv-no-default-features
    @just test-msrv-all-features

ci: fmt-check clippy test-default-matrix
    @echo "✓ CI checks passed on default toolchain!"

ci-all-versions: fmt-check clippy test-matrix
    @echo "✓ CI checks passed on all configured toolchains!"
