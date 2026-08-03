# Cross-cutting commands. Each toolchain is also self-sufficient at its own root:
# a Rust engineer runs `cargo test` and never touches nx; a TS engineer does the inverse.

default:
    @just --list

check:
    cargo fmt --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace

fmt:
    cargo fmt

test:
    cargo test --workspace
