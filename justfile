# Cross-cutting commands. Each toolchain is also self-sufficient at its own root:
# a Rust engineer runs `cargo test` and never touches nx; a TS engineer does the inverse.

default:
    @just --list

# The definition of done. Everything, not just the part you touched.
check: fmt-check lint test scenarios

fmt:
    cargo fmt

fmt-check:
    cargo fmt --check

lint:
    cargo clippy --workspace --all-targets -- -D warnings

test:
    cargo test --workspace

# End-to-end scenarios, driven through the real binary.
scenarios:
    cargo build -p borg-cli -p borg-server
    bash scenarios/run-all.sh

# Fan-out measurement. Not a correctness check; see SPEC.md §16.3.
bench:
    cargo test --release -p borg-engine --test scale -- --ignored --nocapture
