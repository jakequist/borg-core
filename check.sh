#!/usr/bin/env bash
# The definition of done. Everything, not just the part you touched.
#
# `just check` runs the same steps for anyone who has just installed; this script is the one that
# always works, and is what CI and agents should call.
set -euo pipefail
cd "$(dirname "$0")"

step() { printf '\n\033[1m▸ %s\033[0m\n' "$1"; }

step "fmt"
cargo fmt --check

step "clippy"
cargo clippy --workspace --all-targets -- -D warnings

step "tests"
cargo test --workspace

step "scenarios"
cargo build -p borg-cli
bash scenarios/run-all.sh

printf '\n\033[1;32mall checks passed\033[0m\n'
