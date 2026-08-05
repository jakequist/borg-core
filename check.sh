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

step "typescript"
# The TypeScript SDK's own tests. Not everywhere has a JavaScript runtime, and this script has to
# work everywhere, so a missing toolchain is a loud skip rather than a failure — the engine's half of
# the socket transport is covered by `borg-exec-process`'s tests, which need only cargo.
if command -v node >/dev/null 2>&1 && command -v pnpm >/dev/null 2>&1; then
    (cd packages/borg-sdk && pnpm install --silent && pnpm run --silent check)
else
    printf '  \033[33m⚠ skipped: node and pnpm are not both installed\033[0m\n'
fi

step "scenarios"
cargo build -p borg-cli
bash scenarios/run-all.sh

printf '\n\033[1;32mall checks passed\033[0m\n'
