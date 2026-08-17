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

step "binaries"
# Before the SDK suites, not just before the scenarios. The TypeScript client suite drives a real
# `borg-server` and skips itself when the binaries are absent (§17.5) — so building them *after* it
# meant that on a tree nobody had built yet, thirty-one tests silently did not run and the script
# still said "all checks passed". That is every CI checkout. The build has to precede both consumers,
# so it is its own step rather than the first line of the scenarios one.
cargo build -p borg-cli -p borg-server

step "typescript"
# The TypeScript SDK's own tests. Not everywhere has a JavaScript runtime, and this script has to
# work everywhere, so a missing toolchain is a loud skip rather than a failure — the engine's half of
# the socket transport is covered by `borg-exec-process`'s tests, which need only cargo.
if command -v node >/dev/null 2>&1 && command -v pnpm >/dev/null 2>&1; then
    (cd packages/borg-sdk && pnpm install --silent && pnpm run --silent check)
else
    printf '  \033[33m⚠ skipped: node and pnpm are not both installed\033[0m\n'
fi

step "python"
# The Python SDK's own tests. They are `unittest` cases with no dependencies, so any Python 3.11+
# runs them — no pip, no virtualenv, no network. `pytest` runs the same files and is the nicer
# runner; nothing requires it, which is what keeps this step from being a second optional toolchain.
if command -v python3 >/dev/null 2>&1 \
    && python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)'; then
    (cd packages/borg-sdk-py && PYTHONPATH=src python3 -m unittest discover -s tests -q)
else
    printf '  \033[33m⚠ skipped: python3 3.11+ is not installed\033[0m\n'
fi

step "scenarios"
# Both binaries — the client every scenario drives, and the server seven of them start (§17.6) —
# were built above, because the TypeScript suite needs them too.
bash scenarios/run-all.sh

printf '\n\033[1;32mall checks passed\033[0m\n'
