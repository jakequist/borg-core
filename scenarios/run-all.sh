#!/usr/bin/env bash
# Run every scenario in order. Exits non-zero on the first failure.
set -euo pipefail

cd "$(dirname "$0")"
export BORG_BIN="${BORG_BIN:-$(cd .. && pwd)/target/debug/borg}"

if [ ! -x "$BORG_BIN" ]; then
    echo "borg binary not found at $BORG_BIN — run: cargo build -p borg-cli" >&2
    exit 1
fi

failed=0
for dir in [0-9]*/; do
    name="${dir%/}"
    echo "▸ $name"
    if ! (cd "$name" && bash ./run.sh); then
        echo "  ✗ scenario failed: $name" >&2
        failed=1
        break
    fi
done

[ $failed -eq 0 ] && echo && echo "all scenarios passed"
exit $failed
