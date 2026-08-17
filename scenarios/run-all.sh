#!/usr/bin/env bash
# Run every scenario in order. Exits non-zero on the first failure.
#
# Two binaries: `borg`, which every scenario drives, and `borg-server`, which the seven that need a
# server start (250, 260, 270, 280, 300, 310, 320).
set -euo pipefail

cd "$(dirname "$0")"
export BORG_BIN="${BORG_BIN:-$(cd .. && pwd)/target/debug/borg}"
export BORG_SERVER_BIN="${BORG_SERVER_BIN:-$(cd .. && pwd)/target/debug/borg-server}"

if [ ! -x "$BORG_BIN" ]; then
    echo "borg binary not found at $BORG_BIN — run: cargo build -p borg-cli" >&2
    exit 1
fi
if [ ! -x "$BORG_SERVER_BIN" ]; then
    echo "borg-server binary not found at $BORG_SERVER_BIN — run: cargo build -p borg-server" >&2
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
