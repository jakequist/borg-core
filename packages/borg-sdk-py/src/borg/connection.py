"""
The connection to the engine: newline-delimited JSON, in strict request/response order.

## Why a socket

A worker may be spoken to over its own stdio, and the shell pipelines are. That is not survivable in
a language with a ``print()``: one stray line anywhere — in the pipeline, in a library, in a warning
the runtime emits — desynchronises the stream permanently, and the failure surfaces far from its
cause. So this SDK declares ``"transport": "socket"``, the engine listens on a unix socket and passes
its path in ``BORG_WORKER_SOCKET``, and **stdout is left entirely to the author** (§17.4).

The stdio path is still implemented, because the transport is the engine's choice and not this
library's, and because a worker started by hand has no socket to connect to.

## Requests are serialised

The protocol is one reply per request on one stream, so two requests in flight would read each
other's answers.

In TypeScript that is a first-day hazard — ``await Promise.all([c.get("a"), c.get("b")])`` is a thing
an author writes without thinking — and the TS SDK chains every request behind the last to make it
correct rather than merely discouraged. Nothing in idiomatic Python can produce that overlap: a
synchronous ``get`` has returned before the next statement runs. The mutex below is therefore *not*
that machinery; it is four lines that keep the same guarantee for a body that reaches for
``threading`` or ``concurrent.futures``, which is a deliberate act rather than an accident.
"""

from __future__ import annotations

import json
import os
import socket
import sys
import threading
from typing import Any, BinaryIO, Mapping

__all__ = ["SOCKET_ENV", "BorgProtocolError", "Connection", "connect"]

#: The environment variable the engine names the socket in. Contract, not this library's choice.
SOCKET_ENV = "BORG_WORKER_SOCKET"


class BorgProtocolError(Exception):
    """The conversation with the engine went wrong, rather than the pipeline."""


class Connection:
    """
    A newline-delimited message stream over one duplex pair.

    Reading is buffered — ``readline`` on a socket without a buffer is one syscall per byte —
    and the buffer belongs to this object, so nothing else may read the same descriptor.
    """

    def __init__(self, reader: BinaryIO, writer: BinaryIO, closer: Any = None) -> None:
        self._reader = reader
        self._writer = writer
        self._closer = closer
        self._lock = threading.Lock()

    def receive(self) -> dict[str, Any] | None:
        """The next message from the engine, or ``None`` once it has hung up."""
        while True:
            line = self._reader.readline()
            if not line:
                return None
            text = line.decode("utf-8").strip()
            # Blank lines are ignored rather than fatal, matching the engine's own reader.
            if not text:
                continue
            try:
                return json.loads(text)
            except ValueError as err:
                raise BorgProtocolError(
                    f"the engine sent something that is not JSON: {text}"
                ) from err

    def send(self, message: Mapping[str, Any]) -> None:
        self._writer.write(json.dumps(message, separators=(",", ":")).encode("utf-8") + b"\n")
        self._writer.flush()

    def request(self, message: Mapping[str, Any]) -> dict[str, Any]:
        """Send one request and read its reply, with nothing else able to interleave."""
        with self._lock:
            self.send(message)
            reply = self.receive()
        if reply is None:
            raise BorgProtocolError("the engine hung up in the middle of an invocation")
        return reply

    def close(self) -> None:
        if self._closer is not None:
            self._closer()


def connect(env: Mapping[str, str] | None = None) -> Connection:
    """Connect however the engine asked to be spoken to."""
    env = os.environ if env is None else env
    path = env.get(SOCKET_ENV)
    if not path:
        # No socket on offer: the engine is speaking over this process's own pipes, and everything
        # written to stdout from here on is a protocol message.
        return Connection(sys.stdin.buffer, sys.stdout.buffer)

    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        client.connect(path)
    except OSError as err:
        client.close()
        raise BorgProtocolError(f"{SOCKET_ENV}={path}: {err}") from err
    # `makefile` gives one buffered reader and one buffered writer over the same descriptor, which is
    # what lets `readline` be a real read-ahead rather than a byte at a time.
    reader = client.makefile("rb")
    writer = client.makefile("wb")

    def close() -> None:
        reader.close()
        writer.close()
        client.close()

    return Connection(reader, writer, close)
