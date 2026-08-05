"""
Choosing a transport, from the worker's side.

The engine decides; this side only reads what it was told. ``BORG_WORKER_SOCKET`` means a socket, and
nothing means the process's own pipes — which is where every byte written to stdout from then on is a
protocol message.
"""

import io
import sys
import unittest

from borg import SOCKET_ENV, BorgProtocolError, connect


class _Stream:
    """Something with a `.buffer`, which is what the stdio transport actually writes to."""

    def __init__(self, initial=b""):
        self.buffer = io.BytesIO(initial)


class ChoosingATransport(unittest.TestCase):
    def test_with_no_socket_on_offer_the_connection_is_this_process_s_own_pipes(self):
        out, inp = _Stream(), _Stream(b'{"shutdown":{}}\n')
        original = (sys.stdout, sys.stdin)
        sys.stdout, sys.stdin = out, inp
        try:
            conn = connect({})
            conn.send({"done": {}})
            received = conn.receive()
        finally:
            sys.stdout, sys.stdin = original

        self.assertEqual(out.buffer.getvalue(), b'{"done":{}}\n')
        self.assertEqual(received, {"shutdown": {}})

    def test_a_socket_that_is_not_there_is_refused_by_name_rather_than_hanging(self):
        with self.assertRaisesRegex(
            BorgProtocolError, r"BORG_WORKER_SOCKET=/nonexistent/borg\.sock"
        ):
            connect({SOCKET_ENV: "/nonexistent/borg.sock"})

    def test_an_empty_socket_path_means_stdio_rather_than_a_socket_called_nothing(self):
        out, inp = _Stream(), _Stream(b"")
        original = (sys.stdout, sys.stdin)
        sys.stdout, sys.stdin = out, inp
        try:
            conn = connect({SOCKET_ENV: ""})
            conn.send({"done": {}})
            # End of stream is a hang-up, not an error: the engine has finished with this worker.
            self.assertIsNone(conn.receive())
        finally:
            sys.stdout, sys.stdin = original
        self.assertEqual(out.buffer.getvalue(), b'{"done":{}}\n')


if __name__ == "__main__":
    unittest.main()
