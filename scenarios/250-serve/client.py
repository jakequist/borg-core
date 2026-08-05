#!/usr/bin/env python3
"""A borg client. Not an SDK — a socket, `json`, and eighty lines.

The point is the same one `030-shell-pipeline` makes about the worker protocol: if this is
workable, the client protocol has no hidden client-library complexity. There is no generated code
here, no transaction object, no retry policy. There is a handshake, and then one request per line
with one response per line.

    client.py SOCKET [--client-version L7] REQUEST...

Every request is sent on **one connection**, in order, and every response is printed as one line of
compact JSON. `%TX%` in a request is replaced by the handle from the last `tx` response, which is
the only piece of state a client of this size needs to keep. A request prefixed `raw:` is sent
verbatim, which is how the scenario asks what a malformed line gets in reply.

Exiting closes the socket. That is deliberate and is how the scenario abandons a transaction: a
client that walks away is a client that walks away, and nothing about it is a special message.
"""

import json
import socket
import sys

# A hung server should fail the scenario, not wedge it.
TIMEOUT = 30


def main(argv):
    if not argv:
        print(__doc__, file=sys.stderr)
        return 2

    path, argv = argv[0], argv[1:]
    client_version = None
    if argv[:1] == ["--client-version"]:
        client_version = argv[1]
        argv = argv[2:]

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(TIMEOUT)
    sock.connect(path)
    stream = sock.makefile("rwb")

    # The server speaks first, and always in JSON: a handshake cannot be encoded in a codec that has
    # not been agreed yet.
    hello = json.loads(stream.readline())
    reply = {"version": hello["version"], "codec": "json"}
    if client_version is not None:
        # The def-layer this client's code was generated from (SPEC.md §5.4). A client with no
        # generated code — this one — says nothing and is read as "the schema as it stands".
        reply["client_version"] = client_version
    send(stream, reply)

    tx = None
    for request in argv:
        request = request.replace("%TX%", tx or "")
        if request.startswith("raw:"):
            # Sent as typed, without being parsed first — the only way to ask what the server does
            # with a line a client got wrong.
            stream.write((request[4:] + "\n").encode())
            stream.flush()
        else:
            send(stream, json.loads(request))
        line = stream.readline()
        if not line:
            print("the server closed the connection", file=sys.stderr)
            return 1
        response = json.loads(line)
        if "tx" in response:
            tx = response["tx"]["tx"]
        print(json.dumps(response, sort_keys=True))
    return 0


def send(stream, message):
    stream.write((json.dumps(message) + "\n").encode())
    stream.flush()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
