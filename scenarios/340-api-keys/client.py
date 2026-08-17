#!/usr/bin/env python3
"""A credentialed borg client: a socket, `json`, and a key in the handshake. SPEC.md §17.5, §17.6.

    client.py SOCKET REGISTRY KEY VALUE

The same shape as `scenarios/250-serve/client.py` and deliberately so — what this one adds is the
**one field** an authenticating client needs, `credential` in the `ClientHello`, which is the whole
of what static API keys cost a client. An empty KEY falls back to `$BORG_TOKEN`, which is how a
deployment carries a key that is not written into a url.

It does a transaction end to end — begin, set, commit — and then reads the cell back outside it,
because "the handshake was accepted" is a weaker claim than "the write landed". Prints two lines:

    landed=L12
    value: 41
"""

import json
import os
import socket
import sys

# A hung server should fail the scenario, not wedge it.
TIMEOUT = 30


def main(argv):
    if len(argv) != 4:
        print(__doc__, file=sys.stderr)
        return 2
    path, registry, key, value = argv
    credential = key or os.environ.get("BORG_TOKEN") or None

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(TIMEOUT)
    sock.connect(path)
    stream = sock.makefile("rwb")

    def send(message):
        stream.write((json.dumps(message) + "\n").encode())
        stream.flush()

    def recv():
        line = stream.readline()
        if not line:
            raise SystemExit("the server hung up")
        return json.loads(line)

    recv()  # the server's hello
    # **The credential is one field in the handshake and nothing else changes**, which is the
    # argument for having reserved it: a client that had to learn a new message shape on the day
    # authentication arrived would be a client every deployment had to update at once.
    send(
        {
            "version": 2,
            "codec": "json",
            "registry": registry,
            "credential": credential,
        }
    )
    ack = recv()
    if "refused" in ack:
        raise SystemExit("refused: " + ack["refused"]["reason"])

    send({"tx_begin": {}})
    tx = recv()["tx"]["tx"]
    send({"tx_set": {"tx": tx, "cell": "Company#1.headcount", "value": value}})
    recv()
    send({"tx_commit": {"tx": tx}})
    print("landed=" + recv()["committed"]["landed"])

    send({"get": {"cell": "Company#1.headcount"}})
    print("value: " + str(recv()["cell"]["value"]))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
