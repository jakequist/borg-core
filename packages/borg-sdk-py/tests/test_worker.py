"""
The worker loop, against a stand-in engine on a real unix socket.

A fake socket would test the code and not the contract; this one binds, accepts a connection,
performs the handshake the engine performs, and answers the messages the engine answers. What it
asserts is the conversation — which cells were asked for, in which order, and with what text —
because that conversation *is* the dependency capture. The SDK records nothing, so if the right
``get`` does not cross the wire, the right invalidation does not happen.

The worker runs on a thread because ``main()`` blocks: this SDK is synchronous all the way down, so
the loop that TypeScript runs as a promise runs here as a thread the test joins. The engine's half —
the assertions — stays on the main thread, which is where a failure should be reported from.
"""

import json
import os
import shutil
import socket
import tempfile
import threading
import unittest

import borg
from borg import SOCKET_ENV, producer_id

INPUT = "Company:o-04068"

Company = borg.struct(
    "Company",
    {
        "website": borg.string(),
        "headcount": borg.int_(),
        "founded": borg.int_(),
        "isInvestible": borg.bool_().derived(),
    },
)


def investing(body):
    invest = borg.pipeline("invest", Company, ["isInvestible"], body)
    return borg.repo(id=1, structs=[Company], pipelines=[invest])


class Engine:
    """The engine's half of the connection, in the shape a test wants to write."""

    def __init__(self, repo):
        self.dir = tempfile.mkdtemp(prefix="borg-sdk-py-")
        self.path = os.path.join(self.dir, "worker.sock")
        self.previous = os.environ.get(SOCKET_ENV)
        os.environ[SOCKET_ENV] = self.path

        self.listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.listener.bind(self.path)
        self.listener.listen(1)

        self.failure = None
        # The listener exists before the worker does, which is what the engine itself does and for
        # the same reason: a worker that starts first finds nothing to connect to.
        self.worker = threading.Thread(target=self._run, args=(repo,), daemon=True)
        self.worker.start()

        self.conn, _ = self.listener.accept()
        self.reader = self.conn.makefile("rb")
        self.writer = self.conn.makefile("wb")

        self.send({"version": 1, "codecs": ["msgpack", "json"]})
        assert self.receive() == {"codec": "json"}

    def _run(self, repo):
        try:
            repo.main([])
        except BaseException as err:  # noqa: BLE001 — reported on the main thread
            self.failure = err

    def send(self, message):
        self.writer.write(json.dumps(message).encode("utf-8") + b"\n")
        self.writer.flush()

    def receive(self):
        line = self.reader.readline()
        assert line, "the worker hung up"
        return json.loads(line)

    def stop(self):
        try:
            self.send({"shutdown": {}})
            self.worker.join(timeout=5)
            assert not self.worker.is_alive(), "the worker did not stop when told to"
        finally:
            self.reader.close()
            self.writer.close()
            self.conn.close()
            self.listener.close()
            shutil.rmtree(self.dir, ignore_errors=True)
            if self.previous is None:
                os.environ.pop(SOCKET_ENV, None)
            else:
                os.environ[SOCKET_ENV] = self.previous
        if self.failure is not None:
            raise self.failure


class ServingInvocations(unittest.TestCase):
    def start(self, body):
        engine = Engine(investing(body))
        self.addCleanup(engine.stop)
        return engine

    def test_a_get_is_a_wire_message_and_a_set_carries_the_canonical_text(self):
        def invest(c):
            website = c.get("website")
            headcount = c.get("headcount")
            c.set(
                "isInvestible",
                website is not None and website.endswith(".ai") and (headcount or 0) > 10,
            )

        engine = self.start(invest)
        engine.send({"invoke": {"producer": producer_id("invest"), "input": INPUT}})
        self.assertEqual(engine.receive(), {"get": f"{INPUT}.website"})
        engine.send({"value": "acme.ai"})
        self.assertEqual(engine.receive(), {"get": f"{INPUT}.headcount"})
        engine.send({"value": "40"})
        self.assertEqual(
            engine.receive(), {"set": {"cell": f"{INPUT}.isInvestible", "value": "true"}}
        )
        engine.send({"ok": {}})
        self.assertEqual(engine.receive(), {"done": {}})

    def test_a_pipeline_may_print_mid_invocation_without_touching_the_protocol(self):
        """
        The point of the socket, from this side: the protocol is on a descriptor of its own, so
        stdout is the author's and a `print()` costs nothing.
        """

        def invest(c):
            print("about to read the website")
            website = c.get("website")
            print("no newline, either", end="")
            c.set("isInvestible", website is not None)

        engine = self.start(invest)
        engine.send({"invoke": {"producer": producer_id("invest"), "input": INPUT}})
        self.assertEqual(engine.receive(), {"get": f"{INPUT}.website"})
        engine.send({"value": "acme.ai"})
        self.assertEqual(
            engine.receive(), {"set": {"cell": f"{INPUT}.isInvestible", "value": "true"}}
        )
        engine.send({"ok": {}})
        self.assertEqual(engine.receive(), {"done": {}})

    def test_an_absent_cell_and_a_tombstone_both_read_as_none_and_none_writes_a_tombstone(self):
        """`{"value": null}` is a cell that has never been written; `~` is one deleted (§8.1)."""
        seen = []

        def invest(c):
            seen.append(c.get("headcount"))
            seen.append(c.get("founded"))
            c.set("isInvestible", None)

        engine = self.start(invest)
        engine.send({"invoke": {"producer": producer_id("invest"), "input": INPUT}})
        engine.receive()
        engine.send({"value": None})
        engine.receive()
        engine.send({"value": "~"})
        self.assertEqual(
            engine.receive(), {"set": {"cell": f"{INPUT}.isInvestible", "value": "~"}}
        )
        engine.send({"ok": {}})
        self.assertEqual(engine.receive(), {"done": {}})
        self.assertEqual(seen, [None, None])

    def test_the_world_is_random_access_to_any_cell_stringly_or_converted(self):
        seen = []

        def invest(c, world):
            seen.append(world.get("Company:o-99999.website"))
            seen.append(world.get("Company:o-99999.headcount", borg.int_()))
            world.set("Note:o-12345.body", "seen")
            c.set("isInvestible", False)

        engine = self.start(invest)
        engine.send({"invoke": {"producer": producer_id("invest"), "input": INPUT}})
        self.assertEqual(engine.receive(), {"get": "Company:o-99999.website"})
        engine.send({"value": "rival.ai"})
        self.assertEqual(engine.receive(), {"get": "Company:o-99999.headcount"})
        engine.send({"value": "7"})
        self.assertEqual(engine.receive(), {"set": {"cell": "Note:o-12345.body", "value": "seen"}})
        engine.send({"ok": {}})
        engine.receive()
        engine.send({"ok": {}})
        self.assertEqual(engine.receive(), {"done": {}})
        self.assertEqual(seen, ["rival.ai", 7])

    def test_attribute_access_sends_exactly_the_message_get_and_set_send(self):
        """
        The gate's own probe, run rather than argued. `__getattr__` blocks on the socket read and
        returns the value; the wire sees no difference at all, which is the finding — mediated
        property access was never a protocol question, only a JavaScript one.
        """

        def invest(c):
            c.isInvestible = c.headcount is not None and c.headcount > 10

        engine = self.start(invest)
        engine.send({"invoke": {"producer": producer_id("invest"), "input": INPUT}})
        self.assertEqual(engine.receive(), {"get": f"{INPUT}.headcount"})
        engine.send({"value": "40"})
        # Two reads, because `c.headcount` is read twice: nothing is cached, so the read-set is
        # exactly what the body asked for and no more.
        self.assertEqual(engine.receive(), {"get": f"{INPUT}.headcount"})
        engine.send({"value": "40"})
        self.assertEqual(
            engine.receive(), {"set": {"cell": f"{INPUT}.isInvestible", "value": "true"}}
        )
        engine.send({"ok": {}})
        self.assertEqual(engine.receive(), {"done": {}})


class FailureStaysInsideOneInvocation(unittest.TestCase):
    def start(self, body):
        engine = Engine(investing(body))
        self.addCleanup(engine.stop)
        return engine

    def test_a_pipeline_that_raises_reports_the_failure_and_the_worker_keeps_serving(self):
        attempts = []

        def invest(c):
            attempts.append(1)
            if len(attempts) == 1:
                raise ValueError("no website to speak of")
            c.set("isInvestible", False)

        engine = self.start(invest)
        engine.send({"invoke": {"producer": producer_id("invest"), "input": INPUT}})
        self.assertEqual(
            engine.receive(), {"error": {"message": "ValueError: no website to speak of"}}
        )

        # The stream is still in step, which is what makes a failed invocation cost one entity and
        # not the process.
        engine.send({"invoke": {"producer": producer_id("invest"), "input": INPUT}})
        self.assertEqual(
            engine.receive(), {"set": {"cell": f"{INPUT}.isInvestible", "value": "false"}}
        )
        engine.send({"ok": {}})
        self.assertEqual(engine.receive(), {"done": {}})

    def test_writing_a_field_the_pipeline_did_not_declare_fails_before_it_reaches_the_engine(self):
        def invest(c):
            c.set("website", "acme.ai")

        engine = self.start(invest)
        engine.send({"invoke": {"producer": producer_id("invest"), "input": INPUT}})
        reply = engine.receive()
        self.assertRegex(reply["error"]["message"], r"does not declare `website`")

    def test_an_invocation_this_repo_does_not_implement_is_reported_rather_than_ignored(self):
        engine = self.start(lambda c: None)
        engine.send({"invoke": {"producer": producer_id("unknown"), "input": INPUT}})
        reply = engine.receive()
        self.assertRegex(reply["error"]["message"], "does not implement")

    def test_a_leaked_context_stops_working_the_moment_its_invocation_ends(self):
        leaked = []

        def invest(c):
            leaked.append(c)
            c.set("isInvestible", True)

        engine = self.start(invest)
        engine.send({"invoke": {"producer": producer_id("invest"), "input": INPUT}})
        engine.receive()
        engine.send({"ok": {}})
        self.assertEqual(engine.receive(), {"done": {}})

        with self.assertRaisesRegex(borg.BorgProtocolError, "already finished"):
            leaked[0].get("website")


if __name__ == "__main__":
    unittest.main()
