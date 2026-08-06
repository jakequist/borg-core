"""
Describe assembly: what a repo reports, and everything it refuses to report.

The payload is the whole contract with ``borg repo push`` (§17.4), and the cross-checks are the SDK
draft's §2.2 made real — ownership is stated twice, on the field and on the pipeline, and the two
statements have to agree in both directions.

The assertions here are deliberately the same assertions as ``packages/borg-sdk/test/describe.test.ts``,
including the literal JSON: two SDKs describing the same repo have to describe it identically, or the
engine is being handed two dialects of one contract.
"""

import importlib
import json
import os
import shutil
import sys
import tempfile
import unittest

import borg
from borg.repo import _fingerprint, _payload


def investing():
    Company = borg.struct(
        "Company",
        {
            "website": borg.string(),
            "headcount": borg.int_(),
            "employees": borg.list_(borg.ref("Employee")),
            "isInvestible": borg.bool_().derived(),
        },
    )

    @borg.pipeline("invest", Company, writes=["isInvestible"])
    def invest(c):
        c.set("isInvestible", c.get("headcount") is not None)

    return Company, invest


class WhatARepoReports(unittest.TestCase):
    def test_it_is_the_shape_borg_repo_push_reads_field_types_and_all(self):
        Company, invest = investing()
        self.assertEqual(
            borg.describe([Company], [invest], id=2),
            {
                "structs": [
                    {
                        "name": "Company",
                        "fields": [
                            {"name": "website", "type": "String"},
                            {"name": "headcount", "type": "Int"},
                            {"name": "employees", "type": "Employee[]"},
                            {"name": "isInvestible", "type": "Bool", "derived_by": "invest"},
                        ],
                    }
                ],
                "producers": [{"name": "invest", "source": "Company"}],
                "transport": "socket",
                "repo": 2,
            },
        )

    def test_the_payload_is_the_bytes_the_typescript_sdk_would_have_written(self):
        """
        `JSON.stringify` pads nothing and escapes nothing; `json.dumps` does both by default. The
        engine parses either, so this is not correctness — it is that a difference between two SDKs
        describing the same repo is a difference somebody has to explain.
        """
        Company, invest = investing()
        self.assertEqual(
            _payload(borg.describe([Company], [invest], id=2)),
            '{"structs":[{"name":"Company","fields":['
            '{"name":"website","type":"String"},'
            '{"name":"headcount","type":"Int"},'
            '{"name":"employees","type":"Employee[]"},'
            '{"name":"isInvestible","type":"Bool","derived_by":"invest"}]}],'
            '"producers":[{"name":"invest","source":"Company"}],'
            '"transport":"socket","repo":2}',
        )

    def test_it_always_declares_the_socket_transport(self):
        """
        A socket is the only arrangement in which a `print()` is survivable, so every repo written
        with this SDK asks for one — and asks *in describe*, which is the one thing the engine reads
        before it decides how to spawn the worker.
        """
        Company, invest = investing()
        self.assertEqual(borg.describe([Company], [invest])["transport"], "socket")

    def test_it_omits_the_repo_id_when_the_author_did_not_state_one(self):
        Company, invest = investing()
        self.assertNotIn("repo", borg.describe([Company], [invest]))

    def test_ownership_names_the_pipeline_the_engine_will_hash(self):
        Company, invest = investing()
        fields = borg.describe([Company], [invest])["structs"][0]["fields"]
        owned = [f for f in fields if f["name"] == "isInvestible"][0]
        self.assertEqual(owned["derived_by"], "invest")
        # The id the engine derives from that name — the same FNV-1a, past 2⁵³, which is why it
        # crosses the wire as a string. The literal is the one the TypeScript SDK asserts.
        self.assertEqual(borg.producer_id("invest"), "12342029420047889112")

    def test_a_producer_id_is_a_string_even_though_python_could_hold_the_number(self):
        self.assertIsInstance(borg.producer_id("invest"), str)
        self.assertGreater(int(borg.producer_id("invest")), 2**53)


class TheCrossChecksInBothDirections(unittest.TestCase):
    def test_a_derived_field_no_pipeline_writes_is_refused(self):
        Company = borg.struct(
            "Company",
            {
                "headcount": borg.int_(),
                "isInvestible": borg.bool_().derived(),
                "score": borg.int_().derived(),
            },
        )
        invest = borg.pipeline("invest", Company, ["isInvestible"], lambda c: None)
        with self.assertRaisesRegex(
            borg.BorgDefinitionError, r"`Company\.score` is declared derived\(\) but no pipeline"
        ):
            borg.describe([Company], [invest])

    def test_a_pipeline_writing_a_field_nobody_marked_derived_is_refused_too(self):
        Company = borg.struct("Company", {"headcount": borg.int_()})
        invest = borg.pipeline("invest", Company, ["headcount"], lambda c: None)
        with self.assertRaisesRegex(borg.BorgDefinitionError, r"not declared derived\(\)"):
            borg.describe([Company], [invest])

    def test_a_pipeline_writing_a_field_the_struct_does_not_have_is_refused(self):
        Company = borg.struct("Company", {"headcount": borg.int_()})
        invest = borg.pipeline("invest", Company, ["isInvestible"], lambda c: None)
        with self.assertRaisesRegex(borg.BorgDefinitionError, "does not declare"):
            borg.describe([Company], [invest])

    def test_two_pipelines_claiming_one_field_is_refused_and_both_are_named(self):
        """Single writer per field is what lets derived layers commit concurrently (§16.3)."""
        Company = borg.struct("Company", {"isInvestible": borg.bool_().derived()})
        first = borg.pipeline("invest", Company, ["isInvestible"], lambda c: None)
        second = borg.pipeline("score", Company, ["isInvestible"], lambda c: None)
        with self.assertRaisesRegex(borg.BorgDefinitionError, "both `invest` and `score`"):
            borg.describe([Company], [first, second])

    def test_a_pipeline_over_a_struct_the_repo_does_not_declare_is_refused(self):
        Company = borg.struct("Company", {"isInvestible": borg.bool_().derived()})
        invest = borg.pipeline("invest", Company, ["isInvestible"], lambda c: None)
        with self.assertRaisesRegex(borg.BorgDefinitionError, "does not list in"):
            borg.describe([], [invest])

    def test_a_pipeline_that_writes_nothing_is_refused(self):
        Company = borg.struct("Company", {"headcount": borg.int_()})
        idle = borg.pipeline("idle", Company, [], lambda c: None)
        with self.assertRaisesRegex(borg.BorgDefinitionError, "no `writes`"):
            borg.describe([Company], [idle])

    def test_a_name_declared_twice_in_one_repo_is_refused(self):
        Company = borg.struct("Company", {"isInvestible": borg.bool_().derived()})
        invest = borg.pipeline("invest", Company, ["isInvestible"], lambda c: None)
        with self.assertRaisesRegex(borg.BorgDefinitionError, "declared twice"):
            borg.describe([Company, Company], [invest])
        with self.assertRaisesRegex(borg.BorgDefinitionError, "declared twice"):
            borg.describe([Company], [invest, invest])

    def test_borg_repo_refuses_at_import_time_not_at_the_first_invocation(self):
        Company = borg.struct("Company", {"score": borg.int_().derived()})
        with self.assertRaises(borg.BorgDefinitionError):
            borg.repo(structs=[Company], pipelines=[])

    def test_a_field_declared_with_something_that_is_not_a_field_type_is_refused(self):
        """`borg.int_` un-called is this language's mistake; naming it beats an AttributeError."""
        with self.assertRaisesRegex(borg.BorgDefinitionError, "not a field type"):
            borg.struct("Company", {"headcount": borg.int_})


class HowAPipelineBodyMayBeWritten(unittest.TestCase):
    def test_it_takes_the_entity_alone_or_the_entity_and_the_world(self):
        Company = borg.struct("Company", {"isInvestible": borg.bool_().derived()})

        @borg.pipeline("invest", Company, writes=["isInvestible"])
        def alone(c):
            pass

        @borg.pipeline("score", Company, writes=["isInvestible"])
        def with_world(c, world):
            pass

        # JS lets a body ignore an argument by not naming it; Python raises, so the arity is read
        # once from the signature rather than guessed at every invocation.
        self.assertFalse(alone.wants_world)
        self.assertTrue(with_world.wants_world)

    def test_the_decorator_and_the_call_produce_the_same_definition(self):
        Company = borg.struct("Company", {"isInvestible": borg.bool_().derived()})

        def body(c):
            pass

        decorated = borg.pipeline("invest", Company, writes=["isInvestible"])(body)
        called = borg.pipeline("invest", Company, ["isInvestible"], body)
        self.assertEqual(
            borg.describe([Company], [decorated]), borg.describe([Company], [called])
        )


class DescribeMode(unittest.TestCase):
    def test_describe_prints_the_payload_and_serves_nothing(self):
        import contextlib
        import io

        Company, invest = investing()
        repo = borg.repo(id=1, structs=[Company], pipelines=[invest])
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            repo.main(["describe"])
        self.assertEqual(json.loads(out.getvalue()), repo.describe())


class TheImplementationFingerprint(unittest.TestCase):
    """
    §9.2's *pushing new pipeline source moves the producer's ClientVersion* needs something that
    moves when the source does. The diff compares name, source buffer and writes, and an edited body
    touches none of them — so without this a code change is invisible to the push and the old output
    goes on being served labelled ``current``.
    """

    def scratch(self, *files):
        """A directory of modules, the first of which is the entry. Returns its path."""
        root = tempfile.mkdtemp(prefix="borg-fingerprint-")
        entry = None
        for name, body in files:
            path = os.path.join(root, name)
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(body)
            entry = entry or path
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        return entry

    def test_it_changes_when_the_entry_module_changes(self):
        before = self.scratch(("pipeline.py", "# one\n"))
        after = self.scratch(("pipeline.py", "# two\n"))
        self.assertNotEqual(_fingerprint(before), _fingerprint(after))

    def test_it_does_not_change_when_the_code_does_not(self):
        # Two directories, one program. Content and relative name, never absolute path, so a repo
        # checked out somewhere else is not a repo whose code changed.
        once = self.scratch(("pipeline.py", "# same\n"))
        again = self.scratch(("pipeline.py", "# same\n"))
        self.assertEqual(_fingerprint(once), _fingerprint(again))
        self.assertEqual(_fingerprint(once), _fingerprint(once))

    def test_it_covers_an_imported_module_sitting_beside_the_entry(self):
        """
        The half the TypeScript SDK cannot reach. A helper module next to the pipeline is code this
        repo ships, and editing it changes what the pipeline computes.
        """
        entry = self.scratch(("pipeline.py", "import helper\n"), ("helper.py", "VALUE = 1\n"))
        root = os.path.dirname(entry)
        sys.path.insert(0, root)
        try:
            importlib.invalidate_caches()
            helper = importlib.import_module("helper")
            before = _fingerprint(entry)
            with open(os.path.join(root, "helper.py"), "w", encoding="utf-8") as handle:
                handle.write("VALUE = 2\n")
            self.assertNotEqual(before, _fingerprint(entry))
        finally:
            sys.path.remove(root)
            sys.modules.pop("helper", None)
            del helper

    def test_it_says_what_produced_it(self):
        self.assertRegex(_fingerprint(self.scratch(("p.py", "x"))), r"^sha256:[0-9a-f]{64}$")

    def test_an_entry_module_that_cannot_be_read_yields_no_fingerprint_at_all(self):
        # `borg repo push` falls back to hashing the command file, so nothing is lost — and refusing
        # to describe over this would make an SDK repo un-pushable for a reason that does not matter.
        self.assertIsNone(_fingerprint(os.path.join(tempfile.gettempdir(), "borg-no-such-file")))

    def test_every_producer_a_repo_describes_carries_it(self):
        # `sys.argv[0]` is what the engine executed, and under `python -m unittest` it is not a file
        # at all — so the entry module is staged rather than assumed. This is the only test here that
        # exercises the default, and standing it up is what makes the default worth trusting.
        Company, invest = investing()
        entry = self.scratch(("pipeline.py", "# a repo\n"))
        argv0, sys.argv[0] = sys.argv[0], entry
        try:
            described = borg.repo(structs=[Company], pipelines=[invest]).describe()
        finally:
            sys.argv[0] = argv0
        self.assertRegex(described["producers"][0]["fingerprint"], r"^sha256:[0-9a-f]{64}$")

    def test_a_repo_whose_entry_is_not_a_file_describes_itself_without_one(self):
        # `borg repo push` hashes the command file in that case, so this is a fallback rather than a
        # loss — and describing has to keep working, or an SDK repo becomes un-pushable over a hash.
        Company, invest = investing()
        argv0 = sys.argv[0]
        sys.argv[0] = os.path.join(tempfile.gettempdir(), "borg-no-such-file")
        try:
            described = borg.repo(structs=[Company], pipelines=[invest]).describe()
        finally:
            sys.argv[0] = argv0
        self.assertNotIn("fingerprint", described["producers"][0])

    def test_the_pure_describe_payload_carries_none(self):
        # `describe` is a pure function of the definitions and stays one: the fingerprint is a fact
        # about files, so it is attached by `repo()`, which is already the impure half.
        Company, invest = investing()
        self.assertNotIn("fingerprint", borg.describe([Company], [invest])["producers"][0])


if __name__ == "__main__":
    unittest.main()
