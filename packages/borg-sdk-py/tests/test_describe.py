"""
Describe assembly: what a repo reports, and everything it refuses to report.

The payload is the whole contract with ``borg repo push`` (§17.4), and the cross-checks are the SDK
draft's §2.2 made real — ownership is stated twice, on the field and on the pipeline, and the two
statements have to agree in both directions.

The assertions here are deliberately the same assertions as ``packages/borg-sdk/test/describe.test.ts``,
including the literal JSON: two SDKs describing the same repo have to describe it identically, or the
engine is being handed two dialects of one contract.
"""

import json
import unittest

import borg
from borg.repo import _payload


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


if __name__ == "__main__":
    unittest.main()
