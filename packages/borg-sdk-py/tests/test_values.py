"""
The conversion table, in both directions.

These are the rules ``borg_core::parse`` applies on the other side of the wire, restated for a
language whose integers are unbounded and whose booleans are integers. Where the two could disagree,
the SDK refuses rather than guesses — a value that looks almost right is the worst available outcome,
because nothing complains and the data is wrong.

The TypeScript SDK's table is the reference and these cases are its twin, *except* where the
difference is the finding: see ``an_Int_is_bounded_by_the_engine``.
"""

import unittest

from borg import (
    BorgValueError,
    Ref,
    bigint,
    binary,
    bool_,
    double,
    int_,
    list_,
    ref,
    string,
)


class TheTextFormsValuesTakeOnTheWire(unittest.TestCase):
    def test_every_type_round_trips_through_the_text_the_engine_renders(self):
        cases = [
            (string(), "acme.ai", "acme.ai"),
            (string(), "", ""),
            # Against a *declared* String there are no reserved spellings, so these are their
            # characters.
            (string(), "true", "true"),
            (string(), "42", "42"),
            (string(), "@jake", "@jake"),
            (int_(), 42, "42"),
            (int_(), -1, "-1"),
            (int_(), 0, "0"),
            (double(), 1.5, "1.5"),
            (double(), -0.25, "-0.25"),
            (bool_(), True, "true"),
            (bool_(), False, "false"),
            (bigint(), 2**127, "170141183460469231731687303715884105728n"),
            (bigint(), -129, "-129n"),
            (binary(), b"\xde\xad\xbe\xef", "0xdeadbeef"),
            (binary(), b"", "0x"),
            (ref("Company"), Ref("o-1234abcd"), "@o-1234abcd"),
            (list_(ref("Employee")), Ref("l-5678wxyz"), "@l-5678wxyz"),
        ]
        for declared, value, text in cases:
            with self.subTest(type=declared.wire_type, value=value):
                self.assertEqual(declared.encode(value), text)
                self.assertEqual(declared.decode(text), value)

    def test_a_whole_double_survives_the_engine_rendering_it_without_a_point(self):
        # Rust's f64 Display drops the `.0`, so this is what a Double cell reads back as — and this
        # SDK writes the same, so the round trip is a fixpoint rather than an oscillation.
        self.assertEqual(double().decode("1"), 1.0)
        self.assertEqual(double().encode(1.0), "1")
        self.assertEqual(double().decode("1e-7"), 1e-7)
        # A Double is not content-addressed, so past the point where the three languages spell large
        # magnitudes differently, all that matters is that each reads the others.
        self.assertEqual(double().decode(double().encode(1e30)), 1e30)
        self.assertEqual(double().decode("1" + "0" * 30), 1e30)

    def test_a_bigint_reads_with_or_without_its_suffix_and_always_writes_with_it(self):
        self.assertEqual(bigint().decode("-129"), -129)
        self.assertEqual(bigint().decode("-129n"), -129)
        self.assertEqual(bigint().encode(-129), "-129n")

    def test_the_declared_type_is_what_a_describe_payload_names(self):
        self.assertEqual(string().wire_type, "String")
        self.assertEqual(int_().wire_type, "Int")
        self.assertEqual(double().wire_type, "Double")
        self.assertEqual(bool_().wire_type, "Bool")
        self.assertEqual(binary().wire_type, "Binary")
        self.assertEqual(bigint().wire_type, "BigInt")
        self.assertEqual(ref("Employee").wire_type, "Employee")
        self.assertEqual(list_(ref("Employee")).wire_type, "Employee[]")
        self.assertEqual(list_(string()).wire_type, "String[]")


class WhatTheSdkRefusesRatherThanCorrupts(unittest.TestCase):
    def test_an_int_is_bounded_by_the_engine_rather_than_by_this_language(self):
        """
        The TypeScript SDK refuses an Int past 2⁵³, because a JS number is a double and everything
        beyond that is representable and wrong. Python's int is arbitrary precision, so the *rule* —
        never silently lose digits — lands where the engine's own boundary is, which is `i64`.

        Copying 2⁵³ here would refuse values the store holds perfectly well, on the strength of a
        limitation this language does not have.
        """
        self.assertEqual(int_().encode(9007199254740993), "9007199254740993")
        self.assertEqual(int_().decode("9007199254740993"), 9007199254740993)
        self.assertEqual(int_().encode(2**63 - 1), "9223372036854775807")

        with self.assertRaisesRegex(BorgValueError, r"bigint\(\)"):
            int_().encode(2**63)
        with self.assertRaisesRegex(BorgValueError, r"bigint\(\)"):
            int_().encode(-(2**63) - 1)

    def test_an_int_field_refuses_a_value_that_is_not_a_whole_number(self):
        with self.assertRaisesRegex(BorgValueError, "whole number"):
            int_().encode(1.5)
        with self.assertRaisesRegex(BorgValueError, "whole number"):
            int_().decode("1.5")
        with self.assertRaisesRegex(BorgValueError, "whole number"):
            int_().decode("acme")

    def test_a_bool_is_not_an_int_here_even_though_python_says_it_is(self):
        """
        `bool` subclasses `int`, so `True` would encode as `1` into an Int cell and read back as the
        number — a silent type change, in a table whose entire job is to prevent them. Nothing in
        the TypeScript SDK needs this check, and nothing in the protocol asks for it: it is one
        language's hazard, handled in that language's SDK.
        """
        with self.assertRaisesRegex(BorgValueError, "whole number"):
            int_().encode(True)
        with self.assertRaisesRegex(BorgValueError, "int"):
            bigint().encode(False)
        with self.assertRaisesRegex(BorgValueError, "finite number"):
            double().encode(True)
        with self.assertRaisesRegex(BorgValueError, "bool"):
            bool_().encode(1)

    def test_a_double_field_refuses_infinities_and_nan(self):
        with self.assertRaisesRegex(BorgValueError, "finite"):
            double().encode(float("nan"))
        with self.assertRaisesRegex(BorgValueError, "finite"):
            double().encode(float("inf"))
        # `float()` reads all three of these; the engine reads none of them.
        with self.assertRaisesRegex(BorgValueError, "finite"):
            double().decode("nan")
        with self.assertRaisesRegex(BorgValueError, "finite"):
            double().decode("infinity")
        with self.assertRaisesRegex(BorgValueError, "finite"):
            double().decode("")

    def test_a_string_field_refuses_the_tombstone_spelling_and_points_at_none(self):
        with self.assertRaisesRegex(BorgValueError, "pass None"):
            string().encode("~")

    def test_a_type_refuses_a_python_value_it_cannot_carry_and_says_what_it_got(self):
        with self.assertRaisesRegex(BorgValueError, "str"):
            bool_().encode("true")
        with self.assertRaisesRegex(BorgValueError, "str"):
            int_().encode("42")
        with self.assertRaisesRegex(BorgValueError, "bytes"):
            binary().encode([1, 2])
        with self.assertRaisesRegex(BorgValueError, "Ref"):
            ref("Company").encode("o-1")

    def test_a_malformed_answer_from_the_engine_is_an_error_not_a_value_that_looks_almost_right(self):
        with self.assertRaises(BorgValueError):
            bool_().decode("yes")
        with self.assertRaisesRegex(BorgValueError, "whole octets"):
            binary().decode("deadbeef")
        with self.assertRaisesRegex(BorgValueError, "whole octets"):
            binary().decode("0xf")
        with self.assertRaisesRegex(BorgValueError, "reference"):
            ref("Company").decode("o-1234abcd")

    def test_a_ref_holds_a_pid_and_not_its_wire_form(self):
        with self.assertRaisesRegex(BorgValueError, "drop the @"):
            Ref("@o-1234abcd")


class DerivedMarksTheFieldAndNothingElse(unittest.TestCase):
    def test_it_returns_a_new_type_so_a_shared_field_type_is_not_contaminated(self):
        shared = bool_()
        owned = shared.derived()
        self.assertTrue(owned.derived_field)
        self.assertFalse(shared.derived_field)
        self.assertEqual(owned.wire_type, shared.wire_type)
        self.assertEqual(owned.encode(True), "true")


class ARefNamesCells(unittest.TestCase):
    def test_it_builds_the_address_of_an_entity_and_of_one_of_its_fields(self):
        employee = Ref("o-1234abcd")
        self.assertEqual(employee.cell("Employee"), "Employee:o-1234abcd")
        self.assertEqual(employee.cell("Employee", "name"), "Employee:o-1234abcd.name")


if __name__ == "__main__":
    unittest.main()
