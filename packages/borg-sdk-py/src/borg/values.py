"""
Field types, and the text forms their values take on the wire.

**Values cross the wire as text** — ``42``, ``true``, ``~``, ``acme.ai``, ``@o-1234abcd`` — the same
forms the CLI accepts and ``borg_core::parse`` reads (SPEC §3.4, §17.4). A worker never sends a JSON
number: JSON has one number type and the engine has ``Int``, ``Double`` and ``BigInt``, so a number
on the wire would need a rule to disambiguate it and every language's rule would be slightly
different. Text has no such gap.

Each field type is therefore a pair of methods, ``decode`` and ``encode``, plus the name the declared
type goes by in a ``describe`` payload. Nothing here reaches the network, and nothing here remembers
anything: conversion is total and local.

## Where this deliberately differs from the TypeScript SDK

``int_()`` refuses what the *engine* cannot hold, which is an ``i64``. The TS SDK refuses at 2⁵³
because a JS number is a double and everything past that point is representable and wrong; Python's
``int`` is arbitrary precision, so the same rule — *never silently lose digits* — lands at the
engine's own boundary instead of at the language's. The rule is contract; the threshold was never
contract, and putting 2⁵³ here would refuse values the engine stores perfectly well.

The other direction of the same coin: ``int_()`` and ``bigint()`` carry the *identical* Python type.
The distinction between them is purely a statement about what the store holds, which is what a
declared type is supposed to be.
"""

from __future__ import annotations

import copy
import re
from typing import Any

__all__ = [
    "TOMBSTONE",
    "BorgValueError",
    "FieldType",
    "Ref",
    "bigint",
    "binary",
    "bool_",
    "double",
    "int_",
    "list_",
    "ref",
    "string",
]

#: The tombstone form, reserved on every declared type (§8.1).
TOMBSTONE = "~"

_INTEGER = re.compile(r"^[+-]?\d+$")
_OCTETS = re.compile(r"^0x(?:[0-9a-fA-F]{2})*$")

# `borg_core::parse` reads a declared `Int` with `i64::from_str`, so this is the range — the
# *engine's*, not this language's. See the module docstring.
_INT_MIN = -(2**63)
_INT_MAX = 2**63 - 1


class BorgValueError(Exception):
    """Something the SDK refused to convert, named with enough detail to fix it."""


class Ref:
    """A reference to another entity. On the wire, ``@`` and a PID."""

    __slots__ = ("pid",)

    def __init__(self, pid: str) -> None:
        if pid.startswith("@"):
            raise BorgValueError(
                f"a Ref holds a PID, not its wire form — drop the @ from `{pid}`"
            )
        self.pid = pid

    def __str__(self) -> str:
        """The wire form: ``@o-1234abcd``."""
        return f"@{self.pid}"

    def __repr__(self) -> str:
        return f"Ref({self.pid!r})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Ref) and other.pid == self.pid

    def __hash__(self) -> int:
        return hash((Ref, self.pid))

    def cell(self, struct: str, field: str | None = None) -> str:
        """The cell this reference names, so a pipeline can hop to one of its fields."""
        return f"{struct}:{self.pid}" if field is None else f"{struct}:{self.pid}.{field}"


class FieldType:
    """
    A declared field type: what it is called in ``describe``, and how its values convert.

    ``decode``/``encode`` never see absence. A cell that has never been written, and a cell holding a
    tombstone, both read as ``None`` and are written back as ``None``; that collapse happens one
    level up, in the pipeline context, because it is the same rule for every type (§8.1).
    """

    #: ``Int``, ``String``, ``Company``, ``Employee[]`` — what ``describe`` calls this.
    wire_type: str
    #: Whether a pipeline owns this field, rather than clients writing it (§8).
    derived_field: bool

    def __init__(self, wire_type: str) -> None:
        self.wire_type = wire_type
        self.derived_field = False

    def derived(self) -> FieldType:
        """Declare that a pipeline writes this field. Returns a new type; the receiver is unchanged."""
        owned = copy.copy(self)
        owned.derived_field = True
        return owned

    def decode(self, text: str) -> Any:
        raise NotImplementedError

    def encode(self, value: Any) -> str:
        raise NotImplementedError

    def __repr__(self) -> str:
        suffix = ".derived()" if self.derived_field else ""
        return f"{self.wire_type}{suffix}"

    def _refuse(self, reason: str) -> Any:
        raise BorgValueError(reason)


class _String(FieldType):
    def __init__(self) -> None:
        super().__init__("String")

    def decode(self, text: str) -> str:
        return text

    def encode(self, value: Any) -> str:
        if not isinstance(value, str):
            self._refuse(f"a String field takes a str, not {_what(value)}")
        # `~` is a tombstone on every declared type (§8.1), so a String field cannot hold those two
        # characters: the engine would read the write back as a deletion. Refusing beats writing a
        # value that does not read back, and `None` is how a pipeline means deletion here.
        if value == TOMBSTONE:
            self._refuse(
                "`~` is the tombstone form on every field, so it cannot be stored as a string — "
                "pass None to delete the cell"
            )
        return value


class _Int(FieldType):
    def __init__(self) -> None:
        super().__init__("Int")

    def decode(self, text: str) -> int:
        if not _INTEGER.match(text):
            self._refuse(f"an Int field answered `{text}`, which is not a whole number")
        return int(text)

    def encode(self, value: Any) -> str:
        # `bool` is a subclass of `int` in Python, so `True` would encode as `1` and land in an Int
        # cell without a word of complaint. That is a silent type change, which is the one thing this
        # table exists to prevent.
        if not isinstance(value, int) or isinstance(value, bool):
            self._refuse(f"an Int field takes a whole number, not {_what(value)}")
        if not _INT_MIN <= value <= _INT_MAX:
            self._refuse(
                f"{value} does not fit the i64 an Int field holds — declare the field bigint()"
            )
        return str(value)


class _Double(FieldType):
    def __init__(self) -> None:
        super().__init__("Double")

    def decode(self, text: str) -> float:
        try:
            value = float(text)
        except ValueError:
            value = float("nan")
        # `float()` also reads `nan`, `inf` and `infinity`, which the engine refuses (§3.4) and which
        # have no round-tripping text form anyway.
        if value != value or value in (float("inf"), float("-inf")):
            self._refuse(f"a Double field answered `{text}`, which is not a finite number")
        return value

    def encode(self, value: Any) -> str:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            self._refuse(f"a Double field takes a finite number, not {_what(value)}")
        value = float(value)
        if value != value or value in (float("inf"), float("-inf")):
            self._refuse(f"a Double field takes a finite number, not {_what(value)}")
        # `repr` is the shortest text that reads back as the same f64, which is also what Rust's
        # `f64` Display produces — except that Rust never uses exponent notation and Python does past
        # 1e16, and that Python writes the `.0` Rust drops. Only the `.0` is worth removing: it is
        # the case an author meets (`c.set("ratio", 1.0)`), and dropping it makes the text identical
        # to the engine's own rendering and to the TypeScript SDK's.
        #
        # The exponent difference is left alone deliberately. A `Double` is not content-addressed
        # (§3.1), so its text has only to parse — `f64::from_str` reads `1e+30` and `float()` reads
        # the thirty-one digits, and the value is the same f64 either way. The types where spelling
        # *is* identity — String, BigInt, Binary — have one form here and there.
        text = repr(value)
        return text[:-2] if text.endswith(".0") else text


class _Bool(FieldType):
    def __init__(self) -> None:
        super().__init__("Bool")

    def decode(self, text: str) -> bool:
        if text == "true":
            return True
        if text == "false":
            return False
        self._refuse(f"a Bool field answered `{text}`, which is neither true nor false")

    def encode(self, value: Any) -> str:
        if not isinstance(value, bool):
            self._refuse(f"a Bool field takes a bool, not {_what(value)}")
        return "true" if value else "false"


class _Binary(FieldType):
    def __init__(self) -> None:
        super().__init__("Binary")

    def decode(self, text: str) -> bytes:
        if not _OCTETS.match(text):
            self._refuse(f"a Binary field answered `{text}`, which is not `0x` and whole octets")
        return bytes.fromhex(text[2:])

    def encode(self, value: Any) -> str:
        if not isinstance(value, (bytes, bytearray, memoryview)):
            self._refuse(f"a Binary field takes bytes, not {_what(value)}")
        return "0x" + bytes(value).hex()


class _BigInt(FieldType):
    def __init__(self) -> None:
        super().__init__("BigInt")

    def decode(self, text: str) -> int:
        # The engine renders a BigInt with a trailing `n`, which is what tells an *untyped* parse a
        # BigInt from an Int. Against a declared BigInt it carries nothing, so both spellings read.
        digits = text[:-1] if text.endswith("n") else text
        if not _INTEGER.match(digits):
            self._refuse(f"a BigInt field answered `{text}`, which is not decimal digits")
        return int(digits)

    def encode(self, value: Any) -> str:
        if not isinstance(value, int) or isinstance(value, bool):
            self._refuse(f"a BigInt field takes an int, not {_what(value)}")
        # Written with the suffix even though a declared BigInt does not need it: the same text then
        # means the same value on an `Any` field, where the suffix is the only thing distinguishing
        # it from an Int.
        return f"{value}n"


class _Ref(FieldType):
    def decode(self, text: str) -> Ref:
        if not text.startswith("@"):
            self._refuse(f"a {self.wire_type} field answered `{text}`, which is not a reference")
        return Ref(text[1:])

    def encode(self, value: Any) -> str:
        if not isinstance(value, Ref):
            self._refuse(f"a {self.wire_type} field takes a Ref, not {_what(value)}")
        return str(value)


def string() -> FieldType:
    return _String()


def int_() -> FieldType:
    """An ``i64``. Named with a trailing underscore because ``int`` is a builtin (PEP 8)."""
    return _Int()


def double() -> FieldType:
    return _Double()


def bool_() -> FieldType:
    """A ``Bool``. Trailing underscore for the same reason as :func:`int_`."""
    return _Bool()


def binary() -> FieldType:
    return _Binary()


def bigint() -> FieldType:
    return _BigInt()


def ref(struct: str) -> FieldType:
    """A reference to an entity of a named struct."""
    return _Ref(struct)


def list_(element: FieldType) -> FieldType:
    """
    A list field. Its *value* is a reference to a list, not the elements: elements are cells of their
    own (``Employee[]:l-….[3]``), which is what makes a list appendable without rewriting it (§4.2).

    v1 goes no further than the handle. Reading through it needs element addressing in the pipeline
    surface, and ``hasMany`` — a derived reverse index — needs aggregations, deferred to §18.
    """
    return _Ref(f"{element.wire_type}[]")


def _what(value: Any) -> str:
    """What the caller actually passed, for an error message that saves a debugging session."""
    if value is None:
        return "None (pass None through set() to delete the cell)"
    if isinstance(value, (str, int, float, bool)):
        return f"{type(value).__name__} `{value}`"
    return f"a {type(value).__name__}"
