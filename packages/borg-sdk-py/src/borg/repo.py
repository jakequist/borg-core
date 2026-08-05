"""
The two modes of a repo module: describe itself, or serve invocations.

One artifact, two modes, matching the bash worker's shape. ``describe`` stays a plain ``argv[1] ==
"describe"`` invocation printing JSON to stdout — that call has no stream to corrupt, and keeping it
plain is what keeps a bash repo one ``jq -n``. Everything else is the worker loop.

## The SDK records nothing

Every ``get`` and every ``set`` below is a wire message, and the engine records the read-set
server-side (§9.4). There is no dependency tracking in this file, no cache, and no place to put one:
attribute access or a preload would only *translate* accesses, and a preload would translate the
wrong ones — an object-granular read-set instead of a field-granular one, which is the difference
scenario 030 exists to demonstrate. Tracking ships in no SDK, ever.

## Attribute access, and what it is doing here

:class:`EntityContext` answers ``c.headcount`` as well as ``c.get("headcount")``. It is four lines of
``__getattr__``, because a synchronous ``get`` is a plain function call, and it is here as **evidence
rather than as the surface**: the TypeScript SDK cannot offer the same thing without a worker thread
and ``Atomics.wait``, and the reason is entirely JavaScript's, not the protocol's. Both spellings
send the identical message, so nothing about the contract moves either way. ``get``/``set`` stay the
documented surface until the client contract freezes and the two SDKs can agree.
"""

from __future__ import annotations

import json
import sys
from typing import Any, Iterable, Sequence

from .connection import BorgProtocolError, Connection, connect
from .dsl import BorgDefinitionError, PipelineDef, StructDef, describe
from .protocol import DESCRIBE_ARG, producer_id
from .values import TOMBSTONE, FieldType

__all__ = ["EntityContext", "Repo", "World", "repo"]


class _Invocation:
    """One invocation's connection, and whether it is still the current one."""

    __slots__ = ("conn", "definition", "input", "live")

    def __init__(self, conn: Connection, definition: PipelineDef, input: str) -> None:
        self.conn = conn
        self.definition = definition
        self.input = input
        self.live = True

    def check(self) -> None:
        # Anything the body kept a reference to stops working the moment the invocation is over.
        # Without this, a stray call from a thread the body started would send a `get` in the middle
        # of the *next* invocation and take the reply belonging to it — one entity's value quietly
        # written to another.
        if not self.live:
            raise BorgProtocolError(
                f"this invocation of `{self.definition.name}` has already finished — "
                f"a get() or set() now would read another entity's answer"
            )

    def value(self, message: dict[str, Any]) -> str | None:
        self.check()
        reply = self.conn.request(message)
        if "value" not in reply:
            raise BorgProtocolError(f"expected a value from the engine, got {reply}")
        return _present(reply["value"])

    def ack(self, message: dict[str, Any]) -> None:
        self.check()
        reply = self.conn.request(message)
        if "ok" not in reply:
            raise BorgProtocolError(f"expected an acknowledgement from the engine, got {reply}")


class EntityContext:
    """
    The input entity of one invocation. Every access is a wire message; nothing is cached.

    ``c.get("headcount")`` returns the value — there is no future to await, because the read is a
    blocking socket round trip. See the module docstring for what ``c.headcount`` is doing here.
    """

    __slots__ = ("ref", "_at")

    _INTERNAL = frozenset(__slots__)

    def __init__(self, at: _Invocation) -> None:
        object.__setattr__(self, "_at", at)
        #: The entity's cell address, as the engine named it: ``Company:o-1234abcd``.
        object.__setattr__(self, "ref", at.input)

    def get(self, field: str) -> Any:
        """Read one of the entity's fields. Recorded server-side whether or not it exists."""
        at = self._at
        declared = _field_type(at.definition, field)
        text = at.value({"get": f"{at.input}.{field}"})
        return None if text is None else declared.decode(text)

    def set(self, field: str, value: Any) -> None:
        """Write one of the entity's fields. ``None`` deletes it."""
        at = self._at
        declared = _field_type(at.definition, field)
        if field not in at.definition.writes:
            # The engine would refuse this too, having validated the write against declared ownership
            # (§8) — but it would refuse it in the middle of a round, naming a producer rather than a
            # line of code. This is the same rule stated where the author can act on it.
            raise BorgDefinitionError(
                f"`{at.definition.name}` writes {', '.join(at.definition.writes)} and does not "
                f"declare `{field}` — add it to `writes` and mark the field derived()"
            )
        text = TOMBSTONE if value is None else declared.encode(value)
        at.ack({"set": {"cell": f"{at.input}.{field}", "value": text}})

    def __getattr__(self, field: str) -> Any:
        # Reached only when normal lookup fails, so `get`, `set` and `ref` are never shadowed.
        if field.startswith("_"):
            raise AttributeError(field)
        return self.get(field)

    def __setattr__(self, field: str, value: Any) -> None:
        if field in EntityContext._INTERNAL:
            object.__setattr__(self, field, value)
        else:
            self.set(field, value)

    def __repr__(self) -> str:
        return f"EntityContext({self.ref!r})"


class World:
    """
    Random access to anything else, for the hops a pipeline makes beyond its own entity.

    Stringly in v1: a cell is named by its text address, and a value by its text form unless a field
    type is supplied to convert it. Generated types are what will make this typed — the second
    argument is where they slot in, taking the same :class:`~borg.values.FieldType` the DSL already
    produces — so the shape is chosen now and only the source of the type changes later.
    """

    __slots__ = ("_at",)

    def __init__(self, at: _Invocation) -> None:
        self._at = at

    def get(self, cell: str, as_type: FieldType | None = None) -> Any:
        text = self._at.value({"get": cell})
        if text is None or as_type is None:
            return text
        return as_type.decode(text)

    def set(self, cell: str, value: Any, as_type: FieldType | None = None) -> None:
        if value is None:
            text = TOMBSTONE
        elif as_type is not None:
            text = as_type.encode(value)
        elif isinstance(value, str):
            text = value
        else:
            raise TypeError(
                f"world.set({cell}, …) takes text, or a value plus the field type to convert it with"
            )
        self._at.ack({"set": {"cell": cell, "value": text}})


class Repo:
    """A repo module: its definitions, and the worker that serves them."""

    __slots__ = ("_description", "_by_id")

    def __init__(
        self,
        id: int | None = None,
        structs: Iterable[StructDef] = (),
        pipelines: Iterable[PipelineDef] = (),
    ) -> None:
        defined = tuple(pipelines)
        # Validation happens now, at import, so a mistake fails `describe` as well as the worker loop
        # — one is a push-time error the author sees immediately, the other a mid-round failure they
        # would see much later.
        self._description = describe(structs, defined, id)
        # Producer id → pipeline. The engine invokes by id, and one module may implement several.
        self._by_id = {producer_id(p.name): p for p in defined}

    def describe(self) -> dict[str, Any]:
        """The ``describe`` payload this repo reports. Pure; useful in tests."""
        return self._description

    def main(self, argv: Sequence[str] | None = None) -> None:
        """
        Run as ``borg`` invokes it: ``describe``, or the worker loop.

        ``argv`` defaults to ``sys.argv[1:]``, so ``argv[0]`` here is what the engine passed as the
        process's first argument — ``describe`` or nothing.
        """
        argv = sys.argv[1:] if argv is None else argv
        if argv and argv[0] == DESCRIBE_ARG:
            # The one place this SDK writes to stdout, and the reason `describe` is a separate mode:
            # this process's *whole* stdout is the payload, so a `print()` at import time corrupts
            # it. That failure is immediate and quotes the offending text, which is the best outcome
            # available — the socket cannot help here, because there is no socket yet.
            sys.stdout.write(_payload(self._description) + "\n")
            sys.stdout.flush()
            return
        _serve(self._by_id)


def repo(
    id: int | None = None,
    structs: Iterable[StructDef] = (),
    pipelines: Iterable[PipelineDef] = (),
) -> Repo:
    """
    Define a repo.

    ``id`` is optional and cross-checked rather than used: ``borg.toml`` is authoritative, because a
    repo is a directory and one directory has one id however many modules it contains. Stating it
    here gets that copy verified at push time instead of quietly ignored.
    """
    return Repo(id=id, structs=structs, pipelines=pipelines)


def _payload(description: dict[str, Any]) -> str:
    """
    The describe payload, byte-for-byte as the TypeScript SDK emits it.

    ``separators`` because ``json.dumps`` pads by default and ``JSON.stringify`` does not;
    ``ensure_ascii=False`` because ``JSON.stringify`` emits UTF-8 rather than ``\\uXXXX`` escapes. The
    engine parses either, so this is not correctness — it is that two SDKs describing the same repo
    should produce the same bytes, or the difference is something a reader has to explain.
    """
    return json.dumps(description, separators=(",", ":"), ensure_ascii=False)


def _serve(by_id: dict[str, PipelineDef]) -> None:
    """The worker loop: handshake, then invocations until the engine says stop."""
    conn = connect()
    # The handshake is always JSON, because a codec cannot be encoded in one not yet agreed. JSON is
    # also all this SDK offers: MessagePack would buy a dependency, and the framing that makes a
    # shell worker possible is what makes this one dependency-free.
    hello = conn.receive()
    if hello is None:
        raise BorgProtocolError("the engine hung up before saying hello")
    conn.send({"codec": "json"})

    while True:
        message = conn.receive()
        if message is None or "shutdown" in message:
            break
        if "invoke" not in message:
            continue

        invocation = message["invoke"]
        try:
            # `str()` because the id arrives as a string and must stay one: it is past 2⁵³, and a
            # dispatch table keyed on a number would resolve a producer that does not exist.
            definition = by_id.get(str(invocation["producer"]))
            if definition is None:
                raise BorgDefinitionError(
                    f"the engine invoked producer {invocation['producer']}, "
                    f"which this repo does not implement"
                )
            _invoke(conn, definition, invocation["input"])
            conn.send({"done": {}})
        except Exception as err:  # noqa: BLE001 — see below
            # A pipeline that raises on one entity is not a broken process: the engine aborts that
            # invocation's layer and poisons the producer (§14), and the conversation is still in
            # step because every request above completed its reply before this could be reached.
            # `BaseException` is deliberately not caught — a KeyboardInterrupt is not one entity's
            # problem.
            conn.send({"error": {"message": _explain(err)}})
    conn.close()


def _invoke(conn: Connection, definition: PipelineDef, input: str) -> None:
    at = _Invocation(conn, definition, input)
    try:
        definition.run(EntityContext(at), World(at))
    finally:
        at.live = False


def _field_type(definition: PipelineDef, field: str) -> FieldType:
    declared = definition.source.fields.get(field)
    if declared is None:
        raise BorgDefinitionError(
            f"`{definition.source.name}` declares no field `{field}`"
        )
    return declared


def _present(text: str | None) -> str | None:
    """
    Absence, however the engine spelled it.

    A cell that has never been written answers ``null``; one explicitly deleted answers ``~`` (§8.1).
    The distinction is real in the store and survives on the wire, and it is collapsed here because a
    pipeline has nothing different to do with the two: both mean "there is no value at this cell",
    and ``decode`` would have to grow a tombstone case for every type to say so a second way. Writing
    ``None`` back is a tombstone, so the round trip is closed.
    """
    return None if text is None or text == TOMBSTONE else text


def _explain(err: BaseException) -> str:
    """
    Whatever was raised, as one line the engine can report.

    The exception's type is included, which the TypeScript SDK does not do: ``str(KeyError("website"))``
    is ``"'website'"`` and says nothing on its own, whereas an ``Error``'s message in JS is the whole
    of what the author wrote. The engine treats the string as opaque, so this costs nothing but
    reads far better in a ``borg producer list`` report.
    """
    message = str(err)
    return f"{type(err).__name__}: {message}" if message else type(err).__name__
