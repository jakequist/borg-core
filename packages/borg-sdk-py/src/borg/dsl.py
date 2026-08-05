"""
The author-side DSL: structs, pipelines, and the repo they make up.

This half is pure declaration — nothing here opens a connection or reads a cell. What it produces is
a ``describe`` payload, which is the DSL's compile target and the whole of the contract between an
SDK and ``borg repo push`` (§17.4). A repo that describes itself needs no new engine surface.

## Ownership is explicit, and checked in both directions

``borg.bool_().derived()`` says a pipeline owns the field; ``writes=["isInvestible"]`` says which
one. Neither is inferred from the other, and assembling the description errors if they disagree — a
``derived()`` field nobody writes is a field no client may write either and no pipeline ever will,
and a ``writes`` naming a field that is not ``derived()`` is a write the engine will refuse at the
first invocation. Both are static facts, so both are push-time errors rather than runtime ones.

Inference is later sugar, and would happen here rather than at runtime.
"""

from __future__ import annotations

import inspect
import re
from typing import Any, Callable, Iterable, Mapping, Sequence

from .protocol import TRANSPORT
from .values import FieldType

__all__ = [
    "BorgDefinitionError",
    "PipelineDef",
    "StructDef",
    "describe",
    "pipeline",
    "struct",
]

_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class BorgDefinitionError(Exception):
    """A repo the DSL rejected before it ever reached the engine."""


class StructDef:
    """A struct, as this repo declares it."""

    __slots__ = ("name", "fields")

    def __init__(self, name: str, fields: Mapping[str, FieldType]) -> None:
        self.name = name
        # Copied so a caller mutating the mapping afterwards cannot change what was described, and
        # ordered because a dict is: `describe` emits fields in the order they were written.
        self.fields = dict(fields)

    def __repr__(self) -> str:
        return f"StructDef({self.name!r}, {list(self.fields)!r})"


def struct(name: str, fields: Mapping[str, FieldType]) -> StructDef:
    """
    Declare a struct. Names are used exactly as written, here and on the wire: a ``headcount`` in the
    DSL is ``Company#1.headcount`` at the CLI. No case is converted — not to ``snake_case`` either,
    which is what a Python SDK would be tempted to do. A mapping that is invisible in both directions
    is one somebody eventually has to reverse-engineer from an error message, and it would also mean
    the same repo pushed from two languages declared two different schemas.
    """
    if not _NAME.match(name):
        raise BorgDefinitionError(f"`{name}` is not a usable struct name")
    if not fields:
        raise BorgDefinitionError(f"struct `{name}` declares no fields")
    for field, declared in fields.items():
        if not isinstance(declared, FieldType):
            raise BorgDefinitionError(
                f"`{name}.{field}` is {declared!r}, not a field type — "
                f"declare it with borg.string(), borg.int_(), and so on"
            )
    return StructDef(name, fields)


class PipelineDef:
    """A producer that maps over one struct, one entity at a time (§4.2)."""

    __slots__ = ("name", "source", "writes", "body", "wants_world")

    def __init__(
        self,
        name: str,
        source: StructDef,
        writes: Sequence[str],
        body: Callable[..., Any],
    ) -> None:
        self.name = name
        self.source = source
        self.writes = tuple(writes)
        self.body = body
        # Decided once, here, rather than on every invocation. A JS body may ignore the second
        # argument by simply not naming it; Python raises, so the arity is read from the signature.
        self.wants_world = _takes_world(body)

    def run(self, entity: Any, world: Any) -> None:
        if self.wants_world:
            self.body(entity, world)
        else:
            self.body(entity)

    def __repr__(self) -> str:
        return f"PipelineDef({self.name!r}, {self.source.name!r}, writes={list(self.writes)!r})"


def _takes_world(body: Callable[..., Any]) -> bool:
    try:
        signature = inspect.signature(body)
    except (TypeError, ValueError):  # a builtin or a C callable: hand it everything
        return True
    positional = 0
    for parameter in signature.parameters.values():
        if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
            return True
        if parameter.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            positional += 1
    return positional >= 2


def pipeline(
    name: str,
    source: StructDef,
    writes: Sequence[str],
    body: Callable[..., Any] | None = None,
) -> Any:
    """
    Declare a pipeline. Usable as a decorator or as a call::

        @borg.pipeline("invest", Company, writes=["isInvestible"])
        def invest(c, world):
            c.set("isInvestible", c.get("headcount") is not None)

        invest = borg.pipeline("invest", Company, ["isInvestible"], score_it)

    **The body is synchronous, and that is the whole of the difference from the TypeScript SDK.**
    ``c.get("headcount")`` blocks on a socket read and returns the value. Every one is still a round
    trip whose result the engine records as a dependency — which is what makes invalidation
    field-granular — and nothing is preloaded, because preloading would collapse the server-recorded
    read-set to object granularity and cost exactly the property scenario 030 demonstrates. TS spells
    that ``await``; Python spells it nothing at all. The verbosity was never the contract.

    The body may take ``(c)`` or ``(c, world)``; ``world`` is random access to any other cell.
    """
    if not _NAME.match(name):
        raise BorgDefinitionError(f"`{name}` is not a usable pipeline name")

    def define(function: Callable[..., Any]) -> PipelineDef:
        return PipelineDef(name, source, writes, function)

    return define if body is None else define(body)


def describe(
    structs: Iterable[StructDef],
    pipelines: Iterable[PipelineDef],
    id: int | None = None,
) -> dict[str, Any]:
    """
    Assemble the ``describe`` payload, refusing anything the engine would refuse later — or worse,
    would accept and leave unwritable.

    ``id`` shadows the builtin deliberately: it is the keyword an author writes in ``borg.repo(id=1,
    …)``, and matching ``borg.toml``'s spelling matters more here than avoiding the shadow inside one
    function body.
    """
    by_name: dict[str, StructDef] = {}
    for definition in structs:
        if definition.name in by_name:
            raise BorgDefinitionError(
                f"struct `{definition.name}` is declared twice in this repo"
            )
        by_name[definition.name] = definition

    # Which pipeline claims which field. Built first, because the two cross-checks below are its two
    # directions and both need the whole map.
    owners: dict[str, str] = {}
    ordered: list[PipelineDef] = []
    seen: set[str] = set()
    for p in pipelines:
        if p.name in seen:
            raise BorgDefinitionError(f"pipeline `{p.name}` is declared twice in this repo")
        seen.add(p.name)
        ordered.append(p)

        if by_name.get(p.source.name) is not p.source:
            raise BorgDefinitionError(
                f"pipeline `{p.name}` maps over `{p.source.name}`, which this repo does not list "
                f"in `structs` — a producer's source struct has to be declared by the repo that "
                f"implements it"
            )
        if not p.writes:
            raise BorgDefinitionError(
                f"pipeline `{p.name}` declares no `writes`, so nothing could ever invoke it"
            )

        for field in p.writes:
            what = f"{p.source.name}.{field}"
            declared = p.source.fields.get(field)
            if declared is None:
                raise BorgDefinitionError(
                    f"pipeline `{p.name}` writes `{what}`, which `{p.source.name}` does not declare"
                )
            # Direction one: a claim on a field nobody marked derived. The engine validates every
            # write against declared ownership (§8), so this pipeline would be refused at its first
            # invocation.
            if not declared.derived_field:
                raise BorgDefinitionError(
                    f"pipeline `{p.name}` writes `{what}`, which is not declared derived() — "
                    f"add .derived() to the field, or drop it from `writes`"
                )
            already = owners.get(what)
            if already is not None:
                # Single writer per field is what lets derived layers commit concurrently without
                # conflicting (§16.3). Two claims is not a merge, it is a design mistake.
                raise BorgDefinitionError(
                    f"`{what}` is written by both `{already}` and `{p.name}` — "
                    f"a field has one writer"
                )
            owners[what] = p.name

    specs: list[dict[str, Any]] = []
    for definition in by_name.values():
        fields: list[dict[str, Any]] = []
        for field, declared in definition.fields.items():
            what = f"{definition.name}.{field}"
            owner = owners.get(what)
            # Direction two: a field declared derived that nothing implements. Clients may not write
            # it (§8) and no producer ever will, so it is a cell that can only ever be empty.
            if declared.derived_field and owner is None:
                raise BorgDefinitionError(
                    f"`{what}` is declared derived() but no pipeline in this repo writes it — "
                    f"add it to a pipeline's `writes`, or drop .derived()"
                )
            spec: dict[str, Any] = {"name": field, "type": declared.wire_type}
            if owner is not None:
                spec["derived_by"] = owner
            fields.append(spec)
        specs.append({"name": definition.name, "fields": fields})

    description: dict[str, Any] = {
        "structs": specs,
        "producers": [{"name": p.name, "source": p.source.name} for p in ordered],
        # Declared, so the engine knows before it spawns anything that this worker's stdout is its
        # own. Never detected — a detector would have to tell "has not connected yet" from "printed
        # to stdout first", which is the case the socket exists to make harmless (§17.4).
        "transport": TRANSPORT,
    }
    if id is not None:
        description["repo"] = id
    return description
