"""
# borg

Author a Borg repo in Python: declare structs, write pipelines, and serve them to the engine.

```python
#!/usr/bin/env python3
import borg

Company = borg.struct("Company", {
    "website": borg.string(),
    "headcount": borg.int_(),
    "isInvestible": borg.bool_().derived(),
})


@borg.pipeline("invest", Company, writes=["isInvestible"])
def invest(c):
    website = c.get("website")
    headcount = c.get("headcount")
    c.set("isInvestible", website is not None and website.endswith(".ai")
          and (headcount or 0) > 10)


borg.repo(id=1, structs=[Company], pipelines=[invest]).main()
```

The module is the whole repo: run with `describe` it prints its definitions, run without it serves
invocations over the socket the engine offers in `BORG_WORKER_SOCKET`.

## There is no `await` here, and that is the point

This SDK exists as a **neutrality gate**. The TypeScript SDK's pipeline bodies are `async` and every
access is `await c.get("headcount")`; here the same access is `c.get("headcount")` and returns the
value. The asymmetry is not an inconsistency to be fixed — it is the gate reporting that the
verbosity was JavaScript's, not the protocol's. What the two SDKs *do* is identical: one wire message
per access, no preload, no cache, and the read-set recorded server-side.

The same reading applies to `c.headcount`, which works (see `borg.repo`). Attribute-mediated access
needs a worker thread and `Atomics.wait` in TypeScript and four lines of `__getattr__` here. Anything
that is hard in one language and trivial in the other is, by construction, not contract.

## The three things worth knowing

**The SDK records nothing.** Every `get` and `set` is a wire message and the engine records the
read-set server-side. That is what makes invalidation field-granular — write a field the pipeline
never read and nothing re-runs — without a line of tracking code here.

**Ownership is stated twice and checked both ways.** `.derived()` on the field says a pipeline owns
it; `writes=[...]` on the pipeline says which one. Assembling the description errors if they disagree
in either direction, at import time, so both fail at push rather than mid-round.

**Field names are used verbatim.** `isInvestible` in the DSL is `Company#1.isInvestible` at the CLI.
Nothing is converted to `snake_case` on the way out, however much a Python SDK would like to: a
silent mapping is one somebody has to reverse-engineer from an error, and it would make the same repo
mean two different schemas depending on which language pushed it.

Zero runtime dependencies: `socket` and `json` from the standard library are the whole of the
transport.
"""

from __future__ import annotations

from .connection import SOCKET_ENV, BorgProtocolError, Connection, connect
from .dsl import BorgDefinitionError, PipelineDef, StructDef, describe, pipeline, struct
from .protocol import DESCRIBE_ARG, TRANSPORT, VERSION, producer_id
from .repo import EntityContext, Repo, World, repo
from .values import (
    TOMBSTONE,
    BorgValueError,
    FieldType,
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

__all__ = [
    # The DSL, which is what a pipeline file uses.
    "struct",
    "pipeline",
    "repo",
    "string",
    "int_",
    "double",
    "bool_",
    "binary",
    "bigint",
    "ref",
    "list_",
    # The types and errors those produce.
    "BorgDefinitionError",
    "BorgProtocolError",
    "BorgValueError",
    "Connection",
    "EntityContext",
    "FieldType",
    "PipelineDef",
    "Ref",
    "Repo",
    "StructDef",
    "World",
    # The protocol, for anything speaking it directly.
    "DESCRIBE_ARG",
    "SOCKET_ENV",
    "TOMBSTONE",
    "TRANSPORT",
    "VERSION",
    "connect",
    "describe",
    "producer_id",
]

__version__ = "0.1.0"
