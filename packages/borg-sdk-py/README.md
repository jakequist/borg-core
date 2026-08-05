# borg-sdk (Python)

Author a Borg repo in Python: declare structs, write pipelines, serve them to the engine.

This package exists as a **neutrality gate**. The TypeScript SDK (`packages/borg-sdk`) came first and
is the reference; this one asks whether anything in the `describe`/invoke contract turned out to be
TypeScript-shaped. What it found is in `SDK-DRAFT.md` §4.2 — the short version is on this page under
*What the gate found*.

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
    c.set("isInvestible", website is not None and website.endswith(".ai") and (headcount or 0) > 10)


borg.repo(id=1, structs=[Company], pipelines=[invest]).main()
```

Drop that in `pipelines/` inside a repo directory, `chmod +x` it, and `borg repo push` it. The module
is the whole repo: run with `describe` it prints its definitions, run without it serves invocations.

## There is no `await`, and that is the deliverable

The TypeScript SDK spells every access `await c.get("headcount")`. Here it is `c.get("headcount")`
and it returns the value, because the read is a blocking socket round trip.

**This asymmetry is not a bug to be reconciled.** It is the gate reporting that the verbosity was
JavaScript's and not the protocol's. What the two SDKs do is identical: one wire message per access,
nothing preloaded, nothing cached, the read-set recorded server-side. The `async` in the TS surface
buys concurrency this language does not need and does not change one byte on the wire.

The same reading applies to attribute access. `c.headcount` and `c.isInvestible = True` work, and
send exactly the messages `get` and `set` send:

```python
@borg.pipeline("invest", Company, writes=["isInvestible"])
def invest(c):
    c.isInvestible = (c.headcount or 0) > 10
```

That is four lines of `__getattr__`. In TypeScript the same sugar needs a worker thread and
`Atomics.wait`, which is why `SDK-DRAFT.md` §2.1 defers it. **`get`/`set` remain the documented
surface in both SDKs** — a pipeline should read the same in either language until the client contract
freezes — but the attribute form is kept, and tested, because it is the gate's evidence: anything
trivial in one language and hard in the other is, by construction, not contract.

## The three things worth knowing

**The SDK records nothing.** Every `get` and `set` is a wire message, and the engine records the
read-set server-side. That is what makes invalidation field-granular — write a field the pipeline
never read and nothing re-runs — without a line of tracking code here.

**Ownership is stated twice and checked both ways.** `.derived()` on the field says a pipeline owns
it; `writes=[...]` on the pipeline says which one. Assembling the description errors if they disagree
in either direction, at import time, so both fail at push rather than mid-round.

**Field names are used verbatim.** `isInvestible` in the DSL is `Company#1.isInvestible` at the CLI.
Nothing is converted to `snake_case`, however much a Python SDK would like to: a silent mapping is
one somebody has to reverse-engineer from an error message, and it would make the same repo mean two
different schemas depending on which language pushed it.

## Values cross the wire as text

`42`, `true`, `~`, `acme.ai`, `@o-1234abcd` — the same forms the CLI accepts. The SDK converts:

| Field type    | Python value | Wire text        | Notes                                                |
| ------------- | ------------ | ---------------- | ---------------------------------------------------- |
| `string()`    | `str`        | the text itself  | no reserved spellings, except `~` — see below         |
| `int_()`      | `int`        | `42`, `-1`       | refuses anything outside `i64`; use `bigint()`        |
| `double()`    | `float`      | `1.5`, `1`       | refuses `nan` and infinities, as the engine does      |
| `bool_()`     | `bool`       | `true` / `false` | refuses an `int`, which Python would otherwise accept |
| `binary()`    | `bytes`      | `0xdeadbeef`     | whole octets only                                     |
| `bigint()`    | `int`        | `-129n`          | reads with or without the suffix, writes with it      |
| `ref(N)`      | `Ref`        | `@o-1234abcd`    |                                                       |
| `list_(T)`    | `Ref`        | `@l-5678wxyz`    | the handle; element access is not in v1               |

The trailing underscores are PEP 8's spelling for a name that would shadow a builtin. `string`,
`double`, `binary`, `bigint`, `ref` need none, so they have none — a uniform `int_`/`string_` would
be tidier and would also be wrong Python.

Three rules that are easy to get wrong and are therefore enforced:

- **`None` is absence in both directions.** A cell never written and a cell holding a tombstone both
  read as `None`; writing `None` writes a tombstone. The store distinguishes the two and a pipeline
  has nothing different to do with them.
- **`int_()` refuses what the *engine* cannot hold.** The TS SDK refuses past 2⁵³ because a JS number
  is a double; Python's `int` is unbounded, so the same rule — never silently lose digits — lands at
  the engine's `i64` instead. Copying 2⁵³ here would refuse values the store keeps perfectly well.
- **A `bool` is not an `int` here.** Python says it is, so `int_().encode(True)` would write `1` into
  an `Int` cell without complaint. That is a silent type change, in the one table whose job is to
  prevent them.

`world.get(cell)` / `world.set(cell, value)` are the random-access hops beyond the input entity, and
are stringly in v1: a cell is its text address, a value is its text form unless you pass a field type
to convert with (`world.get(cell, borg.int_())`). Generated types slot into that second argument.

## The socket, and why your stdout is yours

The worker protocol can run over a worker's own stdin and stdout, and the shell pipelines do. That is
not survivable here: one `print()` — yours, a library's, a `DeprecationWarning` — would corrupt the
stream, and the failure would surface far from its cause.

So a repo written with this SDK declares `"transport": "socket"` in its `describe` output. The engine
reads that *before* it spawns anything, listens on a unix socket, and passes the path in
`BORG_WORKER_SOCKET`. The protocol lives on that descriptor and **stdout is entirely yours**. The
engine points a worker's stdout at its own stderr, so what you print is visible and can never be
mistaken for a message or corrupt the CLI's own output.

The one exception is `describe` mode, where this process's whole stdout *is* the payload: a `print()`
at import time corrupts it. That fails immediately, quoting the offending text, which is the best
outcome available — there is no socket yet, and cannot be.

## What the gate found

In full in `SDK-DRAFT.md` §4.2. In one line each:

- **Nothing in the protocol was harder in Python.** Same describe payload, byte for byte; same
  messages; same handshake; zero changes to any Rust crate.
- **Two pieces of the TS SDK turned out to be ergonomics, not contract**: the promise chain that
  serialises requests (nothing in synchronous Python can overlap two), and `await` itself.
- **One number in `SDK-DRAFT.md` §4.1 reads as contract and is not**: "`int()` refuses values past
  2⁵³". The contract is *never round an integer*; 2⁵³ is JavaScript's boundary, `i64` is the
  engine's.
- **Python needs one check TypeScript does not**: `bool` is an `int` here.

## Development

```
PYTHONPATH=src python3 -m unittest discover -s tests     # no installer needed
pytest                                                   # same tests, if you have pytest
```

The tests are `unittest` cases, which pytest discovers and runs natively. That is deliberate: the
suite then runs on any machine with Python 3.11+, with no `pip`, no virtualenv and no network — which
is the same property that lets `check.sh` run the Rust tests everywhere. `pytest` is the nicer
runner and is declared in `[project.optional-dependencies].dev`; nothing requires it.

Python 3.11+. **Zero runtime dependencies.**
