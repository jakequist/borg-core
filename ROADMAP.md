# Roadmap and decision log

`SPEC.md` says what the system is. This says what we are building next, and records decisions taken
in design conversation that would otherwise live only in someone's memory.

---

## Where we are

The **derivation half** of Borg works and is proven: field-granular invalidation through multi-hop
random access, migrations as producers, branching and merge, guards doubling as the merge-conflict
detector, definitions travelling the log, a SQLite backend behind a clean seam, and pipelines running
as subprocesses over a wire protocol — demonstrated by a pipeline written in bash.

The **definition half is decorative.** Definitions can be pushed, folded and read, but the write path
never consults them. `ValueType` has never rejected a write. And there is no way to write a
`String`, `Binary` or `BigInt` at all, because content-addressed interning was never built.

That gap is what the next three milestones close, and it is why "Act 1 is the modern ORM" is not yet
true.

---

## Milestones

### A — values become real

Content-addressed interning for `String`, `Binary` and `BigInt`: the hashing, the buffers, and
support in the CLI and the wire protocol.

Everything is blocked behind this. No realistic scenario can exist while every field is an integer —
the spec's own motivating example is `company.website.ends_with('.ai')`, which is unwritable today.
Mostly filling in a shape already drawn: `PidKind::String` exists, `StringBuffer` is specified,
§3.1 defines the hashing rule.

### A′ — the PID text form

A lossless, human-usable text form for PIDs, and a cell syntax built on it:

```
Company:o-1234abcd.website     an object property
Company:o-1234abcd             an existence cell
Founder[]:l-5678wxyz           a list's own cell (its length)
Founder[]:l-5678wxyz[0]        a list element
```

The current `Company#100` form is **lossy** — a PID is `(kind, branch, allocator, counter)` and it
carries one of the four. That is why shorthand has to allocate against the root, and why scenario 010
found a real bug. Encoding the whole PID fixes it properly.

Goes with A because both touch the value and PID text layer; doing them together is one round of
scenario churn instead of two.

**Done.** The codec is in `borg-core/src/pid.rs` and the form is normative in `SPEC.md` §3.1 and
§4.1: LEB128 varints for an allocated PID, all 32 bytes for a content hash, Crockford base32, and
the kind letters `o l a j y m s b n`. `Company#100` survives as input-only shorthand.

### B — definitions become load-bearing

Writes validate against the def view: unknown struct or field rejected, type mismatch rejected, and
`origin` enforced so a client cannot write a derived field.

This connects the two halves. It will break every existing scenario, because none of them declare
anything — that is the right kind of breakage, and the scenarios should end up looking like real
usage.

Includes **repos emitting their own definitions** (see decisions below) and two branch-visibility
tests that nothing currently covers.

### C — migrations end to end, through the CLI

`MutateField` already carries `up`/`down` producer ids, but there is no way to supply an
implementation. With A and B done, a migration is just another script in the repo, and
`borg repo push` already knows how to turn a script into a producer definition.

This unlocks §18's first acceptance scenario, which has **never run**: fork, change a field's type
with a migration, read old data correctly through the new lens on the child while the parent is
untouched, then def-only merge and watch the parent's values migrate.

It is the most valuable demo in the project — the thing no other ORM does, that the whole system was
designed around, and that we have never seen work.

### D — background derivation, and concurrency

Run derivation continuously instead of on demand; make `FreshnessRequirement::Current` actually
compute inline; add `frontier.reaches()`; add the branch-scoped pause switch.

Concurrency is folded in here because it is the same work: a background loop is exactly where you
parallelise, and `settle()`'s round-ceiling is what both need reworked. Deferring it this long is a
deliberate bet that the sequential assumption lives in one function — it should not slide past C.

### Deferred, still

Aggregations, `Set`/`Map`, container isolation, generated SDKs. Nothing has argued for pulling any of
them forward, and the CLI is doing the SDK's job well enough to keep learning from it first.

---

## Decisions

Design decisions taken in conversation, with the reasoning. Where these change the spec, the spec is
the normative statement — this records *why*.

### Cell syntax uses a colon, not parentheses

`Company:o-1234abcd.website`. Parentheses read well but are shell metacharacters, and we have taken
a deliberately shell-first stance on the worker protocol. The colon buys the same readability while
staying shell-safe by construction.

`Company#1` remains accepted **on input only**, as a documented convenience for hand-authored data,
meaning "root branch, allocator 0, counter 1". Output is always canonical.

### Field ownership is declared, not discovered

§8 originally said ownership is discovered at runtime. Once B lands, every write must name a declared
field — so a producer's output field must be declared too, and the only thing that knows it exists is
the repo implementing the producer.

What we ruled out earlier was *derivation writing back into defs*, which would mean the engine
emitting def events. An author declaring ownership up front is different and strictly better: a
violation is caught on the **first** wrong write rather than on a second producer's collision.
Runtime enforcement becomes a check against the declaration rather than the mechanism.

### Repos emit their own definitions

`describe` should return repo identity, struct definitions and producers together, and
`borg repo push` folds all of it into **one def layer** — a producer and the field it writes should
land together or not at all.

This is not a convenience. After B, a producer cannot write anything unless its output field is
declared, and the repo is the only thing that knows. It also sets up the DSL story: a Python repo
defines structs through the SDK, the runtime emits them on `describe`, and `defs/*.json` becomes one
way of producing the same thing rather than a parallel path.

### Auto-derivation is a branch-scoped switch

Default on, but pausable per branch — useful for deterministic testing and for freezing automation in
an emergency.

Two calls:

- **It is operational config, not log data.** Pausing does not change what is true, only when the
  system catches up. It lives beside the store like the producer-implementation table. In the log it
  would be branchable and time-travellable, which sounds elegant and is meaningless — nobody wants to
  ask "was derivation paused at layer 400?".
- **Pause means "do not auto-derive", not "refuse to derive".** `borg derive` still works on a paused
  branch. That is what makes it useful in an emergency: freeze the automation, then step it manually.

Per-*producer* pausing is skipped. The broken-producer case is already covered by producer-scoped
`IllegalState`, and "expensive but not broken" is a scheduling-policy problem better solved properly
than with a second switch.

**Pausing is self-documenting.** A paused branch's frontier stops advancing, and every read of
derived data already reports `stale` with a watermark showing how far behind. No new vocabulary
needed — a pause *is* lag, and the freshness envelope already describes lag.

### Producer implementations resolve outside the log

The log records that producer P exists; a sidecar table maps its id to a command. Writing a local
path into the log would tie the data model to one machine's filesystem. A container runtime keeps an
image reference in exactly the same place.

### Writing a property implies the object exists

Producers map over a struct's `ObjectBuffer`, which holds existence cells, so an object whose fields
were set but which was never explicitly created is invisible to every pipeline.

Only when absent, never on every write: the existence cell lives in the buffer producers subscribe
to, so rewriting it would make any property write look like a new entity.

---

## Tests we owe

- **Branch visibility of definitions.** Fork, define `Company` on the fork, assert main can neither
  see it nor write to it until merged. The read side is covered today; the *write rejection* side
  becomes possible only after B.
- **Second-order forks.** A fork of a fork sees `Company`, adds `Company.founded`, and that field is
  invisible to both main and the first fork. **Nothing in the codebase exercises a branch chain
  deeper than one fork** — `read_path` walks arbitrary depth and should handle it, which is exactly
  the kind of "should" that deserves a test.
- **Unit coverage generally.** Tests are almost entirely integration-level. `borg-storage`,
  `borg-engine`'s internals and the CLI have essentially none of their own. Landing alongside each
  milestone rather than as a separate push.
