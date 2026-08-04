# Roadmap and decision log

`SPEC.md` says what the system is. This says what we are building next, and records decisions taken
in design conversation that would otherwise live only in someone's memory.

---

## Where we are

The **derivation half** of Borg works and is proven: field-granular invalidation through multi-hop
random access, migrations as producers, branching and merge, guards doubling as the merge-conflict
detector, definitions travelling the log, a SQLite backend behind a clean seam, and pipelines running
as subprocesses over a wire protocol — demonstrated by a pipeline written in bash.

Values are real: `String`, `Binary` and `BigInt` are interned on the way in and resolved on the way
out, invisibly, so a pipeline can finally read `company.website.ends_with('.ai')`.

The **definition half is now load-bearing.** Every cell write — from the CLI, from a producer, from
anywhere — is validated against the def-view of its branch, ownership is declared rather than
discovered, value parsing is directed by the declared type, and a repo emits its own struct
definitions alongside its producers.

**Migrations run end to end.** A repo declares a field mutation and the scripts that bridge it; the
def change and its migrations land in one layer; old data reads through the new lens on a fork while
the parent is untouched; a def-only merge carries both and the parent's values follow; and a client
authored against the old schema goes on reading and writing the old shape — or is told plainly that
it is broken, when the push supplied no way back.

**Derivation runs without being asked.** A write catches its branch up on the way out, a read can pay
to be current at the call site instead of taking the lag, a client can wait for the frontier to reach
its own write, and a report can read the whole branch at the point everything agrees. The automation
is pausable per branch, and a paused branch reports its lag in the vocabulary that already existed.

What remains before "Act 1 is the modern ORM" is true: concurrency — the second half of D.

---

## Milestones

### A — values become real

Content-addressed interning for `String`, `Binary` and `BigInt`: the hashing, the storage, and
support in the CLI and the wire protocol.

Everything was blocked behind this. No realistic scenario could exist while every field was an
integer — the spec's own motivating example is `company.website.ends_with('.ai')`.

**Done.** Interning existed in storage and nothing called it; it is now wired to the client surface
end to end. The value text form is normative in `SPEC.md` §3.4 and the `BigInt` byte encoding in
§3.1. Three decisions came out of it, below. `scenarios/050-values` is the proof, and 030's bash
pipeline now reads a real string and a real number.

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
ownership enforced so a client cannot write a derived field.

This connects the two halves. It broke every existing scenario, because none of them declared
anything — the right kind of breakage, and the scenarios now look like real usage.

**Done.** `FieldDef.origin` became `Ownership::{Source, Derived(ProducerId)}` and `DeclareField`
carries it, so a derived field is declarable at all for the first time;
`DependencyIndexProvider::writer_of` and the state behind it are gone. Enforcement lives in
`borg_engine::write::WriteSession` — one open layer plus the branch's def-view, with
`LayerHandle::put` made crate-private so there is no second door. Parsing is type-directed
(`parse::value_as`), which lifts both reservations §3.4 recorded as temporary. `describe` returns
structs as well as producers, and `borg repo push` folds them into one def layer. §5.1, §5.2, §6.1,
§8, §9.2, §16.1 and §17.4 are updated; §8 is rewritten. `scenarios/060-definitions-enforced` and
`070-branch-visibility` are the proof, and every other scenario now declares a schema first.

### C — migrations end to end, through the CLI

`MutateField` already carries `up`/`down` producer ids, but there is no way to supply an
implementation. With A and B done, a migration is just another script in the repo, and
`borg repo push` already knows how to turn a script into a producer definition.

This unlocks §18's first acceptance scenario, which has **never run**: fork, change a field's type
with a migration, read old data correctly through the new lens on the child while the parent is
untouched, then def-only merge and watch the parent's values migrate.

It is the most valuable demo in the project — the thing no other ORM does, that the whole system was
designed around, and that we have never seen work.

**Done.** `scenarios/080-migration` is the acceptance scenario, running end to end through the real
binary, in both directions and with a no-`down` case that reports `broken`. Five decisions came out
of it, below. The blocker was `WriteSession` validating against the branch rather than the writer —
now split, shape at the ClientVersion and permission at the branch (§8.0). Clients got a real
ClientVersion: the branch's def-version, with `--client-version` to act as an older one and
`borg def version` to see it. `ProducerKind::Migration` lost its version pair, which is now folded
per branch — the thing that makes a def-only merge of a migration work at all. §5.3, §5.4, §5.5,
§8.0, §9.2, §9.3, §9.6, §10.4, §13, §17.3 and §17.4 are updated.

### D — background derivation, and concurrency

Run derivation continuously instead of on demand; make `FreshnessRequirement::Current` actually
compute inline; add `frontier.reaches()`; add the branch-scoped pause switch.

Concurrency is folded in here because it is the same work: a background loop is exactly where you
parallelise, and `settle()`'s round-ceiling is what both need reworked. Deferring it this long is a
deliberate bet that the sequential assumption lives in one function — it should not slide past C.

**The freshness half is done.** `Current` computes inline for real, through a narrow
`InlineDerivation` seam the resolver holds instead of the engine; it follows recorded read-sets so a
cell's inputs are computed before the cell, and it runs migration hops, which closes the gap C left
where a version nothing had materialized read `stale` forever. Derivation now follows every commit
unless the branch is paused, and `borg derive pause | resume | status` is that switch, beside the
store. `frontier.reaches()` and settled-frontier reads exist and are exposed as
`borg frontier reaches <layer>` and `borg get --settled`. §9.6, §10.5, §16.4 and §16.6 are updated,
`scenarios/090-freshness-controls` is the proof, and 040 now demonstrates lag by pausing rather than
by declining to derive. Three decisions came out of it, below.

**Concurrency is not.** `settle()` is still sequential and its round-ceiling still assumes derivation
is the only writer.

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

### `BufferId` has no interning variants

`String`, `Binary` and `BigInt` were dropped from `BufferId`. §4.2 already said the interning stores
hold *values, not cells* — an interned value has no version, no origin and no writing layer, so every
field of a `CellRecord` is meaningless for it. A `BufferId` variant therefore named a cell partition
that cannot exist, and would have been the first place a branch or a layer crept back into a scheme
whose entire value is having neither.

`AnyObject` and `AnyArray` stay. Those are mutable containers, so their contents genuinely are cells,
even though nothing implements them yet.

Dropping them forced `CellRef`'s `Display` to become total, which it should always have been: the old
`{:?}` fallthrough emitted an unparseable second dialect in exactly the places — panics, lineage
output, error messages — where a pasteable address matters most.

### Bare values parse as strings

`borg set Company#1.website acme.ai`. No quotes and no prefix: a shell worker is the target audience,
and a form that needs quoting is one that will eventually be typed unquoted. `0x…` is `Binary`, a
trailing `n` on digits is `BigInt`, and everything unmatched is a `String` — which makes value
parsing infallible, since every input names some value.

The cost was real and was documented rather than hidden (§3.4): `true` is a `Bool`, so a string field
could not hold the text "true"; likewise `42`, `0xff`, `7n`. Quoting was the alternative and it buys
the same thing at the cost of the shell-first stance.

**B resolved it**, as predicted, by making parsing type-directed against the declared `FieldDef`: the
write path calls `parse::value_as`, so a field declared `String` reads `true` as four characters and
a field declared `Int` refuses `acme`. The guessing parse survives only where there is genuinely no
declared type to consult — an `Any` field, an error message, a `describe` payload — so its
reservations no longer reach stored data.

### Interning is invisible to workers

A pipeline reading `company.website` receives `acme.ai`, never `@s-1a2b3c`. A worker writing a string
sends the text and is finished; the engine interns it. No second round trip in either direction, and
nothing above the storage line needs to know that content addressing exists — the same call as
batching being a runtime concern rather than a user concern (§17.1).

Where the conversion lives took some deciding. It is **not** the resolver: resolution deals in
`Value`, the engine's internal currency, and rendering there would push every internal consumer
through a string round trip to serve the two edges that actually want text. It is **not**
`ProducerCtx` alone either, because `borg set` writes source cells with no `ProducerCtx` in sight, and
a second implementation there is how two dialects start. It is one engine-level type beside storage
(`borg_engine::values`), which `ProducerCtx` exposes and delegates to — the exposure being necessary
because a producer runtime holds no store handle and must not acquire one.

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

**Implemented in D.** The switch is a per-branch flag in a sidecar next to the implementation table,
and the check lives in the CLI's auto-derive path rather than inside `catch_up`. The engine's job is
the mechanism; a mechanism that consulted an operational switch is one `borg derive` would have to
reach around, which is the shape that eventually gets it wrong. A fork does not inherit the flag,
which follows from the flag not being in the log.

### Background derivation follows the commit that caused it

The CLI is process-per-command, so "background" had to mean something. Considered: a `borg watch`
foreground loop, and a daemon. Chosen: **the process that commits a layer catches the branch up
before it exits** — after `set`, `delete`, `def push`, `repo push` and the parent side of a merge.

A daemon is a real answer and the wrong one to build first: it needs a lifecycle, a socket and a
supervision story, none of which the scenarios could then run without. A `watch` loop is a daemon
with the supervision left to the user, and would leave every existing scenario needing a second
terminal.

What is bought is that **no scenario says `borg derive` unless it is making a point about rounds**,
which is the observable claim §9.6 makes. What is not bought is asynchrony from the *writer's* point
of view: a write now pays for the derivation it causes. That is a v1 latency property and not a
semantic one — §9.6's licence is precisely that scheduling policy cannot affect correctness — and it
is the property a server with a worker pool changes by moving one call behind a signal.

A def push auto-derives too, which is not obvious: it commits no data. It creates work anyway, since
a producer newer than its data owes its whole source buffer (§9.6) and a `MutateField` appoints
migrations that owe every existing value.

### An inline computation does not advance a watermark

`FreshnessRequirement::Current` runs producers, and the obvious thing — label the output with head,
like a round does — is wrong twice over. A watermark is a claim about *all* of a producer's output,
so one entity computed on demand advancing it would declare a whole branch caught up on the strength
of one read; and because watermarks are rebuilt from derived layers' `reflects` on open, the lie
would survive the process.

So an inline run labels its *layer* with the watermark the producer already had, while labelling the
*cell* with head. The cell's claim is true — its inputs were computed first, recursively, precisely
so that it is — and the producer's claim does not move.

The side effect is the good one: the work stays outstanding, so the next round redoes it and
propagates the consequences a round propagates. Without that, an inline computation would silently
strand every downstream producer, since nothing but a round walks the invalidation index forward.

### The resolver holds an interface, not the engine

`Current` needs to run producers, and handing `Resolver` the `DerivationEngine` would make the read
path a second entry point into derivation — two callers of `settle`, two opinions about what a round
is, and a dependency edge pointing both ways as soon as anything in the engine wanted to read.

`InlineDerivation` is one method: *bring this cell up to date*. Not catch a branch up, not settle a
round, not register a producer. That is the whole of what a read needs, and stating it as an
interface is what makes §10.5's claim structural rather than aspirational: with this seam the read
path is a **client** of derivation, which is exactly what "lazy materialization is a per-read client
mode, not a system architecture" says it is.

### A schema change is a diff, not an instruction

`describe` has no "mutate this field" — a repo emits the shape it believes in now, and `borg repo
push` compares it with the definitions in force. A field nobody declared becomes a `DeclareField`;
one whose type moved becomes a `MutateField`; one that is unchanged is a repeat.

A repo *cannot* express the mutation directly, because it does not know what it is mutating from: the
branch does, and on another branch the answer differs. The same repo pushed on main and on a fork
that already changed the field means two different things, and only the store can say which. It also
follows from repos emitting their whole schema every push — the diff is where that idempotence and
schema evolution meet, rather than two mechanisms sitting beside each other.

The migrations are named on the *field* (`up`, `down`) rather than beside the change, because the
field is what persists across pushes and the change does not.

### A migration definition records a direction, not a version pair

`ProducerKind::Migration` used to carry `from` and `to`. It cannot: a def-only merge replays the
`MutateField` that appointed the migration onto the parent as a **different layer**, so the pair
baked in on the fork names versions no reader on the parent will ever ask for — the headline scenario
would produce a migration writing into nowhere.

Which two versions a migration bridges is a fact about the branch's version chain (§5.3), folded from
the `MutateField` alongside everything else. The author declares the one half that is genuinely
theirs — which direction this code runs in — and the log supplies the rest. The same reasoning
retired the authored `version` on every `ProducerDef`: a producer's ClientVersion *is* the def-layer
it was pushed at, and that id does not exist until the layer opens.

### Two def-views on the write path

Shape is checked at the writer's ClientVersion; permission at the branch's (§8.0). Milestone B
validated everything against the branch and recorded that as a known shortcut, and it defeats exactly
the feature C exists to demonstrate: a client authored before a schema change writes the old shape by
definition, and so does a `down` migration.

The reverse split does not work either. Permission cannot be a ClientVersion question, because a
`down` migration's own view is *older* than the `MutateField` that named it as the field's `down` —
asked there, the branch's own appointed migration is an ownership violation.

Where a writer is current the two views are the same object, which is the common case. Where the
branch has since dropped a field the writer knows about, permission falls back to the writer's
declaration: an old client is entitled to the schema it was written against.

### The CLI's ClientVersion is the branch's def-version, and nothing is recorded

Considered and rejected: a sidecar file beside the store, like the producer-implementation table. It
looks symmetric and is not. An implementation table records something *true* — where code lives — and
a remembered client version records something that was true once: it goes stale the moment anyone
pushes a def, and a single value is wrong for any branch it was never synced on while a per-branch
map is state nobody asked for.

So the CLI is a client that regenerates itself on every invocation: its ClientVersion is the schema
as it stands, which is exactly what a freshly generated SDK would carry. `--client-version` pins an
older one, and has to exist — §5.4's whole claim is that a v1 client keeps working after the schema
moves, and with no generated SDKs (§18) there is otherwise no way to *have* a v1 client to test it
with. `borg def version` prints the default, which is what makes the concept visible at all.

### `up` and `down` are two projections of one value

Neither triggers the other, on either trigger path. Each writes exactly the version the other reads,
so unfiltered they run until the cycle detector fires (§16.6) — on the ordinary configuration rather
than on a cycle. This is the one filter by author, and §9.3's rule that the read-set trigger is *not*
filtered by author still holds for everything else, which is what keeps genuine cycles catchable.

The same fact bites in seeding: a producer that has never run takes its whole source buffer as work
(§9.6), and `down` seeded after `up` had already derived the new version would migrate that back and
overwrite the source value `up` read it from. Filtering the seed by the same step membership makes
the round order-independent, which it has to be — nothing prescribes the order producers run in.

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

- **Unit coverage generally.** Tests are almost entirely integration-level. `borg-storage`,
  `borg-engine`'s internals and the CLI have essentially none of their own. Landing alongside each
  milestone rather than as a separate push.

Paid off in B: branch visibility of definitions, including the write-rejection half
(`scenarios/070-branch-visibility`), and second-order forks — a fork of a fork, in that scenario and
in `def_events.rs`. Nothing had exercised a branch chain deeper than one fork before.

Paid off in C: both migration directions and their non-interaction (`migration.rs`), a producer
seeded over data that predates it, migration roles resolved from a folded chain, and the two-def-view
write path (`write_validation.rs`). Nothing had run a `down` migration at all before.

Paid off in D: the three read modes distinguished from each other, inline computation recursing
through a chain and terminating on a cyclic read-set, an inline run proved not to advance a
watermark, the settled frontier read against the ragged head, and `reaches` in all three of its
cases (`freshness.rs`) — plus a migration hop computed on demand (`migration.rs`).
