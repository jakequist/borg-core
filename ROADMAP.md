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

**And it runs concurrently.** A round discovers a wave of invocations and runs them at once; a
producer sees a peer's output because a round has a branch of its own and a client's concurrent write
is above its fork point; and a producer's workers are a pool rather than one process behind a lock.
The invariants that were written to permit this were checked rather than assumed, and three of them
were not true as implemented — see the decisions below.

**Versions are per definition, and watermarks compose.** A stored record is keyed by the def-version
of its own field rather than by whoever wrote it, so declaring an unrelated field no longer hides
data or quietly severs the dependencies recorded against it; and validation returns §10.3's
composition, so a producer reading another producer's output can reach `current` instead of being
reported stale forever. `110-def-push-keeps-data` and `120-invalidation-survives-a-def-push` are the
two scenarios; the second exists because S1 structurally cannot see that failure.

**Every client write is a transaction.** A client forks, writes in isolation and merges; `borg set`
is that done in one process. A transaction records what it reads, and at commit those reads *are* its
guards — so the safe path is now the only path, and last-write-wins is what remains for a cell nobody
read rather than what everybody gets by default. Two transactions racing to increment one counter
resolve to one increment in either commit order, and two racing to create one object resolve to one
object. Transactions are ephemeral and reaped on an idle timeout; branches are the durable form of
the same idea, and nothing reaps those.

Act 1 is the modern ORM.

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

**Concurrency is done too.** `settle()` alternates a sequential *discovery* pass with a concurrent
*execution* pass — one wave of invocations at a time, bounded by `set_parallelism` (default: one per
core, `BORG_DERIVE_PARALLELISM` in the CLI). The ceiling is committed state raised by every layer the
round commits, rather than a value threaded through a loop. `ProcessExecutor` holds a pool per
command instead of one worker behind a lock.

Turning it on found three things that were untrue, and one that cannot be made true without a design
change; all four are below. `crates/borg-engine/tests/concurrency.rs` is the proof — seven scenarios,
each replayed 30–120 times at sixteen-way parallelism, asserting the settled result and never the
schedule. §6.2, §7.3, §16.3, §16.4, §16.5 and §17.3 are updated.

### E — events get identity; layers reference them

Phase 1 of the transactional model (`SPEC-DRAFT.md` §1, §4). A stored record no longer carries the
layer it lives in: an `Event` carries `authored`, the layer it was *first* committed to, and layers
name their members. Merge stops copying — a parent layer names the child layer's events — and the
lineage the old rewrite destroyed survives, so a read reports both where a value was authored and
where it landed. `StorageProvider` grew `author_event` / `include_event` / `rebuild_read_index`,
`read_layer` yields membership, and both backends keep a materialised
`(branch, cell, version) -> (layer, event)` index maintained as events stream into an open layer.
§4.3, §6.2, §10.4, §11, §13, §16.3 and §17.1 are updated;
`crates/borg-engine/tests/events.rs` is S11 and S12, and the `StorageProvider` contract now runs
against **both** backends rather than SQLite alone.

### F — every client write is a transaction, and its guards are automatic

Phase 2 of the transactional model (`SPEC-DRAFT.md` §2, §5, §7.1). A client no longer writes to a
shared branch: `borg tx begin | get | set | delete | commit | abort` forks, writes in isolation and
merges, and a bare `borg set X v` is that same thing done in one process. What a transaction read
becomes what its commit is contingent on, re-evaluated against the parent since the fork point — so
guards stopped being opt-in without the guard machinery changing at all. `Transaction` is a value
type in `borg-core` carrying the fork point, the read-set and the write-set;
`BranchManager::merge_transaction` is the commit; `LayerManager::check_reads` is the automatic half
of `check_guards`. `WriteSession` now reports what it probed and what it wrote, which is what puts
the implied-existence read (§8) into the read-set where it belongs. Transaction state lives beside
the store with the pause flags, and is reaped on an idle timeout swept when a process opens the
store. §12 and §13 are updated; `scenarios/130`, `140` and `150` are S2–S6 plus the surface and the
reaping, and `crates/borg-engine/tests/transactions.rs` has the guard-derivation rules underneath
them.

Four decisions came out of it, below. The fan-out curve is unchanged: 128k entities at four cores
derive in 1.61s against 1.56s before, and the shape is still linear.

### G — derivation is a transaction, and the round ceiling is gone

Phase 3 of the transactional model (`SPEC-DRAFT.md` §3, §4, §7.1), and the last of it. A round forks
the branch at the source layer it settles, runs every producer on the fork, and merges what settled —
so a producer's read path is `[(round branch, head), (trunk, fork point)]` and there is no
high-water mark anywhere. `reflects` is the fork point by construction; a client write landing
mid-round is above the bound and is simply not in the path.

`RoundCeiling` is deleted, not kept as a fallback, and with it the prefix-versus-filter hole recorded
below, the `ReadPath` that would have had to carry admitted layers, and the `reflects` column the
provider line was never going to get. `borg_core::Round` is `N` invocations sharing one fork point,
sitting beside `Transaction` because the two guard rules have to be read together;
`BranchManager::merge_round` is the merge; `DerivationEngine::settle` is public, because the
interleavings worth testing are statements about *which* layer a round settles.
§12, §13, §16.4 and §16.5 are updated; `crates/borg-engine/tests/rounds.rs` is S7–S10 and
`scenarios/160-rounds-are-transactions` is S7 through the real binary.

Eight decisions came out of it, below.

**The fan-out benchmark moved, and this is where it went.** 128k entities at four cores derive in
1.88s against 1.62s before (+16%); the re-derive after flipping the one shared cell is 1.50s against
1.14s (+30%). The shape is unchanged — still linear in `n`, still scaling with parallelism the same
way.

Measured, not guessed, and in two parts. **The merge is ~0.10s of it** at 128k, after three fixes
worth naming because each was a factor of several: the cell-touch index no longer clones a `CellRef`
per probe (it was keyed on a `(BranchId, CellRef)` tuple, and `HashMap::get` wants the whole key); a
merge whose parent has had *nothing* written to it since the fork point skips building the guard set
at all, which is one comparison instead of a million; and the round's `n` per-invocation layers are
regrouped into one layer per producer, so the trunk gains one layer rather than 128k. The naive
version of the merge was 0.48s.

**The remaining ~0.26s is the second read-path segment**, and it is the cost SPEC-DRAFT §7.6
predicted: every producer read now walks `[(round branch, head), (trunk, fork point)]` instead of one
segment, so it is two index probes rather than one, ~900k extra probes in a 128k round. Halving that
back would mean a read index keyed cell-first rather than branch-first, which makes `scan_buffer`
proportional to the whole store rather than to a branch — a trade worth measuring on its own and not
worth taking blind. What was cheap was removing a `CellRef` clone from the probe itself, which is
done: `MemoryStorage`'s read index is nested now for the same reason the touch index is.

### Deferred, still

Aggregations, `Set`/`Map`, container isolation, generated SDKs. Nothing has argued for pulling any of
them forward, and the CLI is doing the SDK's job well enough to keep learning from it first.

`O(1)` merge, explicitly. A parent layer that *references* a child layer's event set rather than
enumerating it is what would make merge asymptotically free; the model now permits it and the old
one forbade it. It needs read-path compaction to pay for itself, so what landed is the honest
version: `n` membership rows and `n` index entries per merged layer instead of `n` full records.

A **`Watermark` newtype**, and it is the one deferral that has already cost four bugs. Four pairs of
`LayerId` have been conflated so far — `read_at` vs `reflects`, `authored` vs `landed`, the round
ceiling as a bound vs as a filter, and a derived dependency's landing layer vs its watermark — and
every one was two ids of the same Rust type meaning different things. A watermark is a position in
the **source** stream (§10.1); `landed`, `authored` and a read ceiling are positions in the layer
stream, which includes derived layers. Wrapping the first would make `landed_at > fresh_as_of` fail
to compile, which is exactly the line that had to be rewritten here.

What stopped it being done now is that it is not only a rename. `Resolved.fresh_as_of` is compared
against, and set to, the layer being read — a ceiling that is usually a *derived* layer once a
branch has settled — so a `Watermark` type immediately asks whether the read target should be the
highest **source** layer at or below the ceiling instead. That is almost certainly the right answer
and it makes §10.1 literally true, but it changes the layer id every settled derived read reports,
which `100-watermark-truth` forks at and replays. It deserves its own change with its own scenario,
not a rider on a bug fix.

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

### A round is a sequence of waves, not a stream of invocations

`settle()` alternates a **sequential discovery pass** with a **concurrent execution pass**: turn the
layers in front of you into a set of invocations, run all of them, then turn the layers *they*
committed into the next set. Discovery stays sequential because it runs no user code — it is a walk
over changesets and index lookups, and running it concurrently would buy contention on the one index
mutex in exchange for nothing measurable.

The barrier between waves is not incidental, it is what makes §16.5's self-correction true. A
producer records its read-set before its own layer commits, so a run that read an input its upstream
had not written yet is already subscribed to that cell by the time any later wave scans the layer
that supplied it. Overlap the waves and the correcting trigger can be missed: the upstream's layer is
scanned, finds nobody subscribed, and the downstream then commits a value computed from nothing.

The bound on a wave is one invocation per core by default, `set_parallelism` in the engine and
`BORG_DERIVE_PARALLELISM` in the CLI. An environment variable rather than a flag or a sidecar: it is
not log data and it is not a fact about the store — the same store derived on a laptop and on a build
box wants different numbers — so there is nothing to record, and every command that derives would
otherwise need the same flag.

Discovery also deduplicates within a wave. Two layers in one wave can dirty the same invocation, and
running it twice at once would put two layers of one round on one cell and two `record` calls racing
to describe one run. Deferring the duplicate is free, because this wave's own output re-triggers it
if it is still dirty.

### The round ceiling was a prefix used to express a filter — and is now deleted, not fixed

*(Superseded by milestone G. Kept because the shape of the problem is why the fix is a fork.)*

The ceiling was *"the highest layer that is either ≤ L, or is a derived layer with `reflects == L`"* —
a **filter** — held as a `ReadPath` bound, which is a **prefix**. The two coincide while derivation is
the only writer on the branch and diverge the moment a client commits a source layer `L'` mid-round
with an id below one of this round's: the prefix admitted `L'`, so output labelled `fresh_as_of: L`
could have incorporated `L'`.

The strict repair was implemented and was worse. Advancing only over a contiguous run of ids the round
itself produced means a ceiling stalled below `L'` **never rises again**, so every re-run of a
downstream producer reads the same absent input and the round stops converging — a lost update, found
by a test that pushed a source layer while a round was settling.

**What actually fixed it was not a better bound.** A round forks its own branch at `L`, so `L'` is on
another branch and the filter is expressed exactly: everything the round wrote is at *my head*,
everything else is bounded at the fork point, and there is no third category to admit by accident.
The two design changes this entry said would be needed — a `ReadPath` carrying admitted layers, and a
`reflects` column the provider may filter on — are both unnecessary, which is the strongest argument
the transactional model made for itself.

### Three invariants were not true as implemented

Turning concurrency on was supposed to test four claims. Three of them needed fixing first, and none
of the three was a design problem — all were places where a sequential engine had made a shortcut
invisible.

- **A branch's head was the last commit, not the highest.** `LayerManager::commit` assigned
  `heads[branch] = id` unconditionally. Ids are assigned at open and order is established at commit
  (§7.3), so a layer opened first and committed second walked the head *backwards* — and the head
  bounds every read path and every producer's work gap. Now a maximum (§7.3).
- **`MemoryStorage` read a cell's history in commit order and assumed id order.** It walked backwards
  to the first write at or below the bound; out-of-order commits made that the *older* of two writes.
  Now a maximum. SQLite never had the bug — it says `ORDER BY written_at DESC`.
- **`catch_up` settled layers whatever state they were in.** A layer only becomes a changeset at
  commit (§9.6), and with a client writing concurrently the loop could reach one still open. On
  SQLite that reads rows which may yet be abandoned. It now stops at the first uncommitted layer of
  the branch and waits — available only because there is no queue to stall — and a layer found
  uncommitted when a store is reopened is aborted, since an open layer is exclusive to a process that
  no longer exists (§6.2).

The fourth claim, single-writer-per-field, **held** — but it is a statement about fields, and what
extends it to invocations is v1 pipelines being per-entity maps. A producer that wrote across
entities would have two invocations racing one cell, and nothing enforces that it does not. Recorded
in §16.3 beside idempotency, where the other unenforced obligation on producer authors already lives.

### Workers are a pool per command

`ProcessExecutor` kept one worker behind a mutex, which serialised every invocation through one
subprocess whatever the scheduler decided — the queue, reintroduced below the seam. It now holds a
pool per command, bounded by a permit, with idle workers handed back rather than respawned.

A worker whose conversation ended in anything but a clean `Done` is **discarded rather than
returned**. The protocol is request/response over a pipe, so a failed invocation leaves it at an
unknown offset and reusing it would answer the next invocation with the last one's reply. A worker
that reported `Error` is kept, because it answered: raising on one entity is not a broken process.

### The def-view fold was O(all layers), and it hid the parallelism

Not planned work, and done because without it the milestone's own measurement said nothing.
`DefRegistry::def_layers` found def layers by filtering every layer the log knew about. Every write
session folds two def-views (§8.0) and every producer run is a write session, so a fan-out of `n`
invocations — each of which commits a layer — cost `O(n²)`, and at 32k it was **44s of the 45s the
fan-out took**. Parallelism against that measured as a slowdown, because the contention it added was
all there was left to add.

`LayerManager` now indexes committed def layers per branch. Def layers are rare by construction: a
schema changes far less often than data does.

`scale.rs`, one shared upstream cell flipped under `n` companies, release build, four cores. `p` is
the degree of parallelism; the first column is the engine as it stood before this milestone.

| `n` | before | `p=1` | `p=2` | `p=4` | `p=8` |
|---|---|---|---|---|---|
| 1,000 | 44.8ms | 11.4ms | 6.9ms | 6.0ms | 5.3ms |
| 8,000 | 2.48s | 112ms | 63.8ms | 60.2ms | 67.4ms |
| 32,000 | 42.4s | 460ms | 298ms | 271ms | 270ms |
| 128,000 | 1278s | 2.00s | 1.25s | 1.17s | 1.16s |

Read it as two separate results. The `before → p=1` column is the def-view fix, and it is where the
order of magnitude is: 640× at 128k, and quadratic to linear — the fan-out was 21µs per invocation
at 1k and 10ms per invocation at 128k, and is now 16µs at both. **Parallelism is the smaller
number**: 1.7× at two, and flat after that. Not a disappointment so much as a measurement of what is
left — with the fold gone, essentially all of the remaining work is under `MemoryStorage`'s single
mutex and `LayerManager`'s, so the second core saturates them and the third and fourth wait. Sharding
either is the next thing worth doing, and both already sit behind an interface that permits it
(§17.2). A deployment whose producers do real work rather than three comparisons will see more, which
is exactly what Amdahl says and not an excuse.

### The CLI exits quietly when its reader goes away

`borg get … | head -1` closes the pipe as soon as it has its line, Rust turns the `EPIPE` into a
panic, and `println!` crashed the process. It is in three scenarios and it failed about one run in
forty — but only under load, which is why twenty quiet runs of `./check.sh` had never shown it. That
is precisely the frequency the C backfill bug taught us to treat as a bug and not a flake, and it was
found by stress-testing the concurrency work rather than by it.

Not fixed with a signal disposition, which needs `unsafe` and the workspace forbids it. One macro
that writes, and exits `0` if nobody is listening — which is what a Unix tool does. Errors keep going
to stderr, which the pipe did not close.

---

### Layer membership is not part of a layer's metadata

`SPEC-DRAFT.md` §1 sketches `Layer { …, events: [EventId] }`, and taking that literally would break
the constraint that governs everything else about layers: a layer may hold millions of events, and
its metadata is written and read *whole* (`put_layer_meta` is one row). A `Vec<EventId>` on it would
buffer a layer's membership in memory to write one commit.

So membership lives in storage as a `(layer, event)` relation, enumerated by `read_layer` and
extended by `include_event`. `Layer` in `borg-core` is unchanged. The model is the draft's — layers
reference events — but the representation is a relation rather than a field, for the same reason
commit streams.

### The read index is durable, and not rebuilt on open

The dependency and touch indexes are rebuilt by replaying the log at every `Registry::open`, and the
obvious symmetry would be to do the same for the `(branch, cell, version) -> (layer, event)` index.
It is the wrong symmetry: those two are in-memory caches of a process, and this one is what makes a
read a single indexed lookup in the store itself. Rebuilding it per CLI invocation would turn the
`O(log)` read we already pay into an `O(log)` *write*.

It is still a projection, and `rebuild_read_index` on `StorageProvider` is how that stays a fact
rather than a claim: a test throws the index away, rebuilds it from membership, and asserts no
answer changed. Nothing on the read or write path calls it.

### The index is maintained on the way in, not at commit

Index rows stream in with the events they project and are invisible by the same join against the
layer's state. Building the index at commit instead would make commit `O(rows)` — the identical
mistake as flipping a `visible` flag per row, which §17.1 already rejects. It also makes the merge
case correct by construction: the membership and the index entries for a merge land in the same
invisible layer, so a read can never see one without the other.

### Two writes to one cell in one layer are two events, and one index row

Membership keeps both — the layer really does contain two, and `read_layer` yields both, which is
what the invalidator sees. The read index keeps one, the later. That is the same collapse the old
`cells` table got from its primary key, and stating it explicitly is what keeps `MemoryStorage` and
SQLite answering identically: with one index row per landing layer, "the newest landing" is a
maximum with no tie to break, in either backend.

### A record is keyed by its field's def-version, not by its writer's ClientVersion

`DefVersion` is now a type of its own. `ClientVersion` is a whole schema and advances on every def
push; a def-version belongs to one definition and advances only when that definition is mutated
(§5.3). Storage, `CellAt`, `Event`, read-sets, the dependency index, migration chains and
`ProducerCtx::get_at` all speak the second; every actor still carries the first, and
`DefView::version_of` is the only bridge.

Keying records by the writer's ClientVersion made a def push that named some *other* field move
every subsequent write to a version no reader looked for. Two consequences, and the second is the
one that hurt: the value read back `broken` — correctly, since a field that never changed shape has
no migration chain and so no route to the new version — and, invisibly, every read-set entry
recorded against the old version stopped matching, so invalidation stopped with no error, no `stale`
and no watermark left behind. A derived value simply froze at its last computation and went on
calling itself current.

`100-watermark-truth` cannot see the second half, and understanding why is the point: a producer
authored at the old def-version genuinely reads the old world, so replaying at the watermark
faithfully reproduces the frozen value. The result is self-consistent and wrong.
`120-invalidation-survives-a-def-push` compares a derived cell against the source cell it is a copy
of instead, which is the comparison a replay structurally cannot make.

The type is what keeps this from coming back. There is no arithmetic from one version to the other —
the answer is a fact about the schema — so the compiler now insists a def-view be asked.

### Existence cells, lists and untyped containers are unversioned

They have no `FieldDef`, sit on no migration chain, and nothing about their shape can change. Before
they took the writer's ClientVersion, which meant a def push made `imply_existence` write a *second*
existence cell for an object that already had one — into the very buffer producers map over, so
every declaration looked like a fresh entity to every pipeline. `DefVersion::UNVERSIONED` is one
fixed key that stays findable across every push.

### A derived dependency is validated against its watermark, not where it landed

`Resolver::validate` compared each dependency's landing layer against the value's own watermark. For
source data that is right. For derived data it compares a derived layer id against a source one: a
derived layer sits *above* the source layer it reflects, by construction (§6.3), so a producer
reading another producer's output was reported stale on a fully caught-up branch — permanently, and
§10.4's `current` was unreachable for any chain.

§10.3 already specified the answer, `W(B) = min(target, W(A), W(other deps))`, and nothing
implemented it. Validation now returns that composed layer rather than a verdict, recursing into
derived dependencies and memoizing per dependency so a diamond stays linear; the memo doubles as the
cycle guard. Merges cannot reach the derived arm at all — `replayable_layers` carries only source
layers — so the "landed, never authored" protection that arm was originally written for stays
exactly where it belongs, on the source arm.

`Landed::reflects()` names the quantity once, in core: *the source layer this record's content
reflects*, which is where it landed for source data and its producer's watermark for derived. Both
arms of the comparison are now written in one place, which is the cheap half of the type distinction
this class of bug keeps asking for. The expensive half — a `Watermark` newtype separating
source-stream positions from layer ids everywhere — is **not** done; see *Deferred, still*.

### `freshness: current` validates before it computes

An inline computation deliberately advances no watermark (§10.5), so it leaves nothing behind that a
later read could recognise, and every `current` read re-ran the producer — and, on a chain, the whole
chain — however settled the branch was. The read now validates first and computes only when
validation does not already reach the layer being read. Validation runs no user code and is the same
walk the read performs anyway.

What is *not* done: `refresh` still re-runs every hop of a chain once any hop is behind, rather than
only the hops that are. That costs work, never correctness, and needs validation to be callable from
the derivation engine — which today would mean either duplicating it or handing the engine the
resolver, and the second is the dependency direction `InlineDerivation` exists to keep one-way.

### The read-minus-written rule is about order, not about set difference

`SPEC-DRAFT.md` §2 says *"guard the cells you read and did not write"*, and taken literally as a set
difference it deletes compare-and-swap. A transaction that reads `X` and then writes `X` — the
ordinary read-modify-write, and the case §2 itself says the guard should *fall out* of — would have
`X` in both sets, so `X` would be dropped and two concurrent increments would both land with one
silently lost.

The reason §2 gives is the correct rule: a read that returned the transaction's **own write** says
nothing about the parent. So a read is recorded unless the transaction has *already* written that
cell, and `Transaction::observe` enforces that at the moment of the read rather than by subtracting
sets at the end. Write-then-read contributes nothing; read-then-write is a compare-and-swap. §12.1
now states it this way, and `140-transaction-conflicts` is the counter-example that would have
caught the set-difference version.

### `since` is the fork point for every guard, and per-read tracking would be wrong

The obvious refinement — record *when* each read happened and use that as its `since` — is not merely
unnecessary, it is unsound. A transaction's read path is bounded at the fork point (§7.2), so every
read observes the parent as the parent stood *then*, whenever during the transaction's life it
happens. Using the moment of the read would ignore every parent write between the fork and the read
— writes the transaction provably did not see, because they were above its bound — which is the exact
set a guard exists to catch. One `since`, and it is the snapshot the reads came from.

### An automatic guard on a derived cell is dropped, not rejected

`check_guards` rejects a guard naming a derived cell (§12): guarding a shadow is meaningless. Applied
to an automatic guard that rule makes a transaction unable to commit *because it looked at a computed
value*, which is a strange thing to punish — and it catches migrated data too, where the field is
declared source but its records are a migration's output, so reading `Company.founded` in
`100-watermark-truth`'s store would have been enough.

`LayerManager::check_reads` therefore asks the touch question and not the derivedness one. It is also
the cheaper half: the touch index records source layers only, so a derived cell can never be in it
and the guard could not have tripped anyway, while `is_derived_anywhere` costs a storage read per
cell per version on a read-set §7.7 says is unbounded. The hand-written guard keeps its rejection,
because that one is a client asserting something it cannot mean.

### The implicit existence read counts for `borg set` too

`SPEC-DRAFT.md` §5 says a bare `borg set X v` "reads nothing, so it carries no guards". That is true
of the cell it writes and false of the object it may create: the implied-existence probe (§8) is a
read, and `borg set` is *the* common path on which two clients race to create the same object. Making
the one-shot behave differently from `begin; set; commit` would also make "every client write is a
transaction" true only in the telling.

So the one-shot carries exactly the guards the explicit form would: none on the cell it writes, which
is last-write-wins as §12 promises, and one on the existence cell it probed. When the object already
exists that guard can only be tripped by a concurrent *deletion*, which is a conflict anyone would
want reported.

### A transaction that fails to commit stays open

A rejected commit leaves the transaction where it was rather than aborting it. Its snapshot is stale
and its commit cannot succeed, but the read-set is what a client needs in order to decide whether to
retry or give up, and destroying it there leaves them holding an error and nothing else. `borg tx
abort` is the explicit half, and the idle timeout collects the ones nobody comes back to.

### An empty branch's first write is not a transaction, and cannot be

A transaction forks the highest layer its branch can see, so a branch whose entire ancestry is empty
has nothing to fork. `borg set` on such a branch writes directly. This is safe rather than a hole:
§8.0 makes every write contingent on definitions, definitions are def layers, and a branch with no
layers has none — so the write is going to be rejected whatever path it takes, and taking the direct
one is what gets the caller *"no struct named `Wombat`"* instead of *"nothing to fork from"*. There is
also nothing to isolate from: anything concurrent would have left a layer.

`scenarios/060-definitions-enforced` opens on exactly this write, which is how the case was found.

### A round guards what it read and did not produce, as a set difference

§12.1's rule for a client is about **order** — a read before your own write is still guarded, or every
compare-and-swap silently stops being protected. A round cannot use that rule: its invocations are
independent by construction and run concurrently, so whether `tier`'s read of `is_investible` came
"before" `invest`'s write is a fact about the interleaving, and a guard set that depended on it would
differ from run to run. So the subtraction is round-wide and a plain set difference.

**Taking it as a set difference deletes nothing here**, which is why the two rules can differ without
one of them being wrong. Everything a round writes is derived; a derived cell is never in the
cell-touch index (§12.4); so no guard on one could ever have tripped. And the read-modify-write the
ordering rule protects cannot arise — a producer that reads a cell it writes is a cycle (§16.6), not
a compare-and-swap.

That is also why `Round` holds its invocations as the read-sets and write-sets they already are
rather than as `Transaction`s. Building one `Transaction` per invocation was the first shape and it
was **12% of the fan-out benchmark**, spent on two `BTreeSet`s per invocation to compute a guard set
provably identical to the one the raw vectors give. What is reused is what should be — the guard
*rule*, stated once and next to the client's version of it in `borg-core`, and the whole of the merge
machinery underneath: `check_reads`, the cell-touch index, and `since` being the fork point.

### Partial application has to be closed under the round's own dependencies

This is the one thing the draft did not anticipate, and it is a correctness bug rather than a
refinement. Draft §4 says a round "applies the invocations whose guards held and drops the rest". Take
that literally with a chained producer and the round publishes a lie: `invest`'s layer is dropped
because a client moved `headcount`, but `tier` — which read `is_investible` off the round's own branch
and carries no guard on it, correctly — lands anyway. The trunk then holds a `tier` derived from an
`is_investible` that never existed, labelled `reflects: L`, and replaying `L` does not reproduce it.
That is exactly the class of failure `100-watermark-truth` exists to catch, reintroduced by the fix
for a different one.

So a dropped invocation takes everything in the same round that consumed its output with it,
transitively (`Round::cascade`). Dropping more is free for the same reason dropping any of it is:
the edges are recorded on the trunk, the layer that failed the guard is a source layer some later
round settles, and that round rediscovers the whole chain. The closure is built only when something
has already failed, so an uncontended round pays nothing for it.

### A round merges one layer per producer, not one per invocation

One layer per invocation on the *round* branch, because partial application decides per invocation and
a guard is a fact about one invocation. But nothing downstream of the merge needs that granularity — a
layer is an ordered group of events (§6.2) and `LayerAuthor::Derived` describes the whole group — so
the accepted layers are regrouped by producer on the way across.

Without it, fork-and-merge would double the log: a 128k fan-out would commit 128k layers on the round
branch and 128k more on the trunk, and `Registry::open` replays the log on every CLI invocation. With
it the trunk gains one layer per producer per round, which is *fewer* than before the change.

### The dependency index is keyed on the trunk, never on the round's branch

A round branch is where events land on the way through; the dependency graph is a fact about the data,
which lives on the trunk. Keyed on the round branch, the index would be discarded with the round — and
then an invocation whose merge was rejected would never be rediscovered, because rediscovery is
`dependents(branch, cells)` and the edges would be under a branch id nobody looks up. Partial
application is only safe because those edges are already on the trunk when the merge decides.

### An inline computation does not fork

`freshness: current` computes one cell because one client asked, and advances no watermark (§10.5). A
round forks because it is `N` computations that must land or not land together with respect to the
world they read. One invocation has no such structure, and forking it would buy a branch and a merge
to isolate a single run from a snapshot it has no claim on. It writes to the branch directly, at head,
as it always did — and a round in flight cannot see it, because its layer is above the round's fork
point like any other.

### A round isolates data, not definitions

The fork point bounds a round's *data* reads and nothing else. Definitions are folded along the
trunk's full ancestry, which is what §8.0's two def-views already did when the bound was a ceiling:
a layer holds value events xor def events (§6.2), so nothing a round commits can move a definition,
and bounding the def-view at the fork point hides exactly the `MutateField` that appoints a migration
from the round that has to run it. Bounding it was tried; the symptom is that a migration pushed over
existing data never runs at all. `WriteSession::open_on` is where the two branches are passed
separately, and it exists only for this caller.

### A branch id is not free just because no row claims it

A round forks on every settle, so branch ids are minted by the engine rather than by a caller who
knows what they are doing. `BranchManager` therefore skips an id that already names layers, not only
one that already has a branch row — two branches sharing an id breaks the one thing §6.2 says about
layers and branches, and the cost of being sure is one map lookup.

### A backlog of source layers still costs re-runs

Rounds settle one source layer each, so when several are committed before any is settled, the round
settling the earlier layer merges *above* the later layer's id — and the round settling the later one,
forked at it, cannot see that output. This predates the change (a ceiling stalled at `L'` had the same
blind spot in its first wave, and only saw past it by way of the prefix hole) and is unchanged by it.

It costs re-runs rather than correctness in the shapes v1 produces, because each round recomputes what
its own source layer dirtied, chains included. The exposure is an invocation dirtied by `L'` that
depends on a derived cell only an earlier round produced. Settling a *range* rather than a single
layer is the shape that closes it, and it changes what a watermark counts — its own change, with its
own scenario.

### A poisoning is operational state, and the log is what retires it

§14 has been true of the engine and false of every CLI user since the CLI existed. `broken` producers
lived in a `HashMap` on `DerivationEngine`, and the CLI is process-per-command — so the judgement
died with the command that made it. The next `borg get` called a poisoned producer's output `stale`,
which promises a catch-up that was never coming, and the next `borg derive` re-ran the failing code
from scratch, burning the work and repeating whatever partial effects it had. The resolver never
consulted the map at all, even in-process: `Freshness::Broken` was reachable only through the
unreachable-version path.

Where it belongs was the whole question, and it splits in two.

- **The record is operational state, beside the store.** Not a value event and not a def event, and a
  layer holds one or the other (§6.2). It is *discovered* — the same objection that moved field
  ownership out of the log, in the opposite direction: ownership became declared, and this cannot,
  because nobody writes it. In the log it would be forkable, mergeable and time-travellable, so a
  fork would inherit a poisoning its own code never earned. It joins the pause flags, the transaction
  table and the implementation table in the sidecar, for the reasons all three are there.
- **The clearing edge is already in the log.** §14's recovery is *push a new ClientVersion*, and a
  producer's ClientVersion is the def-layer it was pushed at (§9.2) — so a record that names the
  version it was recorded against **expires by itself**. Nothing has to remember to clear it, no
  command has to run in the right order, and a record restored from a backup cannot poison code that
  has since been replaced. The record is not a fact in its own right; it is a claim about a fact the
  log holds, which is what makes durable state outside the log safe here rather than merely
  convenient.

Storage was considered and rejected in one line: nothing above the provider line teaches a
`StorageProvider` what derivation is (§17.1), and a poisoned producer is derivation through and
through. So it reaches durability through a `PoisonProvider` of its own — in-memory for anything that
*is* the process, a file for a client that is not.

Three consequences that were not obvious going in:

- **Recovery has to rewind the frontier.** A round advances every producer's watermark whether or not
  it ran, so a producer skipped while broken stands at head claiming to have incorporated everything
  it never saw. Without the rewind, "invalidates and recomputes its output" would be a promise the
  next write happens to keep. The rewind is `recompute`'s, for the same reason.
- **The watermark advancing while broken is right.** Holding it back would stall the settled frontier
  and every `frontier reaches` on the branch behind one bad pipeline — branch-wide poisoning wearing
  a different hat. The read envelope is the honest channel, and it now says `broken`.
- **`--retry-broken` is not redundant with recovery.** A fix the log cannot see — a worker's
  environment repaired, a service it calls back up — has no version bump to expire the record.
  Retrying by *default* is what turns one bad deploy into the same failure repeated by every command.

Not covered: a producer that has never succeeded writes no cells, so there is nothing for a read to
label. `broken` is a label on a stored record (§10.4), and enumerating the cells a producer *might*
have written is not a set anything can produce.

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

Paid off in D's second half (`concurrency.rs`): a three-hop chain racing itself, the same scenario
proved identical at one invocation at a time and at sixteen, two producers on sibling fields of one
object, a migration pair in one wave, a client write landing mid-round, and a constructed interleaving
where the upstream is *held* until its downstream has read past it. Each is replayed 30–120 times,
because a concurrency bug that shows up one run in six reads as a flake and is not one. The first
unit tests in `borg-engine` itself arrived with them, on the round ceiling — the interleavings that
distinguish a prefix from a maximum are exactly the ones an integration test cannot provoke on
purpose.

Paid off in G (`crates/borg-engine/tests/rounds.rs`): S7–S10, plus a deletion landing mid-round. The
last four need two writers overlapping in time, so they hold a round open inside a producer while the
test writes — which turned up a fragility in the *existing* gate in `concurrency.rs` as well.
`tokio::task::yield_now` hands the worker back to its own local run queue, so under load every worker
can spin on a gate only a task in the global queue can open; the upstream-commits-late test failed its
own precondition about one run in forty that way, and both gates poll on a timer now. That frequency
is exactly the one this file says to take seriously, and it was a test bug rather than a product one.

And `borg-exec-process` got its first tests at all (`pool.rs`): concurrent invocations served by
concurrent processes, the pool bounded by its size, and a worker reused when nothing overlaps. They
are not a scenario because the CLI writes one layer per `borg set`, so a wave driven through the CLI
is one invocation wide — the pool only matters where one source layer dirties many, which is a
fan-out flip, a migration backfill, or a producer pushed over existing data.
