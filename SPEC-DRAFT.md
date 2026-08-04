# Draft: the transactional model

> **Status: draft, not normative.** `SPEC.md` describes the system that exists. This describes a
> proposed successor. **Phases 1 and 2 are built** — what they became is normative in `SPEC.md`, and
> the sections here are marked. Phase 3 is not started.

Three changes, which turn out to be one change seen from three angles:

1. ~~**Events get identity; layers reference them.**~~ **Built** — `SPEC.md` §4.3, §6.2, §13. Today an
   event carries the layer it lives in. Inverting that lets one event belong to several layers, which
   is what makes a merge cheap.
2. ~~**Every client write is a transaction.**~~ **Built** — `SPEC.md` §12, §13. A client forks, writes
   in isolation, and merges. It never writes to a shared branch.
3. **Derivation is a transaction too.** A round forks, runs, and merges. Producers are writers like
   any other. *Not started.*

Together they delete more than they add. The round ceiling (§16.5), the read-path hole it leaves,
and the merge-versus-round exclusion that would otherwise be needed all stop existing rather than
getting solved.

---

## 1. Events and layers

### Today

```
CellRecord { value, version, written_at: LayerId, origin, derivation }
```

Every stored record carries `written_at` — the layer it belongs to — and a `branch`. An event's
identity is fused to its location, which is why merge must rewrite every cell it carries across.

### Proposed

```
Event {
    id:         EventId
    cell:       CellRef
    value:      Value
    version:    ClientVersion      // the def-view it was authored against
    origin:     Source | Derived
    derivation: Option<Derivation>
    authored:   LayerId            // where this event was first committed
}

Layer {
    id: LayerId
    branch, kind, author, state, parent
    events: [EventId]              // membership — many layers may name one event
}
```

§6 already says *"a layer is an ordered group of events."* Groups contain their members; members do
not carry their group. This is the model the prose already describes.

**A layer still belongs to exactly one branch.** Only *events* are shared. The invariant that
matters is untouched; the one that was incidental is removed.

### What `written_at` becomes

It splits, and the split recovers information the current model destroys.

| | meaning |
|---|---|
| `Event.authored` | where this event was first committed — on whichever branch wrote it |
| the layer you reached it through | where it landed on *this* branch |

Today a merge rewrites `written_at` to the new trunk layer, so "authored on `b2` at L20, landed on
trunk at L30" collapses to L30. With membership separate, both survive, and the provenance envelope
can report both. That is strictly better lineage from a change made for other reasons.

**Reads resolve by membership.** "The latest write to cell `C` visible on branch `B` at layer ≤ `N`"
becomes: the layers of `B`'s chain up to `N`, their events, those touching `C`, the latest. A
materialised `(branch, cell, version) → (layer, event)` index keeps that a single lookup; like every
other index in the system it is a projection of the log and can be rebuilt from it (§16 registry).

---

## 2. Transactions are the only write path

> **Built.** Normative in `SPEC.md` §12. One thing changed on the way: *"guard the cells you read and
> did not write"* is implemented as an **ordering** rule rather than a set difference, because taken
> as a set difference it deletes read-modify-write — see §12.1 and `ROADMAP.md`. Two things below are
> still open and belong to phase 3: rounds as transactions, and partial application.

No client writes to a shared branch. A write is:

```
fork → write → merge
```

The fork's read path is bounded at the fork point, so a transaction reads **a consistent snapshot**.
Guards re-evaluated against the parent since the fork point are already the conflict detector (§13).
Together that is snapshot isolation with optimistic concurrency control, assembled entirely from
mechanisms that already exist.

Three consequences:

- **Every trunk layer is one complete intent.** Never a partial write, never two intents interleaved.
- **The safe path is the only path.** Guards are opt-in today, so §13's last-write-wins is what people
  actually get. This inverts that default.
- **Transaction branches are created with derivation paused.** Deriving on a branch about to be
  merged is waste — merge does not carry derived layers, and the parent recomputes.

### Guards are automatic

A transaction records what it **read**; at merge, those reads become its guards. This is the same
read-set machinery producers already use, and it is not optional — it is what makes the whole
concurrency model work (§3).

Two rules make it correct:

**Guard the cells you read and did not write.** A transaction that writes `X` and then reads `X` saw
its own write, not the parent's state, so guarding it would make a transaction fail on itself. This
matters most for chained producers: within one round `invest` writes `is_investible` and `tier` reads
it, and `tier` must not guard on a cell its own round produced.

**Evaluate every guard against the parent as it stood before the merge, then apply.** Otherwise the
first layer to land trips the second's guard.

### A write with no reads has no guards

A transaction that only writes carries an empty read-set, so it is last-write-wins on the cells it
touches. That is honest — the client expressed no dependency on prior state — and it is what every
database does with a blind write. A client wanting compare-and-swap reads the cell first, and the
guard falls out.

---

## 3. Derivation is a transaction

A round forks the branch at the source layer it is settling, runs every producer on that fork, and
merges when it settles.

**This is what deletes the ceiling.** A producer's read path becomes:

```
[(round_branch, head), (trunk, L10)]
```

- A producer sees its siblings' output because that output is on the round's own branch, bounded at
  *its own head*. There is no high-water mark to maintain — "the head of my branch" is already
  exactly "my source layer plus everything this round has committed".
- The trunk segment is bounded at the **fork point**. A client merging while the round runs is above
  that bound and simply is not in the path.

§16.5's ceiling, and the hole it leaves when a bound is used to express a filter, both cease to
exist. Not fixed — absent. And `reflects` becomes the fork point by construction, so it cannot drift
from what the round actually saw.

### Guards make round ordering irrelevant

Two rounds may run concurrently — one settling L10, one settling L12 — and both may invoke the same
producer on the same entity. Single-writer-per-field does not help, because it is the *same*
producer. Without something else, whichever merges last wins, and that may be the older result.

Automatic guards settle it without any ordering rule. The L10 round read `E.headcount` at L10, so it
carries *"`E.headcount` unchanged since L10"*. For it to be in danger at all, L12 must already be on
the trunk — otherwise the L12 round could not have forked from it. **So its guard fails whichever
order the merges attempt.**

The stale round is not sequenced behind the fresher one; it is *rejected*. That is stronger than an
ordering rule, because it needs no queue and no serialisation point — the bad interleaving becomes
harmless rather than prevented.

Note that this only works because every producer read goes through `ProducerCtx`. A round's guards
are exactly its producers' read-sets, which the engine already captures.

---

## 4. Merge

Merge stops copying. It creates a layer on the parent whose membership names the child's events.

```
merge(child, parent):
    validate everything            // unchanged: def divergence, dangling writes, guards
    for each replayable child layer:
        create a parent layer naming that layer's events
```

Events are not rewritten, so their `authored` survives. The parent layer records where they landed.

**Cost.** Membership is `(layer_id, event_id)` pairs rather than full records — a large constant
factor, not an asymptotic one. Genuine `O(1)` needs a parent layer to *reference* a child layer's
event set rather than enumerate it; the model permits that and the current one forbids it, but it
grows the read path per merge and needs compaction. Deferred, deliberately.

**Def-only versus def+data** is unchanged. Derived layers are still skipped when merging a *client*
branch; a *round* branch carries only derived layers and merges them, which is the whole point.

### Rounds may apply partially; client transactions may not

§13 currently rejects a whole merge rather than applying it partially. That stays true for a **client
transaction**, which expresses one intent.

It must *not* be true for a **round**. A round is `N` independent computations with no invariant
spanning any two of them, and whole-round rejection means one contended cell can kill a
128k-invocation round — and under sustained contention it never lands at all. So a round applies the
invocations whose guards held and drops the rest.

Dropping them is safe because of the freshness design: a dropped invocation's cell is still dirty in
the dependency index, so the next round recomputes it, and in the meantime the value reads `stale`
with a watermark that says so. The system already describes exactly this situation.

---

## 5. Capturing a client's read-set

> **Built.** Normative in `SPEC.md` §12.1 and §12.2. The CLI surface below is exactly what shipped,
> plus `borg tx delete`, `borg tx list`, `borg tx timeout`, and a `--tx` handle (or `$BORG_TX`) so
> that one shell can hold two transactions open at once — which is what makes the interleavings in
> §9's S2–S6 expressible at all. One correction: the bare `borg set` does carry the implicit
> existence read, and so is a blind write on the cell it writes and a guarded one on the object it
> may create; see `ROADMAP.md`.

A producer's read-set is free: every access goes through `ProducerCtx`, so the engine sees it. A
client has no equivalent, and this is the one piece of the model with no existing machinery.

### The surface

```
borg tx begin                 → forks the branch, prints a transaction id
borg tx get   <cell>          → reads, and records the read
borg tx set   <cell> <value>  → writes
borg tx commit                → merges, guarded by everything it read
borg tx abort                 → drops the branch
```

A bare `borg set X v` is **an implicit one-shot transaction** — begin, set, commit. That keeps the
common case one command, and means every write is a transaction without every user typing three.
Because it reads nothing, it is a blind write and takes last-write-wins, as §2 says.

### Where the state lives

A transaction spans several CLI processes, so it needs somewhere to keep two things: which branch it
forked, and what it has read so far. That goes beside the store, with the pause flags and the
producer-implementation table — it is neither log data nor a fact about the data model, and it dies
when the transaction ends.

The read-set is recorded as `CellAt` — cell *and* the version it was read at — matching what
producers record, so one guard mechanism serves both.

### What is captured, and what is not

**Captured:** every read made *through* the transaction, including reads that found nothing. Absence
is a legitimate thing to have acted on, and a later write to that cell must invalidate the decision —
the same rule producers already follow (§9.4).

**Not captured:** anything read outside the transaction. A client that runs `borg get X`, thinks, then
opens a transaction and writes based on what it saw gets no protection for that read. This is the
ordinary limitation of every optimistic system, and it should be said plainly rather than implied
away: **a transaction can only guard what it observed through the transaction.**

**Implicit reads count.** The existence probe a write performs (§8, implied existence) is a read, and
belongs in the read-set. Otherwise two transactions could each conclude an object did not exist and
both create it.

### How this maps to an SDK

The CLI shape is deliberately the awkward version — an explicit handle threaded through separate
processes. A generated SDK has it easier: a transaction is an object, reads go through it, and the
read-set accumulates without the caller doing anything. The CLI is proving the *protocol*, not the
ergonomics.

The wire form is the same one producers already use: the engine mediates every access, so the read-set
is captured server-side and the client never has to be trusted to report it honestly.

---

## 6. What this deletes

| | |
|---|---|
| §16.5 round ceiling | absent — a branch boundary expresses the filter exactly |
| the ceiling/prefix hole | cannot form; the concurrent layer is on another branch |
| merge-versus-round exclusion | not needed; they cannot observe each other |
| `ReadPath` carrying admitted layers | not needed; the fork point already does it |
| storage filtering on `reflects` | not needed, so the provider line stays clean |

The architecture gets smaller. That is the strongest argument for it.

---

## 7. Concerns

Ordered by how much they worry me.

### 7.1 Abandoned transactions — answered

> **Built for client transactions.** Normative in `SPEC.md` §12.3. `borg tx timeout` is the switch,
> beside the store; the default is 24h; the sweep runs when a process opens the store. Reaping
> **rounds** by the same mechanism waits for phase 3, and so does the divergence-based refinement
> below. What a reaped transaction leaves behind is its branch row — see `SPEC.md` §12.3 and §7.5
> here.

A `tx begin` with no commit would leak a branch and its state forever. The answer is a configured
**idle timeout**: a transaction untouched for longer than that is reaped.

Idle rather than elapsed, so a long but active transaction survives. Reaping sweeps opportunistically
when a process opens the store — the same place indexes are already rebuilt — so there is no daemon,
and an idle store sweeps nothing because nothing is growing.

**Rounds are reaped by the same mechanism**, since they are transactions too and a wedged producer
leaks identically. That is safe by construction: a reaped round's output is discarded, but the cells
it was computing are still dirty in the dependency index, so the next round rediscovers them — the
same property that makes partial application safe. It is also why idle beats absolute: a legitimate
128k-invocation round runs long but is never idle.

This draws a line worth naming: **transactions are ephemeral and reaped; branches are durable and
explicit.** A client that wants to walk away and come back wanted a branch.

A refinement worth keeping in view: the real predictor of a doomed transaction is not age but
**divergence** — layers committed on the parent since the fork. A transaction open an hour on an idle
store is harmless; one open ten seconds on a busy store already has guards certain to fail. Measuring
that would turn reaping from janitorial into useful, telling a client to give up rather than making
it wait for a merge that cannot succeed.

The error when a client touches a reaped transaction must say *expired after N minutes idle*, not
*unknown transaction*. The first tells you what to do.

### 7.2 `O(1)` merge is not free, and the draft does not claim it

Membership rows are small but there are still `n` of them, and the read index must be updated
atomically with the merge or reads miss data. True `O(1)` requires reference-not-enumeration plus
read-path compaction — a subsystem, not a tweak. The honest claim is a large constant-factor win now
and an asymptotic one made *possible* later.

### 7.3 Derived output can land after source layers it predates

A round reflecting L10 may merge at trunk position L13, after a client's L12. Derived history is then
non-monotonic in `reflects`. Reading at exactly L12 shows L12's source data and *not* the derived
output computed from L10 — which is correct, since at L12 the round had not landed, but it is
surprising and the resolver's rule for picking a derived layer (§6.3) needs restating in terms of
position rather than `reflects` alone.

### 7.4 Write cost multiplies

One `borg set` becomes: fork, write, merge, then a round which is itself fork, run, merge. Two
branches and several layers per user write. Forks are `O(1)` and layers are cheap, but this is the
common case, and the CLI already pays `O(log)` per `Registry::open`. Worth measuring before
committing — the fan-out benchmark is the right instrument.

### 7.5 Branch proliferation, and no reaper

One branch per transaction plus one per round, retained forever in a table with no GC. They are
cheap, but the branch tree becomes mostly spent transactions. Whether merged branches are reaped or
kept as history is a real choice and should be made deliberately rather than by default.

### 7.6 Every read path grows a segment

A transaction's read path is `[(txn, head), (parent, fork)]`, and a round's likewise. Two segments
rather than one, on every read, for every write in flight. Bounded and small — but it is the hot
path, and worth confirming the resolver's segment walk stays cheap.

### 7.7 A long transaction is a large guard set, and a likely conflict

Read-sets are unbounded: a transaction reading ten thousand cells carries ten thousand guards, all
checked at merge, and the longer it runs the likelier one of them moved. That is the ordinary
optimistic-concurrency trade. Borg's guards are *cell-granular*, so a long transaction fails only if
something it actually touched moved — precise rather than merely numerous. Worth measuring rather
than assuming.

### 7.8 A client read-set only covers what went through the transaction

Covered in §5. The risk is not correctness but *expectation*: a user who reads outside a transaction
and writes inside it will believe they were protected. The CLI should make that boundary obvious
rather than quiet.

---

## 8. What I would want to prove before building

- **A stale round cannot land.** Two rounds settling different source layers, both invoking one
  producer on one entity, merged in both orders — the older must be rejected by its own guard either
  way. Write it failing first, with guards switched off.
- **A chained producer does not trip its own round's guard.** `invest` writes what `tier` reads;
  neither may fail on the other. This is the case the parent-reads-only rule exists for.
- **A contended cell does not kill a round.** One invocation's guard fails, the rest still land, and
  the failed one recomputes next round.
- **The scenario that motivated all of this** — a client merge landing mid-round — produces a value
  labelled with a watermark that is actually true, repeatedly, under parallelism.
- **The fan-out benchmark does not regress.** 128k entities currently derive in 1.1s at four cores;
  fork-and-merge per round must not undo that.

---

## 9. Acceptance scenarios

Organised by the *failure class* they attack rather than by feature, because the bugs this project has
actually hit cluster into a few repeated shapes: ordering assumptions that hold sequentially and break
concurrently, two similar-looking quantities getting conflated, features that work alone and not
together, and labels that claim more than was verified.

Each says what would be broken if it failed. That is what makes it a stress test rather than a
feature test.

### The claim that must not be false

**S1 — every watermark tells the truth.** For any derived cell: read its stated `reflects`, fork
there, recompute from scratch, compare. Identical, always. — **built**,
`scenarios/100-watermark-truth`.

This checks §10.1's headline claim directly rather than by proxy, and every ordering bug found so far
would have surfaced here. It is a *property* over whatever state other scenarios leave behind, which
makes it the cheapest ongoing insurance available.

### Guards, the newly load-bearing mechanism

**S2 — a stale transaction is rejected in either merge order.** *Failing means order-enforcement crept
back in.* — **built**, `scenarios/140-transaction-conflicts`.

**S3 — absence is a guarded read.** Two transactions both observe a cell absent and both try to create
it; one must lose. *Failing means absence tracking is decorative and concurrent creates silently
duplicate.* — **built**, `scenarios/140-transaction-conflicts`.

**S4 — a transaction does not conflict with itself.** Write `X`, read `X`, commit. *Failing means the
parent-reads-only rule is wrong and every read-modify-write deadlocks itself.* — **built**,
`scenarios/130-transactions`.

**S5 — guards do not over-reject.** Two transactions writing different fields of one object both land.
*Failing means guards are object-granular in practice and cell granularity is fiction.* — **built**,
`scenarios/130-transactions`.

**S6 — deleting an object conflicts with writing to it.** *This is the test for "implicit reads count":
the writer's existence probe is what makes it a conflict.* — **built**,
`scenarios/140-transaction-conflicts`.

### Derivation as a transaction

**S7 — a chained producer does not trip its own round's guard.** *Failing means rounds containing any
producer chain never commit.*

**S8 — a stale round cannot land, in either order.** *Failing means the deleted ordering rule was
necessary after all.*

**S9 — one contended cell does not kill a round.** *Failing means one hot cell starves a large round
forever.*

**S10 — a client merge landing mid-round produces a true watermark.** The original motivating bug, now
expected to be structurally impossible. *Failing means the branch boundary does not express the filter
and we have re-derived the ceiling problem.*

### The events/layers inversion

**S11 — authorship survives merge.** A merged value reports both where it was authored and where it
landed. *Failing means we inverted the pointers and kept rewriting anyway: no cost saved, no lineage
gained.* — **built**, `crates/borg-engine/tests/events.rs`.

**S12 — time travel across a merge is coherent**, and one event referenced by two layers resolves to
one identity rather than two values. *This is the specific risk the inversion introduces.* —
**built**, `crates/borg-engine/tests/events.rs`.

### Composition — features that have never met

This class has produced the most bugs and has the least coverage.

**S13 — migration under a concurrent client write.** *Migrations and concurrency have never been
exercised together.*

**S14 — a def-only merge landing while a round computes under the old def.**

**S15 — a second-order fork with a migration**, migrating data inherited through two levels.

### Determinism

**S16 — identical settled state across many parallel runs**, asserting the settled result and never
the schedule.

Frequency matters: milestone C's ordering bug appeared **one run in six**, and an `EPIPE` panic **one
in forty, under load only**. Both read as flakes and were not. Fewer than ~50 runs is not evidence.
