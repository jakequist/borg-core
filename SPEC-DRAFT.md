# Draft: the transactional model

> **Status: draft, not normative.** `SPEC.md` describes the system that exists. This describes a
> proposed successor. Nothing here is built.

Three changes, which turn out to be one change seen from three angles:

1. **Events get identity; layers reference them.** Today an event carries the layer it lives in.
   Inverting that lets one event belong to several layers, which is what makes a merge cheap.
2. **Every client write is a transaction.** A client forks, writes in isolation, and merges. It never
   writes to a shared branch.
3. **Derivation is a transaction too.** A round forks, runs, and merges. Producers are writers like
   any other.

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

### 7.1 Abandoned transactions leak

`borg tx begin` with no matching commit leaves a branch and its state behind forever. Under implicit
one-shot transactions that is rare, but an interactive or crashed client will do it, and nothing
reaps them. A TTL, or a sweep of transaction branches with no activity, needs designing — this is the
one concern with no existing answer anywhere in the system.

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
