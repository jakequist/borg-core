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

### Guards should become automatic

A transaction records what it *read*; at merge those reads become its guards. This is the same
read-set machinery producers already use. Without it, conflict detection stays opt-in and the model
offers the *appearance* of safety, which is worse than the honest last-write-wins it replaces.

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

### Rounds merge in `reflects` order

**This is a new rule and it is load-bearing.** Two rounds may run concurrently — one settling L10,
one settling L12. Both may invoke the same producer on the same entity, and both will write that
entity's cell. If the L10 round merges *second*, its older result wins.

So a round's merge must be ordered by the source layer it reflects. Concretely: a round may not merge
until every round reflecting an earlier source layer has merged. This is a queue on the branch, not a
lock on derivation — producers inside a round still run concurrently.

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

---

## 5. What this deletes

| | |
|---|---|
| §16.5 round ceiling | absent — a branch boundary expresses the filter exactly |
| the ceiling/prefix hole | cannot form; the concurrent layer is on another branch |
| merge-versus-round exclusion | not needed; they cannot observe each other |
| `ReadPath` carrying admitted layers | not needed; the fork point already does it |
| storage filtering on `reflects` | not needed, so the provider line stays clean |

The architecture gets smaller. That is the strongest argument for it.

---

## 6. Concerns

Ordered by how much they worry me.

### 6.1 Round merge ordering is a new correctness requirement

Covered in §3. Without it a stale round silently overwrites a fresher one, and the symptom is a
wrong value with no error — the same class as the ordering bug that failed one run in six during
milestone C. Any implementation must make it structural rather than incidental.

### 6.2 `O(1)` merge is not free, and the draft does not claim it

Membership rows are small but there are still `n` of them, and the read index must be updated
atomically with the merge or reads miss data. True `O(1)` requires reference-not-enumeration plus
read-path compaction — a subsystem, not a tweak. The honest claim is a large constant-factor win now
and an asymptotic one made *possible* later.

### 6.3 Derived output can land after source layers it predates

A round reflecting L10 may merge at trunk position L13, after a client's L12. Derived history is then
non-monotonic in `reflects`. Reading at exactly L12 shows L12's source data and *not* the derived
output computed from L10 — which is correct, since at L12 the round had not landed, but it is
surprising and the resolver's rule for picking a derived layer (§6.3) needs restating in terms of
position rather than `reflects` alone.

### 6.4 Write cost multiplies

One `borg set` becomes: fork, write, merge, then a round which is itself fork, run, merge. Two
branches and several layers per user write. Forks are `O(1)` and layers are cheap, but this is the
common case, and the CLI already pays `O(log)` per `Registry::open`. Worth measuring before
committing — the fan-out benchmark is the right instrument.

### 6.5 Branch proliferation, and no reaper

One branch per transaction plus one per round, retained forever in a table with no GC. They are
cheap, but the branch tree becomes mostly spent transactions. Whether merged branches are reaped or
kept as history is a real choice and should be made deliberately rather than by default.

### 6.6 Every read path grows a segment

A transaction's read path is `[(txn, head), (parent, fork)]`, and a round's likewise. Two segments
rather than one, on every read, for every write in flight. Bounded and small — but it is the hot
path, and it is worth confirming the resolver's segment walk stays cheap.

---

## 7. What I would want to prove before building

- **A round merging out of order loses an update.** Write the failing test first; make the ordering
  rule the thing that fixes it.
- **The scenario that motivated all of this** — a client merge landing mid-round — produces a value
  labelled with a watermark that is actually true, repeatedly, under parallelism.
- **The fan-out benchmark does not regress.** 128k entities currently derive in 1.1s at four cores;
  fork-and-merge per round must not undo that.
