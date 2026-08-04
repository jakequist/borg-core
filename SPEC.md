# Borg — v1 Specification

> **Status:** design spec for Act 1. Normative for v1 implementation.
> Sections marked *(deferred)* describe intent for later versions and are **not** to be built now.

---

## 1. Overview

Borg is an event-sourced data backend. The long-term ambition is to subsume the modern use cases of
ORMs, data pipelines, ETL and reverse-ETL. **Act 1 — this document — is the modern ORM.**

Three ideas define the system. Everything else follows from them.

**1. Definition changes are data changes.** Schema mutations travel the same event log, on the same
branches, through the same merge machinery as value mutations. There is no offline migration ritual.
A schema change is something you can fork, test, time-travel through, and merge.

**2. Identity is universal.** Every non-primitive value has a PID. A reference is a PID sitting in a
cell, so traversal is a pointer dereference rather than a join. This removes the economic pressure
that makes other systems denormalize, which in turn keeps the data small enough to afford rich
per-cell metadata.

**3. Derived data is honest about freshness.** Borg does not pretend computed values are current. It
tells you what they reflect, how stale they may be, and where they came from. This converts
eager-vs-lazy from an architectural commitment into a scheduling policy that cannot affect
correctness.

---

## 2. Glossary

| Term | Meaning |
|---|---|
| **Registry** | The root container. One per company, typically. Owns the branch tree and all repos. |
| **Repo** | A namespace-less contribution unit. Teams define structs, fields, and producers through repos. |
| **PID** | Point ID. Universal identifier for every non-primitive value. |
| **Cell** | The universal addressable unit: `(pid, field)` or `(pid, index)`. |
| **Buffer** | A physical partition of cells: one per struct for objects, global for interned values. |
| **Def** | A definition — `ObjectDef`, `ListDef`. |
| **Event** | A single mutation. Either a `ValueEvent` or a `DefEvent`. |
| **Layer** | An ordered group of events. Belongs to exactly one branch. |
| **Branch** | First-class fork of the registry timeline. |
| **ClientVersion** | A LayerId identifying the def-view an actor's code was authored against. |
| **Producer** | Anything that computes derived data: a pipeline or a migration. |
| **Watermark** | The source layer through which a derived value's inputs are fully incorporated. |
| **Frontier** | The layer through which all derived data on a branch is caught up. |

---

## 3. Identity and Values

### 3.1 PIDs

A PID identifies every value except primitives. Two flavors, split by mutability:

| Kind | PID allocation | Mutable |
|---|---|---|
| `Object`, `Any`, `AnyObject`, `AnyArray`, `AnyNumber`, `List` | allocated identity, survives mutation | yes |
| `String`, `Binary`, `BigInt` | content-addressed (hash of content) | no |
| `Int`, `Boolean`, `Double` | none — primitive, stored inline | n/a |

**Allocated PIDs** are `(branchId, allocatorId, counter)`. Small, ordered within an allocator, and
collision-free by construction — which makes merge safe without coordination.

> The `allocatorId` component exists so that **any node may allocate PIDs without coordinating.** A
> bare `(branchId, counter)` would require a global per-branch counter — a coordination point on the
> hottest path in the system. This is a persisted format, so it is fixed now rather than retrofitted
> (§17.2).

**Content-addressed PIDs** are branch-independent and eternal. The string `"hello"` has the same PID
on every branch, forever. Consequences: string writes can never conflict across branches, and equal
strings are stored exactly once registry-wide.

A PID encodes its own kind, so the dispatch to the correct buffer requires no lookup.

### 3.2 Primitives

`Int`, `Boolean` and `Double` have no PID because the identifier would cost more than the payload.
They are stored inline in the cell that holds them.

### 3.3 Deferred value types

`Set<T>` and `Map<K,V>` are **deferred**. When introduced they will dedupe by a JVM-style
`hashCode`/`equals` contract on the element type, not by PID.

---

## 4. Storage Model

### 4.1 Everything is a cell

The universal addressable unit is the **cell**:

```
CellRef { buffer: BufferId, key: CellKey }  ->  CellRecord

key = Pid                 // an object property, or an object's existence
    | (Pid, index)        // a list element
```

**The buffer is part of the address, not derived from it.** A sharded store must be able to route a
request from the cell address alone; if the shard key required a schema lookup first, every read
would need the defs before it could be sent anywhere (§17.2).

The cell is the right granularity because every mechanism in Borg is already field-granular:
transaction guards, producer dependencies, field ownership, migration staleness, and merge conflict
resolution all key on the same primitive.

Cross-repo extension falls out for free. When Repo#2 adds `Company.website`, it writes cells with a
new field key and touches none of Repo#1's storage.

### 4.2 Buffers

A **buffer** is a partition of cells, and the partition key is a def:

> **One buffer per def.** Values that have no def — interned and untyped ones — get exactly one
> buffer each.

| Buffer | One per | Holds |
|---|---|---|
| `ObjectBuffer` | `ObjectDef` | existence cells for that struct |
| `ObjectPropBuffer` | `FieldDef` | property cells for that one field |
| `ListBuffer` | `ListDef` | list existence cells |
| `ListElemBuffer` | `ListDef` | element cells |
| `StringBuffer` | — | interned strings, keyed by content hash |
| `BinaryBuffer` | — | interned binary |
| `BigIntBuffer` | — | interned bigints |
| `AnyObjectBuffer` | — | untyped object cells |
| `AnyArrayBuffer` | — | untyped array cells |

The interning buffers are singular by necessity: registry-wide deduplication is their entire purpose.
The `Any*` buffers are singular because untyped values have no def to partition by.

**Why this granularity.** Buffers are expected to do a great deal of work, and partitioning them
finely is what makes scaling them horizontally possible later (§17.2). Per-field partitioning also
matches Borg's actual access pattern: producers read *specific fields* and hop — that is the entire
point of field-level tracking — and almost never materialize a whole object. A def-mutation is then
confined to exactly one buffer, and a cross-repo extension like Repo#2's `Company.website` is
physically isolated from Repo#1's storage rather than merely logically distinct.

**A producer maps over an `ObjectBuffer`** (§9.2), which is precisely the set of instances of one
struct. Discovering new entities is one buffer scan rather than a filtered walk.

**Logical, not physical.** `BufferId` is a partition key, not a placement decision. A placement
policy is free to co-locate all of a struct's field buffers on one node for clients that do want
whole objects. Keeping those two concerns separate is what preserves the option.

Buffers hold complex values only. Primitives live inline in cells.

### 4.3 The cell record

```
CellRecord {
  value:      Primitive | Pid
  version:    LayerId        // the ClientVersion this value was written at
  writtenAt:  LayerId        // the layer that produced this value
  origin:     Source | Derived
  // derived only:
  producer:   ProducerId
  freshAsOf:  LayerId
  readSet:    CellRef[]
}
```

**Every cell carries its own def-version tag.** A def-mutation touching one field does not stale the
other fields of the object.

Source cells carry only `value`, `version`, `writtenAt`. The heavy metadata — watermark, read-set,
producer — attaches to derived cells only, which in a normalized model are the minority.

### 4.4 Lists

Lists are **append-only in v1**. Element cells are keyed `(pid, index)`; mid-list insertion would
shift every downstream key and is deferred.

**A list's own cell holds its length.** The container cell holds whatever is true of the container as
a whole — for an object, that it exists; for a list, how far it extends. This is what makes
*iterating* a tracked dependency: a producer that walks a list reads the length cell, so an append
changes a cell it read and recomputes it. Without that, appends would be invisible to anyone
iterating.

Note what this does *not* do: reading element `n` depends on element `n` alone. A producer that
takes only the last element of a list is unaffected by an earlier element changing, even though both
live in the same buffer.

---

## 5. Definitions

### 5.1 Defs

```
ObjectDef  { name, fields: { name -> FieldDef } }
FieldDef   { name, type, declaringRepo, origin: Source | Derived, version: LayerId }
           // note: no `writer` — discovered ownership lives in the dependency index (§8)
ListDef    { elementType }
```

`SetDef` and `MapDef` are deferred alongside their value types.

**There is no separate schema store.** The definitions in force at a point are *folded* from the
`DefEvent`s on the def layers along a read path (§7.2), exactly as data is resolved from value
layers. That is what makes a schema change forkable, time-travellable and mergeable rather than an
offline ritual — and it means "the def-view at layer L on branch B" needs no machinery beyond what
reading data already requires.

Note there is deliberately no `CreateObjectDef` in §6.1: a struct has no owner and exists because
someone declared a field on it (§5.2), so creation falls out of declaration.

### 5.2 Repos and the flat namespace

A **repo** is a contribution unit that lets different teams — eventually in different languages —
contribute to one shared data model. Repos are **not** an isolation mechanism.

The namespace is **flat**, and there is **no explicit `extends`**. If Repo#1 declares `Company.name`
and Repo#2 declares `Company.website`, they simply merge. A struct's definition is the union of all
field declarations across all repos.

Consequences:

- A struct has **no owner**. Only its *fields* do. `Company` exists because someone declared a field
  on it.
- A repo may mutate or delete only the fields it declared.
- Two repos declaring the same field name on the same struct is a **hard error** at push time.
- There is no repo dependency DAG. Declaration order does not matter.

### 5.3 Def-versions

**A def-version is a LayerId** — specifically, the def-layer that most recently mutated that
definition. No separate versioning scheme exists. The def-version DAG is the branch/layer DAG
restricted to def-layers.

### 5.4 ClientVersion

**Every actor that executes code carries a ClientVersion**: a LayerId identifying the def-view its
code was authored against. All reads by that actor resolve at that def-view.

| Actor | ClientVersion |
|---|---|
| External SDK | the layer its generated code was built from |
| Pipeline | the layer its repo's code was pushed at |
| Migration `up_v1→v2` | the layer of the def-mutation that introduced v2 |

This unifies clients, pipelines and migrations into one concept.

**Writes are stored at the writing actor's ClientVersion and are never coerced or rewritten.** A v1
client and a v5 client may read and write concurrently; the read path composes migrations in
whichever direction is required. This is why `down` migrations matter — they are what keep old
clients working.

### 5.5 The live-version set

Because migrations are eager producers (§9.1) and writes are never coerced, a cell can end up
materialized at *every* def-version anyone might read it at. Five live ClientVersions means up to
five copies of every affected cell and five migration chains fired per source write.

The registry therefore tracks a **live-version set** — the ClientVersions that actually have
registered clients. The derivation engine materializes only for versions in that set; anything else
is computed on demand via `freshness: 'current'` (§10.5). When the last client on a version
disconnects, that version's derived layers become droppable.

**v1 eats the storage cost** in exchange for accuracy; reduction policies are deferred.

The live-version set does double duty: it is also what powers the push-time warning *"this def change
has no `down` migration and will break the N clients currently on versions X and Y."*

---

## 6. Events and Layers

### 6.1 Events

**ValueEvents** mutate data:
`CreateObject`, `SetObjectProp`, `UnsetObjectProp`, `DeleteObject`, `ListAppend`, `CreateList`,
`DeleteList`.

**DefEvents** mutate definitions:
`CreateObjectDef`, `MutateObjectDef`, `DeleteObjectDef`, `DeclareField`, `MutateField`,
`DeleteField`, `PushProducer`.

Every `DefEvent` that alters the shape of existing data **must** supply a migration (§9.3).

### 6.2 Layers

A layer contains ValueEvents **xor** DefEvents, never both. This is what makes "the def-version as
of layer L" well-defined.

A layer belongs to exactly one branch. LayerIds are registry-unique.

**A layer is the universal unit of atomicity**, for client transactions and producer runs alike. Both
follow the same state machine:

```
        open ──────► sealed ──────► committed
         │             │
         └─────────────┴──────────► aborted
```

| State | Meaning |
|---|---|
| `open` | exclusive to its owner; writes accumulate; invisible to every reader |
| `sealed` | writes closed; durability and validation happen here (source layers validate guards at seal) |
| `committed` | visible to readers — **this edge is what triggers dependent producers** |
| `aborted` | discarded; never visible |

The lock is held **per layer, not per branch.** Many layers may be open concurrently — a client
transaction alongside several producer runs — because the single-writer rule (§8) guarantees no two
producers can ever target the same cell, so committed derived layers can never conflict and may
commit in any order.

**A layer can be enormous.** One producer run writes all of its output into exactly one layer, so a
flip of `school.is_top_ten` may produce a single layer containing 100k mutations. Layer commit must
therefore **stream**; a layer can never be assembled in memory and flushed. This is the single
hardest constraint the log places on `StorageProvider` (§17).

### 6.3 Source layers and derived layers

The log is two interleaved streams:

- **Source layers** — authored by external clients. The ground truth.
- **Derived layers** — appended by the derivation engine as it catches up. Each carries
  `reflects: LayerId`, the source layer it brings the world up to.

The **watermark is a pointer into the source stream.** Derived layers chase it.

Time travel resolves: source cells at layer ≤ L; derived cells from the derived layer with the
greatest `reflects` ≤ L.

> **Derived layers are droppable.** They are a cache that happens to live in the log. Garbage-collect
> them and the fallback is recompute. No data loss is possible, because source is separate.

**Derived history is deterministic.** One producer run produces exactly one layer, and v1 performs
**no coalescing**: a producer emits one derived layer per `(producer, source layer)` even when
several source layers landed while it was busy. Two Borg instances replaying the same source log
therefore emit identical derived content at every `reflects` point.

Ordering is enforced where it is meaningful — a producer reading another's output cannot start until
that producer's layer commits — and the only residual variation is which LayerId was assigned to
which of two *concurrent independent* producers. That is unobservable, because history is always
addressed by **source** layer and derived data resolved by `reflects`, never by derived LayerId.

*(Coalescing across source layers is the natural v2 optimization and is a scheduler policy, not a
redesign. It trades reproducible history granularity for less recomputation.)*

**The cost of no-coalescing is one layer per invocation.** A source write that invalidates 100k
entities produces 100k derived layers in a single round, each holding that invocation's output. This
is inherent to "one run, one layer" and is what buys reproducible derived history. It is also the
single largest constant in the fan-out path, and the first thing a coalescing policy would reclaim.

---

## 7. Branches

### 7.1 Model

```
Branch {
  id:      BranchId
  name?:   string
  origin?: LayerId    // null => root of the tree
}
```

The parent branch is inferred from the origin layer; no explicit parent pointer exists.

Branches are **registry-scoped** — one global branch tree spanning all repos. This is what makes
cross-repo def-mutations atomic and gives a producer in one repo an unambiguous version at which to
read another repo's data.

### 7.2 Reads

A read is `(branchId, layerId?, clientVersion, freshness?)`. Omitting the layer reads HEAD.

The engine resolves this into a **read path**: the branch bounded at the requested layer (or its
head), then each ancestor bounded at the fork point below it. Storage walks the segments outward and
**the first segment holding any record wins** — *any record*, not any value, because a tombstone on a
child must stop the walk rather than fall through and resurrect the parent's value.

An ancestor is bounded at the fork point, and further clamped only when an explicit layer was
requested, which may sit below that fork point. Clamping an ancestor by the child's own head instead
would leave a fork that has not yet written anything unable to see its parent at all.

Storage is handed the read path and never has to know what a branch *is* — which keeps
`StorageProvider` (§17.1) free of branch semantics.

### 7.3 Commit

Layers are totally ordered within a branch, but that order is established **at commit**, not at open.
Many layers may be open on a branch simultaneously (§6.2); sequence is assigned as each seals and
commits.

### 7.4 Fork cost

**A fork is O(1) even under eager derivation.** A new branch inherits its parent's derived layers by
ancestry exactly as it inherits source layers, and diverges only where it writes.

The one expensive case is forking and immediately pushing a def-mutation, since every affected cell
must migrate on the new branch. Watermarks absorb this: the fork commits instantly, the migration
producer begins chasing, and the frontier reports how far it has got.

---

## 8. Source and Derived Data

This split is the organizing distinction of the system.

**Source data** is pushed by external clients. It is ground truth. It is always current — a source
cell written at L400 is correct at L400 and at every layer after, until overwritten.

**Derived data** is computed by producers. It is *never assumed fresh*. It carries a watermark and a
lineage, and the system's job is to be honest about both rather than to pretend.

**Origin is a property of the `(struct, field)` pair, not of the object.** A single `Company` may
carry source cells (`name`, `website`) and derived cells (`is_investible`) side by side.

**Every field has exactly one writer.** In v1 this is discovered at runtime and enforced by throwing
on violation.

**Discovered ownership lives in the dependency index, never in the def.** Defs are mutated by
DefEvents, which live in def-layers; discovery happens during derivation, which emits value layers.
Recording an owner into the def would therefore mean the derivation engine emitting def-mutations —
violating the value-xor-def rule (§6.2) and letting a producer's first run silently rewrite the
schema. Ownership is discovered state, and belongs with the other discovered state.

### 8.1 Tombstones

A **tombstone** is a value a cell can hold, meaning *explicitly removed* — as distinct from *never
written*, which is absence. Both are legitimate tracked reads and a producer must be able to tell
them apart.

Because a tombstone is cell-valued, **the cell it occupies determines what was removed**, and the
concept generalizes with no new machinery:

| Event | Tombstones | Meaning |
|---|---|---|
| `UnsetObjectProp` | a property cell | that field is unset |
| `DeleteObject` | an existence cell | the object is gone |
| *(deferred)* set-member / map-entry removal | that member's cell | one element removed |

Nothing is ever physically removed, because time travel requires the history to remain intact.

**Existence is itself a cell.** It must be, or it could not appear in a read-set — and then deletion
could not invalidate anything.

**`DeleteObject` tombstones the existence cell and every property cell defined on the struct** —
`O(fields)` writes, bounded by the def rather than by what was actually set. The alternative was to
tombstone existence alone and make every cell read implicitly depend on its object's existence cell,
which keeps deletion `O(1)`. That trade is the wrong way round: it taxes *every read forever* with an
extra read-set entry in order to make a rare operation cheap. Reads vastly outnumber deletes.

Tombstoning every defined field also keeps invalidation exact with no special-casing — a dependent on
`company#100.name` observes a real write to that exact cell — and it distinguishes a field that was
*removed* from one that was merely never set.

With that in place, deletion needs no dedicated machinery at all: **a tombstone is just a write**, so
it flows through the dependency index and invalidates dependents like any other change. A producer
whose input vanished produces nothing, so its outputs — including deterministically-PID'd output
objects (§9.5) — are tombstoned by the same path. Deletion cascades through the derivation graph for
free.

### 8.2 Dangling references

**v1 permits dangling references.** Refcounting a graph in which strings are content-addressed and
shared registry-wide is a global operation on a system that is otherwise strictly local, and it is
not worth it yet. A cell holding a PID whose object has been deleted resolves to `state: 'tombstoned'`
in the read envelope (§10.4) rather than throwing. Opt-in referential integrity is deferred.

---

## 9. Producers

### 9.1 Unified model

A **producer** is anything that computes derived data. Pipelines and migrations are the same
mechanism with different triggers:

| | Pipeline | Migration |
|---|---|---|
| Triggered by | a source data write | a def-mutation |
| Input | cells from one or more source buffers | the same cell at an older def-version |
| Output | cells (fields on existing objects, or new objects) | the same cell at a newer def-version |

Both record read-sets, both participate in the dependency index, both are subject to cycle
detection, both are scheduled by the same policy, both carry watermarks.

### 9.2 Pipelines

A pipeline is imperative user code, defined in a repo, that reads cells and writes cells.

**v1 pipelines are per-entity maps** over a source buffer: one invocation per entity, one read-set
per invocation. Aggregations are deferred *(later: graded lineage tracking — per-record at one end,
whole-buffer at the other, with probabilistic middle ground such as bloom filters).*

Pipelines must be **idempotent**. Enforcing this is the user's responsibility.

Example:

```python
def should_invest_in_startup(company: Company):
  score = 0
  if company.website.ends_with('.ai'):
    score += 3
  for founder in company.founders:
    if founder.last_education().school().is_top_ten:
      score += 3
  company.is_investible = score > 5
```

The recorded read-set is `{(company, website), (company, founders), (founder_i, educations),
(school_j, is_top_ten), …}`. A write to `company.name` triggers nothing.

**Pushing new pipeline source is a `DefEvent`** that moves the producer's ClientVersion and
invalidates all of its prior output, triggering a full recompute across its source buffer.

**Definition and implementation are separate.** The log records only the *definition* — `ProducerId`,
source buffer, ClientVersion. The `ExecutionProvider` (§17) resolves that ID to an *implementation*.
In v1 that resolution is a static registry of Rust functions compiled into the binary; later it is a
container image reference reached over a socket. The log's model is identical either way.

### 9.3 Migrations

A migration accompanies every shape-changing `DefEvent`. It is **a set of per-output-field
functions**, each with its own read-set — not a whole-object transform. Reading `(acme, region)` at
v2 invokes only the function producing `region`, which reads only `(acme, country_code)` at v1.
Version-heterogeneous objects are therefore not a problem.

> A migration that *splits* one field into two runs its logic once per projected output field. This
> is wasteful but cacheable, and preferable to reintroducing whole-object semantics.

Migrations may read arbitrary data — they are full producers, not pure local functions. Their reads
are **live**: they resolve at the *reader's* layer, so a later correction to a dependency becomes
visible through the new lens. A migrated cell is a reactive view, not a one-time result.

Migration reads resolve at the migration's own ClientVersion (§5.4). A migration that reads another
value of the type it is migrating recursively invokes itself on that value — which terminates on a
DAG and trips the cycle detector otherwise.

**A migration maps over the field's buffer**, not the struct's — it is defined per output field, and
per-field buffers (§4.2) make that exactly expressible. A write at the source version is work for the
migration in precisely the way a data write is work for a pipeline.

**A producer is never triggered by its own output.** A migration writes into the very buffer it
consumes from, one version along, and would otherwise re-trigger itself forever. Its own output
appearing in its source buffer is not a new entity. This applies only to the new-entity trigger —
the read-set trigger is deliberately *not* filtered by author, because a producer disturbing a cell
it reads is exactly a cycle and must be caught rather than hidden.

**Optimization:** if a migration's recorded read-set contains only its own source cell, the system
*infers* purity and caches the result permanently with no invalidation. Inferred, never declared.

**v1 trusts `down`.** If the user supplies one, it is assumed correct. Probabilistic detection of
lossy or partial `down` migrations is deferred.

### 9.4 Dependency tracking

Dependencies are captured at **field granularity, through hops**, including negative reads (checking
a field that is absent is a dependency on its absence).

**Read-sets record the version each cell was read at**, not the cell alone. `CellRef` is the shard
key — where a cell lives, computable without a schema lookup; `CellAt` is the record key — which of
that cell's versions you mean. The dependency index and field ownership both key on `CellAt`. Keying
them on `CellRef` would make a migration, which reads `C@v1` and writes `C@v9`, observe its own
output as a change to its own input and poison itself as a cycle. It also means a pipeline reading
`C@v1` is correctly left alone when a migration materializes `C@v9`.

**Field ownership is per version.** The same cell at v1 and at v9 is legitimately written by
different producers — a client and the migration carrying its value forward.

**Capture is automatic and requires no declaration.** Every cell access a producer makes goes through
`ProducerCtx` (§17), so the engine observes reads and writes exactly, with nothing for an author to
declare or mis-declare. This is why `ExecutionProvider` is defined as *"run this code, mediating
every cell access through me"* rather than merely *"run this code"* — the mediation is what makes
tracking free, and it is equally satisfiable by an in-process call and by a socket round-trip.

Example: `is_top_company` reads `Company#100.description` and `Person#300.title`, then writes
`Company#100.is_top_company`. v1 records the fully-enumerated edge set:

```
[Company#100.description, Person#300.title]  ->  [Company#100.is_top_company]
```

This is deliberately verbose and costly. Compressed and probabilistic tracking policies are deferred
to v2+.

The dependency index is maintained in both directions:

```
forward:  cell -> dependent invocations    (drives invalidation)
backward: cell -> its dependencies         (drives lineage)
```

**One structure, read two ways.**

### 9.5 Deterministic output identity

A producer that emits new objects must derive their PIDs deterministically:

```
outputPid = hash(producerId, inputPid, outputSlot)
```

This is **required**, not an optimization. A producer re-running after a dependency change must
update the *same* output object rather than creating a duplicate. This is what makes eager
re-execution safe and is the concrete form of the idempotency guarantee.

### 9.6 Scheduling

Materialization is **asynchronous and continuous**. A source write commits immediately; the
derivation engine chases it and advances watermarks.

Because every derived value states what it reflects, the scheduling policy **cannot affect
correctness, only latency.** `NaiveEagerProducerPolicy` in v1; smarter prioritized policies later,
provably without regression.

**Invalidation is driven by layer commit, not by buffer instrumentation.** A committed layer *is* the
changeset — it already names precisely which cells moved — so buffers require no observation
machinery at all. One pass over a committing layer's contents answers both trigger questions:

| In the layer | Triggers |
|---|---|
| cell writes | existing invocations that depend on those cells go dirty |
| object creations | new invocations for producers subscribed to that buffer |

Keeping this above the provider line matters: were tracking to live inside the buffers, every
`StorageProvider` would have to reimplement it. The storage interface stays small — get cell, put
cell, stream a layer, scan a buffer — which is what keeps both a plain KV store and Postgres viable
behind one seam.

The engine internally enumerates a producer's source buffer in order to discover new entities. This
is engine-internal; enumeration is **not** exposed as a user-facing query in v1.

A commit on branch B triggers producers on branch B only, since a layer belongs to exactly one
branch.

---

## 10. Watermarks and Freshness

### 10.1 Definition

> **Watermark:** the source layer up to which all of a derived value's inputs have been incorporated.

The claim it makes is: *if you replayed the world at layer W, you would get exactly this value.* The
value may also still be correct at later layers; that is a separate question, answered by validation.

### 10.2 Validate vs. recompute

Two distinct operations, and separating them is where the efficiency lives:

| Operation | What it does | Runs user code | Advances |
|---|---|---|---|
| **Validate** | checks the dependency index for changes in `(freshAsOf, target]` | no | `freshAsOf` |
| **Recompute** | re-runs the producer, yielding a new value and read-set | yes | `writtenAt` **and** `freshAsOf` |

Most derived cells most of the time need only validation. A cell depending on `website` and three
schools is unaffected by the other 40,000 writes that landed meanwhile, and its watermark advances
to HEAD for the cost of a few index lookups.

**Reads validate before reporting**, so the returned watermark is tight rather than pessimistically
understated.

### 10.3 Composition

Watermarks propagate through chained producers:

```
W(B) = min(target, W(A), W(other deps...))
```

Any derived cell can therefore report an honest *transitive* freshness — the minimum over its entire
derivation chain, migrations included. A v1 client reading a v3-produced field through a
down-migration receives a watermark accounting for both hops.

### 10.4 The read envelope

Every cell read returns provenance, not a bare value:

```ts
// Named `Resolved<T>` in the Rust implementation: `Cell` collides with CellRef,
// CellRecord and std::cell.
type Cell<T> = {
  value:     T
  origin:    'source' | 'derived'
  writtenAt: LayerId    // when this value was actually produced
  freshAsOf: LayerId    // certain-correct through here
  state:     'current' | 'unvalidated' | 'stale' | 'broken' | 'tombstoned'
  by?:       ProducerId
  // deferred: expectedFreshAt — an ETA for when catch-up will complete
}
```

| State | Meaning |
|---|---|
| `current` | `freshAsOf == requested layer`. Guaranteed correct. Source cells are always this. |
| `unvalidated` | Behind, but unchecked. Cheap to resolve. |
| `stale` | A dependency is known to have moved. Definitely out of date. |
| `broken` | The producer threw or cycled. `IllegalState`, scoped to this cell. |
| `tombstoned` | Explicitly removed (§8.1), or reached through a dangling reference (§8.2). |

For source cells `writtenAt` and `freshAsOf` collapse — source data is written once and correct
thereafter. The distinction only carries information for derived data.

Worked example, reading `company#100` at HEAD = L500:

```
company#100:
  name             "Acme"   source    writtenAt L120   freshAsOf L500   current
  is_hot_startup   true     derived   writtenAt L400   freshAsOf L450   stale
                                      by pipeline:invest_score
```

### 10.5 Client controls

**Freshness requirement on read:**

```ts
read(pid, field, { freshness: 'any' | 'validated' | 'current' })
```

`current` forces inline computation and blocks. **Lazy materialization is therefore a per-read client
mode, not a system architecture** — a client that needs a fresh answer pays for it at the call site;
everyone else takes the lag.

**Await the frontier:** `await branch.frontier.reaches(L500)` — read-after-write consistency for the
clients that need it, and deterministic tests without a synchronous system.

**Two consistency modes, both honest:**

- **Ragged head** — read at HEAD. Latest of everything; freshness varies per field.
- **Settled frontier** — read at the highest layer through which *all* derived data is caught up.
  Fully coherent snapshot, slightly in the past.

A dashboard wants ragged; a report wants settled.

---

## 11. Lineage

Lineage requires no new storage. It is the dependency index read backwards.

```
explain(company#100, is_hot_startup) →
  produced by  pipeline:invest_score @ ClientVersion L380
  writtenAt    L400
  freshAsOf    L450
  from
    (company#100, website)     source    @ L120
    (company#100, founders)    source    @ L390
    (school#77,  is_top_ten)   derived   @ L400   ← recursively expandable
```

---

## 12. Transactions

v1 supports `ObjectTransaction` only. `ListTransaction` and others are deferred.

```ts
type ObjectTransactionEvent = {
  mutation: Event
  guards: {
    pid:    Pid
    fields: FieldName[]
    since:  LayerId
  }[]
}
```

A guard asserts that nothing has touched those cells since that layer. **Validated at seal** — before
anything becomes visible, so a rejected transaction leaves no trace on the branch.

Checked against a **cell-touch index** (`cell -> layers that wrote it`). Only *source* layers are
recorded in it: guards may name source cells only, so a derived write can never appear in a guard,
and derived layers are the enormous ones. Skipping them bounds the index by authored data rather than
by everything the derivation engine produces.

The index is queried along the **read path** (§7.2) rather than one branch, which is exactly what
lets a child's guard be re-evaluated against its parent at merge time.

**Guards may reference source cells only.** Guarding a derived field is meaningless — its value is a
function of source data with a lag, so the guard would be checking a shadow. Guard the sources
instead.

---

## 13. Merge

Merge replays the child branch's **source** events onto the parent as **new** layers. Derived layers
are tagged and skipped — the child's derived values are wrong on the parent by construction, because
the underlying data differs. The parent's derivation engine re-derives in the background.

The user chooses **def-only** or **def+data**.

**Def-only.** Replays DefEvents. If the parent moved the *same* def since the fork point, the merge
is rejected — re-fork from head and redo. Different defs touched, no conflict.

**Def+data.** Also replays ValueEvents. Each retains its authored ClientVersion, so the parent's
readers migrate as needed; nothing is coerced.

**Validation precedes any write.** v1 rejects a whole merge rather than applying it partially, so
every layer is checked before the first one is replayed and a rejected merge leaves no trace on the
parent.

**Failure and conflict rules:**

| Situation | Result |
|---|---|
| Parent moved the same def since fork | reject merge — the child authored against a def-view the parent has since moved, so re-fork from head and redo |
| Child wrote to an object the parent deleted | reject merge |
| Child guard fails when re-evaluated against parent history since the fork point | reject merge |
| Two branches wrote the same cell, unguarded | last-write-wins — child wins |

**Guards are the conflict detector.** Re-evaluating a child's guards against the parent's history
since the fork point is exactly the question "did the parent touch this while I was working?" LWW is
the default; guards are the opt-in to safety. Cell granularity makes LWW far less destructive than
object-level LWW would be.

---

## 14. Failure Model

Failures are detectable only at runtime: cycles, throwing migrations, and field-ownership violations
cannot be caught statically.

**`IllegalState` attaches to the producer, not to the branch.** Because source and derived data are
separated, a cycling pipeline or a throwing migration corrupts derived data only — source data is
untouched and still correct. The affected cells report `state: 'broken'` with lineage explaining why,
and everything else keeps working.

This means **main never breaks because someone merged a bad pipeline.** It is also strictly less
machinery than branch-wide poisoning, since no broken-layer-range tracking is needed.

Recovery: fix the producer and push a new ClientVersion, which invalidates and recomputes its output.

v1 is deliberately strict elsewhere — whole-merge rejection rather than partial application. Softening
these edges is later work.

---

## 15. Code Generation

**Generated SDKs are deferred out of v1.** A generated client needs a transport to reach the engine,
and v1 has no network layer — building one competes directly with building the engine.

When SDKs arrive they will come with a socket/network layer, and the generation contract is:

- Generated from the registry's defs at a chosen layer; that layer becomes the client's ClientVersion.
- All fields emitted as writable; ownership violations throw at runtime. Static read-only marking of
  derived fields is deferred further still.
- Reads return the provenance envelope of §10.4.
- TypeScript first, then Python, Rust, Go.

**The v1 constraint that makes this possible later:** every engine operation must have a
**serializable command/response form** — no callbacks, no borrowed references escaping the API
surface, no in-process-only affordances. This is nearly free now and expensive to retrofit; it is the
difference between "add a transport" and "redesign the API."

---

## 16. Internal Architecture

Five planes. The derivation plane is a cycle, and that cycle is the system.

```
  ┌─ STORAGE ─────────────────────────────────────────────┐
  │  Buffers  ──over──►  StorageProvider  (KV | Postgres) │
  └───────────────────────────────────────────────────────┘

  ┌─ LOG ─────────────────────────────────────────────────┐
  │  LayerManager    open / seal / commit / abort         │
  │  BranchManager   fork / merge / ancestry              │
  └───────────────────────────────────────────────────────┘

  ┌─ DEFINITION ──────────────────────────────────────────┐
  │  DefRegistry     defs, def-versions, ClientVersion,   │
  │                  live-version set                     │
  └───────────────────────────────────────────────────────┘

  ┌─ DERIVATION (the cycle) ──────────────────────────────┐
  │                                                        │
  │    layer committed                                     │
  │         │                                              │
  │         ▼                                              │
  │    Invalidator ──lookup──► DependencyIndex             │
  │         │                    fwd: invalidation         │
  │         │                    bwd: lineage              │
  │         ▼                                              │
  │    Scheduler ◄── ProducerPolicyProvider                │
  │         │                                              │
  │         ▼                                              │
  │    ProducerRuntime   opens a layer, runs user code,    │
  │         │            read-proxy records and verifies   │
  │         │            declared read/write sets          │
  │         └──────────► commits ──┐                       │
  │                                │                       │
  │         ┌──────────────────────┘                       │
  │         ▼                                              │
  │    (triggers the next producers)                       │
  └────────────────────────────────────────────────────────┘

  ┌─ READ ────────────────────────────────────────────────┐
  │  Resolver         cell → migrate for version skew →   │
  │                   validate watermark → envelope       │
  │  FrontierTracker  per-producer watermarks, settled    │
  │                   frontier, frontier.reaches()        │
  └───────────────────────────────────────────────────────┘
```

### 16.1 Components

| Component | Responsibility |
|---|---|
| `Buffers` | cell storage per value class; no derivation awareness |
| `LayerManager` | layer identity and the state machine of §6.2; streaming commit |
| `BranchManager` | branch tree, fork, merge, ancestry queries |
| `DefRegistry` | defs, def-versions, ClientVersion resolution, live-version set |
| `Invalidator` | walks a committing layer, converts it into dirty invocations |
| `DependencyIndex` | bidirectional cell ↔ invocation graph; in-memory-primary |
| `Scheduler` | **stateless** (§16.4); derives pending work from watermark gaps, settles each source layer as a closure (§16.5) |
| `ProducerRuntime` | executes user code; owns the read proxy that records and verifies read/write sets |
| `Resolver` | read path: locate cell, migrate for version skew, validate, build envelope |
| `FrontierTracker` | per-producer watermarks, settled frontier, `frontier.reaches()` |
| `CellTouchIndex` | `cell → layers that wrote it`; backs guard validation (§12) |

### 16.2 Verbs

| Plane | Verbs |
|---|---|
| Log | `open` · `seal` · `commit` · `abort` |
| Branch | `fork` · `merge` |
| Derivation | `invalidate` · `schedule` · `run` |
| Watermark | `validate` · `recompute` |
| Read | `resolve` · `explain` |
| Definition | `push` |
| Codegen | `generate` |

### 16.3 Load-bearing invariants

1. **Single-writer per field ⇒ derived layers never conflict.** This is what permits many producer
   layers to be open at once and to commit in any order.
2. **Layer commit is the only invalidation trigger.** Buffers carry no observation machinery, which
   keeps `StorageProvider` small and swappable.
3. **Commit streams.** A layer may hold millions of mutations and can never be buffered whole.
4. **Locks are per-layer, never per-branch.** A branch-wide write lock would serialize derivation.
5. **Derived data is addressed by `reflects`, never by derived LayerId.** This is what makes the
   ordering of concurrent independent producers unobservable.
6. **No membership test in the dependency index may be a linear scan.** A widely-shared upstream
   cell accumulates one dependent per invocation, and every one of them retracts itself on re-run.
   Vector membership turns a fan-out of `n` into `O(n²)` — measured, and the difference between
   1.3s and 0.28s at 32k dependents. Sets everywhere the index dedupes or removes.

### 16.4 The scheduler is stateless

**There is no work queue.** Pending work is fully implied by the gap between a producer's watermark
and the branch head, plus the dependency index. The scheduler *derives* what to run by streaming the
layers in that gap, rather than materializing a list of invocations.

This is not merely an optimization. Without it, naive-eager with no coalescing is exactly the
configuration that explodes: one write to a widely-depended-on cell enqueues 100k invocations, and a
def-mutation on a large type enqueues millions. Three properties fall out of having no queue:

- **Bounded memory** — work is streamed, never accumulated.
- **Free crash recovery** — restart and recompute the gap; there is no queue to lose or replay.
- **Distributability** — workers derive their own work from shared state instead of contending on a
  shared queue.

### 16.5 A source layer settles as a closure

A committed layer triggers producers; their output commits further layers, which trigger more.
**All of it carries the same `reflects`**, because it is all the consequence of one source layer. A
producer's watermark advances to `L` only once that whole closure has settled — which is precisely
what makes the watermark's claim true: *replay the world at `L` and you get exactly this.*

Only **source** layers open a round. Derived layers are consequences, picked up inside the closure
rather than driving rounds of their own. Skipping derived layers entirely is tempting and wrong: it
silently breaks every chained producer, since a producer consuming another's output can only ever be
triggered by that producer's derived layer.

**The layer a producer reads at is not the layer it reflects.** Within a round, reads resolve at the
round's **ceiling** — the source layer plus every derived layer already committed as a consequence of
it — while the output is labelled with the source layer. Without that separation a downstream
producer could never see its upstream's output, because that output lives in a derived layer with a
*higher* id than the source layer they both reflect.

Ordering within a round is not prescribed. If a downstream producer happens to run before its
upstream, it computes from an absent input, the upstream then commits, and the downstream's
dependency on that cell brings it back round. The fixpoint self-corrects; only the settled result is
guaranteed.

> The ceiling is safe because derivation is the only writer while a round settles. Under concurrent
> source writes the precise formulation is *"the highest layer that is either ≤ L, or is a derived
> layer with `reflects == L`"* — same meaning, expressed without relying on quiescence.

### 16.6 Cycle detection

A cycle is a producer that transitively depends on a field it writes. It cannot be detected
statically, and under a stateless scheduler it does not surface as re-entry — it **livelocks**: the
producer runs, advances its watermark, dirties its own input, and is rediscovered forever.

**v1 detection is a per-invocation re-run counter scoped to one round (§16.5).** If an invocation
runs more than `K` times while settling a single source layer, it is cycling; the producer is marked
broken (§14) and its output cells report `state: 'broken'`.

*(The precise form — walk the forward index from each written cell and test whether it reaches
anything in the transitive closure of the read-set — is deferred to a v2 policy provider.)*

---

## 17. Provider Interfaces

Every substitutable mechanism sits behind a provider so that v1's naive implementation can be swapped
without semantic change.

| Provider | v1 implementation | Later |
|---|---|---|
| `StorageProvider` | SQLite (`borg-storage-sqlite`), or in-memory | native Borg storage |
| `DependencyIndexProvider` | in-memory, key-ranged | persistent, sharded by cell key |
| `ProducerPolicyProvider` | `NaiveEagerProducerPolicy` | prioritized, incremental, batched |
| `ExecutionProvider` | in-process Rust, or a subprocess over stdio (§17.4) | container over a socket |
| `ErrorPolicyProvider` | `NaiveProducerPoisonPolicy` | partial / per-cell recovery |
| `CodegenProvider` | — (deferred, §15) | TypeScript, Python, Rust, Go |

The dependency index is designed **in-memory-primary** rather than as a disk structure with a cache.
This is a deliberate bet on the normalization thesis (§1): identity makes normalization free,
normalization keeps the working set small, and a small working set makes `validate` a handful of
memory lookups.

### 17.1 `StorageProvider` surface

Deliberately minimal, so that a plain KV store and Postgres remain equally viable:

```
get_cell(branch, cell, layer) -> CellRecord?
put_cell(open_layer, cell, CellRecord)          // streaming; never buffered whole
scan_buffer(branch, buffer, layer) -> Iterator   // engine-internal enumeration only
open_layer / seal_layer / commit_layer / abort_layer
```

**Streaming commit is the binding constraint** (§6.2). Any provider that cannot accept an unbounded
write stream into an uncommitted, invisible layer is disqualified.

The SQL shape that satisfies it: rows are inserted as they arrive and **visibility is a join, not a
rewrite** — every read joins the cell table against a layer table and keeps only rows whose layer is
committed. Committing is then a single-row update, `O(1)` however large the layer. Flipping a
`visible` column on each row instead would make commit `O(rows)` and undo the very property the
interface exists to preserve.

Reading at HEAD is expressed as *no layer*, not as the branch's head: a fork that has not written
anything yet has no head of its own, and its effective ceiling is the fork point it inherits.

**Blocking backends belong on a blocking pool.** The trait is async and the whole engine above it
awaits, so a synchronous store is the provider's problem alone and must not occupy an async worker
thread.

**Writes batch; reads do not need to.** `put_cell` is called an unbounded number of times, so
dispatching each one individually makes overhead dominate. A provider is free to accumulate writes
into a **bounded** buffer and flush in one transaction — safe precisely because an open layer is
invisible, so nothing can observe the difference between a row written immediately and one written
at the next flush. Bounded is the operative word: "never buffer a layer whole" still holds, and a
ten-million-cell layer passes through in fixed memory. Nothing about derivation,
dependency tracking or watermarks appears in this interface — all of that lives above the provider
line.

### 17.2 Distributable, not distributed

Borg is **designed to be distributable and implemented single-node.** Building a distributed system
from day one reliably produces coordination overhead everywhere, distributed code paths no test
exercises, and a system that is both slower and still not distributed.

Instead, every point that would require coordination sits behind a trait with a naive in-process
implementation. Distribution later is five swaps, not a rewrite:

| Seam | v1 | Later |
|---|---|---|
| `LayerSequencer` | in-process atomic counter per branch | consensus, or partition by branch |
| `PidAllocator` | one allocator id per process | many, no coordination (§3.1) |
| `LockManager` | in-process map with expiry | lease service |
| `WorkSource` | in-process, stateless (§16.4) | unchanged — workers derive their own work |
| `DependencyIndexProvider` | in-memory, key-ranged | sharded by cell key |

Two decisions taken earlier for unrelated reasons turn out to be what makes this viable:
content-addressed string PIDs require no coordination at all (any node computes the same hash), and
deterministic output PIDs (§9.5) mean two workers racing the same invocation produce identical
output — which is what makes at-least-once work dispatch safe.

**Constraint on the index interface:** all dependency-index access must be expressed as key-ranged
lookups, never "iterate everything." Identical single-node, shardable later.

### 17.3 `ExecutionProvider` surface

The contract is *"run this code, mediating every cell access through me"* — not merely "run this
code." That mediation is what makes dependency capture free (§9.4) and is equally satisfiable by an
in-process call and a socket round-trip.

```rust
#[async_trait]
pub trait ExecutionProvider {
    async fn run(&self, producer: &ProducerRef, input: Pid,
                 ctx: &mut dyn ProducerCtx) -> Result<()>;
}

#[async_trait]
pub trait ProducerCtx {
    async fn get(&mut self, cell: CellRef) -> Result<Option<Value>>;  // recorded
    async fn set(&mut self, cell: CellRef, v: Value) -> Result<()>;   // ownership-checked
}
```

### 17.4 The worker protocol

A producer worker speaks a framed message stream: the engine invokes, the worker asks for cells, the
engine answers. Codecs are negotiated in a handshake and framing is **per codec** — newline-delimited
for text so a shell worker can use `read`, length-prefixed for binary. Every encoding is produced by
the same serde impls on the same types, so no mapping document exists between them to drift.

Two shapes were forced by targeting a shell worker first, and both are better than what they replaced:

- **Cells and values travel as text** — `"Company#100.website"`, `"9"`, `"@Company#101"`, `"~"` —
  the same forms the CLI accepts. A worker cannot reasonably assemble the structural JSON of a cell
  address, and a protocol only usable through a generated client library is one whose complexity is
  hidden rather than absent. Text also removes the `Int`/`Double` ambiguity a bare JSON number has.
- **Every message is a single-key object**, including the payload-free ones. A worker dispatches on
  one key without special cases.

**`ProducerCtx` is async from day one**, even though the v1 in-process implementation only ever
returns ready futures. A socket-backed provider performs a round-trip per cell read, and retrofitting
async through the derivation engine afterwards is a far larger change than paying for it now.

---

## 18. v1 Scope

**In:**

- Cell-addressed storage, buffers, content-addressed interning
- `StorageProvider` interface, SQLite and in-memory backends, streaming commit
- Event log; source and derived layers; the layer state machine
- Registry-scoped branch tree; time travel; O(1) fork
- Def-events travelling the log; defs folded from them along a read path
- Multi-repo defs, flat namespace, implicit extension, collision errors
- Def-versions, ClientVersion resolution, live-version set
- Tombstones as a general cell-valued concept; dangling references permitted
- Unified producer engine: pipelines and migrations, in-process Rust under full trust
- `ExecutionProvider` / `ProducerCtx` with automatic, ctx-mediated dependency capture
- Dependency index (bidirectional, key-ranged), re-run cycle detection, producer-scoped `IllegalState`
- Stateless scheduler — work derived from watermark gaps, no queue
- Watermarks, validate/recompute, provenance envelopes, `explain()`
- Frontier tracking, `freshness` read modes, settled-frontier reads
- Object transactions with source-only guards, validated at seal
- Cell-touch index over source layers
- Merge with guard-based conflict detection
- Distribution seams (§17.2) behind traits, naive in-process implementations
- Serializable command/response form for every engine operation

**Out:**

- Containerization / untrusted execution — full trust, in-process
- Sinks
- `Set`, `Map`
- Aggregation pipelines
- Mid-list insertion
- **All generated SDKs** (§15) — they arrive with the network layer
- Network / server layer — v1 is a library exercised by Rust tests
- Actual distribution — only the seams (§17.2)
- Coalescing of derived layers across source layers
- `down`-migration validation
- `expectedFreshAt` ETAs
- Referential integrity / dangling-reference prevention

**Acceptance scenarios:**

1. `Foo.value: string → int` — fork at a layer, def-mutate with a migration, verify both branches
   read correctly at their own ClientVersions, then def-only merge and verify main's existing values
   read through the new lens.
2. `should_invest_in_startup` — multi-hop pipeline, verify that a write to a depended-on field
   recomputes and a write to an undepended field does not, and that watermarks and lineage report
   correctly throughout.
