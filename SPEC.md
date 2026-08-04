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
strings are stored exactly once registry-wide. Interning storage therefore has no branch column, no
layer and no def-version — there is nothing here for two branches to disagree about.

The hash is **SHA-256 of the value's canonical byte encoding** — UTF-8 for `String`, the octets
themselves for `Binary`, and for `BigInt` **two's-complement big-endian, minimal length, with the
empty slice being zero**. *Canonical* is load-bearing: two encodings of one value would intern as two
values and defeat the deduplication that is interning's entire purpose.

Minimal length is what does the work in the `BigInt` case. Without it `1` could be spelled `01`,
`0001`, or any longer run of leading zeros, and every spelling would hash differently — one number
stored under arbitrarily many names. The empty slice for zero falls out of the same rule and settles
`0` and `-0` as one value rather than two.

The kind is **not** part of the preimage. It is a field of the PID in its own right, so `String("x")`
and `Binary("x")` are already distinct PIDs without paying for domain separation, and the hash stays
reproducible by anything that can run `sha256sum`. Like `allocatorId`, this is a persisted format and
is fixed now rather than retrofitted (§17.2): changing the function or the preimage renames every
interned value ever stored.

A PID encodes its own kind, so the dispatch to the correct buffer requires no lookup.

**Text form.** A PID is written `<kind>-<id>`: a single kind letter, a hyphen, and the rest of the
PID in [Crockford base32](https://www.crockford.com/base32.html) — lowercase on output,
case-insensitive on input, with `i`/`l` read as `1` and `o` as `0`.

| Letter | Kind | Letter | Kind |
|---|---|---|---|
| `o` | `Object` | `s` | `String` |
| `l` | `List` | `b` | `Binary` |
| `a` | `Any` | `n` | `BigInt` |
| `j` | `AnyObject` | `y` | `AnyArray` |
| `m` | `AnyNumber` | | |

`o`, `l`, `s`, `b` are initials; `n` is BigInt (*number*) and `a` is `Any`. The untyped family takes
the next distinctive letter of its own name — an*yObj*ect, arra*y*, nu*m*ber — because its initials
are already spoken for.

The id is **lossless** — this is the whole point of it. An allocated PID encodes `branchId`,
`allocatorId` and `counter` as three LEB128 varints; a content-addressed PID encodes all 32 bytes of
its hash. A truncated hash would make two distinct strings indistinguishable everywhere a human or a
shell script handles one, and lengthening the prefix moves the birthday bound rather than removing
it.

The two flavors are told apart on decode by payload length alone: three varints reach at most 25
bytes, a hash is exactly 32, so the encoding needs no discriminator.

> A text form that dropped components — as an earlier `Company#100` did, carrying only the counter —
> is not a naming convenience but a defect. It forces every consumer to *assume* the missing
> components, and two consumers assuming differently name different objects while appearing to
> agree.

### 3.2 Primitives

`Int`, `Boolean` and `Double` have no PID because the identifier would cost more than the payload.
They are stored inline in the cell that holds them.

### 3.3 Deferred value types

`Set<T>` and `Map<K,V>` are **deferred**. When introduced they will dedupe by a JVM-style
`hashCode`/`equals` contract on the element type, not by PID.

### 3.4 Values in text

A value has one text form, and it is the one the CLI accepts, the one a worker sends and receives
(§17.4), and the one errors quote back:

```
42                     Int
1.5                    Double
true / false           Bool
~                      a tombstone (§8.1)
@o-1234abcd            a reference — a bare PID (§3.1)
@Company:o-1234abcd    the same reference, named through a cell it identifies
0xdeadbeef             Binary — whole octets only
-129n                  BigInt — decimal digits with a trailing `n`
acme.ai                String — anything that matched none of the above
```

**A bare word is a string.** No quoting, because a shell worker is the target audience and a form
that needs quotes is one that will eventually be typed unquoted.

**Parsing is type-directed.** The table above is what a value means when nothing knows what it is
*for*. A write does know: it names a cell, the cell names a field, and the field declares a type
(§5.1). So the write path parses **against** the declared type, and the table's ambiguities do not
arise there:

- A field declared `String` takes `true`, `42`, `0x`, `7n` and `@jake` as exactly those characters.
- A field declared `Int` refuses `acme` rather than storing a string that looks almost right, and
  names `BigInt` when the digits are too large for an `Int`.
- A field declared `Object` or a list type refuses anything that is not a reference, so a mistyped
  `@…` is an error rather than data.
- `~` stays reserved on every field whatever its type: deletion has to be expressible (§8.1).

The untyped parse survives for the surfaces that genuinely have no declared type in hand — a
`describe` payload, an error message, a field declared `Any` — and it keeps the reservations that
come with guessing: `@` and `0x` are sigils there, so a malformed remainder is an *error* rather than
a string, because a mistyped reference quietly becoming data that looks almost right is the worst
available outcome.

**Interning is invisible.** A `String`, `Binary` or `BigInt` written in this form is interned by the
engine, and a cell holding a reference to a *content-addressed* PID renders back as its content — so
a pipeline reading `company.website` receives `acme.ai`, never `@s-1a2b3c` plus a round trip to
resolve it. A reference to an *allocated* PID still renders as `@o-…`, because there is nothing
behind it to render. Clients therefore never learn that interning exists, in the same way they never
learn that a provider batches writes (§17.1): a runtime concern, not a user concern.

> **What the ambiguity costs, and where.** In the untyped parse the forms above win, so their
> spellings are not strings: `true` is a `Bool`, `42` an `Int`, `0xff` `Binary`, `7n` a `BigInt`.
> That is the price of guessing, and it is confined to the surfaces that must guess. It does not
> reach stored data, because no write goes through them.

---

## 4. Storage Model

### 4.1 Everything is a cell

The universal addressable unit is the **cell**:

```
CellRef { buffer: BufferId, key: CellKey }  ->  Event

key = Pid                 // an object property, or an object's existence
    | (Pid, index)        // a list element
```

**The buffer is part of the address, not derived from it.** A sharded store must be able to route a
request from the cell address alone; if the shard key required a schema lookup first, every read
would need the defs before it could be sent anywhere (§17.2).

**Text form.** A cell address is written as its buffer, a colon, and the PID (§3.1):

```
Company:o-1234abcd            an object's existence cell
Company:o-1234abcd.website    an object property
Founder[]:l-5678wxyz          a list's own cell — its value is the list's length (§4.4)
Founder[]:l-5678wxyz[0]       a list element
```

This is the form the CLI accepts and the form cells travel in on the worker protocol (§17.4), parsed
and rendered in one place so the two cannot drift. **A colon, not parentheses**: parentheses read
marginally better but are shell metacharacters, and a worker is expected to be a shell script — a
form that needs quoting is one that will eventually be typed unquoted.

`Company#100` is additionally accepted **on input only**, meaning counter 100 on the root branch
with allocator 0. It is a convenience for hand-authored data and nothing renders it; the shorthand
in this document's later examples is that form, abbreviating a PID for readability.

The cell is the right granularity because every mechanism in Borg is already field-granular:
transaction guards, producer dependencies, field ownership, migration staleness, and merge conflict
resolution all key on the same primitive.

Cross-repo extension falls out for free. When Repo#2 adds `Company.website`, it writes cells with a
new field key and touches none of Repo#1's storage.

### 4.2 Buffers

A **buffer** is a partition of cells, and the partition key is a def:

> **One buffer per def.** Values that have no def — the untyped ones — get exactly one buffer each.

| Buffer | One per | Holds |
|---|---|---|
| `ObjectBuffer` | `ObjectDef` | existence cells for that struct |
| `ObjectPropBuffer` | `FieldDef` | property cells for that one field |
| `ListBuffer` | `ListDef` | list existence cells |
| `ListElemBuffer` | `ListDef` | element cells |
| `AnyObjectBuffer` | — | untyped object cells |
| `AnyArrayBuffer` | — | untyped array cells |

The `Any*` buffers are singular because untyped values have no def to partition by. They are genuine
cell partitions all the same: an `Any` container is mutable, so its contents are cells with versions
and origins like any other.

**Interning is not a buffer.** The interning stores hold **values, not cells**. An interned value has
no def-version, no origin and no authoring layer — an `Event`'s fields are all meaningless for it —
so it is reached through `intern` / `read_interned` (§17.1) rather than through a cell read, and a
`BufferId` is never named: the PID already carries its kind (§3.1). `BufferId` therefore has **no**
`String`, `Binary` or `BigInt` variant. Naming one would promise a cell partition that cannot exist,
and would be the first place a branch or a layer crept back into a scheme whose whole value is having
neither.

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

### 4.3 The event

A cell's contents are not a record that names a layer. They are an **event with an identity of its
own**, which layers name (§6.2):

```
Event {
  id:         EventId
  cell:       CellRef
  value:      Primitive | Pid
  version:    LayerId        // the def-version of this cell's field (§5.3)
  origin:     Source | Derived
  authored:   LayerId        // where this event was FIRST committed
  // derived only:
  producer:   ProducerId
  freshAsOf:  LayerId
  readSet:    CellAt[]        // cell *and* def-version (§9.4)
}
```

**An event does not carry the layer it lives in.** It carries only where it was first committed.
That inversion is what lets one event belong to several layers, which is what lets a merge *name* a
child's events instead of rewriting them (§13). Groups contain their members; members do not carry
their group.

**What a read returns is a pair**, and both halves are provenance:

| | meaning |
|---|---|
| `event.authored` | where this event was first committed — on whichever branch wrote it |
| the layer it was reached through | where it landed on *this* branch |

They coincide until a merge shares the event, and then they do not. A single `writtenAt` could only
report the second, because merge rewrote the first away: "authored on `feature` at L20, landed on
main at L30" collapsed into L30. Both now survive and the read envelope reports both (§10.4).

**Everything else compares against the landing layer, never `authored`.** Time travel, guard checks
and watermark validation all ask "was this visible here, by then", and an event authored on another
branch at a low layer id can land on this one arbitrarily late. Ordering by where an event was
written would rank a freshly merged value beneath everything the branch has done since.

**Every cell carries its own def-version tag** — the version of *its own field* as the writer's
def-view named it (§5.3, §5.4), not the writer's whole-schema ClientVersion. A def-mutation touching
one field therefore does not stale, move, or even mention the other fields of the object. Cells with
no definition to version — existence cells, lists, untyped containers — are tagged unversioned.

Source events carry only `value`, `version`, `origin`, `authored`. The heavy metadata — watermark,
read-set, producer — attaches to derived events only, which in a normalized model are the minority.

**A materialised read index keeps reads a single lookup.** "The latest write to cell `C` visible on
branch `B` at layer ≤ `N`" is now a question about membership, so a provider maintains
`(branch, cell, version) -> (layer, event)` as events are put into a layer. Like every other index
in the system it is a projection of the log and must be rebuildable from it (§17.1); unlike the
dependency and touch indexes it is durable, because rebuilding it on open would turn an `O(log)`
read into an `O(log)` write.

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
FieldDef   { name, type, declaringRepo, ownership, version: LayerId }
Ownership  = Source | Derived(ProducerId)
ListDef    { elementType }
```

**`ownership` is one enum, not an origin beside an optional writer.** Every field has exactly one
writer (§8), so a pair of loose fields would make "derived but unowned" spellable. The `Origin` a
*record* carries (§4.3) is derived from it — with one deliberate difference: an `up` migration is a
producer writing a field declared `Source`, and the record it leaves is still derived (§9.3).

**Definitions are load-bearing, not descriptive.** Every cell write is validated against the def-view
of its branch before it lands: unknown struct rejected, unknown field rejected, wrong type rejected,
and a writer the declaration does not name rejected. §8 states the rule; this is where the shape it
reads from lives.

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
- The *same* repo redeclaring the *same* shape is a repeat, not a conflict, and is a no-op. A repo
  emits its whole schema on every push (§17.4), so a push that only worked once would be a push
  nobody trusts. Changing a declared field still requires `MutateField` and a migration (§6.1).
- There is no repo dependency DAG. Declaration order does not matter.

### 5.3 Def-versions

**A def-version is a LayerId** — specifically, the def-layer that most recently mutated that
definition. No separate versioning scheme exists. The def-version DAG is the branch/layer DAG
restricted to def-layers.

A field's chain of def-versions, and the migrations bridging each step, are therefore **folded per
branch** from the `MutateField` events along a read path, exactly as the definitions themselves are.
Nothing about which two versions a migration bridges is recorded in the migration's own definition:
a def-only merge replays that `MutateField` onto the parent as a *different* layer, and the same
producer must then bridge the parent's versions rather than the ones it was pushed against (§13).

**Per definition is what makes this a version at all.** A def-version is the record key of every
stored cell (§4.3), the version half of every `CellAt` (§9.4), and the node a migration chain walks
— and all three only work because it moves when *that* definition moves and at no other time. A
whole-schema version in the same place would renumber every field on every push, breaking the chain
that was never mutated. §5.4 is where the difference bites.

### 5.4 ClientVersion

**Every actor that executes code carries a ClientVersion**: a LayerId identifying the def-view its
code was authored against. All reads by that actor resolve at that def-view.

| Actor | ClientVersion |
|---|---|
| External SDK | the layer its generated code was built from |
| CLI | the branch's current def-version — it has no generated code, so it is authored anew each invocation |
| Pipeline | the layer its repo's code was pushed at |
| Migration `up_v1→v2` | v2 — the layer of the def-mutation that introduced it |
| Migration `down_v2→v1` | v1 |

This unifies clients, pipelines and migrations into one concept.

A migration's ClientVersion is **the version it produces**, in both directions: it is the lens for
that version, and sees the rest of the world as a client on that version does. The one cell it does
*not* read there is the one it is translating, which it reads at the other end of its step (§9.3).

**Writes are stored at the def-version their author's view puts that field at, and are never coerced
or rewritten.** A v1 client and a v5 client may read and write concurrently; the read path composes
migrations in whichever direction is required. This is why `down` migrations matter — they are what
keep old clients working.

**Not at the actor's ClientVersion — at the field's def-version (§5.3).** The two are both def-layer
ids and they are different quantities: a ClientVersion is a whole schema and moves on every push, a
def-version belongs to one definition and moves only when that definition does. They coincide only
if every push touches every field.

Storing a cell at the writer's whole-schema version instead is wrong twice over, and both follow
from the same fact — that a field nobody mutated has one version and would have acquired many:

- **The value becomes unreadable.** A reader whose ClientVersion has moved past an unrelated push
  asks for a version nothing was stored at, and §5.3 offers no route to it, because a field that
  never changed shape has no chain and owes no migration. The honest report for that is `broken`
  (§10.4) — a correct answer to a question that should never have been asked.
- **Dependencies stop matching, silently.** A read-set entry is a `CellAt` — cell *plus* version
  (§9.4) — so a source write landing at a version no producer recorded invalidates nothing.
  Nothing fails: the derived value simply stops following its input and goes on labelling itself
  current. That is the more serious half, because the first has a symptom and this one has none.

A cell with no definition to version — an object's existence cell, a list, an untyped container — is
stored **unversioned**. Nothing about their shape can change, so they sit on no chain, and a fixed
version is what keeps them findable across every def push.

The def-version a write is keyed at is asked of the **writer's own** def-view, which is what makes
the rule uniform across every actor: a client on an old view keys where its view says, a pipeline
where the schema it was built against says, and a migration — whose view is folded to the version it
produces — exactly where the chain says its output belongs.

**And validated at it.** The shape a write must fit — is the struct declared, is the field declared,
does the value fit its type — is asked of the writer's *own* def-view, not of the branch's (§8.0).
Anything else would reject exactly the writes backwards compatibility consists of: a v1 client
storing a v1-shaped value after the schema moved to v5, and a `down` migration whose entire output is
old-shaped by construction.

### 5.5 The live-version set

Because migrations are eager producers (§9.1) and writes are never coerced, a cell can end up
materialized at *every* def-version anyone might read it at. Five live ClientVersions means up to
five copies of every affected cell and five migration chains fired per source write.

The registry therefore tracks a **live-version set** — the ClientVersions that actually have
registered clients. The derivation engine materializes only for versions in that set; anything else
is computed on demand via `freshness: 'current'` (§10.5). When the last client on a version
disconnects, that version's derived layers become droppable.

**An empty live-version set means "materialize everything."** Nothing registers a client in v1 —
there are no generated SDKs (§18) and the CLI is a fresh actor per invocation — so the set is a
filter that is switched off rather than one that excludes everybody. It is exercised, and its
behaviour when non-empty is what the deferred reduction policies build on.

**v1 eats the storage cost** in exchange for accuracy; reduction policies are deferred.

The live-version set does double duty: it is also what powers the push-time warning *"this def change
has no `down` migration and will break the N clients currently on versions X and Y."*

---

## 6. Events and Layers

### 6.1 Events

**ValueEvents** mutate data:
`CreateObject`, `SetObjectProp`, `UnsetObjectProp`, `DeleteObject`, `ListAppend`, `CreateList`,
`DeleteList`.

A ValueEvent is stored as an `Event` (§4.3): one cell, one value, and an identity of its own. Those
identities are what layers hold.

**DefEvents** mutate definitions:
`CreateObjectDef`, `MutateObjectDef`, `DeleteObjectDef`, `DeclareField`, `MutateField`,
`DeleteField`, `PushProducer`.

`DeclareField` carries the field's `ownership` (§5.1, §8) along with its type and declaring repo.
That is what makes a derived field declarable at all: without it every field would be implicitly
`Source`, and no producer could legally write anything.

Every `DefEvent` that alters the shape of existing data **must** supply a migration (§9.3).

### 6.2 Layers

**A layer is an ordered group of events, and it holds its members by reference.** Membership is a
`(layer, event)` relation: many layers may name one event, and an event names none of them (§4.3).
That is the model this prose always described; it is now also the model the storage implements.

A layer contains ValueEvents **xor** DefEvents, never both. This is what makes "the def-version as
of layer L" well-defined.

**A layer belongs to exactly one branch**, unchanged and load-bearing. Only *events* are shared.
LayerIds are registry-unique.

Membership is not part of a layer's metadata, which is written whole. A layer may hold millions of
events, so its members are enumerated and streamed like its contents always were — the same
constraint that governs commit governs reading a layer back.

Two writes to one cell in one layer are **two events**, both members. It is the read index, not the
group, that decides which of them resolves: the later one.

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

`open` is exclusive to its owner, and that ownership does not survive the owner. A layer found in
`open` or `sealed` when a store is reopened belongs to a process that no longer exists; it never
became visible, so it is **aborted on recovery** rather than left in a state the scheduler would wait
on forever (§16.4).

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

Dropping them is an operation and not only a thought experiment: recompute rewinds a branch's
watermarks so every producer owes its whole source buffer again, and the layers it writes shadow the
ones it had. On a fork that is a replay of the world at the fork point, which is how §10.1's claim is
checked rather than trusted.

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

Ids, however, are assigned at open, so a layer opened first may commit second — the ordinary case
once a round runs its invocations concurrently (§16.5). **A branch's head is therefore the highest
committed layer, not the most recently committed one.** The head bounds every read path and every
producer's work gap, so a head that walked backwards would hide the layer that overtook it from every
subsequent read.

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

**Every field has exactly one writer, and the declaration says which.** `FieldDef.ownership` (§5.1)
is either `Source` — written by clients — or `Derived(ProducerId)`, naming the one producer that
computes it. It is one enum rather than an origin beside an optional writer, because "derived but
unowned" and "source but owned by P1" are not states the system has an answer for.

**Ownership is declared, not discovered.** An earlier version of this section had ownership
discovered at runtime — whoever wrote a cell first owned it — on the grounds that recording an owner
into the def would mean the derivation engine emitting def-mutations, violating the value-xor-def
rule (§6.2). That reasoning still holds and is why the engine still never writes a def. What changed
is that the *author* declares ownership up front, which the engine merely reads:

- A violation is caught on the **first** wrong write, rather than on a second producer's collision
  with a first that already succeeded.
- Which producer ends up owning a contested field stops depending on scheduling order.
- It is forced anyway. Once a write must name a declared field, a producer's output field must be
  declared too, and the only thing that knows a producer's output field exists is the repo
  implementing it (§9.2, §17.4).

### 8.0 Every write is validated

A cell write — from a client, from a producer, from anywhere — is checked against the definitions
(§5.1, §7.2) before it lands. Four questions, in the order a human would ask them:

1. **Is the struct declared?** A struct exists because someone declared a field on it (§5.2), so an
   unknown name is not an empty struct — it is a typo.
2. **Is the field declared?**
3. **Does ownership permit this writer?** A client may not write a `Derived` field; a producer may
   write only fields it owns. The one exception is a **migration**, which writes another field's
   cells at a newer def-version because that is its entire job (§9.3): the declaration it is checked
   against is the one naming it as `up` or `down`.
4. **Does the value fit the declared `ValueType`?** A tombstone satisfies every type — it means
   *explicitly removed* (§8.1), and deletion has to be expressible on every field.

**Two of those questions are asked of two different def-views**, and the split is the whole of
backwards compatibility:

| Question | Asked of | Why |
|---|---|---|
| shape — 1, 2 and 4 | the writer's **ClientVersion** (§5.4) | writes are stored at their author's version and never coerced, so an old client goes on writing the old shape and a `down` migration writes nothing else |
| permission — 3 | the **branch** | who may write a field is a fact about the schema as it stands, and a `down` migration's own view is by definition older than the `MutateField` that named it, so it cannot see its own appointment |

Where the writer is current these are the same view, which is the common case. Where the branch has
dropped a field the writer still knows about, permission falls back to the writer's own declaration:
an old client is entitled to the schema it was written against.

Because the branch is what grants permission, **a definition that has not merged is not merely
invisible to the parent — it is unusable there.** Those are the same fact.

Two limits, stated rather than hidden:

- **A reference is checked for kind, not for struct.** A `Ref` carries a PID, and a PID records a
  kind, not which struct the object belongs to (§3.1). So a field declared `Object(Company)` is
  checked for "is this an object at all" and no further.
- **Lists and the untyped containers are unchecked.** There is no `ListDef` event in §6.1 and no way
  to declare one, so requiring a declaration would make them unwritable rather than validated. They
  become checkable in the change that gives them something to check against.

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

**A pipeline's output field must be declared, and its repo is what declares it.** A producer write is
validated like any other (§8), so a pipeline whose output field nobody declared cannot write at all.
The repo therefore emits its struct definitions alongside its producers, and both land in one def
layer (§17.4).

A producer's ClientVersion **is** the def-layer it was pushed at, so it is folded rather than
authored: the layer id does not exist when the event is built, and a merge replays that event onto
another branch as a different layer. What the event carries is a placeholder.

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

**Its own input is the one exception, and it has a verb of its own.** `get_input` reads the cell
being translated at the version the migration reads *from* — the older version for `up`, the newer
for `down`. It is not `get` because `get` resolves at the migration's ClientVersion, which is the
version it *writes*, so `up` would recurse straight into the value it is producing. It is not `get`
with an explicit layer id either: a migration should not have to do arithmetic over def-versions to
reach the one cell it exists to translate, and a worker that had to would not be writable in bash
(§17.4).

**A migration's definition records a direction and nothing more.** Which two versions it bridges is
read out of the field's version chain on the branch it is running on (§5.3), because that is where
the answer differs — a def-only merge replays the `MutateField` that appointed it onto the parent as
a different layer, and a migration carrying a hard-coded pair would go on writing a version no reader
on that branch will ever ask for.

**A migration maps over the field's buffer**, not the struct's — it is defined per output field, and
per-field buffers (§4.2) make that exactly expressible. More precisely it maps over **one version**
of that buffer: a write at the version it reads from is work for it in precisely the way a data write
is work for a pipeline, and a write at any other version is somebody else's business.

**A migration is never triggered by either half of its own step.** A migration writes into the very
buffer it consumes from, one version along, and would otherwise re-trigger itself forever — and `up`
and `down` for one step do it to each other, each writing exactly the version the other reads.
Leaving that unfiltered would fire the cycle detector (§16.6) on the ordinary case rather than on a
cycle. They are two projections of one value, not two producers disturbing each other's inputs.

This is the one filter by author, and it covers **both** trigger paths, the read-set one included.
The general rule that the read-set trigger is *not* filtered by author still holds and is what
catches genuine cycles; a migration re-expressing its own input in the other direction is not one.

**A migration owes the values that predate it**, which no layer stream mentions. A def-mutation
normally lands long after the data, and on a fork the data was written on the parent — so nothing in
any layer belonging to the migration's branch names it. A producer that has never run therefore takes
its whole source buffer as work, enumerated directly (§9.6), filtered to entities holding a value at
its input version that the other half of its step did not write. Without that filter the seeding
order of `up` and `down` would decide whether `down` overwrote the source value `up` had just read
from.

**Optimization:** if a migration's recorded read-set contains only its own source cell, the system
*infers* purity and caches the result permanently with no invalidation. Inferred, never declared.

**v1 trusts `down`.** If the user supplies one, it is assumed correct. Probabilistic detection of
lossy or partial `down` migrations is deferred.

**Supplying no `down` is a decision with a stated consequence.** Values written after the change have
no path back to the older versions, so a reader there is not behind — it is stuck. The read envelope
reports `broken` (§10.4), which is the same answer a cycled producer gets and for the same reason:
there is no honest value to serve, and serving the pre-change value instead would be silently wrong.
Values written *before* the change are untouched at their own version and go on reading normally.

### 9.4 Dependency tracking

Dependencies are captured at **field granularity, through hops**, including negative reads (checking
a field that is absent is a dependency on its absence).

**Read-sets record the version each cell was read at**, not the cell alone. `CellRef` is the shard
key — where a cell lives, computable without a schema lookup; `CellAt` is the record key — which of
that cell's versions you mean. The dependency index and field ownership both key on `CellAt`. Keying
them on `CellRef` would make a migration, which reads `C@v1` and writes `C@v9`, observe its own
output as a change to its own input and poison itself as a cycle. It also means a pipeline reading
`C@v1` is correctly left alone when a migration materializes `C@v9`.

**Ownership spans versions, and the migration exception is what makes that work.** The same cell at
v1 and at v9 is legitimately written by different actors — a client and the migration carrying its
value forward — so a migration is checked against the declaration naming it as `up`/`down` rather
than against the field's ordinary writer (§8).

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
derivation engine chases it and advances watermarks. Nothing has to ask.

Because every derived value states what it reflects, the scheduling policy **cannot affect
correctness, only latency.** `NaiveEagerProducerPolicy` in v1; smarter prioritized policies later,
provably without regression.

That licence is what makes the *shape* of the chase an implementation matter. v1 runs it in the
process that commits the layer: catch-up follows commit, before the caller returns. It is therefore
synchronous with the writer and asynchronous with every reader, which is the half that carries the
meaning — a reader never waits for derivation, and never receives a value pretending not to have
waited. A server hosting a worker pool moves the same call behind a signal, and nothing above it
changes.

#### Pausing

Auto-derivation is a **branch-scoped switch**, default on. Pausing a branch stops the chase; it does
not stop derivation. Explicit catch-up still runs on a paused branch — freeze the automation, then
step it by hand — which is what makes the switch usable in an emergency rather than a way to lose
data.

It is **operational config, not log data**, and lives beside the store with the producer
implementations (§9.2). Pausing changes when the system catches up, not what is true, so it is not
an event: in the log it would be forkable, mergeable and time-travellable, and *"was derivation
paused at layer 400?"* is not a question with a use.

**A pause needs no vocabulary of its own.** A paused branch's frontier stops advancing, and every
read of derived data already reports how far behind it is (§10.4). A pause *is* lag, and the
freshness envelope already describes lag — so nothing is added to it.

There is deliberately no per-*producer* pause. A broken producer is already scoped by `IllegalState`
(§14), and *"expensive but not broken"* is a scheduling-policy question, better answered by a policy
than by a second switch.

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

Enumeration is what a **producer newer than its data** needs, and it is the one thing the layer
changeset cannot supply: the entities it owes were written before it existed, or on a branch it was
forked from, and no layer belonging to its own branch names them. A producer that has never run on a
branch therefore takes its whole source buffer, read through that branch's ancestry, and streams
normally thereafter.

A commit on branch B triggers producers on branch B only, since a layer belongs to exactly one
branch.

---

## 10. Watermarks and Freshness

### 10.1 Definition

> **Watermark:** the source layer up to which all of a derived value's inputs have been incorporated.

The claim it makes is: *if you replayed the world at layer W, you would get exactly this value.* The
value may also still be correct at later layers; that is a separate question, answered by validation.

**The claim is checkable, and is checked.** Fork at W, recompute the fork from source instead of
letting it inherit what its parent derived, and compare. The second step is what §6.3's droppable
derived layers make possible and `borg derive --rebuild` is what performs it — a fork's ancestors stay
bounded at the fork point, so the producers re-run against exactly the world W names.
`scenarios/100-watermark-truth` does this for every derived cell it can reach: a value its own
watermark does not reproduce is a label the system had no right to write.

### 10.2 Validate vs. recompute

Two distinct operations, and separating them is where the efficiency lives:

| Operation | What it does | Runs user code | Advances |
|---|---|---|---|
| **Validate** | checks the dependency index for changes in `(freshAsOf, target]` | no | `freshAsOf` |
| **Recompute** | re-runs the producer, yielding a new value and read-set | yes | a new event, so `landedAt` **and** `freshAsOf` |

Most derived cells most of the time need only validation. A cell depending on `website` and three
schools is unaffected by the other 40,000 writes that landed meanwhile, and its watermark advances
to HEAD for the cost of a few index lookups.

**Reads validate before reporting**, so the returned watermark is tight rather than pessimistically
understated.

**"Has this dependency changed?" is asked on the source stream**, because that is where a watermark
points (§10.1) — and the two kinds of dependency answer it differently:

| Dependency | Reflects |
|---|---|
| **source** | the layer it *landed* on this branch — landed, never authored, since a merge can carry an old event onto this branch late (§13) |
| **derived** | its producer's watermark — never the derived layer it sits in |

The second is the one that is easy to get wrong. A derived layer sits *above* the source layer it
reflects, by construction (§6.3), so comparing where a derived dependency landed against a watermark
compares a derived id with a source one. It is always larger, every chained value therefore looks
permanently overtaken, and `current` becomes unreachable for any producer reading another producer's
output.

### 10.3 Composition

Watermarks propagate through chained producers:

```
W(B) = min(target, W(A), W(other deps...))
```

This is what validation returns: not a yes/no but the composed layer, `target` where nothing bounds
it. A source dependency bounds nothing — source data is ground truth and is correct at every layer
after it lands. A derived dependency bounds `B` by whatever *it* validates to, recursively, so a
chain is exactly as fresh as the hop behind it and no fresher.

Any derived cell can therefore report an honest *transitive* freshness — the minimum over its entire
derivation chain, migrations included. A v1 client reading a v3-produced field through a
down-migration receives a watermark accounting for both hops.

### 10.4 The read envelope

Every cell read returns provenance, not a bare value:

```ts
// Named `Resolved<T>` in the Rust implementation: `Cell` collides with CellRef
// and std::cell.
type Cell<T> = {
  value:      T
  origin:     'source' | 'derived'
  event?:     EventId    // which write this is — absent when nothing is stored here
  authoredAt: LayerId    // where this value was first committed
  landedAt:   LayerId    // where it arrived on the branch being read
  freshAsOf:  LayerId    // certain-correct through here
  state:      'current' | 'unvalidated' | 'stale' | 'broken' | 'tombstoned'
  by?:        ProducerId
  // deferred: expectedFreshAt — an ETA for when catch-up will complete
}
```

`authoredAt` and `landedAt` are equal until the value arrives by merge, and then they are the two
halves of its lineage (§4.3, §13). `event` is reported because "is this the same write I saw on the
other branch?" is now a question with an answer: a merged event is one event named by two layers.

| State | Meaning |
|---|---|
| `current` | `freshAsOf == requested layer`. Guaranteed correct. Source cells are always this. |
| `unvalidated` | Behind, but unchecked. Cheap to resolve. |
| `stale` | A dependency is known to have moved. Definitely out of date. |
| `broken` | The producer threw or cycled. `IllegalState`, scoped to this cell. |
| `tombstoned` | Explicitly removed (§8.1), or reached through a dangling reference (§8.2). |

For source cells `landedAt` and `freshAsOf` collapse — source data is written once and correct
thereafter. The distinction only carries information for derived data.

**A cell not materialized at the reader's def-version is one of three different facts**, and the
state is what tells them apart. Never written at any version: `current`, with no value — the cell is
simply absent. Written at some version the reader's can be reached from (§5.3): `stale`, because a
migration owes it and has not run. Written only at versions with no path here: `broken` — which is
what a def-push with no `down` does to the clients it left behind (§9.3). Migrations are eager
producers (§9.1), so "not yet materialized" is lag like any other lag, and the inline alternative is
`freshness: 'current'` (§10.5).

Worked example, reading `company#100` at HEAD = L500:

```
company#100:
  name             "Acme"   source    authored L120  landed L120  freshAsOf L500  current
  is_hot_startup   true     derived   authored L400  landed L400  freshAsOf L450  stale
                                      by pipeline:invest_score
```

### 10.5 Client controls

**Freshness requirement on read:**

```ts
read(pid, field, { freshness: 'any' | 'validated' | 'current' })
```

| Requirement | What it does | Runs user code |
|---|---|---|
| `any` | serves the stored value and states whether it is at the requested layer | no |
| `validated` | walks the read-set first, so the reported watermark is tight (§10.2) | no |
| `current` | computes inline and blocks until the answer is correct | yes |

`current` forces inline computation and blocks. **Lazy materialization is therefore a per-read client
mode, not a system architecture** — a client that needs a fresh answer pays for it at the call site;
everyone else takes the lag.

Four things follow from `current` being a *read*:

- **It validates before it computes, so it converges.** A value that already reaches the layer being
  read is the answer; nothing runs. Without that check every such read recomputes forever, because
  an inline computation deliberately advances no watermark (below) and so leaves nothing behind that
  the next read would recognise — the work is done, and no cheaper operation can tell. Validation is
  what can tell: it runs no user code (§10.2), and it is the same walk the read performs anyway.
- **It computes what it needs, not what it is asked for.** A cell's inputs may themselves be behind,
  and computing from a stale input while labelling the result current is the one thing §10 does not
  permit. So the read-set is followed first, recursively, and the producers behind it run in
  dependency order. A chain that leads back to a cell already being computed stops there, which is
  what bounds the recursion — the round-scoped re-run counter of §16.6 cannot help, because an
  inline computation is one client's request and not the consequence of a source layer.
- **It does not advance a watermark.** A watermark is a claim about *all* of a producer's output
  (§10.1); one entity computed on demand says nothing about the other hundred thousand. The work
  therefore stays outstanding and the next round does it again in the ordinary way — which is what
  keeps the consequences a round would have propagated, downstream producers included, from being
  lost.
- **A missing version is one of the things it computes.** A value not yet materialized at the
  reader's def-version is a migration that has not run (§10.4), and the hops the reachability check
  walks to prove the version is reachable are exactly the ones `current` runs.

A read pinned to a layer *below* head is a historical read, and `current` means what `validated`
means there: nothing can make the past current, and a value computed now would land in a layer above
the one being read through.

**Await the frontier:** `await branch.frontier.reaches(L500)` — read-after-write consistency for the
clients that need it, and deterministic tests without a synchronous system. It returns when every
producer on the branch has incorporated `L500`, and takes no deadline of its own: how long to wait is
the caller's policy, and a primitive that chooses one has to be worked around.

**Two consistency modes, both honest:**

- **Ragged head** — read at HEAD. Latest of everything; freshness varies per field.
- **Settled frontier** — read at the highest layer through which *all* derived data is caught up.
  Fully coherent snapshot, slightly in the past.

A dashboard wants ragged; a report wants settled.

A watermark points into the **source** stream, and the derived layers carrying a source layer's
consequences have higher ids than it — so the layer a settled read resolves at is not the watermark
itself but the highest layer below which nothing is unsettled: layers at or under the watermark, plus
derived layers reflecting one of them. That is the *settled ceiling* — a property of the branch,
computed on the read path, and not to be confused with anything a round maintains: a round expresses
what it can see with a branch boundary and holds no bound of its own (§16.5).

---

## 11. Lineage

Lineage requires no new storage. It is the dependency index read backwards.

```
explain(company#100, is_hot_startup) →
  produced by  pipeline:invest_score @ ClientVersion L380
  authored     L400
  freshAsOf    L450
  from
    (company#100, website)     source    @ L120
    (company#100, founders)    source    @ L390
    (school#77,  is_top_ten)   derived   @ L400   ← recursively expandable
```

An input's layer is where it **landed** on the branch being explained, which for a merged input is
the merge rather than the write. Where a value's own `authored` and landing layers differ, both are
reported — that is lineage merge used to destroy (§4.3, §13).

---

## 12. Transactions

**A transaction is the only client write path.** No client writes to a shared branch. A write is:

```
fork → write → merge
```

The fork's read path is bounded at the fork point (§7.2), so a transaction reads **a consistent
snapshot**; guards re-evaluated against the parent since that fork point are already the
merge-conflict detector (§13). Together that is snapshot isolation with optimistic concurrency
control, assembled entirely out of mechanisms that existed for other reasons.

**Derivation is a transaction too.** A round forks, runs its producers, and merges, guarded by what
they read — so producers are writers like any other and there is exactly one write path in the
system. Everything in this section applies to a round; §16.5 is where the two differences live.

Three consequences:

- **Every trunk layer is one complete intent.** Never a partial write, never two intents interleaved.
- **The safe path is the only path.** Guards used to be opt-in, which meant §13's last-write-wins was
  what people actually got. It is now what a client gets only where it expressed no dependency at all.
- **Transaction branches are created with derivation paused.** Deriving on a branch that exists to be
  merged is waste: a client merge does not carry derived layers, and the parent recomputes. A round
  branch is paused for a sharper reason — a round deriving on its own branch would be a round inside
  a round (§16.5).

### 12.1 Guards are automatic

A transaction records what it **read**; at commit, those reads *are* its guards. This is the same
read-set shape producers already record (§9.4) — `CellAt`, cell and the def-version it was read at —
so one mechanism serves both.

**Captured:** every read made *through* the transaction, including reads that found nothing, and
including the implicit ones. **Not captured:** anything read outside it. A client that runs `borg
get X`, thinks, then opens a transaction and writes based on what it saw gets no protection for that
read. This is the ordinary limitation of every optimistic system and is worth saying rather than
implying: **a transaction can only guard what it observed through the transaction.**

Four rules make it correct, each of which exists because something breaks without it.

**Guard the cells you read and had not yet written.** A transaction that writes `X` and *then* reads
`X` saw its own write, not the parent's state, so that read expresses no dependency on the parent.
The rule is about **order**, not about set difference: a read that came *before* the transaction's
own write did observe the parent and is guarded, which is exactly what makes compare-and-swap fall
out of reading a cell before writing it. Collapse the two into "reads minus writes" and every
read-modify-write silently stops being protected.

**Evaluate every guard against the parent as it stood before the merge, then apply.** A transaction
that wrote two layers would otherwise trip its own guard: the first layer to land touches a cell the
second's guard names, and the transaction conflicts with itself for no reason but the order the
merge walks its layers in.

**Implicit reads count.** The existence probe a write performs (§8, implied existence) is a read and
belongs in the read-set. Otherwise two transactions can each conclude an object does not exist and
each create it, with the second silently overwriting a decision the first made.

**Reads that found nothing count.** Absence is a legitimate thing to have acted on, and a later write
to that cell must invalidate the decision — the same rule producers already follow (§9.4).

**A write with no reads has no guards.** Such a transaction is last-write-wins on the cells it
touches. That is honest — the client expressed no dependency on prior state — and it is what every
database does with a blind write.

**An automatic guard on a derived cell contributes nothing, silently.** Guarding derived data is
meaningless (below), but a client that *read* it asserted nothing, so the read is dropped rather than
the commit refused. A hand-written guard naming a derived cell is still an error: that one is a
client saying something it cannot mean.

### 12.2 Where a transaction's state lives

A transaction spans several client calls, so it needs somewhere to keep which branch it forked, where
it forked, and what it has read. That is **operational state, not log data** — it dies when the
transaction ends — so it sits beside the store with the pause flags and the producer-implementation
table (§9.2), never in the log, where it would be forkable, mergeable and time-travellable for no
one's benefit.

A transaction carries its parent branch explicitly rather than inferring it from the branch it forked.
The two differ in the ordinary case of a transaction opened on a branch with no layers of its own: the
fork is taken at the highest layer that branch can *see*, which belongs to an ancestor, while the
merge must still land on the branch the client named.

### 12.3 Transactions are ephemeral; branches are durable

A transaction opened and never finished would leak a branch and its read-set forever. The answer is a
configured **idle timeout**: a transaction untouched for longer than that is reaped.

Idle rather than elapsed, so a long but active transaction survives and an abandoned short one does
not — the predictor of a doomed transaction is silence, not age. Reaping sweeps **opportunistically
when a process opens the store**, the same place the indexes are already rebuilt (§16), so there is
no daemon and an idle store sweeps nothing because nothing is growing.

The error when a client touches a reaped transaction must say *expired after N idle*, never *unknown
transaction*. The first tells you what to do.

**Rounds are reaped by the same mechanism, and cost nothing to lose.** A round is a transaction too
(§16.5) and a wedged producer leaks identically. A reaped round's output is discarded, but the cells
it was computing are still dirty in the dependency index, so the next round rediscovers them — the
same property that makes partial application safe. It is also why *idle* beats *elapsed*: a
legitimate 128k-invocation round runs long but is never idle.

This draws a line worth naming: **transactions are ephemeral and reaped; branches are durable and
explicit.** A client that wants to walk away and come back wanted a branch (§7).

### 12.4 The guard mechanism

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
instead. A derived cell is never in the touch index, so such a guard could not trip in any case;
rejecting it exists to catch a client asserting something it cannot mean, which is why an
*automatic* guard on one is dropped instead (§12.1).

---

## 13. Merge

**There are two kinds of merge, and which layers they carry is the difference.**

Merging a **client** branch replays its **source** events onto the parent as **new** layers. Derived
layers are tagged and skipped — the child's derived values are wrong on the parent by construction,
because the underlying data differs. The parent's derivation engine re-derives in the background.

Merging a **round** branch (§16.5) replays its **derived** layers, and there is nothing else on it.
Carrying them is the entire purpose of the branch: a round computed them from data the parent has,
which is exactly what a client branch's derived output is not. The two are separate operations rather
than one operation reading what a branch happens to contain, because "skip derived layers" and "carry
only derived layers" are opposite rules and the difference must be stated by the caller.

Everything below describes the client merge. A round merge differs in three further ways, all of them
consequences of a round being `N` independent computations rather than one intent: it applies
**partially**, it evaluates a guard set per invocation rather than one for the whole child, and it
regroups the layers it carries by producer. §16.5 is where those live.

**Merge does not copy events.** Each new parent layer *names* the events of the child layer it
replays (§4.3, §6.2). Nothing is rewritten, so:

- an event keeps its identity — one event named by two layers, not a copy on each branch;
- it keeps `authored`, so lineage survives the merge instead of being overwritten by it;
- it keeps the ClientVersion it was authored at, so the parent's readers migrate and nothing is
  coerced.

**What this costs.** Membership is `(layer, event)` rather than a full record carrying value,
version, origin, derivation and read-set, and the read index gains one entry per event. That is a
large constant factor and deliberately **not** an asymptotic one: there are still `n` rows per
merged layer, and the index must be updated with the merge or reads miss the data. Genuine `O(1)`
would need a parent layer to *reference* a child layer's event set rather than enumerate it, which
grows the read path per merge and needs compaction to pay for itself. Deferred.

The events a merge names are also why an aborted merge is harmless: aborting a layer discards what
it *authored*, and it authored nothing.

The user chooses **def-only** or **def+data**.

**Def-only.** Replays DefEvents. If the parent moved the *same* def since the fork point, the merge
is rejected — re-fork from head and redo. Different defs touched, no conflict.

A `MutateField` and the migrations it appoints replay together, because they were pushed together
into one layer (§17.4), and land on the parent as a new layer with a new id. The step they bridge is
therefore *the parent's* — from whatever version the parent's copy of the field was at, to the merged
layer — which is why a migration definition records no version pair of its own (§5.3). The parent's
existing values then migrate exactly as the child's did, as ordinary derivation work.

**Def+data.** Also replays ValueEvents. Each retains its authored ClientVersion, so the parent's
readers migrate as needed; nothing is coerced.

**Validation precedes any write.** v1 rejects a whole client merge rather than applying it partially,
so every layer is checked before the first one is replayed and a rejected merge leaves no trace on the
parent. A transaction expresses one intent and there is no half of it worth having. A **round** is
the deliberate exception (§16.5): it is `N` computations with no invariant spanning any two of them,
and one contended cell must cost one invocation rather than the round. Validation still precedes any
write there — every guard is checked against the parent as it stood before the merge, and only then
is anything applied.

**Failure and conflict rules:**

| Situation | Result |
|---|---|
| Parent moved the same def since fork | reject merge — the child authored against a def-view the parent has since moved, so re-fork from head and redo |
| Child guard fails when re-evaluated against parent history since the fork point | reject merge |
| Child wrote to an object the parent deleted | reject merge |
| Two branches wrote the same cell, neither having read it | last-write-wins — child wins |

**Guards are the conflict detector.** Re-evaluating a child's guards against the parent's history
since the fork point is exactly the question "did the parent touch this while I was working?" Since
§12 makes those guards automatic, LWW is no longer the default — it is what remains for a cell
nobody read. Cell granularity makes that far less destructive than object-level LWW would be.

**Guards are checked before the dangling-write check**, which is a reordering the automatic read-set
earned. A writer's own existence probe is now in its read-set, so "the child wrote to an object the
parent deleted" surfaces as a guard failure first — and that is the more useful of the two things to
be told, because it names the cell that moved rather than the cell that suffered for it. The dangling
check remains as the backstop for a *blind* write, which observed nothing and so carries nothing to
check.

**A transaction's own guards are checked once, before any of its layers is applied.** Per-layer
checking would let the first layer of a merge trip a guard belonging to the second (§12.1).

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
- Derived fields are known statically now that ownership is declared (§8), so a generator can mark
  them read-only. v1 emits everything as writable and lets the runtime rejection do the work; the
  static marking is deferred with the SDKs themselves.
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

  ┌─ WRITE ───────────────────────────────────────────────┐
  │  WriteSession    one open layer + the branch's        │
  │                  def-view, bound together. Every      │
  │                  write — client or producer — goes    │
  │                  through it and is validated (§8).    │
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
| `Scheduler` | **stateless** (§16.4); derives pending work from watermark gaps, settles each source layer as a transaction (§16.5) |
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
   layers to be open at once and to commit in any order. It is a statement about *fields*, and
   v1 pipelines being per-entity maps (§9.2) is what extends it to invocations: one invocation writes
   its own entity's cells, so two invocations of one producer cannot collide either. A producer that
   wrote across entities would break that, and nothing enforces it — it sits with idempotency (§9.2)
   as an obligation on the author rather than a property of the engine.
2. **Layer commit is the only invalidation trigger.** Buffers carry no observation machinery, which
   keeps `StorageProvider` small and swappable.
3. **Commit streams.** A layer may hold millions of mutations and can never be buffered whole.
4. **Locks are per-layer, never per-branch.** A branch-wide write lock would serialize derivation.
5. **Derived data is addressed by `reflects`, never by derived LayerId.** This is what makes the
   ordering of concurrent independent producers unobservable.
6. **The read index is consistent with the merge that changed it.** A merge adds membership without
   writing values, so the `(branch, cell, version) -> (layer, event)` projection has to gain its
   entries in the same invisible layer the membership went into. An index updated separately from
   the merge is a read that misses data.
7. **No membership test in the dependency index may be a linear scan.** A widely-shared upstream
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

The pause switch (§9.6) does not qualify this. It is a single per-branch flag saying whether the
scheduler is *invoked*, not a record of what it would do; nothing about which work is pending is
remembered anywhere, and a paused branch resumed a week later derives exactly the same gap it would
have derived a week earlier. The same is true of a `current` read, which invokes producers without
enqueueing anything.

**The degree of parallelism does not qualify it either.** The scheduler discovers a *wave* of
invocations from the layers in front of it and runs them concurrently, bounded by a number that is
deployment configuration and nothing more (v1: one per core, overridable). It is a bound on what is
in flight, not a queue of what is outstanding: nothing is remembered between waves, and a worker
killed mid-wave leaves the same gap it started from. Setting it to `1` is the sequential scheduler,
and §9.6 requires the two to settle on the same result.

Discovery stops at the first layer on the branch that has not committed. A layer *is* the changeset
(§9.6) and means nothing before commit, so settling one would derive from writes that may yet be
abandoned, and stepping over one would advance every watermark past a layer nothing had incorporated.
Waiting is the only honest option, and it is available precisely because there is no queue to stall.
An **aborted** layer is not waited for — it will never commit — and a layer found uncommitted when a
store is reopened is aborted, because an open layer is exclusive to a process that no longer exists
(§6.2).

**A round forking a branch of its own does not qualify it either** (§16.5). The fork is where a
round's output goes, not a record of what a round has left to do: nothing about pending work is
written to it, and a process killed mid-round leaves a branch holding derived layers no other branch
can see, plus the same watermark gap it started from. The next round rediscovers exactly the same
work and forks again. This is the same statelessness the pause switch and the parallelism bound leave
intact, applied to isolation instead of to scheduling.

### 16.5 A source layer settles as a transaction

A committed layer triggers producers; their output commits further layers, which trigger more.
**All of it carries the same `reflects`**, because it is all the consequence of one source layer. A
producer's watermark advances to `L` only once that whole closure has settled — which is precisely
what makes the watermark's claim true: *replay the world at `L` and you get exactly this.*

**A round is a transaction like any other write** (§12). It forks the branch at the source layer it
is settling, runs every producer on the fork, and merges when it settles:

```
fork at L → run producers → merge, guarded by what they read
```

Only **source** layers open a round. Derived layers are consequences, picked up inside the closure
rather than driving rounds of their own. Skipping derived layers entirely is tempting and wrong: it
silently breaks every chained producer, since a producer consuming another's output can only ever be
triggered by that producer's derived layer.

#### The fork point is the filter

**The layer a producer reads at is not the layer it reflects**, and the branch boundary is what
expresses the difference. A producer's read path is:

```
[(round branch, its own head), (trunk, L)]
```

- It sees its siblings' output, because that output is on the round's own branch bounded at that
  branch's head. There is no high-water mark to maintain: *"the head of my branch"* already means
  *"my source layer plus everything this round has committed"*, which is exactly the world a
  downstream producer must see to consume its upstream's output.
- It cannot see anything a client lands on the trunk meanwhile, because the trunk segment is bounded
  at the fork point and a layer committed afterwards is above that bound.

**`reflects` is therefore true by construction rather than by bookkeeping.** A round cannot label its
output with a layer it did not fork at, because the fork point is the only thing it can see. Earlier
drafts of this section maintained the same idea as a *ceiling* — a monotonic high-water mark raised
by every layer the round committed — and that was a `ReadPath` bound, which is a prefix, used to
express a filter. The two diverge exactly when a second writer exists: a client's source layer `L'`
committed mid-round with an id below one of the round's was admitted by the prefix, and output
labelled `fresh_as_of: L` could have incorporated data from `L'`. A branch boundary has no such gap,
because `L'` is not on the round's branch at all. The hole is not closed; it cannot form.

Three things stop existing with it: the ceiling, a `ReadPath` that would have had to carry admitted
layers beside its bound, and a `reflects` column the storage provider would have had to filter on —
which §17.1 forbids. The provider line is untouched.

#### Guards make round ordering irrelevant

Two rounds may be in flight at once — one settling `L`, one settling `L'` — and both may invoke the
same producer on the same entity. Single-writer-per-field does not help, because it is the *same*
producer. Without something else, whichever merges last wins, and that may be the older result.

Automatic guards settle it with **no ordering rule at all**. A round's guards are its producers'
read-sets, which the engine already captures (§9.4). The `L` round read some source cell at `L`, so
it carries *"unchanged since `L`"*. For it to be in danger at all, `L'` must already be on the trunk —
otherwise the `L'` round could not have forked from it — so its guard fails **whichever order the
merges are attempted in**. The stale round is not sequenced behind the fresher one; it is rejected.
That is stronger than an ordering rule, because it needs no queue and no serialization point: the bad
interleaving becomes harmless rather than prevented.

**A round guards the cells it read and the round did not write.** The subtraction is round-wide, not
per-invocation, and that is what lets a chain commit: within one round `invest` writes
`is_investible` and `tier` reads it, and `tier` must not fail on a cell its own round produced. §12.1
states the client's version of this rule as an *ordering* — a read before your own write is still
guarded — because taken as a set difference it deletes read-modify-write. A round has no order to
appeal to, and does not need one: everything it writes is derived, a derived cell is never in the
cell-touch index (§12.4), so no guard on one could ever have tripped; and a producer that reads a
cell it writes is a cycle (§16.6), not a compare-and-swap.

#### Rounds apply partially

§13 rejects a whole merge rather than applying it partially, and that stays true for a **client
transaction**, which expresses one intent. It is not true for a **round**. A round is `N` independent
computations with no invariant spanning any two of them, and whole-round rejection would let one
contended cell kill a 128k-invocation round — and under sustained contention it would never land at
all. So a round applies the invocations whose guards held and drops the rest.

Dropping is safe because of the freshness design rather than in spite of it. A dropped invocation's
edges were recorded in the dependency index when it ran — on the trunk, never on the round's own
branch — so its cells are still dirty; the layer that failed its guard is itself a source layer some
later round settles, and that round rediscovers the invocation through the very cell that moved. In
the meantime the value reads `stale` with a watermark that says so (§10.2).

**The applied subset must be closed under the round's own dependencies.** If an invocation is
dropped, everything in the same round that read what it wrote is dropped with it, transitively.
Otherwise the round would publish a value derived from one that never landed, labelled with a
watermark claiming exactly the replay that would not reproduce it (§10.1) — a lie of precisely the
kind §16.5 exists to make impossible. The closure is computed over the round's own reads and writes
only: a read of something no invocation in this round wrote is a read of the snapshot, and the
snapshot is not going anywhere.

Guards are evaluated **first, all of them, against the trunk as it stood before any of the merge
landed**, and only then is anything applied. Checking them interleaved with application would let one
invocation of a round trip another's guard, which is the mistake §12.1 rules out for a transaction's
own layers.

#### Rounds are ephemeral

A round branch is created with derivation paused, like every other transaction branch (§12) — a round
deriving on its own branch would be a round inside a round. It is never merged into by anything, and
nothing else in the system can see it: a branch is visible only to its descendants (§7.2).

**A wedged round is reaped by being abandoned.** A process that dies mid-round leaves a branch row and
derived layers no other branch can reach; the watermark never advanced, so the next round rediscovers
the same work and forks again. That is the same property that makes partial application safe, and it
is why *idle* rather than *elapsed* is the right measure for transactions generally (§12.3): a
legitimate 128k-invocation round runs long but is never idle.

#### Concurrency within a round

Ordering within a round is not prescribed, and **a round runs its invocations concurrently**. The
invocations discovered from one layer cannot collide: single-writer-per-field (§16.3) means no two of
them target the same cell, so their layers commit in any order. If a downstream producer runs before
its upstream, it computes from an absent input, the upstream then commits, and the downstream's
dependency on that cell brings it back round. The fixpoint self-corrects; only the settled result is
guaranteed.

**A round alternates discovery with execution, and the alternation is load-bearing.** One wave of
invocations is discovered, run to completion, and only then are the layers it produced turned into
the next wave's work. That barrier is what makes "the fixpoint self-corrects" true rather than
hopeful: a producer records its read-set *before* its layer commits, so a run that missed an input is
already subscribed to it by the time any later wave scans the layer that supplied it. Without the
barrier a run could commit after the layer it needed had already been scanned, and the correction
would never be triggered. The barrier is about that ordering and nothing else; it long predates the
fork and is unaffected by it.

**One layer per invocation on the round branch, one layer per producer on the trunk.** The
per-invocation layer is the unit partial application decides on, and a guard is a fact about one
invocation. Nothing downstream of the merge needs that granularity — a layer is an ordered group of
events (§6.2) and `LayerAuthor::Derived` describes the whole group — so the accepted layers are
regrouped by producer on the way across, and a fan-out of 128k invocations lands as one layer rather
than 128k.

#### The residue: cost, and ordering

**Every producer read walks two read-path segments.** That is the price of the branch boundary and it
is paid on the hot path: a 128k-invocation round makes roughly a million reads, each now resolved
through `[(round branch, head), (trunk, fork point)]` rather than through one segment. Measured at
about +16% on a full derive and +30% on a re-derive; the shape of the curve is unchanged. The merge
itself is a small part of that, because a merge whose parent has had nothing written to it since the
fork point does not build a guard set at all, and because the round's per-invocation layers are
regrouped by producer.

**Derived output can land after source layers it predates.**

A round reflecting `L` may merge at a trunk position above a client's `L'`. Derived history is
therefore non-monotonic in `reflects`, and a **time-travel read pinned at exactly `L`** does not see
output computed from `L` — which is correct, because at `L` the round had not landed, and is
surprising. Derived data is addressed by `reflects` rather than by derived LayerId (§16.3), so this
affects where in the log a value is found and never which value it is.

A second residue is a **backlog**: when several source layers are committed before any of them is
settled, the round settling the earlier one merges above the later one's id, so the round settling the
later one — forked at that later layer — cannot see it. Each round recomputes what its own source
layer dirtied, chains included, so this costs re-runs rather than correctness in the shapes v1
produces; but an invocation dirtied by `L'` that depends on a derived cell only some earlier round
produced would read it absent. Settling a *range* rather than a single layer is the shape that closes
it, and it changes what a watermark counts, so it is its own change.

### 16.6 Cycle detection

A cycle is a producer that transitively depends on a field it writes. It cannot be detected
statically, and under a stateless scheduler it does not surface as re-entry — it **livelocks**: the
producer runs, advances its watermark, dirties its own input, and is rediscovered forever.

**v1 detection is a per-invocation re-run counter scoped to one round (§16.5).** If an invocation
runs more than `K` times while settling a single source layer, it is cycling; the producer is marked
broken (§14) and its output cells report `state: 'broken'`.

An inline computation (§10.5) has no round to scope a counter to, and does not livelock either: it
walks *recorded* read-sets rather than being rediscovered by a scheduler, so a cycle appears as
re-entry and is stopped by refusing to compute a cell already being computed further up the same
call. That cell keeps whatever value it has, and the read describes it honestly.

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
get_cell(branch, cell, layer) -> (Event, landedAt)?
author_event(open_layer, cell, EventDraft) -> EventId   // streaming; never buffered whole
include_event(open_layer, EventId)              // membership — what makes merge not copy
read_layer(layer) -> Iterator<Event>            // a layer's membership, in order
read_membership(layer) -> [EventId]             // the same, as identities — what merge actually wants
rebuild_read_index()                            // the index is a projection, and this proves it
scan_buffer(branch, buffer, layer) -> Iterator   // engine-internal enumeration only
open_layer / seal_layer / commit_layer / abort_layer
intern(kind, bytes) -> Pid                       // content-addressed; no branch, no layer
read_interned(pid) -> bytes?
```

**Interning is unscoped, and that is the whole point.** `intern` takes no branch, no layer and no
def-version, because a content PID has none (§3.1); a provider that scoped it would reintroduce
exactly the conflict the scheme exists to eliminate. Nor is it written into a layer: interning takes
effect immediately, since an interned value nobody references is garbage rather than corruption, and
an aborted layer stranding one costs only space. `read_interned` answering `None` is a legitimate
result — a PID travels further than the bytes behind it, and rendering such a value as its bare PID
is then the honest answer rather than a failure.

**Nothing above this line calls these directly, and no client calls them at all.** The engine interns
on the way in and resolves on the way out, at whatever surface accepts or emits value text (§3.4).
Two placements were considered and rejected: `ProducerCtx` alone, which is not the only writer —
`borg set` writes source cells with no `ProducerCtx` in sight — and the read path, whose currency is
`Value` and which would have to push every internal consumer through a string round trip to serve the
two edges that want text. `ProducerCtx` does *expose* interning, because a producer runtime holds no
store handle of its own and must not acquire one, but it delegates rather than reimplementing.

**Streaming commit is the binding constraint** (§6.2). Any provider that cannot accept an unbounded
write stream into an uncommitted, invisible layer is disqualified.

The SQL shape that satisfies it: rows are inserted as they arrive and **visibility is a join, not a
rewrite** — every read joins the event and membership tables against a layer table and keeps only
rows whose layer is committed. Committing is then a single-row update, `O(1)` however large the
layer. Flipping a `visible` column on each row instead would make commit `O(rows)` and undo the very
property the interface exists to preserve.

**Membership is readable without its events.** A merge names a child layer's events on the parent
(§13) and wants nothing about them but their identities — and a round's output is `n` events each
carrying the read-set it was computed from, so reading them whole to discard everything but the id is
a deep copy per event where a pointer would do. `read_membership` is that read; `read_layer` remains
the one the invalidator uses, because a changeset is only a changeset if you can see the cells.

**The read index is maintained on the way in, for the same reason.** Its rows stream in with the
events they project and are invisible by the same join; building it at commit instead would make
commit `O(rows)` just as surely as a `visible` flag would. `rebuild_read_index` exists so that
"the index is a projection of the log, not a second source of truth" is checkable rather than
merely claimed — it is on the interface, not on the write path, and nothing in the engine calls it.

Reading at HEAD is expressed as *no layer*, not as the branch's head: a fork that has not written
anything yet has no head of its own, and its effective ceiling is the fork point it inherits.

**Blocking backends belong on a blocking pool.** The trait is async and the whole engine above it
awaits, so a synchronous store is the provider's problem alone and must not occupy an async worker
thread.

**Writes batch; reads do not need to.** `author_event` is called an unbounded number of times, so
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

**Workers are stateless, so a provider holds a pool and not a worker.** A round runs invocations
concurrently (§16.5), and one reused worker behind a lock would serialize every one of them through
one process whatever the scheduler decided — the queue, reintroduced below the seam. Reuse is an
optimization over spawning per entity, never a semantic: deterministic output PIDs (§9.5) mean two
workers racing one invocation produce identical output, and a stateless scheduler (§16.4) makes a
lost worker cost a retry. A worker whose conversation ended in anything but a clean `Done` is
**discarded rather than returned**, because a request/response stream left at an unknown offset would
answer the next invocation with the last one's reply.

```rust
#[async_trait]
pub trait ExecutionProvider {
    async fn run(&self, producer: &ProducerRef, input: Pid,
                 ctx: &mut dyn ProducerCtx) -> Result<()>;
}

#[async_trait]
pub trait ProducerCtx {
    async fn get(&mut self, cell: CellRef) -> Result<Option<Value>>;  // recorded, at my ClientVersion
    async fn get_at(&mut self, cell: CellRef, v: ClientVersion) -> Result<Option<Value>>;
    async fn get_input(&mut self, cell: CellRef) -> Result<Option<Value>>;  // §9.3
    async fn set(&mut self, cell: CellRef, v: Value) -> Result<()>;   // validated against the defs
    async fn set_text(&mut self, cell: CellRef, t: &str) -> Result<()>; // parsed against the defs
}
```

### 17.4 The worker protocol

A producer worker speaks a framed message stream: the engine invokes, the worker asks for cells, the
engine answers. Codecs are negotiated in a handshake and framing is **per codec** — newline-delimited
for text so a shell worker can use `read`, length-prefixed for binary. Every encoding is produced by
the same serde impls on the same types, so no mapping document exists between them to drift.

Two shapes were forced by targeting a shell worker first, and both are better than what they replaced:

- **Cells and values travel as text** — `"Company:o-1234abcd.website"`, `"9"`, `"@o-5678wxyz"`,
  `"~"`, `"acme.ai"` (§3.1, §3.4, §4.1) — the same forms the CLI accepts. A worker cannot reasonably
  assemble the structural JSON of a cell
  address, and a protocol only usable through a generated client library is one whose complexity is
  hidden rather than absent. Text also removes the `Int`/`Double` ambiguity a bare JSON number has.
- **Every message is a single-key object**, including the payload-free ones. A worker dispatches on
  one key without special cases.

**Strings on the wire are strings.** A `Get` of a string cell is answered `{"value":"acme.ai"}`, not
with the `@s-…` that is physically stored, and a `Set` carrying `"acme.ai"` is complete — the engine
interns it before the write lands. A worker therefore never makes a second round trip to resolve or
create a string, and never has to know that content addressing exists (§3.4). Anything else would put
an extra round trip on the hottest path in the protocol in exchange for exposing a storage detail.

**A worker's writes are text, and the engine parses them against the declared type.** A `Set` carries
the text, not a parsed value, because only the engine knows what the field is declared to hold
(§3.4). Parsing worker-side would be parsing without a declared type, which would leave `true`
unstorable in a `String` field over the wire while the CLI stored it fine — two dialects for one
value model.

**A repo describes its definitions as well as its producers.** `describe` runs once at push time and
returns both, and `borg repo push` folds all of it into **one def layer**: a producer and the field
it writes land together or not at all. This is not tidiness. A producer cannot write anything unless
its output field is declared (§8), and the repo implementing the producer is the only thing that
knows the field exists. It is also the DSL story — a Python repo defines structs through an SDK and
its runtime emits this shape, making `defs/*.json` one way of producing it rather than a parallel
path.

The payload is shaped so a shell script can produce it with one `jq -n`:

```json
{ "structs": [ { "name": "Company", "fields": [
      { "name": "website",       "type": "String" },
      { "name": "founded",       "type": "Int", "up": "founded_up", "down": "founded_down" },
      { "name": "is_investible", "type": "Bool", "derived_by": "invest" } ] } ],
  "producers":  [ { "name": "invest",     "source": "Company" } ],
  "migrations": [ { "name": "founded_up" }, { "name": "founded_down" } ] }
```

`derived_by`, `up` and `down` name producers **by name**, not by id: a repo knows what it calls its
own code and should not have to compute the hash the engine turns that into. A name the repo does not
implement is a push-time error — a field nothing can ever write, or a migration nothing can ever run
— and so is a migration no field names, which would be registered and unreachable. Every list
defaults to empty: a repo of pure schema and a repo of pure code are both legitimate.

**A schema change is a diff, not an instruction.** There is deliberately no way to say "mutate this
field": a repo emits the shape it believes in now, and the push compares it with the definitions in
force — a field nobody has declared becomes a `DeclareField`, one whose type has moved becomes a
`MutateField` (§6.1), one that is unchanged is a repeat and a no-op. A repo cannot spell a mutation
directly because it does not know what it is mutating *from*; the branch does, and on another branch
the answer differs. That is also why the migrations are named on the field rather than beside the
change: the field is the thing that persists across pushes.

A migration spec carries only a name. Which field it bridges, and in which direction, comes from the
field naming it as `up` or `down` — one source of truth, so the two cannot disagree.

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
