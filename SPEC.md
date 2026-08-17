# Borg — v1 Specification

> **Status:** design spec for Act 1. Normative for v1 implementation.
> Sections marked *(deferred)* describe intent for later versions and are **not** to be built now.

---

## 1. Overview

Borg is an event-sourced data backend. The long-term ambition is to subsume the modern use cases of
ORMs, data pipelines, ETL and reverse-ETL. **Act 1 — this document — is the modern ORM.**

Four ideas define the system. Everything else follows from them.

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

**4. Every write is a transaction.** Nothing writes to a shared branch. A client forks, writes in
isolation and merges; a derivation round does the same thing with `N` computations instead of one
intent (§12, §16.5). What a writer *read* is what its commit is contingent on, so guards are
automatic and the merge-conflict detector is the read-set that was already being captured for
dependency tracking. There is one write path in the system, and it is the safe one.

The fourth idea is the youngest and it arrived by deletion. Isolation used to be expressed as a
bound on a read path — a *ceiling* — and a bound is a prefix being asked to express a filter. A fork
point expresses the filter exactly, so the ceiling, the hole it left, and the machinery proposed to
patch that hole all stopped existing rather than getting fixed (§16.5).

---

## 2. Glossary

| Term | Meaning |
|---|---|
| **Registry** | The root container. One per company, typically. Owns the branch tree and all repos. |
| **Repo** | A namespace-less contribution unit. Teams define structs, fields, and producers through repos. |
| **PID** | Point ID. Universal identifier for every non-primitive value. |
| **Cell** | The universal addressable unit: a buffer plus a key — a PID (an object property, or an object's existence) or a `(PID, index)` (a list element). The *field* is the buffer, not part of the key (§4.1). |
| **Buffer** | A partition of cells, keyed by a def: one per struct, one per field, one per list def, one each for the untyped containers. Interned values are not cells and have no buffer (§4.2). |
| **Def** | A definition — `ObjectDef`, `ListDef`. |
| **Def-version** | The def-layer that most recently mutated **one** definition. The version half of every stored record. Not a whole-schema version (§5.3). |
| **Event** | A single mutation, with an identity of its own. Either a `ValueEvent` or a `DefEvent`. An event names no layer; layers name events (§4.3). |
| **Layer** | An ordered group of events, held **by reference**. Belongs to exactly one branch; one event may belong to many layers. The unit of atomicity (§6.2). |
| **Branch** | First-class fork of the registry timeline. |
| **ClientVersion** | A LayerId identifying the def-view an actor's code was authored against. A whole schema; moves on every push (§5.4). |
| **Transaction** | The only write path: fork, write in isolation, merge. A client transaction lands whole or not at all (§12). |
| **Round** | Derivation as a transaction — the `N` producer invocations that settle one range of layers, forked and merged together, applying partially (§16.5). |
| **Guard** | An assertion that nothing has touched a cell since a layer. Guards are **automatic**: what a writer read is what its commit is contingent on, and re-evaluating those reads against the parent *is* the merge-conflict detector (§12.1, §13). |
| **Merge** | Replaying a child branch's layers onto its parent by **naming** their events, never by copying them (§13). |
| **Producer** | Anything that computes derived data: a pipeline or a migration. |
| **Reflects** | The source layer a derived layer brings the world up to. Derived data is addressed by `reflects`, never by derived LayerId (§6.3). |
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

There are two allocating authorities before there are two nodes, and they are the first thing the
component buys. Allocator `0` belongs to **ids written by hand**: `Company#1` is the shorthand for
counter 1 there (§3.4), and a scenario, a fixture or a person at a terminal chooses counters in it.
A store allocating ids on a client's behalf (`tx_create`, §17.5) issues under an allocator of its
own, so the two id spaces are disjoint by construction rather than by a convention somebody has to
remember — nothing has to ask what already exists before creating something.

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
| `sealed` | writes closed; durability and validation happen here — a layer's own hand-written guards are checked at this edge, so a rejected layer never becomes visible (§12.4) |
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

**Derived layers coalesce across a range.** A round settles everything between a producer's
watermark and head (§16.5), so a producer emits **one derived layer per round**, labelled with the
top *source* layer of the range it settled. Several source layers landing while a producer is busy
therefore cost one layer between them, not one each.

v1's rule was the opposite — one derived layer per `(producer, source layer)`, **no coalescing** —
and it was retired for a correctness-shaped reason rather than an economy one. A round per source
layer computes each layer from a world the next one has already moved, and the automatic guards that
catch that (§16.5) then reject work the *schedule* had guaranteed would be stale: under sustained
backlog most derivation work was run and thrown away. The rule also left a producer whose input is
written only by a derived layer with nothing to trigger it, because derived layers open no rounds —
so a chained migration, or a pipeline pushed over data that is already derived, was never discovered
at all.

**What is lost is derived-history granularity, and it is now schedule-dependent.** Two Borg instances
replaying the same source log agree on every *settled* value and on every label those values carry.
They do not agree on how many intermediate snapshots exist: an instance that settled `L10`, `L11` and
`L12` one at a time holds three generations of derived layers, and one that settled them as a range
holds one. Nothing can ask which. History is addressed by **source** layer and derived data resolved
by `reflects`, never by derived LayerId — a time-travel read at `L11` takes the derived layer with
the greatest `reflects ≤ L11`, which is `L11`'s own output in the first instance and the range's in
the second, and both are what §10.1 promises for the world they name.

**What still holds is every label.** `reflects` is true by construction rather than by bookkeeping: a
round cannot label its output with a layer it did not fork at, and it forks at the top of its range
(§16.5). And settled values stay deterministic, which is the property `scenarios/200-determinism`
sweeps — its digest strips layer ids, because those were always a property of the schedule.

Ordering is enforced where it is meaningful — a producer reading another's output cannot start until
that producer's layer commits — and the residual variation is which LayerId was assigned to which of
two *concurrent independent* producers, and how many rounds a given backlog took. Neither is
observable, for the reason above.

**One layer per invocation, on the round's own branch.** A source write that invalidates 100k
entities produces 100k layers *inside* the round: a guard is a fact about one invocation, and partial
application decides per invocation (§16.5). They are regrouped by producer on the way across the
merge, so the trunk gains one layer per producer rather than 100k — which is what keeps fork-and-merge
from doubling the log, and is the other half of what "one derived layer per producer per round"
means.

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

A producer standing **below its own ClientVersion** has not run at the version it is now defined at,
which is the same statement as "has never run here" and takes the same path: its whole source buffer,
enumerated (§9.6). This is what makes the sentence above true rather than merely asserted — the
alternative is a store in which one field holds output from two different programs, every value
labelled `current`, with nothing in the envelope (§10.4) to tell them apart.

**A repo says what its code is, and that is part of the producer's definition.** `describe` (§17.4)
carries an optional **implementation fingerprint** per producer: an opaque string whose only contract
is that it changes when the code changes. `ProducerDef` records it, so it is folded, forked and merged
like the rest of the definition, and `borg repo push` diffs it like the rest of the definition — a
changed fingerprint re-emits `PushProducer` and nothing else does.

It has to be part of the *definition* rather than a fact about a machine, because the diff is where
this decision is made. A repo emits the shape it believes in and the branch says what it already
believes (§17.4); before the fingerprint, the comparable surface was a producer's name, source buffer
and declared fields, and **an edit to a pipeline body moves none of them**. So an edited pipeline
diffed as unchanged, no event was emitted, and the machinery above never fired. Where the code *lives*
stays outside the log all the same — see *Definition and implementation are separate* below, which is
the line this sits on and does not cross.

Four consequences, all deliberate:

- **A fingerprint change is not a schema change.** It emits no `MutateField`, demands no migration and
  moves no field's def-version. The producer's own definition re-lands and its output is recomputed in
  place, at the version those values already had (§5.3). A migration bridges two shapes that coexist;
  here the old output is simply wrong.
- **An unchanged repo emits nothing at all**, and that is what makes this affordable. The recompute is
  `O(the producer's source buffer)` per *edit*, not per push, which is the difference between a
  guarantee and a dev loop nobody would leave switched on.
- **Absent means never invalidate on a code change.** A fingerprint is optional on the wire: a repo
  that supplies none gets one from the pusher, which hashes the executable it just ran — that is what
  covers a worker written in `bash` and `jq`, which cannot reasonably digest itself. A producer that
  can be fingerprinted by neither route behaves exactly as every producer did before fingerprints
  existed. What an SDK's own fingerprint covers is the SDK's to state and varies by language; nothing
  compares one producer's fingerprint with another's, or with the same producer's under a different
  scheme, so they need not agree on anything but changing.
- **Absent → present is a change**, so the first push against a store that predates this recomputes
  once. Nothing recorded what produced the values already there, and one recompute is the only honest
  answer to that.

**A pipeline's output field must be declared, and its repo is what declares it.** A producer write is
validated like any other (§8), so a pipeline whose output field nobody declared cannot write at all.
The repo therefore emits its struct definitions alongside its producers, and both land in one def
layer (§17.4).

A producer's ClientVersion **is** the def-layer it was pushed at, so it is folded rather than
authored: the layer id does not exist when the event is built, and a merge replays that event onto
another branch as a different layer. What the event carries is a placeholder.

**Definition and implementation are separate.** The log records only the *definition* — `ProducerId`,
source buffer, ClientVersion, and a fingerprint of the implementation. The `ExecutionProvider` (§17)
resolves that ID to an *implementation*. In v1 that resolution is a static registry of Rust functions
compiled into the binary; later it is a container image reference reached over a socket. The log's
model is identical either way.

The fingerprint does not weaken that separation, and the line it sits on is worth being exact about.
What the log records is *that the code is this one and not that one*, which is a fact about the repo:
the same on every machine, forkable, mergeable, and the thing a diff has to compare. What stays out is
*where the code is* — a path, an image reference, a registry entry — which is a fact about one
deployment and would tie the data model to a filesystem. Swapping a build in behind the log's back is
therefore invisible on purpose, and the operation that exists for it is `borg derive --rebuild`.

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
correctness, only latency.** v1's policy is naive-eager and is written in one place rather than
behind a trait (§17); smarter prioritized policies come later, provably without regression.

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

**The walk covers every layer a round is settling, derived layers included.** A round settles a
*range* (§16.5), and a cell written by a previous round's merged output is a trigger like any other
write: it is the only thing that can ever start a producer whose input exists only in derived data —
a chained migration, or a pipeline pushed over data something else already derived. Skipping derived
layers is what left those undiscovered until an operator ran a full rebuild.

A layer is skipped for a producer that has already incorporated it, and *already incorporated* is a
question about the source stream: a layer's **position** is its own id if it is a source layer and
its `reflects` if it is derived, and a producer standing at watermark `W` has incorporated everything
positioned at or below `W`. Without that test a settled branch would re-derive itself for ever, since
a round's own merged output is inside the next round's range.

Keeping this above the provider line matters: were tracking to live inside the buffers, every
`StorageProvider` would have to reimplement it. The storage interface stays small — get cell, put
cell, stream a layer, scan a buffer — which is what keeps both a plain KV store and Postgres viable
behind one seam.

The engine internally enumerates a producer's source buffer in order to discover new entities.

> This **was excluded** from the client surface: *"enumeration is not exposed as a user-facing query
> in v1."* The reason was that a query surface is not a field on a message — it is filters, ordering,
> paging and joins, and a system that grows one by accident grows it in the shape of whatever asked
> first. The exclusion is reversed by an application that could not otherwise be written: an app
> cannot ask *"which contacts are there"*, and no arrangement of `get` answers it. So §17.5 has
> `list`, and the reason for the original exclusion is what bounds it — **ids of one struct, at head,
> unfiltered, unpaged, and outside any transaction**. It is this scan with tombstoned existence cells
> skipped, and nothing else. What is still out is the query layer; what came in is the one read the
> store already performed.

Enumeration is not, and cannot be, a **guardable** read. A guard is a question the cell-touch index
answers about a cell (§12.4), and *"the set of Contacts"* is not a cell; the honest guard for a
listing would be *"no object of this struct was created or deleted since the fork"*, which is the
absence-guard problem (§12.1) widened from one cell to a whole buffer and would make every creation
conflict with every enumeration. So a listing is a read outside any transaction, buying exactly what
a `get` outside one buys: nothing at commit. There is deliberately no `tx_list`.

Enumeration is what a **producer newer than its data** needs, and it is the one thing the layer
changeset cannot supply: the entities it owes were written before it existed, or on a branch it was
forked from, and no layer belonging to its own branch names them. A producer that has never run on a
branch therefore takes its whole source buffer, read through that branch's ancestry, and streams
normally thereafter.

It is scanned **at the top of the range**, which is where the round forks and therefore where the
world is complete. Scanning at the bottom is what spent the one chance a brand-new producer gets on a
fork point where the data it wanted did not exist yet.

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
| `broken` | The producer threw or cycled. `IllegalState`, scoped to the producer (§14) — so this is not a worse `stale` but a different fact: nothing is catching up, and nothing will until code is pushed. |
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

**There is exactly one exception, and it is the empty case.** A transaction forks the highest layer
its branch can *see*, so a branch whose entire ancestry holds no layers has nothing to fork from,
and a write there goes to the branch directly. That is safe rather than a hole: §8.0 makes every
write contingent on definitions, definitions live in def layers, and a branch with no layers has
none — so the write is going to be rejected whatever path it takes, and the direct path is what gets
the caller *"no struct named `Wombat`"* instead of *"nothing to fork from"*. There is also nothing to
isolate from, because anything concurrent would have left a layer.

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
when a process opens the store**, so there is no daemon and an idle store sweeps nothing because
nothing is growing. For a process that holds the store open, that reads as *whenever it takes a
request*: the sweep is a scan of one small file beside the store and does not depend on anything the
open rebuilt (§17.1).

The sweep sits *above* the store rather than inside it. Transaction state is operational state beside
the store (§12.2), and the layer that opens a `StorageProvider` is the one place that must never
learn what a sidecar file is (§17.1). So "when a process opens the store" means the process — for the
CLI, its entry point; for a server, wherever it opens the store — and not the storage seam.

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

A guard is one shape, whoever produced it:

```
Guard { cells: CellRef[], since: LayerId }
```

It asserts that nothing has touched those cells since that layer. Note the currency: **the guard is
over `CellRef` even though the read that produced it was over `CellAt`.** A guard is a question about
a cell, and a write at any def-version is a write; the version survives only in the *subtraction*
that decides which reads become guards, where it is load-bearing for exactly one producer (§16.5).

**`since` is the fork point, for every guard a transaction carries.** Recording the moment of each
read instead is not merely unnecessary, it is unsound: a transaction's read path is bounded at the
fork point (§7.2), so every read observes the parent as the parent stood *then*, whenever it happens.
Using the read's own moment would ignore every parent write between the fork and the read — writes
the transaction provably did not see, because they were above its bound — which is the exact set a
guard exists to catch.

**Two producers of guards, and they are checked at different edges.**

| Guard | Produced by | Checked |
|---|---|---|
| automatic | the transaction's read-set, `since` = the fork point (§12.1) | at **merge**, against the parent's history since the fork point |
| hand-written | a caller attaching one to a layer | at **seal**, against its own `since` — and again at merge, because a merged layer's guards are re-evaluated against the parent |

The seal check is what makes a rejected layer leave no trace: nothing has become visible yet. The
merge check is what makes guards the conflict detector (§13). They are the same question asked of two
different histories, so they are one function asked twice rather than two.

Both are checked against a **cell-touch index** (`cell -> layers that wrote it`). Only *source*
layers are recorded in it: guards may name source cells only, so a derived write can never appear in
a guard, and derived layers are the enormous ones. Skipping them bounds the index by authored data
rather than by everything the derivation engine produces. A merge whose parent has had nothing
written to it since the fork point answers its whole guard set from that index without touching a
cell — which is why an uncontended round pays almost nothing for its guards.

The index is queried along the **read path** (§7.2) rather than one branch, which is exactly what
lets a child's guard be re-evaluated against its parent at merge time.

v1 supports `ObjectTransaction` only. `ListTransaction` and others are deferred.

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

### 14.1 What a poisoned producer costs

Three things follow, and each is a promise to a different reader:

- **Its cells read `broken`, not `stale`** (§10.4). The two are not degrees of the same thing.
  `stale` says a catch-up is coming and a client may wait for it; for a poisoned producer none is
  coming until somebody pushes code, and a client told `stale` waits forever. The state overrides
  what validation concluded rather than refining it — `freshAsOf` is left where validation put it,
  because the value really is correct through there and the news is only that it will not move.
  The last good value is still served: a broken cell is labelled, never withheld (§10.4).
- **`explain` says why**, carrying the error the producer raised. A state without a reason sends
  whoever read it looking through logs for one.
- **It is not scheduled**, including for the invocations that have nothing to do with what failed,
  and including for a `freshness: 'current'` read that asked to pay for a computation. The producer
  is the unit that is judged. Re-running it on the next command would repeat the failure, burn the
  work, and repeat whatever partial effects it had.

**Its watermark still advances.** Holding it back would stall the settled frontier (§10.5) and every
`frontier.reaches` on the branch behind one bad pipeline — which is the branch-wide poisoning this
section exists to avoid. The read envelope is the channel that carries the news instead, and the work
skipped meanwhile is handed back at recovery.

### 14.2 Where poisoning lives

A poisoning is **operational state, not log data**, kept beside the store rather than in it — in the
same class as the pause flags (§9.6) and the producer-implementation table (§9.2).

It is not a value event and not a def event, and a layer holds one or the other (§6.2). It is
*discovered*, the way field ownership used to be before it became declared (§8), so nobody wrote it
and there is no author to attribute it to. In the log it would be forkable, mergeable and
time-travellable: a fork would inherit a poisoning its own code never earned, and a merge would carry
one back onto a branch that is working. *"Was P broken at layer 400?"* is not a question anybody has.
It is not storage's concern either — nothing above the provider line teaches a `StorageProvider` what
derivation is (§17.1) — so it reaches durability through a provider of its own.

**Recovery is the log's, even though the record is not.** Fix the producer and push a new
ClientVersion, which invalidates and recomputes its output. A producer's ClientVersion is the
def-layer it was pushed at (§9.2), so a poisoning that **names the version it was recorded against**
expires by itself: it applies while the branch still appoints that version, and stops the moment a
push moves it. Nothing has to remember to clear it, no command has to be run in the right order, and
a record restored from a backup cannot poison code that has since been replaced. The record is not a
fact in its own right — it is a claim about a fact the log holds, and the log decides whether the
claim is still live.

Recovery hands back the work the producer missed: its watermark is rewound, so the layers skipped
while it was broken are re-derived rather than silently passed over. A fix the log *cannot* see — a
worker's environment repaired, a service it calls back up — has no expiry to wait for, and is spelled
`borg derive --retry-broken`. Retrying by default is what turns one bad deploy into the same failure
repeated by every command.

v1 is deliberately strict elsewhere — whole-merge rejection rather than partial application. Softening
these edges is later work.

**A producer that has never succeeded has no cells to report anything.** `broken` is a label on a
stored record (§10.4), and a pipeline that threw on its first run wrote none — so its output reads as
simply absent. Saying more would mean materializing an envelope for every cell a producer *might*
have written, which is not a set anything can enumerate.

---

## 15. Code Generation

Generated SDKs were deferred out of v1 because a generated client needs a transport to reach the
engine, and building one competed directly with building the engine. **They arrived with §17.5**, as
that deferral said they would: `borg generate --lang ts -o <dir>` emits one module per branch.

The generation contract, now realised:

- **Generated from the branch's def view at a layer, and that layer becomes the client's
  ClientVersion.** It is baked into the module and sent in the handshake (§5.4, §17.5), which is what
  makes an old generated client a first-class actor rather than a stale one: it keeps writing the
  shape it knows and keeps reading through `down` migrations. Regenerating is how a schema change is
  adopted; not regenerating is a supported state. Generation reads the def view through the socket
  when the store is served and opens the store when it is not, because §17.5's advisory lock would
  otherwise make "stop the server to regenerate" a workflow.
- **Derived fields are marked read-only.** Ownership is declared (§8), so it is a static fact, and
  the earlier plan to emit everything as writable and lean on the runtime rejection was explicitly
  deferred "with the SDKs themselves" — this is them.
- **Reads return the provenance envelope of §10.4.** A generated handle offers both the value and the
  envelope; the value-shaped shortcut refuses a `broken` cell rather than answering `null`, because
  §9.3 forbids substituting "there is nothing here" for "there is no path to a value from your
  version".
- TypeScript first, then Python, Rust, Go.

**The v1 constraint that made this possible:** every engine operation has a **serializable
command/response form** — no callbacks, no borrowed references escaping the API surface, no
in-process-only affordances. It was nearly free at the time and would have been expensive to
retrofit; it was the difference between "add a transport" and "redesign the API", and §17.5 was
indeed only a transport.

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

  ┌─ DERIVATION (the cycle, inside one round) ────────────┐
  │                                                        │
  │    a range of committed layers   ── fork ──► round     │
  │         │                                     branch   │
  │         ▼                                              │
  │    Invalidator ──lookup──► DependencyIndex             │
  │         │                    fwd: invalidation         │
  │         │                    bwd: lineage              │
  │         ▼                    (keyed on the trunk)      │
  │    Scheduler     policy: eager and stateless (§16.4)   │
  │         │        one wave, discovered whole            │
  │         ▼                                              │
  │    ProducerRuntime   a layer per invocation, run       │
  │         │            concurrently; ProducerCtx         │
  │         │            observes every cell access        │
  │         └──────────► commits ──┐                       │
  │                                │                       │
  │         ┌──────────────────────┘                       │
  │         ▼                                              │
  │    (its layers are the next wave — barrier between)    │
  │         │                                              │
  │         ▼                                              │
  │    merge_round ──► trunk, guarded by what was read,    │
  │                    applying partially (§16.5)          │
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
| `Invalidator` | walks every layer a round is settling — derived ones included (§9.6) — and converts it into dirty invocations |
| `DependencyIndex` | bidirectional cell ↔ invocation graph; in-memory-primary; keyed on the **trunk**, never on a round's own branch |
| `Scheduler` | **stateless** (§16.4); derives pending work from watermark gaps, settles the whole gap as one transaction (§16.5) |
| `ProducerRuntime` | executes user code through `ProducerCtx`, which observes every cell access — nothing is declared, so nothing can be mis-declared (§9.4) |
| `Resolver` | read path: locate cell, migrate for version skew, validate, build envelope. Holds `InlineDerivation` — one method — rather than the engine, so the read path is a *client* of derivation (§10.5) |
| `FrontierTracker` | per-producer watermarks, settled frontier, `frontier.reaches()` |
| `CellTouchIndex` | `cell → layers that wrote it`, source layers only; backs guard validation (§12.4) |
| `PoisonProvider` | which producers are broken, and the ClientVersion each judgement was made against (§14.2) |
| `Projection` | what the three rows above **are**: a fold over committed layers, with a position. Opening a store brings each to head; a projection already at head folds nothing (§17.1) |

### 16.2 Verbs

| Plane | Verbs |
|---|---|
| Log | `open` · `seal` · `commit` · `abort` |
| Branch | `fork` · `merge` |
| Transaction | `begin` · `get` · `set` · `commit` · `abort` |
| Derivation | `invalidate` · `schedule` · `run` · `settle` |
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
   1.3s and 0.28s at 32k dependents. Sets everywhere the index dedupes or removes. The same rule
   governs the round's own dedupes: a scan that only ever saw two candidates saw 128k the moment
   the buffer scan moved to the top of a range (§9.6).
8. **The dependency index is keyed on the trunk, never on a round's own branch.** A round branch is
   where events land on the way through; the dependency graph is a fact about the data, which lives
   on the trunk. Keyed on the round branch it would be discarded with the round, and an invocation
   whose merge was rejected would never be rediscovered — rediscovery is a lookup by branch and
   cell, and the edges would sit under a branch id nobody looks up. Partial application (§16.5) is
   only safe because those edges are already on the trunk when the merge decides.
9. **A round's applied subset is closed under the round's own dependencies.** Dropping an invocation
   drops everything in the same round that read what it wrote, transitively. Otherwise the round
   publishes a value derived from one that never landed, labelled with a watermark claiming exactly
   the replay that would not reproduce it (§10.1).

### 16.4 The scheduler is stateless

**There is no work queue.** Pending work is fully implied by the gap between a producer's watermark
and the branch head, plus the dependency index. The scheduler *derives* what to run by streaming the
layers in that gap, rather than materializing a list of invocations.

This is not merely an optimization. Without it, naive-eager is exactly the configuration that
explodes: one write to a widely-depended-on cell enqueues 100k invocations, and a def-mutation on a
large type enqueues millions. Coalescing across a *range* (§6.3) does not help here and was never
meant to — it reduces how many derived layers a backlog leaves, not how many invocations one layer
dirties. Three properties fall out of having no queue:

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

**The gap is closed in one round, not one round per layer** (§16.5). The gap is the statement of
what is pending; how it is divided is scheduling policy, and dividing it per source layer is the
policy that made a backlog run work its own guards were guaranteed to reject. Nothing about
statelessness changes: the range is recomputed from watermarks and head on every call, and a worker
killed mid-range leaves exactly the gap it started from.

Discovery stops at the first *source* layer on the branch that has not committed. A layer *is* the
changeset (§9.6) and means nothing before commit, so settling one would derive from writes that may
yet be abandoned, and stepping over one would advance every watermark past a layer nothing had
incorporated. Waiting is the only honest option, and it is available precisely because there is no
queue to stall. It also bounds the range, which is what keeps the round's fork point a snapshot: a
layer that committed below the fork point after the round forked would appear in its read path
halfway through. An **aborted** layer is not waited for — it will never commit — and a layer found
uncommitted when a store is reopened is aborted, because an open layer is exclusive to a process that
no longer exists (§6.2). A derived layer is skipped in whatever state it is in, so a round abandoned
by a panicking peer costs a layer id and nothing else.

**A round forking a branch of its own does not qualify it either** (§16.5). The fork is where a
round's output goes, not a record of what a round has left to do: nothing about pending work is
written to it, and a process killed mid-round leaves a branch holding derived layers no other branch
can see, plus the same watermark gap it started from. The next round rediscovers exactly the same
work and forks again. This is the same statelessness the pause switch and the parallelism bound leave
intact, applied to isolation instead of to scheduling.

### 16.5 A range settles as a transaction

A committed layer triggers producers; their output commits further layers, which trigger more.
**All of it carries the same `reflects`**, because it is all the consequence of one settling. A
producer's watermark advances to `L` only once that whole closure has settled — which is precisely
what makes the watermark's claim true: *replay the world at `L` and you get exactly this.*

**A round is a transaction like any other write** (§12). It forks the branch at the top of the range
it is settling, runs every producer on the fork, and merges when it settles:

```
fork at the top of [watermark+1 … head] → run producers → merge, guarded by what they read
```

#### The unit is a range, not a layer

A round settles **everything between the watermark and head**, not one source layer. The alternative
was tried and is the shape of two failures, neither of which is a cost question:

* **A backlog becomes a treadmill.** With `L10`, `L11` and `L12` committed before anything settles, a
  round per layer computes `L10` from the world at `L10` and is rejected at merge by its own guard,
  because `L11` moved its input while it ran; the `L11` round likewise; only `L12` lands. The guards
  are behaving correctly. The *schedule* guaranteed the work was stale before it ran, and under
  sustained backlog most derivation work is run and then rejected.
* **A producer whose input is only ever derived is never discovered.** `up2` reads what `up1` writes,
  and `up1` writes only into derived layers, which open no rounds. Its other route — §9.6's seeding
  for a producer that has never run — fires at the fork point of the *earliest* unsettled layer,
  because a brand-new producer drags the minimum watermark to the bottom of the log, and the world it
  wanted does not exist there. The same shape catches a pipeline pushed over data that is already
  derived.

Both close for the same reason: the opening wave is every layer in the range, derived layers
included, and the fork is at the top of the range, where the world is complete. A layer is skipped
for a producer that has already incorporated it — see §9.6 on a layer's *position* in the source
stream — which is what keeps a settled branch from re-deriving itself off its own merged output.

#### The fork is at the top layer; `reflects` is the top source layer

These are two different quantities and collapsing them is a bug in either direction.

`reflects` must be a **source** position, because that is what a watermark points into (§6.3) and
what every freshness comparison is against (§10.2). So it is the highest source layer in the range.

The **fork point** is the top of the range whatever authored it, which on a settled branch is a
derived layer that an earlier round merged. Forking at the top *source* layer instead would hide
exactly that output — a derived layer sits above the source layer it reflects, by construction — and
that is the residue a round per layer had: an invocation dirtied by `L'` that depends on a derived
cell only an earlier round produced would read it absent.

`reflects` stays true by construction all the same. Everything between it and the fork point is
derived output reflecting `reflects` or lower, which is part of the world `reflects` names rather
than something above it — so *replay the world at `reflects` and you get exactly this* is still what
the fork point can see, and nothing else is.

Only **source** layers bound a range, and only they are waited for (§16.4). Derived layers are
consequences: they are swept into the wave and they never stop or extend it.

#### The fork point is the filter

**The layer a producer reads at is not the layer it reflects**, and the branch boundary is what
expresses the difference. A producer's read path is:

```
[(round branch, its own head), (trunk, the top of the range)]
```

- It sees its siblings' output, because that output is on the round's own branch bounded at that
  branch's head. There is no high-water mark to maintain: *"the head of my branch"* already means
  *"my range plus everything this round has committed"*, which is exactly the world a downstream
  producer must see to consume its upstream's output.
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

**A round guards the records it read and the round did not write.** The subtraction is round-wide,
not per-invocation, and that is what lets a chain commit: within one round `invest` writes
`is_investible` and `tier` reads it, and `tier` must not fail on a cell its own round produced. §12.1
states the client's version of this rule as an *ordering* — a read before your own write is still
guarded — because taken as a set difference it deletes read-modify-write. A round has no order to
appeal to, and does not need one: everything it writes is derived, a derived cell is never in the
cell-touch index (§12.4), so no guard on one could ever have tripped; and a producer that reads a
cell it writes is a cycle (§16.6), not a compare-and-swap.

**The subtraction is over `CellAt`, and the guard it produces is over `CellRef`** (§16.3). The two
differ for exactly one kind of producer, and for that one it is the difference between a guard and no
guard: a **migration** reads `C@v1` and writes `C@v9`, which is the whole of what a migration is
(§9.3). Subtract by cell and its guard on the source record it migrated *from* disappears — and that
record is source data a client owns, so it is in the touch index and the guard was the only thing
standing between a stale migration round and a lost update. Subtract by record and the migration
guards `C` because it read `C@v1` and produced only `C@v9`, while `tier` still does not guard
`is_investible` because it read and the round produced the same record. The guard itself stays a
question about the cell, because a write at any version is a write.

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

The **backlog** that used to be the second residue here is not one any more: a round settles the
whole range, so there is no earlier round merging above a later round's fork point to be blind to.
What remains of it is a smaller thing, and it is stated in §6.3 rather than here — how many
intermediate derived snapshots a backlog leaves behind is now a property of the schedule, and nothing
can ask.

### 16.6 Cycle detection

A cycle is a producer that transitively depends on a field it writes. It cannot be detected
statically, and under a stateless scheduler it does not surface as re-entry — it **livelocks**: the
producer runs, advances its watermark, dirties its own input, and is rediscovered forever.

**v1 detection is a per-invocation re-run counter scoped to one round (§16.5).** If an invocation
runs more than `K` times while one round settles, it is cycling; the producer is marked broken (§14)
and its output cells report `state: 'broken'`.

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
| `ExecutionProvider` | in-process Rust, or a subprocess over stdio (§17.4) | container over a socket |
| `PoisonProvider` | in-memory for a server; a file beside the store for a process-per-command client (§14.2) | shared, with the rest of operational state |
| *(scheduling policy)* | not a trait yet — eager and stateless, hard-wired (§16.4) | `ProducerPolicyProvider`: prioritized, incremental, batched |
| *(error policy)* | not a trait yet — a producer is poisoned whole (§14) | `ErrorPolicyProvider`: partial / per-cell recovery |
| *(codegen)* | — (deferred, §15) | `CodegenProvider`: TypeScript, Python, Rust, Go |

The bottom three are named intent, not code. A seam is worth its abstraction once there is a second
implementation to hold, and for those three there is exactly one — so what exists is the policy
itself, in one place, with the name reserved for when a second arrives. Saying so is the point: a
table of providers that lists traits nobody wrote is a map a reader checks once and stops trusting.

The dependency index is designed **in-memory-primary** rather than as a disk structure with a cache.
This is a deliberate bet on the normalization thesis (§1): identity makes normalization free,
normalization keeps the working set small, and a small working set makes `validate` a handful of
memory lookups.

### 17.1 `StorageProvider` surface

Deliberately minimal, so that a plain KV store and Postgres remain equally viable:

```
// Reads. A ReadPath, never a branch — see below.
get_cell(path, cell, version) -> Landed?          // Landed = the event, plus where it landed here
cell_versions(path, cell) -> [DefVersion]         // which versions this cell is materialized at
scan_buffer(path, buffer) -> Iterator<Event>      // entity discovery (§9.6), and `list` (§17.5)

// Writing into an open layer.
author_event(cell, EventDraft) -> EventId         // streaming; never buffered whole
adopt_event(Event)                                // an event that already has an identity (§19.5)
include_event(EventId)                            // membership — what makes merge not copy
put_def(DefEvent)                                 // a layer holds values xor defs (§6.2)
seal / abort

// Reading the log back.
read_layer(layer) -> Iterator<Event>              // a layer's membership, in order
read_membership(layer) -> [EventId]               // the same, as identities — what merge wants
read_def_layer(layer) -> [DefEvent]
open_layer(branch, id) / commit_layer(sealed)
put_layer_meta(Layer) / read_layers() / put_branch(Branch) / read_branches()
rebuild_read_index()                              // the index is a projection, and this proves it

// Interned values.
intern(kind, bytes) -> Pid                        // content-addressed; no path, no layer
read_interned(pid) -> bytes?
```

**A writer names neither an id nor a layer** — `author_event` takes a *draft*, and `authored` is the
layer it is called on — which is what makes it impossible to author an event claiming to have been
written somewhere it was not. `adopt_event` is the one exception and is import's alone (§19.5): an
event id is *referenced*, by every membership row and every read-set in a stream, so a replay that
re-minted one would rewrite the lineage it was restoring. It takes a whole `Event` and a provider
**must refuse one whose `authored` is not the layer it is being replayed into**, which keeps the
property above intact rather than merely mostly intact — an event can only be replayed into the
place it says it came from.

**A read takes a `ReadPath`, not a branch.** The engine resolves ancestry (§7.2) and hands storage a
list of `(branch, bound)` segments to walk outward; storage never learns what a branch *is*, which is
what keeps branch semantics out of every backend. `open_layer` and the branch table take a `BranchId`
because they are the log's *shape* rather than a question about it — a layer belongs to exactly one
branch (§6.2), and that fact has to be durable.

The layer and branch tables are here for the same reason: they are the structure of the log, not a
projection of it. Everything else the engine holds — the dependency index, the cell-touch index,
watermarks, poisonings — is a cache rebuildable by replaying committed layers, so none of it appears
above.

**"Rebuildable" is a named seam, not a property nobody exercises.** Each of those caches is a
`Projection`: what it answers, how to fold one committed layer into it, and the **position** it has
folded through. Opening a store is *bring every projection to head*, and the cost of that is the
distance from each position to the head rather than the length of the log. Two lifecycles fall out,
and they are the same code:

- **Rebuilt from zero.** Positions start at `L0`, so opening folds every committed layer. This is
  what a process-per-command client does, and `O(log)` per invocation is the honest price of exiting
  between commands.
- **Maintained live.** A process that stays up folds each layer as it commits, so its projections are
  already at head and opening folds nothing. This is what lets a server hold a store open. Which
  layers a projection reads back is its own business: the touch index is folded from source layers at
  commit, while the dependency index and the watermarks are told what an invocation read and wrote by
  the engine that ran it, before the layer holding it commits (§16.3.8) — re-reading a derived layer
  to learn what the engine already said would put a scan of the largest layers in the system on the
  write path.

The two must answer identically, and that is a testable claim rather than a design intention: fold a
store from zero and compare, question for question, against the live-maintained set. Approximate
implementations of this seam — a snapshotted index, a probabilistic summary — are legitimate provided
their error is **one-sided** in the direction `CellTouchIndex::moved_since` already establishes: a
`true` may only mean *check properly*, never stand in for the answer.

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

**Framing is what a byte stream needs, and only a byte stream needs it.** A pipe and a unix socket
carry bytes, so something has to say where one message ends; a WebSocket carries messages already,
so over that transport the framing layer *disappears* rather than being wrapped — a codec picks the
frame kind instead, text for JSON and binary for MessagePack (§17.6). Wrapping the byte framing
inside a message transport would put two delimiters around one message and leave the inner one being
written by a sender that nobody parses it from. What is shared across all of it is the **encoding**:
one function turns a message into bytes and one turns bytes back, and every transport uses those two.

Two shapes were forced by targeting a shell worker first, and both are better than what they replaced:

- **Cells and values travel as text** — `"Company:o-1234abcd.website"`, `"9"`, `"@o-5678wxyz"`,
  `"~"`, `"acme.ai"` (§3.1, §3.4, §4.1) — the same forms the CLI accepts. A worker cannot reasonably
  assemble the structural JSON of a cell
  address, and a protocol only usable through a generated client library is one whose complexity is
  hidden rather than absent. Text also removes the `Int`/`Double` ambiguity a bare JSON number has.
- **Every message is a single-key object**, including the payload-free ones. A worker dispatches on
  one key without special cases.

**An `Invoke` names its producer as a string.** A `ProducerId` is a hash of the producer's name
(§9.2), so it uses the whole `u64` range, and JSON has no integers — read as a number it rounds to 53
bits and names a producer that does not exist. This is the same reasoning that makes the producer
table write ids as strings, applied to the one message that carries one. It cost nothing while every
worker implemented exactly one producer and ignored the field; a worker serving a whole repo has to
dispatch on it.

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
  "producers":  [ { "name": "invest",     "source": "Company", "fingerprint": "sha256:9f2c…" } ],
  "migrations": [ { "name": "founded_up" }, { "name": "founded_down" } ] }
```

Two optional keys sit beside those three. `"transport"` declares how the executable wants to be
spoken to once it is a worker — see *Two transports* above — and defaults to `"stdio"`. `"repo"`
states the repo id the executable believes it belongs to; the authoritative id is the one in
`borg.toml`, because a repo is a directory and one directory has one id however many executables it
holds, so this is a cross-check. An SDK that makes an author write the id in code as well should have
that copy verified rather than quietly ignored, and a repo that says nothing skips the check.

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

**A producer or migration spec may also carry a `"fingerprint"`**, which is what makes the diff able
to see a code change at all (§9.2). Everything else in this payload describes a *shape*, and editing a
pipeline's body changes no shape — so without it an edited pipeline diffs as unchanged and its old
output is never invalidated. The string is opaque and its only contract is that it changes when the
code changes; nothing ever compares one producer's with another's.

It is optional because a `jq -n` repo cannot reasonably digest itself: `borg repo push` falls back to
hashing the executable it just ran, which is a strictly better answer than making this a feature of
languages that ship an SDK. An SDK supplies its own only where it can cover *more* than that one file
— which is a per-language question, and each SDK states what it reaches and what it does not rather
than claiming a guarantee the other would not keep.

**`ProducerCtx` is async from day one**, even though the v1 in-process implementation only ever
returns ready futures. A socket-backed provider performs a round-trip per cell read, and retrofitting
async through the derivation engine afterwards is a far larger change than paying for it now.

#### Two transports, one protocol

A worker may be spoken to over **its own stdio** or over a **unix socket** the engine creates, one
per worker process, whose path arrives in `BORG_WORKER_SOCKET`. Same handshake, same messages, same
per-codec framing; only the descriptors differ.

Stdio is what a shell worker wants — `read` and `echo` and nothing else — and its cost is that the
worker's stdout carries the protocol, so anything printed for a human corrupts it. A shell author can
be told that once. It is not survivable in a real client library, where a stray `console.log` in a
pipeline, a dependency, or a runtime warning desynchronises the stream and surfaces far from its
cause. On the socket, the protocol has a descriptor of its own and **stdout is entirely the
author's**.

**The transport is declared, not detected.** It rides on `describe`, which is the one thing the
engine asks an executable before it must decide how to spawn it — and the decision has to be made
before the spawn, because by then stdout has been claimed. Detection was the obvious alternative and
it cannot work: the engine would have to tell "has not connected yet" from "printed to stdout first",
which is exactly the case the socket exists to make harmless. The detector would be broken by the
thing it was detecting. An absent declaration means stdio, so a worker written before transports
existed is untouched and no socket is created for it.

**A socket worker's stdout is pointed at the engine's stderr**, by duplicating the descriptor at
spawn time. Not inherited: the engine's own stdout is a contract too — `borg get --value` is parsed
by scripts — and handing a subprocess a pipe into it moves the corruption up one level rather than
removing it. Not discarded either, because a `console.log` nobody ever sees is its own kind of bug.
This is where the provider already sends a worker's stderr, and it costs one `dup` and no reader
thread.

`describe` itself stays a plain `argv[1] == "describe"` invocation printing JSON to stdout on both
transports. That call is one short-lived process whose entire output *is* the payload: there is no
stream to desynchronise, a corrupted one fails the push immediately with the offending text, and
leaving it alone is what keeps a repo of shell pipelines a single `jq -n`.
### 17.5 The client protocol

§17.4 is the engine talking to code it invoked. This is the reverse: **a client's transaction surface
over a socket instead of over argv**, which is what an SDK speaks and what `borg-server` answers.

It reuses §17.4's framing whole — same codecs, same per-codec framing, same **single-key object**
rule — because the two protocols differ in who is asking and not in how a message is carried. The
operations are the ones the CLI already has: `tx_begin`, `tx_get`, `tx_set`, `tx_create`,
`tx_commit`, `tx_abort`, `get`, `list`, `explain`, `branch_list`, `branch_head`, `def_show`,
`repo_push`, `export`. That is a constraint rather than a coincidence: the CLI is the testbed for
what a client is like to use, so a protocol needing an operation the CLI lacks would be evidence
about the CLI. The three that are not lifted subcommands are `registries`, `registry_create` and
`import`, and none of them is about a store at all — they are about the server hosting it (§17.6),
and `import` in particular *creates* the registry it names, so there is no store for it to have been
lifted onto.

Two operations arrived with the first real application rather than with the CLI, and both are on the
CLI too, because the rule above holds in that direction as well: an operation a client needs is one
the CLI should have.

**`list` names every object of one struct, as ids.** It is the enumeration §9.6 excluded, bounded by
the reason for the exclusion — one struct, at head, unfiltered, unpaged, ids only, and outside any
transaction, because an enumeration is not a guardable read (§9.6). Ids only is a real cost: reading
a field of each object is a read per object. That is the N+1 an ORM has, and it is left visible
rather than hidden behind a reply that would have to grow a query language to keep being useful.

**`tx_create` allocates an object and writes its existence cell, in the transaction, in one step.**
Before it, every write named an object whose id the client already had, so an application had to
invent ids and two of them would eventually invent the same one. The server allocates under an
`AllocatorId` of its own (§3.1) — *not* the one the `Company#1` shorthand names — so ids an
application creates and ids a person types can never collide, whichever came first. The two halves
are one message because an id nothing wrote is not an object, and because splitting them would make
an allocation something a client could lose by disconnecting in between. Creation reads nothing and
writes one cell, so two transactions creating objects can never conflict.

One message is not a lifted subcommand, and it is worth saying why. **`def_view` returns a branch's
whole def view and the def-version it was read at**, which is what codegen reads (§15). Neither half
could be composed from the rest: `def_show` answers about a struct you can already name, and a
generator's whole job is to not know the names in advance; and the version a generated module stamps
itself with is the branch's *def*-version, which is not `branch_head` — head moves on every data
write and a def-version moves only on a def push (§5.3). They travel together in one message because
they are one read: taken separately, a generator could be handed a schema and a version from either
side of a push.

**Everything travels as canonical text**, layer ids included — `"L120"`, the form §10.4's envelope is
printed in. A `by` is a `ProducerId`, which is a hash spanning the whole `u64` range, and JSON has no
integers; the same rounding that corrupts a producer id in a sidecar file would corrupt it here.

**A read answers with the §10.4 envelope, never a bare value.** A transaction is named by its id on
every message, and by nothing else: transaction state lives beside the store (§12.2), so a handle
outlives the connection that opened it. A client that disconnects mid-transaction has abandoned it,
not destroyed it — it may reconnect and carry on, and if it does not, §12.3's idle reaper collects it
like any other silence. This is what makes a browser tab closing an ordinary event.

**A commit answers with a conflict or a landing, and a conflict names the cell.** "Your commit was
rejected" is not something a client can act on; "the cell you read moved, and here it is" is the
input to deciding whether to retry (§13).

One message is a **write the client does not perform**. `repo_push` names a repo and the *server*
runs the push against it: the definitions land through the registry the server is holding open, and
the producer implementations it resolves are the server's, because a push reads code off a
filesystem and the filesystem it reads is the one the code will run on. §17.6 has the rest, including
why this is local-only today and what the remote form is.

The handshake carries the client's **ClientVersion** — the def-layer its generated code was built
from (§5.4) — so that old generated clients keep reading through `down` migrations, and so the engine
can eventually name which live clients a def push would break (§5.5). Absent means the branch's
current def-version, which is what an un-generated client honestly is. It also carries the
**registry** the connection is for and a **credential** (§17.6).

#### The handshake is answered

Three messages, in order: the server's hello, the client's hello, and the server's **acknowledgement
of it**. The third is protocol version 2, and it exists because its absence cost three things at
once. *Accepted* and *not answered yet* were the same observation, so a client could not know it was
connected except by asking something. A refusal was written and hung up on immediately, so a client
that wrote its first request without waiting met a reset — and a reset discards the receive buffer,
taking with it the very answer it was racing. And a routing failure had nowhere to be delivered, so
it was deferred to the first request that needed a registry, which meant an SDK's context resolved
happily against a store that does not exist.

An accepted handshake is answered with the negotiated **codec**, the protocol **version** and the
**server's** own version, and the **registry the connection settled on**. That last is confirmation
for a client that named one and news for a client that did not: a server hosting exactly one
registry resolves it and says which.

A refusal names the reason and is followed by a **lingering close** — the server stops writing,
drains whatever the client already sent, and only then lets go — which is what makes the answer
arrive rather than race.

**Routing is decided here, and the two outcomes are not symmetric.** A hello that *names* a registry
has made a claim, and a claim the server cannot honour is refused at the handshake, with the hosted
registries named so that a client need not reconnect to discover them. A hello that names *nothing*
has made no claim that could be wrong: against a server hosting exactly one registry it settles on
that one, and against a server hosting none or several it is still accepted and settles on nothing —
because that is precisely the connection an administrative client makes, and `registries` is the one
message that needs no store. The ambiguity is then reported by the first request that does need one,
naming the options.

**The handshake is JSON throughout**, whatever the body will be. A codec that has not been agreed
cannot carry the message agreeing it, and that stays true of the acknowledgement: a client reading
the reply that names the negotiated codec cannot already be decoding in it.

A version the server does not speak is refused by name, and **version 1 in particular**: a version-1
client writes its first request without waiting, so an acknowledgement would land where it expects a
response and every answer after it would be one message out — silently, which is the failure a
version number exists to turn into a sentence. That refusal is deliverable precisely because of the
acknowledgement it is refusing over, which is what makes this a clean break rather than a flag day.
The worker protocol (§17.4) is a separate contract over the same framing and did not move.

**One process serves a store.** The transaction table, the producer table and the pause flags are
files beside the store, and the sequencer is in-process; none of that is multi-process safe, and it
was not before a server existed either. A server therefore takes an advisory lock on every store it
hosts and every other invocation against one of them is refused and told the socket to speak to. The
lock's liveness test is the socket itself — a record whose socket does not answer is stale and is
cleared — because a lock that can outlive its holder is worse than no lock. This is v1 honesty and
not the destination: the destination is the CLI connecting to the socket rather than being turned
away by it.

**A server opens a store once and holds it**, and the lock above is what makes that sound rather
than merely convenient. A registry's indexes are projections of the log (§17.1); holding them across
requests is safe exactly when every mutation of the store flows through the instance maintaining
them, which is what "one process serves a store" already guarantees. Holding the registry also means
holding the `ExecutionProvider` on it: derivation and the read path must share one registry, because
two live over one store is precisely what the single-process assumption forbids — a server that
opened one per operation could never chase a write without dropping the registry it was answering
through. Opening per request instead costs an `O(log)` replay *per read*, which is the multiplication
that made the first application on this system unusable at a hundred and forty objects.

Requests are still answered one at a time, **per registry**. That is a separate decision from the one
above: process-per-command gave a client serialisation for free, a server has to choose it, and
choosing it is what keeps a served store no less correct than an unserved one. It is per registry
rather than per server because what it protects — the files beside a store, and that store's
sequencer — is per store; two clients on two registries share none of it.

### 17.6 A server hosts a directory of registries

§17.5 is what a client says. This is what answers it: **one process, one socket, many registries**,
where a registry is a store and everything beside it. `borg-server start --data-dir <dir>` hosts
every store under `<dir>`, addressable by the name of its directory.

This is the local instance of a hosted platform rather than a smaller different thing. What a
multi-tenant deployment adds is a credential that means something and a place other than a directory
to keep the registries; what it does not add is a routing concept, a second protocol, or a second
server.

**The registry is the unit of tenancy.** Not the connection, not the branch, and not the process. A
branch is a fork of one history and shares its definitions, its transaction table and its PID
counter with what it forked from (§7, §12.2), so two applications that must not see each other's
schema need two registries and not two branches. Everything a registry owns is already
registry-shaped — the log, the sidecars, the advisory lock — which is what makes the multi-tenant
case the same code as the single one.

**The handshake routes; the messages do not.** `ClientHello` carries `registry`, settled once per
connection. Absent means the server's sole registry *when it has exactly one* — which is what keeps a
one-registry server the thing a local developer already expects, with no name anywhere — and is an
error naming the options when it hosts more than one, because at n≥2 any default would be a guess
over somebody else's data. The messages that may name another registry are `repo_push` and `export`,
so that a deploy client pushing to several and a backup client covering several do not need a
connection each; and the messages that need no registry at all are `registries`, `registry_create`
and `import` — the first is what lets a client that guessed wrong discover what to name, and the
other two make a registry that does not exist yet.

`ClientHello` also carries a **`credential`**, and nothing checks it. Its existence is the point: a
local server has no one to authenticate, since the socket's file permissions are the boundary, but
adding the field once authentication exists would mean moving the wire at exactly the moment there
is a deployment that cannot take a wire change.

#### Two transports, one protocol

A server listens on a **unix socket always**, and on a **WebSocket where it is told to**:
`borg-server start --listen ws://0.0.0.0:7717`, repeatable, and beside the socket rather than
instead of it. The local transport is what every `borg` invocation speaks and what the advisory
lock's liveness test *is*, so a server that could be told to stop speaking it would be one a
developer could lock themselves out of.

The listen addresses may also come from **`BORG_LISTEN`**, comma- or space-separated, and that is
the only flag with an environment twin. A container is configured by environment and its command is
baked into the image, so a port that could only be a flag would make changing it a rebuild; the
address is deployment configuration and nothing in the protocol is a function of it. `--listen` on
the command line wins outright rather than merging, because two half-specified sources are how an
operator ends up listening somewhere they did not ask for.

**The WebSocket is one framing, not one protocol more.** The messages, the codecs, the handshake and
the single-key rule are §17.5's, unchanged; what differs is that a WebSocket is message-framed
already, so §17.4's newline and length prefix disappear and a codec picks the frame kind instead —
JSON in text frames, MessagePack in binary ones. That is what the two frame kinds are for: a
browser's `event.data` is a string for the first and a blob for the second with no configuration,
and anything between the two ends that prints a text frame prints the same line a shell client would
have written.

It exists because a browser cannot open a unix socket, and because a WebSocket rides infrastructure
that already exists — a load balancer, a reverse proxy, an ingress — with no special configuration.

**TLS is terminated in front, and the server trusts no header.** `borg-server` speaks plaintext
`ws://`; a deployment puts a proxy in front of it and forwards to that port, which is what every
other component of such a deployment already expects. `wss://` as a *listen* address is refused by
name rather than quietly served in plaintext, because an operator who believed the wire was
encrypted and was wrong is the worst available outcome. Nothing in §17.5 is a function of the
client's address or scheme, so no forwarded header is read: not `X-Forwarded-For`, not
`X-Forwarded-Proto`, not `X-Real-IP`. Trusting one would introduce a spoofable identity in order to
answer a question nobody asks. When authentication arrives it arrives in `ClientHello`'s
`credential`, which is reserved for it and travels on a channel a proxy does not write.

**One HTTP endpoint, on the WebSocket's port: `GET /health`.** It answers `200` with the server's
version and how many registries it hosts. A WebSocket *is* an upgraded HTTP request, so a listener
that speaks one is already parsing the other, and refusing to answer a health probe on the port it
is already listening on would make a supervisor open a second one. It reports the registry **count**
and not their names, because it is unauthenticated and a registry name is tenancy; `registries` on
§17.5 is where the names live, behind a handshake that will one day carry a credential. Everything
else on that port is `404` — a second endpoint would be an API beside the API, and the API is §17.5.

**Registries open lazily and lock eagerly.** Opening one brings its projections to head, which for a
fresh set is a replay of its log (§17.1), so a server that opened everything at boot would pay every
registry's history to answer a request about one of them. Taking the advisory lock costs a file
write, and not taking it leaves a window in which another process may walk into a store this server
is about to hold. They are not symmetric and are not done at the same time.

**Creating a registry is a server operation.** A directory appearing under a running server's data
directory is a store it has not locked, is not hosting and will not route to, so `registry_create`
is on the protocol and `borg-server create` uses it whenever a server is up. It also creates one
directly when no server is running, because a data directory has to be fillable before there is a
server to fill it.

**`repo_push` is a write the server performs.** A repo is a directory of code, and pushing it moves
two things: definitions, which travel the log, and implementations, which are a sidecar recording
which file each producer is (§9.2). A client cannot do the second — it is not the machine the code
runs on — and a second *process* doing either is the second writer the advisory lock refuses. So the
server does it: the definitions land through the registry it is holding open, and the worker pool it
built from the old table is reloaded before the branch is caught up, since the catch-up is what runs
the new code. This is what retires *"pushing a schema to a served store means stopping the server"*,
and it is only safe because a push is idempotent and code-change-aware (§9.2): a push that
recomputed every source buffer whether or not anything moved would be a thing nobody could run
against a live server.

The `path` in a `repo_push` is a path **on the server**, and that is part of the contract rather than
an implementation detail. For a local server it is the directory the developer is editing, which is
exactly what they mean. For a remote one it means nothing, and the answer there is an uploaded
artifact — a further field on the same message, not a different message, which is why `path` is
optional and why the payload is expected to grow rather than to be replaced.

### 17.7 How a client is addressed

§17.6 is what a server is. This is how a client is told which one, and which registry on it: **one
string, the way a `DATABASE_URL` is one string.**

```text
borg://localhost/personal-crm               the well-known local address, registry personal-crm
borg://localhost                            the well-known local address, no registry named
borg+unix:///run/user/1000/borg.sock/crm    an explicit socket, registry crm
borg+unix:///tmp/borg.sock                  an explicit socket, no registry named
borg+ws://borg.example:7717/crm             a websocket, registry crm
borg+wss://borg.example/crm                 the same, through a TLS-terminating proxy
```

Everything a client needs to reach a store is *where the server is* and *which registry on it*, and
those two are one fact. Carried separately — a socket variable and a registry variable, a flag and
an environment variable — they can be changed independently, which is how a client ends up pointed
at one deployment's socket with another deployment's registry name. A URL is the shape that cannot
come apart, and it is the shape every deployment system already knows how to carry.

**The scheme names the transport.** `borg://` is *the* local transport, whatever that is; today it
resolves to the well-known address §17.6 defines, and a client that wrote `borg://localhost/crm`
keeps working if that address ever moves. `borg+unix://` says the address out loud, for a scenario,
a second server, or a container mount.

`borg+ws://` is the transport a browser can open (§17.6). It was **reserved and refused by name** for
two milestones before it existed, so that everyone who needed one would not first invent a different
spelling and leave the wire unable to take the real one; the spelling that arrived is the spelling
that was reserved, and the cost of that foresight was one match arm. It is a host and a port, and the
port defaults the way `ws://`'s does — 80 plain, 443 secure — because the argument for a WebSocket is
that it rides infrastructure that already exists and that infrastructure listens on those two.

**`borg+wss://` parses everywhere and is dialled where TLS is free.** A browser or a node process
gets TLS from the runtime's own WebSocket at no cost. A Rust client would have to carry a certificate
store to speak it, for a deployment shape whose whole premise is that a proxy has already terminated
— so `borg` refuses `borg+wss://` at the dial, by name, saying to point it at the `ws://` the proxy
forwards to. The asymmetry is per language and is stated rather than hidden; refusing the scheme
outright would make the deployed address unsayable.

**An absent registry is absent.** The parser does not default it, and neither does any client. §17.6
already says what an absent registry means — the sole registry at n=1, an error naming the options
at n≥2 — and a client that filled in a guess would be re-implementing half of that rule and
disagreeing with the other half. So an absent registry travels absent in `ClientHello`.

**For `borg+unix://`, the last path segment is the registry when it could be one.** The path holds
both halves, so something has to divide them, and the divider is the rule §17.6 already enforces on
registry names: letters, digits, `-` and `_`. `borg+unix:///tmp/borg.sock` is therefore all socket,
because `borg.sock` has a dot in it and no registry may. A trailing slash always means "no
registry", which is what makes both readings of a genuinely ambiguous path sayable:
`borg+unix:///run/borg/crm` is the socket `/run/borg` and the registry `crm`, and
`borg+unix:///run/borg/crm/` is the socket `/run/borg/crm`.

**Nothing listening is not an errno.** `ECONNREFUSED` and `ENOENT` on a borg address mean one thing,
and every client reports it in the same words:

```text
no borg server at /run/user/1000/borg.sock — start one with: borg-server start
```

Anything else — a permission error, a path that is not a socket — is reported as itself, because
then the error *is* the news.

**A connection outlives a socket, over either transport.** A client SDK reconnects: a failed send or
read tears the connection down, and the next operation dials again and repeats the handshake —
including the registry, which is what a connection settles rather than a message. Everything in this
paragraph is a property of the *connection* and none of it is a property of the transport, so it
holds identically over a unix socket and over a WebSocket; a guarantee that held over one and not
the other would be a guarantee with a footnote. Operations that were in flight
when it broke **fail, and are never retried**, because `tx_commit` and `tx_create` are not
idempotent: a commit that reached the server and lost its answer on the way back is
indistinguishable, from the client, from one that never arrived. What survives is the transaction
itself, and by construction rather than by machinery — a transaction is an id beside the store
(§12.2), so one begun before a server restart commits after it.

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
- Transactions as the only write path; object transactions with automatic, source-only guards
- Server-side object allocation, under an allocator of its own; enumeration of one struct's objects
  as ids (§9.6, §17.5) — the query layer around it is still out
- Rounds as transactions: settle a range, fork, apply partially, merge one layer per producer
- Cell-touch index over source layers
- Merge with guard-based conflict detection, naming events rather than copying them
- Producer poisoning as durable operational state, expiring on the ClientVersion that recorded it
- Distribution seams (§17.2) behind traits, naive in-process implementations
- Serializable command/response form for every engine operation
- Export and import: a registry as a canonical event stream, and the version promise it carries
  (§19)

**Out:**

- Containerization / untrusted execution — full trust, in-process
- Sinks
- `Set`, `Map`
- Aggregation pipelines
- Mid-list insertion
- ~~**All generated SDKs** (§15) — they arrive with the network layer~~ — and they did: `borg-server`
  (§17.5) is that layer, and `borg generate --lang ts` is the generator. TypeScript only; Python,
  Rust and Go remain out.
- ~~Network / server layer — v1 is a library exercised by Rust tests~~ — §17.5, §17.6. One process
  serving a directory of registries, on a unix socket **and on a WebSocket** where it is told to
  listen on one, with `GET /health` beside it. Actual distribution is still out, and so is anything
  the `credential` in the handshake would be checked against.
- **TLS.** The server speaks plaintext and expects a proxy in front of it to terminate (§17.6). No
  TLS backend is compiled into either binary, which is what makes that a property rather than a
  policy; `borg+wss://` is therefore a client-side address a browser dials and the Rust client
  refuses by name.
- A query layer. `list` (§17.5) answers ids of one struct at head and nothing else: no filter, no
  ordering, no paging, no projection, no join, and no way to guard one — see §9.6 for the boundary
  and why it is where it is.
- Actual distribution — only the seams (§17.2)
- `O(1)` merge — a parent layer referencing a child's event set rather than enumerating it (§13)
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

---

## 19. Export and Import

**What Borg guarantees across versions is the data, not the bytes.** Before 1.0 an on-disk format
may change: a storage backend may re-shape its tables, a projection may be re-keyed, a sidecar may
be re-spelled. What may not change is a customer's ability to get their data out of one release and
into the next. So every release **exports a registry as a canonical event stream and imports streams
written by prior releases**, and an upgrade is `export → upgrade → import`.

That is one mechanism doing four jobs, and it is worth naming them because they were never four
problems: **backup**, **restore**, **format migration**, and **clone/seed**.

Borg is unusually well placed to keep this promise, for a reason the rest of this document has been
building toward: **the log is the data.** Every index — the read index (§17.1), the dependency index
(§16.3), the cell-touch index (§12.4), the watermarks (§10.3) — is a projection, and each is proven
rebuildable from the log by a test rather than by a claim. So an export is *"walk the log and write
it down"*, and an import is a **replay** — the same operation `Registry::open` performs from the
other end.

### 19.1 What the stream carries

Everything needed to reconstruct a registry exactly, and **nothing that is a projection**.

| In the stream | Why |
|---|---|
| Branches — id, name, origin layer | The structure of the log, not a fold over it (§17.1) |
| Layers — id, branch, kind, author, `reflects`, parent, guards; **committed only** | Likewise. An open or sealed layer never became visible (§6.2) and is not part of what the registry is |
| Events — id, cell, value, def-version, origin, derivation with its **read-set**, `authored` | §4.3. A read-set is a recorded fact about one invocation, not something a replay could recompute without re-running the producer |
| Layer membership, in order | §6.2. An event may be named by layers on two branches; that is what a merge is (§13) |
| Def events, per def layer, in order | §6.1 |
| Interned content — the bytes behind every content PID in use | §3.1 |
| The PID counter (`allocations`) | §3.1, and see below |
| The producer implementation table | §9.2 |
| Pause flags and poisonings | §9.6, §14 |

Not in the stream: the read index, the dependency index, the cell-touch index, watermarks, and every
other projection. Importing them would be importing an answer the log already contains, and would
create the possibility of a restore whose indexes disagree with its log.

### 19.2 Which sidecars are state, and which are residue

The files beside a store are not log data (§14.2), so each was decided separately. The rule:
**a sidecar is exported when losing it would change an answer the restored registry gives, and
skipped when it only describes a process that is over.**

- **The PID counter — exported.** It is the one sidecar a store cannot recover from: lose it and the
  count restarts, so a fresh object can be issued the identity of an existing one. This stream is
  its backup story.
- **The producer implementation table — exported.** Definitions travel the log and implementations
  do not (§9.2), so a restore without the table is a registry holding producer definitions it cannot
  run. The commands in it are paths on the exporting machine and are written back verbatim; a
  restore onto a different machine repairs them with one `repo push`, which is what put them there.
- **Pause flags — exported.** A branch somebody paused on purpose that came back deriving would
  resume work they had deliberately stopped.
- **Poisonings — exported.** This is the decision that could have gone the other way. A poisoning is
  the engine's judgement about code (§14) and looks like operational residue; it is not, because a
  poisoned producer's cells read `broken` rather than `stale` and `explain` reports the error. Drop
  the table and those exact reads change — `broken` becomes `stale`, which is the promise of a
  catch-up that is not coming, precisely the lie §14 exists to prevent — and the first derive after
  a restore re-runs known-broken code to rediscover what was already known. The record is keyed on
  the ClientVersion it was recorded against, so it still self-expires when fixed code is pushed:
  exporting it changes nothing about recovery and everything about whether a restore answers the
  same questions the same way.
- **Open transactions — skipped.** Ephemeral by decree (§12.3). A transaction is reaped on silence,
  and restore is create-then-import, so no client can hold a handle to a registry that did not exist
  a moment ago. The transaction *timeout* is exported, because that is a knob somebody set rather
  than a transaction somebody opened.
- **The advisory lock — skipped.** It is not state at all; it is a live claim naming the socket of a
  process that is not this one (§17.5).

### 19.3 The format

**Versioned, streaming, line-oriented JSON.** One record per line; the first line is a header.

```text
{"header":{"version":1,"binary":"borg 0.1.0"}}
{"allocations":{"next":42}}
{"tx_timeout":{"seconds":86400}}
{"producer":{"id":"12342029420047889112","name":"invest","source":"Company",
             "command":"/srv/repo/pipelines/is_investible.sh","transport":"stdio"}}
{"poison":{"branch":1,"producer":"1234…","version":9,"error":"…","since":12}}
{"paused":{"branch":4}}
{"branch":{"id":1,"name":"main","origin":null}}
{"layer":{"id":1,"branch":1,"kind":"def","author":"source","parent":null,"guards":[]}}
{"def":{"event":{"DeclareField":{"struct_name":"Company","field":"website","ty":"String",…}}}}
{"layer":{"id":3,"branch":1,"kind":"value","author":"source","parent":1,"guards":[]}}
{"content":{"pid":"s-d83eg…","text":"acme.ai"}}
{"event":{"id":2,"cell":"Company:o-04002.website","value":{"ref":"s-d83eg…"},
          "version":1,"origin":"source"}}
{"member":{"event":1}}
```

- **Line-oriented** because a registry may be huge and must never be materialized whole — the same
  discipline §6.2 and §17.1 impose on the storage layer. It streams, it diffs against yesterday's
  backup, and `grep` works on it.
- **JSON** because every other persisted format here already is: def events, layer metadata and cell
  values are stored as their serde encodings, so relaying them costs no second conversion table.
- **Every record is a single-key object**, payload-free ones written `{}` rather than as bare
  strings — the same rule §17.4 imposes, so that `jq 'keys[0]'` can dispatch.
- **A producer id is a string; layer, branch and event ids are numbers.** The same split §9.2's
  implementation table makes, for the same reason: a producer id is a hash using the whole `u64`
  range and every JSON tool silently rounds one above 2⁵³, while the others are sequential counters
  that `jq` should be able to sort.
- **Cells are canonical text** — `Company:o-1234abcd.website` — the one spelling §4 gives them, so a
  cell in a backup is a cell that can be pasted into a client.
- **Content is addressed by the PID that is its hash.** Import re-interns the bytes and requires the
  PID it computes to be the one the line claimed, so content addressing is the integrity check and
  no checksum is bolted on beside it.

**Order is part of the format.** A `layer` record opens a block and everything until the next one
belongs to it, so `event`, `member` and `def` records carry no layer of their own — repeating it on
each of a merge layer's million membership rows would pay real bytes for a fact the position already
carries. Layers are emitted in ascending id, which is what makes a stream importable in **one pass**:
a layer only ever names events authored at or below its own id, so nothing refers forwards.

**Identical registries export byte-identically.** That determinism is testable and is the cheapest
total check available: export, import, export again, compare. It is why the header carries the
stream version and the producing binary's version and *nothing else* — no timestamp, no registry
name, no path. Where a copy came from and when are facts about the copy rather than about the data,
and a filename and an `ls -l` already carry both. Everything with no natural order is sorted.

### 19.4 What an export represents

**The whole log, at the instant the export took the registry.** There is no snapshot machinery,
because exclusion already exists: embedded `borg` is refused against a served store (§17.5) and is
one process besides, and a served export runs under that registry's own gate (§17.6), which
serialises it against every other request. Nothing can commit while it walks. The cost is the honest
one — a large export holds its registry for its duration.

**It is deliberately not a settled read.** §10.5's settled frontier answers *where can a coherent
snapshot be read*, which is a question about derived data lagging source data. Bounding an export
there would silently drop every source layer above the watermark: data loss, dressed as coherence. A
backlog is part of what a registry is, so an export captures the lag faithfully — watermarks and all
— and the restore works the same backlog off. The settled position is *reported* by an export so
that an operator can see what state they captured, and is used for nothing.

### 19.5 Import creates; it does not merge

Importing into a registry that already holds anything is **refused**. Restore is
*create-then-import*, and the alternative is a decision nobody can make correctly: the stream names
layer, branch and event ids, so merging two id spaces means either re-minting — which invalidates
every read-set and every `reflects` in the stream — or colliding with them. Refusing is the only
answer that cannot silently corrupt.

**Ids are preserved, not re-minted**, for the same reason: they are part of the data. An import
therefore writes through `StorageProvider` directly rather than through a `Registry` — it is
replaying a log, not re-executing one. Nothing is validated against definitions (the events were
validated when they were authored, under the def-view in force then), nothing is derived, and
nothing is re-sequenced. A provider must be able to adopt an event that already has an identity, and
must refuse one claiming to have been authored in a layer other than the one it is being replayed
into, so that §17.1's *"a writer cannot claim to have written somewhere it did not"* survives intact.

A malformed or truncated stream fails with the **line number** and what was expected. A stream whose
format version this binary does not know fails naming **both** versions, so a reader knows which end
to fix. A restore that cannot finish removes the store it created, because a half-written store is
worse than no store: under a data directory it is a registry the next server start would host.

### 19.6 The surface

```text
borg export [<file>]                  write --store's registry out; no file, or `-`, is stdout
borg import <file>                    restore into --store, which must hold nothing yet; `-` is stdin
borg-server export [<name>] <file>    …through a running server, so a live registry needs no downtime
borg-server import <name> <file>      …creating the registry it names
```

`borg` addresses a store by path, because that is what embedded Borg is (§17.5). `borg-server`
addresses a registry by name under a data directory (§17.6), and — like `create` — does it through
the server when one is running and directly when one is not. When a server does it, `<file>` is a
path **on the server's machine**, exactly as `repo push --url`'s directory is (§17.6): a response
carrying the stream would be the buffering this format exists to avoid, and a multi-message reply
would be a change of shape rather than a field. The remote form is an uploaded artifact, and that is
a field on the message when it arrives.

A registry restored through a server is created and filled in **one** operation, because the two
halves apart leave a window in which the server hosts an empty registry that clients can route to
and write into.
