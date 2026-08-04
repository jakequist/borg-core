//! Cells — the universal addressable unit. SPEC.md §4.
//!
//! Every mechanism in Borg is field-granular: transaction guards, producer dependencies, field
//! ownership, migration staleness, and merge conflict resolution all key on the same primitive. So
//! the physical unit of storage is the cell, not the object.

use crate::ids::{ClientVersion, EventId, LayerId, ProducerId};
use crate::pid::Pid;
use crate::value::{ObjectTypeName, Value};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

/// A field name. `Arc`-shared because field names repeat across every cell of a type and are
/// compared constantly during dependency-index lookups.
pub type FieldName = Arc<str>;

/// Identifies a buffer — a partition of cells. SPEC.md §4.2.
///
/// **One buffer per def.** Values with no def — untyped ones — get exactly one buffer each.
/// Partitioning this finely is what makes horizontal scaling possible later, and it matches the real
/// access pattern: producers read specific fields and hop, rather than materializing whole objects.
///
/// This is a *logical* partition key, not a placement decision. A placement policy is free to
/// co-locate all of a struct's field buffers on one node; keeping the two separate preserves that
/// option.
///
/// **The interning buffers are deliberately absent.** §4.2 calls `StringBuffer`, `BinaryBuffer` and
/// `BigIntBuffer` buffers, but they hold *values, not cells*: an interned value has no version, no
/// origin and no authoring layer, so an [`Event`]'s fields are all meaningless for it and there is
/// no cell for a `BufferId` to partition. They are reached through `intern` / `read_interned`
/// (§17.1), which need no buffer argument because a PID already carries its kind (§3.1). Naming
/// them here would promise a cell partition that cannot exist.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum BufferId {
    /// One per `ObjectDef`. Holds existence cells — and so *is* the set of instances of one struct,
    /// which is what a producer maps over (SPEC.md §9.2).
    Object(ObjectTypeName),
    /// One per `FieldDef`. Holds the cells of exactly one field.
    ObjectProp(ObjectTypeName, FieldName),
    /// One per `ListDef`.
    List(ObjectTypeName),
    ListElem(ObjectTypeName),
    /// Singular because untyped values have no def to partition by. Unlike the interning buffers,
    /// these are genuine cell partitions: an `Any` container is mutable, so its contents are cells
    /// with versions and origins like any other.
    AnyObject,
    AnyArray,
}

/// Locates a cell within its buffer.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum CellKey {
    /// An object property, or an object's existence. The buffer already names the field, so the PID
    /// is the whole key.
    Pid(Pid),
    /// A list element. Lists are append-only in v1, so indices are stable (SPEC.md §4.4).
    Elem(Pid, u64),
}

impl CellKey {
    pub const fn pid(&self) -> &Pid {
        match self {
            CellKey::Pid(pid) | CellKey::Elem(pid, _) => pid,
        }
    }
}

/// Addresses one cell.
///
/// **The buffer is part of the address, not derived from it.** A sharded store must be able to route
/// a request from the cell address alone; were the shard key to require a schema lookup first, every
/// read would need the defs before it could be sent anywhere (SPEC.md §17.2).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CellRef {
    pub buffer: BufferId,
    pub key: CellKey,
}

impl CellRef {
    pub fn prop(struct_name: ObjectTypeName, field: FieldName, pid: Pid) -> Self {
        Self {
            buffer: BufferId::ObjectProp(struct_name, field),
            key: CellKey::Pid(pid),
        }
    }

    /// An object's existence. A cell like any other — it has to be, or it could not appear in a
    /// read-set, and then deletion could not invalidate anything (SPEC.md §8.1).
    pub fn existence(struct_name: ObjectTypeName, pid: Pid) -> Self {
        Self {
            buffer: BufferId::Object(struct_name),
            key: CellKey::Pid(pid),
        }
    }

    /// A list's own cell. **Its value is the list's length**, so appending changes it — which is
    /// what makes iterating a list a tracked dependency rather than an invisible one.
    ///
    /// The container cell holds whatever is true of the container as a whole: for an object, that it
    /// exists; for a list, how far it extends.
    pub fn list(element: ObjectTypeName, pid: Pid) -> Self {
        Self {
            buffer: BufferId::List(element),
            key: CellKey::Pid(pid),
        }
    }

    pub fn elem(list_def: ObjectTypeName, pid: Pid, index: u64) -> Self {
        Self {
            buffer: BufferId::ListElem(list_def),
            key: CellKey::Elem(pid, index),
        }
    }

    pub const fn pid(&self) -> &Pid {
        self.key.pid()
    }

    /// The existence cell of the object this cell belongs to. Returns `self` when this *is* an
    /// existence cell, or when the cell belongs to something with no existence cell of its own.
    pub fn existence_of(cell: &Self) -> Self {
        match &cell.buffer {
            BufferId::ObjectProp(struct_name, _) => {
                Self::existence(struct_name.clone(), *cell.pid())
            }
            _ => cell.clone(),
        }
    }
}

impl BufferId {
    /// The name this buffer wears in a cell address, and whether that name takes `[]`.
    ///
    /// The untyped buffers have no def and therefore no name of their own, so they borrow `Any`.
    /// Nothing is lost: their PID kind (`j`, `y`) is what tells them apart on the way back in, which
    /// is the same rule §4.2 relies on to dispatch without a schema lookup.
    pub(crate) fn address(&self) -> (&str, bool) {
        match self {
            BufferId::Object(name) | BufferId::ObjectProp(name, _) => (name, false),
            BufferId::List(name) | BufferId::ListElem(name) => (name, true),
            BufferId::AnyObject => ("Any", false),
            BufferId::AnyArray => ("Any", true),
        }
    }
}

impl fmt::Debug for BufferId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BufferId::Object(name) => write!(f, "{name}"),
            BufferId::ObjectProp(name, field) => write!(f, "{name}.{field}"),
            BufferId::List(name) => write!(f, "List<{name}>"),
            BufferId::ListElem(name) => write!(f, "List<{name}>[]"),
            BufferId::AnyObject => f.write_str("AnyObject"),
            BufferId::AnyArray => f.write_str("AnyArray"),
        }
    }
}

impl fmt::Debug for CellRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// The canonical text form of a cell address.
///
/// ```text
/// Company:o-1234abcd            an object's existence cell
/// Company:o-1234abcd.website    a property cell
/// Founder[]:l-5678wxyz          a list's own cell — its length
/// Founder[]:l-5678wxyz[0]       a list element
/// Any:j-9abcdef0                an untyped object
/// Any[]:y-9abcdef0[0]           an untyped array's element
/// ```
///
/// This exists because of shell pipelines. A worker speaking the wire protocol has to name cells,
/// and the structural JSON form is unusable from `jq`. Making the text form canonical — parsed and
/// rendered from one place — keeps the CLI, the protocol and error messages consistent instead of
/// growing three dialects.
///
/// **A colon, not parentheses.** `Company(o-1234abcd)` reads better, but parentheses are shell
/// metacharacters and the worker protocol is deliberately shell-first: a form that needs quoting is
/// a form that will eventually be typed unquoted.
///
/// The id after the colon is the whole PID (see [`Pid`]'s `Display`), so a rendered address names
/// exactly one cell on exactly one branch. `borg_core::parse::cell_ref` also accepts the older
/// `Company#100` shorthand **on input**; nothing renders it.
/// Composed rather than matched pairwise, so that **every** buffer renders as an address a human can
/// paste back. A `{:?}` fallthrough for the combinations the constructors never build would leak a
/// second, unparseable dialect into exactly the places — panics, lineage output, error messages —
/// where a readable address matters most.
impl fmt::Display for CellRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (name, list) = self.buffer.address();
        f.write_str(name)?;
        if list {
            f.write_str("[]")?;
        }
        write!(f, ":{}", self.pid())?;
        if let CellKey::Elem(_, index) = self.key {
            write!(f, "[{index}]")?;
        }
        if let BufferId::ObjectProp(_, field) = &self.buffer {
            write!(f, ".{field}")?;
        }
        Ok(())
    }
}

/// A cell at one def-version — the full address of a stored record.
///
/// `CellRef` is the *shard key*: where the cell lives, computable without a schema lookup. `CellAt`
/// is the *record key*: which of that cell's versions you mean. One cell may be materialized at
/// several versions at once, because writes are never coerced (SPEC.md §5.4).
///
/// Read-sets, the dependency index and field ownership all key on `CellAt`, not `CellRef`. Keying
/// them on `CellRef` alone would make a migration — which reads `C@v1` and writes `C@v9` — observe
/// its own output as a change to its own input, and poison itself as a cycle.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CellAt {
    pub cell: CellRef,
    pub version: ClientVersion,
}

impl CellAt {
    pub const fn new(cell: CellRef, version: ClientVersion) -> Self {
        Self { cell, version }
    }
}

impl fmt::Debug for CellAt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}@{:?}", self.cell, self.version)
    }
}

/// Whether a cell is ground truth or computed. SPEC.md §8.
///
/// Origin is a property of the `(struct, field)` pair, not of the object: one `Company` may carry
/// source cells and derived cells side by side.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub enum Origin {
    Source,
    Derived,
}

/// Who is attempting a write. SPEC.md §8.
///
/// Every cell write names one of these, and it is checked against the field's declared
/// [`Ownership`](crate::def::Ownership): a client may not write a derived field, and a producer may
/// write only the fields it owns. `Origin` is what the resulting record *records* — the two are
/// deliberately different types, because a producer running an `up` migration writes a field whose
/// declared ownership is `Source` (§9.3), and the record it leaves is still derived.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Writer {
    /// An external client: the CLI today, an SDK later.
    Client,
    Producer(ProducerId),
}

impl Writer {
    /// What a record written by this writer says about where its value came from.
    pub const fn origin(&self) -> Origin {
        match self {
            Self::Client => Origin::Source,
            Self::Producer(_) => Origin::Derived,
        }
    }
}

impl fmt::Display for Writer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Client => f.write_str("a client"),
            Self::Producer(id) => write!(f, "producer {id}"),
        }
    }
}

/// One mutation, with an identity of its own. SPEC.md §4.3, §6.1.
///
/// **An event does not carry the layer it lives in.** Layers name their events (§6.2); an event
/// records only where it was *first* committed. That inversion is what lets one event belong to
/// several layers, which is what lets a merge name a child's events instead of rewriting them
/// (§13) — and it is what keeps the lineage a rewrite would destroy: `authored` survives the merge,
/// and the layer you reached the event through says where it landed on *this* branch.
///
/// Source events carry only value, version and origin. The heavy metadata — watermark, read-set,
/// producer — attaches to derived events only, which in a normalized model are the minority.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub id: EventId,
    /// The cell this event mutates. On the event rather than beside it, because an event is now a
    /// thing in its own right that layers merely reference.
    pub cell: CellRef,
    pub value: Value,
    /// The `ClientVersion` this value was *written* at. Never coerced or rewritten; readers at other
    /// versions migrate on the read path (SPEC.md §5.4).
    pub version: ClientVersion,
    pub origin: Origin,
    /// Present only when `origin == Derived`.
    pub derivation: Option<Derivation>,
    /// Where this event was **first** committed — on whichever branch wrote it. A merge never
    /// rewrites it, so "authored on `feature` at L20, landed on main at L30" survives as two facts
    /// rather than collapsing into the second (SPEC.md §13).
    pub authored: LayerId,
}

/// An event before the log has given it one: no id, and no place. SPEC.md §4.3.
///
/// The two fields an [`Event`] has and this does not are exactly the two the log supplies —
/// identity, and the layer that authored it. A writer names neither, which is what makes it
/// impossible to author an event claiming to have been written somewhere it was not.
#[derive(Clone, Debug)]
pub struct EventDraft {
    pub value: Value,
    pub version: ClientVersion,
    pub origin: Origin,
    pub derivation: Option<Derivation>,
}

/// An event as reached through one branch: what it is, and where it arrived here. SPEC.md §4.3.
///
/// The pair is the whole point of the inversion. `event.authored` is a fact about the event and is
/// the same on every branch that names it; `landed_at` is a fact about *this* read path — the layer
/// whose membership carried the event onto the branch being read. They coincide until a merge
/// shares the event, and then they do not.
///
/// Time travel, guard checking and validation all compare against `landed_at`, never `authored`: the
/// question is always "was this visible here, by then", and an event authored on another branch at a
/// low layer id can land on this one arbitrarily late.
#[derive(Clone, Debug)]
pub struct Landed {
    pub event: Event,
    pub landed_at: LayerId,
}

/// Derived-cell metadata. SPEC.md §4.3, §10.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Derivation {
    pub producer: ProducerId,
    /// The source layer through which every input has been incorporated — i.e. *"replay the world at
    /// this layer and you get exactly this value."* SPEC.md §10.1.
    pub fresh_as_of: LayerId,
    /// Exactly the cells this invocation read, *at the versions it read them at*, captured
    /// automatically via `ProducerCtx` (SPEC.md §9.4). Read forwards it drives invalidation; read
    /// backwards, lineage.
    pub read_set: Vec<CellAt>,
}
