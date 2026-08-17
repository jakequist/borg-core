//! **The export stream** — a registry as a canonical event stream. SPEC.md §19.
//!
//! The promise this exists to keep is *the data, not the bytes* (`ROADMAP.md`, the production arc).
//! Pre-1.0 on-disk formats may change; what every release guarantees is that it can write a registry
//! out as this stream and read one written by an earlier release back in. Upgrades are therefore
//! export → upgrade → import, and the same mechanism is backup, restore, format migration and
//! clone/seed. One mechanism, four jobs — because they were never four problems.
//!
//! Borg is unusually well placed for this and it is worth saying why: **the log is the data**. Every
//! index in the system is already a projection proven rebuildable from it
//! (`borg_engine::projection`, `StorageProvider::rebuild_read_index`), so export is *"walk the log
//! and write it down"* and import is a replay — which is what `Registry::open` has always done from
//! the other end.
//!
//! ## What is in the stream, and what is deliberately not
//!
//! Everything needed to reconstruct a registry exactly, and **nothing that is a projection**. So the
//! layer and branch tables are here (§17.1 calls them the structure of the log rather than a fold
//! over it), every event with its identity and its read-set, layer membership, def events, and the
//! bytes behind every content PID in use. The dependency index, the cell-touch index and the
//! watermarks are not, because importing them would be importing an answer the log already contains.
//!
//! Read-sets in particular *are* data and not recomputables: a derived event records the exact cells
//! it was computed from (§4.3), and that is a historical fact about one invocation, not something a
//! replay could re-derive without re-running the producer.
//!
//! ## Which sidecars are state and which are residue
//!
//! The files beside a store (`crate::sidecar`) are not log data, so each had to be decided
//! separately. The rule that fell out: **a sidecar is exported when losing it would change an answer
//! the restored registry gives, and skipped when it only describes a process that is over.**
//!
//! * **`allocations.json` — exported.** The PID counter is the one sidecar a store cannot recover
//!   from: lose it and the count restarts, and a fresh object is issued the id of an existing one
//!   (`CLAUDE.md`). This stream is also its missing backup story.
//! * **`producers.json` — exported.** Definitions travel the log and implementations do not (§9.2),
//!   so a restore without the table is a registry holding producer *definitions* it cannot run. The
//!   commands in it are paths on the exporting machine and are written back verbatim; a restore onto
//!   another machine fixes them with one `repo push`, which is the same thing that put them there.
//! * **Pause flags — exported.** Tiny, and a branch someone paused on purpose that came back
//!   deriving would resume work they had deliberately stopped.
//! * **Poisonings — exported**, and this one was a real decision. A poisoning is the engine's
//!   judgement about code (§14) and looks like operational residue. It is not: a poisoned producer's
//!   cells read `state: broken` rather than `stale`, and `explain` reports the error and the layer it
//!   was recorded at. Drop the table and those exact reads change — `broken` becomes `stale`, which
//!   is a promise of a catch-up that is not coming, the precise lie §14 exists to prevent — and the
//!   first derive after the restore re-runs known-broken code to rediscover what was already known.
//!   The record is keyed on the ClientVersion it was recorded against, so it still self-expires when
//!   fixed code is pushed; exporting it changes nothing about recovery and everything about whether
//!   a restored registry answers the same questions the same way.
//! * **The transaction table — skipped.** Ephemeral by decree (§12.3): a transaction is reaped on
//!   silence, and restore is create-then-import, so no client can be holding a handle to a registry
//!   that did not exist a moment ago. Its *timeout* is exported, because that is a knob somebody set
//!   rather than a transaction somebody opened.
//! * **`serving.json` — skipped.** It is not state at all, it is a live claim naming the socket of a
//!   process that is not this one (`crate::serving`).
//!
//! ## Why NDJSON, and what the ordering buys
//!
//! Line-oriented because a registry may be huge and must never be materialized whole — the same
//! discipline the storage layer keeps (§6.2, §17.1). One record per line means export streams, a
//! backup diffs against the previous one, and `grep` works. JSON because every other persisted format
//! in this project is already JSON: def events, layer metadata and cell values are all stored as
//! their serde encodings, so relaying them costs no second conversion table to keep in step.
//!
//! **A record is a single-key object**, payload-free ones written `{}` rather than as bare strings —
//! the same rule `borg_protocol::client` follows, and for the same reason: `jq 'keys[0]'` has to be
//! able to dispatch.
//!
//! **What an export does hold in memory, stated rather than implied**: the layer table, from
//! `read_layers`, and one layer's distinct content PIDs. Neither is new — `Registry::open` already
//! reads the whole layer table, and `read_layer` already materializes a layer (`CLAUDE.md`, things
//! left undone) — so an export is bounded by the bounds the engine already has and adds none. What it
//! never holds is the events: those stream, one layer at a time, which is the case that actually
//! matters because a single derived layer may hold millions of them.
//!
//! **Order is part of the format.** A `layer` record opens a block and everything until the next one
//! belongs to it, so an `event` and a `member` carry no layer of their own — repeating it on each of
//! a merge layer's million membership rows would pay real bytes for a fact the position already
//! carries. Layers are emitted in ascending id, which is what makes the stream importable in one
//! pass: a layer only ever *names* events authored at or below its own id, so nothing refers
//! forwards.
//!
//! **Byte-identical for identical registries**, which is the cheapest total check available — export
//! a store, import it, export that, `cmp`. That is why the header carries the stream version and the
//! binary's version and *nothing else*: no timestamp, no registry name, no path. Where a copy came
//! from and when are facts about the copy, not about the data, and a filename and an `ls -l` already
//! carry both. Everything with no natural order is sorted: producers by id, poisonings by
//! (branch, producer), pause flags and branches by id.

use crate::ops::{
    self, Allocations, BrokenProducer, DerivationConfig, Implementation, Implementations, Ops,
    Transactions,
};
use crate::sidecar;
use borg_core::{
    AllocatorId, BorgError, Branch, BranchId, CellAt, CellRef, DefEvent, DefVersion, Derivation,
    Event, EventId, Guard, Layer, LayerAuthor, LayerId, LayerKind, LayerState, Origin, Pid,
    ProducerId, Result, Value, parse,
};
use borg_storage::StorageProvider;
use borg_storage_sqlite::SqliteStorage;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io::{BufRead, Write};
use std::path::Path;
use std::sync::Arc;

/// The stream format's own version, moved when a reader of an older stream would get it wrong.
///
/// **Separate from the binary's version**, which the header also carries, because they answer
/// different questions: this one says *can I read this at all*, and the other says *what wrote it*.
/// A release that changes the on-disk format without changing the stream leaves this alone, which is
/// exactly the case the whole policy exists for.
pub const STREAM_VERSION: u32 = 1;

/// What produced a stream, as its header states it.
///
/// `borg <version>` and not the binary's own name, deliberately: `borg export` and
/// `borg-server export` are two front ends over this one module (`CLAUDE.md`), and a header that
/// distinguished them would make the same registry export to different bytes depending on which of
/// them asked — destroying the byte-identity property for no information anybody wants.
fn producing_binary() -> String {
    format!("borg {}", env!("CARGO_PKG_VERSION"))
}

// --- The records -------------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Record {
    /// Always first, and the only record whose absence is fatal on its own.
    Header(Header),
    /// The PID counter. See the module header.
    Allocations {
        next: u64,
    },
    /// `borg tx timeout`'s setting, without the transactions it applies to.
    TxTimeout {
        seconds: u64,
    },
    Producer(ProducerRow),
    Poison(PoisonRow),
    Paused {
        branch: u64,
    },
    Branch(BranchRow),
    /// Opens a layer block. Everything after it belongs to it until the next one.
    Layer(LayerRow),
    /// The bytes behind a content PID, emitted just before the first event in this layer to
    /// reference it. Interning is idempotent and not layered (§3.1), so repeating one across layers
    /// costs a hash lookup and keeps the seen-set bounded by a layer rather than by the registry.
    Content(ContentRow),
    /// One def mutation, in this def layer's order.
    Def {
        event: DefEvent,
    },
    /// An event **authored** in this layer, which is also a membership row at this position.
    Event(EventRow),
    /// A membership row naming an event authored elsewhere — what a merge layer is made of (§13).
    Member {
        event: u64,
    },
}

#[derive(Serialize, Deserialize)]
struct Header {
    version: u32,
    binary: String,
}

/// A producer's implementation row, as `producers.json` holds it.
///
/// The id is a **string**, for the reason `sidecar::producer_id` documents: a `ProducerId` is a hash
/// using the whole `u64` range, and every JSON tool in a shell pipeline rounds anything above 2⁵³
/// into a different producer. Layer, branch and event ids stay numbers — they are sequential
/// counters, and keeping them numeric is what lets `jq` sort and compare them.
#[derive(Serialize, Deserialize)]
struct ProducerRow {
    #[serde(with = "sidecar::producer_id")]
    id: u64,
    name: String,
    source: String,
    command: String,
    transport: borg_protocol::Transport,
}

#[derive(Serialize, Deserialize)]
struct PoisonRow {
    branch: u64,
    #[serde(with = "sidecar::producer_id")]
    producer: u64,
    version: u64,
    error: String,
    since: u64,
}

#[derive(Serialize, Deserialize)]
struct BranchRow {
    id: u64,
    name: Option<String>,
    origin: Option<u64>,
}

#[derive(Serialize, Deserialize)]
struct LayerRow {
    id: u64,
    branch: u64,
    kind: KindRow,
    author: AuthorRow,
    parent: Option<u64>,
    guards: Vec<GuardRow>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum KindRow {
    Value,
    Def,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AuthorRow {
    Source,
    Derived {
        #[serde(with = "sidecar::producer_id")]
        producer: u64,
        reflects: u64,
    },
}

#[derive(Serialize, Deserialize)]
struct GuardRow {
    cells: Vec<String>,
    since: u64,
}

/// Interned bytes, addressed by the PID that *is* their hash (§3.1).
///
/// `text` when the bytes are valid UTF-8 and `hex` otherwise — a pure function of the bytes, so the
/// choice is deterministic, and the common case (a string) stays readable in a backup somebody is
/// squinting at. The PID is not a label: import re-interns the bytes and checks that the PID it gets
/// back is the one the line claimed, so a truncated or corrupted content line is caught by content
/// addressing itself rather than by a checksum bolted on beside it.
#[derive(Serialize, Deserialize)]
struct ContentRow {
    pid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    hex: Option<String>,
}

/// One event. No `layer` and no `authored`: both are the block this record sits in.
#[derive(Serialize, Deserialize)]
struct EventRow {
    id: u64,
    /// The canonical text address (§4) — `Company:o-1234abcd.website`. The one spelling the CLI, the
    /// worker protocol and every error message already share, so a cell in a backup is a cell you can
    /// paste into `borg get`.
    cell: String,
    value: ValueRow,
    /// The def-version of **this cell's field**, never the writer's ClientVersion (§5.3).
    version: u64,
    origin: OriginRow,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    derivation: Option<DerivationRow>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OriginRow {
    Source,
    Derived,
}

#[derive(Serialize, Deserialize)]
struct DerivationRow {
    #[serde(with = "sidecar::producer_id")]
    producer: u64,
    fresh_as_of: u64,
    read_set: Vec<ReadRow>,
}

#[derive(Serialize, Deserialize)]
struct ReadRow {
    cell: String,
    version: u64,
}

/// A cell's value. Single-key, like every other record, and `{"tombstone":{}}` rather than
/// `"tombstone"` for the reason the client protocol gives: a bare string is not something
/// `jq 'keys[0]'` can dispatch on.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ValueRow {
    Int(i64),
    Bool(bool),
    Double(f64),
    /// A PID in canonical text. Content PIDs name a `content` record; allocated ones name an object,
    /// a list or an untyped container, and reference nothing in the stream — a dangling reference is
    /// permitted (§18) and survives the round trip as one.
    Ref(String),
    Tombstone {},
}

// --- Export ------------------------------------------------------------------------------------

/// What one export turned out to be. Counts, and where the log stood.
#[derive(Debug)]
pub struct Exported {
    pub layers: u64,
    pub events: u64,
    pub interned: u64,
    /// The highest committed layer in the registry — *the position this stream represents*.
    pub head: LayerId,
    /// The default branch's settled ceiling (§10.5), reported rather than used. See [`export`].
    pub settled: LayerId,
}

/// Write a registry out as a stream. SPEC.md §19.
///
/// ## What position this represents, and why it is not the settled one
///
/// **The whole log, at the instant this call took the registry.** There is no torn read to worry
/// about and no snapshot machinery here, because exclusion already exists: embedded `borg` is
/// refused outright against a served store (`crate::serving`) and is one process besides, and a
/// served export runs under the registry's own gate (`crate::host`), which serialises it against
/// every other request on that registry. Nothing can commit while this walks. The cost is the honest
/// one — a large export holds its registry for its duration — and it is the same gate whose
/// relaxation is already an open question in `ROADMAP.md` rather than a new one this introduces.
///
/// **Deliberately not a settled read.** `Registry::settled` answers *where can I read a coherent
/// snapshot*, which is a question about derived data lagging source data. Bounding an export there
/// would silently drop every source layer above the watermark — data loss, dressed as coherence. A
/// backlog is part of what a registry is: exporting head captures the lag faithfully, watermarks and
/// all, and the restored registry works the same backlog off. The settled position is *reported*
/// here so an operator can see what state they captured, and is used for nothing.
pub async fn export<W: Write + Send>(args: &Ops, out: &mut W) -> Result<Exported> {
    let registry = ops::open(args).await?;
    let storage = Arc::clone(&registry.storage);

    let mut sink = Sink { out };
    sink.write(&Record::Header(Header {
        version: STREAM_VERSION,
        binary: producing_binary(),
    }))?;

    // The sidecars first, and small enough to be read whole: they are the state a restored registry
    // needs *before* anything asks it a question, and putting them at the head means a truncated
    // stream is missing data rather than missing its allocator.
    sink.write(&Record::Allocations {
        next: ops::load_allocations(args).next,
    })?;
    sink.write(&Record::TxTimeout {
        seconds: ops::load_transactions(args).tx_idle_timeout,
    })?;

    let mut impls = ops::load_impls(args).producers;
    impls.sort_by_key(|producer| producer.id);
    for producer in impls {
        sink.write(&Record::Producer(ProducerRow {
            id: producer.id,
            name: producer.name,
            source: producer.source,
            command: producer.command.display().to_string(),
            transport: producer.transport,
        }))?;
    }

    let derivation = ops::load_derivation(args);
    let mut broken = derivation.broken;
    broken.sort_by_key(|row| (row.branch, row.producer));
    for row in broken {
        sink.write(&Record::Poison(PoisonRow {
            branch: row.branch,
            producer: row.producer,
            version: row.version,
            error: row.error,
            since: row.since,
        }))?;
    }
    let mut paused = derivation.paused;
    paused.sort_unstable();
    paused.dedup();
    for branch in paused {
        sink.write(&Record::Paused { branch })?;
    }

    let mut branches = storage.read_branches().await?;
    branches.sort_by_key(|branch| branch.id.0);
    for branch in &branches {
        sink.write(&Record::Branch(BranchRow {
            id: branch.id.0,
            name: branch.name.clone(),
            origin: branch.origin.map(|layer| layer.0),
        }))?;
    }

    // **Committed only.** An open layer is exclusive to a process that no longer exists and a sealed
    // one never became visible (§6.2), so neither is part of what the registry *is* — and
    // `LayerManager::restore` already treats both as aborted on the way back in. A committed layer's
    // `parent` is the branch's head at the moment it opened, which is committed by construction, so
    // filtering here can never orphan a chain.
    let mut layers: Vec<Layer> = storage
        .read_layers()
        .await?
        .into_iter()
        .filter(|layer| layer.state == LayerState::Committed)
        .collect();
    layers.sort_by_key(|layer| layer.id.0);
    let head = layers.last().map_or(LayerId(0), |layer| layer.id);

    let mut events = 0u64;
    let mut interned = 0u64;
    for layer in &layers {
        sink.write(&Record::Layer(LayerRow {
            id: layer.id.0,
            branch: layer.branch.0,
            kind: match layer.kind {
                LayerKind::Value => KindRow::Value,
                LayerKind::Def => KindRow::Def,
            },
            author: match layer.author {
                LayerAuthor::Source => AuthorRow::Source,
                LayerAuthor::Derived { producer, reflects } => AuthorRow::Derived {
                    producer: producer.0,
                    reflects: reflects.0,
                },
            },
            parent: layer.parent.map(|id| id.0),
            guards: layer
                .guards
                .iter()
                .map(|guard| {
                    Ok(GuardRow {
                        cells: guard.cells.iter().map(address).collect::<Result<_>>()?,
                        since: guard.since.0,
                    })
                })
                .collect::<Result<_>>()?,
        }))?;

        if layer.kind == LayerKind::Def {
            for event in storage.read_def_layer(layer.id).await? {
                sink.write(&Record::Def { event })?;
            }
            continue;
        }

        // Content is emitted per layer rather than per registry, and this set is the reason: it is
        // bounded by one layer's distinct values, which is the bound `read_layer` already imposes
        // (`CLAUDE.md`, things left undone). A registry-wide set would be bounded by the number of
        // distinct strings in the store, which is the thing this format promises not to hold.
        let mut seen: HashSet<Pid> = HashSet::new();
        let mut membership = storage.read_layer(layer.id).await?;
        while let Some(row) = membership.next().await {
            let event = row?;
            if event.authored != layer.id {
                sink.write(&Record::Member { event: event.id.0 })?;
                continue;
            }
            if let Some(pid) = content_pid(&event.value)
                && seen.insert(pid)
            {
                // `None` is a legitimate answer and not an error (§17.1): a PID travels further than
                // the bytes behind it, so a store may hold a reference whose content it has never
                // seen. Emitting nothing reproduces exactly that on the other side.
                if let Some(bytes) = storage.read_interned(&pid).await? {
                    sink.write(&Record::Content(content_row(&pid, &bytes)))?;
                    interned += 1;
                }
            }
            sink.write(&Record::Event(event_row(&event)?))?;
            events += 1;
        }
    }

    sink.out.flush().map_err(io)?;

    // Reported, never used. See the doc comment.
    let settled = match registry.default_branch() {
        Some(branch) => registry.settled(branch).await?,
        None => LayerId(0),
    };
    Ok(Exported {
        layers: layers.len() as u64,
        events,
        interned,
        head,
        settled,
    })
}

struct Sink<'a, W: Write> {
    out: &'a mut W,
}

impl<W: Write> Sink<'_, W> {
    fn write(&mut self, record: &Record) -> Result<()> {
        let line = serde_json::to_string(record)
            .map_err(|err| BorgError::Storage(format!("cannot encode record: {err}")))?;
        self.out.write_all(line.as_bytes()).map_err(io)?;
        self.out.write_all(b"\n").map_err(io)
    }
}

fn io(err: std::io::Error) -> BorgError {
    BorgError::Storage(err.to_string())
}

fn content_pid(value: &Value) -> Option<Pid> {
    match value {
        Value::Ref(pid) if pid.kind().is_content_addressed() => Some(*pid),
        _ => None,
    }
}

fn content_row(pid: &Pid, bytes: &[u8]) -> ContentRow {
    match std::str::from_utf8(bytes) {
        Ok(text) => ContentRow {
            pid: pid.to_string(),
            text: Some(text.to_string()),
            hex: None,
        },
        Err(_) => ContentRow {
            pid: pid.to_string(),
            text: None,
            hex: Some(hex(bytes)),
        },
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(b"0123456789abcdef"[(byte >> 4) as usize]));
        out.push(char::from(b"0123456789abcdef"[(byte & 15) as usize]));
    }
    out
}

fn unhex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    text.as_bytes()
        .chunks(2)
        .map(|pair| {
            let digit = |b: u8| char::from(b).to_digit(16).map(|d| d as u8);
            Some(digit(pair[0])? << 4 | digit(pair[1])?)
        })
        .collect()
}

/// A cell as its canonical text, checked to be readable back as itself.
///
/// The check is here rather than trusted because this is a **backup format**: the text form is
/// documented lossless and is injective over every address the constructors build (§4), but a
/// silently mangled cell on restore is the worst failure this feature has, and turning it into a
/// loud export failure costs one parse per event on a path that is already doing JSON per event. An
/// export that cannot spell one of its own addresses should say so, not produce a stream that reads
/// back as something else.
fn address(cell: &CellRef) -> Result<String> {
    let text = cell.to_string();
    // The canonical form never uses the `Struct#100` shorthand, so the branch and allocator a
    // shorthand would resolve against are never consulted.
    let back = parse::cell_ref(&text, BranchId(0), AllocatorId(0))?;
    if &back != cell {
        return Err(BorgError::Storage(format!(
            "`{cell:?}` has no canonical text form — it renders as `{text}`, which reads back as \
             `{back:?}`. This address cannot be exported without changing it."
        )));
    }
    Ok(text)
}

fn event_row(event: &Event) -> Result<EventRow> {
    Ok(EventRow {
        id: event.id.0,
        cell: address(&event.cell)?,
        value: value_row(&event.value)?,
        version: event.version.0.0,
        origin: match event.origin {
            Origin::Source => OriginRow::Source,
            Origin::Derived => OriginRow::Derived,
        },
        derivation: event
            .derivation
            .as_ref()
            .map(|by| {
                Result::Ok(DerivationRow {
                    producer: by.producer.0,
                    fresh_as_of: by.fresh_as_of.0,
                    read_set: by
                        .read_set
                        .iter()
                        .map(|read| {
                            Ok(ReadRow {
                                cell: address(&read.cell)?,
                                version: read.version.0.0,
                            })
                        })
                        .collect::<Result<_>>()?,
                })
            })
            .transpose()?,
    })
}

fn value_row(value: &Value) -> Result<ValueRow> {
    Ok(match value {
        Value::Int(n) => ValueRow::Int(*n),
        Value::Bool(b) => ValueRow::Bool(*b),
        // JSON has no spelling for NaN or an infinity, and neither does the SQLite backend, which
        // already stores values as their JSON encoding — so this is refused by name rather than
        // written as the `null` serde_json would otherwise emit and then fail to read back.
        Value::Double(d) if !d.is_finite() => {
            return Err(BorgError::Storage(format!(
                "a cell holds the double `{d}`, which JSON cannot represent"
            )));
        }
        Value::Double(d) => ValueRow::Double(*d),
        Value::Ref(pid) => ValueRow::Ref(pid.to_string()),
        Value::Tombstone => ValueRow::Tombstone {},
    })
}

// --- Import ------------------------------------------------------------------------------------

/// What one import turned out to be.
#[derive(Debug)]
pub struct Imported {
    pub layers: u64,
    pub events: u64,
    pub branches: u64,
    pub head: LayerId,
    /// The version of the binary that wrote the stream, as its header stated it.
    pub written_by: String,
}

/// Read a stream into a **fresh** registry at `store`. SPEC.md §19.
///
/// ## It creates; it does not merge
///
/// Importing into a registry that already holds anything is refused. Restore is *create-then-import*
/// and always has been, because the alternative is a decision nobody can make correctly: the stream
/// names layer, branch and event ids, and merging two id spaces means either renaming — which
/// invalidates every read-set and every `reflects` in the stream — or colliding. Refusing is the only
/// answer that cannot silently corrupt.
///
/// ## Ids are preserved, not re-minted
///
/// They are part of the data. A derived event's read-set names cells; a derived *layer* names the
/// source layer it `reflects`; membership names events. Re-mint any of them and the lineage is a
/// plausible fiction. So this writes through `StorageProvider` directly rather than through a
/// `Registry`: it is replaying a log, not re-executing one. Nothing is validated against definitions
/// (the events were validated when they were authored, under the def-view in force then), nothing is
/// derived, and nothing is re-sequenced.
///
/// ## One pass, in order
///
/// A layer only names events authored at or below its own id, and layers are emitted ascending — so
/// every reference in the stream points backwards and a single pass suffices. The read index each
/// backend maintains is built on the way in exactly as it is for an ordinary commit, so the imported
/// store needs no rebuild step.
pub async fn import<R: BufRead + Send>(store: &Path, input: &mut R) -> Result<Imported> {
    // **A failed restore takes its store with it, and only if this call is what made it.** A
    // half-written store left on disk is the worse of the two outcomes: under a data directory it is
    // a registry the next `borg-server start` would discover and host, and anywhere else it is a
    // file that looks like a store and is not one. A store that was already there — empty, but
    // somebody's — is left exactly where it was.
    let existed = store.exists();
    let outcome = restore(store, input).await;
    if outcome.is_err() && !existed {
        let _ = std::fs::remove_file(store);
    }
    outcome
}

async fn restore<R: BufRead + Send>(store: &Path, input: &mut R) -> Result<Imported> {
    if let Some(parent) = store.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .map_err(|err| BorgError::Storage(format!("{}: {err}", parent.display())))?;
    }
    let storage = SqliteStorage::open(store)?;
    if !storage.read_layers().await?.is_empty() || !storage.read_branches().await?.is_empty() {
        return Err(BorgError::Storage(format!(
            "{} already holds a registry — import creates one, so restore is create-then-import \
             and never `borg init` first",
            store.display()
        )));
    }

    let mut reader = Reader { input, at: 0 };
    let Some(first) = reader.next()? else {
        return Err(BorgError::Storage(
            "the stream is empty — expected a `header` record on line 1".into(),
        ));
    };
    let Record::Header(header) = first else {
        return Err(reader.wrong("a `header` record"));
    };
    if header.version != STREAM_VERSION {
        return Err(BorgError::Storage(format!(
            "this stream is format version {}, and this binary reads version {} — export it again \
             with the release that wrote it, or upgrade that release first",
            header.version, STREAM_VERSION
        )));
    }

    let mut state = Restoring::default();
    let mut open: Option<OpenBlock> = None;
    while let Some(record) = reader.next()? {
        match record {
            Record::Header(_) => return Err(reader.wrong("one header, on line 1")),
            Record::Allocations { next } => state.allocations.next = next,
            Record::TxTimeout { seconds } => state.transactions.tx_idle_timeout = seconds,
            Record::Producer(row) => state.producers.push(Implementation {
                id: row.id,
                name: row.name,
                source: row.source,
                command: row.command.into(),
                transport: row.transport,
            }),
            Record::Poison(row) => state.derivation.broken.push(BrokenProducer {
                branch: row.branch,
                producer: row.producer,
                version: row.version,
                error: row.error,
                since: row.since,
            }),
            Record::Paused { branch } => state.derivation.paused.push(branch),
            Record::Branch(row) => {
                storage
                    .put_branch(&Branch {
                        id: BranchId(row.id),
                        name: row.name,
                        origin: row.origin.map(LayerId),
                    })
                    .await?;
                state.branches += 1;
            }
            Record::Layer(row) => {
                if let Some(block) = open.take() {
                    close(&storage, block).await?;
                }
                state.layers += 1;
                state.head = state.head.max(row.id);
                open = Some(open_block(&storage, &reader, row).await?);
            }
            Record::Content(row) => {
                let pid: Pid = row
                    .pid
                    .parse()
                    .map_err(|err| reader.bad(&format!("`{}` is not a pid: {err}", row.pid)))?;
                let bytes = match (row.text, row.hex) {
                    (Some(text), None) => text.into_bytes(),
                    (None, Some(text)) => unhex(&text)
                        .ok_or_else(|| reader.bad("`hex` is not an even run of hex digits"))?,
                    _ => return Err(reader.wrong("exactly one of `text` and `hex`")),
                };
                // Content addressing *is* the integrity check: the PID is the hash of the bytes, so
                // a line that lost or gained a byte cannot re-intern to the id it claims.
                let again = storage.intern(pid.kind(), &bytes).await?;
                if again != pid {
                    return Err(reader.bad(&format!(
                        "content claims to be `{pid}` but its bytes intern as `{again}` — the line \
                         is corrupt"
                    )));
                }
            }
            // **A layer holds value events xor def events** (§6.2, invariant 9). The engine's write
            // path enforces this in `LayerHandle`; an import writes through the provider directly,
            // so it has to say so itself or a hand-edited stream could produce a layer for which
            // "the def-version at layer L" has two answers.
            Record::Def { event } => {
                block(&mut open, &reader, LayerKind::Def)?
                    .put_def(event)
                    .await?;
            }
            Record::Event(row) => {
                let block = block(&mut open, &reader, LayerKind::Value)?;
                let layer = block.layer.id;
                block.adopt_event(event_of(row, layer, &reader)?).await?;
                state.events += 1;
            }
            Record::Member { event } => {
                block(&mut open, &reader, LayerKind::Value)?
                    .include_event(EventId(event))
                    .await?;
            }
        }
    }
    if let Some(block) = open.take() {
        close(&storage, block).await?;
    }

    // The sidecars go down last, because a store that failed halfway through should leave no files
    // beside it claiming things about a registry that is not there.
    sidecar::save(store, &state.allocations)?;
    sidecar::save(store, &state.transactions)?;
    sidecar::save(
        store,
        &Implementations {
            producers: state.producers,
        },
    )?;
    sidecar::save(store, &state.derivation)?;

    Ok(Imported {
        layers: state.layers,
        events: state.events,
        branches: state.branches,
        head: LayerId(state.head),
        written_by: header.binary,
    })
}

/// Everything an import accumulates outside the log itself.
///
/// The sidecars start at their defaults, which is exactly right: a stream that carries no
/// `allocations` record was written by a store that had allocated nothing, and `Default` is what
/// that store's own missing file would have read as (`crate::sidecar`).
#[derive(Default)]
struct Restoring {
    allocations: Allocations,
    transactions: Transactions,
    producers: Vec<Implementation>,
    derivation: DerivationConfig,
    layers: u64,
    events: u64,
    branches: u64,
    /// The highest layer the stream carried. A `u64` rather than a `LayerId` because `LayerId(0)` is
    /// "no layers yet" here and the id type has deliberately never had a zero value to mean that.
    head: u64,
}

/// The layer currently being filled, and the metadata it will be committed with.
struct OpenBlock {
    layer: Layer,
    open: Box<dyn borg_storage::OpenLayer>,
}

impl OpenBlock {
    async fn adopt_event(&mut self, event: Event) -> Result<()> {
        self.open.adopt_event(event).await
    }

    async fn include_event(&mut self, event: EventId) -> Result<()> {
        self.open.include_event(event).await
    }

    async fn put_def(&mut self, event: DefEvent) -> Result<()> {
        self.open.put_def(event).await
    }
}

fn block<'a, R: BufRead>(
    open: &'a mut Option<OpenBlock>,
    reader: &Reader<'_, R>,
    kind: LayerKind,
) -> Result<&'a mut OpenBlock> {
    let block = open
        .as_mut()
        .ok_or_else(|| reader.wrong("a `layer` record before anything that belongs to one"))?;
    if block.layer.kind != kind {
        return Err(reader.bad(&format!(
            "layer {} is a {:?} layer, and a layer holds value events xor def events",
            block.layer.id, block.layer.kind
        )));
    }
    Ok(block)
}

async fn open_block<R: BufRead>(
    storage: &SqliteStorage,
    reader: &Reader<'_, R>,
    row: LayerRow,
) -> Result<OpenBlock> {
    let branch = BranchId(row.branch);
    let layer = Layer {
        id: LayerId(row.id),
        branch,
        kind: match row.kind {
            KindRow::Value => LayerKind::Value,
            KindRow::Def => LayerKind::Def,
        },
        author: match row.author {
            AuthorRow::Source => LayerAuthor::Source,
            AuthorRow::Derived { producer, reflects } => LayerAuthor::Derived {
                producer: ProducerId(producer),
                reflects: LayerId(reflects),
            },
        },
        // Committed is the only state a stream carries, and the only one a replay can honestly
        // produce — see `export`.
        state: LayerState::Committed,
        parent: row.parent.map(LayerId),
        guards: row
            .guards
            .into_iter()
            .map(|guard| {
                Ok(Guard {
                    cells: guard
                        .cells
                        .iter()
                        .map(|text| cell(text, reader))
                        .collect::<Result<_>>()?,
                    since: LayerId(guard.since),
                })
            })
            .collect::<Result<_>>()?,
    };
    let open = storage.open_layer(branch, layer.id).await?;
    Ok(OpenBlock { layer, open })
}

/// Seal, record the metadata, commit — in the order `LayerManager::commit` uses, because it is the
/// order the state machine requires: metadata is written before visibility flips, so a layer can
/// never become visible without the log knowing what kind of thing it is.
async fn close(storage: &SqliteStorage, block: OpenBlock) -> Result<()> {
    let OpenBlock { layer, open } = block;
    let sealed = open.seal().await?;
    storage.put_layer_meta(&layer).await?;
    storage.commit_layer(sealed).await
}

fn event_of<R: BufRead>(row: EventRow, layer: LayerId, reader: &Reader<'_, R>) -> Result<Event> {
    Ok(Event {
        id: EventId(row.id),
        cell: cell(&row.cell, reader)?,
        value: value_of(row.value, reader)?,
        version: DefVersion(LayerId(row.version)),
        origin: match row.origin {
            OriginRow::Source => Origin::Source,
            OriginRow::Derived => Origin::Derived,
        },
        derivation: row
            .derivation
            .map(|by| {
                Result::Ok(Derivation {
                    producer: ProducerId(by.producer),
                    fresh_as_of: LayerId(by.fresh_as_of),
                    read_set: by
                        .read_set
                        .iter()
                        .map(|read| {
                            Ok(CellAt::new(
                                cell(&read.cell, reader)?,
                                DefVersion(LayerId(read.version)),
                            ))
                        })
                        .collect::<Result<_>>()?,
                })
            })
            .transpose()?,
        // **The block, not a field.** An `event` record appears in the layer that authored it by
        // construction, so there is nothing here for a stream to get wrong.
        authored: layer,
    })
}

fn value_of<R: BufRead>(row: ValueRow, reader: &Reader<'_, R>) -> Result<Value> {
    Ok(match row {
        ValueRow::Int(n) => Value::Int(n),
        ValueRow::Bool(b) => Value::Bool(b),
        ValueRow::Double(d) => Value::Double(d),
        ValueRow::Ref(text) => Value::Ref(
            text.parse()
                .map_err(|err| reader.bad(&format!("`{text}` is not a pid: {err}")))?,
        ),
        ValueRow::Tombstone {} => Value::Tombstone,
    })
}

fn cell<R: BufRead>(text: &str, reader: &Reader<'_, R>) -> Result<CellRef> {
    // No shorthand can appear in a stream — `export` writes canonical addresses and checks that they
    // read back as themselves — so the branch and allocator here are never consulted.
    parse::cell_ref(text, BranchId(0), AllocatorId(0))
        .map_err(|err| reader.bad(&format!("`{text}` is not a cell address: {err}")))
}

/// The stream, one line at a time, remembering where it is.
///
/// **The line number is the whole point.** A malformed or truncated backup is discovered by someone
/// who needs to fix it, and *"line 41208: expected a `layer` record"* is a place to look; a bare
/// serde error is not.
struct Reader<'a, R: BufRead> {
    input: &'a mut R,
    at: u64,
}

impl<R: BufRead> Reader<'_, R> {
    /// One record, or `None` at the end.
    ///
    /// Read a line at a time rather than through `Lines`, so a stream carrying a single 100k-cell
    /// layer still costs one line of memory rather than the whole file.
    fn next(&mut self) -> Result<Option<Record>> {
        let mut line = String::new();
        loop {
            line.clear();
            if self.input.read_line(&mut line).map_err(io)? == 0 {
                return Ok(None);
            }
            self.at += 1;
            // A blank line is skipped rather than refused: streams get concatenated, edited and
            // passed through shells, and a trailing newline is not a corruption.
            if line.trim().is_empty() {
                continue;
            }
            return serde_json::from_str(&line).map(Some).map_err(|err| {
                BorgError::Storage(format!(
                    "line {}: {err} — in: {}",
                    self.at,
                    clip(line.trim())
                ))
            });
        }
    }

    fn wrong(&self, expected: &str) -> BorgError {
        BorgError::Storage(format!("line {}: expected {expected}", self.at))
    }

    fn bad(&self, what: &str) -> BorgError {
        BorgError::Storage(format!("line {}: {what}", self.at))
    }
}

/// Enough of an offending line to recognise it, without printing a megabyte read-set into a terminal.
fn clip(line: &str) -> String {
    const LIMIT: usize = 200;
    if line.len() <= LIMIT {
        return line.to_string();
    }
    let mut end = LIMIT;
    while !line.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &line[..end])
}

#[cfg(test)]
mod tests {
    use super::*;
    use borg_core::{BufferId, CellKey, PidKind};

    fn object(counter: u64) -> Pid {
        Pid::Allocated {
            kind: PidKind::Object,
            branch: BranchId(3),
            allocator: AllocatorId(1),
            counter,
        }
    }

    /// The claim `address` makes, over every shape the constructors build. If one of these ever
    /// stops round-tripping, an export of a store holding it would change the address rather than
    /// carry it — which is why `address` checks rather than trusts.
    #[test]
    fn every_cell_address_the_system_builds_survives_its_text_form() {
        let any = |kind| Pid::Allocated {
            kind,
            branch: BranchId(3),
            allocator: AllocatorId(1),
            counter: 9,
        };
        let cells = [
            CellRef::existence("Company".into(), object(1)),
            CellRef::prop("Company".into(), "website".into(), object(2)),
            CellRef::list("Founder".into(), any(PidKind::List)),
            CellRef::elem("Founder".into(), any(PidKind::List), 0),
            CellRef::elem("Founder".into(), any(PidKind::List), 4_000_000_000),
            CellRef {
                buffer: BufferId::AnyObject,
                key: CellKey::Pid(any(PidKind::AnyObject)),
            },
            CellRef {
                buffer: BufferId::AnyArray,
                key: CellKey::Pid(any(PidKind::AnyArray)),
            },
            CellRef {
                buffer: BufferId::AnyArray,
                key: CellKey::Elem(any(PidKind::AnyArray), 7),
            },
        ];
        for cell in cells {
            let text = address(&cell).unwrap_or_else(|err| panic!("{cell:?}: {err}"));
            assert_eq!(
                parse::cell_ref(&text, BranchId(0), AllocatorId(0)).unwrap(),
                cell,
                "{text}"
            );
        }
    }

    /// A string stays readable; anything else becomes hex. Both round-trip, and the choice is a
    /// function of the bytes alone, so two exports of one store agree.
    #[test]
    fn content_is_text_when_it_can_be_and_hex_when_it_cannot() {
        for bytes in [
            &b"acme.ai"[..],
            "h\u{e9}llo \u{2014} utf-8".as_bytes(),
            b"",
            &[0x00, 0xff, 0x00][..],
        ] {
            let pid = borg_core::content::pid(PidKind::String, bytes).unwrap();
            let row = content_row(&pid, bytes);
            let back = match (&row.text, &row.hex) {
                (Some(text), None) => text.clone().into_bytes(),
                (None, Some(text)) => unhex(text).expect("hex decodes"),
                _ => panic!("exactly one of text and hex"),
            };
            assert_eq!(back, bytes);
            assert_eq!(row.text.is_some(), std::str::from_utf8(bytes).is_ok());
        }
    }

    /// Every record is a single-key object, `{}`-payloaded ones included — the rule
    /// `borg_protocol::client` follows so that `jq 'keys[0]'` can dispatch.
    #[test]
    fn every_record_is_a_single_key_object() {
        let records = [
            Record::Header(Header {
                version: STREAM_VERSION,
                binary: "borg 0.0.0".into(),
            }),
            Record::Allocations { next: 1 },
            Record::TxTimeout { seconds: 600 },
            Record::Paused { branch: 2 },
            Record::Member { event: 7 },
            Record::Def {
                event: DefEvent::DeleteField {
                    struct_name: "Company".into(),
                    field: "website".into(),
                    repo: borg_core::RepoId(1),
                },
            },
        ];
        for record in records {
            let json: serde_json::Value = serde_json::to_value(&record).expect("encodes");
            let object = json.as_object().expect("an object");
            assert_eq!(object.len(), 1, "{json}");
        }
        for value in [
            Value::Int(1),
            Value::Bool(true),
            Value::Double(1.5),
            Value::Tombstone,
        ] {
            let json = serde_json::to_value(value_row(&value).unwrap()).unwrap();
            assert_eq!(json.as_object().expect("an object").len(), 1, "{json}");
        }
    }

    /// A double JSON cannot spell is refused by name rather than written as `null` and lost.
    #[test]
    fn a_non_finite_double_is_refused_rather_than_silently_nulled() {
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let refusal = value_row(&Value::Double(bad)).unwrap_err().to_string();
            assert!(refusal.contains("JSON cannot represent"), "{refusal}");
        }
        assert!(value_row(&Value::Double(-0.0)).is_ok());
    }
}
