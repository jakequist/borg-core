//! # borg-storage-sqlite
//!
//! A `StorageProvider` backed by SQLite. SPEC.md §17.1.
//!
//! The point of this crate is to prove the seam: nothing about derivation, dependency tracking,
//! branches or watermarks appears here. It stores cells and def events, and walks whatever
//! [`ReadPath`] it is handed.
//!
//! ## Three tables, and which of them is the log
//!
//! `events` and `layer_events` are the log: an event with an identity of its own, and the layers
//! that name it (SPEC.md §4.3, §6.2). `cell_index` is a **projection** of those two — the
//! materialised `(branch, cell, version) -> (layer, event)` that keeps a read one indexed seek once
//! events no longer carry the layer they live in. [`SqliteStorage::rebuild_read_index`] rebuilds it
//! from the log with one `INSERT ... SELECT`, which is what makes that claim checkable.
//!
//! ## How commit streams
//!
//! A layer may hold millions of mutations and can never be buffered whole (SPEC.md §6.2). So rows
//! are inserted as they arrive — index rows included — and **visibility is a join, not a rewrite**:
//! every read joins against `layers` and keeps only rows whose layer is committed. Committing is
//! therefore a single-row update, `O(1)` however large the layer, and an open layer is invisible
//! without any row of its own being touched.
//!
//! The obvious alternative, flipping a `visible` flag on every row at commit, would make commit
//! `O(rows)` and undo the streaming property the interface exists to preserve. It is also why the
//! index is maintained on the way in rather than at commit: an index built at commit would make
//! commit `O(rows)` just as surely.
//!
//! ## Blocking work stays off the async executor
//!
//! SQLite is synchronous, so every statement runs on Tokio's blocking pool via [`with_conn`]. The
//! `StorageProvider` trait is async and the whole engine above it already awaits, so this crate is
//! the only place that has to know SQLite blocks.
//!
//! Writes are **batched** rather than dispatched one at a time. `put_cell` is called an unbounded
//! number of times, and a `spawn_blocking` round-trip per cell would make dispatch overhead dominate
//! everything else. Rows accumulate into a bounded buffer and flush in a single transaction.
//!
//! Buffering is safe precisely because an open layer is invisible: nothing can observe the
//! difference between a row written immediately and one written at the next flush. The buffer is
//! bounded at [`BATCH`], so "never buffer a layer whole" still holds — a ten-million-cell layer
//! passes through in fixed memory.

use async_trait::async_trait;
use borg_core::{
    BorgError, Branch, BranchId, BufferId, CellRef, ClientVersion, DefEvent, Derivation, Event,
    EventDraft, EventId, Landed, Layer, LayerId, Origin, Pid, PidKind, ReadPath, Result, Value,
    content,
};
use borg_storage::{EventStream, OpenLayer, SealedLayer, StorageProvider};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const OPEN: i64 = 0;
const SEALED: i64 = 1;
const COMMITTED: i64 = 2;

/// How many cell writes accumulate before a flush.
///
/// Large enough that per-batch dispatch is negligible, small enough that memory stays flat however
/// large the layer.
const BATCH: usize = 512;

const SCHEMA: &str = "
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS layers (
    id     INTEGER PRIMARY KEY,
    branch INTEGER NOT NULL,
    state  INTEGER NOT NULL,
    -- Kind, author and guards are the log's shape rather than its contents, and are stored whole
    -- rather than picked apart into columns nothing queries on.
    meta   TEXT
);

CREATE TABLE IF NOT EXISTS branches (
    id     INTEGER PRIMARY KEY,
    branch TEXT NOT NULL
);

-- Events. **No branch, and no layer they live in** — only the layer that first committed them
-- (SPEC.md §4.3). A merged event gets a membership row on the parent and is not rewritten, which is
-- both what makes merge cheap and what keeps `authored` true afterwards.
CREATE TABLE IF NOT EXISTS events (
    id         INTEGER PRIMARY KEY,
    buffer     TEXT    NOT NULL,
    cell_key   TEXT    NOT NULL,
    version    INTEGER NOT NULL,
    value      TEXT    NOT NULL,
    origin     INTEGER NOT NULL,
    derivation TEXT,
    authored   INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS events_by_author ON events(authored);

-- Membership: a layer is an ordered group of events, and many layers may name one event.
CREATE TABLE IF NOT EXISTS layer_events (
    layer INTEGER NOT NULL,
    seq   INTEGER NOT NULL,
    event INTEGER NOT NULL,
    PRIMARY KEY (layer, seq)
) WITHOUT ROWID;

-- The read index — a projection of the two tables above, and the only reason a read stays a single
-- lookup now that an event does not carry a layer. `landed` is the layer *on this branch* whose
-- membership carried the event here, which for a merged event is not `events.authored`.
--
-- **One row per landing**, so `MAX(landed)` is never a tie and every read resolves the same way in
-- both backends. A layer that writes one cell twice keeps both events in its membership — it really
-- does contain two — and replaces its one index row, because only the later write can be the answer.
CREATE TABLE IF NOT EXISTS cell_index (
    branch   INTEGER NOT NULL,
    buffer   TEXT    NOT NULL,
    cell_key TEXT    NOT NULL,
    version  INTEGER NOT NULL,
    landed   INTEGER NOT NULL,
    event    INTEGER NOT NULL,
    PRIMARY KEY (branch, buffer, cell_key, version, landed)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS cell_index_by_layer  ON cell_index(landed);
CREATE INDEX IF NOT EXISTS cell_index_by_buffer ON cell_index(branch, buffer);

-- Interned values — the contents of the String, Binary and BigInt buffers (SPEC.md §3.1, §4.2).
--
-- **No branch column, no layer, no version.** That is the point of the table rather than an
-- omission: a content-addressed PID is branch-independent and eternal, so there is exactly one row
-- per distinct value registry-wide, a string write can never conflict across branches, and there is
-- no history for a read to travel through. Nothing here joins against `layers`.
--
-- `kind` is a column rather than being folded into the hash so that `hash` stays reproducible from
-- the bytes alone — `printf 'hello' | sha256sum` names the PID. It is part of the key because a
-- String and a Binary of the same octets are different values sharing one preimage.
--
-- A rowid table, unlike the tables above: an interned value is arbitrary-length content, and
-- WITHOUT ROWID would carry that payload inside the index B-tree it is keyed by.
CREATE TABLE IF NOT EXISTS interned (
    kind  INTEGER NOT NULL,
    hash  BLOB    NOT NULL,
    bytes BLOB    NOT NULL,
    PRIMARY KEY (kind, hash)
);

CREATE TABLE IF NOT EXISTS def_events (
    layer INTEGER NOT NULL,
    seq   INTEGER NOT NULL,
    event TEXT    NOT NULL,
    PRIMARY KEY (layer, seq)
);
";

type Conn = Arc<Mutex<Connection>>;

/// Run blocking SQLite work on Tokio's blocking pool.
///
/// Every statement in this crate goes through here, so no database call ever occupies an async
/// worker thread.
async fn with_conn<T, F>(conn: &Conn, work: F) -> Result<T>
where
    F: FnOnce(&Connection) -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    let conn = Arc::clone(conn);
    tokio::task::spawn_blocking(move || {
        let guard = conn.lock().unwrap();
        work(&guard)
    })
    .await
    .map_err(sql)?
}

fn sql<E: std::fmt::Display>(err: E) -> BorgError {
    BorgError::Storage(err.to_string())
}

fn encode<T: serde::Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).map_err(sql)
}

fn decode<T: serde::de::DeserializeOwned>(raw: &str) -> Result<T> {
    serde_json::from_str(raw).map_err(sql)
}

/// The columns every event read projects: id, buffer, cell_key, value, version, origin, derivation,
/// authored.
type EventRow = (i64, String, String, String, i64, i64, Option<String>, i64);

fn to_event(row: EventRow) -> Result<Event> {
    let (id, buffer, key, value, version, origin, derivation, authored) = row;
    Ok(Event {
        id: EventId(id as u64),
        cell: CellRef {
            buffer: decode(&buffer)?,
            key: decode(&key)?,
        },
        value: decode::<Value>(&value)?,
        version: ClientVersion(LayerId(version as u64)),
        origin: if origin == 0 {
            Origin::Source
        } else {
            Origin::Derived
        },
        derivation: derivation
            .map(|raw| decode::<Derivation>(&raw))
            .transpose()?,
        authored: LayerId(authored as u64),
    })
}

/// The eight columns of an event, in the order [`to_event`] expects them.
const EVENT_COLUMNS: &str =
    "e.id, e.buffer, e.cell_key, e.value, e.version, e.origin, e.derivation, e.authored";

fn read_event(row: &rusqlite::Row<'_>, from: usize) -> rusqlite::Result<EventRow> {
    Ok((
        row.get(from)?,
        row.get(from + 1)?,
        row.get(from + 2)?,
        row.get(from + 3)?,
        row.get(from + 4)?,
        row.get(from + 5)?,
        row.get(from + 6)?,
        row.get(from + 7)?,
    ))
}

/// One membership row waiting to be flushed: its position in the layer, the event it names, and —
/// for an event this layer is authoring — the event itself, still uninserted.
struct Pending {
    seq: i64,
    event: i64,
    /// `(buffer, cell_key, version, value, origin, derivation)`, or `None` when the event already
    /// exists and this layer is only naming it.
    authored: Option<(String, String, i64, String, i64, Option<String>)>,
}

#[derive(Clone)]
pub struct SqliteStorage {
    conn: Conn,
    /// The next event id to mint. Seeded from the store on open, exactly as the engine resumes layer
    /// ids, so that a second process cannot reuse an id that already exists. A collision would fail
    /// the insert rather than corrupt anything, because `events.id` is a primary key.
    next_event: Arc<AtomicU64>,
}

impl SqliteStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::from_connection(Connection::open(path).map_err(sql)?)
    }

    pub fn in_memory() -> Result<Self> {
        Self::from_connection(Connection::open_in_memory().map_err(sql)?)
    }

    fn from_connection(conn: Connection) -> Result<Self> {
        // A store written before events had identity holds its data in a `cells` table this schema
        // no longer reads. Saying so is the only kind thing to do: `CREATE TABLE IF NOT EXISTS`
        // would otherwise open it successfully and report an empty world.
        let legacy: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'cells'",
                [],
                |row| row.get(0),
            )
            .map_err(sql)?;
        if legacy > 0 {
            return Err(BorgError::Storage(
                "this store predates the event model (SPEC.md §4.3): its cells carry the layer they \
                 live in and cannot be read. Create a new store."
                    .into(),
            ));
        }
        conn.execute_batch(SCHEMA).map_err(sql)?;
        let highest: i64 = conn
            .query_row("SELECT COALESCE(MAX(id), 0) FROM events", [], |row| {
                row.get(0)
            })
            .map_err(sql)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            next_event: Arc::new(AtomicU64::new(highest as u64 + 1)),
        })
    }
}

/// One open layer. Rows accumulate in a bounded buffer, flush in transactions, and stay invisible
/// because this layer's row in `layers` is not yet `COMMITTED`.
pub struct SqliteOpenLayer {
    id: LayerId,
    branch: BranchId,
    defs: u32,
    seq: i64,
    pending: Vec<Pending>,
    next_event: Arc<AtomicU64>,
    conn: Conn,
}

impl SqliteOpenLayer {
    async fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.pending);
        let branch = self.branch.0 as i64;
        let layer = self.id.0 as i64;
        with_conn(&self.conn, move |conn| {
            let tx = conn.unchecked_transaction().map_err(sql)?;
            {
                let mut event = tx
                    .prepare_cached(
                        "INSERT INTO events
                         (id, buffer, cell_key, version, value, origin, derivation, authored)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    )
                    .map_err(sql)?;
                let mut member = tx
                    .prepare_cached(
                        "INSERT INTO layer_events (layer, seq, event) VALUES (?1, ?2, ?3)",
                    )
                    .map_err(sql)?;
                // The index row is derived from the event rather than from the caller, so an
                // included event indexes under the cell it actually names and a layer cannot lie
                // about what it landed.
                let mut index = tx
                    .prepare_cached(
                        "INSERT OR REPLACE INTO cell_index
                         (branch, buffer, cell_key, version, landed, event)
                         SELECT ?1, buffer, cell_key, version, ?2, id
                         FROM events WHERE id = ?3",
                    )
                    .map_err(sql)?;
                for row in rows {
                    if let Some((buffer, key, version, value, origin, derivation)) = row.authored {
                        event
                            .execute(params![
                                row.event, buffer, key, version, value, origin, derivation, layer
                            ])
                            .map_err(sql)?;
                    }
                    member
                        .execute(params![layer, row.seq, row.event])
                        .map_err(sql)?;
                    // Indexing nothing means the named event does not exist. Checking the row count
                    // costs nothing and is what keeps `include_event` from silently writing a
                    // membership row that resolves to no value — a probe per included event would
                    // be a round trip per merged cell, which is exactly the cost merge is shedding.
                    if index
                        .execute(params![branch, layer, row.event])
                        .map_err(sql)?
                        == 0
                    {
                        return Err(BorgError::Storage(format!("unknown event e{}", row.event)));
                    }
                }
            }
            tx.commit().map_err(sql)
        })
        .await
    }
}

#[async_trait]
impl OpenLayer for SqliteOpenLayer {
    fn id(&self) -> LayerId {
        self.id
    }

    async fn author_event(&mut self, cell: &CellRef, draft: EventDraft) -> Result<EventId> {
        let id = EventId(self.next_event.fetch_add(1, Ordering::Relaxed));
        self.pending.push(Pending {
            seq: self.seq,
            event: id.0 as i64,
            authored: Some((
                encode(&cell.buffer)?,
                encode(&cell.key)?,
                draft.version.0.0 as i64,
                encode(&draft.value)?,
                i64::from(draft.origin == Origin::Derived),
                draft.derivation.as_ref().map(encode).transpose()?,
            )),
        });
        self.seq += 1;
        if self.pending.len() >= BATCH {
            self.flush().await?;
        }
        Ok(id)
    }

    async fn include_event(&mut self, event: EventId) -> Result<()> {
        self.pending.push(Pending {
            seq: self.seq,
            event: event.0 as i64,
            authored: None,
        });
        self.seq += 1;
        if self.pending.len() >= BATCH {
            self.flush().await?;
        }
        Ok(())
    }

    async fn put_def(&mut self, event: DefEvent) -> Result<()> {
        let layer = self.id.0 as i64;
        let seq = i64::from(self.defs);
        let encoded = encode(&event)?;
        with_conn(&self.conn, move |conn| {
            conn.execute(
                "INSERT INTO def_events (layer, seq, event) VALUES (?1, ?2, ?3)",
                params![layer, seq, encoded],
            )
            .map_err(sql)?;
            Ok(())
        })
        .await?;
        self.defs += 1;
        Ok(())
    }

    async fn seal(mut self: Box<Self>) -> Result<SealedLayer> {
        self.flush().await?;
        let id = self.id;
        with_conn(&self.conn, move |conn| {
            conn.execute(
                "UPDATE layers SET state = ?1 WHERE id = ?2 AND state = ?3",
                params![SEALED, id.0 as i64, OPEN],
            )
            .map_err(sql)?;
            Ok(())
        })
        .await?;
        Ok(SealedLayer { id })
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        // Anything unflushed simply evaporates; anything already flushed is deleted. Nothing was
        // ever visible either way.
        self.pending.clear();
        let id = self.id.0 as i64;
        with_conn(&self.conn, move |conn| {
            // Events *authored* here go; events merely named here do not — they belong to the layer
            // that authored them and are still named by it.
            for statement in [
                "DELETE FROM cell_index WHERE landed = ?1",
                "DELETE FROM layer_events WHERE layer = ?1",
                "DELETE FROM events WHERE authored = ?1",
                "DELETE FROM def_events WHERE layer = ?1",
                "DELETE FROM layers WHERE id = ?1",
            ] {
                conn.execute(statement, params![id]).map_err(sql)?;
            }
            Ok(())
        })
        .await
    }
}

#[async_trait]
impl StorageProvider for SqliteStorage {
    async fn get_cell(
        &self,
        path: &ReadPath,
        cell: &CellRef,
        version: ClientVersion,
    ) -> Result<Option<Landed>> {
        let segments = path.segments.clone();
        let buffer = encode(&cell.buffer)?;
        let key = encode(&cell.key)?;
        let version = version.0.0 as i64;

        with_conn(&self.conn, move |conn| {
            // One seek down `cell_index`'s primary key per segment, however many layers the branch
            // has and however many of them merged this cell in. `landed`, never `authored`: a merged
            // event was written on another branch at an id that says nothing about when it became
            // visible here.
            let mut stmt = conn
                .prepare_cached(&format!(
                    "SELECT {EVENT_COLUMNS}, i.landed
                     FROM cell_index i JOIN events e ON i.event = e.id
                                       JOIN layers l ON i.landed = l.id
                     WHERE i.branch = ?1 AND i.buffer = ?2 AND i.cell_key = ?3 AND i.version = ?4
                       AND l.state = ?5 AND i.landed <= ?6
                     ORDER BY i.landed DESC LIMIT 1"
                ))
                .map_err(sql)?;

            // Walk outward. The first segment holding *any* record wins — including a tombstone,
            // which must stop the walk rather than fall through to the parent (SPEC.md §7.2).
            for (branch, bound) in &segments {
                let found = stmt
                    .query_row(
                        params![
                            branch.0 as i64,
                            &buffer,
                            &key,
                            version,
                            COMMITTED,
                            bound.0 as i64
                        ],
                        |row| Ok((read_event(row, 0)?, row.get::<_, i64>(8)?)),
                    )
                    .optional()
                    .map_err(sql)?;
                if let Some((event, landed)) = found {
                    return Ok(Some(Landed {
                        event: to_event(event)?,
                        landed_at: LayerId(landed as u64),
                    }));
                }
            }
            Ok(None)
        })
        .await
    }

    async fn cell_versions(&self, path: &ReadPath, cell: &CellRef) -> Result<Vec<ClientVersion>> {
        let segments = path.segments.clone();
        let buffer = encode(&cell.buffer)?;
        let key = encode(&cell.key)?;

        with_conn(&self.conn, move |conn| {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT DISTINCT i.version
                     FROM cell_index i JOIN layers l ON i.landed = l.id
                     WHERE i.branch = ?1 AND i.buffer = ?2 AND i.cell_key = ?3
                       AND l.state = ?4 AND i.landed <= ?5",
                )
                .map_err(sql)?;

            let mut versions = Vec::new();
            for (branch, bound) in &segments {
                let rows = stmt
                    .query_map(
                        params![branch.0 as i64, &buffer, &key, COMMITTED, bound.0 as i64],
                        |row| row.get::<_, i64>(0),
                    )
                    .map_err(sql)?;
                for row in rows {
                    let version = ClientVersion(LayerId(row.map_err(sql)? as u64));
                    if !versions.contains(&version) {
                        versions.push(version);
                    }
                }
            }
            Ok(versions)
        })
        .await
    }

    async fn scan_buffer(&self, path: &ReadPath, buffer: &BufferId) -> Result<EventStream> {
        let segments = path.segments.clone();
        let encoded = encode(buffer)?;

        let rows = with_conn(&self.conn, move |conn| {
            // `MAX(i.landed)` picks the bare columns from the row it maximises, and there is exactly
            // one index row per landing, so the choice is never a tie — which is what makes this
            // agree with `MemoryStorage` rather than merely usually agree.
            let mut stmt = conn
                .prepare_cached(&format!(
                    "SELECT {EVENT_COLUMNS}, MAX(i.landed)
                     FROM cell_index i JOIN events e ON i.event = e.id
                                       JOIN layers l ON i.landed = l.id
                     WHERE i.branch = ?1 AND i.buffer = ?2 AND l.state = ?3 AND i.landed <= ?4
                     GROUP BY i.cell_key, i.version"
                ))
                .map_err(sql)?;

            // A child's own record shadows the parent's, so remember what the inner segments
            // covered.
            let mut seen: Vec<String> = Vec::new();
            let mut rows: Vec<Event> = Vec::new();
            for (branch, bound) in &segments {
                let found = stmt
                    .query_map(
                        params![branch.0 as i64, &encoded, COMMITTED, bound.0 as i64],
                        |row| read_event(row, 0),
                    )
                    .map_err(sql)?;

                let mut segment = Vec::new();
                for row in found {
                    let event = row.map_err(sql)?;
                    if seen.contains(&event.2) {
                        continue;
                    }
                    segment.push(event.2.clone());
                    rows.push(to_event(event)?);
                }
                seen.extend(segment);
            }
            Ok(rows)
        })
        .await?;

        Ok(Box::pin(futures_util::stream::iter(
            rows.into_iter().map(Ok),
        )))
    }

    async fn read_layer(&self, layer: LayerId) -> Result<EventStream> {
        let id = layer.0 as i64;
        let rows = with_conn(&self.conn, move |conn| {
            // Membership, in order — including events this layer named rather than authored, which
            // is what a merge layer is made of.
            let mut stmt = conn
                .prepare_cached(&format!(
                    "SELECT {EVENT_COLUMNS}
                     FROM layer_events m JOIN events e ON m.event = e.id
                     WHERE m.layer = ?1 ORDER BY m.seq"
                ))
                .map_err(sql)?;
            let found = stmt
                .query_map(params![id], |row| read_event(row, 0))
                .map_err(sql)?;

            let mut rows = Vec::new();
            for row in found {
                rows.push(to_event(row.map_err(sql)?)?);
            }
            Ok(rows)
        })
        .await?;

        Ok(Box::pin(futures_util::stream::iter(
            rows.into_iter().map(Ok),
        )))
    }

    async fn put_layer_meta(&self, layer: &Layer) -> Result<()> {
        let id = layer.id.0 as i64;
        let branch = layer.branch.0 as i64;
        let meta = encode(layer)?;
        with_conn(&self.conn, move |conn| {
            conn.execute(
                "INSERT INTO layers (id, branch, state, meta) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(id) DO UPDATE SET meta = excluded.meta",
                params![id, branch, OPEN, meta],
            )
            .map_err(sql)?;
            Ok(())
        })
        .await
    }

    async fn read_layers(&self) -> Result<Vec<Layer>> {
        with_conn(&self.conn, move |conn| {
            let mut stmt = conn
                .prepare("SELECT meta, state FROM layers WHERE meta IS NOT NULL ORDER BY id")
                .map_err(sql)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                })
                .map_err(sql)?;
            let mut layers = Vec::new();
            for row in rows {
                let (meta, state) = row.map_err(sql)?;
                let mut layer: Layer = decode(&meta)?;
                // The `state` column is authoritative: commit updates it without rewriting `meta`,
                // because a commit must stay O(1).
                layer.state = match state {
                    COMMITTED => borg_core::LayerState::Committed,
                    SEALED => borg_core::LayerState::Sealed,
                    _ => borg_core::LayerState::Open,
                };
                layers.push(layer);
            }
            Ok(layers)
        })
        .await
    }

    async fn put_branch(&self, branch: &Branch) -> Result<()> {
        let id = branch.id.0 as i64;
        let encoded = encode(branch)?;
        with_conn(&self.conn, move |conn| {
            conn.execute(
                "INSERT INTO branches (id, branch) VALUES (?1, ?2)
                 ON CONFLICT(id) DO UPDATE SET branch = excluded.branch",
                params![id, encoded],
            )
            .map_err(sql)?;
            Ok(())
        })
        .await
    }

    async fn read_branches(&self) -> Result<Vec<Branch>> {
        with_conn(&self.conn, move |conn| {
            let mut stmt = conn.prepare("SELECT branch FROM branches").map_err(sql)?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(sql)?;
            let mut branches = Vec::new();
            for row in rows {
                branches.push(decode::<Branch>(&row.map_err(sql)?)?);
            }
            Ok(branches)
        })
        .await
    }

    async fn read_def_layer(&self, layer: LayerId) -> Result<Vec<DefEvent>> {
        let id = layer.0 as i64;
        with_conn(&self.conn, move |conn| {
            let mut stmt = conn
                .prepare_cached("SELECT event FROM def_events WHERE layer = ?1 ORDER BY seq")
                .map_err(sql)?;
            let found = stmt
                .query_map(params![id], |row| row.get::<_, String>(0))
                .map_err(sql)?;
            let mut events = Vec::new();
            for row in found {
                events.push(decode::<DefEvent>(&row.map_err(sql)?)?);
            }
            Ok(events)
        })
        .await
    }

    async fn open_layer(&self, branch: BranchId, id: LayerId) -> Result<Box<dyn OpenLayer>> {
        let layer = id.0 as i64;
        let owner = branch.0 as i64;
        with_conn(&self.conn, move |conn| {
            conn.execute(
                "INSERT INTO layers (id, branch, state) VALUES (?1, ?2, ?3)",
                params![layer, owner, OPEN],
            )
            .map_err(|err| BorgError::Storage(format!("layer {layer} already exists: {err}")))?;
            Ok(())
        })
        .await?;

        Ok(Box::new(SqliteOpenLayer {
            id,
            branch,
            defs: 0,
            seq: 0,
            pending: Vec::with_capacity(BATCH),
            next_event: Arc::clone(&self.next_event),
            conn: Arc::clone(&self.conn),
        }))
    }

    async fn intern(&self, kind: PidKind, bytes: &[u8]) -> Result<Pid> {
        let pid = content::pid(kind, bytes)?;
        let hash = content::hash_of(&pid)?.to_vec();
        // The discriminant, not a JSON tag: `PidKind`'s values are assigned explicitly in
        // `borg-core` precisely because this is a persisted format.
        let tag = i64::from(kind as u8);
        let payload = bytes.to_vec();
        with_conn(&self.conn, move |conn| {
            // OR IGNORE, not OR REPLACE: an existing row necessarily holds these exact bytes, so
            // rewriting it would be pure cost. This is what makes interning idempotent in storage
            // as well as in identity.
            conn.execute(
                "INSERT OR IGNORE INTO interned (kind, hash, bytes) VALUES (?1, ?2, ?3)",
                params![tag, hash, payload],
            )
            .map_err(sql)?;
            Ok(())
        })
        .await?;
        Ok(pid)
    }

    async fn read_interned(&self, pid: &Pid) -> Result<Option<Vec<u8>>> {
        // Rejects an allocated PID rather than answering `None` — there is no row it could name, so
        // a miss would be a lie about a caller bug.
        let hash = content::hash_of(pid)?.to_vec();
        let tag = i64::from(pid.kind() as u8);
        with_conn(&self.conn, move |conn| {
            // No join against `layers`, and no `ReadPath` walk: an interned value is visible to
            // every branch the moment it exists (SPEC.md §3.1).
            conn.prepare_cached("SELECT bytes FROM interned WHERE kind = ?1 AND hash = ?2")
                .map_err(sql)?
                .query_row(params![tag, hash], |row| row.get::<_, Vec<u8>>(0))
                .optional()
                .map_err(sql)
        })
        .await
    }

    async fn commit_layer(&self, layer: SealedLayer) -> Result<()> {
        let id = layer.id;
        with_conn(&self.conn, move |conn| {
            // A single-row update, whatever the layer's size. Visibility is a join, not a rewrite.
            let updated = conn
                .execute(
                    "UPDATE layers SET state = ?1 WHERE id = ?2 AND state = ?3",
                    params![COMMITTED, id.0 as i64, SEALED],
                )
                .map_err(sql)?;
            if updated == 0 {
                return Err(BorgError::Storage(format!("layer {id} is not sealed")));
            }
            Ok(())
        })
        .await
    }

    async fn rebuild_read_index(&self) -> Result<()> {
        with_conn(&self.conn, move |conn| {
            let tx = conn.unchecked_transaction().map_err(sql)?;
            tx.execute("DELETE FROM cell_index", []).map_err(sql)?;
            // The whole index in one statement, which is the clearest possible statement of what it
            // is: layer membership joined to the events it names, keyed by the branch each layer
            // belongs to. Replayed in `(layer, seq)` order so that a layer writing one cell twice
            // collapses to the same row it collapsed to on the way in.
            tx.execute(
                "INSERT OR REPLACE INTO cell_index
                 (branch, buffer, cell_key, version, landed, event)
                 SELECT l.branch, e.buffer, e.cell_key, e.version, m.layer, m.event
                 FROM layer_events m JOIN events e ON m.event = e.id
                                     JOIN layers l ON m.layer = l.id
                 ORDER BY m.layer, m.seq",
                [],
            )
            .map_err(sql)?;
            tx.commit().map_err(sql)
        })
        .await
    }
}
