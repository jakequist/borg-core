//! # borg-storage-sqlite
//!
//! A `StorageProvider` backed by SQLite. SPEC.md §17.1.
//!
//! The point of this crate is to prove the seam: nothing about derivation, dependency tracking,
//! branches or watermarks appears here. It stores cells and def events, and walks whatever
//! [`ReadPath`] it is handed.
//!
//! ## How commit streams
//!
//! A layer may hold millions of mutations and can never be buffered whole (SPEC.md §6.2). So rows
//! are inserted as they arrive, and **visibility is a join, not a rewrite**: every read joins
//! `cells` against `layers` and keeps only rows whose layer is committed. Committing is therefore a
//! single-row update — `O(1)` however large the layer — and an open layer is invisible without any
//! row of its own being touched.
//!
//! The obvious alternative, flipping a `visible` flag on every row at commit, would make commit
//! `O(rows)` and undo the streaming property the interface exists to preserve.
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
    BorgError, Branch, BranchId, BufferId, CellRecord, CellRef, ClientVersion, DefEvent,
    Derivation, Layer, LayerId, Origin, ReadPath, Result, Value,
};
use borg_storage::{CellStream, OpenLayer, SealedLayer, StorageProvider};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;
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

CREATE TABLE IF NOT EXISTS cells (
    branch     INTEGER NOT NULL,
    buffer     TEXT    NOT NULL,
    cell_key   TEXT    NOT NULL,
    version    INTEGER NOT NULL,
    written_at INTEGER NOT NULL,
    value      TEXT    NOT NULL,
    origin     INTEGER NOT NULL,
    derivation TEXT,
    PRIMARY KEY (branch, buffer, cell_key, version, written_at)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS cells_by_layer  ON cells(written_at);
CREATE INDEX IF NOT EXISTS cells_by_buffer ON cells(branch, buffer);

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

/// The columns every cell read projects.
type CellRow = (String, i64, i64, i64, Option<String>);

fn to_record(row: CellRow) -> Result<CellRecord> {
    let (value, version, written_at, origin, derivation) = row;
    Ok(CellRecord {
        value: decode::<Value>(&value)?,
        version: ClientVersion(LayerId(version as u64)),
        written_at: LayerId(written_at as u64),
        origin: if origin == 0 {
            Origin::Source
        } else {
            Origin::Derived
        },
        derivation: derivation
            .map(|raw| decode::<Derivation>(&raw))
            .transpose()?,
    })
}

/// One cell write, already encoded, waiting to be flushed.
type PendingCell = (String, String, i64, String, i64, Option<String>);

#[derive(Clone)]
pub struct SqliteStorage {
    conn: Conn,
}

impl SqliteStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::from_connection(Connection::open(path).map_err(sql)?)
    }

    pub fn in_memory() -> Result<Self> {
        Self::from_connection(Connection::open_in_memory().map_err(sql)?)
    }

    fn from_connection(conn: Connection) -> Result<Self> {
        conn.execute_batch(SCHEMA).map_err(sql)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

/// One open layer. Rows accumulate in a bounded buffer, flush in transactions, and stay invisible
/// because this layer's row in `layers` is not yet `COMMITTED`.
pub struct SqliteOpenLayer {
    id: LayerId,
    branch: BranchId,
    defs: u32,
    pending: Vec<PendingCell>,
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
                let mut stmt = tx
                    .prepare_cached(
                        "INSERT OR REPLACE INTO cells
                         (branch, buffer, cell_key, version, written_at, value, origin, derivation)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    )
                    .map_err(sql)?;
                for (buffer, key, version, value, origin, derivation) in rows {
                    stmt.execute(params![
                        branch, buffer, key, version, layer, value, origin, derivation
                    ])
                    .map_err(sql)?;
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

    async fn put_cell(&mut self, cell: &CellRef, record: CellRecord) -> Result<()> {
        self.pending.push((
            encode(&cell.buffer)?,
            encode(&cell.key)?,
            record.version.0.0 as i64,
            encode(&record.value)?,
            i64::from(record.origin == Origin::Derived),
            record.derivation.as_ref().map(encode).transpose()?,
        ));
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
            for statement in [
                "DELETE FROM cells WHERE written_at = ?1",
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
    ) -> Result<Option<CellRecord>> {
        let segments = path.segments.clone();
        let buffer = encode(&cell.buffer)?;
        let key = encode(&cell.key)?;
        let version = version.0.0 as i64;

        with_conn(&self.conn, move |conn| {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT c.value, c.version, c.written_at, c.origin, c.derivation
                     FROM cells c JOIN layers l ON c.written_at = l.id
                     WHERE c.branch = ?1 AND c.buffer = ?2 AND c.cell_key = ?3 AND c.version = ?4
                       AND l.state = ?5 AND c.written_at <= ?6
                     ORDER BY c.written_at DESC LIMIT 1",
                )
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
                        |row| {
                            Ok((
                                row.get(0)?,
                                row.get(1)?,
                                row.get(2)?,
                                row.get(3)?,
                                row.get(4)?,
                            ))
                        },
                    )
                    .optional()
                    .map_err(sql)?;
                if let Some(row) = found {
                    return Ok(Some(to_record(row)?));
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
                    "SELECT DISTINCT c.version
                     FROM cells c JOIN layers l ON c.written_at = l.id
                     WHERE c.branch = ?1 AND c.buffer = ?2 AND c.cell_key = ?3
                       AND l.state = ?4 AND c.written_at <= ?5",
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

    async fn scan_buffer(&self, path: &ReadPath, buffer: &BufferId) -> Result<CellStream> {
        let segments = path.segments.clone();
        let target = buffer.clone();
        let encoded = encode(buffer)?;

        let rows = with_conn(&self.conn, move |conn| {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT c.cell_key, c.value, c.version, MAX(c.written_at), c.origin,
                            c.derivation
                     FROM cells c JOIN layers l ON c.written_at = l.id
                     WHERE c.branch = ?1 AND c.buffer = ?2 AND l.state = ?3 AND c.written_at <= ?4
                     GROUP BY c.cell_key, c.version",
                )
                .map_err(sql)?;

            // A child's own record shadows the parent's, so remember what the inner segments
            // covered.
            let mut seen: Vec<String> = Vec::new();
            let mut rows = Vec::new();
            for (branch, bound) in &segments {
                let found = stmt
                    .query_map(
                        params![branch.0 as i64, &encoded, COMMITTED, bound.0 as i64],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get(1)?,
                                row.get(2)?,
                                row.get(3)?,
                                row.get(4)?,
                                row.get(5)?,
                            ))
                        },
                    )
                    .map_err(sql)?;

                let mut segment = Vec::new();
                for row in found {
                    let (key, value, version, written_at, origin, derivation) = row.map_err(sql)?;
                    if seen.contains(&key) {
                        continue;
                    }
                    segment.push(key.clone());
                    rows.push(Ok((
                        CellRef {
                            buffer: target.clone(),
                            key: decode(&key)?,
                        },
                        to_record((value, version, written_at, origin, derivation))?,
                    )));
                }
                seen.extend(segment);
            }
            Ok(rows)
        })
        .await?;

        Ok(Box::pin(futures_util::stream::iter(rows)))
    }

    async fn read_layer(&self, layer: LayerId) -> Result<CellStream> {
        let id = layer.0 as i64;
        let rows = with_conn(&self.conn, move |conn| {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT buffer, cell_key, value, version, written_at, origin, derivation
                     FROM cells WHERE written_at = ?1",
                )
                .map_err(sql)?;
            let found = stmt
                .query_map(params![id], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                })
                .map_err(sql)?;

            let mut rows = Vec::new();
            for row in found {
                let (buffer, key, value, version, written_at, origin, derivation) =
                    row.map_err(sql)?;
                rows.push(Ok((
                    CellRef {
                        buffer: decode(&buffer)?,
                        key: decode(&key)?,
                    },
                    to_record((value, version, written_at, origin, derivation))?,
                )));
            }
            Ok(rows)
        })
        .await?;

        Ok(Box::pin(futures_util::stream::iter(rows)))
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
            pending: Vec::with_capacity(BATCH),
            conn: Arc::clone(&self.conn),
        }))
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
}
