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
//! ## v1 limitation
//!
//! SQLite is synchronous and these calls block the executor. A real deployment wants
//! `spawn_blocking` or an async driver; single-node v1 does not, and pretending otherwise would add
//! machinery without adding information.

use async_trait::async_trait;
use borg_core::{
    BorgError, BranchId, BufferId, CellRecord, CellRef, ClientVersion, DefEvent, Derivation,
    LayerId, Origin, ReadPath, Result, Value,
};
use borg_storage::{CellStream, OpenLayer, SealedLayer, StorageProvider};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;
use std::sync::{Arc, Mutex};

const OPEN: i64 = 0;
const SEALED: i64 = 1;
const COMMITTED: i64 = 2;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS layers (
    id     INTEGER PRIMARY KEY,
    branch INTEGER NOT NULL,
    state  INTEGER NOT NULL
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

#[derive(Clone)]
pub struct SqliteStorage {
    conn: Arc<Mutex<Connection>>,
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
}

/// One open layer. Rows stream straight into `cells`; they are invisible because this layer's row in
/// `layers` is not yet `COMMITTED`.
pub struct SqliteOpenLayer {
    id: LayerId,
    branch: BranchId,
    defs: u32,
    conn: Arc<Mutex<Connection>>,
}

#[async_trait]
impl OpenLayer for SqliteOpenLayer {
    fn id(&self) -> LayerId {
        self.id
    }

    async fn put_cell(&mut self, cell: &CellRef, record: CellRecord) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO cells
             (branch, buffer, cell_key, version, written_at, value, origin, derivation)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                self.branch.0 as i64,
                encode(&cell.buffer)?,
                encode(&cell.key)?,
                record.version.0.0 as i64,
                self.id.0 as i64,
                encode(&record.value)?,
                i64::from(record.origin == Origin::Derived),
                record.derivation.as_ref().map(encode).transpose()?,
            ],
        )
        .map_err(sql)?;
        Ok(())
    }

    async fn put_def(&mut self, event: DefEvent) -> Result<()> {
        {
            let conn = self.conn.lock().unwrap();
            conn.execute(
                "INSERT INTO def_events (layer, seq, event) VALUES (?1, ?2, ?3)",
                params![self.id.0 as i64, i64::from(self.defs), encode(&event)?],
            )
            .map_err(sql)?;
        }
        self.defs += 1;
        Ok(())
    }

    async fn seal(self: Box<Self>) -> Result<SealedLayer> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE layers SET state = ?1 WHERE id = ?2 AND state = ?3",
            params![SEALED, self.id.0 as i64, OPEN],
        )
        .map_err(sql)?;
        Ok(SealedLayer { id: self.id })
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        // Aborted rows are removed rather than left invisible: nothing will ever read them, and a
        // failed producer run may have written a great many.
        for statement in [
            "DELETE FROM cells WHERE written_at = ?1",
            "DELETE FROM def_events WHERE layer = ?1",
            "DELETE FROM layers WHERE id = ?1",
        ] {
            conn.execute(statement, params![self.id.0 as i64])
                .map_err(sql)?;
        }
        Ok(())
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
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare_cached(
                "SELECT c.value, c.version, c.written_at, c.origin, c.derivation
                 FROM cells c JOIN layers l ON c.written_at = l.id
                 WHERE c.branch = ?1 AND c.buffer = ?2 AND c.cell_key = ?3 AND c.version = ?4
                   AND l.state = ?5 AND c.written_at <= ?6
                 ORDER BY c.written_at DESC LIMIT 1",
            )
            .map_err(sql)?;

        let buffer = encode(&cell.buffer)?;
        let key = encode(&cell.key)?;
        // Walk outward. The first segment holding *any* record wins — including a tombstone, which
        // must stop the walk rather than fall through to the parent (SPEC.md §7.2).
        for (branch, bound) in &path.segments {
            let found = stmt
                .query_row(
                    params![
                        branch.0 as i64,
                        &buffer,
                        &key,
                        version.0.0 as i64,
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
                return Ok(Some(Self::to_record(row)?));
            }
        }
        Ok(None)
    }

    async fn cell_versions(&self, path: &ReadPath, cell: &CellRef) -> Result<Vec<ClientVersion>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare_cached(
                "SELECT DISTINCT c.version
                 FROM cells c JOIN layers l ON c.written_at = l.id
                 WHERE c.branch = ?1 AND c.buffer = ?2 AND c.cell_key = ?3
                   AND l.state = ?4 AND c.written_at <= ?5",
            )
            .map_err(sql)?;

        let buffer = encode(&cell.buffer)?;
        let key = encode(&cell.key)?;
        let mut versions = Vec::new();
        for (branch, bound) in &path.segments {
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
    }

    async fn scan_buffer(&self, path: &ReadPath, buffer: &BufferId) -> Result<CellStream> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare_cached(
                "SELECT c.cell_key, c.value, c.version, MAX(c.written_at), c.origin, c.derivation
                 FROM cells c JOIN layers l ON c.written_at = l.id
                 WHERE c.branch = ?1 AND c.buffer = ?2 AND l.state = ?3 AND c.written_at <= ?4
                 GROUP BY c.cell_key, c.version",
            )
            .map_err(sql)?;

        let encoded = encode(buffer)?;
        // A child's own record shadows the parent's, so remember what the inner segments covered.
        let mut seen: Vec<String> = Vec::new();
        let mut rows = Vec::new();
        for (branch, bound) in &path.segments {
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
                        buffer: buffer.clone(),
                        key: decode(&key)?,
                    },
                    Self::to_record((value, version, written_at, origin, derivation))?,
                )));
            }
            seen.extend(segment);
        }
        Ok(Box::pin(futures_util::stream::iter(rows)))
    }

    async fn read_layer(&self, layer: LayerId) -> Result<CellStream> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare_cached(
                "SELECT buffer, cell_key, value, version, written_at, origin, derivation
                 FROM cells WHERE written_at = ?1",
            )
            .map_err(sql)?;
        let found = stmt
            .query_map(params![layer.0 as i64], |row| {
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
            let (buffer, key, value, version, written_at, origin, derivation) = row.map_err(sql)?;
            rows.push(Ok((
                CellRef {
                    buffer: decode(&buffer)?,
                    key: decode(&key)?,
                },
                Self::to_record((value, version, written_at, origin, derivation))?,
            )));
        }
        Ok(Box::pin(futures_util::stream::iter(rows)))
    }

    async fn read_def_layer(&self, layer: LayerId) -> Result<Vec<DefEvent>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare_cached("SELECT event FROM def_events WHERE layer = ?1 ORDER BY seq")
            .map_err(sql)?;
        let found = stmt
            .query_map(params![layer.0 as i64], |row| row.get::<_, String>(0))
            .map_err(sql)?;
        let mut events = Vec::new();
        for row in found {
            events.push(decode::<DefEvent>(&row.map_err(sql)?)?);
        }
        Ok(events)
    }

    async fn open_layer(&self, branch: BranchId, id: LayerId) -> Result<Box<dyn OpenLayer>> {
        {
            let conn = self.conn.lock().unwrap();
            conn.execute(
                "INSERT INTO layers (id, branch, state) VALUES (?1, ?2, ?3)",
                params![id.0 as i64, branch.0 as i64, OPEN],
            )
            .map_err(|err| BorgError::Storage(format!("layer {id} already exists: {err}")))?;
        }
        Ok(Box::new(SqliteOpenLayer {
            id,
            branch,
            defs: 0,
            conn: Arc::clone(&self.conn),
        }))
    }

    async fn commit_layer(&self, layer: SealedLayer) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        // A single-row update, whatever the layer's size. Visibility is a join, not a rewrite.
        let updated = conn
            .execute(
                "UPDATE layers SET state = ?1 WHERE id = ?2 AND state = ?3",
                params![COMMITTED, layer.id.0 as i64, SEALED],
            )
            .map_err(sql)?;
        if updated == 0 {
            return Err(BorgError::Storage(format!(
                "layer {} is not sealed",
                layer.id
            )));
        }
        Ok(())
    }
}
