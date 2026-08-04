//! An in-memory `StorageProvider`.
//!
//! Exists to get the derivation cycle running end-to-end on the thinnest possible substrate — that
//! loop is the only genuinely unproven part of the design, and everything else can wait behind it.
//! Not durable, not sharded, not efficient.
//!
//! It does honour the two constraints that actually shape the interface: writes stream into an
//! invisible open layer, and nothing becomes visible until commit.

use crate::{CellStream, OpenLayer, SealedLayer, StorageProvider};
use async_trait::async_trait;
use borg_core::{
    BorgError, Branch, BranchId, BufferId, CellRecord, CellRef, ClientVersion, DefEvent, Layer,
    LayerId, Pid, PidKind, ReadPath, Result, content,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type Committed = HashMap<BranchId, HashMap<(CellRef, ClientVersion), Vec<(LayerId, CellRecord)>>>;

#[derive(Default)]
struct Inner {
    /// Per cell, the history of writes in layer order. Time travel is a binary search over this.
    committed: Committed,
    /// Open and sealed layers, invisible to readers until commit.
    staged: HashMap<LayerId, StagedLayer>,
    /// A committed layer's contents — the changeset that drives invalidation (SPEC.md §9.6).
    layer_contents: HashMap<LayerId, Vec<(CellRef, CellRecord)>>,
    /// A committed def layer's events.
    def_contents: HashMap<LayerId, Vec<DefEvent>>,
    layer_meta: HashMap<LayerId, Layer>,
    branches: HashMap<BranchId, Branch>,
    /// Interned values, keyed by their PID and nothing else — no branch, no layer, no version
    /// (SPEC.md §3.1). One map rather than three because the PID carries its own kind, which is
    /// what keeps `String("x")` and `Binary("x")` apart despite sharing a hash.
    interned: HashMap<Pid, Vec<u8>>,
}

struct StagedLayer {
    branch: BranchId,
    writes: Vec<(CellRef, CellRecord)>,
    defs: Vec<DefEvent>,
    sealed: bool,
}

#[derive(Clone, Default)]
pub struct MemoryStorage {
    inner: Arc<Mutex<Inner>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

/// A handle to one open layer. Writes accumulate here and are invisible to every reader until the
/// layer commits (SPEC.md §6.2).
pub struct MemoryOpenLayer {
    id: LayerId,
    inner: Arc<Mutex<Inner>>,
}

#[async_trait]
impl OpenLayer for MemoryOpenLayer {
    fn id(&self) -> LayerId {
        self.id
    }

    async fn put_cell(&mut self, cell: &CellRef, record: CellRecord) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let staged = inner
            .staged
            .get_mut(&self.id)
            .ok_or_else(|| BorgError::Storage(format!("layer {} is not open", self.id)))?;
        if staged.sealed {
            return Err(BorgError::Storage(format!("layer {} is sealed", self.id)));
        }
        staged.writes.push((cell.clone(), record));
        Ok(())
    }

    async fn put_def(&mut self, event: DefEvent) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let staged = inner
            .staged
            .get_mut(&self.id)
            .ok_or_else(|| BorgError::Storage(format!("layer {} is not open", self.id)))?;
        if staged.sealed {
            return Err(BorgError::Storage(format!("layer {} is sealed", self.id)));
        }
        staged.defs.push(event);
        Ok(())
    }

    async fn seal(self: Box<Self>) -> Result<SealedLayer> {
        let mut inner = self.inner.lock().unwrap();
        let staged = inner
            .staged
            .get_mut(&self.id)
            .ok_or_else(|| BorgError::Storage(format!("layer {} is not open", self.id)))?;
        staged.sealed = true;
        Ok(SealedLayer { id: self.id })
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        self.inner.lock().unwrap().staged.remove(&self.id);
        Ok(())
    }
}

#[async_trait]
impl StorageProvider for MemoryStorage {
    async fn get_cell(
        &self,
        path: &ReadPath,
        cell: &CellRef,
        version: ClientVersion,
    ) -> Result<Option<CellRecord>> {
        let inner = self.inner.lock().unwrap();
        // Walk outward. The first segment holding *any* record wins — including a tombstone, which
        // must stop the walk rather than fall through and resurrect the parent's value.
        for (branch, bound) in &path.segments {
            let found = inner
                .committed
                .get(branch)
                .and_then(|cells| cells.get(&(cell.clone(), version)))
                .and_then(|history| {
                    history
                        .iter()
                        .rev()
                        .find(|(written_at, _)| written_at.0 <= bound.0)
                        .map(|(_, record)| record.clone())
                });
            if found.is_some() {
                return Ok(found);
            }
        }
        Ok(None)
    }

    async fn cell_versions(&self, path: &ReadPath, cell: &CellRef) -> Result<Vec<ClientVersion>> {
        let inner = self.inner.lock().unwrap();
        let mut versions = Vec::new();
        for (branch, bound) in &path.segments {
            for ((c, version), history) in inner.committed.get(branch).into_iter().flatten() {
                if c == cell
                    && !versions.contains(version)
                    && history.iter().any(|(at, _)| at.0 <= bound.0)
                {
                    versions.push(*version);
                }
            }
        }
        Ok(versions)
    }

    async fn scan_buffer(&self, path: &ReadPath, buffer: &BufferId) -> Result<CellStream> {
        let inner = self.inner.lock().unwrap();
        // A child's own record shadows the parent's, so remember what the inner segments covered.
        let mut seen: Vec<CellRef> = Vec::new();
        let mut rows = Vec::new();
        for (branch, bound) in &path.segments {
            for ((cell, _), history) in inner.committed.get(branch).into_iter().flatten() {
                if &cell.buffer != buffer || seen.contains(cell) {
                    continue;
                }
                if let Some((_, record)) = history
                    .iter()
                    .rev()
                    .find(|(written_at, _)| written_at.0 <= bound.0)
                {
                    seen.push(cell.clone());
                    rows.push(Ok((cell.clone(), record.clone())));
                }
            }
        }
        Ok(Box::pin(futures_util::stream::iter(rows)))
    }

    async fn read_layer(&self, layer: LayerId) -> Result<CellStream> {
        let inner = self.inner.lock().unwrap();
        let rows: Vec<_> = inner
            .layer_contents
            .get(&layer)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(Ok)
            .collect();
        Ok(Box::pin(futures_util::stream::iter(rows)))
    }

    async fn put_layer_meta(&self, layer: &Layer) -> Result<()> {
        self.inner
            .lock()
            .unwrap()
            .layer_meta
            .insert(layer.id, layer.clone());
        Ok(())
    }

    async fn read_layers(&self) -> Result<Vec<Layer>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .layer_meta
            .values()
            .cloned()
            .collect())
    }

    async fn put_branch(&self, branch: &Branch) -> Result<()> {
        self.inner
            .lock()
            .unwrap()
            .branches
            .insert(branch.id, branch.clone());
        Ok(())
    }

    async fn read_branches(&self) -> Result<Vec<Branch>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .branches
            .values()
            .cloned()
            .collect())
    }

    async fn read_def_layer(&self, layer: LayerId) -> Result<Vec<DefEvent>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.def_contents.get(&layer).cloned().unwrap_or_default())
    }

    async fn open_layer(&self, branch: BranchId, id: LayerId) -> Result<Box<dyn OpenLayer>> {
        let mut inner = self.inner.lock().unwrap();
        if inner.staged.contains_key(&id) || inner.layer_contents.contains_key(&id) {
            return Err(BorgError::Storage(format!("layer {id} already exists")));
        }
        inner.staged.insert(
            id,
            StagedLayer {
                branch,
                writes: Vec::new(),
                defs: Vec::new(),
                sealed: false,
            },
        );
        Ok(Box::new(MemoryOpenLayer {
            id,
            inner: Arc::clone(&self.inner),
        }))
    }

    async fn intern(&self, kind: PidKind, bytes: &[u8]) -> Result<Pid> {
        let pid = content::pid(kind, bytes)?;
        // Re-interning is a no-op rather than an overwrite: an occupied entry already holds these
        // exact bytes, so rewriting it is pure cost.
        self.inner
            .lock()
            .unwrap()
            .interned
            .entry(pid)
            .or_insert_with(|| bytes.to_vec());
        Ok(pid)
    }

    async fn read_interned(&self, pid: &Pid) -> Result<Option<Vec<u8>>> {
        // Rejects an allocated PID rather than answering `None` — there is no interned value it
        // could name, so a miss would be a lie about a caller bug.
        content::hash_of(pid)?;
        Ok(self.inner.lock().unwrap().interned.get(pid).cloned())
    }

    async fn commit_layer(&self, layer: SealedLayer) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let staged = inner
            .staged
            .remove(&layer.id)
            .ok_or_else(|| BorgError::Storage(format!("layer {} is not staged", layer.id)))?;
        if !staged.sealed {
            return Err(BorgError::Storage(format!(
                "layer {} is not sealed",
                layer.id
            )));
        }
        let branch_cells = inner.committed.entry(staged.branch).or_default();
        for (cell, record) in &staged.writes {
            branch_cells
                .entry((cell.clone(), record.version))
                .or_default()
                .push((layer.id, record.clone()));
        }
        inner.layer_contents.insert(layer.id, staged.writes);
        inner.def_contents.insert(layer.id, staged.defs);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use borg_core::{AllocatorId, CellKey, Origin, Value};

    const MAIN: BranchId = BranchId(1);
    const FEATURE: BranchId = BranchId(2);
    const V1: ClientVersion = ClientVersion(LayerId(1));

    fn prop(n: u64, field: &str) -> CellRef {
        CellRef::prop(
            "Company".into(),
            field.into(),
            Pid::Allocated {
                kind: PidKind::Object,
                branch: MAIN,
                allocator: AllocatorId(0),
                counter: n,
            },
        )
    }

    fn record(value: Value, at: LayerId) -> CellRecord {
        CellRecord {
            value,
            version: V1,
            written_at: at,
            origin: Origin::Source,
            derivation: None,
        }
    }

    async fn commit(
        storage: &MemoryStorage,
        branch: BranchId,
        id: LayerId,
        writes: &[(CellRef, CellRecord)],
    ) -> Result<()> {
        let mut layer = storage.open_layer(branch, id).await?;
        for (cell, rec) in writes {
            layer.put_cell(cell, rec.clone()).await?;
        }
        let sealed = layer.seal().await?;
        storage.commit_layer(sealed).await
    }

    #[tokio::test]
    async fn interning_the_same_bytes_twice_yields_the_same_pid() -> Result<()> {
        let storage = MemoryStorage::new();
        let first = storage.intern(PidKind::String, b"acme.ai").await?;
        let again = storage.intern(PidKind::String, b"acme.ai").await?;
        assert_eq!(
            first, again,
            "the PID is a function of the bytes, so interning is idempotent"
        );
        assert_eq!(
            storage.inner.lock().unwrap().interned.len(),
            1,
            "and equal content is stored exactly once (SPEC.md §3.1)"
        );
        Ok(())
    }

    #[tokio::test]
    async fn different_bytes_yield_different_pids() -> Result<()> {
        let storage = MemoryStorage::new();
        assert_ne!(
            storage.intern(PidKind::String, b"acme.ai").await?,
            storage.intern(PidKind::String, b"acme.com").await?
        );
        Ok(())
    }

    #[tokio::test]
    async fn one_string_interned_on_two_branches_is_one_pid() -> Result<()> {
        let storage = MemoryStorage::new();

        // Two branches, each writing `"acme.ai"` into a website cell of its own. Neither interning
        // call mentions a branch, because a content PID has no branch to mention — and that is the
        // property that makes string writes unable to conflict across branches (SPEC.md §3.1).
        let on_main = storage.intern(PidKind::String, b"acme.ai").await?;
        commit(
            &storage,
            MAIN,
            LayerId(1),
            &[(prop(1, "website"), record(Value::Ref(on_main), LayerId(1)))],
        )
        .await?;

        let on_feature = storage.intern(PidKind::String, b"acme.ai").await?;
        commit(
            &storage,
            FEATURE,
            LayerId(2),
            &[(
                prop(2, "website"),
                record(Value::Ref(on_feature), LayerId(2)),
            )],
        )
        .await?;

        assert_eq!(on_main, on_feature);
        assert_eq!(
            storage
                .get_cell(
                    &ReadPath::new(vec![(MAIN, LayerId(9))]),
                    &prop(1, "website"),
                    V1
                )
                .await?
                .map(|r| r.value),
            storage
                .get_cell(
                    &ReadPath::new(vec![(FEATURE, LayerId(9))]),
                    &prop(2, "website"),
                    V1
                )
                .await?
                .map(|r| r.value),
            "the two branches reference one and the same value"
        );
        assert_eq!(
            storage.read_interned(&on_main).await?,
            Some(b"acme.ai".to_vec()),
            "and it is readable without naming a branch at all"
        );
        Ok(())
    }

    #[tokio::test]
    async fn interned_bytes_round_trip() -> Result<()> {
        let storage = MemoryStorage::new();
        for (kind, bytes) in [
            (PidKind::String, "héllo — utf-8".as_bytes()),
            (PidKind::Binary, &[0x00, 0xff, 0x00][..]),
            (PidKind::BigInt, &[0x01, 0x00, 0x00, 0x00, 0x00][..]),
            (PidKind::String, b""),
        ] {
            let pid = storage.intern(kind, bytes).await?;
            assert_eq!(pid.kind(), kind);
            assert_eq!(storage.read_interned(&pid).await?.as_deref(), Some(bytes));
        }
        Ok(())
    }

    #[tokio::test]
    async fn one_preimage_under_two_kinds_stores_two_values() -> Result<()> {
        let storage = MemoryStorage::new();
        let text = storage.intern(PidKind::String, b"x").await?;
        let blob = storage.intern(PidKind::Binary, b"x").await?;
        assert_ne!(text, blob, "the kind is part of the PID, not of the hash");
        assert_eq!(storage.read_interned(&text).await?, Some(b"x".to_vec()));
        assert_eq!(storage.read_interned(&blob).await?, Some(b"x".to_vec()));
        Ok(())
    }

    #[tokio::test]
    async fn an_unknown_content_pid_reads_as_none() -> Result<()> {
        let storage = MemoryStorage::new();
        // A PID travels further than the bytes behind it, so a miss is an answer, not a failure.
        let elsewhere = content::pid(PidKind::String, b"never interned here")?;
        assert_eq!(storage.read_interned(&elsewhere).await?, None);
        Ok(())
    }

    #[tokio::test]
    async fn an_allocated_pid_is_not_an_interned_value() -> Result<()> {
        let storage = MemoryStorage::new();
        let CellKey::Pid(allocated) = prop(1, "website").key else {
            unreachable!()
        };
        assert!(matches!(
            storage.read_interned(&allocated).await,
            Err(BorgError::NotContentAddressed { .. })
        ));
        assert!(matches!(
            storage.intern(PidKind::Object, b"acme.ai").await,
            Err(BorgError::NotContentAddressed { .. })
        ));
        Ok(())
    }
}
