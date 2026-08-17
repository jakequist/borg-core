//! An in-memory `StorageProvider`.
//!
//! Exists to get the derivation cycle running end-to-end on the thinnest possible substrate — that
//! loop is the only genuinely unproven part of the design, and everything else can wait behind it.
//! Not durable, not sharded, not efficient.
//!
//! It does honour the two constraints that actually shape the interface: writes stream into an
//! invisible open layer, and nothing becomes visible until commit.

use crate::{EventStream, OpenLayer, SealedLayer, StorageProvider};
use async_trait::async_trait;
use borg_core::{
    BorgError, Branch, BranchId, BufferId, CellRef, DefEvent, DefVersion, Event, EventDraft,
    EventId, Landed, Layer, LayerId, Pid, PidKind, ReadPath, Result, content,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// The read index: `(branch, cell, version) -> (layer, event)`, one entry per landing layer.
///
/// A projection of layer membership, kept because reads must stay a single lookup once events no
/// longer carry a layer. [`MemoryStorage::rebuild_read_index`] regenerates it from the log, which
/// is what makes it a cache rather than a second source of truth.
///
/// **Nested rather than keyed on a `(CellRef, DefVersion)` tuple**, because `HashMap::get` wants
/// the whole key and a `CellRef` owns two or three `String`s — so a tuple key made every read clone
/// one, *per read-path segment*. A round's read path has two segments now that a round has a branch
/// of its own (§16.5), which doubled a cost nothing had noticed while it was paid once. Nesting
/// removes it rather than halving it: the cell is borrowed and the version is `Copy`. It also turns
/// `cell_versions` from a scan of the branch into a lookup.
type ReadIndex = HashMap<BranchId, HashMap<CellRef, HashMap<DefVersion, Vec<(LayerId, EventId)>>>>;

/// The newest landing at or below a bound.
///
/// A `max_by_key`, not the last entry that fits. Layers commit out of order — ids are assigned at
/// open and order is established at commit (SPEC.md §7.3) — so this history is in *commit* order,
/// not id order, and "walk backwards to the first one that fits" would serve a superseded value
/// whenever two layers on one cell landed in the other order.
///
/// Keyed on the **landing** layer, never on the event's `authored`: a merge carries an event
/// authored long ago onto this branch now, and ordering by where it was written would rank it under
/// whatever the branch has done since.
fn newest_at(history: &[(LayerId, EventId)], bound: LayerId) -> Option<(LayerId, EventId)> {
    history
        .iter()
        .filter(|(landed, _)| landed.0 <= bound.0)
        .max_by_key(|(landed, _)| landed.0)
        .copied()
}

/// Record that `event` landed on `branch` at `layer`.
///
/// One entry per landing layer, replaced rather than appended when a layer writes the same cell
/// twice: within one layer the later write wins, and collapsing it here is what keeps
/// [`newest_at`]'s `max_by_key` unambiguous. Membership keeps both events — the layer really does
/// contain two — but only one of them can be the answer to a read.
fn index(into: &mut ReadIndex, branch: BranchId, layer: LayerId, event: &Event) {
    let history = into
        .entry(branch)
        .or_default()
        .entry(event.cell.clone())
        .or_default()
        .entry(event.version)
        .or_default();
    match history.iter_mut().find(|(landed, _)| *landed == layer) {
        Some(entry) => entry.1 = event.id,
        None => history.push((layer, event.id)),
    }
}

#[derive(Default)]
struct Inner {
    /// Every event, by identity. One entry however many layers name it — which is the property the
    /// whole inversion exists for.
    ///
    /// A `Vec` indexed by id rather than a map, because this store mints the ids itself and mints
    /// them densely from 1. A layer that aborts leaves a hole, which is what `Option` is for. It
    /// matters because a fan-out round authors one event and reads seven per invocation, and a
    /// hash per event is the sort of constant the scale benchmark notices.
    events: Vec<Option<Event>>,
    /// A committed layer's membership, in order (SPEC.md §6.2), and the branch it belongs to — a
    /// layer still belongs to exactly one branch, and only *events* are shared.
    members: HashMap<LayerId, Membership>,
    /// Per branch and cell, where events landed. See [`ReadIndex`].
    reads: ReadIndex,
    /// Open and sealed layers, invisible to readers until commit.
    staged: HashMap<LayerId, StagedLayer>,
    /// A committed def layer's events.
    def_contents: HashMap<LayerId, Vec<DefEvent>>,
    layer_meta: HashMap<LayerId, Layer>,
    branches: HashMap<BranchId, Branch>,
    /// Interned values, keyed by their PID and nothing else — no branch, no layer, no version
    /// (SPEC.md §3.1). One map rather than three because the PID carries its own kind, which is
    /// what keeps `String("x")` and `Binary("x")` apart despite sharing a hash.
    interned: HashMap<Pid, Vec<u8>>,
    next_event: u64,
}

impl Inner {
    fn mint(&mut self) -> EventId {
        self.next_event += 1;
        EventId(self.next_event)
    }

    fn stored(&self, id: EventId) -> Option<&Event> {
        self.events.get(id.0.checked_sub(1)? as usize)?.as_ref()
    }

    /// An event by identity. A membership row naming an event that does not exist is a corrupt log,
    /// not a miss, so this errors rather than returning `None`.
    fn event(&self, id: EventId) -> Result<Event> {
        self.stored(id)
            .cloned()
            .ok_or_else(|| BorgError::Storage(format!("unknown event {id}")))
    }

    fn store(&mut self, event: Event) {
        let slot = event.id.0 as usize - 1;
        if self.events.len() <= slot {
            self.events.resize_with(slot + 1, || None);
        }
        self.events[slot] = Some(event);
    }
}

struct Membership {
    branch: BranchId,
    events: Vec<EventId>,
}

struct StagedLayer {
    branch: BranchId,
    /// Membership in the order it was written, whether authored here or named from elsewhere.
    members: Vec<EventId>,
    /// The events this layer is the author of, still invisible.
    authored: Vec<Event>,
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

/// The staged layer this handle writes into, refusing once it is sealed.
fn staged(inner: &mut Inner, id: LayerId) -> Result<&mut StagedLayer> {
    let staged = inner
        .staged
        .get_mut(&id)
        .ok_or_else(|| BorgError::Storage(format!("layer {id} is not open")))?;
    if staged.sealed {
        return Err(BorgError::Storage(format!("layer {id} is sealed")));
    }
    Ok(staged)
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

    async fn author_event(&mut self, cell: &CellRef, draft: EventDraft) -> Result<EventId> {
        let mut inner = self.inner.lock().unwrap();
        let id = inner.mint();
        let staged = staged(&mut inner, self.id)?;
        staged.members.push(id);
        staged.authored.push(Event {
            id,
            cell: cell.clone(),
            value: draft.value,
            version: draft.version,
            origin: draft.origin,
            derivation: draft.derivation,
            // Authored here, because this is where it is being created. Nothing else can set it.
            authored: self.id,
        });
        Ok(id)
    }

    async fn adopt_event(&mut self, event: Event) -> Result<()> {
        if event.authored != self.id {
            return Err(BorgError::Storage(format!(
                "event {} says it was authored in {}, but is being replayed into {}",
                event.id, event.authored, self.id
            )));
        }
        let mut inner = self.inner.lock().unwrap();
        // Past the adopted id, never backwards: the ids in a stream are dense-ish but not ordered
        // with respect to anything this store has already taken, and a mint that walked back would
        // reissue one of them on the next ordinary write.
        inner.next_event = inner.next_event.max(event.id.0);
        let staged = staged(&mut inner, self.id)?;
        staged.members.push(event.id);
        staged.authored.push(event);
        Ok(())
    }

    async fn include_event(&mut self, event: EventId) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if inner.stored(event).is_none() {
            return Err(BorgError::Storage(format!("unknown event {event}")));
        }
        staged(&mut inner, self.id)?.members.push(event);
        Ok(())
    }

    async fn put_def(&mut self, event: DefEvent) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        staged(&mut inner, self.id)?.defs.push(event);
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
        version: DefVersion,
    ) -> Result<Option<Landed>> {
        let inner = self.inner.lock().unwrap();
        // Walk outward. The first segment holding *any* record wins — including a tombstone, which
        // must stop the walk rather than fall through and resurrect the parent's value.
        for (branch, bound) in &path.segments {
            let found = inner
                .reads
                .get(branch)
                .and_then(|cells| cells.get(cell))
                .and_then(|versions| versions.get(&version))
                .and_then(|history| newest_at(history, *bound));
            if let Some((landed_at, event)) = found {
                return Ok(Some(Landed {
                    event: inner.event(event)?,
                    landed_at,
                }));
            }
        }
        Ok(None)
    }

    async fn cell_versions(&self, path: &ReadPath, cell: &CellRef) -> Result<Vec<DefVersion>> {
        let inner = self.inner.lock().unwrap();
        let mut versions = Vec::new();
        for (branch, bound) in &path.segments {
            let found = inner.reads.get(branch).and_then(|cells| cells.get(cell));
            for (version, history) in found.into_iter().flatten() {
                if !versions.contains(version) && history.iter().any(|(at, _)| at.0 <= bound.0) {
                    versions.push(*version);
                }
            }
        }
        Ok(versions)
    }

    async fn scan_buffer(&self, path: &ReadPath, buffer: &BufferId) -> Result<EventStream> {
        let inner = self.inner.lock().unwrap();
        // A child's own record shadows the parent's, so remember what the inner segments covered.
        // `seen` is extended only once the segment is finished: shadowing is between segments, and
        // a cell materialized at two versions is two rows within one — one per version, because a
        // version is part of the record key (§4.3) and hiding one of them would hide a migration's
        // output from the only enumeration the engine has.
        // A set, not a Vec: this scan now runs at the top of a settled range over a full buffer,
        // which is exactly the shape that turned two Vec::contains into 88 seconds elsewhere
        // (CLAUDE.md invariant 5).
        let mut seen: std::collections::HashSet<CellRef> = std::collections::HashSet::new();
        let mut rows = Vec::new();
        for (branch, bound) in &path.segments {
            let mut segment = Vec::new();
            for (cell, versions) in inner.reads.get(branch).into_iter().flatten() {
                if &cell.buffer != buffer || seen.contains(cell) {
                    continue;
                }
                for history in versions.values() {
                    if let Some((_, event)) = newest_at(history, *bound) {
                        segment.push(cell.clone());
                        rows.push(inner.event(event));
                    }
                }
            }
            seen.extend(segment);
        }
        Ok(Box::pin(futures_util::stream::iter(rows)))
    }

    async fn read_membership(&self, layer: LayerId) -> Result<Vec<EventId>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .members
            .get(&layer)
            .map(|membership| membership.events.clone())
            .unwrap_or_default())
    }

    async fn read_layer(&self, layer: LayerId) -> Result<EventStream> {
        let inner = self.inner.lock().unwrap();
        let rows: Vec<_> = inner
            .members
            .get(&layer)
            .into_iter()
            .flat_map(|membership| &membership.events)
            .map(|id| inner.event(*id))
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
        if inner.staged.contains_key(&id) || inner.members.contains_key(&id) {
            return Err(BorgError::Storage(format!("layer {id} already exists")));
        }
        inner.staged.insert(
            id,
            StagedLayer {
                branch,
                members: Vec::new(),
                authored: Vec::new(),
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
        for event in staged.authored {
            inner.store(event);
        }
        // The read index is updated in membership order, so that a layer writing one cell twice
        // resolves to its later event — and so that a merge's included events index identically to
        // authored ones, since by here the two are indistinguishable.
        //
        // Split-borrowed rather than fetching each event through `Inner::event`, which clones: a
        // layer may hold millions of members, and cloning a derived event to index it would copy
        // its whole read-set per write for the sake of three fields.
        let Inner { events, reads, .. } = &mut *inner;
        for id in &staged.members {
            let event = events
                .get(id.0 as usize - 1)
                .and_then(Option::as_ref)
                .ok_or_else(|| BorgError::Storage(format!("unknown event {id}")))?;
            index(reads, staged.branch, layer.id, event);
        }
        inner.members.insert(
            layer.id,
            Membership {
                branch: staged.branch,
                events: staged.members,
            },
        );
        inner.def_contents.insert(layer.id, staged.defs);
        Ok(())
    }

    async fn rebuild_read_index(&self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.reads.clear();
        // Layer ids are registry-unique and monotonic, so id order is replay order across every
        // branch — the same ordering `Registry::open` replays the log in.
        let mut layers: Vec<LayerId> = inner.members.keys().copied().collect();
        layers.sort_by_key(|id| id.0);
        for layer in layers {
            let membership = &inner.members[&layer];
            let branch = membership.branch;
            for id in membership.events.clone() {
                let event = inner.event(id)?;
                index(&mut inner.reads, branch, layer, &event);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use borg_core::{AllocatorId, CellKey, Origin, Value};

    const MAIN: BranchId = BranchId(1);
    const FEATURE: BranchId = BranchId(2);
    const V1: DefVersion = DefVersion(LayerId(1));

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

    fn draft(value: Value) -> EventDraft {
        EventDraft {
            value,
            version: V1,
            origin: Origin::Source,
            derivation: None,
        }
    }

    async fn commit(
        storage: &MemoryStorage,
        branch: BranchId,
        id: LayerId,
        writes: &[(CellRef, EventDraft)],
    ) -> Result<()> {
        let mut layer = storage.open_layer(branch, id).await?;
        for (cell, draft) in writes {
            layer.author_event(cell, draft.clone()).await?;
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
            &[(prop(1, "website"), draft(Value::Ref(on_main)))],
        )
        .await?;

        let on_feature = storage.intern(PidKind::String, b"acme.ai").await?;
        commit(
            &storage,
            FEATURE,
            LayerId(2),
            &[(prop(2, "website"), draft(Value::Ref(on_feature)))],
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
                .map(|found| found.event.value),
            storage
                .get_cell(
                    &ReadPath::new(vec![(FEATURE, LayerId(9))]),
                    &prop(2, "website"),
                    V1
                )
                .await?
                .map(|found| found.event.value),
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
