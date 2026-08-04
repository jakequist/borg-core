//! The log: layer lifecycle and the state machine. SPEC.md §6.
//!
//! A layer is the universal unit of atomicity — client transactions and producer runs follow the
//! same `open → sealed → committed | aborted` path, through one code path.
//!
//! Two constraints shape everything here:
//!
//! * **Commit streams.** A layer may hold millions of mutations and can never be buffered whole.
//! * **Locks are per-layer, never per-branch.** A branch-wide lock would serialize derivation.

use crate::seams::LayerSequencer;
use crate::touch::CellTouchIndex;
use borg_core::{
    BorgError, Branch, BranchId, CellRef, EventDraft, EventId, Guard, Layer, LayerAuthor, LayerId,
    LayerKind, LayerState, Origin, ReadPath, Result,
};
use borg_storage::{OpenLayer, StorageProvider};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// An open layer, exclusive to its owner. Writes stream through it and are invisible to every reader
/// until commit.
pub struct LayerHandle {
    layer: Layer,
    open: Box<dyn OpenLayer>,
}

impl LayerHandle {
    pub const fn id(&self) -> LayerId {
        self.layer.id
    }

    /// Make this layer contingent on a condition. Validated at seal (SPEC.md §6.2).
    pub fn guard(&mut self, guard: Guard) {
        self.layer.guards.push(guard);
    }

    /// Author one event into this layer. Called an unbounded number of times — a producer run writes
    /// *all* of its output into this one layer.
    ///
    /// **Crate-private on purpose.** This is the log's raw append, and it knows nothing about
    /// definitions; every write from outside the engine goes through `WriteSession`, which validates
    /// against the branch's def-view first (§5.1, §8). Making it public again would make that check
    /// a convention rather than a property.
    pub(crate) async fn put(&mut self, cell: &CellRef, draft: EventDraft) -> Result<EventId> {
        self.open.author_event(cell, draft).await
    }

    /// Name an event that already exists as a member of this layer. SPEC.md §13.
    ///
    /// The merge path, and the only caller. Nothing is validated here because nothing is *written*:
    /// the event was validated against the def-view it was authored under, on the branch that
    /// authored it, and this records only that the layer it landed in on the parent contains it.
    pub(crate) async fn include(&mut self, event: EventId) -> Result<()> {
        self.open.include_event(event).await
    }

    /// Append a def mutation. A layer holds value events *xor* def events (SPEC.md §6.2), so a
    /// layer taking these takes no cells.
    pub async fn put_def(&mut self, event: borg_core::DefEvent) -> Result<()> {
        if self.layer.kind != LayerKind::Value {
            return self.open.put_def(event).await;
        }
        Err(BorgError::MixedLayerKind)
    }
}

pub struct LayerManager {
    storage: Arc<dyn StorageProvider>,
    sequencer: Arc<dyn LayerSequencer>,
    touches: Arc<CellTouchIndex>,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    layers: HashMap<LayerId, Layer>,
    heads: HashMap<BranchId, LayerId>,
    /// Branches live here rather than in `BranchManager` so that guard validation, which needs the
    /// ancestry, does not require the log to depend on the branch layer above it.
    branches: HashMap<BranchId, Branch>,
    /// Committed **def** layers per branch, in id order.
    ///
    /// An index over `layers`, held because folding the def-view is on the hot path — every write
    /// session opens by folding two of them (§8.0), and every producer run is a write session. Found
    /// by filtering `layers`, that fold is `O(all layers)`, and a producer run commits a layer, so a
    /// fan-out of `n` invocations costs `O(n²)` before any producer code runs. Def layers are rare
    /// by construction: a schema changes far less often than data does.
    def_layers: HashMap<BranchId, Vec<LayerId>>,
}

impl LayerManager {
    pub fn new(
        storage: Arc<dyn StorageProvider>,
        sequencer: Arc<dyn LayerSequencer>,
        touches: Arc<CellTouchIndex>,
    ) -> Self {
        Self {
            storage,
            sequencer,
            touches,
            state: Mutex::new(State::default()),
        }
    }

    pub fn register_branch(&self, branch: Branch) {
        self.state
            .lock()
            .unwrap()
            .branches
            .insert(branch.id, branch);
    }

    /// Reinstate a layer read back from storage, without re-running the state machine.
    ///
    /// A layer that comes back **not committed has no owner**: an open layer is exclusive to the
    /// process holding it (§6.2), and that process is gone. It never became visible, so it is
    /// aborted rather than left in a state that would make derivation wait forever for a commit
    /// nobody is going to make.
    pub fn restore(&self, mut layer: Layer) {
        if !matches!(layer.state, LayerState::Committed | LayerState::Aborted) {
            layer.state = LayerState::Aborted;
        }
        let mut state = self.state.lock().unwrap();
        if layer.state == LayerState::Committed {
            let head = state.heads.entry(layer.branch).or_insert(layer.id);
            if layer.id.0 > head.0 {
                *head = layer.id;
            }
        }
        if layer.state == LayerState::Committed && layer.kind == LayerKind::Def {
            state
                .def_layers
                .entry(layer.branch)
                .or_default()
                .push(layer.id);
        }
        state.layers.insert(layer.id, layer);
    }

    /// The committed def layers on a branch at or below a bound, oldest first.
    ///
    /// Sorted on the way out rather than on the way in: restore replays in id order and commit only
    /// ever appends a higher id, so this is almost always already sorted, and a def-version chain is
    /// short enough that being sure costs nothing.
    pub fn def_layers_of(&self, branch: BranchId, bound: LayerId) -> Vec<LayerId> {
        let state = self.state.lock().unwrap();
        let mut found: Vec<LayerId> = state
            .def_layers
            .get(&branch)
            .into_iter()
            .flatten()
            .copied()
            .filter(|id| id.0 <= bound.0)
            .collect();
        found.sort_by_key(|id| id.0);
        found
    }

    pub fn branches(&self) -> Vec<Branch> {
        self.state
            .lock()
            .unwrap()
            .branches
            .values()
            .cloned()
            .collect()
    }

    pub fn branch(&self, id: BranchId) -> Option<Branch> {
        self.state.lock().unwrap().branches.get(&id).cloned()
    }

    /// The ancestry a read resolves through. SPEC.md §7.2.
    pub fn read_path(&self, branch: BranchId, layer: Option<LayerId>) -> Result<ReadPath> {
        let mut current = branch;
        let mut bound = layer.or_else(|| self.head(branch)).unwrap_or(LayerId(0));
        let mut segments = Vec::new();

        loop {
            segments.push((current, bound));
            let Some(origin) = self.branch(current).and_then(|b| b.origin) else {
                break;
            };
            let Some(parent_layer) = self.layer(origin) else {
                break;
            };
            // An ancestor is bounded at the fork point — and *only* further clamped when the caller
            // asked for an explicit layer, which may sit below the fork point. Clamping by the
            // child's own bound instead would leave a fork that has not written anything yet unable
            // to see its parent at all, since its head is still nothing.
            bound = match layer {
                Some(requested) => LayerId(requested.0.min(origin.0)),
                None => origin,
            };
            current = parent_layer.branch;
        }
        Ok(ReadPath::new(segments))
    }

    /// Check a set of guards against a branch's history. SPEC.md §12.
    ///
    /// Used both at seal, with each guard's own `since`, and by merge, with the fork point — which
    /// is what makes guards the merge-conflict detector (SPEC.md §13).
    pub async fn check_guards(
        &self,
        branch: BranchId,
        guards: &[Guard],
        since: Option<LayerId>,
    ) -> Result<()> {
        if guards.is_empty() {
            return Ok(());
        }
        let path = self.read_path(branch, None)?;
        for guard in guards {
            let since = since.unwrap_or(guard.since);
            for cell in &guard.cells {
                // Guarding derived data would be checking a shadow: its value is a function of
                // source data with a lag (SPEC.md §12). Asked of every version this cell is
                // materialized at, because the guard names a `CellRef` and derivedness is a fact
                // about the field, not about one version of it.
                if self.is_derived_anywhere(&path, cell).await? {
                    return Err(BorgError::GuardOnDerivedCell { cell: cell.clone() });
                }
            }
            self.check_touched(&path, &guard.cells, since)?;
        }
        Ok(())
    }

    /// Check an **automatic** guard set — a transaction's read-set — against a branch's history
    /// since a layer. SPEC.md §12.
    ///
    /// The same question [`check_guards`](Self::check_guards) asks, minus one check, and the
    /// difference is deliberate: a cell here was *read*, not asserted about. A client that read a
    /// derived value made no claim the system could hold it to, so the read contributes no guard
    /// rather than making the commit illegal — refusing to commit a transaction because it looked at
    /// derived data would be a strange reward for looking. It could not trip anyway: the touch index
    /// records source layers only (§12), so a derived cell is never in it, and asking the question
    /// costs a storage read per cell per version on a set §7.7 says is unbounded.
    ///
    /// `GuardOnDerivedCell` therefore stays what it always was — the answer to a client *writing* a
    /// guard by hand on a cell it cannot usefully guard.
    pub fn check_reads<'a>(
        &self,
        branch: BranchId,
        cells: impl IntoIterator<Item = &'a CellRef>,
        since: LayerId,
    ) -> Result<()> {
        let path = self.read_path(branch, None)?;
        self.check_touched(&path, cells, since)
    }

    /// Whether anything has been written to this branch since a layer, at all.
    ///
    /// The question a caller with an enormous guard set should ask first: nothing written means no
    /// guard can have failed, whatever it names (§16.5). Only *source* writes count, because only
    /// they are in the touch index and only they can trip a guard (§12.4).
    pub fn touched_since(&self, branch: BranchId, since: LayerId) -> Result<bool> {
        let path = self.read_path(branch, None)?;
        self.touches.moved_since(&path, since)
    }

    /// *Has anything touched these cells since that layer?* — the one question both guard paths ask,
    /// so that neither can drift into asking a slightly different one.
    ///
    /// Borrowed and batched: a round asks this once per invocation over that invocation's whole
    /// read-set, which at a large fan-out is the difference between one lock acquisition and a
    /// million.
    fn check_touched<'a>(
        &self,
        path: &ReadPath,
        cells: impl IntoIterator<Item = &'a CellRef>,
        since: LayerId,
    ) -> Result<()> {
        match self.touches.first_touched_since(path, cells, since)? {
            Some((cell, mutated_at)) => Err(BorgError::GuardViolated {
                cell: cell.clone(),
                since,
                mutated_at,
            }),
            None => Ok(()),
        }
    }

    async fn is_derived_anywhere(&self, path: &ReadPath, cell: &CellRef) -> Result<bool> {
        for version in self.storage.cell_versions(path, cell).await? {
            if self
                .storage
                .get_cell(path, cell, version)
                .await?
                .is_some_and(|found| found.event.origin == Origin::Derived)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The most recently committed layer on a branch.
    pub fn head(&self, branch: BranchId) -> Option<LayerId> {
        self.state.lock().unwrap().heads.get(&branch).copied()
    }

    pub fn layer(&self, id: LayerId) -> Option<Layer> {
        self.state.lock().unwrap().layers.get(&id).cloned()
    }

    /// Who authored a layer, without cloning the layer.
    ///
    /// A `Layer` carries its guards, so asking for one to read a two-word enum copies a `Vec` — which
    /// is invisible until a round asks it once per invocation (§16.5).
    pub fn author_of(&self, id: LayerId) -> Option<LayerAuthor> {
        self.state.lock().unwrap().layers.get(&id).map(|l| l.author)
    }

    /// Every committed layer belonging to a branch. A layer belongs to exactly one branch
    /// (SPEC.md §6.2), so this needs no ancestry walk.
    pub fn layers_of(&self, branch: BranchId) -> Vec<Layer> {
        self.state
            .lock()
            .unwrap()
            .layers
            .values()
            .filter(|layer| layer.branch == branch && layer.state == LayerState::Committed)
            .cloned()
            .collect()
    }

    /// The highest layer that reads as a coherent snapshot of a settled frontier.
    /// SPEC.md §10.5, §16.5.
    ///
    /// A watermark points into the **source** stream, and the derived layers carrying a source
    /// layer's consequences have *higher* ids than it — so bounding a read at the watermark itself
    /// would show the source data and hide everything computed from it, which is the opposite of
    /// coherent. The ceiling is instead the longest prefix of the branch's layers in which nothing
    /// is unsettled: layers at or below the watermark, plus derived layers reflecting one of them,
    /// stopping at the first layer that is neither.
    ///
    /// A prefix rather than a filter, because a `ReadPath` bound is one layer id. That costs
    /// precision in one case — a producer that has run ahead of the slowest one has derived layers
    /// above an unsettled source layer, and they are excluded — and losing them is the safe
    /// direction: what remains is still a world nothing in it is behind.
    pub fn settled_ceiling(&self, branch: BranchId, watermark: LayerId) -> LayerId {
        let mut layers = self.layers_of(branch);
        layers.sort_by_key(|layer| layer.id.0);
        let mut ceiling = LayerId(0);
        for layer in layers {
            let settled = match layer.author {
                LayerAuthor::Source => layer.id.0 <= watermark.0,
                LayerAuthor::Derived { reflects, .. } => reflects.0 <= watermark.0,
            };
            if !settled {
                break;
            }
            ceiling = layer.id;
        }
        ceiling
    }

    /// The highest **source** layer visible anywhere along a read path.
    ///
    /// The layer a branch *stands at* as far as anything derived is concerned, which is not the same
    /// as its head: a watermark points into the source stream (§6.3), and a settled branch's head is
    /// usually the last derived layer some round committed. `None` where the path reaches no source
    /// layer at all — an empty store, where there is nothing to derive from.
    pub fn highest_source_layer(&self, path: &ReadPath) -> Option<LayerId> {
        let state = self.state.lock().unwrap();
        state
            .layers
            .values()
            .filter(|layer| {
                layer.state == LayerState::Committed && matches!(layer.author, LayerAuthor::Source)
            })
            .filter(|layer| {
                path.segments
                    .iter()
                    .any(|(branch, bound)| layer.branch == *branch && layer.id.0 <= bound.0)
            })
            .map(|layer| layer.id)
            .max()
    }

    pub fn storage(&self) -> Arc<dyn StorageProvider> {
        Arc::clone(&self.storage)
    }

    /// Open a layer. Order within a branch is established at *commit*, not here — many layers may be
    /// open on a branch simultaneously (SPEC.md §7.3).
    pub async fn open(
        &self,
        branch: BranchId,
        kind: LayerKind,
        author: LayerAuthor,
    ) -> Result<LayerHandle> {
        let id = self.sequencer.next_layer_id(branch);
        let parent = self.head(branch);
        let layer = Layer {
            id,
            branch,
            kind,
            author,
            state: LayerState::Open,
            parent,
            guards: Vec::new(),
        };
        self.state.lock().unwrap().layers.insert(id, layer.clone());
        let open = self.storage.open_layer(branch, id).await?;
        Ok(LayerHandle { layer, open })
    }

    /// Seal, then commit. **The commit edge is what triggers dependent producers** — callers hand
    /// the returned id to the invalidator.
    pub async fn commit(&self, handle: LayerHandle) -> Result<LayerId> {
        let LayerHandle { layer, open } = handle;
        self.transition(layer.id, LayerState::Open, LayerState::Sealed)?;

        // Guards are validated at seal — before anything becomes visible, so a rejected transaction
        // leaves no trace (SPEC.md §6.2).
        if let Err(violation) = self.check_guards(layer.branch, &layer.guards, None).await {
            open.abort().await?;
            self.transition(layer.id, LayerState::Sealed, LayerState::Aborted)?;
            return Err(violation);
        }
        // Copy the guards onto the stored layer so merge can re-evaluate them later. Only the
        // guards — writing the whole handle back would clobber the state machine with the handle's
        // stale `Open`.
        if let Some(stored) = self.state.lock().unwrap().layers.get_mut(&layer.id) {
            stored.guards = layer.guards.clone();
        }

        let sealed = open.seal().await?;
        // Metadata is written before the commit flips visibility, so a layer can never become
        // visible without the log knowing what kind of thing it is.
        let mut durable = layer.clone();
        durable.state = LayerState::Committed;
        self.storage.put_layer_meta(&durable).await?;
        self.storage.commit_layer(sealed).await?;
        self.transition(layer.id, LayerState::Sealed, LayerState::Committed)?;
        // **Monotonic, because layers commit out of order.** Ids are assigned at open and the order
        // within a branch is established at commit (§7.3), so a layer opened earlier can land after
        // one opened later — which is the ordinary case once a round runs its invocations
        // concurrently. Assigning the head unconditionally would walk it *backwards*, and the head
        // is what bounds every subsequent read path and every producer's work gap.
        {
            let mut state = self.state.lock().unwrap();
            let head = state.heads.entry(layer.branch).or_insert(layer.id);
            if layer.id.0 > head.0 {
                *head = layer.id;
            }
            if layer.kind == LayerKind::Def {
                state
                    .def_layers
                    .entry(layer.branch)
                    .or_default()
                    .push(layer.id);
            }
        }

        // Feed the touch index by streaming the committed layer. Source layers only: guards may name
        // source cells only, and derived layers are the enormous ones.
        if matches!(layer.author, LayerAuthor::Source) {
            let mut stream = self.storage.read_layer(layer.id).await?;
            let mut cells = Vec::new();
            while let Some(row) = stream.next().await {
                cells.push(row?.cell);
            }
            self.touches.record(layer.branch, layer.id, &cells)?;
        }
        Ok(layer.id)
    }

    pub fn touches(&self) -> Arc<CellTouchIndex> {
        Arc::clone(&self.touches)
    }

    /// Discard. The layer never becomes visible — which is what lets a failed producer run leave no
    /// trace (SPEC.md §14).
    pub async fn abort(&self, handle: LayerHandle) -> Result<()> {
        let LayerHandle { layer, open } = handle;
        open.abort().await?;
        self.transition(layer.id, LayerState::Open, LayerState::Aborted)
    }

    fn transition(&self, id: LayerId, expected: LayerState, next: LayerState) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        let layer = state
            .layers
            .get_mut(&id)
            .ok_or(BorgError::Storage(format!("unknown layer {id}")))?;
        if layer.state != expected {
            return Err(BorgError::LayerStateViolation {
                layer: id,
                expected,
                actual: layer.state,
            });
        }
        layer.state = next;
        Ok(())
    }
}
