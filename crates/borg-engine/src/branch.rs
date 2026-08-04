//! Branches, forking, and merge. SPEC.md §7, §12, §13.
//!
//! A fork is O(1) even under eager derivation: a new branch inherits its parent's layers — source
//! and derived alike — by ancestry, and diverges only where it writes.
//!
//! **Every client write comes through here**, because every client write is a transaction (§12):
//! fork, write in isolation, merge. `merge_transaction` is that last step, and the only thing it
//! adds to an ordinary merge is the transaction's read-set, checked against the parent since the
//! fork point. That is the same question a merge already asked — "did the parent touch this while I
//! was working?" — so making guards automatic changed what gets asked, not what does the asking.
//!
//! **There are two kinds of merge, and which layers they carry is the difference.** Merging a
//! *client* branch replays its **source** layers and skips its derived ones, because the child's
//! derived values are wrong on the parent by construction: they were computed from different data.
//! Merging a *round* branch (§16.5) replays its **derived** layers and there is nothing else on it —
//! carrying them is the entire purpose of the branch. The two are separate entry points,
//! [`BranchManager::merge_transaction`] and [`BranchManager::merge_round`], so that the distinction
//! is made on purpose rather than falling out of what a branch happens to contain.
//!
//! **Merge does not copy events.** A parent layer *names* the events of the child layer it replays
//! (§13). The events are not rewritten, so `authored` still says where they were written and the
//! parent layer says where they landed — the two facts the old model collapsed into one. What this
//! costs is `n` membership rows and `n` read-index entries rather than `n` full records carrying
//! value, version, origin, derivation and read-set: a large constant factor, and deliberately not
//! an asymptotic one. Genuine `O(1)` needs a parent layer to reference a child layer's event *set*
//! rather than enumerate it, which grows the read path per merge and needs compaction to pay for
//! itself. Deferred.

use crate::log::LayerManager;
use borg_core::{
    BorgError, Branch, BranchId, CellRef, Event, FieldName, LayerAuthor, LayerId, LayerKind,
    MergeMode, MergeRejection, ObjectTypeName, ReadPath, Result, Round,
};
use borg_storage::StorageProvider;
use futures_util::StreamExt;
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// What became of a round at its merge. SPEC.md §16.5.
///
/// A round is `N` independent computations, so this is a report rather than a verdict: some
/// invocations landed and some did not, and the caller needs to be able to say which.
#[derive(Debug, Default)]
pub struct RoundOutcome {
    /// How many invocations the round ran. Not the same as how many landed, and deliberately not
    /// pinned by any test: how often a downstream producer re-runs is what the interleaving decides
    /// (§9.6).
    pub executed: usize,
    /// The layers created on the parent, one per producer that had anything to land.
    pub landed: Vec<LayerId>,
    /// How many of the round's invocations were applied.
    pub applied: usize,
    /// The invocations rejected outright, and the cell that moved underneath each.
    pub rejected: Vec<(LayerId, CellRef)>,
    /// How many further invocations went with them because they consumed their output.
    pub cascaded: usize,
}

pub struct BranchManager {
    layers: Arc<LayerManager>,
    storage: Arc<dyn StorageProvider>,
    next_id: AtomicU64,
}

impl BranchManager {
    pub fn new(layers: Arc<LayerManager>) -> Self {
        Self::resuming(layers, 1)
    }

    /// Resume id allocation after reopening a store, so a second process cannot mint a branch id
    /// that already exists.
    pub fn resuming(layers: Arc<LayerManager>, next_id: u64) -> Self {
        let storage = layers.storage();
        let next = layers
            .branches()
            .iter()
            .map(|b| b.id.0 + 1)
            .chain(std::iter::once(next_id))
            .max()
            .unwrap_or(1);
        Self {
            layers,
            storage,
            next_id: AtomicU64::new(next),
        }
    }

    /// Branches with no origin — the roots of the tree.
    pub fn roots(&self) -> Vec<BranchId> {
        self.layers
            .branches()
            .into_iter()
            .filter(|b| b.origin.is_none())
            .map(|b| b.id)
            .collect()
    }

    pub fn all(&self) -> Vec<Branch> {
        self.layers.branches()
    }

    /// The next unused branch id.
    ///
    /// **Unused means unused by the log, not merely unregistered.** The counter resumes past every
    /// branch the store knows about, but a branch that has layers and no row would otherwise be
    /// handed out again — and then two branches share an id, which breaks the one thing §6.2 says
    /// about layers and branches: a layer belongs to exactly one branch. That was tolerable while
    /// every fork came from a caller who knew what it was doing; a round forks by itself, on every
    /// settle, and the cost of being sure is one map lookup.
    fn next_branch(&self) -> BranchId {
        loop {
            let id = BranchId(self.next_id.fetch_add(1, Ordering::Relaxed));
            if self.layers.branch(id).is_none() && self.layers.head(id).is_none() {
                return id;
            }
        }
    }

    async fn persist(&self, branch: &Branch) -> Result<()> {
        self.layers.register_branch(branch.clone());
        self.storage.put_branch(branch).await
    }

    /// The root of the tree: a branch with no origin.
    pub async fn create_root(&self, name: Option<String>) -> Result<BranchId> {
        let id = self.next_branch();
        self.persist(&Branch {
            id,
            name,
            origin: None,
        })
        .await?;
        Ok(id)
    }

    /// Fork at a layer. O(1) — nothing is copied.
    pub async fn fork(
        &self,
        parent: BranchId,
        at: LayerId,
        name: Option<String>,
    ) -> Result<BranchId> {
        let layer = self
            .layers
            .layer(at)
            .ok_or(BorgError::Storage(format!("unknown layer {at}")))?;
        if layer.branch != parent {
            return Err(BorgError::LayerNotOnBranch {
                layer: at,
                branch: parent,
            });
        }
        let id = self.next_branch();
        self.persist(&Branch {
            id,
            name,
            origin: Some(at),
        })
        .await?;
        Ok(id)
    }

    pub fn branch(&self, id: BranchId) -> Option<Branch> {
        self.layers.branch(id)
    }

    /// The parent branch, inferred from the origin layer. There is deliberately no parent pointer
    /// (SPEC.md §7.1).
    pub fn parent_of(&self, id: BranchId) -> Option<BranchId> {
        let origin = self.branch(id)?.origin?;
        self.layers.layer(origin).map(|layer| layer.branch)
    }

    /// The ancestry a read resolves through. SPEC.md §7.2.
    pub fn read_path(&self, branch: BranchId, layer: Option<LayerId>) -> Result<ReadPath> {
        self.layers.read_path(branch, layer)
    }

    /// Replay a child's source layers onto its parent. SPEC.md §13.
    ///
    /// Returns the new parent layers. v1 rejects a whole merge rather than applying it partially,
    /// and validates everything *before* writing anything, so a rejected merge leaves no trace.
    pub async fn merge(&self, child: BranchId, mode: MergeMode) -> Result<Vec<LayerId>> {
        let origin = self
            .branch(child)
            .and_then(|b| b.origin)
            .ok_or(BorgError::Storage("cannot merge the root branch".into()))?;
        let parent = self
            .parent_of(child)
            .ok_or(BorgError::Storage("child has no parent".into()))?;
        self.replay(child, parent, origin, mode, &[]).await
    }

    /// Commit a client transaction: merge it, guarded by everything it read. SPEC.md §12, §13.
    ///
    /// The transaction carries its own parent and fork point rather than having them inferred from
    /// the branch's origin layer, because the two differ in the ordinary case of a transaction opened
    /// on a branch that has nothing on it yet: the fork is taken at the highest layer that branch can
    /// *see*, which belongs to an ancestor, while the merge must still land on the branch the client
    /// named.
    ///
    /// **All-or-nothing.** A transaction expresses one intent, so one failed guard rejects the whole
    /// thing (§13). Partial application belongs to rounds, which are `N` independent computations
    /// with no invariant spanning any two of them.
    pub async fn merge_transaction(
        &self,
        transaction: &borg_core::Transaction,
        mode: MergeMode,
    ) -> Result<Vec<LayerId>> {
        self.replay(
            transaction.branch,
            transaction.parent,
            transaction.fork_point,
            mode,
            &transaction.guards(),
        )
        .await
    }

    /// Land a settled round on the branch it was settling. SPEC.md §16.5.
    ///
    /// **Rounds apply partially, and client transactions do not.** A client transaction expresses
    /// one intent, so a single failed guard rejects the whole thing (§13). A round is `N`
    /// independent computations with no invariant spanning any two of them, and whole-round
    /// rejection would let one contended cell kill a 128k-invocation round — and under sustained
    /// contention it would never land at all. So the invocations whose guards held are applied and
    /// the rest are dropped.
    ///
    /// Dropping is safe because of the freshness design, not in spite of it: a dropped invocation's
    /// cells are still dirty in the dependency index, the layer that failed its guard is itself a
    /// source layer some later round will settle, and that round rediscovers the invocation through
    /// the very cell that moved. In the meantime the value reads `stale` with a watermark that says
    /// so (§10.2).
    ///
    /// **Guards are evaluated first, all of them, against the parent as it stood before any of this
    /// merge landed.** Per-layer checking interleaved with application would let one invocation of
    /// a round trip another's guard, which is the same mistake §12.1 rules out for a transaction's
    /// own layers.
    pub async fn merge_round(&self, round: &Round) -> Result<RoundOutcome> {
        let parent = round.parent;
        let fork_point = round.fork_point;
        let mut outcome = RoundOutcome::default();
        if round.is_empty() {
            return Ok(outcome);
        }

        // **Nothing written to the trunk since the fork means no guard can have failed**, whatever
        // the round read — so the guard set is not even built. A round's guard set is the sum of its
        // producers' read-sets and is unbounded (§7.7); a 128k-invocation round carries close to a
        // million guard cells, and on a quiet branch this answers all of them with two map lookups.
        let mut failed: BTreeSet<LayerId> = BTreeSet::new();
        if self.layers.touched_since(parent, fork_point)? {
            for (layer, guards) in round.guards() {
                if let Err(BorgError::GuardViolated { cell, .. }) =
                    self.layers.check_reads(parent, guards, fork_point)
                {
                    failed.insert(layer);
                    outcome.rejected.push((layer, cell));
                }
            }
        }

        // A producer never probes existence (§8, and `WriteSession::imply_existence`), so nothing in
        // a round's read-set names the object it writes to. Deletion therefore has to be checked
        // rather than guarded — but from the *parent's* changeset, which is small, instead of by
        // asking storage about every event a round produced, which is not. Nothing deleted on the
        // parent means nothing to check, which is the ordinary case and costs one comparison.
        let deleted = self.deleted_since(parent, fork_point).await?;
        if !deleted.is_empty() {
            for layer in round.layers() {
                if failed.contains(&layer) {
                    continue;
                }
                let mut stream = self.storage.read_layer(layer).await?;
                while let Some(row) = stream.next().await {
                    let cell = CellRef::existence_of(&row?.cell);
                    if deleted.contains(&cell) {
                        failed.insert(layer);
                        outcome.rejected.push((layer, cell));
                        break;
                    }
                }
            }
        }

        // Everything that consumed a dropped invocation's output goes too, or the round publishes a
        // value computed from one that never landed.
        let dropped = round.cascade(&failed);
        outcome.cascaded = dropped.len() - failed.len();

        // One parent layer per producer, rather than one per invocation. A layer is an ordered group
        // of events (§6.2) and `LayerAuthor::Derived` describes the whole group, so the round's
        // per-invocation layers — which exist because partial application decides per invocation —
        // do not have to survive the crossing. A fan-out of 128k invocations lands as one layer
        // instead of 128k, which is what keeps fork-and-merge from doubling the log.
        //
        // Grouped as layer *ids*, never as their contents: a round's output may hold millions of
        // events and can never be buffered whole (§6.2, invariant 3). They stream from the child
        // layer straight into the parent one below.
        let mut by_producer: Vec<(LayerAuthor, Vec<LayerId>)> = Vec::new();
        for layer in round.layers() {
            if dropped.contains(&layer) {
                continue;
            }
            outcome.applied += 1;
            let author = self.layers.author_of(layer).unwrap_or(LayerAuthor::Source);
            match by_producer.iter_mut().find(|(seen, _)| *seen == author) {
                Some((_, group)) => group.push(layer),
                None => by_producer.push((author, vec![layer])),
            }
        }

        for (author, carried) in by_producer {
            let mut layer = self.layers.open(parent, LayerKind::Value, author).await?;
            let mut named = 0usize;
            for child in carried {
                // Identities only. The parent layer names the child's event rather than rewriting
                // it (§13), so nothing about the event but its id is wanted — and a round's events
                // each carry the read-set they were computed from, so reading them whole to discard
                // all but the id is a deep copy per event where a pointer would do.
                for event in self.storage.read_membership(child).await? {
                    layer.include(event).await?;
                    named += 1;
                }
            }
            if named == 0 {
                // Every invocation of this producer wrote nothing — the ordinary outcome for a
                // producer whose input was not there yet. An empty layer on the trunk would be a
                // layer id spent to say so.
                self.layers.abort(layer).await?;
                continue;
            }
            outcome.landed.push(self.layers.commit(layer).await?);
        }
        Ok(outcome)
    }

    /// Existence cells the parent has tombstoned since the fork point.
    ///
    /// The parent's changeset rather than the child's output: a round's output is unbounded and its
    /// merge must not pay a storage read per event, while the layers a client landed during one
    /// round are few and usually none at all.
    async fn deleted_since(
        &self,
        parent: BranchId,
        fork_point: LayerId,
    ) -> Result<HashSet<CellRef>> {
        let mut deleted = HashSet::new();
        for layer in self.layers.layers_of(parent) {
            if layer.id.0 <= fork_point.0 || !matches!(layer.author, LayerAuthor::Source) {
                continue;
            }
            let mut stream = self.storage.read_layer(layer.id).await?;
            while let Some(row) = stream.next().await {
                let event = row?;
                if event.value.is_tombstone() && CellRef::existence_of(&event.cell) == event.cell {
                    deleted.insert(event.cell);
                }
            }
        }
        Ok(deleted)
    }

    /// The merge itself: validate everything against the parent, then apply.
    ///
    /// `reads` is the committing transaction's read-set turned into guards (§12). It is checked
    /// **once, first, against the parent as it stood before any of this merge landed** — and that
    /// ordering is load-bearing rather than tidy. Checking it per layer as the layers were applied
    /// would let the first layer of a merge trip a guard belonging to the second, so a transaction
    /// that wrote two layers would conflict with itself for no reason other than the order this
    /// function happens to walk them in.
    async fn replay(
        &self,
        child: BranchId,
        parent: BranchId,
        origin: LayerId,
        mode: MergeMode,
        reads: &[CellRef],
    ) -> Result<Vec<LayerId>> {
        if let Err(violation) = self.layers.check_reads(parent, reads, origin) {
            return Err(rejection(violation));
        }

        let replayable = self.replayable_layers(child, mode);

        // Validate the whole merge first. Nothing is written until every layer has passed.
        let moved_on_parent = self.defs_touched_since(parent, origin).await?;

        let mut staged = Vec::new();
        for layer in &replayable {
            // The child authored its def-mutations against the def-view at the fork point. If the
            // parent has moved the same def since, they cannot cleanly rebase — re-fork from head
            // and redo (SPEC.md §13).
            for event in self.storage.read_def_layer(*layer).await? {
                if let Some(touched) = event.touches()
                    && moved_on_parent.contains(&touched)
                {
                    return Err(BorgError::MergeRejected(MergeRejection::DefDiverged {
                        struct_name: touched.0,
                    }));
                }
            }
            let events = self.contents_of(*layer).await?;

            // Re-evaluate the child's guards against the *parent*, since the fork point. "Has the
            // parent touched this while I was working?" is exactly the definition of a merge
            // conflict, so guards double as the conflict detector for free (SPEC.md §13).
            //
            // **Before the dangling check, not after.** Since the writer's own existence probe is
            // now in its read-set (§12), "the child wrote to an object the parent deleted" is a
            // guard failure first and a dangling write second — and the guard is the more useful of
            // the two things to be told, because it names the cell that moved rather than the cell
            // that suffered for it. The dangling check stays as the backstop for a *blind* write,
            // which observed nothing and so carries nothing to check.
            let guards = self
                .layers
                .layer(*layer)
                .map(|l| l.guards)
                .unwrap_or_default();
            if let Err(violation) = self
                .layers
                .check_guards(parent, &guards, Some(origin))
                .await
            {
                return Err(rejection(violation));
            }
            self.check_dangling(parent, origin, &events).await?;
            staged.push(events);
        }

        let mut replayed = Vec::new();
        for (source, events) in replayable.iter().zip(staged) {
            let kind = self
                .layers
                .layer(*source)
                .map_or(LayerKind::Value, |layer| layer.kind);
            let mut layer = self.layers.open(parent, kind, LayerAuthor::Source).await?;
            for event in self.storage.read_def_layer(*source).await? {
                layer.put_def(event).await?;
            }
            for event in events {
                // **The whole change, in one line.** The parent layer names the child's event; it
                // does not rewrite it. The event keeps its identity, the ClientVersion it was
                // authored at — so the parent's readers migrate and nothing is coerced (§13) — and
                // the layer that authored it, which is what makes lineage survive the merge.
                //
                // Membership is also the reason this needs no revalidation. These events were
                // validated once, on the child, against the def-view they were authored under, and
                // the def layers that made them legal are replayed in this same merge. Re-checking
                // them against a def-view that is itself mid-replay would reject a `DefOnly`
                // merge's own data half and turn "the merge is atomic" into "the merge is atomic if
                // you ordered your layers correctly".
                layer.include(event.id).await?;
            }
            replayed.push(self.layers.commit(layer).await?);
        }
        Ok(replayed)
    }

    /// The child's own **source** layers, oldest first.
    ///
    /// Derived layers are skipped: the child's derived values were computed from the child's data
    /// and are wrong on the parent by construction. The parent re-derives (SPEC.md §13).
    fn replayable_layers(&self, child: BranchId, mode: MergeMode) -> Vec<LayerId> {
        let mut layers: Vec<_> = self
            .layers
            .layers_of(child)
            .into_iter()
            .filter(|layer| matches!(layer.author, LayerAuthor::Source))
            .filter(|layer| match mode {
                MergeMode::DefOnly => layer.kind == LayerKind::Def,
                MergeMode::DefAndData => true,
            })
            .collect();
        layers.sort_by_key(|layer| layer.id.0);
        layers.into_iter().map(|layer| layer.id).collect()
    }

    /// Which definitions the parent has moved since the fork point.
    async fn defs_touched_since(
        &self,
        parent: BranchId,
        fork_point: LayerId,
    ) -> Result<Vec<(ObjectTypeName, FieldName)>> {
        let mut touched = Vec::new();
        for layer in self.layers.layers_of(parent) {
            if layer.kind != LayerKind::Def || layer.id.0 <= fork_point.0 {
                continue;
            }
            for event in self.storage.read_def_layer(layer.id).await? {
                if let Some(key) = event.touches()
                    && !touched.contains(&key)
                {
                    touched.push(key);
                }
            }
        }
        Ok(touched)
    }

    /// A layer's membership, oldest first.
    async fn contents_of(&self, layer: LayerId) -> Result<Vec<Event>> {
        let mut stream = self.storage.read_layer(layer).await?;
        let mut events = Vec::new();
        while let Some(row) = stream.next().await {
            events.push(row?);
        }
        Ok(events)
    }

    /// Reject if the child wrote to an object the parent has since deleted. SPEC.md §13.
    async fn check_dangling(
        &self,
        parent: BranchId,
        fork_point: LayerId,
        events: &[Event],
    ) -> Result<()> {
        let path = self.read_path(parent, None)?;
        for event in events {
            let existence = CellRef::existence_of(&event.cell);
            if existence == event.cell {
                continue;
            }
            // *Landed*, not authored: the question is whether the deletion became visible on the
            // parent after the fork, and a tombstone the parent inherited by merge was authored
            // somewhere else entirely.
            //
            // Read unversioned, not at `event.version`: that is the def-version of the *property*
            // this event wrote, and an existence cell sits on no chain of its own (§5.2).
            let deleted = self
                .storage
                .get_cell(&path, &existence, borg_core::DefVersion::UNVERSIONED)
                .await?
                .filter(|found| found.event.value.is_tombstone())
                .filter(|found| found.landed_at.0 > fork_point.0);
            if let Some(found) = deleted {
                return Err(BorgError::MergeRejected(MergeRejection::DanglingWrite {
                    cell: event.cell.clone(),
                    deleted_at: found.landed_at,
                }));
            }
        }
        Ok(())
    }
}

/// A failed guard, said as a merge conflict.
///
/// The two are the same event seen from either end — a guard is checked at seal *and* re-evaluated
/// against a parent — so the translation lives in one place rather than at each call site.
fn rejection(violation: BorgError) -> BorgError {
    match violation {
        BorgError::GuardViolated { cell, .. } => {
            BorgError::MergeRejected(MergeRejection::GuardConflict { cell })
        }
        other => other,
    }
}

//
//
// A cell no transaction read is last-write-wins, which is the documented default and now the only
// way to get it: a client that expressed no dependency on prior state gets what every database gives
// a blind write (SPEC.md §12).
