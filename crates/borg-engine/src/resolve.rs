//! The read path and frontier tracking. SPEC.md §10, §11.
//!
//! Resolving a cell: locate the record at the reader's ClientVersion, account for any def-version
//! skew, validate the watermark, and build the provenance envelope. Borg never returns a bare
//! value — every read states what it reflects and how stale it may be.

use crate::branch::BranchManager;
use crate::defs::DefRegistry;
use crate::index::DependencyIndexProvider;
use crate::poison::{MemoryPoison, PoisonProvider, Poisoning};
use borg_core::{
    BranchId, CellAt, CellRef, ClientVersion, DefVersion, Derivation, Freshness,
    FreshnessRequirement, LayerId, Origin, ProducerId, ReadPath, Resolved, Result, Value,
};
use borg_storage::StorageProvider;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

/// Per-producer watermarks and the settled frontier. SPEC.md §10.3.
///
/// A watermark is the source layer through which a producer has incorporated every input — *"replay
/// the world at this layer and you get exactly this output."*
#[derive(Default)]
pub struct FrontierTracker {
    watermarks: Mutex<HashMap<(BranchId, ProducerId), LayerId>>,
    /// Woken whenever a watermark moves, so [`FrontierTracker::reaches`] re-checks instead of
    /// polling on a timer. Notification carries no payload on purpose: a waiter re-reads the
    /// frontier, which means a coalesced burst of advances is indistinguishable from one — the
    /// property that keeps this correct once several workers advance watermarks at once.
    advanced: Notify,
}

impl FrontierTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// How far this producer has caught up. `LayerId(0)` means "nothing incorporated yet", which is
    /// correct for a producer that has never run.
    pub fn watermark(&self, branch: BranchId, producer: ProducerId) -> LayerId {
        self.watermarks
            .lock()
            .unwrap()
            .get(&(branch, producer))
            .copied()
            .unwrap_or(LayerId(0))
    }

    /// Advance monotonically. Never moves backwards, so a late-arriving run cannot un-catch-up a
    /// producer.
    pub fn advance(&self, branch: BranchId, producer: ProducerId, to: LayerId) {
        let moved = {
            let mut marks = self.watermarks.lock().unwrap();
            let entry = marks.entry((branch, producer)).or_insert(LayerId(0));
            if to.0 > entry.0 {
                *entry = to;
                true
            } else {
                false
            }
        };
        // Notified outside the lock: a waiter wakes to re-read the frontier, and waking it while
        // holding the lock it is about to take is how that becomes a stall.
        if moved {
            self.advanced.notify_waiters();
        }
    }

    /// Forget that this producer has ever run here.
    ///
    /// Not `advance` running backwards — that direction is closed on purpose, because a late run
    /// must not un-catch-up a producer that has since moved on. This is the different statement that
    /// *nothing on this branch has been incorporated*, which is true exactly when the derived layers
    /// standing behind the watermark are being thrown away and recomputed (§6.3). Its one caller is
    /// [`crate::derive::DerivationEngine::recompute`], and going through `advance` would make the
    /// two indistinguishable.
    pub fn rewind(&self, branch: BranchId, producer: ProducerId) {
        self.watermarks
            .lock()
            .unwrap()
            .insert((branch, producer), LayerId(0));
    }

    /// The layer through which *all* derived data on this branch is caught up — the minimum over
    /// every producer. Reading here gives a fully coherent snapshot, slightly in the past, as
    /// opposed to the ragged head (SPEC.md §10.5).
    ///
    /// `None` where the branch has no producers: nothing derives, so nothing can lag, and the
    /// settled frontier *is* the head. Returning `LayerId(0)` for that case would read as "caught up
    /// through nothing" and make a settled read on a producerless branch see an empty world.
    pub fn settled(&self, branch: BranchId, producers: &[ProducerId]) -> Option<LayerId> {
        producers.iter().map(|p| self.watermark(branch, *p)).min()
    }

    /// Block until every producer on this branch has incorporated `target`. SPEC.md §10.5.
    ///
    /// This is read-after-write consistency for the clients that want it, and deterministic tests
    /// without making the system synchronous: write, note the layer, await it, read. Everyone else
    /// keeps reading at whatever the frontier has got to and is told how far behind that is.
    ///
    /// Deliberately has no timeout of its own. A deadline is the caller's policy — a report may
    /// happily wait a minute where an API handler must not wait at all — and a primitive that
    /// chooses one for you is a primitive that has to be worked around.
    pub async fn reaches(&self, branch: BranchId, producers: &[ProducerId], target: LayerId) {
        loop {
            // Interest is registered *before* the check. The other order loses an advance that lands
            // between the two, and the waiter then sleeps through the very wake-up it needed.
            let notified = self.advanced.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .settled(branch, producers)
                .is_none_or(|settled| settled.0 >= target.0)
            {
                return;
            }
            notified.await;
        }
    }
}

/// *Bring this cell up to date, now.* The engine side of `FreshnessRequirement::Current`
/// (SPEC.md §10.5).
///
/// The resolver holds one of these rather than the derivation engine itself, and the narrowness is
/// the point. A read that needs a fresh answer needs exactly one thing — this cell, computed — and
/// nothing else the engine can do: not catching a branch up, not settling a round, not registering
/// producers. Handing the resolver the engine would make the read path a second entry point into
/// derivation, with two callers of `settle` and two opinions about what a round is; handing it this
/// makes the read path a *client* of derivation, which is what §10.5 says it is when it calls lazy
/// materialization "a per-read client mode, not a system architecture".
///
/// It also keeps the dependency pointing one way. Derivation reads cells through storage and never
/// through the resolver, so nothing here can close a loop.
#[async_trait::async_trait]
pub trait InlineDerivation: Send + Sync {
    /// Make this cell, at this version, correct as of the branch head — running whatever producers
    /// that takes, including the ones that produce its inputs.
    ///
    /// Returns `Ok(())` when there is nothing to be done, which includes the cases where the cell is
    /// source data, where no producer here can produce it, and where the chain leading to it turns
    /// out to be cyclic. The read that follows describes the outcome; this reports only failures the
    /// caller could not have discovered by reading.
    async fn compute_now(
        &self,
        branch: BranchId,
        cell: &CellRef,
        version: DefVersion,
    ) -> Result<()>;
}

/// One edge of a cell's provenance. SPEC.md §11.
#[derive(Clone, Debug)]
pub struct LineageEdge {
    pub cell: CellAt,
    pub origin: Origin,
    /// Where this input arrived on the branch being explained. What lineage wants to know about an
    /// input is when it became visible here, which for a merged input is the merge, not the write.
    pub landed_at: LayerId,
}

/// Where a value came from. Requires no storage of its own — it is the dependency index read
/// backwards (SPEC.md §11).
#[derive(Clone, Debug)]
pub struct Lineage {
    pub cell: CellAt,
    pub produced_by: Option<ProducerId>,
    /// Where this value was first committed, and where it landed on the branch being explained.
    /// They differ exactly when the value arrived by merge (SPEC.md §13).
    pub authored_at: LayerId,
    pub landed_at: LayerId,
    pub fresh_as_of: LayerId,
    pub from: Vec<LineageEdge>,
    /// Why this value stopped moving, when its producer is poisoned (SPEC.md §14). `explain` is
    /// where §14's *"lineage explaining why"* is actually said — the envelope carries the state, and
    /// a state without a reason sends whoever read it looking through logs for one.
    pub broken: Option<String>,
}

pub struct Resolver {
    storage: Arc<dyn StorageProvider>,
    index: Arc<dyn DependencyIndexProvider>,
    defs: Arc<DefRegistry>,
    branches: Arc<BranchManager>,
    inline: Arc<dyn InlineDerivation>,
    /// The same table the derivation engine writes to. Two views of one fact: the scheduler stops
    /// running a poisoned producer, and the reader is told that is why (SPEC.md §14, §10.4).
    poison: Arc<dyn PoisonProvider>,
}

impl Resolver {
    pub fn new(
        storage: Arc<dyn StorageProvider>,
        index: Arc<dyn DependencyIndexProvider>,
        defs: Arc<DefRegistry>,
        branches: Arc<BranchManager>,
        inline: Arc<dyn InlineDerivation>,
    ) -> Self {
        Self {
            storage,
            index,
            defs,
            branches,
            inline,
            poison: Arc::new(MemoryPoison::new()),
        }
    }

    /// Read poisonings from where they actually live. SPEC.md §14.
    ///
    /// The default is the in-process table, which is right for a resolver sharing a process with the
    /// engine and empty for one that is not — so a client that exits between commands must pass the
    /// same durable table its deriver was given, or its reads will call a broken producer's output
    /// merely stale.
    #[must_use]
    pub fn with_poison(mut self, poison: Arc<dyn PoisonProvider>) -> Self {
        self.poison = poison;
        self
    }

    /// Read one cell, with provenance.
    ///
    /// The value is `Option` because *absent* and *not yet migrated to your version* are different
    /// facts, and `state` is what distinguishes them.
    pub async fn resolve(
        &self,
        branch: BranchId,
        cell: &CellRef,
        layer: Option<LayerId>,
        version: ClientVersion,
        requirement: FreshnessRequirement,
    ) -> Result<Resolved<Option<Value>>> {
        // **Which record this reader means.** A ClientVersion is a whole schema; a record is keyed
        // by the def-version of *its own field* (§5.3). The reader's own view is what translates one
        // into the other, and asking it is the only way to get from here to there — which is what
        // keeps a def push naming some other field from moving this cell out from under this read.
        //
        // Folded before the inline computation below, deliberately: derivation commits value layers
        // only (§6.2), so no amount of computing can change what this answers.
        let at = self
            .defs
            .view_at(&self.branches.read_path(branch, layer)?, version.0)
            .await?
            .version_of(cell);
        // **`Current` pays at the call site** — but only where there is something to pay for.
        // Everything below this line is the same read every other requirement gets; the difference
        // is that the world may have been brought up to date first. It happens *before* the read
        // path is resolved, because computing commits layers and the answer must be read from after
        // them.
        if requirement == FreshnessRequirement::Current
            && self.can_compute_at(branch, layer)?
            && !self.settled_here(branch, cell, at, layer).await?
        {
            self.inline.compute_now(branch, cell, at).await?;
        }
        // `None` means HEAD — which for a fork that has not written anything yet is the fork point
        // it inherits from, not a head of its own.
        let path = self.branches.read_path(branch, layer)?;
        let layer = layer.unwrap_or_else(|| path.ceiling());
        let Some(found) = self.storage.get_cell(&path, cell, at).await? else {
            return self.resolve_unmaterialized(&path, cell, layer, at).await;
        };
        // Both halves of the provenance, which only exist separately now that events are not
        // rewritten on the way across a merge: where the value was authored, and where the branch
        // being read acquired it (SPEC.md §4.3, §13).
        let landed_at = found.landed_at;
        let record = found.event;

        let Some(derivation) = record.derivation else {
            // Source data is ground truth: written once and correct thereafter, so `landed_at` and
            // `fresh_as_of` collapse and the state is always `Current` (SPEC.md §10.4).
            return Ok(Resolved {
                value: (!record.value.is_tombstone()).then_some(record.value),
                origin: Origin::Source,
                event: Some(record.id),
                authored_at: record.authored,
                landed_at,
                fresh_as_of: layer,
                state: if record.value.is_tombstone() {
                    Freshness::Tombstoned
                } else {
                    Freshness::Current
                },
                by: None,
            });
        };

        // Taken apart rather than borrowed from: a read-set can be enormous (§9.4), and validation
        // consumes it.
        let Derivation {
            producer,
            fresh_as_of: stored,
            read_set,
        } = derivation;

        // Reads validate before reporting, so the returned watermark is tight rather than
        // pessimistically understated (SPEC.md §10.2).
        let (state, fresh_as_of) = match requirement {
            FreshnessRequirement::Any => {
                if stored.0 >= layer.0 {
                    (Freshness::Current, layer)
                } else {
                    (Freshness::Unvalidated, stored)
                }
            }
            // `Current` validates too rather than assuming its own computation succeeded. If the
            // producer that owns this cell is not resolvable here, the honest answer is still the
            // one validation gives — which is what stops "I asked for current" from becoming "I was
            // told current".
            FreshnessRequirement::Validated | FreshnessRequirement::Current => {
                let watermark = self
                    .validate(&path, read_set, stored, layer, &mut HashMap::new())
                    .await?;
                if watermark.0 >= layer.0 {
                    (Freshness::Current, layer)
                } else {
                    (Freshness::Stale, watermark)
                }
            }
        };

        // **A poisoned producer overrides what validation concluded, rather than refining it.**
        // Validation answers *how far can this value honestly claim to be correct*, and its answer
        // here is `stale` — which promises a catch-up. For a producer that threw or cycled no
        // catch-up is coming until somebody pushes new code (SPEC.md §14), and a client told `stale`
        // will wait for one. `fresh_as_of` is left where validation put it: the value really is
        // correct through there, and the news is that it will not move.
        let state = if self.poisoning(branch, producer).await?.is_some() {
            Freshness::Broken
        } else {
            state
        };

        Ok(Resolved {
            value: (!record.value.is_tombstone()).then_some(record.value),
            origin: Origin::Derived,
            event: Some(record.id),
            authored_at: record.authored,
            landed_at,
            fresh_as_of,
            state: if record.value.is_tombstone() {
                Freshness::Tombstoned
            } else {
                state
            },
            by: Some(producer),
        })
    }

    /// Whether the producer that owns a cell is poisoned on this branch. SPEC.md §14.
    ///
    /// Two lookups, and the first is the one that matters for cost: a branch with nothing broken —
    /// every branch, almost always — answers from the poison table alone and never folds anything.
    /// Only a branch that has something recorded pays to find out which ClientVersion its producer
    /// stands at now.
    ///
    /// That version is read from the branch **as it stands**, not through the layer or the
    /// ClientVersion this read is pinned to. Whether the broken code has since been replaced is a
    /// fact about the branch, and a reader on an older view is not entitled to a different answer
    /// about it than a reader on a newer one.
    async fn poisoning(&self, branch: BranchId, producer: ProducerId) -> Result<Option<Poisoning>> {
        let recorded = self.poison.poisoned(branch)?;
        let Some(poisoning) = recorded
            .into_iter()
            .find(|poisoning| poisoning.producer == producer)
        else {
            return Ok(None);
        };
        let version = self
            .defs
            .view(&self.branches.read_path(branch, None)?)
            .await?
            .producers()
            .find(|def| def.id == producer)
            .map(|def| def.version);
        Ok(version
            .is_none_or(|version| poisoning.applies_to(version))
            .then_some(poisoning))
    }

    /// Whether this cell is already correct as of the layer being read, so that computing it would
    /// only re-derive what is already there.
    ///
    /// This is what makes `freshness: 'current'` **converge**. Without it every such read runs the
    /// producer again — and a chained one runs the whole chain again — however settled the branch
    /// is, because an inline computation deliberately advances no watermark (§10.5) and so leaves
    /// behind no mark that the next read could recognise. Validation is that mark: it costs a few
    /// index lookups, runs no user code (§10.2), and is the same walk the read below performs to
    /// decide what to report.
    async fn settled_here(
        &self,
        branch: BranchId,
        cell: &CellRef,
        at: DefVersion,
        layer: Option<LayerId>,
    ) -> Result<bool> {
        let path = self.branches.read_path(branch, layer)?;
        let ceiling = layer.unwrap_or_else(|| path.ceiling());
        // Nothing stored at this version: what is owed is a migration, and running it is exactly
        // what this reader asked to pay for (§10.4).
        let Some(found) = self.storage.get_cell(&path, cell, at).await? else {
            return Ok(false);
        };
        // Source data is ground truth — there is nothing to compute and nothing behind it.
        let Some(by) = found.event.derivation else {
            return Ok(true);
        };
        let watermark = self
            .validate(
                &path,
                by.read_set,
                by.fresh_as_of,
                ceiling,
                &mut HashMap::new(),
            )
            .await?;
        Ok(watermark.0 >= ceiling.0)
    }

    /// Whether computing inline could change this read's answer.
    ///
    /// It cannot for a read pinned below the branch head. That is a *historical* read — "what did
    /// this say at L40" — and nothing can make the past current: whatever a producer computed now
    /// would land in a layer above the one being read through and stay invisible. `Current` there
    /// means what `Validated` means, and the value it returns is already the final answer for that
    /// layer rather than a weaker version of one.
    fn can_compute_at(&self, branch: BranchId, layer: Option<LayerId>) -> Result<bool> {
        let Some(requested) = layer else {
            return Ok(true);
        };
        Ok(requested.0 >= self.branches.read_path(branch, None)?.ceiling().0)
    }

    /// **Validate**: how far this value can honestly claim to be correct.
    ///
    /// Runs no user code — that is the whole point of separating this from recompute. A cell that
    /// depends on three fields is unaffected by the forty thousand writes that landed meanwhile, and
    /// advances to head for the cost of a few lookups (SPEC.md §10.2).
    ///
    /// Returns §10.3's composition rather than a yes/no:
    ///
    /// ```text
    /// W(B) = min(target, W(A), W(other deps...))
    /// ```
    ///
    /// Two things are being asked of each dependency, and they are not the same question:
    ///
    /// * **Has it moved past what I claim to reflect?** [`Landed::reflects`] is what that is asked
    ///   of — the *source* layer a record's content reflects — because a watermark points into the
    ///   source stream. For source data that is where it landed; for derived data it is its
    ///   producer's watermark, and never the derived layer it sits in, which is always above the
    ///   source layer it reflects and so would make every chained value look permanently overtaken.
    /// * **How far is it good for?** Only derived dependencies bound anything: a chain is only as
    ///   fresh as the hop behind it, so this recurses and takes the minimum. Source data is ground
    ///   truth and bounds nothing.
    ///
    /// `composed` memoizes the answer per dependency, which keeps a diamond-shaped graph linear —
    /// and doubles as the cycle guard: a dependency reached while it is still being validated
    /// (`None`) contributes its own stored watermark, which claims nothing it has not earned.
    fn validate<'a>(
        &'a self,
        path: &'a ReadPath,
        read_set: Vec<CellAt>,
        fresh_as_of: LayerId,
        target: LayerId,
        composed: &'a mut HashMap<CellAt, Option<LayerId>>,
    ) -> Pin<Box<dyn Future<Output = Result<LayerId>> + Send + 'a>> {
        Box::pin(async move {
            let mut watermark = target;
            for dependency in read_set {
                // Each dependency is read at the version the producer *read it at*, not at the
                // reader's version. Those differ whenever a migration is involved.
                let Some(found) = self
                    .storage
                    .get_cell(path, &dependency.cell, dependency.version)
                    .await?
                else {
                    // Absent when it was read, absent now. Absence is a dependency like any other
                    // (§9.4) and this one has not changed.
                    continue;
                };
                if found.reflects().0 > fresh_as_of.0 {
                    // Known to have moved: nothing above this value's own watermark is defensible.
                    return Ok(fresh_as_of);
                }
                let Some(upstream) = found.event.derivation else {
                    continue;
                };
                let bound = match composed.get(&dependency) {
                    Some(Some(known)) => *known,
                    Some(None) => upstream.fresh_as_of,
                    None => {
                        composed.insert(dependency.clone(), None);
                        let bound = self
                            .validate(
                                path,
                                upstream.read_set,
                                upstream.fresh_as_of,
                                target,
                                composed,
                            )
                            .await?;
                        composed.insert(dependency.clone(), Some(bound));
                        bound
                    }
                };
                watermark = watermark.min(bound);
            }
            Ok(watermark)
        })
    }

    /// The cell is not materialized at the reader's version.
    ///
    /// This is not an error — it is a migration that has not caught up. If a path to some
    /// materialized version exists, the honest answer is `Stale`; if no path exists, this reader's
    /// ClientVersion is unreachable, which is what a def-push without a `down` migration does to
    /// older clients (SPEC.md §9.3).
    ///
    /// A reader unwilling to take that lag asks for `Current`, which walks the same path and runs
    /// its hops before the read gets here (SPEC.md §10.5). Reaching this with `Current` therefore
    /// means the hops genuinely could not be run, and `Stale` is still the truthful answer.
    async fn resolve_unmaterialized(
        &self,
        path: &ReadPath,
        cell: &CellRef,
        _layer: LayerId,
        version: DefVersion,
    ) -> Result<Resolved<Option<Value>>> {
        let available = self.storage.cell_versions(path, cell).await?;

        // Definitions are resolved along the same ancestry as data, so which migrations exist is
        // itself branch-scoped (SPEC.md §5).
        let defs = self.defs.view(path).await?;
        let reachable = available
            .iter()
            .any(|from| defs.path(&cell.buffer, *from, version).is_some());

        Ok(Resolved {
            value: None,
            origin: Origin::Derived,
            // Nothing is stored at this version, so there is no event to name and no layer that
            // could have carried one.
            event: None,
            authored_at: LayerId(0),
            landed_at: LayerId(0),
            fresh_as_of: LayerId(0),
            state: if available.is_empty() {
                // Genuinely absent at every version — the cell was simply never written.
                Freshness::Current
            } else if reachable {
                Freshness::Stale
            } else {
                Freshness::Broken
            },
            by: None,
        })
    }

    /// Walk the dependency index backwards. SPEC.md §11.
    pub async fn explain(
        &self,
        branch: BranchId,
        cell: &CellRef,
        layer: Option<LayerId>,
        version: ClientVersion,
    ) -> Result<Option<Lineage>> {
        let path = self.branches.read_path(branch, layer)?;
        let at = self.defs.view_at(&path, version.0).await?.version_of(cell);
        let Some(found) = self.storage.get_cell(&path, cell, at).await? else {
            return Ok(None);
        };
        let target = CellAt::new(cell.clone(), at);
        let dependencies = self.index.dependencies(branch, &target)?;

        let mut from = Vec::new();
        for dependency in dependencies {
            if let Some(source) = self
                .storage
                .get_cell(&path, &dependency.cell, dependency.version)
                .await?
            {
                from.push(LineageEdge {
                    cell: dependency,
                    origin: source.event.origin,
                    landed_at: source.landed_at,
                });
            }
        }

        let produced_by = found.event.derivation.as_ref().map(|d| d.producer);
        // §14 promises lineage that explains *why* a cell is broken, and this is where that sentence
        // is. Source data has no producer and so can never be broken, which falls out.
        let broken = match produced_by {
            Some(producer) => self
                .poisoning(branch, producer)
                .await?
                .map(|poisoning| poisoning.error),
            None => None,
        };

        Ok(Some(Lineage {
            cell: target,
            produced_by,
            authored_at: found.event.authored,
            landed_at: found.landed_at,
            fresh_as_of: found
                .event
                .derivation
                .as_ref()
                .map_or(found.landed_at, |d| d.fresh_as_of),
            from,
            broken,
        }))
    }
}
