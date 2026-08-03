//! The read path and frontier tracking. SPEC.md §10, §11.
//!
//! Resolving a cell: locate the record at the reader's ClientVersion, account for any def-version
//! skew, validate the watermark, and build the provenance envelope. Borg never returns a bare
//! value — every read states what it reflects and how stale it may be.

use crate::defs::DefRegistry;
use crate::index::DependencyIndexProvider;
use borg_core::{
    BranchId, CellRef, ClientVersion, Freshness, FreshnessRequirement, LayerId, Origin, ProducerId,
    Resolved, Result, Value,
};
use borg_storage::StorageProvider;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Per-producer watermarks and the settled frontier. SPEC.md §10.3.
///
/// A watermark is the source layer through which a producer has incorporated every input — *"replay
/// the world at this layer and you get exactly this output."*
#[derive(Default)]
pub struct FrontierTracker {
    watermarks: Mutex<HashMap<(BranchId, ProducerId), LayerId>>,
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
        let mut marks = self.watermarks.lock().unwrap();
        let entry = marks.entry((branch, producer)).or_insert(LayerId(0));
        if to.0 > entry.0 {
            *entry = to;
        }
    }

    /// The layer through which *all* derived data on this branch is caught up — the minimum over
    /// every producer. Reading here gives a fully coherent snapshot, slightly in the past, as
    /// opposed to the ragged head (SPEC.md §10.5).
    pub fn settled(&self, branch: BranchId, producers: &[ProducerId]) -> LayerId {
        producers
            .iter()
            .map(|p| self.watermark(branch, *p))
            .min()
            .unwrap_or(LayerId(0))
    }
}

/// One edge of a cell's provenance. SPEC.md §11.
#[derive(Clone, Debug)]
pub struct LineageEdge {
    pub cell: CellRef,
    pub origin: Origin,
    pub written_at: LayerId,
}

/// Where a value came from. Requires no storage of its own — it is the dependency index read
/// backwards (SPEC.md §11).
#[derive(Clone, Debug)]
pub struct Lineage {
    pub cell: CellRef,
    pub produced_by: Option<ProducerId>,
    pub written_at: LayerId,
    pub fresh_as_of: LayerId,
    pub from: Vec<LineageEdge>,
}

pub struct Resolver {
    storage: Arc<dyn StorageProvider>,
    index: Arc<dyn DependencyIndexProvider>,
    defs: Arc<DefRegistry>,
}

impl Resolver {
    pub fn new(
        storage: Arc<dyn StorageProvider>,
        index: Arc<dyn DependencyIndexProvider>,
        defs: Arc<DefRegistry>,
    ) -> Self {
        Self {
            storage,
            index,
            defs,
        }
    }

    /// Read one cell, with provenance.
    ///
    /// The value is `Option` because *absent* and *not yet migrated to your version* are different
    /// facts, and `state` is what distinguishes them.
    pub async fn resolve(
        &self,
        branch: BranchId,
        cell: &CellRef,
        layer: LayerId,
        version: ClientVersion,
        requirement: FreshnessRequirement,
    ) -> Result<Resolved<Option<Value>>> {
        let Some(record) = self.storage.get_cell(branch, cell, layer, version).await? else {
            return self
                .resolve_unmaterialized(branch, cell, layer, version)
                .await;
        };

        let Some(derivation) = record.derivation else {
            // Source data is ground truth: written once and correct thereafter, so `written_at` and
            // `fresh_as_of` collapse and the state is always `Current` (SPEC.md §10.4).
            return Ok(Resolved {
                value: (!record.value.is_tombstone()).then_some(record.value),
                origin: Origin::Source,
                written_at: record.written_at,
                fresh_as_of: layer,
                state: if record.value.is_tombstone() {
                    Freshness::Tombstoned
                } else {
                    Freshness::Current
                },
                by: None,
            });
        };

        // Reads validate before reporting, so the returned watermark is tight rather than
        // pessimistically understated (SPEC.md §10.2).
        let state = match requirement {
            FreshnessRequirement::Any => {
                if derivation.fresh_as_of.0 >= layer.0 {
                    Freshness::Current
                } else {
                    Freshness::Unvalidated
                }
            }
            FreshnessRequirement::Validated | FreshnessRequirement::Current => {
                self.validate(
                    branch,
                    &derivation.read_set,
                    derivation.fresh_as_of,
                    layer,
                    version,
                )
                .await?
            }
        };

        let fresh_as_of = if state == Freshness::Current {
            layer
        } else {
            derivation.fresh_as_of
        };

        Ok(Resolved {
            value: (!record.value.is_tombstone()).then_some(record.value),
            origin: Origin::Derived,
            written_at: record.written_at,
            fresh_as_of,
            state: if record.value.is_tombstone() {
                Freshness::Tombstoned
            } else {
                state
            },
            by: Some(derivation.producer),
        })
    }

    /// **Validate**: check whether anything in the read-set moved since the value was computed.
    ///
    /// Runs no user code — that is the whole point of separating this from recompute. A cell that
    /// depends on three fields is unaffected by the forty thousand writes that landed meanwhile, and
    /// advances to head for the cost of a few lookups (SPEC.md §10.2).
    async fn validate(
        &self,
        branch: BranchId,
        read_set: &[CellRef],
        fresh_as_of: LayerId,
        target: LayerId,
        version: ClientVersion,
    ) -> Result<Freshness> {
        for dependency in read_set {
            let moved = self
                .storage
                .get_cell(branch, dependency, target, version)
                .await?
                .is_some_and(|record| record.written_at.0 > fresh_as_of.0);
            if moved {
                return Ok(Freshness::Stale);
            }
        }
        Ok(Freshness::Current)
    }

    /// The cell is not materialized at the reader's version.
    ///
    /// This is not an error — it is a migration that has not caught up. If a path to some
    /// materialized version exists, the honest answer is `Stale`; if no path exists, this reader's
    /// ClientVersion is unreachable, which is what a def-push without a `down` migration does to
    /// older clients (SPEC.md §9.3).
    async fn resolve_unmaterialized(
        &self,
        branch: BranchId,
        cell: &CellRef,
        layer: LayerId,
        version: ClientVersion,
    ) -> Result<Resolved<Option<Value>>> {
        let available = self.storage.cell_versions(branch, cell, layer).await?;

        let reachable = available
            .iter()
            .any(|from| self.defs.path(&cell.buffer, *from, version).is_some());

        Ok(Resolved {
            value: None,
            origin: Origin::Derived,
            written_at: LayerId(0),
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
        layer: LayerId,
        version: ClientVersion,
    ) -> Result<Option<Lineage>> {
        let Some(record) = self.storage.get_cell(branch, cell, layer, version).await? else {
            return Ok(None);
        };
        let dependencies = self.index.dependencies(branch, cell)?;

        let mut from = Vec::new();
        for dependency in dependencies {
            if let Some(source) = self
                .storage
                .get_cell(branch, &dependency, layer, version)
                .await?
            {
                from.push(LineageEdge {
                    cell: dependency,
                    origin: source.origin,
                    written_at: source.written_at,
                });
            }
        }

        Ok(Some(Lineage {
            cell: cell.clone(),
            produced_by: record.derivation.as_ref().map(|d| d.producer),
            written_at: record.written_at,
            fresh_as_of: record
                .derivation
                .as_ref()
                .map_or(record.written_at, |d| d.fresh_as_of),
            from,
        }))
    }
}
