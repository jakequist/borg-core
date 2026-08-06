//! Projections of the log. SPEC.md §16.1, §17.1.
//!
//! The log is the only source of truth. Every in-memory index the engine holds — the dependency
//! index, the cell-touch index, the per-producer watermarks — is a **fold over committed layers**,
//! and therefore a cached answer to a question the log already contains. §17.1 says so in as many
//! words: the layer and branch tables are the structure of the log, "everything else the engine
//! holds … is a cache rebuildable by replaying committed layers".
//!
//! This module is that sentence made into a type. A [`Projection`] says what it answers, how to fold
//! one committed layer into it, and — the part that matters — **where it has got to**. Opening a
//! store is then not "replay the log" but *bring every projection to head, however each chooses*,
//! and the difference between the system's two real lifecycles is a starting position:
//!
//! * **Rebuilt from zero.** Position `L0`, so the fold reads every committed layer. This is what a
//!   process-per-command CLI does, and it is the honest cost of a process that exits between
//!   commands: `O(log)` per invocation.
//! * **Maintained live.** A projection that has been fed by the process doing the writing is already
//!   at head, so the same call reads nothing. This is what lets `borg serve` hold one registry for
//!   its lifetime instead of paying the replay per request (`examples/personal-crm/FRICTION.md` #9).
//!
//! Both lifecycles must reach the same state — that is the whole claim — and
//! `crates/borg-engine/tests/projections.rs` is where it is checked rather than asserted, by folding
//! a real store from zero and comparing the answers against the live-maintained set, question for
//! question, after derivation, merges and transactions have happened.
//!
//! ## What a position claims
//!
//! `position()` is *"this projection is up to date with every committed layer at or below here"*,
//! and deliberately not *"I have read these layers' events"*. How a projection got current is its
//! own business: the touch index is folded at commit, and the dependency index and the frontier are
//! told by the derivation engine what an invocation read and wrote **before** the layer holding it
//! commits (`derive.rs`), because re-reading a derived layer to learn what the engine already said
//! would put a full scan of the largest layers in the system on the write path.
//!
//! Positions are compared, never subtracted: a rebuild folds every committed layer above a
//! projection's position, in id order. Layers commit out of order within a branch (§7.3), so a live
//! position can name `L5` with `L4` folded a moment later — which is not a gap, because a layer
//! becomes committed in the same call that folds it. A rebuild only ever sees layers whose commit
//! has returned.

use crate::index::{DependencyIndexProvider, Invocation};
use crate::resolve::FrontierTracker;
use borg_core::{CellAt, Event, Layer, LayerAuthor, LayerId, LayerState, Result};
use borg_storage::StorageProvider;
use futures_util::StreamExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// A cached answer folded from committed layers.
///
/// Deliberately small. It is what the two lifecycles above genuinely share and nothing more — there
/// is no hook here for snapshotting a projection to disk, for a probabilistic summary, or for
/// folding a layer backwards. Those are *implementations* of this seam and are recorded as such in
/// `ROADMAP.md`; a hook written before the thing that needs it is a guess about a shape nobody has
/// built yet.
pub trait Projection: Send + Sync {
    /// What it answers. Used in diagnostics and in the names the rebuild-and-diff tests print.
    fn name(&self) -> &'static str;

    /// The committed layer this projection is up to date through. `L0` means "nothing yet", which is
    /// what a projection built this instant honestly is.
    fn position(&self) -> LayerId;

    /// Whether folding this layer's **events** would tell this projection anything.
    ///
    /// The rebuild reads a layer's events only when some projection behind it says yes, and that is
    /// not a micro-optimisation: the touch index has never wanted derived layers (§12.4 — guards may
    /// name source cells only) and derived layers are the enormous ones. A projection folded from
    /// layer *metadata* alone — the watermarks are — says no to everything.
    fn wants(&self, layer: &Layer) -> bool;

    /// Fold one committed layer in and advance the position to it.
    ///
    /// `events` is the layer's membership when [`wants`](Projection::wants) asked for it and empty
    /// otherwise. Folding a layer twice must leave the same state as folding it once: a live
    /// projection may already hold what a replay is about to hand it.
    fn apply(&self, layer: &Layer, events: &[Event]) -> Result<()>;
}

/// The set of projections one registry maintains, and the two ways they are kept current.
///
/// Held by the [`LayerManager`](crate::log::LayerManager) so that a commit can feed them, and by the
/// [`Registry`](crate::registry::Registry) so that opening one can bring them to head.
pub struct Projections {
    members: Vec<Arc<dyn Projection>>,
}

impl Projections {
    pub fn new(members: impl IntoIterator<Item = Arc<dyn Projection>>) -> Self {
        Self {
            members: members.into_iter().collect(),
        }
    }

    pub fn members(&self) -> &[Arc<dyn Projection>] {
        &self.members
    }

    /// Bring every projection up to the head of the log. SPEC.md §17.1.
    ///
    /// **This is what `Registry::open` costs**, and the cost is a function of how far behind the
    /// projections are rather than of how long the log is: a set built this instant folds every
    /// committed layer, and a set this process has been maintaining folds none. One is the CLI's
    /// per-command rebuild and the other is a server's second request.
    ///
    /// Layer ids are registry-unique and monotonic, so id order is replay order across every branch.
    ///
    /// Returns **how many layers it had to fold**, which is the number `FRICTION.md` #9 was
    /// measuring: a served store used to pay it per read, and now pays it once.
    pub async fn bring_to_head(
        &self,
        storage: &dyn StorageProvider,
        known: &[Layer],
    ) -> Result<usize> {
        let mut ordered: Vec<&Layer> = known
            .iter()
            .filter(|layer| layer.state == LayerState::Committed)
            .collect();
        ordered.sort_by_key(|layer| layer.id.0);

        let mut folded = 0;
        for layer in ordered {
            let behind: Vec<&Arc<dyn Projection>> = self
                .members
                .iter()
                .filter(|p| p.position().0 < layer.id.0)
                .collect();
            if behind.is_empty() {
                continue;
            }
            folded += 1;
            let events = if behind.iter().any(|p| p.wants(layer)) {
                read_layer(storage, layer).await?
            } else {
                Vec::new()
            };
            for projection in behind {
                projection.apply(layer, &events)?;
            }
        }
        Ok(folded)
    }

    /// A layer just committed in this process. The live half of the fold.
    ///
    /// **A source layer is read here; a derived layer is not.** Commit is the only witness a source
    /// layer's cells have — nothing else in the process knows what a client wrote — whereas a
    /// derived layer's consequences reached the dependency index and the frontier from the engine
    /// that produced them, before the layer committed (`derive.rs`, and §16.3.8 on why the edges
    /// have to be there first). Reading one back here to learn what the engine already said would
    /// double the cost of every producer run, which is the one place in the system that cannot
    /// afford it.
    ///
    /// So a derived layer folds nothing and only advances the position — which is exactly what
    /// `position` claims: up to date through here, however it got there.
    pub async fn committed(&self, layer: &Layer, storage: &dyn StorageProvider) -> Result<()> {
        let events = if matches!(layer.author, LayerAuthor::Source)
            && self.members.iter().any(|p| p.wants(layer))
        {
            read_layer(storage, layer).await?
        } else {
            Vec::new()
        };
        for projection in &self.members {
            projection.apply(layer, &events)?;
        }
        Ok(())
    }
}

/// A layer's membership, buffered.
///
/// Buffered here and nowhere else that matters: this is the one place the engine deliberately
/// materialises a layer, and it is the same buffering `Registry::open` has always done to replay one
/// (`read_layer` is a stream because commit is; replaying is not commit). A layer holding millions of
/// mutations is why "commit streams" is an invariant (§6.2) — a rebuild that had to fit one in memory
/// is a known limit of the from-zero lifecycle and is recorded in `CLAUDE.md`, not a new one.
async fn read_layer(storage: &dyn StorageProvider, layer: &Layer) -> Result<Vec<Event>> {
    let mut stream = storage.read_layer(layer.id).await?;
    let mut events = Vec::new();
    while let Some(row) = stream.next().await {
        events.push(row?);
    }
    Ok(events)
}

/// Where a projection has got to, as a field.
///
/// A separate little type because three projections need exactly this and an `AtomicU64` spelled out
/// three times invites one of them to be updated in only some of its `apply` paths.
#[derive(Default, Debug)]
pub struct Position(AtomicU64);

impl Position {
    pub fn get(&self) -> LayerId {
        LayerId(self.0.load(Ordering::Relaxed))
    }

    /// Advance to a layer, never backwards. Monotonic for the same reason the frontier is: layers
    /// commit out of order (§7.3), and a lower id arriving second must not walk the position back.
    pub fn reached(&self, layer: LayerId) {
        self.0.fetch_max(layer.0, Ordering::Relaxed);
    }
}

// --- The dependency index as a projection ---------------------------------------------------------

/// The dependency index, folded from what derived events record about themselves.
///
/// A wrapper rather than an impl on [`MemoryDependencyIndex`](crate::index::MemoryDependencyIndex),
/// because the fold is a fact about *events* and not about any one index implementation: a sharded
/// or a disk-backed index would be folded from exactly these fields. It is also what keeps the
/// `DependencyIndexProvider` interface free of layers, which §17.2 wants it to stay.
pub struct DependencyProjection {
    index: Arc<dyn DependencyIndexProvider>,
    position: Position,
}

impl DependencyProjection {
    pub fn new(index: Arc<dyn DependencyIndexProvider>) -> Self {
        Self {
            index,
            position: Position::default(),
        }
    }

    pub fn index(&self) -> &Arc<dyn DependencyIndexProvider> {
        &self.index
    }
}

impl Projection for DependencyProjection {
    fn name(&self) -> &'static str {
        "dependency index"
    }

    fn position(&self) -> LayerId {
        self.position.get()
    }

    fn wants(&self, layer: &Layer) -> bool {
        matches!(layer.author, LayerAuthor::Derived { .. })
    }

    fn apply(&self, layer: &Layer, events: &[Event]) -> Result<()> {
        if let LayerAuthor::Derived { producer, .. } = layer.author {
            for event in events {
                // Only a derived event carries a read-set, and only a read-set is an edge. A merge
                // layer's membership can hold events authored elsewhere; they are folded under the
                // branch the layer belongs to, which is what keys the graph on the trunk (§16.3.8).
                let Some(derivation) = &event.derivation else {
                    continue;
                };
                let invocation = Invocation {
                    producer,
                    input: *event.cell.pid(),
                };
                let written = [CellAt::new(event.cell.clone(), event.version)];
                self.index
                    .record(layer.branch, &invocation, &derivation.read_set, &written)?;
            }
        }
        self.position.reached(layer.id);
        Ok(())
    }
}

// --- The watermarks as a projection ---------------------------------------------------------------

/// The per-producer watermarks, folded from derived layer **metadata**.
///
/// The one projection that wants no events at all: a derived layer records the source layer it
/// reflects in its own author (§6.3), so how far a producer has caught up is readable from the layer
/// table. That is why [`Projection::wants`] exists — a fold that needs no membership should not make
/// a replay read one.
pub struct FrontierProjection {
    frontier: Arc<FrontierTracker>,
    position: Position,
}

impl FrontierProjection {
    pub fn new(frontier: Arc<FrontierTracker>) -> Self {
        Self {
            frontier,
            position: Position::default(),
        }
    }
}

impl Projection for FrontierProjection {
    fn name(&self) -> &'static str {
        "watermarks"
    }

    fn position(&self) -> LayerId {
        self.position.get()
    }

    fn wants(&self, _layer: &Layer) -> bool {
        false
    }

    fn apply(&self, layer: &Layer, _events: &[Event]) -> Result<()> {
        if let LayerAuthor::Derived { producer, reflects } = layer.author {
            self.frontier.advance(layer.branch, producer, reflects);
        }
        self.position.reached(layer.id);
        Ok(())
    }
}
