//! Poisoned producers. SPEC.md §14.
//!
//! A producer that throws or cycles is **poisoned**: `IllegalState` scoped to the producer, never to
//! the branch, so its cells report `broken` (§10.4) and everything else keeps working. This module
//! is where that judgement is kept.
//!
//! ## Why it is not in the log
//!
//! A poisoning is *discovered*, the way field ownership used to be, and the same objection applies:
//! a layer holds value events xor def events, and a poisoning is neither. It is not a cell — nothing
//! reads it as data — and it is not a definition — nobody wrote it, the engine concluded it. Forcing
//! it into a def layer would make it forkable, mergeable and time-travellable, so a fork would
//! inherit a poisoning its own code never earned and a merge would carry one back. *"Was P broken at
//! layer 400?"* is not a question anybody has.
//!
//! It is therefore **operational state**, in the same class as the pause flags, the transaction
//! table and the producer-implementation table: branch-scoped, discovered at runtime, and outside
//! the log. It is not storage's business either — a `StorageProvider` never learns what derivation
//! is — so it reaches persistence through a provider of its own, and the CLI backs that provider
//! with a file beside the store, exactly like the other three.
//!
//! ## Why the *clearing* edge is in the log even though the setting edge is not
//!
//! §14's recovery is *fix the producer and push a new ClientVersion*. A producer's ClientVersion is
//! the def-layer it was pushed at (§9.2), so recovery is an event the log already records — and a
//! poisoning that **names the version it was recorded against** is self-expiring: it applies while
//! the branch still appoints that version, and stops applying the moment a push moves it. Nothing
//! has to remember to clear it, no command has to be run in the right order, and a record left
//! behind by a store that was restored from a backup cannot poison code that has since been
//! replaced.
//!
//! That is what makes durable operational state safe here. The record is not a fact in its own
//! right; it is a *claim about a fact the log holds*, and the log is what decides whether the claim
//! is still live.

use borg_core::{BranchId, LayerId, ProducerId, Result};
use std::collections::HashMap;
use std::sync::Mutex;

/// One poisoned producer, on one branch.
#[derive(Clone, Debug)]
pub struct Poisoning {
    pub producer: ProducerId,
    /// The producer's ClientVersion when it failed — the def-layer it was pushed at (SPEC.md §9.2).
    ///
    /// This is the whole of the recovery mechanism: see [`Poisoning::applies_to`].
    pub version: LayerId,
    /// What went wrong, as the error rendered it. Surfaced in lineage (§11) and by `borg derive`,
    /// because a poisoning nobody can act on is barely better than silence.
    pub error: String,
    /// The source layer the round that poisoned it was settling. What a reader wants to know is
    /// *since when has this stopped moving*, and this is that layer.
    pub since: LayerId,
}

impl Poisoning {
    /// Whether this record still speaks about a producer standing at `version`.
    ///
    /// A def push that changes the producer gives it a new ClientVersion, and §14 says that is
    /// exactly what recovery is — so a record naming the old one has expired rather than been
    /// forgiven. Equality and not `<`: what matters is that the code moved, and a branch can be
    /// rewound to a version that failed before.
    #[must_use]
    pub const fn applies_to(&self, version: LayerId) -> bool {
        self.version.0 == version.0
    }
}

/// Where poisonings are kept between the process that discovers one and the process that must
/// respect it.
///
/// **This is a persistence seam, not a coordination one.** The in-process implementation below is
/// correct for a single long-lived server, and wrong for a client that exits after every command —
/// which is what made §14 unobservable from the CLI until this existed.
///
/// [`Self::poisoned`] returns a whole branch's records rather than answering per producer, and that
/// is deliberate: the set is bounded by the number of producers defined on a branch, every caller
/// wants either all of them or a cheap *is anything broken here* check, and a per-producer query
/// would make the common answer — nothing is broken — cost one lookup per producer instead of one.
pub trait PoisonProvider: Send + Sync {
    /// Every poisoning recorded on this branch, live or expired. Filtering by ClientVersion is the
    /// caller's job, because only the caller holds the definitions in force.
    fn poisoned(&self, branch: BranchId) -> Result<Vec<Poisoning>>;

    /// Record a poisoning, replacing any earlier one for the same producer. The latest failure is
    /// the one worth reporting; a history of them is a log, and this is not one.
    fn poison(&self, branch: BranchId, poisoning: Poisoning) -> Result<()>;

    /// Forget a producer's poisoning. Called when its ClientVersion has moved, and by an explicit
    /// retry.
    fn clear(&self, branch: BranchId, producer: ProducerId) -> Result<()>;
}

/// v1 `PoisonProvider`: a process-local table.
///
/// The right implementation for a server, and the reason `borg get` used to say `stale` about a
/// producer that was never coming back.
#[derive(Default)]
pub struct MemoryPoison {
    broken: Mutex<HashMap<(BranchId, ProducerId), Poisoning>>,
}

impl MemoryPoison {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl PoisonProvider for MemoryPoison {
    fn poisoned(&self, branch: BranchId) -> Result<Vec<Poisoning>> {
        Ok(self
            .broken
            .lock()
            .unwrap()
            .iter()
            .filter(|((on, _), _)| *on == branch)
            .map(|(_, poisoning)| poisoning.clone())
            .collect())
    }

    fn poison(&self, branch: BranchId, poisoning: Poisoning) -> Result<()> {
        self.broken
            .lock()
            .unwrap()
            .insert((branch, poisoning.producer), poisoning);
        Ok(())
    }

    fn clear(&self, branch: BranchId, producer: ProducerId) -> Result<()> {
        self.broken.lock().unwrap().remove(&(branch, producer));
        Ok(())
    }
}
