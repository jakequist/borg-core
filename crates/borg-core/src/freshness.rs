//! Freshness and provenance — what a read actually returns. SPEC.md §10.
//!
//! Borg does not pretend computed values are current. Every read carries what it reflects and how
//! stale it may be. That honesty is what converts eager-vs-lazy from an architectural commitment
//! into a scheduling policy which cannot affect correctness, only latency.

use crate::ids::{LayerId, ProducerId};
use serde::{Deserialize, Serialize};

/// The confidence attached to a resolved value. SPEC.md §10.4.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Freshness {
    /// `fresh_as_of == requested layer`. Guaranteed correct. Source cells are always this.
    Current,
    /// Behind, but unchecked. Cheap to resolve — see `validate` in SPEC.md §10.2.
    Unvalidated,
    /// A dependency is known to have moved. Definitely out of date.
    Stale,
    /// The producer threw or cycled. `IllegalState`, scoped to *this cell* rather than the branch,
    /// which is why main never breaks because someone merged a bad pipeline (SPEC.md §14).
    Broken,
    /// Explicitly removed (SPEC.md §8.1), or reached through a dangling reference (§8.2).
    Tombstoned,
}

/// What a client asks for. SPEC.md §10.5.
///
/// `Current` forces inline computation and blocks — which makes lazy materialization a *per-read
/// client mode* rather than a system architecture.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
pub enum FreshnessRequirement {
    /// Take whatever is stored, however stale.
    #[default]
    Any,
    /// Check the dependency index before answering; no user code runs.
    Validated,
    /// Compute inline if necessary and block until correct — including whatever this cell's inputs
    /// need first, and including a migration hop that has not run.
    ///
    /// It brings *this cell* up to date and deliberately leaves the producer's watermark where it
    /// was: a watermark speaks for all of a producer's output, and one entity computed on demand
    /// says nothing about the rest.
    Current,
}

/// A resolved cell, with provenance. SPEC.md §10.4.
///
/// Named `Resolved` rather than the spec's `Cell<T>` to avoid confusion with `CellRef` and with
/// `std::cell`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Resolved<T> {
    pub value: T,
    pub origin: crate::cell::Origin,
    /// The event this value came from, or `None` where nothing is stored at the reader's version.
    ///
    /// Reported because it is the answer to "is this the same write I saw on the other branch?",
    /// which is a question the model can now answer: a merged event is *one* event named by two
    /// layers, not a copy on each side (SPEC.md §13).
    pub event: Option<crate::ids::EventId>,
    /// Where this value was first committed — on whichever branch authored it.
    pub authored_at: LayerId,
    /// Where it arrived on the branch this read resolved through. Equal to `authored_at` until a
    /// merge carries the event onto another branch, and the two together are the lineage the old
    /// single `written_at` collapsed (SPEC.md §4.3, §13).
    pub landed_at: LayerId,
    /// Certain-correct through here. For source cells this collapses into `landed_at` — source data
    /// is written once and correct thereafter, so the distinction only carries information for
    /// derived data.
    pub fresh_as_of: LayerId,
    pub state: Freshness,
    pub by: Option<ProducerId>,
    // Deferred: expected_fresh_at — an ETA for when catch-up will complete (SPEC.md §10.4).
}

/// How far a producer has caught up. SPEC.md §10.3.
///
/// Watermarks compose through chained producers: `W(B) = min(target, W(A), W(other deps))`. So any
/// derived cell can report an honest *transitive* freshness — the minimum over its entire derivation
/// chain, migrations included.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Watermark {
    pub producer: ProducerId,
    pub reflects: LayerId,
}
