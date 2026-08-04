//! Client transactions. SPEC.md §12, §13.
//!
//! A client never writes to a shared branch. It forks, writes in isolation, and merges — and because
//! the fork's read path is bounded at the fork point, everything it reads is one consistent snapshot.
//! Guards re-evaluated against the parent since that fork point were already the merge-conflict
//! detector (§13), so snapshot isolation with optimistic concurrency comes out of mechanisms that
//! already existed rather than out of new ones.
//!
//! What is new is that the guards are **automatic**. A transaction records what it read; at commit
//! those reads *are* its guards. Guards were opt-in before, which meant §13's last-write-wins was
//! what people actually got; this inverts the default without inverting the machinery.
//!
//! This type is the bookkeeping, and deliberately nothing else. It holds no store, opens no layer
//! and reads nothing: it is a value a CLI can write to a file between processes and an SDK can hold
//! in memory, so that the rule below is stated once and obeyed by both.

use crate::cell::{CellAt, CellRef};
use crate::ids::{BranchId, LayerId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// One client transaction: where it forked, what it read there, and what it wrote.
///
/// Reads are recorded as [`CellAt`] — cell *and* the def-version it was read at — which is what
/// producers already record (§9.4), so one read-set shape and one guard mechanism serve both.
/// Writes are recorded as [`CellRef`], because a guard is a question about a cell rather than about
/// one version of it: the touch index keys on `CellRef` and a write at any version is a write.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Transaction {
    /// The branch this transaction writes into, forked at [`Transaction::fork_point`].
    pub branch: BranchId,
    /// The branch it merges back into. Not inferred from the fork point, because a transaction
    /// opened on a branch that has no layers of its own forks the nearest ancestor that does — and
    /// then the layer's branch is the grandparent, not the branch the client named.
    pub parent: BranchId,
    /// Where the snapshot was taken. Also the `since` of **every** guard this transaction carries;
    /// see [`Transaction::guards`].
    pub fork_point: LayerId,
    reads: BTreeSet<CellAt>,
    writes: BTreeSet<CellRef>,
}

impl Transaction {
    pub const fn new(branch: BranchId, parent: BranchId, fork_point: LayerId) -> Self {
        Self {
            branch,
            parent,
            fork_point,
            reads: BTreeSet::new(),
            writes: BTreeSet::new(),
        }
    }

    /// Record a read made **through** this transaction — including one that found nothing.
    ///
    /// Absence is a legitimate thing to have acted on, and a later write to that cell must invalidate
    /// the decision; it is the same rule producers already follow (§9.4). Without it two transactions
    /// can each conclude an object does not exist and both create it.
    ///
    /// **A read of a cell this transaction has already written is not recorded.** That read returned
    /// the transaction's own write rather than the parent's state, so it expresses no dependency on
    /// the parent and guarding it would be guarding a fact about nobody.
    ///
    /// Note the order-sensitivity, which is the whole content of the rule: a read *before* a write to
    /// the same cell is kept. That read did observe the parent, and keeping it is exactly what makes
    /// compare-and-swap work — read the cell, write it, and the guard falls out. Collapsing this to a
    /// set difference of reads minus writes reads the same on the page and quietly deletes
    /// read-modify-write.
    pub fn observe(&mut self, read: CellAt) {
        if !self.writes.contains(&read.cell) {
            self.reads.insert(read);
        }
    }

    /// Record a write made through this transaction.
    pub fn wrote(&mut self, cell: CellRef) {
        self.writes.insert(cell);
    }

    /// The cells this transaction's commit is contingent on: everything it read and had not already
    /// written, one guard cell per distinct cell however many versions of it were read.
    ///
    /// **`since` is the fork point for all of them, and per-read tracking would be wrong** rather
    /// than merely unnecessary. A transaction's read path is bounded at the fork point (§7.2), so
    /// every read it makes observes the parent as the parent stood *then*, whenever during the
    /// transaction's life the read happens. Recording the moment of each read and using that as its
    /// `since` would ignore every parent write between the fork and the read — writes the transaction
    /// provably did not see, because they were above its bound — which is the exact set of writes a
    /// guard exists to catch. One `since`, and it is the snapshot the reads came from.
    pub fn guards(&self) -> Vec<CellRef> {
        let cells: BTreeSet<CellRef> = self.reads.iter().map(|at| at.cell.clone()).collect();
        cells.into_iter().collect()
    }

    /// How many reads and writes this transaction is carrying. For `borg tx list` and for the
    /// operator asking why a commit is expensive (§7.7).
    pub fn size(&self) -> (usize, usize) {
        (self.reads.len(), self.writes.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cell::{BufferId, CellKey};
    use crate::ids::{AllocatorId, DefVersion};
    use crate::pid::{Pid, PidKind};

    const FORK: LayerId = LayerId(4);
    const V1: DefVersion = DefVersion(LayerId(1));
    const V2: DefVersion = DefVersion(LayerId(9));

    fn acme() -> Pid {
        Pid::Allocated {
            kind: PidKind::Object,
            branch: BranchId(1),
            allocator: AllocatorId(0),
            counter: 1,
        }
    }

    fn prop(field: &str) -> CellRef {
        CellRef::prop("Company".into(), field.into(), acme())
    }

    fn existence() -> CellRef {
        CellRef {
            buffer: BufferId::Object("Company".into()),
            key: CellKey::Pid(acme()),
        }
    }

    fn transaction() -> Transaction {
        Transaction::new(BranchId(2), BranchId(1), FORK)
    }

    #[test]
    fn a_read_becomes_a_guard() {
        let mut tx = transaction();
        tx.observe(CellAt::new(prop("balance"), V1));
        assert_eq!(tx.guards(), vec![prop("balance")]);
    }

    #[test]
    fn a_read_that_found_nothing_is_a_guard_like_any_other() {
        // Absence is a legitimate thing to have acted on, and the transaction cannot tell the
        // difference: it records the cell it asked about, not the answer it got.
        let mut tx = transaction();
        tx.observe(CellAt::new(existence(), DefVersion::UNVERSIONED));
        assert_eq!(tx.guards(), vec![existence()]);
    }

    #[test]
    fn a_read_of_what_the_transaction_already_wrote_is_not_a_guard() {
        // Write X, then read X: that read returned the transaction's own write, so it says nothing
        // about the parent and guarding it would be guarding a fact about nobody.
        let mut tx = transaction();
        tx.wrote(prop("balance"));
        tx.observe(CellAt::new(prop("balance"), V1));
        assert!(tx.guards().is_empty());
    }

    #[test]
    fn a_read_before_a_write_to_the_same_cell_is_still_a_guard() {
        // The half a set difference of reads-minus-writes would silently delete, and with it every
        // compare-and-swap: this read *did* observe the parent, and the write that followed is
        // exactly what it was for.
        let mut tx = transaction();
        tx.observe(CellAt::new(prop("balance"), V1));
        tx.wrote(prop("balance"));
        assert_eq!(
            tx.guards(),
            vec![prop("balance")],
            "read-modify-write must guard the cell it read"
        );
    }

    #[test]
    fn one_cell_read_at_two_versions_is_one_guard() {
        // Reads key on `CellAt` because that is the record key; a guard keys on `CellRef` because
        // the touch index does, and a write at any version is a write (§12).
        let mut tx = transaction();
        tx.observe(CellAt::new(prop("balance"), V1));
        tx.observe(CellAt::new(prop("balance"), V2));
        assert_eq!(tx.guards(), vec![prop("balance")]);
    }

    #[test]
    fn a_transaction_that_only_wrote_carries_no_guards() {
        // A blind write is last-write-wins, honestly: the client expressed no dependency on prior
        // state, and that is what every database does with one.
        let mut tx = transaction();
        tx.wrote(prop("balance"));
        tx.wrote(prop("name"));
        assert!(tx.guards().is_empty());
        assert_eq!(tx.size(), (0, 2));
    }
}
