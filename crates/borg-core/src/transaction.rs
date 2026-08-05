//! Transactions — the client's, and the derivation round's. SPEC.md §12, §13, §16.5.
//!
//! A client never writes to a shared branch. It forks, writes in isolation, and merges — and because
//! the fork's read path is bounded at the fork point, everything it reads is one consistent snapshot.
//! Guards re-evaluated against the parent since that fork point were already the merge-conflict
//! detector (§13), so snapshot isolation with optimistic concurrency comes out of mechanisms that
//! already existed rather than out of new ones.
//!
//! A **round** is the same shape with one difference, and [`Round`] below is where that difference
//! is written down: a client transaction is one intent and lands whole or not at all, while a round
//! is `N` independent computations and lands the ones whose guards held.
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
use std::collections::{BTreeMap, BTreeSet};

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

/// A derivation round, as a transaction. SPEC.md §16.5.
///
/// A round forks the branch at the source layer it settles, runs every producer on that fork, and
/// merges when it settles. That fork point *is* what the round can see, so `reflects` is true by
/// construction rather than maintained — which is the whole of what replaced the round ceiling.
///
/// Each invocation is a transaction against the same snapshot, and the round is the collection
/// of them keyed by the layer each one committed. Keying on the layer is what makes partial application
/// expressible: a merge decides per layer, and one layer is one invocation.
///
/// **The invocations are held as the read-sets and write-sets they already were**, rather than as
/// [`Transaction`]s. A producer's accesses come out of `ProducerCtx` as vectors the dependency index
/// already needs (§9.4), so this takes them by value and copies nothing — which matters, because
/// this is the hot path of a 128k-invocation round and a `Transaction`'s two `BTreeSet`s were 12% of
/// it, spent computing a guard set a round cannot use anyway: a `Transaction` records its writes as
/// cells and drops a read of a cell it has written, which for a migration — one cell, two versions —
/// drops exactly the guard that matters. See [`guards`](Self::guards).
///
/// Two rules are the round's own, and both exist because a round is `N` independent computations
/// rather than one intent. They are [`guards`](Self::guards) and [`cascade`](Self::cascade).
#[derive(Clone, Debug)]
pub struct Round {
    /// The branch the producers write to, forked at [`Round::fork_point`].
    pub branch: BranchId,
    /// The branch this round merges back into — the one whose data it is settling.
    pub parent: BranchId,
    /// The source layer being settled. Also the `since` of every guard the round carries, for the
    /// reason [`Transaction::guards`] gives.
    pub fork_point: LayerId,
    invocations: BTreeMap<LayerId, Accesses>,
}

/// What one invocation touched: the same two sets the dependency index is fed.
///
/// `CellAt` because that is the record key the engine already has in hand (§16.3), and the
/// distinction is load-bearing rather than incidental — see [`Round::guards`], where the subtraction
/// is over records and the guard that comes out of it is over cells.
#[derive(Clone, Debug, Default)]
struct Accesses {
    reads: Vec<CellAt>,
    writes: Vec<CellAt>,
}

impl Accesses {
    /// The records this invocation read.
    fn observed(&self) -> impl Iterator<Item = &CellAt> {
        self.reads.iter()
    }

    /// The records it wrote.
    fn written(&self) -> impl Iterator<Item = &CellAt> {
        self.writes.iter()
    }
}

impl Round {
    pub const fn new(branch: BranchId, parent: BranchId, fork_point: LayerId) -> Self {
        Self {
            branch,
            parent,
            fork_point,
            invocations: BTreeMap::new(),
        }
    }

    /// Record what one invocation read and wrote, and which layer it committed.
    pub fn ran(&mut self, layer: LayerId, reads: Vec<CellAt>, writes: Vec<CellAt>) {
        self.invocations.insert(layer, Accesses { reads, writes });
    }

    /// The layers this round committed, oldest first.
    pub fn layers(&self) -> impl Iterator<Item = LayerId> {
        self.invocations.keys().copied()
    }

    pub fn is_empty(&self) -> bool {
        self.invocations.is_empty()
    }

    /// What each of this round's layers is contingent on: the records it read and **the round** did
    /// not produce, named as the cells a guard is asked about.
    ///
    /// This is [`Transaction::guards`] asked of a round, and it differs in exactly one way: the
    /// subtraction is round-wide. That is the S7 rule — within one round `invest` writes
    /// `is_investible` and `tier` reads it, and `tier` must not fail on a cell its own round
    /// produced.
    ///
    /// **It is also a set difference rather than [`Transaction::observe`]'s ordering, and that is
    /// safe here where it would not be for a client** (§12.1). Three things make it so, and all
    /// three have to hold:
    ///
    /// * A round has no order to appeal to. Its invocations are independent by construction
    ///   (§16.3.1) and run concurrently, so a rule that asked whether `tier`'s read came before
    ///   `invest`'s write would give a different guard set on every interleaving.
    /// * The difference deletes nothing that could have fired. Everything a round writes is derived,
    ///   and a derived cell is never in the cell-touch index (§12.4).
    /// * The read-modify-write the ordering rule exists to protect cannot arise. A producer that
    ///   reads a cell it writes is a cycle (§16.6), not a compare-and-swap.
    ///
    /// **The subtraction is over `CellAt` and the result is over `CellRef`, and mixing the two is a
    /// lost update.** The second bullet is true of every producer *except a migration*, which reads
    /// `C@v1` and writes `C@v9` — that is what a migration is (§9.3). Subtract by cell and the
    /// migration's guard on the record it migrated *from* is deleted; that record is source data a
    /// client owns, so it is in the touch index, and the deleted guard was the only thing stopping a
    /// stale migration round from landing over a fresher one. Subtract by record and `up` guards `C`
    /// (it read `C@v1` and produced only `C@v9`) while `tier` still does not guard `is_investible`
    /// (read and produced the same record). The guard stays a question about the cell, because a
    /// write at any version is a write and the touch index keys on `CellRef`.
    ///
    /// Borrowed rather than cloned, because a round's guard set is the sum of its producers'
    /// read-sets and is unbounded (§7.7).
    pub fn guards(&self) -> Vec<(LayerId, Vec<&CellRef>)> {
        let produced: BTreeSet<&CellAt> = self
            .invocations
            .values()
            .flat_map(Accesses::written)
            .collect();
        self.invocations
            .iter()
            .map(|(layer, invocation)| {
                let guards = invocation
                    .observed()
                    .filter(|at| !produced.contains(at))
                    .map(|at| &at.cell)
                    .collect();
                (*layer, guards)
            })
            .collect()
    }

    /// Everything that must be dropped once these layers are, transitively.
    ///
    /// A round applies partially, and the invocations it applies must still be a **consistent**
    /// subset: an invocation that consumed a sibling's output cannot land when that sibling does
    /// not, or the round would publish a value derived from one that never existed — and it would
    /// be published labelled with a watermark claiming exactly the replay that would not reproduce
    /// it (§10.1).
    ///
    /// Dropping it is safe for the same reason dropping the original is: its cells stay dirty in
    /// the dependency index, so the round that settles the layer which failed the guard rediscovers
    /// the whole chain, and until then the value reads stale.
    ///
    /// The closure is computed over the round's *own* reads and writes only. A read of something no
    /// invocation in this round wrote is a read of the snapshot, and the snapshot is not going
    /// anywhere.
    pub fn cascade(&self, failed: &BTreeSet<LayerId>) -> BTreeSet<LayerId> {
        if failed.is_empty() {
            return BTreeSet::new();
        }
        // cell -> the layers that read it. Built only when something has already failed, so the
        // common case — a round nothing contended with — pays nothing for it.
        //
        // Keyed on the **cell** and not on the record, unlike [`guards`](Self::guards). The two want
        // opposite errors: a guard that is too broad rejects work that was fine, while a cascade that
        // is too narrow publishes a value derived from one that never landed (§10.1). So this
        // over-approximates on purpose — a migration's `C@v9` sweeping up a sibling that read `C@v1`
        // costs a re-run and cannot cost a lie.
        let mut readers: BTreeMap<&CellRef, Vec<LayerId>> = BTreeMap::new();
        for (layer, invocation) in &self.invocations {
            for read in invocation.observed() {
                readers.entry(&read.cell).or_default().push(*layer);
            }
        }

        let mut dropped = failed.clone();
        let mut frontier: Vec<LayerId> = failed.iter().copied().collect();
        while let Some(layer) = frontier.pop() {
            let Some(invocation) = self.invocations.get(&layer) else {
                continue;
            };
            for write in invocation.written() {
                for consumer in readers.get(&write.cell).into_iter().flatten() {
                    if dropped.insert(*consumer) {
                        frontier.push(*consumer);
                    }
                }
            }
        }
        dropped
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

    const L1: LayerId = LayerId(11);
    const L2: LayerId = LayerId(12);
    const L3: LayerId = LayerId(13);

    fn round() -> Round {
        Round::new(BranchId(3), BranchId(1), FORK)
    }

    fn at(field: &str) -> CellAt {
        CellAt::new(prop(field), V1)
    }

    /// S7. `invest` writes `is_investible`; `tier` reads it. Neither may fail the round.
    #[test]
    fn a_chained_producer_does_not_guard_on_its_own_rounds_output() {
        let mut round = round();
        round.ran(L1, vec![at("headcount")], vec![at("is_investible")]);
        round.ran(L2, vec![at("is_investible")], vec![at("tier")]);

        assert_eq!(
            round.guards(),
            vec![
                (L1, vec![&prop("headcount")]),
                // …and *not* `is_investible`, which this round produced.
                (L2, vec![]),
            ]
        );
    }

    /// The source cell a producer read is still a guard — that is what rejects a stale round (S8).
    #[test]
    fn a_round_guards_the_source_cells_it_read() {
        let mut round = round();
        round.ran(L1, vec![at("headcount")], vec![at("tier")]);
        assert_eq!(round.guards(), vec![(L1, vec![&prop("headcount")])]);
    }

    /// A round that lost one invocation must also lose whatever consumed its output — otherwise it
    /// publishes a value derived from one that never landed.
    #[test]
    fn dropping_an_invocation_drops_what_consumed_it() {
        let mut round = round();
        round.ran(L1, vec![at("headcount")], vec![at("is_investible")]);
        round.ran(L2, vec![at("is_investible")], vec![at("tier")]);
        // An unrelated invocation, to show the cascade is a closure and not a blanket rejection.
        round.ran(L3, vec![at("name")], vec![at("slug")]);

        assert_eq!(
            round.cascade(&BTreeSet::from([L1])),
            BTreeSet::from([L1, L2])
        );
    }

    #[test]
    fn a_round_with_nothing_rejected_cascades_to_nothing() {
        let mut round = round();
        round.ran(L1, vec![at("headcount")], vec![at("tier")]);
        assert!(round.cascade(&BTreeSet::new()).is_empty());
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
