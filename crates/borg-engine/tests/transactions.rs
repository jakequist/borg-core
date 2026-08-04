//! Object transactions and guards. SPEC.md §12, §13.
//!
//! Written before the implementation. A guard asserts that nothing has touched the named cells since
//! a given layer — and the same mechanism, re-evaluated against a parent since the fork point, *is*
//! the merge-conflict detector.

use borg_core::{
    BranchId, CellAt, CellRef, ClientVersion, DefEvent, DefVersion, Guard, LayerAuthor, LayerId,
    MergeMode, Ownership, Pid, PidKind, ProducerId, RepoId, Result, Transaction, Value, ValueType,
    Writer,
};
use borg_engine::{
    BranchManager, CellTouchIndex, DefRegistry, InProcessSequencer, LayerManager, WriteSession,
};
use borg_storage::{MemoryStorage, StorageProvider};
use std::sync::Arc;

const V1: ClientVersion = ClientVersion(LayerId(1));
/// The def-version every field in these tests sits at. One declaration, one def-layer, nothing
/// mutated since — so this is where the records are keyed, whatever any actor's whole-schema view
/// has moved on to (SPEC.md §5.3).
const AT_V1: DefVersion = DefVersion(LayerId(1));
const SCORE: ProducerId = ProducerId(1);

fn company(branch: BranchId, n: u64) -> Pid {
    Pid::Allocated {
        kind: PidKind::Object,
        branch,
        allocator: borg_core::AllocatorId(0),
        counter: n,
    }
}

fn prop(pid: Pid, field: &str) -> CellRef {
    CellRef::prop("Company".into(), field.into(), pid)
}

fn existence(pid: Pid) -> CellRef {
    CellRef::existence("Company".into(), pid)
}

struct Harness {
    storage: Arc<MemoryStorage>,
    layers: Arc<LayerManager>,
    branches: Arc<BranchManager>,
    defs: Arc<DefRegistry>,
}

impl Harness {
    fn new() -> Self {
        let storage = Arc::new(MemoryStorage::new());
        let layers = Arc::new(LayerManager::new(
            storage.clone(),
            Arc::new(InProcessSequencer::new()),
            Arc::new(CellTouchIndex::new()),
        ));
        let branches = Arc::new(BranchManager::new(layers.clone()));
        let defs = Arc::new(DefRegistry::new(layers.clone(), storage.clone()));
        Self {
            storage,
            layers,
            branches,
            defs,
        }
    }

    /// A root branch with a schema on it. `is_rich` is declared derived so that a guard can be
    /// tried against a genuinely derived cell (SPEC.md §12).
    async fn root(&self) -> Result<BranchId> {
        let branch = self.branches.create_root(None).await?;
        let declare = |field: &str, ty: ValueType, ownership: Ownership| DefEvent::DeclareField {
            struct_name: "Company".into(),
            field: field.into(),
            ty,
            repo: RepoId(1),
            ownership,
        };
        self.defs
            .push(
                branch,
                vec![
                    declare("balance", ValueType::Int, Ownership::Source),
                    declare("name", ValueType::Int, Ownership::Source),
                    declare("is_rich", ValueType::Bool, Ownership::Derived(SCORE)),
                ],
            )
            .await?;
        Ok(branch)
    }

    async fn push(&self, branch: BranchId, writes: Vec<(CellRef, Value)>) -> Result<LayerId> {
        self.push_guarded(branch, writes, vec![]).await
    }

    /// A transaction: a mutation plus the guards it is conditional on.
    async fn push_guarded(
        &self,
        branch: BranchId,
        writes: Vec<(CellRef, Value)>,
        guards: Vec<Guard>,
    ) -> Result<LayerId> {
        let mut session = WriteSession::open(
            &self.layers,
            &self.defs,
            branch,
            V1,
            Writer::Client,
            LayerAuthor::Source,
        )
        .await?;
        for guard in guards {
            session.guard(guard);
        }
        for (cell, value) in writes {
            session.set(&cell, value).await?;
        }
        session.commit().await
    }

    /// Commit a derived cell, so guards can be tried against one.
    async fn push_derived(
        &self,
        branch: BranchId,
        cell: &CellRef,
        value: Value,
    ) -> Result<LayerId> {
        let mut session = WriteSession::open(
            &self.layers,
            &self.defs,
            branch,
            V1,
            Writer::Producer(SCORE),
            LayerAuthor::Derived {
                producer: SCORE,
                reflects: LayerId(1),
            },
        )
        .await?;
        session.set(cell, value).await?;
        session.commit().await
    }

    async fn read(&self, branch: BranchId, cell: &CellRef) -> Result<Option<Value>> {
        let path = self.branches.read_path(branch, None)?;
        Ok(self
            .storage
            .get_cell(&path, cell, AT_V1)
            .await?
            .map(|found| found.event.value))
    }
}

#[tokio::test]
async fn a_transaction_commits_when_its_guard_holds() -> Result<()> {
    let h = Harness::new();
    let main = h.root().await?;
    let acme = company(main, 1);

    let base = h
        .push(main, vec![(prop(acme, "balance"), Value::Int(100))])
        .await?;

    h.push_guarded(
        main,
        vec![(prop(acme, "balance"), Value::Int(90))],
        vec![Guard {
            cells: vec![prop(acme, "balance")],
            since: base,
        }],
    )
    .await?;

    assert_eq!(
        h.read(main, &prop(acme, "balance")).await?,
        Some(Value::Int(90)),
        "nothing touched the guarded cell in between, so the mutation lands"
    );
    Ok(())
}

#[tokio::test]
async fn a_transaction_is_rejected_when_a_guarded_cell_moved() -> Result<()> {
    let h = Harness::new();
    let main = h.root().await?;
    let acme = company(main, 1);

    let base = h
        .push(main, vec![(prop(acme, "balance"), Value::Int(100))])
        .await?;
    // Someone else got there first.
    h.push(main, vec![(prop(acme, "balance"), Value::Int(50))])
        .await?;

    let outcome = h
        .push_guarded(
            main,
            vec![(prop(acme, "balance"), Value::Int(90))],
            vec![Guard {
                cells: vec![prop(acme, "balance")],
                since: base,
            }],
        )
        .await;
    assert!(outcome.is_err(), "the guard no longer holds");

    assert_eq!(
        h.read(main, &prop(acme, "balance")).await?,
        Some(Value::Int(50)),
        "and the rejected transaction leaves no trace — the layer never became visible"
    );
    Ok(())
}

#[tokio::test]
async fn a_guard_ignores_cells_it_does_not_name() -> Result<()> {
    let h = Harness::new();
    let main = h.root().await?;
    let acme = company(main, 1);

    let base = h
        .push(main, vec![(prop(acme, "balance"), Value::Int(100))])
        .await?;
    // A concurrent write to an entirely different field must not trip the guard. Cell granularity is
    // what makes that true (SPEC.md §13).
    h.push(main, vec![(prop(acme, "name"), Value::Int(7))])
        .await?;

    h.push_guarded(
        main,
        vec![(prop(acme, "balance"), Value::Int(90))],
        vec![Guard {
            cells: vec![prop(acme, "balance")],
            since: base,
        }],
    )
    .await?;

    assert_eq!(
        h.read(main, &prop(acme, "balance")).await?,
        Some(Value::Int(90))
    );
    Ok(())
}

#[tokio::test]
async fn a_guard_on_a_derived_cell_is_rejected() -> Result<()> {
    let h = Harness::new();
    let main = h.root().await?;
    let acme = company(main, 1);

    let base = h
        .push(main, vec![(prop(acme, "balance"), Value::Int(100))])
        .await?;
    h.push_derived(main, &prop(acme, "is_rich"), Value::Bool(true))
        .await?;

    let outcome = h
        .push_guarded(
            main,
            vec![(prop(acme, "balance"), Value::Int(90))],
            vec![Guard {
                cells: vec![prop(acme, "is_rich")],
                since: base,
            }],
        )
        .await;

    // Guarding derived data is meaningless: its value is a function of source data with a lag, so
    // the guard would be checking a shadow (SPEC.md §12).
    assert!(
        outcome.is_err(),
        "guards may reference source cells only, and saying so is better than silently checking a \
         value that trails its inputs"
    );
    Ok(())
}

#[tokio::test]
async fn a_childs_guard_re_evaluated_against_the_parent_detects_a_merge_conflict() -> Result<()> {
    let h = Harness::new();
    let main = h.root().await?;
    let acme = company(main, 1);

    let fork_point = h
        .push(main, vec![(prop(acme, "balance"), Value::Int(100))])
        .await?;
    let feature = h.branches.fork(main, fork_point, None).await?;

    // On the child, the guard holds: nothing on the child touched `balance` since the fork.
    h.push_guarded(
        feature,
        vec![(prop(acme, "balance"), Value::Int(90))],
        vec![Guard {
            cells: vec![prop(acme, "balance")],
            since: fork_point,
        }],
    )
    .await?;

    // Meanwhile the parent moved the very cell the child guarded.
    h.push(main, vec![(prop(acme, "balance"), Value::Int(50))])
        .await?;

    let outcome = h.branches.merge(feature, MergeMode::DefAndData).await;
    assert!(
        outcome.is_err(),
        "re-evaluating the child's guard against the parent since the fork point *is* the \
         merge-conflict detector"
    );
    assert_eq!(
        h.read(main, &prop(acme, "balance")).await?,
        Some(Value::Int(50)),
        "and the parent is untouched by the rejected merge"
    );
    Ok(())
}

#[tokio::test]
async fn an_unguarded_merge_is_last_write_wins() -> Result<()> {
    let h = Harness::new();
    let main = h.root().await?;
    let acme = company(main, 1);

    let fork_point = h
        .push(main, vec![(prop(acme, "balance"), Value::Int(100))])
        .await?;
    let feature = h.branches.fork(main, fork_point, None).await?;

    h.push(feature, vec![(prop(acme, "balance"), Value::Int(90))])
        .await?;
    h.push(main, vec![(prop(acme, "balance"), Value::Int(50))])
        .await?;

    h.branches.merge(feature, MergeMode::DefAndData).await?;

    assert_eq!(
        h.read(main, &prop(acme, "balance")).await?,
        Some(Value::Int(90)),
        "without a guard the child simply wins, because replay puts it later — guards are the \
         opt-in to safety, not the default"
    );
    Ok(())
}

#[tokio::test]
async fn a_guard_the_parent_did_not_disturb_still_merges() -> Result<()> {
    let h = Harness::new();
    let main = h.root().await?;
    let acme = company(main, 1);

    let fork_point = h
        .push(main, vec![(prop(acme, "balance"), Value::Int(100))])
        .await?;
    let feature = h.branches.fork(main, fork_point, None).await?;

    h.push_guarded(
        feature,
        vec![(prop(acme, "balance"), Value::Int(90))],
        vec![Guard {
            cells: vec![prop(acme, "balance")],
            since: fork_point,
        }],
    )
    .await?;

    // The parent moved on, but not in a way the guard named.
    h.push(main, vec![(prop(acme, "name"), Value::Int(7))])
        .await?;

    h.branches.merge(feature, MergeMode::DefAndData).await?;
    assert_eq!(
        h.read(main, &prop(acme, "balance")).await?,
        Some(Value::Int(90)),
        "the guard is field-granular on the parent too"
    );
    Ok(())
}

// --- Automatic guards: a transaction's read-set is what its commit is contingent on. §12 ------------

/// Open a transaction on a branch: fork at its head, and remember where.
async fn begin(h: &Harness, parent: BranchId) -> Result<Transaction> {
    let fork_point = h
        .layers
        .head(parent)
        .expect("a transaction forks a branch that has something on it");
    let branch = h.branches.fork(parent, fork_point, None).await?;
    Ok(Transaction::new(branch, parent, fork_point))
}

/// Write through a transaction, folding what the session read and wrote back into it.
///
/// This is what the CLI does, and the ordering is the same for the same reason: every probe a
/// session makes precedes any write it makes to the same cell, so reads first is what gets the
/// read-before-write rule right.
async fn tx_set(
    h: &Harness,
    transaction: &mut Transaction,
    cell: &CellRef,
    value: Value,
) -> Result<LayerId> {
    let mut session = WriteSession::open(
        &h.layers,
        &h.defs,
        transaction.branch,
        V1,
        Writer::Client,
        LayerAuthor::Source,
    )
    .await?;
    session.set(cell, value).await?;
    for read in session.observed() {
        transaction.observe(read.clone());
    }
    for write in session.authored() {
        transaction.wrote(write.clone());
    }
    session.commit().await
}

/// Read through a transaction, recording the read.
async fn tx_get(
    h: &Harness,
    transaction: &mut Transaction,
    cell: &CellRef,
) -> Result<Option<Value>> {
    let value = h.read(transaction.branch, cell).await?;
    transaction.observe(CellAt::new(cell.clone(), AT_V1));
    Ok(value)
}

#[tokio::test]
async fn what_a_transaction_read_is_what_its_commit_is_contingent_on() -> Result<()> {
    let h = Harness::new();
    let main = h.root().await?;
    let acme = company(main, 1);
    h.push(main, vec![(prop(acme, "balance"), Value::Int(100))])
        .await?;

    let mut transaction = begin(&h, main).await?;
    tx_get(&h, &mut transaction, &prop(acme, "balance")).await?;
    tx_set(&h, &mut transaction, &prop(acme, "name"), Value::Int(7)).await?;

    // Somebody else moves the cell it read. Nobody wrote a guard; the read is the guard.
    h.push(main, vec![(prop(acme, "balance"), Value::Int(50))])
        .await?;

    let outcome = h
        .branches
        .merge_transaction(&transaction, MergeMode::DefAndData)
        .await;
    assert!(
        matches!(
            outcome,
            Err(borg_core::BorgError::MergeRejected(
                borg_core::MergeRejection::GuardConflict { .. }
            ))
        ),
        "a read that moved rejects the commit, with nobody having asked for a guard: {outcome:?}"
    );
    assert_eq!(
        h.read(main, &prop(acme, "name")).await?,
        None,
        "and a rejected transaction leaves no trace on the parent"
    );
    Ok(())
}

#[tokio::test]
async fn a_transaction_does_not_conflict_with_itself() -> Result<()> {
    // Write X, read X, commit. The read saw the transaction's own write, so it expresses no
    // dependency on the parent — and if it did, every read-modify-write would deadlock on itself.
    let h = Harness::new();
    let main = h.root().await?;
    let acme = company(main, 1);
    h.push(main, vec![(prop(acme, "balance"), Value::Int(100))])
        .await?;

    let mut transaction = begin(&h, main).await?;
    tx_set(&h, &mut transaction, &prop(acme, "balance"), Value::Int(90)).await?;
    assert_eq!(
        tx_get(&h, &mut transaction, &prop(acme, "balance")).await?,
        Some(Value::Int(90)),
        "the transaction reads back its own write"
    );

    h.branches
        .merge_transaction(&transaction, MergeMode::DefAndData)
        .await?;
    assert_eq!(
        h.read(main, &prop(acme, "balance")).await?,
        Some(Value::Int(90))
    );
    Ok(())
}

#[tokio::test]
async fn a_read_before_a_write_to_the_same_cell_is_a_compare_and_swap() -> Result<()> {
    // The other half of the rule above, and the one that makes it worth stating in terms of order
    // rather than as a set difference: this read *did* observe the parent.
    let h = Harness::new();
    let main = h.root().await?;
    let acme = company(main, 1);
    h.push(main, vec![(prop(acme, "balance"), Value::Int(100))])
        .await?;

    let mut transaction = begin(&h, main).await?;
    let seen = tx_get(&h, &mut transaction, &prop(acme, "balance")).await?;
    assert_eq!(seen, Some(Value::Int(100)));
    tx_set(
        &h,
        &mut transaction,
        &prop(acme, "balance"),
        Value::Int(101),
    )
    .await?;

    h.push(main, vec![(prop(acme, "balance"), Value::Int(200))])
        .await?;

    assert!(
        h.branches
            .merge_transaction(&transaction, MergeMode::DefAndData)
            .await
            .is_err(),
        "reading a cell before writing it is what makes the write a compare-and-swap"
    );
    assert_eq!(
        h.read(main, &prop(acme, "balance")).await?,
        Some(Value::Int(200)),
        "so the increment is lost rather than silently applied to a value it never saw"
    );
    Ok(())
}

#[tokio::test]
async fn two_transactions_cannot_both_create_the_same_object() -> Result<()> {
    // Neither ever reads anything explicitly. Writing a property implies the object exists (§8), and
    // the probe that decides whether to write the existence cell is a read — of a cell that is
    // absent. Without it in the read-set both creates land.
    let h = Harness::new();
    let main = h.root().await?;
    let acme = company(main, 9);
    h.push(
        main,
        vec![(prop(company(main, 1), "balance"), Value::Int(1))],
    )
    .await?;

    let mut first = begin(&h, main).await?;
    let mut second = begin(&h, main).await?;
    tx_set(&h, &mut first, &prop(acme, "name"), Value::Int(1)).await?;
    tx_set(&h, &mut second, &prop(acme, "name"), Value::Int(2)).await?;

    h.branches
        .merge_transaction(&first, MergeMode::DefAndData)
        .await?;
    assert!(
        h.branches
            .merge_transaction(&second, MergeMode::DefAndData)
            .await
            .is_err(),
        "the second create must lose: the absence it acted on is no longer true"
    );
    assert_eq!(
        h.read(main, &prop(acme, "name")).await?,
        Some(Value::Int(1))
    );
    Ok(())
}

#[tokio::test]
async fn two_transactions_writing_different_fields_of_one_object_both_land() -> Result<()> {
    // Both probe the same existence cell and neither writes it, so neither disturbs what the other
    // read. If either guarded the *object*, the second would lose and cell granularity would be
    // fiction.
    let h = Harness::new();
    let main = h.root().await?;
    let acme = company(main, 1);
    h.push(main, vec![(existence(acme), Value::Bool(true))])
        .await?;

    let mut first = begin(&h, main).await?;
    let mut second = begin(&h, main).await?;
    tx_set(&h, &mut first, &prop(acme, "balance"), Value::Int(1)).await?;
    tx_set(&h, &mut second, &prop(acme, "name"), Value::Int(2)).await?;

    h.branches
        .merge_transaction(&first, MergeMode::DefAndData)
        .await?;
    h.branches
        .merge_transaction(&second, MergeMode::DefAndData)
        .await?;

    assert_eq!(
        h.read(main, &prop(acme, "balance")).await?,
        Some(Value::Int(1))
    );
    assert_eq!(
        h.read(main, &prop(acme, "name")).await?,
        Some(Value::Int(2))
    );
    Ok(())
}

#[tokio::test]
async fn every_guard_is_checked_before_any_layer_is_applied() -> Result<()> {
    // A transaction that wrote two layers must not trip its own guard on the way in. Checking as
    // each layer landed would let the first layer of a merge violate a guard belonging to the
    // second, and the transaction would conflict with itself for no reason but the walk order.
    let h = Harness::new();
    let main = h.root().await?;
    let acme = company(main, 1);
    h.push(main, vec![(prop(acme, "balance"), Value::Int(100))])
        .await?;

    let mut transaction = begin(&h, main).await?;
    tx_get(&h, &mut transaction, &prop(acme, "balance")).await?;
    // Two separate layers on the transaction branch, both touching cells the transaction read.
    tx_set(&h, &mut transaction, &prop(acme, "balance"), Value::Int(1)).await?;
    tx_set(&h, &mut transaction, &prop(acme, "name"), Value::Int(2)).await?;

    let replayed = h
        .branches
        .merge_transaction(&transaction, MergeMode::DefAndData)
        .await?;
    assert_eq!(replayed.len(), 2, "both layers land");
    assert_eq!(
        h.read(main, &prop(acme, "balance")).await?,
        Some(Value::Int(1))
    );
    Ok(())
}

#[tokio::test]
async fn a_transaction_that_read_derived_data_can_still_commit() -> Result<()> {
    // An automatic guard on a derived cell is not a client mistake — it is a read. Guarding it would
    // be checking a shadow (§12), so it contributes nothing; *rejecting* the commit for having
    // looked would make every transaction that reads a computed value unable to write.
    let h = Harness::new();
    let main = h.root().await?;
    let acme = company(main, 1);
    h.push(main, vec![(prop(acme, "balance"), Value::Int(100))])
        .await?;
    h.push_derived(main, &prop(acme, "is_rich"), Value::Bool(true))
        .await?;

    let mut transaction = begin(&h, main).await?;
    tx_get(&h, &mut transaction, &prop(acme, "is_rich")).await?;
    tx_set(&h, &mut transaction, &prop(acme, "name"), Value::Int(7)).await?;

    h.branches
        .merge_transaction(&transaction, MergeMode::DefAndData)
        .await?;
    assert_eq!(
        h.read(main, &prop(acme, "name")).await?,
        Some(Value::Int(7))
    );
    Ok(())
}
