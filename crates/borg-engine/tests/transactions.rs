//! Object transactions and guards. SPEC.md §12, §13.
//!
//! Written before the implementation. A guard asserts that nothing has touched the named cells since
//! a given layer — and the same mechanism, re-evaluated against a parent since the fork point, *is*
//! the merge-conflict detector.

use borg_core::{
    BranchId, CellRef, ClientVersion, DefEvent, DefVersion, Guard, LayerAuthor, LayerId, MergeMode,
    Ownership, Pid, PidKind, ProducerId, RepoId, Result, Value, ValueType, Writer,
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
            None,
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
            None,
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
