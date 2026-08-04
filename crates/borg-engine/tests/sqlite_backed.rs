//! The engine, running on SQLite instead of memory.
//!
//! This is the test that says whether `StorageProvider` is the right seam. The harness differs from
//! the one in `derivation_cycle.rs` by exactly one line — which backend it constructs — and nothing
//! above the provider line changes.

use borg_core::{
    BranchId, BufferId, CellRef, ClientVersion, DefEvent, Freshness, FreshnessRequirement,
    LayerAuthor, LayerId, MergeMode, Ownership, Pid, PidKind, ProducerDef, ProducerId,
    ProducerKind, RepoId, Result, Value, ValueType, Writer,
};
use borg_engine::{
    BranchManager, CellTouchIndex, DefRegistry, DerivationEngine, FrontierTracker,
    InProcessSequencer, LayerManager, MemoryDependencyIndex, Resolver, WriteSession,
};
use borg_exec::ProducerCtx;
use borg_exec_native::NativeExecutor;
use borg_storage_sqlite::SqliteStorage;
use std::sync::Arc;

const SCORE: ProducerId = ProducerId(1);
const V1: ClientVersion = ClientVersion(LayerId(1));

fn company(branch: BranchId, n: u64) -> Pid {
    Pid::Allocated {
        kind: PidKind::Object,
        branch,
        allocator: borg_core::AllocatorId(0),
        counter: n,
    }
}

fn declare(field: &str, ty: ValueType, ownership: Ownership) -> DefEvent {
    DefEvent::DeclareField {
        struct_name: "Company".into(),
        field: field.into(),
        ty,
        repo: RepoId(1),
        ownership,
    }
}

fn existence(pid: Pid) -> CellRef {
    CellRef::existence("Company".into(), pid)
}

fn prop(pid: Pid, field: &str) -> CellRef {
    CellRef::prop("Company".into(), field.into(), pid)
}

struct Harness {
    layers: Arc<LayerManager>,
    branches: Arc<BranchManager>,
    engine: Arc<DerivationEngine>,
    resolver: Resolver,
    defs: Arc<DefRegistry>,
}

impl Harness {
    fn new() -> Result<Self> {
        // The only line that differs from the memory-backed harness.
        let storage = Arc::new(SqliteStorage::in_memory()?);

        let index = Arc::new(MemoryDependencyIndex::new());
        let layers = Arc::new(LayerManager::new(
            storage.clone(),
            Arc::new(InProcessSequencer::new()),
            Arc::new(CellTouchIndex::new()),
        ));
        let branches = Arc::new(BranchManager::new(layers.clone()));
        let defs = Arc::new(DefRegistry::new(layers.clone(), storage.clone()));

        let executor = Arc::new(NativeExecutor::new());
        executor.register(
            SCORE,
            Arc::new(|ctx: &mut dyn ProducerCtx, input: Pid| {
                Box::pin(async move {
                    let website = ctx.get(&prop(input, "website")).await?;
                    let investible = matches!(website, Some(Value::Int(n)) if n > 3);
                    ctx.set(&prop(input, "is_investible"), Value::Bool(investible))
                        .await
                })
            }),
        );

        let engine = Arc::new(DerivationEngine::new(
            storage.clone(),
            layers.clone(),
            index.clone(),
            executor,
            Arc::new(FrontierTracker::new()),
            defs.clone(),
            branches.clone(),
        ));
        engine.register(ProducerDef {
            id: SCORE,
            kind: ProducerKind::Pipeline,
            source: BufferId::Object("Company".into()),
            version: LayerId(1),
            declaring_repo: RepoId(1),
        });

        let resolver = Resolver::new(
            storage,
            index,
            defs.clone(),
            branches.clone(),
            engine.clone(),
        );
        Ok(Self {
            layers,
            branches,
            engine,
            resolver,
            defs,
        })
    }

    /// A root branch with the schema these tests write against.
    async fn root(&self) -> Result<BranchId> {
        let branch = self.branches.create_root(None).await?;
        self.defs
            .push(
                branch,
                vec![
                    declare("website", ValueType::Int, Ownership::Source),
                    declare("name", ValueType::Int, Ownership::Source),
                    declare("is_investible", ValueType::Bool, Ownership::Derived(SCORE)),
                ],
            )
            .await?;
        Ok(branch)
    }

    async fn push(&self, branch: BranchId, writes: Vec<(CellRef, Value)>) -> Result<LayerId> {
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
        for (cell, value) in writes {
            session.set(&cell, value).await?;
        }
        session.commit().await
    }

    async fn read(&self, branch: BranchId, cell: &CellRef) -> Result<Option<Value>> {
        Ok(self
            .resolver
            .resolve(branch, cell, None, V1, FreshnessRequirement::Validated)
            .await?
            .value)
    }
}

#[tokio::test]
async fn the_derivation_cycle_runs_unchanged_on_sqlite() -> Result<()> {
    let h = Harness::new()?;
    let main = h.root().await?;
    let acme = company(main, 1);

    h.push(
        main,
        vec![
            (existence(acme), Value::Bool(true)),
            (prop(acme, "website"), Value::Int(9)),
        ],
    )
    .await?;
    assert_eq!(h.engine.catch_up(main).await?, 1);
    assert_eq!(
        h.read(main, &prop(acme, "is_investible")).await?,
        Some(Value::Bool(true))
    );

    // Field-granular invalidation still holds across a real store.
    h.push(main, vec![(prop(acme, "name"), Value::Int(1))])
        .await?;
    assert_eq!(h.engine.catch_up(main).await?, 0);

    h.push(main, vec![(prop(acme, "website"), Value::Int(1))])
        .await?;
    assert_eq!(h.engine.catch_up(main).await?, 1);
    assert_eq!(
        h.read(main, &prop(acme, "is_investible")).await?,
        Some(Value::Bool(false))
    );
    Ok(())
}

#[tokio::test]
async fn branching_and_merge_run_unchanged_on_sqlite() -> Result<()> {
    let h = Harness::new()?;
    let main = h.root().await?;
    let acme = company(main, 1);

    let fork_point = h
        .push(
            main,
            vec![
                (existence(acme), Value::Bool(true)),
                (prop(acme, "website"), Value::Int(9)),
            ],
        )
        .await?;
    h.engine.catch_up(main).await?;

    let feature = h.branches.fork(main, fork_point, None).await?;
    assert_eq!(
        h.read(feature, &prop(acme, "website")).await?,
        Some(Value::Int(9)),
        "the fork inherits through ancestry, resolved by SQL rather than by a HashMap walk"
    );

    h.push(feature, vec![(prop(acme, "website"), Value::Int(1))])
        .await?;
    assert_eq!(
        h.read(main, &prop(acme, "website")).await?,
        Some(Value::Int(9)),
        "and the parent is untouched"
    );

    h.branches.merge(feature, MergeMode::DefAndData).await?;
    assert_eq!(
        h.read(main, &prop(acme, "website")).await?,
        Some(Value::Int(1))
    );

    // The parent re-derives rather than inheriting the child's derived values.
    h.engine.catch_up(main).await?;
    assert_eq!(
        h.read(main, &prop(acme, "is_investible")).await?,
        Some(Value::Bool(false))
    );
    Ok(())
}

#[tokio::test]
async fn def_events_and_provenance_survive_a_real_store() -> Result<()> {
    let h = Harness::new()?;
    let main = h.root().await?;

    let acme = company(main, 1);
    h.push(
        main,
        vec![
            (existence(acme), Value::Bool(true)),
            (prop(acme, "website"), Value::Int(9)),
        ],
    )
    .await?;
    h.engine.catch_up(main).await?;

    let path = h.branches.read_path(main, None)?;
    let view = h.defs.view(&path).await?;
    assert!(
        view.object(&"Company".into()).is_some(),
        "definitions fold out of def layers stored as rows just as they do from memory"
    );

    let head = h.layers.head(main).unwrap();
    let resolved = h
        .resolver
        .resolve(
            main,
            &prop(acme, "is_investible"),
            Some(head),
            V1,
            FreshnessRequirement::Validated,
        )
        .await?;
    assert_eq!(resolved.state, Freshness::Current);
    assert_eq!(resolved.by, Some(SCORE));

    let lineage = h
        .resolver
        .explain(main, &prop(acme, "is_investible"), Some(head), V1)
        .await?
        .expect("lineage");
    assert_eq!(lineage.from.len(), 1);
    assert_eq!(lineage.from[0].cell.cell, prop(acme, "website"));
    Ok(())
}
