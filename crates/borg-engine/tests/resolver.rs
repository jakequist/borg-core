//! The read path. SPEC.md §10, §11.
//!
//! Borg never returns a bare value. Every read states what it reflects, how stale it may be, and —
//! via `explain` — where it came from.

use borg_core::{
    BranchId, BufferId, CellRef, ClientVersion, DefEvent, Freshness, FreshnessRequirement,
    LayerAuthor, LayerId, Origin, Ownership, Pid, PidKind, ProducerDef, ProducerId, ProducerKind,
    RepoId, Result, Value, ValueType, Writer,
};
use borg_engine::{
    BranchManager, CellTouchIndex, DefRegistry, DerivationEngine, FrontierTracker,
    InProcessSequencer, LayerManager, MemoryDependencyIndex, Resolver, WriteSession,
};
use borg_exec::ProducerCtx;
use borg_exec_native::NativeExecutor;
use borg_storage::MemoryStorage;
use std::sync::Arc;

const BRANCH: BranchId = BranchId(1);
const SCORE: ProducerId = ProducerId(1);
/// The def-view every actor in these tests was authored against.
const V1: ClientVersion = ClientVersion(LayerId(1));

fn company(n: u64) -> Pid {
    Pid::Allocated {
        kind: PidKind::Object,
        branch: BRANCH,
        allocator: borg_core::AllocatorId(0),
        counter: n,
    }
}

fn existence(pid: Pid) -> CellRef {
    CellRef::existence("Company".into(), pid)
}

fn prop(pid: Pid, field: &str) -> CellRef {
    CellRef::prop("Company".into(), field.into(), pid)
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

struct Harness {
    layers: Arc<LayerManager>,
    engine: Arc<DerivationEngine>,
    resolver: Resolver,
    defs: Arc<DefRegistry>,
}

impl Harness {
    async fn new() -> Result<Self> {
        let storage = Arc::new(MemoryStorage::new());
        let index = Arc::new(MemoryDependencyIndex::new());
        let layers = Arc::new(LayerManager::new(
            storage.clone(),
            Arc::new(InProcessSequencer::new()),
            Arc::new(CellTouchIndex::new()),
        ));
        let branches = Arc::new(BranchManager::new(layers.clone()));
        let defs = Arc::new(DefRegistry::new(layers.clone(), storage.clone()));

        let executor = NativeExecutor::new();
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
            Arc::new(executor),
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

        // Nothing may be written until it is declared (SPEC.md §8), and `is_investible` names the
        // one producer allowed to write it.
        defs.push(
            BRANCH,
            vec![
                declare("website", ValueType::Int, Ownership::Source),
                declare("name", ValueType::Int, Ownership::Source),
                declare("is_investible", ValueType::Bool, Ownership::Derived(SCORE)),
            ],
        )
        .await?;

        Ok(Self {
            layers,
            engine,
            resolver: Resolver::new(storage, index, defs.clone(), branches.clone()),
            defs,
        })
    }

    async fn push_at(
        &self,
        version: ClientVersion,
        writes: Vec<(CellRef, Value)>,
    ) -> Result<LayerId> {
        let mut session = WriteSession::open(
            &self.layers,
            &self.defs,
            BRANCH,
            None,
            version,
            Writer::Client,
            LayerAuthor::Source,
        )
        .await?;
        for (cell, value) in writes {
            session.set(&cell, value).await?;
        }
        session.commit().await
    }

    async fn push(&self, writes: Vec<(CellRef, Value)>) -> Result<LayerId> {
        self.push_at(V1, writes).await
    }

    fn head(&self) -> LayerId {
        self.layers.head(BRANCH).unwrap()
    }
}

#[tokio::test]
async fn source_and_derived_cells_report_different_provenance() -> Result<()> {
    let h = Harness::new().await?;
    let acme = company(100);
    h.push(vec![
        (existence(acme), Value::Bool(true)),
        (prop(acme, "website"), Value::Int(9)),
    ])
    .await?;
    h.engine.catch_up(BRANCH).await?;

    let website = h
        .resolver
        .resolve(
            BRANCH,
            &prop(acme, "website"),
            Some(h.head()),
            V1,
            FreshnessRequirement::Validated,
        )
        .await?;
    assert_eq!(website.origin, Origin::Source);
    assert_eq!(
        website.state,
        Freshness::Current,
        "source data is never stale"
    );
    assert_eq!(website.by, None);
    assert_eq!(website.value, Some(Value::Int(9)));

    let derived = h
        .resolver
        .resolve(
            BRANCH,
            &prop(acme, "is_investible"),
            Some(h.head()),
            V1,
            FreshnessRequirement::Validated,
        )
        .await?;
    assert_eq!(derived.origin, Origin::Derived);
    assert_eq!(derived.by, Some(SCORE));
    assert_eq!(derived.value, Some(Value::Bool(true)));
    assert_eq!(derived.state, Freshness::Current);
    Ok(())
}

#[tokio::test]
async fn validation_distinguishes_a_moved_dependency_from_an_unrelated_write() -> Result<()> {
    let h = Harness::new().await?;
    let acme = company(200);
    h.push(vec![
        (existence(acme), Value::Bool(true)),
        (prop(acme, "website"), Value::Int(9)),
    ])
    .await?;
    h.engine.catch_up(BRANCH).await?;

    // Write a field the producer never read, and deliberately do not catch up. Validation must walk
    // the read-set, find nothing moved, and tighten the watermark to head — without running any
    // user code.
    h.push(vec![(prop(acme, "name"), Value::Int(7))]).await?;
    let after_unrelated = h
        .resolver
        .resolve(
            BRANCH,
            &prop(acme, "is_investible"),
            Some(h.head()),
            V1,
            FreshnessRequirement::Validated,
        )
        .await?;
    assert_eq!(
        after_unrelated.state,
        Freshness::Current,
        "an unrelated write does not make a derived value stale"
    );
    assert_eq!(
        after_unrelated.fresh_as_of,
        h.head(),
        "validation advances the watermark to head"
    );

    // Now move something the producer actually read, still without catching up.
    h.push(vec![(prop(acme, "website"), Value::Int(1))]).await?;
    let after_dependency = h
        .resolver
        .resolve(
            BRANCH,
            &prop(acme, "is_investible"),
            Some(h.head()),
            V1,
            FreshnessRequirement::Validated,
        )
        .await?;
    assert_eq!(
        after_dependency.state,
        Freshness::Stale,
        "a moved dependency is reported as stale rather than silently served"
    );
    assert!(
        after_dependency.fresh_as_of.0 < h.head().0,
        "and the watermark stays honestly behind"
    );
    assert_eq!(
        after_dependency.value,
        Some(Value::Bool(true)),
        "the stale value is still returned — labelled, not withheld"
    );
    Ok(())
}

#[tokio::test]
async fn explain_walks_the_dependency_index_backwards() -> Result<()> {
    let h = Harness::new().await?;
    let acme = company(300);
    h.push(vec![
        (existence(acme), Value::Bool(true)),
        (prop(acme, "website"), Value::Int(9)),
    ])
    .await?;
    h.engine.catch_up(BRANCH).await?;

    let lineage = h
        .resolver
        .explain(BRANCH, &prop(acme, "is_investible"), Some(h.head()), V1)
        .await?
        .expect("derived cells have lineage");

    assert_eq!(lineage.produced_by, Some(SCORE));
    assert_eq!(lineage.from.len(), 1, "one input was read, so one edge");
    assert_eq!(lineage.from[0].cell.cell, prop(acme, "website"));
    assert_eq!(
        lineage.from[0].cell.version, V1,
        "the edge records the version the producer read at"
    );
    assert_eq!(
        lineage.from[0].origin,
        Origin::Source,
        "the chain bottoms out in ground truth"
    );
    Ok(())
}

#[tokio::test]
async fn a_reader_on_an_unmaterialized_version_is_told_the_migration_is_behind() -> Result<()> {
    const UP: ProducerId = ProducerId(50);
    let h = Harness::new().await?;

    // The field exists, and is then mutated with an `up` migration. Both def layers are real, so
    // their ids *are* the two def-versions (SPEC.md §5.3).
    let declared = h
        .defs
        .push(
            BRANCH,
            vec![declare("score", ValueType::Int, Ownership::Source)],
        )
        .await?;
    let acme = company(400);
    h.push_at(
        ClientVersion(declared),
        vec![(prop(acme, "score"), Value::Int(9))],
    )
    .await?;
    let mutated = h
        .defs
        .push(
            BRANCH,
            vec![DefEvent::MutateField {
                struct_name: "Company".into(),
                field: "score".into(),
                ty: ValueType::Double,
                repo: RepoId(1),
                up: UP,
                down: None,
            }],
        )
        .await?;

    // The migration exists but has not run.
    let ahead = h
        .resolver
        .resolve(
            BRANCH,
            &prop(acme, "score"),
            Some(h.head()),
            ClientVersion(mutated),
            FreshnessRequirement::Validated,
        )
        .await?;
    assert_eq!(
        ahead.state,
        Freshness::Stale,
        "a value not yet migrated to your version is a migration that has not caught up"
    );
    assert_eq!(ahead.value, None);

    // The version it was actually written at still reads cleanly — writes are never coerced, so
    // both coexist (SPEC.md §5.4).
    let at_source = h
        .resolver
        .resolve(
            BRANCH,
            &prop(acme, "score"),
            Some(h.head()),
            ClientVersion(declared),
            FreshnessRequirement::Validated,
        )
        .await?;
    assert_eq!(at_source.state, Freshness::Current);
    assert_eq!(at_source.value, Some(Value::Int(9)));
    Ok(())
}

#[tokio::test]
async fn a_def_push_without_a_down_migration_is_unreachable_for_older_clients() -> Result<()> {
    let h = Harness::new().await?;

    let declared = h
        .defs
        .push(
            BRANCH,
            vec![declare("rating", ValueType::Int, Ownership::Source)],
        )
        .await?;
    // Mutated with an `up` but deliberately no `down`.
    let mutated = h
        .defs
        .push(
            BRANCH,
            vec![DefEvent::MutateField {
                struct_name: "Company".into(),
                field: "rating".into(),
                ty: ValueType::Double,
                repo: RepoId(1),
                up: ProducerId(50),
                down: None,
            }],
        )
        .await?;

    let acme = company(500);
    // Written by a client on the new version.
    h.push_at(
        ClientVersion(mutated),
        vec![(prop(acme, "rating"), Value::Double(9.0))],
    )
    .await?;

    let old_client = h
        .resolver
        .resolve(
            BRANCH,
            &prop(acme, "rating"),
            Some(h.head()),
            ClientVersion(declared),
            FreshnessRequirement::Validated,
        )
        .await?;
    assert_eq!(
        old_client.state,
        Freshness::Broken,
        "without a down migration the older client's view is unreachable, and is said so plainly"
    );
    Ok(())
}
