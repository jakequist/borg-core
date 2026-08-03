//! The read path. SPEC.md §10, §11.
//!
//! Borg never returns a bare value. Every read states what it reflects, how stale it may be, and —
//! via `explain` — where it came from.

use borg_core::{
    BranchId, BufferId, CellRecord, CellRef, ClientVersion, Freshness, FreshnessRequirement,
    LayerAuthor, LayerId, LayerKind, Origin, Pid, PidKind, ProducerDef, ProducerId, ProducerKind,
    RepoId, Result, Value,
};
use borg_engine::{
    BranchManager, CellTouchIndex, DefRegistry, DerivationEngine, FrontierTracker,
    InProcessSequencer, LayerManager, MemoryDependencyIndex, Resolver, VersionStep,
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

struct Harness {
    layers: Arc<LayerManager>,
    engine: Arc<DerivationEngine>,
    resolver: Resolver,
    defs: Arc<DefRegistry>,
}

impl Harness {
    fn new() -> Self {
        let storage = Arc::new(MemoryStorage::new());
        let index = Arc::new(MemoryDependencyIndex::new());
        let defs = Arc::new(DefRegistry::new());
        let layers = Arc::new(LayerManager::new(
            storage.clone(),
            Arc::new(InProcessSequencer::new()),
            Arc::new(CellTouchIndex::new()),
        ));
        let branches = Arc::new(BranchManager::new(layers.clone()));

        let mut executor = NativeExecutor::new();
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

        Self {
            layers,
            engine,
            resolver: Resolver::new(storage, index, defs.clone(), branches.clone()),
            defs,
        }
    }

    async fn push_at(
        &self,
        version: ClientVersion,
        writes: Vec<(CellRef, Value)>,
    ) -> Result<LayerId> {
        let mut layer = self
            .layers
            .open(BRANCH, LayerKind::Value, LayerAuthor::Source)
            .await?;
        for (cell, value) in writes {
            layer
                .put(
                    &cell,
                    CellRecord {
                        value,
                        version,
                        written_at: layer.id(),
                        origin: Origin::Source,
                        derivation: None,
                    },
                )
                .await?;
        }
        self.layers.commit(layer).await
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
    let h = Harness::new();
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
            h.head(),
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
            h.head(),
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
    let h = Harness::new();
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
            h.head(),
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
            h.head(),
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
    let h = Harness::new();
    let acme = company(300);
    h.push(vec![
        (existence(acme), Value::Bool(true)),
        (prop(acme, "website"), Value::Int(9)),
    ])
    .await?;
    h.engine.catch_up(BRANCH).await?;

    let lineage = h
        .resolver
        .explain(BRANCH, &prop(acme, "is_investible"), h.head(), V1)
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
    const V9: ClientVersion = ClientVersion(LayerId(9));
    const UP: ProducerId = ProducerId(50);

    let h = Harness::new();
    let acme = company(400);
    h.push(vec![(prop(acme, "website"), Value::Int(9))]).await?;

    // A def-mutation exists carrying v1 forward to v9, but the migration has not run yet.
    h.defs.push_step(
        "Company".into(),
        "website".into(),
        VersionStep {
            from: V1,
            to: V9,
            up: UP,
            down: None,
        },
    );

    let ahead = h
        .resolver
        .resolve(
            BRANCH,
            &prop(acme, "website"),
            h.head(),
            V9,
            FreshnessRequirement::Validated,
        )
        .await?;
    assert_eq!(
        ahead.state,
        Freshness::Stale,
        "a value not yet migrated to your version is a migration that has not caught up"
    );
    assert_eq!(ahead.value, None);

    // The version it actually was written at still reads cleanly — writes are never coerced, so both
    // coexist (SPEC.md §5.4).
    let at_source = h
        .resolver
        .resolve(
            BRANCH,
            &prop(acme, "website"),
            h.head(),
            V1,
            FreshnessRequirement::Validated,
        )
        .await?;
    assert_eq!(at_source.state, Freshness::Current);
    assert_eq!(at_source.value, Some(Value::Int(9)));
    Ok(())
}

#[tokio::test]
async fn a_def_push_without_a_down_migration_is_unreachable_for_older_clients() -> Result<()> {
    const V9: ClientVersion = ClientVersion(LayerId(9));

    let h = Harness::new();
    let acme = company(500);
    // Written by a v9 client.
    h.push_at(V9, vec![(prop(acme, "website"), Value::Int(9))])
        .await?;

    // The def-push supplied no `down`, so there is no way back to v1.
    h.defs.push_step(
        "Company".into(),
        "website".into(),
        VersionStep {
            from: V1,
            to: V9,
            up: ProducerId(50),
            down: None,
        },
    );

    let old_client = h
        .resolver
        .resolve(
            BRANCH,
            &prop(acme, "website"),
            h.head(),
            V1,
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
