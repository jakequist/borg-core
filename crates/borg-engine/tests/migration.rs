//! Migrations running through the derivation engine. SPEC.md §9.1, §9.3.
//!
//! The claim under test is that **a migration is a species of pipeline** — same engine, same
//! dependency capture, same watermark, same failure scoping. Only the trigger differs: a pipeline is
//! triggered by a data write, a migration by a def-mutation.

use borg_core::{
    BranchId, BufferId, CellRecord, CellRef, ClientVersion, Freshness, FreshnessRequirement,
    LayerAuthor, LayerId, LayerKind, MigrationDirection, Origin, Pid, PidKind, ProducerDef,
    ProducerId, ProducerKind, RepoId, Result, Value,
};
use borg_engine::{
    BranchManager, DefRegistry, DerivationEngine, FrontierTracker, InProcessSequencer,
    LayerManager, MemoryDependencyIndex, Resolver, VersionStep,
};
use borg_exec::ProducerCtx;
use borg_exec_native::NativeExecutor;
use borg_storage::MemoryStorage;
use std::sync::Arc;

const BRANCH: BranchId = BranchId(1);
/// The original def-view.
const V1: ClientVersion = ClientVersion(LayerId(1));
/// The def-view introduced by the def-mutation under test.
const V9: ClientVersion = ClientVersion(LayerId(9));
const UP: ProducerId = ProducerId(50);
const SCORE: ProducerId = ProducerId(1);

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
    frontier: Arc<FrontierTracker>,
}

impl Harness {
    fn new(executor: NativeExecutor, defs: Arc<DefRegistry>) -> Self {
        let storage = Arc::new(MemoryStorage::new());
        let index = Arc::new(MemoryDependencyIndex::new());
        let layers = Arc::new(LayerManager::new(
            storage.clone(),
            Arc::new(InProcessSequencer::new()),
        ));
        let branches = Arc::new(BranchManager::new(layers.clone()));
        let frontier = Arc::new(FrontierTracker::new());
        let engine = Arc::new(DerivationEngine::new(
            storage.clone(),
            layers.clone(),
            index.clone(),
            Arc::new(executor),
            frontier.clone(),
            defs.clone(),
            branches.clone(),
        ));
        Self {
            layers,
            engine,
            resolver: Resolver::new(storage, index, defs, branches.clone()),
            frontier,
        }
    }

    async fn push(&self, version: ClientVersion, writes: Vec<(CellRef, Value)>) -> Result<LayerId> {
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

    fn head(&self) -> LayerId {
        self.layers.head(BRANCH).unwrap()
    }

    async fn read(
        &self,
        cell: &CellRef,
        version: ClientVersion,
    ) -> Result<borg_core::Resolved<Option<Value>>> {
        self.resolver
            .resolve(
                BRANCH,
                cell,
                self.head(),
                version,
                FreshnessRequirement::Validated,
            )
            .await
    }
}

/// `up_v1→v9` for `Company.website`: reads its own source cell at v1 and writes v9.
///
/// The `get_at` is the one place a migration departs from an ordinary producer. An ordinary `get`
/// resolves at the migration's own ClientVersion — which *is* v9 — and would recurse straight into
/// the value it is supposed to be producing (SPEC.md §9.3).
fn website_up() -> borg_exec_native::ProducerFn {
    Arc::new(|ctx: &mut dyn ProducerCtx, input: Pid| {
        Box::pin(async move {
            let old = ctx.get_at(&prop(input, "website"), V1).await?;
            let migrated = match old {
                Some(Value::Int(n)) => Value::Int(n * 10),
                Some(other) => other,
                None => return Ok(()),
            };
            ctx.set(&prop(input, "website"), migrated).await
        })
    })
}

fn migration_def() -> ProducerDef {
    ProducerDef {
        id: UP,
        kind: ProducerKind::Migration {
            from: V1.0,
            to: V9.0,
            direction: MigrationDirection::Up,
        },
        // A migration maps over the *field's* buffer, not the struct's: it is defined per output
        // field (SPEC.md §9.3), and per-field buffers make that exactly expressible (SPEC.md §4.2).
        source: BufferId::ObjectProp("Company".into(), "website".into()),
        version: V9.0,
        declaring_repo: RepoId(1),
    }
}

fn version_step(down: Option<ProducerId>) -> VersionStep {
    VersionStep {
        from: V1,
        to: V9,
        up: UP,
        down,
    }
}

#[tokio::test]
async fn a_migration_materializes_the_new_version_without_disturbing_the_old() -> Result<()> {
    let defs = Arc::new(DefRegistry::new());
    defs.push_step("Company".into(), "website".into(), version_step(None));

    let mut executor = NativeExecutor::new();
    executor.register(UP, website_up());
    let h = Harness::new(executor, defs);
    h.engine.register(migration_def());

    let acme = company(100);
    let source = h
        .push(V1, vec![(prop(acme, "website"), Value::Int(9))])
        .await?;
    assert_eq!(
        h.engine.catch_up(BRANCH).await?,
        1,
        "a write at v1 is work for the migration, exactly as it would be for a pipeline"
    );

    let at_v9 = h.read(&prop(acme, "website"), V9).await?;
    assert_eq!(at_v9.value, Some(Value::Int(90)), "the migrated view");
    assert_eq!(at_v9.origin, Origin::Derived);
    assert_eq!(
        at_v9.by,
        Some(UP),
        "attributed to the migration that made it"
    );
    assert_eq!(at_v9.state, Freshness::Current);

    // Writes are never coerced, so the value its author wrote is still there, untouched.
    let at_v1 = h.read(&prop(acme, "website"), V1).await?;
    assert_eq!(at_v1.value, Some(Value::Int(9)));
    assert_eq!(at_v1.origin, Origin::Source);

    // And a migration carries a watermark like any other producer — pointing into the *source*
    // stream, not at head, which by now is the derived layer it just committed (SPEC.md §6.3).
    assert_eq!(h.frontier.watermark(BRANCH, UP), source);
    Ok(())
}

#[tokio::test]
async fn a_later_write_at_the_old_version_re_runs_the_migration() -> Result<()> {
    let defs = Arc::new(DefRegistry::new());
    defs.push_step("Company".into(), "website".into(), version_step(None));

    let mut executor = NativeExecutor::new();
    executor.register(UP, website_up());
    let h = Harness::new(executor, defs);
    h.engine.register(migration_def());

    let acme = company(200);
    h.push(V1, vec![(prop(acme, "website"), Value::Int(9))])
        .await?;
    h.engine.catch_up(BRANCH).await?;

    // An old client writes again. The v9 view must follow.
    h.push(V1, vec![(prop(acme, "website"), Value::Int(4))])
        .await?;
    assert_eq!(h.engine.catch_up(BRANCH).await?, 1);
    assert_eq!(
        h.read(&prop(acme, "website"), V9).await?.value,
        Some(Value::Int(40)),
        "an old client's write stays visible to new clients"
    );

    // The migration is not poisoned: writing `C@v9` into the very buffer it consumes `C@v1` from
    // must not read as a new entity, or it would re-trigger itself forever.
    assert!(
        h.engine.is_broken(BRANCH, UP).is_none(),
        "a migration does not mistake its own output for its input"
    );
    Ok(())
}

#[tokio::test]
async fn a_pipeline_at_the_old_version_is_untouched_by_the_migration() -> Result<()> {
    let defs = Arc::new(DefRegistry::new());
    defs.push_step("Company".into(), "website".into(), version_step(None));

    let mut executor = NativeExecutor::new();
    executor.register(UP, website_up());
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

    let h = Harness::new(executor, defs);
    h.engine.register(migration_def());
    h.engine.register(ProducerDef {
        id: SCORE,
        kind: ProducerKind::Pipeline,
        source: BufferId::Object("Company".into()),
        version: V1.0,
        declaring_repo: RepoId(1),
    });

    let acme = company(300);
    h.push(
        V1,
        vec![
            (existence(acme), Value::Bool(true)),
            (prop(acme, "website"), Value::Int(9)),
        ],
    )
    .await?;
    h.engine.catch_up(BRANCH).await?;

    // The v1 pipeline read `website@v1`; the migration wrote `website@v9`. Same CellRef, different
    // record — so the migration's output must not read as a change to the pipeline's input.
    assert_eq!(
        h.engine.catch_up(BRANCH).await?,
        0,
        "everything has settled: neither producer is triggered by the other's output"
    );
    assert_eq!(
        h.read(&prop(acme, "is_investible"), V1).await?.state,
        Freshness::Current,
        "the v1 pipeline's result is not made stale by a migration to v9"
    );
    Ok(())
}

#[tokio::test]
async fn a_migration_is_skipped_when_no_client_is_live_on_its_target() -> Result<()> {
    let defs = Arc::new(DefRegistry::new());
    defs.push_step("Company".into(), "website".into(), version_step(None));
    // Only v1 has clients, so materializing v9 is wasted work (SPEC.md §5.5).
    defs.mark_live(V1);

    let mut executor = NativeExecutor::new();
    executor.register(UP, website_up());
    let h = Harness::new(executor, defs);
    h.engine.register(migration_def());

    let acme = company(400);
    h.push(V1, vec![(prop(acme, "website"), Value::Int(9))])
        .await?;
    assert_eq!(
        h.engine.catch_up(BRANCH).await?,
        0,
        "no live client on v9, so nothing is materialized for it"
    );

    // And the reader is told plainly that v9 is behind rather than being handed a wrong answer.
    assert_eq!(
        h.read(&prop(acme, "website"), V9).await?.state,
        Freshness::Stale
    );
    Ok(())
}
