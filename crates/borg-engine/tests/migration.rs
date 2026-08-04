//! Migrations running through the derivation engine. SPEC.md §9.1, §9.3.
//!
//! The claim under test is that **a migration is a species of pipeline** — same engine, same
//! dependency capture, same watermark, same failure scoping. Only the trigger differs: a pipeline is
//! triggered by a data write, a migration by a def-mutation.

use borg_core::{
    BranchId, BufferId, CellRef, ClientVersion, DefEvent, Freshness, FreshnessRequirement,
    LayerAuthor, LayerId, MigrationDirection, Origin, Ownership, Pid, PidKind, ProducerDef,
    ProducerId, ProducerKind, RepoId, Result, Value, ValueType, Writer,
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
    defs: Arc<DefRegistry>,
    executor: Arc<NativeExecutor>,
}

impl Harness {
    fn new() -> Self {
        let storage = Arc::new(MemoryStorage::new());
        let index = Arc::new(MemoryDependencyIndex::new());
        let layers = Arc::new(LayerManager::new(
            storage.clone(),
            Arc::new(InProcessSequencer::new()),
            Arc::new(CellTouchIndex::new()),
        ));
        let branches = Arc::new(BranchManager::new(layers.clone()));
        let defs = Arc::new(DefRegistry::new(layers.clone(), storage.clone()));
        let executor = Arc::new(NativeExecutor::new());
        let frontier = Arc::new(FrontierTracker::new());
        let engine = Arc::new(DerivationEngine::new(
            storage.clone(),
            layers.clone(),
            index.clone(),
            executor.clone(),
            frontier.clone(),
            defs.clone(),
            branches.clone(),
        ));
        Self {
            layers,
            engine: engine.clone(),
            resolver: Resolver::new(
                storage,
                index,
                defs.clone(),
                branches.clone(),
                engine.clone(),
            ),
            frontier,
            defs,
            executor,
        }
    }

    async fn push(&self, version: ClientVersion, writes: Vec<(CellRef, Value)>) -> Result<LayerId> {
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

    /// Install a producer implementation. The log records the *definition*; this is the other half
    /// (SPEC.md §9.2).
    fn install(&self, id: ProducerId, f: borg_exec_native::ProducerFn) {
        self.executor.register(id, f);
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
                Some(self.head()),
                version,
                FreshnessRequirement::Validated,
            )
            .await
    }
}

/// `up_v1→v9` for `Company.website`: reads its own source cell at v1 and writes v9.
///
/// The `get_input` is the one place a migration departs from an ordinary producer. An ordinary `get`
/// resolves at the migration's own ClientVersion — which *is* v9 — and would recurse straight into
/// the value it is supposed to be producing (SPEC.md §9.3).
fn website_up() -> borg_exec_native::ProducerFn {
    Arc::new(move |ctx: &mut dyn ProducerCtx, input: Pid| {
        Box::pin(async move {
            let old = ctx.get_input(&prop(input, "website")).await?;
            let migrated = match old {
                Some(Value::Int(n)) => Value::Int(n * 10),
                Some(other) => other,
                None => return Ok(()),
            };
            ctx.set(&prop(input, "website"), migrated).await
        })
    })
}

/// The exact inverse, and it too reads its input with `get_input` — which for a `down` migration is
/// the *newer* version. One verb, whichever way a migration runs.
fn website_down() -> borg_exec_native::ProducerFn {
    Arc::new(move |ctx: &mut dyn ProducerCtx, input: Pid| {
        Box::pin(async move {
            let new = ctx.get_input(&prop(input, "website")).await?;
            let migrated = match new {
                Some(Value::Int(n)) => Value::Int(n / 10),
                Some(other) => other,
                None => return Ok(()),
            };
            ctx.set(&prop(input, "website"), migrated).await
        })
    })
}

/// A migration definition records a direction and nothing more. Which two versions it bridges is
/// folded from the `MutateField` that named it, per branch (SPEC.md §5.3).
fn migration_def(id: ProducerId, direction: MigrationDirection) -> ProducerDef {
    ProducerDef {
        id,
        kind: ProducerKind::Migration { direction },
        // A migration maps over the *field's* buffer, not the struct's: it is defined per output
        // field (SPEC.md §9.3), and per-field buffers make that exactly expressible (SPEC.md §4.2).
        source: BufferId::ObjectProp("Company".into(), "website".into()),
        version: LayerId(0),
        declaring_repo: RepoId(1),
    }
}

/// Declare `Company.website`, then mutate it — two def layers, whose ids *are* the two def-versions
/// (SPEC.md §5.3). Returns them.
async fn declare_then_mutate(h: &Harness) -> Result<(ClientVersion, ClientVersion)> {
    declare_then_mutate_with(h, None).await
}

async fn declare_then_mutate_with(
    h: &Harness,
    down: Option<ProducerId>,
) -> Result<(ClientVersion, ClientVersion)> {
    let declared = h
        .defs
        .push(
            BRANCH,
            vec![
                DefEvent::DeclareField {
                    struct_name: "Company".into(),
                    field: "website".into(),
                    ty: ValueType::Int,
                    repo: RepoId(1),
                    ownership: Ownership::Source,
                },
                // The pipeline below writes this one. Ownership is declared, so the field names its
                // producer rather than the producer claiming the field (SPEC.md §8).
                DefEvent::DeclareField {
                    struct_name: "Company".into(),
                    field: "is_investible".into(),
                    ty: ValueType::Bool,
                    repo: RepoId(1),
                    ownership: Ownership::Derived(SCORE),
                },
            ],
        )
        .await?;
    let mutated = h
        .defs
        .push(
            BRANCH,
            vec![DefEvent::MutateField {
                struct_name: "Company".into(),
                field: "website".into(),
                ty: ValueType::Int,
                repo: RepoId(1),
                up: UP,
                down,
            }],
        )
        .await?;
    Ok((ClientVersion(declared), ClientVersion(mutated)))
}

#[tokio::test]
async fn a_migration_materializes_the_new_version_without_disturbing_the_old() -> Result<()> {
    let h = Harness::new();
    let (v_from, v_to) = declare_then_mutate(&h).await?;
    h.install(UP, website_up());
    h.engine.register(migration_def(UP, MigrationDirection::Up));

    let acme = company(100);
    let source = h
        .push(v_from, vec![(prop(acme, "website"), Value::Int(9))])
        .await?;
    assert_eq!(
        h.engine.catch_up(BRANCH).await?,
        1,
        "a write at the old version is work for the migration, exactly as it would be for a pipeline"
    );

    let migrated = h.read(&prop(acme, "website"), v_to).await?;
    assert_eq!(migrated.value, Some(Value::Int(90)), "the migrated view");
    assert_eq!(migrated.origin, Origin::Derived);
    assert_eq!(
        migrated.by,
        Some(UP),
        "attributed to the migration that made it"
    );
    assert_eq!(migrated.state, Freshness::Current);

    // Writes are never coerced, so the value its author wrote is still there, untouched.
    let original = h.read(&prop(acme, "website"), v_from).await?;
    assert_eq!(original.value, Some(Value::Int(9)));
    assert_eq!(original.origin, Origin::Source);

    // A migration carries a watermark like any other producer — pointing into the *source* stream,
    // not at head, which by now is the derived layer it just committed (SPEC.md §6.3).
    assert_eq!(h.frontier.watermark(BRANCH, UP), source);
    Ok(())
}

/// The other half of "a migration is an eager producer, so a version it has not reached yet is lag
/// like any other lag" (SPEC.md §10.4). The lag is real, and a reader unwilling to take it says so.
#[tokio::test]
async fn a_version_no_migration_has_reached_yet_is_computed_on_demand() -> Result<()> {
    let h = Harness::new();
    let (v_from, v_to) = declare_then_mutate(&h).await?;
    h.install(UP, website_up());
    h.engine.register(migration_def(UP, MigrationDirection::Up));

    let acme = company(600);
    h.push(v_from, vec![(prop(acme, "website"), Value::Int(9))])
        .await?;
    // Deliberately no catch-up: the migration exists, is implemented, and has not run.
    let behind = h.read(&prop(acme, "website"), v_to).await?;
    assert_eq!(
        behind.state,
        Freshness::Stale,
        "the reachability check found a path here and reports that nothing has walked it"
    );
    assert_eq!(behind.value, None);

    let computed = h
        .resolver
        .resolve(
            BRANCH,
            &prop(acme, "website"),
            None,
            v_to,
            FreshnessRequirement::Current,
        )
        .await?;
    assert_eq!(
        computed.value,
        Some(Value::Int(90)),
        "the same path the read walked to prove reachability is the one that gets run"
    );
    assert_eq!(computed.state, Freshness::Current);
    assert_eq!(computed.by, Some(UP));

    // The migration has produced one cell, not caught up. Nothing about the rest of the branch was
    // claimed on its behalf.
    assert_eq!(
        h.frontier.watermark(BRANCH, UP),
        LayerId(0),
        "computing one entity inline does not advance a migration's watermark"
    );
    Ok(())
}

#[tokio::test]
async fn a_later_write_at_the_old_version_re_runs_the_migration() -> Result<()> {
    let h = Harness::new();
    let (v_from, v_to) = declare_then_mutate(&h).await?;
    h.install(UP, website_up());
    h.engine.register(migration_def(UP, MigrationDirection::Up));

    let acme = company(200);
    h.push(v_from, vec![(prop(acme, "website"), Value::Int(9))])
        .await?;
    h.engine.catch_up(BRANCH).await?;

    // An old client writes again. The new version's view must follow.
    h.push(v_from, vec![(prop(acme, "website"), Value::Int(4))])
        .await?;
    assert_eq!(h.engine.catch_up(BRANCH).await?, 1);
    assert_eq!(
        h.read(&prop(acme, "website"), v_to).await?.value,
        Some(Value::Int(40)),
        "an old client's write stays visible to new clients"
    );

    // Writing `C@v_to` into the very buffer it consumes `C@v_from` from must not read as a new
    // entity, or the migration would re-trigger itself forever.
    assert!(
        h.engine.is_broken(BRANCH, UP).is_none(),
        "a migration does not mistake its own output for its input"
    );
    Ok(())
}

#[tokio::test]
async fn a_pipeline_at_the_old_version_is_untouched_by_the_migration() -> Result<()> {
    let h = Harness::new();
    let (v_from, _v_to) = declare_then_mutate(&h).await?;
    h.install(UP, website_up());
    h.install(
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
    h.engine.register(migration_def(UP, MigrationDirection::Up));
    h.engine.register(ProducerDef {
        id: SCORE,
        kind: ProducerKind::Pipeline,
        source: BufferId::Object("Company".into()),
        version: v_from.0,
        declaring_repo: RepoId(1),
    });

    let acme = company(300);
    h.push(
        v_from,
        vec![
            (existence(acme), Value::Bool(true)),
            (prop(acme, "website"), Value::Int(9)),
        ],
    )
    .await?;
    h.engine.catch_up(BRANCH).await?;

    // The pipeline read `website@v_from`; the migration wrote `website@v_to`. Same CellRef,
    // different record — so neither may read as a change to the other's input.
    assert_eq!(
        h.engine.catch_up(BRANCH).await?,
        0,
        "everything has settled: neither producer is triggered by the other's output"
    );
    assert_eq!(
        h.read(&prop(acme, "is_investible"), v_from).await?.state,
        Freshness::Current,
        "the old version's pipeline result is not made stale by a migration to the new one"
    );
    Ok(())
}

#[tokio::test]
async fn a_migration_is_skipped_when_no_client_is_live_on_its_target() -> Result<()> {
    let h = Harness::new();
    let (v_from, v_to) = declare_then_mutate(&h).await?;
    h.install(UP, website_up());
    h.engine.register(migration_def(UP, MigrationDirection::Up));
    // Only the old version has clients, so materializing the new one is wasted work (SPEC.md §5.5).
    h.defs.mark_live(v_from);

    let acme = company(400);
    h.push(v_from, vec![(prop(acme, "website"), Value::Int(9))])
        .await?;
    assert_eq!(
        h.engine.catch_up(BRANCH).await?,
        0,
        "no live client on the target version, so nothing is materialized for it"
    );

    // And the reader is told plainly that it is behind rather than handed a wrong answer.
    assert_eq!(
        h.read(&prop(acme, "website"), v_to).await?.state,
        Freshness::Stale
    );
    Ok(())
}

// --- Both directions. SPEC.md §5.4, §9.3 ---------------------------------------------------------

const DOWN: ProducerId = ProducerId(51);

/// Declare, mutate with both directions, and install and register both migrations.
async fn both_directions(h: &Harness) -> Result<(ClientVersion, ClientVersion)> {
    let versions = declare_then_mutate_with(h, Some(DOWN)).await?;
    h.install(UP, website_up());
    h.install(DOWN, website_down());
    h.engine.register(migration_def(UP, MigrationDirection::Up));
    h.engine
        .register(migration_def(DOWN, MigrationDirection::Down));
    Ok(versions)
}

#[tokio::test]
async fn a_new_clients_write_reaches_an_old_client_through_the_down_migration() -> Result<()> {
    let h = Harness::new();
    let (v_from, v_to) = both_directions(&h).await?;

    // Nobody on the old version ever wrote this cell, so without `down` there would be nothing at
    // that version to read — which is exactly what §5.4 says `down` is for.
    let acme = company(500);
    h.push(v_to, vec![(prop(acme, "website"), Value::Int(90))])
        .await?;
    h.engine.catch_up(BRANCH).await?;

    let old = h.read(&prop(acme, "website"), v_from).await?;
    assert_eq!(
        old.value,
        Some(Value::Int(9)),
        "an old client is served the new value through its own lens"
    );
    assert_eq!(
        old.origin,
        Origin::Derived,
        "and told it is a computed view"
    );
    assert_eq!(old.by, Some(DOWN));
    Ok(())
}

#[tokio::test]
async fn up_and_down_are_two_views_of_one_value_rather_than_a_cycle() -> Result<()> {
    let h = Harness::new();
    let (v_from, v_to) = both_directions(&h).await?;

    let acme = company(600);
    h.push(v_from, vec![(prop(acme, "website"), Value::Int(9))])
        .await?;
    h.engine.catch_up(BRANCH).await?;

    // Each writes into the buffer the other reads from, at the version the other reads it at. Left
    // to trigger each other they would run until the cycle detector fired on a configuration that is
    // not a cycle but the ordinary case.
    assert_eq!(
        h.engine.catch_up(BRANCH).await?,
        0,
        "the two directions settle instead of chasing each other"
    );
    assert!(
        h.engine.is_broken(BRANCH, UP).is_none() && h.engine.is_broken(BRANCH, DOWN).is_none(),
        "and neither is poisoned as a cycle"
    );

    // The source value is still the source value: `down` must not overwrite what `up` read.
    let original = h.read(&prop(acme, "website"), v_from).await?;
    assert_eq!(original.value, Some(Value::Int(9)));
    assert_eq!(
        original.origin,
        Origin::Source,
        "a migration adds a version, it does not rewrite one"
    );
    assert_eq!(
        h.read(&prop(acme, "website"), v_to).await?.value,
        Some(Value::Int(90))
    );
    Ok(())
}

#[tokio::test]
async fn a_migration_maps_over_data_that_predates_it() -> Result<()> {
    let h = Harness::new();
    // The def-mutation happens *after* the write, which is the normal order of events: nobody
    // migrates a field before there is anything in it. None of that data was written in a layer the
    // migration's own branch will ever stream, so the layer changeset cannot mention it (§9.6).
    let declared = h
        .defs
        .push(
            BRANCH,
            vec![DefEvent::DeclareField {
                struct_name: "Company".into(),
                field: "website".into(),
                ty: ValueType::Int,
                repo: RepoId(1),
                ownership: Ownership::Source,
            }],
        )
        .await?;
    let v_from = ClientVersion(declared);

    let acme = company(700);
    h.push(v_from, vec![(prop(acme, "website"), Value::Int(9))])
        .await?;

    let mutated = h
        .defs
        .push(
            BRANCH,
            vec![DefEvent::MutateField {
                struct_name: "Company".into(),
                field: "website".into(),
                ty: ValueType::Int,
                repo: RepoId(1),
                up: UP,
                down: None,
            }],
        )
        .await?;
    h.install(UP, website_up());
    h.engine.register(migration_def(UP, MigrationDirection::Up));

    h.engine.catch_up(BRANCH).await?;
    assert_eq!(
        h.read(&prop(acme, "website"), ClientVersion(mutated))
            .await?
            .value,
        Some(Value::Int(90)),
        "a migration owes the values that were already there, not only the ones written after it"
    );
    Ok(())
}

#[tokio::test]
async fn a_migration_bridges_the_versions_its_branch_records() -> Result<()> {
    let h = Harness::new();
    let (v_from, v_to) = declare_then_mutate(&h).await?;

    // The definition records a direction and nothing else. Everything about *which* versions is
    // read back out of the chain the branch folded — which is what lets a def-only merge replay the
    // same event onto a parent whose layer ids are different (SPEC.md §5.3).
    let path = h.layers.read_path(BRANCH, None)?;
    let view = h.defs.view(&path).await?;
    let role = view
        .migration_role(&migration_def(UP, MigrationDirection::Up))
        .expect("the chain names this producer as the field's `up`");
    assert_eq!(role.input, v_from, "up reads the older version");
    assert_eq!(role.output, v_to, "and writes the newer one");
    assert_eq!(
        role.step,
        vec![UP],
        "with no `down` declared, the step is one-sided"
    );

    assert!(
        view.migration_role(&migration_def(DOWN, MigrationDirection::Down))
            .is_none(),
        "a producer the chain does not name has no place in it"
    );
    Ok(())
}
