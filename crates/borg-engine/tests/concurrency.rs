//! Derivation with several invocations in flight at once. SPEC.md §16.3, §16.4, §16.5, §17.2.
//!
//! The engine was designed for this and had never run it. Four claims are load-bearing and each one
//! is asserted here rather than assumed:
//!
//! * **Locks are per layer, never per branch** (§16.3.4) — so many producer layers are open at once.
//! * **Single writer per field** (§16.3.1) — so two producers can never target one cell, and their
//!   layers may commit in any order.
//! * **Ordering within a round is not prescribed** (§16.5) — a downstream producer that runs before
//!   its upstream computes from an absent input, and the dependency brings it back round.
//! * **Workers are stateless** (§17.3) — so they multiply freely.
//!
//! A concurrency bug does not reproduce on demand, so the shape of every test here is *run the same
//! scenario many times and assert the settled result is identical*. Milestone C's order-dependence
//! showed up at roughly one run in six; [`RUNS`] is set well above what that needs.
//!
//! **What is asserted is the settled result, never the number of invocations.** How many times a
//! downstream producer runs is exactly what the interleaving decides — it runs once if its upstream
//! committed first and twice if it did not — and pinning that number would be pinning the schedule,
//! which §9.6 says is precisely the thing that may vary.

use borg_core::{
    AllocatorId, BranchId, BufferId, CellRef, ClientVersion, DefEvent, DefVersion, LayerAuthor,
    LayerId, LayerKind, MigrationDirection, Ownership, Pid, PidKind, ProducerDef, ProducerId,
    ProducerKind, RepoId, Result, Value, ValueType, Writer,
};
use borg_engine::{
    BranchManager, CellTouchIndex, DefRegistry, DerivationEngine, FrontierTracker,
    InProcessSequencer, LayerManager, MemoryDependencyIndex, WriteSession,
};
use borg_exec::ProducerCtx;
use borg_exec_native::{NativeExecutor, ProducerFn};
use borg_storage::{MemoryStorage, StorageProvider};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// How many times a scenario is replayed before its determinism is believed.
const RUNS: usize = 120;

/// The degree of parallelism the racing tests force.
///
/// Larger than the machine's core count on purpose: what is being hunted is an interleaving, and
/// oversubscribing the runtime produces more of them per wall-clock second than matching it does.
const RACING: usize = 16;

const BRANCH: BranchId = BranchId(1);
const V1: ClientVersion = ClientVersion(LayerId(1));
/// The def-version every field in these tests sits at. One declaration, one def-layer, nothing
/// mutated since — so this is where the records are keyed, whatever any actor's whole-schema view
/// has moved on to (SPEC.md §5.3).
const AT_V1: DefVersion = DefVersion(LayerId(1));

const FIRST: ProducerId = ProducerId(1);
const SECOND: ProducerId = ProducerId(2);
const THIRD: ProducerId = ProducerId(3);
const LEFT: ProducerId = ProducerId(4);
const RIGHT: ProducerId = ProducerId(5);
const UP: ProducerId = ProducerId(50);
const DOWN: ProducerId = ProducerId(51);

fn company(n: u64) -> Pid {
    Pid::Allocated {
        kind: PidKind::Object,
        branch: BRANCH,
        allocator: AllocatorId(0),
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
    storage: Arc<MemoryStorage>,
    layers: Arc<LayerManager>,
    branches: Arc<BranchManager>,
    defs: Arc<DefRegistry>,
    engine: Arc<DerivationEngine>,
    executor: Arc<NativeExecutor>,
    frontier: Arc<FrontierTracker>,
}

impl Harness {
    fn new(parallelism: usize) -> Self {
        let storage = Arc::new(MemoryStorage::new());
        let layers = Arc::new(LayerManager::new(
            storage.clone(),
            Arc::new(InProcessSequencer::new()),
            Arc::new(CellTouchIndex::new()),
        ));
        let branches = Arc::new(BranchManager::new(layers.clone()));
        let defs = Arc::new(DefRegistry::new(layers.clone(), storage.clone()));
        let executor = Arc::new(NativeExecutor::new());
        let frontier = Arc::new(FrontierTracker::new());
        let engine = Arc::new(
            DerivationEngine::new(
                storage.clone(),
                layers.clone(),
                Arc::new(MemoryDependencyIndex::new()),
                executor.clone(),
                frontier.clone(),
                defs.clone(),
                branches.clone(),
            )
            .with_parallelism(parallelism),
        );
        Self {
            storage,
            layers,
            branches,
            defs,
            engine,
            executor,
            frontier,
        }
    }

    fn install(&self, id: ProducerId, source: BufferId, f: ProducerFn) {
        self.executor.register(id, f);
        self.engine.register(ProducerDef {
            id,
            kind: ProducerKind::Pipeline,
            source,
            version: LayerId(1),
            declaring_repo: RepoId(1),
            fingerprint: None,
        });
    }

    async fn push(&self, version: ClientVersion, writes: Vec<(CellRef, Value)>) -> Result<LayerId> {
        let mut session = WriteSession::open(
            &self.layers,
            &self.defs,
            BRANCH,
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

    async fn read(&self, cell: &CellRef, version: DefVersion) -> Option<Value> {
        let path = self.branches.read_path(BRANCH, None).unwrap();
        self.storage
            .get_cell(&path, cell, version)
            .await
            .unwrap()
            .map(|found| found.event.value)
    }
}

/// A pipeline reading one `Int` field and writing another, with a suspension point between them.
///
/// The `yield_now` is the whole point: it is the widest possible window for a peer to commit
/// between this producer's read and its write, and it is what turns "these tasks might interleave"
/// into "these tasks do interleave" on a single-threaded runtime as well as a multi-threaded one.
fn hop(from: &'static str, to: &'static str, add: i64, runs: Arc<AtomicUsize>) -> ProducerFn {
    Arc::new(move |ctx: &mut dyn ProducerCtx, input: Pid| {
        let runs = Arc::clone(&runs);
        Box::pin(async move {
            runs.fetch_add(1, Ordering::Relaxed);
            let value = ctx.get(&prop(input, from)).await?;
            tokio::task::yield_now().await;
            let Some(Value::Int(n)) = value else {
                // The input is not there yet. Writing nothing is the honest answer, and the
                // dependency on the absent cell is what brings this invocation back round (§16.5).
                return Ok(());
            };
            tokio::task::yield_now().await;
            ctx.set(&prop(input, to), Value::Int(n + add)).await
        })
    })
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

fn chain_schema() -> Vec<DefEvent> {
    vec![
        declare("website", ValueType::Int, Ownership::Source),
        declare("a", ValueType::Int, Ownership::Derived(FIRST)),
        declare("b", ValueType::Int, Ownership::Derived(SECOND)),
        declare("c", ValueType::Int, Ownership::Derived(THIRD)),
        declare("left", ValueType::Int, Ownership::Derived(LEFT)),
        declare("right", ValueType::Int, Ownership::Derived(RIGHT)),
    ]
}

/// `website → a → b → c`, three producers deep, over `n` companies.
async fn chain(parallelism: usize, n: u64) -> Result<Harness> {
    let h = Harness::new(parallelism);
    h.defs.push(BRANCH, chain_schema()).await?;
    let counter = Arc::new(AtomicUsize::new(0));
    let source = BufferId::Object("Company".into());
    h.install(
        FIRST,
        source.clone(),
        hop("website", "a", 1, Arc::clone(&counter)),
    );
    h.install(
        SECOND,
        source.clone(),
        hop("a", "b", 1, Arc::clone(&counter)),
    );
    h.install(THIRD, source, hop("b", "c", 1, counter));

    let mut writes = Vec::new();
    for i in 0..n {
        writes.push((existence(company(i)), Value::Bool(true)));
        writes.push((prop(company(i), "website"), Value::Int(i as i64)));
    }
    h.push(V1, writes).await?;
    Ok(h)
}

/// The claim §16.5 makes, exercised rather than trusted: a downstream producer racing its upstream
/// costs a re-run, not correctness.
///
/// Every producer in the chain is dirtied by the same source layer and they run in one wave, so on
/// most runs at least one of them reads an input its upstream has not committed yet. If the fixpoint
/// did not self-correct, this would settle on a short chain — sometimes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_downstream_producer_racing_its_upstream_settles_on_the_same_values() -> Result<()> {
    for run in 0..RUNS {
        let h = chain(RACING, 8).await?;
        h.engine.catch_up(BRANCH).await?;
        for i in 0..8u64 {
            assert_eq!(
                h.read(&prop(company(i), "c"), AT_V1).await,
                Some(Value::Int(i as i64 + 3)),
                "run {run}: the far end of a three-hop chain is computed from the near end"
            );
        }
    }
    Ok(())
}

/// The same scenario at one invocation at a time and at sixteen must be indistinguishable.
///
/// This is §9.6's licence stated as a test: scheduling policy may change how long a round takes and
/// how many invocations it runs, and may not change what it settles on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallelism_changes_the_schedule_and_not_the_result() -> Result<()> {
    let sequential = chain(1, 24).await?;
    sequential.engine.catch_up(BRANCH).await?;

    for run in 0..RUNS {
        let parallel = chain(RACING, 24).await?;
        parallel.engine.catch_up(BRANCH).await?;
        for i in 0..24u64 {
            for field in ["a", "b", "c"] {
                assert_eq!(
                    parallel.read(&prop(company(i), field), AT_V1).await,
                    sequential.read(&prop(company(i), field), AT_V1).await,
                    "run {run}: {field} on company {i} differs from the sequential engine's answer"
                );
            }
        }
        assert_eq!(
            parallel.frontier.watermark(BRANCH, THIRD),
            sequential.frontier.watermark(BRANCH, THIRD),
            "run {run}: and the round settled on the same source layer"
        );
    }
    Ok(())
}

/// **Single writer per field, under contention** (§16.3.1).
///
/// Two producers writing two fields of the *same* object, from the same input, in the same wave.
/// They cannot collide on a cell — that is what the invariant says — so both writes must survive
/// every interleaving of their two layers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_producers_writing_sibling_fields_of_one_object_both_land() -> Result<()> {
    for run in 0..RUNS {
        let h = Harness::new(RACING);
        h.defs.push(BRANCH, chain_schema()).await?;
        let counter = Arc::new(AtomicUsize::new(0));
        let source = BufferId::Object("Company".into());
        h.install(
            LEFT,
            source.clone(),
            hop("website", "left", 10, Arc::clone(&counter)),
        );
        h.install(RIGHT, source, hop("website", "right", 20, counter));

        let mut writes = Vec::new();
        for i in 0..16u64 {
            writes.push((existence(company(i)), Value::Bool(true)));
            writes.push((prop(company(i), "website"), Value::Int(i as i64)));
        }
        h.push(V1, writes).await?;
        h.engine.catch_up(BRANCH).await?;

        for i in 0..16u64 {
            assert_eq!(
                h.read(&prop(company(i), "left"), AT_V1).await,
                Some(Value::Int(i as i64 + 10)),
                "run {run}: the left sibling survived"
            );
            assert_eq!(
                h.read(&prop(company(i), "right"), AT_V1).await,
                Some(Value::Int(i as i64 + 20)),
                "run {run}: and so did the right one"
            );
        }
    }
    Ok(())
}

/// The interleaving that motivated rounds-as-transactions (a client landing mid-round),
/// constructed rather than waited for. Under the fork model it must be unobservable.
///
/// The upstream producer refuses to commit until the downstream has read the cell it is about to
/// write, so the downstream reads an absent input on *every* run rather than on some of them. The
/// round must still settle on the full chain — by the upstream's layer, in a later wave, finding the
/// downstream subscribed to the cell it wrote.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_upstream_that_always_commits_late_still_reaches_its_downstream() -> Result<()> {
    for run in 0..RUNS {
        let h = Harness::new(RACING);
        h.defs.push(BRANCH, chain_schema()).await?;
        let source = BufferId::Object("Company".into());

        // Raised by the downstream once it has read the cell the upstream writes.
        let read_first = Arc::new(AtomicUsize::new(0));
        // Raised only when that read found nothing — the interleaving actually under test.
        let read_absent = Arc::new(AtomicUsize::new(0));

        let gate = Arc::clone(&read_first);
        h.install(
            FIRST,
            source.clone(),
            Arc::new(move |ctx: &mut dyn ProducerCtx, input: Pid| {
                let gate = Arc::clone(&gate);
                Box::pin(async move {
                    let website = ctx.get(&prop(input, "website")).await?;
                    // **Sleep, not `yield_now`.** A yield hands the worker back to its *local* run
                    // queue, and the downstream this is waiting for may be sitting in the global
                    // one — so under load every worker can end up spinning on a gate only a task
                    // none of them will poll can open. That showed up as this test's own
                    // precondition failing about one run in forty, which is exactly the frequency
                    // this file exists to take seriously. A timer parks the worker and the
                    // downstream gets scheduled.
                    //
                    // Still bounded, so a scheduling policy that puts the downstream in another wave
                    // degrades this to an ordinary run rather than to a hang.
                    for _ in 0..2_000 {
                        if gate.load(Ordering::Acquire) > 0 {
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_micros(100)).await;
                    }
                    let Some(Value::Int(n)) = website else {
                        return Ok(());
                    };
                    ctx.set(&prop(input, "a"), Value::Int(n + 1)).await
                })
            }),
        );

        let gate = Arc::clone(&read_first);
        let missed = Arc::clone(&read_absent);
        h.install(
            SECOND,
            source,
            Arc::new(move |ctx: &mut dyn ProducerCtx, input: Pid| {
                let gate = Arc::clone(&gate);
                let missed = Arc::clone(&missed);
                Box::pin(async move {
                    let upstream = ctx.get(&prop(input, "a")).await?;
                    gate.fetch_add(1, Ordering::Release);
                    let Some(Value::Int(n)) = upstream else {
                        missed.fetch_add(1, Ordering::Release);
                        return Ok(());
                    };
                    ctx.set(&prop(input, "b"), Value::Int(n + 1)).await
                })
            }),
        );

        let mut writes = Vec::new();
        for i in 0..4u64 {
            writes.push((existence(company(i)), Value::Bool(true)));
            writes.push((prop(company(i), "website"), Value::Int(i as i64)));
        }
        h.push(V1, writes).await?;
        h.engine.catch_up(BRANCH).await?;

        assert!(
            read_absent.load(Ordering::Acquire) > 0,
            "run {run}: the downstream never once read ahead of its upstream, so this run proved \
             nothing — the gate did not hold"
        );
        for i in 0..4u64 {
            assert_eq!(
                h.read(&prop(company(i), "b"), AT_V1).await,
                Some(Value::Int(i as i64 + 2)),
                "run {run}: the downstream caught up after reading an absent input"
            );
        }
    }
    Ok(())
}

/// A migration step's two halves are seeded into the same round and now run in the same wave.
///
/// §9.3 says `up` and `down` are two projections of one value: each writes exactly the version the
/// other reads, so if either could observe the other's output the source value would be overwritten
/// by its own round trip. Milestone C made the seeding order-independent; this asserts the same
/// thing when there is no order at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_migration_pair_running_together_does_not_overwrite_its_own_input() -> Result<()> {
    for run in 0..RUNS {
        let h = Harness::new(RACING);
        let declared = h
            .defs
            .push(
                BRANCH,
                vec![declare("website", ValueType::Int, Ownership::Source)],
            )
            .await?;
        let v1 = ClientVersion(declared);

        let mut writes = Vec::new();
        for i in 0..8u64 {
            writes.push((existence(company(i)), Value::Bool(true)));
            writes.push((prop(company(i), "website"), Value::Int(i as i64 + 1)));
        }
        h.push(v1, writes).await?;

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
                    down: Some(DOWN),
                }],
            )
            .await?;

        for (id, direction, factor) in [
            (UP, MigrationDirection::Up, true),
            (DOWN, MigrationDirection::Down, false),
        ] {
            h.executor.register(
                id,
                Arc::new(move |ctx: &mut dyn ProducerCtx, input: Pid| {
                    Box::pin(async move {
                        let old = ctx.get_input(&prop(input, "website")).await?;
                        tokio::task::yield_now().await;
                        let Some(Value::Int(n)) = old else {
                            return Ok(());
                        };
                        let migrated = if factor { n * 10 } else { n / 10 };
                        ctx.set(&prop(input, "website"), Value::Int(migrated)).await
                    })
                }),
            );
            h.engine.register(ProducerDef {
                id,
                kind: ProducerKind::Migration { direction },
                source: BufferId::ObjectProp("Company".into(), "website".into()),
                version: LayerId(0),
                declaring_repo: RepoId(1),
                fingerprint: None,
            });
        }

        h.engine.catch_up(BRANCH).await?;

        for i in 0..8u64 {
            let authored = i as i64 + 1;
            assert_eq!(
                h.read(&prop(company(i), "website"), DefVersion(declared))
                    .await,
                Some(Value::Int(authored)),
                "run {run}: the source value at the old version is untouched"
            );
            assert_eq!(
                h.read(&prop(company(i), "website"), DefVersion(mutated))
                    .await,
                Some(Value::Int(authored * 10)),
                "run {run}: and the new version is what `up` made of it"
            );
        }
    }
    Ok(())
}

/// A client writing while a round settles: under the fork model the write is above the round's
/// fork point and simply not in its read path (§16.5).
///
/// §16.5's ceiling is *"the highest layer that is either ≤ L, or is a derived layer with
/// `reflects == L`"*, and a `ReadPath` bound can only express a prefix of that — so a source layer
/// landing mid-round must stop the ceiling rather than be swept inside it. Whatever the round makes
/// of the interruption, the branch must end up settled on both layers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_write_landing_mid_round_still_settles() -> Result<()> {
    for run in 0..RUNS / 4 {
        let h = chain(RACING, 8).await?;

        let engine = Arc::clone(&h.engine);
        let settling = tokio::spawn(async move { engine.catch_up(BRANCH).await });
        // A second source layer, opened and committed while the first one's consequences are still
        // being derived. Nothing serialises this: locks are per layer, never per branch (§16.3.4).
        h.push(V1, vec![(prop(company(0), "website"), Value::Int(100))])
            .await?;
        settling.await.expect("the round did not panic")?;

        // The interrupted round is allowed to have left work outstanding — that is what a watermark
        // short of head means. What is not allowed is for the work to be unreachable.
        h.engine.catch_up(BRANCH).await?;

        assert_eq!(
            h.read(&prop(company(0), "c"), AT_V1).await,
            Some(Value::Int(103)),
            "run {run}: the chain settled on the value written mid-round"
        );
        for i in 1..8u64 {
            assert_eq!(
                h.read(&prop(company(i), "c"), AT_V1).await,
                Some(Value::Int(i as i64 + 3)),
                "run {run}: and the entities the interruption did not touch are still right"
            );
        }
        for producer in [FIRST, SECOND, THIRD] {
            assert!(
                h.engine.is_broken(BRANCH, producer)?.is_none(),
                "run {run}: nothing was poisoned by the interruption"
            );
        }
    }
    Ok(())
}

/// **The head is a maximum, not the last commit.**
///
/// Ids are assigned at open and order within a branch is established at commit (§7.3), so a layer
/// opened first may land second — which is the ordinary case once invocations run concurrently. The
/// head bounds every read path and every producer's work gap, so a head that walked backwards would
/// hide the layer that overtook it from every subsequent read.
#[tokio::test]
async fn a_layer_committing_out_of_order_does_not_walk_the_head_backwards() -> Result<()> {
    let storage = Arc::new(MemoryStorage::new());
    let layers = Arc::new(LayerManager::new(
        storage,
        Arc::new(InProcessSequencer::new()),
        Arc::new(CellTouchIndex::new()),
    ));

    let first = layers
        .open(BRANCH, LayerKind::Value, LayerAuthor::Source)
        .await?;
    let second = layers
        .open(BRANCH, LayerKind::Value, LayerAuthor::Source)
        .await?;
    assert!(second.id().0 > first.id().0, "ids are assigned at open");

    let later = layers.commit(second).await?;
    layers.commit(first).await?;

    assert_eq!(
        layers.head(BRANCH),
        Some(later),
        "the head is the highest committed layer, not the most recently committed one"
    );
    Ok(())
}
