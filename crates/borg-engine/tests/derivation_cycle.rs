//! The derivation cycle, end to end. SPEC.md §9, §16.
//!
//! This is the acceptance test for the only genuinely unproven part of the design: that a write can
//! invalidate exactly the right derived data, at field granularity, through hops, with nothing
//! queued anywhere.

use borg_core::{
    BranchId, BufferId, CellAt, CellRecord, CellRef, ClientVersion, LayerAuthor, LayerId,
    LayerKind, Origin, Pid, PidKind, ProducerDef, ProducerId, ProducerKind, RepoId, Result, Value,
};
use borg_engine::{
    BranchManager, CellTouchIndex, DefRegistry, DerivationEngine, FrontierTracker,
    InProcessSequencer, LayerManager, MemoryDependencyIndex,
};
use borg_exec::ProducerCtx;
use borg_exec_native::NativeExecutor;
use borg_storage::{MemoryStorage, StorageProvider};
use std::sync::Arc;

const BRANCH: BranchId = BranchId(1);
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
    storage: Arc<MemoryStorage>,
    branches: Arc<BranchManager>,
    layers: Arc<LayerManager>,
    engine: Arc<DerivationEngine>,
    frontier: Arc<FrontierTracker>,
}

impl Harness {
    fn new(executor: NativeExecutor) -> Self {
        let storage = Arc::new(MemoryStorage::new());
        let layers = Arc::new(LayerManager::new(
            storage.clone(),
            Arc::new(InProcessSequencer::new()),
            Arc::new(CellTouchIndex::new()),
        ));
        let branches = Arc::new(BranchManager::new(layers.clone()));
        let frontier = Arc::new(FrontierTracker::new());
        let engine = Arc::new(DerivationEngine::new(
            storage.clone(),
            layers.clone(),
            Arc::new(MemoryDependencyIndex::new()),
            Arc::new(executor),
            frontier.clone(),
            Arc::new(DefRegistry::new(layers.clone(), storage.clone())),
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
            storage,
            branches,
            layers,
            engine,
            frontier,
        }
    }

    /// Commit a source layer holding the given writes.
    async fn push(&self, writes: Vec<(CellRef, Value)>) -> Result<LayerId> {
        let mut layer = self
            .layers
            .open(BRANCH, LayerKind::Value, LayerAuthor::Source)
            .await?;
        for (cell, value) in writes {
            let record = CellRecord {
                value,
                version: ClientVersion(LayerId(1)),
                written_at: layer.id(),
                origin: Origin::Source,
                derivation: None,
            };
            layer.put(&cell, record).await?;
        }
        self.layers.commit(layer).await
    }

    async fn read(&self, cell: &CellRef) -> Option<CellRecord> {
        let head = self.layers.head(BRANCH).unwrap();
        let path = self.branches.read_path(BRANCH, Some(head)).unwrap();
        self.storage
            .get_cell(&path, cell, ClientVersion(LayerId(1)))
            .await
            .unwrap()
    }
}

/// A producer reading exactly one field and writing exactly one other.
fn score_producer() -> NativeExecutor {
    let executor = NativeExecutor::new();
    executor.register(
        SCORE,
        Arc::new(|ctx: &mut dyn ProducerCtx, input: Pid| {
            Box::pin(async move {
                let website = ctx.get(&prop(input, "website")).await?;
                let investible = matches!(website, Some(Value::Int(len)) if len > 3);
                ctx.set(&prop(input, "is_investible"), Value::Bool(investible))
                    .await
            })
        }),
    );
    executor
}

#[tokio::test]
async fn derives_on_create_and_tracks_at_field_granularity() -> Result<()> {
    let h = Harness::new(score_producer());
    let acme = company(100);

    // A new entity appears in the producer's source buffer.
    h.push(vec![
        (existence(acme), Value::Bool(true)),
        (prop(acme, "website"), Value::Int(9)),
    ])
    .await?;

    assert_eq!(
        h.engine.catch_up(BRANCH).await?,
        1,
        "one invocation for one new entity"
    );
    let derived = h
        .read(&prop(acme, "is_investible"))
        .await
        .expect("derived cell");
    assert_eq!(derived.value, Value::Bool(true));
    assert_eq!(derived.origin, Origin::Derived);

    let derivation = derived.derivation.expect("derived cells carry provenance");
    assert_eq!(derivation.producer, SCORE);
    assert_eq!(
        derivation.read_set,
        vec![CellAt::new(
            prop(acme, "website"),
            ClientVersion(LayerId(1))
        )],
        "the read-set is captured automatically, at the version read, and contains only what was \
         actually read"
    );

    // A write to a field nobody read must trigger nothing. This is the whole point of field-level
    // tracking (SPEC.md §9.4).
    h.push(vec![(prop(acme, "name"), Value::Int(1))]).await?;
    assert_eq!(
        h.engine.catch_up(BRANCH).await?,
        0,
        "writing an undepended field recomputes nothing"
    );

    // A write to a field the producer *did* read must recompute it.
    let last_source = h.push(vec![(prop(acme, "website"), Value::Int(2))]).await?;
    assert_eq!(
        h.engine.catch_up(BRANCH).await?,
        1,
        "writing a depended field recomputes exactly one invocation"
    );
    assert_eq!(
        h.read(&prop(acme, "is_investible")).await.unwrap().value,
        Value::Bool(false),
        "the recomputed value reflects the new input"
    );

    // The watermark is a pointer into the *source* stream (SPEC.md §6.3), so it settles on the last
    // source layer — not on head, which by now is the derived layer the recompute produced.
    assert_eq!(
        h.frontier.watermark(BRANCH, SCORE),
        last_source,
        "the watermark tracks source layers, not derived ones"
    );
    assert!(
        h.layers.head(BRANCH).unwrap().0 > last_source.0,
        "head has moved past it, because derivation committed a derived layer"
    );
    Ok(())
}

#[tokio::test]
async fn a_producer_reading_another_producers_output_is_triggered_by_it() -> Result<()> {
    // The case that motivated running a source layer's consequences to a fixpoint: B depends on A's
    // output, so B can only be triggered by A's *derived* layer.
    const DOWNSTREAM: ProducerId = ProducerId(9);

    let executor = NativeExecutor::new();
    executor.register(
        SCORE,
        Arc::new(|ctx: &mut dyn ProducerCtx, input: Pid| {
            Box::pin(async move {
                let website = ctx.get(&prop(input, "website")).await?;
                let investible = matches!(website, Some(Value::Int(len)) if len > 3);
                ctx.set(&prop(input, "is_investible"), Value::Bool(investible))
                    .await
            })
        }),
    );
    executor.register(
        DOWNSTREAM,
        Arc::new(|ctx: &mut dyn ProducerCtx, input: Pid| {
            Box::pin(async move {
                let investible = ctx.get(&prop(input, "is_investible")).await?;
                let tier = match investible {
                    Some(Value::Bool(true)) => 1,
                    _ => 3,
                };
                ctx.set(&prop(input, "tier"), Value::Int(tier)).await
            })
        }),
    );

    let h = Harness::new(executor);
    h.engine.register(ProducerDef {
        id: DOWNSTREAM,
        kind: ProducerKind::Pipeline,
        source: BufferId::Object("Company".into()),
        version: LayerId(1),
        declaring_repo: RepoId(1),
    });

    let acme = company(500);
    h.push(vec![
        (existence(acme), Value::Bool(true)),
        (prop(acme, "website"), Value::Int(9)),
    ])
    .await?;
    h.engine.catch_up(BRANCH).await?;

    assert_eq!(
        h.read(&prop(acme, "tier"))
            .await
            .expect("downstream ran")
            .value,
        Value::Int(1),
        "the downstream producer saw the upstream producer's output"
    );

    // And a change at the head of the chain must propagate all the way down.
    h.push(vec![(prop(acme, "website"), Value::Int(1))]).await?;
    h.engine.catch_up(BRANCH).await?;
    assert_eq!(
        h.read(&prop(acme, "tier")).await.unwrap().value,
        Value::Int(3),
        "a source change propagates through the whole derivation chain"
    );
    Ok(())
}

#[tokio::test]
async fn absence_is_a_tracked_dependency() -> Result<()> {
    let h = Harness::new(score_producer());
    let acme = company(200);

    // Created with no website at all: the producer reads nothing and must still depend on it.
    h.push(vec![(existence(acme), Value::Bool(true))]).await?;
    h.engine.catch_up(BRANCH).await?;
    assert_eq!(
        h.read(&prop(acme, "is_investible")).await.unwrap().value,
        Value::Bool(false)
    );

    // Filling in the previously-absent field must invalidate, or absence was not really tracked.
    h.push(vec![(prop(acme, "website"), Value::Int(9))]).await?;
    assert_eq!(
        h.engine.catch_up(BRANCH).await?,
        1,
        "a read that found nothing is still a dependency"
    );
    assert_eq!(
        h.read(&prop(acme, "is_investible")).await.unwrap().value,
        Value::Bool(true)
    );
    Ok(())
}

#[tokio::test]
async fn a_cycling_producer_poisons_itself_and_not_the_branch() -> Result<()> {
    // Reads the very field it writes — the definition of a cycle, and undetectable statically.
    let executor = NativeExecutor::new();
    executor.register(
        SCORE,
        Arc::new(|ctx: &mut dyn ProducerCtx, input: Pid| {
            Box::pin(async move {
                let previous = ctx.get(&prop(input, "counter")).await?;
                let next = match previous {
                    Some(Value::Int(n)) => n + 1,
                    _ => 0,
                };
                ctx.set(&prop(input, "counter"), Value::Int(next)).await
            })
        }),
    );

    let h = Harness::new(executor);
    let acme = company(300);
    h.push(vec![(existence(acme), Value::Bool(true))]).await?;
    h.engine.catch_up(BRANCH).await?;

    assert!(
        h.engine.is_broken(BRANCH, SCORE).is_some(),
        "a cycling producer is detected and poisoned"
    );

    // Source data is untouched and still readable — the failure is scoped to the producer, so main
    // never breaks because someone shipped a bad pipeline (SPEC.md §14).
    assert_eq!(
        h.read(&existence(acme)).await.unwrap().value,
        Value::Bool(true),
        "source data survives a poisoned producer"
    );
    Ok(())
}

#[tokio::test]
async fn a_second_writer_to_one_field_is_rejected() -> Result<()> {
    const OTHER: ProducerId = ProducerId(2);

    fn write_investible(
        ctx: &mut dyn ProducerCtx,
        input: Pid,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            ctx.set(&prop(input, "is_investible"), Value::Bool(true))
                .await
        })
    }

    let executor = NativeExecutor::new();
    executor.register(SCORE, Arc::new(write_investible));
    executor.register(OTHER, Arc::new(write_investible));

    let h = Harness::new(executor);
    h.engine.register(ProducerDef {
        id: OTHER,
        kind: ProducerKind::Pipeline,
        source: BufferId::Object("Company".into()),
        version: LayerId(1),
        declaring_repo: RepoId(2),
    });

    let acme = company(400);
    h.push(vec![(existence(acme), Value::Bool(true))]).await?;
    h.engine.catch_up(BRANCH).await?;

    // Every field has exactly one writer. Whichever producer claimed it first, the other is poisoned
    // and the field keeps its owner.
    let broken = [SCORE, OTHER]
        .into_iter()
        .filter(|p| h.engine.is_broken(BRANCH, *p).is_some())
        .count();
    assert_eq!(
        broken, 1,
        "exactly one of two competing writers is poisoned"
    );
    Ok(())
}
