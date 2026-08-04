//! Fan-out at scale.
//!
//! The normalization thesis (SPEC.md §1) has an honest shadow: identity makes traversal cheap, so
//! data stays normalized, so a single upstream cell can be depended on by an enormous number of
//! invocations. The dependency index is where that bill comes due (SPEC.md §16.3).
//!
//! This fixture is the worst case on purpose — `N` companies all hopping to **one** shared `School`,
//! so flipping it invalidates every one of them.
//!
//! It is also where parallel derivation is measured, because it is the only fixture with real
//! contention: one shared upstream cell, `N` independent invocations, and a dependency index every
//! one of them writes to.
//!
//! Run the large case with:
//!   cargo test --release -p borg-engine --test scale -- --ignored --nocapture

use borg_core::{
    AllocatorId, BranchId, BufferId, CellRef, ClientVersion, DefEvent, LayerAuthor, LayerId,
    Ownership, Pid, PidKind, ProducerDef, ProducerId, ProducerKind, RepoId, Result, Value,
    ValueType, Writer,
};
use borg_engine::{
    BranchManager, CellTouchIndex, DefRegistry, DerivationEngine, FrontierTracker,
    InProcessSequencer, LayerManager, MemoryDependencyIndex, WriteSession,
};
use borg_exec::ProducerCtx;
use borg_exec_native::NativeExecutor;
use borg_storage::MemoryStorage;
use std::sync::Arc;
use std::time::Instant;

const SCORE: ProducerId = ProducerId(1);
const V1: ClientVersion = ClientVersion(LayerId(1));
/// Every company hops to this one school.
const SHARED_SCHOOL: u64 = 1;

fn obj(kind: PidKind, n: u64) -> Pid {
    Pid::Allocated {
        kind,
        branch: BranchId(1),
        allocator: AllocatorId(0),
        counter: n,
    }
}

fn company(n: u64) -> Pid {
    obj(PidKind::Object, 1_000_000 + n)
}
fn founder(n: u64) -> Pid {
    obj(PidKind::Object, 2_000_000 + n)
}
fn education(n: u64) -> Pid {
    obj(PidKind::Object, 3_000_000 + n)
}
fn school(n: u64) -> Pid {
    obj(PidKind::Object, 4_000_000 + n)
}
fn founders_of(n: u64) -> Pid {
    obj(PidKind::List, 5_000_000 + n)
}
fn educations_of(n: u64) -> Pid {
    obj(PidKind::List, 6_000_000 + n)
}

fn prop(struct_name: &str, pid: Pid, field: &str) -> CellRef {
    CellRef::prop(struct_name.into(), field.into(), pid)
}

fn invest_pipeline() -> borg_exec_native::ProducerFn {
    Arc::new(|ctx: &mut dyn ProducerCtx, input: Pid| {
        Box::pin(async move {
            let mut score = 0i64;
            if let Some(Value::Ref(founders)) = ctx.get(&prop("Company", input, "founders")).await?
                && let Some(Value::Int(count)) =
                    ctx.get(&CellRef::list("Founder".into(), founders)).await?
            {
                for i in 0..count as u64 {
                    let Some(Value::Ref(f)) = ctx
                        .get(&CellRef::elem("Founder".into(), founders, i))
                        .await?
                    else {
                        continue;
                    };
                    let Some(Value::Ref(educations)) =
                        ctx.get(&prop("Founder", f, "educations")).await?
                    else {
                        continue;
                    };
                    let Some(Value::Ref(edu)) = ctx
                        .get(&CellRef::elem("Education".into(), educations, 0))
                        .await?
                    else {
                        continue;
                    };
                    let Some(Value::Ref(sch)) = ctx.get(&prop("Education", edu, "school")).await?
                    else {
                        continue;
                    };
                    if let Some(Value::Bool(true)) =
                        ctx.get(&prop("School", sch, "is_top_ten")).await?
                    {
                        score += 3;
                    }
                }
            }
            ctx.set(
                &prop("Company", input, "is_investible"),
                Value::Bool(score > 0),
            )
            .await
        })
    })
}

struct Harness {
    layers: Arc<LayerManager>,
    engine: Arc<DerivationEngine>,
    branch: BranchId,
    defs: Arc<DefRegistry>,
}

impl Harness {
    async fn new(parallelism: usize) -> Self {
        let storage = Arc::new(MemoryStorage::new());
        let layers = Arc::new(LayerManager::new(
            storage.clone(),
            Arc::new(InProcessSequencer::new()),
            Arc::new(CellTouchIndex::new()),
        ));
        let branches = Arc::new(BranchManager::new(layers.clone()));
        let defs = Arc::new(DefRegistry::new(layers.clone(), storage.clone()));
        let executor = Arc::new(NativeExecutor::new());
        executor.register(SCORE, invest_pipeline());

        let engine = Arc::new(
            DerivationEngine::new(
                storage,
                layers.clone(),
                Arc::new(MemoryDependencyIndex::new()),
                executor,
                Arc::new(FrontierTracker::new()),
                defs.clone(),
                branches.clone(),
            )
            .with_parallelism(parallelism),
        );
        engine.register(ProducerDef {
            id: SCORE,
            kind: ProducerKind::Pipeline,
            source: BufferId::Object("Company".into()),
            version: LayerId(1),
            declaring_repo: RepoId(1),
        });

        let branch = branches.create_root(Some("main".into())).await.unwrap();
        defs.push(branch, schema()).await.unwrap();
        Self {
            layers,
            engine,
            branch,
            defs,
        }
    }

    async fn push(&self, writes: Vec<(CellRef, Value)>) -> Result<LayerId> {
        let mut session = WriteSession::open(
            &self.layers,
            &self.defs,
            self.branch,
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
}

/// The four structs this fixture writes, declared before any of it is written (SPEC.md §8).
fn schema() -> Vec<DefEvent> {
    let declare = |struct_name: &str, field: &str, ty: ValueType, ownership: Ownership| {
        DefEvent::DeclareField {
            struct_name: struct_name.into(),
            field: field.into(),
            ty,
            repo: RepoId(1),
            ownership,
        }
    };
    let list_of = |name: &str| ValueType::List(Box::new(ValueType::Object(name.into())));
    vec![
        declare("Company", "founders", list_of("Founder"), Ownership::Source),
        declare(
            "Company",
            "is_investible",
            ValueType::Bool,
            Ownership::Derived(SCORE),
        ),
        declare(
            "Founder",
            "educations",
            list_of("Education"),
            Ownership::Source,
        ),
        declare(
            "Education",
            "school",
            ValueType::Object("School".into()),
            Ownership::Source,
        ),
        declare("School", "is_top_ten", ValueType::Bool, Ownership::Source),
    ]
}

/// `n` companies, every one of them four hops from the same school.
fn fixture(n: u64) -> Vec<(CellRef, Value)> {
    let mut writes = vec![(
        prop("School", school(SHARED_SCHOOL), "is_top_ten"),
        Value::Bool(true),
    )];
    for i in 0..n {
        let (c, f, e) = (company(i), founder(i), education(i));
        let (fl, el) = (founders_of(i), educations_of(i));
        writes.extend([
            (CellRef::existence("Company".into(), c), Value::Bool(true)),
            (prop("Company", c, "founders"), Value::Ref(fl)),
            (CellRef::list("Founder".into(), fl), Value::Int(1)),
            (CellRef::elem("Founder".into(), fl, 0), Value::Ref(f)),
            (prop("Founder", f, "educations"), Value::Ref(el)),
            (CellRef::elem("Education".into(), el, 0), Value::Ref(e)),
            (
                prop("Education", e, "school"),
                Value::Ref(school(SHARED_SCHOOL)),
            ),
        ]);
    }
    writes
}

/// Build `n` companies, derive them, then flip the one shared school and re-derive.
async fn measure(n: u64, parallelism: usize) -> Result<()> {
    let h = Harness::new(parallelism).await;

    let writes = fixture(n);
    let cells = writes.len();
    let start = Instant::now();
    h.push(writes).await?;
    let push = start.elapsed();

    let start = Instant::now();
    let first = h.engine.catch_up(h.branch).await?;
    let derive = start.elapsed();
    assert_eq!(first as u64, n, "one invocation per company");

    // The whole point: one cell, four hops upstream of everything.
    h.push(vec![(
        prop("School", school(SHARED_SCHOOL), "is_top_ten"),
        Value::Bool(false),
    )])
    .await?;

    let start = Instant::now();
    let again = h.engine.catch_up(h.branch).await?;
    let refan = start.elapsed();
    assert_eq!(again as u64, n, "every company recomputes");

    eprintln!(
        "n={n:>6}  p={parallelism:>2}  cells={cells:>7}  push={push:>9.2?}  \
         derive={derive:>9.2?}  fan-out={refan:>9.2?}  ({:>7.0} inv/s)",
        n as f64 / refan.as_secs_f64()
    );
    Ok(())
}

/// The fan-out is the same size however many invocations run at once.
///
/// Scheduling policy cannot affect correctness, only latency (SPEC.md §9.6), and the concrete form
/// of that claim here is that the invocation count is identical: no invocation is lost to a race and
/// none is run twice because two workers found the same work.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fan_out_stays_correct_at_a_modest_size() -> Result<()> {
    // Small enough for the normal suite; guards the behaviour the big runs measure.
    for parallelism in [1, 8] {
        measure(200, parallelism).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement, not a correctness check — run with --release --ignored --nocapture"]
async fn fan_out_scaling_curve() -> Result<()> {
    // `p=1` is the sequential engine, in the same binary: the honest before-and-after for a change
    // whose whole claim is that the bound is a knob.
    for n in [1_000, 8_000, 32_000, 128_000] {
        for parallelism in [1, 2, 4, 8] {
            measure(n, parallelism).await?;
        }
    }
    Ok(())
}
