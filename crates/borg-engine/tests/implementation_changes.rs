//! **A producer whose code changed owes its whole source buffer again.** SPEC.md §9.2.
//!
//! §9.2 says pushing new pipeline source moves the producer's ClientVersion and invalidates all of
//! its prior output. The first half was always true and the second never happened: a ClientVersion
//! is the def-layer a producer was pushed at (§5.3), and nothing compared a producer's watermark
//! with it. So a redeployed producer went on standing at head, claiming to have incorporated
//! everything, over values a different program had written.
//!
//! What is asserted here is the engine's half only — *the ClientVersion moved, therefore recompute*.
//! Whether a given push moves it is `borg repo push`'s question, answered by the implementation
//! fingerprint (see `producer_change` in `borg-cli`, and `scenarios/290-a-code-change-invalidates`
//! for the two joined up). Splitting them is deliberate: the engine must not learn what a
//! fingerprint is, or "a code change" becomes a concept in the scheduler rather than a fact about a
//! definition.

use borg_core::{
    BranchId, BufferId, CellRef, ClientVersion, DefEvent, DefVersion, FreshnessRequirement,
    LayerAuthor, LayerId, Ownership, Pid, PidKind, ProducerDef, ProducerId, ProducerKind, RepoId,
    Result, Value, ValueType, Writer,
};
use borg_engine::{
    BranchManager, CellTouchIndex, DefRegistry, DerivationEngine, FrontierTracker,
    InProcessSequencer, LayerManager, MemoryDependencyIndex, Resolver, WriteSession,
};
use borg_exec::ProducerCtx;
use borg_exec_native::NativeExecutor;
use borg_storage::MemoryStorage;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

const BRANCH: BranchId = BranchId(1);
const BAND: ProducerId = ProducerId(1);

fn company(n: u64) -> Pid {
    Pid::Allocated {
        kind: PidKind::Object,
        branch: BRANCH,
        allocator: borg_core::AllocatorId(0),
        counter: n,
    }
}

fn prop(pid: Pid, field: &str) -> CellRef {
    CellRef::prop("Company".into(), field.into(), pid)
}

fn declare(field: &str, ownership: Ownership) -> DefEvent {
    DefEvent::DeclareField {
        struct_name: "Company".into(),
        field: field.into(),
        ty: ValueType::Int,
        repo: RepoId(1),
        ownership,
    }
}

/// The producer's definition, carrying whatever the repo currently says its code is.
///
/// `version` is a placeholder: the fold stamps the def-layer this lands on, and that stamping is
/// exactly what makes re-pushing a producer a new ClientVersion (§9.2).
fn band_def(fingerprint: Option<&str>) -> DefEvent {
    DefEvent::PushProducer(ProducerDef {
        id: BAND,
        kind: ProducerKind::Pipeline,
        source: BufferId::Object("Company".into()),
        version: LayerId(0),
        declaring_repo: RepoId(1),
        fingerprint: fingerprint.map(str::to_string),
    })
}

/// A pipeline whose behaviour can be changed underneath the values it produced — which is what
/// deploying a new build of the same pipeline *is*.
struct Band {
    multiplier: Arc<AtomicI64>,
}

impl Band {
    fn new() -> (Self, NativeExecutor) {
        let multiplier = Arc::new(AtomicI64::new(1));
        let executor = NativeExecutor::new();
        let m = Arc::clone(&multiplier);
        executor.register(
            BAND,
            Arc::new(move |ctx: &mut dyn ProducerCtx, input: Pid| {
                let m = Arc::clone(&m);
                Box::pin(async move {
                    let headcount = ctx.get(&prop(input, "headcount")).await?;
                    let n = match headcount {
                        Some(Value::Int(n)) => n,
                        _ => 0,
                    };
                    ctx.set(
                        &prop(input, "band"),
                        Value::Int(n * m.load(Ordering::Relaxed)),
                    )
                    .await
                })
            }),
        );
        (Self { multiplier }, executor)
    }

    /// Deploy a different build. Nothing about the *definition* changes here — same id, same source
    /// buffer, same output field — which is the whole reason a fingerprint has to exist.
    fn rebuild(&self, multiplier: i64) {
        self.multiplier.store(multiplier, Ordering::Relaxed);
    }
}

struct Harness {
    layers: Arc<LayerManager>,
    branches: Arc<BranchManager>,
    defs: Arc<DefRegistry>,
    engine: Arc<DerivationEngine>,
    resolver: Resolver,
    band: Band,
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
        let (band, executor) = Band::new();
        let engine = Arc::new(DerivationEngine::new(
            storage.clone(),
            layers.clone(),
            index.clone(),
            Arc::new(executor),
            Arc::new(FrontierTracker::new()),
            defs.clone(),
            branches.clone(),
        ));

        defs.push(
            BRANCH,
            vec![
                declare("headcount", Ownership::Source),
                declare("band", Ownership::Derived(BAND)),
                band_def(Some("sha256:build-one")),
            ],
        )
        .await?;

        let harness = Self {
            resolver: Resolver::new(
                storage,
                index,
                defs.clone(),
                branches.clone(),
                engine.clone(),
            ),
            layers,
            branches,
            defs,
            engine,
            band,
        };
        harness.register().await?;
        Ok(harness)
    }

    /// Make the engine aware of the producers the branch defines, the way the CLI does — so the
    /// ClientVersion it compares against is the one the log stamped, not one a test made up.
    async fn register(&self) -> Result<()> {
        let path = self.branches.read_path(BRANCH, None)?;
        for def in self.defs.view(&path).await?.producers() {
            self.engine.register(def.clone());
        }
        Ok(())
    }

    /// Push the producer's definition again, as `borg repo push` does when the fingerprint moved.
    async fn redeploy(&self, fingerprint: &str) -> Result<LayerId> {
        let layer = self
            .defs
            .push(BRANCH, vec![band_def(Some(fingerprint))])
            .await?;
        self.register().await?;
        Ok(layer)
    }

    async fn set_headcount(&self, pid: Pid, to: i64) -> Result<LayerId> {
        let path = self.branches.read_path(BRANCH, None)?;
        let version = ClientVersion(self.defs.head(&path));
        let mut session = WriteSession::open(
            &self.layers,
            &self.defs,
            BRANCH,
            version,
            Writer::Client,
            LayerAuthor::Source,
        )
        .await?;
        session.set(&prop(pid, "headcount"), Value::Int(to)).await?;
        session.commit().await
    }

    async fn band(&self, pid: Pid) -> Result<Option<i64>> {
        let path = self.branches.read_path(BRANCH, None)?;
        let resolved = self
            .resolver
            .resolve(
                BRANCH,
                &prop(pid, "band"),
                None,
                ClientVersion(self.defs.head(&path)),
                FreshnessRequirement::Validated,
            )
            .await?;
        Ok(match resolved.value {
            Some(Value::Int(n)) => Some(n),
            _ => None,
        })
    }

    /// The producer's ClientVersion as the branch's definitions now stand.
    async fn client_version(&self) -> Result<LayerId> {
        let path = self.branches.read_path(BRANCH, None)?;
        Ok(self
            .defs
            .view(&path)
            .await?
            .producer(BAND)
            .expect("the producer is defined on this branch")
            .version)
    }

    /// The def-version of one entity's output cell — the field's, not the producer's (§5.3).
    async fn field_version(&self, pid: Pid) -> Result<DefVersion> {
        let path = self.branches.read_path(BRANCH, None)?;
        Ok(self.defs.view(&path).await?.version_of(&prop(pid, "band")))
    }
}

/// Three companies with data, derived under the first build.
async fn derived() -> Result<Harness> {
    let h = Harness::new().await?;
    for n in 1..=3 {
        h.set_headcount(company(n), n as i64 * 10).await?;
    }
    h.engine.catch_up(BRANCH).await?;
    for n in 1..=3 {
        assert_eq!(h.band(company(n)).await?, Some(n as i64 * 10));
    }
    Ok(h)
}

#[tokio::test]
async fn pushing_a_producer_again_moves_its_client_version() -> Result<()> {
    let h = Harness::new().await?;
    let before = h.client_version().await?;
    let layer = h.redeploy("sha256:build-two").await?;
    let after = h.client_version().await?;

    assert_ne!(
        before, after,
        "a re-pushed producer stands at a new def-layer"
    );
    assert_eq!(
        after, layer,
        "and that layer is the one the push landed on — a ClientVersion is not a counter (§5.3)"
    );
    Ok(())
}

#[tokio::test]
async fn a_producer_pushed_again_recomputes_every_value_it_had_written() -> Result<()> {
    let h = derived().await?;

    // A different build of the same pipeline. Same producer id, same source buffer, same output
    // field: nothing about the *shape* of anything has moved, which is precisely the case that used
    // to invalidate nothing at all.
    h.band.rebuild(100);
    h.redeploy("sha256:build-two").await?;
    let executed = h.engine.catch_up(BRANCH).await?;

    assert_eq!(
        executed, 3,
        "every entity in the producer's source buffer is work again, not just the ones that moved"
    );
    for n in 1..=3 {
        assert_eq!(
            h.band(company(n)).await?,
            Some(n as i64 * 1000),
            "company {n}'s value was computed by the build that is deployed"
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_producer_that_was_not_pushed_again_recomputes_nothing() -> Result<()> {
    let h = derived().await?;

    // The other half, and the one that makes this affordable. Nothing pushed the producer, so its
    // ClientVersion is where it was and it has genuinely incorporated everything.
    h.band.rebuild(100);
    assert_eq!(
        h.engine.catch_up(BRANCH).await?,
        0,
        "a settled branch is a fixpoint: a build swapped in behind the log's back is invisible"
    );
    for n in 1..=3 {
        assert_eq!(h.band(company(n)).await?, Some(n as i64 * 10));
    }
    Ok(())
}

#[tokio::test]
async fn a_recomputed_producer_settles_rather_than_recomputing_for_ever() -> Result<()> {
    let h = derived().await?;
    h.band.rebuild(2);
    h.redeploy("sha256:build-two").await?;

    assert_eq!(h.engine.catch_up(BRANCH).await?, 3, "the recompute happens");
    assert_eq!(
        h.engine.catch_up(BRANCH).await?,
        0,
        "and then stops: the producer has incorporated the layer that defines it"
    );
    Ok(())
}

/// A def layer that moves *some other* producer must not drag this one through its buffer. The
/// per-producer ClientVersion is what makes that true, and it is easy to lose by comparing against
/// the branch's def-head instead.
#[tokio::test]
async fn a_def_push_that_leaves_a_producer_alone_recomputes_nothing_of_its() -> Result<()> {
    let h = derived().await?;
    h.band.rebuild(100);

    h.defs
        .push(BRANCH, vec![declare("city", Ownership::Source)])
        .await?;
    h.register().await?;

    assert_eq!(
        h.engine.catch_up(BRANCH).await?,
        0,
        "declaring an unrelated field is not a new ClientVersion for this producer"
    );
    for n in 1..=3 {
        assert_eq!(h.band(company(n)).await?, Some(n as i64 * 10));
    }
    Ok(())
}

/// A producer's stored values are keyed by the field's def-version, not the producer's — so a
/// recompute overwrites what was there rather than laying a second version beside it (§5.3, §5.4).
#[tokio::test]
async fn a_recompute_overwrites_rather_than_versioning_the_field() -> Result<()> {
    let h = derived().await?;
    let at = h.field_version(company(1)).await?;

    h.band.rebuild(7);
    h.redeploy("sha256:build-two").await?;
    h.engine.catch_up(BRANCH).await?;

    let after = h.field_version(company(1)).await?;
    assert_eq!(
        at, after,
        "the field's def-version is untouched: a fingerprint change is not a schema change"
    );
    assert_eq!(h.band(company(1)).await?, Some(70));
    Ok(())
}
