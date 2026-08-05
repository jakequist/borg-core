//! Poisoned producers, and what a read of their output says. SPEC.md §14, §10.4.
//!
//! §14's claim has two halves and they are usually tested apart: the *scheduler* stops running a
//! producer that threw, and the *reader* is told that is why its value stopped moving. The second
//! half is the one that matters to a client — `stale` promises a catch-up that is coming, and for a
//! poisoned producer nothing is coming — so every test here ends at the read envelope rather than at
//! the engine's own bookkeeping.
//!
//! The engine and the resolver are given the **same** poison table, because they are two views of
//! one fact. The CLI backs that table with a file beside the store; here it is the in-memory
//! implementation, and a resolver built after the fact over the same table stands in for the next
//! process to open the store.

use borg_core::{
    BorgError, BranchId, BufferId, CellRef, ClientVersion, DefEvent, DefVersion, Freshness,
    FreshnessRequirement, LayerAuthor, LayerId, Ownership, Pid, PidKind, ProducerDef, ProducerId,
    ProducerKind, RepoId, Result, Value, ValueType, Writer,
};
use borg_engine::{
    BranchManager, CellTouchIndex, DefRegistry, DependencyIndexProvider, DerivationEngine,
    FrontierTracker, InProcessSequencer, LayerManager, MemoryDependencyIndex, MemoryPoison,
    Resolver, WriteSession,
};
use borg_exec::ProducerCtx;
use borg_exec_native::NativeExecutor;
use borg_storage::{MemoryStorage, StorageProvider};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

const BRANCH: BranchId = BranchId(1);
const SCORE: ProducerId = ProducerId(1);
/// The def-version `headcount` and `risk` sit at. One declaration, nothing mutated since — pushing
/// the producer again moves *its* ClientVersion and leaves the fields where they are (SPEC.md §5.3).
const AT_V1: DefVersion = DefVersion(LayerId(1));

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

fn score_def() -> DefEvent {
    DefEvent::PushProducer(ProducerDef {
        id: SCORE,
        kind: ProducerKind::Pipeline,
        source: BufferId::Object("Company".into()),
        // Overwritten by the fold with the def-layer this lands on, which is exactly what makes
        // pushing it again a new ClientVersion (SPEC.md §9.2).
        version: LayerId(0),
        declaring_repo: RepoId(1),
    })
}

/// A producer that throws on demand, and counts how often it was actually asked to run.
///
/// The counter is what tells "skipped" apart from "ran and failed again" from outside the engine —
/// both leave the producer broken, and only one of them burns the work.
struct Score {
    throwing: Arc<AtomicBool>,
    runs: Arc<AtomicUsize>,
}

impl Score {
    fn new() -> (Self, NativeExecutor) {
        let throwing = Arc::new(AtomicBool::new(false));
        let runs = Arc::new(AtomicUsize::new(0));
        let executor = NativeExecutor::new();
        let (t, r) = (Arc::clone(&throwing), Arc::clone(&runs));
        executor.register(
            SCORE,
            Arc::new(move |ctx: &mut dyn ProducerCtx, input: Pid| {
                let (t, r) = (Arc::clone(&t), Arc::clone(&r));
                Box::pin(async move {
                    r.fetch_add(1, Ordering::Relaxed);
                    if t.load(Ordering::Relaxed) {
                        return Err(BorgError::ProducerFailed {
                            producer: SCORE,
                            message: "no risk model configured".into(),
                        });
                    }
                    let headcount = ctx.get(&prop(input, "headcount")).await?;
                    let risk = match headcount {
                        Some(Value::Int(n)) if n > 10 => 1,
                        _ => 0,
                    };
                    ctx.set(&prop(input, "risk"), Value::Int(risk)).await
                })
            }),
        );
        (Self { throwing, runs }, executor)
    }

    fn throw(&self, yes: bool) {
        self.throwing.store(yes, Ordering::Relaxed);
    }

    fn runs(&self) -> usize {
        self.runs.load(Ordering::Relaxed)
    }
}

struct Harness {
    storage: Arc<MemoryStorage>,
    index: Arc<MemoryDependencyIndex>,
    layers: Arc<LayerManager>,
    branches: Arc<BranchManager>,
    defs: Arc<DefRegistry>,
    engine: Arc<DerivationEngine>,
    resolver: Resolver,
    poison: Arc<MemoryPoison>,
    score: Score,
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
        let poison = Arc::new(MemoryPoison::new());
        let (score, executor) = Score::new();

        let engine = Arc::new(
            DerivationEngine::new(
                storage.clone(),
                layers.clone(),
                index.clone(),
                Arc::new(executor),
                Arc::new(FrontierTracker::new()),
                defs.clone(),
                branches.clone(),
            )
            .with_poison(poison.clone()),
        );

        defs.push(
            BRANCH,
            vec![
                declare("headcount", Ownership::Source),
                declare("risk", Ownership::Derived(SCORE)),
                score_def(),
            ],
        )
        .await?;

        let harness = Self {
            resolver: Resolver::new(
                storage.clone(),
                index.clone(),
                defs.clone(),
                branches.clone(),
                engine.clone(),
            )
            .with_poison(poison.clone()),
            storage,
            index,
            layers,
            branches,
            defs,
            engine,
            poison,
            score,
        };
        harness.register().await?;
        Ok(harness)
    }

    /// Make the engine aware of the producers the branch defines, the way the CLI does — so the
    /// ClientVersion the engine compares a poisoning against is the one the log stamped, not one a
    /// test made up.
    async fn register(&self) -> Result<()> {
        let path = self.branches.read_path(BRANCH, None)?;
        for def in self.defs.view(&path).await?.producers() {
            self.engine.register(def.clone());
        }
        Ok(())
    }

    /// Deploy the producer again: a new def layer, and therefore a new ClientVersion for it. This is
    /// §14's recovery — *fix the producer and push a new ClientVersion* — with the fixing done by
    /// [`Score::throw`].
    async fn redeploy(&self) -> Result<LayerId> {
        let layer = self.defs.push(BRANCH, vec![score_def()]).await?;
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

    async fn read_risk(&self, pid: Pid) -> Result<borg_core::Resolved<Option<Value>>> {
        let path = self.branches.read_path(BRANCH, None)?;
        self.resolver
            .resolve(
                BRANCH,
                &prop(pid, "risk"),
                None,
                ClientVersion(self.defs.head(&path)),
                FreshnessRequirement::Validated,
            )
            .await
    }

    /// A resolver built after the fact over the same store and the same poison table — the next
    /// process to open the store, as far as anything on the read path can tell.
    fn reopened(&self) -> Resolver {
        Resolver::new(
            self.storage.clone(),
            self.index.clone(),
            self.defs.clone(),
            self.branches.clone(),
            self.engine.clone(),
        )
        .with_poison(self.poison.clone())
    }
}

/// Derive once cleanly, then break the producer and disturb its input.
///
/// Every test below starts here, because a cell must exist before a read can say anything about it:
/// a producer that has never succeeded has written nothing, and §10.4's envelope speaks about a
/// stored record.
async fn poisoned() -> Result<(Harness, Pid)> {
    let h = Harness::new().await?;
    let acme = company(100);

    h.set_headcount(acme, 40).await?;
    h.engine.catch_up(BRANCH).await?;
    assert_eq!(
        h.read_risk(acme).await?.state,
        Freshness::Current,
        "the working build derived its field"
    );

    h.score.throw(true);
    h.set_headcount(acme, 5).await?;
    h.engine.catch_up(BRANCH).await?;
    assert!(
        h.engine.is_broken(BRANCH, SCORE)?.is_some(),
        "a producer that threw while a round was settling is poisoned"
    );
    Ok((h, acme))
}

#[tokio::test]
async fn a_poisoned_producers_cells_report_broken_and_not_stale() -> Result<()> {
    let (h, acme) = poisoned().await?;

    let read = h.read_risk(acme).await?;
    assert_eq!(
        read.state,
        Freshness::Broken,
        "the producer threw, so its output is broken — not behind (SPEC.md §14, §10.4)"
    );
    // Broken is a statement about the *label*, not about the value. The last good answer is still
    // served, which is the same bargain §10.4 strikes for every other kind of lag.
    assert_eq!(read.value, Some(Value::Int(1)));
    assert_eq!(read.by, Some(SCORE));

    // Scoped to the producer: source data is untouched and still correct (SPEC.md §14).
    let path = h.branches.read_path(BRANCH, None)?;
    let headcount = h
        .storage
        .get_cell(&path, &prop(acme, "headcount"), AT_V1)
        .await?
        .expect("source data survives a poisoned producer");
    assert_eq!(headcount.event.value, Value::Int(5));
    Ok(())
}

#[tokio::test]
async fn a_resolver_that_never_saw_the_failure_still_reports_broken() -> Result<()> {
    let (h, acme) = poisoned().await?;
    let path = h.branches.read_path(BRANCH, None)?;

    let state = h
        .reopened()
        .resolve(
            BRANCH,
            &prop(acme, "risk"),
            None,
            ClientVersion(h.defs.head(&path)),
            FreshnessRequirement::Validated,
        )
        .await?
        .state;
    assert_eq!(
        state,
        Freshness::Broken,
        "poisoning is read from the table, not remembered by whoever discovered it"
    );
    Ok(())
}

#[tokio::test]
async fn a_broken_cell_is_broken_however_hard_the_reader_asks() -> Result<()> {
    let (h, acme) = poisoned().await?;
    let path = h.branches.read_path(BRANCH, None)?;
    let version = ClientVersion(h.defs.head(&path));

    for requirement in [
        FreshnessRequirement::Any,
        FreshnessRequirement::Validated,
        // `current` computes at the call site — and computing is exactly what a poisoned producer
        // may not do, so the honest answer is the same one.
        FreshnessRequirement::Current,
    ] {
        let read = h
            .resolver
            .resolve(BRANCH, &prop(acme, "risk"), None, version, requirement)
            .await?;
        assert_eq!(
            read.state,
            Freshness::Broken,
            "a poisoned producer is a fact about the producer, not about how far a read validated"
        );
    }
    Ok(())
}

#[tokio::test]
async fn explain_says_why_the_cell_is_broken() -> Result<()> {
    let (h, acme) = poisoned().await?;
    let path = h.branches.read_path(BRANCH, None)?;

    let lineage = h
        .resolver
        .explain(
            BRANCH,
            &prop(acme, "risk"),
            None,
            ClientVersion(h.defs.head(&path)),
        )
        .await?
        .expect("the cell has a stored value to explain");
    let why = lineage.broken.expect("lineage explains why (SPEC.md §14)");
    assert!(
        why.contains("no risk model configured"),
        "the error the producer raised is what lineage reports, not a paraphrase: {why}"
    );
    Ok(())
}

#[tokio::test]
async fn catching_up_skips_a_broken_producer_instead_of_running_it_again() -> Result<()> {
    let (h, acme) = poisoned().await?;
    let before = h.score.runs();

    // More work arrives. A producer that is merely behind would run; a poisoned one must not, or
    // every command re-runs the failure and repeats whatever effects it had.
    h.set_headcount(acme, 7).await?;
    h.engine.catch_up(BRANCH).await?;
    assert_eq!(
        h.score.runs(),
        before,
        "a poisoned producer is skipped, not retried"
    );
    Ok(())
}

#[tokio::test]
async fn pushing_the_producer_again_clears_the_poison_and_recomputes() -> Result<()> {
    let (h, acme) = poisoned().await?;

    // §14's recovery, in the order a human does it: fix the code, push it.
    h.score.throw(false);
    h.redeploy().await?;
    assert!(
        h.engine.is_broken(BRANCH, SCORE)?.is_none(),
        "a poisoning names the ClientVersion it was recorded against, so a new one is not it"
    );

    h.engine.catch_up(BRANCH).await?;
    let read = h.read_risk(acme).await?;
    assert_eq!(read.state, Freshness::Current);
    assert_eq!(
        read.value,
        Some(Value::Int(0)),
        "the input that landed while the producer was broken is what it recomputes from"
    );
    Ok(())
}

#[tokio::test]
async fn a_redeploy_that_is_still_broken_is_poisoned_again() -> Result<()> {
    let (h, acme) = poisoned().await?;

    h.redeploy().await?;
    h.engine.catch_up(BRANCH).await?;
    assert!(
        h.engine.is_broken(BRANCH, SCORE)?.is_some(),
        "clearing the poison is not a claim that the code is fixed — it is a claim that it is new"
    );
    assert_eq!(h.read_risk(acme).await?.state, Freshness::Broken);
    Ok(())
}

#[tokio::test]
async fn retrying_forgets_the_poison_and_redoes_the_work_it_missed() -> Result<()> {
    let (h, acme) = poisoned().await?;
    let before = h.score.runs();

    // The escape hatch for a fix that is not a def push — the producer's own environment changed,
    // or its dependency came back. Nothing about the log moved, so nothing else could clear this.
    h.score.throw(false);
    assert_eq!(h.engine.retry_broken(BRANCH)?, 1);
    h.engine.catch_up(BRANCH).await?;

    assert!(
        h.score.runs() > before,
        "a retry runs the producer again rather than only forgetting the record"
    );
    assert_eq!(
        h.read_risk(acme).await?.state,
        Freshness::Current,
        "and the work it missed while broken is redone, not skipped past"
    );
    Ok(())
}

#[tokio::test]
async fn a_poisoning_is_scoped_to_one_branch() -> Result<()> {
    let (h, acme) = poisoned().await?;
    let head = h.layers.head(BRANCH).expect("the branch has layers");
    let fork = h.branches.fork(BRANCH, head, Some("fix".into())).await?;

    assert!(
        h.engine.is_broken(fork, SCORE)?.is_none(),
        "IllegalState attaches to the producer on the branch it failed on (SPEC.md §14)"
    );
    // The record is keyed by branch, so the fork's own read path is not the poisoned one — even
    // though it inherits the very cell the poisoned producer wrote.
    let path = h.branches.read_path(fork, None)?;
    let state = h
        .resolver
        .resolve(
            fork,
            &prop(acme, "risk"),
            None,
            ClientVersion(h.defs.head(&path)),
            FreshnessRequirement::Validated,
        )
        .await?
        .state;
    assert_ne!(state, Freshness::Broken);
    Ok(())
}

#[tokio::test]
async fn the_dependency_index_is_untouched_by_poisoning() -> Result<()> {
    let (h, acme) = poisoned().await?;
    // The edges recorded by the last successful run are what rediscover the work once the producer
    // comes back. Dropping them on failure would make recovery depend on a buffer scan finding the
    // entity again, which is a different mechanism with different bugs (SPEC.md §16.3).
    let dependents = h.index.dependents(
        BRANCH,
        &[borg_core::CellAt::new(prop(acme, "headcount"), AT_V1)],
    )?;
    assert!(
        dependents.iter().any(|i| i.producer == SCORE),
        "a poisoned producer keeps its dependency edges"
    );
    Ok(())
}
