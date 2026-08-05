//! Derivation as a transaction. SPEC.md §16.5, ROADMAP.md's acceptance scenarios (S7–S10) (S7–S10).
//!
//! A round forks the branch at the top of the range it settles, runs producers on the fork, and
//! merges when it settles. Four claims come out of that, and none of them is checked anywhere else:
//!
//! * **S7** — a chained producer does not trip its own round's guard. *Failing means any round
//!   containing a producer chain never commits.*
//! * **S8** — a stale round cannot land, in either merge order. *Failing means the deleted ordering
//!   rule was necessary after all.*
//! * **S9** — one contended cell does not kill a round. *Failing means one hot cell starves a large
//!   round forever.*
//! * **S10** — a client write landing mid-round produces a **true** watermark. This is the bug the
//!   whole redesign is for, and it is now expected to be structurally impossible: the client's layer
//!   is above the round's fork point and is not in the round's read path at all.
//!
//! The last section is about the *range* rather than about the fork: a round settles
//! `[watermark+1 … head]` (§6.3, §16.5), so a backlog is one round, a producer whose input exists
//! only in a derived layer is discovered at all, and a genuinely concurrent writer still trips the
//! guards of a round that happens to be settling three layers at once.
//!
//! ## Why these are here and not in `scenarios/`
//!
//! S8, S9 and S10 need two writers overlapping in time, and the CLI is process-per-command. Two
//! `borg` processes against one store would each build an `InProcessSequencer` resuming after the
//! highest layer id they saw at open (§17.2), so they would mint the *same* layer id — and the
//! interleaving under test would be swamped by a corruption the test was not about. Concurrency at
//! v1 is in-process by construction, so the test of it is too.
//! `scenarios/160-rounds-are-transactions` covers what the CLI can honestly show.

use borg_core::{
    AllocatorId, BranchId, BufferId, CellRef, ClientVersion, DefEvent, DefVersion, Freshness,
    FreshnessRequirement, LayerAuthor, LayerId, Ownership, Pid, PidKind, ProducerDef, ProducerId,
    ProducerKind, RepoId, Result, Value, ValueType, Writer,
};
use borg_engine::{
    BranchManager, CellTouchIndex, DefRegistry, DerivationEngine, FrontierTracker,
    InProcessSequencer, LayerManager, MemoryDependencyIndex, Resolver, RoundOutcome, WriteSession,
};
use borg_exec::ProducerCtx;
use borg_exec_native::{NativeExecutor, ProducerFn};
use borg_storage::{MemoryStorage, StorageProvider};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

const V1: ClientVersion = ClientVersion(LayerId(1));
const AT_V1: DefVersion = DefVersion(LayerId(1));

/// `headcount` → `is_investible` → `tier`: two hops, so every round here contains a chain.
const INVEST: ProducerId = ProducerId(1);
const TIER: ProducerId = ProducerId(2);

// --- the fixture ------------------------------------------------------------------------------

struct Harness {
    storage: Arc<MemoryStorage>,
    layers: Arc<LayerManager>,
    branches: Arc<BranchManager>,
    defs: Arc<DefRegistry>,
    native: Arc<NativeExecutor>,
    engine: Arc<DerivationEngine>,
    resolver: Resolver,
    branch: BranchId,
}

impl Harness {
    async fn new() -> Result<Self> {
        Self::declaring(vec![
            declare("headcount", ValueType::Int, Ownership::Source),
            declare("is_investible", ValueType::Int, Ownership::Derived(INVEST)),
            declare("tier", ValueType::Int, Ownership::Derived(TIER)),
        ])
        .await
    }

    /// A harness whose branch declares exactly these fields, so a test can push the rest later — the
    /// only honest way to construct *a producer that arrives after the data it consumes*.
    async fn declaring(fields: Vec<DefEvent>) -> Result<Self> {
        let storage = Arc::new(MemoryStorage::new());
        let layers = Arc::new(LayerManager::new(
            storage.clone(),
            Arc::new(InProcessSequencer::new()),
            Arc::new(CellTouchIndex::new()),
        ));
        let branches = Arc::new(BranchManager::new(layers.clone()));
        let defs = Arc::new(DefRegistry::new(layers.clone(), storage.clone()));
        let index = Arc::new(MemoryDependencyIndex::new());
        let native = Arc::new(NativeExecutor::new());
        let engine = Arc::new(DerivationEngine::new(
            storage.clone(),
            layers.clone(),
            index.clone(),
            native.clone(),
            Arc::new(FrontierTracker::new()),
            defs.clone(),
            branches.clone(),
        ));
        let branch = branches.create_root(Some("main".into())).await?;
        defs.push(branch, fields).await?;

        let resolver = Resolver::new(
            storage.clone(),
            index,
            defs.clone(),
            branches.clone(),
            engine.clone(),
        );
        Ok(Self {
            storage,
            layers,
            branches,
            defs,
            native,
            engine,
            resolver,
            branch,
        })
    }

    /// The chain, with the head of it optionally held open by a gate.
    fn install_chain(&self, gate: Option<Arc<Gate>>) {
        self.install(INVEST, "headcount", "is_investible", 2, LayerId(1), gate);
        self.install(TIER, "is_investible", "tier", 3, LayerId(1), None);
    }

    /// One hop of the chain, at the def-version its output field was declared at — which is the
    /// producer's ClientVersion (§5.4), and is only `1` when it was declared with everything else.
    fn install(
        &self,
        id: ProducerId,
        from: &'static str,
        to: &'static str,
        times: i64,
        version: LayerId,
        gate: Option<Arc<Gate>>,
    ) {
        self.native.register(id, hop(from, to, times, gate));
        self.engine.register(ProducerDef {
            id,
            kind: ProducerKind::Pipeline,
            source: BufferId::Object("Company".into()),
            version,
            declaring_repo: RepoId(1),
        });
    }

    async fn push(&self, writes: Vec<(CellRef, Value)>) -> Result<LayerId> {
        let mut session = WriteSession::open(
            &self.layers,
            &self.defs,
            self.branch,
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

    async fn company(&self, n: u64, headcount: i64) -> Result<LayerId> {
        self.push(vec![
            (existence(company(n)), Value::Bool(true)),
            (prop(company(n), "headcount"), Value::Int(headcount)),
        ])
        .await
    }

    async fn read(&self, cell: &CellRef) -> Option<Value> {
        self.read_on(self.branch, cell).await
    }

    async fn read_on(&self, branch: BranchId, cell: &CellRef) -> Option<Value> {
        let path = self.branches.read_path(branch, None).unwrap();
        self.storage
            .get_cell(&path, cell, AT_V1)
            .await
            .unwrap()
            .map(|found| found.event.value)
    }

    /// The source layer a derived cell says it reflects, straight off the record.
    async fn claimed_by(&self, cell: &CellRef) -> Option<LayerId> {
        let path = self.branches.read_path(self.branch, None).unwrap();
        self.storage
            .get_cell(&path, cell, AT_V1)
            .await
            .unwrap()?
            .event
            .derivation
            .map(|by| by.fresh_as_of)
    }

    /// Settle one source layer, in this task.
    async fn settle(&self, layer: LayerId) -> Result<RoundOutcome> {
        self.engine.settle(self.branch, layer).await
    }

    /// Settle one source layer in a task of its own, so the test can write while it runs.
    fn settling(&self, layer: LayerId) -> tokio::task::JoinHandle<Result<RoundOutcome>> {
        let engine = Arc::clone(&self.engine);
        let branch = self.branch;
        tokio::spawn(async move { engine.settle(branch, layer).await })
    }
}

/// Holds a producer inside its run, so a test can construct "while a round is running".
///
/// Polled rather than notified, because a `Notify` waiter that registers after the notification
/// misses it and this gate is opened by a task that cannot know when the producers reached it.
///
/// **Polled on a timer, not with `yield_now`.** A yield hands the worker back to its own local run
/// queue, so under load every worker can end up spinning on a gate that only a task in the global
/// queue can open — which is a hang, not a slow test. A timer parks the worker instead.
#[derive(Default)]
struct Gate {
    entered: AtomicUsize,
    open: AtomicBool,
}

const POLL: std::time::Duration = std::time::Duration::from_micros(100);

impl Gate {
    async fn wait(&self) {
        self.entered.fetch_add(1, Ordering::Release);
        while !self.open.load(Ordering::Acquire) {
            tokio::time::sleep(POLL).await;
        }
    }

    /// Block until a producer is actually inside the gate — so "mid-round" means mid-round rather
    /// than "before the round got going".
    async fn reached(&self) {
        while self.entered.load(Ordering::Acquire) == 0 {
            tokio::time::sleep(POLL).await;
        }
    }

    fn release(&self) {
        self.open.store(true, Ordering::Release);
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

fn company(n: u64) -> Pid {
    Pid::Allocated {
        kind: PidKind::Object,
        branch: BranchId(1),
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

/// A pipeline mapping one Int field onto another, pausing at `gate` between the read and the write.
fn hop(from: &'static str, to: &'static str, times: i64, gate: Option<Arc<Gate>>) -> ProducerFn {
    Arc::new(move |ctx: &mut dyn ProducerCtx, input: Pid| {
        let gate = gate.clone();
        Box::pin(async move {
            let value = ctx.get(&prop(input, from)).await?;
            if let Some(gate) = gate {
                gate.wait().await;
            }
            let Some(Value::Int(n)) = value else {
                // The input is not there yet. Writing nothing is the honest answer; the dependency
                // on the absent cell brings this invocation back round (§16.5).
                return Ok(());
            };
            ctx.set(&prop(input, to), Value::Int(n * times)).await
        })
    })
}

// --- S7 ---------------------------------------------------------------------------------------

/// **S7.** `invest` writes what `tier` reads, and neither may fail the round.
///
/// The guard rule is *cells read and not written*, and for a round the "not written" is round-wide:
/// `tier` read a cell its own round produced, which says nothing about the trunk. Were it guarded,
/// every round containing a producer chain would reject its own second hop — and the failure would
/// look like "the chain never computes" rather than like a guard.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_chained_producer_does_not_trip_its_own_rounds_guard() -> Result<()> {
    let h = Harness::new().await?;
    h.install_chain(None);
    let written = h.company(1, 10).await?;

    let outcome = h.settle(written).await?;

    assert!(
        outcome.rejected.is_empty(),
        "a round with no concurrent writer rejected something: {:?}",
        outcome.rejected
    );
    assert_eq!(outcome.cascaded, 0, "and dropped nothing behind it");
    assert_eq!(
        h.read(&prop(company(1), "is_investible")).await,
        Some(Value::Int(20)),
        "the first hop landed"
    );
    assert_eq!(
        h.read(&prop(company(1), "tier")).await,
        Some(Value::Int(60)),
        "and so did the hop that read it"
    );
    Ok(())
}

// --- S8 ---------------------------------------------------------------------------------------

/// **S8, the stale round attempting first.** Both rounds want to write one cell; the older loses.
///
/// There is no ordering rule and this is why one is not needed: for the L1 round to be in danger at
/// all, L2 must already be on the trunk — otherwise the L2 round could not have forked from it —
/// and its guard on `headcount` therefore fails whichever order the merges are attempted in.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_round_is_rejected_when_it_merges_first() -> Result<()> {
    let h = Harness::new().await?;
    h.install_chain(None);
    let old = h.company(1, 10).await?;
    let new = h
        .push(vec![(prop(company(1), "headcount"), Value::Int(20))])
        .await?;

    let stale = h.settle(old).await?;
    assert!(
        stale.executed > 0,
        "the stale round must actually have computed something, or it proves nothing: without a \
         guard it would now write 20 over a trunk that is about to say 40"
    );
    assert_eq!(
        stale.rejected.len(),
        1,
        "and been rejected — by its own guard, not by an ordering rule"
    );
    assert_eq!(
        stale.rejected[0].1,
        prop(company(1), "headcount"),
        "named by the cell that moved underneath it"
    );
    assert_eq!(stale.applied, 0, "so nothing of it landed");

    h.settle(new).await?;
    assert_eq!(
        h.read(&prop(company(1), "is_investible")).await,
        Some(Value::Int(40)),
        "the fresher round's answer is the one on the branch"
    );
    Ok(())
}

/// **S8, the stale round attempting second.** The same rejection, with the merges the other way up.
///
/// The L1 round is held open inside its producer while the client writes and the L2 round runs to
/// completion. When it is released it has a computed, stale answer in hand and a merge to attempt —
/// which is the interleaving an ordering rule would have been for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_round_is_rejected_when_it_merges_last() -> Result<()> {
    let h = Harness::new().await?;
    let gate = Arc::new(Gate::default());
    h.install_chain(Some(Arc::clone(&gate)));
    let old = h.company(1, 10).await?;

    let stale = h.settling(old);
    gate.reached().await;

    // A client write and a whole fresher round, landing while the first round is still inside a
    // producer. Nothing serialises this: locks are per layer, never per branch (§16.3.4).
    let new = h
        .push(vec![(prop(company(1), "headcount"), Value::Int(20))])
        .await?;
    gate.release();
    let fresh = h.settle(new).await?;
    assert!(fresh.rejected.is_empty(), "the fresher round is unimpeded");

    let stale = stale.await.expect("the stale round did not panic")?;
    assert_eq!(
        stale.rejected.len(),
        1,
        "and the older round is rejected merging second, exactly as it was merging first"
    );
    assert_eq!(stale.applied, 0);
    assert_eq!(
        h.read(&prop(company(1), "is_investible")).await,
        Some(Value::Int(40)),
        "the newer value survives whichever order the two merges are attempted in"
    );
    Ok(())
}

// --- S9 ---------------------------------------------------------------------------------------

/// **S9.** One contended cell costs one invocation, not the round.
///
/// A round is `N` independent computations with no invariant spanning any two of them, so whole-
/// round rejection would let one hot cell starve a 128k-invocation round forever. The invocations
/// whose guards held land; the one that lost recomputes next round.
///
/// The chained hop that consumed the loser's output goes with it — that is the cascade, and it is
/// what keeps the applied subset consistent rather than merely large.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_contended_cell_does_not_kill_a_round() -> Result<()> {
    let h = Harness::new().await?;
    let gate = Arc::new(Gate::default());
    h.install_chain(Some(Arc::clone(&gate)));
    h.company(1, 10).await?;
    let both = h.company(2, 10).await?;

    let round = h.settling(both);
    gate.reached().await;
    let contended = h
        .push(vec![(prop(company(1), "headcount"), Value::Int(50))])
        .await?;
    gate.release();
    let outcome = round.await.expect("the round did not panic")?;

    assert_eq!(
        outcome.rejected.len(),
        1,
        "exactly one invocation lost its cell"
    );
    assert!(
        outcome.cascaded >= 1,
        "and the hop that consumed its output went with it"
    );
    assert_eq!(
        h.read(&prop(company(2), "is_investible")).await,
        Some(Value::Int(20)),
        "the entity nobody contended for landed"
    );
    assert_eq!(
        h.read(&prop(company(2), "tier")).await,
        Some(Value::Int(60)),
        "chain and all"
    );
    assert_eq!(
        h.read(&prop(company(1), "is_investible")).await,
        None,
        "and the contended one was dropped rather than published from a stale read"
    );

    // The dropped invocation is still dirty: its edges were recorded on the trunk when it ran, and
    // the layer that failed its guard is a source layer like any other.
    h.settle(contended).await?;
    assert_eq!(
        h.read(&prop(company(1), "is_investible")).await,
        Some(Value::Int(100)),
        "the next round recomputes what this one dropped"
    );
    assert_eq!(
        h.read(&prop(company(1), "tier")).await,
        Some(Value::Int(300)),
        "and carries the chain the cascade had dropped with it"
    );
    Ok(())
}

/// A deletion landing mid-round drops the invocations that were computing for the deleted object.
///
/// The one thing a round's guards cannot catch by themselves: a producer never probes existence
/// (§8), so nothing in its read-set names the object it writes to, and the client's tombstone
/// touches a cell no guard mentions. Without the check the round would resurrect derived fields on
/// an object that no longer exists — the same failure §13's dangling-write rule exists for,
/// arriving from the other direction.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deletion_landing_mid_round_drops_what_was_computing_for_it() -> Result<()> {
    let h = Harness::new().await?;
    let gate = Arc::new(Gate::default());
    h.install_chain(Some(Arc::clone(&gate)));
    h.company(1, 10).await?;
    let both = h.company(2, 10).await?;

    let round = h.settling(both);
    gate.reached().await;
    h.push(vec![(existence(company(1)), Value::Tombstone)])
        .await?;
    gate.release();
    let outcome = round.await.expect("the round did not panic")?;

    assert!(
        !outcome.rejected.is_empty(),
        "the invocation writing to the deleted object was rejected"
    );
    assert_eq!(
        h.read(&prop(company(1), "is_investible")).await,
        None,
        "nothing was written to a deleted object"
    );
    assert_eq!(
        h.read(&prop(company(2), "is_investible")).await,
        Some(Value::Int(20)),
        "and the object nobody deleted is unaffected"
    );
    Ok(())
}

// --- S10 --------------------------------------------------------------------------------------

/// **S10.** A client write landing mid-round cannot get into that round's output.
///
/// The bug that motivated the whole redesign: a round labelled its output `reflects: L` after
/// reading through a `ReadPath` bound that had climbed above a layer some other writer committed.
/// There is no bound to climb now. The round's fork point *is* L, so a layer committed on the trunk
/// afterwards is not in its read path at all — and the check is the definition of a watermark
/// (§10.1): fork at L, recompute from scratch, compare.
///
/// The mid-round write creates a **new** entity, deliberately. A write to something the round read
/// would be caught by a guard, which proves the guard and not the branch boundary; a write to
/// something it did not read is only kept out by where the fork was taken.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_write_landing_mid_round_produces_a_true_watermark() -> Result<()> {
    let h = Harness::new().await?;
    let gate = Arc::new(Gate::default());
    h.install_chain(Some(Arc::clone(&gate)));
    let settling = h.company(1, 10).await?;

    let round = h.settling(settling);
    gate.reached().await;
    // A whole new entity, appearing on the trunk while the round runs. It did not exist at the
    // layer the round is settling, so nothing the round writes may know about it.
    h.company(2, 30).await?;
    gate.release();
    let outcome = round.await.expect("the round did not panic")?;

    assert!(
        outcome.rejected.is_empty(),
        "the round read nothing the client touched, so it lands whole"
    );
    assert_eq!(
        h.read(&prop(company(2), "is_investible")).await,
        None,
        "and it did not derive an entity that did not exist at its fork point"
    );

    // What the output *claims*, taken from the record rather than from the engine that wrote it.
    let claimed = h.claimed_by(&prop(company(1), "is_investible")).await;
    assert_eq!(
        claimed,
        Some(settling),
        "the round labelled its output with its fork point and nothing higher"
    );

    // The claim, checked the way §10.1 defines it: replay the world at that layer and compare. A
    // fork inherits derived layers by ancestry (§7.4), so `recompute` is what makes this a genuine
    // replay rather than a reading back of the value under test.
    let replay = h
        .branches
        .fork(h.branch, settling, Some("replay".into()))
        .await?;
    h.engine.recompute(replay).await?;

    assert_eq!(
        h.read_on(replay, &prop(company(1), "is_investible")).await,
        h.read(&prop(company(1), "is_investible")).await,
        "replaying the world at the stated watermark reproduces the stated value"
    );
    assert_eq!(
        h.read_on(replay, &prop(company(1), "tier")).await,
        h.read(&prop(company(1), "tier")).await,
        "through the chain as well as at the head of it"
    );
    assert_eq!(
        h.read_on(replay, &prop(company(2), "is_investible")).await,
        None,
        "and that world does not contain the entity the client added mid-round"
    );

    // The label the reader is given is honest in the other direction too: nothing this value depends
    // on moved, so validation is entitled to carry it forward to head (§10.2, §10.3) — which is a
    // claim about `company(1)` and says nothing about the entity it has never heard of.
    let stated = h
        .resolver
        .resolve(
            h.branch,
            &prop(company(1), "is_investible"),
            None,
            V1,
            FreshnessRequirement::Validated,
        )
        .await?;
    assert_eq!(stated.state, Freshness::Current);
    Ok(())
}

// --- Settling a range ---------------------------------------------------------------------------

/// **A backlog settles in one round, not one round per layer.**
///
/// Three writes to one field commit before anything settles. A round per source layer made this a
/// treadmill: the round settling the first computed from a world the second had already moved, and
/// was rejected at merge by its own guard — correctly, because the schedule had guaranteed the work
/// was stale before it ran. Under sustained backlog most derivation work was run and then thrown
/// away.
///
/// One round over `[watermark+1 … head]` has nothing to be stale about. What is asserted is the
/// settled value and the **number of round branches**, because a round forks exactly one (§16.5) and
/// that is the one observable that says "one round" without pinning how many invocations ran inside
/// it — which is precisely what §9.6 leaves to the scheduler.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backlog_settles_as_one_round() -> Result<()> {
    let h = Harness::new().await?;
    h.install_chain(None);

    h.company(1, 10).await?;
    h.push(vec![(prop(company(1), "headcount"), Value::Int(20))])
        .await?;
    let top = h
        .push(vec![(prop(company(1), "headcount"), Value::Int(30))])
        .await?;

    let branches_before = h.branches.all().len();
    h.engine.catch_up(h.branch).await?;

    assert_eq!(
        h.branches.all().len() - branches_before,
        1,
        "three source layers, one fork: the whole backlog settled as a single round"
    );
    assert_eq!(
        h.read(&prop(company(1), "is_investible")).await,
        Some(Value::Int(60)),
        "computed from the newest write, not from the oldest"
    );
    assert_eq!(
        h.read(&prop(company(1), "tier")).await,
        Some(Value::Int(180)),
        "and the chain behind it settled in the same round"
    );

    // What the output claims, and it is the top of the range rather than the layer that happened to
    // dirty the invocation first.
    assert_eq!(
        h.claimed_by(&prop(company(1), "is_investible")).await,
        Some(top),
        "one derived layer per producer per round, reflecting the top of the range"
    );
    let derived: Vec<_> = h
        .layers
        .layers_of(h.branch)
        .into_iter()
        .filter_map(|layer| match layer.author {
            LayerAuthor::Derived { producer, reflects } => Some((producer, reflects)),
            LayerAuthor::Source => None,
        })
        .collect();
    assert_eq!(
        derived.len(),
        2,
        "two producers, two derived layers on the trunk: {derived:?}"
    );
    assert!(
        derived.iter().all(|(_, reflects)| *reflects == top),
        "both reflecting the top of the range: {derived:?}"
    );

    assert_eq!(
        h.engine.catch_up(h.branch).await?,
        0,
        "and nothing is left outstanding — a round does not chase its own merged output"
    );
    Ok(())
}

/// **A pipeline pushed over data that is already derived is discovered by a plain catch-up.**
///
/// The chained-migration bug without a migration in it, and the more likely way to meet it: `tier`
/// is declared and registered *after* `invest` has already produced `is_investible`. Its input was
/// written by a derived layer, and a derived layer opens no round of its own — so while a round
/// settled one source layer at a time, nothing triggered it and §9.6's seeding had nothing to find
/// at the fork point either.
///
/// The range is what finds it: the derived layer `invest`'s round merged is inside
/// `[watermark+1 … head]`, so it is in the opening wave and dirties whoever reads what it wrote.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pipeline_pushed_over_derived_data_is_discovered() -> Result<()> {
    let h = Harness::declaring(vec![
        declare("headcount", ValueType::Int, Ownership::Source),
        declare("is_investible", ValueType::Int, Ownership::Derived(INVEST)),
    ])
    .await?;
    h.install(INVEST, "headcount", "is_investible", 2, LayerId(1), None);

    h.company(1, 10).await?;
    h.engine.catch_up(h.branch).await?;
    assert_eq!(
        h.read(&prop(company(1), "is_investible")).await,
        Some(Value::Int(20)),
        "the first pipeline settled, and its output is in a derived layer"
    );

    // The second pipeline arrives now — declared by a def push, which is how a repo introduces one,
    // and registered against the def-version that declared its output field.
    let declared = h
        .defs
        .push(
            h.branch,
            vec![declare("tier", ValueType::Int, Ownership::Derived(TIER))],
        )
        .await?;
    h.install(TIER, "is_investible", "tier", 3, declared, None);

    h.engine.catch_up(h.branch).await?;
    // Read at the def-version `tier` was declared at: a value is stored at the def-version of its
    // own field, and this field's is the push that introduced it rather than the one everything
    // else here sits at (§5.3).
    let path = h.branches.read_path(h.branch, None)?;
    let tier = h
        .storage
        .get_cell(&path, &prop(company(1), "tier"), DefVersion(declared))
        .await?
        .map(|found| found.event.value);
    assert_eq!(
        tier,
        Some(Value::Int(60)),
        "and it consumes an input that only a derived layer has ever written"
    );
    assert_eq!(
        h.engine.catch_up(h.branch).await?,
        0,
        "settling rather than chasing itself"
    );
    Ok(())
}

/// A backlog round is a round: a client write landing above its fork point still trips its guard,
/// and the cascade still applies.
///
/// The point of the range is that the *schedule* stops manufacturing staleness, not that guards got
/// weaker. A genuinely concurrent writer is still a genuinely concurrent writer, and the invocation
/// that read what it moved is still dropped — with everything in the same round that consumed that
/// invocation's output.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_write_above_a_backlog_rounds_fork_still_trips_its_guard() -> Result<()> {
    let h = Harness::new().await?;
    let gate = Arc::new(Gate::default());
    h.install_chain(Some(Arc::clone(&gate)));

    // The backlog: two entities, then a second layer moving one of them, neither settled.
    h.company(1, 10).await?;
    h.company(2, 10).await?;
    h.push(vec![(prop(company(1), "headcount"), Value::Int(50))])
        .await?;

    let engine = Arc::clone(&h.engine);
    let branch = h.branch;
    let settling = tokio::spawn(async move { engine.catch_up(branch).await });
    gate.reached().await;
    // Above the fork point, because the fork point is the top of the range and this layer is above
    // it. Nothing serialises this: locks are per layer, never per branch (§16.3.4).
    let contended = h
        .push(vec![(prop(company(1), "headcount"), Value::Int(70))])
        .await?;
    gate.release();
    settling.await.expect("the round did not panic")?;

    assert_eq!(
        h.read(&prop(company(1), "is_investible")).await,
        None,
        "the invocation whose input moved underneath it was dropped rather than published"
    );
    assert_eq!(
        h.read(&prop(company(1), "tier")).await,
        None,
        "and the hop that consumed its output cascaded with it"
    );
    assert_eq!(
        h.read(&prop(company(2), "is_investible")).await,
        Some(Value::Int(20)),
        "while the entity nobody contended for landed, backlog and all"
    );

    // The dropped work is still outstanding, and the layer that trod on it is a source layer like
    // any other — so the next range picks it up.
    h.settle(contended).await?;
    assert_eq!(
        h.read(&prop(company(1), "is_investible")).await,
        Some(Value::Int(140)),
        "the next round recomputes what this one dropped, from the value that displaced it"
    );
    assert_eq!(
        h.read(&prop(company(1), "tier")).await,
        Some(Value::Int(420)),
        "chain and all"
    );
    Ok(())
}
