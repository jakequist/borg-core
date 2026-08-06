//! Features that have never met. ROADMAP.md's acceptance scenarios (S13, S14).
//!
//! Every major feature in this project works alone. The bugs cluster where two of them meet for the
//! first time, and this file is the two meetings that need a **mid-round** interleaving:
//!
//! * **S13** — a migration round and a concurrent client write. *Migrations and concurrency have
//!   never been exercised together.* The client write lands on the trunk while the migration is
//!   inside `get_input`, in both merge orders.
//! * **S14** — a def-only merge landing on the trunk while a round is computing there under the
//!   def-view as it stood before. The round's output is labelled at a def-version that stopped being
//!   current halfway through producing it.
//!
//! ## Why these are here and not (only) in `scenarios/`
//!
//! For the same reason S8–S10 are (`rounds.rs`): the CLI is process-per-command and layer ids come
//! from a process-local sequencer (§17.2), so two `borg` processes against one store mint the same
//! layer id and the interleaving under test is swamped by a corruption that has nothing to do with
//! it. `scenarios/170-migration-under-a-concurrent-write` and
//! `scenarios/180-a-def-merge-during-a-round` drive the same feature pairs end to end through the
//! real binary, in the orders one process at a time can express honestly.
//!
//! ## What S13 found
//!
//! A migration is the **only** producer whose output shares a `CellRef` with a cell clients write —
//! `website@v1` is source, `website@v9` is its output, and they differ only in the def-version.
//! `Round::guards` subtracted the round's writes from its reads keyed on `CellRef`, so a migration's
//! guard on the very cell it migrated *from* was deleted, and a stale migration round could land
//! over a fresher one. See `ROADMAP.md`, *A round's guard subtraction was keyed on `CellRef`*.

use borg_core::{
    AllocatorId, BranchId, BufferId, CellRef, ClientVersion, DefEvent, DefVersion, Freshness,
    FreshnessRequirement, LayerAuthor, LayerId, MergeMode, MigrationDirection, Origin, Ownership,
    Pid, PidKind, ProducerDef, ProducerId, ProducerKind, RepoId, Result, Value, ValueType, Writer,
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

const BRANCH: BranchId = BranchId(1);
/// `up` for `Company.website`, and the second `up` a second schema change appoints in S14.
const UP: ProducerId = ProducerId(50);
const UP2: ProducerId = ProducerId(51);

fn company(n: u64) -> Pid {
    Pid::Allocated {
        kind: PidKind::Object,
        branch: BRANCH,
        allocator: AllocatorId(0),
        counter: n,
    }
}

fn prop(pid: Pid, field: &str) -> CellRef {
    CellRef::prop("Company".into(), field.into(), pid)
}

// --- the fixture --------------------------------------------------------------------------------

struct Harness {
    storage: Arc<MemoryStorage>,
    layers: Arc<LayerManager>,
    branches: Arc<BranchManager>,
    defs: Arc<DefRegistry>,
    engine: Arc<DerivationEngine>,
    executor: Arc<NativeExecutor>,
    resolver: Resolver,
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
        let engine = Arc::new(DerivationEngine::new(
            storage.clone(),
            layers.clone(),
            index.clone(),
            executor.clone(),
            Arc::new(FrontierTracker::new()),
            defs.clone(),
            branches.clone(),
        ));
        let resolver = Resolver::new(
            storage.clone(),
            index,
            defs.clone(),
            branches.clone(),
            engine.clone(),
        );
        Self {
            storage,
            layers,
            branches,
            defs,
            engine,
            executor,
            resolver,
        }
    }

    /// A client write at an explicit ClientVersion — which for a migration test is the whole point:
    /// an old client goes on writing the old shape (§5.4).
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

    /// The stored record at one def-version, straight off storage rather than through the resolver:
    /// what is asserted below is *which version a value was filed under*, and the resolver's job is
    /// to hide exactly that.
    async fn stored_at(&self, cell: &CellRef, version: DefVersion) -> Option<Value> {
        let path = self.branches.read_path(BRANCH, None).unwrap();
        self.storage
            .get_cell(&path, cell, version)
            .await
            .unwrap()
            .map(|found| found.event.value)
    }

    async fn read(
        &self,
        cell: &CellRef,
        version: ClientVersion,
    ) -> Result<borg_core::Resolved<Option<Value>>> {
        self.resolver
            .resolve(BRANCH, cell, None, version, FreshnessRequirement::Validated)
            .await
    }

    async fn settle(&self, layer: LayerId) -> Result<RoundOutcome> {
        self.engine.settle(BRANCH, layer).await
    }

    /// Settle one source layer in a task of its own, so the test can write while it runs.
    fn settling(&self, layer: LayerId) -> tokio::task::JoinHandle<Result<RoundOutcome>> {
        let engine = Arc::clone(&self.engine);
        tokio::spawn(async move { engine.settle(BRANCH, layer).await })
    }

    fn install_up(&self, id: ProducerId, f: ProducerFn) {
        self.executor.register(id, f);
        self.engine.register(ProducerDef {
            id,
            kind: ProducerKind::Migration {
                direction: MigrationDirection::Up,
            },
            // A migration maps over the *field's* buffer: it is defined per output field (§9.3).
            source: BufferId::ObjectProp("Company".into(), "website".into()),
            version: LayerId(0),
            declaring_repo: RepoId(1),
            fingerprint: None,
        });
    }
}

/// Holds a producer inside its run, so a test can construct "while a round is running".
///
/// Polled on a timer rather than notified, and for the reason `rounds.rs` records: a `Notify` waiter
/// that registers after the notification misses it, and `yield_now` hands the worker back to its own
/// local run queue, so under load every worker can spin on a gate only a task in the global queue
/// can open.
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

/// `up` for `Company.website`: the old version times ten, optionally pausing between the read and
/// the write.
///
/// `get_input` is the one place a migration departs from an ordinary producer: an ordinary `get`
/// resolves at the producer's own ClientVersion, which for `up` is the version it *writes*, so it
/// would recurse into the value it is producing (§9.3).
fn website_up(times: i64, gate: Option<Arc<Gate>>) -> ProducerFn {
    Arc::new(move |ctx: &mut dyn ProducerCtx, input: Pid| {
        let gate = gate.clone();
        Box::pin(async move {
            let old = ctx.get_input(&prop(input, "website")).await?;
            if let Some(gate) = gate {
                gate.wait().await;
            }
            let Some(Value::Int(n)) = old else {
                // Absent input, nothing to say — and the negative read is still recorded, so a
                // later write at the old version brings this invocation back round (§9.4).
                return Ok(());
            };
            ctx.set(&prop(input, "website"), Value::Int(n * times))
                .await
        })
    })
}

fn declare_website() -> DefEvent {
    DefEvent::DeclareField {
        struct_name: "Company".into(),
        field: "website".into(),
        ty: ValueType::Int,
        repo: RepoId(1),
        ownership: Ownership::Source,
    }
}

fn mutate_website(up: ProducerId) -> DefEvent {
    DefEvent::MutateField {
        struct_name: "Company".into(),
        field: "website".into(),
        ty: ValueType::Int,
        repo: RepoId(1),
        up,
        down: None,
    }
}

/// Declare `Company.website`, then mutate it. The two def-layer ids *are* the two def-versions
/// (§5.3), so they are what everything below is keyed at.
async fn declare_then_mutate(h: &Harness) -> Result<(ClientVersion, ClientVersion)> {
    let declared = h.defs.push(BRANCH, vec![declare_website()]).await?;
    let mutated = h.defs.push(BRANCH, vec![mutate_website(UP)]).await?;
    Ok((ClientVersion(declared), ClientVersion(mutated)))
}

// --- S13: a migration round under a concurrent client write --------------------------------------

/// **S13, the stale round attempting first.** A migration round is a round, and a stale one may not
/// land.
///
/// This is S8 asked of a migration, and it is the composition that broke: `rounds.rs` proves a stale
/// *pipeline* round is rejected, and the same shape with `up` in place of the pipeline was applied
/// happily. A migration is the one producer whose output shares a `CellRef` with a cell clients
/// write — the guard subtraction was keyed on `CellRef`, so a migration deleted its own guard on the
/// very cell it migrates from.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_migration_round_is_rejected_when_it_merges_first() -> Result<()> {
    let h = Harness::new();
    let (v_from, v_to) = declare_then_mutate(&h).await?;
    h.install_up(UP, website_up(10, None));

    let acme = company(100);
    let old = h
        .push(v_from, vec![(prop(acme, "website"), Value::Int(9))])
        .await?;
    // A second client write, at the old version like the first: an old client goes on writing the
    // old shape after the schema moves, which is §5.4's whole promise and is why this is the
    // realistic shape of a write racing a migration.
    let new = h
        .push(v_from, vec![(prop(acme, "website"), Value::Int(7))])
        .await?;

    let stale = h.settle(old).await?;
    assert!(
        stale.executed > 0,
        "the stale round must actually have run the migration, or it proves nothing: without a \
         guard it is now holding 90 for a cell the trunk says is 7"
    );
    assert_eq!(
        stale.rejected.len(),
        1,
        "and been rejected — by its own guard on the cell it migrated from"
    );
    assert_eq!(
        stale.rejected[0].1,
        prop(acme, "website"),
        "named by the cell that moved underneath it"
    );
    assert_eq!(stale.applied, 0, "so nothing of it landed");
    assert_eq!(
        h.stored_at(&prop(acme, "website"), DefVersion(v_to.0))
            .await,
        None,
        "the migrated view is absent rather than wrong: nothing has legitimately produced it yet"
    );

    h.settle(new).await?;
    assert_eq!(
        h.stored_at(&prop(acme, "website"), DefVersion(v_to.0))
            .await,
        Some(Value::Int(70)),
        "and the round that forked above the client's write migrates what the client actually wrote"
    );
    Ok(())
}

/// **S13, the stale round attempting last.** The same rejection with the merges the other way up,
/// and this is the order where the missing guard was a **lost update** rather than a near miss.
///
/// The migration is held inside its own run while the client writes and a fresher round settles that
/// write to completion. Released, it has a computed, stale answer in hand and a merge to attempt.
/// With the guard deleted it won, and the branch was then permanently wrong *and quiet*: the
/// migrated view read `stale` for ever, and `catch_up` reported nothing outstanding, because every
/// watermark had already advanced past the layer that would have corrected it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_migration_round_is_rejected_when_it_merges_last() -> Result<()> {
    let h = Harness::new();
    let (v_from, v_to) = declare_then_mutate(&h).await?;
    let gate = Arc::new(Gate::default());
    h.install_up(UP, website_up(10, Some(Arc::clone(&gate))));

    let acme = company(100);
    let old = h
        .push(v_from, vec![(prop(acme, "website"), Value::Int(9))])
        .await?;

    let stale = h.settling(old);
    gate.reached().await;

    // A client write and a whole fresher round, landing while the migration is still inside its own
    // invocation. Nothing serialises this: locks are per layer, never per branch (§16.3.4).
    let new = h
        .push(v_from, vec![(prop(acme, "website"), Value::Int(7))])
        .await?;
    gate.release();
    let fresh = h.settle(new).await?;
    assert!(
        fresh.rejected.is_empty(),
        "the fresher round is unimpeded: nothing landed above its own fork point"
    );

    let stale = stale.await.expect("the stale round did not panic")?;
    assert_eq!(
        stale.rejected.len(),
        1,
        "the older round is rejected merging second, exactly as it was merging first"
    );
    assert_eq!(stale.applied, 0);

    let migrated = h.read(&prop(acme, "website"), v_to).await?;
    assert_eq!(
        migrated.value,
        Some(Value::Int(70)),
        "the migrated view is a projection of the value that is actually there"
    );
    assert_eq!(
        migrated.state,
        Freshness::Current,
        "and it says current, because it genuinely is — the alternative was a value that read \
         stale for ever with no work outstanding to fix it"
    );
    assert_eq!(
        h.engine.catch_up(BRANCH).await?,
        0,
        "nothing is left outstanding"
    );
    Ok(())
}

/// A client write to a **different entity** landing mid-migration is not a conflict, and the
/// migration's watermark stays true.
///
/// The complement of the two above, and it is what keeps them from being satisfied by a rule that
/// simply rejects every round with a concurrent writer. A migration is a per-entity map like any
/// other producer, so an entity nobody touched must land — and the entity the client wrote mid-round
/// must not appear in the round's output at all, because it did not exist at the fork point.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_to_another_entity_mid_migration_leaves_a_true_watermark() -> Result<()> {
    let h = Harness::new();
    let (v_from, v_to) = declare_then_mutate(&h).await?;
    let gate = Arc::new(Gate::default());
    h.install_up(UP, website_up(10, Some(Arc::clone(&gate))));

    let acme = company(100);
    let other = company(200);
    let settling = h
        .push(v_from, vec![(prop(acme, "website"), Value::Int(9))])
        .await?;

    let round = h.settling(settling);
    gate.reached().await;
    h.push(v_from, vec![(prop(other, "website"), Value::Int(4))])
        .await?;
    gate.release();
    let outcome = round.await.expect("the round did not panic")?;

    assert!(
        outcome.rejected.is_empty(),
        "the round read nothing the client touched, so it lands whole"
    );
    assert_eq!(
        h.stored_at(&prop(acme, "website"), DefVersion(v_to.0))
            .await,
        Some(Value::Int(90)),
    );
    assert_eq!(
        h.stored_at(&prop(other, "website"), DefVersion(v_to.0))
            .await,
        None,
        "and it did not migrate an entity that did not exist at its fork point"
    );

    // The claim, checked the way §10.1 defines it: fork at the stated watermark, recompute from
    // scratch, compare. `recompute` is what makes it a genuine replay rather than a reading back of
    // the value under test, because a fork inherits derived layers by ancestry (§7.4).
    let replay = h
        .branches
        .fork(BRANCH, settling, Some("replay".into()))
        .await?;
    h.engine.recompute(replay).await?;
    let path = h.branches.read_path(replay, None)?;
    let replayed = h
        .storage
        .get_cell(&path, &prop(acme, "website"), DefVersion(v_to.0))
        .await?
        .map(|found| found.event.value);
    assert_eq!(
        replayed,
        Some(Value::Int(90)),
        "replaying the world at the stated watermark reproduces the migrated value"
    );
    Ok(())
}

// --- S14: a def-only merge landing while a round computes under the old def -----------------------

/// **S14.** A second schema change arrives on the trunk, def-only from a fork, while a migration for
/// the *first* one is mid-flight. The round's output is labelled at a def-version that stopped being
/// the current one halfway through producing it.
///
/// The answer the system gives is allowed to be either — the round's guards may reject it, or its
/// output may land correctly versioned. What is asserted is that it is **coherent**: the output is
/// filed at the version it was actually computed under and never at the one that overtook it, and
/// the branch is not wedged. `v3` is a version nothing has materialised yet, which is ordinary lag
/// and reads as lag; one more round produces it, through the migration the merge brought with it.
///
/// The interleaving is the interesting part and it is not incidental. A round folds its def-view
/// **once**, from the trunk, at the moment it opens (§16.5) — so it runs the version chain it saw,
/// whatever lands afterwards. Every *write session* inside it folds a second view for permission,
/// from the trunk as it stands, which is what makes a mid-round def merge visible to the round at
/// all: the migration must still be allowed to write a field whose chain grew a step underneath it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_def_only_merge_landing_mid_round_does_not_mislabel_the_rounds_output() -> Result<()> {
    let h = Harness::new();
    let v1 = ClientVersion(h.defs.push(BRANCH, vec![declare_website()]).await?);
    let v2 = ClientVersion(h.defs.push(BRANCH, vec![mutate_website(UP)]).await?);
    let gate = Arc::new(Gate::default());
    h.install_up(UP, website_up(10, Some(Arc::clone(&gate))));

    let acme = company(100);
    let settling = h
        .push(v1, vec![(prop(acme, "website"), Value::Int(9))])
        .await?;

    // The fork carries the *next* schema change: `website` moves again, and appoints a second `up`.
    let feature = h
        .branches
        .fork(
            BRANCH,
            h.layers.head(BRANCH).unwrap(),
            Some("feature".into()),
        )
        .await?;
    h.defs.push(feature, vec![mutate_website(UP2)]).await?;

    let round = h.settling(settling);
    gate.reached().await;

    // Def-only, so nothing of the fork's data crosses — only the `MutateField` and the migration it
    // appoints (§13). It lands on the trunk while `up` is holding a value it read under the chain as
    // it stood one step shorter.
    h.branches.merge(feature, MergeMode::DefOnly).await?;
    let v3 = ClientVersion(h.defs.head(&h.branches.read_path(BRANCH, None)?));
    assert_ne!(v3.0, v2.0, "the merge moved the trunk's def-version");

    gate.release();
    let outcome = round.await.expect("the round did not panic")?;

    // Whatever became of it, the output must be filed at the version it was computed under. The
    // failure this is written against is the reverse: output labelled `v3` because that is what the
    // schema said by the time the layer committed, when nothing in it ever read the v3 world.
    assert_eq!(
        h.stored_at(&prop(acme, "website"), DefVersion(v3.0)).await,
        None,
        "nothing claims the version that overtook the round"
    );
    if outcome.applied > 0 {
        assert_eq!(
            h.stored_at(&prop(acme, "website"), DefVersion(v2.0)).await,
            Some(Value::Int(90)),
            "the round's output landed at the def-version it was computed under"
        );
    }
    assert_eq!(
        h.stored_at(&prop(acme, "website"), DefVersion(v1.0)).await,
        Some(Value::Int(9)),
        "and the author's own value is untouched, as it is by every migration (§5.3)"
    );

    // A version nothing has reached yet is lag, and lag is reported rather than invented (§10.4).
    let ahead = h.read(&prop(acme, "website"), v3).await?;
    assert_eq!(ahead.value, None);
    assert_eq!(
        ahead.state,
        Freshness::Stale,
        "a version no migration has walked to yet reads stale, not broken and not wrong"
    );

    // Not wedged, in the two senses that matter. The branch settles rather than chasing itself —
    //
    // Registered here rather than at the top, because that is when the trunk learns of it: a
    // producer becomes known to a store when the def layer defining it arrives, and for this trunk
    // that layer is the merge. Registering it up front would also have let the *first* round advance
    // its watermark past work it could not yet have had (§16.4), which is a fixture artefact rather
    // than anything the system does.
    h.install_up(UP2, website_up(2, None));
    h.engine.catch_up(BRANCH).await?;
    assert_eq!(
        h.engine.catch_up(BRANCH).await?,
        0,
        "the branch settles rather than chasing itself"
    );
    if outcome.applied > 0 {
        // The second step of the chain, reached by a plain catch-up: `up2`'s input exists only in the
        // derived layer the first round merged, and a round settles the whole range it is behind on
        // (§16.5), so that layer is in its opening wave. This used to need `--rebuild`.
        assert_eq!(
            h.stored_at(&prop(acme, "website"), DefVersion(v3.0)).await,
            Some(Value::Int(180)),
            "the migration the merge brought with it ran over the round's output, at the version it \
             was filed at: 9 → 90 → 180"
        );
    }

    // — and the trunk still works as a trunk: a client writing the old shape still reaches the view
    // the round was producing, which is the thing a def merge landing mid-round could plausibly have
    // broken.
    let other = company(200);
    h.push(v1, vec![(prop(other, "website"), Value::Int(3))])
        .await?;
    h.engine.catch_up(BRANCH).await?;
    assert_eq!(
        h.stored_at(&prop(other, "website"), DefVersion(v2.0)).await,
        Some(Value::Int(30)),
        "a write after the merge still migrates through the step the round was mid-way through"
    );

    // The same conclusion by the route that owes nothing to whether the round above landed: a
    // rebuild rewinds every watermark and runs the chain from source. *What it computes from* is the
    // proof that the round's label was right — 180 is 90 doubled, so the second step consumed
    // exactly the record the first round filed at `v2`.
    h.engine.recompute(BRANCH).await?;
    let arrived = h.read(&prop(acme, "website"), v3).await?;
    assert_eq!(
        arrived.value,
        Some(Value::Int(180)),
        "the second migration ran over the first one's output, at the version it was filed at: \
         9 → 90 → 180"
    );
    assert_eq!(arrived.origin, Origin::Derived);
    assert_eq!(arrived.state, Freshness::Current);
    Ok(())
}

/// **A migration chained onto a migration materialises on a plain catch-up.** Sequential, no
/// concurrency: this is here because S14 tripped over it, and it used to pin the *gap* rather than
/// the fix.
///
/// `up2`'s input version is written only by a **derived** layer — the one the round settling `up1`
/// merged — and while a round settled one source layer at a time there were two reasons nothing
/// found it. Derived layers opened no rounds, so the layer that wrote the input never triggered
/// anything; and §9.6's seeding, the other route, was spent by a round forked at the bottom of the
/// log, because `catch_up` starts from the *minimum* watermark across producers and a brand-new
/// producer drags that to zero — where the buffer it wanted was still empty.
///
/// Settling a **range** closes both. One round covers `[watermark+1 … head]`, so the derived layer
/// carrying `website@v2` is in the round's opening wave and triggers `up2` through it; and the round
/// forks at the *top* of the range, so the seeding scan sees the world as it now stands rather than
/// as it stood before any of this existed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_migration_chained_onto_a_migration_materialises_on_a_catch_up() -> Result<()> {
    let h = Harness::new();
    let v1 = ClientVersion(h.defs.push(BRANCH, vec![declare_website()]).await?);
    let v2 = ClientVersion(h.defs.push(BRANCH, vec![mutate_website(UP)]).await?);
    h.install_up(UP, website_up(10, None));

    let acme = company(100);
    h.push(v1, vec![(prop(acme, "website"), Value::Int(9))])
        .await?;
    h.engine.catch_up(BRANCH).await?;
    assert_eq!(
        h.stored_at(&prop(acme, "website"), DefVersion(v2.0)).await,
        Some(Value::Int(90)),
        "one step of the chain materialises on a catch-up, as `080-migration` shows"
    );

    let v3 = ClientVersion(h.defs.push(BRANCH, vec![mutate_website(UP2)]).await?);
    h.install_up(UP2, website_up(2, None));
    h.engine.catch_up(BRANCH).await?;
    assert_eq!(
        h.stored_at(&prop(acme, "website"), DefVersion(v3.0)).await,
        Some(Value::Int(180)),
        "and so does the second, over the first one's output: 9 → 90 → 180"
    );
    let arrived = h.read(&prop(acme, "website"), v3).await?;
    assert_eq!(arrived.value, Some(Value::Int(180)));
    assert_eq!(arrived.state, Freshness::Current);
    assert_eq!(
        h.engine.catch_up(BRANCH).await?,
        0,
        "and the branch settles rather than chasing its own derived layers round again"
    );
    Ok(())
}

/// The same merge, landing mid-round, must not *reject* the round either. Either it lands or the
/// branch still owes it — never neither, and never dropped for a reason that is not a reason.
///
/// A def-only merge commits a def layer, and a layer holds value events xor def events (§6.2), so
/// nothing in it can be named by a guard. The claim is worth asserting rather than assuming: the
/// cheap negative the merge path takes is the touch index's per-branch high-water mark, and a def
/// layer moves that mark while contributing no cells at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_def_only_merge_does_not_reject_a_round_it_could_not_have_disturbed() -> Result<()> {
    let h = Harness::new();
    let v1 = ClientVersion(h.defs.push(BRANCH, vec![declare_website()]).await?);
    let v2 = ClientVersion(h.defs.push(BRANCH, vec![mutate_website(UP)]).await?);
    let gate = Arc::new(Gate::default());
    h.install_up(UP, website_up(10, Some(Arc::clone(&gate))));

    let acme = company(100);
    let settling = h
        .push(v1, vec![(prop(acme, "website"), Value::Int(9))])
        .await?;
    let feature = h
        .branches
        .fork(
            BRANCH,
            h.layers.head(BRANCH).unwrap(),
            Some("feature".into()),
        )
        .await?;
    h.defs.push(feature, vec![mutate_website(UP2)]).await?;

    let round = h.settling(settling);
    gate.reached().await;
    h.branches.merge(feature, MergeMode::DefOnly).await?;
    gate.release();
    let outcome = round.await.expect("the round did not panic")?;

    assert!(
        outcome.rejected.is_empty(),
        "a def layer holds no value events, so it can trip no guard: {:?}",
        outcome.rejected
    );
    assert_eq!(
        h.stored_at(&prop(acme, "website"), DefVersion(v2.0)).await,
        Some(Value::Int(90)),
        "the round landed, at the version it computed under"
    );
    Ok(())
}
