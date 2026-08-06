//! Freshness controls. SPEC.md §10.5.
//!
//! §10 spends most of its length insisting that derived data lags and says so. This is the other
//! half: the three things a client can do about it. Ask for whatever is stored, ask for it checked,
//! or pay for it to be computed at the call site — plus the two coherent ways of choosing *when* to
//! read, and the primitive that lets a test wait rather than sleep.

use borg_core::{
    BranchId, BufferId, CellAt, CellRef, ClientVersion, DefEvent, DefVersion, Derivation,
    Freshness, FreshnessRequirement, LayerAuthor, LayerId, Ownership, Pid, PidKind, ProducerDef,
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
use std::time::Duration;

const BRANCH: BranchId = BranchId(1);
/// `website` → `is_investible`.
const SCORE: ProducerId = ProducerId(1);
/// `is_investible` → `tier`. Downstream of `SCORE`, which is what makes an inline computation have
/// to recurse.
const TIER: ProducerId = ProducerId(2);
/// A pair that reads each other's output. Not a configuration anyone should ship; it exists so the
/// inline path can be proved to terminate on one.
const LEFT: ProducerId = ProducerId(3);
const RIGHT: ProducerId = ProducerId(4);
const V1: ClientVersion = ClientVersion(LayerId(1));
/// The def-version every field in these tests sits at. One declaration, one def-layer, nothing
/// mutated since — so this is where the records are keyed, whatever any actor's whole-schema view
/// has moved on to (SPEC.md §5.3).
const AT_V1: DefVersion = DefVersion(LayerId(1));

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

fn declare(field: &str, ty: ValueType, ownership: Ownership) -> DefEvent {
    DefEvent::DeclareField {
        struct_name: "Company".into(),
        field: field.into(),
        ty,
        repo: RepoId(1),
        ownership,
    }
}

fn pipeline(id: ProducerId) -> ProducerDef {
    ProducerDef {
        id,
        kind: ProducerKind::Pipeline,
        source: BufferId::Object("Company".into()),
        version: LayerId(1),
        declaring_repo: RepoId(1),
        fingerprint: None,
    }
}

/// Doubles `website` into `is_investible`'s neighbour field so a recomputation is visible in the
/// value rather than only in the provenance.
fn score() -> borg_exec_native::ProducerFn {
    Arc::new(|ctx: &mut dyn ProducerCtx, input: Pid| {
        Box::pin(async move {
            let website = ctx.get(&prop(input, "website")).await?;
            let investible = matches!(website, Some(Value::Int(n)) if n > 3);
            ctx.set(&prop(input, "is_investible"), Value::Bool(investible))
                .await
        })
    })
}

fn tier() -> borg_exec_native::ProducerFn {
    Arc::new(|ctx: &mut dyn ProducerCtx, input: Pid| {
        Box::pin(async move {
            let investible = ctx.get(&prop(input, "is_investible")).await?;
            let tier = match investible {
                Some(Value::Bool(true)) => 1,
                _ => 3,
            };
            ctx.set(&prop(input, "tier"), Value::Int(tier)).await
        })
    })
}

/// Reads `from` and writes `to`. Two of these pointed at each other is a cycle.
fn relay(from: &'static str, to: &'static str) -> borg_exec_native::ProducerFn {
    Arc::new(move |ctx: &mut dyn ProducerCtx, input: Pid| {
        Box::pin(async move {
            let other = ctx.get(&prop(input, from)).await?;
            let next = match other {
                Some(Value::Int(n)) => n + 1,
                _ => 0,
            };
            ctx.set(&prop(input, to), Value::Int(next)).await
        })
    })
}

struct Harness {
    layers: Arc<LayerManager>,
    defs: Arc<DefRegistry>,
    engine: Arc<DerivationEngine>,
    frontier: Arc<FrontierTracker>,
    resolver: Resolver,
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

        let executor = NativeExecutor::new();
        executor.register(SCORE, score());
        executor.register(TIER, tier());
        executor.register(LEFT, relay("right", "left"));
        executor.register(RIGHT, relay("left", "right"));

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
        for id in [SCORE, TIER, LEFT, RIGHT] {
            engine.register(pipeline(id));
        }

        defs.push(
            BRANCH,
            vec![
                declare("website", ValueType::Int, Ownership::Source),
                declare("is_investible", ValueType::Bool, Ownership::Derived(SCORE)),
                declare("tier", ValueType::Int, Ownership::Derived(TIER)),
                declare("left", ValueType::Int, Ownership::Derived(LEFT)),
                declare("right", ValueType::Int, Ownership::Derived(RIGHT)),
            ],
        )
        .await?;

        Ok(Self {
            layers,
            defs: defs.clone(),
            engine: engine.clone(),
            frontier,
            resolver: Resolver::new(storage, index, defs, branches, engine),
        })
    }

    async fn push(&self, writes: Vec<(CellRef, Value)>) -> Result<LayerId> {
        let mut session = WriteSession::open(
            &self.layers,
            &self.defs,
            BRANCH,
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

    /// Plant a derived record with a read-set of our choosing, without running anything.
    ///
    /// The cyclic case cannot be reached through `catch_up` — the round's re-run counter (§16.5)
    /// poisons it first, which is exactly what that counter is for. Planting the records is how the
    /// *inline* path gets to face a cycle the round-based one has already ruled out.
    async fn plant(&self, producer: ProducerId, cell: CellRef, reads: CellRef) -> Result<LayerId> {
        let mut session = WriteSession::open(
            &self.layers,
            &self.defs,
            BRANCH,
            V1,
            Writer::Producer(producer),
            LayerAuthor::Derived {
                producer,
                reflects: LayerId(0),
            },
        )
        .await?;
        session
            .set_derived(
                &cell,
                Value::Int(0),
                Derivation {
                    producer,
                    fresh_as_of: LayerId(0),
                    read_set: vec![CellAt::new(reads, AT_V1)],
                },
            )
            .await?;
        session.commit().await
    }

    async fn read(
        &self,
        cell: &CellRef,
        requirement: FreshnessRequirement,
    ) -> Result<borg_core::Resolved<Option<Value>>> {
        self.resolver
            .resolve(BRANCH, cell, None, V1, requirement)
            .await
    }

    async fn read_at(
        &self,
        cell: &CellRef,
        layer: LayerId,
    ) -> Result<borg_core::Resolved<Option<Value>>> {
        self.resolver
            .resolve(
                BRANCH,
                cell,
                Some(layer),
                V1,
                FreshnessRequirement::Validated,
            )
            .await
    }
}

/// Set a company up, derive it, then move its input and stop. Everything below starts here.
async fn stale_world() -> Result<(Harness, Pid, LayerId)> {
    let h = Harness::new().await?;
    let acme = company(100);
    // The layer every producer settles on. A watermark points into the *source* stream, so this is
    // the id to expect on the frontier — not the higher-numbered derived layer that carries the
    // output (SPEC.md §6.3).
    let settled = h
        .push(vec![
            (existence(acme), Value::Bool(true)),
            (prop(acme, "website"), Value::Int(9)),
        ])
        .await?;
    h.engine.catch_up(BRANCH).await?;

    h.push(vec![(prop(acme, "website"), Value::Int(1))]).await?;
    Ok((h, acme, settled))
}

#[tokio::test]
async fn the_three_read_modes_give_three_different_answers_about_one_stale_cell() -> Result<()> {
    let (h, acme, _) = stale_world().await?;
    let cell = prop(acme, "is_investible");

    let any = h.read(&cell, FreshnessRequirement::Any).await?;
    assert_eq!(
        any.state,
        Freshness::Unvalidated,
        "`any` takes what is stored and does not even look"
    );
    assert_eq!(any.value, Some(Value::Bool(true)));

    let validated = h.read(&cell, FreshnessRequirement::Validated).await?;
    assert_eq!(
        validated.state,
        Freshness::Stale,
        "`validated` walks the read-set and finds the input moved — without running user code"
    );
    assert_eq!(
        validated.value,
        Some(Value::Bool(true)),
        "and still serves the stale value, labelled rather than withheld"
    );

    let current = h.read(&cell, FreshnessRequirement::Current).await?;
    assert_eq!(
        current.value,
        Some(Value::Bool(false)),
        "`current` is the only one that computes: the answer follows the input that moved"
    );
    assert_eq!(current.state, Freshness::Current);
    assert_eq!(
        current.fresh_as_of,
        h.layers.head(BRANCH).unwrap(),
        "and it reflects the world as it now stands"
    );
    Ok(())
}

#[tokio::test]
async fn computing_a_cell_inline_computes_the_inputs_it_needs_first() -> Result<()> {
    let (h, acme, _) = stale_world().await?;

    // `tier` is two hops from the write: nothing has recomputed `is_investible` either, so
    // recomputing `tier` alone would produce a confidently wrong answer from a stale input.
    let tier = h
        .read(&prop(acme, "tier"), FreshnessRequirement::Current)
        .await?;
    assert_eq!(
        tier.value,
        Some(Value::Int(3)),
        "the whole chain behind the cell was brought up to date, not just the cell"
    );
    assert_eq!(tier.state, Freshness::Current);

    // The intermediate is genuinely materialized, not merely accounted for.
    let middle = h
        .read(
            &prop(acme, "is_investible"),
            FreshnessRequirement::Validated,
        )
        .await?;
    assert_eq!(middle.value, Some(Value::Bool(false)));
    assert_eq!(middle.state, Freshness::Current);
    Ok(())
}

#[tokio::test]
async fn an_inline_computation_does_not_claim_the_producer_has_caught_up() -> Result<()> {
    let (h, acme, settled) = stale_world().await?;
    let before = h.frontier.watermark(BRANCH, SCORE);

    let other = company(200);
    h.push(vec![
        (existence(other), Value::Bool(true)),
        (prop(other, "website"), Value::Int(9)),
    ])
    .await?;
    h.read(&prop(acme, "is_investible"), FreshnessRequirement::Current)
        .await?;

    assert_eq!(
        h.frontier.watermark(BRANCH, SCORE),
        before,
        "one cell computed on demand says nothing about the rest of the producer's output"
    );
    assert_eq!(
        before, settled,
        "…and the watermark is where derivation left it"
    );

    // The work therefore stays outstanding, which is what makes this self-healing: the company the
    // inline read never touched is still owed, and the next round pays it.
    assert!(
        h.engine.catch_up(BRANCH).await? > 0,
        "a round still has work to do after an inline computation"
    );
    assert_eq!(
        h.read(
            &prop(other, "is_investible"),
            FreshnessRequirement::Validated
        )
        .await?
        .value,
        Some(Value::Bool(true)),
        "and the entity the inline read ignored is derived by it"
    );
    Ok(())
}

#[tokio::test]
async fn computing_a_cell_whose_inputs_lead_back_to_it_terminates() -> Result<()> {
    let h = Harness::new().await?;
    let acme = company(300);
    h.push(vec![(existence(acme), Value::Bool(true))]).await?;

    // Each is recorded as having been computed from the other. Following the read-sets is therefore
    // an infinite walk unless something stops it.
    h.plant(LEFT, prop(acme, "left"), prop(acme, "right"))
        .await?;
    h.plant(RIGHT, prop(acme, "right"), prop(acme, "left"))
        .await?;

    let resolved = tokio::time::timeout(
        Duration::from_secs(10),
        h.read(&prop(acme, "left"), FreshnessRequirement::Current),
    )
    .await
    .expect("an inline computation must not recurse forever on a cyclic read-set")?;

    // The value is whatever one pass produced. What matters is that the read returned at all, and
    // that it did not claim more than one pass earns.
    assert!(resolved.value.is_some(), "a value is still served");
    Ok(())
}

#[tokio::test]
async fn a_settled_read_is_coherent_where_a_head_read_is_ragged() -> Result<()> {
    let (h, acme, settled) = stale_world().await?;
    assert_eq!(
        h.frontier.settled(BRANCH, &[SCORE, TIER]),
        Some(settled),
        "the settled frontier is the minimum over every producer on the branch"
    );

    // Ragged head: the latest of everything, and freshness varies field by field.
    let ragged_input = h
        .read(&prop(acme, "website"), FreshnessRequirement::Any)
        .await?;
    let ragged_output = h
        .read(
            &prop(acme, "is_investible"),
            FreshnessRequirement::Validated,
        )
        .await?;
    assert_eq!(ragged_input.value, Some(Value::Int(1)), "the newest input");
    assert_eq!(
        ragged_output.value,
        Some(Value::Bool(true)),
        "beside an output computed from the previous one"
    );
    assert_eq!(ragged_output.state, Freshness::Stale);

    // Settled frontier: a coherent snapshot, slightly in the past. The input is the one the output
    // was actually computed from, so the two agree.
    let ceiling = h.layers.settled_ceiling(BRANCH, settled);
    let coherent_input = h.read_at(&prop(acme, "website"), ceiling).await?;
    let coherent_output = h.read_at(&prop(acme, "is_investible"), ceiling).await?;
    assert_eq!(
        coherent_input.value,
        Some(Value::Int(9)),
        "the settled read does not see the write nothing has incorporated yet"
    );
    assert_eq!(coherent_output.value, Some(Value::Bool(true)));
    assert_eq!(
        coherent_output.state,
        Freshness::Current,
        "and nothing in that snapshot is behind anything else in it"
    );
    Ok(())
}

#[tokio::test]
async fn reaches_returns_once_every_producer_has_incorporated_the_layer() -> Result<()> {
    let frontier = Arc::new(FrontierTracker::new());
    let target = LayerId(7);

    let advancing = Arc::clone(&frontier);
    tokio::spawn(async move {
        advancing.advance(BRANCH, SCORE, target);
        // TIER is deliberately last: the frontier is the *minimum*, so waking on the first advance
        // and not re-checking would return too early.
        tokio::time::sleep(Duration::from_millis(20)).await;
        advancing.advance(BRANCH, TIER, target);
    });

    tokio::time::timeout(
        Duration::from_secs(10),
        frontier.reaches(BRANCH, &[SCORE, TIER], target),
    )
    .await
    .expect("reaches must return once the frontier arrives");

    assert_eq!(frontier.settled(BRANCH, &[SCORE, TIER]), Some(target));
    Ok(())
}

#[tokio::test]
async fn reaches_waits_while_any_producer_is_behind() -> Result<()> {
    let frontier = FrontierTracker::new();
    frontier.advance(BRANCH, SCORE, LayerId(7));
    frontier.advance(BRANCH, TIER, LayerId(3));

    let outcome = tokio::time::timeout(
        Duration::from_millis(50),
        frontier.reaches(BRANCH, &[SCORE, TIER], LayerId(7)),
    )
    .await;
    assert!(
        outcome.is_err(),
        "one producer at L3 means the branch has not reached L7, whatever the others have done"
    );
    Ok(())
}

#[tokio::test]
async fn a_branch_with_no_producers_has_already_reached_every_layer() -> Result<()> {
    let frontier = FrontierTracker::new();
    assert_eq!(
        frontier.settled(BRANCH, &[]),
        None,
        "nothing derives, so there is no watermark to be behind"
    );
    tokio::time::timeout(
        Duration::from_millis(50),
        frontier.reaches(BRANCH, &[], LayerId(500)),
    )
    .await
    .expect("waiting for derived data on a branch with none would never return");
    Ok(())
}

/// A producer that reads another producer's output can be caught up, and must be able to say so.
///
/// `validate` compares each dependency against the layer it landed in. That is the right question
/// for source data, and the wrong one for derived: a producer's output lands in a *derived* layer,
/// which by construction has a higher id than the source layer it reflects — so a chained
/// dependency always looked as though it had moved after the value that read it, and every chained
/// cell was reported stale on a fully settled branch. §10.3 says what to compare instead:
///
///     W(B) = min(target, W(A), W(other deps))
///
/// — the dependency's *watermark*, not where it landed.
#[tokio::test]
async fn a_chained_producer_on_a_caught_up_branch_is_current() -> Result<()> {
    let h = Harness::new().await?;
    let acme = company(400);
    h.push(vec![
        (existence(acme), Value::Bool(true)),
        (prop(acme, "website"), Value::Int(9)),
    ])
    .await?;
    h.engine.catch_up(BRANCH).await?;

    let first = h
        .read(
            &prop(acme, "is_investible"),
            FreshnessRequirement::Validated,
        )
        .await?;
    assert_eq!(
        first.state,
        Freshness::Current,
        "the first hop reads only source data and has always been able to reach current"
    );

    let second = h
        .read(&prop(acme, "tier"), FreshnessRequirement::Validated)
        .await?;
    assert_eq!(second.value, Some(Value::Int(1)));
    assert_eq!(
        second.state,
        Freshness::Current,
        "and the second hop is no less caught up for having read the first"
    );
    Ok(())
}

/// …and `current` is still earned rather than assumed: a chain behind its source is behind.
///
/// The composition is a `min`, so the second hop reports the *first* hop's watermark — not head,
/// which would be a lie, and not its own last run, which would understate how far the chain is
/// known to be good for.
#[tokio::test]
async fn a_chain_is_only_as_fresh_as_the_hop_behind_it() -> Result<()> {
    let (h, acme, settled) = stale_world().await?;

    let first = h
        .read(
            &prop(acme, "is_investible"),
            FreshnessRequirement::Validated,
        )
        .await?;
    assert_eq!(
        first.state,
        Freshness::Stale,
        "the source it reads has moved and nothing has re-run"
    );

    // `tier`'s own dependency — `is_investible` — has not been rewritten at all. It is behind
    // anyway, because what it was computed from is behind.
    let second = h
        .read(&prop(acme, "tier"), FreshnessRequirement::Validated)
        .await?;
    assert_eq!(
        second.state,
        Freshness::Stale,
        "staleness propagates down the chain rather than stopping at the producer that moved"
    );
    assert_eq!(
        second.fresh_as_of, settled,
        "and the watermark it reports is the minimum over the chain (SPEC.md §10.3)"
    );
    Ok(())
}

/// `--freshness current` on a chained cell converges: asking twice does not run the producer twice.
///
/// The pessimistic comparison made this unreachable — every read recomputed, reported stale, and
/// left the next read exactly as much work to do.
#[tokio::test]
async fn asking_a_chained_cell_for_current_twice_does_not_compute_twice() -> Result<()> {
    let (h, acme, _) = stale_world().await?;

    let first = h
        .read(&prop(acme, "tier"), FreshnessRequirement::Current)
        .await?;
    assert_eq!(first.state, Freshness::Current);
    let after_first = h.layers.head(BRANCH).unwrap();

    let second = h
        .read(&prop(acme, "tier"), FreshnessRequirement::Current)
        .await?;
    assert_eq!(second.state, Freshness::Current);
    assert_eq!(
        h.layers.head(BRANCH).unwrap(),
        after_first,
        "the second read found the value already correct and committed nothing"
    );
    Ok(())
}
