//! Rebuild and diff: the two lifecycles of a projection must agree.
//!
//! Every in-memory index the registry holds is a fold over committed layers
//! (`borg_engine::projection`, SPEC.md §17.1), and there are exactly two ways one gets current:
//!
//! * **rebuilt from zero** — a fresh set folded over the whole log, which is what a
//!   process-per-command CLI does on every invocation;
//! * **maintained live** — the set a long-lived process keeps up to date as layers commit through
//!   it, which is what lets a server hold one registry open instead of replaying the log per
//!   request (`examples/personal-crm/FRICTION.md` #9).
//!
//! **If those two ever disagree, the second one is a lie**, and it is a lie no correctness test
//! elsewhere can see: a store rebuilt from zero answers correctly, so every existing test goes on
//! passing while a served store quietly answers something else. So this file asks both of them the
//! same questions and requires the same answers — after real work, not on toy data. The store under
//! test has def layers, source writes, two hops of derivation, a re-run that retracts edges, a round
//! that merged, a transaction that merged and a fork that did not, because the interesting
//! divergences live in merge layers and retracted edges rather than in a single write.
//!
//! This harness is what makes future implementations of the seam safe to try. A snapshotted index or
//! a probabilistic one is a new way of arriving at a position; this is the test that says whether it
//! arrived at the right place.

use borg_core::{
    AllocatorId, BranchId, CellAt, CellRef, ClientVersion, DefEvent, LayerAuthor, LayerId,
    LayerState, MergeMode, Ownership, Pid, PidKind, ProducerId, ReadPath, RepoId, Result,
    Transaction, Value, ValueType, Writer,
};
use borg_engine::{
    CellTouchIndex, DependencyIndexProvider, DependencyProjection, FrontierProjection,
    FrontierTracker, Invocation, MemoryDependencyIndex, Projection, Projections, Registry,
};
use borg_exec::{ExecutionProvider, ProducerCtx};
use borg_exec_native::{NativeExecutor, ProducerFn};
use borg_storage::{MemoryStorage, StorageProvider};
use futures_util::StreamExt;
use std::collections::BTreeSet;
use std::sync::Arc;

const INVEST: ProducerId = ProducerId(1);
const TIER: ProducerId = ProducerId(2);

fn company(n: u64, branch: BranchId) -> Pid {
    Pid::Allocated {
        kind: PidKind::Object,
        branch,
        allocator: AllocatorId(0),
        counter: n,
    }
}

fn prop(pid: Pid, field: &str) -> CellRef {
    CellRef::prop("Company".into(), field.into(), pid)
}

fn existence(pid: Pid) -> CellRef {
    CellRef::existence("Company".into(), pid)
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

fn producer(id: ProducerId) -> DefEvent {
    DefEvent::PushProducer(borg_core::ProducerDef {
        fingerprint: None,
        id,
        kind: borg_core::ProducerKind::Pipeline,
        source: borg_core::BufferId::Object("Company".into()),
        // Overwritten by the fold with the def-layer this lands in (§9.2).
        version: LayerId(0),
        declaring_repo: RepoId(1),
    })
}

/// One hop: read `from`, write `to`. The same shape `rounds.rs` uses — what matters here is the
/// edges a run records, not what it computes.
fn hop(from: &'static str, to: &'static str, times: i64) -> ProducerFn {
    Arc::new(move |ctx: &mut dyn ProducerCtx, input: Pid| {
        Box::pin(async move {
            let Some(Value::Int(n)) = ctx.get(&prop(input, from)).await? else {
                // Not there yet. Writing nothing is the honest answer; the dependency on the absent
                // cell brings this invocation back round (§16.5).
                return Ok(());
            };
            ctx.set(&prop(input, to), Value::Int(n * times)).await
        })
    })
}

// --- the store under test -------------------------------------------------------------------------

/// A registry that has done enough that its projections are worth doubting.
async fn a_store_with_history() -> Result<(Arc<MemoryStorage>, Registry, BranchId)> {
    let storage = Arc::new(MemoryStorage::new());
    let native = Arc::new(NativeExecutor::new());
    native.register(INVEST, hop("headcount", "is_investible", 2));
    native.register(TIER, hop("is_investible", "tier", 3));

    let registry = Registry::open(
        Arc::clone(&storage) as Arc<dyn StorageProvider>,
        Arc::clone(&native) as Arc<dyn ExecutionProvider>,
    )
    .await?;
    let branch = registry.branches.create_root(Some("main".into())).await?;
    registry
        .defs
        .push(
            branch,
            vec![
                declare("headcount", ValueType::Int, Ownership::Source),
                declare("is_investible", ValueType::Int, Ownership::Derived(INVEST)),
                declare("tier", ValueType::Int, Ownership::Derived(TIER)),
                // Declaring a derived *field* says who owns it; pushing the producer is what puts it
                // in the def-view, which is what `register_producers` joins to the implementations.
                producer(INVEST),
                producer(TIER),
            ],
        )
        .await?;
    registry.register_producers(branch).await?;
    let version = ClientVersion(
        registry
            .defs
            .head(&registry.branches.read_path(branch, None)?),
    );

    // Three companies, then a round that settles them: derived layers on a round branch, merged onto
    // the trunk, and a full set of dependency edges.
    for n in 1..=3u64 {
        let pid = company(n, branch);
        write(
            &registry,
            branch,
            version,
            vec![
                (existence(pid), Value::Bool(true)),
                (prop(pid, "headcount"), Value::Int(10 * n as i64)),
            ],
        )
        .await?;
    }
    registry.engine.catch_up(branch).await?;

    // A re-run, so the dependency index has **retracted** edges in it. An index that only ever
    // accumulated would agree with a rebuild by accident.
    write(
        &registry,
        branch,
        version,
        vec![(prop(company(1, branch), "headcount"), Value::Int(99))],
    )
    .await?;
    registry.engine.catch_up(branch).await?;

    // A transaction: fork, write, merge. The merge layer on the parent is what the touch index is
    // easiest to get wrong about — it holds the child's events, and a guard re-evaluated on the
    // parent has to see them *there* (§13).
    let fork_point = registry.branches.read_path(branch, None)?.ceiling();
    let forked = registry
        .branches
        .fork(branch, fork_point, Some("tx".into()))
        .await?;
    let mut state = Transaction::new(forked, branch, fork_point);
    let mut session = registry
        .begin_write(forked, version, Writer::Client)
        .await?;
    session
        .set(&prop(company(2, branch), "headcount"), Value::Int(77))
        .await?;
    for read in session.observed() {
        state.observe(read.clone());
    }
    for wrote in session.authored() {
        state.wrote(wrote.clone());
    }
    session.commit().await?;
    registry
        .branches
        .merge_transaction(&state, MergeMode::DefAndData)
        .await?;
    registry.engine.catch_up(branch).await?;

    // A fork that never merges, so the log holds layers on a branch nothing on the trunk can see.
    let ceiling = registry.branches.read_path(branch, None)?.ceiling();
    let side = registry
        .branches
        .fork(branch, ceiling, Some("side".into()))
        .await?;
    write(
        &registry,
        side,
        version,
        vec![(prop(company(3, branch), "headcount"), Value::Int(5))],
    )
    .await?;

    Ok((storage, registry, branch))
}

async fn write(
    registry: &Registry,
    branch: BranchId,
    version: ClientVersion,
    cells: Vec<(CellRef, Value)>,
) -> Result<LayerId> {
    let mut session = registry
        .begin_write(branch, version, Writer::Client)
        .await?;
    for (cell, value) in cells {
        session.set(&cell, value).await?;
    }
    session.commit().await
}

// --- the two lifecycles, side by side ---------------------------------------------------------------

/// A projection set built this instant and folded over the whole log — what a CLI invocation has.
struct Rebuilt {
    touches: Arc<CellTouchIndex>,
    index: Arc<MemoryDependencyIndex>,
    frontier: Arc<FrontierTracker>,
    set: Projections,
    /// How many layers the fold had to read. The number a server used to pay per request.
    folded: usize,
}

impl Rebuilt {
    async fn from_zero(storage: &dyn StorageProvider) -> Result<Self> {
        let touches = Arc::new(CellTouchIndex::new());
        let index = Arc::new(MemoryDependencyIndex::new());
        let frontier = Arc::new(FrontierTracker::new());
        let set = Projections::new([
            Arc::clone(&touches) as Arc<dyn Projection>,
            Arc::new(DependencyProjection::new(
                Arc::clone(&index) as Arc<dyn DependencyIndexProvider>
            )) as Arc<dyn Projection>,
            Arc::new(FrontierProjection::new(Arc::clone(&frontier))) as Arc<dyn Projection>,
        ]);
        let known = storage.read_layers().await?;
        let folded = set.bring_to_head(storage, &known).await?;
        Ok(Self {
            touches,
            index,
            frontier,
            set,
            folded,
        })
    }
}

/// Everything the log holds, as the questions a projection can be asked about it.
struct Questions {
    /// Every committed layer, in id order.
    layers: Vec<borg_core::Layer>,
    /// Every cell any layer ever wrote.
    cells: Vec<CellRef>,
    /// Every derived record, as the key `dependencies` is asked by.
    derived: Vec<(BranchId, CellAt)>,
    /// Every cell a derived event named as an input, as `dependents` is asked by.
    inputs: Vec<(BranchId, CellAt)>,
    producers: Vec<ProducerId>,
    branches: Vec<BranchId>,
}

async fn questions(storage: &dyn StorageProvider) -> Result<Questions> {
    let mut layers: Vec<_> = storage
        .read_layers()
        .await?
        .into_iter()
        .filter(|layer| layer.state == LayerState::Committed)
        .collect();
    layers.sort_by_key(|layer| layer.id.0);

    let mut cells: BTreeSet<String> = BTreeSet::new();
    let mut named: Vec<CellRef> = Vec::new();
    let mut derived: Vec<(BranchId, CellAt)> = Vec::new();
    let mut inputs: Vec<(BranchId, CellAt)> = Vec::new();
    for layer in &layers {
        let mut stream = storage.read_layer(layer.id).await?;
        while let Some(row) = stream.next().await {
            let event = row?;
            if cells.insert(event.cell.to_string()) {
                named.push(event.cell.clone());
            }
            if let Some(derivation) = &event.derivation {
                derived.push((layer.branch, CellAt::new(event.cell.clone(), event.version)));
                for input in &derivation.read_set {
                    inputs.push((layer.branch, input.clone()));
                }
            }
        }
    }
    derived.sort_by_key(key);
    derived.dedup_by_key(|entry| key(entry));
    inputs.sort_by_key(key);
    inputs.dedup_by_key(|entry| key(entry));

    let producers = layers
        .iter()
        .filter_map(|layer| match layer.author {
            LayerAuthor::Derived { producer, .. } => Some(producer),
            LayerAuthor::Source => None,
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let branches = layers
        .iter()
        .map(|layer| layer.branch)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    Ok(Questions {
        layers,
        cells: named,
        derived,
        inputs,
        producers,
        branches,
    })
}

fn key(entry: &(BranchId, CellAt)) -> (u64, String) {
    (entry.0.0, format!("{}@{}", entry.1.cell, entry.1.version))
}

fn sorted(cells: Vec<CellAt>) -> Vec<String> {
    let mut named: Vec<String> = cells
        .into_iter()
        .map(|cell| format!("{}@{}", cell.cell, cell.version))
        .collect();
    named.sort();
    named
}

fn sorted_invocations(found: Vec<Invocation>) -> Vec<String> {
    let mut named: Vec<String> = found
        .into_iter()
        .map(|i| format!("{}/{}", i.producer, i.input))
        .collect();
    named.sort();
    named
}

/// Every layer id in the log, plus `L0`. A guard's `since` is a fork point and any layer can be one,
/// so the touch index is compared at all of them rather than at a convenient one.
fn every_layer_boundary(asked: &Questions) -> Vec<LayerId> {
    let mut boundaries = vec![LayerId(0)];
    boundaries.extend(asked.layers.iter().map(|layer| layer.id));
    boundaries
}

fn first(
    index: &CellTouchIndex,
    path: &ReadPath,
    cells: &[CellRef],
    since: LayerId,
) -> Result<Option<(String, LayerId)>> {
    Ok(index
        .first_touched_since(path, cells, since)?
        .map(|(cell, layer)| (cell.to_string(), layer)))
}

// --- the properties ----------------------------------------------------------------------------

/// **The touch index a live process holds is the touch index a replay produces.**
///
/// Asked the way guard validation asks it — over a read path, for a set of cells, since a layer —
/// and asked at every layer boundary in the log.
#[tokio::test]
async fn the_touch_index_folds_to_the_same_answers_as_the_log() -> Result<()> {
    let (storage, live, _) = a_store_with_history().await?;
    let rebuilt = Rebuilt::from_zero(storage.as_ref()).await?;
    let asked = questions(storage.as_ref()).await?;

    for branch in &asked.branches {
        let path = live.layers.read_path(*branch, None)?;
        for since in every_layer_boundary(&asked) {
            assert_eq!(
                live.touches.moved_since(&path, since)?,
                rebuilt.touches.moved_since(&path, since)?,
                "moved_since disagreed on {branch} since {since}"
            );
            for cell in &asked.cells {
                let one = [cell.clone()];
                assert_eq!(
                    first(&live.touches, &path, &one, since)?,
                    first(&rebuilt.touches, &path, &one, since)?,
                    "first_touched_since disagreed for {cell} on {branch} since {since}"
                );
            }
        }
    }
    Ok(())
}

/// **The dependency graph a live process holds is the one a replay produces** — in both directions.
///
/// Forward (cell → dependents) is what invalidation reads; backward (cell → dependencies) is what
/// `explain` reads. A rebuild that got the forward edges right and the backward ones wrong would
/// derive correctly and lie about provenance, which is the failure §11 exists to prevent.
///
/// **Asked on the branches anything can name.** A round forks an unnamed branch, merges it and
/// abandons it (§16.5), so no read path ever includes one and no client can address one. The two
/// lifecycles disagree there, and only there: the replay keys a round's own derived layers on the
/// round branch as well as on the trunk it merged onto, where the live index keys them on the trunk
/// alone — which is the invariant (§16.3.8, `CLAUDE.md` #11), asserted below. That surplus is a
/// pre-existing property of the replay rather than anything the live path introduces; the harness
/// found it, and `ROADMAP.md` carries what it costs.
#[tokio::test]
async fn the_dependency_index_folds_to_the_same_edges_as_the_log() -> Result<()> {
    let (storage, live, _) = a_store_with_history().await?;
    let rebuilt = Rebuilt::from_zero(storage.as_ref()).await?;
    let asked = questions(storage.as_ref()).await?;

    assert!(
        !asked.derived.is_empty(),
        "a store with no derived records proves nothing about the dependency index"
    );
    let addressable = addressable(&live);
    assert!(
        asked.branches.iter().any(|b| !addressable.contains(b)),
        "the fixture should have run at least one round, which forks a branch nothing can name"
    );

    for (branch, cell) in &asked.derived {
        if !addressable.contains(branch) {
            continue;
        }
        assert_eq!(
            sorted(live.index.dependencies(*branch, cell)?),
            sorted(rebuilt.index.dependencies(*branch, cell)?),
            "lineage disagreed for {} on {branch}",
            cell.cell
        );
    }
    for (branch, cell) in &asked.inputs {
        if !addressable.contains(branch) {
            continue;
        }
        let one = [cell.clone()];
        assert_eq!(
            sorted_invocations(live.index.dependents(*branch, &one)?),
            sorted_invocations(rebuilt.index.dependents(*branch, &one)?),
            "dependents disagreed for {} on {branch}",
            cell.cell
        );
    }
    Ok(())
}

/// **The live index keys nothing on a round's own branch.** SPEC.md §16.3.8, `CLAUDE.md` #11.
///
/// The invariant that makes partial application safe, stated as a test rather than as a comment:
/// keyed on the round branch, an invocation whose merge was rejected would be discarded with the
/// round and never rediscovered.
#[tokio::test]
async fn the_live_index_keys_no_edge_on_a_round_branch() -> Result<()> {
    let (storage, live, _) = a_store_with_history().await?;
    let asked = questions(storage.as_ref()).await?;
    let addressable = addressable(&live);

    for (branch, cell) in &asked.derived {
        if addressable.contains(branch) {
            continue;
        }
        assert!(
            live.index.dependencies(*branch, cell)?.is_empty(),
            "the live index holds lineage for {} keyed on the round branch {branch}",
            cell.cell
        );
    }
    for (branch, cell) in &asked.inputs {
        if addressable.contains(branch) {
            continue;
        }
        assert!(
            live.index
                .dependents(*branch, std::slice::from_ref(cell))?
                .is_empty(),
            "the live index holds dependents of {} keyed on the round branch {branch}",
            cell.cell
        );
    }
    Ok(())
}

/// The branches something could ask about: a round's fork is deliberately nameless.
fn addressable(registry: &Registry) -> BTreeSet<BranchId> {
    registry
        .branches
        .all()
        .into_iter()
        .filter(|branch| branch.name.is_some())
        .map(|branch| branch.id)
        .collect()
}

/// **The watermarks a live process holds are the watermarks a replay produces.**
///
/// This is the one with teeth: a watermark is the claim *replay the world at `L` and you get exactly
/// this* (§10.1), and `settled` — the coherent-snapshot read (§10.5) — is computed from it. A live
/// frontier ahead of the log's would serve a snapshot the log cannot reproduce.
#[tokio::test]
async fn the_watermarks_fold_to_the_same_frontier_as_the_log() -> Result<()> {
    let (storage, live, branch) = a_store_with_history().await?;
    let rebuilt = Rebuilt::from_zero(storage.as_ref()).await?;
    let asked = questions(storage.as_ref()).await?;

    assert!(
        !asked.producers.is_empty(),
        "a store nothing derived on proves nothing about the frontier"
    );
    for on in &asked.branches {
        for producer in &asked.producers {
            assert_eq!(
                live.frontier.watermark(*on, *producer),
                rebuilt.frontier.watermark(*on, *producer),
                "the watermark for {producer} on {on} disagreed"
            );
        }
    }
    assert_eq!(
        live.frontier.settled(branch, &asked.producers),
        rebuilt.frontier.settled(branch, &asked.producers),
        "the settled frontier disagreed"
    );
    Ok(())
}

/// **A live registry is at the head of the log, and bringing it there again reads nothing.**
///
/// This is the whole of why holding a registry open is cheap, and it is the measurement behind
/// `FRICTION.md` #9 stated as a property: the cost of an open is the distance between a projection's
/// position and the head, not the length of the log. Asserted rather than assumed, because a
/// projection that quietly stopped advancing would still answer correctly *today* and would replay
/// the whole log on the next open.
#[tokio::test]
async fn an_open_costs_the_distance_to_head_and_not_the_length_of_the_log() -> Result<()> {
    let (storage, live, _) = a_store_with_history().await?;
    let asked = questions(storage.as_ref()).await?;
    let head = asked.layers.last().expect("the store has layers").id;
    assert!(
        asked.layers.len() > 5,
        "the fixture should have a log worth replaying"
    );

    for projection in live.projections.members() {
        assert_eq!(
            projection.position(),
            head,
            "the live {} is behind the log",
            projection.name()
        );
    }
    let known = storage.read_layers().await?;
    assert_eq!(
        live.projections
            .bring_to_head(storage.as_ref(), &known)
            .await?,
        0,
        "a registry whose projections are at head must fold nothing to open"
    );

    // The other lifecycle, for contrast: a set starting from zero folds the whole log to get there.
    let rebuilt = Rebuilt::from_zero(storage.as_ref()).await?;
    assert_eq!(
        rebuilt.folded,
        asked.layers.len(),
        "a set folded from zero reads every committed layer"
    );
    for projection in rebuilt.set.members() {
        assert_eq!(
            projection.position(),
            head,
            "the rebuilt {} did not reach the head of the log",
            projection.name()
        );
    }
    Ok(())
}
