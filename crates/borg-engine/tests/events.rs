//! Events have identity; layers reference them. SPEC.md §4.3, §6.2, §13.
//!
//! Written before the implementation. Two claims matter here and neither could be stated at all
//! under the old model, where a stored record carried the layer it lived in:
//!
//! * **S11 — authorship survives merge.** A value written on a branch and merged reports both where
//!   it was authored and where it landed. Under the old model merge rewrote `written_at`, so
//!   "authored on `feature` at L6, landed on main at L8" collapsed into L8 and the first half was
//!   destroyed.
//! * **S12 — time travel across a merge is coherent**, and one event named by two layers resolves to
//!   **one identity rather than two values**. That is the specific risk the inversion introduces:
//!   the whole point is sharing events, so sharing must be shown not to double-count.
//!
//! Every test runs against **both** backends. A previous milestone found `MemoryStorage` reading a
//! cell's history in id order where it is really commit order — a divergence SQLite never had —
//! which is the class of bug one backend alone cannot expose.

use borg_core::{
    BranchId, CellRef, ClientVersion, DefEvent, FreshnessRequirement, LayerAuthor, LayerId,
    MergeMode, Origin, Ownership, Pid, PidKind, RepoId, Result, Value, ValueType, Writer,
};
use borg_engine::{
    BranchManager, CellTouchIndex, DefRegistry, DerivationEngine, FrontierTracker,
    InProcessSequencer, LayerManager, MemoryDependencyIndex, Resolver, WriteSession,
};
use borg_exec_native::NativeExecutor;
use borg_storage::{MemoryStorage, StorageProvider};
use borg_storage_sqlite::SqliteStorage;
use futures_util::StreamExt;
use std::sync::Arc;

const V1: ClientVersion = ClientVersion(LayerId(1));

#[derive(Clone, Copy, Debug)]
enum Backend {
    Memory,
    Sqlite,
}

const BOTH: [Backend; 2] = [Backend::Memory, Backend::Sqlite];

fn company(branch: BranchId, n: u64) -> Pid {
    Pid::Allocated {
        kind: PidKind::Object,
        branch,
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
    storage: Arc<dyn StorageProvider>,
    layers: Arc<LayerManager>,
    branches: Arc<BranchManager>,
    resolver: Resolver,
    defs: Arc<DefRegistry>,
}

impl Harness {
    fn new(backend: Backend) -> Result<Self> {
        let storage: Arc<dyn StorageProvider> = match backend {
            Backend::Memory => Arc::new(MemoryStorage::new()),
            Backend::Sqlite => Arc::new(SqliteStorage::in_memory()?),
        };
        let index = Arc::new(MemoryDependencyIndex::new());
        let layers = Arc::new(LayerManager::new(
            storage.clone(),
            Arc::new(InProcessSequencer::new()),
            Arc::new(CellTouchIndex::new()),
        ));
        let branches = Arc::new(BranchManager::new(layers.clone()));
        let defs = Arc::new(DefRegistry::new(layers.clone(), storage.clone()));
        let engine = Arc::new(DerivationEngine::new(
            storage.clone(),
            layers.clone(),
            index.clone(),
            Arc::new(NativeExecutor::new()),
            Arc::new(FrontierTracker::new()),
            defs.clone(),
            branches.clone(),
        ));
        let resolver = Resolver::new(
            storage.clone(),
            index,
            defs.clone(),
            branches.clone(),
            engine,
        );
        Ok(Self {
            storage,
            layers,
            branches,
            resolver,
            defs,
        })
    }

    async fn root(&self) -> Result<BranchId> {
        let branch = self.branches.create_root(Some("main".into())).await?;
        self.defs
            .push(
                branch,
                vec![DefEvent::DeclareField {
                    struct_name: "Company".into(),
                    field: "name".into(),
                    ty: ValueType::Int,
                    repo: RepoId(1),
                    ownership: Ownership::Source,
                }],
            )
            .await?;
        Ok(branch)
    }

    async fn push(&self, branch: BranchId, writes: Vec<(CellRef, Value)>) -> Result<LayerId> {
        let mut session = WriteSession::open(
            &self.layers,
            &self.defs,
            branch,
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

    async fn resolve(
        &self,
        branch: BranchId,
        cell: &CellRef,
        at: Option<LayerId>,
    ) -> Result<borg_core::Resolved<Option<Value>>> {
        self.resolver
            .resolve(branch, cell, at, V1, FreshnessRequirement::Any)
            .await
    }

    /// A layer's membership, in order.
    async fn members(&self, layer: LayerId) -> Result<Vec<borg_core::Event>> {
        let mut stream = self.storage.read_layer(layer).await?;
        let mut events = Vec::new();
        while let Some(row) = stream.next().await {
            events.push(row?);
        }
        Ok(events)
    }
}

/// The fixture every test below shares: a value authored on a fork, then merged.
struct Merged {
    main: BranchId,
    feature: BranchId,
    cell: CellRef,
    fork_point: LayerId,
    authored: LayerId,
    landed: LayerId,
}

async fn merge_fixture(h: &Harness) -> Result<Merged> {
    let main = h.root().await?;
    let acme = company(main, 1);
    let cell = prop(acme, "name");

    let fork_point = h
        .push(
            main,
            vec![
                (existence(acme), Value::Bool(true)),
                (cell.clone(), Value::Int(1)),
            ],
        )
        .await?;
    let feature = h
        .branches
        .fork(main, fork_point, Some("feature".into()))
        .await?;
    let authored = h.push(feature, vec![(cell.clone(), Value::Int(2))]).await?;

    // The parent moves on somewhere else, so that the merge lands above a layer of the parent's own
    // and "where it landed" is a different layer from "the head at the fork".
    h.push(main, vec![(prop(company(main, 2), "name"), Value::Int(7))])
        .await?;

    let replayed = h.branches.merge(feature, MergeMode::DefAndData).await?;
    Ok(Merged {
        main,
        feature,
        cell,
        fork_point,
        authored,
        landed: *replayed.last().expect("one replayed layer"),
    })
}

/// S11. *Failing means we inverted the pointers and kept rewriting anyway: no cost saved, no
/// lineage gained.*
#[tokio::test]
async fn a_merged_value_reports_where_it_was_authored_and_where_it_landed() -> Result<()> {
    for backend in BOTH {
        let h = Harness::new(backend)?;
        let m = merge_fixture(&h).await?;

        let on_main = h.resolve(m.main, &m.cell, None).await?;
        assert_eq!(on_main.value, Some(Value::Int(2)), "{backend:?}");
        assert_eq!(
            on_main.authored_at, m.authored,
            "{backend:?}: the event still says where it was first committed — on the fork"
        );
        assert_eq!(
            on_main.landed_at, m.landed,
            "{backend:?}: and the layer it was reached through says where it landed here"
        );
        assert_ne!(
            on_main.authored_at, on_main.landed_at,
            "{backend:?}: which is the information the old model collapsed"
        );

        let on_feature = h.resolve(m.feature, &m.cell, None).await?;
        assert_eq!(
            (on_feature.authored_at, on_feature.landed_at),
            (m.authored, m.authored),
            "{backend:?}: on the branch that wrote it, authored and landed are the same layer"
        );
    }
    Ok(())
}

/// S12, first half. *This is the specific risk the inversion introduces.*
#[tokio::test]
async fn one_event_named_by_two_layers_is_one_identity_not_two_values() -> Result<()> {
    for backend in BOTH {
        let h = Harness::new(backend)?;
        let m = merge_fixture(&h).await?;

        let on_main = h.resolve(m.main, &m.cell, None).await?;
        let on_feature = h.resolve(m.feature, &m.cell, None).await?;
        assert_eq!(
            on_main.event, on_feature.event,
            "{backend:?}: both branches resolve to the same event, not to two copies of it"
        );
        assert!(on_main.event.is_some(), "{backend:?}");
        assert_eq!(on_main.value, on_feature.value, "{backend:?}");
        assert_eq!(on_main.origin, Origin::Source, "{backend:?}");

        // And the merge layer *names* the child's event rather than carrying a rewritten copy of
        // it: this is the whole cost argument.
        let landed = h.members(m.landed).await?;
        let authored = h.members(m.authored).await?;
        assert_eq!(
            landed.iter().map(|e| e.id).collect::<Vec<_>>(),
            authored.iter().map(|e| e.id).collect::<Vec<_>>(),
            "{backend:?}: merge names the child's events; it does not author new ones"
        );
        assert!(
            landed.iter().all(|e| e.authored == m.authored),
            "{backend:?}: and the events it names still point at the layer that authored them"
        );
    }
    Ok(())
}

/// S12, second half.
#[tokio::test]
async fn time_travel_across_a_merge_is_coherent() -> Result<()> {
    for backend in BOTH {
        let h = Harness::new(backend)?;
        let m = merge_fixture(&h).await?;

        assert_eq!(
            h.resolve(m.main, &m.cell, Some(m.fork_point)).await?.value,
            Some(Value::Int(1)),
            "{backend:?}: reading main below the merge does not see merged data"
        );
        assert_eq!(
            h.resolve(m.main, &m.cell, Some(LayerId(m.landed.0 - 1)))
                .await?
                .value,
            Some(Value::Int(1)),
            "{backend:?}: not even at the layer immediately below it"
        );
        assert_eq!(
            h.resolve(m.main, &m.cell, Some(m.landed)).await?.value,
            Some(Value::Int(2)),
            "{backend:?}: and does at the merge layer itself"
        );

        // The event was authored at a layer that is *below* the merge point but on another branch.
        // Bounding main's read path by a layer id must not let it leak in through the id alone.
        assert!(
            m.authored.0 < m.landed.0,
            "{backend:?}: fixture check — the authoring layer has the lower id"
        );
        assert_eq!(
            h.resolve(m.main, &m.cell, Some(m.authored)).await?.value,
            Some(Value::Int(1)),
            "{backend:?}: reading main at the authoring layer's id sees main, not the fork"
        );
    }
    Ok(())
}

/// Membership is ordered, and the order is the order events were put into the layer — not event-id
/// order, and not whatever a hash map iterates in.
#[tokio::test]
async fn a_layers_membership_reads_back_in_the_order_it_was_written() -> Result<()> {
    for backend in BOTH {
        let h = Harness::new(backend)?;
        let main = h.root().await?;
        let acme = company(main, 1);

        let layer = h
            .push(
                main,
                vec![
                    (existence(acme), Value::Bool(true)),
                    (prop(acme, "name"), Value::Int(1)),
                    (prop(acme, "name"), Value::Int(2)),
                ],
            )
            .await?;

        let members = h.members(layer).await?;
        assert_eq!(
            members.iter().map(|e| e.cell.clone()).collect::<Vec<_>>(),
            vec![existence(acme), prop(acme, "name"), prop(acme, "name")],
            "{backend:?}: a layer is an *ordered* group of events, including two writes to one cell"
        );
        assert!(
            members.iter().all(|e| e.authored == layer),
            "{backend:?}: all authored here"
        );
        assert_eq!(
            h.resolve(main, &prop(acme, "name"), None).await?.value,
            Some(Value::Int(2)),
            "{backend:?}: and the later write within one layer is the one that resolves"
        );
    }
    Ok(())
}
