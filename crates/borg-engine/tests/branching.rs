//! Branching, inheritance, and merge. SPEC.md §7, §13.
//!
//! Written before the implementation. The behaviour here is fully specified, so the tests state what
//! `fork` and `merge` mean and the code is made to agree.

use borg_core::{
    BranchId, CellRecord, CellRef, ClientVersion, LayerAuthor, LayerId, LayerKind, MergeMode,
    Origin, Pid, PidKind, Result, Value,
};
use borg_engine::{BranchManager, CellTouchIndex, InProcessSequencer, LayerManager};
use borg_storage::{MemoryStorage, StorageProvider};
use std::sync::Arc;

const V1: ClientVersion = ClientVersion(LayerId(1));

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
    storage: Arc<MemoryStorage>,
    layers: Arc<LayerManager>,
    branches: Arc<BranchManager>,
}

impl Harness {
    fn new() -> Self {
        let storage = Arc::new(MemoryStorage::new());
        let layers = Arc::new(LayerManager::new(
            storage.clone(),
            Arc::new(InProcessSequencer::new()),
            Arc::new(CellTouchIndex::new()),
        ));
        let branches = Arc::new(BranchManager::new(layers.clone()));
        Self {
            storage,
            layers,
            branches,
        }
    }

    async fn push(&self, branch: BranchId, writes: Vec<(CellRef, Value)>) -> Result<LayerId> {
        let mut layer = self
            .layers
            .open(branch, LayerKind::Value, LayerAuthor::Source)
            .await?;
        for (cell, value) in writes {
            layer
                .put(
                    &cell,
                    CellRecord {
                        value,
                        version: V1,
                        written_at: layer.id(),
                        origin: Origin::Source,
                        derivation: None,
                    },
                )
                .await?;
        }
        self.layers.commit(layer).await
    }

    /// Read through the branch's full ancestry, at its head.
    async fn read(&self, branch: BranchId, cell: &CellRef) -> Result<Option<Value>> {
        let path = self.branches.read_path(branch, None)?;
        Ok(self
            .storage
            .get_cell(&path, cell, V1)
            .await?
            .map(|record| record.value))
    }

    async fn read_at(
        &self,
        branch: BranchId,
        cell: &CellRef,
        layer: LayerId,
    ) -> Result<Option<Value>> {
        let path = self.branches.read_path(branch, Some(layer))?;
        Ok(self
            .storage
            .get_cell(&path, cell, V1)
            .await?
            .map(|record| record.value))
    }
}

#[tokio::test]
async fn a_fork_inherits_its_parents_data_without_copying_it() -> Result<()> {
    let h = Harness::new();
    let main = h.branches.create_root(Some("main".into())).await?;
    let acme = company(main, 1);

    let before_fork = h
        .push(
            main,
            vec![
                (existence(acme), Value::Bool(true)),
                (prop(acme, "name"), Value::Int(1)),
            ],
        )
        .await?;

    let feature = h
        .branches
        .fork(main, before_fork, Some("feature".into()))
        .await?;

    assert_eq!(
        h.read(feature, &prop(acme, "name")).await?,
        Some(Value::Int(1)),
        "a fork sees its parent's data through ancestry, not through a copy"
    );
    Ok(())
}

#[tokio::test]
async fn a_child_shadows_its_parent_without_mutating_it() -> Result<()> {
    let h = Harness::new();
    let main = h.branches.create_root(Some("main".into())).await?;
    let acme = company(main, 1);

    let fork_point = h
        .push(main, vec![(prop(acme, "name"), Value::Int(1))])
        .await?;
    let feature = h.branches.fork(main, fork_point, None).await?;

    h.push(feature, vec![(prop(acme, "name"), Value::Int(2))])
        .await?;

    assert_eq!(
        h.read(feature, &prop(acme, "name")).await?,
        Some(Value::Int(2)),
        "the child sees its own write"
    );
    assert_eq!(
        h.read(main, &prop(acme, "name")).await?,
        Some(Value::Int(1)),
        "and the parent is entirely unaffected"
    );
    Ok(())
}

#[tokio::test]
async fn a_write_after_the_fork_point_is_invisible_to_the_child() -> Result<()> {
    let h = Harness::new();
    let main = h.branches.create_root(None).await?;
    let acme = company(main, 1);

    let fork_point = h
        .push(main, vec![(prop(acme, "name"), Value::Int(1))])
        .await?;
    let feature = h.branches.fork(main, fork_point, None).await?;

    // The parent moves on. The child forked from an earlier point and must not see it.
    h.push(main, vec![(prop(acme, "name"), Value::Int(99))])
        .await?;

    assert_eq!(
        h.read(feature, &prop(acme, "name")).await?,
        Some(Value::Int(1)),
        "the ancestry segment is bounded at the fork point"
    );
    Ok(())
}

#[tokio::test]
async fn a_tombstone_on_a_child_hides_an_inherited_value() -> Result<()> {
    let h = Harness::new();
    let main = h.branches.create_root(None).await?;
    let acme = company(main, 1);

    let fork_point = h
        .push(main, vec![(prop(acme, "name"), Value::Int(1))])
        .await?;
    let feature = h.branches.fork(main, fork_point, None).await?;

    h.push(feature, vec![(prop(acme, "name"), Value::Tombstone)])
        .await?;

    assert_eq!(
        h.read(feature, &prop(acme, "name")).await?,
        Some(Value::Tombstone),
        "a deletion on the child must stop the lookup rather than falling through to the parent"
    );
    assert_eq!(
        h.read(main, &prop(acme, "name")).await?,
        Some(Value::Int(1)),
        "and the parent still has its value"
    );
    Ok(())
}

#[tokio::test]
async fn time_travel_reaches_back_across_the_fork_point() -> Result<()> {
    let h = Harness::new();
    let main = h.branches.create_root(None).await?;
    let acme = company(main, 1);

    let first = h
        .push(main, vec![(prop(acme, "name"), Value::Int(1))])
        .await?;
    let second = h
        .push(main, vec![(prop(acme, "name"), Value::Int(2))])
        .await?;
    let feature = h.branches.fork(main, second, None).await?;
    h.push(feature, vec![(prop(acme, "name"), Value::Int(3))])
        .await?;

    assert_eq!(
        h.read_at(feature, &prop(acme, "name"), first).await?,
        Some(Value::Int(1)),
        "reading the child at a pre-fork layer sees the parent's history at that point"
    );
    assert_eq!(
        h.read_at(feature, &prop(acme, "name"), second).await?,
        Some(Value::Int(2))
    );
    Ok(())
}

#[tokio::test]
async fn merging_replays_the_childs_source_layers_onto_the_parent() -> Result<()> {
    let h = Harness::new();
    let main = h.branches.create_root(None).await?;
    let acme = company(main, 1);

    let fork_point = h
        .push(
            main,
            vec![
                (existence(acme), Value::Bool(true)),
                (prop(acme, "name"), Value::Int(1)),
            ],
        )
        .await?;
    let feature = h.branches.fork(main, fork_point, None).await?;
    h.push(feature, vec![(prop(acme, "name"), Value::Int(2))])
        .await?;

    let replayed = h.branches.merge(feature, MergeMode::DefAndData).await?;
    assert_eq!(
        replayed.len(),
        1,
        "one child source layer, one new parent layer"
    );

    assert_eq!(
        h.read(main, &prop(acme, "name")).await?,
        Some(Value::Int(2)),
        "the child's write now lands on the parent"
    );
    assert_ne!(
        replayed[0], fork_point,
        "merge creates new layers on the parent rather than grafting the child's"
    );
    Ok(())
}

#[tokio::test]
async fn merge_is_rejected_when_the_parent_deleted_what_the_child_wrote() -> Result<()> {
    let h = Harness::new();
    let main = h.branches.create_root(None).await?;
    let acme = company(main, 1);

    let fork_point = h
        .push(main, vec![(existence(acme), Value::Bool(true))])
        .await?;
    let feature = h.branches.fork(main, fork_point, None).await?;

    // The child edits an object the parent then deletes.
    h.push(feature, vec![(prop(acme, "name"), Value::Int(2))])
        .await?;
    h.push(main, vec![(existence(acme), Value::Tombstone)])
        .await?;

    let outcome = h.branches.merge(feature, MergeMode::DefAndData).await;
    assert!(
        outcome.is_err(),
        "v1 rejects the whole merge rather than applying it partially"
    );

    assert_eq!(
        h.read(main, &prop(acme, "name")).await?,
        None,
        "and nothing from the rejected merge is left behind on the parent"
    );
    Ok(())
}

#[tokio::test]
async fn merge_skips_derived_layers() -> Result<()> {
    let h = Harness::new();
    let main = h.branches.create_root(None).await?;
    let acme = company(main, 1);

    let fork_point = h
        .push(main, vec![(existence(acme), Value::Bool(true))])
        .await?;
    let feature = h.branches.fork(main, fork_point, None).await?;

    h.push(feature, vec![(prop(acme, "name"), Value::Int(2))])
        .await?;

    // A derived layer on the child, as the derivation engine would produce.
    let mut derived = h
        .layers
        .open(
            feature,
            LayerKind::Value,
            LayerAuthor::Derived {
                producer: borg_core::ProducerId(1),
                reflects: fork_point,
            },
        )
        .await?;
    derived
        .put(
            &prop(acme, "is_investible"),
            CellRecord {
                value: Value::Bool(true),
                version: V1,
                written_at: derived.id(),
                origin: Origin::Derived,
                derivation: None,
            },
        )
        .await?;
    h.layers.commit(derived).await?;

    let replayed = h.branches.merge(feature, MergeMode::DefAndData).await?;
    assert_eq!(
        replayed.len(),
        1,
        "only the source layer is replayed; the derived one is skipped"
    );
    assert_eq!(
        h.read(main, &prop(acme, "is_investible")).await?,
        None,
        "the parent re-derives rather than inheriting the child's derived values, which are wrong \
         on the parent by construction"
    );
    Ok(())
}
