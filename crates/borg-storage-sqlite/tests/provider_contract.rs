//! The `StorageProvider` contract, checked against SQLite. SPEC.md §17.1.

use borg_core::{
    BranchId, BufferId, CellKey, CellRecord, CellRef, ClientVersion, DefEvent, LayerId, Origin,
    Pid, PidKind, ReadPath, RepoId, Result, Value, ValueType,
};
use borg_storage::StorageProvider;
use borg_storage_sqlite::SqliteStorage;
use futures_util::StreamExt;

const MAIN: BranchId = BranchId(1);
const V1: ClientVersion = ClientVersion(LayerId(1));
const V9: ClientVersion = ClientVersion(LayerId(9));

fn pid(n: u64) -> Pid {
    Pid::Allocated {
        kind: PidKind::Object,
        branch: MAIN,
        allocator: borg_core::AllocatorId(0),
        counter: n,
    }
}

fn prop(n: u64, field: &str) -> CellRef {
    CellRef::prop("Company".into(), field.into(), pid(n))
}

fn record(value: Value, version: ClientVersion, at: LayerId) -> CellRecord {
    CellRecord {
        value,
        version,
        written_at: at,
        origin: Origin::Source,
        derivation: None,
    }
}

fn path(segments: &[(BranchId, LayerId)]) -> ReadPath {
    ReadPath::new(segments.to_vec())
}

/// Write one cell into a layer and commit it.
async fn commit(
    storage: &SqliteStorage,
    branch: BranchId,
    id: LayerId,
    writes: &[(CellRef, CellRecord)],
) -> Result<()> {
    let mut layer = storage.open_layer(branch, id).await?;
    for (cell, rec) in writes {
        layer.put_cell(cell, rec.clone()).await?;
    }
    let sealed = layer.seal().await?;
    storage.commit_layer(sealed).await
}

#[tokio::test]
async fn an_open_layer_is_invisible_until_it_commits() -> Result<()> {
    let storage = SqliteStorage::in_memory()?;
    let here = path(&[(MAIN, LayerId(100))]);

    let mut layer = storage.open_layer(MAIN, LayerId(1)).await?;
    layer
        .put_cell(&prop(1, "name"), record(Value::Int(1), V1, LayerId(1)))
        .await?;

    assert!(
        storage
            .get_cell(&here, &prop(1, "name"), V1)
            .await?
            .is_none(),
        "rows stream in as they arrive but stay invisible — visibility is a join against the \
         layer's state, not a flag on each row"
    );

    let sealed = layer.seal().await?;
    assert!(
        storage
            .get_cell(&here, &prop(1, "name"), V1)
            .await?
            .is_none(),
        "still invisible once sealed; only commit reveals it"
    );

    storage.commit_layer(sealed).await?;
    assert_eq!(
        storage
            .get_cell(&here, &prop(1, "name"), V1)
            .await?
            .map(|r| r.value),
        Some(Value::Int(1))
    );
    Ok(())
}

#[tokio::test]
async fn an_aborted_layer_leaves_nothing_behind() -> Result<()> {
    let storage = SqliteStorage::in_memory()?;
    let here = path(&[(MAIN, LayerId(100))]);

    let mut layer = storage.open_layer(MAIN, LayerId(1)).await?;
    layer
        .put_cell(&prop(1, "name"), record(Value::Int(1), V1, LayerId(1)))
        .await?;
    layer.abort().await?;

    assert!(
        storage
            .get_cell(&here, &prop(1, "name"), V1)
            .await?
            .is_none()
    );
    // The id is free again, which is what makes an aborted producer run leave no trace at all.
    assert!(storage.open_layer(MAIN, LayerId(1)).await.is_ok());
    Ok(())
}

#[tokio::test]
async fn one_cell_coexists_at_several_versions() -> Result<()> {
    let storage = SqliteStorage::in_memory()?;
    let here = path(&[(MAIN, LayerId(100))]);

    commit(
        &storage,
        MAIN,
        LayerId(1),
        &[(prop(1, "website"), record(Value::Int(9), V1, LayerId(1)))],
    )
    .await?;
    commit(
        &storage,
        MAIN,
        LayerId(2),
        &[(prop(1, "website"), record(Value::Int(90), V9, LayerId(2)))],
    )
    .await?;

    // Writes are never coerced, so the value a v1 client wrote and the migrated v9 view are
    // different records at the same address (SPEC.md §5.4).
    assert_eq!(
        storage
            .get_cell(&here, &prop(1, "website"), V1)
            .await?
            .map(|r| r.value),
        Some(Value::Int(9))
    );
    assert_eq!(
        storage
            .get_cell(&here, &prop(1, "website"), V9)
            .await?
            .map(|r| r.value),
        Some(Value::Int(90))
    );

    let mut versions = storage.cell_versions(&here, &prop(1, "website")).await?;
    versions.sort_by_key(|v| v.0.0);
    assert_eq!(versions, vec![V1, V9]);
    Ok(())
}

#[tokio::test]
async fn a_read_path_walks_outward_and_a_tombstone_stops_it() -> Result<()> {
    let storage = SqliteStorage::in_memory()?;
    const CHILD: BranchId = BranchId(2);

    commit(
        &storage,
        MAIN,
        LayerId(1),
        &[
            (prop(1, "name"), record(Value::Int(1), V1, LayerId(1))),
            (prop(1, "kept"), record(Value::Int(7), V1, LayerId(1))),
        ],
    )
    .await?;
    commit(
        &storage,
        CHILD,
        LayerId(2),
        &[(prop(1, "name"), record(Value::Tombstone, V1, LayerId(2)))],
    )
    .await?;

    // The child bounded at its head, then the parent bounded at the fork point.
    let child = path(&[(CHILD, LayerId(2)), (MAIN, LayerId(1))]);

    assert_eq!(
        storage
            .get_cell(&child, &prop(1, "name"), V1)
            .await?
            .map(|r| r.value),
        Some(Value::Tombstone),
        "the first segment holding *any* record wins — a tombstone must stop the walk rather than \
         fall through and resurrect the parent's value"
    );
    assert_eq!(
        storage
            .get_cell(&child, &prop(1, "kept"), V1)
            .await?
            .map(|r| r.value),
        Some(Value::Int(7)),
        "and anything the child did not touch is inherited"
    );

    // The parent alone is unaffected.
    let parent = path(&[(MAIN, LayerId(100))]);
    assert_eq!(
        storage
            .get_cell(&parent, &prop(1, "name"), V1)
            .await?
            .map(|r| r.value),
        Some(Value::Int(1))
    );
    Ok(())
}

#[tokio::test]
async fn a_scan_prefers_the_innermost_segments_record() -> Result<()> {
    let storage = SqliteStorage::in_memory()?;
    const CHILD: BranchId = BranchId(2);

    commit(
        &storage,
        MAIN,
        LayerId(1),
        &[
            (prop(1, "name"), record(Value::Int(1), V1, LayerId(1))),
            (prop(2, "name"), record(Value::Int(2), V1, LayerId(1))),
        ],
    )
    .await?;
    commit(
        &storage,
        CHILD,
        LayerId(2),
        &[(prop(1, "name"), record(Value::Int(99), V1, LayerId(2)))],
    )
    .await?;

    let child = path(&[(CHILD, LayerId(2)), (MAIN, LayerId(1))]);
    let mut stream = storage
        .scan_buffer(
            &child,
            &BufferId::ObjectProp("Company".into(), "name".into()),
        )
        .await?;

    let mut found = Vec::new();
    while let Some(row) = stream.next().await {
        let (cell, rec) = row?;
        let CellKey::Pid(p) = cell.key else {
            unreachable!()
        };
        found.push((p, rec.value));
    }
    found.sort_by_key(|(p, _)| format!("{p:?}"));

    assert_eq!(
        found,
        vec![(pid(1), Value::Int(99)), (pid(2), Value::Int(2))],
        "the child's record shadows the parent's, and untouched entities are still enumerated"
    );
    Ok(())
}

#[tokio::test]
async fn def_events_round_trip_in_order() -> Result<()> {
    let storage = SqliteStorage::in_memory()?;

    let mut layer = storage.open_layer(MAIN, LayerId(1)).await?;
    for field in ["name", "website"] {
        layer
            .put_def(DefEvent::DeclareField {
                struct_name: "Company".into(),
                field: field.into(),
                ty: ValueType::String,
                repo: RepoId(1),
            })
            .await?;
    }
    let sealed = layer.seal().await?;
    storage.commit_layer(sealed).await?;

    let events = storage.read_def_layer(LayerId(1)).await?;
    assert_eq!(events.len(), 2);
    assert_eq!(
        events
            .iter()
            .filter_map(|e| e.touches().map(|(_, f)| f.to_string()))
            .collect::<Vec<_>>(),
        vec!["name", "website"],
        "def events keep their order within a layer"
    );
    Ok(())
}

#[tokio::test]
async fn data_survives_reopening_the_file() -> Result<()> {
    let file = std::env::temp_dir().join("borg-sqlite-reopen-test.db");
    let _ = std::fs::remove_file(&file);
    let here = path(&[(MAIN, LayerId(100))]);

    {
        let storage = SqliteStorage::open(&file)?;
        commit(
            &storage,
            MAIN,
            LayerId(1),
            &[(prop(1, "name"), record(Value::Int(42), V1, LayerId(1)))],
        )
        .await?;
    }

    let reopened = SqliteStorage::open(&file)?;
    assert_eq!(
        reopened
            .get_cell(&here, &prop(1, "name"), V1)
            .await?
            .map(|r| r.value),
        Some(Value::Int(42)),
        "committed layers are durable — which MemoryStorage could never demonstrate"
    );

    std::fs::remove_file(&file).ok();
    Ok(())
}

#[tokio::test]
async fn a_layer_larger_than_one_batch_flushes_without_losing_rows() -> Result<()> {
    let storage = SqliteStorage::in_memory()?;
    let here = path(&[(MAIN, LayerId(100))]);

    // Comfortably past the internal batch size, so several flushes happen mid-layer.
    const COUNT: u64 = 1500;

    let mut layer = storage.open_layer(MAIN, LayerId(1)).await?;
    for n in 0..COUNT {
        layer
            .put_cell(
                &prop(n, "name"),
                record(Value::Int(n as i64), V1, LayerId(1)),
            )
            .await?;
    }

    // Rows have already been flushed to disk, but the layer is still open — so none of them are
    // visible. Buffering is only safe because of this.
    assert!(
        storage
            .get_cell(&here, &prop(0, "name"), V1)
            .await?
            .is_none(),
        "flushed-but-uncommitted rows stay invisible"
    );

    let sealed = layer.seal().await?;
    storage.commit_layer(sealed).await?;

    for n in [0, 511, 512, 1023, 1024, COUNT - 1] {
        assert_eq!(
            storage
                .get_cell(&here, &prop(n, "name"), V1)
                .await?
                .map(|r| r.value),
            Some(Value::Int(n as i64)),
            "row {n} survived, including on the batch boundaries"
        );
    }
    Ok(())
}
