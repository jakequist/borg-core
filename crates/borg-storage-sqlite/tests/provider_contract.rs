//! The `StorageProvider` contract. SPEC.md §17.1.
//!
//! **Every test here runs against both backends.** A previous milestone found `MemoryStorage`
//! reading a cell's history in id order where it is really commit order — a divergence SQLite never
//! had, and one no single-backend suite could see. The contract is what both must satisfy, so the
//! expectations are written once and applied twice; the handful of tests below that name SQLite
//! alone are the ones about durability and batching, which are properties of that backend rather
//! than of the interface.

use borg_core::{
    BranchId, BufferId, CellKey, CellRef, ClientVersion, DefEvent, EventDraft, EventId, LayerId,
    Origin, Pid, PidKind, ReadPath, RepoId, Result, Value, ValueType,
};
use borg_storage::{MemoryStorage, StorageProvider};
use borg_storage_sqlite::SqliteStorage;
use futures_util::StreamExt;
use std::sync::Arc;

const MAIN: BranchId = BranchId(1);
const V1: ClientVersion = ClientVersion(LayerId(1));
const V9: ClientVersion = ClientVersion(LayerId(9));

#[derive(Clone, Copy, Debug)]
enum Backend {
    Memory,
    Sqlite,
}

const BOTH: [Backend; 2] = [Backend::Memory, Backend::Sqlite];

fn provider(backend: Backend) -> Result<Arc<dyn StorageProvider>> {
    Ok(match backend {
        Backend::Memory => Arc::new(MemoryStorage::new()),
        Backend::Sqlite => Arc::new(SqliteStorage::in_memory()?),
    })
}

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

/// An event before the log gives it one — no id, and no layer, which is the whole point (§4.3).
fn draft(value: Value, version: ClientVersion) -> EventDraft {
    EventDraft {
        value,
        version,
        origin: Origin::Source,
        derivation: None,
    }
}

fn path(segments: &[(BranchId, LayerId)]) -> ReadPath {
    ReadPath::new(segments.to_vec())
}

/// Author some events into a layer and commit it.
async fn commit(
    storage: &dyn StorageProvider,
    branch: BranchId,
    id: LayerId,
    writes: &[(CellRef, EventDraft)],
) -> Result<Vec<EventId>> {
    let mut layer = storage.open_layer(branch, id).await?;
    let mut ids = Vec::new();
    for (cell, draft) in writes {
        ids.push(layer.author_event(cell, draft.clone()).await?);
    }
    let sealed = layer.seal().await?;
    storage.commit_layer(sealed).await?;
    Ok(ids)
}

/// A layer that names events it did not author — what a merge builds (§13).
async fn commit_naming(
    storage: &dyn StorageProvider,
    branch: BranchId,
    id: LayerId,
    events: &[EventId],
) -> Result<()> {
    let mut layer = storage.open_layer(branch, id).await?;
    for event in events {
        layer.include_event(*event).await?;
    }
    let sealed = layer.seal().await?;
    storage.commit_layer(sealed).await
}

/// A layer's membership, in order.
async fn members(storage: &dyn StorageProvider, layer: LayerId) -> Result<Vec<borg_core::Event>> {
    let mut stream = storage.read_layer(layer).await?;
    let mut events = Vec::new();
    while let Some(row) = stream.next().await {
        events.push(row?);
    }
    Ok(events)
}

#[tokio::test]
async fn an_open_layer_is_invisible_until_it_commits() -> Result<()> {
    for backend in BOTH {
        let storage = provider(backend)?;
        let here = path(&[(MAIN, LayerId(100))]);

        let mut layer = storage.open_layer(MAIN, LayerId(1)).await?;
        layer
            .author_event(&prop(1, "name"), draft(Value::Int(1), V1))
            .await?;

        assert!(
            storage
                .get_cell(&here, &prop(1, "name"), V1)
                .await?
                .is_none(),
            "{backend:?}: events and their index rows stream in as they arrive but stay invisible \
             — visibility is a join against the layer's state, not a flag on each row"
        );

        let sealed = layer.seal().await?;
        assert!(
            storage
                .get_cell(&here, &prop(1, "name"), V1)
                .await?
                .is_none(),
            "{backend:?}: still invisible once sealed; only commit reveals it"
        );

        storage.commit_layer(sealed).await?;
        assert_eq!(
            storage
                .get_cell(&here, &prop(1, "name"), V1)
                .await?
                .map(|found| found.event.value),
            Some(Value::Int(1)),
            "{backend:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn an_authored_event_records_the_layer_that_authored_it() -> Result<()> {
    for backend in BOTH {
        let storage = provider(backend)?;
        let ids = commit(
            storage.as_ref(),
            MAIN,
            LayerId(1),
            &[(prop(1, "name"), draft(Value::Int(1), V1))],
        )
        .await?;

        let found = storage
            .get_cell(&path(&[(MAIN, LayerId(100))]), &prop(1, "name"), V1)
            .await?
            .expect("just written");
        assert_eq!(found.event.id, ids[0], "{backend:?}: the id the log minted");
        assert_eq!(
            (found.event.authored, found.landed_at),
            (LayerId(1), LayerId(1)),
            "{backend:?}: authored and landed coincide until a merge separates them"
        );
        assert_eq!(found.event.cell, prop(1, "name"), "{backend:?}");
    }
    Ok(())
}

/// The property the whole inversion exists for. *Failing means a merge would have to copy.*
#[tokio::test]
async fn one_event_named_by_two_layers_is_one_event() -> Result<()> {
    for backend in BOTH {
        let storage = provider(backend)?;
        const FEATURE: BranchId = BranchId(2);

        let authored = commit(
            storage.as_ref(),
            FEATURE,
            LayerId(1),
            &[(prop(1, "name"), draft(Value::Int(7), V1))],
        )
        .await?;
        // What a merge does: a layer on the parent naming the child's events, writing no value.
        commit_naming(storage.as_ref(), MAIN, LayerId(2), &authored).await?;

        let on_main = storage
            .get_cell(&path(&[(MAIN, LayerId(100))]), &prop(1, "name"), V1)
            .await?
            .expect("named by the merge layer");
        let on_feature = storage
            .get_cell(&path(&[(FEATURE, LayerId(100))]), &prop(1, "name"), V1)
            .await?
            .expect("authored here");

        assert_eq!(
            on_main.event.id, on_feature.event.id,
            "{backend:?}: one identity, reachable from two branches"
        );
        assert_eq!(on_main.event.value, Value::Int(7), "{backend:?}");
        assert_eq!(
            on_main.event.authored,
            LayerId(1),
            "{backend:?}: still says where it was written"
        );
        assert_eq!(
            on_main.landed_at,
            LayerId(2),
            "{backend:?}: and the layer it was reached through says where it landed"
        );
        assert_eq!(
            (on_feature.event.authored, on_feature.landed_at),
            (LayerId(1), LayerId(1)),
            "{backend:?}: while the branch that authored it is unchanged by the sharing"
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_layer_is_an_ordered_group_and_keeps_both_writes_to_one_cell() -> Result<()> {
    for backend in BOTH {
        let storage = provider(backend)?;
        let ids = commit(
            storage.as_ref(),
            MAIN,
            LayerId(1),
            &[
                (prop(1, "name"), draft(Value::Int(1), V1)),
                (prop(2, "name"), draft(Value::Int(2), V1)),
                (prop(1, "name"), draft(Value::Int(3), V1)),
            ],
        )
        .await?;

        assert_eq!(
            members(storage.as_ref(), LayerId(1))
                .await?
                .iter()
                .map(|event| (event.id, event.value))
                .collect::<Vec<_>>(),
            vec![
                (ids[0], Value::Int(1)),
                (ids[1], Value::Int(2)),
                (ids[2], Value::Int(3)),
            ],
            "{backend:?}: membership is ordered, and two writes to one cell are two events"
        );
        assert_eq!(
            storage
                .get_cell(&path(&[(MAIN, LayerId(100))]), &prop(1, "name"), V1)
                .await?
                .map(|found| found.event.value),
            Some(Value::Int(3)),
            "{backend:?}: and the later of the two is what resolves"
        );
    }
    Ok(())
}

#[tokio::test]
async fn naming_an_event_that_does_not_exist_is_rejected() -> Result<()> {
    for backend in BOTH {
        let storage = provider(backend)?;
        // A membership row pointing at nothing would resolve to no value while looking like a
        // write, so it is refused rather than stored.
        assert!(
            commit_naming(storage.as_ref(), MAIN, LayerId(1), &[EventId(404)])
                .await
                .is_err(),
            "{backend:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn an_aborted_layer_leaves_nothing_behind() -> Result<()> {
    for backend in BOTH {
        let storage = provider(backend)?;
        let here = path(&[(MAIN, LayerId(100))]);

        let mut layer = storage.open_layer(MAIN, LayerId(1)).await?;
        layer
            .author_event(&prop(1, "name"), draft(Value::Int(1), V1))
            .await?;
        layer.abort().await?;

        assert!(
            storage
                .get_cell(&here, &prop(1, "name"), V1)
                .await?
                .is_none(),
            "{backend:?}"
        );
        // The id is free again, which is what makes an aborted producer run leave no trace at all.
        assert!(
            storage.open_layer(MAIN, LayerId(1)).await.is_ok(),
            "{backend:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn aborting_a_layer_does_not_discard_events_it_only_named() -> Result<()> {
    for backend in BOTH {
        let storage = provider(backend)?;
        const FEATURE: BranchId = BranchId(2);

        let authored = commit(
            storage.as_ref(),
            FEATURE,
            LayerId(1),
            &[(prop(1, "name"), draft(Value::Int(7), V1))],
        )
        .await?;

        // A merge layer that never commits. Membership goes; the events belong to the layer that
        // authored them and must survive — otherwise one abandoned merge would delete the child's
        // data, which is precisely the risk sharing introduces.
        let mut layer = storage.open_layer(MAIN, LayerId(2)).await?;
        layer.include_event(authored[0]).await?;
        layer.abort().await?;

        assert!(
            storage
                .get_cell(&path(&[(MAIN, LayerId(100))]), &prop(1, "name"), V1)
                .await?
                .is_none(),
            "{backend:?}: nothing landed on the parent"
        );
        assert_eq!(
            storage
                .get_cell(&path(&[(FEATURE, LayerId(100))]), &prop(1, "name"), V1)
                .await?
                .map(|found| found.event.value),
            Some(Value::Int(7)),
            "{backend:?}: and the child still has its own write"
        );
    }
    Ok(())
}

#[tokio::test]
async fn the_read_index_rebuilds_from_layer_membership() -> Result<()> {
    for backend in BOTH {
        let storage = provider(backend)?;
        const FEATURE: BranchId = BranchId(2);

        let authored = commit(
            storage.as_ref(),
            FEATURE,
            LayerId(1),
            &[
                (prop(1, "name"), draft(Value::Int(1), V1)),
                (prop(1, "name"), draft(Value::Int(2), V1)),
            ],
        )
        .await?;
        commit_naming(storage.as_ref(), MAIN, LayerId(2), &authored).await?;
        commit(
            storage.as_ref(),
            MAIN,
            LayerId(3),
            &[(prop(2, "name"), draft(Value::Int(9), V1))],
        )
        .await?;

        let before = [
            storage
                .get_cell(&path(&[(MAIN, LayerId(100))]), &prop(1, "name"), V1)
                .await?,
            storage
                .get_cell(&path(&[(MAIN, LayerId(100))]), &prop(2, "name"), V1)
                .await?,
            storage
                .get_cell(&path(&[(MAIN, LayerId(2))]), &prop(1, "name"), V1)
                .await?,
        ];

        // The index is a projection of the log, exactly as the dependency and touch indexes are —
        // so throwing it away and rebuilding it from membership must change no answer.
        storage.rebuild_read_index().await?;

        let after = [
            storage
                .get_cell(&path(&[(MAIN, LayerId(100))]), &prop(1, "name"), V1)
                .await?,
            storage
                .get_cell(&path(&[(MAIN, LayerId(100))]), &prop(2, "name"), V1)
                .await?,
            storage
                .get_cell(&path(&[(MAIN, LayerId(2))]), &prop(1, "name"), V1)
                .await?,
        ];

        for (before, after) in before.iter().zip(&after) {
            assert_eq!(
                before.as_ref().map(|f| (f.event.id, f.landed_at)),
                after.as_ref().map(|f| (f.event.id, f.landed_at)),
                "{backend:?}: rebuilding the index changes no answer"
            );
        }
        assert_eq!(
            after[0].as_ref().map(|f| f.event.value),
            Some(Value::Int(2)),
            "{backend:?}: including which of two writes in one layer wins"
        );
    }
    Ok(())
}

#[tokio::test]
async fn one_cell_coexists_at_several_versions() -> Result<()> {
    for backend in BOTH {
        let storage = provider(backend)?;
        let here = path(&[(MAIN, LayerId(100))]);

        commit(
            storage.as_ref(),
            MAIN,
            LayerId(1),
            &[(prop(1, "website"), draft(Value::Int(9), V1))],
        )
        .await?;
        commit(
            storage.as_ref(),
            MAIN,
            LayerId(2),
            &[(prop(1, "website"), draft(Value::Int(90), V9))],
        )
        .await?;

        // Writes are never coerced, so the value a v1 client wrote and the migrated v9 view are
        // different events at the same address (SPEC.md §5.4).
        assert_eq!(
            storage
                .get_cell(&here, &prop(1, "website"), V1)
                .await?
                .map(|found| found.event.value),
            Some(Value::Int(9)),
            "{backend:?}"
        );
        assert_eq!(
            storage
                .get_cell(&here, &prop(1, "website"), V9)
                .await?
                .map(|found| found.event.value),
            Some(Value::Int(90)),
            "{backend:?}"
        );

        let mut versions = storage.cell_versions(&here, &prop(1, "website")).await?;
        versions.sort_by_key(|v| v.0.0);
        assert_eq!(versions, vec![V1, V9], "{backend:?}");
    }
    Ok(())
}

#[tokio::test]
async fn a_read_path_walks_outward_and_a_tombstone_stops_it() -> Result<()> {
    for backend in BOTH {
        let storage = provider(backend)?;
        const CHILD: BranchId = BranchId(2);

        commit(
            storage.as_ref(),
            MAIN,
            LayerId(1),
            &[
                (prop(1, "name"), draft(Value::Int(1), V1)),
                (prop(1, "kept"), draft(Value::Int(7), V1)),
            ],
        )
        .await?;
        commit(
            storage.as_ref(),
            CHILD,
            LayerId(2),
            &[(prop(1, "name"), draft(Value::Tombstone, V1))],
        )
        .await?;

        // The child bounded at its head, then the parent bounded at the fork point.
        let child = path(&[(CHILD, LayerId(2)), (MAIN, LayerId(1))]);

        assert_eq!(
            storage
                .get_cell(&child, &prop(1, "name"), V1)
                .await?
                .map(|found| found.event.value),
            Some(Value::Tombstone),
            "{backend:?}: the first segment holding *any* record wins — a tombstone must stop the \
             walk rather than fall through and resurrect the parent's value"
        );
        assert_eq!(
            storage
                .get_cell(&child, &prop(1, "kept"), V1)
                .await?
                .map(|found| found.event.value),
            Some(Value::Int(7)),
            "{backend:?}: and anything the child did not touch is inherited"
        );

        // The parent alone is unaffected.
        let parent = path(&[(MAIN, LayerId(100))]);
        assert_eq!(
            storage
                .get_cell(&parent, &prop(1, "name"), V1)
                .await?
                .map(|found| found.event.value),
            Some(Value::Int(1)),
            "{backend:?}"
        );
    }
    Ok(())
}

/// Layers commit out of order — ids are assigned at open, order is established at commit (§7.3) —
/// so "the latest landing at or below the bound" is a maximum, not the last row inserted.
#[tokio::test]
async fn the_newest_landing_wins_however_the_layers_committed() -> Result<()> {
    for backend in BOTH {
        let storage = provider(backend)?;
        let here = path(&[(MAIN, LayerId(100))]);

        // Both layers open, and the **higher id commits first**. A backend that took the last
        // landing it stored rather than the highest would answer L2 here — which is exactly the
        // divergence a previous milestone found in `MemoryStorage` and SQLite never had.
        let mut lower = storage.open_layer(MAIN, LayerId(2)).await?;
        lower
            .author_event(&prop(1, "name"), draft(Value::Int(2), V1))
            .await?;
        let mut higher = storage.open_layer(MAIN, LayerId(3)).await?;
        higher
            .author_event(&prop(1, "name"), draft(Value::Int(3), V1))
            .await?;

        let higher = higher.seal().await?;
        storage.commit_layer(higher).await?;
        let lower = lower.seal().await?;
        storage.commit_layer(lower).await?;

        assert_eq!(
            storage
                .get_cell(&here, &prop(1, "name"), V1)
                .await?
                .map(|found| found.event.value),
            Some(Value::Int(3)),
            "{backend:?}: the highest landing wins, not the most recent commit"
        );
        assert_eq!(
            storage
                .get_cell(&path(&[(MAIN, LayerId(2))]), &prop(1, "name"), V1)
                .await?
                .map(|found| found.event.value),
            Some(Value::Int(2)),
            "{backend:?}: and reading below it still sees the earlier layer"
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_scan_prefers_the_innermost_segments_record() -> Result<()> {
    for backend in BOTH {
        let storage = provider(backend)?;
        const CHILD: BranchId = BranchId(2);

        commit(
            storage.as_ref(),
            MAIN,
            LayerId(1),
            &[
                (prop(1, "name"), draft(Value::Int(1), V1)),
                (prop(2, "name"), draft(Value::Int(2), V1)),
            ],
        )
        .await?;
        commit(
            storage.as_ref(),
            CHILD,
            LayerId(2),
            &[(prop(1, "name"), draft(Value::Int(99), V1))],
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
            let event = row?;
            let CellKey::Pid(p) = event.cell.key else {
                unreachable!()
            };
            found.push((p, event.value));
        }
        found.sort_by_key(|(p, _)| format!("{p:?}"));

        assert_eq!(
            found,
            vec![(pid(1), Value::Int(99)), (pid(2), Value::Int(2))],
            "{backend:?}: the child's record shadows the parent's, and untouched entities are \
             still enumerated"
        );
    }
    Ok(())
}

/// One cell materialized at two def-versions is two rows, because a version is part of the record
/// key (§4.3). Enumeration is what a migration uses to find the entities it owes, so collapsing the
/// two would hide one end of a migration step from the only enumeration the engine has.
#[tokio::test]
async fn a_scan_enumerates_a_cell_once_per_version() -> Result<()> {
    for backend in BOTH {
        let storage = provider(backend)?;

        commit(
            storage.as_ref(),
            MAIN,
            LayerId(1),
            &[
                (prop(1, "name"), draft(Value::Int(1), V1)),
                (prop(1, "name"), draft(Value::Int(9), V9)),
            ],
        )
        .await?;

        let mut stream = storage
            .scan_buffer(
                &path(&[(MAIN, LayerId(100))]),
                &BufferId::ObjectProp("Company".into(), "name".into()),
            )
            .await?;
        let mut found = Vec::new();
        while let Some(row) = stream.next().await {
            let event = row?;
            found.push((event.version, event.value));
        }
        found.sort_by_key(|(version, _)| version.0.0);

        assert_eq!(
            found,
            vec![(V1, Value::Int(1)), (V9, Value::Int(9))],
            "{backend:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn def_events_round_trip_in_order() -> Result<()> {
    for backend in BOTH {
        let storage = provider(backend)?;

        let mut layer = storage.open_layer(MAIN, LayerId(1)).await?;
        for field in ["name", "website"] {
            layer
                .put_def(DefEvent::DeclareField {
                    struct_name: "Company".into(),
                    field: field.into(),
                    ty: ValueType::String,
                    repo: RepoId(1),
                    ownership: borg_core::Ownership::Source,
                })
                .await?;
        }
        let sealed = layer.seal().await?;
        storage.commit_layer(sealed).await?;

        let events = storage.read_def_layer(LayerId(1)).await?;
        assert_eq!(events.len(), 2, "{backend:?}");
        assert_eq!(
            events
                .iter()
                .filter_map(|e| e.touches().map(|(_, f)| f.to_string()))
                .collect::<Vec<_>>(),
            vec!["name", "website"],
            "{backend:?}: def events keep their order within a layer"
        );
    }
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
            &[(prop(1, "name"), draft(Value::Int(42), V1))],
        )
        .await?;
    }

    let reopened = SqliteStorage::open(&file)?;
    assert_eq!(
        reopened
            .get_cell(&here, &prop(1, "name"), V1)
            .await?
            .map(|found| found.event.value),
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
            .author_event(&prop(n, "name"), draft(Value::Int(n as i64), V1))
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
                .map(|found| found.event.value),
            Some(Value::Int(n as i64)),
            "row {n} survived, including on the batch boundaries"
        );
    }
    Ok(())
}

// --- Interned values (SPEC.md §3.1, §4.2) ---

#[tokio::test]
async fn interning_the_same_bytes_twice_yields_the_same_pid() -> Result<()> {
    let storage = SqliteStorage::in_memory()?;
    let first = storage.intern(PidKind::String, b"acme.ai").await?;
    let again = storage.intern(PidKind::String, b"acme.ai").await?;
    assert_eq!(
        first, again,
        "the PID is a function of the bytes, so interning is idempotent"
    );
    assert_eq!(
        storage.read_interned(&first).await?,
        Some(b"acme.ai".to_vec()),
        "and the second write neither duplicated nor corrupted the row"
    );
    Ok(())
}

#[tokio::test]
async fn different_bytes_yield_different_pids() -> Result<()> {
    let storage = SqliteStorage::in_memory()?;
    let ai = storage.intern(PidKind::String, b"acme.ai").await?;
    let com = storage.intern(PidKind::String, b"acme.com").await?;
    assert_ne!(ai, com);
    assert_eq!(storage.read_interned(&ai).await?, Some(b"acme.ai".to_vec()));
    assert_eq!(
        storage.read_interned(&com).await?,
        Some(b"acme.com".to_vec())
    );
    Ok(())
}

#[tokio::test]
async fn one_string_interned_on_two_branches_is_one_pid() -> Result<()> {
    let storage = SqliteStorage::in_memory()?;
    let feature = BranchId(2);

    // Two branches each write `"acme.ai"` into a website cell of their own. Neither interning call
    // names a branch, because a content PID has no branch to name — this is what makes a string
    // write unable to conflict across branches, and why `interned` has no branch column.
    let on_main = storage.intern(PidKind::String, b"acme.ai").await?;
    commit(
        &storage,
        MAIN,
        LayerId(1),
        &[(prop(1, "website"), draft(Value::Ref(on_main), V1))],
    )
    .await?;

    let on_feature = storage.intern(PidKind::String, b"acme.ai").await?;
    commit(
        &storage,
        feature,
        LayerId(2),
        &[(prop(2, "website"), draft(Value::Ref(on_feature), V1))],
    )
    .await?;

    assert_eq!(on_main, on_feature);
    assert_eq!(
        storage
            .get_cell(&path(&[(MAIN, LayerId(100))]), &prop(1, "website"), V1)
            .await?
            .map(|found| found.event.value),
        storage
            .get_cell(&path(&[(feature, LayerId(100))]), &prop(2, "website"), V1)
            .await?
            .map(|found| found.event.value),
        "the two branches reference one and the same value"
    );
    assert_eq!(
        storage.read_interned(&on_main).await?,
        Some(b"acme.ai".to_vec()),
        "and it is readable without naming a branch at all"
    );
    Ok(())
}

#[tokio::test]
async fn interned_bytes_round_trip() -> Result<()> {
    let storage = SqliteStorage::in_memory()?;
    for (kind, bytes) in [
        (PidKind::String, "héllo — utf-8".as_bytes()),
        // Embedded NULs and high bytes: stored as a BLOB, so nothing here is text-truncated.
        (PidKind::Binary, &[0x00, 0xff, 0x00][..]),
        (PidKind::BigInt, &[0x01, 0x00, 0x00, 0x00, 0x00][..]),
        (PidKind::String, b""),
    ] {
        let pid = storage.intern(kind, bytes).await?;
        assert_eq!(pid.kind(), kind);
        assert_eq!(storage.read_interned(&pid).await?.as_deref(), Some(bytes));
    }
    Ok(())
}

#[tokio::test]
async fn one_preimage_under_two_kinds_stores_two_values() -> Result<()> {
    let storage = SqliteStorage::in_memory()?;
    let text = storage.intern(PidKind::String, b"x").await?;
    let blob = storage.intern(PidKind::Binary, b"x").await?;
    assert_ne!(text, blob, "the kind is part of the PID, not of the hash");
    assert_eq!(storage.read_interned(&text).await?, Some(b"x".to_vec()));
    assert_eq!(
        storage.read_interned(&blob).await?,
        Some(b"x".to_vec()),
        "equal hashes under different kinds are separate rows, not a collision"
    );
    Ok(())
}

#[tokio::test]
async fn an_unknown_content_pid_reads_as_none() -> Result<()> {
    let storage = SqliteStorage::in_memory()?;
    // A PID travels further than the bytes behind it, so a miss is an answer, not a failure.
    let elsewhere = borg_core::content::pid(PidKind::String, b"never interned here")?;
    assert_eq!(storage.read_interned(&elsewhere).await?, None);
    Ok(())
}

#[tokio::test]
async fn an_allocated_pid_is_not_an_interned_value() -> Result<()> {
    let storage = SqliteStorage::in_memory()?;
    assert!(matches!(
        storage.read_interned(&pid(1)).await,
        Err(borg_core::BorgError::NotContentAddressed { .. })
    ));
    assert!(matches!(
        storage.intern(PidKind::Object, b"acme.ai").await,
        Err(borg_core::BorgError::NotContentAddressed { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn interned_values_survive_reopening_the_file() -> Result<()> {
    let file = std::env::temp_dir().join("borg-sqlite-interning-test.db");
    let _ = std::fs::remove_file(&file);

    let stored = {
        let storage = SqliteStorage::open(&file)?;
        storage.intern(PidKind::String, b"acme.ai").await?
    };

    let reopened = SqliteStorage::open(&file)?;
    assert_eq!(
        reopened.read_interned(&stored).await?,
        Some(b"acme.ai".to_vec()),
        "interned values are eternal, so they had better outlive the process"
    );
    assert_eq!(
        reopened.intern(PidKind::String, b"acme.ai").await?,
        stored,
        "and re-interning across a restart still lands on the same PID"
    );

    std::fs::remove_file(&file).ok();
    Ok(())
}
