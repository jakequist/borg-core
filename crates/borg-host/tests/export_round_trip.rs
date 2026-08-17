//! **The round trip is the specification.** SPEC.md §19.
//!
//! The format policy's whole promise is *the data, not the bytes*: every release can write a
//! registry out and read one back, so an upgrade is export → upgrade → import. There is exactly one
//! way to check a claim like that, and it is the S1 pattern — not "does the code do what it says",
//! but *"ask the two registries the same questions and require the same answers."*
//!
//! So this file builds one store with as much of the system in it as will fit — two branches, a
//! merge, a field materialized at two def-versions with a real migration between them, derived data
//! from a pipeline, one interned string shared by two cells, a producer poisoned *after* it had
//! succeeded, a paused branch and an advanced PID counter — exports it, imports it into a fresh
//! store, and then never looks at a file again. Every assertion below is a question asked of both.
//!
//! ## Why a poisoned producer had to be in the fixture
//!
//! Because it is the one sidecar decision that could have gone the other way (`borg_host::stream`).
//! A poisoning looks like operational residue, and if it were skipped every test that only read
//! *values* would still pass — the values are in the log. What changes is the **label**: a poisoned
//! producer's output reads `broken`, and without the table it would read `stale`, which is a promise
//! of a catch-up that is not coming. That is a difference only an envelope comparison can see, and
//! it is why the comparison here is of whole envelopes rather than of values.
//!
//! ## And why the byte-identity check comes before the further write
//!
//! `export(import(export(x))) == export(x)` is the cheapest total check available: it compares
//! everything at once, including all the things nobody thought to assert. It is therefore taken
//! while both stores are still untouched, and only then does the last test write to both and require
//! them to move the same way.

use borg_core::{
    AllocatorId, BranchId, CellRef, ClientVersion, DefEvent, FreshnessRequirement, LayerId,
    MergeMode, MigrationDirection, Ownership, Pid, PidKind, ProducerDef, ProducerId, ProducerKind,
    RepoId, Result, Value, ValueType, Writer,
};
use borg_engine::Registry;
use borg_exec::ProducerCtx;
use borg_exec_native::NativeExecutor;
use borg_host::ops::{self, Ops};
use borg_host::stream;
use borg_storage_sqlite::SqliteStorage;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Computes `Company.score` from `Company.website`. An ordinary pipeline, and the source of the
/// derived data whose read-sets and watermarks the round trip has to preserve.
const SCORE: ProducerId = ProducerId(0x5c07e);
/// Computes `Company.risk` from `Company.flag`, and **throws on one particular input**. It succeeds
/// first, so it leaves cells behind that a later poisoning re-labels — which is the case §14 can
/// actually show, since a producer that has never succeeded has no cell to call `broken`
/// (`CLAUDE.md`).
const BOOM: ProducerId = ProducerId(0xb0011);
/// `up` for `Company.website`, appointed by the def-mutation on the fork.
const UP: ProducerId = ProducerId(0x11);

/// The value that makes `BOOM` throw, written into `Company.flag` last of all — so the fixture has a
/// producer that worked and then stopped, which is the only shape §14 can actually show.
const CURSED: i64 = 13;

fn company(counter: u64) -> Pid {
    Pid::Allocated {
        kind: PidKind::Object,
        branch: BranchId(1),
        allocator: AllocatorId(0),
        counter,
    }
}

fn prop(counter: u64, field: &str) -> CellRef {
    CellRef::prop("Company".into(), field.into(), company(counter))
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "borg-stream-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn ops_for(store: &Path, branch: Option<&str>) -> Ops {
    Ops {
        store: store.to_path_buf(),
        branch: branch.map(str::to_string),
        version: None,
        freshness: FreshnessRequirement::Validated,
        settled: false,
        held: None,
    }
}

/// A registry over `store` with the fixture's producers wired in.
///
/// The poison table comes from beside the store, exactly as `ops::open` builds it, because that is
/// what makes a poisoning outlive the process that discovered it (§14) — and therefore what makes it
/// something an export can carry.
async fn deriving(store: &Path) -> Result<Registry> {
    let storage = Arc::new(SqliteStorage::open(store)?);
    let executor = Arc::new(NativeExecutor::new());
    executor.register(
        SCORE,
        Arc::new(|ctx: &mut dyn ProducerCtx, input: Pid| {
            Box::pin(async move {
                let website = CellRef::prop("Company".into(), "website".into(), input);
                let Some(Value::Int(n)) = ctx.get(&website).await? else {
                    return Ok(());
                };
                ctx.set(
                    &CellRef::prop("Company".into(), "score".into(), input),
                    Value::Int(n * 2),
                )
                .await
            })
        }),
    );
    executor.register(
        BOOM,
        Arc::new(|ctx: &mut dyn ProducerCtx, input: Pid| {
            Box::pin(async move {
                let flag = CellRef::prop("Company".into(), "flag".into(), input);
                let Some(Value::Int(n)) = ctx.get(&flag).await? else {
                    return Ok(());
                };
                if n == CURSED {
                    return Err(borg_core::BorgError::Execution(
                        "the risk model does not believe in 13".into(),
                    ));
                }
                ctx.set(
                    &CellRef::prop("Company".into(), "risk".into(), input),
                    Value::Int(n + 1),
                )
                .await
            })
        }),
    );
    executor.register(
        UP,
        Arc::new(|ctx: &mut dyn ProducerCtx, input: Pid| {
            Box::pin(async move {
                let cell = CellRef::prop("Company".into(), "website".into(), input);
                let Some(Value::Int(n)) = ctx.get_input(&cell).await? else {
                    return Ok(());
                };
                ctx.set(&cell, Value::Int(n * 100)).await
            })
        }),
    );
    Registry::open_with_poison(
        storage,
        executor,
        Arc::new(ops::FilePoison::new(&ops_for(store, None))),
    )
    .await
}

fn pipeline(id: ProducerId) -> ProducerDef {
    ProducerDef {
        id,
        kind: ProducerKind::Pipeline,
        source: borg_core::BufferId::Object("Company".into()),
        version: LayerId(0),
        declaring_repo: RepoId(1),
        fingerprint: Some(format!("test:{}", id.0)),
    }
}

fn migration(id: ProducerId) -> ProducerDef {
    ProducerDef {
        id,
        kind: ProducerKind::Migration {
            direction: MigrationDirection::Up,
        },
        source: borg_core::BufferId::ObjectProp("Company".into(), "website".into()),
        version: LayerId(0),
        declaring_repo: RepoId(1),
        fingerprint: Some("test:up".into()),
    }
}

/// Everything the round trip has to carry, in one store.
///
/// Written through the engine rather than through `ops::` so that the producers are real Rust
/// closures rather than subprocesses — this test is about the *stream*, and it should not fail on a
/// machine without a shell. `scenarios/320-export-and-import` drives the same feature through the
/// real binaries with a real pipeline; the two are deliberately different routes to the same claim.
async fn build_fixture(store: &Path) -> Result<()> {
    let base = ops_for(store, None);
    ops::init(&base).await?;
    let main = BranchId(1);

    let registry = deriving(store).await?;
    registry
        .defs
        .push(
            main,
            vec![
                DefEvent::DeclareField {
                    struct_name: "Company".into(),
                    field: "website".into(),
                    ty: ValueType::Int,
                    repo: RepoId(1),
                    ownership: Ownership::Source,
                },
                DefEvent::DeclareField {
                    struct_name: "Company".into(),
                    field: "name".into(),
                    ty: ValueType::String,
                    repo: RepoId(1),
                    ownership: Ownership::Source,
                },
                // BOOM's input, and it is a **different field from the migrated one** on purpose.
                // A value is stored at the def-version of its own field and never at the writer's
                // whole-schema ClientVersion (invariant 7), so a write to `flag` still matches the
                // dependency BOOM recorded on `flag` however many times some *other* field has been
                // mutated. That is what lets the fixture break a producer at the very end, after the
                // schema has moved elsewhere.
                DefEvent::DeclareField {
                    struct_name: "Company".into(),
                    field: "flag".into(),
                    ty: ValueType::Int,
                    repo: RepoId(1),
                    ownership: Ownership::Source,
                },
                DefEvent::DeclareField {
                    struct_name: "Company".into(),
                    field: "score".into(),
                    ty: ValueType::Int,
                    repo: RepoId(1),
                    ownership: Ownership::Derived(SCORE),
                },
                DefEvent::DeclareField {
                    struct_name: "Company".into(),
                    field: "risk".into(),
                    ty: ValueType::Int,
                    repo: RepoId(1),
                    ownership: Ownership::Derived(BOOM),
                },
                DefEvent::PushProducer(pipeline(SCORE)),
                DefEvent::PushProducer(pipeline(BOOM)),
                DefEvent::PushProducer(migration(UP)),
            ],
        )
        .await?;
    registry.register_producers(main).await?;

    // Two companies with **the same name**, which is one interned value referenced twice: the whole
    // point of content addressing, and something a stream carrying bytes per reference would quietly
    // turn back into two.
    let acme = registry
        .values
        .intern(borg_core::ValueInput::string("acme.ai"))
        .await?;
    write(
        &registry,
        main,
        vec![
            (prop(1, "website"), Value::Int(4)),
            (prop(1, "name"), acme),
            (prop(1, "flag"), Value::Int(1)),
            (prop(2, "website"), Value::Int(7)),
            (prop(2, "name"), acme),
            (prop(2, "flag"), Value::Int(2)),
        ],
    )
    .await?;
    registry.engine.catch_up(main).await?;

    // A fork with data of its own, merged back **with** its data — which is what makes the parent's
    // merge layer *name* events authored on the child rather than copy them (§13). In the stream
    // those are the membership rows with no event record beside them, and they are the reason import
    // has to adopt event ids rather than mint new ones.
    let feature = registry
        .branches
        .fork(
            main,
            registry.layers.head(main).expect("main has a head"),
            Some("feature".into()),
        )
        .await?;
    write(
        &registry,
        feature,
        vec![
            (prop(3, "website"), Value::Int(5)),
            (prop(3, "name"), acme),
            (prop(3, "flag"), Value::Int(3)),
        ],
    )
    .await?;
    registry.engine.catch_up(feature).await?;
    registry
        .branches
        .merge(feature, MergeMode::DefAndData)
        .await?;
    registry.engine.catch_up(main).await?;

    // A second fork carrying a **def-mutation with a migration**, caught up but deliberately left
    // unmerged. That materializes `website` at a second def-version over there while the first is
    // untouched everywhere (§5.4) — and leaving it unmerged is what keeps main's ClientVersion
    // still, so the poisoning below stays recorded rather than retiring itself against a schema
    // that moved after it (§14).
    let schema = registry
        .branches
        .fork(
            main,
            registry.layers.head(main).expect("main has a head"),
            Some("schema".into()),
        )
        .await?;
    registry
        .defs
        .push(
            schema,
            vec![DefEvent::MutateField {
                struct_name: "Company".into(),
                field: "website".into(),
                ty: ValueType::AnyNumber,
                repo: RepoId(1),
                up: UP,
                down: None,
            }],
        )
        .await?;
    registry.register_producers(schema).await?;
    registry.engine.catch_up(schema).await?;

    // …and now break one producer, after it has already succeeded — the only case §14 can show,
    // since a producer that has never succeeded has no cell to call `broken`. `catch_up` reports the
    // count rather than failing: a broken pipeline is not a broken write.
    write(&registry, main, vec![(prop(2, "flag"), Value::Int(CURSED))]).await?;
    let _ = registry.engine.catch_up(main).await;
    assert!(
        !registry.engine.broken(main)?.is_empty(),
        "the fixture needs a poisoned producer, and BOOM did not throw"
    );
    drop(registry);

    // The two sidecars nothing above touches, set the way the commands that own them would.
    ops::set_paused(&base, schema, true)?;
    ops::save_allocations(&base, &ops::Allocations { next: 77 })?;
    Ok(())
}

async fn write(
    registry: &Registry,
    branch: BranchId,
    writes: Vec<(CellRef, Value)>,
) -> Result<LayerId> {
    let path = registry.branches.read_path(branch, None)?;
    let version = ClientVersion(registry.defs.head(&path));
    let mut session = registry
        .begin_write(branch, version, Writer::Client)
        .await?;
    for (cell, value) in writes {
        session.set(&cell, value).await?;
    }
    session.commit().await
}

// --- Asking the two stores the same questions ---------------------------------------------------

/// Every cell the fixture can be asked about, spelled the way a client would spell it.
fn cells() -> Vec<String> {
    let mut all = Vec::new();
    for counter in [1u64, 2, 3, 4] {
        all.push(CellRef::existence("Company".into(), company(counter)).to_string());
        for field in ["website", "name", "flag", "score", "risk"] {
            all.push(prop(counter, field).to_string());
        }
    }
    all
}

/// One cell's whole envelope, as a line. Everything §10.4 puts on a read, so a difference in *any*
/// of it — the value, where it was authored, where it landed, what it reflects, its state, the
/// producer that made it — fails the comparison rather than only a difference in the value.
async fn envelope(store: &Path, branch: &str, cell: &str) -> String {
    let args = ops_for(store, Some(branch));
    match ops::get(&args, cell).await {
        Ok(read) => format!(
            "{} = {:?} interned={:?} origin={:?} state={:?} by={:?} authored={} landed={} \
             reflects={} event={:?}",
            read.cell,
            read.rendered,
            read.interned,
            read.resolved.origin,
            read.resolved.state,
            read.resolved.by,
            read.resolved.authored_at,
            read.resolved.landed_at,
            read.resolved.fresh_as_of,
            read.resolved.event,
        ),
        Err(err) => format!("{cell} !! {err}"),
    }
}

async fn explained(store: &Path, branch: &str, cell: &str) -> String {
    let args = ops_for(store, Some(branch));
    match ops::explain(&args, cell).await {
        Ok((cell, lineage)) => format!("{cell} <- {lineage:?}"),
        Err(err) => format!("{cell} !! {err}"),
    }
}

async fn defs_of(store: &Path, branch: &str) -> String {
    let args = ops_for(store, Some(branch));
    match ops::def_view(&args).await {
        Ok((version, structs)) => format!("{version} {structs:?}"),
        Err(err) => format!("!! {err}"),
    }
}

async fn export_to(store: &Path, file: &Path) -> Result<stream::Exported> {
    let handle = std::fs::File::create(file).unwrap();
    let mut out = std::io::BufWriter::new(handle);
    stream::export(&ops_for(store, None), &mut out).await
}

/// The fixture, exported, and imported into a fresh store beside it. Returns both stores.
async fn round_trip(name: &str) -> (PathBuf, PathBuf, PathBuf) {
    let dir = temp_dir(name);
    let original = dir.join("original.db");
    let stream_file = dir.join("original.ndjson");
    let restored = dir.join("restored.db");

    build_fixture(&original).await.expect("fixture builds");
    export_to(&original, &stream_file).await.expect("exports");
    let handle = std::fs::File::open(&stream_file).unwrap();
    let mut input = std::io::BufReader::new(handle);
    stream::import(&restored, &mut input)
        .await
        .expect("imports");
    (original, restored, dir)
}

/// **The headline claim.** Every cell, on every branch, answers identically — the whole envelope and
/// not merely the value, because the labels are where the sidecar decisions show up.
#[tokio::test]
async fn every_cell_on_every_branch_reads_identically_after_a_round_trip() {
    let (original, restored, dir) = round_trip("envelopes").await;
    let mut checked = 0;
    for branch in ["main", "feature", "schema"] {
        for cell in cells() {
            assert_eq!(
                envelope(&original, branch, &cell).await,
                envelope(&restored, branch, &cell).await,
                "{branch}: {cell}"
            );
            checked += 1;
        }
    }
    assert!(checked > 20, "the fixture should have real breadth");

    // The fixture is only worth anything if it actually contains the hard cases, so this asserts the
    // fixture rather than the round trip: a derived value, and one whose producer is poisoned.
    let score = envelope(&restored, "main", &prop(1, "score").to_string()).await;
    assert!(score.contains("Derived"), "{score}");
    assert!(
        score.contains("value: Some(\"8\")") || score.contains("Some(\"8\")"),
        "{score}"
    );
    let risk = envelope(&restored, "main", &prop(2, "risk").to_string()).await;
    assert!(
        risk.contains("Broken"),
        "a poisoning that did not survive the round trip would read `Stale` here: {risk}"
    );

    // …and the migration. `website` is one cell materialized at two def-versions, and the two
    // branches read it through different lenses — which is only a meaningful round-trip test if the
    // restore really does answer differently on the two branches.
    let plain = envelope(&restored, "main", &prop(1, "website").to_string()).await;
    let migrated = envelope(&restored, "schema", &prop(1, "website").to_string()).await;
    assert_ne!(
        plain, migrated,
        "the fixture needs a field materialized at two def-versions"
    );
    assert!(migrated.contains("Derived"), "{migrated}");

    // The merge. One event, named by a layer on each branch — so it reports the same identity from
    // both sides, and the round trip has carried a membership row rather than a copy (§13).
    let on_child = envelope(&restored, "feature", &prop(3, "flag").to_string()).await;
    let on_parent = envelope(&restored, "main", &prop(3, "flag").to_string()).await;
    assert!(on_parent.contains("event=Some"), "{on_parent}");
    assert_eq!(
        on_child.split("event=").nth(1),
        on_parent.split("event=").nth(1),
        "a merged value is one event named twice, not a copy on each side"
    );

    std::fs::remove_dir_all(dir).unwrap();
}

/// Lineage is the dependency index read backwards (§11), and the index is rebuilt from the log on
/// open — so this is the check that the *read-sets* came across, not just the values.
#[tokio::test]
async fn lineage_is_identical_after_a_round_trip() {
    let (original, restored, dir) = round_trip("lineage").await;
    for branch in ["main", "feature", "schema"] {
        for cell in cells() {
            assert_eq!(
                explained(&original, branch, &cell).await,
                explained(&restored, branch, &cell).await,
                "{branch}: {cell}"
            );
        }
    }
    // Again, an assertion about the fixture: `explain` on a poisoned cell is where §14's reason is
    // actually said, and it comes out of the sidecar this stream carries.
    let risk = explained(&restored, "main", &prop(2, "risk").to_string()).await;
    assert!(
        risk.contains("does not believe in 13"),
        "the poisoning's reason has to survive too: {risk}"
    );
    std::fs::remove_dir_all(dir).unwrap();
}

/// Definitions travel the log, so this is really a check that def layers and their event order came
/// across — including the mutation that only exists on the fork and the def-only merge that carried
/// it to the parent.
#[tokio::test]
async fn def_views_are_identical_after_a_round_trip() {
    let (original, restored, dir) = round_trip("defs").await;
    for branch in ["main", "feature", "schema"] {
        assert_eq!(
            defs_of(&original, branch).await,
            defs_of(&restored, branch).await,
            "{branch}"
        );
    }
    std::fs::remove_dir_all(dir).unwrap();
}

/// The sidecar decisions, asserted one at a time — because each of them was a decision and a silent
/// regression in any of them looks like nothing until somebody restores a backup.
#[tokio::test]
async fn the_sidecars_that_are_state_come_across_and_the_ones_that_are_not_do_not() {
    let (original, restored, dir) = round_trip("sidecars").await;
    let there = ops_for(&original, None);
    let here = ops_for(&restored, None);

    assert_eq!(
        ops::load_allocations(&here).next,
        ops::load_allocations(&there).next,
        "the PID counter is the one sidecar a store cannot recover from"
    );
    assert_eq!(ops::load_allocations(&here).next, 77);

    assert_eq!(
        ops::paused_branches(&here),
        ops::paused_branches(&there),
        "a branch someone paused on purpose must not come back deriving"
    );
    assert!(!ops::paused_branches(&here).is_empty(), "fixture check");

    let broken = ops::load_derivation(&here).broken;
    assert_eq!(broken.len(), ops::load_derivation(&there).broken.len());
    assert!(!broken.is_empty(), "fixture check");
    assert_eq!(broken[0].producer, BOOM.0);
    assert_eq!(
        broken[0].version,
        ops::load_derivation(&there).broken[0].version,
        "a poisoning is keyed on the ClientVersion it was recorded against, and that is what makes \
         it self-expiring — carrying the row without the version would carry a record that never \
         retires"
    );

    // And the one that is deliberately absent. A restored registry has no open transactions,
    // because a transaction is ephemeral by decree (§12.3) and nobody can be holding a handle to a
    // registry that did not exist a moment ago.
    assert!(
        ops::load_transactions(&here).open.is_empty(),
        "open transactions are residue, not state"
    );
    assert_eq!(
        ops::load_transactions(&here).tx_idle_timeout,
        ops::load_transactions(&there).tx_idle_timeout,
        "…but the timeout is a knob somebody set, and that is state"
    );

    std::fs::remove_dir_all(dir).unwrap();
}

/// **The cheap total check.** Export the import and require the same bytes.
///
/// It compares everything at once, including everything nobody thought to assert above — and it is
/// what the header's deliberate emptiness buys: no timestamp, no registry name, no path, so two
/// exports of one registry differ only if the registry does.
#[tokio::test]
async fn exporting_the_import_reproduces_the_stream_byte_for_byte() {
    let (_original, restored, dir) = round_trip("bytes").await;
    let again = dir.join("restored.ndjson");
    let report = export_to(&restored, &again).await.expect("exports");

    let first = std::fs::read(dir.join("original.ndjson")).unwrap();
    let second = std::fs::read(&again).unwrap();
    assert_eq!(
        String::from_utf8_lossy(&first),
        String::from_utf8_lossy(&second),
        "a registry and its restore are the same registry, so they export the same bytes"
    );
    assert!(report.layers > 5, "the fixture should have real depth");
    assert!(
        report.interned >= 1,
        "one interned string, shared by two cells"
    );
    std::fs::remove_dir_all(dir).unwrap();
}

/// A restored registry is not a museum piece: the next write has to land in the same layer, derive
/// the same way, and answer the same thing. This is the check that ids were *adopted* rather than
/// merely stored — a store whose sequencers had not been advanced past the import would mint an id
/// that already exists.
#[tokio::test]
async fn a_further_write_and_derive_behaves_identically_on_both() {
    let (original, restored, dir) = round_trip("carrying-on").await;

    let mut answers = Vec::new();
    for store in [&original, &restored] {
        let registry = deriving(store).await.expect("opens");
        registry
            .register_producers(BranchId(1))
            .await
            .expect("registers");
        let landed = write(
            &registry,
            BranchId(1),
            vec![(prop(3, "website"), Value::Int(21))],
        )
        .await
        .expect("writes");
        let ran = registry
            .engine
            .catch_up(BranchId(1))
            .await
            .expect("derives");
        drop(registry);
        answers.push(format!(
            "landed={landed} ran={ran} {} {}",
            envelope(store, "main", &prop(3, "score").to_string()).await,
            envelope(store, "main", &prop(3, "risk").to_string()).await,
        ));
    }
    assert_eq!(
        answers[0], answers[1],
        "the same write on a registry and on its restore must land in the same layer and derive \
         the same values"
    );
    assert!(
        answers[0].contains("landed=L"),
        "fixture check: {}",
        answers[0]
    );
    std::fs::remove_dir_all(dir).unwrap();
}

/// A stream nobody can read is refused with the line number and what was expected, and a stream from
/// a format version this binary does not know names **both** versions. A backup discovered to be
/// broken is discovered by somebody who has to fix it.
#[tokio::test]
async fn a_malformed_or_future_stream_is_refused_by_name() {
    let dir = temp_dir("malformed");
    let good = dir.join("good.ndjson");
    let source = dir.join("source.db");
    build_fixture(&source).await.expect("fixture builds");
    export_to(&source, &good).await.expect("exports");
    let lines: Vec<String> = std::fs::read_to_string(&good)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect();

    let missing_header = refuse(&dir, "noheader", lines[1..].join("\n")).await;
    assert!(missing_header.contains("header"), "{missing_header}");

    let future = refuse(
        &dir,
        "future",
        format!(
            "{}\n{}",
            r#"{"header":{"version":9999,"binary":"borg 99.0.0"}}"#,
            lines[1..].join("\n")
        ),
    )
    .await;
    assert!(future.contains("9999"), "{future}");
    assert!(
        future.contains(&stream::STREAM_VERSION.to_string()),
        "a version refusal names both versions, or the reader cannot tell which end to fix: \
         {future}"
    );

    // A line mangled in the middle: the error says which line, which is the whole reason the format
    // is line-oriented in the first place.
    let mut torn = lines.clone();
    let at = torn.len() / 2;
    torn[at] = "{\"event\":{\"id\":\"not a number\"}}".to_string();
    let mangled = refuse(&dir, "torn", torn.join("\n")).await;
    assert!(
        mangled.contains(&format!("line {}", at + 1)),
        "expected the offending line number in: {mangled}"
    );

    // A record nothing has opened a layer for. The one structural rule the format has — a `layer`
    // record opens a block and everything until the next one belongs to it — said back as a refusal
    // rather than assumed.
    let orphan = refuse(
        &dir,
        "orphan",
        format!("{}\n{{\"member\":{{\"event\":1}}}}", lines[0]),
    )
    .await;
    assert!(orphan.contains("`layer` record"), "{orphan}");

    std::fs::remove_dir_all(dir).unwrap();
}

/// Import a stream that is not one, and say what came back.
///
/// A free function rather than a closure because it has to be `async`, and one place rather than
/// four because the four failures below should read alike — a refusal that names a line and what was
/// expected is the behaviour under test, not the fact of failing.
async fn refuse(dir: &Path, name: &str, text: String) -> String {
    let store = dir.join(format!("{name}.db"));
    let mut input = std::io::BufReader::new(std::io::Cursor::new(text));
    let err = stream::import(&store, &mut input).await;
    let _ = std::fs::remove_file(&store);
    err.err()
        .map(|err| err.to_string())
        .expect("this stream must be refused")
}

/// Importing into a registry that already holds anything is refused. Restore is create-then-import,
/// and the alternative — merging two id spaces — would mean either renaming events, which
/// invalidates every read-set in the stream, or colliding with them.
#[tokio::test]
async fn importing_into_a_registry_that_holds_anything_is_refused() {
    let dir = temp_dir("occupied");
    let source = dir.join("source.db");
    let file = dir.join("source.ndjson");
    build_fixture(&source).await.expect("fixture builds");
    export_to(&source, &file).await.expect("exports");

    let occupied = dir.join("occupied.db");
    ops::init(&ops_for(&occupied, None)).await.expect("inits");

    let handle = std::fs::File::open(&file).unwrap();
    let mut input = std::io::BufReader::new(handle);
    let refusal = stream::import(&occupied, &mut input)
        .await
        .expect_err("must be refused")
        .to_string();
    assert!(refusal.contains("already holds a registry"), "{refusal}");
    assert!(
        refusal.contains("borg init"),
        "the refusal should say what to do instead: {refusal}"
    );
    std::fs::remove_dir_all(dir).unwrap();
}
