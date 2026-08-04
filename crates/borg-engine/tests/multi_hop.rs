//! The motivating example: a pipeline with random access across the graph. SPEC.md §9.2.
//!
//! Written before the implementation. Every other pipeline test reads one or two fields directly off
//! its input; this one hops:
//!
//! ```text
//! company.founders[].last_education().school().is_top_ten
//! ```
//!
//! Four levels deep, through two lists, ending in a `School` that many companies share. Field-level
//! tracking either survives this or the design does not work.

use borg_core::{
    AllocatorId, BranchId, BufferId, CellRecord, CellRef, ClientVersion, DefEvent, LayerAuthor,
    LayerId, ObjectTypeName, Ownership, Pid, PidKind, ProducerDef, ProducerId, ProducerKind,
    RepoId, Result, Value, ValueType, Writer,
};
use borg_engine::{
    BranchManager, CellTouchIndex, DefRegistry, DerivationEngine, FrontierTracker,
    InProcessSequencer, LayerManager, MemoryDependencyIndex, WriteSession,
};
use borg_exec::ProducerCtx;
use borg_exec_native::NativeExecutor;
use borg_storage::{MemoryStorage, StorageProvider};
use std::sync::Arc;

const SCORE: ProducerId = ProducerId(1);
const V1: ClientVersion = ClientVersion(LayerId(1));

fn obj(kind: PidKind, n: u64) -> Pid {
    Pid::Allocated {
        kind,
        branch: BranchId(1),
        allocator: AllocatorId(0),
        counter: n,
    }
}

fn company(n: u64) -> Pid {
    obj(PidKind::Object, 1_000 + n)
}
fn founder(n: u64) -> Pid {
    obj(PidKind::Object, 2_000 + n)
}
fn education(n: u64) -> Pid {
    obj(PidKind::Object, 3_000 + n)
}
fn school(n: u64) -> Pid {
    obj(PidKind::Object, 4_000 + n)
}
fn list(n: u64) -> Pid {
    obj(PidKind::List, 5_000 + n)
}

fn prop(struct_name: &str, pid: Pid, field: &str) -> CellRef {
    CellRef::prop(struct_name.into(), field.into(), pid)
}

fn exists(struct_name: &str, pid: Pid) -> CellRef {
    CellRef::existence(struct_name.into(), pid)
}

/// A list's own cell. Its value is the list's length, so appending changes it — and therefore
/// invalidates anyone who iterated (SPEC.md §4.2).
fn list_len(element: &str, pid: Pid) -> CellRef {
    CellRef::list(element.into(), pid)
}

fn elem(element: &str, pid: Pid, index: u64) -> CellRef {
    CellRef::elem(element.into(), pid, index)
}

/// `should_invest_in_startup`, hopping the whole way down.
fn invest_pipeline() -> borg_exec_native::ProducerFn {
    Arc::new(|ctx: &mut dyn ProducerCtx, input: Pid| {
        Box::pin(async move {
            let mut score = 0i64;

            if let Some(Value::Int(reach)) = ctx.get(&prop("Company", input, "website")).await?
                && reach > 3
            {
                score += 3;
            }

            if let Some(Value::Ref(founders)) = ctx.get(&prop("Company", input, "founders")).await?
                && let Some(Value::Int(count)) = ctx.get(&list_len("Founder", founders)).await?
            {
                for i in 0..count as u64 {
                    let Some(Value::Ref(f)) = ctx.get(&elem("Founder", founders, i)).await? else {
                        continue;
                    };
                    let Some(Value::Ref(educations)) =
                        ctx.get(&prop("Founder", f, "educations")).await?
                    else {
                        continue;
                    };
                    // `last_education()` — only the final element is read, so an earlier one
                    // changing must not recompute anything.
                    let Some(Value::Int(depth)) =
                        ctx.get(&list_len("Education", educations)).await?
                    else {
                        continue;
                    };
                    if depth == 0 {
                        continue;
                    }
                    let Some(Value::Ref(edu)) = ctx
                        .get(&elem("Education", educations, depth as u64 - 1))
                        .await?
                    else {
                        continue;
                    };
                    let Some(Value::Ref(sch)) = ctx.get(&prop("Education", edu, "school")).await?
                    else {
                        continue;
                    };
                    if let Some(Value::Bool(true)) =
                        ctx.get(&prop("School", sch, "is_top_ten")).await?
                    {
                        score += 3;
                    }
                }
            }

            ctx.set(
                &prop("Company", input, "is_investible"),
                Value::Bool(score > 5),
            )
            .await
        })
    })
}

struct Harness {
    layers: Arc<LayerManager>,
    branches: Arc<BranchManager>,
    engine: Arc<DerivationEngine>,
    storage: Arc<MemoryStorage>,
    branch: BranchId,
    defs: Arc<DefRegistry>,
}

impl Harness {
    async fn new() -> Self {
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
        executor.register(SCORE, invest_pipeline());

        let engine = Arc::new(DerivationEngine::new(
            storage.clone(),
            layers.clone(),
            index,
            executor,
            Arc::new(FrontierTracker::new()),
            defs.clone(),
            branches.clone(),
        ));
        engine.register(ProducerDef {
            id: SCORE,
            kind: ProducerKind::Pipeline,
            source: BufferId::Object("Company".into()),
            version: LayerId(1),
            declaring_repo: RepoId(1),
        });

        let branch = branches.create_root(Some("main".into())).await.unwrap();
        // Four structs, declared before anything is written to them (SPEC.md §8). The list-typed
        // fields are what the hops travel along.
        defs.push(branch, schema()).await.unwrap();
        Self {
            layers,
            branches,
            engine,
            storage,
            branch,
            defs,
        }
    }

    async fn push(&self, writes: Vec<(CellRef, Value)>) -> Result<LayerId> {
        let mut session = WriteSession::open(
            &self.layers,
            &self.defs,
            self.branch,
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

    async fn read(&self, cell: &CellRef) -> Result<Option<CellRecord>> {
        let path = self.branches.read_path(self.branch, None)?;
        self.storage.get_cell(&path, cell, V1).await
    }

    async fn investible(&self, c: Pid) -> Result<Option<Value>> {
        Ok(self
            .read(&prop("Company", c, "is_investible"))
            .await?
            .map(|r| r.value))
    }
}

fn schema() -> Vec<DefEvent> {
    let declare = |struct_name: &str, field: &str, ty: ValueType, ownership: Ownership| {
        DefEvent::DeclareField {
            struct_name: struct_name.into(),
            field: field.into(),
            ty,
            repo: RepoId(1),
            ownership,
        }
    };
    let list_of = |name: &str| ValueType::List(Box::new(ValueType::Object(name.into())));
    vec![
        declare("Company", "website", ValueType::Int, Ownership::Source),
        declare("Company", "founders", list_of("Founder"), Ownership::Source),
        declare(
            "Company",
            "is_investible",
            ValueType::Bool,
            Ownership::Derived(SCORE),
        ),
        declare(
            "Founder",
            "educations",
            list_of("Education"),
            Ownership::Source,
        ),
        declare(
            "Education",
            "school",
            ValueType::Object("School".into()),
            Ownership::Source,
        ),
        declare("School", "is_top_ten", ValueType::Bool, Ownership::Source),
    ]
}

/// One founder, one education, at the given school.
fn founder_at(f: u64, school_id: u64) -> Vec<(CellRef, Value)> {
    let (founder, education, educations) = (founder(f), education(f), list(100 + f));
    vec![
        (exists("Founder", founder), Value::Bool(true)),
        (
            prop("Founder", founder, "educations"),
            Value::Ref(educations),
        ),
        (list_len("Education", educations), Value::Int(1)),
        (elem("Education", educations, 0), Value::Ref(education)),
        (
            prop("Education", education, "school"),
            Value::Ref(school(school_id)),
        ),
    ]
}

/// A company with a single founder.
fn company_with(c: u64, reach: i64, f: u64) -> Vec<(CellRef, Value)> {
    let (company, founders) = (company(c), list(c));
    let mut writes = vec![
        (exists("Company", company), Value::Bool(true)),
        (prop("Company", company, "website"), Value::Int(reach)),
        (prop("Company", company, "founders"), Value::Ref(founders)),
        (list_len("Founder", founders), Value::Int(1)),
        (elem("Founder", founders, 0), Value::Ref(founder(f))),
    ];
    writes.extend(founder_at(f, 1));
    writes
}

#[tokio::test]
async fn a_multi_hop_pipeline_captures_every_cell_it_traversed() -> Result<()> {
    let h = Harness::new().await;
    h.push(vec![(
        prop("School", school(1), "is_top_ten"),
        Value::Bool(true),
    )])
    .await?;
    h.push(company_with(1, 9, 1)).await?;
    h.engine.catch_up(h.branch).await?;

    assert_eq!(h.investible(company(1)).await?, Some(Value::Bool(true)));

    let derivation = h
        .read(&prop("Company", company(1), "is_investible"))
        .await?
        .and_then(|r| r.derivation)
        .expect("derived");
    let read: Vec<CellRef> = derivation.read_set.iter().map(|c| c.cell.clone()).collect();

    // Every level of the hop appears, including the terminal cell on an object four hops away that
    // the pipeline never names directly.
    for expected in [
        prop("Company", company(1), "website"),
        prop("Company", company(1), "founders"),
        list_len("Founder", list(1)),
        elem("Founder", list(1), 0),
        prop("Founder", founder(1), "educations"),
        list_len("Education", list(101)),
        elem("Education", list(101), 0),
        prop("Education", education(1), "school"),
        prop("School", school(1), "is_top_ten"),
    ] {
        assert!(
            read.contains(&expected),
            "read-set is missing {expected:?} — dependency capture does not survive hops"
        );
    }
    Ok(())
}

#[tokio::test]
async fn flipping_a_shared_upstream_recomputes_exactly_its_dependents() -> Result<()> {
    let h = Harness::new().await;
    h.push(vec![
        (prop("School", school(1), "is_top_ten"), Value::Bool(true)),
        (prop("School", school(2), "is_top_ten"), Value::Bool(true)),
    ])
    .await?;

    // Two companies reach school 1; the third reaches school 2.
    h.push(company_with(1, 9, 1)).await?;
    h.push(company_with(2, 9, 2)).await?;
    let mut third = company_with(3, 9, 3);
    third.extend(founder_at(3, 2));
    h.push(third).await?;
    h.engine.catch_up(h.branch).await?;

    for c in 1..=3 {
        assert_eq!(h.investible(company(c)).await?, Some(Value::Bool(true)));
    }

    // One school, four hops upstream of two companies, changes.
    h.push(vec![(
        prop("School", school(1), "is_top_ten"),
        Value::Bool(false),
    )])
    .await?;

    assert_eq!(
        h.engine.catch_up(h.branch).await?,
        2,
        "exactly the two companies that traversed school 1 recompute — fan-out is precise even \
         four hops from the change"
    );
    assert_eq!(h.investible(company(1)).await?, Some(Value::Bool(false)));
    assert_eq!(h.investible(company(2)).await?, Some(Value::Bool(false)));
    assert_eq!(
        h.investible(company(3)).await?,
        Some(Value::Bool(true)),
        "the company reaching a different school is untouched"
    );
    Ok(())
}

#[tokio::test]
async fn appending_to_a_traversed_list_recomputes_the_iterator() -> Result<()> {
    let h = Harness::new().await;
    h.push(vec![
        (prop("School", school(1), "is_top_ten"), Value::Bool(true)),
        (prop("School", school(2), "is_top_ten"), Value::Bool(true)),
    ])
    .await?;
    // Website alone is not enough; one top-ten founder tips it over.
    h.push(company_with(1, 0, 1)).await?;
    h.engine.catch_up(h.branch).await?;
    assert_eq!(h.investible(company(1)).await?, Some(Value::Bool(false)));

    // Append a second founder. The list's own cell holds its length, so the append changes a cell
    // the pipeline read — which is what makes iteration a tracked dependency.
    let mut append = vec![
        (list_len("Founder", list(1)), Value::Int(2)),
        (elem("Founder", list(1), 1), Value::Ref(founder(9))),
    ];
    append.extend(founder_at(9, 2));
    h.push(append).await?;

    assert_eq!(h.engine.catch_up(h.branch).await?, 1);
    assert_eq!(
        h.investible(company(1)).await?,
        Some(Value::Bool(true)),
        "appending to a list the pipeline iterated recomputes it"
    );
    Ok(())
}

#[tokio::test]
async fn an_untraversed_element_changing_recomputes_nothing() -> Result<()> {
    let h = Harness::new().await;
    h.push(vec![(
        prop("School", school(1), "is_top_ten"),
        Value::Bool(true),
    )])
    .await?;

    // Two educations; `last_education()` reads only the second.
    let mut writes = company_with(1, 9, 1);
    writes.extend([
        (list_len("Education", list(101)), Value::Int(2)),
        (elem("Education", list(101), 1), Value::Ref(education(50))),
        (
            prop("Education", education(50), "school"),
            Value::Ref(school(1)),
        ),
    ]);
    h.push(writes).await?;
    h.engine.catch_up(h.branch).await?;
    assert_eq!(h.investible(company(1)).await?, Some(Value::Bool(true)));

    // Change the school on the education that was *not* the last one.
    h.push(vec![(
        prop("Education", education(1), "school"),
        Value::Ref(school(2)),
    )])
    .await?;

    assert_eq!(
        h.engine.catch_up(h.branch).await?,
        0,
        "an element the pipeline never dereferenced is not a dependency, however close it sits to \
         ones that are"
    );
    Ok(())
}

#[tokio::test]
async fn a_hop_through_a_missing_link_still_recomputes_when_the_link_appears() -> Result<()> {
    let h = Harness::new().await;
    // A company whose founder has no education yet: the hop runs out partway.
    h.push(vec![
        (exists("Company", company(1)), Value::Bool(true)),
        (prop("Company", company(1), "website"), Value::Int(9)),
        (prop("Company", company(1), "founders"), Value::Ref(list(1))),
        (list_len("Founder", list(1)), Value::Int(1)),
        (elem("Founder", list(1), 0), Value::Ref(founder(1))),
        (exists("Founder", founder(1)), Value::Bool(true)),
    ])
    .await?;
    h.engine.catch_up(h.branch).await?;
    assert_eq!(h.investible(company(1)).await?, Some(Value::Bool(false)));

    // The missing link arrives. The absent read must have been recorded, or nothing fires.
    h.push(vec![
        (
            prop("Founder", founder(1), "educations"),
            Value::Ref(list(101)),
        ),
        (list_len("Education", list(101)), Value::Int(1)),
        (elem("Education", list(101), 0), Value::Ref(education(1))),
        (
            prop("Education", education(1), "school"),
            Value::Ref(school(1)),
        ),
        (prop("School", school(1), "is_top_ten"), Value::Bool(true)),
    ])
    .await?;

    assert_eq!(h.engine.catch_up(h.branch).await?, 1);
    assert_eq!(
        h.investible(company(1)).await?,
        Some(Value::Bool(true)),
        "a hop that dead-ended is still a dependency on the cell it died at"
    );
    Ok(())
}

/// Producers map over a struct's `ObjectBuffer` (SPEC.md §4.2), and only `Company` has one here —
/// so the intermediate structs must never spawn invocations of their own.
#[tokio::test]
async fn intermediate_objects_do_not_become_pipeline_inputs() -> Result<()> {
    let h = Harness::new().await;
    h.push(vec![(
        prop("School", school(1), "is_top_ten"),
        Value::Bool(true),
    )])
    .await?;
    h.push(company_with(1, 9, 1)).await?;

    assert_eq!(
        h.engine.catch_up(h.branch).await?,
        1,
        "one company, one invocation — founders, educations and schools are traversed, not mapped"
    );

    let obj_types: [ObjectTypeName; 3] = ["Founder".into(), "Education".into(), "School".into()];
    for name in obj_types {
        assert!(
            h.read(&CellRef::prop(
                name.clone(),
                "is_investible".into(),
                founder(1)
            ))
            .await?
            .is_none(),
            "{name} was not mapped over, so it has no derived output"
        );
    }
    Ok(())
}
