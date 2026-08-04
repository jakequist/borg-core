//! Writes validated against the definitions in force. SPEC.md §5.1, §8.
//!
//! This is the milestone that makes definitions load-bearing: before it, `ValueType` had never
//! rejected a write and `borg set 'Wombat#1.nonsense' 7` was accepted. Every claim here is about
//! *what a write is allowed to be*, which is why they are all phrased as one.

use borg_core::{
    BorgError, BranchId, BufferId, CellRef, ClientVersion, DefEvent, DefVersion, LayerAuthor,
    LayerId, Ownership, Pid, PidKind, ProducerDef, ProducerId, ProducerKind, RepoId, Result, Value,
    ValueType, WriteRejection, Writer,
};
use borg_engine::{
    BranchManager, CellTouchIndex, DefRegistry, DerivationEngine, FrontierTracker,
    InProcessSequencer, LayerManager, MemoryDependencyIndex, WriteSession,
};
use borg_exec::ProducerCtx;
use borg_exec_native::NativeExecutor;
use borg_storage::MemoryStorage;
use std::sync::Arc;

const REPO: RepoId = RepoId(1);
const INVEST: ProducerId = ProducerId(1);
const OTHER: ProducerId = ProducerId(2);
/// Every actor in these tests is authored against the store's initial def-view (SPEC.md §5.4).
const V1: ClientVersion = ClientVersion(LayerId(1));
/// The def-version every field in these tests sits at. One declaration, one def-layer, nothing
/// mutated since — so this is where the records are keyed, whatever any actor's whole-schema view
/// has moved on to (SPEC.md §5.3).
const AT_V1: DefVersion = DefVersion(LayerId(1));

fn company(n: u64) -> Pid {
    Pid::Allocated {
        kind: PidKind::Object,
        branch: BranchId(1),
        allocator: borg_core::AllocatorId(0),
        counter: n,
    }
}

fn prop(pid: Pid, field: &str) -> CellRef {
    CellRef::prop("Company".into(), field.into(), pid)
}

struct Harness {
    branch: BranchId,
    layers: Arc<LayerManager>,
    defs: Arc<DefRegistry>,
    engine: Arc<DerivationEngine>,
    executor: Arc<NativeExecutor>,
}

impl Harness {
    async fn new() -> Result<Self> {
        let storage = Arc::new(MemoryStorage::new());
        let layers = Arc::new(LayerManager::new(
            storage.clone(),
            Arc::new(InProcessSequencer::new()),
            Arc::new(CellTouchIndex::new()),
        ));
        let branches = Arc::new(BranchManager::new(layers.clone()));
        let branch = branches.create_root(Some("main".into())).await?;
        let defs = Arc::new(DefRegistry::new(layers.clone(), storage.clone()));
        let executor = Arc::new(NativeExecutor::new());
        let engine = Arc::new(DerivationEngine::new(
            storage.clone(),
            layers.clone(),
            Arc::new(MemoryDependencyIndex::new()),
            executor.clone(),
            Arc::new(FrontierTracker::new()),
            defs.clone(),
            branches.clone(),
        ));

        // `website` is ground truth; `is_investible` is computed by one named producer. Declaring
        // ownership up front is what lets a wrong write be caught on its *first* attempt.
        defs.push(
            branch,
            vec![
                declare("website", ValueType::String, Ownership::Source),
                declare("headcount", ValueType::Int, Ownership::Source),
                declare("is_investible", ValueType::Bool, Ownership::Derived(INVEST)),
            ],
        )
        .await?;

        Ok(Self {
            branch,
            layers,
            defs,
            engine,
            executor,
        })
    }

    async fn session(&self, writer: Writer) -> Result<WriteSession> {
        self.session_at(V1, writer).await
    }

    async fn session_at(&self, version: ClientVersion, writer: Writer) -> Result<WriteSession> {
        WriteSession::open(
            &self.layers,
            &self.defs,
            self.branch,
            None,
            version,
            writer,
            LayerAuthor::Source,
        )
        .await
    }

    /// One client write, committed if it is accepted.
    async fn write(&self, cell: &CellRef, text: &str) -> Result<LayerId> {
        self.write_as(V1, Writer::Client, cell, text).await
    }

    /// The same, by a named actor at a named ClientVersion.
    async fn write_as(
        &self,
        version: ClientVersion,
        writer: Writer,
        cell: &CellRef,
        text: &str,
    ) -> Result<LayerId> {
        let mut session = self.session_at(version, writer).await?;
        match session.set_text(cell, text).await {
            Ok(()) => session.commit().await,
            Err(rejection) => {
                session.abort().await?;
                Err(rejection)
            }
        }
    }

    /// Register a producer that writes one cell, and run it over one company.
    async fn run_producer(&self, id: ProducerId, cell: CellRef, value: Value) -> Result<()> {
        self.executor.register(
            id,
            Arc::new(move |ctx: &mut dyn ProducerCtx, _input: Pid| {
                let (cell, value) = (cell.clone(), value);
                Box::pin(async move { ctx.set(&cell, value).await })
            }),
        );
        self.engine.register(ProducerDef {
            id,
            kind: ProducerKind::Pipeline,
            source: BufferId::Object("Company".into()),
            version: LayerId(1),
            declaring_repo: REPO,
        });
        self.engine.catch_up(self.branch).await?;
        // A producer failure poisons the producer rather than the branch (SPEC.md §14), so the
        // rejection is read back from there rather than from `catch_up`'s return.
        match self.engine.is_broken(self.branch, id) {
            Some(message) => Err(BorgError::Execution(message)),
            None => Ok(()),
        }
    }
}

fn declare(field: &str, ty: ValueType, ownership: Ownership) -> DefEvent {
    DefEvent::DeclareField {
        struct_name: "Company".into(),
        field: field.into(),
        ty,
        repo: REPO,
        ownership,
    }
}

/// The rejection behind an error, so a test can assert on *why* rather than on a message.
fn rejection(error: BorgError) -> WriteRejection {
    match error {
        BorgError::WriteRejected(rejection) => *rejection,
        other => panic!("expected a write rejection, got {other}"),
    }
}

#[tokio::test]
async fn a_write_to_an_undeclared_struct_is_refused() -> Result<()> {
    let h = Harness::new().await?;
    let wombat = CellRef::prop("Wombat".into(), "nonsense".into(), company(1));

    let error = rejection(h.write(&wombat, "7").await.unwrap_err());
    assert!(
        matches!(error, WriteRejection::UndeclaredStruct { .. }),
        "got {error}"
    );
    assert!(
        error.to_string().contains("Wombat"),
        "the rejection should name the struct: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn a_write_to_an_undeclared_field_is_refused_and_names_the_alternatives() -> Result<()> {
    let h = Harness::new().await?;

    let error = rejection(
        h.write(&prop(company(1), "nonsense"), "7")
            .await
            .unwrap_err(),
    );
    let WriteRejection::UndeclaredField { known, .. } = &error else {
        panic!("got {error}");
    };
    assert!(
        known.contains("website") && known.contains("headcount"),
        "a rejection that lists the declared fields turns a typo into a one-line fix: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn a_value_of_the_wrong_type_is_refused() -> Result<()> {
    let h = Harness::new().await?;

    // Type-directed parsing catches this before a value even exists: `acme` is not an Int.
    let error = h.write(&prop(company(1), "headcount"), "acme").await;
    assert!(
        error.unwrap_err().to_string().contains("Int"),
        "the rejection should name the declared type"
    );

    // …and the value-level check catches what parsing cannot, for a caller writing a `Value`
    // directly rather than text.
    let mut session = h.session(Writer::Client).await?;
    let error = rejection(
        session
            .set(&prop(company(1), "headcount"), Value::Bool(true))
            .await
            .unwrap_err(),
    );
    assert!(
        matches!(error, WriteRejection::TypeMismatch { .. }),
        "got {error}"
    );
    session.abort().await
}

#[tokio::test]
async fn a_client_may_not_write_a_derived_field() -> Result<()> {
    let h = Harness::new().await?;

    let error = rejection(
        h.write(&prop(company(1), "is_investible"), "true")
            .await
            .unwrap_err(),
    );
    assert!(
        matches!(error, WriteRejection::OwnershipViolation { .. }),
        "got {error}"
    );
    assert!(
        error.to_string().contains("P1"),
        "the rejection should name the producer that does own it: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn a_producer_may_write_only_the_field_it_is_declared_to_own() -> Result<()> {
    let h = Harness::new().await?;
    h.write(&prop(company(1), "website"), "acme.ai").await?;

    h.run_producer(INVEST, prop(company(1), "is_investible"), Value::Bool(true))
        .await?;

    // A second producer writing the first one's field is refused against the *declaration*, not
    // against whoever happened to write there first.
    let error = h
        .run_producer(OTHER, prop(company(1), "is_investible"), Value::Bool(false))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("may not write"), "got {error}");

    // And so is a producer writing a field declared as source data.
    let error = h
        .run_producer(OTHER, prop(company(1), "website"), Value::Bool(false))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("may not write"),
        "a producer may not write ground truth: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn a_tombstone_is_accepted_on_any_declared_field() -> Result<()> {
    let h = Harness::new().await?;
    h.write(&prop(company(1), "headcount"), "40").await?;
    h.write(&prop(company(1), "headcount"), "~").await?;
    Ok(())
}

/// The reservation §3.4 recorded as temporary, lifted end to end: a declared type is what parsing
/// is directed by, so a `String` field holds the four characters `true`.
#[tokio::test]
async fn a_string_field_holds_the_text_of_a_form_that_used_to_win() -> Result<()> {
    let h = Harness::new().await?;
    let cell = prop(company(1), "website");

    for text in ["true", "42", "0x", "@jake"] {
        h.write(&cell, text).await?;
        let path = h.layers.read_path(h.branch, None)?;
        let stored = h
            .layers
            .storage()
            .get_cell(&path, &cell, AT_V1)
            .await?
            .expect("just written")
            .event;
        let Value::Ref(pid) = stored.value else {
            panic!(
                "{text} should have interned as a string, got {:?}",
                stored.value
            );
        };
        assert_eq!(
            pid.kind(),
            PidKind::String,
            "{text} should be a String in a String field"
        );
    }
    Ok(())
}

/// A rejected write leaves nothing behind — the layer it would have landed in never commits.
#[tokio::test]
async fn a_rejected_write_leaves_no_layer_behind() -> Result<()> {
    let h = Harness::new().await?;
    let before = h.layers.head(h.branch);

    assert!(h.write(&prop(company(1), "nonsense"), "7").await.is_err());
    assert_eq!(
        h.layers.head(h.branch),
        before,
        "the branch head must not move for a write that was refused"
    );
    Ok(())
}

// --- Which def-view a write is checked against. SPEC.md §5.4, §8.0 -------------------------------
//
// Two views, two questions. *Shape* is asked of the writer's own ClientVersion, because writes are
// stored at their author's version and never coerced; *permission* is asked of the branch, because
// who may write a field is a fact about the schema as it stands.

/// `headcount: Int` becomes `headcount: String`, with a migration. Returns the new def-version.
async fn widen_headcount(h: &Harness, down: Option<ProducerId>) -> Result<ClientVersion> {
    let layer = h
        .defs
        .push(
            h.branch,
            vec![DefEvent::MutateField {
                struct_name: "Company".into(),
                field: "headcount".into(),
                ty: ValueType::String,
                repo: REPO,
                up: OTHER,
                down,
            }],
        )
        .await?;
    Ok(ClientVersion(layer))
}

#[tokio::test]
async fn an_old_client_goes_on_writing_the_shape_its_own_def_view_declares() -> Result<()> {
    let h = Harness::new().await?;
    let v2 = widen_headcount(&h, None).await?;

    // This is the whole of backwards compatibility. The branch now says `headcount` is a String; a
    // client authored against v1 still says Int, and its writes are shaped and stored that way.
    h.write_as(V1, Writer::Client, &prop(company(1), "headcount"), "40")
        .await?;
    let stored = h
        .layers
        .storage()
        .get_cell(
            &h.layers.read_path(h.branch, None)?,
            &prop(company(1), "headcount"),
            AT_V1,
        )
        .await?
        .expect("just written")
        .event;
    assert_eq!(
        stored.value,
        Value::Int(40),
        "an old client's write is parsed against the type *it* was written against"
    );

    // …and is held to it. Its view is old, not absent.
    let error = h
        .write_as(V1, Writer::Client, &prop(company(2), "headcount"), "acme")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Int"), "got {error}");

    // The same text from a current client is a String, because that is what the branch now declares.
    h.write_as(v2, Writer::Client, &prop(company(3), "headcount"), "acme")
        .await?;
    Ok(())
}

#[tokio::test]
async fn a_down_migration_may_write_a_version_that_predates_its_own_declaration() -> Result<()> {
    let h = Harness::new().await?;
    widen_headcount(&h, Some(OTHER)).await?;

    // `down` writes v1 cells, so it runs at v1 — a def-view folded *before* the `MutateField` that
    // named it as this field's `down`. Asking that view for permission would have it reject the one
    // producer the branch declared for the job.
    h.write_as(
        V1,
        Writer::Producer(OTHER),
        &prop(company(1), "headcount"),
        "40",
    )
    .await?;

    // The exemption is not a blanket one: it names this producer, for this field.
    let error = rejection(
        h.write_as(
            V1,
            Writer::Producer(INVEST),
            &prop(company(1), "headcount"),
            "40",
        )
        .await
        .unwrap_err(),
    );
    assert!(
        matches!(error, WriteRejection::OwnershipViolation { .. }),
        "got {error}"
    );
    Ok(())
}
