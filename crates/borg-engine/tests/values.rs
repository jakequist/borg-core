//! Values that are not primitives, end to end through the engine. SPEC.md §3.1, §3.4, §4.2.
//!
//! Interning existed in storage long before anything called it, so the claim under test here is not
//! that `intern` works — it is that **a producer never has to know it exists**. A pipeline writes
//! the text of a string and reads the text of a string; the `@s-…` in between is the engine's
//! business, and these tests are what keep it that way.

use borg_core::{
    BranchId, BufferId, CellRef, ClientVersion, DefEvent, DefVersion, Event, LayerAuthor, LayerId,
    Ownership, Pid, PidKind, ProducerDef, ProducerId, ProducerKind, RepoId, Result, Value,
    ValueInput, ValueType, Writer,
};
use borg_engine::{
    BranchManager, CellTouchIndex, DefRegistry, DerivationEngine, FrontierTracker,
    InProcessSequencer, LayerManager, MemoryDependencyIndex, Values, WriteSession,
};
use borg_exec::ProducerCtx;
use borg_exec_native::NativeExecutor;
use borg_storage::{MemoryStorage, StorageProvider};
use std::sync::Arc;

const BRANCH: BranchId = BranchId(1);
const SHOUT: ProducerId = ProducerId(1);
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

fn prop(pid: Pid, field: &str) -> CellRef {
    CellRef::prop("Company".into(), field.into(), pid)
}

struct Harness {
    storage: Arc<MemoryStorage>,
    branches: Arc<BranchManager>,
    layers: Arc<LayerManager>,
    defs: Arc<DefRegistry>,
    engine: Arc<DerivationEngine>,
    values: Values,
}

impl Harness {
    fn new(executor: NativeExecutor) -> Self {
        let storage = Arc::new(MemoryStorage::new());
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
            Arc::new(MemoryDependencyIndex::new()),
            Arc::new(executor),
            Arc::new(FrontierTracker::new()),
            defs.clone(),
            branches.clone(),
        ));
        engine.register(ProducerDef {
            id: SHOUT,
            kind: ProducerKind::Pipeline,
            source: BufferId::Object("Company".into()),
            version: LayerId(1),
            declaring_repo: RepoId(1),
            fingerprint: None,
        });
        Self {
            values: Values::new(storage.clone()),
            storage,
            branches,
            layers,
            defs,
            engine,
        }
    }

    /// Write source cells the way a client does: text in, parsed against the declared type,
    /// interned, and the implied existence cell written — all inside the session.
    async fn set(&self, writes: &[(CellRef, &str)]) -> Result<LayerId> {
        let mut session = WriteSession::open(
            &self.layers,
            &self.defs,
            BRANCH,
            V1,
            Writer::Client,
            LayerAuthor::Source,
        )
        .await?;
        for (cell, text) in writes {
            session.set_text(cell, text).await?;
        }
        session.commit().await
    }

    async fn record(&self, cell: &CellRef) -> Option<Event> {
        let head = self.layers.head(BRANCH).unwrap();
        let path = self.branches.read_path(BRANCH, Some(head)).unwrap();
        self.storage
            .get_cell(&path, cell, AT_V1)
            .await
            .unwrap()
            .map(|found| found.event)
    }

    /// What a client sees for a cell — the text, not the storage.
    async fn text(&self, cell: &CellRef) -> Option<String> {
        let record = self.record(cell).await?;
        Some(self.values.render(&record.value).await.unwrap())
    }
}

/// Reads `website`, writes `slogan`. Both are strings, and the producer handles neither PID: it is
/// given text and it hands back text.
fn shouting_producer() -> NativeExecutor {
    let executor = NativeExecutor::new();
    executor.register(
        SHOUT,
        Arc::new(|ctx: &mut dyn ProducerCtx, input: Pid| {
            Box::pin(async move {
                let Some(value) = ctx.get(&prop(input, "website")).await? else {
                    return Ok(());
                };
                let website = ctx.render(&value).await?;
                let slogan = ctx
                    .intern(ValueInput::string(&website.to_uppercase()))
                    .await?;
                ctx.set(&prop(input, "slogan"), slogan).await
            })
        }),
    );
    executor
}

async fn harness() -> Harness {
    let harness = Harness::new(shouting_producer());
    let branch = harness.branches.create_root(None).await.unwrap();
    let declare = |field: &str, ty: ValueType, ownership: Ownership| DefEvent::DeclareField {
        struct_name: "Company".into(),
        field: field.into(),
        ty,
        repo: RepoId(1),
        ownership,
    };
    // One field per content-addressed kind, plus the producer's output. Declaring the types is what
    // lets `set` below parse text against them rather than guessing (SPEC.md §3.4).
    harness
        .defs
        .push(
            branch,
            vec![
                declare("website", ValueType::String, Ownership::Source),
                declare("logo", ValueType::Binary, Ownership::Source),
                declare("valuation", ValueType::BigInt, Ownership::Source),
                declare("slogan", ValueType::String, Ownership::Derived(SHOUT)),
            ],
        )
        .await
        .unwrap();
    harness
}

/// The headline claim. A pipeline reading `company.website` receives `acme.ai` — not `@s-1a2b3c`
/// and not a second round trip to resolve one.
#[tokio::test]
async fn a_producer_reads_a_string_as_its_content() -> Result<()> {
    let harness = harness().await;
    harness
        .set(&[(prop(company(1), "website"), "acme.ai")])
        .await?;
    harness.engine.catch_up(BRANCH).await?;

    assert_eq!(
        harness.text(&prop(company(1), "slogan")).await.as_deref(),
        Some("ACME.AI"),
        "the producer saw the content of the string it read"
    );
    Ok(())
}

/// The other direction: a producer writing text produces a stored `Ref` to a content PID, because
/// the engine interned it. Nothing in the producer said so.
#[tokio::test]
async fn a_string_written_by_a_producer_is_interned() -> Result<()> {
    let harness = harness().await;
    harness
        .set(&[(prop(company(1), "website"), "acme.ai")])
        .await?;
    harness.engine.catch_up(BRANCH).await?;

    let record = harness
        .record(&prop(company(1), "slogan"))
        .await
        .expect("the producer wrote a slogan");
    let Value::Ref(pid) = record.value else {
        panic!("a string cell holds a reference, not {:?}", record.value);
    };
    assert_eq!(pid.kind(), PidKind::String);
    assert!(
        !pid.is_mutable(),
        "a string's PID is its content hash, so it is not allocated identity"
    );
    assert_eq!(
        harness.storage.read_interned(&pid).await?.as_deref(),
        Some(b"ACME.AI".as_slice()),
        "and the bytes behind it are in the store"
    );
    Ok(())
}

/// Interning's whole purpose. Two companies with the same website hold the *same* PID — no branch,
/// no layer, no writer involved in deciding that (§3.1).
#[tokio::test]
async fn two_equal_strings_are_one_stored_value() -> Result<()> {
    let harness = harness().await;
    harness
        .set(&[
            (prop(company(1), "website"), "acme.ai"),
            (prop(company(2), "website"), "acme.ai"),
            (prop(company(3), "website"), "rival.ai"),
        ])
        .await?;

    async fn pid_of(harness: &Harness, n: u64) -> Pid {
        match harness.record(&prop(company(n), "website")).await {
            Some(Event {
                value: Value::Ref(pid),
                ..
            }) => pid,
            other => panic!("expected a reference, got {other:?}"),
        }
    }
    assert_eq!(
        pid_of(&harness, 1).await,
        pid_of(&harness, 2).await,
        "equal content is one interned value"
    );
    assert_ne!(
        pid_of(&harness, 1).await,
        pid_of(&harness, 3).await,
        "and different content is not"
    );
    Ok(())
}

/// Binary and bigints go the same way as strings — one mechanism, three kinds — and survive the
/// round trip a client actually performs: read the text, write it back.
#[tokio::test]
async fn every_content_addressed_kind_round_trips_through_its_text() -> Result<()> {
    let harness = harness().await;
    for (field, text, kind) in [
        ("website", "acme.ai", PidKind::String),
        ("logo", "0xdeadbeef", PidKind::Binary),
        (
            "valuation",
            "170141183460469231731687303715884105728n",
            PidKind::BigInt,
        ),
    ] {
        harness.set(&[(prop(company(9), field), text)]).await?;
        let cell = prop(company(9), field);

        assert_eq!(harness.text(&cell).await.as_deref(), Some(text));
        assert_eq!(
            harness
                .record(&cell)
                .await
                .unwrap()
                .value
                .as_ref_pid()
                .unwrap()
                .kind(),
            kind
        );

        // What came out, back in. A client that cannot do this cannot copy a value between cells.
        let echoed = harness.text(&cell).await.unwrap();
        harness.set(&[(prop(company(10), field), &echoed)]).await?;
        assert_eq!(
            harness
                .record(&prop(company(10), field))
                .await
                .unwrap()
                .value,
            harness.record(&cell).await.unwrap().value,
            "{text} named the same value the second time"
        );
    }
    Ok(())
}

/// A PID travels further than the bytes behind it (§17.1), so a `read_interned` miss is a legitimate
/// answer rather than a failure — and the honest thing to render is the PID.
#[tokio::test]
async fn content_this_store_has_never_seen_renders_as_its_pid() -> Result<()> {
    let harness = harness().await;
    let stranger = Pid::Content {
        kind: PidKind::String,
        hash: [42u8; 32],
    };
    assert_eq!(
        harness.values.render(&Value::Ref(stranger)).await?,
        format!("@{stranger}")
    );
    Ok(())
}
