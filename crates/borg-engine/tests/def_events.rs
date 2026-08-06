//! Definitions travelling the log. SPEC.md §5, §6.1, §13.
//!
//! Written before the implementation. Definitions are not configuration held off to one side — they
//! are mutations on a branch, forkable and mergeable like any other.

use borg_core::{
    BranchId, DefEvent, FieldName, LayerId, MergeMode, ObjectTypeName, Ownership, ProducerId,
    RepoId, Result, ValueType,
};
use borg_engine::{BranchManager, CellTouchIndex, DefRegistry, InProcessSequencer, LayerManager};
use borg_storage::MemoryStorage;
use std::sync::Arc;

const SALES: RepoId = RepoId(1);
const FINANCE: RepoId = RepoId(2);

struct Harness {
    branches: Arc<BranchManager>,
    defs: Arc<DefRegistry>,
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
        let defs = Arc::new(DefRegistry::new(layers.clone(), storage));
        Self { branches, defs }
    }

    async fn push(&self, branch: BranchId, events: Vec<DefEvent>) -> Result<LayerId> {
        self.defs.push(branch, events).await
    }

    async fn field_type(
        &self,
        branch: BranchId,
        struct_name: &str,
        field: &str,
    ) -> Result<Option<ValueType>> {
        let path = self.branches.read_path(branch, None)?;
        let view = self.defs.view(&path).await?;
        Ok(view
            .object(&ObjectTypeName::from(struct_name))
            .and_then(|def| {
                def.fields
                    .get(&FieldName::from(field))
                    .map(|f| f.ty.clone())
            }))
    }
}

fn declare(repo: RepoId, struct_name: &str, field: &str, ty: ValueType) -> DefEvent {
    DefEvent::DeclareField {
        struct_name: struct_name.into(),
        field: field.into(),
        ty,
        repo,
        ownership: Ownership::Source,
    }
}

#[tokio::test]
async fn a_declared_field_becomes_visible_in_the_def() -> Result<()> {
    let h = Harness::new();
    let main = h.branches.create_root(None).await?;

    h.push(
        main,
        vec![declare(SALES, "Company", "name", ValueType::String)],
    )
    .await?;

    assert_eq!(
        h.field_type(main, "Company", "name").await?,
        Some(ValueType::String)
    );
    Ok(())
}

#[tokio::test]
async fn two_repos_extend_one_struct_with_no_explicit_extends() -> Result<()> {
    let h = Harness::new();
    let main = h.branches.create_root(None).await?;

    h.push(
        main,
        vec![declare(SALES, "Company", "name", ValueType::String)],
    )
    .await?;
    // A different team, a different repo, the same struct. There is no `extends` — declarations
    // simply merge, and a struct is the union of every repo's fields (SPEC.md §5.2).
    h.push(
        main,
        vec![declare(FINANCE, "Company", "revenue", ValueType::Int)],
    )
    .await?;

    assert_eq!(
        h.field_type(main, "Company", "name").await?,
        Some(ValueType::String)
    );
    assert_eq!(
        h.field_type(main, "Company", "revenue").await?,
        Some(ValueType::Int),
        "the second repo's field lands on the same struct without ceremony"
    );
    Ok(())
}

#[tokio::test]
async fn two_repos_declaring_the_same_field_is_a_hard_error() -> Result<()> {
    let h = Harness::new();
    let main = h.branches.create_root(None).await?;

    h.push(
        main,
        vec![declare(SALES, "Company", "score", ValueType::Int)],
    )
    .await?;
    let outcome = h
        .push(
            main,
            vec![declare(FINANCE, "Company", "score", ValueType::Double)],
        )
        .await;

    assert!(
        outcome.is_err(),
        "repos may never conflict, and a collision is caught at the point of intent"
    );
    assert_eq!(
        h.field_type(main, "Company", "score").await?,
        Some(ValueType::Int),
        "the rejected push leaves the def untouched"
    );
    Ok(())
}

#[tokio::test]
async fn a_repo_may_not_mutate_a_field_it_did_not_declare() -> Result<()> {
    let h = Harness::new();
    let main = h.branches.create_root(None).await?;

    h.push(
        main,
        vec![declare(SALES, "Company", "name", ValueType::String)],
    )
    .await?;

    let outcome = h
        .push(
            main,
            vec![DefEvent::MutateField {
                struct_name: "Company".into(),
                field: "name".into(),
                ty: ValueType::Int,
                repo: FINANCE,
                up: ProducerId(50),
                down: None,
            }],
        )
        .await;

    assert!(
        outcome.is_err(),
        "a struct has no owner, but each of its fields does (SPEC.md §5.2)"
    );
    Ok(())
}

#[tokio::test]
async fn a_migration_may_not_be_appointed_for_a_field_a_producer_owns() -> Result<()> {
    let h = Harness::new();
    let main = h.branches.create_root(None).await?;

    h.push(
        main,
        vec![DefEvent::DeclareField {
            struct_name: "Company".into(),
            field: "tier".into(),
            ty: ValueType::Int,
            repo: SALES,
            ownership: Ownership::Derived(ProducerId(7)),
        }],
    )
    .await?;

    let outcome = h
        .push(
            main,
            vec![DefEvent::MutateField {
                struct_name: "Company".into(),
                field: "tier".into(),
                ty: ValueType::String,
                repo: SALES,
                up: ProducerId(50),
                down: None,
            }],
        )
        .await;

    // The point is *when* and *why*, not merely that it fails. Before this, the push was accepted
    // and P50 was appointed to write a field P7 owns — a contradiction that surfaced later as an
    // ownership violation from whichever round happened to run the migration.
    let Err(err) = outcome else {
        panic!("a producer owns `Company.tier`, so nothing can be appointed to migrate it");
    };
    let said = err.to_string();
    assert!(
        said.contains("derived by P7"),
        "the rejection names the producer whose field it is, so the push can be fixed: {said}"
    );

    // And the definitions are untouched: a rejected push leaves nothing behind.
    assert_eq!(
        h.field_type(main, "Company", "tier").await?,
        Some(ValueType::Int),
        "a rejected def push does not move the field it was rejected over"
    );
    Ok(())
}

#[tokio::test]
async fn a_def_pushed_on_a_child_is_invisible_to_the_parent() -> Result<()> {
    let h = Harness::new();
    let main = h.branches.create_root(None).await?;
    let fork_point = h
        .push(
            main,
            vec![declare(SALES, "Company", "name", ValueType::String)],
        )
        .await?;
    let feature = h.branches.fork(main, fork_point, None).await?;

    h.push(
        feature,
        vec![declare(SALES, "Company", "website", ValueType::String)],
    )
    .await?;

    assert_eq!(
        h.field_type(feature, "Company", "website").await?,
        Some(ValueType::String),
        "the child sees its own def"
    );
    assert_eq!(
        h.field_type(main, "Company", "website").await?,
        None,
        "and the parent does not — a schema change is branch-scoped like any other mutation"
    );
    Ok(())
}

/// A fork of a fork. **Nothing else in the codebase exercises a branch chain deeper than one
/// fork** — `read_path` walks arbitrary depth and this is what says so.
///
/// The grandchild sees the whole ancestry, and what it declares is invisible *upwards* in both
/// directions: not to its parent, and not to its grandparent.
#[tokio::test]
async fn a_fork_of_a_fork_sees_its_whole_ancestry_and_leaks_nothing_back() -> Result<()> {
    let h = Harness::new();
    let main = h.branches.create_root(None).await?;
    let root_layer = h
        .push(
            main,
            vec![declare(SALES, "Company", "name", ValueType::String)],
        )
        .await?;

    let feature = h.branches.fork(main, root_layer, None).await?;
    let feature_layer = h
        .push(
            feature,
            vec![declare(SALES, "Company", "website", ValueType::String)],
        )
        .await?;

    let experiment = h.branches.fork(feature, feature_layer, None).await?;
    h.push(
        experiment,
        vec![declare(SALES, "Company", "founded", ValueType::Int)],
    )
    .await?;

    // Downwards: everything an ancestor declared is in force.
    for field in ["name", "website", "founded"] {
        assert!(
            h.field_type(experiment, "Company", field).await?.is_some(),
            "a fork of a fork resolves `{field}` through two fork points"
        );
    }
    assert_eq!(
        h.field_type(feature, "Company", "name").await?,
        Some(ValueType::String),
        "and the middle branch still sees the root's"
    );

    // Upwards: nothing.
    assert_eq!(
        h.field_type(feature, "Company", "founded").await?,
        None,
        "the grandchild's field is invisible to its parent"
    );
    assert_eq!(
        h.field_type(main, "Company", "founded").await?,
        None,
        "and to its grandparent"
    );
    assert_eq!(
        h.field_type(main, "Company", "website").await?,
        None,
        "as is the middle branch's own"
    );
    Ok(())
}

#[tokio::test]
async fn a_def_only_merge_lands_the_childs_defs_on_the_parent() -> Result<()> {
    let h = Harness::new();
    let main = h.branches.create_root(None).await?;
    let fork_point = h
        .push(
            main,
            vec![declare(SALES, "Company", "name", ValueType::String)],
        )
        .await?;
    let feature = h.branches.fork(main, fork_point, None).await?;

    h.push(
        feature,
        vec![declare(SALES, "Company", "website", ValueType::String)],
    )
    .await?;

    h.branches.merge(feature, MergeMode::DefOnly).await?;

    assert_eq!(
        h.field_type(main, "Company", "website").await?,
        Some(ValueType::String),
        "def-only merge carries the schema change across"
    );
    Ok(())
}

#[tokio::test]
async fn merge_is_rejected_when_both_branches_moved_the_same_def() -> Result<()> {
    let h = Harness::new();
    let main = h.branches.create_root(None).await?;
    let fork_point = h
        .push(
            main,
            vec![declare(SALES, "Company", "score", ValueType::Int)],
        )
        .await?;
    let feature = h.branches.fork(main, fork_point, None).await?;

    // Both sides change the same field's type.
    h.push(
        feature,
        vec![DefEvent::MutateField {
            struct_name: "Company".into(),
            field: "score".into(),
            ty: ValueType::Double,
            repo: SALES,
            up: ProducerId(50),
            down: None,
        }],
    )
    .await?;
    h.push(
        main,
        vec![DefEvent::MutateField {
            struct_name: "Company".into(),
            field: "score".into(),
            ty: ValueType::String,
            repo: SALES,
            up: ProducerId(51),
            down: None,
        }],
    )
    .await?;

    let outcome = h.branches.merge(feature, MergeMode::DefOnly).await;
    assert!(
        outcome.is_err(),
        "the child's mutation was authored against a def the parent has since moved; re-fork from \
         head and redo (SPEC.md §13)"
    );
    assert_eq!(
        h.field_type(main, "Company", "score").await?,
        Some(ValueType::String),
        "and the parent keeps its own version"
    );
    Ok(())
}

#[tokio::test]
async fn a_def_mutation_records_a_migration_step() -> Result<()> {
    let h = Harness::new();
    let main = h.branches.create_root(None).await?;
    h.push(
        main,
        vec![declare(SALES, "Company", "score", ValueType::Int)],
    )
    .await?;
    let mutated = h
        .push(
            main,
            vec![DefEvent::MutateField {
                struct_name: "Company".into(),
                field: "score".into(),
                ty: ValueType::String,
                repo: SALES,
                up: ProducerId(50),
                down: Some(ProducerId(51)),
            }],
        )
        .await?;

    let path = h.branches.read_path(main, None)?;
    let view = h.defs.view(&path).await?;

    assert_eq!(
        view.object(&ObjectTypeName::from("Company"))
            .and_then(|d| d.fields.get(&FieldName::from("score")).map(|f| f.version)),
        Some(mutated),
        "a def-version *is* the def-layer that produced it — there is no separate scheme"
    );

    let hops = view
        .path(
            &borg_core::BufferId::ObjectProp("Company".into(), "score".into()),
            borg_core::DefVersion(LayerId(1)),
            borg_core::DefVersion(mutated),
        )
        .expect("a path exists, because the mutation supplied migrations");
    assert_eq!(hops.len(), 1, "one def-mutation, one hop");
    Ok(())
}

/// A producer's *implementation* travels in its definition, and the fold carries it. SPEC.md §9.2.
///
/// The fold deliberately has no opinion about whether the implementation moved: it stamps the
/// ClientVersion and stores what it was handed. Deciding whether to emit a `PushProducer` at all is
/// `borg repo push`'s diff (`producer_change` in `borg-cli`), because that is the only place that
/// can see both what the repo says now and what the branch already believes.
mod the_implementation_fingerprint {
    use super::*;
    use borg_core::{BufferId, ProducerDef, ProducerKind};

    const SCORE: ProducerId = ProducerId(7);

    fn push_producer(fingerprint: Option<&str>) -> DefEvent {
        DefEvent::PushProducer(ProducerDef {
            id: SCORE,
            kind: ProducerKind::Pipeline,
            source: BufferId::Object("Company".into()),
            // A placeholder: the fold stamps the layer this lands on (SPEC.md §9.2).
            version: LayerId(0),
            declaring_repo: SALES,
            fingerprint: fingerprint.map(str::to_string),
        })
    }

    #[tokio::test]
    async fn it_survives_the_fold_and_is_what_the_next_push_diffs_against() -> Result<()> {
        let h = Harness::new();
        let main = h.branches.create_root(None).await?;
        h.push(main, vec![push_producer(Some("sha256:one"))])
            .await?;

        let path = h.branches.read_path(main, None)?;
        let view = h.defs.view(&path).await?;
        assert_eq!(
            view.producer(SCORE).and_then(|def| def.fingerprint.clone()),
            Some("sha256:one".to_string()),
            "what the repo said its code was is readable back out of the definitions in force"
        );
        Ok(())
    }

    /// A ClientVersion is stamped by the fold, so re-landing the definition is what moves it — and
    /// moving it is what hands the producer its source buffer back (SPEC.md §9.2).
    #[tokio::test]
    async fn re_pushing_it_moves_the_producers_client_version() -> Result<()> {
        let h = Harness::new();
        let main = h.branches.create_root(None).await?;
        let first = h
            .push(main, vec![push_producer(Some("sha256:one"))])
            .await?;
        let second = h
            .push(main, vec![push_producer(Some("sha256:two"))])
            .await?;
        assert_ne!(first, second);

        let path = h.branches.read_path(main, None)?;
        let view = h.defs.view(&path).await?;
        let def = view.producer(SCORE).expect("the producer is defined");
        assert_eq!(
            def.version, second,
            "its ClientVersion is the layer it was last pushed at"
        );
        assert_eq!(def.fingerprint.as_deref(), Some("sha256:two"));
        Ok(())
    }

    /// A producer re-pushed on a fork picks up the fork's layer id, like every other def event —
    /// which is what keeps a code change local to the branch it was deployed on.
    #[tokio::test]
    async fn a_fork_that_redeploys_moves_only_its_own_client_version() -> Result<()> {
        let h = Harness::new();
        let main = h.branches.create_root(None).await?;
        let at = h
            .push(main, vec![push_producer(Some("sha256:one"))])
            .await?;
        let fork = h.branches.fork(main, at, None).await?;
        let redeployed = h
            .push(fork, vec![push_producer(Some("sha256:two"))])
            .await?;

        let on_fork = h.defs.view(&h.branches.read_path(fork, None)?).await?;
        let on_main = h.defs.view(&h.branches.read_path(main, None)?).await?;
        assert_eq!(
            on_fork.producer(SCORE).map(|def| def.version),
            Some(redeployed)
        );
        assert_eq!(on_main.producer(SCORE).map(|def| def.version), Some(at));
        assert_eq!(
            on_main.producer(SCORE).and_then(|d| d.fingerprint.clone()),
            Some("sha256:one".to_string()),
            "main is still running the build it was pushed"
        );
        Ok(())
    }
}
