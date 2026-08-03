//! Definitions travelling the log. SPEC.md §5, §6.1, §13.
//!
//! Written before the implementation. Definitions are not configuration held off to one side — they
//! are mutations on a branch, forkable and mergeable like any other.

use borg_core::{
    BranchId, DefEvent, FieldName, LayerId, MergeMode, ObjectTypeName, ProducerId, RepoId, Result,
    ValueType,
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
    }
}

#[tokio::test]
async fn a_declared_field_becomes_visible_in_the_def() -> Result<()> {
    let h = Harness::new();
    let main = h.branches.create_root(None);

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
    let main = h.branches.create_root(None);

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
    let main = h.branches.create_root(None);

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
    let main = h.branches.create_root(None);

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
async fn a_def_pushed_on_a_child_is_invisible_to_the_parent() -> Result<()> {
    let h = Harness::new();
    let main = h.branches.create_root(None);
    let fork_point = h
        .push(
            main,
            vec![declare(SALES, "Company", "name", ValueType::String)],
        )
        .await?;
    let feature = h.branches.fork(main, fork_point, None)?;

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

#[tokio::test]
async fn a_def_only_merge_lands_the_childs_defs_on_the_parent() -> Result<()> {
    let h = Harness::new();
    let main = h.branches.create_root(None);
    let fork_point = h
        .push(
            main,
            vec![declare(SALES, "Company", "name", ValueType::String)],
        )
        .await?;
    let feature = h.branches.fork(main, fork_point, None)?;

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
    let main = h.branches.create_root(None);
    let fork_point = h
        .push(
            main,
            vec![declare(SALES, "Company", "score", ValueType::Int)],
        )
        .await?;
    let feature = h.branches.fork(main, fork_point, None)?;

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
    let main = h.branches.create_root(None);
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
            borg_core::ClientVersion(LayerId(1)),
            borg_core::ClientVersion(mutated),
        )
        .expect("a path exists, because the mutation supplied migrations");
    assert_eq!(hops.len(), 1, "one def-mutation, one hop");
    Ok(())
}
