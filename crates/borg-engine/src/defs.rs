//! Definitions, as a projection of the def-event stream. SPEC.md §5, §6.1.
//!
//! There is no separate schema store. A `DefView` is folded from the `DefEvent`s on the def layers
//! along a read path, exactly as data is resolved from value layers — which is what makes a schema
//! change forkable, time-travellable and mergeable rather than an offline ritual.
//!
//! **A def-version is a LayerId**: the def-layer that produced it. No separate versioning scheme
//! exists (SPEC.md §5.3).

use crate::log::LayerManager;
use borg_core::{
    BorgError, BranchId, BufferId, CellRef, ClientVersion, DefEvent, FieldDef, FieldName,
    LayerAuthor, LayerId, LayerKind, MigrationDirection, ObjectDef, ObjectTypeName, Ownership,
    ProducerDef, ProducerId, ReadPath, Result, Value, ValueType, WriteRejection, Writer,
};
use borg_storage::StorageProvider;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// One step in a field's def-version chain, and the migrations that bridge it.
#[derive(Clone, Copy, Debug)]
pub struct VersionStep {
    pub from: ClientVersion,
    pub to: ClientVersion,
    /// Carries values forward. Required for a shape-changing def-mutation (SPEC.md §6.1).
    pub up: ProducerId,
    /// Carries values back, so clients on older versions keep reading. v1 trusts these
    /// (SPEC.md §9.3). `None` means the def-push knowingly broke older clients.
    pub down: Option<ProducerId>,
}

/// A direction-resolved hop along the chain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MigrationHop {
    pub producer: ProducerId,
    pub direction: MigrationDirection,
    pub from: ClientVersion,
    pub to: ClientVersion,
}

/// The definitions in force at one point on one branch.
#[derive(Default, Clone, Debug)]
pub struct DefView {
    objects: HashMap<ObjectTypeName, ObjectDef>,
    chains: HashMap<(ObjectTypeName, FieldName), Vec<VersionStep>>,
    producers: HashMap<ProducerId, ProducerDef>,
}

impl DefView {
    pub fn object(&self, name: &ObjectTypeName) -> Option<&ObjectDef> {
        self.objects.get(name)
    }

    pub fn producers(&self) -> impl Iterator<Item = &ProducerDef> {
        self.producers.values()
    }

    /// The declaration governing a cell, if the cell names a declared field.
    pub fn field(&self, cell: &CellRef) -> Option<&FieldDef> {
        let BufferId::ObjectProp(struct_name, field) = &cell.buffer else {
            return None;
        };
        self.objects.get(struct_name)?.fields.get(field)
    }

    /// Validate one cell write against the definitions in force. SPEC.md §5.1, §8.
    ///
    /// This is the whole of what "definitions are load-bearing" means, and it lives on the def-view
    /// rather than beside the store because it is a pure question about a *branch's* schema: no I/O,
    /// no layer, nothing to mock. `WriteSession` is what guarantees it is asked.
    ///
    /// Four things are checked, in the order a human would ask them: does the struct exist, does the
    /// field exist, may this writer write it, and will the value fit.
    ///
    pub fn check_write(&self, cell: &CellRef, value: &Value, writer: Writer) -> Result<()> {
        match &cell.buffer {
            BufferId::ObjectProp(struct_name, field) => {
                let Some(object) = self.objects.get(struct_name) else {
                    return Err(WriteRejection::UndeclaredStruct {
                        cell: cell.clone(),
                        struct_name: struct_name.clone(),
                    }
                    .into());
                };
                let Some(declared) = object.fields.get(field) else {
                    return Err(WriteRejection::UndeclaredField {
                        cell: cell.clone(),
                        struct_name: struct_name.clone(),
                        field: field.to_string(),
                        known: object
                            .fields
                            .keys()
                            .map(|name| name.to_string())
                            .collect::<Vec<_>>()
                            .join(", "),
                    }
                    .into());
                };
                self.check_ownership(cell, struct_name, field, declared, writer)?;
                if !declared.ty.accepts(value) {
                    return Err(WriteRejection::TypeMismatch {
                        cell: cell.clone(),
                        expected: declared.ty.clone(),
                        actual: borg_core::parse::render(value),
                    }
                    .into());
                }
                Ok(())
            }
            // An existence cell has no `FieldDef` of its own — a struct exists because someone
            // declared a field *on* it (§5.2), so "is this struct declared" is the whole check. It
            // holds `true` or a tombstone and nothing else, and either writer may set it: a client
            // creating an object, or a producer whose output is a new object (§9.5).
            BufferId::Object(struct_name) => {
                if !self.objects.contains_key(struct_name) {
                    return Err(WriteRejection::UndeclaredStruct {
                        cell: cell.clone(),
                        struct_name: struct_name.clone(),
                    }
                    .into());
                }
                if ValueType::Bool.accepts(value) {
                    Ok(())
                } else {
                    Err(WriteRejection::TypeMismatch {
                        cell: cell.clone(),
                        expected: ValueType::Bool,
                        actual: borg_core::parse::render(value),
                    }
                    .into())
                }
            }
            // Lists and the untyped containers are **deliberately unchecked**. There is no
            // `ListDef` event in §6.1 and no way to declare one, so requiring a declaration would
            // make them unwritable rather than validated. They become checkable in the same change
            // that gives them a declaration to check against.
            BufferId::List(_)
            | BufferId::ListElem(_)
            | BufferId::AnyObject
            | BufferId::AnyArray => Ok(()),
        }
    }

    /// Whether this writer is the one the declaration names. SPEC.md §8.
    fn check_ownership(
        &self,
        cell: &CellRef,
        struct_name: &ObjectTypeName,
        field: &FieldName,
        declared: &FieldDef,
        writer: Writer,
    ) -> Result<()> {
        let permitted = match (declared.ownership, writer) {
            (Ownership::Source, Writer::Client) => true,
            (Ownership::Derived(owner), Writer::Producer(attempted)) => owner == attempted,
            // A migration writes *someone else's* field — the same cell at a newer def-version is
            // its entire job (§9.3) — so the declaration it is checked against is the one naming it
            // as `up` or `down`, not the one naming the field's ordinary writer.
            (_, Writer::Producer(attempted)) => self.migrates(struct_name, field, attempted),
            (Ownership::Derived(_), Writer::Client) => false,
        };
        if permitted {
            return Ok(());
        }
        Err(WriteRejection::OwnershipViolation {
            cell: cell.clone(),
            ownership: declared.ownership,
            attempted: writer,
        }
        .into())
    }

    /// Whether this producer is a declared migration for this field.
    fn migrates(
        &self,
        struct_name: &ObjectTypeName,
        field: &FieldName,
        producer: ProducerId,
    ) -> bool {
        self.chains
            .get(&(struct_name.clone(), field.clone()))
            .is_some_and(|chain| {
                chain
                    .iter()
                    .any(|step| step.up == producer || step.down == Some(producer))
            })
    }

    /// Fold one event in. Returns an error when the event is not permitted, which is how collisions
    /// and cross-repo mutations are caught at push time.
    fn apply(&mut self, event: &DefEvent, at: LayerId) -> Result<()> {
        match event {
            DefEvent::DeclareField {
                struct_name,
                field,
                ty,
                repo,
                ownership,
            } => {
                let object = self
                    .objects
                    .entry(struct_name.clone())
                    .or_insert(ObjectDef {
                        name: struct_name.clone(),
                        fields: BTreeMap::new(),
                    });
                // Two repos declaring the same field is a hard error — the "repos never conflict"
                // guarantee, checked at the point of intent (SPEC.md §5.2).
                //
                // The *same* repo redeclaring the *same* shape is not a conflict but a repeat, and
                // is a no-op. `borg repo push` emits a repo's whole schema every time it runs, so
                // without this the second push of an unchanged repo would fail — and a push that
                // only works once is a push nobody trusts. Changing a declared field's shape still
                // requires `MutateField` and a migration (§6.1).
                if let Some(existing) = object.fields.get(field) {
                    let unchanged = existing.declaring_repo == *repo
                        && existing.ty == *ty
                        && existing.ownership == *ownership;
                    if unchanged {
                        return Ok(());
                    }
                    return Err(BorgError::FieldCollision {
                        struct_name: struct_name.clone(),
                        field: field.to_string(),
                        existing: existing.declaring_repo,
                    });
                }
                object.fields.insert(
                    field.clone(),
                    FieldDef {
                        name: field.clone(),
                        ty: ty.clone(),
                        declaring_repo: *repo,
                        ownership: *ownership,
                        version: at,
                    },
                );
            }
            DefEvent::MutateField {
                struct_name,
                field,
                ty,
                repo,
                up,
                down,
            } => {
                let existing = self
                    .objects
                    .get_mut(struct_name)
                    .and_then(|object| object.fields.get_mut(field))
                    .ok_or_else(|| BorgError::MissingMigration {
                        struct_name: struct_name.clone(),
                        field: field.to_string(),
                    })?;
                // A struct has no owner, but each of its fields does.
                if existing.declaring_repo != *repo {
                    return Err(BorgError::NotDeclaringRepo {
                        repo: *repo,
                        owner: existing.declaring_repo,
                        struct_name: struct_name.clone(),
                        field: field.to_string(),
                    });
                }
                let from = ClientVersion(existing.version);
                existing.ty = ty.clone();
                existing.version = at;
                self.chains
                    .entry((struct_name.clone(), field.clone()))
                    .or_default()
                    .push(VersionStep {
                        from,
                        to: ClientVersion(at),
                        up: *up,
                        down: *down,
                    });
            }
            DefEvent::DeleteField {
                struct_name,
                field,
                repo,
            } => {
                if let Some(object) = self.objects.get_mut(struct_name) {
                    if let Some(existing) = object.fields.get(field)
                        && existing.declaring_repo != *repo
                    {
                        return Err(BorgError::NotDeclaringRepo {
                            repo: *repo,
                            owner: existing.declaring_repo,
                            struct_name: struct_name.clone(),
                            field: field.to_string(),
                        });
                    }
                    object.fields.remove(field);
                }
            }
            DefEvent::PushProducer(def) => {
                self.producers.insert(def.id, def.clone());
            }
        }
        Ok(())
    }

    /// The migration path from one def-version to another, for one field. SPEC.md §5.3.
    ///
    /// v1 chains are linear, so this walks up or down. Once a def-version DAG spans branches this
    /// becomes *down to the common ancestor, then up* — the shape of the walk changes, not its
    /// meaning.
    ///
    /// `None` means the path is broken: a def-push supplied no `down`, and an older client is now
    /// asking for a version that cannot be reached.
    pub fn path(
        &self,
        buffer: &BufferId,
        from: ClientVersion,
        to: ClientVersion,
    ) -> Option<Vec<MigrationHop>> {
        let BufferId::ObjectProp(struct_name, field) = buffer else {
            return (from == to).then(Vec::new);
        };
        if from == to {
            return Some(Vec::new());
        }
        let chain = self.chains.get(&(struct_name.clone(), field.clone()))?;

        let mut hops = Vec::new();
        if from.0.0 < to.0.0 {
            for step in chain
                .iter()
                .filter(|s| s.from.0.0 >= from.0.0 && s.to.0.0 <= to.0.0)
            {
                hops.push(MigrationHop {
                    producer: step.up,
                    direction: MigrationDirection::Up,
                    from: step.from,
                    to: step.to,
                });
            }
        } else {
            for step in chain
                .iter()
                .rev()
                .filter(|s| s.to.0.0 <= from.0.0 && s.from.0.0 >= to.0.0)
            {
                hops.push(MigrationHop {
                    producer: step.down?,
                    direction: MigrationDirection::Down,
                    from: step.to,
                    to: step.from,
                });
            }
        }
        Some(hops)
    }
}

pub struct DefRegistry {
    layers: Arc<LayerManager>,
    storage: Arc<dyn StorageProvider>,
    /// Which ClientVersions currently have registered clients. The derivation engine materializes
    /// only for these; anything else is computed on demand (SPEC.md §5.5).
    live_versions: Mutex<Vec<ClientVersion>>,
}

impl DefRegistry {
    pub fn new(layers: Arc<LayerManager>, storage: Arc<dyn StorageProvider>) -> Self {
        Self {
            layers,
            storage,
            live_versions: Mutex::new(Vec::new()),
        }
    }

    pub fn mark_live(&self, version: ClientVersion) {
        let mut live = self.live_versions.lock().unwrap();
        if !live.contains(&version) {
            live.push(version);
        }
    }

    pub fn live_versions(&self) -> Vec<ClientVersion> {
        self.live_versions.lock().unwrap().clone()
    }

    /// The def layers along a read path, oldest first.
    ///
    /// LayerIds are registry-unique and monotonic, so sorting by id gives the correct fold order
    /// across the whole ancestry.
    pub fn def_layers(&self, path: &ReadPath) -> Vec<LayerId> {
        let mut found: Vec<LayerId> = path
            .segments
            .iter()
            .flat_map(|(branch, bound)| {
                self.layers
                    .layers_of(*branch)
                    .into_iter()
                    .filter(|layer| layer.kind == LayerKind::Def)
                    .filter(move |layer| layer.id.0 <= bound.0)
                    .map(|layer| layer.id)
            })
            .collect();
        found.sort_by_key(|id| id.0);
        found
    }

    /// Fold the def-event stream along a path into the definitions in force there.
    pub async fn view(&self, path: &ReadPath) -> Result<DefView> {
        let mut view = DefView::default();
        for layer in self.def_layers(path) {
            for event in self.storage.read_def_layer(layer).await? {
                view.apply(&event, layer)?;
            }
        }
        Ok(view)
    }

    /// Push a def mutation as a layer on a branch.
    ///
    /// Validated against the current view *before* the layer commits, so a rejected push leaves the
    /// definitions untouched.
    pub async fn push(&self, branch: BranchId, events: Vec<DefEvent>) -> Result<LayerId> {
        let path = self.layers.read_path(branch, None)?;
        let mut view = self.view(&path).await?;

        let mut layer = self
            .layers
            .open(branch, LayerKind::Def, LayerAuthor::Source)
            .await?;
        for event in &events {
            if let Err(rejection) = view.apply(event, layer.id()) {
                self.layers.abort(layer).await?;
                return Err(rejection);
            }
        }
        for event in events {
            layer.put_def(event).await?;
        }
        self.layers.commit(layer).await
    }
}
