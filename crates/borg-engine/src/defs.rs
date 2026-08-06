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
    BorgError, BranchId, BufferId, CellRef, ClientVersion, DefEvent, DefVersion, FieldDef,
    FieldName, LayerAuthor, LayerId, LayerKind, MigrationDirection, ObjectDef, ObjectTypeName,
    Ownership, ProducerDef, ProducerId, ProducerKind, ReadPath, Result, Value, ValueType,
    WriteRejection, Writer,
};
use borg_storage::StorageProvider;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// One step in a field's def-version chain, and the migrations that bridge it.
#[derive(Clone, Copy, Debug)]
pub struct VersionStep {
    pub from: DefVersion,
    pub to: DefVersion,
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
    pub from: DefVersion,
    pub to: DefVersion,
}

/// One migration's place in a field's version chain, resolved from the definitions in force rather
/// than read off its own definition. SPEC.md §5.3, §9.3.
///
/// A migration definition records only a direction; everything below is a fact about the branch, so
/// the same producer replayed onto another branch by a def-only merge picks up that branch's layer
/// ids instead of the ones it was pushed against.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationRole {
    /// The def-version it reads its input at — the older version for `up`, the newer for `down`.
    pub input: DefVersion,
    /// The def-version it writes. It is also what its ClientVersion is folded to: a migration is
    /// the lens for the version it produces, and sees the rest of the world the way a client on
    /// that version does (§5.4).
    pub output: DefVersion,
    /// Both halves of this step. Neither triggers the other — `up` and `down` are two projections of
    /// one value, not two producers disturbing each other's inputs (SPEC.md §9.3).
    pub step: Vec<ProducerId>,
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

    /// Every struct this view declares.
    ///
    /// Unordered, like the map behind it. Codegen sorts — see
    /// [`SchemaDef`](borg_protocol::client::SchemaDef) for why the *emitter* owns that and not this:
    /// a stable file is a property of the artifact, and a caller that wanted insertion order could
    /// not get it from here anyway (a struct has no owner and no declaration point, only its fields
    /// do — SPEC.md §5.2).
    pub fn objects(&self) -> impl Iterator<Item = &ObjectDef> {
        self.objects.values()
    }

    pub fn producers(&self) -> impl Iterator<Item = &ProducerDef> {
        self.producers.values()
    }

    /// One producer's definition as this view has it, which is what a push diffs against (§9.2).
    pub fn producer(&self, id: ProducerId) -> Option<&ProducerDef> {
        self.producers.get(&id)
    }

    /// The declaration governing a cell, if the cell names a declared field.
    pub fn field(&self, cell: &CellRef) -> Option<&FieldDef> {
        let BufferId::ObjectProp(struct_name, field) = &cell.buffer else {
            return None;
        };
        self.objects.get(struct_name)?.fields.get(field)
    }

    /// **The def-version of a cell, as this view names it.** SPEC.md §5.3.
    ///
    /// This is the one bridge from a whole-schema view to a per-definition version, and the only
    /// way a `DefVersion` is ever obtained. Every stored record is keyed by it, so an actor writes
    /// and reads a field at whatever version *its own* def-view puts that field at — which is why a
    /// def push touching some other field moves nothing here, and why a migration, whose view is
    /// folded to the version it produces, keys its output exactly where the chain says it should
    /// (§9.3).
    ///
    /// Cells with no definition are [`DefVersion::UNVERSIONED`]: an existence cell, a list, an
    /// untyped container. Nothing about their shape can change, so they sit on no chain and must
    /// stay findable across every def push.
    ///
    /// An **undeclared** field is unversioned too. The write path rejects it a moment later by name
    /// (§8.0), and a rejected write must fail as a schema problem rather than as an unanswerable
    /// question about a version.
    pub fn version_of(&self, cell: &CellRef) -> DefVersion {
        self.field(cell)
            .map_or(DefVersion::UNVERSIONED, |def| DefVersion(def.version))
    }

    /// Where a migration sits in its field's version chain. SPEC.md §5.3, §9.3.
    ///
    /// `None` for a pipeline, and for a migration whose step this view does not know — which is what
    /// a producer definition that reached a branch without the `MutateField` naming it looks like.
    pub fn migration_role(&self, def: &ProducerDef) -> Option<MigrationRole> {
        let ProducerKind::Migration { direction } = def.kind else {
            return None;
        };
        let BufferId::ObjectProp(struct_name, field) = &def.source else {
            return None;
        };
        let chain = self.chains.get(&(struct_name.clone(), field.clone()))?;
        let step = chain.iter().find(|step| match direction {
            MigrationDirection::Up => step.up == def.id,
            MigrationDirection::Down => step.down == Some(def.id),
        })?;
        let (input, output) = match direction {
            MigrationDirection::Up => (step.from, step.to),
            MigrationDirection::Down => (step.to, step.from),
        };
        Some(MigrationRole {
            input,
            output,
            step: [Some(step.up), step.down].into_iter().flatten().collect(),
        })
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
    /// **`self` is the writer's view; `authority` is the branch's.** Shape is a ClientVersion
    /// question — a v1 client stores v1-shaped values long after the schema moved, and a `down`
    /// migration's whole output is old-shaped by construction (SPEC.md §5.4). Permission is not: who
    /// may write a field, and which producers are its declared migrations, are facts the branch
    /// holds, and a def-view old enough to predate a `MutateField` cannot know that the migration it
    /// is about to reject was declared by that very event. Pass the same view twice where the writer
    /// is current, which is the common case.
    pub fn check_write(
        &self,
        cell: &CellRef,
        value: &Value,
        writer: Writer,
        authority: &Self,
    ) -> Result<()> {
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
                // Ownership is asked of the branch, falling back to the writer's own view for a
                // field the branch has since dropped: an old client is still entitled to the
                // declaration it was written against.
                let governing = authority.field(cell).unwrap_or(declared);
                authority.check_ownership(cell, struct_name, field, governing, writer)?;
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
                // **A migration cannot be appointed for a field a producer owns.** Ownership is
                // checked before the migration exemption is reached ([`check_ownership`]), so the
                // migrations this event names would be forbidden to write the field it names them
                // for — the push would be accepted and the failure would arrive later, as an
                // ownership violation, from whichever round happened to run them. Refusing here says
                // why, once, to whoever pushed it.
                if let Ownership::Derived(owner) = existing.ownership {
                    return Err(BorgError::MigrationOnDerivedField {
                        struct_name: struct_name.clone(),
                        field: field.to_string(),
                        owner,
                    });
                }
                let from = DefVersion(existing.version);
                existing.ty = ty.clone();
                existing.version = at;
                self.chains
                    .entry((struct_name.clone(), field.clone()))
                    .or_default()
                    .push(VersionStep {
                        from,
                        to: DefVersion(at),
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
                // A producer's ClientVersion is the def-layer it was pushed at (SPEC.md §9.2), and
                // only the fold knows which layer that is: the id does not exist when the event is
                // built, and a def-only merge replays the event onto the parent as a different one.
                self.producers.insert(
                    def.id,
                    ProducerDef {
                        version: at,
                        ..def.clone()
                    },
                );
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
        from: DefVersion,
        to: DefVersion,
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
            .flat_map(|(branch, bound)| self.layers.def_layers_of(*branch, *bound))
            .collect();
        found.sort_by_key(|id| id.0);
        found
    }

    /// The def-version in force at the end of a path — the highest def-layer along it.
    ///
    /// This is what a client with no generated code carries as its ClientVersion: the schema as it
    /// stands (SPEC.md §5.4). `LayerId(0)` when nothing has been declared, which is a view in which
    /// no write is legal — correctly, since no field exists to write.
    pub fn head(&self, path: &ReadPath) -> LayerId {
        self.def_layers(path).last().copied().unwrap_or(LayerId(0))
    }

    /// Fold the def-event stream along a path into the definitions in force there.
    pub async fn view(&self, path: &ReadPath) -> Result<DefView> {
        self.view_at(path, LayerId(u64::MAX)).await
    }

    /// Fold the same stream, stopping at a def-version. SPEC.md §5.4.
    ///
    /// This is the view an actor at that ClientVersion has of the world, and the one its writes are
    /// shaped against. Bounding by layer id rather than by re-deriving a path keeps it total: a
    /// version reached on some other branch simply contributes nothing here, rather than producing a
    /// path that means something else.
    pub async fn view_at(&self, path: &ReadPath, ceiling: LayerId) -> Result<DefView> {
        let mut view = DefView::default();
        for layer in self.def_layers(path) {
            if layer.0 > ceiling.0 {
                break;
            }
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
