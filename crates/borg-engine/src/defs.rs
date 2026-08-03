//! The def registry. SPEC.md §5.
//!
//! Holds struct definitions and, per field, the chain of def-versions with the migration that
//! bridges each step. **A def-version is a LayerId** — the def-layer that last mutated that
//! definition — so there is no separate versioning scheme and the def-version chain is just the
//! layer sequence restricted to def-layers (SPEC.md §5.3).

use borg_core::{
    BufferId, ClientVersion, FieldName, LayerId, MigrationDirection, ObjectDef, ObjectTypeName,
    ProducerId,
};
use std::collections::HashMap;
use std::sync::Mutex;

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
    /// The version this hop reads its source cell at.
    pub from: ClientVersion,
    /// The version this hop writes.
    pub to: ClientVersion,
}

#[derive(Default)]
pub struct DefRegistry {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    objects: HashMap<ObjectTypeName, ObjectDef>,
    /// Per field, its version chain in ascending order.
    chains: HashMap<(ObjectTypeName, FieldName), Vec<VersionStep>>,
    /// Which ClientVersions currently have registered clients. The derivation engine materializes
    /// only for these; anything else is computed on demand (SPEC.md §5.5).
    live_versions: Vec<ClientVersion>,
}

impl DefRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put_object(&self, def: ObjectDef) {
        self.inner
            .lock()
            .unwrap()
            .objects
            .insert(def.name.clone(), def);
    }

    pub fn object(&self, name: &ObjectTypeName) -> Option<ObjectDef> {
        self.inner.lock().unwrap().objects.get(name).cloned()
    }

    /// Record a def-mutation of one field, with the migrations that bridge it.
    pub fn push_step(&self, struct_name: ObjectTypeName, field: FieldName, step: VersionStep) {
        let mut inner = self.inner.lock().unwrap();
        let chain = inner.chains.entry((struct_name, field)).or_default();
        chain.push(step);
        chain.sort_by_key(|s| s.from.0.0);
    }

    pub fn mark_live(&self, version: ClientVersion) {
        let mut inner = self.inner.lock().unwrap();
        if !inner.live_versions.contains(&version) {
            inner.live_versions.push(version);
        }
    }

    pub fn live_versions(&self) -> Vec<ClientVersion> {
        self.inner.lock().unwrap().live_versions.clone()
    }

    /// The migration path from one def-version to another, for one field.
    ///
    /// v1 chains are linear, so this walks up or down. Once branching lands this becomes *down to
    /// the common ancestor, then up* (SPEC.md §5.3) — the shape of the walk changes, not its
    /// meaning.
    ///
    /// Returns `None` if the path is broken, which in practice means a def-push supplied no `down`
    /// and an older client is now asking for a version that cannot be reached.
    pub fn path(
        &self,
        cell_buffer: &BufferId,
        from: ClientVersion,
        to: ClientVersion,
    ) -> Option<Vec<MigrationHop>> {
        let BufferId::ObjectProp(struct_name, field) = cell_buffer else {
            // Only object properties carry migrations in v1.
            return (from == to).then(Vec::new);
        };
        let inner = self.inner.lock().unwrap();
        let chain = inner.chains.get(&(struct_name.clone(), field.clone()))?;

        let mut hops = Vec::new();
        match from.0.0.cmp(&to.0.0) {
            std::cmp::Ordering::Equal => {}
            std::cmp::Ordering::Less => {
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
            }
            std::cmp::Ordering::Greater => {
                for step in chain
                    .iter()
                    .rev()
                    .filter(|s| s.to.0.0 <= from.0.0 && s.from.0.0 >= to.0.0)
                {
                    // No `down` means this def-push knowingly broke older clients.
                    hops.push(MigrationHop {
                        producer: step.down?,
                        direction: MigrationDirection::Down,
                        from: step.to,
                        to: step.from,
                    });
                }
            }
        }
        Some(hops)
    }

    /// The def-layer a field is currently at.
    pub fn field_version(
        &self,
        struct_name: &ObjectTypeName,
        field: &FieldName,
    ) -> Option<LayerId> {
        let inner = self.inner.lock().unwrap();
        inner
            .objects
            .get(struct_name)
            .and_then(|def| def.fields.get(field))
            .map(|f| f.version)
    }
}
