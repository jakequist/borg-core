//! The write path. SPEC.md §5.1, §6.2, §8.
//!
//! **You cannot write a cell without consulting definitions.** That is what this module exists to
//! make true, and it is arranged so the rule is structural rather than remembered: a
//! [`WriteSession`] holds a branch's [`DefView`] and its open [`LayerHandle`] together,
//! `LayerHandle::put` is crate-private, and every write — the CLI, a producer through
//! `RecordingCtx`, an SDK later — comes through here. There is no second door to forget to lock.
//!
//! ## Why the check is not simply inside the layer
//!
//! The obvious placement is `LayerHandle::put`, and it is wrong: `DefRegistry` reads def layers
//! *through* `LayerManager`, so teaching the log to validate against definitions would make the log
//! depend on the thing that depends on it. Validation therefore sits one level up, in the only place
//! that already holds both — which is also the level that knows *who* is writing, a fact the log has
//! no business carrying.
//!
//! ## What a session is scoped to
//!
//! One session is one layer, which is one unit of atomicity (§6.2). The def-view is folded **once**,
//! when the session opens, rather than per write: a schema change committed halfway through a client
//! transaction must not change what the second half of that transaction is allowed to say.
//!
//! ## Which def-view — two of them, deliberately
//!
//! A session folds the definitions **at the writer's ClientVersion** and the definitions **in force
//! on the branch**, and asks each a different question (§5.4, §8.0).
//!
//! *Shape* — is the struct declared, is the field declared, does the value fit — is asked of the
//! writer's own view. Writes are never coerced, so a client authored before a schema change goes on
//! writing the old shape, and a `down` migration's entire output is old-shaped by construction
//! (§9.3). Checking either against the branch's current view would reject exactly the writes that
//! backwards compatibility consists of.
//!
//! **That same view also decides where the record is keyed.** A cell is stored at the *def-version
//! of its field* — `DefView::version_of`, asked of the writer's own view — and not at the writer's
//! whole-schema ClientVersion (§5.3, §5.4). The two coincide only when every def push touches every
//! field: key by the second and declaring an unrelated field moves every write after it to a
//! version no reader looks for and no migration leads to, which hides the data and severs every
//! dependency recorded against it.
//!
//! *Permission* — may this writer write this field at all — is asked of the branch. Ownership is a
//! fact about the schema as it stands, and the migration exemption especially so: the declaration
//! naming a producer as a field's `up` or `down` arrives with the `MutateField`, so a `down`
//! migration's own view, which is by definition older than that event, cannot see it.
//!
//! Both are folded along the branch's **full** ancestry even when the session is bounded at a layer.
//! The bound exists to pin *data* reads to a settling round's ceiling (§16.5); a layer holds value
//! events xor def events (§6.2), so derivation never commits a def layer and the ceiling can only
//! ever hide definitions the round predates — which is how a migration ends up checked against the
//! very schema its own def-mutation introduced.

use crate::defs::{DefRegistry, DefView};
use crate::log::{LayerHandle, LayerManager};
use crate::values::Values;
use borg_core::{
    AllocatorId, BranchId, BufferId, CellAt, CellRef, ClientVersion, Derivation, EventDraft,
    LayerAuthor, LayerId, LayerKind, ReadPath, Result, Value, ValueType, Writer, parse,
};
use borg_storage::StorageProvider;
use std::collections::HashSet;
use std::sync::Arc;

/// v1 runs one PID-allocating authority per process (§3.1), so the shorthand `Company#1` always
/// means allocator 0. This is the same constant the CLI and the worker protocol assume; naming it
/// once is what keeps them meaning the same object.
const ALLOCATOR: AllocatorId = AllocatorId(0);

/// An open layer that validates every write against the definitions in force on its branch.
pub struct WriteSession {
    layers: Arc<LayerManager>,
    storage: Arc<dyn StorageProvider>,
    values: Values,
    /// The definitions the writer's own code was built against. Shape is checked here.
    defs: DefView,
    /// The definitions in force on the branch. Permission is checked here.
    authority: DefView,
    /// The ancestry this session's *data* reads resolve through, bounded by the round's ceiling
    /// where there is one.
    path: ReadPath,
    writer: Writer,
    handle: LayerHandle,
    /// Existence cells this session has already written, so the implied-existence write below
    /// happens at most once per object per layer.
    touched: HashSet<CellRef>,
    /// What a client session read and wrote, for the transaction that owns it (SPEC.md §12).
    ///
    /// **Client sessions only.** A producer's read-set already comes from `ProducerCtx`, and a
    /// derived layer may hold millions of mutations that must never be buffered whole (§6.2) — so
    /// recording every cell a producer writes would break the one constraint the log is built
    /// around. A client transaction's sets are unbounded too (§7.7), and that is a cost it has
    /// chosen: it is going to carry them to its own commit either way.
    observed: Vec<CellAt>,
    authored: Vec<CellRef>,
}

impl WriteSession {
    /// Fold the branch's definitions and open a layer to write into.
    ///
    /// `at` bounds both the def-view and the existence probe, which is what lets a producer run
    /// inside a settling round see the round's own ceiling rather than the branch head (§16.5).
    ///
    /// Public because this *is* the write path: `Registry::begin_write` is a convenience over it for
    /// callers that hold a whole registry. What is not public is `LayerHandle::put` — there is one
    /// door, and it is this one.
    pub async fn open(
        layers: &Arc<LayerManager>,
        defs: &DefRegistry,
        branch: BranchId,
        at: Option<LayerId>,
        version: ClientVersion,
        writer: Writer,
        author: LayerAuthor,
    ) -> Result<Self> {
        let path = layers.read_path(branch, at)?;
        let def_path = layers.read_path(branch, None)?;
        let authority = defs.view(&def_path).await?;
        let view = defs.view_at(&def_path, version.0).await?;
        let handle = layers.open(branch, LayerKind::Value, author).await?;
        let storage = layers.storage();
        Ok(Self {
            layers: Arc::clone(layers),
            values: Values::new(Arc::clone(&storage)),
            storage,
            defs: view,
            authority,
            path,
            writer,
            handle,
            touched: HashSet::new(),
            observed: Vec::new(),
            authored: Vec::new(),
        })
    }

    /// What this session read on the way to writing — today, exactly the existence probes below.
    ///
    /// Handed to the owning transaction as part of its read-set (SPEC.md §12). **An implicit read is
    /// a read**: without it two transactions can each probe an object, each find it absent, and each
    /// create it, with the second silently overwriting a decision the first made.
    ///
    /// Every probe precedes every write to the same cell within one session, which is what lets a
    /// caller drain these in a batch and still get the ordering rule
    /// ([`Transaction::observe`](crate::transaction::Transaction::observe)) right.
    pub fn observed(&self) -> &[CellAt] {
        &self.observed
    }

    /// Every cell this session wrote, implied existence cells included.
    pub fn authored(&self) -> &[CellRef] {
        &self.authored
    }

    /// The layer this session writes into. Reserved at open, which is why it can be named before
    /// anyone knows whether the session will commit or abort (SPEC.md §7.3).
    pub const fn layer(&self) -> LayerId {
        self.handle.id()
    }

    /// The writer's own def-view — the schema its code was authored against.
    ///
    /// Exposed for one reason: a producer's *reads* must resolve at the same def-versions its
    /// *writes* are keyed at, and the session is where that view already lives. Handing out the
    /// view rather than folding a second one is what makes the two impossible to disagree.
    pub const fn defs(&self) -> &DefView {
        &self.defs
    }

    /// Make this layer contingent on a condition, validated at seal (SPEC.md §12).
    pub fn guard(&mut self, guard: borg_core::Guard) {
        self.handle.guard(guard);
    }

    /// Write text, parsed **against the field's declared type** (SPEC.md §3.4).
    ///
    /// This is the entry point for every surface that speaks text — the CLI and the worker protocol
    /// — and the reason they cannot drift: one parse, directed by one def-view, reached from both.
    /// A field declared `String` takes `@jake` and `true` as those characters; a field declared
    /// `Int` refuses `acme` rather than storing a string that looks almost right.
    pub async fn set_text(&mut self, cell: &CellRef, text: &str) -> Result<()> {
        let value = self.parse(cell, text).await?;
        self.put(cell, value, None).await
    }

    /// Write a value.
    pub async fn set(&mut self, cell: &CellRef, value: Value) -> Result<()> {
        self.put(cell, value, None).await
    }

    /// Write a value with its derivation metadata — the producer path (SPEC.md §4.3).
    pub async fn set_derived(
        &mut self,
        cell: &CellRef,
        value: Value,
        derivation: Derivation,
    ) -> Result<()> {
        self.put(cell, value, Some(derivation)).await
    }

    /// [`set_text`](Self::set_text) from a producer: the worker protocol speaks text too, and must
    /// get the same type-directed parse the CLI gets rather than a second one of its own.
    pub async fn set_text_derived(
        &mut self,
        cell: &CellRef,
        text: &str,
        derivation: Derivation,
    ) -> Result<()> {
        let value = self.parse(cell, text).await?;
        self.put(cell, value, Some(derivation)).await
    }

    /// Text to value, directed by the declared type and interned on the way.
    async fn parse(&mut self, cell: &CellRef, text: &str) -> Result<Value> {
        // An undeclared field has no type to parse against. Rather than report a syntax problem for
        // what is really a schema problem, guess syntactically here and let the write itself be
        // rejected by name.
        let input = match self.declared_type(cell) {
            Some(ty) => parse::value_as(&ty, text, self.shorthand_branch(), ALLOCATOR)?,
            None => parse::value(text, self.shorthand_branch(), ALLOCATOR)?,
        };
        self.values.intern(input).await
    }

    /// The single validated write. Everything above funnels here.
    async fn put(
        &mut self,
        cell: &CellRef,
        value: Value,
        derivation: Option<Derivation>,
    ) -> Result<()> {
        self.defs
            .check_write(cell, &value, self.writer, &self.authority)?;
        self.imply_existence(cell).await?;
        // The draft names no layer: the log stamps `authored` with the layer it goes into, which is
        // the one fact a writer must not be able to assert about itself (§4.3).
        let draft = EventDraft {
            value,
            version: self.defs.version_of(cell),
            origin: self.writer.origin(),
            derivation,
        };
        self.handle.put(cell, draft).await?;
        if self.writer == Writer::Client {
            self.authored.push(cell.clone());
        }
        Ok(())
    }

    /// The declared type of a cell, where it has one.
    fn declared_type(&self, cell: &CellRef) -> Option<ValueType> {
        match &cell.buffer {
            // An existence cell holds `true` or a tombstone; it has no `FieldDef` because a struct
            // has no owner, only its fields do (§5.2).
            BufferId::Object(_) => Some(ValueType::Bool),
            _ => self.defs.field(cell).map(|def| def.ty.clone()),
        }
    }

    /// The branch the `Company#1` shorthand allocates against.
    ///
    /// Always the root of this branch's ancestry, never the branch being written: a PID records
    /// where an object was *allocated*, not where it lives, so `Company#1` has to name one object
    /// across a whole fork tree or a fork could never read what its parent wrote (§3.1). The read
    /// path already ends at that root, so nothing has to be configured to know it.
    fn shorthand_branch(&self) -> BranchId {
        self.path
            .segments
            .last()
            .map_or_else(|| self.path.segments[0].0, |(branch, _)| *branch)
    }

    /// Writing a property implies the object exists.
    ///
    /// Producers map over a struct's `ObjectBuffer`, which holds existence cells (§4.2), so without
    /// this a `Company` whose fields were set but which was never explicitly created would be
    /// invisible to every pipeline. Only when absent, never on every write: the existence cell lives
    /// in the buffer producers subscribe to, so rewriting it would make *any* property write look
    /// like a new entity.
    ///
    /// **Client writes only.** A derived layer must not inject into the buffer that triggers
    /// derivation, or a producer's own output would look like a new entity and re-trigger it
    /// forever. A producer whose output *is* a new object writes that existence cell itself,
    /// deliberately, at a deterministic PID (§9.5).
    async fn imply_existence(&mut self, cell: &CellRef) -> Result<()> {
        if self.writer != Writer::Client {
            return Ok(());
        }
        let existence = CellRef::existence_of(cell);
        // The probe reads *committed* data, so it cannot see an existence cell this same layer
        // already wrote — and writing one twice into one layer is a duplicate record key, not a
        // no-op. Remembering the objects this session has touched is what closes that gap; it costs
        // one entry per distinct object in one open layer, which is the same order as the layer.
        // Writing the existence cell *explicitly* counts too — otherwise the next property write on
        // the same object would imply one on top of it.
        if !self.touched.insert(existence.clone()) || existence == *cell {
            return Ok(());
        }
        // Unversioned, both to probe and to write. An existence cell has no `FieldDef` and so no
        // def-version (§5.2); versioning it by the writer's schema view would make every def push
        // look like a fresh entity to every producer mapping over the buffer, and re-derive the
        // world on a declaration that said nothing about it.
        let version = self.defs.version_of(&existence);
        // Recorded whatever the probe finds. *Absence is a legitimate thing to have acted on*
        // (§9.4): deciding to create an object because it was not there is a decision a later
        // creation must invalidate, and a probe recorded only when it succeeds records exactly the
        // half that needed no protection.
        self.observed.push(CellAt::new(existence.clone(), version));
        if self
            .storage
            .get_cell(&self.path, &existence, version)
            .await?
            .is_some()
        {
            return Ok(());
        }
        let draft = EventDraft {
            value: Value::Bool(true),
            version,
            origin: self.writer.origin(),
            derivation: None,
        };
        self.handle.put(&existence, draft).await?;
        self.authored.push(existence);
        Ok(())
    }

    /// Seal and commit. The commit edge is what triggers dependent producers (§6.2).
    pub async fn commit(self) -> Result<LayerId> {
        self.layers.commit(self.handle).await
    }

    /// Discard. Nothing written becomes visible, so a rejected write leaves no trace (§14).
    pub async fn abort(self) -> Result<()> {
        self.layers.abort(self.handle).await
    }
}
