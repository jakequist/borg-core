//! The command layer: what `borg` does, with nothing about how it was asked or how it is shown.
//!
//! This used to be the middle of `main.rs`, and it was lifted out for `borg serve` (SDK-DRAFT.md
//! §2.6). The rule the split enforces is worth stating, because it is the whole reason serve is
//! small: **an operation returns what happened; the caller renders it.** `main.rs` renders it as
//! lines of text, `serve.rs` renders it as a [`borg_protocol::client::Response`], and neither of
//! them contains a second implementation of a transaction.
//!
//! Every function here is one the CLI already had. Nothing was invented for the socket — if the
//! socket had needed an operation the CLI does not have, that would have been a finding about the
//! CLI, since the CLI is the testbed for what a client is like to use.
//!
//! ## What did *not* come along
//!
//! Two things that look like they belong here and do not.
//!
//! **Choosing which transaction to speak to.** `--tx`, then `$BORG_TX`, then "the only one open" is
//! a shell affordance — it exists so that a terminal that exported one variable reads like §12 is
//! written. A socket client always names its transaction, because it may hold several and there is
//! no environment to inherit one from. So the *selection* stays in `main.rs` and every operation
//! here takes an explicit id.
//!
//! **Printing.** No `outln!`, no `eprintln!`. The one exception is [`auto_derive`], which reports a
//! producer that broke while chasing a write — see there.

use crate::sidecar::{self, Sidecar};
use borg_core::{
    BorgError, BranchId, CellAt, CellRef, ClientVersion, Freshness, FreshnessRequirement, LayerId,
    MergeMode, ObjectDef, ObjectTypeName, Origin, ProducerId, Resolved, Result, Transaction, Value,
    Writer, parse,
};
use borg_engine::{Poisoning, Registry};
use borg_exec_native::NativeExecutor;
use borg_storage_sqlite::SqliteStorage;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const ALLOCATOR: borg_core::AllocatorId = borg_core::AllocatorId(0);

/// What every operation needs to know, however it was asked.
///
/// The CLI fills this from argv; `borg serve` fills it from the message it is answering, which is
/// exactly why it is a struct and not a pile of arguments — a request that names a branch and a
/// freshness is naming the same two things `--branch` and `--freshness` do.
#[derive(Clone, Debug)]
pub struct Ops {
    pub store: PathBuf,
    /// The branch to operate on. `None` is the store's default branch.
    pub branch: Option<String>,
    /// A pinned ClientVersion. `None` means the branch's current def-version — see
    /// [`client_version`].
    pub version: Option<LayerId>,
    pub freshness: FreshnessRequirement,
    /// Read at the settled frontier rather than at the ragged head (SPEC.md §10.5).
    pub settled: bool,
}

impl Ops {
    /// The same store, asked a different question. `borg serve` builds one `Ops` per connection and
    /// then varies branch and freshness per message, which is what this is for.
    pub fn on(&self, branch: Option<String>) -> Self {
        Self {
            branch,
            ..self.clone()
        }
    }
}

// --- Opening the store --------------------------------------------------------------------------

pub async fn open(args: &Ops) -> Result<Registry> {
    let storage = Arc::new(SqliteStorage::open(&args.store)?);
    // The poison table comes from beside the store even on the read path, and especially there:
    // the reader is the one §14 owes an explanation to, and it is never the process that discovered
    // the failure (SPEC.md §14).
    Registry::open_with_poison(
        storage,
        Arc::new(NativeExecutor::new()),
        Arc::new(FilePoison::new(args)),
    )
    .await
}

/// The branch the `Struct#100` shorthand names ids on.
///
/// A PID's branch component records where an object was *allocated*, not where it lives — the whole
/// point of `(branch, allocator, counter)` is that ids never collide, so a fork can inherit an
/// object without renaming it. The shorthand therefore always resolves against the root, or
/// `Company#1` would mean a different object on every branch and a fork could never read what its
/// parent wrote. A canonical address needs none of this: it carries its own branch.
pub fn allocation_branch(registry: &Registry) -> Result<BranchId> {
    registry
        .default_branch()
        .ok_or_else(|| BorgError::Storage("store has no branches — run `borg init`".into()))
}

/// Resolve a branch name, falling back to the root. This selects the *timeline*, which is a separate
/// question from which object a shorthand names.
pub fn branch_of(registry: &Registry, name: Option<&str>) -> Result<BranchId> {
    match name {
        Some(name) => registry
            .branches
            .all()
            .into_iter()
            .find(|b| b.name.as_deref() == Some(name))
            .map(|b| b.id)
            .ok_or_else(|| BorgError::Storage(format!("no branch named `{name}`"))),
        None => registry
            .default_branch()
            .ok_or_else(|| BorgError::Storage("store has no branches — run `borg init`".into())),
    }
}

/// The ClientVersion this operation acts at. SPEC.md §5.4.
///
/// **The branch's current def-version, unless pinned.** Every actor that executes code carries the
/// def-layer its code was authored against; the CLI has no generated code, so each invocation is
/// authored *now*, against the schema as it stands — a client that regenerates itself every time it
/// runs. Nothing is recorded beside the store, because there would be nothing true to record: a
/// remembered version would go stale the moment someone else pushed a def, and one recorded per
/// branch would still be wrong for a branch it was never synced on.
///
/// A pin is how an *older* client is spelled, and it has to exist: §5.4's whole claim is that a v1
/// client keeps reading and writing after the schema moves to v5. `--client-version` is the CLI's
/// spelling; a generated SDK sends the same thing in its hello, which is what makes an SDK client
/// and a pinned CLI invocation the same kind of actor (SDK-DRAFT §2.4).
pub async fn client_version(
    registry: &Registry,
    args: &Ops,
    branch: BranchId,
) -> Result<ClientVersion> {
    if let Some(pinned) = args.version {
        return Ok(ClientVersion(pinned));
    }
    let path = registry.branches.read_path(branch, None)?;
    Ok(ClientVersion(registry.defs.head(&path)))
}

// --- Reads --------------------------------------------------------------------------------------

/// A cell read, rendered far enough that the caller needs no store.
///
/// The rendering has to happen while the registry is open — interned content reads back as content
/// (§3.4), and that is a storage lookup — so it happens here and the envelope leaves whole.
pub struct Read {
    /// The canonical cell. A caller may have asked with the `Company#1` shorthand; this is always
    /// the canonical form.
    pub cell: CellRef,
    pub resolved: Resolved<Option<Value>>,
    /// The value as text — what `borg set` would accept back. Absent means the cell has never been
    /// written, which is distinct from a tombstone.
    pub rendered: Option<String>,
    /// The content-addressed PID behind the value, when there is one. Proof that equal content is
    /// stored once (§3.1); there is nothing to show for a primitive.
    pub interned: Option<String>,
}

async fn read_of(
    registry: &Registry,
    cell: CellRef,
    resolved: Resolved<Option<Value>>,
) -> Result<Read> {
    let rendered = match &resolved.value {
        Some(value) => Some(registry.values.render(value).await?),
        None => None,
    };
    let interned = resolved
        .value
        .as_ref()
        .and_then(|v| registry.values.content_pid(v))
        .map(|pid| format!("@{pid}"));
    Ok(Read {
        cell,
        resolved,
        rendered,
        interned,
    })
}

/// Read a cell, with provenance. SPEC.md §10.4.
pub async fn get(args: &Ops, cell: &str) -> Result<Read> {
    // Computing inline needs the same executor `borg derive` needs, and nothing else does. Paying
    // for it on every read would double the cost of the cheap modes to serve the expensive one.
    let (registry, workers) = if args.freshness == FreshnessRequirement::Current {
        let (registry, workers) = open_deriving(args).await?;
        (registry, Some(workers))
    } else {
        (open(args).await?, None)
    };
    let branch = branch_of(&registry, args.branch.as_deref())?;
    let cell = parse::cell_ref(cell, allocation_branch(&registry)?, ALLOCATOR)?;

    // Two honest consistency modes (SPEC.md §10.5). Ragged head is the latest of everything with
    // per-field freshness; the settled frontier is one layer where nothing is behind anything else.
    let at = if args.settled {
        Some(registry.settled(branch).await?)
    } else {
        None
    };

    let resolved = registry
        .resolver
        .resolve(
            branch,
            &cell,
            at,
            client_version(&registry, args, branch).await?,
            args.freshness,
        )
        .await;
    // Stopped before the read's outcome is raised: a `current` read that could not compute is still
    // a read that started workers, and leaving them for process exit to clean up is how a longer-
    // lived host inherits a leak.
    if let Some(workers) = workers {
        workers.shutdown().await;
    }
    read_of(&registry, cell, resolved?).await
}

/// Where a value came from. SPEC.md §11. `None` where nothing is stored.
pub async fn explain(args: &Ops, cell: &str) -> Result<(CellRef, Option<borg_engine::Lineage>)> {
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;
    let cell = parse::cell_ref(cell, allocation_branch(&registry)?, ALLOCATOR)?;
    let version = client_version(&registry, args, branch).await?;
    let lineage = registry
        .resolver
        .explain(branch, &cell, None, version)
        .await?;
    Ok((cell, lineage))
}

/// Every branch in the store, in creation order.
pub async fn branch_list(args: &Ops) -> Result<Vec<borg_core::Branch>> {
    let registry = open(args).await?;
    let mut branches = registry.branches.all();
    branches.sort_by_key(|b| b.id.0);
    Ok(branches)
}

/// A branch and its head layer. `L0` where the branch holds nothing of its own.
pub async fn branch_head(args: &Ops) -> Result<(BranchId, LayerId)> {
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;
    Ok((branch, registry.layers.head(branch).unwrap_or(LayerId(0))))
}

/// A struct's definitions, as this branch holds them. What codegen reads (SPEC.md §15).
pub async fn def_show(args: &Ops, name: &str) -> Result<ObjectDef> {
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;
    let path = registry.branches.read_path(branch, None)?;
    let view = registry.defs.view(&path).await?;

    let name: ObjectTypeName = name.into();
    view.object(&name)
        .cloned()
        .ok_or_else(|| BorgError::Storage(format!("no struct named `{name}`")))
}

/// A branch's definitions, whole, and the def-version they were read at. What codegen reads
/// (SPEC.md §15, SDK-DRAFT §4.4).
///
/// **Both facts from one open**, which is the reason this is not `def_show` in a loop beside
/// `def_version`. A generated module stamps itself with the version of the schema it was generated
/// from; taking the two separately would let a def push land in between and produce a module whose
/// stamp names a schema it does not contain.
///
/// Sorted here rather than by the caller, so that `borg generate` and the socket answer in the same
/// order and a regenerated file is byte-identical when nothing moved.
pub async fn def_view(args: &Ops) -> Result<(LayerId, Vec<ObjectDef>)> {
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;
    let path = registry.branches.read_path(branch, None)?;
    let view = registry.defs.view(&path).await?;
    let mut structs: Vec<ObjectDef> = view.objects().cloned().collect();
    structs.sort_by(|a, b| a.name.cmp(&b.name));
    Ok((registry.defs.head(&path), structs))
}

/// The def-version in force on a branch — the ClientVersion a client generated right now would carry
/// (SPEC.md §5.3, §5.4). A def-version *is* a layer id; there is no separate scheme.
pub async fn def_version(args: &Ops) -> Result<LayerId> {
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;
    let path = registry.branches.read_path(branch, None)?;
    Ok(registry.defs.head(&path))
}

// --- Transactions. SPEC.md §12, §13. ------------------------------------------------------------

/// How long a transaction may sit untouched before it is reaped, when nobody has said otherwise.
///
/// Generous on purpose: the cost of reaping too eagerly is a client losing work it was in the middle
/// of, and the cost of reaping too late is a branch row and a read-set sitting in a file. Those are
/// not the same size of mistake.
pub const DEFAULT_TX_IDLE_TIMEOUT: u64 = 24 * 60 * 60;

/// How many ended transactions are remembered, so that touching one can say what became of it rather
/// than "unknown transaction". Bounded because this is a courtesy, not a log.
const REMEMBERED_TRANSACTIONS: usize = 64;

/// The open transactions, beside the store.
///
/// A transaction spans several client calls, so it needs somewhere to keep two things: which branch
/// it forked, and what it has read so far. That goes here, with the pause flags and the
/// producer-implementation table (§9.2), for the same reason they do — see `crate::sidecar`.
///
/// **This is also why a transaction survives a dropped socket.** The table is beside the *store*,
/// not inside the process serving it and certainly not inside the connection: a browser that
/// reloaded mid-transaction reconnects and carries on by naming the same id, and one that never
/// comes back is reaped like any other silence (SDK-DRAFT §2.5, §12.3).
///
/// Every id in this file is a layer or a branch id, which are sequential counters; there is no
/// producer id in it, and so nothing that needs `sidecar::producer_id`.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct Transactions {
    /// Seconds. Rendered and accepted as a duration by `borg tx timeout`.
    pub tx_idle_timeout: u64,
    /// The next handle to hand out. Monotonic and never reused, so a stale id names a transaction
    /// that existed rather than one that is about to.
    pub next: u64,
    pub open: Vec<Open>,
    /// What became of the transactions that ended.
    pub spent: Vec<Spent>,
}

impl Default for Transactions {
    fn default() -> Self {
        Self {
            tx_idle_timeout: DEFAULT_TX_IDLE_TIMEOUT,
            next: 1,
            open: Vec::new(),
            spent: Vec::new(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Open {
    pub id: String,
    /// Unix seconds, when this transaction was last used. **Idle, not elapsed**: a legitimate
    /// transaction may run for hours, and what predicts an abandoned one is silence.
    pub touched: u64,
    pub state: Transaction,
}

/// A transaction that ended, and how.
///
/// The `fate` is a phrase rather than a code because it is going straight into a sentence, and the
/// sentence is the point: *"expired after 2 minutes idle"* tells a client what to do next, and
/// *"unknown transaction"* tells it to go and look for a bug it does not have.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Spent {
    pub id: String,
    pub fate: String,
}

impl Sidecar for Transactions {
    const EXTENSION: &'static str = "transactions.json";
}

pub fn load_transactions(args: &Ops) -> Transactions {
    sidecar::load(&args.store)
}

pub fn save_transactions(args: &Ops, table: &Transactions) -> Result<()> {
    sidecar::save(&args.store, table)
}

pub fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

/// `90s`, `10m`, `24h`, `7d`, or a bare count of seconds.
pub fn duration(text: &str) -> Option<u64> {
    let (digits, scale) = match text.as_bytes().last()? {
        b's' => (&text[..text.len() - 1], 1),
        b'm' => (&text[..text.len() - 1], 60),
        b'h' => (&text[..text.len() - 1], 60 * 60),
        b'd' => (&text[..text.len() - 1], 24 * 60 * 60),
        _ => (text, 1),
    };
    digits.parse::<u64>().ok().map(|n| n * scale)
}

/// The largest whole unit that describes this exactly, which is how it was almost certainly typed.
pub fn render_duration(seconds: u64) -> String {
    for (scale, suffix) in [(86400, 'd'), (3600, 'h'), (60, 'm')] {
        if seconds >= scale && seconds.is_multiple_of(scale) {
            return format!("{}{suffix}", seconds / scale);
        }
    }
    format!("{seconds}s")
}

/// The same duration spelled for a sentence rather than for a config file.
pub fn spell_duration(seconds: u64) -> String {
    let (count, unit) = [(86400, "day"), (3600, "hour"), (60, "minute")]
        .into_iter()
        .find(|(scale, _)| seconds >= *scale && seconds.is_multiple_of(*scale))
        .map_or((seconds, "second"), |(scale, unit)| (seconds / scale, unit));
    format!("{count} {unit}{}", if count == 1 { "" } else { "s" })
}

/// Drop transactions that have been idle too long. SPEC.md §12.3.
///
/// **No daemon.** This runs when a process opens the store, which is the same place the indexes are
/// already rebuilt from the log — so an idle store sweeps nothing, because nothing is growing, and a
/// busy one sweeps constantly for free. For the CLI "a process opens the store" is `run()`; for
/// `borg serve` it is every request, since serve opens the store per request for the same reason the
/// CLI does (see `crate::serve`).
///
/// A reaped transaction's *state* is dropped, which is what makes it unusable: nothing can be
/// written through it and its layers can never merge. Its branch row is left where it is, because
/// whether spent branches are reaped or kept as history is a real choice and is deliberately not
/// being made by a janitor as a side effect (ROADMAP.md, open questions).
pub fn reap_transactions(args: &Ops) -> Result<()> {
    let mut table = load_transactions(args);
    if table.open.is_empty() {
        return Ok(());
    }
    let now = now();
    let timeout = table.tx_idle_timeout;
    let mut reaped = Vec::new();
    table.open.retain(|open| {
        if now.saturating_sub(open.touched) <= timeout {
            return true;
        }
        reaped.push((open.id.clone(), open.state.branch));
        false
    });
    if reaped.is_empty() {
        return Ok(());
    }
    for (id, branch) in reaped {
        set_paused(args, branch, false)?;
        retire(
            &mut table,
            id,
            format!("expired after {} idle", spell_duration(timeout)),
        );
    }
    save_transactions(args, &table)
}

pub fn retire(table: &mut Transactions, id: String, fate: String) {
    table.spent.push(Spent { id, fate });
    let excess = table.spent.len().saturating_sub(REMEMBERED_TRANSACTIONS);
    table.spent.drain(..excess);
}

/// Find an open transaction by id.
///
/// **Never "unknown transaction" for one that existed.** §12.3 is explicit about this: the first
/// tells you what happened and what to do; the second sends you looking for a bug in your own
/// bookkeeping. It matters more over a socket than it did in a shell, because the client that
/// reconnects to find its transaction gone is the browser tab that was reloaded.
pub fn transaction_index(table: &Transactions, id: &str) -> Result<usize> {
    if let Some(index) = table.open.iter().position(|open| open.id == id) {
        return Ok(index);
    }
    if let Some(spent) = table.spent.iter().find(|spent| spent.id == id) {
        return Err(BorgError::Storage(format!(
            "transaction {id} {}",
            spent.fate
        )));
    }
    Err(BorgError::Storage(format!("unknown transaction {id}")))
}

/// The highest layer this branch can see — its own head, or the fork point it inherits when it has
/// written nothing yet. `None` where the whole ancestry is empty.
///
/// A transaction forks *here* rather than at `head(branch)`, which is what lets the first write on a
/// fresh fork be a transaction like every other write. The branch it merges back into is still the
/// one the client named, which is why a transaction carries its parent rather than inferring it.
pub fn fork_point_of(registry: &Registry, branch: BranchId) -> Result<Option<LayerId>> {
    let ceiling = registry.branches.read_path(branch, None)?.ceiling();
    Ok((ceiling.0 != 0).then_some(ceiling))
}

pub fn owner_of(registry: &Registry, layer: LayerId) -> Result<BranchId> {
    registry
        .layers
        .layer(layer)
        .map(|layer| layer.branch)
        .ok_or_else(|| BorgError::Storage(format!("unknown layer {layer}")))
}

/// Fold a finished write session's reads and writes into the transaction that owns it.
///
/// Reads before writes, always: every probe a session makes precedes any write it makes to the same
/// cell, so draining them in this order is what gets [`Transaction::observe`]'s ordering rule right
/// — a read that came *before* the transaction's own write is a real dependency on the parent and is
/// guarded, and a read that came after saw only the transaction itself.
pub fn absorb(transaction: &mut Transaction, session: &borg_engine::WriteSession) {
    for read in session.observed() {
        transaction.observe(read.clone());
    }
    for write in session.authored() {
        transaction.wrote(write.clone());
    }
}

/// The layer a transaction landed in on its parent — what a client awaits with `frontier reaches`.
/// A transaction that wrote nothing landed nowhere, and says head.
pub fn landing(registry: &Registry, transaction: &Transaction, replayed: &[LayerId]) -> LayerId {
    replayed.last().copied().unwrap_or_else(|| {
        registry
            .layers
            .head(transaction.parent)
            .unwrap_or(LayerId(0))
    })
}

/// Fork the branch and open a transaction. Returns the handle.
pub async fn tx_begin(args: &Ops) -> Result<String> {
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;
    let fork_point = fork_point_of(&registry, branch)?
        .ok_or_else(|| BorgError::Storage("nothing to fork from — the branch is empty".into()))?;

    let mut table = load_transactions(args);
    let id = format!("tx-{}", table.next);
    table.next += 1;
    let forked = registry
        .branches
        .fork(
            owner_of(&registry, fork_point)?,
            fork_point,
            Some(id.clone()),
        )
        .await?;
    // **Created paused.** Deriving on a branch that exists to be merged is waste: merge does not
    // carry derived layers, and the parent recomputes what it needs (§13).
    set_paused(args, forked, true)?;

    table.open.push(Open {
        id: id.clone(),
        touched: now(),
        state: Transaction::new(forked, branch, fork_point),
    });
    save_transactions(args, &table)?;
    Ok(id)
}

/// Read through a transaction, and **record the read**.
///
/// The recording is the whole difference from [`get`], and it is what makes the guard at commit
/// automatic. Reads that find nothing are recorded too: absence is a legitimate thing to have acted
/// on, and a later write to that cell must invalidate the decision (§9.4).
pub async fn tx_get(args: &Ops, tx: &str, cell: &str) -> Result<Read> {
    let mut table = load_transactions(args);
    let index = transaction_index(&table, tx)?;
    let registry = open(args).await?;
    let cell = parse::cell_ref(cell, allocation_branch(&registry)?, ALLOCATOR)?;
    let branch = table.open[index].state.branch;

    let version = client_version(&registry, args, branch).await?;
    // The def-version of *this field*, as this reader's own view names it — the record key, and the
    // only bridge from a whole-schema ClientVersion to one (§5.3, §5.4). Recording the read at the
    // reader's ClientVersion instead would file it under a version nothing else uses.
    let path = registry.branches.read_path(branch, None)?;
    let at = registry
        .defs
        .view_at(&path, version.0)
        .await?
        .version_of(&cell);

    let resolved = registry
        .resolver
        .resolve(branch, &cell, None, version, args.freshness)
        .await?;

    let open = &mut table.open[index];
    open.state.observe(CellAt::new(cell.clone(), at));
    open.touched = now();
    save_transactions(args, &table)?;

    read_of(&registry, cell, resolved).await
}

/// Write through a transaction. Returns the layer it landed in **on the transaction's own branch**,
/// which nobody else can see until it merges.
pub async fn tx_set(args: &Ops, tx: &str, cell: &str, value: &str) -> Result<LayerId> {
    let mut table = load_transactions(args);
    let index = transaction_index(&table, tx)?;
    let registry = open(args).await?;
    let cell = parse::cell_ref(cell, allocation_branch(&registry)?, ALLOCATOR)?;
    let branch = table.open[index].state.branch;

    let version = client_version(&registry, args, branch).await?;
    let mut session = registry
        .begin_write(branch, version, Writer::Client)
        .await?;
    if let Err(rejection) = session.set_text(&cell, value).await {
        session.abort().await?;
        return Err(rejection);
    }
    let open = &mut table.open[index];
    absorb(&mut open.state, &session);
    open.touched = now();

    let layer = session.commit().await?;
    save_transactions(args, &table)?;
    Ok(layer)
}

/// Merge, guarded by everything the transaction read. SPEC.md §12, §13.
///
/// Returns the layer it landed in on its parent, and catches the parent up — the auto-derivation a
/// write owes is part of committing, not part of being a CLI, which is what keeps a socket commit
/// and a `borg tx commit` the same event.
pub async fn tx_commit(args: &Ops, tx: &str) -> Result<LayerId> {
    let mut table = load_transactions(args);
    let index = transaction_index(&table, tx)?;
    let registry = open(args).await?;
    let state = table.open[index].state.clone();

    let replayed = match registry
        .branches
        .merge_transaction(&state, MergeMode::DefAndData)
        .await
    {
        Ok(replayed) => replayed,
        Err(rejection) => {
            // **The transaction stays open.** Its snapshot is stale and its commit cannot succeed,
            // but the read-set is what a client needs in order to decide whether to retry or to give
            // up, and throwing it away here would leave them holding an error and nothing else. The
            // other half of that is `tx_abort`, and the timeout is what collects the ones nobody
            // comes back to.
            table.open[index].touched = now();
            save_transactions(args, &table)?;
            return Err(rejection);
        }
    };

    let spent = table.open.remove(index);
    retire(&mut table, spent.id, "already committed".into());
    save_transactions(args, &table)?;
    set_paused(args, state.branch, false)?;

    let landed = landing(&registry, &state, &replayed);
    drop(registry);
    // **The commit landed, whatever happens next.** Derivation is chased here because there is no
    // scheduler to hand it to (§9.6), and a merge that succeeded must not be reported as a failure
    // because the executor behind somebody else's pipeline could not be built — the same reasoning
    // `auto_derive` already applies to a round that fails once it is running, applied to the one
    // failure that used to escape it. Over a socket this stops being cosmetic: a client told its
    // commit failed, whose commit landed, will do the work twice.
    if let Err(err) = auto_derive(args, state.parent).await {
        eprintln!("warning: auto-derivation did not start: {err}");
    }
    Ok(landed)
}

/// Drop a transaction. Nothing it wrote is on the parent, because nothing it wrote ever left its own
/// branch — which is what makes an abort free.
pub fn tx_abort(args: &Ops, tx: &str) -> Result<()> {
    let mut table = load_transactions(args);
    let index = transaction_index(&table, tx)?;
    let spent = table.open.remove(index);
    set_paused(args, spent.state.branch, false)?;
    retire(&mut table, spent.id, "was aborted".into());
    save_transactions(args, &table)
}

// --- Producers, derivation and the state beside the store ---------------------------------------

/// Where a producer's implementation lives.
///
/// Deliberately *not* in the log. §9.2 separates a producer's definition from its implementation:
/// the log records that producer P exists at some ClientVersion, and the `ExecutionProvider`
/// resolves that id to code. Writing a local file path into the log would tie the data model to one
/// machine's filesystem. This sidecar is the CLI's own resolution table, and a container-backed
/// runtime would keep an image reference in exactly the same place.
#[derive(Default, serde::Serialize, serde::Deserialize)]
pub struct Implementations {
    pub producers: Vec<Implementation>,
}

impl Sidecar for Implementations {
    const EXTENSION: &'static str = "producers.json";
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Implementation {
    /// A string, and this is the file that made the case for it — see `sidecar::producer_id`.
    #[serde(with = "sidecar::producer_id")]
    pub id: u64,
    pub name: String,
    pub source: String,
    pub command: PathBuf,
    /// How to speak to this producer's worker (§17.4). It comes from the same `describe` that
    /// declared the producer, so it lives beside the command rather than in the log: like the
    /// command, it is a fact about *this machine's* copy of the code and not about what is true.
    ///
    /// A table written before transports existed has no such field, and defaults to stdio.
    #[serde(default)]
    pub transport: borg_protocol::Transport,
}

pub fn load_impls(args: &Ops) -> Implementations {
    sidecar::load(&args.store)
}

pub fn save_impls(args: &Ops, impls: &Implementations) -> Result<()> {
    sidecar::save(&args.store, impls)
}

/// Record where a producer's code lives. Producer ids are stable across pushes, so this replaces
/// rather than accumulates.
pub fn remember(
    impls: &mut Implementations,
    id: ProducerId,
    name: &str,
    source: &str,
    command: &Path,
    transport: borg_protocol::Transport,
) {
    impls.producers.retain(|p| p.id != id.0);
    impls.producers.push(Implementation {
        id: id.0,
        name: name.to_string(),
        transport,
        // The struct a worker is invoked over. A migration maps over one of its *fields* (§9.3), but
        // what it is handed is still the entity — `Company:o-1234abcd`, to which it appends the
        // field name — so the struct is what this needs to render.
        source: source.to_string(),
        command: command
            .canonicalize()
            .unwrap_or_else(|_| command.to_path_buf()),
    });
}

/// Open the store with an executor that can run this store's producers, and make the engine aware
/// of the ones defined on the branch.
///
/// Three paths need exactly this and need it identically: `borg derive`, the auto-derivation that
/// follows a write, and a `--freshness current` read. Producer *definitions* live in the log and
/// producer *implementations* in the sidecar beside it (§9.2), so being able to run anything at all
/// means joining the two — and doing that in three places is how they drift.
pub async fn open_deriving(
    args: &Ops,
) -> Result<(Registry, Arc<borg_exec_process::ProcessExecutor>)> {
    let storage = Arc::new(SqliteStorage::open(&args.store)?);
    let impls = load_impls(args);

    // A worker resolves the `Company#1` shorthand against the allocating branch, which is a fact
    // about the store — so the store has to be open before the executor can be built.
    let probe = Registry::open(
        Arc::clone(&storage) as Arc<_>,
        Arc::new(NativeExecutor::new()),
    )
    .await?;
    let allocation = allocation_branch(&probe)?;
    drop(probe);

    let parallelism = derive_parallelism();
    let executor = borg_exec_process::from_registrations(
        allocation,
        impls
            .producers
            .iter()
            .map(|p| borg_exec_process::Registration {
                producer: p.id,
                command: p.command.clone(),
                source: p.source.clone(),
                transport: p.transport,
            }),
    )
    // The pool matches the scheduler, because a pool smaller than the degree of parallelism would
    // put the queue back that the pool exists to remove.
    .with_pool_size(parallelism);
    let executor = Arc::new(executor);
    let registry = Registry::open_with_poison(
        storage,
        Arc::clone(&executor) as Arc<_>,
        Arc::new(FilePoison::new(args)),
    )
    .await?;
    registry.engine.set_parallelism(parallelism);

    registry
        .register_producers(branch_of(&registry, args.branch.as_deref())?)
        .await?;
    Ok((registry, executor))
}

/// How many invocations a round runs at once, and how many worker processes back them.
///
/// An environment variable rather than a flag or a sidecar file. It is neither log data nor a fact
/// about the store — the same store derived on a laptop and on a build box wants different numbers —
/// so there is nothing to record, and every command that derives would otherwise need the same flag.
/// Unset means one per core, which is what the engine picks for itself.
pub fn derive_parallelism() -> usize {
    std::env::var("BORG_DERIVE_PARALLELISM")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get)
        })
}

/// Catch a branch up after a commit. SPEC.md §9.6.
///
/// **This is what "materialization is continuous" means here.** The CLI is process-per-command, so
/// there is no daemon to run a loop in; the process that commits a layer is the one in a position to
/// chase it, and it does so before exiting. A worker pool with its own scheduler is a strictly better
/// shape and needs a server to live in — which is the point at which this call becomes a signal
/// rather than a call, with nothing above it changing, because §9.6 already says scheduling policy
/// cannot affect correctness. `borg serve` is not yet that server: it calls this, exactly as the CLI
/// does, so that a socket commit and a `borg tx commit` derive identically.
pub async fn auto_derive(args: &Ops, branch: BranchId) -> Result<()> {
    if paused_branches(args).contains(&branch.0) {
        return Ok(());
    }
    // No implementations means nothing here can run, and building an executor to discover that would
    // charge every store without producers for the ones that have them.
    if load_impls(args).producers.is_empty() {
        return Ok(());
    }
    let (registry, workers) = open_deriving(args).await?;
    let before: Vec<_> = broken_here(args, &registry, branch)?
        .into_iter()
        .map(|(_, poisoning)| poisoning.producer)
        .collect();
    let caught_up = registry.engine.catch_up(branch).await;
    workers.shutdown().await;

    // **A producer that broke while chasing this write is news, and only this process has it.** The
    // write itself is fine — §14 scopes the failure to the producer — but the derived data that was
    // about to follow is not coming, and saying nothing here is how somebody finds out from a
    // `state: broken` an hour later. On stderr rather than returned, because it is not the outcome of
    // the command: the write landed. A server's stderr is its log, which is where this belongs there
    // too.
    for (name, poisoning) in broken_here(args, &registry, branch)? {
        if !before.contains(&poisoning.producer) {
            eprintln!("warning: {name} is now broken: {}", poisoning.error);
        }
    }

    if let Err(err) = caught_up {
        // Reported, not raised. The write this followed is ground truth whether or not anything has
        // chased it yet, and §9.6's licence is exactly that scheduling cannot affect correctness — a
        // write that failed because somebody else's pipeline is unrunnable would be a write failing
        // for a reason that has nothing to do with it.
        eprintln!("warning: auto-derivation did not complete: {err}");
    }
    Ok(())
}

/// Derivation's operational state, beside the store.
///
/// Beside the store, like the producer-implementation table and the transaction table, and for the
/// same reason (`crate::sidecar`): neither half of this changes what is true. Pausing changes only
/// when the system catches up; a poisoning is the engine's judgement about code, discovered at
/// runtime. In the log both would be forkable, mergeable and time-travellable, and "was derivation
/// paused at layer 400?" is not a question anybody has.
///
/// Branch *ids*, not names: a branch may be renamed or unnamed, and the id is what the log uses.
#[derive(Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct DerivationConfig {
    pub paused: Vec<u64>,
    /// Producers judged broken. SPEC.md §14, and `borg_engine::poison` for why they belong here
    /// rather than in the log.
    pub broken: Vec<BrokenProducer>,
}

impl Sidecar for DerivationConfig {
    const EXTENSION: &'static str = "derivation.json";
}

/// One poisoning, as this client writes it down.
///
/// The wire form lives here and not in the engine because the file is the CLI's, the way
/// `producers.json` is: a server keeps the same facts somewhere else entirely, and neither of them
/// is the engine's business.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct BrokenProducer {
    pub branch: u64,
    #[serde(with = "sidecar::producer_id")]
    pub producer: u64,
    /// The ClientVersion it was running at. This is what makes the record self-expiring, and what
    /// makes §14's recovery — push fixed code — need no other machinery.
    pub version: u64,
    pub error: String,
    pub since: u64,
}

pub fn load_derivation(args: &Ops) -> DerivationConfig {
    sidecar::load(&args.store)
}

pub fn save_derivation(args: &Ops, config: &DerivationConfig) -> Result<()> {
    sidecar::save(&args.store, config)
}

pub fn paused_branches(args: &Ops) -> Vec<u64> {
    load_derivation(args).paused
}

/// Turn auto-derivation on or off for one branch.
///
/// Factored out because transactions use it too: a transaction branch is **created paused** (§12),
/// and a transaction that ends resumes what it paused rather than leaving a flag behind for a branch
/// id that will never be written to again.
pub fn set_paused(args: &Ops, branch: BranchId, pause: bool) -> Result<()> {
    let mut config = load_derivation(args);
    config.paused.retain(|id| *id != branch.0);
    if pause {
        config.paused.push(branch.0);
    }
    save_derivation(args, &config)
}

/// The poison table, beside the store. SPEC.md §14.
///
/// **This is the whole of what makes §14 true for a client that exits after every command.** The
/// engine's own table is a `HashMap` in the process that discovered the failure; for the CLI that
/// process is gone by the time anybody reads, so the next `borg get` used to call a broken
/// producer's output `stale` — a promise of a catch-up that was never coming — and the next
/// `borg derive` used to run the failing code again from scratch.
pub struct FilePoison {
    /// The store, not the sidecar's own path: the file is named from it, and naming it in one place
    /// is what keeps this and `load_derivation` reading the same file.
    store: PathBuf,
    /// Read once and held for the life of the command. One process does one thing here, so nothing
    /// can move the file underneath it — and re-reading per lookup would put a syscall inside the
    /// scheduler's per-producer loop.
    table: std::sync::Mutex<Vec<BrokenProducer>>,
}

impl FilePoison {
    pub fn new(args: &Ops) -> Self {
        Self {
            store: args.store.clone(),
            table: std::sync::Mutex::new(load_derivation(args).broken),
        }
    }

    /// Write the table back, preserving the pause flags it shares the file with.
    ///
    /// Re-read rather than remembered, for the same reason [`set_paused`] re-reads: the two halves
    /// are edited by different commands, and holding a whole-file snapshot across a command would
    /// let one of them silently revert the other.
    fn flush(&self, table: &[BrokenProducer]) -> Result<()> {
        let mut config: DerivationConfig = sidecar::load(&self.store);
        config.broken = table.to_vec();
        sidecar::save(&self.store, &config)
    }
}

impl borg_engine::PoisonProvider for FilePoison {
    fn poisoned(&self, branch: BranchId) -> Result<Vec<Poisoning>> {
        Ok(self
            .table
            .lock()
            .unwrap()
            .iter()
            .filter(|row| row.branch == branch.0)
            .map(|row| Poisoning {
                producer: ProducerId(row.producer),
                version: LayerId(row.version),
                error: row.error.clone(),
                since: LayerId(row.since),
            })
            .collect())
    }

    fn poison(&self, branch: BranchId, poisoning: Poisoning) -> Result<()> {
        let mut table = self.table.lock().unwrap();
        table.retain(|row| row.branch != branch.0 || row.producer != poisoning.producer.0);
        table.push(BrokenProducer {
            branch: branch.0,
            producer: poisoning.producer.0,
            version: poisoning.version.0,
            error: poisoning.error,
            since: poisoning.since.0,
        });
        self.flush(&table)
    }

    fn clear(&self, branch: BranchId, producer: ProducerId) -> Result<()> {
        let mut table = self.table.lock().unwrap();
        table.retain(|row| row.branch != branch.0 || row.producer != producer.0);
        self.flush(&table)
    }
}

/// What a human calls a producer. The log knows ids; only the implementation table knows names.
pub fn producer_name(names: &Implementations, producer: ProducerId) -> String {
    names
        .producers
        .iter()
        .find(|impl_| impl_.id == producer.0)
        .map_or_else(|| producer.to_string(), |impl_| impl_.name.clone())
}

/// Which producers are broken on this branch, under the names a human gave them. SPEC.md §14.
///
/// Everything that reports a poisoning goes through here, and everything reports it on **stderr**
/// and never as a failure: a broken producer is not a broken command. `borg derive` still derives
/// everything else, and a write followed by a poisoned pipeline is still a write that landed.
pub fn broken_here(
    args: &Ops,
    registry: &Registry,
    branch: BranchId,
) -> Result<Vec<(String, Poisoning)>> {
    let names = load_impls(args);
    Ok(registry
        .engine
        .broken(branch)?
        .into_iter()
        .map(|poisoning| (producer_name(&names, poisoning.producer), poisoning))
        .collect())
}

// --- The names the envelope's enums have on every surface ----------------------------------------

/// SPEC.md §10.4's state names. One spelling, so what `borg get` prints and what the socket answers
/// cannot drift into two vocabularies for one fact.
pub const fn state_name(state: Freshness) -> &'static str {
    match state {
        Freshness::Current => "current",
        Freshness::Unvalidated => "unvalidated",
        Freshness::Stale => "stale",
        Freshness::Broken => "broken",
        Freshness::Tombstoned => "tombstoned",
    }
}

pub const fn origin_name(origin: Origin) -> &'static str {
    match origin {
        Origin::Source => "source",
        Origin::Derived => "derived",
    }
}

/// What a read is willing to pay for. SPEC.md §10.5.
///
/// `validated` is the default because it is the one that is always honest and never expensive: it
/// walks the read-set and runs no user code. `any` is cheaper and says less; `current` computes and
/// blocks.
pub fn freshness(mode: &str) -> Option<FreshnessRequirement> {
    match mode {
        "any" => Some(FreshnessRequirement::Any),
        "validated" => Some(FreshnessRequirement::Validated),
        "current" => Some(FreshnessRequirement::Current),
        _ => None,
    }
}
