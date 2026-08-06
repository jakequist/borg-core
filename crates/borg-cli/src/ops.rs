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
use borg_storage::StorageProvider;
use borg_storage_sqlite::SqliteStorage;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The allocator the `Company#1` shorthand names. SPEC.md §3.1.
///
/// Zero, and it belongs to **hand-authored ids**: `Company#1` means counter 1 under this allocator on
/// the root branch, so a scenario, a fixture or a person at a terminal picks their own counters here
/// and nothing else may.
pub const ALLOCATOR: borg_core::AllocatorId = borg_core::AllocatorId(0);

/// The allocator [`tx_create`] hands out ids under. SPEC.md §3.1, §17.5.
///
/// **Not `0`**, and that is the whole of why server-side allocation needs no coordination with the
/// people typing `Contact#5` into a shell. A PID is `(branch, allocator, counter)` so that any
/// number of allocating authorities can issue ids without agreeing on anything; two of them exist
/// here — a human choosing counters by hand, and this — and giving them separate allocator ids makes
/// their ids disjoint *by construction* rather than by a convention somebody has to remember. It is
/// the same property a second node would rely on, arriving one node early.
pub const SERVER_ALLOCATOR: borg_core::AllocatorId = borg_core::AllocatorId(1);

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
    /// The registry this process holds open for its whole life, if it is a process that holds one.
    ///
    /// **`None` is the CLI and `Some` is `borg serve`**, and it is on `Ops` rather than threaded
    /// through every operation for a reason worth stating: not one function below changes shape.
    /// [`open`] answers "the registry to work through" either way, so an operation cannot tell which
    /// lifecycle it is running in — which is exactly the property that makes a held registry a
    /// lifecycle change rather than a second implementation of every command.
    pub held: Option<Arc<Held>>,
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

/// A store held open for the life of a process, with the workers that derive on it.
///
/// **One `Registry`, and that is the whole point.** `Registry::open` brings the log's projections to
/// head (`borg_engine::projection`), which for a fresh set means replaying every committed layer. A
/// process-per-command CLI pays that once per command, which is honest. A server that opened per
/// *request* paid it per read, and that multiplication is `examples/personal-crm/FRICTION.md` #9:
/// 18.4 ms per read at L441 rising to 53.0 ms at L1391, on a request whose size never changed.
///
/// **Safe because the advisory lock already made serve the only writer.** `borg serve` takes the
/// lock and every other `borg` invocation against that store is refused by name (`crate::serve`), so
/// every mutation of this store flows through this instance — which is precisely the precondition a
/// cache needs. The lock was built for honesty about the single-process assumption; holding a
/// registry is what that honesty buys.
///
/// The executor is here rather than rebuilt per derivation because it is the other half of the same
/// lifecycle: `tx_commit` used to drop its registry so that `auto_derive` could open another one
/// *with* an executor, and two live registries over one store are what the single-process assumption
/// forbids. Carrying the executor on the long-lived registry removes the dance rather than working
/// around it.
pub struct Held {
    registry: Registry,
    workers: Arc<borg_exec_process::ProcessExecutor>,
    /// The producer table as it stood when the executor was built. See [`Held::producers_moved`].
    registered: Vec<String>,
}

impl std::fmt::Debug for Held {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Held({} producers)", self.registered.len())
    }
}

impl Held {
    /// Whether the producer sidecar has moved under a running server.
    ///
    /// **It cannot, and this is the assertion that says so.** `borg repo push` writes
    /// `producers.json` and reads a directory off this machine's disk, so it is not on the socket
    /// (SDK-DRAFT §4.3) and is refused outright while a store is served — pushing a schema means
    /// stopping the server (`CLAUDE.md`). That refusal is what makes registration-at-boot sound: the
    /// executor is built once from a table nothing can edit while it is in use. If that ever stops
    /// being true, the symptom would be a producer silently running the wrong binary, which is the
    /// worst possible way to find out — so it is checked rather than assumed, on the derivation path
    /// where a small file read is already lost in the noise.
    fn producers_moved(&self, args: &Ops) -> bool {
        current_registrations(&load_impls(args)) != self.registered
    }

    /// Stop the worker pool. Called when the server stops, and only then.
    pub async fn shutdown(&self) {
        self.workers.shutdown().await;
    }
}

/// Everything about the producer table that the executor was built from.
///
/// All four fields, not just the command: a producer whose `source` moved maps over a different
/// struct and one whose transport changed is spoken to differently, and either would make the pool
/// wrong in a way no error would name.
fn current_registrations(impls: &Implementations) -> Vec<String> {
    let mut found: Vec<String> = impls
        .producers
        .iter()
        .map(|p| {
            format!(
                "{}|{}|{}|{:?}",
                p.id,
                p.source,
                p.command.display(),
                p.transport
            )
        })
        .collect();
    found.sort();
    found
}

/// Open the store for the life of a server: one registry, one worker pool, both held.
///
/// The counterpart to every `let registry = open(args).await?` below — those answer *this* registry
/// once it exists. See [`Held`] for why it is safe and what it is worth.
pub async fn hold(args: &Ops) -> Result<Arc<Held>> {
    let storage = Arc::new(SqliteStorage::open(&args.store)?);
    let impls = load_impls(args);
    // **A store with no root branch is refused here rather than per request.** It used to start and
    // answer *run `borg init`* to everything asked of it, because it opened per request; a server
    // that decides once has to decide at boot. Said as its own sentence, because "no branches" at
    // startup reads like a defect in the server rather than a store that has not been created.
    let (registry, workers) = open_with_workers(args, storage, &impls)
        .await
        .map_err(|err| match err {
            BorgError::Storage(message) if message.contains("borg init") => BorgError::Storage(
                format!("{message} — a store has to exist before it can be served"),
            ),
            other => other,
        })?;
    // Producers for the default branch, at boot. A request naming another branch registers that
    // branch's producers on the way through (`auto_derive`); `register` is an insert into a map, so
    // doing it again costs a def-view fold and nothing else.
    //
    // **A `--branch` naming a branch that does not exist is not a boot failure.** It is a default a
    // request may never take: every message may name its own branch, and refusing to start over a
    // default nothing asks for would be a new opinion arriving on a lifecycle change. A store with
    // no branches *at all* was already refused above, which is the different statement.
    if let Ok(branch) = branch_of(&registry, args.branch.as_deref()) {
        registry.register_producers(branch).await?;
    }
    Ok(Arc::new(Held {
        registry,
        workers,
        registered: current_registrations(&impls),
    }))
}

/// The registry an operation works through: the one this process holds, or one opened for this call.
///
/// A `Deref` rather than two code paths. Every operation below says `open(args).await?` and then
/// uses it as a `Registry`; whether that registry outlives the call is not a question any of them
/// asks, and `drop`ping this is a no-op when it is held — which is what lets `tx_commit` keep its
/// explicit drop and mean the same thing in both lifecycles.
pub enum Session<'a> {
    Owned(Registry),
    Held(&'a Registry),
}

impl std::ops::Deref for Session<'_> {
    type Target = Registry;

    fn deref(&self) -> &Registry {
        match self {
            Self::Owned(registry) => registry,
            Self::Held(registry) => registry,
        }
    }
}

pub async fn open(args: &Ops) -> Result<Session<'_>> {
    if let Some(held) = &args.held {
        return Ok(Session::Held(&held.registry));
    }
    let storage = Arc::new(SqliteStorage::open(&args.store)?);
    // The poison table comes from beside the store even on the read path, and especially there:
    // the reader is the one §14 owes an explanation to, and it is never the process that discovered
    // the failure (SPEC.md §14).
    Ok(Session::Owned(
        Registry::open_with_poison(
            storage,
            Arc::new(NativeExecutor::new()),
            Arc::new(FilePoison::new(args)),
        )
        .await?,
    ))
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

/// Every object of one struct, by PID. SPEC.md §9.6, §17.5.
///
/// **The enumeration §9.6 spent v1 declining to expose.** The store always held it — a struct's
/// existence buffer *is* the set of its instances (§4.2), and the scheduler has always scanned it to
/// discover entities — and what changed is that an application which cannot ask *"which contacts are
/// there"* cannot be written. The exclusion was about not shipping a query language by accident, and
/// that reason survives: this is one buffer scan, not a query.
///
/// **Read-only, at head, and outside any transaction.** There is no `tx list`, and the omission is
/// the point rather than a gap: a guard is a question the touch index can answer about *a cell*
/// (§12.4), and "the set of Contacts" is not a cell. Guarding an enumeration means guarding the
/// absence of every object that does not exist yet — the absence-guard problem (§12.1) generalised
/// from one cell to a whole buffer — and the only honest implementation would conflict every
/// creation with every enumeration. So a listing buys no protection at commit, exactly as a `borg
/// get` outside a transaction does not, and SDK-DRAFT §5 carries the question rather than this
/// carrying a half-answer.
///
/// **Ids only.** Reading a field of each is one round trip per object, which is the N+1 every ORM
/// has; it is a finding waiting for a query layer and not an argument for widening the reply — see
/// [`borg_protocol::client::Request::List`].
///
/// The struct must be declared **at this caller's ClientVersion**, so a name nobody declared is an
/// error rather than an empty list: an empty list is what a typo would look like, and this is a
/// question about a struct, which is the kind of question `def show` already refuses by name.
pub async fn list(args: &Ops, struct_name: &str) -> Result<Vec<borg_core::Pid>> {
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;
    let path = registry.branches.read_path(branch, None)?;
    let name: ObjectTypeName = struct_name.into();
    let version = client_version(&registry, args, branch).await?;
    if registry
        .defs
        .view_at(&path, version.0)
        .await?
        .object(&name)
        .is_none()
    {
        return Err(BorgError::Storage(format!("no struct named `{name}`")));
    }
    registry.object_ids(branch, &name).await
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

/// The PID counters this store has issued, beside the store.
///
/// **Why a sidecar and not the store.** A layer sequencer resumes from the log because the log
/// already answers *"the highest layer"* in one read — `InProcessSequencer::resuming_after` is
/// exactly that, and it is the pattern this wanted to copy. There is no equivalent one-read answer
/// for a PID: a counter is `(branch, allocator)`-scoped and therefore spans every struct at once, so
/// deriving it from the store means scanning *every* object buffer, and doing that per create turns
/// creating `n` objects into `O(n²)`. That is the one thing a create must not be. So the counter is
/// written down, with the pause flags and the transaction table, for the same reason they are
/// (`crate::sidecar`): operational state that changes nothing about what is true.
///
/// **It is saved before the write it names, never after**, which decides which way a crash goes
/// wrong. Persist first and a process that dies mid-create burns a counter — nothing notices, ids
/// are not required to be dense. Persist afterwards and it would hand the same id out twice, which
/// is the one outcome the whole `(branch, allocator, counter)` scheme exists to prevent.
///
/// What this does *not* survive is somebody deleting the file: ids would restart, and a fresh
/// `Contact` could then land on the address of an old one. Every sidecar loses something —
/// `producers.json` loses where code lives, `derivation.json` loses poisonings — but this is the
/// only one that loses something a store cannot recreate by being told again, and it is recorded in
/// `CLAUDE.md` as such rather than defended against with a scan nobody can afford.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct Allocations {
    /// The next counter [`SERVER_ALLOCATOR`] will issue. Monotonic and never reused.
    pub next: u64,
}

impl Default for Allocations {
    fn default() -> Self {
        // From 1, so that the first id a store issues reads as the first one rather than as the
        // zeroth — these appear in error messages and in scenarios.
        Self { next: 1 }
    }
}

impl Sidecar for Allocations {
    const EXTENSION: &'static str = "allocations.json";
}

pub fn load_allocations(args: &Ops) -> Allocations {
    sidecar::load(&args.store)
}

pub fn save_allocations(args: &Ops, table: &Allocations) -> Result<()> {
    sidecar::save(&args.store, table)
}

/// Take the next PID for an object of any struct on this store. See [`Allocations`].
///
/// The **allocation branch** — the root — and not the branch being written, for the reason
/// [`allocation_branch`] gives: a PID records where an object was allocated, and a transaction
/// branch is a mechanism rather than a place. Recording one would put an ephemeral, reaped-adjacent
/// branch id inside a permanent identity, and buy nothing: uniqueness here comes from the allocator
/// and the counter.
fn allocate(args: &Ops, branch: BranchId) -> Result<borg_core::Pid> {
    let mut table = load_allocations(args);
    let counter = table.next;
    table.next += 1;
    save_allocations(args, &table)?;
    Ok(borg_core::Pid::Allocated {
        kind: borg_core::PidKind::Object,
        branch,
        allocator: SERVER_ALLOCATOR,
        counter,
    })
}

/// Allocate an object and write its existence cell, in one step. SPEC.md §3.1, §8, §17.5.
///
/// One step because the two halves are useless apart. An id nothing wrote is not an object — nothing
/// enumerates it, no producer maps over it, and a client holding it cannot tell it from one it
/// invented — and an existence cell needs an id to be written at. Splitting them would also make the
/// allocation itself an event a client could lose by disconnecting between the two calls.
///
/// **Validated like any other write**, because it *is* one: it goes through the same
/// `WriteSession`, so a struct nobody declared is refused by name (§8.0), and the write is isolated
/// on the transaction's branch until it merges. The id is taken before the write is checked, so a
/// refused creation burns a counter — the same harmless outcome a crash between the two has, and the
/// alternative is a second copy of the declaration check outside the one door that owns it.
///
/// **It reads nothing.** `WriteSession::imply_existence` probes an existence cell before implying
/// one, and that probe is what stops two transactions concluding an object is absent and both
/// creating it — but an *explicit* existence write skips the probe, because there is nothing to
/// conclude: the id is fresh, so nobody else can hold it. The consequence is the one worth asserting
/// (see the tests): two transactions each creating an object never conflict, since they wrote
/// different cells and neither observed the other's.
pub async fn tx_create(args: &Ops, tx: &str, struct_name: &str) -> Result<borg_core::Pid> {
    let mut table = load_transactions(args);
    let index = transaction_index(&table, tx)?;
    let registry = open(args).await?;
    let branch = table.open[index].state.branch;

    let pid = allocate(args, allocation_branch(&registry)?)?;
    let cell = CellRef::existence(struct_name.into(), pid);

    let version = client_version(&registry, args, branch).await?;
    let mut session = registry
        .begin_write(branch, version, Writer::Client)
        .await?;
    if let Err(rejection) = session.set(&cell, Value::Bool(true)).await {
        session.abort().await?;
        return Err(rejection);
    }
    let open = &mut table.open[index];
    absorb(&mut open.state, &session);
    open.touched = now();

    session.commit().await?;
    save_transactions(args, &table)?;
    Ok(pid)
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
    // **Dropped here, and it used to be load-bearing.** `auto_derive` opens a registry *with* an
    // executor, and two live `Registry` instances over one store are what the single-process
    // assumption forbids — so this drop was what made the next line legal, and it is why `borg serve`
    // could not hold a store open (`CLAUDE.md`, `crate::serve`). A held registry carries the executor
    // itself, so the drop is a no-op there and `auto_derive` reuses the same instance. Kept because
    // the CLI still opens one per command and still has to put it down before opening the next.
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
pub async fn open_deriving(args: &Ops) -> Result<(Session<'_>, Workers)> {
    if let Some(held) = &args.held {
        // Already open, already registered, workers already warm. The one call that used to be
        // impossible to make twice against one store is now the same call as any other read.
        return Ok((Session::Held(&held.registry), Workers::Held));
    }
    let storage = Arc::new(SqliteStorage::open(&args.store)?);
    let impls = load_impls(args);
    let (registry, executor) = open_with_workers(args, storage, &impls).await?;
    registry
        .register_producers(branch_of(&registry, args.branch.as_deref())?)
        .await?;
    Ok((Session::Owned(registry), Workers::Owned(executor)))
}

/// Build the executor and the registry behind it, for whoever is going to hold them.
///
/// Shared by [`open_deriving`] and [`hold`] so that a server and a CLI command derive through
/// identically-configured machinery: the pool size, the poison table and the allocation branch a
/// worker resolves shorthands against are all decided in exactly one place.
async fn open_with_workers(
    args: &Ops,
    storage: Arc<SqliteStorage>,
    impls: &Implementations,
) -> Result<(Registry, Arc<borg_exec_process::ProcessExecutor>)> {
    // A worker resolves the `Company#1` shorthand against the allocating branch, which is a fact
    // about the store — so the store has to be readable before the executor can be built. Read from
    // the branch table rather than by opening a whole registry: §17.1 says the branch table is the
    // structure of the log rather than a projection of it, so the one question asked here is answered
    // by one read, and opening a registry to ask it used to replay the entire log a second time on
    // every deriving command.
    let allocation = allocation_branch_of(storage.as_ref()).await?;

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
    Ok((registry, executor))
}

/// The worker pool an operation derived through: this call's, or the server's.
///
/// [`shutdown`](Workers::shutdown) is a no-op on a held pool, which is the whole reason this is a
/// type and not an `Option`. `auto_derive` shuts its workers down because it started them; a server's
/// pool outlives every request, and an operation that could not tell the difference would tear down
/// the server's workers after the first commit.
pub enum Workers {
    Owned(Arc<borg_exec_process::ProcessExecutor>),
    Held,
}

impl Workers {
    pub async fn shutdown(&self) {
        match self {
            Self::Owned(workers) => workers.shutdown().await,
            Self::Held => {}
        }
    }
}

/// The root branch, straight from the branch table.
///
/// The same answer [`allocation_branch`] gives, without a registry to give it: a PID's branch
/// component names where an object was *allocated*, so it always resolves against the root.
async fn allocation_branch_of(storage: &SqliteStorage) -> Result<BranchId> {
    storage
        .read_branches()
        .await?
        .into_iter()
        .filter(|branch| branch.origin.is_none())
        .map(|branch| branch.id)
        .min_by_key(|id| id.0)
        .ok_or_else(|| BorgError::Storage("store has no branches — run `borg init`".into()))
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
    if let Some(held) = &args.held {
        // See `Held::producers_moved`: `repo push` is refused while a store is served, so the table
        // the executor was built from cannot have moved. Said out loud because the failure mode of a
        // wrong assumption here is a producer running stale code and nothing looking wrong.
        if held.producers_moved(args) {
            eprintln!(
                "warning: the producer table changed under a running server — the worker pool was \
                 built at boot and is now stale. Stop the server and start it again."
            );
        }
    }
    let (registry, workers) = open_deriving(args).await?;
    // The branch being caught up, which is not always the one the command named: a commit catches up
    // the transaction's *parent*. Registration is an insert into a map, so doing it for a branch
    // already registered costs a def-view fold and nothing else (§9.2).
    registry.register_producers(branch).await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use borg_core::Pid;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "borg-ops-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A store with one struct declared, driven through the same ops the CLI calls.
    async fn store_with_contacts(dir: &Path) -> Ops {
        let args = Ops {
            store: dir.join("borg.db"),
            held: None,
            branch: None,
            version: None,
            freshness: FreshnessRequirement::Validated,
            settled: false,
        };
        let registry = open(&args).await.unwrap();
        registry
            .branches
            .create_root(Some("main".into()))
            .await
            .unwrap();
        let branch = branch_of(&registry, None).unwrap();
        registry
            .defs
            .push(
                branch,
                vec![borg_core::DefEvent::DeclareField {
                    struct_name: "Contact".into(),
                    field: "name".into(),
                    ty: borg_core::ValueType::String,
                    repo: borg_core::RepoId(1),
                    ownership: borg_core::Ownership::Source,
                }],
            )
            .await
            .unwrap();
        args
    }

    /// One transaction that creates an object and commits, answering the id it was given.
    async fn create_one(args: &Ops) -> Pid {
        let tx = tx_begin(args).await.unwrap();
        let pid = tx_create(args, &tx, "Contact").await.unwrap();
        tx_commit(args, &tx).await.unwrap();
        pid
    }

    fn ids(pids: &[Pid]) -> Vec<String> {
        pids.iter().map(ToString::to_string).collect()
    }

    /// The whole of primitive one: an object that was created is one of the objects.
    #[tokio::test]
    async fn a_created_object_is_listed_and_an_uncommitted_one_is_not() {
        let dir = temp_dir("list");
        let args = store_with_contacts(&dir).await;
        assert!(
            list(&args, "Contact").await.unwrap().is_empty(),
            "a struct nobody has instantiated has no objects"
        );

        let pid = create_one(&args).await;
        assert_eq!(ids(&list(&args, "Contact").await.unwrap()), ids(&[pid]));

        // Isolation, seen from the enumeration: a create that has not merged is on a branch nobody
        // else can read, so listing the parent cannot see it (§7.2, §12).
        let open = tx_begin(&args).await.unwrap();
        tx_create(&args, &open, "Contact").await.unwrap();
        assert_eq!(
            ids(&list(&args, "Contact").await.unwrap()),
            ids(&[pid]),
            "an uncommitted creation is not yet one of the objects"
        );
        tx_abort(&args, &open).unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A deleted object is not one of the objects (§8.1). The scan answers the tombstone like any
    /// other record; deciding what it means is the enumeration's job.
    #[tokio::test]
    async fn a_tombstoned_object_is_skipped() {
        let dir = temp_dir("tombstone");
        let args = store_with_contacts(&dir).await;
        let first = create_one(&args).await;
        let second = create_one(&args).await;

        let tx = tx_begin(&args).await.unwrap();
        tx_set(&args, &tx, &format!("Contact:{first}"), "~")
            .await
            .unwrap();
        tx_commit(&args, &tx).await.unwrap();

        assert_eq!(
            ids(&list(&args, "Contact").await.unwrap()),
            ids(&[second]),
            "the deleted contact is gone from the listing, and the other one is not"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Writing a property implies existence (§8), so an object created the old way — by writing one
    /// of its fields at a hand-written id — is listed beside the ones the server allocated. This is
    /// what makes `list` an enumeration of the *store* rather than of what `tx create` did.
    #[tokio::test]
    async fn a_hand_authored_object_is_listed_beside_a_server_allocated_one() {
        let dir = temp_dir("mixed");
        let args = store_with_contacts(&dir).await;
        let allocated = create_one(&args).await;

        let tx = tx_begin(&args).await.unwrap();
        tx_set(&args, &tx, "Contact#5.name", "Ada").await.unwrap();
        tx_commit(&args, &tx).await.unwrap();

        let listed = list(&args, "Contact").await.unwrap();
        assert_eq!(listed.len(), 2, "both objects, however they were made");
        assert!(ids(&listed).contains(&allocated.to_string()));
        // `Contact#5` is counter 5 under allocator 0 on the root branch — a different allocator
        // from the one the server issues under, which is the point.
        let hand = listed
            .iter()
            .find(|pid| **pid != allocated)
            .expect("the hand-authored contact");
        let (Pid::Allocated { allocator, .. }, Pid::Allocated { counter, .. }) = (hand, hand)
        else {
            panic!("an object's pid is allocated, not content-addressed")
        };
        assert_eq!(*allocator, ALLOCATOR);
        assert_eq!(*counter, 5);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// **Allocator separation, stated as the collision it prevents.** The server's ids are issued
    /// under [`SERVER_ALLOCATOR`], so counter 1 issued here and the `Contact#1` somebody types are
    /// two different objects — which is what makes `tx create` safe to run against a store full of
    /// hand-written fixtures without either side knowing about the other (§3.1).
    #[tokio::test]
    async fn server_allocated_ids_cannot_collide_with_hand_written_ones() {
        let dir = temp_dir("allocator");
        let args = store_with_contacts(&dir).await;
        let pid = create_one(&args).await;

        let Pid::Allocated {
            allocator, counter, ..
        } = pid
        else {
            panic!("an object's pid is allocated")
        };
        assert_eq!(allocator, SERVER_ALLOCATOR);
        assert_ne!(SERVER_ALLOCATOR, ALLOCATOR, "…and never the shorthand's");
        assert_eq!(counter, 1, "the first id a store issues");

        // The same counter under the shorthand's allocator is a different object, and the store
        // holds both at once.
        let tx = tx_begin(&args).await.unwrap();
        tx_set(&args, &tx, &format!("Contact#{counter}.name"), "Ada")
            .await
            .unwrap();
        tx_commit(&args, &tx).await.unwrap();
        assert_eq!(
            list(&args, "Contact").await.unwrap().len(),
            2,
            "same counter, different allocator, two objects"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// **The counter lives in the store's sidecar and nowhere else**, which is what makes it survive
    /// the process that issued it — the CLI is process-per-command, so anything held in memory is
    /// gone by the next `borg tx create`. Seeded here rather than restarted, because reading it from
    /// disk and writing it back *is* the whole of what a restart exercises; `scenarios/280` runs the
    /// real binary across real process boundaries and asserts the same thing end to end.
    #[tokio::test]
    async fn the_counter_is_read_from_the_store_and_written_back() {
        let dir = temp_dir("counter");
        let args = store_with_contacts(&dir).await;
        save_allocations(&args, &Allocations { next: 42 }).unwrap();

        let pid = create_one(&args).await;
        let Pid::Allocated { counter, .. } = pid else {
            panic!("an object's pid is allocated")
        };
        assert_eq!(
            counter, 42,
            "a new process resumes where the last one left off"
        );
        assert_eq!(
            load_allocations(&args).next,
            43,
            "and moves it on before the write it names, so a crash burns an id rather than reusing one"
        );

        let next = create_one(&args).await;
        assert_ne!(pid, next, "no id is ever issued twice");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// **Two transactions creating objects never conflict.** Neither read anything, and the cells
    /// they wrote are distinct by construction — so there is no guard to trip and nothing to
    /// serialise. This is the property the allocator design buys, asserted rather than assumed.
    #[tokio::test]
    async fn two_transactions_creating_objects_both_commit_with_distinct_ids() {
        let dir = temp_dir("concurrent");
        let args = store_with_contacts(&dir).await;

        let a = tx_begin(&args).await.unwrap();
        let b = tx_begin(&args).await.unwrap();
        let first = tx_create(&args, &a, "Contact").await.unwrap();
        let second = tx_create(&args, &b, "Contact").await.unwrap();
        assert_ne!(first, second);

        tx_commit(&args, &a).await.unwrap();
        // The second commit is the one that would fail if creation guarded anything: it forked
        // before the first landed, and the first has since written to the same buffer.
        tx_commit(&args, &b).await.unwrap();

        assert_eq!(list(&args, "Contact").await.unwrap().len(), 2);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Creation is a write, so it is validated like one (§8.0) — and enumeration answers about a
    /// struct, so a name nobody declared is refused rather than answered with an empty list, which
    /// is what a typo would otherwise look like.
    #[tokio::test]
    async fn an_undeclared_struct_is_refused_by_name_on_both_sides() {
        let dir = temp_dir("undeclared");
        let args = store_with_contacts(&dir).await;

        let listed = list(&args, "Wombat").await.unwrap_err().to_string();
        assert!(listed.contains("Wombat"), "{listed}");

        let tx = tx_begin(&args).await.unwrap();
        let created = tx_create(&args, &tx, "Wombat")
            .await
            .unwrap_err()
            .to_string();
        assert!(created.contains("Wombat"), "{created}");
        // The refused creation left nothing behind, so the transaction is still usable.
        tx_create(&args, &tx, "Contact").await.unwrap();
        tx_commit(&args, &tx).await.unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
