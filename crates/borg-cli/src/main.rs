//! `borg` — the command line client.
//!
//! This is the testbed for what a client is like to use. Every command goes through the same engine
//! an SDK eventually will, so if the CLI is awkward the design is awkward.
//!
//! Each invocation opens the store, does one thing, and exits. Layers and branches are durable; the
//! indexes are rebuilt from the log on open (see `Registry`).

use borg_core::{
    BorgError, BranchId, CellAt, CellRef, ClientVersion, DefEvent, Freshness, FreshnessRequirement,
    LayerAuthor, LayerId, LayerKind, MergeMode, ObjectTypeName, Origin, Ownership, ProducerId,
    RepoId, Resolved, Result, Transaction, Value, ValueType, Writer, parse,
};
use borg_engine::Registry;
use borg_exec_native::NativeExecutor;
use borg_storage_sqlite::SqliteStorage;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

const ALLOCATOR: borg_core::AllocatorId = borg_core::AllocatorId(0);

/// `println!`, for a reader that is allowed to stop listening.
///
/// `borg get … | head -1` closes the pipe as soon as it has its line, and Rust turns the resulting
/// `EPIPE` into a **panic** — so a perfectly ordinary shell idiom crashed the process, whenever the
/// writer happened to lose the race. It was found by running the scenario suite under load, where it
/// failed about one run in forty; at rest it never failed at all, which is exactly the shape of bug
/// that gets filed as a flake.
///
/// Exiting quietly is what a Unix tool does when its reader goes away: the output was wanted up to
/// the point it was read, and nothing after that is anybody's business. Errors keep going to stderr,
/// which the pipe did not close.
macro_rules! outln {
    () => { outln!("") };
    ($($arg:tt)*) => {{
        use std::io::Write as _;
        if writeln!(std::io::stdout(), $($arg)*).is_err() {
            std::process::exit(0);
        }
    }};
}

struct Args {
    store: PathBuf,
    branch: Option<String>,
    /// `--client-version`. See [`client_version`].
    version: Option<LayerId>,
    value_only: bool,
    count_only: bool,
    /// `--rebuild`. See [`derive`].
    rebuild: bool,
    /// `--freshness`. What a read is willing to pay for (SPEC.md §10.5).
    freshness: FreshnessRequirement,
    /// `--settled`: read at the settled frontier rather than at the ragged head.
    settled: bool,
    /// `--timeout`, in whole seconds. How long `borg frontier reaches` will wait.
    timeout: u64,
    /// `--tx`. Which open transaction a `borg tx …` command speaks to. See [`transaction_of`].
    tx: Option<String>,
    rest: Vec<String>,
}

fn usage() -> ! {
    eprintln!(
        "\
borg — an event-sourced data backend

  borg init                            create a store
  borg set <cell> <value>              write a source cell
  borg delete <cell>                   tombstone a cell
  borg get <cell> [--value]            read a cell, with provenance
  borg explain <cell>                  show where a value came from

  borg tx begin                        fork the branch; prints a transaction handle
  borg tx get <cell> [--tx <id>]       read through a transaction, recording the read
  borg tx set <cell> <value> [--tx]    write to a transaction
  borg tx delete <cell> [--tx <id>]    tombstone through a transaction
  borg tx commit [--tx <id>]           merge, guarded by everything the transaction read
  borg tx abort [--tx <id>]            drop it
  borg tx list | borg tx timeout [<duration>]

  borg branch list
  borg branch fork <parent> [--at <layer>] [--name <name>]
  borg branch merge <child> [--defs-only]

  borg def push <file.json>            push definition mutations
  borg def show <Struct>               show a struct's definition
  borg def version                     the branch's current def-version

  borg repo push <dir>                 push a repo: defs and pipelines
  borg producer list                   registered producers
  borg derive [--count]                run producers until caught up
  borg derive --rebuild                recompute derived data from source, ignoring the cache
  borg derive pause | resume | status  auto-derivation on this branch

  borg layer list | borg layer head
  borg frontier                        how far each producer has caught up
  borg frontier reaches <layer>        wait until every producer has incorporated it

Cells are written Struct:pid.field, Struct:pid, Element[]:pid or Element[]:pid[n], where a pid
looks like o-1234abcd and names the whole identity. Struct#100 is accepted on input as a
shorthand for counter 100 on the root branch; what borg prints is always the canonical form.

A value is parsed against the type its field is declared to hold, so a String field takes `true`,
`42` and `@jake` as those characters and an Int field refuses `acme`. Strings, binary and bigints
are interned by content, which borg does for you — you write the text and read the text back.
`~` is a tombstone on every field, whatever its type.

Every write is checked against the definitions on the branch: the struct must be declared, the
field must be declared, the value must fit, and a field declared derived may be written only by
the producer that owns it. Declare a schema with `borg def push` or `borg repo push` first.

**Every client write is a transaction.** A transaction forks the branch, writes in isolation and
merges, so it reads one consistent snapshot and never writes to a shared branch. Guards are
automatic: what a transaction read becomes what its commit is contingent on, re-evaluated against
the parent since the fork point, and a commit is rejected whole if any of it moved. Reads that found
nothing count, and so does the existence probe a write performs — two transactions cannot both
conclude an object is absent and both create it.

A bare `borg set X v` is an implicit one-shot transaction: begin, set, commit, in one process. It
reads nothing it did not write, so it is honestly last-write-wins on the cell it writes. A client
wanting compare-and-swap reads the cell first and the guard falls out. A transaction can only guard
what it observed *through* the transaction — `borg get` outside one buys no protection.

Transactions are ephemeral and reaped; branches are durable and explicit. A transaction untouched
for longer than `borg tx timeout` is dropped, swept when the next command opens the store. Idle, not
elapsed, so a long but busy transaction survives. A client that wants to walk away and come back
wanted `borg branch fork`, and nothing reaps that.

Every actor carries a ClientVersion — the def-layer its view was built from (§5.4). borg's is the
branch's current def-version, as printed by `borg def version`: the CLI has no generated code, so
each invocation is authored against the schema as it stands. `--client-version` pins an older one,
which is how you act as a client written before a schema change: it writes the old shape and reads
values back through the migrations that lead to its version.

Derived data lags, and says so. Every write catches its branch up afterwards unless auto-derivation
is paused there, and a read states how far behind what it returns may be. `--freshness current`
computes the value at the call site instead of taking the lag; `--settled` reads the whole branch at
the point everything is caught up to, which is a coherent snapshot slightly in the past rather than
the latest of every field.

A paused branch needs no special vocabulary: its frontier stops advancing, and every read of derived
data already reports how far behind it is. `borg derive` still works while paused — that is what
makes pausing useful in an emergency.

Derived layers are a cache that happens to live in the log, and dropping them loses nothing because
source is separate. `borg derive --rebuild` is that fallback: it forgets what has been derived here
and recomputes it from source. Run on a fork, it recomputes the world as of the fork point without
touching the branch it forked from — which is how you check a watermark rather than trust it.

Options:
  --store <path>            store file (default ./borg.db)
  --branch <name>           branch to operate on (default: the root branch)
  --client-version <layer>  act as a client authored against this def-layer
  --freshness <mode>        any | validated (default) | current
  --settled                 read at the settled frontier, not at the ragged head
  --timeout <seconds>       how long `frontier reaches` waits (default 0)
  --tx <id>                 which open transaction to speak to (or $BORG_TX)
  --value                   print only the value
  --count                   print only a count
  --rebuild                 `derive`: recompute from source instead of catching up"
    );
    std::process::exit(2)
}

fn parse_args() -> Args {
    let mut args = Args {
        store: PathBuf::from("borg.db"),
        branch: None,
        version: None,
        value_only: false,
        count_only: false,
        rebuild: false,
        freshness: FreshnessRequirement::Validated,
        settled: false,
        timeout: 0,
        tx: None,
        rest: Vec::new(),
    };
    let mut raw = std::env::args().skip(1);
    while let Some(arg) = raw.next() {
        match arg.as_str() {
            "--store" => args.store = raw.next().unwrap_or_else(|| usage()).into(),
            "--branch" => args.branch = raw.next(),
            "--client-version" => args.version = raw.next().as_deref().map(layer_id),
            "--freshness" => args.freshness = freshness(raw.next().as_deref()),
            "--settled" => args.settled = true,
            "--timeout" => {
                args.timeout = raw
                    .next()
                    .and_then(|secs| secs.parse().ok())
                    .unwrap_or_else(|| usage());
            }
            "--tx" => args.tx = raw.next(),
            "--value" => args.value_only = true,
            "--count" => args.count_only = true,
            "--rebuild" => args.rebuild = true,
            "-h" | "--help" => usage(),
            _ => args.rest.push(arg),
        }
    }
    args
}

#[tokio::main]
async fn main() {
    let args = parse_args();
    if let Err(err) = run(args).await {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

async fn open(args: &Args) -> Result<Registry> {
    let storage = Arc::new(SqliteStorage::open(&args.store)?);
    Registry::open(storage, Arc::new(NativeExecutor::new())).await
}

/// The branch the `Struct#100` shorthand names ids on.
///
/// A PID's branch component records where an object was *allocated*, not where it lives — the whole
/// point of `(branch, allocator, counter)` is that ids never collide, so a fork can inherit an
/// object without renaming it. The shorthand therefore always resolves against the root, or
/// `Company#1` would mean a different object on every branch and a fork could never read what its
/// parent wrote. A canonical address needs none of this: it carries its own branch.
fn allocation_branch(registry: &Registry) -> Result<BranchId> {
    registry
        .default_branch()
        .ok_or_else(|| BorgError::Storage("store has no branches — run `borg init`".into()))
}

/// What a read is willing to pay for. SPEC.md §10.5.
///
/// `validated` is the default because it is the one that is always honest and never expensive: it
/// walks the read-set and runs no user code. `any` is cheaper and says less; `current` computes and
/// blocks.
fn freshness(mode: Option<&str>) -> FreshnessRequirement {
    match mode {
        Some("any") => FreshnessRequirement::Any,
        Some("validated") => FreshnessRequirement::Validated,
        Some("current") => FreshnessRequirement::Current,
        _ => usage(),
    }
}

/// A layer id as written by `borg layer head` — `L7` — or bare.
fn layer_id(text: &str) -> LayerId {
    LayerId(
        text.trim_start_matches('L')
            .parse::<u64>()
            .unwrap_or_else(|_| usage()),
    )
}

/// The ClientVersion this invocation acts at. SPEC.md §5.4.
///
/// **The branch's current def-version, unless pinned.** Every actor that executes code carries the
/// def-layer its code was authored against; the CLI has no generated code, so each invocation is
/// authored *now*, against the schema as it stands — a client that regenerates itself every time it
/// runs. Nothing is recorded beside the store, because there would be nothing true to record: a
/// remembered version would go stale the moment someone else pushed a def, and one recorded per
/// branch would still be wrong for a branch it was never synced on.
///
/// `--client-version` is how an *older* client is spelled, and it has to exist: §5.4's whole claim
/// is that a v1 client keeps reading and writing after the schema moves to v5, and until generated
/// SDKs arrive (§18) there is otherwise no way to have a v1 client at all.
async fn client_version(
    registry: &Registry,
    args: &Args,
    branch: BranchId,
) -> Result<ClientVersion> {
    if let Some(pinned) = args.version {
        return Ok(ClientVersion(pinned));
    }
    let path = registry.branches.read_path(branch, None)?;
    Ok(ClientVersion(registry.defs.head(&path)))
}

/// Resolve `--branch`, falling back to the root. This selects the *timeline*, which is a separate
/// question from which object a shorthand names.
fn branch_of(registry: &Registry, name: Option<&str>) -> Result<BranchId> {
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

async fn run(args: Args) -> Result<()> {
    let verb = args.rest.first().map(String::as_str).unwrap_or("");
    let rest: Vec<&str> = args.rest.iter().skip(1).map(String::as_str).collect();

    // Reaping sweeps **opportunistically, when a process opens the store** — the same place the
    // indexes are already rebuilt — so there is no daemon, and an idle store sweeps nothing because
    // nothing is growing (SPEC.md §12).
    reap_transactions(&args)?;

    match (verb, rest.as_slice()) {
        ("init", _) => init(&args).await,
        ("set", [cell, value]) => set(&args, cell, value).await,
        ("delete", [cell]) => set(&args, cell, "~").await,
        ("get", [cell]) => get(&args, cell).await,
        ("tx", ["begin"]) => tx_begin(&args).await,
        ("tx", ["get", cell]) => tx_get(&args, cell).await,
        ("tx", ["set", cell, value]) => tx_set(&args, cell, value).await,
        ("tx", ["delete", cell]) => tx_set(&args, cell, "~").await,
        ("tx", ["commit"]) => tx_commit(&args).await,
        ("tx", ["abort"]) => tx_abort(&args).await,
        ("tx", ["list"]) => tx_list(&args),
        ("tx", ["timeout", rest @ ..]) => tx_timeout(&args, rest.first().copied()),
        ("explain", [cell]) => explain(&args, cell).await,
        ("branch", ["list"]) => branch_list(&args).await,
        ("branch", ["fork", parent, tail @ ..]) => branch_fork(&args, parent, tail).await,
        ("branch", ["merge", child, tail @ ..]) => branch_merge(&args, child, tail).await,
        ("def", ["push", file]) => def_push(&args, file).await,
        ("def", ["show", name]) => def_show(&args, name).await,
        ("def", ["version"]) => def_version(&args).await,
        ("layer", ["list"]) => layer_list(&args).await,
        ("layer", ["head"]) => layer_head(&args).await,
        ("repo", ["push", dir]) => repo_push(&args, dir).await,
        ("producer", ["list"]) => producer_list(&args).await,
        ("derive", ["pause"]) => derive_pause(&args, true).await,
        ("derive", ["resume"]) => derive_pause(&args, false).await,
        ("derive", ["status"]) => derive_status(&args).await,
        ("derive", _) => derive(&args).await,
        ("frontier", ["reaches", layer]) => frontier_reaches(&args, layer).await,
        ("frontier", _) => frontier(&args).await,
        _ => usage(),
    }
}

async fn init(args: &Args) -> Result<()> {
    if args.store.exists() {
        return Err(BorgError::Storage(format!(
            "{} already exists",
            args.store.display()
        )));
    }
    let registry = open(args).await?;
    let id = registry.branches.create_root(Some("main".into())).await?;
    outln!("initialised {} (branch main = {id})", args.store.display());
    Ok(())
}

/// Write one source cell — as an **implicit one-shot transaction**. SPEC.md §12.
///
/// begin, set, commit, in one process. That is what keeps the common case one command while making
/// "every client write is a transaction" literally true rather than aspirationally true, and it is
/// what stops there being a second, unguarded write path for anybody to reach for.
///
/// It reads nothing it did not write, so it carries no guard on the cell it writes and is honestly
/// last-write-wins there — what every database gives a blind write, and what §12 says a client that
/// expressed no dependency on prior state has asked for. The one read it does make is the existence
/// probe (§8, implied existence), and that one counts: without it two of these racing to create the
/// same object would both succeed.
///
/// Everything else still happens inside the session: it folds the branch's definitions, parses the
/// text against the field's *declared* type, rejects an undeclared struct or field or a value that
/// does not fit, and interns content on the way in (§3.4, §5.1, §8).
async fn set(args: &Args, cell: &str, value: &str) -> Result<()> {
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;
    let cell = parse::cell_ref(cell, allocation_branch(&registry)?, ALLOCATOR)?;

    let Some(fork_point) = fork_point_of(&registry, branch)? else {
        // Nothing anywhere in this branch's ancestry, so there are no definitions — and §8.0 makes
        // every write contingent on definitions, so this write is going to be rejected whatever
        // path it takes. Going straight to the session gets the caller the rejection they deserve
        // ("no struct named `Wombat`") rather than "nothing to fork from", which is true and
        // useless. Nothing is given up by not isolating: an empty branch has no concurrent state to
        // be isolated from, because anything concurrent would have left a layer.
        return write_directly(&registry, args, branch, &cell, value).await;
    };
    // Anonymous, and it is the only branch in the system that should be: a handle exists so that a
    // *later* process can name a transaction, and this one dies before there is a later process.
    let scratch = registry
        .branches
        .fork(owner_of(&registry, fork_point)?, fork_point, None)
        .await?;

    let version = client_version(&registry, args, scratch).await?;
    let mut session = registry
        .begin_write(scratch, version, Writer::Client)
        .await?;
    if let Err(rejection) = session.set_text(&cell, value).await {
        // A rejected write leaves no trace, so the layer it would have landed in never commits.
        session.abort().await?;
        return Err(rejection);
    }
    let mut transaction = Transaction::new(scratch, branch, fork_point);
    absorb(&mut transaction, &session);
    session.commit().await?;

    let replayed = registry
        .branches
        .merge_transaction(&transaction, MergeMode::DefAndData)
        .await?;
    // The layer on the **parent**, not on the scratch branch: this is what a client awaits with
    // `borg frontier reaches`, and the one on the branch nobody else can see is no use for that.
    outln!("{}", landing(&registry, &transaction, &replayed));
    drop(registry);
    auto_derive(args, branch).await
}

/// The one write path that is not a transaction, and the only branch state it can exist on is none
/// at all. See the caller for why that is safe.
async fn write_directly(
    registry: &Registry,
    args: &Args,
    branch: BranchId,
    cell: &CellRef,
    value: &str,
) -> Result<()> {
    let version = client_version(registry, args, branch).await?;
    let mut session = registry
        .begin_write(branch, version, Writer::Client)
        .await?;
    if let Err(rejection) = session.set_text(cell, value).await {
        session.abort().await?;
        return Err(rejection);
    }
    outln!("{}", session.commit().await?);
    Ok(())
}

async fn get(args: &Args, cell: &str) -> Result<()> {
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
    let resolved = resolved?;
    report(&registry, args, &cell, &resolved).await
}

/// The provenance envelope, as `borg get` and `borg tx get` both print it.
///
/// One renderer, because a read through a transaction is a read: if the two drifted, the CLI would
/// be teaching that a transaction's reads are a different kind of thing from a branch's, which is
/// exactly the belief §12 exists to remove.
async fn report(
    registry: &Registry,
    args: &Args,
    cell: &CellRef,
    resolved: &Resolved<Option<Value>>,
) -> Result<()> {
    // Interned content reads back as content, so a string field prints `acme.ai` rather than the
    // `@s-…` that is physically stored (§3.4). What `borg get --value` prints is what `borg set`
    // accepts, which is the property a shell pipeline actually relies on.
    let rendered = match &resolved.value {
        Some(value) => Some(registry.values.render(value).await?),
        None => None,
    };

    if args.value_only {
        if let Some(value) = rendered {
            outln!("{value}");
        }
        return Ok(());
    }

    outln!("{cell}");
    outln!(
        "  value:       {}",
        rendered.as_deref().unwrap_or("<absent>")
    );
    // Shown only when there is one: it is the proof that equal content is stored once, registry-wide
    // and branch-independently (§3.1), and there is nothing to show for a primitive.
    if let Some(pid) = resolved
        .value
        .as_ref()
        .and_then(|v| registry.values.content_pid(v))
    {
        outln!("  interned:    @{pid}");
    }
    outln!(
        "  origin:      {}",
        match resolved.origin {
            Origin::Source => "source",
            Origin::Derived => "derived",
        }
    );
    outln!("  state:       {}", state_name(resolved.state));
    // Two layers, not one. `authored at` is where the value was first committed — on whichever
    // branch wrote it — and `landed at` is where it arrived on the branch being read. They differ
    // exactly when the value came across a merge, and the old single `written at` reported only the
    // second because merge rewrote the first away (SPEC.md §4.3, §13).
    outln!("  authored at: {}", resolved.authored_at);
    outln!("  landed at:   {}", resolved.landed_at);
    outln!("  fresh as of: {}", resolved.fresh_as_of);
    if let Some(producer) = resolved.by {
        outln!("  produced by: {producer}");
    }
    Ok(())
}

const fn state_name(state: Freshness) -> &'static str {
    match state {
        Freshness::Current => "current",
        Freshness::Unvalidated => "unvalidated",
        Freshness::Stale => "stale",
        Freshness::Broken => "broken",
        Freshness::Tombstoned => "tombstoned",
    }
}

async fn explain(args: &Args, cell: &str) -> Result<()> {
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;
    let cell = parse::cell_ref(cell, allocation_branch(&registry)?, ALLOCATOR)?;

    let version = client_version(&registry, args, branch).await?;
    let Some(lineage) = registry
        .resolver
        .explain(branch, &cell, None, version)
        .await?
    else {
        outln!("{cell}: nothing stored");
        return Ok(());
    };

    outln!("{cell}");
    let landed = if lineage.landed_at == lineage.authored_at {
        String::new()
    } else {
        // Said only when it is news: on the branch that authored a value the two coincide, and
        // repeating the same layer twice would make the merged case harder to spot rather than
        // easier.
        format!(", landed at {}", lineage.landed_at)
    };
    match lineage.produced_by {
        Some(producer) => outln!(
            "  produced by {producer}, authored at {}{landed}",
            lineage.authored_at
        ),
        None => outln!("  source, authored at {}{landed}", lineage.authored_at),
    }
    if !lineage.from.is_empty() {
        outln!("  from");
        for edge in lineage.from {
            outln!(
                "    {}  {}  @{}",
                edge.cell.cell,
                match edge.origin {
                    Origin::Source => "source ",
                    Origin::Derived => "derived",
                },
                edge.landed_at
            );
        }
    }
    Ok(())
}

async fn branch_list(args: &Args) -> Result<()> {
    let registry = open(args).await?;
    let mut branches = registry.branches.all();
    branches.sort_by_key(|b| b.id.0);
    for branch in branches {
        let name = branch.name.unwrap_or_else(|| "<unnamed>".into());
        match branch.origin {
            Some(origin) => outln!("{:<6} {name:<16} forked at {origin}", branch.id.to_string()),
            None => outln!("{:<6} {name:<16} root", branch.id.to_string()),
        }
    }
    Ok(())
}

async fn branch_fork(args: &Args, parent: &str, tail: &[&str]) -> Result<()> {
    let registry = open(args).await?;
    let parent_id = branch_of(&registry, Some(parent))?;
    let at = flag(tail, "--at")
        .and_then(|v| v.trim_start_matches('L').parse::<u64>().ok())
        .map(LayerId)
        .or_else(|| registry.layers.head(parent_id))
        .ok_or_else(|| BorgError::Storage("nothing to fork from — the branch is empty".into()))?;
    let name = flag(tail, "--name").map(str::to_string);

    let id = registry.branches.fork(parent_id, at, name).await?;
    outln!("{id}");
    Ok(())
}

async fn branch_merge(args: &Args, child: &str, tail: &[&str]) -> Result<()> {
    let registry = open(args).await?;
    let child_id = branch_of(&registry, Some(child))?;
    let mode = if tail.contains(&"--defs-only") {
        MergeMode::DefOnly
    } else {
        MergeMode::DefAndData
    };
    let replayed = registry.branches.merge(child_id, mode).await?;
    outln!("{} layer(s) replayed", replayed.len());

    // The branch that gained layers is the one that owes derivation, and it is not the one the
    // command names — a merge replays a child onto its parent, so the parent is read back off the
    // child's fork point.
    let parent = registry
        .branches
        .all()
        .into_iter()
        .find(|b| b.id == child_id)
        .and_then(|child| child.origin)
        .and_then(|origin| registry.layers.layer(origin))
        .map(|layer| layer.branch);
    drop(registry);
    match parent {
        Some(parent) => auto_derive(args, parent).await,
        None => Ok(()),
    }
}

// --- Transactions. SPEC.md §12, §13. ---------------------------------------------------------------

/// How long a transaction may sit untouched before it is reaped, when nobody has said otherwise.
///
/// Generous on purpose: the cost of reaping too eagerly is a client losing work it was in the middle
/// of, and the cost of reaping too late is a branch row and a read-set sitting in a file. Those are
/// not the same size of mistake.
const DEFAULT_TX_IDLE_TIMEOUT: u64 = 24 * 60 * 60;

/// How many ended transactions are remembered, so that touching one can say what became of it rather
/// than "unknown transaction". Bounded because this is a courtesy, not a log.
const REMEMBERED_TRANSACTIONS: usize = 64;

/// The open transactions, beside the store.
///
/// A transaction spans several CLI processes, so it needs somewhere to keep two things: which branch
/// it forked, and what it has read so far. That goes here, with the pause flags and the
/// producer-implementation table (§9.2), for the same reason they do — it is **operational state,
/// not log data**, and it dies when the transaction ends. In the log it would be forkable,
/// mergeable and time-travellable, and "which transactions were open at layer 400" is not a question
/// anybody has.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(default)]
struct Transactions {
    /// Seconds. Rendered and accepted as a duration by `borg tx timeout`.
    tx_idle_timeout: u64,
    /// The next handle to hand out. Monotonic and never reused, so a stale `--tx` names a
    /// transaction that existed rather than one that is about to.
    next: u64,
    open: Vec<Open>,
    /// What became of the transactions that ended.
    spent: Vec<Spent>,
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
struct Open {
    id: String,
    /// Unix seconds, when this transaction was last used. **Idle, not elapsed**: a legitimate
    /// transaction may run for hours, and what predicts an abandoned one is silence.
    touched: u64,
    state: Transaction,
}

/// A transaction that ended, and how.
///
/// The `fate` is a phrase rather than a code because it is going straight into a sentence, and the
/// sentence is the point: *"expired after 2 minutes idle"* tells a client what to do next, and
/// *"unknown transaction"* tells it to go and look for a bug it does not have.
#[derive(serde::Serialize, serde::Deserialize)]
struct Spent {
    id: String,
    fate: String,
}

fn transactions_path(args: &Args) -> PathBuf {
    args.store.with_extension("transactions.json")
}

fn load_transactions(args: &Args) -> Transactions {
    std::fs::read_to_string(transactions_path(args))
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_default()
}

fn save_transactions(args: &Args, table: &Transactions) -> Result<()> {
    let raw =
        serde_json::to_string_pretty(table).map_err(|err| BorgError::Storage(err.to_string()))?;
    std::fs::write(transactions_path(args), raw).map_err(|err| BorgError::Storage(err.to_string()))
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

/// `90s`, `10m`, `24h`, `7d`, or a bare count of seconds.
fn duration(text: &str) -> Option<u64> {
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
fn render_duration(seconds: u64) -> String {
    for (scale, suffix) in [(86400, 'd'), (3600, 'h'), (60, 'm')] {
        if seconds >= scale && seconds.is_multiple_of(scale) {
            return format!("{}{suffix}", seconds / scale);
        }
    }
    format!("{seconds}s")
}

/// The same duration spelled for a sentence rather than for a config file.
fn spell_duration(seconds: u64) -> String {
    let (count, unit) = [(86400, "day"), (3600, "hour"), (60, "minute")]
        .into_iter()
        .find(|(scale, _)| seconds >= *scale && seconds.is_multiple_of(*scale))
        .map_or((seconds, "second"), |(scale, unit)| (seconds / scale, unit));
    format!("{count} {unit}{}", if count == 1 { "" } else { "s" })
}

/// Drop transactions that have been idle too long. SPEC.md §12.
///
/// **No daemon.** This runs when a process opens the store, which is the same place the indexes are
/// already rebuilt from the log — so an idle store sweeps nothing, because nothing is growing, and a
/// busy one sweeps constantly for free.
///
/// A reaped transaction's *state* is dropped, which is what makes it unusable: nothing can be
/// written through it and its layers can never merge. Its branch row is left where it is, because
/// whether spent branches are reaped or kept as history is a real choice and is deliberately not
/// being made by a janitor as a side effect (SPEC-DRAFT §7.5).
fn reap_transactions(args: &Args) -> Result<()> {
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

fn retire(table: &mut Transactions, id: String, fate: String) {
    table.spent.push(Spent { id, fate });
    let excess = table.spent.len().saturating_sub(REMEMBERED_TRANSACTIONS);
    table.spent.drain(..excess);
}

/// Which open transaction this command speaks to.
///
/// `--tx`, then `$BORG_TX`, then — when exactly one is open — that one. The environment variable is
/// what makes the surface in §12 read as written (`borg tx get <cell>`, no handle in sight) from a
/// shell that exported it once; the flag is what lets one shell hold two transactions open at the
/// same time, which is the whole reason the handle is explicit and is an interleaving a
/// single-process API cannot express.
fn transaction_of(args: &Args, table: &Transactions) -> Result<usize> {
    let named = args
        .tx
        .clone()
        .or_else(|| std::env::var("BORG_TX").ok().filter(|id| !id.is_empty()));
    let Some(id) = named else {
        return match table.open.len() {
            0 => Err(BorgError::Storage(
                "no transaction is open — start one with `borg tx begin`".into(),
            )),
            1 => Ok(0),
            _ => Err(BorgError::Storage(format!(
                "several transactions are open ({}) — name one with --tx",
                table
                    .open
                    .iter()
                    .map(|open| open.id.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))),
        };
    };
    if let Some(index) = table.open.iter().position(|open| open.id == id) {
        return Ok(index);
    }
    // **Never "unknown transaction" for one that existed.** The first tells you what happened and
    // what to do; the second sends you looking for a bug in your own bookkeeping.
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
fn fork_point_of(registry: &Registry, branch: BranchId) -> Result<Option<LayerId>> {
    let ceiling = registry.branches.read_path(branch, None)?.ceiling();
    Ok((ceiling.0 != 0).then_some(ceiling))
}

fn owner_of(registry: &Registry, layer: LayerId) -> Result<BranchId> {
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
fn absorb(transaction: &mut Transaction, session: &borg_engine::WriteSession) {
    for read in session.observed() {
        transaction.observe(read.clone());
    }
    for write in session.authored() {
        transaction.wrote(write.clone());
    }
}

/// The layer a transaction landed in on its parent — what a client awaits with
/// `borg frontier reaches`. A transaction that wrote nothing landed nowhere, and says head.
fn landing(registry: &Registry, transaction: &Transaction, replayed: &[LayerId]) -> LayerId {
    replayed.last().copied().unwrap_or_else(|| {
        registry
            .layers
            .head(transaction.parent)
            .unwrap_or(LayerId(0))
    })
}

async fn tx_begin(args: &Args) -> Result<()> {
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
    outln!("{id}");
    Ok(())
}

/// Read through a transaction, and **record the read**.
///
/// The recording is the whole difference from `borg get`, and it is what makes the guard at commit
/// automatic. Reads that find nothing are recorded too: absence is a legitimate thing to have acted
/// on, and a later write to that cell must invalidate the decision (§9.4).
async fn tx_get(args: &Args, cell: &str) -> Result<()> {
    let mut table = load_transactions(args);
    let index = transaction_of(args, &table)?;
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

    report(&registry, args, &cell, &resolved).await
}

async fn tx_set(args: &Args, cell: &str, value: &str) -> Result<()> {
    let mut table = load_transactions(args);
    let index = transaction_of(args, &table)?;
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
    outln!("{layer}");
    Ok(())
}

/// Merge, guarded by everything the transaction read. SPEC.md §12, §13.
async fn tx_commit(args: &Args) -> Result<()> {
    let mut table = load_transactions(args);
    let index = transaction_of(args, &table)?;
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
            // other half of that is `borg tx abort`, and the timeout is what collects the ones
            // nobody comes back to.
            table.open[index].touched = now();
            save_transactions(args, &table)?;
            return Err(rejection);
        }
    };

    let spent = table.open.remove(index);
    retire(&mut table, spent.id, "already committed".into());
    save_transactions(args, &table)?;
    set_paused(args, state.branch, false)?;

    outln!("{}", landing(&registry, &state, &replayed));
    drop(registry);
    auto_derive(args, state.parent).await
}

/// Drop a transaction. Nothing it wrote is on the parent, because nothing it wrote ever left its own
/// branch — which is what makes an abort free.
async fn tx_abort(args: &Args) -> Result<()> {
    let mut table = load_transactions(args);
    let index = transaction_of(args, &table)?;
    let spent = table.open.remove(index);
    let id = spent.id.clone();
    set_paused(args, spent.state.branch, false)?;
    retire(&mut table, spent.id, "was aborted".into());
    save_transactions(args, &table)?;
    outln!("{id} aborted");
    Ok(())
}

fn tx_list(args: &Args) -> Result<()> {
    let table = load_transactions(args);
    if table.open.is_empty() {
        outln!("no open transactions");
        return Ok(());
    }
    let now = now();
    for open in &table.open {
        let (reads, writes) = open.state.size();
        outln!(
            "{:<8} {:<6} forked at {:<6} {reads} read, {writes} written, idle {}",
            open.id,
            open.state.branch.to_string(),
            open.state.fork_point.to_string(),
            render_duration(now.saturating_sub(open.touched))
        );
    }
    Ok(())
}

/// Read or set the idle timeout. SPEC.md §12.
fn tx_timeout(args: &Args, spec: Option<&str>) -> Result<()> {
    let mut table = load_transactions(args);
    if let Some(spec) = spec {
        table.tx_idle_timeout = duration(spec).ok_or_else(|| {
            BorgError::Storage(format!(
                "`{spec}` is not a duration — try 90s, 10m, 24h or 7d"
            ))
        })?;
        save_transactions(args, &table)?;
    }
    outln!(
        "tx_idle_timeout = {}",
        render_duration(table.tx_idle_timeout)
    );
    Ok(())
}

/// The JSON a `borg def push` file holds.
///
/// The repo is stated once at the top rather than repeated on every event, because a push comes from
/// exactly one repo.
#[derive(serde::Deserialize)]
struct DefFile {
    repo: u32,
    events: Vec<DefEventSpec>,
}

/// Matches the variant names of `DefEvent` so the file format reads like the log.
#[allow(clippy::enum_variant_names)]
#[derive(serde::Deserialize)]
enum DefEventSpec {
    DeclareField {
        struct_name: String,
        field: String,
        ty: String,
        /// The producer that owns this field, if it is derived. Absent means source data, written
        /// by clients (SPEC.md §8). A numeric id here because this file *is* the log's own form; a
        /// repo names its producers by name and `borg repo push` resolves them (§9.2).
        #[serde(default)]
        derived_by: Option<u64>,
    },
    MutateField {
        struct_name: String,
        field: String,
        ty: String,
        up: u64,
        #[serde(default)]
        down: Option<u64>,
    },
    DeleteField {
        struct_name: String,
        field: String,
    },
}

/// A field is source data unless a producer is named as its writer (SPEC.md §8).
const fn ownership(producer: Option<ProducerId>) -> Ownership {
    match producer {
        Some(producer) => Ownership::Derived(producer),
        None => Ownership::Source,
    }
}

fn value_type(name: &str) -> ValueType {
    match name {
        "Int" => ValueType::Int,
        "Bool" => ValueType::Bool,
        "Double" => ValueType::Double,
        "String" => ValueType::String,
        "Binary" => ValueType::Binary,
        "BigInt" => ValueType::BigInt,
        "Any" => ValueType::Any,
        other => ValueType::Object(other.into()),
    }
}

async fn def_push(args: &Args, file: &str) -> Result<()> {
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;

    let raw = std::fs::read_to_string(file)
        .map_err(|err| BorgError::Storage(format!("{file}: {err}")))?;
    let spec: DefFile =
        serde_json::from_str(&raw).map_err(|err| BorgError::Storage(format!("{file}: {err}")))?;
    let repo = RepoId(spec.repo);

    let events = spec
        .events
        .into_iter()
        .map(|event| match event {
            DefEventSpec::DeclareField {
                struct_name,
                field,
                ty,
                derived_by,
            } => DefEvent::DeclareField {
                struct_name: struct_name.into(),
                field: field.into(),
                ty: value_type(&ty),
                repo,
                ownership: ownership(derived_by.map(ProducerId)),
            },
            DefEventSpec::MutateField {
                struct_name,
                field,
                ty,
                up,
                down,
            } => DefEvent::MutateField {
                struct_name: struct_name.into(),
                field: field.into(),
                ty: value_type(&ty),
                repo,
                up: ProducerId(up),
                down: down.map(ProducerId),
            },
            DefEventSpec::DeleteField { struct_name, field } => DefEvent::DeleteField {
                struct_name: struct_name.into(),
                field: field.into(),
                repo,
            },
        })
        .collect();

    let layer = registry.defs.push(branch, events).await?;
    outln!("{layer}");
    drop(registry);
    // A def push commits no data, and still creates work: a producer newer than the data it maps
    // over owes its whole source buffer (§9.6), and a `MutateField` appoints migrations that owe
    // every existing value.
    auto_derive(args, branch).await
}

async fn def_show(args: &Args, name: &str) -> Result<()> {
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;
    let path = registry.branches.read_path(branch, None)?;
    let view = registry.defs.view(&path).await?;

    let name: ObjectTypeName = name.into();
    let Some(def) = view.object(&name) else {
        return Err(BorgError::Storage(format!("no struct named `{name}`")));
    };
    outln!("{name}");
    for (field, def) in &def.fields {
        // Ownership is shown because it is now enforced: this line is the answer to "why was my
        // write rejected" (§8).
        outln!(
            "  {field:<16} {:<10} {:<24} repo {:<4} v{}",
            def.ty.to_string(),
            def.ownership
                .producer()
                .map_or_else(|| "source".to_string(), |p| format!("derived by {p}")),
            def.declaring_repo.0,
            def.version
        );
    }
    Ok(())
}

/// The def-version in force on this branch — the ClientVersion a client generated right now would
/// carry (SPEC.md §5.3, §5.4). A def-version *is* a layer id; there is no separate scheme.
async fn def_version(args: &Args) -> Result<()> {
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;
    let path = registry.branches.read_path(branch, None)?;
    outln!("{}", registry.defs.head(&path));
    Ok(())
}

async fn layer_list(args: &Args) -> Result<()> {
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;
    let mut layers = registry.layers.layers_of(branch);
    layers.sort_by_key(|l| l.id.0);
    for layer in layers {
        let author = match layer.author {
            LayerAuthor::Source => "source".to_string(),
            LayerAuthor::Derived { producer, reflects } => {
                format!("derived by {producer}, reflects {reflects}")
            }
        };
        outln!(
            "{:<6} {:<6} {author}",
            layer.id.to_string(),
            match layer.kind {
                LayerKind::Value => "value",
                LayerKind::Def => "def",
            }
        );
    }
    Ok(())
}

async fn layer_head(args: &Args) -> Result<()> {
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;
    outln!("{}", registry.layers.head(branch).unwrap_or(LayerId(0)));
    Ok(())
}

/// Wait until every producer on the branch has incorporated a layer. SPEC.md §10.5.
///
/// This is read-after-write consistency for the clients that want it: note the layer your write
/// landed in, wait for it, then read. `FrontierTracker::reaches` is the primitive and it awaits an
/// in-process signal — but the CLI is process-per-command, so the frontier this process holds only
/// moves if this process derives. Whoever is catching the branch up is someone else's process, and
/// the store is where the two meet, which is why the wait is a sequence of awaits over freshly
/// opened registries rather than one long one. When derivation runs in-process, the loop is what
/// goes away; the await inside it is already the right shape.
///
/// `--timeout 0`, the default, therefore means "answer now", and exit status is the answer.
async fn frontier_reaches(args: &Args, layer: &str) -> Result<()> {
    const POLL: Duration = Duration::from_millis(100);
    let target = layer_id(layer);
    let deadline = Instant::now() + Duration::from_secs(args.timeout);

    loop {
        let registry = open(args).await?;
        let branch = branch_of(&registry, args.branch.as_deref())?;
        let producers = registry.producers_of(branch).await?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        let slice = remaining.min(POLL);

        if tokio::time::timeout(slice, registry.frontier.reaches(branch, &producers, target))
            .await
            .is_ok()
        {
            outln!("{target} reached");
            return Ok(());
        }
        if remaining <= POLL {
            // Non-zero exit, so `borg frontier reaches L7 && report` does the obvious thing.
            return Err(BorgError::Storage(format!(
                "{target} not reached: the branch is settled through {}",
                registry.settled(branch).await?
            )));
        }
        drop(registry);
    }
}

async fn frontier(args: &Args) -> Result<()> {
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;
    let path = registry.branches.read_path(branch, None)?;
    let view = registry.defs.view(&path).await?;

    // The log knows producer ids; only the implementation table knows what a human called them.
    // Joining the two here is the CLI's job precisely because the log must not hold either.
    let names = load_impls(args);
    let head = registry.layers.head(branch).unwrap_or(LayerId(0));
    let mut any = false;
    for producer in view.producers() {
        any = true;
        let name = names
            .producers
            .iter()
            .find(|impl_| impl_.id == producer.id.0)
            .map_or_else(|| producer.id.to_string(), |impl_| impl_.name.clone());
        outln!(
            "{name:<16} caught up through {} (head {head})",
            registry.frontier.watermark(branch, producer.id)
        );
    }
    if !any {
        outln!("no producers registered");
    }
    Ok(())
}

fn flag<'a>(tail: &[&'a str], name: &str) -> Option<&'a str> {
    tail.iter()
        .position(|arg| *arg == name)
        .and_then(|i| tail.get(i + 1))
        .copied()
}

// --- Repos, producers and derivation ---

/// Where a producer's implementation lives.
///
/// Deliberately *not* in the log. §9.2 separates a producer's definition from its implementation:
/// the log records that producer P exists at some ClientVersion, and the `ExecutionProvider`
/// resolves that id to code. Writing a local file path into the log would tie the data model to one
/// machine's filesystem. This sidecar is the CLI's own resolution table, and a container-backed
/// runtime would keep an image reference in exactly the same place.
#[derive(Default, serde::Serialize, serde::Deserialize)]
struct Implementations {
    producers: Vec<Implementation>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Implementation {
    id: u64,
    name: String,
    source: String,
    command: PathBuf,
}

fn impls_path(args: &Args) -> PathBuf {
    args.store.with_extension("producers.json")
}

fn load_impls(args: &Args) -> Implementations {
    std::fs::read_to_string(impls_path(args))
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_default()
}

fn save_impls(args: &Args, impls: &Implementations) -> Result<()> {
    let raw =
        serde_json::to_string_pretty(impls).map_err(|err| BorgError::Storage(err.to_string()))?;
    std::fs::write(impls_path(args), raw).map_err(|err| BorgError::Storage(err.to_string()))
}

/// Push a repo: ask each script to describe itself, record its definitions in the log, and remember
/// where the code lives.
///
/// **Definitions and producers land in one def layer.** A producer and the field it writes must
/// arrive together or not at all: after §8, a producer cannot write anything unless its output field
/// is declared, and half a push would leave a pipeline that is registered and legally mute.
async fn repo_push(args: &Args, dir: &str) -> Result<()> {
    let dir = PathBuf::from(dir);
    let repo = read_repo_id(&dir)?;
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;

    let mut scripts: Vec<PathBuf> = std::fs::read_dir(dir.join("pipelines"))
        .map_err(|err| BorgError::Storage(format!("{}/pipelines: {err}", dir.display())))?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| path.is_file())
        .collect();
    scripts.sort();

    // Everything the repo describes, gathered before anything is emitted: a `derived_by` may name a
    // producer implemented by a different script in the same repo, so ownership can only be resolved
    // once the whole repo has spoken.
    let mut described = Vec::new();
    for command in scripts {
        // The script is the source of truth for what it implements, so a producer definition cannot
        // exist without the code that satisfies it.
        described.push((command.clone(), borg_exec_process::describe(&command)?));
    }

    let mut impls = load_impls(args);
    let mut events = Vec::new();
    for (command, description) in &described {
        for spec in &description.producers {
            let id = ProducerId(spec.id());
            events.push(DefEvent::PushProducer(borg_core::ProducerDef {
                id,
                kind: borg_core::ProducerKind::Pipeline,
                source: borg_core::BufferId::Object(spec.source.as_str().into()),
                version: LayerId(0),
                declaring_repo: repo,
            }));
            remember(&mut impls, id, &spec.name, &spec.source, command);
            outln!("{} -> {id}", spec.name);
        }
    }

    let known: Vec<&str> = described
        .iter()
        .flat_map(|(_, d)| {
            let pipelines = d.producers.iter().map(|p| p.name.as_str());
            pipelines.chain(d.migrations.iter().map(|m| m.name.as_str()))
        })
        .collect();
    let resolve = |owner: &str, what: &str| -> Result<ProducerId> {
        if known.contains(&owner) {
            return Ok(ProducerId(borg_protocol::producer_id(owner)));
        }
        Err(BorgError::Storage(format!(
            "{what} names `{owner}`, which this repo does not implement (it implements: {})",
            known.join(", ")
        )))
    };

    // The definitions this push is a *diff against*. A repo emits its whole schema every time
    // (§5.2), so what it means by a field depends on what is already declared: nothing yet is a
    // declaration, a different type is a mutation, the same type is a repeat.
    let path = registry.branches.read_path(branch, None)?;
    let view = registry.defs.view(&path).await?;

    for (_, description) in &described {
        for spec in &description.structs {
            let struct_name: ObjectTypeName = spec.name.as_str().into();
            for field in &spec.fields {
                let ty = value_type(&field.ty);
                let what = format!("{}.{}", spec.name, field.name);

                // A migration's definition names the field buffer it maps over (§9.3) and its
                // direction; which two versions it bridges is folded from the `MutateField` below,
                // on whichever branch that event ends up on.
                let source = borg_core::BufferId::ObjectProp(
                    struct_name.clone(),
                    field.name.as_str().into(),
                );
                let mut migration =
                    |name: &Option<String>, direction| -> Result<Option<ProducerId>> {
                        let Some(name) = name else { return Ok(None) };
                        let id = resolve(name, &what)?;
                        events.push(DefEvent::PushProducer(borg_core::ProducerDef {
                            id,
                            kind: borg_core::ProducerKind::Migration { direction },
                            source: source.clone(),
                            version: LayerId(0),
                            declaring_repo: repo,
                        }));
                        let command = described
                            .iter()
                            .find(|(_, d)| d.migrations.iter().any(|m| m.name == *name))
                            .map(|(command, _)| command.clone())
                            .expect("resolve() accepted the name, so some script described it");
                        remember(&mut impls, id, name, &spec.name, &command);
                        Ok(Some(id))
                    };
                let up = migration(&field.up, borg_core::MigrationDirection::Up)?;
                let down = migration(&field.down, borg_core::MigrationDirection::Down)?;

                let name: borg_core::FieldName = field.name.as_str().into();
                let declared = view
                    .object(&struct_name)
                    .and_then(|object| object.fields.get(&name));
                match declared {
                    // The type moved. §6.1 says that needs migrations, and the field is where they
                    // are named — a repo cannot say "mutate from String" because it does not know
                    // what it is mutating from, and on another branch the answer differs.
                    Some(existing) if existing.ty != ty => {
                        let Some(up) = up else {
                            return Err(BorgError::Storage(format!(
                                "{what} changes from {} to {ty}, which needs an `up` migration to \
                                 carry the existing values forward",
                                existing.ty
                            )));
                        };
                        events.push(DefEvent::MutateField {
                            struct_name: struct_name.clone(),
                            field: field.name.as_str().into(),
                            ty,
                            repo,
                            up,
                            down,
                        });
                        outln!("{what} {} -> {}", existing.ty, field.ty);
                    }
                    _ => {
                        let owner = match &field.derived_by {
                            // A field owned by a producer this repo does not implement would be a
                            // field nothing can ever write. Caught here rather than at the first
                            // write attempt.
                            Some(name) => Some(resolve(name, &what)?),
                            None => None,
                        };
                        events.push(DefEvent::DeclareField {
                            struct_name: struct_name.clone(),
                            field: field.name.as_str().into(),
                            ty,
                            repo,
                            ownership: ownership(owner),
                        });
                        outln!("{what} {}", field.ty);
                    }
                }
            }
        }
    }

    // A migration nothing names bridges nothing. It would be registered, implemented and never
    // reachable, which is worth a push-time error rather than a puzzle later.
    for (_, description) in &described {
        for spec in &description.migrations {
            if !impls.producers.iter().any(|p| p.name == spec.name) {
                return Err(BorgError::Storage(format!(
                    "`{}` is implemented but no field names it as its `up` or `down`",
                    spec.name
                )));
            }
        }
    }

    if !events.is_empty() {
        registry.defs.push(branch, events).await?;
    }
    drop(registry);
    save_impls(args, &impls)?;
    auto_derive(args, branch).await
}

/// Record where a producer's code lives. Producer ids are stable across pushes, so this replaces
/// rather than accumulates.
fn remember(impls: &mut Implementations, id: ProducerId, name: &str, source: &str, command: &Path) {
    impls.producers.retain(|p| p.id != id.0);
    impls.producers.push(Implementation {
        id: id.0,
        name: name.to_string(),
        // The struct a worker is invoked over. A migration maps over one of its *fields* (§9.3), but
        // what it is handed is still the entity — `Company:o-1234abcd`, to which it appends the
        // field name — so the struct is what this needs to render.
        source: source.to_string(),
        command: command
            .canonicalize()
            .unwrap_or_else(|_| command.to_path_buf()),
    });
}

/// Read the repo id out of `borg.toml`.
fn read_repo_id(dir: &Path) -> Result<RepoId> {
    let manifest = dir.join("borg.toml");
    let raw = std::fs::read_to_string(&manifest)
        .map_err(|err| BorgError::Storage(format!("{}: {err}", manifest.display())))?;
    for line in raw.lines() {
        if let Some((key, value)) = line.split_once('=')
            && key.trim() == "id"
            && let Ok(id) = value.trim().parse::<u32>()
        {
            return Ok(RepoId(id));
        }
    }
    Err(BorgError::Storage(format!(
        "{}: no `id` under [repo]",
        manifest.display()
    )))
}

async fn producer_list(args: &Args) -> Result<()> {
    for producer in load_impls(args).producers {
        outln!(
            "{:<20} {:<10} maps {:<12} {}",
            producer.name,
            format!("P{}", producer.id),
            producer.source,
            producer.command.display()
        );
    }
    Ok(())
}

/// Open the store with an executor that can run this store's producers, and make the engine aware
/// of the ones defined on the branch.
///
/// Three paths need exactly this and need it identically: `borg derive`, the auto-derivation that
/// follows a write, and a `--freshness current` read. Producer *definitions* live in the log and
/// producer *implementations* in the sidecar beside it (§9.2), so being able to run anything at all
/// means joining the two — and doing that in three places is how they drift.
async fn open_deriving(args: &Args) -> Result<(Registry, Arc<borg_exec_process::ProcessExecutor>)> {
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
            .map(|p| (p.id, p.command.clone(), p.source.clone())),
    )
    // The pool matches the scheduler, because a pool smaller than the degree of parallelism would
    // put the queue back that the pool exists to remove.
    .with_pool_size(parallelism);
    let executor = Arc::new(executor);
    let registry = Registry::open(storage, Arc::clone(&executor) as Arc<_>).await?;
    registry.engine.set_parallelism(parallelism);

    let branch = branch_of(&registry, args.branch.as_deref())?;
    let path = registry.branches.read_path(branch, None)?;
    for def in registry.defs.view(&path).await?.producers() {
        registry.engine.register(def.clone());
    }
    Ok((registry, executor))
}

/// How many invocations a round runs at once, and how many worker processes back them.
///
/// An environment variable rather than a flag or a sidecar file. It is neither log data nor a fact
/// about the store — the same store derived on a laptop and on a build box wants different numbers —
/// so there is nothing to record, and every command that derives would otherwise need the same flag.
/// Unset means one per core, which is what the engine picks for itself.
fn derive_parallelism() -> usize {
    std::env::var("BORG_DERIVE_PARALLELISM")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get)
        })
}

/// Run every producer forward to head.
///
/// **Works on a paused branch.** Pausing means "do not auto-derive", not "refuse to derive" — which
/// is the whole point of having the switch: freeze the automation, then step it by hand.
///
/// `--rebuild` swaps catching up for recomputing: this branch forgets what it has derived and
/// derives it again from source (§6.3). On a fork that is a replay of the world as of the fork
/// point, which is the operation a watermark's meaning is defined in terms of (§10.1) and the one
/// `scenarios/100-watermark-truth` uses to check that meaning holds.
async fn derive(args: &Args) -> Result<()> {
    let (registry, workers) = open_deriving(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;
    let executed = if args.rebuild {
        registry.engine.recompute(branch).await?
    } else {
        registry.engine.catch_up(branch).await?
    };
    workers.shutdown().await;

    if args.count_only {
        outln!("{executed}");
    } else {
        outln!("{executed} invocation(s)");
    }
    Ok(())
}

/// Catch a branch up after a commit. SPEC.md §9.6.
///
/// **This is what "materialization is continuous" means here.** The CLI is process-per-command, so
/// there is no daemon to run a loop in; the process that commits a layer is the one in a position to
/// chase it, and it does so before exiting. A worker pool with its own scheduler is a strictly better
/// shape and needs a server to live in — which is the point at which this call becomes a signal
/// rather than a call, with nothing above it changing, because §9.6 already says scheduling policy
/// cannot affect correctness.
///
/// The pause check lives *here* and not in `catch_up`. The engine's job is the mechanism, and a
/// mechanism that consults an operational switch is one `borg derive` would have to reach around —
/// which is the shape that eventually gets it wrong.
async fn auto_derive(args: &Args, branch: BranchId) -> Result<()> {
    if paused_branches(args).contains(&branch.0) {
        return Ok(());
    }
    // No implementations means nothing here can run, and building an executor to discover that would
    // charge every store without producers for the ones that have them.
    if load_impls(args).producers.is_empty() {
        return Ok(());
    }
    let (registry, workers) = open_deriving(args).await?;
    let caught_up = registry.engine.catch_up(branch).await;
    workers.shutdown().await;

    if let Err(err) = caught_up {
        // Reported, not raised. The write this followed is ground truth whether or not anything has
        // chased it yet, and §9.6's licence is exactly that scheduling cannot affect correctness — a
        // write that failed because somebody else's pipeline is unrunnable would be a write failing
        // for a reason that has nothing to do with it.
        eprintln!("warning: auto-derivation did not complete: {err}");
    }
    Ok(())
}

/// Which branches auto-derivation is paused on.
///
/// Beside the store, like the producer-implementation table and for the same reason: this is
/// **operational config, not log data**. Pausing does not change what is true, only when the system
/// catches up. In the log it would be forkable, mergeable and time-travellable, and "was derivation
/// paused at layer 400?" is not a question anybody has.
///
/// Branch *ids*, not names: a branch may be renamed or unnamed, and the id is what the log uses.
#[derive(Default, serde::Serialize, serde::Deserialize)]
struct DerivationConfig {
    paused: Vec<u64>,
}

fn derivation_path(args: &Args) -> PathBuf {
    args.store.with_extension("derivation.json")
}

fn paused_branches(args: &Args) -> Vec<u64> {
    std::fs::read_to_string(derivation_path(args))
        .ok()
        .and_then(|raw| serde_json::from_str::<DerivationConfig>(&raw).ok())
        .unwrap_or_default()
        .paused
}

/// Turn auto-derivation on or off for one branch.
///
/// Factored out because transactions use it too: a transaction branch is **created paused** (§12),
/// and a transaction that ends resumes what it paused rather than leaving a flag behind for a branch
/// id that will never be written to again.
fn set_paused(args: &Args, branch: BranchId, pause: bool) -> Result<()> {
    let mut paused = paused_branches(args);
    paused.retain(|id| *id != branch.0);
    if pause {
        paused.push(branch.0);
    }
    let raw = serde_json::to_string_pretty(&DerivationConfig { paused })
        .map_err(|err| BorgError::Storage(err.to_string()))?;
    std::fs::write(derivation_path(args), raw).map_err(|err| BorgError::Storage(err.to_string()))
}

async fn derive_pause(args: &Args, pause: bool) -> Result<()> {
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;
    set_paused(args, branch, pause)?;
    outln!(
        "auto-derivation {} on {branch}",
        if pause { "paused" } else { "resumed" }
    );
    Ok(())
}

/// Whether this branch catches itself up after a write.
///
/// **A pause needs no other vocabulary.** A paused branch's frontier stops advancing and every read
/// of derived data already reports how far behind it is — so there is nothing to add to the read
/// envelope, because a pause *is* lag and the freshness machinery already describes lag.
async fn derive_status(args: &Args) -> Result<()> {
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;
    let paused = paused_branches(args).contains(&branch.0);
    outln!(
        "auto-derivation {} on {branch}",
        if paused { "paused" } else { "running" }
    );
    outln!("settled through {}", registry.settled(branch).await?);
    Ok(())
}
