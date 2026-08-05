//! `borg` — the command line client.
//!
//! This is the testbed for what a client is like to use. Every command goes through the same engine
//! an SDK eventually will, so if the CLI is awkward the design is awkward.
//!
//! Each invocation opens the store, does one thing, and exits. Layers and branches are durable; the
//! indexes are rebuilt from the log on open (see `Registry`).
//!
//! **This file is argv and rendering.** What the commands actually *do* lives in [`crate::ops`],
//! because `borg serve` does the same things over a socket and there must not be two implementations
//! of a transaction (SDK-DRAFT.md §2.6). A command here reads as: parse the arguments, call one op,
//! print what it returned.

use borg_core::{
    BorgError, BranchId, DefEvent, LayerAuthor, LayerId, LayerKind, MergeMode, ObjectTypeName,
    Ownership, ProducerId, RepoId, Result, Transaction, ValueType, Writer, parse,
};
use borg_engine::Registry;
use ops::Ops;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

mod ops;
mod serve;
mod sidecar;

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
    /// Everything an operation needs: the store, `--branch`, `--client-version`, `--freshness`,
    /// `--settled`. Held as one struct because `borg serve` fills the same struct from a message —
    /// a request naming a branch and a freshness is naming the same two things these flags do.
    ops: Ops,
    value_only: bool,
    /// `--quiet`. See [`derive`] — it selects an output *format*, and does not make the command a
    /// query.
    quiet: bool,
    /// `--outstanding`. See [`derive_status`].
    outstanding: bool,
    /// `--rebuild`. See [`derive`].
    rebuild: bool,
    /// `--retry-broken`. See [`derive`].
    retry_broken: bool,
    /// `--timeout`, in whole seconds. How long `borg frontier reaches` will wait.
    timeout: u64,
    /// `--tx`. Which open transaction a `borg tx …` command speaks to. See [`transaction_id`].
    tx: Option<String>,
    /// `--socket`. Where `borg serve` listens, and where every other command is told to look when
    /// the store is being served (see [`crate::serve`]).
    socket: Option<PathBuf>,
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
  borg derive [--quiet]                run producers until caught up
  borg derive --rebuild                recompute derived data from source, ignoring the cache
  borg derive --retry-broken           run producers this branch has judged broken
  borg derive pause | resume | status  auto-derivation on this branch
  borg derive --outstanding            what each producer has yet to incorporate — runs nothing

  borg layer list | borg layer head
  borg frontier                        how far each producer has caught up
  borg frontier reaches <layer>        wait until every producer has incorporated it

  borg serve --socket <path>           serve this store's client protocol on a unix socket

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

`borg serve` puts this same surface on a socket: transactions, reads with their provenance,
branches and definitions, as newline-delimited JSON one message per line. It is what an SDK speaks,
and it is deliberately the same operations these subcommands call rather than a second
implementation. A transaction belongs to the store and not to the connection, so a client that
disconnects can reconnect and name the same handle, and one that never comes back is reaped like any
other idle transaction. **One process serves a store**: while a store is served, every other borg
invocation against it is refused and told the socket to speak to.

A paused branch needs no special vocabulary: its frontier stops advancing, and every read of derived
data already reports how far behind it is. `borg derive` still works while paused — that is what
makes pausing useful in an emergency.

A producer that throws or cycles is **poisoned** — scoped to that producer, so main never breaks
because someone shipped a bad pipeline. Its cells read `state: broken` instead of `stale`, because
`stale` means a catch-up is coming and here none is; `borg explain` says what the error was, and
`borg derive` skips it rather than running the failure again. Recovery is pushing fixed code: a
producer's ClientVersion is the def-layer it was pushed at, and the poisoning names the version it
was recorded against, so a new one retires it. `--retry-broken` is for a fix the log cannot see.

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
  --socket <path>           `serve`: the unix socket to listen on
  --value                   print only the value
  --quiet                   `derive`: print the bare invocation count, without the prose
  --outstanding             `derive`: report pending work as a query, deriving nothing
  --rebuild                 `derive`: recompute from source instead of catching up
  --retry-broken            `derive`: run producers judged broken, instead of skipping them"
    );
    std::process::exit(2)
}

fn parse_args() -> Args {
    let mut args = Args {
        ops: Ops {
            store: PathBuf::from("borg.db"),
            branch: None,
            version: None,
            freshness: borg_core::FreshnessRequirement::Validated,
            settled: false,
        },
        value_only: false,
        quiet: false,
        outstanding: false,
        rebuild: false,
        retry_broken: false,
        timeout: 0,
        tx: None,
        socket: None,
        rest: Vec::new(),
    };
    let mut raw = std::env::args().skip(1);
    while let Some(arg) = raw.next() {
        match arg.as_str() {
            "--store" => args.ops.store = raw.next().unwrap_or_else(|| usage()).into(),
            "--branch" => args.ops.branch = raw.next(),
            "--client-version" => args.ops.version = raw.next().as_deref().map(layer_id),
            "--freshness" => args.ops.freshness = freshness(raw.next().as_deref()),
            "--settled" => args.ops.settled = true,
            "--socket" => args.socket = raw.next().map(PathBuf::from),
            "--timeout" => {
                args.timeout = raw
                    .next()
                    .and_then(|secs| secs.parse().ok())
                    .unwrap_or_else(|| usage());
            }
            "--tx" => args.tx = raw.next(),
            "--value" => args.value_only = true,
            "--quiet" => args.quiet = true,
            "--outstanding" => args.outstanding = true,
            "--rebuild" => args.rebuild = true,
            "--retry-broken" => args.retry_broken = true,
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

/// `--freshness`, refused by `usage()` rather than by an error, because a mode nobody has is a typo
/// in the command line and not a fact about the store. SPEC.md §10.5.
fn freshness(mode: Option<&str>) -> borg_core::FreshnessRequirement {
    mode.and_then(ops::freshness).unwrap_or_else(|| usage())
}

/// A layer id as written by `borg layer head` — `L7` — or bare.
fn layer_id(text: &str) -> LayerId {
    LayerId(
        text.trim_start_matches('L')
            .parse::<u64>()
            .unwrap_or_else(|_| usage()),
    )
}

async fn run(args: Args) -> Result<()> {
    let verb = args.rest.first().map(String::as_str).unwrap_or("");
    let rest: Vec<&str> = args.rest.iter().skip(1).map(String::as_str).collect();

    // **One process serves a store.** Sidecars and the in-process sequencer are not multi-process
    // safe, and they were not before `borg serve` either — what serve changes is that the second
    // process is now likely rather than hypothetical. So a served store refuses everyone else by
    // name, and says where the socket is (see `crate::serve`).
    if verb != "serve" {
        serve::refuse_if_served(&args.ops)?;
    }

    // Reaping sweeps **opportunistically, when a process opens the store** — the same place the
    // indexes are already rebuilt — so there is no daemon, and an idle store sweeps nothing because
    // nothing is growing (SPEC.md §12).
    ops::reap_transactions(&args.ops)?;

    match (verb, rest.as_slice()) {
        ("init", _) => init(&args).await,
        ("serve", _) => serve::run(&args.ops, args.socket.as_deref()).await,
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
    if args.ops.store.exists() {
        return Err(BorgError::Storage(format!(
            "{} already exists",
            args.ops.store.display()
        )));
    }
    let registry = ops::open(&args.ops).await?;
    let id = registry.branches.create_root(Some("main".into())).await?;
    outln!(
        "initialised {} (branch main = {id})",
        args.ops.store.display()
    );
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
    let registry = ops::open(&args.ops).await?;
    let branch = ops::branch_of(&registry, args.ops.branch.as_deref())?;
    let cell = parse::cell_ref(cell, ops::allocation_branch(&registry)?, ops::ALLOCATOR)?;

    let Some(fork_point) = ops::fork_point_of(&registry, branch)? else {
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
        .fork(ops::owner_of(&registry, fork_point)?, fork_point, None)
        .await?;

    let version = ops::client_version(&registry, &args.ops, scratch).await?;
    let mut session = registry
        .begin_write(scratch, version, Writer::Client)
        .await?;
    if let Err(rejection) = session.set_text(&cell, value).await {
        // A rejected write leaves no trace, so the layer it would have landed in never commits.
        session.abort().await?;
        return Err(rejection);
    }
    let mut transaction = Transaction::new(scratch, branch, fork_point);
    ops::absorb(&mut transaction, &session);
    session.commit().await?;

    let replayed = registry
        .branches
        .merge_transaction(&transaction, MergeMode::DefAndData)
        .await?;
    // The layer on the **parent**, not on the scratch branch: this is what a client awaits with
    // `borg frontier reaches`, and the one on the branch nobody else can see is no use for that.
    outln!("{}", ops::landing(&registry, &transaction, &replayed));
    drop(registry);
    ops::auto_derive(&args.ops, branch).await
}

/// The one write path that is not a transaction, and the only branch state it can exist on is none
/// at all. See the caller for why that is safe.
async fn write_directly(
    registry: &Registry,
    args: &Args,
    branch: BranchId,
    cell: &borg_core::CellRef,
    value: &str,
) -> Result<()> {
    let version = ops::client_version(registry, &args.ops, branch).await?;
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
    report(args, &ops::get(&args.ops, cell).await?)
}

/// The provenance envelope, as `borg get` and `borg tx get` both print it.
///
/// One renderer, because a read through a transaction is a read: if the two drifted, the CLI would
/// be teaching that a transaction's reads are a different kind of thing from a branch's, which is
/// exactly the belief §12 exists to remove. `borg serve` renders the same [`ops::Read`] as an
/// envelope message, and `scenarios/250-serve` asserts the two agree field for field.
fn report(args: &Args, read: &ops::Read) -> Result<()> {
    // Interned content reads back as content, so a string field prints `acme.ai` rather than the
    // `@s-…` that is physically stored (§3.4). What `borg get --value` prints is what `borg set`
    // accepts, which is the property a shell pipeline actually relies on.
    if args.value_only {
        if let Some(value) = &read.rendered {
            outln!("{value}");
        }
        return Ok(());
    }

    let resolved = &read.resolved;
    outln!("{}", read.cell);
    outln!(
        "  value:       {}",
        read.rendered.as_deref().unwrap_or("<absent>")
    );
    // Shown only when there is one: it is the proof that equal content is stored once, registry-wide
    // and branch-independently (§3.1), and there is nothing to show for a primitive.
    if let Some(pid) = &read.interned {
        outln!("  interned:    {pid}");
    }
    outln!("  origin:      {}", ops::origin_name(resolved.origin));
    outln!("  state:       {}", ops::state_name(resolved.state));
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

async fn explain(args: &Args, cell: &str) -> Result<()> {
    let (cell, lineage) = ops::explain(&args.ops, cell).await?;
    let Some(lineage) = lineage else {
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
    // §14 promises lineage that explains *why* a cell is broken, and this is the sentence. Said
    // second, straight after the producer's name, because the two belong in one thought.
    if let Some(why) = &lineage.broken {
        outln!("  broken: {why}");
    }
    if !lineage.from.is_empty() {
        outln!("  from");
        for edge in lineage.from {
            outln!(
                "    {}  {}  @{}",
                edge.cell.cell,
                // Padded to the width of `derived`, so the layer column lines up.
                format!("{:<7}", ops::origin_name(edge.origin)),
                edge.landed_at
            );
        }
    }
    Ok(())
}

async fn branch_list(args: &Args) -> Result<()> {
    for branch in ops::branch_list(&args.ops).await? {
        let name = branch.name.unwrap_or_else(|| "<unnamed>".into());
        match branch.origin {
            Some(origin) => outln!("{:<6} {name:<16} forked at {origin}", branch.id.to_string()),
            None => outln!("{:<6} {name:<16} root", branch.id.to_string()),
        }
    }
    Ok(())
}

async fn branch_fork(args: &Args, parent: &str, tail: &[&str]) -> Result<()> {
    let registry = ops::open(&args.ops).await?;
    let parent_id = ops::branch_of(&registry, Some(parent))?;
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
    let registry = ops::open(&args.ops).await?;
    let child_id = ops::branch_of(&registry, Some(child))?;
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
        Some(parent) => ops::auto_derive(&args.ops, parent).await,
        None => Ok(()),
    }
}

// --- Transactions. SPEC.md §12, §13. ---------------------------------------------------------------

/// Which open transaction this command speaks to.
///
/// `--tx`, then `$BORG_TX`, then — when exactly one is open — that one. The environment variable is
/// what makes the surface in §12 read as written (`borg tx get <cell>`, no handle in sight) from a
/// shell that exported it once; the flag is what lets one shell hold two transactions open at the
/// same time, which is the whole reason the handle is explicit and is an interleaving a
/// single-process API cannot express.
///
/// **This stayed in the CLI when the rest of the transaction moved to `ops`**, and the reason is a
/// small finding about the surface: every one of these defaults is a way of *not* naming a
/// transaction, and they all borrow from the shell — a flag, an exported variable, or the fact that
/// a terminal usually has one thing going on. A socket client has none of that. It holds its handles
/// in variables and names one on every message, so `ops` takes an explicit id and this is the only
/// place that guesses.
fn transaction_id(args: &Args, table: &ops::Transactions) -> Result<String> {
    let named = args
        .tx
        .clone()
        .or_else(|| std::env::var("BORG_TX").ok().filter(|id| !id.is_empty()));
    if let Some(id) = named {
        return Ok(id);
    }
    match table.open.len() {
        0 => Err(BorgError::Storage(
            "no transaction is open — start one with `borg tx begin`".into(),
        )),
        1 => Ok(table.open[0].id.clone()),
        _ => Err(BorgError::Storage(format!(
            "several transactions are open ({}) — name one with --tx",
            table
                .open
                .iter()
                .map(|open| open.id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ))),
    }
}

/// The handle a `borg tx …` command is speaking to, resolved against the table on disk.
///
/// The table is loaded here and again inside the operation. That is one extra read of a small file
/// per command, and it buys the operations an argument list that says what they need — an id — rather
/// than a table plus an index into it, which is a shape only this caller could ever produce.
fn tx_target(args: &Args) -> Result<String> {
    transaction_id(args, &ops::load_transactions(&args.ops))
}

async fn tx_begin(args: &Args) -> Result<()> {
    outln!("{}", ops::tx_begin(&args.ops).await?);
    Ok(())
}

async fn tx_get(args: &Args, cell: &str) -> Result<()> {
    let read = ops::tx_get(&args.ops, &tx_target(args)?, cell).await?;
    report(args, &read)
}

async fn tx_set(args: &Args, cell: &str, value: &str) -> Result<()> {
    outln!(
        "{}",
        ops::tx_set(&args.ops, &tx_target(args)?, cell, value).await?
    );
    Ok(())
}

async fn tx_commit(args: &Args) -> Result<()> {
    outln!("{}", ops::tx_commit(&args.ops, &tx_target(args)?).await?);
    Ok(())
}

async fn tx_abort(args: &Args) -> Result<()> {
    let id = tx_target(args)?;
    ops::tx_abort(&args.ops, &id)?;
    outln!("{id} aborted");
    Ok(())
}

fn tx_list(args: &Args) -> Result<()> {
    let table = ops::load_transactions(&args.ops);
    if table.open.is_empty() {
        outln!("no open transactions");
        return Ok(());
    }
    let now = ops::now();
    for open in &table.open {
        let (reads, writes) = open.state.size();
        outln!(
            "{:<8} {:<6} forked at {:<6} {reads} read, {writes} written, idle {}",
            open.id,
            open.state.branch.to_string(),
            open.state.fork_point.to_string(),
            ops::render_duration(now.saturating_sub(open.touched))
        );
    }
    Ok(())
}

/// Read or set the idle timeout. SPEC.md §12.
fn tx_timeout(args: &Args, spec: Option<&str>) -> Result<()> {
    let mut table = ops::load_transactions(&args.ops);
    if let Some(spec) = spec {
        table.tx_idle_timeout = ops::duration(spec).ok_or_else(|| {
            BorgError::Storage(format!(
                "`{spec}` is not a duration — try 90s, 10m, 24h or 7d"
            ))
        })?;
        ops::save_transactions(&args.ops, &table)?;
    }
    outln!(
        "tx_idle_timeout = {}",
        ops::render_duration(table.tx_idle_timeout)
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
    let registry = ops::open(&args.ops).await?;
    let branch = ops::branch_of(&registry, args.ops.branch.as_deref())?;

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
    ops::auto_derive(&args.ops, branch).await
}

async fn def_show(args: &Args, name: &str) -> Result<()> {
    let object = ops::def_show(&args.ops, name).await?;
    outln!("{}", object.name);
    for (field, def) in &object.fields {
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
    outln!("{}", ops::def_version(&args.ops).await?);
    Ok(())
}

async fn layer_list(args: &Args) -> Result<()> {
    let registry = ops::open(&args.ops).await?;
    let branch = ops::branch_of(&registry, args.ops.branch.as_deref())?;
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
    outln!("{}", ops::branch_head(&args.ops).await?.1);
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
        let registry = ops::open(&args.ops).await?;
        let branch = ops::branch_of(&registry, args.ops.branch.as_deref())?;
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
    let registry = ops::open(&args.ops).await?;
    let branch = ops::branch_of(&registry, args.ops.branch.as_deref())?;
    let path = registry.branches.read_path(branch, None)?;
    let view = registry.defs.view(&path).await?;

    // The log knows producer ids; only the implementation table knows what a human called them.
    // Joining the two here is the CLI's job precisely because the log must not hold either.
    let names = ops::load_impls(&args.ops);
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

// --- Repos, producers and derivation. The state these read and write lives in `ops`. ---

async fn repo_push(args: &Args, dir: &str) -> Result<()> {
    let dir = PathBuf::from(dir);
    let repo = read_repo_id(&dir)?;
    let registry = ops::open(&args.ops).await?;
    let branch = ops::branch_of(&registry, args.ops.branch.as_deref())?;

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

    let mut impls = ops::load_impls(&args.ops);
    let mut events = Vec::new();
    // Held back until the push is accepted — see the header.
    let mut report: Vec<String> = Vec::new();
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
            ops::remember(&mut impls, id, &spec.name, &spec.source, command);
            report.push(format!("{} -> {id}", spec.name));
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
                        ops::remember(&mut impls, id, name, &spec.name, &command);
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
                        // Asked before the missing-`up` question, because for a derived field the
                        // answer is not "name a migration" — no migration can be appointed for it at
                        // all, so advising one would send the author to write code the next push
                        // would reject.
                        if let Some(owner) = existing.ownership.producer() {
                            return Err(BorgError::MigrationOnDerivedField {
                                struct_name: struct_name.clone(),
                                field: field.name.clone(),
                                owner,
                            });
                        }
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
                        report.push(format!("{what} {} -> {}", existing.ty, field.ty));
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
                        report.push(format!("{what} {}", field.ty));
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
    ops::save_impls(&args.ops, &impls)?;
    // Everything is accepted and committed, so this is a report rather than a running commentary.
    for line in report {
        outln!("{line}");
    }
    ops::auto_derive(&args.ops, branch).await
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
    for producer in ops::load_impls(&args.ops).producers {
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

async fn derive(args: &Args) -> Result<()> {
    // `--outstanding` names a query wherever it appears, and this is the spelling somebody reaches
    // for first. Running a round because the flag was attached to the verb rather than to `status`
    // would be the one outcome a caller asking what is outstanding did not want.
    if args.outstanding {
        let registry = ops::open(&args.ops).await?;
        let branch = ops::branch_of(&registry, args.ops.branch.as_deref())?;
        return outstanding(args, &registry, branch).await;
    }
    let (registry, workers) = ops::open_deriving(&args.ops).await?;
    let branch = ops::branch_of(&registry, args.ops.branch.as_deref())?;

    if args.retry_broken {
        for (name, poisoning) in ops::broken_here(&args.ops, &registry, branch)? {
            eprintln!("note: retrying {name}, broken since {}", poisoning.since);
        }
        registry.engine.retry_broken(branch)?;
    }
    // Said *before* the work, because it is the explanation for the count printed after it. A
    // producer poisoned during this run is different news, and is reported below.
    let mut skipped = Vec::new();
    for (name, poisoning) in ops::broken_here(&args.ops, &registry, branch)? {
        eprintln!(
            "note: skipping {name} — broken since {}: {}",
            poisoning.since, poisoning.error
        );
        skipped.push(poisoning.producer);
    }
    if !skipped.is_empty() {
        eprintln!(
            "note: push fixed code to recover, or `borg derive --retry-broken` to run it anyway"
        );
    }

    let executed = if args.rebuild {
        registry.engine.recompute(branch).await?
    } else {
        registry.engine.catch_up(branch).await?
    };
    workers.shutdown().await;

    for (name, poisoning) in ops::broken_here(&args.ops, &registry, branch)? {
        if !skipped.contains(&poisoning.producer) {
            eprintln!("warning: {name} is now broken: {}", poisoning.error);
        }
    }

    // `--quiet` chooses a *format*, not a mode. The command ran the round either way; a caller that
    // only wants to know what is left to do wants `borg derive status --outstanding`, which runs
    // nothing.
    if args.quiet {
        outln!("{executed}");
    } else {
        outln!("{executed} invocation(s)");
    }
    Ok(())
}

async fn derive_pause(args: &Args, pause: bool) -> Result<()> {
    let registry = ops::open(&args.ops).await?;
    let branch = ops::branch_of(&registry, args.ops.branch.as_deref())?;
    ops::set_paused(&args.ops, branch, pause)?;
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
///
/// `--outstanding` is the **read-only** half, and the only one there is: it prints the gap between
/// each producer's watermark and head (§16.4) and runs nothing. It deliberately does not report an
/// invocation *count*, because there is no honest way to have one without scheduling — work is
/// implied by the gap plus the dependency index, and the only thing that turns the implication into
/// a number is a round that forks a branch and walks the changesets. `borg derive --quiet` is that
/// round, and its number is what it did rather than what was owed.
async fn derive_status(args: &Args) -> Result<()> {
    let registry = ops::open(&args.ops).await?;
    let branch = ops::branch_of(&registry, args.ops.branch.as_deref())?;
    if args.outstanding {
        return outstanding(args, &registry, branch).await;
    }
    let paused = ops::paused_branches(&args.ops).contains(&branch.0);
    outln!(
        "auto-derivation {} on {branch}",
        if paused { "paused" } else { "running" }
    );
    outln!("settled through {}", registry.settled(branch).await?);
    // Registered so the poison table can be read against the ClientVersions the branch appoints —
    // a record naming a version that has since been replaced is spent, and printing it would send
    // somebody to fix code that has already been fixed (§14).
    registry.register_producers(branch).await?;
    for (name, poisoning) in ops::broken_here(&args.ops, &registry, branch)? {
        outln!(
            "broken      {name} since {}: {}",
            poisoning.since,
            poisoning.error
        );
    }
    Ok(())
}

/// What each producer has yet to incorporate. SPEC.md §16.4.
///
/// A pure query over the frontier and the log: no executor is built, no round is forked, nothing is
/// written. That is the whole reason it exists — asking "is anything outstanding" used to mean
/// running `borg derive` and reading the count it printed, which answers the question by doing the
/// work.
async fn outstanding(args: &Args, registry: &Registry, branch: BranchId) -> Result<()> {
    // Producers are registered so that the engine can be asked about the ones this branch *defines*
    // rather than the ones this process happens to have seen.
    registry.register_producers(branch).await?;
    let names = ops::load_impls(&args.ops);
    let mut any = false;
    for producer in registry.producers_of(branch).await? {
        let Some(gap) = registry.engine.pending(branch, producer) else {
            continue;
        };
        any = true;
        // Both ends are positions in the *source* stream, which is the only stream a watermark is
        // comparable with (§6.3) — so on a settled branch this says nothing rather than naming the
        // derived layer the last round merged.
        outln!(
            "outstanding {:<16} incorporated through {}, owes up to {}",
            ops::producer_name(&names, producer),
            gap.from,
            gap.to
        );
    }
    if !any {
        outln!("nothing outstanding");
    }
    Ok(())
}
