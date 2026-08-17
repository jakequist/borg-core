//! `borg` — the command line client.
//!
//! This is the testbed for what a client is like to use. Every command goes through the same engine
//! an SDK eventually will, so if the CLI is awkward the design is awkward.
//!
//! Each invocation opens the store, does one thing, and exits. Layers and branches are durable; the
//! indexes are rebuilt from the log on open (see `Registry`).
//!
//! **This file is argv and rendering.** What the commands actually *do* lives in `borg_host`,
//! because `borg-server` does the same things over a socket and there must not be two
//! implementations of a transaction (SDK-DRAFT.md §2.6). A command here reads as: parse the
//! arguments, call one op, print what it returned.
//!
//! ## What this binary is, now that there is a server
//!
//! **Embedded Borg, and that is a permanent role rather than a leftover.** `borg` operates directly
//! on a store nobody is serving: a scenario, a fixture, a build step, `borg init`, a one-off script.
//! There is no socket, no daemon and nothing to supervise, and the whole cost of that is the
//! `O(log)` open a process that exits has no way to avoid.
//!
//! What it is *not* any more is a server. `borg serve` is gone; `borg-server` is the process that
//! stays up, and it hosts a directory of registries rather than the one store a `--store` names
//! (SPEC.md §17.6). While a store is served, every command here is refused by name and told the
//! socket — except the two that *connect* to it, [`crate::generate`] and `repo push`, which take a
//! `--url` naming a server and a registry rather than a `--store` naming a file (§17.7).

use borg_core::{
    BorgError, BranchId, DefEvent, LayerAuthor, LayerId, LayerKind, MergeMode, ProducerId, RepoId,
    Result, Transaction, Writer, parse,
};
use borg_engine::Registry;
use borg_host::ops::{self, Ops};
use borg_host::{push, serving, stream};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

mod generate;

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
    /// `--settled`. Held as one struct because `borg-server` fills the same struct from a message
    /// — a request naming a branch and a freshness is naming the same two things these flags do.
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
    /// `--url`, or `$BORG_URL`. **Which server, and which registry on it** (SPEC.md §17.7).
    ///
    /// Read by the two commands that speak to a socket rather than opening a store — `generate`
    /// and `repo push` — and by nothing else, because nothing else here is a protocol client. An
    /// explicit `--url` on any other command is refused rather than ignored: it says the caller
    /// meant a server, and the answer they would silently have got is about a file.
    url: Option<String>,
    /// Whether [`Args::url`] came from the flag rather than from `$BORG_URL`. See there.
    url_was_a_flag: bool,
    /// `--lang`. Which SDK `borg generate` emits.
    lang: String,
    /// `-o` / `--out`. Where `borg generate` writes its module.
    out: Option<PathBuf>,
    /// `--watch`. See [`crate::generate`].
    watch: bool,
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
  borg list <Struct>                   every object of a struct, one pid per line

  borg tx begin                        fork the branch; prints a transaction handle
  borg tx create <Struct> [--tx <id>]  allocate an object and create it; prints its pid
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
  borg repo push <dir> --url <url>     …asking a running server to push it, live
  borg producer list                   registered producers
  borg derive [--quiet]                run producers until caught up
  borg derive --rebuild                recompute derived data from source, ignoring the cache
  borg derive --retry-broken           run producers this branch has judged broken
  borg derive pause | resume | status  auto-derivation on this branch
  borg derive --outstanding            what each producer has yet to incorporate — runs nothing

  borg layer list | borg layer head
  borg frontier                        how far each producer has caught up
  borg frontier reaches <layer>        wait until every producer has incorporated it

  borg export [<file>]                 write this registry out as a canonical event stream
  borg import <file>                   restore one into --store, which must hold nothing yet

  borg generate --lang ts -o <dir>     emit a typed client, pinned to this branch's def-version
  borg generate ... --watch            and rewrite it whenever that def-version moves

Cells are written Struct:pid.field, Struct:pid, Element[]:pid or Element[]:pid[n], where a pid
looks like o-1234abcd and names the whole identity. Struct#100 is accepted on input as a
shorthand for counter 100 on the root branch; what borg prints is always the canonical form.

`borg tx create` allocates a pid rather than taking one, under an allocator of its own — so ids it
issues and ids you write by hand as Struct#100 can never collide, whoever creates what first. `borg
list` is the other half: it enumerates a struct's objects and prints their pids, skipping the ones
that were deleted. It is a read at head and it is not part of a transaction: what a listing found
buys no protection at commit, because `the set of Contacts` is not a cell a guard can be asked
about. It answers ids and nothing else — reading a field of each is a read of each.

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

`borg-server` puts this same surface on a socket: transactions, reads with their provenance,
branches and definitions, as newline-delimited JSON one message per line. It is what an SDK speaks,
and it is deliberately the same operations these subcommands call rather than a second
implementation — a separate binary because a process that stays up and a process that exits after
one command are opposite lifecycles, not two modes. **One process serves a store**: while a store is
served, every borg invocation against it is refused and told the socket to speak to. `borg` itself
stays what it always was, which is a client operating directly on a store nobody is serving.

**A connection url is how you name a server instead of a store**: one string carrying both halves a
client needs, the way DATABASE_URL does.

  borg://localhost/personal-crm              the well-known socket, registry personal-crm
  borg+unix:///path/to/borg.sock/crm         an explicit socket; the last segment is the registry
  borg+unix:///path/to/borg.sock             …and a trailing slash, or no name, says none

A registry left out of the url is left out of the handshake, and the server answers with its sole
registry when it hosts one and names the options when it hosts more. `borg+ws://` is reserved for
the browser transport and is refused by name today rather than invented later.

`--url`, or $BORG_URL, is read by the two commands that speak to a server rather than opening a
store — `generate`, which reads a def view, and `repo push`, which asks the *server* to run the
push against a path on its own disk, so a schema can be pushed into a running server without
stopping it. Every other command here is embedded borg and works on --store; passing --url to one
is refused rather than quietly ignored.

`borg generate` writes the other half: a TypeScript module holding an interface and a runtime
descriptor per struct, and a `createBorgContext` with **this branch's def-version baked in as the
client's ClientVersion**. That stamp is the point. borg itself has no generated code and so is
authored anew on every invocation, but a generated client was authored once, and keeping working
after the schema moves on is what `down` migrations are for (§5.4). Regenerating is how you adopt a
new schema; not regenerating is supported. Generation reads through the socket when the store is
being served and opens the store directly when it is not, because a served store would otherwise
have to be stopped to generate against it.

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

**What borg guarantees across versions is the data, not the bytes.** On-disk formats may change
before 1.0; every release writes a registry out as a canonical event stream and reads streams
written by earlier releases back in, so an upgrade is export, upgrade, import. `borg export` with
no file writes to stdout and `borg import -` reads stdin, so a backup is a pipe and a clone is
`borg export | borg --store copy.db import -`. The stream carries the log — layers, branches,
events with their read-sets, membership, def events, interned bytes — plus the PID counter, the
producer table, pause flags and poisonings. It does not carry open transactions, which are
ephemeral, and it does not carry any index, because every index in borg is a fold over the log.
Import creates the registry: importing into one that already holds anything is refused, because
restore is create-then-import and merging two id spaces would rewrite the lineage the stream exists
to preserve. Two exports of one registry are byte-identical, which is how you check a restore.

Options:
  --store <path>            store file (default ./borg.db)
  --branch <name>           branch to operate on (default: the root branch)
  --client-version <layer>  act as a client authored against this def-layer
  --freshness <mode>        any | validated (default) | current
  --settled                 read at the settled frontier, not at the ragged head
  --timeout <seconds>       how long `frontier reaches` waits (default 0)
  --tx <id>                 which open transaction to speak to (or $BORG_TX)
  --url <url>               which server and registry: `generate` and `repo push` only
  --lang <name>             `generate`: which SDK to emit (ts)
  -o, --out <dir>           `generate`: where to write the module
  --watch                   `generate`: rewrite it whenever the def-version moves
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
            // The CLI is process-per-command, so it holds nothing: every command opens the store,
            // rebuilds the projections and exits. That is the honest cost of a process that does not
            // stay up, and it is unchanged — `borg-server` is the one caller that fills this in.
            held: None,
        },
        value_only: false,
        quiet: false,
        outstanding: false,
        rebuild: false,
        retry_broken: false,
        timeout: 0,
        tx: None,
        // `$BORG_URL` is the ambient form and the flag overrides it, exactly as `DATABASE_URL` and
        // a `--database-url` relate. An empty value is treated as unset, so `BORG_URL= borg …` is
        // how a shell that exported one opts back out for one command.
        url: std::env::var("BORG_URL").ok().filter(|url| !url.is_empty()),
        url_was_a_flag: false,
        lang: "ts".to_string(),
        out: None,
        watch: false,
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
            "--url" => {
                args.url = raw.next();
                args.url_was_a_flag = true;
            }
            "--lang" => args.lang = raw.next().unwrap_or_else(|| usage()),
            "-o" | "--out" => args.out = raw.next().map(PathBuf::from),
            "--watch" => args.watch = true,
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

/// Where a socket-speaking command should connect, if the caller named a server. SPEC.md §17.7.
///
/// **Two commands here are protocol clients** — `generate`, which reads a def view, and `repo
/// push`, which asks the server to run a push against a path on its own disk. Everything else is
/// embedded Borg and works on a `--store`. So this answers `None` when no URL was given, and the
/// two commands fall back to what they did before: `generate` reads the store's own lock record,
/// and `repo push` opens the store.
///
/// `borg://` is resolved here rather than in the parser because the well-known address is
/// `borg_host::host`'s to know (`borg_protocol::url::Transport::Local`).
struct Dial {
    socket: PathBuf,
    /// The registry to name in the handshake. `None` is `None` on the wire — the server's n=1
    /// convenience and n≥2 refusal are one rule and live there (§17.6).
    registry: Option<String>,
}

fn dial(args: &Args) -> Result<Option<Dial>> {
    let Some(text) = args.url.as_deref() else {
        return Ok(None);
    };
    let url = borg_protocol::url::ConnectionUrl::parse(text)?;
    Ok(Some(Dial {
        socket: url.socket(&borg_host::host::well_known_socket()),
        registry: url.registry,
    }))
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

    // The two commands that are protocol clients rather than embedded operations (SPEC.md §17.7).
    // Named in one place because three things below key off the same fact: whether a URL is
    // honoured, whether the store is opened at all, and whether an explicit `--url` was a mistake.
    let connects = matches!(
        (verb, rest.as_slice()),
        ("generate", _) | ("repo", ["push", _])
    );
    let dialled = if connects { dial(&args)? } else { None };
    if args.url_was_a_flag && !connects {
        // Refused rather than ignored: `--url` says the caller meant a server, and what they would
        // silently have got is an answer about `--store`. `$BORG_URL` is ambient and is left alone,
        // for the same reason an exported variable does not break every unrelated command.
        return Err(BorgError::Storage(format!(
            "`--url` names a server, and `borg {verb}` operates on a store directly — the commands \
             that connect are `generate` and `repo push`"
        )));
    }

    // **One process serves a store.** Sidecars and the in-process sequencer are not multi-process
    // safe, and they were not before there was a server either — what a server changes is that the
    // second process is now likely rather than hypothetical. So a served store refuses everyone else
    // by name, and says where the socket is (`borg_host::serving`).
    //
    // The commands that connect are the exception, and it is not an exemption: they do not open a
    // served store either, they *speak to the socket* instead. That is SDK-DRAFT §2.6's
    // remote-connection future arriving for two commands — a pure read and a push the server
    // performs — and deliberately not for the write path, which needs answers about `--tx` and
    // `$BORG_TX` that a socket has none of.
    let embedded = !connects || (verb == "repo" && dialled.is_none());
    if embedded {
        serving::refuse_if_served(&args.ops.store)?;
    }

    // Reaping sweeps **opportunistically, when a process opens the store** — the same place the
    // indexes are already rebuilt — so there is no daemon, and an idle store sweeps nothing because
    // nothing is growing (SPEC.md §12). Not for a command that connects, which may not be touching
    // this store's files at all: sweeping a served store's transaction table from a second process
    // is exactly the thing the lock exists to prevent.
    if embedded {
        ops::reap_transactions(&args.ops)?;
    }

    match (verb, rest.as_slice()) {
        ("init", _) => init(&args).await,
        ("set", [cell, value]) => set(&args, cell, value).await,
        ("delete", [cell]) => set(&args, cell, "~").await,
        ("get", [cell]) => get(&args, cell).await,
        ("list", [name]) => list(&args, name).await,
        ("tx", ["begin"]) => tx_begin(&args).await,
        ("tx", ["create", name]) => tx_create(&args, name).await,
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
        ("export", rest) => export(&args, rest.first().copied()).await,
        ("import", [file]) => import(&args, file).await,
        ("repo", ["push", dir]) => repo_push(&args, dialled.as_ref(), dir).await,
        ("producer", ["list"]) => producer_list(&args).await,
        ("derive", ["pause"]) => derive_pause(&args, true).await,
        ("derive", ["resume"]) => derive_pause(&args, false).await,
        ("derive", ["status"]) => derive_status(&args).await,
        ("derive", _) => derive(&args).await,
        ("frontier", ["reaches", layer]) => frontier_reaches(&args, layer).await,
        ("frontier", _) => frontier(&args).await,
        ("generate", _) => {
            let out = args.out.clone().unwrap_or_else(|| usage());
            generate::run(
                &args.ops,
                &generate::Generate {
                    lang: args.lang.clone(),
                    out,
                    watch: args.watch,
                    socket: dialled.as_ref().map(|dial| dial.socket.clone()),
                    registry: dialled.as_ref().and_then(|dial| dial.registry.clone()),
                },
            )
            .await
        }
        _ => usage(),
    }
}

async fn init(args: &Args) -> Result<()> {
    let id = ops::init(&args.ops).await?;
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
/// exactly the belief §12 exists to remove. `borg-server` renders the same [`ops::Read`] as an
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

/// Every object of a struct, one pid per line. SPEC.md §9.6, §17.5.
///
/// One pid per line and nothing else, because that is what a shell can use: `borg list Contact |
/// while read id; do borg get "Contact:$id.name" --value; done` is the N+1 made visible, and making
/// it visible is better than hiding it behind a column this command would then have to grow options
/// for.
async fn list(args: &Args, name: &str) -> Result<()> {
    for pid in ops::list(&args.ops, name).await? {
        outln!("{pid}");
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

/// Allocate an object and create it, in the current transaction. Prints its pid.
///
/// The pid and nothing else, so that `id=$(borg tx create Contact)` works and the next line can say
/// `borg tx set "Contact:$id.name" Ada`.
async fn tx_create(args: &Args, name: &str) -> Result<()> {
    outln!(
        "{}",
        ops::tx_create(&args.ops, &tx_target(args)?, name).await?
    );
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
                ty: ops::value_type(&ty),
                repo,
                ownership: ops::ownership(derived_by.map(ProducerId)),
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
                ty: ops::value_type(&ty),
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

/// Write this store out as a canonical event stream. SPEC.md §19.
///
/// **No file, or `-`, means stdout**, which is what makes a backup a pipe and a clone one line:
/// `borg export | borg --store copy.db import -`. The summary then goes to stderr, so it does not
/// end up inside the backup — a report that corrupts the artifact it describes is the one thing this
/// command must not do.
async fn export(args: &Args, file: Option<&str>) -> Result<()> {
    let to_stdout = matches!(file, None | Some("-"));
    let report = if to_stdout {
        let mut out = std::io::BufWriter::new(std::io::stdout());
        stream::export(&args.ops, &mut out).await?
    } else {
        let path = file.unwrap_or_else(|| usage());
        let handle = std::fs::File::create(path)
            .map_err(|err| BorgError::Storage(format!("{path}: {err}")))?;
        let mut out = std::io::BufWriter::new(handle);
        stream::export(&args.ops, &mut out).await?
    };

    // Where the log stood, said plainly, because that is the whole answer to *what did I just
    // capture*: the export is the log at head, not a settled read — settling would silently drop
    // every source layer above the watermark (see `stream::export`). The settled position is
    // reported beside it so a backlog is visible rather than surprising.
    let lag = if report.settled.0 < report.head.0 {
        format!(
            "; the default branch is settled to {} — the backlog is captured with it",
            report.settled
        )
    } else {
        String::new()
    };
    let summary = format!(
        "exported {} layers, {} events, {} interned values — the log ends at {}{lag}",
        report.layers, report.events, report.interned, report.head
    );
    if to_stdout {
        eprintln!("{summary}");
    } else {
        outln!("{summary}");
    }
    Ok(())
}

/// Restore a stream into `--store`, which must not already hold a registry. SPEC.md §19.
async fn import(args: &Args, file: &str) -> Result<()> {
    let report = if file == "-" {
        let mut input = std::io::BufReader::new(std::io::stdin());
        stream::import(&args.ops.store, &mut input).await?
    } else {
        let handle = std::fs::File::open(file)
            .map_err(|err| BorgError::Storage(format!("{file}: {err}")))?;
        let mut input = std::io::BufReader::new(handle);
        stream::import(&args.ops.store, &mut input).await?
    };
    outln!(
        "restored {} into {}: {} layers, {} events, {} branches, head {} (written by {})",
        file,
        args.ops.store.display(),
        report.layers,
        report.events,
        report.branches,
        report.head,
        report.written_by
    );
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

/// Push a repo: definitions and pipelines, as one diff against what the branch believes (§9.2).
///
/// **Argv and printing, like every other command here.** The push itself is
/// [`borg_host::push::repo_push`], which is what `borg-server` runs when a client sends `repo_push`
/// — so a schema pushed from a terminal and a schema pushed into a running server are the same
/// operation reported twice, rather than two things that happen to agree today.
///
/// **With a `--url` it is the second of those**, and that is what retires *"pushing a schema means
/// stopping the server"*. A push moves definitions, which travel the log, and implementations,
/// which are a sidecar beside the store — so a second process doing it is the second writer the
/// advisory lock refuses (§17.6). The way out is not to let this process write; it is to ask the
/// server to, which is one `repo_push` message and no new operation.
async fn repo_push(args: &Args, dial: Option<&Dial>, dir: &str) -> Result<()> {
    let report = match dial {
        Some(dial) => over_socket_push(args, dial, dir)?,
        None => push::repo_push(&args.ops, Path::new(dir)).await?.report,
    };
    // Everything is accepted and committed by now, so this is a report rather than a running
    // commentary — and it is the same report either way, because it is the same operation.
    for line in &report {
        outln!("{line}");
    }
    Ok(())
}

/// Ask the server to push, and hand back the lines it reported. SPEC.md §17.6.
fn over_socket_push(args: &Args, dial: &Dial, dir: &str) -> Result<Vec<String>> {
    // **Absolute, because `path` is a path on the server's disk** and this process's working
    // directory is not the server's. Canonicalised here rather than sent raw so that the failure
    // for a directory that does not exist is *this* one — naming the path the caller typed — and
    // not a server-side error about a path the caller never wrote.
    let path = std::fs::canonicalize(dir)
        .map_err(|err| BorgError::Storage(format!("{dir}: {err}")))?
        .display()
        .to_string();
    let request = borg_protocol::client::Request::RepoPush {
        // Absent: the handshake already settled which registry this connection is for, and naming
        // it twice would let the two disagree. The field exists for a deploy client pushing to
        // several registries over one connection, which is not this.
        registry: None,
        branch: args.ops.branch.clone(),
        path: Some(path),
    };
    match borg_protocol::client::ask(&dial.socket, dial.registry.as_deref(), &request)? {
        borg_protocol::client::Response::Pushed { report, .. } => Ok(report),
        borg_protocol::client::Response::Error { message } => Err(BorgError::Storage(message)),
        other => Err(BorgError::Storage(format!(
            "{}: expected a push report, got {other:?}",
            dial.socket.display()
        ))),
    }
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
