//! `borg-server` — the dedicated server, hosting a directory of registries. SPEC.md §17.5, §17.6.
//!
//! **A binary of its own, and the split is a decision rather than tidiness.** `borg` is a client and
//! stays one: it operates directly on a store nobody is serving, which is embedded Borg and is
//! legitimate forever. This is the process that stays up — it holds registries open, owns the
//! advisory lock on each, runs the worker pools, and answers §17.5 on a socket.
//!
//! Three reasons the two are not one binary:
//!
//! * **They have opposite lifecycles.** Everything the CLI is — open, do one thing, exit — is what a
//!   server must not do, and every piece of state that made `borg serve` awkward was a piece of
//!   state a process-per-command client had no reason to have. A binary that is both teaches neither.
//! * **They will be deployed apart.** The server is what runs in a container, under systemd, and on
//!   platform.borg-hq.com; the CLI is what a developer installs. Shipping the second everywhere the
//!   first goes is a bigger artifact and a bigger attack surface for no gain.
//! * **Merging later is trivial and splitting later is surgery.** `psql`/`postgres` and
//!   `redis-cli`/`redis-server` are the precedent, and neither project has ever wished it back.
//!
//! What is deliberately *not* split is the code: everything either binary does to a store lives in
//! `borg_host`, so the two front ends cannot drift into two answers about what a transaction is.
//!
//! ## The commands
//!
//! ```text
//! borg-server start [--foreground]     serve --data-dir; backgrounds unless told otherwise
//! borg-server stop                     SIGTERM, then wait for the socket to go quiet
//! borg-server status                   the address, the pid, and what is hosted
//! borg-server logs [-n N] [-f]         what a backgrounded server has printed
//! borg-server create <name>            make a registry, through the server if one is running
//! ```

use borg_core::{FreshnessRequirement, Result};
use borg_host::host;
use borg_host::ops::Ops;
use borg_protocol::client::RegistryInfo;
use std::path::PathBuf;

mod lifecycle;
mod serve;

struct Args {
    verb: String,
    data_dir: Option<PathBuf>,
    socket: Option<PathBuf>,
    foreground: bool,
    follow: bool,
    lines: usize,
    rest: Vec<String>,
}

fn usage() -> ! {
    eprintln!(
        "\
borg-server — hosts a directory of borg registries

  borg-server start [--foreground]      serve the data directory
  borg-server stop                      stop the server serving it
  borg-server status                    where it is listening and what it hosts
  borg-server logs [-n <count>] [-f]    what a backgrounded server has printed
  borg-server create <name>             create a registry in the data directory

A server hosts a **data directory of registries**: every store under --data-dir, addressable by
name. The registry is the unit of tenancy — one advisory lock per store, one held registry per
registry anything has actually used, and one socket for all of them, because the client's handshake
says which registry it is for. A handshake that names none gets the sole registry when there is
exactly one, and is told the options when there is more than one.

`start` backgrounds by default and writes a pidfile and a log beside the registries. Use
--foreground under a supervisor — systemd, docker, or a scenario — where staying in the foreground
and logging to stdout is the whole contract and daemonizing is the wrong thing.

The socket defaults to $XDG_RUNTIME_DIR/borg.sock when that directory exists and to
<data-dir>/borg.sock when it does not — which for the default data directory is ~/.borg/borg.sock.
One well-known address for the whole server; --socket overrides it.

While a store is served, every `borg` invocation against it is refused and told the socket to speak
to, because one process serves a store: the sidecars beside a store and its layer sequencer are not
multi-process safe. Pushing a schema is the exception that used to prove the rule and no longer
does — `repo_push` is on the protocol, so the *server* runs the push against a path on its own disk
and the registry it is holding open sees the new definitions and the new code without a restart.

Options:
  --data-dir <path>     the directory of registries to host (default ~/.borg)
  --socket <path>       where to listen (default: see above)
  --foreground          do not background; log to stdout
  -n, --lines <count>   `logs`: how many lines to show (default 50)
  -f, --follow          `logs`: keep printing as more arrives"
    );
    std::process::exit(2)
}

fn parse_args() -> Args {
    let mut args = Args {
        verb: String::new(),
        data_dir: None,
        socket: None,
        foreground: false,
        follow: false,
        lines: 50,
        rest: Vec::new(),
    };
    let mut raw = std::env::args().skip(1);
    while let Some(arg) = raw.next() {
        match arg.as_str() {
            "--data-dir" => args.data_dir = raw.next().map(PathBuf::from),
            "--socket" => args.socket = raw.next().map(PathBuf::from),
            "--foreground" => args.foreground = true,
            "-f" | "--follow" => args.follow = true,
            "-n" | "--lines" => {
                args.lines = raw
                    .next()
                    .and_then(|n| n.parse().ok())
                    .unwrap_or_else(|| usage());
            }
            "-h" | "--help" => usage(),
            _ if args.verb.is_empty() => args.verb = arg,
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

/// The defaults a request varies from. The `store` is a placeholder: every operation works through
/// the store its registry names, and this carries only the flags a message may override.
fn base(data_dir: &std::path::Path) -> Ops {
    Ops {
        store: data_dir.join(host::STORE_FILE),
        branch: None,
        version: None,
        freshness: FreshnessRequirement::Validated,
        settled: false,
        held: None,
    }
}

async fn run(args: Args) -> Result<()> {
    let (data_dir, socket) = lifecycle::addresses(args.data_dir.clone(), args.socket.clone());
    match args.verb.as_str() {
        "start" if args.foreground => {
            lifecycle::foreground(&data_dir, &socket, &base(&data_dir)).await
        }
        "start" => {
            lifecycle::background(&data_dir, &socket)?;
            report_status(&lifecycle::status(&data_dir, &socket));
            Ok(())
        }
        "stop" => {
            lifecycle::stop(&data_dir, &socket)?;
            println!("stopped the server on {}", socket.display());
            Ok(())
        }
        "status" => {
            let status = lifecycle::status(&data_dir, &socket);
            let running = status.registries.is_some();
            report_status(&status);
            if running {
                return Ok(());
            }
            // Non-zero, so `borg-server status && ...` does the obvious thing. The sentence above
            // already said how to start one.
            std::process::exit(1)
        }
        "logs" => lifecycle::logs(&data_dir, args.lines, args.follow),
        "create" => {
            let Some(name) = args.rest.first() else {
                usage()
            };
            let through_server =
                lifecycle::create(&data_dir, &socket, name, &base(&data_dir)).await?;
            println!(
                "created registry {name} in {}{}",
                data_dir.display(),
                if through_server {
                    format!(" — hosted now on {}", socket.display())
                } else {
                    String::new()
                }
            );
            Ok(())
        }
        _ => usage(),
    }
}

/// What `status` prints, and what `start` prints once the server is answering.
///
/// The address first, because it is what a client needs; the registries after, because that is what
/// a person came to check. **`open` is reported** rather than hidden: a registry nobody has used has
/// not had its log replayed, and a server that presented everything as warm would be claiming a boot
/// cost it deliberately does not pay.
fn report_status(status: &lifecycle::Status) {
    let Some(registries) = &status.registries else {
        println!(
            "borg-server is not running: nothing is answering on {}",
            status.socket.display()
        );
        println!(
            "start one with `borg-server start --data-dir {}`",
            status.data_dir.display()
        );
        return;
    };
    let pid = status
        .pid
        .map_or_else(String::new, |pid| format!(" (pid {pid})"));
    println!("borg-server running on {}{pid}", status.socket.display());
    println!("data dir {}", status.data_dir.display());
    if registries.is_empty() {
        println!(
            "no registries yet — create one with `borg-server create <name> --data-dir {}`",
            status.data_dir.display()
        );
        return;
    }
    println!("registries:");
    for registry in registries {
        println!("  {}", named(registry));
    }
}

fn named(registry: &RegistryInfo) -> String {
    format!(
        "{:<20} {}",
        registry.name,
        if registry.open { "open" } else { "not opened" }
    )
}
