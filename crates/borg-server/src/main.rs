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
//! borg-server start [--foreground] [--listen ws://host:port]
//!                                      serve --data-dir; backgrounds unless told otherwise
//! borg-server stop                     SIGTERM, then wait for the socket to go quiet
//! borg-server status                   the address, the pid, and what is hosted
//! borg-server logs [-n N] [-f]         what a backgrounded server has printed
//! borg-server create <name>            make a registry, through the server if one is running
//! borg-server export [<name>] <file>   write a registry out as an event stream (§19)
//! borg-server import <name> <file>     restore one, creating the registry
//! borg-server keygen <label>           issue an api key; the first one flips the server to authed
//! borg-server keys [list|revoke <l>]   what is issued, and how to un-issue it
//! ```
//!
//! ## The key commands are filesystem commands, not protocol ones
//!
//! `keygen`, `keys list` and `keys revoke` write and read a file in the data directory and never
//! speak to the socket, which is deliberate and is what makes the bootstrap work at all: minting the
//! *first* credential over a connection that already requires one is a circle. It also means they
//! work against a server that is stopped, and that a running server picks up a revocation on its
//! next handshake without being restarted or told (`borg_host::keys`).
//!
//! The boundary they rely on is the filesystem's: whoever can write the data directory can issue
//! keys, and could already read every store under it.

use borg_core::{FreshnessRequirement, Result};
use borg_host::host;
use borg_host::keys;
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
    /// `--listen ws://host:port`, repeatable. Beside the unix socket, never instead of it.
    listen: Vec<String>,
    /// `--registries a,b` for `keygen`. Absent is every registry (§17.6).
    registries: Option<String>,
    follow: bool,
    lines: usize,
    rest: Vec<String>,
}

fn usage() -> ! {
    eprintln!(
        "\
borg-server — hosts a directory of borg registries

  borg-server start [--foreground] [--listen ws://host:port]
                                        serve the data directory
  borg-server stop                      stop the server serving it
  borg-server status                    where it is listening and what it hosts
  borg-server logs [-n <count>] [-f]    what a backgrounded server has printed
  borg-server create <name>             create a registry in the data directory
  borg-server export [<name>] <file>    write a registry out as a canonical event stream
  borg-server import <name> <file>      restore a stream, creating the registry it names
  borg-server keygen <label>            issue an api key and print it once
  borg-server keys [list]               the keys this data directory holds, by label
  borg-server keys revoke <label>       stop honouring one

A server hosts a **data directory of registries**: every store under --data-dir, addressable by
name. The registry is the unit of tenancy — one advisory lock per store, one held registry per
registry anything has actually used, and one socket for all of them, because the client's handshake
says which registry it is for. A handshake that names none gets the sole registry when there is
exactly one, and is told the options when there is more than one.

`--listen ws://0.0.0.0:7717` adds a WebSocket listener beside the unix socket — both at once, and
the unix socket is always there because it is what every local `borg` invocation speaks. A
WebSocket is what a browser can open and what rides an ordinary load balancer; that port also
answers `GET /health` with the server version and how many registries are hosted, which is the one
HTTP endpoint there is. TLS is **not** terminated here: put a proxy in front and forward plaintext
ws:// to this port. The server trusts no forwarded header — nothing in the protocol is a function
of the client's address, and authentication is a field in the handshake rather than a header.

**Authentication is off until the first `keygen`, and on from then on.** A data directory with no
keys file is an open server: anyone who can reach the socket reaches everything, which is what
makes `borg-server start` on a laptop a thing with no ceremony. `borg-server keygen ci` writes the
file, prints the key **once** and never again, and every handshake from that moment must present a
credential — over the unix socket exactly as over a websocket, because an exemption for the local
transport would make the two mean different things. A key is presented in the connection url's
userinfo, `borg://:<key>@localhost/<registry>`, or in $BORG_TOKEN.

`--registries a,b` scopes a key to those registries; the default is `*`. A scoped key cannot reach
the others and cannot see them either — `registries` and every refusal it can provoke are filtered
to its own scope, so a credential learns no name it did not already have.

`status`, `create`, `export` and `import` are clients of the server they administer, so a running
server mints a `*`-scoped token into <data-dir>/borg-server.admin (mode 0600) and removes it when
it stops. They present it automatically; $BORG_TOKEN overrides it, which is what reaching a server
on another machine needs.

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

**What borg guarantees across versions is the data, not the bytes.** On-disk formats may change
before 1.0; every release exports a registry as a canonical event stream and imports streams from
earlier releases, so an upgrade is export, upgrade, import — and the same one mechanism is backup,
restore, format migration and clone. `export` and `import` work through the server when one is
running, so a live registry is backed up without stopping it: the export runs under that registry's
own gate, which makes it a snapshot of the whole log at one instant. `<file>` is a path on the
*server's* machine, as `borg repo push --url`'s directory is; a relative one is resolved against the
shell that typed it. `import` creates the registry it names and refuses one that already exists,
because restore is create-then-import and merging two id spaces would rewrite the lineage the stream
exists to preserve.

Options:
  --data-dir <path>     the directory of registries to host (default ~/.borg)
  --socket <path>       where to listen (default: see above)
  --foreground          do not background; log to stdout
  --listen <ws url>     also listen for websockets here (repeatable); port 0 binds an
                        ephemeral one and the log names it
  --registries <a,b>    `keygen`: which registries the key may reach (default: all of them)
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
        listen: Vec::new(),
        registries: None,
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
            "--listen" => match raw.next() {
                Some(address) => args.listen.push(address),
                None => usage(),
            },
            "--registries" => args.registries = raw.next(),
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
            lifecycle::foreground(&data_dir, &socket, &base(&data_dir), &args.listen).await
        }
        "start" => {
            lifecycle::background(&data_dir, &socket, &args.listen)?;
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
            // **Running** means a server answered, not that it answered *this*: one that refused
            // our credential is emphatically up, and exiting non-zero would tell a script to start
            // a second one on a socket that is already busy.
            let running = status.registries.is_some() || status.refused.is_some();
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
        // **The registry name is optional and the file is not.** A one-registry server is the local
        // case and naming its sole registry adds nothing, so `borg-server export backup.ndjson`
        // works exactly where `borg-server status` needs no name either — and the n≥2 refusal is
        // `Host::route`'s one sentence rather than a second opinion here (§17.6).
        "export" => {
            let (name, file) = match args.rest.as_slice() {
                [file] => (None, file),
                [name, file] => (Some(name.as_str()), file),
                _ => usage(),
            };
            let moved = lifecycle::export(
                &data_dir,
                &socket,
                name,
                std::path::Path::new(file),
                &base(&data_dir),
            )
            .await?;
            report_moved(&moved, &socket);
            Ok(())
        }
        "import" => {
            let [name, file] = args.rest.as_slice() else {
                usage()
            };
            let moved = lifecycle::import(
                &data_dir,
                &socket,
                name,
                std::path::Path::new(file),
                &base(&data_dir),
            )
            .await?;
            report_moved(&moved, &socket);
            Ok(())
        }
        // **The key commands write a file and never speak to the socket** — see the header for why
        // that is what makes issuing the first credential possible at all.
        "keygen" => {
            let Some(label) = args.rest.first() else {
                usage()
            };
            let scope = match args.registries.as_deref() {
                None => keys::Scope::all(),
                Some(list) => keys::Scope::Only(
                    list.split(',')
                        .map(str::trim)
                        .filter(|name| !name.is_empty())
                        .map(str::to_string)
                        .collect(),
                ),
            };
            let first = keys::load(&data_dir)?.is_none();
            let key = keys::issue(&data_dir, label, scope)?;
            report_key(&data_dir, label, &key, first);
            Ok(())
        }
        "keys" => match args
            .rest
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .as_slice()
        {
            [] | ["list"] => {
                report_keys(&data_dir);
                Ok(())
            }
            ["revoke", label] => {
                keys::revoke(&data_dir, label)?;
                println!("revoked `{label}`");
                println!(
                    "connections opened with it are not torn down; every new handshake is refused \
                     from now on"
                );
                Ok(())
            }
            _ => usage(),
        },
        _ => usage(),
    }
}

/// **The one time a key exists in plaintext.** SPEC.md §17.6.
///
/// Printed to stdout so it can be piped into a secret store, with everything else on stderr — a
/// `borg-server keygen ci | doppler secrets set BORG_TOKEN` should carry the key and not the
/// commentary. Nothing here ever prints it again, because nothing anywhere has it again: the file
/// holds a digest.
fn report_key(data_dir: &std::path::Path, label: &str, key: &str, first: bool) {
    println!("{key}");
    eprintln!("issued `{label}` — this is the only time the key is shown; store it now");
    eprintln!(
        "present it as borg://:<key>@localhost/<registry>, or in ${}",
        keys::TOKEN_ENV
    );
    if first {
        eprintln!(
            "this server now requires a credential on every handshake, over every transport — \
             {} is what says so",
            keys::keys_path(data_dir).display()
        );
    }
}

/// What `keys list` prints. **Labels, scopes and ages — never a key and never a digest**: a digest
/// is not a secret but it is not information either, and a list somebody can paste into an issue is
/// worth more than one they have to redact first.
fn report_keys(data_dir: &std::path::Path) {
    match keys::load(data_dir) {
        Err(err) => eprintln!("error: {err}"),
        Ok(None) => {
            println!(
                "no keys — this server is open, and the first `borg-server keygen <label>` is what \
                 changes that"
            );
        }
        Ok(Some(file)) if file.keys.is_empty() => {
            println!(
                "no keys, and a keys file — every handshake is refused until one is issued; \
                 delete {} to reopen the server",
                keys::keys_path(data_dir).display()
            );
        }
        Ok(Some(file)) => {
            println!("{:<20} {:<24} issued", "label", "registries");
            for key in &file.keys {
                println!(
                    "{:<20} {:<24} {}",
                    key.label,
                    key.registries.written(),
                    keys::ago(key.created)
                );
            }
        }
    }
}

/// What an export or a restore did, and **whether a server did it**.
///
/// The second half is not decoration: an export taken through a running server is a snapshot of what
/// that server is holding, and one taken directly is a snapshot of a directory nobody is serving.
/// They are the same bytes when nothing is writing and different questions when something is, so the
/// sentence says which one was asked.
fn report_moved(moved: &lifecycle::Moved, socket: &std::path::Path) {
    println!("{}", moved.summary);
    if moved.served {
        println!("through the server on {}", socket.display());
    }
}

/// What `status` prints, and what `start` prints once the server is answering.
///
/// The address first, because it is what a client needs; the registries after, because that is what
/// a person came to check. **`open` is reported** rather than hidden: a registry nobody has used has
/// not had its log replayed, and a server that presented everything as warm would be claiming a boot
/// cost it deliberately does not pay.
fn report_status(status: &lifecycle::Status) {
    if status.registries.is_none() && status.refused.is_none() {
        println!(
            "borg-server is not running: nothing is answering on {}",
            status.socket.display()
        );
        println!(
            "start one with `borg-server start --data-dir {}`",
            status.data_dir.display()
        );
        return;
    }
    let pid = status
        .pid
        .map_or_else(String::new, |pid| format!(" (pid {pid})"));
    println!("borg-server running on {}{pid}", status.socket.display());
    println!("data dir {}", status.data_dir.display());
    println!("{}", authentication(status));
    // A server that would not talk to *us*. It is running, and the sentence it gave is the whole of
    // what there is to act on.
    let Some(registries) = &status.registries else {
        if let Some(refused) = &status.refused {
            println!("but it would not answer: {refused}");
        }
        return;
    };
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

/// **Open or authed, at a glance.** SPEC.md §17.6.
///
/// The mode comes from the handshake, because it is a fact about the server; the key count comes
/// from the file, because it is a fact about this directory and is deliberately not on the wire —
/// an unauthenticated caller asking how many keys exist is one question too many.
fn authentication(status: &lifecycle::Status) -> String {
    let counted = match status.keys {
        Some(1) => " (1 key issued)".to_string(),
        Some(n) => format!(" ({n} keys issued)"),
        None => String::new(),
    };
    match status.auth.as_deref() {
        Some("open") => format!(
            "auth   open — anyone who can reach {} reaches every registry; `borg-server keygen \
             <label>` changes that",
            status.socket.display()
        ),
        Some("required") => format!("auth   api key required on every handshake{counted}"),
        Some(other) => format!("auth   {other}{counted}"),
        // Refused at the handshake, so there was no acknowledgement to read a mode out of — which
        // is itself the answer: a server that refuses a credential is one that requires one.
        None => format!("auth   api key required on every handshake{counted}"),
    }
}

fn named(registry: &RegistryInfo) -> String {
    format!(
        "{:<20} {}",
        registry.name,
        if registry.open { "open" } else { "not opened" }
    )
}
