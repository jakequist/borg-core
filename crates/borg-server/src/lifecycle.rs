//! `start`, `stop`, `status`, `logs` — being a process somebody has to operate.
//!
//! A server that only knew how to run in the foreground would make every user invent this, and they
//! would each invent it differently. So the four verbs are here, and each one's shape was decided
//! against the two things that actually run servers: a person at a terminal, and a supervisor.
//!
//! ## `start` backgrounds by default, and `--foreground` is for the supervisor
//!
//! The default is what a person wants: run `borg-server start`, get the prompt back, and have the
//! thing be up. That means a pidfile and a log file, because a backgrounded process with neither is
//! one you cannot stop or debug.
//!
//! `--foreground` is the opposite and is what systemd, docker and a scenario's `&` all want: stay in
//! the foreground, log to stdout, daemonize nothing. A supervisor's whole job is to be the parent
//! process, and a server that forked away from it would have to be tracked by pidfile — which is the
//! thing supervisors were built to stop doing.
//!
//! **Backgrounding is a re-exec of this binary with `--foreground`, not a fork.** `fork` without
//! `exec` is a minefield in a process that has a tokio runtime, and this way the backgrounded
//! process is an ordinary `borg-server start --foreground` — the same code path a supervisor runs,
//! so there is not a second lifecycle that only exists in the background case. The child is put in a
//! **process group of its own** so that a `^C` in the shell that started it does not reach it; that
//! is `CommandExt::process_group`, which is safe, rather than `setsid`, which would need `libc` and
//! an `unsafe` block this workspace forbids.
//!
//! ## `start` waits for the server to answer, and does not merely spawn it
//!
//! A socket file exists a moment before anything is listening on it, so a `start` that returned as
//! soon as it had a pid would make every caller write the wait loop. It waits for a connection to be
//! accepted, which is the same liveness test the advisory lock uses (`borg_host::serving`), and
//! reports the log when the child dies instead.
//!
//! ## Everything that fails says how to start one
//!
//! `stop`, `status` and `logs` against nothing are the commonest confusion there is, and *"no
//! server is running"* on its own sends somebody to read `--help`. Each of them names the data
//! directory it looked in and the command that would start one.

use borg_core::{BorgError, Result};
use borg_host::host::{self, Host, LOG_FILE, PID_FILE};
use borg_host::keys;
use borg_host::ops::Ops;
use borg_host::{serving, stream};
use borg_protocol::client::{Connect, Request, Response};
use borg_protocol::url::Address;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// How long `start` waits for a backgrounded server to answer, and `stop` for one to go away.
///
/// Generous, because what it covers is a cold sqlite open on a loaded machine, and nothing waits on
/// it in the healthy case: both loops return the moment what they are waiting for happens.
const PATIENCE: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(50);

/// **The credential this server's own CLI clients present.** SPEC.md §17.6, `borg_host::keys`.
///
/// `$BORG_TOKEN` first, then the token the running server minted into its data directory. That
/// order is the useful one: the environment variable is how a lifecycle command reaches a server it
/// is not on the same filesystem as, and the minted token is how the local case needs no
/// configuration at all.
///
/// `None` where there is neither, which is exactly right against an **open** server — it has no
/// keys file and authorises everybody, so there is nothing to present and nothing is presented.
fn admin(data_dir: &Path) -> Option<String> {
    std::env::var(keys::TOKEN_ENV)
        .ok()
        .filter(|token| !token.is_empty())
        .or_else(|| keys::read_admin(data_dir))
}

pub fn pid_path(data_dir: &Path) -> PathBuf {
    data_dir.join(PID_FILE)
}

pub fn log_path(data_dir: &Path) -> PathBuf {
    data_dir.join(LOG_FILE)
}

/// The sentence every failure ends with. One place, so that the flags in it cannot drift from the
/// flags the caller actually used.
fn how_to_start(data_dir: &Path) -> String {
    format!(
        "start one with `borg-server start --data-dir {}`",
        data_dir.display()
    )
}

/// Run the server in this process. What `--foreground` does, and what the backgrounded child is.
pub async fn foreground(
    data_dir: &Path,
    socket: &Path,
    base: &Ops,
    websockets: &[String],
) -> Result<()> {
    let host = Host::open(data_dir, socket)?;
    crate::serve::run(&host, base, websockets).await
}

/// Start a server in the background: re-exec, wait for it to answer, write the pidfile.
pub fn background(data_dir: &Path, socket: &Path, websockets: &[String]) -> Result<()> {
    if serving::is_listening(socket) {
        return Err(BorgError::Storage(format!(
            "something is already listening on {} — `borg-server status` will say what",
            socket.display()
        )));
    }
    std::fs::create_dir_all(data_dir)
        .map_err(|err| BorgError::Storage(format!("{}: {err}", data_dir.display())))?;

    let log = log_path(data_dir);
    // Appended rather than truncated: a restart that erased the reason the last one stopped would
    // erase it exactly when somebody is looking for it.
    let out = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log)
        .map_err(|err| BorgError::Storage(format!("{}: {err}", log.display())))?;
    let errs = out
        .try_clone()
        .map_err(|err| BorgError::Storage(format!("{}: {err}", log.display())))?;

    let exe = std::env::current_exe()
        .map_err(|err| BorgError::Storage(format!("cannot find borg-server: {err}")))?;
    let mut child = Command::new(exe);
    let child = child
        .arg("start")
        .arg("--foreground")
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--socket")
        .arg(socket);
    // Passed through, because the backgrounded server *is* an ordinary `--foreground` one and a
    // flag that survived only in the foreground case would be a second lifecycle.
    for address in websockets {
        child.arg("--listen").arg(address);
    }
    let mut child = child
        .stdin(Stdio::null())
        .stdout(out)
        .stderr(errs)
        // Its own process group, so a `^C` in this shell does not reach it. See the header.
        .process_group(0)
        .spawn()
        .map_err(|err| BorgError::Storage(format!("cannot start borg-server: {err}")))?;

    let deadline = Instant::now() + PATIENCE;
    loop {
        if serving::is_listening(socket) {
            break;
        }
        // The child died before it answered. Its log is the only place the reason is.
        if matches!(child.try_wait(), Ok(Some(_))) {
            return Err(BorgError::Storage(format!(
                "borg-server exited before it could serve — see {}",
                log.display()
            )));
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            return Err(BorgError::Storage(format!(
                "borg-server did not answer on {} within {}s — see {}",
                socket.display(),
                PATIENCE.as_secs(),
                log.display()
            )));
        }
        std::thread::sleep(POLL);
    }

    // Written after it answered, not before: a pidfile naming a process that never served is what
    // makes `stop` report success against nothing.
    std::fs::write(pid_path(data_dir), child.id().to_string())
        .map_err(|err| BorgError::Storage(format!("{}: {err}", pid_path(data_dir).display())))?;
    Ok(())
}

/// The pid a running server last wrote here, if the file names one.
pub fn running_pid(data_dir: &Path) -> Option<u32> {
    std::fs::read_to_string(pid_path(data_dir))
        .ok()
        .and_then(|raw| raw.trim().parse::<u32>().ok())
}

/// Stop a running server: `SIGTERM`, then wait for the process to be gone.
///
/// **`SIGTERM`, not a protocol message**, and the choice is worth recording. A `shutdown` request on
/// §17.5 would be a wire on which anyone who can connect can stop the server — which is exactly the
/// shape that must not exist on the day `credential` starts meaning something. A signal is the
/// operating system's own authorisation model and it is the one a supervisor already uses.
///
/// Sent by running `kill`, because sending a signal from Rust means `libc` and an `unsafe` block,
/// and this workspace forbids the second and takes the first as a decision. `kill` is POSIX and
/// costs one process on a path a person runs by hand.
pub fn stop(data_dir: &Path, socket: &Path) -> Result<()> {
    let Some(pid) = running_pid(data_dir) else {
        return Err(BorgError::Storage(format!(
            "no borg-server pidfile in {} — {}",
            data_dir.display(),
            how_to_start(data_dir)
        )));
    };
    if !serving::is_listening(socket) {
        // The pidfile outlived its process. Cleared rather than reported, for the same reason a
        // stale lock is cleared: a record nobody holds is not information.
        let _ = std::fs::remove_file(pid_path(data_dir));
        return Err(BorgError::Storage(format!(
            "no server is answering on {} (a stale pidfile named {pid}, and has been removed) — {}",
            socket.display(),
            how_to_start(data_dir)
        )));
    }

    let sent = Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|err| BorgError::Storage(format!("cannot signal {pid}: {err}")))?;
    if !sent.success() {
        return Err(BorgError::Storage(format!(
            "could not signal borg-server (pid {pid})"
        )));
    }

    // **Waits for the process, not for the socket**, and the difference is a race somebody has to
    // lose. A server stops accepting the moment its listener is dropped, and *then* stops its worker
    // subprocesses and releases its locks; a `stop` that returned when the socket went quiet would
    // hand control back with the advisory locks still held, and the next command would be refused by
    // a server that is on its way out. Gone means gone.
    let deadline = Instant::now() + PATIENCE;
    while is_alive(pid) {
        if Instant::now() >= deadline {
            return Err(BorgError::Storage(format!(
                "borg-server (pid {pid}) is still running after {}s",
                PATIENCE.as_secs()
            )));
        }
        std::thread::sleep(POLL);
    }
    let _ = std::fs::remove_file(pid_path(data_dir));
    Ok(())
}

/// Whether a process exists. `kill -0`, which signals nothing and answers exactly this.
fn is_alive(pid: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

/// What a `status` found. Rendered by the caller, like every other operation in this system.
pub struct Status {
    pub data_dir: PathBuf,
    pub socket: PathBuf,
    pub pid: Option<u32>,
    /// `None` when nothing is answering.
    pub registries: Option<Vec<borg_protocol::client::RegistryInfo>>,
    /// **Which mode the server is in**: `open` or `required` (§17.6).
    ///
    /// From the handshake's acknowledgement rather than from the keys file on disk, and for the
    /// same reason `registries` comes over the socket: it is a fact about the *server*, and a
    /// directory read would answer about a directory — which is not the same answer once the server
    /// is on another machine, or once somebody has edited the file since it started.
    ///
    /// `None` when nothing answered, and `None` when the server refused the handshake — see
    /// [`Status::refused`], which is the case that must not be reported as *not running*.
    pub auth: Option<String>,
    /// **What a server said when it would not talk to us.** `None` when it did.
    ///
    /// A server that refuses this command's credential is emphatically *running*, and reporting it
    /// as stopped would send an operator to start a second one on a socket that is already busy. So
    /// the refusal is carried rather than collapsed into the absence of an answer.
    pub refused: Option<String>,
    /// How many keys are in the file this process can see, when it can see one. Local, and labelled
    /// as such by [`Status::auth`] being the thing that says the *mode* — a count is what an
    /// operator wants next ("did the revoke land?") and it is not on the wire, because a key count
    /// is one more thing an unauthenticated caller would be able to ask for.
    pub keys: Option<usize>,
}

/// Ask the server what it is. **Through the socket**, because which registries a server hosts and
/// which it has opened are facts about the server rather than about the directory — one read from
/// the filesystem would answer about a directory, and would answer nothing at all once the server
/// being asked about is on another machine. `borg_protocol::client::ask` is the thirty lines that
/// takes, shared with the CLI's two socket commands so there is one handshake and not three.
pub fn status(data_dir: &Path, socket: &Path) -> Status {
    let token = admin(data_dir);
    let answered = borg_protocol::client::greet(
        &Address::Unix(socket.into()),
        &Connect::to(None, token.as_deref()),
        &Request::Registries {},
    );
    // Three outcomes, and the middle one is the one worth having a field for: an answer, a server
    // that would not answer *this caller*, and nothing listening at all.
    let (auth, registries, refused) = match answered {
        Ok((accepted, Response::Registries(hosted))) => (Some(accepted.auth), Some(hosted), None),
        Ok((accepted, Response::Error { message })) => (Some(accepted.auth), None, Some(message)),
        Ok((accepted, other)) => (
            Some(accepted.auth),
            None,
            Some(format!("unexpected answer to registries: {other:?}")),
        ),
        // **`Unreachable` is the only "no server"**, and it is `borg_protocol::url::unreachable`'s
        // own classification (§17.7). Anything else — a refused credential, a protocol version —
        // came *from* a server, which is the distinction that keeps `status` from lying.
        Err(BorgError::Unreachable(_)) => (None, None, None),
        Err(err) => (None, None, Some(err.to_string())),
    };
    Status {
        data_dir: data_dir.to_path_buf(),
        socket: socket.to_path_buf(),
        pid: running_pid(data_dir),
        registries,
        auth,
        refused,
        keys: keys::load(data_dir)
            .ok()
            .flatten()
            .map(|file| file.keys.len()),
    }
}

/// Make a registry — through the server when one is running, directly when one is not.
///
/// **Both, and the pair is the point.** A directory appearing under a *running* server's data dir is
/// a store it has not locked, is not hosting and will not route to, so while a server is up the
/// creation has to go through it. But a data directory has to be fillable before there is a server
/// to fill it, or `borg-server start` on a fresh machine would host nothing and there would be no
/// way to give it anything.
pub async fn create(data_dir: &Path, socket: &Path, name: &str, base: &Ops) -> Result<bool> {
    if serving::is_listening(socket) {
        let token = admin(data_dir);
        return match borg_protocol::client::ask(
            &Address::Unix(socket.into()),
            &Connect::to(None, token.as_deref()),
            &Request::RegistryCreate {
                name: name.to_string(),
            },
        )? {
            Response::Ok {} => Ok(true),
            Response::Error { message } => Err(BorgError::Storage(message)),
            other => Err(BorgError::Storage(format!(
                "unexpected answer to registry_create: {other:?}"
            ))),
        };
    }
    let host = Host::open(data_dir, socket)?;
    host.create(name, base).await?;
    Ok(false)
}

/// What an export or a restore turned out to be, and whether a server did it.
///
/// One struct for both halves because the two commands print the same shape of sentence, and the
/// `served` flag is the one thing an operator needs to know that neither count tells them: an export
/// taken *through* a running server is a snapshot of what that server is holding, and one taken
/// directly is a snapshot of a directory nobody is serving.
pub struct Moved {
    pub served: bool,
    pub summary: String,
}

/// Export a registry — through the server when one is running, directly when one is not.
///
/// The pair is the same one [`create`] draws, and here it is load-bearing rather than convenient: a
/// running server holds the advisory lock on every registry it hosts, so a second process reading
/// the store behind its back is exactly what the lock forbids. Through the socket, the export runs
/// under that registry's own gate and is a coherent snapshot for free (SPEC.md §19). With no server
/// up, this process is the only one there is and reads the store directly.
///
/// **`file` is a path on the server's machine** when a server does it. That is the same contract
/// `repo push --url` has, and it is stated rather than hidden: the alternative for a remote server is
/// carrying the bytes, which is a field on the message rather than a different shape.
pub async fn export(
    data_dir: &Path,
    socket: &Path,
    name: Option<&str>,
    file: &Path,
    base: &Ops,
) -> Result<Moved> {
    let path = absolute(file);
    if serving::is_listening(socket) {
        let request = Request::Export {
            registry: name.map(str::to_string),
            path: path.display().to_string(),
        };
        let token = admin(data_dir);
        return match borg_protocol::client::ask(
            &Address::Unix(socket.into()),
            &Connect::to(name, token.as_deref()),
            &request,
        )? {
            Response::Exported {
                path,
                layers,
                events,
                interned,
                head,
                settled,
            } => Ok(Moved {
                served: true,
                summary: exported(&path, layers, events, interned, &head, &settled),
            }),
            Response::Error { message } => Err(BorgError::Storage(message)),
            other => Err(BorgError::Storage(format!(
                "unexpected answer to export: {other:?}"
            ))),
        };
    }
    let host = Host::open(data_dir, socket)?;
    let slot = host.route(name)?;
    let handle = std::fs::File::create(&path)
        .map_err(|err| BorgError::Storage(format!("{}: {err}", path.display())))?;
    let mut out = std::io::BufWriter::new(handle);
    let report = stream::export(
        &Ops {
            store: slot.store.clone(),
            held: None,
            ..base.clone()
        },
        &mut out,
    )
    .await?;
    Ok(Moved {
        served: false,
        summary: exported(
            &path.display().to_string(),
            report.layers,
            report.events,
            report.interned,
            &report.head.to_string(),
            &report.settled.to_string(),
        ),
    })
}

/// Restore a registry from a stream — through the server when one is running, directly when not.
///
/// Creating and filling are **one** operation either way (`Host::restore`): a registry that existed
/// empty for a moment is a registry a client could have routed to and written into, and the write
/// would then be either refused or silently kept beside the restore.
pub async fn import(
    data_dir: &Path,
    socket: &Path,
    name: &str,
    file: &Path,
    base: &Ops,
) -> Result<Moved> {
    let path = absolute(file);
    if serving::is_listening(socket) {
        let request = Request::Import {
            name: name.to_string(),
            path: path.display().to_string(),
        };
        let token = admin(data_dir);
        return match borg_protocol::client::ask(
            &Address::Unix(socket.into()),
            &Connect::to(None, token.as_deref()),
            &request,
        )? {
            Response::Imported {
                name,
                layers,
                events,
                branches,
                head,
                written_by,
            } => Ok(Moved {
                served: true,
                summary: restored(&name, layers, events, branches, &head, &written_by),
            }),
            Response::Error { message } => Err(BorgError::Storage(message)),
            other => Err(BorgError::Storage(format!(
                "unexpected answer to import: {other:?}"
            ))),
        };
    }
    let host = Host::open(data_dir, socket)?;
    let (_, report) = host.restore(name, base, &path).await?;
    Ok(Moved {
        served: false,
        summary: restored(
            name,
            report.layers,
            report.events,
            report.branches,
            &report.head.to_string(),
            &report.written_by,
        ),
    })
}

/// The path a *server* will read or write, resolved here rather than there.
///
/// A relative path means something to the shell that typed it and nothing to a daemon whose working
/// directory is wherever it was started. Resolving it against this process's cwd is what makes
/// `borg-server export main backup.ndjson` write beside the operator rather than somewhere they
/// would have to go looking — and it is the honest half of the local-path contract: it works because
/// the server is on this machine, and the day it is not, the path has to be replaced by the bytes.
fn absolute(file: &Path) -> PathBuf {
    if file.is_absolute() {
        return file.to_path_buf();
    }
    std::env::current_dir().map_or_else(|_| file.to_path_buf(), |cwd| cwd.join(file))
}

fn exported(
    path: &str,
    layers: u64,
    events: u64,
    interned: u64,
    head: &str,
    settled: &str,
) -> String {
    let lag = if head == settled {
        String::new()
    } else {
        format!("; derived data is settled to {settled}, and the backlog is captured with it")
    };
    format!(
        "wrote {path}: {} layers, {} events, {} interned values — the log ends at {head}{lag}",
        layers, events, interned
    )
}

fn restored(
    name: &str,
    layers: u64,
    events: u64,
    branches: u64,
    head: &str,
    written_by: &str,
) -> String {
    format!(
        "restored registry {name}: {layers} layers, {events} events, {branches} branches, head \
         {head} — the stream was written by {written_by}"
    )
}

/// The server's log, or the sentence explaining why there is none.
///
/// `lines` is a tail because a log is read to find out what just happened; `follow` polls the file's
/// length, which is what a log written by an append-only process allows and is the whole of what
/// `tail -f` does to one.
pub fn logs(data_dir: &Path, lines: usize, follow: bool) -> Result<()> {
    let path = log_path(data_dir);
    let mut read = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(err) => {
            return Err(BorgError::Storage(format!(
                "{}: {err} — a server that has only ever run with --foreground logs to its own \
                 stdout and leaves nothing here; otherwise, {}",
                path.display(),
                how_to_start(data_dir)
            )));
        }
    };
    let tail: Vec<&str> = read.lines().rev().take(lines).collect();
    for line in tail.into_iter().rev() {
        println!("{line}");
    }
    if !follow {
        return Ok(());
    }
    let mut seen = read.len();
    loop {
        std::thread::sleep(Duration::from_millis(200));
        read = std::fs::read_to_string(&path).unwrap_or_default();
        // A file that shrank was rotated or replaced; start again from its beginning rather than
        // slicing at an offset that no longer means anything.
        if read.len() < seen {
            seen = 0;
        }
        // `get` rather than an index: `seen` is a byte offset into what the file used to hold, and
        // a file that was replaced rather than appended to could put it inside a character. Nothing
        // is worth panicking a `logs -f` over.
        if let Some(fresh) = read.get(seen..)
            && !fresh.is_empty()
        {
            print!("{fresh}");
            seen = read.len();
        }
    }
}

/// Where a command's data directory and socket are, given what the caller said.
pub fn addresses(data_dir: Option<PathBuf>, socket: Option<PathBuf>) -> (PathBuf, PathBuf) {
    let data_dir = data_dir.unwrap_or_else(host::default_data_dir);
    let socket = socket.unwrap_or_else(|| host::default_socket(&data_dir));
    (data_dir, socket)
}
