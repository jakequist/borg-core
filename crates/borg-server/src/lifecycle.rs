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
use borg_host::ops::Ops;
use borg_host::serving;
use borg_protocol::client::{Request, Response};
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
}

/// Ask the server what it is. **Through the socket**, because which registries a server hosts and
/// which it has opened are facts about the server rather than about the directory — one read from
/// the filesystem would answer about a directory, and would answer nothing at all once the server
/// being asked about is on another machine. `borg_protocol::client::ask` is the thirty lines that
/// takes, shared with the CLI's two socket commands so there is one handshake and not three.
pub fn status(data_dir: &Path, socket: &Path) -> Status {
    let registries = match borg_protocol::client::ask(
        &Address::Unix(socket.into()),
        None,
        &Request::Registries {},
    ) {
        Ok(Response::Registries(hosted)) => Some(hosted),
        _ => None,
    };
    Status {
        data_dir: data_dir.to_path_buf(),
        socket: socket.to_path_buf(),
        pid: running_pid(data_dir),
        registries,
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
        return match borg_protocol::client::ask(
            &Address::Unix(socket.into()),
            None,
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
