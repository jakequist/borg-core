//! The advisory lock: **one process serves a store.** SPEC.md §17.5.
//!
//! The sidecars and `InProcessSequencer` are not multi-process safe; they were not before a server
//! existed either, but a server makes the second process likely rather than hypothetical. So a
//! server records itself beside every store it hosts, and every other `borg` invocation against one
//! of them is refused *by name* ([`refuse_if_served`]).
//!
//! **The socket is the lock, and the pid is only for the message.** A server that crashed leaves the
//! record behind, and a lock that outlives its holder is worse than no lock — but a socket whose
//! listener is gone refuses connections, so whoever notices can clear it and carry on. That also
//! makes the check say something useful when the lock *is* live: the answer to "why was I refused"
//! is the address of the thing that refused you.
//!
//! **The record names the registry as well as the socket**, which is what a data directory of
//! registries added. One server, one socket, many stores: the handshake routes (§17.6), so a client
//! reaching a store through its lock record needs to know what that store is *called* on the other
//! end. `borg generate` is the first caller of that and will not be the last.

use crate::sidecar::{self, Sidecar};
use borg_core::{BorgError, Result};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

/// The record that a store is being served, beside the store with the other operational state.
#[derive(Default, serde::Serialize, serde::Deserialize)]
pub struct Serving {
    pub socket: String,
    /// Reported in the refusal so a human can find the process. Never used to decide anything — see
    /// the module header.
    pub pid: u32,
    /// What this store is called on that socket (§17.6). Absent in a record written before a server
    /// hosted more than one store, which then means "whatever the sole registry is" — the same
    /// default the handshake takes.
    #[serde(default)]
    pub registry: Option<String>,
}

impl Sidecar for Serving {
    const EXTENSION: &'static str = "serving.json";
}

/// Whether a server is listening on this socket right now.
///
/// Connect-and-drop. It is one accept the server will read nothing on, which it handles the same way
/// it handles a client that hangs up.
#[must_use]
pub fn is_listening(socket: &Path) -> bool {
    UnixStream::connect(socket).is_ok()
}

/// Where a live server is answering for this store, and what it calls it.
///
/// A stale record — one whose socket does not answer — is cleared here rather than reported, because
/// a lock nobody holds is not information.
///
/// **Two callers, and they do opposite things with the answer**, which is the point of it being a
/// question rather than a refusal. Every ordinary command turns it into [`refuse_if_served`]; `borg
/// generate` turns it into a *connection*, because it only reads and the socket is the one way to
/// read a served store. That is SDK-DRAFT §2.6's remote-connection future arriving for exactly one
/// read-only command, and deliberately not for the write path.
pub fn served_on(store: &Path) -> Option<Served> {
    let record: Serving = sidecar::load(store);
    if record.socket.is_empty() {
        return None;
    }
    let socket = PathBuf::from(&record.socket);
    if !is_listening(&socket) {
        let _ = std::fs::remove_file(sidecar::path::<Serving>(store));
        return None;
    }
    Some(Served {
        socket,
        pid: record.pid,
        registry: record.registry,
    })
}

/// A live server, as the store's own lock record describes it.
pub struct Served {
    pub socket: PathBuf,
    pub pid: u32,
    /// What the server calls this store. `None` for a record that predates named registries.
    pub registry: Option<String>,
}

/// Claim a store for this process. Returns whether the record was written.
pub fn claim(store: &Path, socket: &Path, registry: &str) -> Result<()> {
    sidecar::save(
        store,
        &Serving {
            socket: socket.display().to_string(),
            pid: std::process::id(),
            registry: Some(registry.to_string()),
        },
    )
}

/// Give a store back. Best-effort: a `kill -9` leaves the record behind and the next process to
/// look clears it, which is the whole reason liveness is a connect and not a file's existence.
pub fn release(store: &Path) {
    let _ = std::fs::remove_file(sidecar::path::<Serving>(store));
}

/// Refuse to touch a store somebody is serving, and say where to find them.
///
/// Called by every `borg` command except `generate`, which connects instead. See [`served_on`].
pub fn refuse_if_served(store: &Path) -> Result<()> {
    let Some(served) = served_on(store) else {
        return Ok(());
    };
    let named = served
        .registry
        .as_deref()
        .map_or_else(String::new, |name| format!(" as `{name}`"));
    Err(BorgError::Storage(format!(
        "{} is being served on {} (pid {}){named} — one process serves a store, so this command \
         would be the second writer. Speak to the socket, or stop the server with `borg-server \
         stop`.",
        store.display(),
        served.socket.display(),
        served.pid,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "borg-serving-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// The lock is the socket. A record left behind by a dead server must not lock a store out
    /// forever — see the module header.
    #[test]
    fn a_served_store_refuses_others_and_a_dead_one_does_not() {
        let dir = temp_dir("lock");
        let store = dir.join("borg.db");
        let socket = dir.join("borg.sock");

        // Nothing is serving: no record, no refusal.
        refuse_if_served(&store).unwrap();

        // A record whose socket answers.
        let listener = UnixListener::bind(&socket).unwrap();
        claim(&store, &socket, "main").unwrap();
        let refusal = refuse_if_served(&store).unwrap_err().to_string();
        assert!(
            refusal.contains(&socket.display().to_string()),
            "the refusal must name the socket, got: {refusal}"
        );
        assert!(
            refusal.contains("`main`"),
            "…and what the server calls this store, got: {refusal}"
        );

        // The server dies. The record is stale, and the next command clears it rather than being
        // locked out by a process that no longer exists.
        drop(listener);
        std::fs::remove_file(&socket).unwrap();
        refuse_if_served(&store).unwrap();
        assert!(!sidecar::path::<Serving>(&store).exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A client reaching a served store through its lock record needs the name the server routes on
    /// — one socket, many registries (§17.6).
    #[test]
    fn the_lock_record_names_the_registry_the_server_calls_this_store() {
        let dir = temp_dir("named");
        let store = dir.join("borg.db");
        let socket = dir.join("borg.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        claim(&store, &socket, "analytics").unwrap();

        let served = served_on(&store).expect("the socket answers, so the record is live");
        assert_eq!(served.registry.as_deref(), Some("analytics"));
        assert_eq!(served.socket, socket);

        drop(listener);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
