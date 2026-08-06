//! A **data directory of registries**, which is what a server hosts. SPEC.md §17.6.
//!
//! `borg-server start --data-dir ~/.borg` hosts every store under it, addressable by name. One
//! process, one socket, one advisory lock per store, and one held `Registry` per registry that is
//! actually used.
//!
//! ## The registry is the unit of tenancy
//!
//! Not the connection, not the branch, and not the process. A branch is a fork of one history and
//! shares its definitions, its transaction table and its PID counter with the branch it forked from;
//! two applications that must not see each other's schema need two *registries*. Everything a
//! registry owns is already registry-shaped — the log, the sidecars, the advisory lock — so making
//! it the tenant costs nothing new and makes the multi-tenant case the same code as the single one.
//!
//! It is also the seam this file exists to hold still. Today a registry is a directory under a data
//! dir on one machine; on the platform it is a tenant, and what routes to it is a name in a
//! handshake either way. Nothing above [`Host::route`] knows which of those it is talking to.
//!
//! ## Lazily opened, eagerly locked, and the two are not the same claim
//!
//! **Opening** a registry brings its projections to head, which for a fresh set means replaying its
//! log (`borg_engine::projection`). A server that opened every hosted store at boot would pay every
//! registry's replay to answer a request about one of them, and a data dir is exactly the shape that
//! grows registries nobody has touched this week. So a registry opens on first use and stays open.
//!
//! **Locking** is the opposite: it happens for every hosted registry at boot, before the socket is
//! announced. The lock is a file naming the socket (`crate::serving`) and costs a `write`, and the
//! alternative is a window in which `borg set` may walk into a store this server is about to hold —
//! which is the multi-process case the lock exists to prevent. Cheap to take, expensive to be
//! without: they are not symmetric and are not done at the same time.
//!
//! ## One gate per registry, not one per server
//!
//! The old `borg serve` held one gate for the whole process, and store-wide was exactly right for
//! the thing it protects:
//! the transaction table, the pause flags and the PID counter are read-modify-write on files beside
//! **a store**, and the sequencer is per store too. Two clients on two registries share none of
//! that, so serialising them would be a limit nothing asks for — and would make the second registry
//! wait behind the first's derivation. What is *not* changed is the gate itself: within one
//! registry, requests are still answered one at a time, which is the serialisation
//! process-per-command gave the CLI for free (`ROADMAP.md`).

use crate::ops::{self, Held, Ops};
use crate::serving;
use borg_core::{BorgError, Result};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

/// The store file inside a registry's directory. Fixed rather than configurable: a registry is
/// addressed by name and the name is the directory, so a second knob would only let two data dirs
/// disagree about what a registry is.
pub const STORE_FILE: &str = "borg.db";
/// Where the server writes its pid, in the data directory beside the registries.
pub const PID_FILE: &str = "borg-server.pid";
/// Where a backgrounded server's stdout and stderr go.
pub const LOG_FILE: &str = "borg-server.log";
/// The socket's name, wherever it is put. See [`default_socket`].
pub const SOCKET_FILE: &str = "borg.sock";

/// The data directory a server hosts when nothing says otherwise.
///
/// `$HOME/.borg`, and `./.borg` for the homeless — a server that refused to start because `$HOME`
/// was unset would be a server that cannot run in a container, which is precisely where it will.
#[must_use]
pub fn default_data_dir() -> PathBuf {
    std::env::var_os("HOME").map_or_else(
        || PathBuf::from(".borg"),
        |home| Path::new(&home).join(".borg"),
    )
}

/// The well-known address, and the rule is worth stating because clients hard-code it.
///
/// **`$XDG_RUNTIME_DIR/borg.sock` if that directory exists, otherwise `<data-dir>/borg.sock`** —
/// which with the default data dir is `~/.borg/borg.sock`. The runtime dir is where a per-user
/// socket belongs: it is user-private, on tmpfs, and cleaned up at logout, so a crashed server
/// leaves nothing behind across a reboot. The fallback is beside the data it serves, which is the
/// only other place a client can find without being told.
///
/// Two servers on two data dirs with one `$XDG_RUNTIME_DIR` therefore want the same address, and the
/// second is refused because something is already listening. That is the right failure — one
/// well-known address is the point — and `--socket` is how you say otherwise.
#[must_use]
pub fn default_socket(data_dir: &Path) -> PathBuf {
    match std::env::var_os("XDG_RUNTIME_DIR") {
        Some(dir) if Path::new(&dir).is_dir() => Path::new(&dir).join(SOCKET_FILE),
        _ => data_dir.join(SOCKET_FILE),
    }
}

/// **The address `borg://localhost` resolves to.** SPEC.md §17.7.
///
/// [`default_socket`] over [`default_data_dir`], named because a client's URL says *the local
/// transport* rather than a path, and something has to be the one place that turns the first into
/// the second. `borg_protocol::url` deliberately does not: it sits below this crate, and a second
/// copy of the `$XDG_RUNTIME_DIR`-or-data-dir rule is exactly the drift CLAUDE.md forbids.
#[must_use]
pub fn well_known_socket() -> PathBuf {
    default_socket(&default_data_dir())
}

/// Whether a name may be a registry.
///
/// Letters, digits, `-` and `_`. Deliberately narrow: the name is a directory under the data dir, a
/// word in a handshake and a word in an error message, so anything that could be a path traversal, a
/// shell surprise or an invisible duplicate is refused at the one door that creates them. Excluding
/// `.` is also what keeps the server's own files (`borg-server.pid`, `borg.sock`) out of the
/// namespace by construction rather than by a reserved list somebody has to maintain.
fn name_is_valid(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

fn check_name(name: &str) -> Result<()> {
    if name_is_valid(name) {
        return Ok(());
    }
    Err(BorgError::Storage(format!(
        "`{name}` is not a registry name — letters, digits, `-` and `_`"
    )))
}

/// One hosted registry: where its store is, the gate that serialises operations on it, and the
/// `Registry` once something has needed one.
#[derive(Debug)]
pub struct Slot {
    pub name: String,
    pub store: PathBuf,
    /// Held for a whole operation. See the module header for why it is per registry.
    gate: Mutex<()>,
    /// `None` until first use. Read and written only under [`Slot::gate`].
    held: Mutex<Option<Arc<Held>>>,
}

impl Slot {
    /// Take this registry for the duration of one operation.
    pub fn enter(&self) -> MutexGuard<'_, ()> {
        self.gate.lock().unwrap_or_else(|err| err.into_inner())
    }

    /// The `Ops` an operation on this registry works through, **opening the store if this is the
    /// first time anything has asked**.
    ///
    /// Must be called with [`Slot::enter`]'s guard held; the guard is not a parameter because a
    /// `MutexGuard` living across an `await` is a lint and a trap, and the caller is holding it
    /// across a `block_on` rather than an `await`. What the gate buys here is that two requests
    /// cannot both find the slot empty and both replay the log.
    pub async fn ops(&self, base: &Ops) -> Result<Ops> {
        let existing = self.held.lock().unwrap().clone();
        let held = match existing {
            Some(held) => held,
            None => {
                let opening = Ops {
                    store: self.store.clone(),
                    held: None,
                    ..base.clone()
                };
                let held = ops::hold(&opening).await?;
                *self.held.lock().unwrap() = Some(Arc::clone(&held));
                held
            }
        };
        Ok(Ops {
            store: self.store.clone(),
            held: Some(held),
            ..base.clone()
        })
    }

    /// Whether anything has opened this registry yet. What makes lazy opening observable — `status`
    /// reports it, and a test can assert that asking about one registry did not open the other.
    #[must_use]
    pub fn is_open(&self) -> bool {
        self.held.lock().unwrap().is_some()
    }

    /// The held registry, if there is one. For the operations that need more than `Ops` — a live
    /// `repo push` has to tell the worker pool its table moved.
    pub fn held(&self) -> Option<Arc<Held>> {
        self.held.lock().unwrap().clone()
    }
}

/// One registry, as `status` and the `registries` message report it.
pub struct Hosted {
    pub name: String,
    pub open: bool,
}

/// Every registry under one data directory.
pub struct Host {
    pub data_dir: PathBuf,
    /// The one address for the whole server. Not per registry — the handshake routes (§17.6).
    pub socket: PathBuf,
    registries: Mutex<BTreeMap<String, Arc<Slot>>>,
    /// Whether this process is *serving* these registries, rather than merely holding a handle on
    /// the directory. It decides one thing — whether a registry created here is claimed — and the
    /// reason it has to is that [`Host`] answers both callers: the server, which is serving, and
    /// `borg-server create` against a data directory nobody is serving, which is not. Claiming from
    /// the second would leave a lock record naming a socket that answers nothing.
    serving: AtomicBool,
}

impl Host {
    /// Discover what is under `data_dir`. Opens nothing.
    ///
    /// A directory with a `borg.db` in it is a registry; anything else is ignored rather than
    /// refused, because a data dir is a place people put things and a server that failed to start
    /// over a stray file would be a server people work around.
    pub fn open(data_dir: &Path, socket: &Path) -> Result<Arc<Self>> {
        std::fs::create_dir_all(data_dir)
            .map_err(|err| BorgError::Storage(format!("{}: {err}", data_dir.display())))?;
        let mut registries = BTreeMap::new();
        let entries = std::fs::read_dir(data_dir)
            .map_err(|err| BorgError::Storage(format!("{}: {err}", data_dir.display())))?;
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !path.is_dir() || !name_is_valid(name) || !path.join(STORE_FILE).is_file() {
                continue;
            }
            registries.insert(name.to_string(), Arc::new(slot(name, &path)));
        }
        Ok(Arc::new(Self {
            data_dir: data_dir.to_path_buf(),
            socket: socket.to_path_buf(),
            registries: Mutex::new(registries),
            serving: AtomicBool::new(false),
        }))
    }

    /// The registries this server hosts, and which of them are open.
    #[must_use]
    pub fn hosted(&self) -> Vec<Hosted> {
        self.registries
            .lock()
            .unwrap()
            .values()
            .map(|slot| Hosted {
                name: slot.name.clone(),
                open: slot.is_open(),
            })
            .collect()
    }

    #[must_use]
    pub fn names(&self) -> Vec<String> {
        self.registries.lock().unwrap().keys().cloned().collect()
    }

    #[must_use]
    pub fn slots(&self) -> Vec<Arc<Slot>> {
        self.registries.lock().unwrap().values().cloned().collect()
    }

    /// **Which registry a request is for.** SPEC.md §17.6.
    ///
    /// A name routes to that registry, and a name nobody hosts is an error listing what is on offer
    /// — a client that guessed wrong should not have to guess again.
    ///
    /// **Absent is the sole registry when there is exactly one**, which is what makes a one-registry
    /// server the thing a local developer already expects: `borg-server start` and connect, with no
    /// name anywhere. It is a convenience that must not survive contact with a second registry,
    /// because "the obvious one" stops being obvious the moment there are two and any answer it gave
    /// would be a coin toss over somebody's data. So at n≥2 it is an error, and the error names the
    /// options rather than merely refusing.
    pub fn route(&self, named: Option<&str>) -> Result<Arc<Slot>> {
        let registries = self.registries.lock().unwrap();
        if let Some(name) = named {
            return registries.get(name).cloned().ok_or_else(|| {
                BorgError::Storage(format!(
                    "no registry named `{name}` — this server hosts {}",
                    listed(&registries.keys().cloned().collect::<Vec<_>>())
                ))
            });
        }
        let mut all = registries.values();
        match (all.next(), all.next()) {
            (Some(only), None) => Ok(Arc::clone(only)),
            (None, _) => Err(BorgError::Storage(format!(
                "this server hosts no registries — create one with `borg-server create <name> \
                 --data-dir {}`",
                self.data_dir.display()
            ))),
            (Some(_), Some(_)) => Err(BorgError::Storage(format!(
                "this server hosts {} — name one in the handshake, because there is no obvious \
                 default with more than one",
                listed(&registries.keys().cloned().collect::<Vec<_>>())
            ))),
        }
    }

    /// Make a registry. SPEC.md §17.6.
    ///
    /// **A server operation, not a filesystem one**, and that is the whole reason it is here: a
    /// directory appearing under the data dir while a server is up is a store the server has not
    /// locked, has not hosted, and will not route to. Going through the host means the new registry
    /// is claimed, routable and reported by `status` from the moment it exists — and on a remote
    /// server, where there is no filesystem to reach, this is the only shape that could have worked.
    pub async fn create(&self, name: &str, base: &Ops) -> Result<Arc<Slot>> {
        check_name(name)?;
        if self.registries.lock().unwrap().contains_key(name) {
            return Err(BorgError::Storage(format!(
                "a registry named `{name}` already exists"
            )));
        }
        let dir = self.data_dir.join(name);
        let made = slot(name, &dir);
        ops::init(&Ops {
            store: made.store.clone(),
            held: None,
            ..base.clone()
        })
        .await?;
        // Claimed **only if this process is serving** — see [`Host::serving`]. A registry created
        // while a server is up must be locked from the moment it exists, because it is hosted from
        // the moment it exists; one created against a directory nobody is serving must not be.
        if self.serving.load(Ordering::SeqCst) {
            serving::claim(&made.store, &self.socket, name)?;
        }
        let made = Arc::new(made);
        self.registries
            .lock()
            .unwrap()
            .insert(name.to_string(), Arc::clone(&made));
        Ok(made)
    }

    /// Claim every hosted store for this process, before anything is announced. See the header.
    pub fn claim_all(&self) -> Result<()> {
        for slot in self.slots() {
            serving::refuse_if_served(&slot.store)?;
            serving::claim(&slot.store, &self.socket, &slot.name)?;
        }
        self.serving.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Give every store back. Best-effort, and paired with `claim_all` rather than with `open`.
    pub fn release_all(&self) {
        self.serving.store(false, Ordering::SeqCst);
        for slot in self.slots() {
            serving::release(&slot.store);
        }
    }

    /// Stop the worker pools of every registry that has one.
    ///
    /// Workers are **subprocesses**, so they outlive this process unless they are told: leaving a
    /// pipeline's interpreter running after the server that started it has stopped is the kind of
    /// leak a supervisor discovers days later. A registry nothing opened has no pool and costs
    /// nothing here, which is lazy opening paying out twice.
    pub async fn shutdown(&self) {
        for slot in self.slots() {
            if let Some(held) = slot.held() {
                held.shutdown().await;
            }
        }
    }
}

fn slot(name: &str, dir: &Path) -> Slot {
    Slot {
        name: name.to_string(),
        store: dir.join(STORE_FILE),
        gate: Mutex::new(()),
        held: Mutex::new(None),
    }
}

/// `nothing`, `1 registry (main)`, `2 registries (analytics, crm)`. Written out because these
/// sentences are the whole of what a misrouted client has to go on.
fn listed(names: &[String]) -> String {
    match names.len() {
        0 => "nothing".to_string(),
        1 => format!("1 registry ({})", names[0]),
        n => format!("{n} registries ({})", names.join(", ")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use borg_core::FreshnessRequirement;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "borg-host-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn base(dir: &Path) -> Ops {
        Ops {
            store: dir.join(STORE_FILE),
            branch: None,
            version: None,
            freshness: FreshnessRequirement::Validated,
            settled: false,
            held: None,
        }
    }

    async fn host_with(dir: &Path, names: &[&str]) -> Arc<Host> {
        let host = Host::open(dir, &dir.join(SOCKET_FILE)).unwrap();
        for name in names {
            host.create(name, &base(dir)).await.unwrap();
        }
        host
    }

    /// The n=1 convenience: one registry, and a client that names nothing gets it.
    #[tokio::test]
    async fn a_client_that_names_nothing_gets_the_sole_registry() {
        let dir = temp_dir("sole");
        let host = host_with(&dir, &["main"]).await;
        assert_eq!(host.route(None).unwrap().name, "main");
        assert_eq!(host.route(Some("main")).unwrap().name, "main");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// **…and it must not survive a second registry.** There is no obvious default at n=2, so any
    /// answer would be a coin toss over somebody's data — and the refusal names the options, because
    /// a client that guessed wrong should not have to guess again.
    #[tokio::test]
    async fn naming_nothing_with_two_registries_is_an_error_that_names_both() {
        let dir = temp_dir("ambiguous");
        let host = host_with(&dir, &["crm", "analytics"]).await;

        let refusal = host.route(None).unwrap_err().to_string();
        assert!(refusal.contains("crm"), "{refusal}");
        assert!(refusal.contains("analytics"), "{refusal}");
        assert!(refusal.contains("2 registries"), "{refusal}");

        // Naming one is never ambiguous, whatever else is hosted.
        assert_eq!(host.route(Some("crm")).unwrap().name, "crm");
        assert_eq!(host.route(Some("analytics")).unwrap().name, "analytics");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A name nobody hosts is refused with the list, not with "not found".
    #[tokio::test]
    async fn an_unknown_registry_is_refused_with_the_ones_that_do_exist() {
        let dir = temp_dir("unknown");
        let host = host_with(&dir, &["crm"]).await;
        let refusal = host.route(Some("crmm")).unwrap_err().to_string();
        assert!(refusal.contains("`crmm`"), "{refusal}");
        assert!(refusal.contains("crm"), "{refusal}");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// **Nothing is open until something asks**, and asking about one registry opens exactly one.
    #[tokio::test]
    async fn a_registry_opens_on_first_use_and_only_the_one_that_was_used() {
        let dir = temp_dir("lazy");
        let host = host_with(&dir, &["crm", "analytics"]).await;
        assert!(
            host.hosted().iter().all(|r| !r.open),
            "creating a registry must not open it, and neither must starting a server"
        );

        let crm = host.route(Some("crm")).unwrap();
        {
            // Exactly what a request does: take the registry, then open it under that gate. The
            // guard is scoped rather than held across the await because a `MutexGuard` living over
            // one is a trap — the server holds it across a `block_on` instead (`crate::host`).
            let _gate = crm.enter();
        }
        crm.ops(&base(&dir)).await.unwrap();

        assert!(crm.is_open());
        assert!(
            !host.route(Some("analytics")).unwrap().is_open(),
            "a request about one registry must not pay for another's log"
        );
        host.shutdown().await;
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A server started against a data dir finds what is already there — which is what makes
    /// `borg-server stop` and `borg-server start` a restart rather than a fresh installation.
    #[tokio::test]
    async fn a_restart_rediscovers_the_registries_on_disk() {
        let dir = temp_dir("rediscover");
        let host = host_with(&dir, &["crm", "analytics"]).await;
        drop(host);

        let again = Host::open(&dir, &dir.join(SOCKET_FILE)).unwrap();
        assert_eq!(again.names(), vec!["analytics", "crm"]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// The data dir holds the server's own files too, and they are not registries. Excluding `.`
    /// from a name is what makes that structural rather than a reserved list.
    #[tokio::test]
    async fn the_servers_own_files_are_not_registries() {
        let dir = temp_dir("notregistries");
        let host = host_with(&dir, &["main"]).await;
        std::fs::write(dir.join(PID_FILE), "1234").unwrap();
        std::fs::write(dir.join(LOG_FILE), "hello").unwrap();
        std::fs::create_dir_all(dir.join("borg-server.pid")).unwrap_or_default();

        let again = Host::open(&dir, &dir.join(SOCKET_FILE)).unwrap();
        assert_eq!(again.names(), vec!["main"]);
        for bad in ["../escape", "has.dot", ""] {
            assert!(
                host.create(bad, &base(&dir)).await.is_err(),
                "`{bad}` is not a registry name"
            );
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Creating one twice is an error rather than a silent adoption: the second caller believes it
    /// made an empty store.
    #[tokio::test]
    async fn a_registry_cannot_be_created_twice() {
        let dir = temp_dir("twice");
        let host = host_with(&dir, &["main"]).await;
        let again = host.create("main", &base(&dir)).await.unwrap_err();
        assert!(again.to_string().contains("already exists"), "{again}");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
