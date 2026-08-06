//! `borg serve` — the command layer, over a socket instead of over argv. SDK-DRAFT.md §2.5, §2.6.
//!
//! **Built as the local instance of a future remote shape, not as a separate species.** SDK-DRAFT
//! §2.6 expects this to be superseded by real remote-connection features, so the loop is kept thin
//! on purpose: read a message, call the function `borg`'s own subcommand calls, write the answer
//! back. Everything it calls lives in [`crate::ops`], which is where the CLI's commands live too —
//! there is one implementation of a transaction in this binary and both front ends use it.
//!
//! The whole of the dispatch is [`answer`], and it is worth reading as the statement it is: every
//! arm is one `ops::` call plus a rendering. Nothing in this file knows what a guard is.
//!
//! ## Three design points, each with the reason it was decided that way
//!
//! **Transactions bind to the store, not to the connection.** The transaction table is a sidecar
//! beside the store (§12.2), so a handle outlives the socket that opened it: a browser that reloads
//! mid-transaction reconnects, names the same id, and carries on. That is the disconnect story
//! SDK-DRAFT §2.5 asks for, and it needed no code — what it needed was *not* keeping the transaction
//! in the connection, which is the tempting shape. A client that never comes back is the idle
//! reaper's problem and nobody else's (§12.3), and because the reaper sweeps whenever a process
//! opens the store, a busy server sweeps constantly for free.
//!
//! **One process serves a store.** The sidecars and `InProcessSequencer` are not multi-process safe;
//! they were not before this either, but a server makes the second process likely rather than
//! hypothetical. So `serve` takes an advisory lock and every other `borg` invocation against that
//! store fails naming the socket ([`refuse_if_served`]). This is honest v1 and not the final answer:
//! the final answer is the CLI connecting to the socket instead of being turned away by it, which is
//! the same remote-connection feature that supersedes `serve` itself.
//!
//! **The store is opened once and held.** `serve` boots one deriving `Registry` and every request
//! goes through it. This was the first thing a real server had to do and it was a change to
//! derivation's lifecycle rather than to this file: `tx_commit` used to drop its registry so that
//! `auto_derive` could open another one *with* an executor, and two live `Registry` instances over
//! one store are exactly what the single-process assumption forbids — so the long-lived registry
//! carries the executor and the dance is gone (`crate::ops::Held`).
//!
//! **What makes it safe is the lock, not the loop.** A registry's in-memory indexes are projections
//! of the log (`borg_engine::projection`); holding them across requests is only sound if every
//! mutation of the store flows through the instance maintaining them, and that is exactly what the
//! advisory lock below already guarantees — the CLI is refused by name, and `repo push` requires the
//! server to be stopped. The lock was built to be honest about the single-process assumption; it
//! turns out to be the precondition for the cache.
//!
//! What this was worth: `examples/personal-crm/FRICTION.md` #9 measured a read costing 18.4 ms at
//! branch head L441 and 53.0 ms at L1391 — a cost tracking the length of the log rather than the
//! size of the request, because opening the store per request replayed the log per request.
//!
//! **Requests are still serialised**, one at a time, store-wide. The replay was the cost, not the
//! gate; relaxing the gate is a separate change with a soak of its own (`ROADMAP.md`).
//!
//! ## The sidecars, one at a time
//!
//! Holding a registry means holding whatever it read on the way up, so every piece of state beside
//! the store had to be re-examined: is it *owned* by this process, *re-read* whenever it is used, or
//! *unreachable* while serving? Anything else would be a cache with no invalidation.
//!
//! * **`borg.serving.json`** — owned. Written here at boot, removed on the way out, and read only by
//!   other processes deciding whether to refuse. Nothing holds a copy.
//! * **`borg.transactions.json`** — re-read per use. Every `tx_*` operation and the per-request reap
//!   sweep load and save it; nothing survives a request.
//! * **`borg.derivation.json`, the pause flags** — re-read per use. `auto_derive` loads them on every
//!   call, and `set_paused` re-reads before writing so that the two halves of the file cannot clobber
//!   each other.
//! * **`borg.derivation.json`, the poison table** — owned, and the one this change lengthened.
//!   `ops::FilePoison` reads it once and holds it, which used to mean *for one command* and now means
//!   *for the server's life*. Sound because the holder is also the only writer: every poisoning and
//!   every clear goes through this instance, which updates memory and flushes. The file can only move
//!   underneath it if a second process writes it (refused by the lock) or if fixed code is pushed
//!   (`repo push`, which requires stopping the server). §14's recovery is unchanged: push fixed code,
//!   which still means a restart here.
//! * **`borg.producers.json`** — stable while serving, and asserted rather than assumed. `repo push`
//!   is the only writer and is refused while served, so the worker pool built at boot cannot be
//!   running the wrong code; `ops::Held::producers_moved` checks anyway and says so loudly.
//! * **`borg.allocations.json`** — re-read per use. `ops::allocate` loads, increments and saves
//!   before every creation, which is what makes the counter crash-safe (SDK-DRAFT §4.5).
//!
//! ## Transport
//!
//! [`Transport`] and [`Peer`] exist so a WebSocket listener can slot in without touching the message
//! layer (SDK-DRAFT §5): the *messages* are shared and only the framing differs — over a unix socket
//! it is `borg_protocol`'s per-codec framing, over a WebSocket it would be the browser's own frames
//! and `Codec` would not appear at all. The HTTP listener is deliberately **not** built here; the
//! trait is the part that has to exist before the browser client, not the server.

use crate::ops::{self, Ops};
use crate::sidecar::{self, Sidecar};
use borg_core::{BorgError, MergeRejection, Result};
use borg_protocol::client::{
    BranchInfo, ClientHello, Envelope, FieldDef, Lineage, LineageInput, Request, Response,
    SchemaDef, StructDef,
};
use borg_protocol::{Codec, ProtocolError, ServerHello, negotiate};
use std::io::BufReader;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// The codecs a server offers, best first. The same two the worker protocol speaks, because it is
/// the same framing (SPEC.md §17.4).
const CODECS: [Codec; 2] = [Codec::Json, Codec::Msgpack];

// --- The advisory lock ---------------------------------------------------------------------------

/// The record that a store is being served, beside the store with the other operational state.
///
/// A sidecar rather than a lock syscall, and the socket rather than the pid is what makes it
/// trustworthy: **the liveness test is connecting to the socket the file names.** A server that
/// crashed leaves this file behind, and a lock that outlives its holder is worse than no lock — but
/// a socket whose listener is gone refuses connections, so whoever notices can clear it and carry
/// on. That also makes the check say something useful when the lock *is* live, which a pid could
/// not: the answer to "why was I refused" is the address of the thing that refused you.
#[derive(Default, serde::Serialize, serde::Deserialize)]
struct Serving {
    socket: String,
    /// Reported in the refusal so a human can find the process. Never used to decide anything — see
    /// above.
    pid: u32,
}

impl Sidecar for Serving {
    const EXTENSION: &'static str = "serving.json";
}

/// Whether a server is listening on this socket right now.
///
/// Connect-and-drop. It is one accept the server will read nothing on, which it handles the same way
/// it handles a client that hangs up — see [`session`].
fn is_listening(socket: &Path) -> bool {
    UnixStream::connect(socket).is_ok()
}

/// The lock, held for the life of the server and released on the way out.
struct Lock {
    store: PathBuf,
    socket: PathBuf,
}

impl Drop for Lock {
    fn drop(&mut self) {
        // Best-effort, both of them. A `kill -9` leaves the pair behind and the next `borg` clears
        // them, which is the whole reason liveness is a connect and not a file's existence.
        let _ = std::fs::remove_file(sidecar::path::<Serving>(&self.store));
        let _ = std::fs::remove_file(&self.socket);
    }
}

/// The socket a live server is answering this store on, if there is one.
///
/// A stale record — one whose socket does not answer — is cleared here rather than reported, because
/// a lock nobody holds is not information.
///
/// **Two callers, and they do opposite things with the answer**, which is the point of it being a
/// question rather than a refusal. Every ordinary command turns it into [`refuse_if_served`]; `borg
/// generate` turns it into a *connection*, because it only reads and the socket is the one way to
/// read a served store. That is the remote-connection future of SDK-DRAFT §2.6 arriving for exactly
/// one read-only command, and deliberately not for the write path.
pub fn served_on(store: &Path) -> Option<(PathBuf, u32)> {
    let record: Serving = sidecar::load(store);
    if record.socket.is_empty() {
        return None;
    }
    let socket = PathBuf::from(&record.socket);
    if !is_listening(&socket) {
        let _ = std::fs::remove_file(sidecar::path::<Serving>(store));
        return None;
    }
    Some((socket, record.pid))
}

/// Refuse to touch a store somebody is serving, and say where to find them.
///
/// Called by every command except `serve` and `generate`. See [`served_on`].
pub fn refuse_if_served(args: &Ops) -> Result<()> {
    let Some((socket, pid)) = served_on(&args.store) else {
        return Ok(());
    };
    Err(BorgError::Storage(format!(
        "{} is being served on {} (pid {pid}) — one process serves a store, so this command would \
         be the second writer. Speak to the socket, or stop the server.",
        args.store.display(),
        socket.display(),
    )))
}

// --- Transport ------------------------------------------------------------------------------------

/// Where connections come from.
///
/// The seam a WebSocket listener slots into. It is over *accepting* and *framing* and not over the
/// protocol: [`Request`] and [`Response`] are the contract and do not change with the transport
/// (SDK-DRAFT §2.5).
pub trait Transport {
    fn accept(&self) -> std::io::Result<Box<dyn Peer>>;
}

/// One connection, as typed messages.
///
/// Deliberately typed rather than bytes. A unix peer frames per codec because that is what a shell
/// client can read with `read` (SPEC.md §17.4); a WebSocket peer would frame natively and never
/// mention [`Codec`] — so a byte-level trait would force one of the two to fake the other's framing.
pub trait Peer: Send {
    /// Greet the client and settle the codec. Returns what the client said about itself.
    fn hello(&mut self) -> std::result::Result<ClientHello, ProtocolError>;
    fn recv(&mut self) -> std::result::Result<Request, ProtocolError>;
    fn send(&mut self, response: &Response) -> std::result::Result<(), ProtocolError>;
}

pub struct UnixTransport(UnixListener);

impl UnixTransport {
    pub fn bind(socket: &Path) -> std::io::Result<Self> {
        UnixListener::bind(socket).map(Self)
    }
}

impl Transport for UnixTransport {
    fn accept(&self) -> std::io::Result<Box<dyn Peer>> {
        let (stream, _) = self.0.accept()?;
        Ok(Box::new(UnixPeer {
            reader: BufReader::new(stream.try_clone()?),
            writer: stream,
            codec: Codec::Json,
        }))
    }
}

struct UnixPeer {
    reader: BufReader<UnixStream>,
    writer: UnixStream,
    /// JSON until the handshake says otherwise — a hello cannot be encoded in a codec that has not
    /// been agreed yet.
    codec: Codec,
}

impl Peer for UnixPeer {
    fn hello(&mut self) -> std::result::Result<ClientHello, ProtocolError> {
        borg_protocol::write_message(
            &mut self.writer,
            Codec::Json,
            &ServerHello {
                version: borg_protocol::client::VERSION,
                codecs: CODECS.iter().map(|c| c.name().to_string()).collect(),
            },
        )?;
        let hello: ClientHello = borg_protocol::read_message(&mut self.reader, Codec::Json)?;
        self.codec = negotiate(&CODECS, &hello.codec)?;
        Ok(hello)
    }

    fn recv(&mut self) -> std::result::Result<Request, ProtocolError> {
        borg_protocol::read_message(&mut self.reader, self.codec)
    }

    fn send(&mut self, response: &Response) -> std::result::Result<(), ProtocolError> {
        borg_protocol::write_message(&mut self.writer, self.codec, response)
    }
}

// --- The server ------------------------------------------------------------------------------------

/// What every connection shares: the store, and the right to touch it one at a time.
///
/// The `Ops` here carries the held registry (`ops::Held`), which is what makes this a store and not
/// a path to one. Every session clones it, so every request in the process works through the same
/// `Registry` and the same worker pool.
pub struct Store {
    args: Ops,
    /// Store-wide, held across a whole operation. Two `borg` processes could never interleave
    /// mid-command; two connections must not either, and the mutex is what buys serve the same
    /// discipline process-per-command gave the CLI for free.
    ///
    /// **Kept deliberately.** Holding one registry removed the per-request replay; it did not make
    /// concurrent requests safe, and pretending otherwise would trade a measured win for an unmeasured
    /// risk. Letting reads overlap is its own change, with its own soak — `ROADMAP.md`.
    gate: Mutex<()>,
}

/// Serve a store until it is asked to stop.
pub async fn run(args: &Ops, socket: Option<&Path>) -> Result<()> {
    let socket = socket.ok_or_else(|| {
        BorgError::Storage("borg serve needs --socket <path> to listen on".into())
    })?;

    // Asked in this order on purpose: *is anyone serving this store* comes before *is this address
    // free*, because a second server on a different socket is the failure that matters and the one a
    // free address would not catch.
    refuse_if_served(args)?;
    if is_listening(socket) {
        return Err(BorgError::Storage(format!(
            "something is already listening on {}",
            socket.display()
        )));
    }
    // Whatever a dead server left behind. Removing it is safe precisely because the connect above
    // proved nothing is answering there.
    let _ = std::fs::remove_file(socket);

    let listener = UnixTransport::bind(socket)
        .map_err(|err| BorgError::Storage(format!("{}: {err}", socket.display())))?;
    let lock = Lock {
        store: args.store.clone(),
        socket: socket.to_path_buf(),
    };
    sidecar::save(
        &args.store,
        &Serving {
            socket: socket.display().to_string(),
            pid: std::process::id(),
        },
    )?;

    // **The one open**, and it happens after the lock so that a store somebody else is serving is
    // refused before this process replays its log. Every request below works through this registry;
    // see `ops::Held` for why that is safe and what it is worth.
    //
    // Before the line below rather than after it, because that line is what a supervisor waits for:
    // a server that announced itself and *then* spent the length of the log opening the store would
    // be telling its watcher it was ready while it was not.
    let held = ops::hold(args).await?;

    // Printed once the socket is bound and the store is open, which together are what "serving"
    // means — a scenario or a supervisor waits for this line rather than for the path to exist.
    eprintln!("serving {} on {}", args.store.display(), socket.display());

    let store = Arc::new(Store {
        args: Ops {
            held: Some(Arc::clone(&held)),
            ..args.clone()
        },
        gate: Mutex::new(()),
    });

    // The accept loop is blocking (see `serve_on`), so waiting for a signal means being somewhere
    // else while it runs.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let accepting = {
        let (store, stop) = (Arc::clone(&store), Arc::clone(&stop));
        // Captured here rather than looked up in the thread: `Handle::current` answers only from
        // inside the runtime, and the accept loop is deliberately outside it.
        let handle = tokio::runtime::Handle::current();
        std::thread::spawn(move || serve_on(&listener, &store, &stop, &handle))
    };

    await_shutdown().await;

    // **Shut down properly rather than leaning on the stale check.** A lock that is stale after
    // every ordinary stop is a lock nobody can read anything into, and the stale path exists for
    // `kill -9`, not for `^C`. Waking the loop takes a connection, because unlinking a socket does
    // not unblock an `accept` already waiting on it.
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let _ = UnixStream::connect(socket);
    let _ = accepting.join();
    // Open connections are threads we do not join: a client mid-request is holding the gate, and a
    // server that waited for every reader to hang up would not stop when told. The lock file and the
    // socket go now, which is what the next `borg` needs.
    //
    // The worker pool *is* joined, because it is subprocesses rather than threads: they outlive this
    // process unless they are told, and leaving a pipeline's interpreter running after the server
    // that started it has stopped is the kind of leak a supervisor discovers days later.
    held.shutdown().await;
    drop(lock);
    eprintln!("stopped serving {}", args.store.display());
    Ok(())
}

/// Wait for `^C` or a `kill`.
///
/// Both, because the two ways a server is stopped are a terminal and a supervisor, and a server that
/// handled only the first would leave its lock behind every time anything automated stopped it.
async fn await_shutdown() {
    let mut term = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
        Ok(term) => term,
        // Nothing here is worth refusing to serve over; fall back to `^C` alone.
        Err(_) => {
            let _ = tokio::signal::ctrl_c().await;
            return;
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

/// The accept loop. One thread per connection.
///
/// Threads rather than tasks because the framing is synchronous — `read_message` takes a `BufRead`,
/// which is what lets a shell worker and a shell client use the identical codec (SPEC.md §17.4). A
/// blocked thread costs a stack; making the framing async to save one would mean a second framing
/// implementation, which is the thing the crate exists to prevent.
fn serve_on(
    transport: &dyn Transport,
    store: &Arc<Store>,
    stop: &std::sync::atomic::AtomicBool,
    handle: &tokio::runtime::Handle,
) {
    loop {
        let peer = match transport.accept() {
            Ok(peer) => peer,
            // The listener is gone: the socket was unlinked, or we are shutting down.
            Err(_) => return,
        };
        // Checked *after* the accept, because the accept is what was blocking: shutdown wakes this
        // loop with a connection of its own, and that connection is the one being dropped here.
        if stop.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        let store = Arc::clone(store);
        let handle = handle.clone();
        std::thread::spawn(move || session(peer, &store, &handle));
    }
}

/// One connection, from hello to hangup.
///
/// **A dropped connection is not an error.** It is a client that finished, a liveness probe, or a
/// browser tab that closed — and the last of those may have a transaction open, which is exactly the
/// case §12.3's reaper exists for. So this returns quietly and leaves the transaction table alone.
fn session(mut peer: Box<dyn Peer>, store: &Store, handle: &tokio::runtime::Handle) {
    let hello = match peer.hello() {
        Ok(hello) => hello,
        // A codec we do not speak, or a hello we could not read. Refused *by name* — the handshake
        // is JSON whatever was going to be negotiated, so there is always a channel to say so on,
        // and a client left guessing why a socket went quiet is the worst outcome available.
        Err(err) => {
            let _ = peer.send(&Response::Error {
                message: err.to_string(),
            });
            return;
        }
    };

    // The def-layer the client's generated code was built from (§5.4, SDK-DRAFT §2.4). Absent means
    // "the branch as it stands", which is what an un-generated client honestly is — the same default
    // the CLI takes for want of generated code.
    let version = match hello.client_version.as_deref().map(parse_layer) {
        Some(Some(layer)) => Some(layer),
        Some(None) => {
            let _ = peer.send(&Response::Error {
                message: format!(
                    "client_version `{}` is not a layer id — try L7",
                    hello.client_version.unwrap_or_default()
                ),
            });
            return;
        }
        None => None,
    };
    let base = Ops {
        version: version.or(store.args.version),
        ..store.args.clone()
    };

    loop {
        let request = match peer.recv() {
            Ok(request) => request,
            // The client hung up, or the socket did. Both are ordinary — see the header.
            Err(ProtocolError::Closed | ProtocolError::Io(_)) => return,
            // A message we could not read. **Answered rather than fatal**, and it is the framing
            // that makes that safe: both codecs are self-delimiting — a newline for JSON, a length
            // prefix for MessagePack — so exactly one message was consumed and the stream is still
            // aligned. A shell client fixing a typo should not have to reconnect to find out
            // whether it worked, and a connection that dies on a bad line teaches people to stop
            // hand-writing the protocol, which is the one property §17.4 is protecting.
            Err(err) => {
                if peer
                    .send(&Response::Error {
                        message: err.to_string(),
                    })
                    .is_err()
                {
                    return;
                }
                continue;
            }
        };
        let response = {
            // Held for the whole operation, released before the reply is written: the answer goes
            // to one client and blocking the store while it is written would let a slow reader
            // stall everybody else.
            let _guard = store.gate.lock().unwrap_or_else(|err| err.into_inner());
            handle.block_on(answer(&base, request))
        };
        if peer.send(&response).is_err() {
            return;
        }
    }
}

/// `L7`, or bare `7`. The same spelling `--client-version` accepts.
fn parse_layer(text: &str) -> Option<borg_core::LayerId> {
    text.trim_start_matches('L')
        .parse::<u64>()
        .ok()
        .map(borg_core::LayerId)
}

/// One request, answered.
///
/// Read it as the claim it makes: each arm is an `ops::` call and a rendering, and there is no
/// branch of it in which serve decides anything the CLI would have decided differently.
async fn answer(base: &Ops, request: Request) -> Response {
    // The same opportunistic sweep the CLI does on entry. For a server, "when a process opens the
    // store" is per request, because that is when serve opens it (§12.3).
    if let Err(err) = ops::reap_transactions(base) {
        return Response::Error {
            message: err.to_string(),
        };
    }

    match request {
        Request::TxBegin { branch } => ops::tx_begin(&base.on(branch))
            .await
            .map(|tx| Response::Tx { tx })
            .unwrap_or_else(failed),

        Request::TxGet {
            tx,
            cell,
            freshness,
        } => match with_freshness(base, freshness.as_deref()) {
            Ok(args) => ops::tx_get(&args, &tx, &cell)
                .await
                .map(envelope)
                .unwrap_or_else(failed),
            Err(message) => Response::Error { message },
        },

        Request::TxSet { tx, cell, value } => ops::tx_set(base, &tx, &cell, &value)
            .await
            .map(|_| Response::Ok {})
            .unwrap_or_else(failed),

        // The one operation with an outcome that is neither success nor error: a rejected merge is
        // a fact about the world, and a client deciding whether to retry needs to know which cell
        // moved (SPEC.md §13).
        Request::TxCommit { tx } => match ops::tx_commit(base, &tx).await {
            Ok(landed) => Response::Committed {
                landed: landed.to_string(),
            },
            Err(BorgError::MergeRejected(rejection)) => conflict(&rejection),
            Err(err) => failed(err),
        },

        // Enumeration and creation, the two things a client could not say before (§9.6, §17.5).
        // Both are one `ops::` call and a rendering, like every other arm — which is the claim the
        // file makes and the reason these two needed no server-side machinery of their own.
        Request::List {
            branch,
            struct_name,
        } => ops::list(&base.on(branch), &struct_name)
            .await
            .map(|ids| Response::Ids(ids.iter().map(ToString::to_string).collect()))
            .unwrap_or_else(failed),

        Request::TxCreate { tx, struct_name } => ops::tx_create(base, &tx, &struct_name)
            .await
            .map(|pid| Response::Created {
                id: pid.to_string(),
            })
            .unwrap_or_else(failed),

        Request::TxAbort { tx } => ops::tx_abort(base, &tx)
            .map(|()| Response::Ok {})
            .unwrap_or_else(failed),

        Request::Get {
            branch,
            cell,
            freshness,
            settled,
        } => match with_freshness(&base.on(branch), freshness.as_deref()) {
            Ok(args) => ops::get(&Ops { settled, ..args }, &cell)
                .await
                .map(envelope)
                .unwrap_or_else(failed),
            Err(message) => Response::Error { message },
        },

        Request::Explain { branch, cell } => match ops::explain(&base.on(branch), &cell).await {
            Ok((cell, Some(lineage))) => Response::Lineage(lineage_of(&cell.to_string(), lineage)),
            // Nothing stored is not a failure; it is the answer.
            Ok((cell, None)) => Response::Lineage(Lineage {
                cell: cell.to_string(),
                produced_by: None,
                authored_at: "L0".into(),
                landed_at: "L0".into(),
                fresh_as_of: "L0".into(),
                broken: None,
                from: Vec::new(),
            }),
            Err(err) => failed(err),
        },

        Request::BranchList {} => ops::branch_list(base)
            .await
            .map(|branches| {
                Response::Branches(
                    branches
                        .into_iter()
                        .map(|branch| BranchInfo {
                            id: branch.id.to_string(),
                            name: branch.name,
                            forked_at: branch.origin.map(|at| at.to_string()),
                        })
                        .collect(),
                )
            })
            .unwrap_or_else(failed),

        Request::BranchHead { branch } => ops::branch_head(&base.on(branch))
            .await
            .map(|(branch, head)| Response::Head {
                branch: branch.to_string(),
                layer: head.to_string(),
            })
            .unwrap_or_else(failed),

        Request::DefShow {
            branch,
            struct_name,
        } => ops::def_show(&base.on(branch), &struct_name)
            .await
            .map(|object| Response::Def(struct_def(&object)))
            .unwrap_or_else(failed),

        Request::DefView { branch } => ops::def_view(&base.on(branch))
            .await
            .map(|(version, structs)| {
                Response::Defs(SchemaDef {
                    version: version.to_string(),
                    structs: structs.iter().map(struct_def).collect(),
                })
            })
            .unwrap_or_else(failed),
    }
}

/// A struct definition as the wire carries it. One renderer for [`Request::DefShow`],
/// [`Request::DefView`] and `borg generate`'s direct-store path, because a struct is a struct — and
/// codegen reading a different shape depending on whether it went through a socket would be the one
/// bug that only shows up on a served store.
pub fn struct_def(object: &borg_core::ObjectDef) -> StructDef {
    StructDef {
        name: object.name.to_string(),
        fields: object
            .fields
            .values()
            .map(|def| FieldDef {
                name: def.name.to_string(),
                ty: def.ty.to_string(),
                // By id, because an id is all the log holds — only the implementation table knows
                // what a human called it (§9.2).
                derived_by: def.ownership.producer().map(|p| p.to_string()),
                repo: def.declaring_repo.0,
                version: def.version.to_string(),
            })
            .collect(),
    }
}

/// The request's freshness, or the sentence to send back instead. A `String` error rather than a
/// `Response`, so that a message big enough to carry an envelope is not the error half of every
/// read's return type.
fn with_freshness(base: &Ops, mode: Option<&str>) -> std::result::Result<Ops, String> {
    let Some(mode) = mode else {
        return Ok(base.clone());
    };
    let Some(freshness) = ops::freshness(mode) else {
        return Err(format!(
            "`{mode}` is not a freshness — try any, validated or current"
        ));
    };
    Ok(Ops {
        freshness,
        ..base.clone()
    })
}

/// The §10.4 envelope, from the same [`ops::Read`] `borg get` prints. `scenarios/250-serve` asserts
/// the two agree field for field, which is the only way to be sure this stays a rendering rather
/// than becoming a second read path.
fn envelope(read: ops::Read) -> Response {
    let resolved = &read.resolved;
    Response::Cell(Envelope {
        cell: read.cell.to_string(),
        value: read.rendered.clone(),
        origin: ops::origin_name(resolved.origin).into(),
        state: ops::state_name(resolved.state).into(),
        event: resolved.event.map(|e| e.to_string()),
        authored_at: resolved.authored_at.to_string(),
        landed_at: resolved.landed_at.to_string(),
        fresh_as_of: resolved.fresh_as_of.to_string(),
        by: resolved.by.map(|p| p.to_string()),
    })
}

fn lineage_of(cell: &str, lineage: borg_engine::Lineage) -> Lineage {
    Lineage {
        cell: cell.to_string(),
        produced_by: lineage.produced_by.map(|p| p.to_string()),
        authored_at: lineage.authored_at.to_string(),
        landed_at: lineage.landed_at.to_string(),
        fresh_as_of: lineage.fresh_as_of.to_string(),
        broken: lineage.broken,
        from: lineage
            .from
            .into_iter()
            .map(|edge| LineageInput {
                cell: edge.cell.cell.to_string(),
                origin: ops::origin_name(edge.origin).into(),
                landed_at: edge.landed_at.to_string(),
            })
            .collect(),
    }
}

/// A rejected merge, structured. The cell is the point: "your commit failed" is not actionable and
/// "the cell you read moved" is.
fn conflict(rejection: &MergeRejection) -> Response {
    let (cell, reason) = match rejection {
        MergeRejection::GuardConflict { cell } => (Some(cell.to_string()), "guard"),
        MergeRejection::DanglingWrite { cell, .. } => (Some(cell.to_string()), "dangling_write"),
        // A def that moved is a fact about a struct, not about a cell.
        MergeRejection::DefDiverged { .. } => (None, "def_diverged"),
    };
    Response::Conflict {
        cell,
        reason: reason.into(),
        message: rejection.to_string(),
    }
}

/// Everything else. The message is the one the CLI would have printed on stderr — including §12.3's
/// promise that a reaped transaction says *expired after N idle*.
fn failed(err: BorgError) -> Response {
    Response::Error {
        message: err.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use borg_core::FreshnessRequirement;

    /// A client and a server over a socketpair, with no sockets, no threads and no store: enough to
    /// assert the handshake and the framing agree with themselves.
    struct Pipe {
        to_server: std::io::Cursor<Vec<u8>>,
    }

    fn ops_for(store: &Path) -> Ops {
        Ops {
            store: store.to_path_buf(),
            branch: None,
            version: None,
            freshness: FreshnessRequirement::Validated,
            settled: false,
            held: None,
        }
    }

    /// A store with a schema and one company in it, driven through the real CLI ops.
    ///
    /// **Returned with the registry held**, because that is what `run` hands every session and
    /// therefore what every assertion below should be making claims about. The fixture is built
    /// through the unheld path first, which is also honest: a store is created by a CLI before a
    /// server is pointed at it.
    async fn store_with_a_company(dir: &Path) -> Ops {
        let args = ops_for(&dir.join("borg.db"));
        let registry = ops::open(&args).await.unwrap();
        registry
            .branches
            .create_root(Some("main".into()))
            .await
            .unwrap();
        let branch = ops::branch_of(&registry, None).unwrap();
        registry
            .defs
            .push(
                branch,
                vec![borg_core::DefEvent::DeclareField {
                    struct_name: "Company".into(),
                    field: "headcount".into(),
                    ty: borg_core::ValueType::Int,
                    repo: borg_core::RepoId(1),
                    ownership: borg_core::Ownership::Source,
                }],
            )
            .await
            .unwrap();
        drop(registry);

        let tx = ops::tx_begin(&args).await.unwrap();
        ops::tx_set(&args, &tx, "Company#1.headcount", "10")
            .await
            .unwrap();
        ops::tx_commit(&args, &tx).await.unwrap();

        Ops {
            held: Some(ops::hold(&args).await.unwrap()),
            ..args
        }
    }

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "borg-serve-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// The claim the whole file rests on: what comes back over the socket is what `borg get`
    /// printed, because it is the same read rendered twice.
    #[tokio::test]
    async fn a_read_over_the_protocol_is_the_read_the_cli_prints() {
        let dir = temp_dir("envelope");
        let args = store_with_a_company(&dir).await;

        let direct = ops::get(&args, "Company#1.headcount").await.unwrap();
        let Response::Cell(envelope) = answer(
            &args,
            Request::Get {
                branch: None,
                cell: "Company#1.headcount".into(),
                freshness: None,
                settled: false,
            },
        )
        .await
        else {
            panic!("a get should answer with a cell")
        };

        assert_eq!(envelope.cell, direct.cell.to_string());
        assert_eq!(envelope.value, direct.rendered);
        assert_eq!(envelope.state, ops::state_name(direct.resolved.state));
        assert_eq!(envelope.origin, ops::origin_name(direct.resolved.origin));
        assert_eq!(
            envelope.fresh_as_of,
            direct.resolved.fresh_as_of.to_string()
        );
        assert_eq!(envelope.landed_at, direct.resolved.landed_at.to_string());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// **A held registry answers what a freshly-opened one answers.**
    ///
    /// The serve-level statement of the engine's rebuild-and-diff property
    /// (`crates/borg-engine/tests/projections.rs`): `serve` now holds one registry for its lifetime,
    /// so every question a client can ask has to come back the same as it would from a process that
    /// opened the store this instant. Asked **after** writes have gone through the held instance,
    /// because a maintained cache is the only kind worth doubting — an untouched one agrees with a
    /// rebuild trivially.
    #[tokio::test]
    async fn a_held_registry_answers_what_a_freshly_opened_one_answers() {
        let dir = temp_dir("held");
        let held = store_with_a_company(&dir).await;
        let fresh = ops_for(&held.store);

        // Three more writes through the held instance, each of which moves the projections: a
        // creation, a write and a commit that catches the parent up.
        for value in ["11", "12", "13"] {
            let Response::Tx { tx } = answer(&held, Request::TxBegin { branch: None }).await else {
                panic!("tx_begin should answer with a handle")
            };
            let Response::Ok {} = answer(
                &held,
                Request::TxSet {
                    tx: tx.clone(),
                    cell: "Company#1.headcount".into(),
                    value: value.into(),
                },
            )
            .await
            else {
                panic!("tx_set should be accepted")
            };
            let Response::Committed { .. } = answer(&held, Request::TxCommit { tx }).await else {
                panic!("the commit should land")
            };
        }

        let by_held = ops::get(&held, "Company#1.headcount").await.unwrap();
        let by_fresh = ops::get(&fresh, "Company#1.headcount").await.unwrap();
        assert_eq!(by_held.rendered.as_deref(), Some("13"));
        assert_eq!(by_held.rendered, by_fresh.rendered);
        assert_eq!(
            by_held.resolved.landed_at, by_fresh.resolved.landed_at,
            "a held registry and a fresh one must agree about where a value landed"
        );
        assert_eq!(
            by_held.resolved.fresh_as_of, by_fresh.resolved.fresh_as_of,
            "…and about how fresh it is, which is the watermark projection speaking"
        );

        // The head, which is what the layer table says, and the enumeration, which is a buffer scan:
        // one is the durable half of the log and the other is a read through the branch's ancestry.
        assert_eq!(
            ops::branch_head(&held).await.unwrap(),
            ops::branch_head(&fresh).await.unwrap(),
        );
        assert_eq!(
            ops::list(&held, "Company").await.unwrap(),
            ops::list(&fresh, "Company").await.unwrap(),
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// S2 through the protocol: both read, both write, the second commit is rejected and **names the
    /// cell** (SPEC.md §12, §13). The two clients are two sequences of messages, which is the
    /// interleaving a single connection could not express if transactions bound to connections.
    #[tokio::test]
    async fn the_second_commit_is_refused_and_names_the_guard_cell() {
        let dir = temp_dir("conflict");
        let args = store_with_a_company(&dir).await;

        let begin = || answer(&args, Request::TxBegin { branch: None });
        let Response::Tx { tx: a } = begin().await else {
            panic!("tx_begin should answer with a handle")
        };
        let Response::Tx { tx: b } = begin().await else {
            panic!("tx_begin should answer with a handle")
        };

        // Read-modify-write, both of them, interleaved: the read precedes the write, so it observed
        // the parent and is guarded (§12.1).
        for tx in [&a, &b] {
            let Response::Cell(seen) = answer(
                &args,
                Request::TxGet {
                    tx: tx.clone(),
                    cell: "Company#1.headcount".into(),
                    freshness: None,
                },
            )
            .await
            else {
                panic!("tx_get should answer with a cell")
            };
            let next: i64 = seen.value.unwrap().parse::<i64>().unwrap() + 1;
            let Response::Ok {} = answer(
                &args,
                Request::TxSet {
                    tx: tx.clone(),
                    cell: "Company#1.headcount".into(),
                    value: next.to_string(),
                },
            )
            .await
            else {
                panic!("tx_set should be accepted")
            };
        }

        let Response::Committed { .. } = answer(&args, Request::TxCommit { tx: a }).await else {
            panic!("the first commit should land")
        };
        let Response::Conflict { cell, reason, .. } =
            answer(&args, Request::TxCommit { tx: b.clone() }).await
        else {
            panic!("the second commit should be refused")
        };
        assert_eq!(reason, "guard");
        assert!(
            cell.as_deref().unwrap_or_default().contains("headcount"),
            "the conflict must name the cell that moved, got {cell:?}"
        );

        // The increment happened exactly once, not twice with one silently lost.
        let Response::Cell(envelope) = answer(
            &args,
            Request::Get {
                branch: None,
                cell: "Company#1.headcount".into(),
                freshness: None,
                settled: false,
            },
        )
        .await
        else {
            panic!("a get should answer with a cell")
        };
        assert_eq!(envelope.value.as_deref(), Some("11"));

        // **The rejected transaction is still open**, which is what lets a client decide whether to
        // retry (§12, `ops::tx_commit`). Over a socket that matters more than it did in a shell: the
        // client holding it may have gone away, and the reaper is what collects it.
        let Response::Ok {} = answer(&args, Request::TxAbort { tx: b }).await else {
            panic!("the rejected transaction should still be abortable")
        };
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A client that opens a transaction and never comes back. Nothing about the connection is
    /// involved — the transaction is in the store, and silence is what the reaper measures (§12.3).
    #[tokio::test]
    async fn an_abandoned_transaction_is_reaped_and_says_so() {
        let dir = temp_dir("reap");
        let args = store_with_a_company(&dir).await;

        let mut table = ops::load_transactions(&args);
        table.tx_idle_timeout = 0;
        ops::save_transactions(&args, &table).unwrap();

        let Response::Tx { tx } = answer(&args, Request::TxBegin { branch: None }).await else {
            panic!("tx_begin should answer with a handle")
        };
        // A timeout of zero makes "idle at all" the condition, so the next request sweeps it — which
        // is the same sweep any other request would have done a day later at the default.
        std::thread::sleep(std::time::Duration::from_millis(1100));

        let Response::Error { message } = answer(
            &args,
            Request::TxGet {
                tx,
                cell: "Company#1.headcount".into(),
                freshness: None,
            },
        )
        .await
        else {
            panic!("a reaped transaction should be an error")
        };
        // §12.3: *expired after N idle*, never *unknown transaction*. The first tells you what to do.
        assert!(
            message.contains("expired after"),
            "the client must be told the transaction expired, got: {message}"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A transaction survives the connection that opened it: the handle is looked up in the store,
    /// so a second `session` — a reconnect — finds it (SDK-DRAFT §2.5).
    #[tokio::test]
    async fn a_transaction_outlives_the_connection_that_opened_it() {
        let dir = temp_dir("reconnect");
        let args = store_with_a_company(&dir).await;

        let Response::Tx { tx } = answer(&args, Request::TxBegin { branch: None }).await else {
            panic!("tx_begin should answer with a handle")
        };
        // Everything a reconnect changes is that these two calls came from different sockets, and
        // nothing in the path between here and the transaction table can tell.
        let Response::Ok {} = answer(
            &args,
            Request::TxSet {
                tx: tx.clone(),
                cell: "Company#1.headcount".into(),
                value: "99".into(),
            },
        )
        .await
        else {
            panic!("a write through a resumed transaction should be accepted")
        };
        let Response::Committed { .. } = answer(&args, Request::TxCommit { tx }).await else {
            panic!("a resumed transaction should commit")
        };
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A bad request is answered, not fatal. A connection that died on a typo would make a shell
    /// client unusable.
    #[tokio::test]
    async fn a_freshness_nobody_has_is_refused_by_name() {
        let dir = temp_dir("freshness");
        let args = store_with_a_company(&dir).await;
        let Response::Error { message } = answer(
            &args,
            Request::Get {
                branch: None,
                cell: "Company#1.headcount".into(),
                freshness: Some("eventually".into()),
                settled: false,
            },
        )
        .await
        else {
            panic!("an unknown freshness should be an error")
        };
        assert!(message.contains("validated"), "{message}");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// The handshake, over a real pair of pipes: the server speaks first in JSON, the client answers
    /// with what it chose, and the body is framed in the codec they agreed on.
    #[test]
    fn the_handshake_settles_a_codec_before_the_body() {
        let mut client_to_server = Vec::new();
        borg_protocol::write_message(
            &mut client_to_server,
            Codec::Json,
            &ClientHello {
                version: borg_protocol::client::VERSION,
                client_version: Some("L4".into()),
                codec: "msgpack".into(),
            },
        )
        .unwrap();
        borg_protocol::write_message(
            &mut client_to_server,
            Codec::Msgpack,
            &Request::BranchList {},
        )
        .unwrap();

        let mut pipe = Pipe {
            to_server: std::io::Cursor::new(client_to_server),
        };
        let mut from_server = Vec::new();

        // What `UnixPeer::hello` does, with the socket taken out of it.
        borg_protocol::write_message(
            &mut from_server,
            Codec::Json,
            &ServerHello {
                version: borg_protocol::client::VERSION,
                codecs: CODECS.iter().map(|c| c.name().to_string()).collect(),
            },
        )
        .unwrap();
        let hello: ClientHello =
            borg_protocol::read_message(&mut pipe.to_server, Codec::Json).unwrap();
        let codec = negotiate(&CODECS, &hello.codec).unwrap();
        assert_eq!(codec, Codec::Msgpack);
        assert_eq!(parse_layer(&hello.client_version.unwrap()).unwrap().0, 4);

        let request: Request = borg_protocol::read_message(&mut pipe.to_server, codec).unwrap();
        assert!(matches!(request, Request::BranchList {}));

        // The server's opening word is JSON whatever was chosen, or the client could not read it.
        let opening = String::from_utf8_lossy(&from_server);
        assert!(opening.starts_with('{'), "{opening}");
    }

    /// The lock is the socket. A record left behind by a dead server must not lock a store out
    /// forever — see [`Serving`].
    #[test]
    fn a_served_store_refuses_others_and_a_dead_one_does_not() {
        let dir = temp_dir("lock");
        let args = ops_for(&dir.join("borg.db"));
        let socket = dir.join("borg.sock");

        // Nothing is serving: no record, no refusal.
        refuse_if_served(&args).unwrap();

        // A record whose socket answers.
        let listener = UnixListener::bind(&socket).unwrap();
        sidecar::save(
            &args.store,
            &Serving {
                socket: socket.display().to_string(),
                pid: std::process::id(),
            },
        )
        .unwrap();
        let refusal = refuse_if_served(&args).unwrap_err().to_string();
        assert!(
            refusal.contains(&socket.display().to_string()),
            "the refusal must name the socket, got: {refusal}"
        );

        // The server dies. The record is stale, and the next command clears it rather than being
        // locked out by a process that no longer exists.
        drop(listener);
        std::fs::remove_file(&socket).unwrap();
        refuse_if_served(&args).unwrap();
        assert!(!sidecar::path::<Serving>(&args.store).exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
