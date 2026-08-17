//! The serve loop: §17.5 over a socket, in front of a directory of registries. SPEC.md §17.5, §17.6.
//!
//! **Built as the local instance of a hosted platform, not as a separate species.** The same binary,
//! the same messages and the same routing are what a multi-tenant deployment would run; what a
//! laptop leaves out is a name in a handshake and a credential nothing checks yet. So the loop is
//! kept thin on purpose: read a message, decide which registry it is for, call the function `borg`'s
//! own subcommand calls, write the answer back. Everything it calls lives in `borg_host`, which is
//! where the CLI's commands live too — there is one implementation of a transaction across both
//! binaries and both front ends use it.
//!
//! The whole of the dispatch is [`answer`], and it is worth reading as the statement it is: every
//! arm is one `ops::` call plus a rendering. Nothing in this file knows what a guard is.
//!
//! ## Five design points, each with the reason it was decided that way
//!
//! **The handshake routes; the messages do not.** One server, one socket, many registries — so
//! *which store* is settled once per connection ([`session`]) rather than repeated per message.
//! Absent names the sole registry when there is exactly one, which is what keeps a laptop's
//! experience unchanged, and is an error naming the options at two, because there is no obvious
//! default over somebody else's data. The exceptions are `repo_push` and `export`, which may name
//! another registry: a deploy client should not need three connections to push to three registries,
//! and neither should a backup client covering three. `import` is a third kind of exception — it
//! *creates* the registry it names (§19), so like `registry_create` it is answered by the host
//! before any routing happens.
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
//! hypothetical. So the server takes an advisory lock on **every** registry it hosts, at boot, and
//! every other `borg` invocation against one of them fails naming the socket
//! (`borg_host::serving`). This is honest v1 and not the final answer: the final answer is the CLI
//! connecting to the socket instead of being turned away by it.
//!
//! **A store is opened once and held — and not before it is needed.** Each registry boots one
//! deriving `Registry` on first use and every later request goes through it. That was a change to
//! derivation's lifecycle rather than to this file: `tx_commit` used to drop its registry so that
//! `auto_derive` could open another one *with* an executor, and two live `Registry` instances over
//! one store are exactly what the single-process assumption forbids — so the long-lived registry
//! carries the executor and the dance is gone (`borg_host::ops::Held`).
//!
//! **What makes it safe is the lock, not the loop.** A registry's in-memory indexes are projections
//! of the log (`borg_engine::projection`); holding them across requests is only sound if every
//! mutation of the store flows through the instance maintaining them, and that is exactly what the
//! advisory lock guarantees — including for `repo_push`, which is on the socket now precisely so
//! that the one write that used to require stopping the server flows through the held instance like
//! everything else.
//!
//! What this was worth: `examples/personal-crm/FRICTION.md` #9 measured a read costing 18.4 ms at
//! branch head L441 and 53.0 ms at L1391 — a cost tracking the length of the log rather than the
//! size of the request, because opening the store per request replayed the log per request.
//!
//! **Requests are still serialised per registry**, one at a time. The replay was the cost, not the
//! gate; relaxing the gate is a separate change with a soak of its own (`ROADMAP.md`). What did
//! change is that the gate is per **registry** rather than server-wide, because what it protects —
//! the sidecars and the sequencer — is per store.
//!
//! ## The sidecars, one at a time
//!
//! Holding a registry means holding whatever it read on the way up, so every piece of state beside
//! the store had to be re-examined: is it *owned* by this process, *re-read* whenever it is used, or
//! *unreachable* while serving? Anything else would be a cache with no invalidation.
//!
//! * **`borg.serving.json`** — owned. Written at boot for every hosted registry, removed on the way
//!   out, and read only by other processes deciding whether to refuse. Nothing holds a copy.
//! * **`borg.transactions.json`** — re-read per use. Every `tx_*` operation and the per-request reap
//!   sweep load and save it; nothing survives a request.
//! * **`borg.derivation.json`, the pause flags** — re-read per use. `auto_derive` loads them on every
//!   call, and `set_paused` re-reads before writing so that the two halves of the file cannot clobber
//!   each other.
//! * **`borg.derivation.json`, the poison table** — owned. `ops::FilePoison` reads it once and holds
//!   it, which for a server means *for the registry's life*. Sound because the holder is also the
//!   only writer: every poisoning and every clear goes through this instance. §14's recovery —
//!   pushing fixed code — moves a producer's ClientVersion, which retires the record against the old
//!   one, and that now happens **without a restart**, because the push goes through this process.
//! * **`borg.producers.json`** — owned, and re-read exactly when it moves. `repo_push` is the only
//!   writer and is refused from anywhere else by the lock, so the pool cannot be running code the
//!   table does not name; `Held::reload_producers` is what keeps that true across a live push, and
//!   `ops::Held::producers_moved` asserts it on the derivation path rather than assuming it.
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

use borg_core::{BorgError, MergeRejection, Result};
use borg_host::host::{Host, Slot};
use borg_host::ops::{self, Ops};
use borg_host::render::struct_def;
use borg_host::{push, serving, stream};
use borg_protocol::client::{
    BranchInfo, ClientHello, Envelope, Lineage, LineageInput, RegistryInfo, Request, Response,
    SchemaDef,
};
use borg_protocol::{Codec, ProtocolError, ServerHello, negotiate};
use std::io::BufReader;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::Arc;

/// The codecs a server offers, best first. The same two the worker protocol speaks, because it is
/// the same framing (SPEC.md §17.4).
const CODECS: [Codec; 2] = [Codec::Json, Codec::Msgpack];

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

/// What the hosted registries are given up on the way out: their locks, and the socket.
struct Lock {
    host: Arc<Host>,
}

impl Drop for Lock {
    fn drop(&mut self) {
        // Best-effort, all of them. A `kill -9` leaves them behind and the next `borg` clears them,
        // which is the whole reason liveness is a connect and not a file's existence.
        self.host.release_all();
        let _ = std::fs::remove_file(&self.host.socket);
    }
}

/// Serve a data directory until the server is asked to stop.
pub async fn run(host: &Arc<Host>, base: &Ops) -> Result<()> {
    let socket = host.socket.clone();
    if let Some(parent) = socket.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|err| BorgError::Storage(format!("{}: {err}", parent.display())))?;
    }

    // Asked in this order on purpose: *is anyone serving these stores* comes before *is this address
    // free*, because a second server on a different socket is the failure that matters and the one a
    // free address would not catch.
    host.claim_all()?;
    if serving::is_listening(&socket) {
        host.release_all();
        return Err(BorgError::Storage(format!(
            "something is already listening on {}",
            socket.display()
        )));
    }
    // Whatever a dead server left behind. Removing it is safe precisely because the connect above
    // proved nothing is answering there.
    let _ = std::fs::remove_file(&socket);

    let listener = match UnixTransport::bind(&socket) {
        Ok(listener) => listener,
        Err(err) => {
            host.release_all();
            return Err(BorgError::Storage(format!("{}: {err}", socket.display())));
        }
    };
    let lock = Lock {
        host: Arc::clone(host),
    };

    // **No registry is opened here.** Locking every hosted store is a file write per registry;
    // opening one replays its log. A server that did the second at boot would pay every registry's
    // history to answer a request about one of them — see `borg_host::host`.
    //
    // Printed once the socket is bound and every store is claimed, which together are what "serving"
    // means: a scenario or a supervisor waits for this line rather than for the path to exist.
    eprintln!(
        "serving {} on {} ({})",
        host.data_dir.display(),
        socket.display(),
        registries_phrase(&host.names()),
    );

    let served = Arc::new(Served {
        host: Arc::clone(host),
        base: base.clone(),
    });

    // The accept loop is blocking (see `serve_on`), so waiting for a signal means being somewhere
    // else while it runs.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let accepting = {
        let (served, stop) = (Arc::clone(&served), Arc::clone(&stop));
        // Captured here rather than looked up in the thread: `Handle::current` answers only from
        // inside the runtime, and the accept loop is deliberately outside it.
        let handle = tokio::runtime::Handle::current();
        std::thread::spawn(move || serve_on(&listener, &served, &stop, &handle))
    };

    await_shutdown().await;

    // **Shut down properly rather than leaning on the stale check.** A lock that is stale after
    // every ordinary stop is a lock nobody can read anything into, and the stale path exists for
    // `kill -9`, not for `^C`. Waking the loop takes a connection, because unlinking a socket does
    // not unblock an `accept` already waiting on it.
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    // **The join is conditional on the wake having worked, and that is the whole of the fix for a
    // server that would not stop.** If the socket file has been removed out from under a running
    // server — a scenario's `rm -rf` on its scratch directory, a `tmpfiles` sweep, somebody
    // tidying — there is no longer any way to reach the `accept` that thread is blocked in, and
    // joining it waits forever: `SIGTERM` arrives, the signal handler runs, and the process hangs
    // here holding its locks with nothing left that could ever release them. Found by noticing
    // wedged `borg-server` processes left behind by scenario runs that had removed their own
    // socket. Skipping the join loses only orderliness — the thread dies with the process a few
    // lines below, and it can accept nothing, because `stop` is already set and the address it is
    // listening on no longer exists.
    if UnixStream::connect(&socket).is_ok() {
        let _ = accepting.join();
    }
    // Open connections are threads we do not join: a client mid-request is holding a gate, and a
    // server that waited for every reader to hang up would not stop when told. The lock files and
    // the socket go now, which is what the next `borg` needs.
    //
    // The worker pools *are* joined, because they are subprocesses rather than threads: they outlive
    // this process unless they are told, and leaving a pipeline's interpreter running after the
    // server that started it has stopped is the kind of leak a supervisor discovers days later.
    host.shutdown().await;
    drop(lock);
    eprintln!("stopped serving {}", host.data_dir.display());
    Ok(())
}

fn registries_phrase(names: &[String]) -> String {
    match names.len() {
        0 => "no registries yet".to_string(),
        1 => format!("registry {}", names[0]),
        _ => format!("registries {}", names.join(", ")),
    }
}

/// Wait for `^C` or a `kill`.
///
/// Both, because the two ways a server is stopped are a terminal and a supervisor — and
/// `borg-server stop` is the second of those, so a server that handled only `^C` would leave its
/// locks behind on every scripted shutdown.
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

/// What every connection shares: the registries, and the defaults a request varies from.
pub struct Served {
    host: Arc<Host>,
    base: Ops,
}

impl Served {
    /// Only the tests build one directly; [`run`] makes its own from the host it is serving.
    #[cfg(test)]
    pub fn new(host: Arc<Host>, base: Ops) -> Self {
        Self { host, base }
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
    served: &Arc<Served>,
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
        let served = Arc::clone(served);
        let handle = handle.clone();
        std::thread::spawn(move || session(peer, &served, &handle));
    }
}

/// One connection, from hello to hangup.
///
/// **A dropped connection is not an error.** It is a client that finished, a liveness probe, or a
/// browser tab that closed — and the last of those may have a transaction open, which is exactly the
/// case §12.3's reaper exists for. So this returns quietly and leaves the transaction table alone.
fn session(mut peer: Box<dyn Peer>, served: &Served, handle: &tokio::runtime::Handle) {
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
    // **`credential` is read and discarded, deliberately.** Nothing authenticates on a unix socket,
    // where the file's permissions are the boundary; the field exists so the wire does not have to
    // move when that stops being true (§17.6).
    let _credential = hello.credential.as_deref();

    let base = Ops {
        version: version.or(served.base.version),
        ..served.base.clone()
    };

    // **Routing is settled here and remembered — and a failure to route is remembered too.** A
    // handshake naming nothing against a two-registry server cannot be honoured, and the answer is a
    // sentence naming the options; what there is nowhere to put is that sentence *now*. The server
    // does not acknowledge an accepted hello (it has nothing to say), so a client is not reading at
    // this point, and one that were could not tell a refusal from an answer. So the error is kept
    // and handed to the first request that needs a registry — which is every request except
    // `registries`, and that exception is what lets a misrouted client find out what to name.
    let target = served.host.route(hello.registry.as_deref());

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
        let response = dispatch(served, target.as_ref(), &base, request, handle);
        if peer.send(&response).is_err() {
            return;
        }
    }
}

/// One request: find the registry it is for, take that registry, answer. SPEC.md §17.6.
fn dispatch(
    served: &Served,
    target: std::result::Result<&Arc<Slot>, &BorgError>,
    base: &Ops,
    request: Request,
    handle: &tokio::runtime::Handle,
) -> Response {
    // The questions that are about the *server* rather than about a store, and therefore the ones a
    // connection that settled no registry can still ask. `import` is here rather than below because
    // it *creates* the registry it names (§19) — there is no slot to route to yet, which is the same
    // reason `registry_create` is here.
    match &request {
        Request::Registries {} => {
            return Response::Registries(
                served
                    .host
                    .hosted()
                    .into_iter()
                    .map(|registry| RegistryInfo {
                        name: registry.name,
                        open: registry.open,
                    })
                    .collect(),
            );
        }
        Request::RegistryCreate { name } => {
            return match handle.block_on(served.host.create(name, base)) {
                Ok(_) => Response::Ok {},
                Err(err) => failed(err),
            };
        }
        Request::Import { name, path } => {
            return match handle.block_on(served.host.restore(name, base, Path::new(path))) {
                Ok((_, report)) => Response::Imported {
                    name: name.clone(),
                    layers: report.layers,
                    events: report.events,
                    branches: report.branches,
                    head: report.head.to_string(),
                    written_by: report.written_by,
                },
                Err(err) => failed(err),
            };
        }
        _ => {}
    }

    // The two messages that may name another registry — `repo_push` for a deploy client and
    // `export` for a backup one. Everything else is for the one the handshake settled.
    let named = match &request {
        Request::RepoPush { registry, .. } | Request::Export { registry, .. } => registry.clone(),
        _ => None,
    };
    let slot = match named {
        Some(name) => match served.host.route(Some(&name)) {
            Ok(slot) => slot,
            Err(err) => return failed(err),
        },
        None => match target {
            Ok(slot) => Arc::clone(slot),
            Err(err) => {
                return Response::Error {
                    message: err.to_string(),
                };
            }
        },
    };

    // Held for the whole operation, released before the reply is written: the answer goes to one
    // client, and blocking the registry while it is written would let a slow reader stall everybody
    // else on that registry. Per registry rather than server-wide — `borg_host::host`.
    let _guard = slot.enter();
    handle.block_on(async {
        let ops = match slot.ops(base).await {
            Ok(ops) => ops,
            Err(err) => return failed(err),
        };
        answer(&ops, request).await
    })
}

/// `L7`, or bare `7`. The same spelling `--client-version` accepts.
fn parse_layer(text: &str) -> Option<borg_core::LayerId> {
    text.trim_start_matches('L')
        .parse::<u64>()
        .ok()
        .map(borg_core::LayerId)
}

/// One request, answered against one registry.
///
/// Read it as the claim it makes: each arm is an `ops::` call and a rendering, and there is no
/// branch of it in which the server decides anything the CLI would have decided differently.
pub async fn answer(base: &Ops, request: Request) -> Response {
    // The same opportunistic sweep the CLI does on entry. For a server, "when a process opens the
    // store" is per request, because that is when the server takes the store (§12.3).
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

        // **The write that used to require stopping the server** (SPEC.md §9.2, §17.6). Local-only:
        // `path` is a directory on this machine, because a push reads code off a filesystem and the
        // filesystem it reads is the server's. The remote form is an uploaded artifact and is a
        // field on this message rather than a message of its own.
        Request::RepoPush { branch, path, .. } => {
            let Some(path) = path else {
                return Response::Error {
                    message: "repo_push needs a `path` — a repo directory on the server".into(),
                };
            };
            push::repo_push(&base.on(branch), Path::new(&path))
                .await
                .map(|pushed| Response::Pushed {
                    layer: pushed.layer.map(|layer| layer.to_string()),
                    report: pushed.report,
                })
                .unwrap_or_else(failed)
        }

        // **A live export.** SPEC.md §19. The registry's gate is held around this whole call
        // (`dispatch`), which is what makes it a coherent snapshot without any snapshot machinery:
        // nothing else on this registry runs while it walks. The cost is that a large export holds
        // its registry for its duration, which is the same gate `ROADMAP.md` already tracks.
        Request::Export { path, .. } => {
            let handle = match std::fs::File::create(&path) {
                Ok(file) => file,
                Err(err) => {
                    return Response::Error {
                        message: format!("{path}: {err}"),
                    };
                }
            };
            let mut out = std::io::BufWriter::new(handle);
            stream::export(base, &mut out)
                .await
                .map(|report| Response::Exported {
                    path,
                    layers: report.layers,
                    events: report.events,
                    interned: report.interned,
                    head: report.head.to_string(),
                    settled: report.settled.to_string(),
                })
                .unwrap_or_else(failed)
        }

        // Answered in `dispatch`, before any registry was needed. Unreachable rather than
        // impossible, so it says so instead of panicking.
        Request::Registries {} | Request::RegistryCreate { .. } | Request::Import { .. } => {
            Response::Error {
                message: "this request is answered by the host, not by a registry".into(),
            }
        }
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
    use borg_host::host::{SOCKET_FILE, STORE_FILE};
    use std::path::PathBuf;

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

    fn base_for(dir: &Path) -> Ops {
        Ops {
            // A placeholder: every operation works through the store a `Slot` names, and the base is
            // only carrying the flags a request may vary.
            store: dir.join(STORE_FILE),
            branch: None,
            version: None,
            freshness: FreshnessRequirement::Validated,
            settled: false,
            held: None,
        }
    }

    /// A host with `names` registries, each carrying one declared struct so that writes are legal.
    async fn host_with(dir: &Path, names: &[&str]) -> Arc<Host> {
        let host = Host::open(dir, &dir.join(SOCKET_FILE)).unwrap();
        for name in names {
            let slot = host.create(name, &base_for(dir)).await.unwrap();
            let args = Ops {
                store: slot.store.clone(),
                ..base_for(dir)
            };
            let registry = ops::open(&args).await.unwrap();
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
        }
        host
    }

    /// One committed write to `Company#1.headcount`, through the ops both front ends call.
    async fn set_headcount(args: &Ops, value: &str) {
        let tx = ops::tx_begin(args).await.unwrap();
        ops::tx_set(args, &tx, "Company#1.headcount", value)
            .await
            .unwrap();
        ops::tx_commit(args, &tx).await.unwrap();
    }

    async fn read_headcount(args: &Ops) -> Option<String> {
        ops::get(args, "Company#1.headcount")
            .await
            .unwrap()
            .rendered
    }

    /// One request against one registry, through the same routing a connection performs.
    async fn ask(
        host: &Arc<Host>,
        dir: &Path,
        registry: Option<&str>,
        request: Request,
    ) -> Response {
        let served = Served::new(Arc::clone(host), base_for(dir));
        let target = served.host.route(registry);
        let handle = tokio::runtime::Handle::current();
        let base = served.base.clone();
        // The dispatch blocks on the runtime the way a connection thread does, so it is run on a
        // blocking thread rather than on this one.
        tokio::task::block_in_place(|| dispatch(&served, target.as_ref(), &base, request, &handle))
    }

    fn value(response: &Response) -> Option<String> {
        match response {
            Response::Cell(envelope) => envelope.value.clone(),
            other => panic!("expected a cell, got {other:?}"),
        }
    }

    /// **Two registries, addressed independently, on one server.** The claim scenario 300 makes end
    /// to end, made here where the interleaving is exact: a write routed to one is invisible to the
    /// other, and neither had to know the other existed.
    #[tokio::test(flavor = "multi_thread")]
    async fn two_registries_on_one_server_are_two_stores() {
        let dir = temp_dir("two");
        let host = host_with(&dir, &["crm", "analytics"]).await;

        for (registry, headcount) in [("crm", "10"), ("analytics", "99")] {
            let Response::Tx { tx } = ask(
                &host,
                &dir,
                Some(registry),
                Request::TxBegin { branch: None },
            )
            .await
            else {
                panic!("tx_begin should answer with a handle")
            };
            ask(
                &host,
                &dir,
                Some(registry),
                Request::TxSet {
                    tx: tx.clone(),
                    cell: "Company#1.headcount".into(),
                    value: headcount.into(),
                },
            )
            .await;
            ask(&host, &dir, Some(registry), Request::TxCommit { tx }).await;
        }

        let read = |registry: &'static str| {
            let host = Arc::clone(&host);
            let dir = dir.clone();
            async move {
                value(
                    &ask(
                        &host,
                        &dir,
                        Some(registry),
                        Request::Get {
                            branch: None,
                            cell: "Company#1.headcount".into(),
                            freshness: None,
                            settled: false,
                        },
                    )
                    .await,
                )
            }
        };
        assert_eq!(read("crm").await.as_deref(), Some("10"));
        assert_eq!(
            read("analytics").await.as_deref(),
            Some("99"),
            "the same cell in another registry is another cell"
        );

        host.shutdown().await;
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// **A handshake that cannot be routed is answered on the first request that needs a registry**,
    /// and `registries` still works — which is what lets a client that guessed wrong find out what
    /// to name rather than being hung up on. See [`session`].
    #[tokio::test(flavor = "multi_thread")]
    async fn an_ambiguous_handshake_is_told_the_options_and_can_still_ask_what_is_hosted() {
        let dir = temp_dir("ambiguous");
        let host = host_with(&dir, &["crm", "analytics"]).await;

        let Response::Error { message } = ask(&host, &dir, None, Request::BranchList {}).await
        else {
            panic!("a request on an unrouted connection should be an error")
        };
        assert!(message.contains("crm"), "{message}");
        assert!(message.contains("analytics"), "{message}");

        let Response::Registries(hosted) = ask(&host, &dir, None, Request::Registries {}).await
        else {
            panic!("`registries` needs no registry and must answer")
        };
        assert_eq!(
            hosted.iter().map(|r| r.name.as_str()).collect::<Vec<_>>(),
            vec!["analytics", "crm"]
        );

        host.shutdown().await;
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// **A registry is opened by the first request that needs one, and not by its neighbours.**
    /// Lazy opening is what keeps a data dir of a hundred registries a cheap thing to serve, and
    /// `registries` reports it rather than claiming everything is warm.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_registry_is_opened_by_the_request_that_needs_it() {
        let dir = temp_dir("lazyserve");
        let host = host_with(&dir, &["crm", "analytics"]).await;

        let Response::Registries(before) = ask(&host, &dir, None, Request::Registries {}).await
        else {
            panic!("registries")
        };
        assert!(
            before.iter().all(|r| !r.open),
            "a server that has answered nothing has opened nothing"
        );

        ask(&host, &dir, Some("crm"), Request::BranchList {}).await;

        let Response::Registries(after) = ask(&host, &dir, None, Request::Registries {}).await
        else {
            panic!("registries")
        };
        let open: Vec<&str> = after
            .iter()
            .filter(|r| r.open)
            .map(|r| r.name.as_str())
            .collect();
        assert_eq!(open, vec!["crm"], "only the registry that was asked about");

        host.shutdown().await;
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A registry made over the socket is hosted, routable and reported from that moment — which is
    /// the whole reason creating one is a server operation rather than a `mkdir`.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_registry_created_over_the_socket_is_immediately_addressable() {
        let dir = temp_dir("create");
        let host = host_with(&dir, &["crm"]).await;

        let Response::Ok {} = ask(
            &host,
            &dir,
            Some("crm"),
            Request::RegistryCreate {
                name: "analytics".into(),
            },
        )
        .await
        else {
            panic!("registry_create should be accepted")
        };

        let Response::Branches(branches) =
            ask(&host, &dir, Some("analytics"), Request::BranchList {}).await
        else {
            panic!("the new registry should answer as a registry")
        };
        assert_eq!(
            branches
                .iter()
                .filter_map(|b| b.name.as_deref())
                .collect::<Vec<_>>(),
            vec!["main"],
            "a fresh registry is a store with a root branch and nothing else"
        );

        host.shutdown().await;
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// The `Ops` an operation on one registry works through, with that registry held open.
    ///
    /// No gate is taken: these tests drive one registry from one task at a time, and the gate is
    /// what `dispatch` holds when several connections do not.
    async fn held_ops(host: &Arc<Host>, dir: &Path, registry: &str) -> Ops {
        let slot = host.route(Some(registry)).unwrap();
        slot.ops(&base_for(dir)).await.unwrap()
    }

    /// The claim the whole file rests on: what comes back over the socket is what `borg get`
    /// printed, because it is the same read rendered twice.
    #[tokio::test]
    async fn a_read_over_the_protocol_is_the_read_the_cli_prints() {
        let dir = temp_dir("envelope");
        let host = host_with(&dir, &["main"]).await;
        let args = held_ops(&host, &dir, "main").await;
        set_headcount(&args, "10").await;

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
        host.shutdown().await;
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// **A held registry answers what a freshly-opened one answers.**
    ///
    /// The serve-level statement of the engine's rebuild-and-diff property
    /// (`crates/borg-engine/tests/projections.rs`): a registry is held for the server's life, so
    /// every question a client can ask has to come back the same as it would from a process that
    /// opened the store this instant. Asked **after** writes have gone through the held instance,
    /// because a maintained cache is the only kind worth doubting — an untouched one agrees with a
    /// rebuild trivially.
    #[tokio::test]
    async fn a_held_registry_answers_what_a_freshly_opened_one_answers() {
        let dir = temp_dir("held");
        let host = host_with(&dir, &["main"]).await;
        let held = held_ops(&host, &dir, "main").await;
        let fresh = Ops {
            store: held.store.clone(),
            held: None,
            ..base_for(&dir)
        };

        // Four writes through the held instance, each of which moves the projections: a creation, a
        // write and a commit that catches the parent up.
        for value in ["10", "11", "12", "13"] {
            set_headcount(&held, value).await;
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
        assert_eq!(
            ops::branch_head(&held).await.unwrap(),
            ops::branch_head(&fresh).await.unwrap(),
        );
        assert_eq!(
            ops::list(&held, "Company").await.unwrap(),
            ops::list(&fresh, "Company").await.unwrap(),
        );
        host.shutdown().await;
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// S2 through the protocol: both read, both write, the second commit is rejected and **names the
    /// cell** (SPEC.md §12, §13). The two clients are two sequences of messages, which is the
    /// interleaving a single connection could not express if transactions bound to connections.
    #[tokio::test]
    async fn the_second_commit_is_refused_and_names_the_guard_cell() {
        let dir = temp_dir("conflict");
        let host = host_with(&dir, &["main"]).await;
        let args = held_ops(&host, &dir, "main").await;
        set_headcount(&args, "10").await;

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
        assert_eq!(read_headcount(&args).await.as_deref(), Some("11"));

        // **The rejected transaction is still open**, which is what lets a client decide whether to
        // retry (§12, `ops::tx_commit`). Over a socket that matters more than it did in a shell: the
        // client holding it may have gone away, and the reaper is what collects it.
        let Response::Ok {} = answer(&args, Request::TxAbort { tx: b }).await else {
            panic!("the rejected transaction should still be abortable")
        };
        host.shutdown().await;
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A client that opens a transaction and never comes back. Nothing about the connection is
    /// involved — the transaction is in the store, and silence is what the reaper measures (§12.3).
    #[tokio::test]
    async fn an_abandoned_transaction_is_reaped_and_says_so() {
        let dir = temp_dir("reap");
        let host = host_with(&dir, &["main"]).await;
        let args = held_ops(&host, &dir, "main").await;

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
        host.shutdown().await;
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A transaction survives the connection that opened it: the handle is looked up in the store,
    /// so a second `session` — a reconnect — finds it (SDK-DRAFT §2.5).
    #[tokio::test]
    async fn a_transaction_outlives_the_connection_that_opened_it() {
        let dir = temp_dir("reconnect");
        let host = host_with(&dir, &["main"]).await;
        let args = held_ops(&host, &dir, "main").await;
        set_headcount(&args, "10").await;

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
        host.shutdown().await;
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A bad request is answered, not fatal. A connection that died on a typo would make a shell
    /// client unusable.
    #[tokio::test]
    async fn a_freshness_nobody_has_is_refused_by_name() {
        let dir = temp_dir("freshness");
        let host = host_with(&dir, &["main"]).await;
        let args = held_ops(&host, &dir, "main").await;
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
        host.shutdown().await;
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
                registry: Some("crm".into()),
                credential: None,
            },
        )
        .unwrap();
        borg_protocol::write_message(
            &mut client_to_server,
            Codec::Msgpack,
            &Request::BranchList {},
        )
        .unwrap();

        let mut to_server = std::io::Cursor::new(client_to_server);
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
        let hello: ClientHello = borg_protocol::read_message(&mut to_server, Codec::Json).unwrap();
        let codec = negotiate(&CODECS, &hello.codec).unwrap();
        assert_eq!(codec, Codec::Msgpack);
        assert_eq!(parse_layer(&hello.client_version.unwrap()).unwrap().0, 4);
        assert_eq!(
            hello.registry.as_deref(),
            Some("crm"),
            "the registry is settled in the handshake and never repeated (§17.6)"
        );

        let request: Request = borg_protocol::read_message(&mut to_server, codec).unwrap();
        assert!(matches!(request, Request::BranchList {}));

        // The server's opening word is JSON whatever was chosen, or the client could not read it.
        let opening = String::from_utf8_lossy(&from_server);
        assert!(opening.starts_with('{'), "{opening}");
    }

    /// A repo whose one pipeline multiplies `headcount` by `factor` into `doubled`.
    ///
    /// Written by the test rather than borrowed from `scenarios/`, for two reasons: `cargo test`
    /// must not acquire a `jq` dependency the scenarios already have, and **the point of the test is
    /// that the code changes** — so the body has to be something this function can vary.
    fn write_repo(dir: &Path, factor: u32) {
        let pipelines = dir.join("pipelines");
        std::fs::create_dir_all(&pipelines).unwrap();
        // The same repo id `host_with` declared `Company.headcount` under, so that the field is a
        // repeat rather than a collision — which is also what makes the *only* thing this second
        // push changes the pipeline's body.
        std::fs::write(dir.join("borg.toml"), "[repo]\nid = 1\n").unwrap();
        let worker = pipelines.join("double.sh");
        std::fs::write(
            &worker,
            format!(
                r#"#!/bin/sh
# A worker in `sh` and `sed`: enough to prove a push moves code, without a JSON parser.
if [ "$1" = "describe" ]; then
  printf '%s\n' '{{"structs":[{{"name":"Company","fields":[{{"name":"headcount","type":"Int"}},{{"name":"doubled","type":"Int","derived_by":"double"}}]}}],"producers":[{{"name":"double","source":"Company"}}]}}'
  exit 0
fi
read -r _hello
printf '{{"codec":"json"}}\n'
while IFS= read -r msg; do
  case "$msg" in
    *'"shutdown"'*) exit 0 ;;
    *'"invoke"'*) ;;
    *) continue ;;
  esac
  input=$(printf '%s' "$msg" | sed -n 's/.*"input":"\([^"]*\)".*/\1/p')
  printf '{{"get":"%s.headcount"}}\n' "$input"
  IFS= read -r reply
  value=$(printf '%s' "$reply" | sed -n 's/.*"value":"\([0-9]*\)".*/\1/p')
  [ -n "$value" ] || value=0
  printf '{{"set":{{"cell":"%s.doubled","value":"%s"}}}}\n' "$input" "$((value * {factor}))"
  IFS= read -r _ack
  printf '{{"done":{{}}}}\n'
done
"#
            ),
        )
        .unwrap();
        let mut perms = std::fs::metadata(&worker).unwrap().permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o755);
        std::fs::set_permissions(&worker, perms).unwrap();
    }

    /// **A schema can be pushed into a server that is running.** SPEC.md §9.2, §17.6.
    ///
    /// This is the sentence the whole `repo_push` message exists to delete: *pushing a schema to a
    /// served store means stopping the server*. Two claims, and the second is the one that is easy
    /// to get wrong:
    ///
    /// * the **definitions** land, and the registry the server is holding open answers from them —
    ///   which would fail if the push went through a second `Registry` over the same store;
    /// * the **code** lands, so a push that changes only a pipeline's body recomputes what that
    ///   pipeline wrote — which would fail if the worker pool built at boot survived the push, since
    ///   the pool is keyed on a path and the path did not move.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_repo_pushed_into_a_running_server_moves_both_its_definitions_and_its_code() {
        let dir = temp_dir("livepush");
        let host = host_with(&dir, &["crm"]).await;
        let repo = dir.join("repo");
        write_repo(&repo, 2);

        // The registry is open and answering *before* the push, which is what makes this a push
        // against a live server rather than a push against a store.
        let args = held_ops(&host, &dir, "crm").await;
        assert_eq!(
            ops::branch_head(&args).await.unwrap().1.to_string(),
            "L1",
            "a fresh registry with one def layer"
        );

        let pushed = ask(
            &host,
            &dir,
            Some("crm"),
            Request::RepoPush {
                registry: None,
                branch: None,
                path: Some(repo.display().to_string()),
            },
        )
        .await;
        let Response::Pushed { layer, .. } = &pushed else {
            panic!("a push should land: {pushed:?}")
        };
        assert!(layer.is_some(), "a repo nothing has seen lands a def layer");

        // The held instance answers from the new definitions — no restart, no second registry.
        let Response::Defs(schema) =
            ask(&host, &dir, Some("crm"), Request::DefView { branch: None }).await
        else {
            panic!("def_view")
        };
        let fields: Vec<&str> = schema.structs[0]
            .fields
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        assert!(fields.contains(&"doubled"), "{fields:?}");

        set_headcount(&args, "5").await;
        assert_eq!(
            ops::get(&args, "Company#1.doubled").await.unwrap().rendered,
            Some("10".into()),
            "the pipeline the push registered ran, in the server that took the push"
        );

        // **The code changes and nothing else does.** No field moves, no producer is added: the only
        // thing this push can be noticed by is the implementation fingerprint (§9.2).
        write_repo(&repo, 3);
        let pushed = ask(
            &host,
            &dir,
            Some("crm"),
            Request::RepoPush {
                registry: None,
                branch: None,
                path: Some(repo.display().to_string()),
            },
        )
        .await;
        let Response::Pushed { report, .. } = &pushed else {
            panic!("a second push should land: {pushed:?}")
        };
        assert!(
            report
                .iter()
                .any(|line| line.contains("implementation changed")),
            "an edited body is a change: {report:?}"
        );
        assert_eq!(
            ops::get(&args, "Company#1.doubled").await.unwrap().rendered,
            Some("15".into()),
            "the held server recomputed with the new code, without being restarted"
        );

        // And a push of the unchanged repo is a no-op, which is the precondition for any of this
        // being usable in a dev loop.
        let again = ask(
            &host,
            &dir,
            Some("crm"),
            Request::RepoPush {
                registry: None,
                branch: None,
                path: Some(repo.display().to_string()),
            },
        )
        .await;
        let Response::Pushed { layer, report } = &again else {
            panic!("a repeat push should still answer: {again:?}")
        };
        assert!(layer.is_none(), "an unchanged repo lands no layer");
        assert!(
            report.iter().any(|line| line.contains("unchanged")),
            "{report:?}"
        );

        host.shutdown().await;
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A push with no source is refused by name rather than answered with a guess — the field is
    /// optional because the *next* source is an artifact, not because absent means anything.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_repo_push_with_nothing_to_push_says_so() {
        let dir = temp_dir("nopath");
        let host = host_with(&dir, &["crm"]).await;
        let Response::Error { message } = ask(
            &host,
            &dir,
            Some("crm"),
            Request::RepoPush {
                registry: None,
                branch: None,
                path: None,
            },
        )
        .await
        else {
            panic!("a push with no source should be an error")
        };
        assert!(message.contains("path"), "{message}");
        host.shutdown().await;
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
