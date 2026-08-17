//! The wire contract between a **client** and `borg-server`. SDK-DRAFT.md §2.5, §3.
//!
//! The worker protocol in [`crate`] is `ProducerCtx` over a pipe. This is the other direction: the
//! *client* surface — transactions, reads with provenance, and the handful of queries a generated
//! SDK needs — over a socket instead of over argv.
//!
//! ## It is the CLI's surface, lifted
//!
//! Nothing here is new. `tx_begin`, `tx_get`, `tx_set`, `tx_commit`, `tx_abort`, `get`, `explain`,
//! `branch list`, `layer head` and `def show` are the commands `borg` already has, and the server is
//! a thin loop over the same functions those subcommands call. That is deliberate: the CLI is the
//! testbed for what a client is like to use (SPEC.md §12, and `borg-cli`'s own header), so a
//! protocol that needed a *different* set of operations would be evidence the CLI had the wrong ones.
//!
//! ## Framing, codecs and the single-key rule are inherited whole
//!
//! Same [`Codec`](crate::Codec), same [`read_message`](crate::read_message) /
//! [`write_message`](crate::write_message), same per-codec framing (SPEC.md §17.4): NDJSON by
//! default so a shell client with `jq` stays viable, length-prefixed MessagePack when a real SDK
//! wants it. **Every message is a single-key object**, payload-free ones included — which is why the
//! empty variants below are written `Ok {}` and not as unit variants, since serde encodes a unit
//! variant as a bare string and `"ok"` is not something `jq 'keys[0]'` can dispatch on.
//!
//! ## Everything is canonical text
//!
//! Cells are `"Company:o-1234abcd.website"`, values are `"9"` / `"true"` / `"~"` / `"acme.ai"`
//! (SPEC.md §3.4) — the same forms the CLI accepts and the worker protocol already carries.
//!
//! **Layer ids travel as text too** — `"L120"`, the form `borg get` prints and `--client-version`
//! accepts. This is the one place the convention had to be chosen rather than inherited, and it went
//! this way for two reasons. A JSON number would repeat the mistake `sidecar::producer_id` documents
//! from the other side: `by` is a `ProducerId`, which is a hash using the whole `u64` range, and
//! every `jq` in the world would silently round it. And the envelope is the thing a client compares
//! with what `borg get` printed for the same cell (`scenarios/250-serve`), so a text form makes that
//! comparison a string equality rather than a reformatting exercise. Arithmetic on a layer id is
//! `s/^L//`, which is what the CLI itself does.
//!
//! ## One request, one response, in order
//!
//! A connection carries one request at a time and its answer comes back before the next is read.
//! There are no correlation ids because there is nothing to correlate — adding pipelining later is
//! adding a field, not changing a shape. A connection is otherwise stateless: **a transaction is
//! named by its id on every message**, because transactions bind to the store and not to the socket
//! (§12.2, SDK-DRAFT §2.5) — a client that drops its connection and comes back can carry on, and one
//! that never comes back is the idle reaper's problem (§12.3).
//!
//! ## The connection names a registry; the messages do not
//!
//! A server hosts a **directory of registries** on one socket and the registry is the unit of
//! tenancy (§17.6), so [`ClientHello::registry`] settles *which store* once and no message repeats
//! it. That is the opposite of the transaction rule above and for the opposite reason: a transaction
//! outlives its connection and so cannot be implied by one, while a registry is what the connection
//! is *to* — routing it per message would make one socket two stores' worth of state and put a
//! tenancy decision on every line a shell client writes.
//!
//! The one exception is [`Request::RepoPush`], which may name another, because a deploy client
//! pushing to three registries should not need three connections to do it.
//!
//! ## The handshake is answered
//!
//! Three messages, in order: the server's [`ServerHello`](crate::ServerHello), the client's
//! [`ClientHello`], the server's [`HelloAck`]. The third is what a version-1 server did not send,
//! and its absence is what made *accepted* indistinguishable from *not answered yet*, made a
//! refusal losable to an `EPIPE`, and left routing failures to be discovered at whatever request
//! came first. The routing decision is taken here now, where it was always meant to be.
//!
//! **The handshake is JSON throughout**, whatever the body will be: a codec that has not been agreed
//! cannot carry the message agreeing it, and that stays true of the acknowledgement — a client
//! reading the reply that names the negotiated codec cannot already be decoding in it.

use crate::url::Address;
use borg_core::{BorgError, Result};
use serde::{Deserialize, Serialize};
use std::io::BufReader;
use std::os::unix::net::UnixStream;

/// **One request to a running server: connect, greet, *hear back*, ask, read.** SPEC.md §17.5.
///
/// Thirty lines, and that is the claim rather than an accident — §17.5 has no hidden client
/// library, which is the same thing `scenarios/250-serve`'s `client.py` says from the other side.
/// What it is *not* is thirty lines in three places: `borg-server status`, `borg-server create`,
/// `borg generate` and `borg repo push --url` all speak this protocol, and four copies of a
/// handshake is four places for a field to be forgotten when the handshake grows one. Everything
/// stateful — a connection held across requests, a transaction, a reconnect — is an SDK's job and
/// deliberately not here (`packages/borg-sdk`).
///
/// **The hello is acknowledged, and this waits for the acknowledgement before it asks.** That is
/// one round trip a fire-and-forget handshake did not pay, and what it buys is that a refusal —
/// a registry this server does not host, a protocol version it does not speak — arrives *as a
/// refusal* rather than as a mysterious error on whatever the first request happened to be. See
/// [`HelloAck`].
///
/// `registry` is the handshake's (§17.6) and is `None` for the questions that are about the
/// *server* rather than about a store — `registries` and `registry_create` — which a connection
/// that settled no registry can still ask.
///
/// **A refusal to connect is [`crate::url::unreachable`]'s sentence**, not an `io::Error`: nothing
/// listening on a socket is the commonest failure a client has and "Connection refused" is the
/// least useful way to report it.
pub fn ask(address: &Address, registry: Option<&str>, request: &Request) -> Result<Response> {
    let broke = |what: &str, err: &dyn std::fmt::Display| {
        BorgError::Storage(format!("{address}: {what}: {err}"))
    };
    let mut conn = Conn::dial(address)?;

    let _: crate::ServerHello = conn
        .recv(crate::Codec::Json)
        .map_err(|err| broke("no hello", &err))?;
    let hello = ClientHello {
        version: VERSION,
        // **No `client_version`.** Every caller of this is asking what the store *is* right now —
        // administering a server, or generating code from the schema in force. Stating a version
        // would be making a claim about somebody else's code (§5.4).
        client_version: None,
        codec: "json".to_string(),
        registry: registry.map(str::to_string),
        // Nothing to present, and nothing checks it yet (§17.6).
        credential: None,
    };
    conn.send(crate::Codec::Json, &hello)
        .map_err(|err| broke("cannot greet", &err))?;
    match conn
        .recv::<HelloAck>(crate::Codec::Json)
        .map_err(|err| broke("no acknowledgement", &err))?
    {
        HelloAck::Accepted(_) => {}
        // The server's own sentence, unwrapped: a handshake that named a registry nobody hosts is
        // already carrying the list of the ones that exist, and prefixing it with an address would
        // bury the part that says what to do.
        HelloAck::Refused { reason } => return Err(BorgError::Storage(reason)),
    }

    conn.send(crate::Codec::Json, request)
        .map_err(|err| broke("cannot ask", &err))?;
    let answer = conn
        .recv(crate::Codec::Json)
        .map_err(|err| broke("no answer", &err));
    conn.close();
    answer
}

/// One connection, over whichever transport the address named. See [`Conn::dial`].
///
/// A private enum rather than a trait: there are two transports, the whole of the difference is how
/// a message is framed, and [`ask`] is the only caller. A trait here would be a seam nothing else
/// slots into — `borg_server::serve::Peer` is the seam, on the side that has several.
enum Conn {
    Unix {
        reader: BufReader<UnixStream>,
        writer: UnixStream,
    },
    /// Boxed because a `WebSocket` carries its read and write buffers inline and is five times the
    /// size of the pair of file descriptors beside it — and every `ask` on a laptop is the small
    /// variant.
    Ws(Box<crate::ws::Client>),
}

impl Conn {
    fn dial(address: &Address) -> Result<Self> {
        match address {
            Address::Unix(path) => {
                let stream = UnixStream::connect(path)
                    .map_err(|err| crate::url::unreachable(address, &err))?;
                let reader = BufReader::new(stream.try_clone().map_err(|err| {
                    BorgError::Storage(format!("{}: cannot read: {err}", path.display()))
                })?);
                Ok(Self::Unix {
                    reader,
                    writer: stream,
                })
            }
            // **`borg+wss://` is refused here rather than at the parser**, because the url is
            // perfectly well-formed and is the one a deployment behind a TLS proxy is configured
            // with — what is missing is a TLS client in *this* binary, and saying so is more useful
            // than pretending the address is a typo. The browser SDK dials the same url happily.
            Address::Ws { secure: true, .. } => Err(BorgError::Storage(format!(
                "{address}: this build has no TLS client — a borg+wss:// address is reached by a \
                 browser or a node process, and `borg` speaks borg+ws:// to a server whose TLS a \
                 proxy has already terminated"
            ))),
            Address::Ws {
                secure: false,
                host,
                port,
            } => crate::ws::Client::dial(host, *port)
                .map(|client| Self::Ws(Box::new(client)))
                .map_err(|err| crate::url::unreachable(address, &err)),
        }
    }

    fn send<T: Serialize>(
        &mut self,
        codec: crate::Codec,
        message: &T,
    ) -> std::result::Result<(), crate::ProtocolError> {
        match self {
            Self::Unix { writer, .. } => crate::write_message(writer, codec, message),
            Self::Ws(client) => client.send(codec, message),
        }
    }

    fn recv<T: for<'de> Deserialize<'de>>(
        &mut self,
        codec: crate::Codec,
    ) -> std::result::Result<T, crate::ProtocolError> {
        match self {
            Self::Unix { reader, .. } => crate::read_message(reader, codec),
            Self::Ws(client) => client.recv(codec),
        }
    }

    fn close(self) {
        if let Self::Ws(client) = self {
            client.close();
        }
    }
}

/// The client protocol's version, negotiated separately from the worker protocol's
/// [`crate::VERSION`]. They are two contracts over one framing and there is no reason they should
/// have to move together — which is exactly what this bump demonstrates: `2` adds [`HelloAck`] to
/// the client handshake and the worker protocol stays at `1`, untouched.
///
/// **A server refuses `1` by name.** A version-1 client sends its hello and then writes its first
/// request without waiting, so an acknowledgement would arrive where it expects a response and every
/// answer after it would be off by one — silently, which is the failure mode a version number exists
/// to convert into a sentence. The refusal is deliverable *because* of the acknowledgement it is
/// refusing over, which is the only reason this could be a clean break rather than a flag day.
pub const VERSION: u32 = 2;

/// **What the server says back to a hello.** SPEC.md §17.5, §17.6.
///
/// For two milestones the server said nothing: an accepted handshake was acknowledged by silence, so
/// *accepted* and *not answered yet* were the same observation, a refusal was written and then hung
/// up on fast enough that the answer could be lost to an `EPIPE` on the client's next write, and a
/// routing failure had nowhere to be delivered and was deferred to the first request that needed a
/// registry. That last one had a visible cost: `createBorgContext({url})` resolved happily against a
/// registry the server does not host, and the refusal arrived at some later line.
///
/// This is the channel those three needed. It is one message, and the client reads it before it
/// writes a request — which is what makes a refusal deliverable at all.
///
/// **A refusal is followed by a lingering close**, never by an immediate one: the server stops
/// writing, drains whatever the client had already sent, and only then drops the connection. A
/// client that wrote its hello and its first request in one breath would otherwise get a reset that
/// discards the very answer it was racing.
///
/// Single-key, like everything else on this wire: `{"accepted":{…}}` or `{"refused":{"reason":…}}`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HelloAck {
    Accepted(Accepted),
    /// Why not, in the words the client should show a human. The connection is over.
    Refused {
        reason: String,
    },
}

/// What an accepted handshake settled. See [`HelloAck`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Accepted {
    /// The client-protocol version this server speaks — [`VERSION`], and the number a client
    /// compares its own against.
    pub version: u32,
    /// The server's own version, as its package reports it. Not protocol: this is what a status
    /// page, a bug report or a support conversation needs, and asking for it separately would be a
    /// second round trip for a string the handshake was already sending.
    pub server: String,
    /// The codec that was negotiated, named back. A client already knows what it asked for; what it
    /// does not know until now is that the server agreed.
    pub codec: String,
    /// **Which registry this connection settled on**, resolved (§17.6).
    ///
    /// A client that named one gets it back, which is a confirmation rather than news. A client that
    /// named *none* against a server hosting exactly one gets that one's name, which is news — it is
    /// how a local developer's connection finds out what it is talking to without asking.
    ///
    /// `null` means the connection settled no registry, which happens when none was named and the
    /// server hosts none or several. That is **not** a refusal: a hello naming nothing has made no
    /// claim that could be wrong, and it is exactly what an administrative client — `borg-server
    /// status`, asking [`Request::Registries`] — has to be able to make. The ambiguity is reported
    /// at the first request that needs a store, and names the options.
    pub registry: Option<String>,
}

/// The client's reply to the server's [`ServerHello`](crate::ServerHello). Always JSON, whatever is
/// negotiated for the body — a handshake cannot be encoded in a codec that has not been agreed yet.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientHello {
    /// The client protocol version this client speaks.
    #[serde(default = "default_version")]
    pub version: u32,
    /// The def-layer this client's generated code was built from — its ClientVersion (SPEC.md §5.4,
    /// SDK-DRAFT §2.4). `"L7"`, or absent.
    ///
    /// **Absent means the branch head as it stands**, which is what the CLI already does for want of
    /// generated code: an un-generated client is authored *now*, against the schema in force. Making
    /// it required would mean a shell client had to run `borg def version` before it could connect,
    /// to state something it does not actually believe.
    #[serde(default)]
    pub client_version: Option<String>,
    /// The codec chosen from the ones the server offered.
    #[serde(default = "default_codec")]
    pub codec: String,
    /// **Which registry on this server the connection is for.** SPEC.md §17.6.
    ///
    /// A server hosts a directory of registries on one socket, and the registry is the unit of
    /// tenancy — so *which store* is a property of the connection, settled once, rather than a field
    /// repeated on every message. This is the field a multi-tenant deployment routes on and it is
    /// deliberately the same field a laptop leaves out.
    ///
    /// **Absent means the server's sole registry, when it has exactly one.** That is what keeps a
    /// one-registry server the thing a local developer already expects — start it, connect, name
    /// nothing. It must not survive a second registry: with two hosted, any answer would be a coin
    /// toss over somebody's data, so the server refuses and names the options instead.
    #[serde(default)]
    pub registry: Option<String>,
    /// **Reserved for authentication. Nothing checks it.** SPEC.md §17.6.
    ///
    /// Its existence is the point rather than its behaviour. A local server has no one to
    /// authenticate — the socket's file permissions are the boundary — and the hosted platform this
    /// is the local instance of has nothing else it could be. Adding the field once auth exists would
    /// mean a wire change at exactly the moment there is a deployment that cannot take one, so the
    /// shape is settled now and left empty; a client that sends nothing today sends nothing valid
    /// tomorrow, and one that sends a credential to a server that ignores it is not misled, because
    /// it was refused nothing.
    #[serde(default)]
    pub credential: Option<String>,
}

fn default_version() -> u32 {
    VERSION
}

fn default_codec() -> String {
    "json".to_string()
}

/// Client → server. Every operation the CLI has that a client can reach.
///
/// Absent `branch` means the store's default branch, exactly as omitting `--branch` does.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Request {
    /// Fork the branch and open a transaction. Answered with [`Response::Tx`].
    TxBegin {
        #[serde(default)]
        branch: Option<String>,
    },
    /// Read through a transaction, **recording the read** — which is the whole difference from
    /// [`Request::Get`], and what makes the guard at commit automatic (SPEC.md §12.1).
    TxGet {
        tx: String,
        cell: String,
        /// `any` | `validated` | `current`. Absent is `validated`, as on the CLI.
        #[serde(default)]
        freshness: Option<String>,
    },
    /// Write through a transaction, isolated on its branch until it merges.
    ///
    /// Answered with a bare [`Response::Ok`], where the CLI prints a layer id. The layer is on the
    /// transaction's own branch, which nobody else can see and nothing can await — the layer a
    /// client cares about is the one the commit lands in, and that is what [`Response::Committed`]
    /// carries.
    TxSet {
        tx: String,
        cell: String,
        value: String,
    },
    /// Merge, guarded by everything the transaction read. Answered with [`Response::Committed`] or
    /// [`Response::Conflict`].
    TxCommit {
        tx: String,
    },
    TxAbort {
        tx: String,
    },
    /// A read *outside* any transaction, and so one that buys no protection at commit (§12.1).
    Get {
        #[serde(default)]
        branch: Option<String>,
        cell: String,
        #[serde(default)]
        freshness: Option<String>,
        /// Read at the settled frontier rather than at the ragged head (SPEC.md §10.5).
        #[serde(default)]
        settled: bool,
    },
    Explain {
        #[serde(default)]
        branch: Option<String>,
        cell: String,
    },
    BranchList {},
    BranchHead {
        #[serde(default)]
        branch: Option<String>,
    },
    /// A struct's definition, structured. What a *debugging* client reads about one struct it can
    /// already name.
    DefShow {
        #[serde(default)]
        branch: Option<String>,
        /// Spelled `struct` on the wire even though that is a Rust keyword, following `type` in
        /// [`FieldSpec`](crate::FieldSpec): a client writing this by hand in `jq` should not have to
        /// know what Rust reserves.
        #[serde(rename = "struct")]
        struct_name: String,
    },
    /// **Every object of one struct, as ids.** SPEC.md §9.6, SDK-DRAFT §4.5.
    ///
    /// The enumeration §9.6 spent v1 refusing to expose, exposed — because an application that
    /// cannot ask *"which contacts are there"* cannot be written at all, and the answer was already
    /// in the store: this is the `scan_buffer` over a struct's existence buffer the scheduler has
    /// always used to discover entities. Tombstoned existence cells are skipped, because a deleted
    /// object is not one of the objects (SPEC.md §8.1).
    ///
    /// **Read-only, at head, and outside any transaction — there is deliberately no `tx_list`.**
    /// A read that a commit could guard has to name a cell the touch index can be asked about
    /// (§12.4), and *"the set of Contacts"* is not a cell: it is the absence-guard problem
    /// generalised from one cell to a whole buffer. A guard on it would have to mean "no object of
    /// this struct was created or deleted since the fork", which is a coarser thing than anything
    /// else in §12 and would make every creation conflict with every enumeration. Naming that
    /// honestly and leaving it out is the v1 answer; it is recorded as an open question in
    /// SDK-DRAFT §5 rather than half-built here.
    ///
    /// **Ids only, and deliberately.** A client that wants each contact's name reads each contact's
    /// name — one round trip per object, which is the N+1 every ORM has. That is a finding waiting
    /// for a query layer rather than an argument for widening this reply: adding fields here would
    /// answer one shape of question (`list` + one field) and leave every other shape — filters,
    /// ordering, joins, aggregates — exactly where it was, while making the first thing anybody
    /// builds on top of §17.5 a thing that has to be un-built.
    List {
        #[serde(default)]
        branch: Option<String>,
        /// Spelled `struct` on the wire, as in [`Request::DefShow`].
        #[serde(rename = "struct")]
        struct_name: String,
    },
    /// Allocate an object and write its existence cell, in the transaction, in one step.
    ///
    /// The other half of what an application needs and could not say: [`Request::TxSet`] can only
    /// write cells of an object whose id the client already had. The id comes back as canonical PID
    /// text, which is what [`Request::TxSet`]'s `cell` and a reference value are both built from.
    ///
    /// **The server allocates, and it allocates under an `AllocatorId` of its own** (SPEC.md §3.1).
    /// A PID is `(branch, allocator, counter)` precisely so that two allocating authorities never
    /// have to coordinate; allocator `0` is the one the hand-authored `Company#1` shorthand names,
    /// so server-created objects take another, and an application's objects can never collide with a
    /// scenario's or a fixture's by construction.
    ///
    /// It participates in guards like any other write: the existence cell is in the transaction's
    /// write-set, and nothing is read — so two transactions each creating an object never conflict,
    /// because they wrote different cells and neither observed the other's.
    TxCreate {
        tx: String,
        #[serde(rename = "struct")]
        struct_name: String,
    },
    /// **The whole def view of a branch — what codegen reads** (SPEC.md §15, SDK-DRAFT §4.4).
    ///
    /// The one message added after the server shipped, and it was added rather than composed
    /// because neither half of it could be. [`Request::DefShow`] answers about a struct you can
    /// already name, and codegen's entire job is to not know the names in advance — there was no
    /// enumeration on the socket at all. And the ClientVersion a generated module has to stamp
    /// itself with is the branch's *def*-version, which is not [`Request::BranchHead`]: head moves
    /// on every data write, and a def-version moves only on a def push (SPEC.md §5.3). Two facts,
    /// one round trip, because they are the same read: a client that took them separately could be
    /// handed a schema and a version from either side of a push.
    DefView {
        #[serde(default)]
        branch: Option<String>,
    },
    /// **Push a repo into a registry, executed by the server.** SPEC.md §9.2, §17.6.
    ///
    /// The one message that is not a lifted read or write, and the one that retires *"pushing a
    /// schema to a served store means stopping the server"*. A push moves definitions, which travel
    /// the log, and implementations, which are a sidecar beside the store — so a second process
    /// doing it would be the second writer the advisory lock exists to refuse. The way out is not to
    /// let the client write; it is to have the **server** do the push.
    ///
    /// **`path` is a path on the server's disk, and that is part of the contract rather than an
    /// implementation detail.** For a local server it is the directory the developer is editing,
    /// which is exactly what they mean. For a remote one it means nothing, and the answer there is
    /// an uploaded artifact rather than a path — so `path` is optional and this message is expected
    /// to grow a sibling field carrying the bytes. That is a field, not a shape: a client that sends
    /// `path` today keeps working against a server that also accepts artifacts.
    ///
    /// `registry` overrides the connection's, for a deploy client that pushes to several without
    /// reconnecting. Absent means the one the handshake settled.
    RepoPush {
        #[serde(default)]
        registry: Option<String>,
        #[serde(default)]
        branch: Option<String>,
        /// A directory **on the server**. See above.
        #[serde(default)]
        path: Option<String>,
    },
    /// What this server hosts. SPEC.md §17.6.
    ///
    /// The one message that needs no registry, which is what makes it answerable by a client that
    /// has not settled one — `borg-server status` asks exactly this, and so does anyone who has just
    /// been told their handshake was ambiguous.
    Registries {},
    /// Make a registry on this server. SPEC.md §17.6.
    ///
    /// Creating one is a server operation because a directory appearing under a running server's
    /// data dir is a store it has not locked, is not hosting and will not route to. It is also the
    /// only shape that could work against a server whose filesystem the caller cannot reach.
    RegistryCreate {
        name: String,
    },
}

/// Server → client.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Response {
    /// A transaction handle. Durable beyond this connection — see the module header.
    Tx {
        tx: String,
    },
    /// The §10.4 read envelope, for both [`Request::Get`] and [`Request::TxGet`]. **Never a bare
    /// value**: derived data is never presented as fresh, so a read carries what it reflects.
    Cell(Envelope),
    /// A write, an abort — anything whose success carries no payload.
    Ok {},
    /// The layer the transaction landed in **on its parent**, which is what a client awaits. A
    /// transaction that wrote nothing landed nowhere and says the parent's head.
    Committed {
        landed: String,
    },
    /// The commit was rejected whole (SPEC.md §13). Structured rather than a sentence, because
    /// deciding whether to retry means knowing *which* cell moved.
    Conflict {
        /// The guard cell whose re-evaluation against the parent failed. Absent only for
        /// `def_diverged`, which is a fact about a struct rather than about a cell.
        #[serde(default)]
        cell: Option<String>,
        /// `guard` | `def_diverged` | `dangling_write`.
        reason: String,
        /// The same rejection as prose, so a client that does not switch on `reason` still has
        /// something to show a human.
        message: String,
    },
    Branches(Vec<BranchInfo>),
    /// The answer to [`Request::List`]: canonical PID text, one per object, sorted.
    ///
    /// Sorted so that the same store answers the same order twice — a list whose order moved
    /// between two identical requests would make every diff of it noise. The order is
    /// `(branch, allocator, counter)`, which is allocation order within one allocator and nothing a
    /// caller may read meaning into across allocators; there is no `ORDER BY` here, and no cursor
    /// either. See [`Request::List`].
    Ids(Vec<String>),
    /// The object [`Request::TxCreate`] allocated: its PID, as text.
    ///
    /// The id and not the cell, because the id is what everything else is built from — `Contact:` +
    /// id is the existence cell, `+ ".name"` a property cell, `@` + id a reference value.
    Created {
        id: String,
    },
    Head {
        branch: String,
        layer: String,
    },
    Def(StructDef),
    /// The answer to [`Request::DefView`]: every struct the branch declares, and the def-version
    /// they were read at.
    Defs(SchemaDef),
    Lineage(Lineage),
    /// What a [`Request::RepoPush`] turned out to be.
    ///
    /// `layer` is absent when the push landed nothing, which is the ordinary case in a dev loop and
    /// not a failure: a repo describing exactly what is already in force has nothing to say (§9.2).
    /// `report` is the same lines `borg repo push` prints, so the two front ends say the same words
    /// about the same push.
    Pushed {
        #[serde(default)]
        layer: Option<String>,
        report: Vec<String>,
    },
    /// The answer to [`Request::Registries`]: what this server hosts, and which of them it has
    /// opened. `open` is lazy opening made visible — a registry nobody has used has not had its log
    /// replayed, and a server that claimed otherwise would be hiding its own boot cost.
    Registries(Vec<RegistryInfo>),
    /// Anything that went wrong and is not a conflict: an unparsable cell, a write the definitions
    /// reject, a transaction that expired. The message is the one the CLI would have printed, which
    /// includes §12.3's promise that a reaped transaction says *expired after N idle* and never
    /// *unknown transaction*.
    Error {
        message: String,
    },
}

/// The read envelope. SPEC.md §10.4.
///
/// Field-for-field what `borg get` prints, under the names the spec gives them. `value` absent means
/// the cell has never been written, which is distinct from a tombstone — that arrives as the value
/// `"~"` with `state: "tombstoned"`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Envelope {
    /// The cell, canonicalised by the server. A client may have asked with the `Company#1`
    /// shorthand; what comes back is always the canonical form.
    pub cell: String,
    pub value: Option<String>,
    /// `source` | `derived`.
    pub origin: String,
    /// `current` | `unvalidated` | `stale` | `broken` | `tombstoned`.
    pub state: String,
    /// Which write this is — absent when nothing is stored here. Reported because "is this the same
    /// write I saw on the other branch?" now has an answer (SPEC.md §13).
    pub event: Option<String>,
    pub authored_at: String,
    pub landed_at: String,
    pub fresh_as_of: String,
    /// The producer that wrote it, for derived data.
    pub by: Option<String>,
}

/// One registry a server hosts. SPEC.md §17.6.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegistryInfo {
    /// What the handshake's `registry` names.
    pub name: String,
    /// Whether the server has opened it yet. See [`Response::Registries`].
    pub open: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BranchInfo {
    pub id: String,
    pub name: Option<String>,
    /// Where it forked. Absent on a root branch.
    pub forked_at: Option<String>,
}

/// A struct's definitions as the branch holds them — the read side of [`crate::Description`].
///
/// Note the asymmetry with [`StructSpec`](crate::StructSpec), and that it is not an accident: a repo
/// *describing* itself names its producers by name, because it knows what it calls its own code; a
/// branch *reporting* a definition names them by id, because an id is all the log holds (SPEC.md
/// §9.2). Only the implementation table knows the two are the same thing.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StructDef {
    pub name: String,
    pub fields: Vec<FieldDef>,
}

/// A branch's definitions, whole. The input to codegen (SPEC.md §15).
///
/// **`version` is the ClientVersion a module generated from this would carry** (SPEC.md §5.3, §5.4)
/// — the branch's def-version, the same thing `borg def version` prints. It is deliberately part of
/// this message rather than a second one, because a generated module's stamp has to be the version
/// of *the schema it was generated from* and nothing else.
///
/// Structs come sorted by name, and each struct's fields sorted by name, so that regenerating an
/// unchanged schema produces an unchanged file. A generator whose output reordered itself would put
/// noise in every diff and make "did the schema move?" unanswerable by looking.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchemaDef {
    pub version: String,
    pub structs: Vec<StructDef>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FieldDef {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    /// The producer that owns this field. Absent means source data, written by clients (SPEC.md §8).
    pub derived_by: Option<String>,
    pub repo: u32,
    /// The **def-version of this field** — the def-layer that last mutated it, not the branch's
    /// whole-schema version (SPEC.md §5.3).
    pub version: String,
}

/// Where a value came from. SPEC.md §11.
///
/// A cell with nothing stored is answered with a lineage whose layers are all `"L0"` and whose
/// `from` is empty, rather than with an error: nothing stored is the answer to the question, not a
/// failure to answer it. `L0` is the store's own spelling for "no layer" — the same one a branch
/// with no layers reports as its head — and an absent cell's envelope says it too.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Lineage {
    pub cell: String,
    pub produced_by: Option<String>,
    pub authored_at: String,
    pub landed_at: String,
    pub fresh_as_of: String,
    /// Why this value stopped moving, when its producer is poisoned (SPEC.md §14).
    pub broken: Option<String>,
    pub from: Vec<LineageInput>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LineageInput {
    pub cell: String,
    /// `source` | `derived`.
    pub origin: String,
    pub landed_at: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Codec, read_message, write_message};

    fn envelope() -> Envelope {
        Envelope {
            cell: "Company:o-1234abcd.headcount".into(),
            value: Some("42".into()),
            origin: "source".into(),
            state: "current".into(),
            event: Some("e7".into()),
            authored_at: "L120".into(),
            landed_at: "L120".into(),
            fresh_as_of: "L500".into(),
            by: None,
        }
    }

    fn requests() -> Vec<Request> {
        vec![
            Request::TxBegin {
                branch: Some("main".into()),
            },
            Request::TxGet {
                tx: "tx-1".into(),
                cell: "Company#1.headcount".into(),
                freshness: None,
            },
            Request::TxSet {
                tx: "tx-1".into(),
                cell: "Company#1.headcount".into(),
                value: "43".into(),
            },
            Request::TxCommit { tx: "tx-1".into() },
            Request::TxAbort { tx: "tx-1".into() },
            Request::Get {
                branch: None,
                cell: "Company#1.headcount".into(),
                freshness: Some("current".into()),
                settled: true,
            },
            Request::Explain {
                branch: None,
                cell: "Company#1.is_investible".into(),
            },
            Request::BranchList {},
            Request::BranchHead { branch: None },
            Request::List {
                branch: Some("main".into()),
                struct_name: "Contact".into(),
            },
            Request::TxCreate {
                tx: "tx-1".into(),
                struct_name: "Contact".into(),
            },
            Request::DefShow {
                branch: None,
                struct_name: "Company".into(),
            },
            Request::DefView { branch: None },
            Request::RepoPush {
                registry: Some("crm".into()),
                branch: None,
                path: Some("/srv/crm/repo".into()),
            },
            Request::Registries {},
            Request::RegistryCreate {
                name: "analytics".into(),
            },
        ]
    }

    fn struct_def() -> StructDef {
        StructDef {
            name: "Company".into(),
            fields: vec![FieldDef {
                name: "headcount".into(),
                ty: "Int".into(),
                derived_by: None,
                repo: 1,
                version: "L4".into(),
            }],
        }
    }

    fn responses() -> Vec<Response> {
        vec![
            Response::Tx { tx: "tx-1".into() },
            Response::Cell(envelope()),
            Response::Ok {},
            Response::Committed {
                landed: "L501".into(),
            },
            Response::Conflict {
                cell: Some("Company:o-1234abcd.headcount".into()),
                reason: "guard".into(),
                message: "guard on … no longer holds against the parent".into(),
            },
            Response::Branches(vec![BranchInfo {
                id: "b1".into(),
                name: Some("main".into()),
                forked_at: None,
            }]),
            Response::Ids(vec!["o-1234abcd".into(), "o-1234abce".into()]),
            Response::Created {
                id: "o-1234abcf".into(),
            },
            Response::Head {
                branch: "b1".into(),
                layer: "L500".into(),
            },
            Response::Def(struct_def()),
            Response::Defs(SchemaDef {
                version: "L4".into(),
                structs: vec![struct_def()],
            }),
            Response::Lineage(Lineage {
                cell: "Company:o-1234abcd.is_investible".into(),
                produced_by: Some("P12342029420047889112".into()),
                authored_at: "L400".into(),
                landed_at: "L400".into(),
                fresh_as_of: "L450".into(),
                broken: None,
                from: vec![LineageInput {
                    cell: "Company:o-1234abcd.headcount".into(),
                    origin: "source".into(),
                    landed_at: "L120".into(),
                }],
            }),
            Response::Pushed {
                layer: Some("L12".into()),
                report: vec!["display_name -> P123 (implementation changed)".into()],
            },
            Response::Registries(vec![RegistryInfo {
                name: "crm".into(),
                open: true,
            }]),
            Response::Error {
                message: "transaction tx-9 expired after 2 minutes idle".into(),
            },
        ]
    }

    /// The point of sharing the framing: every codec carries the identical message, because they are
    /// the same serde impls on the same types.
    #[test]
    fn every_codec_round_trips_the_same_client_messages() {
        for codec in [Codec::Json, Codec::Msgpack] {
            let mut buffer = Vec::new();
            for message in &requests() {
                write_message(&mut buffer, codec, message).unwrap();
            }
            let mut cursor = std::io::Cursor::new(buffer);
            for expected in &requests() {
                let got: Request = read_message(&mut cursor, codec).unwrap();
                assert_eq!(
                    format!("{got:?}"),
                    format!("{expected:?}"),
                    "{} round trip",
                    codec.name()
                );
            }

            let mut buffer = Vec::new();
            for message in &responses() {
                write_message(&mut buffer, codec, message).unwrap();
            }
            let mut cursor = std::io::Cursor::new(buffer);
            for expected in &responses() {
                let got: Response = read_message(&mut cursor, codec).unwrap();
                assert_eq!(
                    format!("{got:?}"),
                    format!("{expected:?}"),
                    "{} round trip",
                    codec.name()
                );
            }
        }
    }

    /// The invariant a shell client relies on: one key, always, so `jq 'keys[0]'` always works —
    /// including on the payload-free messages, which is where a unit variant would leak out as a
    /// bare string (SPEC.md §17.4).
    #[test]
    fn every_client_message_is_a_single_key_object() {
        let mut buffer = Vec::new();
        for message in &requests() {
            write_message(&mut buffer, Codec::Json, message).unwrap();
        }
        for message in &responses() {
            write_message(&mut buffer, Codec::Json, message).unwrap();
        }

        for line in String::from_utf8(buffer).unwrap().lines() {
            let parsed: serde_json::Value = serde_json::from_str(line).unwrap();
            let object = parsed
                .as_object()
                .unwrap_or_else(|| panic!("`{line}` is not an object — a unit variant leaked"));
            assert_eq!(object.len(), 1, "`{line}` should have exactly one key");
        }
    }

    /// A shell client writes these by hand, so the JSON shape is part of the contract.
    #[test]
    fn json_is_the_shape_a_shell_client_would_write() {
        let one = |message: &Request| {
            let mut buffer = Vec::new();
            write_message(&mut buffer, Codec::Json, message).unwrap();
            String::from_utf8(buffer).unwrap().trim().to_string()
        };

        assert_eq!(
            one(&Request::TxGet {
                tx: "tx-1".into(),
                cell: "Company#1.headcount".into(),
                freshness: None
            }),
            r#"{"tx_get":{"tx":"tx-1","cell":"Company#1.headcount","freshness":null}}"#
        );
        assert_eq!(one(&Request::BranchList {}), r#"{"branch_list":{}}"#);
        // `struct`, not `struct_name`: the wire is written by people who do not write Rust.
        assert_eq!(
            one(&Request::DefShow {
                branch: None,
                struct_name: "Company".into()
            }),
            r#"{"def_show":{"branch":null,"struct":"Company"}}"#
        );
        assert_eq!(
            one(&Request::DefView { branch: None }),
            r#"{"def_view":{"branch":null}}"#
        );
        // The two enumeration-and-creation messages, spelled `struct` for the same reason
        // `def_show` is.
        assert_eq!(
            one(&Request::List {
                branch: None,
                struct_name: "Contact".into()
            }),
            r#"{"list":{"branch":null,"struct":"Contact"}}"#
        );
        assert_eq!(
            one(&Request::TxCreate {
                tx: "tx-1".into(),
                struct_name: "Contact".into()
            }),
            r#"{"tx_create":{"tx":"tx-1","struct":"Contact"}}"#
        );
    }

    /// Enumeration answers **ids and nothing else**, and creation answers **one id**. Asserted
    /// rather than assumed, because the pressure on both of these will be to grow a field — the
    /// first time somebody writes `list` followed by a loop of `get`, the obvious fix looks like
    /// putting a value in here. See [`Request::List`] for why that is a query layer's job.
    #[test]
    fn enumeration_answers_ids_and_creation_answers_one_id() {
        let ids = serde_json::to_value(Response::Ids(vec!["o-1234abcd".into()])).unwrap();
        assert_eq!(ids["ids"][0], "o-1234abcd");
        assert!(ids["ids"][0].is_string(), "a pid is text, never a number");

        let created = serde_json::to_value(Response::Created {
            id: "o-1234abcd".into(),
        })
        .unwrap();
        assert_eq!(created["created"]["id"], "o-1234abcd");
        assert_eq!(
            created["created"].as_object().unwrap().len(),
            1,
            "a creation answers the id it allocated and nothing else"
        );
    }

    /// The version a generated module stamps itself with is a **def**-layer, in the same text form
    /// as everything else on this wire — and it travels with the schema it describes rather than in
    /// a message of its own, so a generator cannot pair one branch's structs with another's version
    /// (SPEC.md §5.3, SDK-DRAFT §4.4).
    #[test]
    fn the_def_view_carries_the_client_version_its_schema_would_be_generated_at() {
        let json = serde_json::to_value(Response::Defs(SchemaDef {
            version: "L4".into(),
            structs: vec![struct_def()],
        }))
        .unwrap();
        assert_eq!(json["defs"]["version"], "L4");
        assert!(json["defs"]["version"].is_string(), "not a JSON number");
        assert_eq!(json["defs"]["structs"][0]["name"], "Company");
        assert_eq!(json["defs"]["structs"][0]["fields"][0]["version"], "L4");
    }

    /// Everything a shell client may leave out, left out — the fields it would otherwise have to
    /// state a belief about in order to say nothing.
    #[test]
    fn the_shell_shaped_message_omits_what_it_has_no_opinion_on() {
        let request: Request =
            serde_json::from_str(r#"{"get":{"cell":"Company#1.headcount"}}"#).unwrap();
        let Request::Get {
            branch,
            freshness,
            settled,
            ..
        } = request
        else {
            panic!("parsed as the wrong request: {request:?}")
        };
        assert!(branch.is_none() && freshness.is_none() && !settled);

        // An un-generated client has no ClientVersion to state, and must not have to invent one —
        // and a client on a one-registry server has no registry to name and no credential to hold.
        let hello: ClientHello = serde_json::from_str("{}").unwrap();
        assert_eq!(hello.version, VERSION);
        assert_eq!(hello.codec, "json");
        assert!(hello.client_version.is_none());
        assert!(hello.registry.is_none());
        assert!(hello.credential.is_none());
    }

    /// **The handshake routes, and it has room for a credential before there is one to check.**
    /// SPEC.md §17.6. Both fields are asserted here rather than only where they are used, because
    /// the whole argument for `credential` existing now is that the wire shape must not have to move
    /// when auth arrives — and a field nothing serialises is a field that will be forgotten.
    #[test]
    fn the_handshake_can_name_a_registry_and_carry_a_credential() {
        let raw = r#"{"registry":"crm","credential":"tok","codec":"msgpack"}"#;
        let hello: ClientHello = serde_json::from_str(raw).unwrap();
        assert_eq!(hello.registry.as_deref(), Some("crm"));
        assert_eq!(hello.credential.as_deref(), Some("tok"));
        assert_eq!(hello.codec, "msgpack");

        let round = serde_json::to_string(&hello).unwrap();
        assert!(round.contains(r#""registry":"crm""#), "{round}");
        assert!(round.contains(r#""credential":"tok""#), "{round}");
    }

    /// A push is a *server-side* path today and an artifact one day, so the field is optional and
    /// the message is the thing that stays. A client that sends `path` must go on working against a
    /// server that has learned to accept bytes as well — which is what makes this a field rather
    /// than a shape (see [`Request::RepoPush`]).
    #[test]
    fn a_repo_push_names_a_path_on_the_server_and_leaves_room_for_what_replaces_it() {
        let one = |message: &Request| {
            let mut buffer = Vec::new();
            write_message(&mut buffer, Codec::Json, message).unwrap();
            String::from_utf8(buffer).unwrap().trim().to_string()
        };
        assert_eq!(
            one(&Request::RepoPush {
                registry: None,
                branch: None,
                path: Some("/srv/repo".into())
            }),
            r#"{"repo_push":{"registry":null,"branch":null,"path":"/srv/repo"}}"#
        );
        // Everything optional, so a future arm can arrive without any of these moving.
        let bare: Request = serde_json::from_str(r#"{"repo_push":{}}"#).unwrap();
        let Request::RepoPush {
            registry,
            branch,
            path,
        } = bare
        else {
            panic!("parsed as the wrong request")
        };
        assert!(registry.is_none() && branch.is_none() && path.is_none());
    }

    /// Asking what a server hosts is the one question that needs no registry — which is what lets a
    /// client that has just been told its handshake was ambiguous find out what to name.
    #[test]
    fn asking_what_a_server_hosts_needs_no_registry() {
        let mut buffer = Vec::new();
        write_message(&mut buffer, Codec::Json, &Request::Registries {}).unwrap();
        assert_eq!(
            String::from_utf8(buffer).unwrap().trim(),
            r#"{"registries":{}}"#
        );

        let answer = serde_json::to_value(Response::Registries(vec![RegistryInfo {
            name: "crm".into(),
            open: false,
        }]))
        .unwrap();
        assert_eq!(answer["registries"][0]["name"], "crm");
        assert_eq!(answer["registries"][0]["open"], false);
    }

    /// **Every handshake is answered, and the answer round-trips in both codecs.** SPEC.md §17.5.
    ///
    /// The acknowledgement is JSON on the wire whatever the body will be, but it is the same serde
    /// impl on the same type as everything else — so asserting it in both codecs is asserting that
    /// nothing about it is special-cased into a shape the other encoding could not carry, which is
    /// what would rot the day a transport wants the handshake in MessagePack.
    #[test]
    fn a_hello_is_answered_with_an_ack_or_a_refusal_in_every_codec() {
        let acks = vec![
            HelloAck::Accepted(Accepted {
                version: VERSION,
                server: "0.1.0".into(),
                codec: "msgpack".into(),
                registry: Some("crm".into()),
            }),
            // A connection that settled no registry — an administrative client, which is a thing
            // the protocol has to keep being able to be.
            HelloAck::Accepted(Accepted {
                version: VERSION,
                server: "0.1.0".into(),
                codec: "json".into(),
                registry: None,
            }),
            HelloAck::Refused {
                reason: "no registry named `nope` — this server hosts analytics, crm".into(),
            },
        ];
        for codec in [Codec::Json, Codec::Msgpack] {
            let mut buffer = Vec::new();
            for ack in &acks {
                write_message(&mut buffer, codec, ack).unwrap();
            }
            let mut cursor = std::io::Cursor::new(buffer);
            for expected in &acks {
                let got: HelloAck = read_message(&mut cursor, codec).unwrap();
                assert_eq!(
                    format!("{got:?}"),
                    format!("{expected:?}"),
                    "{} round trip",
                    codec.name()
                );
            }
        }
    }

    /// The ack obeys the single-key rule like every other message, and says the four things a
    /// client asked for by connecting: which protocol, which server, which codec, which store.
    #[test]
    fn an_accepted_handshake_names_the_codec_the_server_and_the_registry() {
        let json = serde_json::to_value(HelloAck::Accepted(Accepted {
            version: VERSION,
            server: "0.1.0".into(),
            codec: "json".into(),
            registry: Some("crm".into()),
        }))
        .unwrap();
        assert_eq!(json.as_object().unwrap().len(), 1, "one key: {json}");
        assert_eq!(json["accepted"]["version"], VERSION);
        assert_eq!(json["accepted"]["server"], "0.1.0");
        assert_eq!(json["accepted"]["codec"], "json");
        assert_eq!(json["accepted"]["registry"], "crm");

        let refused = serde_json::to_value(HelloAck::Refused {
            reason: "nope".into(),
        })
        .unwrap();
        assert_eq!(refused.as_object().unwrap().len(), 1, "one key: {refused}");
        assert_eq!(refused["refused"]["reason"], "nope");
    }

    /// **The version a client states is the version this contract is at**, and it moved when the
    /// acknowledgement arrived while the worker protocol stayed where it was. Asserted because the
    /// two numbers living in one file is exactly how they would end up being bumped together.
    #[test]
    fn the_client_protocol_versions_independently_of_the_worker_protocol() {
        assert_eq!(VERSION, 2, "the ack is what moved this");
        assert_eq!(crate::VERSION, 1, "…and the worker protocol did not move");
        let hello: ClientHello = serde_json::from_str("{}").unwrap();
        assert_eq!(hello.version, VERSION);
    }

    /// A layer id is text, and it is the same text `borg get` prints — see the module header for why
    /// this is not a number.
    #[test]
    fn layer_ids_travel_in_the_form_the_cli_prints() {
        let json = serde_json::to_value(Response::Cell(envelope())).unwrap();
        let cell = &json["cell"];
        assert_eq!(cell["fresh_as_of"], "L500");
        assert!(cell["fresh_as_of"].is_string(), "not a JSON number");
        assert_eq!(
            cell["value"], "42",
            "values are text too, never JSON numbers"
        );
    }
}
