//! What it takes to **host** a Borg store: opening one, holding one open, the operational state
//! beside it, and the operations both front ends perform on it.
//!
//! Two binaries depend on this and neither may have its own copy of any of it:
//!
//! * `borg` (`crates/borg-cli`) — the command line client, process-per-command, operating directly
//!   on a store nobody is serving. That is **embedded Borg**, and it is legitimate forever: a
//!   scenario, a fixture, a one-off script and a `borg init` all want a store and no server.
//! * `borg-server` (`crates/borg-server`) — the dedicated server, which holds a directory of stores
//!   open and answers §17.5 on a socket.
//!
//! The rule the split enforces is the one `ops` already enforced between `main.rs` and the old
//! `serve.rs`: **an operation returns what happened; the caller renders it.** The CLI renders lines
//! of text, the server renders `borg_protocol::client::Response`s, and there is exactly one
//! implementation of a transaction, of a repo push, and of what a sidecar means.
//!
//! ## The modules, and what each is for
//!
//! * [`ops`] — the command layer. Every operation the CLI has, with nothing about how it was asked.
//! * [`push`] — `repo push`, which is its own module because it is a *diff* rather than a command
//!   (§9.2) and because it is the one operation that both front ends and the socket perform.
//! * [`sidecar`] — the files beside a store: transactions, producers, pause flags, poisonings, the
//!   PID counter.
//! * [`serving`] — the advisory lock. One process serves a store (§17.5), and this is how every
//!   other process finds out and is told where to go instead.
//! * [`keys`] — **static API keys** (§17.6): the file a server checks a handshake's credential
//!   against, the scope that decides which registries a key reaches, and the local admin token that
//!   lets the server's own CLI clients in without the unix socket being exempt from the rule.
//! * [`host`] — a **data directory of registries**, which is what a server hosts. The registry is
//!   the unit of tenancy; this is the map from a name to a store, the lazy opening, and the
//!   per-registry gate.
//! * [`render`] — the one renderer both `borg generate` and the server use for a struct definition,
//!   because codegen reading a different shape depending on whether it went through a socket would
//!   be the bug that only shows up on a served store.
//! * [`stream`] — **export and import** (§19): a registry as a canonical event stream, which is what
//!   makes the format policy real. One mechanism, four jobs — backup, restore, format migration and
//!   clone/seed — because the log is the data and every index is already a fold over it.

pub mod host;
pub mod keys;
pub mod ops;
pub mod push;
pub mod render;
pub mod serving;
pub mod sidecar;
pub mod stream;
