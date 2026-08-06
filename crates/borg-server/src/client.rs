//! One request to a running server, from the outside.
//!
//! `borg-server status` and `borg-server create` are **clients of the server they administer**, and
//! deliberately so: a lifecycle command that read the data directory instead would be answering
//! from the filesystem what the server is the authority on — which registries it actually hosts and
//! which of them it has opened — and would answer nothing at all once the server it is asked about
//! is on another machine.
//!
//! It is thirty lines because §17.5 has no hidden client library, which is the same claim
//! `scenarios/250-serve`'s `client.py` makes from the other side. Connect, hello, one request, one
//! response.

use borg_core::{BorgError, Result};
use borg_protocol::client::{ClientHello, Request, Response};
use borg_protocol::{Codec, ServerHello};
use std::io::BufReader;
use std::os::unix::net::UnixStream;
use std::path::Path;

/// Ask a running server one question.
///
/// `registry` is the handshake's, and is `None` for the questions that are about the *server* —
/// `registries` and `registry_create` — because those are answerable by a connection that settled no
/// registry (see `crate::serve::session`).
pub fn ask(socket: &Path, registry: Option<&str>, request: &Request) -> Result<Response> {
    let refused = |what: &str, err: &dyn std::fmt::Display| {
        BorgError::Storage(format!("{}: {what}: {err}", socket.display()))
    };
    let stream = UnixStream::connect(socket).map_err(|err| refused("cannot connect", &err))?;
    let mut reader = BufReader::new(
        stream
            .try_clone()
            .map_err(|err| refused("cannot read", &err))?,
    );
    let mut writer = stream;

    let _: ServerHello = borg_protocol::read_message(&mut reader, Codec::Json)
        .map_err(|err| refused("no hello", &err))?;
    let hello = ClientHello {
        version: borg_protocol::client::VERSION,
        // Administering a server is not acting as a client authored against a schema, so there is no
        // ClientVersion to state and stating one would be a claim about somebody else's code.
        client_version: None,
        codec: "json".to_string(),
        registry: registry.map(str::to_string),
        credential: None,
    };
    borg_protocol::write_message(&mut writer, Codec::Json, &hello)
        .map_err(|err| refused("cannot greet", &err))?;
    borg_protocol::write_message(&mut writer, Codec::Json, request)
        .map_err(|err| refused("cannot ask", &err))?;

    borg_protocol::read_message(&mut reader, Codec::Json).map_err(|err| refused("no answer", &err))
}
