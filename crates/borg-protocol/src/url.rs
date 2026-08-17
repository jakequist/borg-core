//! **A connection URL: one string that configures a client.** SPEC.md §17.7.
//!
//! `DATABASE_URL`'s shape, for the same reason `DATABASE_URL` has it: everything a client needs to
//! reach a store is *where the server is* and *which registry on it* (§17.6), and those two travel
//! together — through an environment variable, a config file, a deployment's secrets — or they get
//! separated and somebody points a staging client at production's socket with production's registry
//! name still in the other variable.
//!
//! ```text
//! borg://localhost/personal-crm               the well-known local address, registry personal-crm
//! borg://localhost                            the well-known local address, no registry named
//! borg+unix:///run/user/1000/borg.sock/crm    an explicit socket, registry crm
//! borg+unix:///tmp/borg.sock                  an explicit socket, no registry named
//! borg+ws://borg.example:7717/crm             a websocket, registry crm
//! borg+wss://borg.example/crm                 the same, through a TLS-terminating proxy
//! ```
//!
//! **An absent registry stays absent.** It is not defaulted here to `main`, to the first directory
//! under a data dir, or to anything else: the server's rule is that a handshake naming no registry
//! gets the sole registry at n=1 and is refused with the options at n≥2 (§17.6), and a client that
//! filled in a guess would be re-implementing half of that rule and disagreeing with the other half.
//! So the parser answers `None` and the handshake carries `None`.
//!
//! ## Why the scheme names the transport
//!
//! `borg://` is *the local transport*, whatever that turns out to be — today a unix socket at the
//! well-known address (§17.6), and a client that writes `borg://localhost/crm` keeps working if that
//! address moves. `borg+unix://` is the escape hatch for when it has to be said: a scenario, a
//! second server, a container mount. Naming the transport in the scheme is what keeps those two
//! different strings rather than one string with a flag beside it.
//!
//! **`borg+ws://` is the transport a browser can open**, and it was reserved by name for two
//! milestones before it existed precisely so that this day would need no migration. It is a host and
//! a port rather than a path, and it defaults its port the way `ws://` does — 80 plain, 443 secure —
//! because the whole argument for a WebSocket is that it rides infrastructure that already exists,
//! and infrastructure that already exists listens on those two.
//!
//! **`borg+wss://` is parsed everywhere and dialled only where TLS is free.** The server speaks
//! plaintext and expects a proxy in front of it to terminate TLS (SPEC.md §17.6), so nothing in this
//! workspace needs a TLS client — and a Rust client that grew one would pull a certificate store
//! into a binary whose only remote transport is behind somebody else's proxy. So
//! [`crate::client::ask`] refuses `borg+wss://` by name and says what to do about it, while a
//! browser or a node process — where the
//! runtime's own WebSocket does TLS at no cost — dials it. The asymmetry is per *language* and is
//! stated rather than hidden, because the alternative was refusing the scheme outright and making
//! the deployed shape unsayable.
//!
//! ## Where the socket ends and the registry begins
//!
//! For `borg+unix://` the path holds both, so something has to divide them, and the divider is the
//! rule the server already enforces on registry names: **letters, digits, `-` and `_`, and nothing
//! else** (`borg_host::host`). The last path segment is the registry when it could *be* a registry
//! name, and part of the socket path when it could not — which makes `borg+unix:///tmp/borg.sock`
//! read as the socket it obviously is, because `borg.sock` has a dot in it and no registry ever can.
//!
//! A trailing slash always means "no registry", so both readings of an ambiguous path are sayable:
//! `borg+unix:///run/borg/crm` is the socket `/run/borg` and the registry `crm`, and
//! `borg+unix:///run/borg/crm/` is the socket `/run/borg/crm` with no registry. That the ambiguity
//! exists at all is the cost of putting two things in one path; the alternative was a query
//! parameter (`?registry=crm`), which is a second syntax for the thing the path was already doing.

use borg_core::{BorgError, Result};
use std::path::{Path, PathBuf};

/// How a connection URL says to reach a server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Transport {
    /// `borg://` — **the** local transport, resolved by the caller to the well-known address.
    ///
    /// Deliberately unresolved here: the well-known address is
    /// `borg_host::host::default_socket`, which reads `$XDG_RUNTIME_DIR` and `$HOME`, and this
    /// crate sits below `borg-host`. A second copy of that rule is exactly the drift CLAUDE.md
    /// forbids, so the parser answers *which* address and the caller supplies *where* it is
    /// ([`ConnectionUrl::address`]).
    Local,
    /// `borg+unix://` — this socket, said out loud.
    Unix(PathBuf),
    /// `borg+ws://` and `borg+wss://` — a host and a port. See the module header.
    Ws {
        secure: bool,
        host: String,
        port: u16,
    },
}

/// **Where a client actually dials**, once a `borg://` has been resolved against the well-known
/// address. What [`crate::client::ask`] takes and what an error message names.
///
/// A separate type from [`Transport`] because `Local` is a question and this is an answer: nothing
/// below `borg-host` can turn `borg://` into a path, and nothing above it should be able to hand a
/// half-resolved address to a dial.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Address {
    Unix(PathBuf),
    Ws {
        secure: bool,
        host: String,
        port: u16,
    },
}

impl Address {
    /// The `ws://host:port/` a WebSocket client asks for.
    ///
    /// **The path is `/` and carries no registry.** The registry travels in `ClientHello` and
    /// nowhere else (§17.6) — putting it in the request path as well would be a second place for it
    /// to be said and therefore a place for the two to disagree, and a proxy routing on the path
    /// while the handshake said something else is the exact failure one source of truth avoids.
    #[must_use]
    pub fn ws_url(&self) -> Option<String> {
        match self {
            Self::Unix(_) => None,
            Self::Ws { secure, host, port } => Some(format!(
                "{}://{host}:{port}/",
                if *secure { "wss" } else { "ws" }
            )),
        }
    }
}

impl std::fmt::Display for Address {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unix(path) => write!(out, "{}", path.display()),
            Self::Ws { secure, host, port } => {
                write!(
                    out,
                    "{}://{host}:{port}",
                    if *secure { "wss" } else { "ws" }
                )
            }
        }
    }
}

/// A parsed connection URL: where the server is, and which registry on it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionUrl {
    pub transport: Transport,
    /// The registry named in the URL. **`None` means none was named**, which is what the handshake
    /// then carries — see the module header.
    pub registry: Option<String>,
}

impl ConnectionUrl {
    /// Parse one. Every refusal quotes the URL back, because a URL is usually in a variable
    /// somebody has to go and find.
    pub fn parse(text: &str) -> Result<Self> {
        let Some((scheme, rest)) = text.split_once("://") else {
            return Err(malformed(
                text,
                "it needs a scheme — borg://localhost/<registry> or \
                 borg+unix:///path/to/borg.sock/<registry>",
            ));
        };
        if let Some(bad) = rest.find(['?', '#']) {
            return Err(malformed(
                text,
                &format!(
                    "`{}` has no meaning here — a borg url is a transport, an address and a \
                     registry, and nothing else",
                    &rest[bad..bad + 1]
                ),
            ));
        }
        match scheme {
            "borg" => local(text, rest),
            "borg+unix" => unix(text, rest),
            "borg+ws" => websocket(text, rest, false),
            "borg+wss" => websocket(text, rest, true),
            other => Err(malformed(
                text,
                &format!(
                    "`{other}` is not a borg transport — try borg://, borg+unix://, borg+ws:// or \
                     borg+wss://"
                ),
            )),
        }
    }

    /// The address to dial. `well_known` is `borg_host::host::default_socket`'s answer, which only a
    /// caller above the provider line can compute — see [`Transport::Local`].
    #[must_use]
    pub fn address(&self, well_known: &Path) -> Address {
        match &self.transport {
            Transport::Local => Address::Unix(well_known.to_path_buf()),
            Transport::Unix(path) => Address::Unix(path.clone()),
            Transport::Ws { secure, host, port } => Address::Ws {
                secure: *secure,
                host: host.clone(),
                port: *port,
            },
        }
    }
}

/// `borg://<host>[/<registry>]`.
fn local(text: &str, rest: &str) -> Result<ConnectionUrl> {
    let (host, tail) = rest.split_once('/').unwrap_or((rest, ""));
    // An empty host is accepted (`borg:///crm`) because it is what a URL library produces when the
    // authority is omitted, and because there is exactly one thing it could mean.
    if !host.is_empty() && host != "localhost" {
        return Err(malformed(
            text,
            &format!(
                "`{host}` is not reachable over the local transport — borg:// is this machine's \
                 well-known socket, and a remote server is borg+ws://"
            ),
        ));
    }
    Ok(ConnectionUrl {
        transport: Transport::Local,
        registry: registry_segment(text, tail)?,
    })
}

/// `borg+unix://<socket-path>[/<registry>]`. See the module header for where the two divide.
fn unix(text: &str, rest: &str) -> Result<ConnectionUrl> {
    if !rest.starts_with('/') {
        return Err(malformed(
            text,
            "borg+unix names an absolute socket path, so it takes three slashes — \
             borg+unix:///tmp/borg.sock",
        ));
    }
    // A trailing slash is the way to say "this whole path is the socket, and I am naming no
    // registry", which is what makes both readings of an ambiguous path sayable.
    if let Some(socket) = rest.strip_suffix('/') {
        if socket.is_empty() {
            return Err(malformed(text, "it names no socket path"));
        }
        return Ok(ConnectionUrl {
            transport: Transport::Unix(PathBuf::from(socket)),
            registry: None,
        });
    }

    let split = rest.rfind('/').unwrap_or(0);
    let (head, last) = (&rest[..split], &rest[split + 1..]);
    if !head.is_empty() && name_is_valid(last) {
        return Ok(ConnectionUrl {
            transport: Transport::Unix(PathBuf::from(head)),
            registry: Some(last.to_string()),
        });
    }
    Ok(ConnectionUrl {
        transport: Transport::Unix(PathBuf::from(rest)),
        registry: None,
    })
}

/// `borg+ws://<host>[:<port>][/<registry>]`, and the same for `borg+wss://`.
///
/// **The port defaults the way `ws://`'s does** — 80 plain, 443 secure — rather than to a
/// borg-specific number. A WebSocket exists here to ride infrastructure that already exists, and
/// that infrastructure listens on those two; a deployment that terminates TLS at a proxy and
/// forwards to `borg-server` is reached at `borg+wss://borg.example/crm` with nothing else said,
/// which is the whole point. A developer running a server directly writes the port out.
fn websocket(text: &str, rest: &str, secure: bool) -> Result<ConnectionUrl> {
    let (authority, tail) = rest.split_once('/').unwrap_or((rest, ""));
    if authority.is_empty() {
        return Err(malformed(
            text,
            "it names no host — borg+ws://<host>[:<port>]/<registry>",
        ));
    }
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) => {
            let Ok(port) = port.parse::<u16>() else {
                return Err(malformed(
                    text,
                    &format!("`{port}` is not a port — borg+ws://<host>:<port>/<registry>"),
                ));
            };
            (host, port)
        }
        None => (authority, if secure { 443 } else { 80 }),
    };
    if host.is_empty() {
        return Err(malformed(
            text,
            "it names no host — borg+ws://<host>[:<port>]/<registry>",
        ));
    }
    Ok(ConnectionUrl {
        transport: Transport::Ws {
            secure,
            host: host.to_string(),
            port,
        },
        registry: registry_segment(text, tail)?,
    })
}

/// The one path segment a `borg://` url may carry after the host.
fn registry_segment(text: &str, tail: &str) -> Result<Option<String>> {
    let tail = tail.trim_end_matches('/');
    if tail.is_empty() {
        return Ok(None);
    }
    if tail.contains('/') {
        return Err(malformed(
            text,
            &format!(
                "`{tail}` is more than one path segment — a borg url names one registry, as \
                 borg://localhost/<registry>"
            ),
        ));
    }
    if !name_is_valid(tail) {
        return Err(malformed(
            text,
            &format!("`{tail}` is not a registry name — letters, digits, `-` and `_`"),
        ));
    }
    Ok(Some(tail.to_string()))
}

/// The server's own rule for what may be a registry, restated where a client can apply it before
/// spending a connection on it (`borg_host::host::name_is_valid`). Duplicated across the crate
/// boundary rather than shared because `borg-host` is above this one and a client parses URLs
/// without one; it is asserted against the server's answer by `scenarios/310`.
fn name_is_valid(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

fn malformed(text: &str, why: &str) -> BorgError {
    BorgError::Storage(format!("`{text}` is not a borg url: {why}"))
}

/// **What a client says when nothing is listening.** SPEC.md §17.7.
///
/// `ECONNREFUSED` and `ENOENT` on a borg address mean one thing — there is no server there — and
/// every client that reported the `io::Error` instead was reporting the *symptom* to somebody who
/// needed the *cause*. This is the sentence, in one place, so the CLI and the SDK say the same
/// words about the same silence. `examples/personal-crm/FRICTION.md` is where the cost of not
/// having it was recorded.
///
/// Anything else — a permission error, a path that is not a socket — is reported as itself, because
/// then the io error *is* the news.
///
/// **The same sentence for a socket and for a websocket**, and deliberately: the value of it is that
/// a reader recognises it, and a second wording for the transport that happens to be remote would
/// cost exactly that. What varies is the address in the middle, which is the part that says where to
/// look.
#[must_use]
pub fn unreachable(address: &Address, err: &std::io::Error) -> BorgError {
    match err.kind() {
        std::io::ErrorKind::NotFound
        | std::io::ErrorKind::ConnectionRefused
        | std::io::ErrorKind::HostUnreachable
        | std::io::ErrorKind::NetworkUnreachable => BorgError::Unreachable(format!(
            "no borg server at {address} — start one with: borg-server start"
        )),
        _ => BorgError::Storage(format!("{address}: {err}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The well-known address a `borg://` url resolves against, in these tests. The real one comes
    /// from `borg_host::host::default_socket`; what this module owns is that `Local` means *that*
    /// address and not one of its own.
    const WELL_KNOWN: &str = "/run/user/1000/borg.sock";

    fn parsed(text: &str) -> (String, Option<String>) {
        let url = ConnectionUrl::parse(text).unwrap_or_else(|err| panic!("{text}: {err}"));
        (url.address(Path::new(WELL_KNOWN)).to_string(), url.registry)
    }

    /// **The table.** `packages/borg-sdk/test/url.test.ts` holds the same cases, because one string
    /// that means two things in two languages is the failure this shape exists to prevent — a
    /// developer writes the URL once and pastes it into both.
    #[test]
    fn a_url_names_a_socket_and_a_registry() {
        let table = [
            // The two forms in the documentation, which are the two anybody writes.
            (
                "borg://localhost/personal-crm",
                WELL_KNOWN,
                Some("personal-crm"),
            ),
            (
                "borg+unix:///path/to/borg.sock/personal-crm",
                "/path/to/borg.sock",
                Some("personal-crm"),
            ),
            // No registry: absent here is absent in the handshake, and the server decides.
            ("borg://localhost", WELL_KNOWN, None),
            ("borg://localhost/", WELL_KNOWN, None),
            ("borg:///crm", WELL_KNOWN, Some("crm")),
            // A socket whose last segment cannot be a registry name is all socket — which is the
            // case that makes the obvious spelling do the obvious thing.
            ("borg+unix:///tmp/borg.sock", "/tmp/borg.sock", None),
            ("borg+unix:///tmp/borg.sock/", "/tmp/borg.sock", None),
            // Both readings of an ambiguous path, said two ways.
            ("borg+unix:///run/borg/crm", "/run/borg", Some("crm")),
            ("borg+unix:///run/borg/crm/", "/run/borg/crm", None),
            // Registry names take the characters the server accepts, and no others.
            ("borg://localhost/my_app-2", WELL_KNOWN, Some("my_app-2")),
            // The websocket transport: a host, a port, and the same registry rule as everything
            // else. The port defaults the way `ws://`'s does, because a deployed server is behind
            // a proxy that already listens on 443.
            (
                "borg+ws://borg.example:7717/crm",
                "ws://borg.example:7717",
                Some("crm"),
            ),
            (
                "borg+ws://borg.example/crm",
                "ws://borg.example:80",
                Some("crm"),
            ),
            (
                "borg+wss://borg.example/crm",
                "wss://borg.example:443",
                Some("crm"),
            ),
            ("borg+ws://127.0.0.1:9000", "ws://127.0.0.1:9000", None),
        ];
        for (text, socket, registry) in table {
            assert_eq!(
                parsed(text),
                (socket.to_string(), registry.map(str::to_string)),
                "{text}"
            );
        }
    }

    /// Every refusal **quotes the url back**, because the url is usually in an environment variable
    /// rather than on the command line the person is looking at.
    #[test]
    fn a_url_that_is_not_one_says_so_and_quotes_itself() {
        let table = [
            ("/tmp/borg.sock", "it needs a scheme"),
            ("borg.sock", "it needs a scheme"),
            ("", "it needs a scheme"),
            (
                "postgres://localhost/crm",
                "`postgres` is not a borg transport",
            ),
            ("borg://example.com/crm", "`example.com` is not reachable"),
            ("borg://localhost/a/b", "more than one path segment"),
            ("borg://localhost/has.dot", "is not a registry name"),
            ("borg+unix://tmp/borg.sock", "three slashes"),
            ("borg+unix:///", "it names no socket path"),
            ("borg://localhost/crm?tls=1", "`?` has no meaning here"),
            ("borg+ws:///crm", "it names no host"),
            ("borg+ws://borg.example:http/crm", "`http` is not a port"),
            ("borg+ws://borg.example/a/b", "more than one path segment"),
        ];
        for (text, needle) in table {
            let refusal = ConnectionUrl::parse(text).unwrap_err().to_string();
            assert!(
                refusal.contains(needle),
                "parsing `{text}` should have mentioned `{needle}`, said: {refusal}"
            );
            assert!(
                refusal.contains(&format!("`{text}`")),
                "a refusal must quote the url back, said: {refusal}"
            );
        }
    }

    /// **The transport that was reserved is the transport that arrived, at the spelling that was
    /// reserved for it.** Two milestones of `borg+ws://` being parsed and refused by name is what
    /// makes this a match arm rather than a migration — nobody had invented a different spelling in
    /// the meantime, because the refusal named this one.
    #[test]
    fn the_reserved_websocket_spelling_is_the_one_that_now_connects() {
        let url = ConnectionUrl::parse("borg+ws://borg.example/crm").unwrap();
        assert_eq!(
            url.transport,
            Transport::Ws {
                secure: false,
                host: "borg.example".into(),
                port: 80
            }
        );
        assert_eq!(url.registry.as_deref(), Some("crm"));
        assert_eq!(
            url.address(Path::new(WELL_KNOWN)).ws_url().as_deref(),
            Some("ws://borg.example:80/"),
            "the request path is `/` — the registry travels in the handshake and nowhere else"
        );
    }

    /// Nothing here defaults a registry. The server's n=1 convenience and n≥2 refusal are one rule
    /// living in one place (§17.6), and a client that guessed would be disagreeing with half of it.
    #[test]
    fn an_absent_registry_is_absent_rather_than_guessed() {
        for text in ["borg://localhost", "borg+unix:///tmp/borg.sock"] {
            assert_eq!(ConnectionUrl::parse(text).unwrap().registry, None, "{text}");
        }
    }

    /// The sentence FRICTION recorded the absence of. Asserted here because two binaries and an SDK
    /// have to say it identically for it to be recognisable.
    #[test]
    fn nothing_listening_says_how_to_start_one() {
        let socket = Address::Unix(PathBuf::from("/tmp/borg.sock"));
        let absent = std::io::Error::from(std::io::ErrorKind::NotFound);
        let refused = std::io::Error::from(std::io::ErrorKind::ConnectionRefused);
        for err in [absent, refused] {
            let said = unreachable(&socket, &err).to_string();
            assert_eq!(
                said,
                "no borg server at /tmp/borg.sock — start one with: borg-server start"
            );
        }
        // Anything else is its own news and is reported as itself.
        let denied = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
        let said = unreachable(&socket, &denied).to_string();
        assert!(said.contains("/tmp/borg.sock"), "{said}");
        assert!(!said.contains("borg-server start"), "{said}");

        // **The same sentence over a websocket**, because what makes it worth having is that it is
        // recognisable, and a second wording for the remote case would cost exactly that.
        let remote = Address::Ws {
            secure: false,
            host: "borg.example".into(),
            port: 7717,
        };
        assert_eq!(
            unreachable(
                &remote,
                &std::io::Error::from(std::io::ErrorKind::ConnectionRefused)
            )
            .to_string(),
            "no borg server at ws://borg.example:7717 — start one with: borg-server start"
        );
    }
}
