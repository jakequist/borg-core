//! The wire contract between a **client** and `borg serve`. SDK-DRAFT.md §2.5, §3.
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

use serde::{Deserialize, Serialize};

/// The client protocol's version, negotiated separately from the worker protocol's [`crate::VERSION`]
/// even though both currently read `1`. They are two contracts over one framing and there is no
/// reason they should have to move together.
pub const VERSION: u32 = 1;

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
    /// A struct's definition, structured — this is what codegen reads (SPEC.md §15, SDK-DRAFT §4.4).
    DefShow {
        #[serde(default)]
        branch: Option<String>,
        /// Spelled `struct` on the wire even though that is a Rust keyword, following `type` in
        /// [`FieldSpec`](crate::FieldSpec): a client writing this by hand in `jq` should not have to
        /// know what Rust reserves.
        #[serde(rename = "struct")]
        struct_name: String,
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
    Head {
        branch: String,
        layer: String,
    },
    Def(StructDef),
    Lineage(Lineage),
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
            Request::DefShow {
                branch: None,
                struct_name: "Company".into(),
            },
        ]
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
            Response::Head {
                branch: "b1".into(),
                layer: "L500".into(),
            },
            Response::Def(StructDef {
                name: "Company".into(),
                fields: vec![FieldDef {
                    name: "headcount".into(),
                    ty: "Int".into(),
                    derived_by: None,
                    repo: 1,
                    version: "L4".into(),
                }],
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

        // An un-generated client has no ClientVersion to state, and must not have to invent one.
        let hello: ClientHello = serde_json::from_str("{}").unwrap();
        assert_eq!(hello.version, VERSION);
        assert_eq!(hello.codec, "json");
        assert!(hello.client_version.is_none());
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
