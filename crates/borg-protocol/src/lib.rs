//! # borg-protocol
//!
//! The wire contract between the engine and a producer worker. SPEC.md §17.3.
//!
//! This is `ProducerCtx` over a pipe: the engine invokes, the worker asks for cells, the engine
//! answers. Nothing else. A worker holds no state between invocations, so the engine may spawn,
//! terminate and eventually parallelise them at will.
//!
//! ## One source of truth, several encodings
//!
//! The message types below are the contract. JSON and MessagePack are produced by the *same* serde
//! impls on the *same* types, so the two views cannot drift — there is no mapping document between
//! them, only one set of definitions. Adding a codec is adding a match arm.
//!
//! ## Cells and values travel as text
//!
//! A cell is `"Company:o-1234abcd.website"` and a value is `"9"`, `"true"`, `"@o-5678wxyz"`, `"~"`
//! or `"acme.ai"` — exactly the forms the CLI accepts, parsed and rendered by `borg_core::parse`.
//!
//! **A string is a string here.** `Get` on a string cell answers `{"value":"acme.ai"}`, not the
//! `@s-…` that is physically stored, and `Set` with `"acme.ai"` is complete — the engine interns it
//! before the write lands (SPEC.md §3.4). A worker never makes a second round trip to resolve or
//! create a string, and never learns that content addressing exists. Doing otherwise would put an
//! extra round trip on the hottest path in the protocol in exchange for exposing a storage detail.
//!
//! The cell form uses a colon rather than parentheses because a worker is expected to be a shell
//! script: `Company(o-1234abcd)` would need quoting everywhere it appeared, and a form that needs
//! quoting is one that will eventually be typed unquoted.
//!
//! This is a deliberate choice and it was forced by the target audience. A bash worker cannot
//! reasonably assemble the structural JSON of a `CellRef`, and a protocol only usable from a
//! generated client library is one whose complexity is hidden rather than absent. Text also removes
//! the ambiguity a JSON number would introduce between `Int` and `Double`, and keeps every encoding
//! carrying the identical shape.

use serde::{Deserialize, Serialize};
use std::io::{BufRead, Write};

pub const VERSION: u32 = 1;

/// How messages are encoded on the wire.
///
/// Framing is **per codec**, not universal: a text codec is newline-delimited so a shell worker can
/// use `read`, while a binary codec is length-prefixed. Forcing one framing scheme would make one of
/// those two unpleasant for no gain.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Codec {
    /// Newline-delimited JSON. The default, and the only one a shell worker needs.
    Json,
    /// Length-prefixed MessagePack.
    Msgpack,
}

impl Codec {
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "json" => Some(Self::Json),
            "msgpack" => Some(Self::Msgpack),
            _ => None,
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Msgpack => "msgpack",
        }
    }
}

/// The engine's opening message. Always JSON, whatever is negotiated for the body — a handshake
/// cannot be encoded in a codec that has not been agreed yet.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerHello {
    pub version: u32,
    /// Codecs the engine will accept, best first.
    pub codecs: Vec<String>,
}

/// The worker's reply, naming the one it chose.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerHello {
    #[serde(default = "default_codec")]
    pub codec: String,
}

fn default_codec() -> String {
    "json".to_string()
}

/// Engine → worker.
///
/// **Every message is a single-key object**, so a worker can dispatch on one key without caring
/// which message it received. That is why the payload-free variants below are written `Ok {}` rather
/// than as unit variants: serde encodes a unit variant as a bare string, and `{"ok"}`-shaped
/// messages force a shell worker to special-case them. Uniformity is worth two empty braces.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToWorker {
    /// Run this producer against this entity.
    Invoke { producer: u64, input: String },
    /// The answer to a `Get`. `None` means the cell has never been written — distinct from a
    /// tombstone, which arrives as the value `"~"`.
    Value(Option<String>),
    /// The acknowledgement of a `Set`.
    Ok {},
    /// No more work. The worker should exit.
    Shutdown {},
}

/// Worker → engine.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FromWorker {
    /// Read a cell. Recorded into the read-set whether or not it exists.
    Get(String),
    /// Read a cell at the version this producer takes its **input** at.
    ///
    /// Only a migration needs it, and every migration needs it in both directions: `up` reads the
    /// older version, `down` the newer, and neither should have to say which. The alternative — a
    /// `Get` carrying an explicit layer id — would put arithmetic over def-versions in a bash script
    /// to reach the one cell the migration was written to translate (SPEC.md §9.3).
    GetInput(String),
    /// Write a cell. Checked against field ownership.
    Set { cell: String, value: String },
    /// This invocation is finished; ready for another.
    Done {},
    /// This invocation failed. The engine aborts its layer and poisons the producer.
    Error { message: String },
}

/// What a repo reports when asked to `describe` itself.
///
/// This runs once at push time and is what `borg repo push` turns into **one def layer** — so a
/// producer's definition, and the declaration of the field it writes, come from the same artifact as
/// its implementation and land together or not at all (SPEC.md §5.2, §9.2).
///
/// **Structs are here because they have to be.** Once a write is validated against the def-view
/// (§8), a producer cannot write anything unless its output field is declared, and the repo
/// implementing the producer is the only thing that knows what that field is. `defs/*.json` becomes
/// one way of producing this rather than a parallel path — and a Python repo defining structs
/// through an SDK emits exactly the same shape from its runtime.
///
/// **A schema change is a diff, not an instruction.** A repo emits the shape it believes in now, and
/// `borg repo push` compares it with the definitions in force: a field nobody has declared becomes a
/// `DeclareField`, and one whose type has moved becomes a `MutateField` — which §6.1 says must be
/// accompanied by migrations, so the field names them. There is deliberately no way to spell "mutate
/// this field" directly. A repo does not know what it is mutating *from*; the branch does, and on
/// another branch the answer is different.
///
/// Every list defaults to empty: a repo of pure schema and a repo of pure code are both legitimate,
/// and neither should have to write `"structs": []` to say so.
///
/// ```json
/// { "structs": [ { "name": "Company", "fields": [
///       { "name": "website",       "type": "String" },
///       { "name": "founded",       "type": "Int", "up": "founded_up", "down": "founded_down" },
///       { "name": "is_investible", "type": "Bool", "derived_by": "invest" } ] } ],
///   "producers":  [ { "name": "invest",     "source": "Company" } ],
///   "migrations": [ { "name": "founded_up" }, { "name": "founded_down" } ] }
/// ```
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Description {
    #[serde(default)]
    pub structs: Vec<StructSpec>,
    #[serde(default)]
    pub producers: Vec<ProducerSpec>,
    /// Migrations this repo implements, named. Which field each bridges and in which direction comes
    /// from the field that names it as `up` or `down` — one source of truth, so the two cannot
    /// disagree, and a migration nothing names is a push-time error rather than dead code.
    #[serde(default)]
    pub migrations: Vec<MigrationSpec>,
}

/// A struct's fields, as one repo declares them. The namespace is flat and there is no `extends`:
/// two repos naming the same struct simply merge (SPEC.md §5.2).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StructSpec {
    pub name: String,
    pub fields: Vec<FieldSpec>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FieldSpec {
    pub name: String,
    /// `Int`, `Bool`, `Double`, `String`, `Binary`, `BigInt`, `Any`, or a struct name.
    ///
    /// Spelled `type` on the wire even though that is a Rust keyword: a repo author writing this by
    /// hand in `jq` should not have to know what Rust reserves.
    #[serde(rename = "type")]
    pub ty: String,
    /// The producer that writes this field, named as this repo names it. Absent means source data,
    /// written by clients (SPEC.md §8).
    ///
    /// A **name**, not an id: a repo knows what it calls its pipelines and should not have to
    /// compute the hash the engine turns that into.
    #[serde(default)]
    pub derived_by: Option<String>,
    /// The migration that carries existing values forward when this field's type changes (SPEC.md
    /// §6.1, §9.3). Required for a change to be pushable at all — a type that moves with no way to
    /// bring the data with it is not a schema change, it is data loss.
    #[serde(default)]
    pub up: Option<String>,
    /// The migration that carries values back, so clients authored against the previous version keep
    /// reading (SPEC.md §5.4). Optional, and omitting it is a decision: values written after the
    /// change become unreachable from those versions, and reads there report `broken` (§9.3, §10.4).
    #[serde(default)]
    pub down: Option<String>,
}

/// A migration this repo implements. See [`Description::migrations`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigrationSpec {
    /// Stable across pushes, and hashed to a `ProducerId` exactly as a pipeline's name is.
    pub name: String,
}

impl MigrationSpec {
    pub fn id(&self) -> u64 {
        producer_id(&self.name)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProducerSpec {
    /// Stable across pushes: the engine derives the producer's id from it, so re-pushing a repo
    /// updates a producer rather than creating a second one.
    pub name: String,
    /// The struct this producer maps over — its `ObjectBuffer` (SPEC.md §4.2).
    pub source: String,
}

impl ProducerSpec {
    /// A stable id for this producer.
    ///
    /// Derived rather than assigned so that pushing the same repo twice is idempotent, and so two
    /// stores of the same repo agree on ids without coordinating.
    pub fn id(&self) -> u64 {
        producer_id(&self.name)
    }
}

/// The id a producer name hashes to. SPEC.md §9.2.
///
/// A free function as well as a method because a `derived_by` names a producer by name, and
/// resolving it must produce the *same* id the producer's own definition gets — one hash, one
/// place, no chance of the field pointing at an owner that does not exist.
pub fn producer_id(name: &str) -> u64 {
    // FNV-1a. Small, stable, and dependency-free; nothing here needs cryptographic strength.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in name.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    // Keep it away from the small ids a human might type into a def file.
    hash | (1 << 32)
}

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("encoding: {0}")]
    Encoding(String),
    #[error("the worker closed the connection")]
    Closed,
    #[error("the worker offered no codec we speak (we speak: {0})")]
    NoSharedCodec(String),
}

type Result<T> = std::result::Result<T, ProtocolError>;

/// Write one framed message.
pub fn write_message<T: Serialize, W: Write>(out: &mut W, codec: Codec, message: &T) -> Result<()> {
    match codec {
        Codec::Json => {
            let line = serde_json::to_string(message)
                .map_err(|e| ProtocolError::Encoding(e.to_string()))?;
            writeln!(out, "{line}")?;
        }
        Codec::Msgpack => {
            let body = rmp_serde::to_vec_named(message)
                .map_err(|e| ProtocolError::Encoding(e.to_string()))?;
            let len = u32::try_from(body.len())
                .map_err(|_| ProtocolError::Encoding("message too large".into()))?;
            out.write_all(&len.to_be_bytes())?;
            out.write_all(&body)?;
        }
    }
    out.flush()?;
    Ok(())
}

/// Read one framed message, or `Closed` at end of stream.
pub fn read_message<T: for<'de> Deserialize<'de>, R: BufRead>(
    input: &mut R,
    codec: Codec,
) -> Result<T> {
    match codec {
        Codec::Json => {
            let mut line = String::new();
            loop {
                line.clear();
                if input.read_line(&mut line)? == 0 {
                    return Err(ProtocolError::Closed);
                }
                // Blank lines are ignored rather than fatal: a shell worker echoing an empty string
                // should not take down the connection.
                if !line.trim().is_empty() {
                    break;
                }
            }
            serde_json::from_str(line.trim())
                .map_err(|e| ProtocolError::Encoding(format!("{e} in `{}`", line.trim())))
        }
        Codec::Msgpack => {
            let mut len = [0u8; 4];
            if let Err(err) = input.read_exact(&mut len) {
                return Err(if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    ProtocolError::Closed
                } else {
                    err.into()
                });
            }
            let mut body = vec![0u8; u32::from_be_bytes(len) as usize];
            input.read_exact(&mut body)?;
            rmp_serde::from_slice(&body).map_err(|e| ProtocolError::Encoding(e.to_string()))
        }
    }
}

/// Pick the codec a worker asked for, from the ones we offer.
pub fn negotiate(offered: &[Codec], chosen: &str) -> Result<Codec> {
    let codec = Codec::parse(chosen).filter(|c| offered.contains(c));
    codec.ok_or_else(|| {
        ProtocolError::NoSharedCodec(
            offered
                .iter()
                .map(|c| c.name())
                .collect::<Vec<_>>()
                .join(", "),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The point of the crate: every codec carries the identical message, because they are the same
    /// serde impl on the same type.
    #[test]
    fn every_codec_round_trips_the_same_messages() {
        let messages = vec![
            FromWorker::Get("Company#1.website".into()),
            FromWorker::GetInput("Company#1.founded".into()),
            FromWorker::Set {
                cell: "Company#1.is_investible".into(),
                value: "true".into(),
            },
            FromWorker::Done {},
            FromWorker::Error {
                message: "boom".into(),
            },
        ];

        for codec in [Codec::Json, Codec::Msgpack] {
            let mut buffer = Vec::new();
            for message in &messages {
                write_message(&mut buffer, codec, message).unwrap();
            }
            let mut cursor = std::io::Cursor::new(buffer);
            for expected in &messages {
                let got: FromWorker = read_message(&mut cursor, codec).unwrap();
                assert_eq!(
                    format!("{got:?}"),
                    format!("{expected:?}"),
                    "{} round trip",
                    codec.name()
                );
            }
        }
    }

    /// A shell worker writes these by hand, so the JSON shape is part of the contract.
    #[test]
    fn json_is_the_shape_a_shell_worker_would_write() {
        let mut buffer = Vec::new();
        write_message(
            &mut buffer,
            Codec::Json,
            &FromWorker::Get("Company#1.website".into()),
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(buffer).unwrap().trim(),
            r#"{"get":"Company#1.website"}"#
        );

        let mut buffer = Vec::new();
        write_message(&mut buffer, Codec::Json, &ToWorker::Value(Some("9".into()))).unwrap();
        assert_eq!(
            String::from_utf8(buffer).unwrap().trim(),
            r#"{"value":"9"}"#
        );

        // A migration's one extra verb, and it takes a cell and nothing else — no layer id for a
        // shell script to compute (SPEC.md §9.3).
        let mut buffer = Vec::new();
        write_message(
            &mut buffer,
            Codec::Json,
            &FromWorker::GetInput("Company#1.founded".into()),
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(buffer).unwrap().trim(),
            r#"{"get_input":"Company#1.founded"}"#
        );
    }

    /// The invariant a shell worker relies on: one key, always, so `jq 'keys[0]'` always works.
    #[test]
    fn every_message_is_a_single_key_object() {
        let mut buffer = Vec::new();
        for message in [ToWorker::Ok {}, ToWorker::Shutdown {}] {
            write_message(&mut buffer, Codec::Json, &message).unwrap();
        }
        write_message(&mut buffer, Codec::Json, &FromWorker::Done {}).unwrap();

        for line in String::from_utf8(buffer).unwrap().lines() {
            let parsed: serde_json::Value = serde_json::from_str(line).unwrap();
            let object = parsed
                .as_object()
                .unwrap_or_else(|| panic!("`{line}` is not an object — a unit variant leaked"));
            assert_eq!(object.len(), 1, "`{line}` should have exactly one key");
        }
    }

    #[test]
    fn producer_ids_are_stable_across_pushes() {
        let spec = |name: &str| ProducerSpec {
            name: name.into(),
            source: "Company".into(),
        };
        assert_eq!(spec("invest").id(), spec("invest").id());
        assert_ne!(spec("invest").id(), spec("score").id());
    }

    #[test]
    fn a_codec_we_do_not_speak_is_refused_by_name() {
        let error = negotiate(&[Codec::Json], "protobuf").unwrap_err();
        assert!(error.to_string().contains("json"), "{error}");
    }
}
