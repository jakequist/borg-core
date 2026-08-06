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

/// The **client** wire contract — `borg-server` and the SDKs — over this same framing.
///
/// A separate module rather than more variants here, because the two protocols answer to different
/// people: this one is `ProducerCtx` over a pipe and is spoken by code the engine invoked, that one
/// is the transaction surface over a socket and is spoken by code that invoked the engine. What they
/// share is the framing, the codecs and the single-key rule, and sharing those is the whole point of
/// them living in one crate.
pub mod client;

/// **How a client is told where to connect** — one string, like `DATABASE_URL` (SPEC.md §17.7).
///
/// Beside the protocol rather than inside it because a URL never crosses the wire: it is what a
/// client parses in order to *decide* what to dial and what to put in `ClientHello::registry`. It
/// lives here because there must be one parser per language, and this crate is the one both the
/// `borg` CLI and `borg-server` already depend on.
pub mod url;

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

/// Where a worker expects to find the message stream. SPEC.md §17.4.
///
/// **Declared, never sniffed.** The engine learns a worker's transport from its `describe` output,
/// before it spawns anything, rather than by watching to see which channel answers first. Sniffing
/// looks cheaper and is the trap: the failure it would have to distinguish — a worker that prints to
/// stdout before it has connected — is exactly the failure a socket exists to make harmless, so the
/// detector would be broken by the thing it was detecting. A declaration cannot race.
///
/// Absent means [`Transport::Stdio`], so a worker written before this existed keeps working with no
/// change and pays nothing: no socket is created for it at all.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Transport {
    /// The worker's own stdin and stdout carry messages. Simple, and what a shell worker wants —
    /// at the cost that anything the worker prints for a human corrupts the stream.
    #[default]
    Stdio,
    /// The engine listens on a unix socket and passes its path in `BORG_WORKER_SOCKET`; the worker
    /// connects and speaks the identical protocol there. Its stdout is then its own, which is the
    /// only arrangement in which a `console.log` in a real client library is survivable.
    Socket,
}

/// The environment variable carrying the socket path, for [`Transport::Socket`] workers.
///
/// Named here rather than in the provider because it is contract: every SDK reads it, and a provider
/// is free to be replaced.
pub const SOCKET_ENV: &str = "BORG_WORKER_SOCKET";

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
    Invoke {
        /// The producer's id, **as a string**, for the reason the producer table writes it as one:
        /// it is `producer_id(name)`, a hash, so it uses the whole `u64` range, and JSON has no
        /// integers. A worker that read it as a JSON number would get a value rounded to 53 bits —
        /// silently naming a producer that does not exist. That corrupts nothing today only because
        /// every existing worker implements exactly one producer and ignores the field; a worker
        /// that serves a whole repo has to dispatch on it.
        ///
        /// A producer id is one identity and never arithmetic, so a string loses nothing.
        #[serde(with = "id_as_string")]
        producer: u64,
        input: String,
    },
    /// The answer to a `Get`. `None` means the cell has never been written — distinct from a
    /// tombstone, which arrives as the value `"~"`.
    Value(Option<String>),
    /// The acknowledgement of a `Set`.
    Ok {},
    /// No more work. The worker should exit.
    Shutdown {},
}

/// A `u64` identity that must survive a JSON round trip through a client that has only doubles.
/// See [`ToWorker::Invoke::producer`].
mod id_as_string {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(id: &u64, out: S) -> Result<S::Ok, S::Error> {
        out.serialize_str(&id.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(input: D) -> Result<u64, D::Error> {
        // MessagePack has integers and writes one, so both forms have to be readable — the wire is
        // one message type across every codec, and only the JSON view needs the string.
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Written {
            Text(String),
            Number(u64),
        }
        match Written::deserialize(input)? {
            Written::Text(text) => text.parse().map_err(serde::de::Error::custom),
            Written::Number(id) => Ok(id),
        }
    }
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
    /// How this executable wants to be spoken to once it is running as a worker. See [`Transport`].
    ///
    /// It rides on `describe` because that is the one thing the engine asks an executable *before*
    /// it has to decide how to spawn it — and the decision has to be made before the spawn, or
    /// stdout has already been claimed.
    #[serde(default)]
    pub transport: Transport,
    /// The repo id this executable believes it belongs to, if it says.
    ///
    /// The authoritative id is the one in `borg.toml`, because a repo is a directory and one
    /// directory has one id however many executables it contains. This is a cross-check: an SDK that
    /// makes the author write the id in code as well should have that copy verified rather than
    /// quietly ignored. Absent — every shell worker — skips the check.
    #[serde(default)]
    pub repo: Option<u32>,
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
    /// See [`ProducerSpec::fingerprint`]. A migration is a producer (SPEC.md §9.1) and its code
    /// changes the same way.
    #[serde(default)]
    pub fingerprint: Option<String>,
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
    /// **What this producer's code currently is**, as an opaque string that changes when the code
    /// changes (SPEC.md §9.2).
    ///
    /// Nothing interprets it: `borg repo push` compares it with the one in force and re-emits the
    /// producer's definition when the two differ. That is the whole mechanism, and it is here rather
    /// than beside the command on disk because *which program this is* belongs to the producer's
    /// definition — a fact about the repo, forkable and mergeable — while *where the program lives*
    /// is a fact about one machine and stays in the sidecar (§9.2).
    ///
    /// A repo that omits it is not opting out: `borg repo push` falls back to hashing the command
    /// file, which is what gives a `jq`-and-`bash` repo coverage without asking it to compute a
    /// digest. An SDK supplies its own only where it can cover **more** than the one file — see the
    /// two SDKs, which state exactly what each reaches.
    #[serde(default)]
    pub fingerprint: Option<String>,
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
/// The fingerprint of some bytes of implementation. SPEC.md §9.2.
///
/// `sha256:<hex>`, tagged rather than bare so that the string says what produced it. Both SDKs emit
/// the same form, which is what makes "these two came from the same kind of thing" checkable by eye
/// rather than by folklore.
///
/// **The tag is not a promise that two producers of it agree.** This function hashes the bytes it is
/// given and nothing else; the Python SDK folds several files and their names into one digest, so
/// its answer for a one-file repo is deliberately *not* this function's. Nothing compares
/// fingerprints across producers or across sources — the only comparison anyone makes is one
/// producer's new fingerprint against its own previous one — so agreement would buy nothing and
/// pretending to it would cost the Python SDK its module graph. The one visible consequence is that
/// a repo which stops supplying its own fingerprint recomputes once as the fallback takes over,
/// which is the same one-time cost as arriving with no fingerprint at all.
///
/// SHA-256 because it is already in the workspace for content addressing, and because a collision
/// here is a code change that invalidates nothing — silent, and exactly the failure fingerprints
/// exist to remove. Worth more than the microseconds a weaker hash saves on one file read per push.
pub fn fingerprint(bytes: &[u8]) -> String {
    let mut out = String::from("sha256:");
    for byte in borg_core::content::hash(bytes) {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

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

    /// The id that started this, in `producers.json` and now here: `producer_id("invest")` is past
    /// 2⁵³, so a worker dispatching on a JSON *number* would resolve a producer that does not exist.
    #[test]
    fn an_invocation_names_its_producer_in_a_form_a_json_client_cannot_round() {
        let invoke = ToWorker::Invoke {
            producer: producer_id("invest"),
            input: "Company:o-04068".into(),
        };
        let mut buffer = Vec::new();
        write_message(&mut buffer, Codec::Json, &invoke).unwrap();
        let line = String::from_utf8(buffer).unwrap();
        assert!(
            line.contains(&format!(r#""producer":"{}""#, producer_id("invest"))),
            "{line}"
        );

        // And every codec still carries the same value, whichever form it uses underneath.
        for codec in [Codec::Json, Codec::Msgpack] {
            let mut buffer = Vec::new();
            write_message(&mut buffer, codec, &invoke).unwrap();
            let mut cursor = std::io::Cursor::new(buffer);
            let back: ToWorker = read_message(&mut cursor, codec).unwrap();
            let ToWorker::Invoke { producer, .. } = back else {
                panic!("{} lost the variant", codec.name())
            };
            assert_eq!(producer, producer_id("invest"), "{}", codec.name());
        }
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

    /// The compatibility claim the socket transport rests on: a `describe` payload written before
    /// transports existed — every shell worker in the repo — asks for stdio, and asks for no repo
    /// cross-check.
    #[test]
    fn a_describe_payload_that_says_nothing_asks_for_stdio() {
        let described: Description = serde_json::from_str(
            r#"{"structs":[],"producers":[{"name":"invest","source":"Company"}]}"#,
        )
        .unwrap();
        assert_eq!(described.transport, Transport::Stdio);
        assert_eq!(described.repo, None);
    }

    /// …and the declaration an SDK writes is one lowercase word, because it is read and written by
    /// hand as often as by a serializer.
    #[test]
    fn a_transport_is_declared_by_name() {
        let described: Description =
            serde_json::from_str(r#"{"transport":"socket","repo":2}"#).unwrap();
        assert_eq!(described.transport, Transport::Socket);
        assert_eq!(described.repo, Some(2));
        assert!(
            serde_json::to_string(&described)
                .unwrap()
                .contains(r#""transport":"socket""#)
        );
    }

    #[test]
    fn producer_ids_are_stable_across_pushes() {
        let spec = |name: &str| ProducerSpec {
            name: name.into(),
            source: "Company".into(),
            fingerprint: None,
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
