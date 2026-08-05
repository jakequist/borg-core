//! The files that sit **beside** the store, and the one rule they share.
//!
//! Three of them: the producer-implementation table (§9.2), the derivation config — pause flags and
//! poisonings (§14) — and the open transactions (§12). They are all the same kind of thing and are
//! all here for the same reason: this is **operational state, not log data**. None of it changes
//! what is true. In the log each would be forkable, mergeable and time-travellable, and *"which
//! transactions were open at layer 400"* is not a question anybody has.
//!
//! Each was carrying its own `…_path`, `load_…` and `save_…` triple, which is three places for the
//! naming convention to drift and three places to get the "a missing or corrupt file reads as the
//! default" rule slightly differently. It is one convention, so it is written once.
//!
//! ## A missing file is an empty file
//!
//! Reading is total: no file, unreadable file, or unparsable file all yield the default. A sidecar
//! holds nothing that cannot be recreated by doing the thing again — push the repo, pause the
//! branch, start the transaction — so failing a command over one would trade a recoverable state for
//! an unrecoverable one.

use borg_core::{BorgError, Result};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::path::{Path, PathBuf};

/// One file beside the store.
pub trait Sidecar: Default + Serialize + DeserializeOwned {
    /// What `borg.db` becomes for this file — `borg.producers.json` and so on. The store's own name
    /// is the stem, so two stores in one directory keep their sidecars apart without anybody
    /// configuring anything.
    const EXTENSION: &'static str;
}

pub fn path<S: Sidecar>(store: &Path) -> PathBuf {
    store.with_extension(S::EXTENSION)
}

/// Read a sidecar, or the default where there is nothing usable to read. See the header.
pub fn load<S: Sidecar>(store: &Path) -> S {
    std::fs::read_to_string(path::<S>(store))
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_default()
}

/// Write a sidecar back. Pretty-printed, because these are meant to be read and edited by hand when
/// something has gone wrong.
pub fn save<S: Sidecar>(store: &Path, value: &S) -> Result<()> {
    let raw =
        serde_json::to_string_pretty(value).map_err(|err| BorgError::Storage(err.to_string()))?;
    std::fs::write(path::<S>(store), raw).map_err(|err| BorgError::Storage(err.to_string()))
}

/// A producer id, as a **string**, in a file JSON tools are expected to read.
///
/// A `ProducerId` is `borg_protocol::producer_id(name)` — a hash — so it uses the whole `u64` range,
/// and JSON has no integers. `jq`, every browser, and most JSON libraries parse a number into a
/// double and silently round anything above 2⁵³: `12342029420047889112` came back out of
/// `producers.json` as `12342029420047890000`, which is a different producer, and the shell
/// pipelines these files exist to serve are exactly the readers that do it.
///
/// A producer id is one identity and never arithmetic, so a string loses nothing. Layer and branch
/// ids are left as numbers on purpose — they are sequential counters that will not reach 2⁵³ this
/// side of the heat death, and keeping them numeric is what lets `jq` sort and compare them.
///
/// **The old numeric form is still read**, so a store written by an earlier `borg` keeps working;
/// the next save rewrites it as a string. These are dev stores, but a sidecar that silently stopped
/// resolving a producer would look exactly like a producer that had never been pushed.
pub mod producer_id {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(id: &u64, out: S) -> Result<S::Ok, S::Error> {
        out.serialize_str(&id.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(input: D) -> Result<u64, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Written {
            Text(String),
            /// What `borg` wrote before this, and what it still accepts.
            Number(u64),
        }
        match Written::deserialize(input)? {
            Written::Text(text) => text.parse().map_err(serde::de::Error::custom),
            Written::Number(id) => Ok(id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The id that started this: `producer_id("invest")`, and the smallest thing that reproduces the
    /// corruption is a round trip through a JSON number.
    const BIG: u64 = 12_342_029_420_047_889_112;

    #[derive(Default, serde::Serialize, serde::Deserialize)]
    struct Row {
        #[serde(with = "producer_id")]
        id: u64,
    }

    #[test]
    fn a_producer_id_is_written_as_a_string() {
        let json = serde_json::to_string(&Row { id: BIG }).unwrap();
        assert_eq!(json, r#"{"id":"12342029420047889112"}"#);
    }

    #[test]
    fn a_producer_id_survives_a_round_trip_that_a_json_number_would_not() {
        let json = serde_json::to_string(&Row { id: BIG }).unwrap();
        assert_eq!(serde_json::from_str::<Row>(&json).unwrap().id, BIG);

        // The half that was broken: read as a double, as every JSON tool in a shell pipeline does.
        let as_number = format!(r#"{{"id":{BIG}}}"#);
        let rounded = serde_json::from_str::<f64>(&BIG.to_string()).unwrap() as u64;
        assert_ne!(rounded, BIG, "the number form is what loses the identity");
        // …and is still accepted, so a store written by an older borg keeps resolving its producers.
        assert_eq!(serde_json::from_str::<Row>(&as_number).unwrap().id, BIG);
    }
}
