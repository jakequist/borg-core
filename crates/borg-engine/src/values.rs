//! Text ↔ value, where the conversion needs the store. SPEC.md §3.1, §3.4, §4.2.
//!
//! `String`, `Binary` and `BigInt` are content-addressed: their PID *is* the hash of their content
//! (§3.1), so text cannot become a [`Value`] without a store to intern into, and a stored `Ref` to a
//! content PID cannot become text again without one to read from. Everything else — primitives,
//! references, tombstones — converts purely, in `borg_core::parse`.
//!
//! ## Why interning lives here and not somewhere more obvious
//!
//! Two other placements were considered and are wrong:
//!
//! * **`ProducerCtx` alone.** It is the only channel a producer runtime has to the store, so it does
//!   have to expose interning — but it is not the only writer. `borg set` writes source cells
//!   without any `ProducerCtx` in sight, and duplicating the rule there is exactly how two dialects
//!   start. `RecordingCtx` therefore *delegates* here rather than implementing it.
//! * **The resolver, on read.** Resolution deals in `Value`, which is the engine's internal
//!   currency: `validate` compares records, `explain` walks the index, one producer consumes
//!   another's output. Rendering content into text there would push every internal consumer through
//!   a string round trip to serve two edges — the CLI and the wire — that are the only ones who
//!   want text at all.
//!
//! So: interning happens on the way in, at whatever surface accepts text, and resolution happens on
//! the way out, at whatever surface emits it. Both go through this one type, and the layers between
//! them see nothing but `Value`.
//!
//! ## Interning is invisible
//!
//! A worker asking for `company.website` is answered `acme.ai`, not `@s-1a2b3c`, and a worker
//! writing `acme.ai` is done — no second round trip to create the string first, and nothing to know
//! about interning at all. That is the same call as batching in §17.1: a runtime concern, not a user
//! concern.

use borg_core::{Pid, Result, Value, ValueInput, parse};
use borg_storage::StorageProvider;
use std::sync::Arc;

pub struct Values {
    storage: Arc<dyn StorageProvider>,
}

impl Values {
    pub const fn new(storage: Arc<dyn StorageProvider>) -> Self {
        Self { storage }
    }

    /// Turn a parsed value into a storable one, interning content on the way.
    ///
    /// Interning is not layered and takes effect immediately (§17.1), so this is safe to call before
    /// a layer is open and costs nothing but space if that layer is later aborted.
    pub async fn intern(&self, input: ValueInput) -> Result<Value> {
        Ok(match input {
            ValueInput::Immediate(value) => value,
            ValueInput::Content { kind, bytes } => {
                Value::Ref(self.storage.intern(kind, &bytes).await?)
            }
        })
    }

    /// Render a value as the text `borg_core::parse::value` accepts, resolving interned content.
    ///
    /// A content PID whose bytes this store has never seen falls back to `@s-…`. That is not a
    /// failure: §17.1 makes a `read_interned` miss a legitimate answer, because a PID travels
    /// further than the bytes behind it, and `@s-…` is then the most honest thing to say.
    pub async fn render(&self, value: &Value) -> Result<String> {
        if let Some(pid) = self.content_pid(value)
            && let Some(bytes) = self.storage.read_interned(&pid).await?
        {
            return Ok(parse::render_interned(&pid, &bytes));
        }
        Ok(parse::render(value))
    }

    /// The content-addressed PID a value stores, if it stores one. This is what a client-facing
    /// surface shows to prove that two equal strings are one stored value.
    pub fn content_pid(&self, value: &Value) -> Option<Pid> {
        value
            .as_ref_pid()
            .copied()
            .filter(|pid| pid.kind().is_content_addressed())
    }
}
