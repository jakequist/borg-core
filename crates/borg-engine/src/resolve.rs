//! The read path. SPEC.md §10, §11.
//!
//! Resolving a cell: locate the record, compose migrations across any def-version skew between the
//! stored version and the reader's ClientVersion, validate the watermark, and build the provenance
//! envelope.
//!
//! Reads **validate before reporting**, so the returned watermark is tight rather than
//! pessimistically understated. Validation runs no user code — it only checks the dependency index
//! for changes in `(fresh_as_of, target]`.

// TODO(v1): Resolver — resolve + explain.
// TODO(v1): FrontierTracker — per-producer watermarks, settled frontier, frontier.reaches().
// TODO(v1): migration path composition — down to the common ancestor, then up (§5.3).
