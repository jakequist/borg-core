//! The log: layer lifecycle and the state machine. SPEC.md §6.
//!
//! A layer is the universal unit of atomicity — client transactions and producer runs follow the
//! same `open → sealed → committed | aborted` path, through one code path.
//!
//! Two constraints shape everything here:
//!
//! * **Commit streams.** A layer may hold millions of mutations and can never be buffered whole.
//! * **Locks are per-layer, never per-branch.** A branch-wide lock would serialize derivation.

// TODO(v1): LayerManager — open/seal/commit/abort over StorageProvider + LayerSequencer.
// TODO(v1): CellTouchIndex — `cell -> layers that wrote it`, backing guard validation (§12).
