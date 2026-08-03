//! Branches, forking, and merge. SPEC.md §7, §13.
//!
//! A fork is O(1) even under eager derivation: a new branch inherits its parent's derived layers by
//! ancestry exactly as it inherits source layers, and diverges only where it writes.
//!
//! Merge replays the child's **source** events onto the parent as new layers; derived layers are
//! skipped, because the child's derived values are wrong on the parent by construction. Guards
//! re-evaluated against the parent's history since the fork point *are* the conflict detector.

// TODO(v1): BranchManager — fork, ancestry, layer-chain resolution.
// TODO(v1): merge — def-only and def+data, with the four rejection rules of §13.
