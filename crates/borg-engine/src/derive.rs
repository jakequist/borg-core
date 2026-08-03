//! The derivation cycle. SPEC.md §9, §16.4, §16.5.
//!
//! Invalidation is driven by **layer commit**, not by buffer instrumentation: a committed layer *is*
//! the changeset, so one pass over it answers both trigger questions — cell writes dirty existing
//! invocations, and object creations produce new ones.
//!
//! The scheduler is **stateless**. Work is derived from watermark gaps rather than queued, which
//! bounds memory, makes crash recovery free, and lets workers derive their own work.

// TODO(v1): Invalidator — walk a committing layer through the forward index.
// TODO(v1): Scheduler — drive WorkGaps through ProducerPolicyProvider.
// TODO(v1): ProducerRuntime — the ProducerCtx impl that records reads and checks write ownership.
// TODO(v1): cycle detection — per-invocation re-run counter at a fixed head (§16.5).
