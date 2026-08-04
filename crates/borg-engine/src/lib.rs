//! # borg-engine
//!
//! The engine: log, branches, derivation, and the read path.
//!
//! These are deliberately **modules rather than separate crates**. The resolver calls the scheduler,
//! the scheduler commits layers, and the derivation cycle is intertwined by nature; splitting it now
//! would buy circular-dependency pain and `pub use` plumbing and nothing else. Promote a module to a
//! crate once its boundary has held still. The *trait* crates (`borg-storage`, `borg-exec`) are
//! separate from day one, because those are the actual swappability seams.
//!
//! ## The derivation cycle
//!
//! This loop is the system, and it is the only genuinely unproven part of the design:
//!
//! ```text
//!   layer committed
//!        │
//!        ▼
//!   Invalidator ──lookup──► DependencyIndex
//!        │                    fwd: invalidation
//!        │                    bwd: lineage
//!        ▼
//!   Scheduler ◄── ProducerPolicyProvider
//!        │
//!        ▼
//!   ProducerRuntime   opens a layer, runs user code through ProducerCtx
//!        │
//!        └──────────► commits ──┐
//!                               │
//!        ┌──────────────────────┘
//!        ▼
//!   (triggers the next producers)
//! ```
//!
//! See `SPEC.md` §16 for the full architecture and §16.3 for the invariants that hold it together.

pub mod branch;
pub mod defs;
pub mod derive;
pub mod index;
pub mod log;
pub mod registry;
pub mod resolve;
pub mod seams;
pub mod touch;
pub mod values;

pub use branch::BranchManager;
pub use defs::{DefRegistry, DefView, MigrationHop, VersionStep};
pub use derive::DerivationEngine;
pub use index::{DependencyIndexProvider, Invocation, MemoryDependencyIndex};
pub use log::{LayerHandle, LayerManager};
pub use registry::Registry;
pub use resolve::FrontierTracker;
pub use resolve::{Lineage, LineageEdge, Resolver};
pub use seams::InProcessSequencer;
pub use seams::{LayerSequencer, LockManager, WorkGap, WorkSource};
pub use touch::CellTouchIndex;
pub use values::Values;
