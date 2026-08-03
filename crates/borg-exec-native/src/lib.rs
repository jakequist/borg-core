//! # borg-exec-native
//!
//! An `ExecutionProvider` that runs producers as in-process Rust functions under full trust.
//! SPEC.md §17.3.
//!
//! This is the *implementation* half of the definition/implementation split (SPEC.md §9.2): the log
//! records only that producer `P` exists at some ClientVersion, and this registry resolves that id
//! to code. A container-backed provider swaps in here without the log noticing.

use async_trait::async_trait;
use borg_core::{BorgError, Pid, ProducerId, Result};
use borg_exec::{ExecutionProvider, ProducerCtx, ProducerRef};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// A producer implementation: given a mediated view of the world and one input entity, read what it
/// needs and write its outputs.
pub type ProducerFn = Arc<
    dyn for<'a> Fn(
            &'a mut dyn ProducerCtx,
            Pid,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>
        + Send
        + Sync,
>;

#[derive(Default)]
pub struct NativeExecutor {
    registry: HashMap<ProducerId, ProducerFn>,
}

impl NativeExecutor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, id: ProducerId, f: ProducerFn) {
        self.registry.insert(id, f);
    }
}

#[async_trait]
impl ExecutionProvider for NativeExecutor {
    async fn run(
        &self,
        producer: &ProducerRef,
        input: Pid,
        ctx: &mut dyn ProducerCtx,
    ) -> Result<()> {
        let f = self
            .registry
            .get(&producer.id)
            .ok_or_else(|| {
                BorgError::Execution(format!("no implementation for {:?}", producer.id))
            })?
            .clone();
        f(ctx, input).await
    }
}
