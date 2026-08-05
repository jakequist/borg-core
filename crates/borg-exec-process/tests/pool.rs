//! The worker pool. SPEC.md §17.3.
//!
//! A round runs its invocations concurrently (§16.5), so a provider that kept one worker behind a
//! lock would serialize every one of them through one subprocess whatever the scheduler decided —
//! the queue, put back below the seam. These tests are here rather than in a scenario because the
//! CLI writes one layer per `borg set`, so a wave through the CLI is usually one invocation wide;
//! what the pool exists for only shows up when one source layer dirties many.

use async_trait::async_trait;
use borg_core::{
    BranchId, CellRef, ClientVersion, DefVersion, LayerId, Pid, PidKind, ProducerId, Result, Value,
};
use borg_exec::{ExecutionProvider, ProducerCtx, ProducerRef, ValueCodec};
use borg_exec_process::{ProcessExecutor, Registration};
use std::collections::HashSet;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const PRODUCER: ProducerId = ProducerId(1);
const BRANCH: BranchId = BranchId(1);

/// A worker that records which process served each invocation and does nothing else.
///
/// It sleeps before answering, so that an executor holding one worker cannot hide behind being fast:
/// with one process the wall clock is `n × 50ms`, with a pool it is `n / pool × 50ms`, and the pid
/// log says which happened.
fn worker(dir: &Path, log: &Path) -> PathBuf {
    let path = dir.join("worker.sh");
    let script = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
IFS= read -r _hello
printf '%s\n' '{{"codec":"json"}}'
while IFS= read -r msg; do
  case "$msg" in
    *shutdown*) exit 0 ;;
  esac
  sleep 0.05
  printf '%s\n' "$$" >> {log}
  printf '%s\n' '{{"done":{{}}}}'
done
"#,
        log = log.display()
    );
    let mut file = std::fs::File::create(&path).expect("write worker");
    file.write_all(script.as_bytes()).expect("write worker");
    drop(file);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    path
}

/// A `ProducerCtx` that mediates nothing, because this worker accesses nothing.
struct Silent;

#[async_trait]
impl ProducerCtx for Silent {
    async fn get(&mut self, _cell: &CellRef) -> Result<Option<Value>> {
        Ok(None)
    }
    async fn get_at(&mut self, _cell: &CellRef, _version: DefVersion) -> Result<Option<Value>> {
        Ok(None)
    }
    async fn get_input(&mut self, _cell: &CellRef) -> Result<Option<Value>> {
        Ok(None)
    }
    async fn set(&mut self, _cell: &CellRef, _value: Value) -> Result<()> {
        Ok(())
    }
    async fn set_text(&mut self, _cell: &CellRef, _text: &str) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl ValueCodec for Silent {
    async fn intern(&mut self, _input: borg_core::ValueInput) -> Result<Value> {
        unreachable!("this worker writes nothing")
    }
    async fn render(&mut self, _value: &Value) -> Result<String> {
        unreachable!("this worker writes nothing")
    }
}

fn entity(n: u64) -> Pid {
    Pid::Allocated {
        kind: PidKind::Object,
        branch: BRANCH,
        allocator: borg_core::AllocatorId(0),
        counter: n,
    }
}

/// Run `n` invocations — all at once, or one after another — and report the distinct processes that
/// served them.
async fn distinct_workers(pool_size: usize, invocations: u64, at_once: bool) -> usize {
    let dir = std::env::temp_dir().join(format!(
        "borg-pool-{}-{pool_size}-{invocations}-{at_once}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    let log = dir.join("pids");
    let command = worker(&dir, &log);

    let mut executor = ProcessExecutor::new(BRANCH).with_pool_size(pool_size);
    executor.register(Registration {
        producer: PRODUCER.0,
        command,
        source: "Company".into(),
        // The pool is transport-agnostic; the transport tests cover the other one.
        transport: borg_protocol::Transport::Stdio,
    });
    let executor = Arc::new(executor);

    let producer = ProducerRef {
        id: PRODUCER,
        version: ClientVersion(LayerId(1)),
    };
    if at_once {
        let mut running = tokio::task::JoinSet::new();
        for i in 0..invocations {
            let executor = Arc::clone(&executor);
            running.spawn(async move {
                executor
                    .run(&producer, entity(i), &mut Silent)
                    .await
                    .expect("the worker answered");
            });
        }
        while let Some(joined) = running.join_next().await {
            joined.expect("no invocation panicked");
        }
    } else {
        for i in 0..invocations {
            executor
                .run(&producer, entity(i), &mut Silent)
                .await
                .expect("the worker answered");
        }
    }
    executor.shutdown().await;

    let pids: HashSet<String> = std::fs::read_to_string(&log)
        .expect("the worker recorded itself")
        .lines()
        .map(str::to_owned)
        .collect();
    let served = std::fs::read_to_string(&log).unwrap().lines().count();
    assert_eq!(
        served, invocations as usize,
        "every invocation was served exactly once"
    );
    let _ = std::fs::remove_dir_all(&dir);
    pids.len()
}

/// The claim: invocations that run at once are served by processes that run at once.
///
/// This is the test that fails against the executor this replaced, which kept one worker behind a
/// `Mutex` — eight concurrent invocations, one process, one at a time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_invocations_are_served_by_concurrent_workers() {
    let workers = distinct_workers(4, 8, true).await;
    assert!(
        workers > 1,
        "eight concurrent invocations were served by {workers} process(es): the pool is a queue"
    );
}

/// And the pool is a bound, not a suggestion — a process per invocation would defeat the reuse the
/// provider exists to provide.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_pool_never_exceeds_its_size() {
    let workers = distinct_workers(2, 12, true).await;
    assert!(
        (1..=2).contains(&workers),
        "twelve invocations through a pool of two used {workers} processes"
    );
}

/// A worker is reused across invocations, which is the whole reason a pool exists rather than a
/// spawn per entity. Six invocations, none of them overlapping, and a pool of four: one process.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_invocation_at_a_time_reuses_one_worker() {
    assert_eq!(
        distinct_workers(4, 6, false).await,
        1,
        "invocations that never overlap need only ever have used one process"
    );
}
