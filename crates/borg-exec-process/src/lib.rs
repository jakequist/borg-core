//! # borg-exec-process
//!
//! An `ExecutionProvider` that runs producers as **separate processes**, speaking
//! [`borg_protocol`] over their stdio. SPEC.md §17.3.
//!
//! This is not a mock of the eventual container runtime — it is that runtime with a different
//! launcher. `ExecutionProvider` is defined as *"run this code, mediating every cell access through
//! me"*, and a pipe mediates exactly as well as a function call does.
//!
//! ## Workers are stateless
//!
//! A worker holds nothing between invocations, which is what makes it safe to spawn, kill and run
//! several at once. Two things already in the design cash that in: deterministic output PIDs (§9.5)
//! mean two workers racing the same invocation produce identical output, and the scheduler is
//! stateless (§16.4), so a lost worker costs a retry and nothing else.
//!
//! Reuse across invocations is therefore an *optimisation*, not a semantic. This provider keeps
//! workers alive because spawning a process per entity would dominate everything else, and it may
//! discard any of them at any point without consequence.
//!
//! ## Workers are a pool, not a worker
//!
//! One worker behind a lock would serialise every invocation through one subprocess whatever the
//! scheduler did, so the pool is the point rather than a refinement. Each command gets its own pool:
//! a permit bounds how many of its processes exist, and idle workers are handed back rather than
//! respawned.
//!
//! **A worker that errored is dropped rather than returned.** The protocol is a request/response
//! stream over a pipe, so a failed invocation leaves it at an unknown offset; reusing it would feed
//! one invocation's reply to the next one. Spawning a replacement is the cheap, obviously-correct
//! answer, and it costs nothing that is not already being paid on an error path.
//!
//! **The pipe is read with blocking I/O on the async runtime.** That was invisible while one worker
//! served everything and is worth naming now that several do: a pool of `n` occupies `n` runtime
//! threads waiting on `read`, which costs latency for anything else scheduled there. It cannot
//! deadlock — a blocked read is waiting on a subprocess, which needs nothing from this runtime to
//! proceed — and the fix is the same one that makes a socket transport worth having, so it is
//! deferred to that rather than paid for twice.
//!
//! ## stdout belongs to the protocol
//!
//! A worker's stdout carries messages, so anything it prints for humans must go to stderr. That is
//! a real trap for SDK authors — a stray `console.log` corrupts the stream — and the reason a
//! socket transport, where stdout stays entirely the user's, is the better default once real client
//! libraries exist.

use async_trait::async_trait;
use borg_core::{AllocatorId, BorgError, BranchId, Pid, Result, parse};
use borg_exec::{ExecutionProvider, ProducerCtx, ProducerRef, ValueCodec};
use borg_protocol::{
    Codec, Description, FromWorker, ServerHello, ToWorker, VERSION, WorkerHello, negotiate,
    read_message, write_message,
};
use std::collections::HashMap;
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, Mutex};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Codecs this engine offers, best first.
const OFFERED: [Codec; 2] = [Codec::Msgpack, Codec::Json];

fn exec(err: impl std::fmt::Display) -> BorgError {
    BorgError::Execution(err.to_string())
}

/// Ask an executable to describe the producers it implements.
///
/// Run once at push time, in a separate short-lived process, before any worker exists.
pub fn describe(command: &Path) -> Result<Description> {
    let output = Command::new(command)
        .arg("describe")
        .stdin(Stdio::null())
        .output()
        .map_err(|err| exec(format!("{}: {err}", command.display())))?;
    if !output.status.success() {
        return Err(exec(format!(
            "{} describe failed: {}",
            command.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    serde_json::from_slice(&output.stdout).map_err(|err| {
        exec(format!(
            "{} describe emitted unusable JSON: {err}",
            command.display()
        ))
    })
}

/// A live worker process and the codec it agreed to.
struct Worker {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    codec: Codec,
}

impl Worker {
    fn spawn(command: &Path) -> Result<Self> {
        let mut child = Command::new(command)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            // stderr is deliberately inherited: whatever a worker prints for humans should reach
            // the terminal rather than being swallowed or mistaken for a message.
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|err| exec(format!("{}: {err}", command.display())))?;

        let stdin = child.stdin.take().ok_or_else(|| exec("no stdin"))?;
        let stdout = BufReader::new(child.stdout.take().ok_or_else(|| exec("no stdout"))?);
        let mut worker = Self {
            child,
            stdin,
            stdout,
            codec: Codec::Json,
        };
        worker.handshake()?;
        Ok(worker)
    }

    /// Agree a codec. Always JSON, because a handshake cannot be encoded in something not yet
    /// agreed.
    fn handshake(&mut self) -> Result<()> {
        let hello = ServerHello {
            version: VERSION,
            codecs: OFFERED.iter().map(|c| c.name().to_string()).collect(),
        };
        write_message(&mut self.stdin, Codec::Json, &hello).map_err(exec)?;
        let reply: WorkerHello = read_message(&mut self.stdout, Codec::Json).map_err(exec)?;
        self.codec = negotiate(&OFFERED, &reply.codec).map_err(exec)?;
        Ok(())
    }

    fn send(&mut self, message: &ToWorker) -> Result<()> {
        write_message(&mut self.stdin, self.codec, message).map_err(exec)
    }

    fn receive(&mut self) -> Result<FromWorker> {
        read_message(&mut self.stdout, self.codec).map_err(exec)
    }

    fn shutdown(mut self) {
        let _ = write_message(&mut self.stdin, self.codec, &ToWorker::Shutdown {});
        let _ = self.stdin.flush();
        drop(self.stdin);
        let _ = self.child.wait();
    }
}

/// How many processes one command may have alive at once when nothing says otherwise.
///
/// One per core, matching the scheduler's own default: a pool smaller than the degree of parallelism
/// would reintroduce the queue the pool exists to remove, and a larger one only buys anything for a
/// worker that spends its time waiting on something other than us.
fn default_pool_size() -> usize {
    std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get)
}

/// The live processes for one command: a permit bounding how many exist, and the idle ones.
struct Pool {
    /// Holding a permit is the right to *have* a process, not the right to use a shared one — which
    /// is the difference between this and the single-worker mutex it replaces.
    permits: Arc<Semaphore>,
    idle: Mutex<Vec<Worker>>,
}

impl Pool {
    /// Take a worker, spawning one if the pool is under its bound and has none idle.
    async fn checkout(self: &Arc<Self>, command: &Path) -> Result<Checkout> {
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(exec)?;
        let idle = self.idle.lock().unwrap().pop();
        let worker = match idle {
            Some(worker) => worker,
            None => Worker::spawn(command)?,
        };
        Ok(Checkout {
            pool: Arc::clone(self),
            worker: Some(worker),
            _permit: permit,
        })
    }
}

/// A worker checked out of a pool. Killed on drop unless it was released first.
struct Checkout {
    pool: Arc<Pool>,
    worker: Option<Worker>,
    _permit: OwnedSemaphorePermit,
}

impl Checkout {
    fn worker(&mut self) -> &mut Worker {
        self.worker.as_mut().expect("held until released")
    }

    /// Give the worker back for the next invocation.
    fn release(mut self) {
        if let Some(worker) = self.worker.take() {
            self.pool.idle.lock().unwrap().push(worker);
        }
    }
}

impl Drop for Checkout {
    fn drop(&mut self) {
        // Reached only when `release` was not called — an error path. The stream is at an unknown
        // offset, so the process is killed rather than handed to the next invocation.
        if let Some(worker) = self.worker.take() {
            worker.shutdown();
        }
    }
}

pub struct ProcessExecutor {
    /// Producer id → the executable that implements it, and the struct it maps over. This mapping
    /// is the provider's own business: the log records only that a producer exists (§9.2).
    commands: HashMap<u64, (PathBuf, String)>,
    /// One pool per command.
    pools: Mutex<HashMap<PathBuf, Arc<Pool>>>,
    pool_size: usize,
    branch: BranchId,
}

impl ProcessExecutor {
    /// `branch` is the branch the `Company#1` shorthand resolves against — the same one the CLI
    /// uses, so a worker naming a cell that way means what a human naming it means. Canonical
    /// addresses, which is what workers are actually handed, carry their own branch and ignore it.
    pub fn new(branch: BranchId) -> Self {
        Self {
            commands: HashMap::new(),
            pools: Mutex::new(HashMap::new()),
            pool_size: default_pool_size(),
            branch,
        }
    }

    /// How many processes one command may have alive at once.
    #[must_use]
    pub fn with_pool_size(mut self, workers: usize) -> Self {
        self.pool_size = workers.max(1);
        self
    }

    pub fn register(&mut self, producer: u64, command: PathBuf, source: String) {
        self.commands.insert(producer, (command, source));
    }

    fn pool(&self, command: &Path) -> Arc<Pool> {
        Arc::clone(
            self.pools
                .lock()
                .unwrap()
                .entry(command.to_path_buf())
                .or_insert_with(|| {
                    Arc::new(Pool {
                        permits: Arc::new(Semaphore::new(self.pool_size)),
                        idle: Mutex::new(Vec::new()),
                    })
                }),
        )
    }

    /// Stop every worker. Called when the engine is finished with them.
    pub async fn shutdown(&self) {
        for (_, pool) in self.pools.lock().unwrap().drain() {
            for worker in pool.idle.lock().unwrap().drain(..) {
                worker.shutdown();
            }
        }
    }
}

#[async_trait]
impl ExecutionProvider for ProcessExecutor {
    async fn run(
        &self,
        producer: &ProducerRef,
        input: Pid,
        ctx: &mut dyn ProducerCtx,
    ) -> Result<()> {
        let (command, source) = self.commands.get(&producer.id.0).cloned().ok_or_else(|| {
            exec(format!(
                "no implementation registered for {:?}",
                producer.id
            ))
        })?;

        let pool = self.pool(&command);
        let mut checkout = pool.checkout(&command).await?;

        // The worker is handed the *entity* in the canonical text form and appends field names to
        // it — so `Company:o-1234abcd` is exactly the prefix it needs, and it never has to know
        // which branch it is running on.
        checkout.worker().send(&ToWorker::Invoke {
            producer: producer.id.0,
            input: borg_core::CellRef::existence(source.into(), input).to_string(),
        })?;

        // Service the worker's cell access until it says it is finished. Every read and write goes
        // through `ctx`, so dependency capture is exactly as automatic here as it is in-process.
        //
        // The `?`s below all leave the loop without releasing the checkout, so the process is killed
        // on the way out: a conversation abandoned half way through is not one the next invocation
        // can inherit. Only the two deliberate returns hand the worker back.
        loop {
            match checkout.worker().receive()? {
                // Text in both directions, and interning is the engine's business, not the
                // worker's: a string cell answers with `acme.ai` rather than `@s-1a2b3c` plus a
                // round trip to resolve it, and a worker writing `acme.ai` is finished (§3.4).
                FromWorker::Get(cell) => {
                    let cell = parse::cell_ref(&cell, self.branch, AllocatorId(0))?;
                    let value = ctx.get(&cell).await?;
                    let rendered = render(ctx, value).await?;
                    checkout.worker().send(&ToWorker::Value(rendered))?;
                }
                // The one message a migration needs and a pipeline never sends: the same cell at the
                // version this producer reads from, whichever side of the step that is (§9.3).
                FromWorker::GetInput(cell) => {
                    let cell = parse::cell_ref(&cell, self.branch, AllocatorId(0))?;
                    let value = ctx.get_input(&cell).await?;
                    let rendered = render(ctx, value).await?;
                    checkout.worker().send(&ToWorker::Value(rendered))?;
                }
                // The text goes across untouched: parsing it needs the field's declared type, which
                // lives on the other side of `ctx` (§3.4). Guessing here would give a worker a
                // different value model from the CLI's for the same text.
                FromWorker::Set { cell, value } => {
                    let cell = parse::cell_ref(&cell, self.branch, AllocatorId(0))?;
                    ctx.set_text(&cell, &value).await?;
                    checkout.worker().send(&ToWorker::Ok {})?;
                }
                FromWorker::Done {} => {
                    checkout.release();
                    return Ok(());
                }
                FromWorker::Error { message } => {
                    // The worker reported failure rather than desynchronising, so it is still usable
                    // — a producer that raises on one entity is not a broken process.
                    checkout.release();
                    return Err(BorgError::ProducerFailed {
                        producer: producer.id,
                        message,
                    });
                }
            }
        }
    }
}

/// A cell value as the text a worker reads. Interning stays the engine's business (§3.4).
///
/// Takes the **codec** and not the whole context, because rendering is all it does: nothing here can
/// reach a cell, and so nothing here can record a dependency the worker did not ask for.
async fn render(
    codec: &mut dyn ValueCodec,
    value: Option<borg_core::Value>,
) -> Result<Option<String>> {
    match value {
        Some(value) => Ok(Some(codec.render(&value).await?)),
        None => Ok(None),
    }
}

/// Build an executor from producer registrations.
pub fn from_registrations(
    branch: BranchId,
    registrations: impl IntoIterator<Item = (u64, PathBuf, String)>,
) -> ProcessExecutor {
    let mut executor = ProcessExecutor::new(branch);
    for (producer, command, source) in registrations {
        executor.register(producer, command, source);
    }
    executor
}
