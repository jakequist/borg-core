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
//! A worker holds nothing between invocations, which is what makes it safe to spawn, kill and
//! eventually run several at once. Two things already in the design cash that in: deterministic
//! output PIDs (§9.5) mean two workers racing the same invocation produce identical output, and
//! the scheduler is stateless (§16.4), so a lost worker costs a retry and nothing else.
//!
//! Reuse across invocations is therefore an *optimisation*, not a semantic. This provider keeps one
//! worker alive because spawning a process per entity would dominate everything else, and it may
//! discard it at any point without consequence.
//!
//! ## stdout belongs to the protocol
//!
//! A worker's stdout carries messages, so anything it prints for humans must go to stderr. That is
//! a real trap for SDK authors — a stray `console.log` corrupts the stream — and the reason a
//! socket transport, where stdout stays entirely the user's, is the better default once real client
//! libraries exist.

use async_trait::async_trait;
use borg_core::{AllocatorId, BorgError, BranchId, Pid, Result, parse};
use borg_exec::{ExecutionProvider, ProducerCtx, ProducerRef};
use borg_protocol::{
    Codec, Description, FromWorker, ServerHello, ToWorker, VERSION, WorkerHello, negotiate,
    read_message, write_message,
};
use std::collections::HashMap;
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::Arc;
use tokio::sync::Mutex;

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

pub struct ProcessExecutor {
    /// Producer id → the executable that implements it, and the struct it maps over. This mapping
    /// is the provider's own business: the log records only that a producer exists (§9.2).
    commands: HashMap<u64, (PathBuf, String)>,
    /// One worker per command, reused across invocations.
    workers: Mutex<HashMap<PathBuf, Worker>>,
    branch: BranchId,
}

impl ProcessExecutor {
    /// `branch` is the allocation branch for cell shorthand — the same one the CLI uses, so a
    /// worker naming `Company#1` means what a human naming it means.
    pub fn new(branch: BranchId) -> Self {
        Self {
            commands: HashMap::new(),
            workers: Mutex::new(HashMap::new()),
            branch,
        }
    }

    pub fn register(&mut self, producer: u64, command: PathBuf, source: String) {
        self.commands.insert(producer, (command, source));
    }

    /// Stop every worker. Called when the engine is finished with them.
    pub async fn shutdown(&self) {
        for (_, worker) in self.workers.lock().await.drain() {
            worker.shutdown();
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

        let mut workers = self.workers.lock().await;
        if !workers.contains_key(&command) {
            workers.insert(command.clone(), Worker::spawn(&command)?);
        }
        let worker = workers.get_mut(&command).expect("just inserted");

        // The worker is handed the *entity* in the same text form a human would type, and appends
        // field names to it — so `Company#1` is exactly the prefix it needs.
        worker.send(&ToWorker::Invoke {
            producer: producer.id.0,
            input: borg_core::CellRef::existence(source.into(), input).to_string(),
        })?;

        // Service the worker's cell access until it says it is finished. Every read and write goes
        // through `ctx`, so dependency capture is exactly as automatic here as it is in-process.
        loop {
            match worker.receive()? {
                FromWorker::Get(cell) => {
                    let cell = parse::cell_ref(&cell, self.branch, AllocatorId(0))?;
                    let value = ctx.get(&cell).await?;
                    worker.send(&ToWorker::Value(value.as_ref().map(parse::render)))?;
                }
                FromWorker::Set { cell, value } => {
                    let cell = parse::cell_ref(&cell, self.branch, AllocatorId(0))?;
                    let value = parse::value(&value, self.branch, AllocatorId(0))?;
                    ctx.set(&cell, value).await?;
                    worker.send(&ToWorker::Ok {})?;
                }
                FromWorker::Done {} => return Ok(()),
                FromWorker::Error { message } => {
                    return Err(BorgError::ProducerFailed {
                        producer: producer.id,
                        message,
                    });
                }
            }
        }
    }
}

/// Build an executor from producer registrations.
pub fn from_registrations(
    branch: BranchId,
    registrations: impl IntoIterator<Item = (u64, PathBuf, String)>,
) -> Arc<ProcessExecutor> {
    let mut executor = ProcessExecutor::new(branch);
    for (producer, command, source) in registrations {
        executor.register(producer, command, source);
    }
    Arc::new(executor)
}
