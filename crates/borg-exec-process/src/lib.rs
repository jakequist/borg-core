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
//! ## Two transports, one protocol
//!
//! Over **stdio** a worker's stdout carries messages, so anything it prints for humans must go to
//! stderr. That is a trap a shell author can be told about once and remember; it is not survivable
//! in a real client library, where a `console.log` anywhere in a dependency corrupts the stream.
//!
//! So a worker may instead declare `"transport": "socket"` in its `describe` output, and the engine
//! **listens on a unix socket, one per worker process, and passes the path in `BORG_WORKER_SOCKET`**
//! before spawning it. Same handshake, same messages, same per-codec framing — only the file
//! descriptors differ. `describe` itself stays a plain `argv[1] == "describe"` invocation printing
//! JSON to stdout on both transports: that call has no stream to corrupt, and leaving it alone is
//! what keeps a bash repo one `jq -n`.
//!
//! **The transport is declared, not sniffed.** See [`borg_protocol::Transport`]: a detector would
//! have to tell "a worker that has not connected yet" from "a worker that printed to stdout first",
//! which is precisely the case the socket exists to make harmless. A worker that says nothing gets
//! stdio and no socket is created for it, so every existing shell worker is untouched and pays
//! nothing.
//!
//! **A socket worker's stdout is pointed at the engine's stderr.** Not inherited: the engine's own
//! stdout is a contract too — `borg get --value` is parsed by scripts — and handing a subprocess a
//! pipe into it would move the corruption up one level rather than removing it. Not swallowed
//! either, because a `console.log` a developer never sees is its own kind of bug. Redirecting costs
//! one `dup`, needs no reader thread, and puts human output exactly where this provider already
//! sends a worker's stderr.

use async_trait::async_trait;
use borg_core::{AllocatorId, BorgError, BranchId, Pid, Result, parse};
use borg_exec::{ExecutionProvider, ProducerCtx, ProducerRef, ValueCodec};
use borg_protocol::{
    Codec, Description, FromWorker, SOCKET_ENV, ServerHello, ToWorker, Transport, VERSION,
    WorkerHello, negotiate, read_message, write_message,
};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::os::fd::AsFd;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
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

/// How long a worker that declared a socket has to connect back before the engine gives up on it.
///
/// Generous, because it covers a language runtime starting cold — a Node process importing a repo
/// module is hundreds of milliseconds on a warm machine and worse on a loaded one — and because
/// nothing waits on it in the healthy case: the accept returns the moment the worker connects.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// A socket path that unlinks itself. One per worker process, so nothing is ever multiplexed and a
/// worker's identity is the connection it arrived on.
struct SocketPath(PathBuf);

impl Drop for SocketPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn next_socket_path() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    // Short on purpose: a unix socket path is capped near 108 bytes, well under what a descriptive
    // name in a nested temp directory would cost.
    std::env::temp_dir().join(format!(
        "borg-w{}-{}.sock",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

/// A live worker process, the streams that reach it, and the codec it agreed to.
///
/// The streams are boxed rather than an enum over the two transports because nothing above this
/// point may branch on which one is in use — the protocol is the same protocol, and a `match` on the
/// transport in `run` would be the first crack in that.
struct Worker {
    child: Child,
    /// Engine → worker.
    out: Box<dyn Write + Send>,
    /// Worker → engine.
    inp: Box<dyn BufRead + Send>,
    codec: Codec,
    /// Kept alive so the socket file outlives the worker that is using it, and no longer.
    _socket: Option<SocketPath>,
}

impl Worker {
    async fn spawn(command: &Path, transport: Transport, connect: Duration) -> Result<Self> {
        let mut worker = match transport {
            Transport::Stdio => Self::over_stdio(command),
            Transport::Socket => Self::over_socket(command, connect).await,
        }?;
        worker.handshake()?;
        Ok(worker)
    }

    fn over_stdio(command: &Path) -> Result<Self> {
        let mut child = spawn(
            Command::new(command)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                // stderr is deliberately inherited: whatever a worker prints for humans should
                // reach the terminal rather than being swallowed or mistaken for a message.
                .stderr(Stdio::inherit()),
            command,
        )?;

        let stdin = child.stdin.take().ok_or_else(|| exec("no stdin"))?;
        let stdout = child.stdout.take().ok_or_else(|| exec("no stdout"))?;
        Ok(Self {
            child,
            out: Box::new(stdin),
            inp: Box::new(BufReader::new(stdout)),
            codec: Codec::Json,
            _socket: None,
        })
    }

    /// Listen first, then spawn. The listener must exist before the child does or a fast worker
    /// races the engine to a socket that is not there yet.
    async fn over_socket(command: &Path, connect: Duration) -> Result<Self> {
        let path = next_socket_path();
        let listener = tokio::net::UnixListener::bind(&path)
            .map_err(|err| exec(format!("listening on {}: {err}", path.display())))?;
        let socket = SocketPath(path.clone());

        let mut child = spawn(
            Command::new(command)
                .env(SOCKET_ENV, &path)
                // stdin is closed rather than inherited: a worker that reads it would be competing
                // with the engine's own caller for a terminal, which is a worse theft than the one
                // this transport exists to prevent.
                .stdin(Stdio::null())
                // See the module header — the worker's stdout is the engine's stderr.
                .stdout(engine_stderr()?)
                .stderr(Stdio::inherit()),
            command,
        )?;

        let stream = Self::accept(&listener, &mut child, command, &path, connect).await?;

        // Back to blocking I/O, which is what the rest of this provider speaks. The module header
        // records why that is acceptable and what it costs.
        stream
            .set_nonblocking(false)
            .map_err(|err| exec(format!("{}: {err}", command.display())))?;
        let reader = stream
            .try_clone()
            .map_err(|err| exec(format!("{}: {err}", command.display())))?;

        Ok(Self {
            child,
            out: Box::new(stream),
            inp: Box::new(BufReader::new(reader)),
            codec: Codec::Json,
            _socket: Some(socket),
        })
    }

    /// Wait for the worker to connect, watching it for signs of having died first.
    ///
    /// The timeout alone would be correct and unusable: a pipeline with a syntax error never
    /// connects, and the honest report — it exited, with this status, and whatever it printed is
    /// already on your terminal — would arrive thirty seconds after the process that could have said
    /// so. Polling for the exit turns the common failure into an immediate one and leaves the
    /// timeout for the case it was meant for, which is a worker that is alive and simply not
    /// talking.
    async fn accept(
        listener: &tokio::net::UnixListener,
        child: &mut Child,
        command: &Path,
        path: &Path,
        connect: Duration,
    ) -> Result<std::os::unix::net::UnixStream> {
        let deadline = tokio::time::Instant::now() + connect;
        loop {
            tokio::select! {
                // `accept` is cancel-safe, so losing this race costs nothing.
                accepted = listener.accept() => {
                    return accepted
                        .map(|(stream, _)| stream)
                        .map_err(|err| exec(format!("{}: accept failed: {err}", command.display())))
                        .and_then(|stream| stream.into_std().map_err(|err| exec(err.to_string())));
                }
                () = tokio::time::sleep_until(deadline.min(
                    tokio::time::Instant::now() + Duration::from_millis(25),
                )) => {
                    if let Ok(Some(status)) = child.try_wait() {
                        return Err(exec(format!(
                            "{} exited ({status}) without connecting to {}",
                            command.display(),
                            path.display()
                        )));
                    }
                    if tokio::time::Instant::now() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        return Err(exec(format!(
                            "{} declared `\"transport\": \"socket\"` but did not connect to {} \
                             within {}s",
                            command.display(),
                            path.display(),
                            connect.as_secs_f32()
                        )));
                    }
                }
            }
        }
    }

    /// Agree a codec. Always JSON, because a handshake cannot be encoded in something not yet
    /// agreed.
    fn handshake(&mut self) -> Result<()> {
        let hello = ServerHello {
            version: VERSION,
            codecs: OFFERED.iter().map(|c| c.name().to_string()).collect(),
        };
        write_message(&mut self.out, Codec::Json, &hello).map_err(exec)?;
        let reply: WorkerHello = read_message(&mut self.inp, Codec::Json).map_err(exec)?;
        self.codec = negotiate(&OFFERED, &reply.codec).map_err(exec)?;
        Ok(())
    }

    fn send(&mut self, message: &ToWorker) -> Result<()> {
        write_message(&mut self.out, self.codec, message).map_err(exec)
    }

    fn receive(&mut self) -> Result<FromWorker> {
        read_message(&mut self.inp, self.codec).map_err(exec)
    }

    fn shutdown(self) {
        let Self {
            mut child,
            mut out,
            codec,
            inp,
            _socket,
        } = self;
        let _ = write_message(&mut out, codec, &ToWorker::Shutdown {});
        let _ = out.flush();
        // Both halves go before the wait: a socket worker sees end-of-stream only once the engine
        // has dropped the reader too, and a worker still blocked on a read is one this would wait
        // for forever.
        drop(out);
        drop(inp);
        let _ = child.wait();
    }
}

/// Spawn, retrying briefly while the executable is momentarily un-executable.
///
/// `ETXTBSY` — *"Text file busy"* — is `exec` refusing a file that some process has open for
/// writing, and this provider manufactures exactly that race by design. Spawning is fork-then-exec,
/// and a fork duplicates **every** open descriptor in the process; a descriptor writing some other
/// file lives on in the child until its own `exec`. So one thread writing a pipeline script while
/// another spawns a worker leaves the script briefly unrunnable, through no fault of either.
///
/// It is a real condition and not only a test one: editing a pipeline while `borg derive` runs is a
/// developer's normal Tuesday. The window is one fork, so a handful of short retries closes it, and
/// anything longer is a genuinely busy file that should be reported.
///
/// This is the diagnosis of *"a pool test failed once in 37 runs, under full-suite load only"* in
/// `ROADMAP.md`: load meant concurrent spawns, and the harness writes its workers as it goes.
fn spawn(command: &mut Command, path: &Path) -> Result<Child> {
    const ATTEMPTS: u32 = 10;
    for attempt in 1..=ATTEMPTS {
        match command.spawn() {
            Ok(child) => return Ok(child),
            // 26 rather than `ErrorKind::ExecutableFileBusy`, which is still unstable.
            Err(err) if err.raw_os_error() == Some(26) && attempt < ATTEMPTS => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(err) => return Err(exec(format!("{}: {err}", path.display()))),
        }
    }
    unreachable!("the loop either returns or exhausts its attempts into the error arm")
}

/// The engine's own stderr, as something a child can be given for its stdout.
///
/// A `dup`, so the redirection costs no thread and no buffering: what the worker writes appears
/// when it writes it, interleaved with its own stderr exactly as it would be on a terminal.
fn engine_stderr() -> Result<Stdio> {
    std::io::stderr()
        .as_fd()
        .try_clone_to_owned()
        .map(Stdio::from)
        .map_err(|err| exec(format!("duplicating stderr for a worker: {err}")))
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
    async fn checkout(
        self: &Arc<Self>,
        command: &Path,
        transport: Transport,
        connect: Duration,
    ) -> Result<Checkout> {
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(exec)?;
        let idle = self.idle.lock().unwrap().pop();
        let worker = match idle {
            Some(worker) => worker,
            None => Worker::spawn(command, transport, connect).await?,
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

/// What the provider knows about one producer: where its code is, what it maps over, and how to
/// speak to it. None of it is in the log — the log records only that a producer exists (§9.2).
#[derive(Clone, Debug)]
pub struct Registration {
    pub producer: u64,
    pub command: PathBuf,
    /// The struct this producer maps over.
    pub source: String,
    pub transport: Transport,
}

pub struct ProcessExecutor {
    /// Producer id → everything needed to run it.
    commands: HashMap<u64, Registration>,
    /// One pool per command.
    pools: Mutex<HashMap<PathBuf, Arc<Pool>>>,
    pool_size: usize,
    connect_timeout: Duration,
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
            connect_timeout: CONNECT_TIMEOUT,
            branch,
        }
    }

    /// How many processes one command may have alive at once.
    #[must_use]
    pub fn with_pool_size(mut self, workers: usize) -> Self {
        self.pool_size = workers.max(1);
        self
    }

    /// How long a socket worker has to connect back. See [`CONNECT_TIMEOUT`]; only a test that is
    /// deliberately provoking the failure has any reason to shorten it.
    #[must_use]
    pub fn with_connect_timeout(mut self, connect: Duration) -> Self {
        self.connect_timeout = connect;
        self
    }

    pub fn register(&mut self, registration: Registration) {
        self.commands.insert(registration.producer, registration);
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
        let known = self.commands.get(&producer.id.0).cloned().ok_or_else(|| {
            exec(format!(
                "no implementation registered for {:?}",
                producer.id
            ))
        })?;
        let Registration {
            command, source, ..
        } = &known;

        let pool = self.pool(command);
        let mut checkout = pool
            .checkout(command, known.transport, self.connect_timeout)
            .await?;

        // The worker is handed the *entity* in the canonical text form and appends field names to
        // it — so `Company:o-1234abcd` is exactly the prefix it needs, and it never has to know
        // which branch it is running on.
        checkout.worker().send(&ToWorker::Invoke {
            producer: producer.id.0,
            input: borg_core::CellRef::existence(source.as_str().into(), input).to_string(),
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
    registrations: impl IntoIterator<Item = Registration>,
) -> ProcessExecutor {
    let mut executor = ProcessExecutor::new(branch);
    for registration in registrations {
        executor.register(registration);
    }
    executor
}
