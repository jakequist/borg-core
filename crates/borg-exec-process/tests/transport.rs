//! The two transports. SPEC.md §17.4.
//!
//! One protocol reached two ways: over a worker's stdio, or over a unix socket the engine offers in
//! `BORG_WORKER_SOCKET`. The claims worth testing are that both carry the same conversation, that a
//! worker which declares nothing still gets the old one, and that on the socket a worker may write
//! whatever it likes to stdout without any of it reaching the protocol — which is the entire reason
//! the socket exists.

use async_trait::async_trait;
use borg_core::{
    BranchId, CellRef, ClientVersion, DefVersion, LayerId, Pid, PidKind, ProducerId, Result, Value,
};
use borg_exec::{ExecutionProvider, ProducerCtx, ProducerRef, ValueCodec};
use borg_exec_process::{ProcessExecutor, Registration};
use borg_protocol::Transport;
use std::collections::HashMap;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

const PRODUCER: ProducerId = ProducerId(1);
const BRANCH: BranchId = BranchId(1);
const FIXTURE: &str = env!("CARGO_BIN_EXE_borg-socket-worker-fixture");

/// A context that remembers the conversation, so a test can assert what crossed the wire rather
/// than only that nothing crashed.
#[derive(Default)]
struct Recorder {
    reads: Vec<String>,
    writes: Vec<(String, String)>,
    stored: HashMap<String, String>,
}

#[async_trait]
impl ProducerCtx for Recorder {
    async fn get(&mut self, cell: &CellRef) -> Result<Option<Value>> {
        let key = cell.to_string();
        self.reads.push(key.clone());
        // The text comes back through `render`; the value only has to say that there was one.
        Ok(self.stored.get(&key).map(|_| Value::Int(0)))
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
    async fn set_text(&mut self, cell: &CellRef, text: &str) -> Result<()> {
        self.writes.push((cell.to_string(), text.to_string()));
        Ok(())
    }
}

#[async_trait]
impl ValueCodec for Recorder {
    async fn intern(&mut self, _input: borg_core::ValueInput) -> Result<Value> {
        unreachable!("nothing here writes through the typed path")
    }
    async fn render(&mut self, _value: &Value) -> Result<String> {
        Ok(self
            .reads
            .last()
            .and_then(|cell| self.stored.get(cell))
            .cloned()
            .unwrap_or_default())
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

fn producer() -> ProducerRef {
    ProducerRef {
        id: PRODUCER,
        version: ClientVersion(LayerId(1)),
    }
}

fn executor(command: &Path, transport: Transport) -> ProcessExecutor {
    let executor = ProcessExecutor::new(BRANCH).with_pool_size(1);
    executor.register(Registration {
        producer: PRODUCER.0,
        command: command.to_path_buf(),
        source: "Company".into(),
        transport,
    });
    executor
}

/// A scratch directory of its own for each test, because the shims below are files and these tests
/// run at once.
fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("borg-transport-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

fn executable(path: &Path, script: &str) {
    std::fs::File::create(path)
        .and_then(|mut file| file.write_all(script.as_bytes()))
        .expect("write script");
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).expect("chmod");
}

/// The socket fixture, configured.
///
/// Configuration goes through a generated shim rather than `std::env::set_var`, because the
/// environment belongs to the whole test binary and these tests run concurrently — one test's
/// `FIXTURE_FAIL` would be another's mystery.
fn socket_worker(dir: &Path, settings: &[(&str, &str)]) -> PathBuf {
    let path = dir.join("worker.sh");
    let mut script = String::from("#!/usr/bin/env bash\nset -euo pipefail\n");
    for (key, value) in settings {
        script.push_str(&format!("export {key}='{value}'\n"));
    }
    script.push_str(&format!("exec '{FIXTURE}' \"$@\"\n"));
    executable(&path, &script);
    path
}

/// A stdio worker, in bash, which is the audience the stdio transport was shaped for.
///
/// It fails loudly if the engine offered it a socket: a worker that declared nothing should have no
/// socket created for it, and paying for one it will never open is a cost with no buyer.
fn stdio_worker(dir: &Path) -> PathBuf {
    let path = dir.join("worker.sh");
    executable(
        &path,
        r#"#!/usr/bin/env bash
set -euo pipefail
if [ -n "${BORG_WORKER_SOCKET:-}" ]; then
  echo "a stdio worker was offered a socket at $BORG_WORKER_SOCKET" >&2
  exit 1
fi
IFS= read -r _hello
printf '%s\n' '{"codec":"json"}'
while IFS= read -r msg; do
  case "$msg" in
    *shutdown*) exit 0 ;;
  esac
  printf '%s\n' '{"set":{"cell":"Company:o-04068.served","value":"stdio"}}'
  IFS= read -r _ack
  printf '%s\n' '{"done":{}}'
done
"#,
    );
    path
}

/// The claim the socket exists to make: a worker may print anything it likes to stdout, at any
/// moment, and the conversation is untouched.
///
/// The fixture prints before its first message — the case that permanently desynchronises a pipe —
/// and again in the middle of every invocation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_socket_worker_may_write_anything_to_stdout_without_corrupting_the_protocol() {
    let dir = scratch("noisy");
    let command = socket_worker(
        &dir,
        &[
            ("FIXTURE_NOISE", "1"),
            ("FIXTURE_GET", "Company:o-04068.website"),
            ("FIXTURE_SET", "Company:o-04068.is_investible=true"),
        ],
    );

    let mut ctx = Recorder::default();
    ctx.stored
        .insert("Company:o-04068.website".into(), "acme.ai".into());

    let executor = executor(&command, Transport::Socket);
    for i in 0..3 {
        executor
            .run(&producer(), entity(i), &mut ctx)
            .await
            .expect("the noisy worker still served the invocation");
    }
    executor.shutdown().await;

    assert_eq!(
        ctx.reads,
        vec!["Company:o-04068.website".to_string(); 3],
        "every read crossed the socket"
    );
    assert_eq!(
        ctx.writes,
        vec![
            (
                "Company:o-04068.is_investible".to_string(),
                "true".to_string()
            );
            3
        ],
        "and so did every write"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A worker that declares nothing keeps the transport it was written for, and is not made to pay
/// for one it does not use — the script asserts it was offered no socket at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_that_declares_nothing_is_spoken_to_over_stdio() {
    let dir = scratch("stdio");
    let command = stdio_worker(&dir);
    let mut ctx = Recorder::default();

    let executor = executor(&command, Transport::Stdio);
    executor
        .run(&producer(), entity(0), &mut ctx)
        .await
        .expect("the bash worker served the invocation");
    executor.shutdown().await;

    assert_eq!(
        ctx.writes,
        vec![("Company:o-04068.served".to_string(), "stdio".to_string())],
        "the stdio conversation is the same conversation"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Reuse is a property of the pool, not of the pipe, so it must survive the change of transport:
/// invocations that never overlap are served by one process.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_socket_worker_is_reused_across_invocations() {
    let dir = scratch("reuse");
    let command = socket_worker(&dir, &[("FIXTURE_SET", "Company:o-04068.served=$PID")]);
    let mut ctx = Recorder::default();

    let executor = executor(&command, Transport::Socket);
    for i in 0..4 {
        executor
            .run(&producer(), entity(i), &mut ctx)
            .await
            .expect("served");
    }
    executor.shutdown().await;

    let pids: std::collections::HashSet<&str> =
        ctx.writes.iter().map(|(_, value)| value.as_str()).collect();
    assert_eq!(
        pids.len(),
        1,
        "four sequential invocations used {} processes",
        pids.len()
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A worker that reports failure is still a usable worker — the stream is synchronised, it simply
/// said no — and the failure reaches the engine as the producer's, not the transport's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_socket_worker_that_reports_failure_fails_the_invocation_and_not_the_connection() {
    let dir = scratch("failing");
    let command = socket_worker(&dir, &[("FIXTURE_FAIL", "no website")]);
    let mut ctx = Recorder::default();

    let executor = executor(&command, Transport::Socket);
    let error = executor
        .run(&producer(), entity(0), &mut ctx)
        .await
        .expect_err("the worker said no");
    assert!(error.to_string().contains("no website"), "{error}");

    // …and the same process answers the next one, which is what "still usable" means.
    let again = executor
        .run(&producer(), entity(1), &mut ctx)
        .await
        .expect_err("the worker said no again");
    assert!(again.to_string().contains("no website"), "{again}");
    executor.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// The common failure of a socket worker, and the one a timeout alone would report thirty seconds
/// late: it died on the way up. A pipeline with a syntax error is exactly this.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_socket_worker_that_dies_on_the_way_up_is_reported_at_once() {
    let dir = scratch("dying");
    let command = dir.join("worker.sh");
    executable(&command, "#!/usr/bin/env bash\nexit 3\n");
    let mut ctx = Recorder::default();

    let executor =
        executor(&command, Transport::Socket).with_connect_timeout(Duration::from_secs(30));
    let started = std::time::Instant::now();
    let error = executor
        .run(&producer(), entity(0), &mut ctx)
        .await
        .expect_err("the worker died");
    assert!(
        error.to_string().contains("exited") && error.to_string().contains("without connecting"),
        "{error}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "waited {:?} to notice a process that had already exited",
        started.elapsed()
    );
    executor.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// The failure a declared transport can still have: the worker started and never connected. It has
/// to be given up on, and the message has to name the executable and the socket, because from the
/// outside this looks exactly like a hang.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_socket_worker_that_never_connects_is_given_up_on_by_name() {
    let dir = scratch("silent");
    let command = socket_worker(&dir, &[("FIXTURE_SILENT", "1")]);
    let mut ctx = Recorder::default();

    let executor =
        executor(&command, Transport::Socket).with_connect_timeout(Duration::from_millis(300));
    let error = executor
        .run(&producer(), entity(0), &mut ctx)
        .await
        .expect_err("nothing ever connected");
    let message = error.to_string();
    assert!(
        message.contains("worker.sh") && message.contains("did not connect"),
        "{message}"
    );
    executor.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}
