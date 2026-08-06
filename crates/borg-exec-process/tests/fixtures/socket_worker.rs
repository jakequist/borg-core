//! A worker that speaks the protocol over `BORG_WORKER_SOCKET`. A test fixture, not a product.
//!
//! It exists because the socket transport's whole point is that a worker can write anything it
//! likes to stdout, and the only way to test that claim is to have a worker that does. A bash
//! worker cannot open a unix socket without `socat` or `nc -U`, neither of which is guaranteed to
//! be installed, so the fixture is Rust — a `[[bin]]` target with its source under `tests/` so
//! that `CARGO_BIN_EXE_…` finds it and nobody mistakes it for shipped code.
//!
//! Behaviour is chosen by environment variables so that one binary covers every case the transport
//! tests need:
//!
//! * `FIXTURE_NOISE=1`     — print to stdout on every invocation, both before and after the reply.
//! * `FIXTURE_SILENT=1`    — never connect to the socket at all; sit still until killed.
//! * `FIXTURE_GET=<cell>`  — read this cell before finishing.
//! * `FIXTURE_SET=<cell>=<value>` — write this cell before finishing.
//! * `FIXTURE_FAIL=<msg>`  — report the invocation as failed instead of done.

use borg_protocol::{
    Codec, Description, FromWorker, ProducerSpec, SOCKET_ENV, ServerHello, ToWorker, Transport,
    WorkerHello, read_message, write_message,
};
use std::io::{BufReader, Write};
use std::os::unix::net::UnixStream;

fn main() {
    if std::env::args().nth(1).as_deref() == Some("describe") {
        // `describe` is a plain argv invocation printing JSON to stdout, on every transport. That
        // path has no stream to corrupt, and keeping it as it is keeps bash simple.
        let description = Description {
            producers: vec![ProducerSpec {
                name: "fixture".into(),
                source: "Company".into(),
                // A worker that says nothing about its own code, which is the ordinary case: `borg
                // repo push` hashes the command file instead (SPEC.md §9.2).
                fingerprint: None,
            }],
            transport: Transport::Socket,
            ..Description::default()
        };
        println!("{}", serde_json::to_string(&description).unwrap());
        return;
    }

    if std::env::var("FIXTURE_SILENT").is_ok() {
        // Declared a socket and never connected. The engine has to give up on this rather than wait
        // for it forever.
        std::thread::sleep(std::time::Duration::from_secs(60));
        return;
    }

    let path = std::env::var(SOCKET_ENV).expect("the engine names the socket in the environment");
    let stream = UnixStream::connect(&path).expect("connect to the engine");
    let mut out = stream.try_clone().expect("clone the socket");
    let mut inp = BufReader::new(stream);

    let _hello: ServerHello = read_message(&mut inp, Codec::Json).expect("server hello");
    write_message(
        &mut out,
        Codec::Json,
        &WorkerHello {
            codec: "json".into(),
        },
    )
    .expect("worker hello");

    let noisy = std::env::var("FIXTURE_NOISE").is_ok();
    if noisy {
        // The classic corruption, at the worst possible moment: before the first message. Over
        // stdio this desynchronises the stream permanently.
        println!("a client library said hello on stdout");
    }

    loop {
        let message: ToWorker = match read_message(&mut inp, Codec::Json) {
            Ok(message) => message,
            // The engine dropped the socket: same meaning as EOF on a pipe.
            Err(_) => return,
        };
        match message {
            ToWorker::Shutdown {} => return,
            ToWorker::Invoke { .. } => {}
            _ => continue,
        }

        if noisy {
            println!("mid-invocation chatter that would have corrupted a pipe");
        }

        if let Ok(cell) = std::env::var("FIXTURE_GET") {
            write_message(&mut out, Codec::Json, &FromWorker::Get(cell)).expect("get");
            let _: ToWorker = read_message(&mut inp, Codec::Json).expect("value");
        }
        if let Ok(pair) = std::env::var("FIXTURE_SET") {
            let (cell, value) = pair.split_once('=').expect("FIXTURE_SET is cell=value");
            // `$PID` in the value is how a test tells which process served an invocation, and so
            // whether the pool reused one.
            let value = value.replace("$PID", &std::process::id().to_string());
            write_message(
                &mut out,
                Codec::Json,
                &FromWorker::Set {
                    cell: cell.into(),
                    value,
                },
            )
            .expect("set");
            let _: ToWorker = read_message(&mut inp, Codec::Json).expect("ack");
        }

        if noisy {
            println!("and some more, after the work");
            std::io::stdout().flush().ok();
        }

        let reply = match std::env::var("FIXTURE_FAIL") {
            Ok(message) => FromWorker::Error { message },
            Err(_) => FromWorker::Done {},
        };
        write_message(&mut out, Codec::Json, &reply).expect("reply");
    }
}
