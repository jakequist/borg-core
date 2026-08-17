# Working in this repository

Borg is an event-sourced data backend. **`SPEC.md` is normative** — it describes what the system is
and why. `ROADMAP.md` holds the milestone plan and the log of decisions taken in design
conversations. Read both before changing behaviour.

Code comments cite spec sections (`SPEC.md §9.4`, or just `§9.4`). If you change behaviour the spec
describes, **update the spec in the same change**. A spec that lags the code is worse than no spec.

## Commands

```
./check.sh                  # fmt, clippy -D warnings, all Rust tests, both binaries, both SDKs,
                            # all scenarios
cargo test --workspace      # Rust tests only
bash scenarios/run-all.sh   # end-to-end scenarios only (needs `cargo build -p borg-cli -p borg-server`)
cargo fmt
```

**The `binaries` step comes before `typescript` on purpose.** The TypeScript client suite drives a
real `borg-server` and *skips itself* when the binaries are missing, so building them as part of the
scenarios step meant that on any tree nobody had built yet — which is every CI checkout — thirty-one
tests quietly did not run and the script still said "all checks passed". Do not fold that build back
into the scenarios step.

There are **two binaries**: `borg` (the client, `crates/borg-cli`) and `borg-server` (the server,
`crates/borg-server`). Scenarios 250, 260, 270, 280, 300, 310, 320, 330 and 340 start a real server,
so both have to be built before `run-all.sh`; `check.sh` builds both. 330 also needs a free TCP port,
because it starts a server listening on a WebSocket as well as on its socket.

```
borg export [<file>] / borg import <file>            # a registry as a canonical event stream (§19)
borg-server export [<name>] <file> / import <name> <file>
```

**Export/import is the format policy made real** — pre-1.0 the on-disk bytes may change, the *data*
may not. If you change anything the stream carries, `crates/borg-host/tests/export_round_trip.rs` and
`scenarios/320-export-and-import` are what say so; if you change what a *store* holds without
changing the stream, the round trip is what proves the promise still holds.

```
cd packages/borg-sdk && pnpm install && pnpm run check          # typecheck, vitest, build
cd packages/borg-sdk-py && PYTHONPATH=src python3 -m unittest discover -s tests
```

The TypeScript steps need node and pnpm. `check.sh` skips them loudly where those are missing, and
so do scenarios 230, 260 and 270 — `scenarios/ts-lib.sh` is the shared skip-and-build harness, and
the SDK's own client suite skips the same way when `target/debug/borg` has not been built (it drives
a real `borg-server`, not a stand-in). The Python SDK needs only a Python 3.11+ — its tests are `unittest` cases with
no dependencies, so `pytest` runs them but nothing needs it — and scenario 240 skips loudly without
one. Everything else works everywhere.

```
cargo test --release -p borg-engine --test scale -- --ignored --nocapture
```

**Run the fan-out benchmark whenever you touch derivation, definitions or the write path.** It is
`--ignored`, so `check.sh` does not run it — and that is how an `O(n²)` regression once hid for two
milestones, until measuring made a 32k fan-out take 44 seconds instead of 0.3. Correctness tests
cannot see this class of bug; only the curve can.

**`./check.sh` must pass before any work is reported complete.** Not "the tests I added pass" — all
of it. A `justfile` mirrors these steps for anyone who has `just`, but `check.sh` is the one that
always works and the one to call.

## Layout

```
crates/borg-core            pure types: PIDs, cells, values, defs, layers, errors, text parsing
crates/borg-storage         StorageProvider trait + MemoryStorage
crates/borg-storage-sqlite  SQLite backend
crates/borg-exec            ExecutionProvider + ProducerCtx traits
crates/borg-exec-native     in-process Rust producers
crates/borg-exec-process    subprocess producers, over stdio or a unix socket
crates/borg-protocol        the worker wire contract; `client.rs` is the client one (§17.5) and
                            holds the thirty-line `ask` every Rust caller speaks it with, plus the
                            hello acknowledgement; `url.rs` is the connection-url parser a client is
                            configured from (§17.7); `ws.rs` is the WebSocket framing both ends
                            share and the Rust client's dial
crates/borg-engine          log, branches, defs, derivation, resolver, registry
crates/borg-host            what it takes to *host* a store, shared by both binaries: `ops.rs` is
                            what the commands do, `push.rs` is `repo push`, `sidecar.rs` the files
                            beside a store, `serving.rs` the advisory lock, `host.rs` a data
                            directory of registries (§17.6), `stream.rs` export/import — a
                            registry as a canonical event stream (§19) — and `keys.rs` the static
                            API keys a handshake's credential is checked against, which is the one
                            piece of state beside a store whose *corruption* is a refusal rather
                            than a default (§17.6)
crates/borg-cli             the `borg` binary — argv and printing over `borg-host`, plus
                            `generate.rs`, which emits the typed client (§15). Embedded Borg: it
                            operates directly on a store nobody is serving, and has no `serve`.
                            `generate` and `repo push` are the two commands that take a `--url`
                            and speak to a server instead (§17.7); the rest take `--store`
crates/borg-server          the `borg-server` binary — `serve.rs` is `borg-host`'s ops over a
                            socket *and* over a WebSocket (`Transport`/`Peer` is the seam, and
                            `GET /health` is the one HTTP endpoint), and `lifecycle.rs` is
                            start/stop/status/logs plus `keygen`/`keys`; `status`, `create`,
                            `export` and `import` are clients of the server they administer, over
                            `borg_protocol::client::ask` — presenting the `*`-scoped credential the
                            server minted at boot, because the unix socket is deliberately **not**
                            exempt from authentication (§17.6) — and fall back to operating on the
                            data directory directly when nothing is serving it
packages/borg-sdk           the TypeScript SDK. Two entry points, deliberately opposite: `borg-sdk`
                            is the author-side DSL and the worker protocol, `borg-sdk/client` is
                            the consumer-side client over `borg-server`. `values.ts` is the one
                            conversion table both use, `lines.ts` the framing — a shared base with
                            one subclass per transport, so the reconnect semantics cannot differ
                            between them — and `connection.ts` the url parser (the same table as
                            `borg-protocol`'s, in the other language) plus the dial, over a unix
                            socket or the runtime's own `WebSocket`, and its reconnect (§17.7)
packages/borg-sdk-py        the Python SDK: the pipeline half, and the neutrality gate on the
                            contract
scenarios/                  end-to-end scenarios driving the real binaries; `ts-lib.sh` is the
                            skip-if-no-node harness the TypeScript ones share, and `lib.sh` holds
                            `BORG_SERVER_BIN` for the seven that start a server
Dockerfile                  the deployment artifact: `borg-server` and `borg` on Debian slim, with
                            node, python3, bash and jq, because a pipeline is a subprocess the
                            server spawns and the image's package list *is* the set of languages a
                            pipeline may be written in. `DEPLOY.md` is the operator's half and
                            `docker-compose.yml` the example; `ROADMAP.md`, *Deployment*, has the
                            reasoning
.github/workflows/ci.yml    CI. Runs `./check.sh` whole — not a subset, not a matrix — and **fails
                            if anything skipped**: every skip in the tree announces itself with a
                            `⚠` and that character is used for nothing else, which is what makes
                            the guard exhaustive. A second job records the fan-out benchmark as an
                            artifact without gating on it
```

Dependency arrows point inward to `borg-core`. Trait crates (`borg-storage`, `borg-exec`) are
separate from their implementations on purpose — that is the swappability seam.

**`borg-cli` and `borg-server` are two front ends over one `borg-host`, and neither may grow its own
copy of anything.** The rule is the one `ops` already enforced inside the CLI: *an operation returns
what happened; the caller renders it.* If a command's behaviour is in a binary rather than in
`borg-host`, embedded Borg and the served kind will drift, and the first thing to drift will be
something nobody is testing on both paths.

## Invariants that must not be broken

These are load-bearing. Breaking one is a design change, not a refactor, and needs discussion first.

1. **Nothing above the provider line knows what a backend is.** `StorageProvider` sees cells, def
   events, layers and a `ReadPath`. It never learns about derivation, dependencies or watermarks.
2. **Commit streams.** A layer may hold millions of mutations and can never be buffered whole.
   Visibility is a join, not a per-row rewrite (§6.2, §17.1).
3. **Locks are per-layer, never per-branch.** A branch-wide write lock serialises derivation. This
   includes a provider holding one worker behind a mutex, which is the same lock wearing a disguise.
4. **Single writer per field.** This is what lets derived layers commit concurrently without
   conflicting. It reaches invocations only because v1 pipelines are per-entity maps; a producer
   writing across entities breaks it and nothing checks (§16.3).
5. **No membership test in the dependency index may be a linear scan** (§16.3). A widely-shared cell
   accumulates one dependent per invocation and each retracts itself on re-run; a `Vec` makes fan-out
   quadratic. This was measured, not guessed.
6. **`CellRef` is the shard key; `CellAt` is the record key.** Read-sets, the dependency index and
   ownership all key on `CellAt`. Keying on `CellRef` makes a migration observe its own output as a
   change to its own input.
7. **Writes are never coerced.** A value is stored at the **def-version of its own field** — as the
   author's def-view names it, never the author's whole-schema ClientVersion — forever. Readers
   migrate on the read path. The two versions coincide only when every def push touches every field
   (§5.3, §5.4); `DefView::version_of` is the only bridge between them, and `DefVersion` is a
   separate type so that it stays the only one.
8. **Derived data is never presented as fresh.** Every read returns a provenance envelope. A stale
   value is served *and labelled*, never silently served or withheld.
9. **A layer holds value events xor def events.** This is what makes "the def-version at layer L"
   well-defined.
10. **Every write is a transaction: fork, write, merge.** Guards are the transaction's own read-set
    with `since` = the fork point, and re-evaluating them against the parent *is* the merge-conflict
    detector (§12, §13). The one exception is a branch whose whole ancestry is empty, which has
    nothing to fork from — see `ROADMAP.md`, *An empty branch's first write is not a transaction*.
11. **The dependency index is keyed on the trunk, never on a round's own branch** (§16.3). Keyed on
    the round branch it would be discarded with the round, and an invocation whose merge was
    rejected would never be rediscovered. Partial application is only safe because of this.
12. **A round's applied subset is closed under the round's own dependencies** (§16.5). Drop an
    invocation and everything in that round which read what it wrote goes too, transitively —
    otherwise the round publishes a value derived from one that never landed, wearing a watermark
    that replaying would not reproduce.

## Conventions

**Comments explain why, not what.** The code says what it does. Comments exist for the reasoning a
reader cannot recover — why this shape and not the obvious one, what breaks otherwise, what was
measured. Density should match the surrounding file.

**TDD where behaviour is specified.** Branching, transactions and def events were written test-first
because the spec enumerated them, and it caught real bugs. Write tests first when you are
implementing something the spec already describes; implementation-first is fine when the design is
still being discovered.

**Tests assert behaviour, not implementation.** A test name should state a claim about the system.
Prefer `a_tombstone_on_a_child_hides_an_inherited_value` over `test_get_cell_2`.

**Dependencies are a decision.** Do not add a crate without a reason that survives being said out
loud. The workspace is deliberately thin.

**Scenarios use the real binary.** No in-process shortcuts. If a scenario passes, that devex works.

## Things left undone on purpose

Do not "fix" these without discussion — they are tracked in `ROADMAP.md`:

- Auto-derivation happens in the process that commits a layer, not in a scheduler of its own. A
  write therefore pays for the derivation it causes; §9.6 says that is a latency property, not a
  semantic one, and a server moves the same call behind a signal.
- `scan_buffer` and `read_layer` materialise results before streaming them.
- **`Registry::open` brings the log's projections to head, which for a fresh set means replaying the
  log — `O(log)` per CLI invocation.** That is process-per-command's honest cost and it is unchanged:
  a process that exits between commands has nothing to keep. What changed is that it is no longer
  *also* per read, because a server holds one registry per hosted store (below). The indexes are
  `borg_engine::projection::Projection`s — a fold over committed layers, with a position — and the
  two lifecycles (rebuilt from zero, maintained live) are held to the same answers by
  `crates/borg-engine/tests/projections.rs`. **A performance change to an index belongs behind that
  seam and is not finished until those tests pass against both lifecycles.**
- Writes to list and untyped-container cells are **not** validated: there is no `ListDef` event to
  validate against, so requiring a declaration would make them unwritable (§8).
- Nothing registers a ClientVersion as live (§5.5), so the live-version set is empty and every
  migration materializes. Real clients arrive with the network layer.
- `borg frontier reaches` polls the store between awaits, because the CLI is process-per-command and
  the frontier one process holds only moves if that process derives. The await inside the loop is
  the primitive; the loop is what an in-process deriver removes.
- A watermark is a `LayerId` like any other, so nothing stops one being compared with a layer id
  that is not a source layer. Four bugs have come from that family already; `ROADMAP.md`'s
  *Deferred features* records what a `Watermark` newtype would cost and why it is its own change.
- **A round forks before it knows whether it has any work**, so a source layer that dirties nothing
  still costs a branch row. Forking lazily means threading the round's read path into the scheduler
  rather than its branch id, which is a real restructuring for a row.
- **Transaction branches are never reaped**, and neither are round branches. Reaping drops a
  transaction's *state*, which is what makes it unusable, and leaves the branch row; a round holds no
  state outside the process running it, so an abandoned one leaves a branch row and derived layers
  nothing can reach. Whether spent branches are collected or kept as history is a real choice
  (`ROADMAP.md`, *Concerns carried over from the transactional-model draft*) and should not be made
  by a janitor as a side effect. Note this is now **two** branches per `borg set`: the transaction's,
  and the round's.
- **The reap sweep lives above the store, not in `Registry::open`.** §12.3 says "when a process opens
  the store", and for the CLI that is `run()`; for the server it is every request, because that is
  when the server takes the store. The transaction table is a filesystem sidecar like the pause flags and
  the producer table; `Registry::open` sits below the provider line, where a filesystem sidecar has no
  business.
- **How many intermediate derived snapshots a backlog leaves is schedule-dependent.** A round settles
  the whole range `[watermark+1 … head]` (§6.3, §16.5), so one that settles `L10`, `L11` and `L12`
  together leaves one generation of derived layers where three rounds would leave three. Settled
  values and every label on them are unaffected and are what `scenarios/200-determinism` sweeps;
  nothing can ask the other question, because derived data is addressed by `reflects` and never by
  derived LayerId. This is deliberate — see `ROADMAP.md`, *Settling a range is a schedule change*.
- **Every `borg set` now costs four layers** in the one-producer case — one on its transaction
  branch, one on the parent naming it, and then a round, which is one derived layer per *invocation*
  on its own branch and one per *producer* on the parent (§16.5). Forks are `O(1)` and layers are
  cheap, but `Registry::open` replays the log on every CLI invocation, so the `O(log)` open grows
  with it. The transactional-model draft flagged this; the fan-out benchmark cannot see it, because
  it drives the engine rather than the CLI. **Multiplied by a server that opened per request, this
  was `examples/personal-crm/FRICTION.md` #9** — the first time the two costs above met, and the
  reason the sentence "documented separately, never multiplied" is worth watching for.
  it drives the engine rather than the CLI.
- **A producer's implementation fingerprint does not cover what it imports.** §9.2's "pushing new
  pipeline source moves the producer's ClientVersion" is implemented by hashing the code and putting
  the digest in `PushProducer`, and what "the code" reaches is per language: the TypeScript SDK hashes
  the entry module's bytes only, because ESM exposes no loaded-module registry; the Python SDK adds
  every already-imported module beside the entry file and stops at the repo, because hashing the
  environment would recompute everything after an unrelated `pip install`; a shell worker gets `borg
  repo push`'s fallback, which hashes the command file. So a pipeline whose logic lives in an imported
  module can change without invalidating anything. `borg derive --rebuild` owes nothing to
  fingerprints and is the answer meanwhile. A producer that can be fingerprinted by neither route is
  *documented* to invalidate on nothing — see `producer_change` in `crates/borg-host/src/push.rs`.
- **Swapping a build in behind the log's back is invisible, deliberately.** The log records *which*
  program a producer is, never *where* it is (§9.2) — so editing `borg.producers.json` to point at
  different code changes no definition and triggers no recompute. `scenarios/100-watermark-truth`
  depends on that: it is how the sweep manufactures a value its own watermark no longer reproduces.
- **A producer that has never succeeded has no cell to call `broken`.** §14's state is a label on a
  stored record (§10.4), and a pipeline that threw on its first run wrote none, so its output reads
  as simply absent. Enumerating the cells a producer *might* have written is not a set anything can
  produce.
- **`borg list` cannot be guarded, has no ordering contract and has no cursor.** Enumeration is a
  read outside any transaction, because "the set of Contacts" is not a cell and a guard is a question
  about a cell (§9.6, §12.4) — so *list, decide, write* has no protection against an object appearing
  in between. It sorts by PID so two identical reads answer identically and promises nothing more,
  and it materializes the whole answer like the `scan_buffer` beneath it. All three are the same
  decision: the query layer is out (§18), and half of one is worse than none. SDK-DRAFT §5 carries
  the shapes that were considered.
- **The PID counter is a sidecar, and it is the one sidecar whose loss a store cannot recover from.**
  `borg.allocations.json` holds the next counter `tx create` will issue. Every other sidecar loses
  something you can restore by doing the thing again — push the repo, pause the branch — but deleting
  this one restarts the count, and a fresh object can then be issued the id of an existing one. It is
  a sidecar because the store cannot answer the question cheaply: a counter spans every struct, so
  deriving it means scanning every object buffer, and doing that per create is `O(n²)`. It is written
  *before* the write it names, so a crash burns an id rather than reusing one. SDK-DRAFT §4.5.
- `refresh` re-runs every hop of a chain when any hop is behind, rather than only the hops that are.
  Correctness is unaffected; making it precise needs validation callable from the derivation engine
  without handing the engine the resolver.
- **`borg-server` holds one registry per hosted store and still answers one request at a time per
  registry.** The held registry was the fix (`FRICTION.md` #9): a store used to be opened per
  request, which replayed the log per read, and the reason was derivation's lifecycle rather than the
  socket — `ops::tx_commit` dropped its registry so that `auto_derive` could open another one *with*
  an executor, because two live `Registry` instances over one store break the single-process
  assumption. The long-lived registry now carries the executor, so both use the same instance. What
  makes it safe is the advisory lock: the server is the only writer, so every mutation flows through
  the instance maintaining the projections.
  **The gate is deliberately still there.** Requests are serialised per registry; the replay was the
  cost, not the gate, and letting reads overlap is its own change with its own soak (`ROADMAP.md`).
  It is per *registry* and not per server because what it protects — the files beside a store, and
  that store's sequencer — is per store. What is still deferred is turning the post-write `catch_up`
  call into a signal (§9.6) — a write still pays for the derivation it causes, inside the request
  that caused it.
- **A registry a server hosts is opened on first use, not at boot** (`crates/borg-host/src/host.rs`).
  Opening replays a log; locking is a file write, and *that* is done for every hosted registry before
  the socket is announced. The asymmetry is deliberate and `borg-server status` reports which
  registries are open, so the laziness is visible rather than assumed.
- **A served store locks every other `borg` invocation out** — except `borg generate`, which speaks
  to the socket instead (SPEC.md §17.5, `crates/borg-cli/src/generate.rs`). The lock is honest about
  the assumption that was always there; the CLI connecting rather than being refused is the
  remote-connection feature SDK-DRAFT.md §2.6 describes, and `generate` is the first and so far only
  command to do it, because it is a pure read and needs none of the answers the write path needs
  about `--tx` and `$BORG_TX`. Extending it further is an open question in SDK-DRAFT §5, not a
  pattern to copy. **`repo push` is the one write that no longer needs it**: `repo_push` is a
  protocol message and the *server* runs the push against a path on its own disk (§17.6), so pushing
  a schema no longer means stopping the server. `def push` is still not on the socket, and is the
  smaller case — it reads one JSON file a repo would emit anyway.
- **`borg generate --watch` polls, because §17.5 has no server push.** One request, one response, in
  order; a subscription would be a change of shape rather than a field, so the loop asks for the def
  view every 400ms and rewrites the file when it moved. Recorded in SDK-DRAFT §4.4.
- **A handshake naming *no* registry against a server hosting none or several is accepted, and the
  ambiguity is reported by the first request that needs a store.** This is the residue of a
  deviation that is otherwise closed: client protocol 2 answers every hello, so a hello that *names*
  a registry the server does not host is refused at the handshake with the options listed. A hello
  that names nothing has made no claim that could be wrong — and it is exactly the connection
  `borg-server status` makes, since `registries` needs no store — so refusing it would make §17.6's
  discovery escape hatch unreachable. The asymmetry is deliberate and is asserted in
  `scenarios/300`, in `serve.rs`'s handshake tests and in the SDK's client suite. The rest of the
  old entry here — the lost `EPIPE`, "accepted" being indistinguishable from "not answered yet" —
  is fixed; `ROADMAP.md`, *The handshake is answered*.
- **A client SDK's transparent reconnect is best-effort, and the honest path is the failing one.**
  A socket the peer has already closed is dropped *before* the next request is written, so an
  ordinary server bounce costs an idle client nothing — but that depends on the runtime having had a
  turn to deliver the close. A client that was busy right through the outage discovers it by
  failing, with `BorgDisconnectedError`, and **nothing is ever retried**: `tx_commit` is not
  idempotent, so a commit whose answer was lost is indistinguishable from one that never arrived.
  `scenarios/310` asserts the failing case deliberately, by bouncing the server from inside a
  blocking call, because it is the case that is easy to stop testing once the happy one works.
- **`borg-server` terminates no TLS and trusts no forwarded header.** It speaks plaintext `ws://`
  and expects a proxy in front of it (§17.6); `tungstenite` is taken with `default-features = false`
  so there is no TLS backend in the binary to reach by accident. Nothing in §17.5 is a function of
  the client's address or scheme, so `X-Forwarded-For`, `X-Forwarded-Proto` and `X-Real-IP` are all
  read by nothing — trusting one would be a spoofable identity answering a question nobody asks, and
  authentication is already reserved a field in `ClientHello`. `wss://` as a **listen** address is
  refused by name rather than served in plaintext.
- **A WebSocket listener answers its health probe on the accept thread**, with a two-second bound on
  reading the request head. `Transport::accept` may only return a session, so a `GET /health` is
  answered inside the accept loop and the loop goes round again; the bound is what stops a client
  that connects and says nothing from stalling the listener rather than only itself. Widening
  `accept` to return an `Option` would move that read nowhere, which is why it was not done.
- **The TypeScript client SDK offers only JSON**, over both transports. MessagePack would buy a
  dependency, and being dependency-free is what lets the package be dropped into anything; a binary
  frame arriving is therefore reported rather than decoded. The Rust client and `borg-server` speak
  both codecs on both transports.
- **`borg+wss://` is dialled by the SDK and refused by the CLI**, and the asymmetry is per language
  rather than per address. A browser or a node process gets TLS from the runtime's own `WebSocket`
  for free; a Rust client would have to carry a certificate store to speak it, for a deployment
  whose premise is that a proxy has already terminated. So the parser accepts it everywhere and
  `borg_protocol::client::ask` refuses it at the dial, saying to point at the `ws://` the proxy
  forwards to.
- **Revoking an API key does not close connections that are already open.** Revocation is read at the
  handshake, so the *next* connection with that key is refused and the live one carries on until it
  hangs up. Closing them needs a registry of live sessions and a way to signal every session thread —
  the same machinery relaxing the per-registry gate would want, so it should be one change rather
  than two — for a window that closes on its own, because connections are short-lived and a
  reconnecting client re-presents its credential. Asserted in both directions in
  `crates/borg-server/src/serve.rs` and `scenarios/340`, so that nobody discovers it during an
  incident.
- **`borg-server keygen` and `keys revoke` write a file and never speak to the socket**, so they need
  filesystem access to the data directory and work against a stopped server. That is what makes
  minting the *first* credential possible at all — doing it over a connection that already requires
  one is a circle — and it is why enforcement and rotation need no restart. The boundary it draws is
  the filesystem's: whoever can write the data directory can issue keys, and could already read every
  store under it.
- **A key's scope is a list of registry names or `*`, and there is nothing finer.** No read-only key,
  no per-branch scope, no expiry, no audit log, no rate limit — see `ROADMAP.md`, *Static API keys*.
  The unit a read/write scope would attach to is not settled (a branch is not a tenant, §17.6), and a
  key with an expiry is the platform's signed token wearing a worse disguise.
- **`borg generate` against a *served* store reads that store's lock record, which names a socket and
  a registry but not the data directory** — so it cannot find the admin token, and generating against
  an enforcing local server needs `$BORG_TOKEN` or a `--url` carrying a key. Against an open server —
  every local one until somebody runs `keygen` — nothing changed. Widening the lock record to name
  the token would work and would put the admin credential's path beside every store, which is a
  decision rather than a tidy-up.
- **The Python SDK has no client half and therefore no url parser.** It is the pipeline half and the
  neutrality gate on the worker contract (§17.4); nothing in it connects to a `borg-server`. The
  parser is one file per language that has a client, which today is Rust and TypeScript.
- **Reading a cell of a struct nobody declared answers an absent envelope rather than an error**, and
  its `origin` reads `derived`. `borg get Wombat#1.nose` prints exactly that, and the SDK reproduces
  it because there is one read path. Pre-dates the SDKs; asserted in `packages/borg-sdk`'s client
  suite so that nobody "fixes" it in one of the two places.
- **An export holds its registry for its whole duration, and that is the price of the snapshot.**
  There is no snapshot machinery in `stream::export` because there is no torn read to prevent: a
  served export runs under the registry's gate and embedded `borg` is one process the lock has
  already made exclusive. It is the same gate `ROADMAP.md`'s *Concurrent requests within one
  registry* tracks, so the fix — if a multi-gigabyte export blocking a registry ever becomes the
  complaint — is that change and not a second one.
- **An export is at head, never at the settled frontier.** Settling would drop every source layer
  above the watermark, which is losing the most recent writes out of a backup. The settled position
  is *reported* so a captured backlog is visible; it bounds nothing. Do not "fix" this — see
  `ROADMAP.md`, *An export is the whole log, and settling it would be data loss*.
- **`stream::export` re-parses every cell address it writes.** The canonical text form is documented
  lossless and is injective over every address the constructors build, but this is a backup format
  and a silently mangled cell on restore is the worst failure it has — so a mismatch is a loud export
  failure rather than a stream that reads back as something else. One parse per event, on a path that
  is already doing JSON per event.
- **Interned content is emitted per layer, not per registry.** The seen-set is bounded by one layer's
  distinct values, which is the bound `read_layer` already imposes; a registry-wide set would be
  bounded by the number of distinct strings in the store, which is exactly what a streaming format
  promises not to hold. Re-emitting a shared string once per layer costs a hash lookup on import,
  because interning is idempotent.
- **A restored registry keeps its transaction branches**, as branch rows with layers and nothing
  pointing at them — the same residue an abandoned transaction leaves on the store it came from. The
  stream reproduces the registry including the parts nobody has collected yet, which is right until
  reaping is a decision somebody has made (above).
- `Set`, `Map`, aggregation pipelines, mid-list insertion and container isolation are deferred (§18).
  Generated SDKs are no longer: `borg generate --lang ts` is built, and Python, Rust and Go are not.
  A generated list field yields the handle to the list and not its elements, because element
  addressing goes with the rest of the list story.

## Reporting work

State what you changed, what you verified, and what you did **not** do. If something is half-done,
say so plainly. A confident report on unverified work is worse than an honest partial one.
