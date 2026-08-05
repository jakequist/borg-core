# Working in this repository

Borg is an event-sourced data backend. **`SPEC.md` is normative** — it describes what the system is
and why. `ROADMAP.md` holds the milestone plan and the log of decisions taken in design
conversations. Read both before changing behaviour.

Code comments cite spec sections (`SPEC.md §9.4`, or just `§9.4`). If you change behaviour the spec
describes, **update the spec in the same change**. A spec that lags the code is worse than no spec.

## Commands

```
./check.sh                  # fmt, clippy -D warnings, all Rust tests, all scenarios
cargo test --workspace      # Rust tests only
bash scenarios/run-all.sh   # end-to-end CLI scenarios only (needs `cargo build -p borg-cli` first)
cargo fmt
```

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
crates/borg-exec-process    subprocess producers over stdio
crates/borg-protocol        the worker wire contract; `client.rs` is the client one (§17.5)
crates/borg-engine          log, branches, defs, derivation, resolver, registry
crates/borg-cli             the `borg` binary — `ops.rs` is what the commands do, `main.rs` is
                            argv and printing, `serve.rs` is the same ops over a socket
scenarios/                  end-to-end scenarios driving the real binary
```

Dependency arrows point inward to `borg-core`. Trait crates (`borg-storage`, `borg-exec`) are
separate from their implementations on purpose — that is the swappability seam.

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
- SQLite `Registry::open` rebuilds indexes by replaying the log — `O(log)` per CLI invocation.
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
  the store", and for the CLI that is `run()`; for `borg serve` it is every request, because that is
  when serve opens the store. The transaction table is a filesystem sidecar like the pause flags and
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
  it drives the engine rather than the CLI.
- **A producer that has never succeeded has no cell to call `broken`.** §14's state is a label on a
  stored record (§10.4), and a pipeline that threw on its first run wrote none, so its output reads
  as simply absent. Enumerating the cells a producer *might* have written is not a set anything can
  produce.
- `refresh` re-runs every hop of a chain when any hop is behind, rather than only the hops that are.
  Correctness is unaffected; making it precise needs validation callable from the derivation engine
  without handing the engine the resolver.
- **`borg serve` opens the store per request and serves one request at a time.** A real server holds
  the store open, and the reason this one cannot is not the socket: `ops::tx_commit` drops its
  registry so that `auto_derive` can open another with an executor, because two live `Registry`
  instances over one store break the single-process assumption. Making the server hold one is a change
  to derivation's lifecycle — the same change that turns the post-write `catch_up` call into a signal
  (§9.6) — and should be made there rather than worked around here.
- **A served store locks every other `borg` invocation out** rather than letting the CLI speak to the
  socket (SPEC.md §17.5). The lock is honest about the assumption that was always there; the CLI
  connecting instead of being refused is the remote-connection feature that supersedes `borg serve`
  altogether (SDK-DRAFT.md §2.6), and is deliberately not a v1 of itself. The consequence to know
  about: **pushing a schema to a served store means stopping the server**, because `def push` and
  `repo push` read from a filesystem and are not on the socket — see SDK-DRAFT.md §4.3 for why
  putting them there would be the wrong shape rather than merely missing work.
- `Set`, `Map`, aggregation pipelines, mid-list insertion, container isolation and generated SDKs
  are all deferred (§18).

## Reporting work

State what you changed, what you verified, and what you did **not** do. If something is half-done,
say so plainly. A confident report on unverified work is worse than an honest partial one.
