# Working in this repository

Borg is an event-sourced data backend. **`SPEC.md` is normative** — it describes what the system is
and why. `ROADMAP.md` holds the milestone plan and the log of decisions taken in design
conversations. Read both before changing behaviour.

Code comments cite spec sections (`SPEC.md §9.4`, or just `§9.4`). If you change behaviour the spec
describes, **update the spec in the same change**. A spec that lags the code is worse than no spec.

## Commands

```
just check      # fmt, clippy -D warnings, all Rust tests, all scenarios — the definition of done
just test       # Rust tests only
just scenarios  # end-to-end CLI scenarios only
just fmt
```

`just check` must pass before any work is reported complete. Not "the tests I added pass" — all of
it.

## Layout

```
crates/borg-core            pure types: PIDs, cells, values, defs, layers, errors, text parsing
crates/borg-storage         StorageProvider trait + MemoryStorage
crates/borg-storage-sqlite  SQLite backend
crates/borg-exec            ExecutionProvider + ProducerCtx traits
crates/borg-exec-native     in-process Rust producers
crates/borg-exec-process    subprocess producers over stdio
crates/borg-protocol        the worker wire contract
crates/borg-engine          log, branches, defs, derivation, resolver, registry
crates/borg-cli             the `borg` binary
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
3. **Locks are per-layer, never per-branch.** A branch-wide write lock serialises derivation.
4. **Single writer per field.** This is what lets derived layers commit concurrently without
   conflicting.
5. **No membership test in the dependency index may be a linear scan** (§16.3). A widely-shared cell
   accumulates one dependent per invocation and each retracts itself on re-run; a `Vec` makes fan-out
   quadratic. This was measured, not guessed.
6. **`CellRef` is the shard key; `CellAt` is the record key.** Read-sets, the dependency index and
   ownership all key on `CellAt`. Keying on `CellRef` makes a migration observe its own output as a
   change to its own input.
7. **Writes are never coerced.** A value is stored at its author's ClientVersion, forever. Readers
   migrate on the read path.
8. **Derived data is never presented as fresh.** Every read returns a provenance envelope. A stale
   value is served *and labelled*, never silently served or withheld.
9. **A layer holds value events xor def events.** This is what makes "the def-version at layer L"
   well-defined.

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

- Derivation runs only when asked (`borg derive`). There is no scheduler yet.
- `FreshnessRequirement::Current` behaves as `Validated` — it does not compute inline yet.
- `settle()` is sequential; parallel workers need its round-ceiling reworked.
- `scan_buffer` and `read_layer` materialise results before streaming them.
- SQLite `Registry::open` rebuilds indexes by replaying the log — `O(log)` per CLI invocation.
- `Set`, `Map`, aggregation pipelines, mid-list insertion, container isolation and generated SDKs
  are all deferred (§18).

## Reporting work

State what you changed, what you verified, and what you did **not** do. If something is half-done,
say so plainly. A confident report on unverified work is worse than an honest partial one.
