# Roadmap and decision log

`SPEC.md` says what the system is. This says where the system stands, what is deliberately left
open, and *why* each design decision was taken — reasoning that would otherwise live only in
someone's memory.

Read it in order. **Where we are** is the state of the system today; **Open questions and
deferrals** is what is knowingly not done; **Acceptance scenarios** is the coverage map; the
**Decisions** are grouped by the part of the system they are about; and the **milestone history**
at the end is how it got here, kept because a decision reads differently when you know what it
replaced.

---

## Where we are

The **derivation half** of Borg works and is proven: field-granular invalidation through multi-hop
random access, migrations as producers, branching and merge, guards doubling as the merge-conflict
detector, definitions travelling the log, a SQLite backend behind a clean seam, and pipelines running
as subprocesses over a wire protocol — demonstrated by a pipeline written in bash.

Values are real: `String`, `Binary` and `BigInt` are interned on the way in and resolved on the way
out, invisibly, so a pipeline can finally read `company.website.ends_with('.ai')`.

The **definition half is now load-bearing.** Every cell write — from the CLI, from a producer, from
anywhere — is validated against the def-view of its branch, ownership is declared rather than
discovered, value parsing is directed by the declared type, and a repo emits its own struct
definitions alongside its producers.

**Migrations run end to end.** A repo declares a field mutation and the scripts that bridge it; the
def change and its migrations land in one layer; old data reads through the new lens on a fork while
the parent is untouched; a def-only merge carries both and the parent's values follow; and a client
authored against the old schema goes on reading and writing the old shape — or is told plainly that
it is broken, when the push supplied no way back.

**Derivation runs without being asked.** A write catches its branch up on the way out, a read can pay
to be current at the call site instead of taking the lag, a client can wait for the frontier to reach
its own write, and a report can read the whole branch at the point everything agrees. The automation
is pausable per branch, and a paused branch reports its lag in the vocabulary that already existed.

**And it runs concurrently.** A round discovers a wave of invocations and runs them at once; a
producer sees a peer's output because a round has a branch of its own and a client's concurrent write
is above its fork point; and a producer's workers are a pool rather than one process behind a lock.
The invariants that were written to permit this were checked rather than assumed, and three of them
were not true as implemented — see *Rounds as transactions* under **Decisions**.

**Versions are per definition, and watermarks compose.** A stored record is keyed by the def-version
of its own field rather than by whoever wrote it, so declaring an unrelated field no longer hides
data or quietly severs the dependencies recorded against it; and validation returns §10.3's
composition, so a producer reading another producer's output can reach `current` instead of being
reported stale forever. `110-def-push-keeps-data` and `120-invalidation-survives-a-def-push` are the
two scenarios; the second exists because S1 structurally cannot see that failure.

**Every client write is a transaction.** A client forks, writes in isolation and merges; `borg set`
is that done in one process. A transaction records what it reads, and at commit those reads *are* its
guards — so the safe path is now the only path, and last-write-wins is what remains for a cell nobody
read rather than what everybody gets by default. Two transactions racing to increment one counter
resolve to one increment in either commit order, and two racing to create one object resolve to one
object. Transactions are ephemeral and reaped on an idle timeout; branches are the durable form of
the same idea, and nothing reaps those.

**And a client can now make an object and find one.** `borg tx create <Struct>` allocates an id
nobody chose — under an allocator of its own, so what an application creates and what somebody types
as `Contact#5` can never be the same object — and `borg list <Struct>` names every object of a
struct, skipping the deleted ones. Both are on the socket and in the TypeScript client. The second
reverses §9.6's exclusion of enumeration, bounded by the reason for the exclusion: ids of one struct,
at head, unfiltered, unpaged, and outside any transaction, because an enumeration is not something a
guard can be asked about. The query layer is still out, and what it would cost is written down rather
than half-built.

**And the features have now met each other.** A migration runs while a client writes the field it is
migrating, in both merge orders; a def-only merge lands on a trunk that owes a round and neither
mislabels its output nor wedges it; a fork of a fork migrates data inherited through two levels while
both ancestors stay exactly as they were; and one contended workload settles byte-identically however
the scheduler orders it. That class found a lost update — a migration deleted its own guard, because
the guard subtraction was keyed on the cell rather than on the record — and it is the only producer
in the system that could have.

**And derivation settles a range rather than a layer.** A round covers everything between the
watermark and head, so a backlog no longer runs work its own guards are guaranteed to reject, and a
producer whose input exists only in derived data — a chained migration, a pipeline pushed over
something already derived — is discovered by an ordinary `borg derive` instead of needing a full
rebuild. What it gives up is written down in §6.3: how many intermediate derived snapshots a backlog
leaves is now a property of the schedule, and nothing can ask.

**And the first application found the cost nobody had multiplied.** `examples/personal-crm` is the
instrument and `FRICTION.md` is the reading; #9 measured a read costing 18 ms at branch head L441 and
53 ms at L1391 — a cost tracking the log rather than the request, because the server opened the
store per request and each open replayed the log. Both halves were documented; nothing said what they
multiplied to. The fix drew the seam first: the rebuilt indexes are `Projection`s of the log, opening
is *bring each to head*, and a process that stays up is already there. The server holds one deriving
registry, per-read cost is flat at 0.3–0.6 ms from L451 to L4001, and the two lifecycles are held to
the same answers by a rebuild-and-diff harness rather than by argument.
**And changing a pipeline's code invalidates what it wrote.** §9.2 promised this from the beginning
and it never once happened: `describe` describes shapes, `repo push` is a diff over shapes, and an
edit to a pipeline body changes no shape — so nothing was emitted, the producer's ClientVersion never
moved, and one field ended up holding output from two different programs, every value labelled
`current`. A repo now states an **implementation fingerprint** per producer, carried in the
`PushProducer` event because *which program this is* belongs to a producer's definition, and the
existing machinery does the rest. The same change makes `repo push` idempotent, which is not a bonus
but the precondition: recomputing a source buffer on every push would be worse than not recomputing
at all. `290-a-code-change-invalidates` is `examples/personal-crm`'s FRICTION #17, measured again and
the other way round.

**And a client is one string, and outlives its server.** A connection url — `borg://localhost/crm`,
or `borg+unix:///tmp/borg.sock/crm` — carries the two halves a client needs as one fact, so they
cannot be changed independently into a client pointed at one deployment's socket with another's
registry name. `borg+ws://` is reserved and refused by name rather than left to be invented. The
TypeScript SDK reconnects: a broken connection is torn down, the next operation dials and
re-handshakes, and what was in flight fails with an error that says it was **not** retried, because
`tx_commit` is not idempotent. Transactions survive it by construction, which is what §12.2 was for.
And nothing listening now says *no borg server at <addr> — start one with: `borg-server start`*,
identically from the CLI and the SDK — the exact sentence the first application wanted and did not
have. `examples/personal-crm` sheds server ownership with it: `dev.sh` ensures a server rather than
being one, pushes its schema into the running one, and the api survives the server being stopped
under it.

**And serving is a server.** `borg-server` is a binary of its own, hosting a **directory of
registries** — every store under a data dir, addressable by name, one socket for all of them because
the handshake routes. The registry is the unit of tenancy, which makes this the local instance of a
multi-tenant platform rather than a smaller different thing; what the platform adds is a credential
that means something, and the field for it is already on the wire. Registries open lazily and lock
eagerly, `start | stop | status | logs` is the operating surface, and the flaw the first application
was built around is gone: a **schema can be pushed into a running server**, because the server
performs the push and the fingerprint work made a push cost what the change costs. `borg` keeps its
embedded mode and loses `serve`.

Act 1 is the modern ORM.

---

## The production arc (P1–P3)

The project is reorienting to a deployed product: borg-hq.com, run from a Proxmox host now and a
cloud later, with the platform's own control plane built on Borg itself.

**Decisions taken (with the reasoning that survives):**

- **The engine is single-tenant, everywhere.** Tenant isolation lives at the VM boundary — the
  strongest primitive available — so pipelines survive on every plan and container isolation
  returns to being defense-in-depth rather than a security prerequisite. Cheap plans are cheaper
  VMs, not shared engines. A server's registries mean one org's *projects and environments*, not
  many customers. The trade held with open eyes: VM-per-tenant has a per-tenant floor cost; the
  eventual fix is lighter VMs, in the platform's provisioning layer, never in the engine.
- **Control plane / data plane split.** The platform (private `borg-cloud`) owns identity, orgs,
  memberships, plans, provisioning and routing; `borg-server` owns registries and verifies
  platform-issued credentials without owning identity — signed tokens, no phone-home, so on-prem
  verifies offline. Orgs are platform data, and the engine never learns what one is.
- **The platform runs on Borg** — first production Borg application, registry `platform`, with the
  bootstrap loop (platform needs its own borg-server up) accepted and documented.
- **Open core.** This repo (engine, server, CLI, SDKs, examples) is public as `jakequist/borg-core`
  under **Apache-2.0**; `borg-cloud` is private. Apache now does not preclude FSL/BSL later for
  future versions — the flip cost is a fork of the last Apache release, negligible pre-traction —
  but relicensing requires owning all contributions, so **a CLA must exist before the first outside
  PR is accepted**.
- **Repos are siblings, never nested; borg-cloud pins artifacts, not source.** A gitignored nested
  clone records no version (irreproducible CI) and gitignored-but-precious directories are how the
  CRM's data got deleted. borg-cloud consumes the server image, SDK and CLI by pinned version, with
  local path-overrides for same-day cross-repo work — feeling exactly the packaging a customer
  feels.
- **Format policy: guarantee the data, not the bytes.** Pre-1.0 on-disk formats may change; every
  release exports a canonical event stream and imports streams of prior releases; upgrades are
  export → upgrade → import. Additive changes stay serde-compatible without ceremony. **Built** —
  SPEC.md §19, `crates/borg-host/src/stream.rs`, `scenarios/320-export-and-import`. See *Export and
  import: which sidecars are state* below for the decisions the build forced.
- **Secrets live in Doppler** (project `borg`); deploys target the Proxmox host (`m3`) first with a
  thin VM-provider seam named for the eventual cloud move.

**P1 — networked, authed, deployed:** WebSocket transport (browser-ready, rides standard infra) ·
hello acknowledgement (closes the routing deviation) · static org-scoped API keys · ~~export/import~~
**done** · Dockerfile + CI · one server live on the Proxmox host.
**P2 — the platform:** control-plane app on Borg — orgs, users, memberships, token issuance,
provisioning, subdomain routing, platform.borg-hq.com.
**P3 — tiering:** dedicated-server provisioning via the Proxmox API, on-prem packaging, and
(optionally, as hardening) containerized workers.

## Open questions and deferrals

What is knowingly not done, and what it would cost. Nothing here is a bug; each is a decision to
spend the effort elsewhere, and each says what would have to be true to change that.

### Deferred features

Aggregations, `Set`/`Map`, container isolation, generated SDKs. Nothing has argued for pulling any of
them forward, and the CLI is doing the SDK's job well enough to keep learning from it first.

`O(1)` merge, explicitly. A parent layer that *references* a child layer's event set rather than
enumerating it is what would make merge asymptotically free; the model now permits it and the old
one forbade it. It needs read-path compaction to pay for itself, so what landed is the honest
version: `n` membership rows and `n` index entries per merged layer instead of `n` full records.

**Concurrent requests within one registry.** The server holds one registry per hosted store now, so
the per-request replay is gone, but requests are still serialised through one gate per registry. (The
gate stopped being *server*-wide when a server started hosting several registries — see *A server
hosts a directory of registries* — which is a different change: it lets two tenants proceed at once
and nothing about one tenant's requests.) Relaxing it is a separate
change and needs a soak of its own: the engine's internals are `Arc`/`Mutex` and were soaked at
parallelism 16 in the concurrency milestone, but that proves the engine tolerates concurrent *tasks*,
not that two client operations may interleave mid-flight. What has to be established first is the
sidecars — the transaction table, the pause flags and the PID counter are read-modify-write on files
beside the store, and today the gate is the only thing making that safe. The gate is also what buys a
served store the serialisation process-per-command gave the CLI for free, so removing it is a change
to what a client is promised, not only to throughput.

A **`Watermark` newtype**, and it is the one deferral that has already cost four bugs. Four pairs of
`LayerId` have been conflated so far — `read_at` vs `reflects`, `authored` vs `landed`, the round
ceiling as a bound vs as a filter, and a derived dependency's landing layer vs its watermark — and
every one was two ids of the same Rust type meaning different things. A watermark is a position in
the **source** stream (§10.1); `landed`, `authored` and a read ceiling are positions in the layer
stream, which includes derived layers. Wrapping the first would make `landed_at > fresh_as_of` fail
to compile, which is exactly the line that had to be rewritten here.

What stopped it being done now is that it is not only a rename. `Resolved.fresh_as_of` is compared
against, and set to, the layer being read — a ceiling that is usually a *derived* layer once a
branch has settled — so a `Watermark` type immediately asks whether the read target should be the
highest **source** layer at or below the ceiling instead. That is almost certainly the right answer
and it makes §10.1 literally true, but it changes the layer id every settled derived read reports,
which `100-watermark-truth` forks at and replays. It deserves its own change with its own scenario,
not a rider on a bug fix.

### Concerns carried over from the transactional-model draft

`SPEC-DRAFT.md` — the three-phase draft that became §12, §13 and §16.5 — is deleted. Everything it
proposed is built and normative in `SPEC.md`, and a superseded draft left lying around gets quoted by
accident. It raised eight concerns; four were answered by building the thing, by measuring it
(milestone G has the numbers), or by being recorded in `CLAUDE.md`. These four were not, and are
still live.

**A better predictor of a doomed transaction than idleness is divergence** — layers committed on the
parent since the fork. A transaction open an hour on an idle store is harmless; one open ten seconds
on a busy store already carries guards certain to fail. Measuring that would turn reaping from
janitorial into useful: it could tell a client to give up rather than making it wait for a merge that
cannot succeed. The idle timeout (§12.3) is what shipped.

**Branch proliferation, and no reaper.** One branch per transaction plus one per round, retained
forever in a table with no GC. They are cheap, and `CLAUDE.md` records that nothing collects them.
Whether spent branches are reaped or kept as history is a real choice and should be made
deliberately rather than by a janitor as a side effect.

**A long transaction is a large guard set, and a likely conflict.** Read-sets are unbounded: a
transaction reading ten thousand cells carries ten thousand guards, all checked at merge, and the
longer it runs the likelier one of them moved. That is the ordinary optimistic-concurrency trade.
Borg's guards are *cell-granular*, so a long transaction fails only if something it actually touched
moved — precise rather than merely numerous. Worth measuring rather than assuming; nothing measures
it today. Code comments citing "§7.7" for this are citing the deleted draft.

**A client read-set only covers what went through the transaction.** The risk is not correctness but
*expectation*: a user who reads outside a transaction and writes inside it will believe they were
protected. §12.1 says so plainly; the CLI does not make the boundary visible at the moment it
matters.

---

## Acceptance scenarios

Organised by the *failure class* they attack rather than by feature, because the bugs this project
has actually hit cluster into a few repeated shapes: ordering assumptions that hold sequentially and
break concurrently, two similar-looking quantities getting conflated, features that work alone and
not together, and labels that claim more than was verified.

Each says what would be broken if it failed. That is what makes it a stress test rather than a
feature test. The `S`-numbers are cited from scenario headers and test files, so they are names
rather than decoration.

### The claim that must not be false

**S1 — every watermark tells the truth.** For any derived cell: read its stated `reflects`, fork
there, recompute from scratch, compare. Identical, always. — `scenarios/100-watermark-truth`.

This checks §10.1's headline claim directly rather than by proxy, and every ordering bug found so
far would have surfaced here. It is a *property* over whatever state other scenarios leave behind,
which makes it the cheapest ongoing insurance available.

### Guards, the newly load-bearing mechanism

**S2 — a stale transaction is rejected in either merge order.** *Failing means order-enforcement
crept back in.* — `scenarios/140-transaction-conflicts`.

**S3 — absence is a guarded read.** Two transactions both observe a cell absent and both try to
create it; one must lose. *Failing means absence tracking is decorative and concurrent creates
silently duplicate.* — `scenarios/140-transaction-conflicts`.

**S4 — a transaction does not conflict with itself.** Write `X`, read `X`, commit. *Failing means the
parent-reads-only rule is wrong and every read-modify-write deadlocks itself.* —
`scenarios/130-transactions`.

**S5 — guards do not over-reject.** Two transactions writing different fields of one object both
land. *Failing means guards are object-granular in practice and cell granularity is fiction.* —
`scenarios/130-transactions`.

**S6 — deleting an object conflicts with writing to it.** *This is the test for "implicit reads
count": the writer's existence probe is what makes it a conflict.* —
`scenarios/140-transaction-conflicts`.

### Derivation as a transaction

**S7 — a chained producer does not trip its own round's guard.** *Failing means rounds containing any
producer chain never commit.* — `crates/borg-engine/tests/rounds.rs` and
`scenarios/160-rounds-are-transactions`.

**S8 — a stale round cannot land, in either order.** *Failing means the deleted ordering rule was
necessary after all.* — `crates/borg-engine/tests/rounds.rs`.

**S9 — one contended cell does not kill a round.** *Failing means one hot cell starves a large round
forever.* — `crates/borg-engine/tests/rounds.rs`.

**S10 — a client merge landing mid-round produces a true watermark.** The original motivating bug,
now structurally impossible. *Failing means the branch boundary does not express the filter and we
have re-derived the ceiling problem.* — `crates/borg-engine/tests/rounds.rs`. S8–S10 are Rust tests
rather than scenarios because the CLI is process-per-command and layer ids are minted per process
(§17.2), so two `borg` processes overlapping in time is a corruption rather than an interleaving.

### The events/layers inversion

**S11 — authorship survives merge.** A merged value reports both where it was authored and where it
landed. *Failing means we inverted the pointers and kept rewriting anyway: no cost saved, no lineage
gained.* — `crates/borg-engine/tests/events.rs`.

**S12 — time travel across a merge is coherent**, and one event referenced by two layers resolves to
one identity rather than two values. *This is the specific risk the inversion introduces.* —
`crates/borg-engine/tests/events.rs`.

### Composition — features that have never met

This class has produced the most bugs and had the least coverage.

**S13 — migration under a concurrent client write.** *Migrations and concurrency had never been
exercised together.* — `crates/borg-engine/tests/composition.rs` and
`scenarios/170-migration-under-a-concurrent-write`.

> **This one found a bug, which is what it was for.** A round guards *what it read minus what the
> round wrote*, and the subtraction was keyed on `CellRef`. A migration is the only producer that
> reads and writes one cell at two def-versions, so it subtracted away its own guard on the source
> record it migrates from — and a stale migration round landed over a fresher one, permanently, with
> every watermark advanced and nothing outstanding to correct it. The subtraction is over `CellAt`
> now; `SPEC.md` §16.5 states the rule and *A round's guard subtraction was keyed on `CellRef`*
> below records why it was not caught before.

**S14 — a def-only merge landing while a round computes under the old def.** —
`crates/borg-engine/tests/composition.rs` and `scenarios/180-a-def-merge-during-a-round`. The answer
is coherent: a round folds its def-view once, at the trunk, when it opens, so it produces the version
chain it saw; the output is filed at the version it was computed under and never at the one that
overtook it; and a def layer carries no value events, so it can trip no guard. What it turned up was
a limitation — a migration chained onto a migration was not discovered by a catch-up — which
milestone I closed.

**S15 — a second-order fork with a migration**, migrating data inherited through two levels. —
`scenarios/190-a-fork-of-a-fork-migrates`. Nothing was wrong: a migration on a fork-of-a-fork carries
values it inherited from both ancestors, neither ancestor moves, and merging inward moves exactly one
branch per step.

### Tenancy

**S17 — one server, two registries, addressed independently.** Two clients name two registries over
one socket; a write to one is invisible to the other, a *repo push* into one leaves the other's
definitions byte-identical, and `status` names both. *Failing means the tenancy seam is decoration
and the platform needs a different server rather than this one with a name in a handshake.* —
`scenarios/300-a-server-hosts-registries`, and `crates/borg-server/src/serve.rs`.

**S18 — a schema pushed into a running server takes effect in it.** Both halves: the definitions,
which the held registry must answer from without being reopened, and the *code*, which the worker
pool must be running rather than what it was built with at boot. *Failing means either a second
`Registry` over one store — which the single-process assumption forbids — or a producer silently
executing the program the log no longer describes, which is the FRICTION #17 failure arriving by a
different route.* — `scenarios/250-serve` and `crates/borg-server/src/serve.rs`. The second half is
the one that needs a live check: it was verified to fail with the pool reload removed.

**S19 — a client is configured by one string, and survives its server restarting.** A url naming a
registry reaches that registry over a socket hosting several; an address with nothing on it says how
to start one, in the same words from the CLI and the SDK; and a session that is busy right through a
server bounce loses exactly the operation that met the dead socket — with an error saying it was not
retried — and carries on, committing a transaction that was open before the restart. *Failing means
a client is configured by two variables that can disagree, or that a server restart is an
application restart, which is what it was.* — `scenarios/310-connection-urls`,
`packages/borg-sdk/test/client.test.ts` and `crates/borg-protocol/src/url.rs`.

### Durability

**S20 — a registry and its restore are the same registry.** Export a store with real complexity in it
— two branches, a merge sharing events rather than copying them, a field materialized at two
def-versions with a migration between them, derived data, one interned string shared by two cells, a
producer poisoned after it had succeeded, a paused branch and an advanced PID counter — import it
into a fresh store, and require identical answers: every cell's whole envelope on every branch,
`explain`, the def views, and the same further write deriving the same way. Then export the import
and require the *bytes* to match, which is the cheap total check. *Failing means the format policy is
aspirational and on-disk formats are frozen in practice, because there is no supported way off
them.* — `crates/borg-host/tests/export_round_trip.rs`, and
`scenarios/320-export-and-import` for the same claim through the real binaries, plus the one only a
CLI can make: an object created after a restore does not land on the address of one that existed
before it.

### Determinism

**S16 — identical settled state across many parallel runs**, asserting the settled result and never
the schedule. — `scenarios/200-determinism`.

Frequency matters: milestone C's ordering bug appeared **one run in six**, and an `EPIPE` panic **one
in forty, under load only**. Both read as flakes and were not. Fewer than ~50 runs is not evidence.
So the run count is `BORG_DETERMINISM_RUNS`, defaulting to 5 so `./check.sh` stays a smoke test.

---

## Decisions

Design decisions taken in conversation, with the reasoning. Where these change the spec, the spec is
the normative statement — this records *why*, including the reasoning behind rules that have since
been reversed, because a rule reads differently once you know what it replaced.

Grouped by the part of the system they are about rather than by when they were taken; the milestone
each belongs to is in the history at the end.

### Values, addresses and text forms

#### Cell syntax uses a colon, not parentheses

`Company:o-1234abcd.website`. Parentheses read well but are shell metacharacters, and we have taken
a deliberately shell-first stance on the worker protocol. The colon buys the same readability while
staying shell-safe by construction.

`Company#1` remains accepted **on input only**, as a documented convenience for hand-authored data,
meaning "root branch, allocator 0, counter 1". Output is always canonical.

#### Server-allocated ids take an allocator of their own

`tx_create` (§17.5) issues ids under `AllocatorId(1)`; allocator `0` stays the shorthand's, which is
to say the one belonging to whoever is typing. This is the first cash-in of `(branch, allocator,
counter)` and it arrives one node before there is a second node: the component exists so that
allocating authorities need not coordinate, and there have been two authorities since the first
scenario — a person choosing counters, and now the store choosing them on an application's behalf.
Separate allocators make the two id spaces disjoint **by construction**, so `borg tx create` is safe
to run against a store full of hand-written fixtures and neither side has to know the other exists.
A shared allocator would have needed the store to read what already exists before creating anything,
which is the coordination point §3.1 was written to remove.

The counter itself is a sidecar (`borg.allocations.json`), and the reasoning is in SDK-DRAFT §4.5:
`InProcessSequencer::resuming_after` resumes from the store because the log answers "the highest
layer" in one read, and there is no equivalent for a PID — a counter spans every struct, so deriving
it means scanning every object buffer, which makes creating `n` objects `O(n²)`. Written before the
write it names, so a crash burns an id rather than issuing one twice. What it costs is in
`CLAUDE.md`: it is the one sidecar whose loss a store cannot recover from.

#### `BufferId` has no interning variants

`String`, `Binary` and `BigInt` were dropped from `BufferId`. §4.2 already said the interning stores
hold *values, not cells* — an interned value has no version, no origin and no writing layer, so every
field of a stored record is meaningless for it (a `CellRecord` when this was decided; an `Event`
since milestone E). A `BufferId` variant therefore named a cell partition
that cannot exist, and would have been the first place a branch or a layer crept back into a scheme
whose entire value is having neither.

`AnyObject` and `AnyArray` stay. Those are mutable containers, so their contents genuinely are cells,
even though nothing implements them yet.

Dropping them forced `CellRef`'s `Display` to become total, which it should always have been: the old
`{:?}` fallthrough emitted an unparseable second dialect in exactly the places — panics, lineage
output, error messages — where a pasteable address matters most.

#### Bare values parse as strings

`borg set Company#1.website acme.ai`. No quotes and no prefix: a shell worker is the target audience,
and a form that needs quoting is one that will eventually be typed unquoted. `0x…` is `Binary`, a
trailing `n` on digits is `BigInt`, and everything unmatched is a `String` — which makes value
parsing infallible, since every input names some value.

The cost was real and was documented rather than hidden (§3.4): `true` is a `Bool`, so a string field
could not hold the text "true"; likewise `42`, `0xff`, `7n`. Quoting was the alternative and it buys
the same thing at the cost of the shell-first stance.

**B resolved it**, as predicted, by making parsing type-directed against the declared `FieldDef`: the
write path calls `parse::value_as`, so a field declared `String` reads `true` as four characters and
a field declared `Int` refuses `acme`. The guessing parse survives only where there is genuinely no
declared type to consult — an `Any` field, an error message, a `describe` payload — so its
reservations no longer reach stored data.

#### Interning is invisible to workers

A pipeline reading `company.website` receives `acme.ai`, never `@s-1a2b3c`. A worker writing a string
sends the text and is finished; the engine interns it. No second round trip in either direction, and
nothing above the storage line needs to know that content addressing exists — the same call as
batching being a runtime concern rather than a user concern (§17.1).

Where the conversion lives took some deciding. It is **not** the resolver: resolution deals in
`Value`, the engine's internal currency, and rendering there would push every internal consumer
through a string round trip to serve the two edges that actually want text. It is **not**
`ProducerCtx` alone either, because `borg set` writes source cells with no `ProducerCtx` in sight, and
a second implementation there is how two dialects start. It is one engine-level type beside storage
(`borg_engine::values`), which `ProducerCtx` exposes and delegates to — the exposure being necessary
because a producer runtime holds no store handle and must not acquire one.

### Definitions, versions and the write path

#### Field ownership is declared, not discovered

§8 originally said ownership is discovered at runtime. Once B lands, every write must name a declared
field — so a producer's output field must be declared too, and the only thing that knows it exists is
the repo implementing the producer.

What we ruled out earlier was *derivation writing back into defs*, which would mean the engine
emitting def events. An author declaring ownership up front is different and strictly better: a
violation is caught on the **first** wrong write rather than on a second producer's collision.
Runtime enforcement becomes a check against the declaration rather than the mechanism.

#### Repos emit their own definitions

`describe` should return repo identity, struct definitions and producers together, and
`borg repo push` folds all of it into **one def layer** — a producer and the field it writes should
land together or not at all.

This is not a convenience. After B, a producer cannot write anything unless its output field is
declared, and the repo is the only thing that knows. It also sets up the DSL story: a Python repo
defines structs through the SDK, the runtime emits them on `describe`, and `defs/*.json` becomes one
way of producing the same thing rather than a parallel path.

#### A schema change is a diff, not an instruction

`describe` has no "mutate this field" — a repo emits the shape it believes in now, and `borg repo
push` compares it with the definitions in force. A field nobody declared becomes a `DeclareField`;
one whose type moved becomes a `MutateField`; one that is unchanged is a repeat.

A repo *cannot* express the mutation directly, because it does not know what it is mutating from: the
branch does, and on another branch the answer differs. The same repo pushed on main and on a fork
that already changed the field means two different things, and only the store can say which. It also
follows from repos emitting their whole schema every push — the diff is where that idempotence and
schema evolution meet, rather than two mechanisms sitting beside each other.

The migrations are named on the *field* (`up`, `down`) rather than beside the change, because the
field is what persists across pushes and the change does not.

#### A producer's implementation is part of what the diff compares

The decision above has a hole in it that took an application to find, and `examples/personal-crm`'s
FRICTION #17 is the measurement: *a repo emits the shape it believes in* is exactly right, and **a
pipeline's code is not part of its shape.** Editing a body changes no field, no name, no `writes`, no
source buffer — so the diff found nothing to emit, no def layer landed, the producer's ClientVersion
stayed where it was, and §9.2's *invalidates all of its prior output* never fired. The store then
held one field written by two different programs, both labelled `current`, with `state`, `origin` and
`by` identical. That is a watermark lie by S1's own standard — fork at `reflects`, recompute with
today's code, and the two disagree — reached by a route S1 cannot sweep, because S1 replays with
whatever code is deployed *now*.

The fix is one opaque string per producer whose only contract is that it changes when the code
changes. Four choices in it are worth keeping written down:

**It travels in `PushProducer`, not beside the command on disk.** The producer-implementation table
(§9.2) is where "which file is this" lives, and putting the fingerprint there was the obvious
alternative — it is where the pusher computes it, and it needs no event. It is wrong for the same
reason the table is right: that table is a fact about one machine, and this has to be forkable and
mergeable, because *is this the code the branch believes in* is a question with a different answer on
a different branch. It also has to be in the def fold, because the fold is where the diff already
lives. Putting it in the event costs nothing the definition was not already paying.

**The engine learns nothing.** `backfill` compares a producer's watermark with its own ClientVersion
and hands over the whole source buffer when it is behind — the same path a producer newer than its
data already took. Nothing in the scheduler knows what a fingerprint is, and it must not: the moment
"a code change" is a concept in derivation rather than a fact about a definition, every question about
it becomes a scheduler question.

**An unchanged push must emit nothing, or the guarantee is unusable.** Recomputing is
`O(the producer's source buffer)`, and `repo push` is a deploy command people run in a loop. Paying
that per edit is the price of the guarantee; paying it per push is a mechanism people disable. So the
same change made the diff complete — repeated `DeclareField`s are dropped too, which the fold already
treated as no-ops — and `repo push` became idempotent, closing FRICTION #2 as a side effect and
deleting the `cksum` stamp the CRM's `dev.sh` had grown.

**Absent is a documented answer, not a gap.** The pusher hashes the command file when `describe`
supplies nothing, which is what covers a `bash`-and-`jq` worker without asking it to compute a digest
— an SDK-only mechanism would have made this a feature of languages with SDKs, which is the opposite
of what the worker protocol is for. An SDK supplies its own only where it covers *more*: Python walks
the modules beside the entry file, TypeScript cannot (ESM has no loaded-module registry) and says so
rather than implying a guarantee it does not keep. A producer neither route can fingerprint behaves
exactly as everything did before, written down rather than discovered.

What this deliberately does not do: **it is not a schema change.** No `MutateField`, no migration
demanded, no field def-version moved. A migration bridges two shapes that coexist and both remain
readable; here the old output is simply wrong, and the right thing to do with it is overwrite it.

#### A migration definition records a direction, not a version pair

`ProducerKind::Migration` used to carry `from` and `to`. It cannot: a def-only merge replays the
`MutateField` that appointed the migration onto the parent as a **different layer**, so the pair
baked in on the fork names versions no reader on the parent will ever ask for — the headline scenario
would produce a migration writing into nowhere.

Which two versions a migration bridges is a fact about the branch's version chain (§5.3), folded from
the `MutateField` alongside everything else. The author declares the one half that is genuinely
theirs — which direction this code runs in — and the log supplies the rest. The same reasoning
retired the authored `version` on every `ProducerDef`: a producer's ClientVersion *is* the def-layer
it was pushed at, and that id does not exist until the layer opens.

#### Two def-views on the write path

Shape is checked at the writer's ClientVersion; permission at the branch's (§8.0). Milestone B
validated everything against the branch and recorded that as a known shortcut, and it defeats exactly
the feature C exists to demonstrate: a client authored before a schema change writes the old shape by
definition, and so does a `down` migration.

The reverse split does not work either. Permission cannot be a ClientVersion question, because a
`down` migration's own view is *older* than the `MutateField` that named it as the field's `down` —
asked there, the branch's own appointed migration is an ownership violation.

Where a writer is current the two views are the same object, which is the common case. Where the
branch has since dropped a field the writer knows about, permission falls back to the writer's
declaration: an old client is entitled to the schema it was written against.

#### The CLI's ClientVersion is the branch's def-version, and nothing is recorded

Considered and rejected: a sidecar file beside the store, like the producer-implementation table. It
looks symmetric and is not. An implementation table records something *true* — where code lives — and
a remembered client version records something that was true once: it goes stale the moment anyone
pushes a def, and a single value is wrong for any branch it was never synced on while a per-branch
map is state nobody asked for.

So the CLI is a client that regenerates itself on every invocation: its ClientVersion is the schema
as it stands, which is exactly what a freshly generated SDK would carry. `--client-version` pins an
older one, and has to exist — §5.4's whole claim is that a v1 client keeps working after the schema
moves, and with no generated SDKs (§18) there is otherwise no way to *have* a v1 client to test it
with. `borg def version` prints the default, which is what makes the concept visible at all.

#### `up` and `down` are two projections of one value

Neither triggers the other, on either trigger path. Each writes exactly the version the other reads,
so unfiltered they run until the cycle detector fires (§16.6) — on the ordinary configuration rather
than on a cycle. This is the one filter by author, and §9.3's rule that the read-set trigger is *not*
filtered by author still holds for everything else, which is what keeps genuine cycles catchable.

The same fact bites in seeding: a producer that has never run takes its whole source buffer as work
(§9.6), and `down` seeded after `up` had already derived the new version would migrate that back and
overwrite the source value `up` read it from. Filtering the seed by the same step membership makes
the round order-independent, which it has to be — nothing prescribes the order producers run in.

#### Writing a property implies the object exists

Producers map over a struct's `ObjectBuffer`, which holds existence cells, so an object whose fields
were set but which was never explicitly created is invisible to every pipeline.

Only when absent, never on every write: the existence cell lives in the buffer producers subscribe
to, so rewriting it would make any property write look like a new entity.

#### A record is keyed by its field's def-version, not by its writer's ClientVersion

`DefVersion` is now a type of its own. `ClientVersion` is a whole schema and advances on every def
push; a def-version belongs to one definition and advances only when that definition is mutated
(§5.3). Storage, `CellAt`, `Event`, read-sets, the dependency index, migration chains and
`ProducerCtx::get_at` all speak the second; every actor still carries the first, and
`DefView::version_of` is the only bridge.

Keying records by the writer's ClientVersion made a def push that named some *other* field move
every subsequent write to a version no reader looked for. Two consequences, and the second is the
one that hurt: the value read back `broken` — correctly, since a field that never changed shape has
no migration chain and so no route to the new version — and, invisibly, every read-set entry
recorded against the old version stopped matching, so invalidation stopped with no error, no `stale`
and no watermark left behind. A derived value simply froze at its last computation and went on
calling itself current.

`100-watermark-truth` cannot see the second half, and understanding why is the point: a producer
authored at the old def-version genuinely reads the old world, so replaying at the watermark
faithfully reproduces the frozen value. The result is self-consistent and wrong.
`120-invalidation-survives-a-def-push` compares a derived cell against the source cell it is a copy
of instead, which is the comparison a replay structurally cannot make.

The type is what keeps this from coming back. There is no arithmetic from one version to the other —
the answer is a fact about the schema — so the compiler now insists a def-view be asked.

#### Existence cells, lists and untyped containers are unversioned

They have no `FieldDef`, sit on no migration chain, and nothing about their shape can change. Before
they took the writer's ClientVersion, which meant a def push made `imply_existence` write a *second*
existence cell for an object that already had one — into the very buffer producers map over, so
every declaration looked like a fresh entity to every pipeline. `DefVersion::UNVERSIONED` is one
fixed key that stays findable across every push.

#### A migration cannot be appointed for a derived field

Noticed while designing S14 and left alone. `DefView::check_ownership` matches
`(Ownership::Derived(owner), Writer::Producer(attempted))` before it reaches the migration exemption,
so the exemption is reachable only for a `Source` field — a `MutateField` on a derived field would
name migrations that are then forbidden to write it.

Left as it is because it is not obviously wrong: a derived field's shape is its producer's business,
and a producer that changes its output type re-derives rather than migrating. But nothing *says* so,
and the failure would arrive as an ownership violation at run time rather than as a rejection at push
time. Worth a decision of its own rather than a rider.

### Events, layers and the read index

#### Layer membership is not part of a layer's metadata

The transactional-model draft sketched `Layer { …, events: [EventId] }`, and taking that literally
would break the constraint that governs everything else about layers: a layer may hold millions of
events, and its metadata is written and read *whole* (`put_layer_meta` is one row). A `Vec<EventId>`
on it would buffer a layer's membership in memory to write one commit.

So membership lives in storage as a `(layer, event)` relation, enumerated by `read_layer` and
extended by `include_event`. `Layer` in `borg-core` is unchanged. The model is the draft's — layers
reference events — but the representation is a relation rather than a field, for the same reason
commit streams.

#### The read index is durable, and not rebuilt on open

The dependency and touch indexes are rebuilt by replaying the log at every `Registry::open`, and the
obvious symmetry would be to do the same for the `(branch, cell, version) -> (layer, event)` index.
It is the wrong symmetry: those two are in-memory caches of a process, and this one is what makes a
read a single indexed lookup in the store itself. Rebuilding it per CLI invocation would turn the
`O(log)` read we already pay into an `O(log)` *write*.

It is still a projection, and `rebuild_read_index` on `StorageProvider` is how that stays a fact
rather than a claim: a test throws the index away, rebuilds it from membership, and asserts no
answer changed. Nothing on the read or write path calls it.

#### The index is maintained on the way in, not at commit

Index rows stream in with the events they project and are invisible by the same join against the
layer's state. Building the index at commit instead would make commit `O(rows)` — the identical
mistake as flipping a `visible` flag per row, which §17.1 already rejects. It also makes the merge
case correct by construction: the membership and the index entries for a merge land in the same
invisible layer, so a read can never see one without the other.

#### Two writes to one cell in one layer are two events, and one index row

Membership keeps both — the layer really does contain two, and `read_layer` yields both, which is
what the invalidator sees. The read index keeps one, the later. That is the same collapse the old
`cells` table got from its primary key, and stating it explicitly is what keeps `MemoryStorage` and
SQLite answering identically: with one index row per landing layer, "the newest landing" is a
maximum with no tie to break, in either backend.

### Transactions and guards

#### Enumeration is a read, and there is no guarded form of it

`list` (§9.6, §17.5) is outside any transaction, and the reason is not that a `tx_list` would be
extra work — it is that there is nothing coherent for it to guard. A guard is a question the
cell-touch index answers about **a cell** (§12.4), and "the set of Contacts" is not a cell. The
honest guard would be *"no object of this struct was created or deleted since the fork"*, which is
the absence-guard problem §12.1 solves for one cell, widened to a whole buffer: correct, and coarse
enough that every creation would conflict with every enumeration.

So a listing buys what a `get` outside a transaction buys, which is nothing at commit — and that
cost is real rather than theoretical: *list, decide, write* has no protection against an object
having appeared in between, and the available workaround (guard the specific objects you acted on)
does not cover a decision that depended on the *set*. Shipping it this way is a bet that an
application which cannot enumerate at all is worse than one that can enumerate unguarded, and it is
the same bet §12.1 already makes for reads outside transactions. SDK-DRAFT §5 carries the three
shapes a guarded form could take and why none was built.

The related decision is that `list` answers **ids only**. One requested field in the reply would
answer a single shape of question and leave filters, ordering, joins and aggregates exactly where
they were, while making the first thing anybody builds on §17.5 a thing that has to be un-built. The
N+1 is left visible on purpose; it is a finding waiting for a query layer.

#### The read-minus-written rule is about order, not about set difference

The transactional-model draft said *"guard the cells you read and did not write"*, and taken
literally as a set difference it deletes compare-and-swap. A transaction that reads `X` and then
writes `X` — the ordinary read-modify-write, and the case the draft itself said the guard should
*fall out* of — would have `X` in both sets, so `X` would be dropped and two concurrent increments
would both land with one silently lost.

The reason the draft gave is the correct rule: a read that returned the transaction's **own write** says
nothing about the parent. So a read is recorded unless the transaction has *already* written that
cell, and `Transaction::observe` enforces that at the moment of the read rather than by subtracting
sets at the end. Write-then-read contributes nothing; read-then-write is a compare-and-swap. §12.1
now states it this way, and `140-transaction-conflicts` is the counter-example that would have
caught the set-difference version.

#### `since` is the fork point for every guard, and per-read tracking would be wrong

The obvious refinement — record *when* each read happened and use that as its `since` — is not merely
unnecessary, it is unsound. A transaction's read path is bounded at the fork point (§7.2), so every
read observes the parent as the parent stood *then*, whenever during the transaction's life it
happens. Using the moment of the read would ignore every parent write between the fork and the read
— writes the transaction provably did not see, because they were above its bound — which is the exact
set a guard exists to catch. One `since`, and it is the snapshot the reads came from.

#### An automatic guard on a derived cell is dropped, not rejected

`check_guards` rejects a guard naming a derived cell (§12): guarding a shadow is meaningless. Applied
to an automatic guard that rule makes a transaction unable to commit *because it looked at a computed
value*, which is a strange thing to punish — and it catches migrated data too, where the field is
declared source but its records are a migration's output, so reading `Company.founded` in
`100-watermark-truth`'s store would have been enough.

`LayerManager::check_reads` therefore asks the touch question and not the derivedness one. It is also
the cheaper half: the touch index records source layers only, so a derived cell can never be in it
and the guard could not have tripped anyway, while `is_derived_anywhere` costs a storage read per
cell per version on a read-set that is unbounded by design. The hand-written guard keeps its rejection,
because that one is a client asserting something it cannot mean.

#### The implicit existence read counts for `borg set` too

The transactional-model draft said a bare `borg set X v` "reads nothing, so it carries no guards".
That is true of the cell it writes and false of the object it may create: the implied-existence probe (§8) is a
read, and `borg set` is *the* common path on which two clients race to create the same object. Making
the one-shot behave differently from `begin; set; commit` would also make "every client write is a
transaction" true only in the telling.

So the one-shot carries exactly the guards the explicit form would: none on the cell it writes, which
is last-write-wins as §12 promises, and one on the existence cell it probed. When the object already
exists that guard can only be tripped by a concurrent *deletion*, which is a conflict anyone would
want reported.

#### A transaction that fails to commit stays open

A rejected commit leaves the transaction where it was rather than aborting it. Its snapshot is stale
and its commit cannot succeed, but the read-set is what a client needs in order to decide whether to
retry or give up, and destroying it there leaves them holding an error and nothing else. `borg tx
abort` is the explicit half, and the idle timeout collects the ones nobody comes back to.

#### An empty branch's first write is not a transaction, and cannot be

A transaction forks the highest layer its branch can see, so a branch whose entire ancestry is empty
has nothing to fork. `borg set` on such a branch writes directly. This is safe rather than a hole:
§8.0 makes every write contingent on definitions, definitions are def layers, and a branch with no
layers has none — so the write is going to be rejected whatever path it takes, and taking the direct
one is what gets the caller *"no struct named `Wombat`"* instead of *"nothing to fork from"*. There is
also nothing to isolate from: anything concurrent would have left a layer.

`scenarios/060-definitions-enforced` opens on exactly this write, which is how the case was found.

### Scheduling, freshness and the read path

#### Auto-derivation is a branch-scoped switch

Default on, but pausable per branch — useful for deterministic testing and for freezing automation in
an emergency.

Two calls:

- **It is operational config, not log data.** Pausing does not change what is true, only when the
  system catches up. It lives beside the store like the producer-implementation table. In the log it
  would be branchable and time-travellable, which sounds elegant and is meaningless — nobody wants to
  ask "was derivation paused at layer 400?".
- **Pause means "do not auto-derive", not "refuse to derive".** `borg derive` still works on a paused
  branch. That is what makes it useful in an emergency: freeze the automation, then step it manually.

Per-*producer* pausing is skipped. The broken-producer case is already covered by producer-scoped
`IllegalState`, and "expensive but not broken" is a scheduling-policy problem better solved properly
than with a second switch.

**Pausing is self-documenting.** A paused branch's frontier stops advancing, and every read of
derived data already reports `stale` with a watermark showing how far behind. No new vocabulary
needed — a pause *is* lag, and the freshness envelope already describes lag.

**Implemented in D.** The switch is a per-branch flag in a sidecar next to the implementation table,
and the check lives in the CLI's auto-derive path rather than inside `catch_up`. The engine's job is
the mechanism; a mechanism that consulted an operational switch is one `borg derive` would have to
reach around, which is the shape that eventually gets it wrong. A fork does not inherit the flag,
which follows from the flag not being in the log.

#### Background derivation follows the commit that caused it

The CLI is process-per-command, so "background" had to mean something. Considered: a `borg watch`
foreground loop, and a daemon. Chosen: **the process that commits a layer catches the branch up
before it exits** — after `set`, `delete`, `def push`, `repo push` and the parent side of a merge.

A daemon is a real answer and the wrong one to build first: it needs a lifecycle, a socket and a
supervision story, none of which the scenarios could then run without. A `watch` loop is a daemon
with the supervision left to the user, and would leave every existing scenario needing a second
terminal.

What is bought is that **no scenario says `borg derive` unless it is making a point about rounds**,
which is the observable claim §9.6 makes. What is not bought is asynchrony from the *writer's* point
of view: a write now pays for the derivation it causes. That is a v1 latency property and not a
semantic one — §9.6's licence is precisely that scheduling policy cannot affect correctness — and it
is the property a server with a worker pool changes by moving one call behind a signal.

A def push auto-derives too, which is not obvious: it commits no data. It creates work anyway, since
a producer newer than its data owes its whole source buffer (§9.6) and a `MutateField` appoints
migrations that owe every existing value.

#### An inline computation does not advance a watermark

`FreshnessRequirement::Current` runs producers, and the obvious thing — label the output with head,
like a round does — is wrong twice over. A watermark is a claim about *all* of a producer's output,
so one entity computed on demand advancing it would declare a whole branch caught up on the strength
of one read; and because watermarks are rebuilt from derived layers' `reflects` on open, the lie
would survive the process.

So an inline run labels its *layer* with the watermark the producer already had, while labelling the
*cell* with head. The cell's claim is true — its inputs were computed first, recursively, precisely
so that it is — and the producer's claim does not move.

The side effect is the good one: the work stays outstanding, so the next round redoes it and
propagates the consequences a round propagates. Without that, an inline computation would silently
strand every downstream producer, since nothing but a round walks the invalidation index forward.

#### The resolver holds an interface, not the engine

`Current` needs to run producers, and handing `Resolver` the `DerivationEngine` would make the read
path a second entry point into derivation — two callers of `settle`, two opinions about what a round
is, and a dependency edge pointing both ways as soon as anything in the engine wanted to read.

`InlineDerivation` is one method: *bring this cell up to date*. Not catch a branch up, not settle a
round, not register a producer. That is the whole of what a read needs, and stating it as an
interface is what makes §10.5's claim structural rather than aspirational: with this seam the read
path is a **client** of derivation, which is exactly what "lazy materialization is a per-read client
mode, not a system architecture" says it is.

#### A derived dependency is validated against its watermark, not where it landed

`Resolver::validate` compared each dependency's landing layer against the value's own watermark. For
source data that is right. For derived data it compares a derived layer id against a source one: a
derived layer sits *above* the source layer it reflects, by construction (§6.3), so a producer
reading another producer's output was reported stale on a fully caught-up branch — permanently, and
§10.4's `current` was unreachable for any chain.

§10.3 already specified the answer, `W(B) = min(target, W(A), W(other deps))`, and nothing
implemented it. Validation now returns that composed layer rather than a verdict, recursing into
derived dependencies and memoizing per dependency so a diamond stays linear; the memo doubles as the
cycle guard. Merges cannot reach the derived arm at all — `replayable_layers` carries only source
layers — so the "landed, never authored" protection that arm was originally written for stays
exactly where it belongs, on the source arm.

`Landed::reflects()` names the quantity once, in core: *the source layer this record's content
reflects*, which is where it landed for source data and its producer's watermark for derived. Both
arms of the comparison are now written in one place, which is the cheap half of the type distinction
this class of bug keeps asking for. The expensive half — a `Watermark` newtype separating
source-stream positions from layer ids everywhere — is **not** done; see *Deferred features*.

#### `freshness: current` validates before it computes

An inline computation deliberately advances no watermark (§10.5), so it leaves nothing behind that a
later read could recognise, and every `current` read re-ran the producer — and, on a chain, the whole
chain — however settled the branch was. The read now validates first and computes only when
validation does not already reach the layer being read. Validation runs no user code and is the same
walk the read performs anyway.

What is *not* done: `refresh` still re-runs every hop of a chain once any hop is behind, rather than
only the hops that are. That costs work, never correctness, and needs validation to be callable from
the derivation engine — which today would mean either duplicating it or handing the engine the
resolver, and the second is the dependency direction `InlineDerivation` exists to keep one-way.

#### Producer implementations resolve outside the log

The log records that producer P exists; a sidecar table maps its id to a command. Writing a local
path into the log would tie the data model to one machine's filesystem. A container runtime keeps an
image reference in exactly the same place.

### Rounds as transactions

#### A round is a sequence of waves, not a stream of invocations

`settle()` alternates a **sequential discovery pass** with a **concurrent execution pass**: turn the
layers in front of you into a set of invocations, run all of them, then turn the layers *they*
committed into the next set. Discovery stays sequential because it runs no user code — it is a walk
over changesets and index lookups, and running it concurrently would buy contention on the one index
mutex in exchange for nothing measurable.

The barrier between waves is not incidental, it is what makes §16.5's self-correction true. A
producer records its read-set before its own layer commits, so a run that read an input its upstream
had not written yet is already subscribed to that cell by the time any later wave scans the layer
that supplied it. Overlap the waves and the correcting trigger can be missed: the upstream's layer is
scanned, finds nobody subscribed, and the downstream then commits a value computed from nothing.

The bound on a wave is one invocation per core by default, `set_parallelism` in the engine and
`BORG_DERIVE_PARALLELISM` in the CLI. An environment variable rather than a flag or a sidecar: it is
not log data and it is not a fact about the store — the same store derived on a laptop and on a build
box wants different numbers — so there is nothing to record, and every command that derives would
otherwise need the same flag.

Discovery also deduplicates within a wave. Two layers in one wave can dirty the same invocation, and
running it twice at once would put two layers of one round on one cell and two `record` calls racing
to describe one run. Deferring the duplicate is free, because this wave's own output re-triggers it
if it is still dirty.

#### Three invariants were not true as implemented

Turning concurrency on was supposed to test four claims. Three of them needed fixing first, and none
of the three was a design problem — all were places where a sequential engine had made a shortcut
invisible.

- **A branch's head was the last commit, not the highest.** `LayerManager::commit` assigned
  `heads[branch] = id` unconditionally. Ids are assigned at open and order is established at commit
  (§7.3), so a layer opened first and committed second walked the head *backwards* — and the head
  bounds every read path and every producer's work gap. Now a maximum (§7.3).
- **`MemoryStorage` read a cell's history in commit order and assumed id order.** It walked backwards
  to the first write at or below the bound; out-of-order commits made that the *older* of two writes.
  Now a maximum. SQLite never had the bug — it says `ORDER BY written_at DESC`.
- **`catch_up` settled layers whatever state they were in.** A layer only becomes a changeset at
  commit (§9.6), and with a client writing concurrently the loop could reach one still open. On
  SQLite that reads rows which may yet be abandoned. It now stops at the first uncommitted layer of
  the branch and waits — available only because there is no queue to stall — and a layer found
  uncommitted when a store is reopened is aborted, since an open layer is exclusive to a process that
  no longer exists (§6.2).

The fourth claim, single-writer-per-field, **held** — but it is a statement about fields, and what
extends it to invocations is v1 pipelines being per-entity maps. A producer that wrote across
entities would have two invocations racing one cell, and nothing enforces that it does not. Recorded
in §16.3 beside idempotency, where the other unenforced obligation on producer authors already lives.

#### The round ceiling was a prefix used to express a filter — and is now deleted, not fixed

*(Superseded by milestone G. Kept because the shape of the problem is why the fix is a fork.)*

The ceiling was *"the highest layer that is either ≤ L, or is a derived layer with `reflects == L`"* —
a **filter** — held as a `ReadPath` bound, which is a **prefix**. The two coincide while derivation is
the only writer on the branch and diverge the moment a client commits a source layer `L'` mid-round
with an id below one of this round's: the prefix admitted `L'`, so output labelled `fresh_as_of: L`
could have incorporated `L'`.

The strict repair was implemented and was worse. Advancing only over a contiguous run of ids the round
itself produced means a ceiling stalled below `L'` **never rises again**, so every re-run of a
downstream producer reads the same absent input and the round stops converging — a lost update, found
by a test that pushed a source layer while a round was settling.

**What actually fixed it was not a better bound.** A round forks its own branch at `L`, so `L'` is on
another branch and the filter is expressed exactly: everything the round wrote is at *my head*,
everything else is bounded at the fork point, and there is no third category to admit by accident.
The two design changes this entry said would be needed — a `ReadPath` carrying admitted layers, and a
`reflects` column the provider may filter on — are both unnecessary, which is the strongest argument
the transactional model made for itself.

#### A round guards what it read and did not produce, as a set difference

§12.1's rule for a client is about **order** — a read before your own write is still guarded, or every
compare-and-swap silently stops being protected. A round cannot use that rule: its invocations are
independent by construction and run concurrently, so whether `tier`'s read of `is_investible` came
"before" `invest`'s write is a fact about the interleaving, and a guard set that depended on it would
differ from run to run. So the subtraction is round-wide and a plain set difference.

**Taking it as a set difference deletes nothing here**, which is why the two rules can differ without
one of them being wrong. Everything a round writes is derived; a derived cell is never in the
cell-touch index (§12.4); so no guard on one could ever have tripped. And the read-modify-write the
ordering rule protects cannot arise — a producer that reads a cell it writes is a cycle (§16.6), not
a compare-and-swap.

That is also why `Round` holds its invocations as the read-sets and write-sets they already are
rather than as `Transaction`s. Building one `Transaction` per invocation was the first shape and it
was **12% of the fan-out benchmark**, spent on two `BTreeSet`s per invocation to compute a guard set
provably identical to the one the raw vectors give. What is reused is what should be — the guard
*rule*, stated once and next to the client's version of it in `borg-core`, and the whole of the merge
machinery underneath: `check_reads`, the cell-touch index, and `since` being the fork point.

#### A round's guard subtraction was keyed on `CellRef`, and a migration is the one producer that shows it

The composition bug S13 was written to look for, found on the first try, and it is a **lost update**
rather than a near miss.

A round guards *what it read and the round did not write* (§16.5), and the subtraction was over
`CellRef`. That is stated in the code as safe because "everything a round writes is derived, and a
derived cell is never in the cell-touch index" — which is true of every producer **except a
migration**. A migration reads `C@v1` and writes `C@v9`: same cell, two def-versions, and that is the
whole of what a migration is (§9.3). Projecting the version away made `up` look like it had produced
the very record it consumed, so its guard on that record was subtracted and it carried **no guard at
all** on the source cell a client owns.

What that costs, in the order it was found:

* Merging first, the stale round lands and the next round overwrites it. Only a wasted layer — which
  is why `scenarios/080-migration` and every existing test pass with the bug in place.
* Merging last — two rounds in flight, which `settle` being public makes ordinary — the stale round
  lands **on top of** the fresh one. The trunk then holds a migrated view of a value that is no
  longer there, `catch_up` reports nothing outstanding because every watermark advanced, and the read
  says `stale` for ever with no work left that would ever correct it. That is exactly the failure S8
  exists to prevent, arriving through the one producer S8's fixture could not contain.

The fix is to subtract over `CellAt` and emit `CellRef`: `up` guards `C` because it read `C@v1` and
produced only `C@v9`, while `tier` still does not guard `is_investible` because it read and the round
produced the same record. The guard itself stays a question about the cell — the touch index keys on
`CellRef` and a write at any version is a write.

**`Round::cascade` was deliberately left keyed on `CellRef`.** The two want opposite errors: a guard
that is too broad rejects work that was fine and costs a re-run, while a cascade that is too narrow
publishes a value derived from one that never landed — the §10.1 lie. Over-approximating there is
free and getting it wrong is not.

It is the same *shape* as the four `LayerId` conflations *Deferred features* records, with a different
pair of types: two quantities that look interchangeable, are not, and share a Rust type that will not
say so. `CLAUDE.md`'s invariant 6 already names this one — *`CellRef` is the shard key; `CellAt` is
the record key* — and warns that keying on `CellRef` "makes a migration observe its own output as a
change to its own input". This is the mirror of that sentence and cost the same: the migration
observed a change to its own input as its own output. The types were right everywhere the invariant
was thinking about; the loss happened in a `filter`, where projecting a version away reads as a
simplification.

#### Partial application has to be closed under the round's own dependencies

This is the one thing the draft did not anticipate, and it is a correctness bug rather than a
refinement. Draft §4 says a round "applies the invocations whose guards held and drops the rest". Take
that literally with a chained producer and the round publishes a lie: `invest`'s layer is dropped
because a client moved `headcount`, but `tier` — which read `is_investible` off the round's own branch
and carries no guard on it, correctly — lands anyway. The trunk then holds a `tier` derived from an
`is_investible` that never existed, labelled `reflects: L`, and replaying `L` does not reproduce it.
That is exactly the class of failure `100-watermark-truth` exists to catch, reintroduced by the fix
for a different one.

So a dropped invocation takes everything in the same round that consumed its output with it,
transitively (`Round::cascade`). Dropping more is free for the same reason dropping any of it is:
the edges are recorded on the trunk, the layer that failed the guard is a source layer some later
round settles, and that round rediscovers the whole chain. The closure is built only when something
has already failed, so an uncontended round pays nothing for it.

#### A round merges one layer per producer, not one per invocation

One layer per invocation on the *round* branch, because partial application decides per invocation and
a guard is a fact about one invocation. But nothing downstream of the merge needs that granularity — a
layer is an ordered group of events (§6.2) and `LayerAuthor::Derived` describes the whole group — so
the accepted layers are regrouped by producer on the way across.

Without it, fork-and-merge would double the log: a 128k fan-out would commit 128k layers on the round
branch and 128k more on the trunk, and `Registry::open` replays the log on every CLI invocation. With
it the trunk gains one layer per producer per round, which is *fewer* than before the change.

#### The dependency index is keyed on the trunk, never on the round's branch

A round branch is where events land on the way through; the dependency graph is a fact about the data,
which lives on the trunk. Keyed on the round branch, the index would be discarded with the round — and
then an invocation whose merge was rejected would never be rediscovered, because rediscovery is
`dependents(branch, cells)` and the edges would be under a branch id nobody looks up. Partial
application is only safe because those edges are already on the trunk when the merge decides.

#### An inline computation does not fork

`freshness: current` computes one cell because one client asked, and advances no watermark (§10.5). A
round forks because it is `N` computations that must land or not land together with respect to the
world they read. One invocation has no such structure, and forking it would buy a branch and a merge
to isolate a single run from a snapshot it has no claim on. It writes to the branch directly, at head,
as it always did — and a round in flight cannot see it, because its layer is above the round's fork
point like any other.

#### A round isolates data, not definitions

The fork point bounds a round's *data* reads and nothing else. Definitions are folded along the
trunk's full ancestry, which is what §8.0's two def-views already did when the bound was a ceiling:
a layer holds value events xor def events (§6.2), so nothing a round commits can move a definition,
and bounding the def-view at the fork point hides exactly the `MutateField` that appoints a migration
from the round that has to run it. Bounding it was tried; the symptom is that a migration pushed over
existing data never runs at all. `WriteSession::open_on` is where the two branches are passed
separately, and it exists only for this caller.

#### A branch id is not free just because no row claims it

A round forks on every settle, so branch ids are minted by the engine rather than by a caller who
knows what they are doing. `BranchManager` therefore skips an id that already names layers, not only
one that already has a branch row — two branches sharing an id breaks the one thing §6.2 says about
layers and branches, and the cost of being sure is one map lookup.

### Settling a range

#### A chained migration is not discovered by a catch-up — **fixed in I**

Turned up by S14 and left as a limitation for two milestones, because it is the entry below in a
shape where it costs more than re-runs. Both are closed by *settle a range*; what follows is the
diagnosis as it stood. Sequential, no concurrency needed:

```
declare Company.website;  mutate → up1;  write a value;  derive   # website@v2 materializes
mutate → up2;  derive                                            # website@v3 never appears
derive --rebuild                                                 # website@v3 = up2(up1(value))
```

Two things have to line up. A producer's work is the source layers between its watermark and head
(§16.4), and `up2`'s input version is only ever written by a **derived** layer, which opens no round
— so nothing triggers it. Its other route is §9.6's seeding, where a producer that has never run
takes its whole source buffer; but `catch_up` starts from the **minimum** watermark across all
producers, which a brand-new producer drags to the bottom of the log, so the seeding round forks at
the very first layer, finds the buffer empty there, and advances the watermark past zero. The one
chance is spent on the wrong fork point.

The same shape catches a **pipeline pushed over data whose inputs are already derived**, which is the
more likely way to meet it.

`borg derive --rebuild` was the escape hatch, and it worked: it rewinds every watermark and settles
the highest source layer, so the whole chain runs inside one round where each hop sees the previous
one on the round's own branch. It is `O(everything derivable)` and it needs an operator to know to
run it, which is the objection.

**Fixed by settling a range.** The derived layer carrying `up1`'s output is now in the opening wave
of the round that follows it, so the trigger exists; and the round forks at the *top* of the range,
so §9.6's seeding scans a world that contains what it is looking for. Both halves were needed —
either alone leaves the case unreachable. `crates/borg-engine/tests/composition.rs` flipped from
pinning the gap to proving the fix, `crates/borg-engine/tests/rounds.rs` has the pipeline shape
without a migration in it, and `scenarios/180` now reaches the second hop with a plain `borg derive`.

#### A backlog of source layers still costs re-runs — **fixed in I**

Rounds settled one source layer each, so when several were committed before any was settled, the
round settling the earlier layer merged *above* the later layer's id — and the round settling the
later one, forked at it, could not see that output. It predated the round-as-transaction change (a
ceiling stalled at `L'` had the same blind spot in its first wave, and only saw past it by way of the
prefix hole) and was unchanged by it.

It cost re-runs rather than correctness in the shapes v1 produces, because each round recomputes what
its own source layer dirtied, chains included. The exposure was an invocation dirtied by `L'` that
depends on a derived cell only an earlier round produced.

**Fixed by settling a range**, and the re-run half turned out to be the more interesting of the two.
A round per source layer does not merely *risk* staleness under backlog — it manufactures it: the
round settling `L10` is rejected by its own guard on an input `L11` has already moved, and the guard
is right. The schedule had guaranteed the work was stale before it ran. See *Settling a range is a
schedule change, and it retires §6.3's no-coalescing rule* below.

#### Settling a range is a schedule change, and it retires §6.3's no-coalescing rule

`catch_up` used to open one round per source layer. It now opens **one round for
`[watermark+1 … head]`**. The two entries above are both this change, and neither of them is a
performance entry — which is why it is a decision and not a tuning note.

**The backlog case is a schedule manufacturing staleness.** With `L10`, `L11` and `L12` committed
before anything settles, the `L10` round reads the world at `L10`, and by the time it merges its
guard on an input `L11` moved has failed. The guard is correct. The *schedule* chose to run work it
had already guaranteed would be rejected, and under sustained backlog most derivation work goes that
way. A range has nothing to be stale about: the fork point is the top of it.

**Three things had to move together**, and any one alone leaves a hole:

1. **The invalidation walk covers derived layers.** This is the semantic change, not the
   optimisation. A cell written by a previous round's merged output now counts as a trigger for
   producers that read it — which is the only route to a chained migration or a pipeline pushed over
   already-derived data, because derived layers open no rounds. It needs its own guard against
   runaway: a layer's **position** in the source stream (its id if source, its `reflects` if derived)
   is compared with each producer's watermark, per layer rather than once per round. Without that a
   settled branch re-derives itself for ever off the layers its own last round merged, and every
   *"the branch settles rather than chasing itself"* assertion in the suite would go red.
2. **The fork is at the top *layer*; `reflects` is the top *source* layer.** These came apart and had
   to. A watermark points into the source stream (§6.3) so what the output claims must be a source
   position; but the world at that position includes the derived consequences of the layers below it,
   and those sit *above* it in the log. Forking at the top source layer would hide exactly them —
   which is the residue §16.5 recorded. `reflects` is still true by construction, because everything
   between it and the fork point reflects it or lower and is therefore part of the world it names.
3. **The buffer scan runs at the top of the range.** It follows from (2) — `backfill` reads through
   the round branch's ancestry — and it is the half that fixes the *seeding* route rather than the
   trigger route.

**What is given up is derived-history granularity, and §6.3 said so in advance.** v1's rule was one
derived layer per `(producer, source layer)`, with the reasoning recorded: *"coalescing across source
layers is the natural v2 optimisation and is a scheduler policy, not a redesign."* That is exactly
what this is, except that it is not an optimisation. The new rule is **one derived layer per producer
per round**, `reflects` = the top of the range. Two instances replaying the same source log still
agree on every settled value and on every label; they no longer agree on how many intermediate
snapshots exist. Nothing can ask: derived data is addressed by `reflects` and never by derived
LayerId, so a time-travel read at `L11` takes the greatest `reflects ≤ L11` and gets the world at
`L11` in both. `200-determinism`'s digest already strips layer ids, which was designed for this.

**What is unchanged is the guard model.** A genuinely concurrent writer still trips a round's guards,
partial application still drops the invocations that lost, and the cascade still takes what consumed
their output. The treadmill went away because the schedule stopped manufacturing staleness, not
because anything got weaker — `rounds.rs` asserts the backlog-plus-concurrent-writer case for exactly
that reason.

**Two things it costs that are worth saying plainly.**

`scenarios/180` changed its claims rather than its numbers. It asserted that a derived layer names
the source layer that dirtied it; a range names the *top* of the range, which in that scenario is the
def layer the merge landed. Both are positions in the source stream — a def layer is authored by a
client and is a source layer — and the newer label is the tighter of the two, because the round did
compute under that def-view. The scenario also stopped needing `--rebuild` for the second hop of its
chain, which is the fix arriving where it was documented as missing.

And **a producer that is exactly caught up to the top of a range does not participate in it.** If
`P1` stands at `reflects` and a brand-new `P2` runs in the same round and writes something `P1`
reads, `P1` is not re-run. It is the pre-existing per-round gate asked per layer, and in practice a
new producer arrives with a def push, which is itself a source layer above `P1`'s watermark and puts
`P1` back in the round. Worth knowing rather than worth a mechanism.

`recompute` was deliberately **not** converted to a range. A rebuild is the one operation that must
not see what earlier rounds derived — §10.1's check is *fork at W, recompute, compare*, and a fork
point above the derived output would let a producer read its own previous answer and confirm itself.
It also has no head of its own to take a range from, since `100-watermark-truth` rebuilds a fresh
fork. It keeps forking at the highest source layer, which is a one-layer range by another name.

### Failure and operational state

#### A poisoning is operational state, and the log is what retires it

§14 has been true of the engine and false of every CLI user since the CLI existed. `broken` producers
lived in a `HashMap` on `DerivationEngine`, and the CLI is process-per-command — so the judgement
died with the command that made it. The next `borg get` called a poisoned producer's output `stale`,
which promises a catch-up that was never coming, and the next `borg derive` re-ran the failing code
from scratch, burning the work and repeating whatever partial effects it had. The resolver never
consulted the map at all, even in-process: `Freshness::Broken` was reachable only through the
unreachable-version path.

Where it belongs was the whole question, and it splits in two.

- **The record is operational state, beside the store.** Not a value event and not a def event, and a
  layer holds one or the other (§6.2). It is *discovered* — the same objection that moved field
  ownership out of the log, in the opposite direction: ownership became declared, and this cannot,
  because nobody writes it. In the log it would be forkable, mergeable and time-travellable, so a
  fork would inherit a poisoning its own code never earned. It joins the pause flags, the transaction
  table and the implementation table in the sidecar, for the reasons all three are there.
- **The clearing edge is already in the log.** §14's recovery is *push a new ClientVersion*, and a
  producer's ClientVersion is the def-layer it was pushed at (§9.2) — so a record that names the
  version it was recorded against **expires by itself**. Nothing has to remember to clear it, no
  command has to run in the right order, and a record restored from a backup cannot poison code that
  has since been replaced. The record is not a fact in its own right; it is a claim about a fact the
  log holds, which is what makes durable state outside the log safe here rather than merely
  convenient.

Storage was considered and rejected in one line: nothing above the provider line teaches a
`StorageProvider` what derivation is (§17.1), and a poisoned producer is derivation through and
through. So it reaches durability through a `PoisonProvider` of its own — in-memory for anything that
*is* the process, a file for a client that is not.

Three consequences that were not obvious going in:

- **Recovery has to rewind the frontier.** A round advances every producer's watermark whether or not
  it ran, so a producer skipped while broken stands at head claiming to have incorporated everything
  it never saw. Without the rewind, "invalidates and recomputes its output" would be a promise the
  next write happens to keep. The rewind is `recompute`'s, for the same reason.
- **The watermark advancing while broken is right.** Holding it back would stall the settled frontier
  and every `frontier reaches` on the branch behind one bad pipeline — branch-wide poisoning wearing
  a different hat. The read envelope is the honest channel, and it now says `broken`.
- **`--retry-broken` is not redundant with recovery.** A fix the log cannot see — a worker's
  environment repaired, a service it calls back up — has no version bump to expire the record.
  Retrying by *default* is what turns one bad deploy into the same failure repeated by every command.

Not covered: a producer that has never succeeded writes no cells, so there is nothing for a read to
label. `broken` is a label on a stored record (§10.4), and enumerating the cells a producer *might*
have written is not a set anything can produce.

### The server

#### A server is a binary of its own

`borg serve` is gone. Serving is `borg-server`, a separate crate and a separate binary; `borg` keeps
its embedded mode — direct operation on a store nobody is serving — because that is what a scenario,
a fixture, a build step and `borg init` all want and will want forever.

Three reasons, and the third is the one that decided it:

- **Opposite lifecycles.** Everything the CLI is — open, do one thing, exit — is what a server must
  not do. Every piece of state that made `borg serve` awkward to write (a held registry, a worker
  pool, an advisory lock, a signal handler) is state a process-per-command client has no reason to
  have, and a binary that is both teaches neither shape clearly.
- **They will be deployed apart.** The server runs in a container, under systemd, and on
  platform.borg-hq.com; the CLI is what a developer installs. Shipping the second everywhere the
  first goes is a bigger artifact for no gain.
- **Merging later is trivial and splitting later is surgery.** psql/postgres and
  redis-cli/redis-server are the precedent. If this turns out to be wrong, one `[[bin]]` fixes it.

What is *not* split is the code. `crates/borg-host` holds everything either binary does to a store —
opening one, holding one, the operations, the sidecars, the advisory lock, `repo push` — and both
front ends render its results their own way. The rule `ops` already enforced between `main.rs` and
the old `serve.rs` is now enforced across a crate boundary: **an operation returns what happened;
the caller renders it.** The extraction was a move rather than a fork, so `borg`'s embedded mode and
`borg-server` cannot drift into two answers about what a transaction is.

#### A server hosts a directory of registries, and the registry is the unit of tenancy

`borg-server start --data-dir ~/.borg` hosts every store under it, addressable by the name of its
directory. Not one store per server: one *process* for a machine's registries.

**Why the registry and not the branch.** A branch is a fork of one history — it shares definitions,
the transaction table and the PID counter with what it forked from — so two applications that must
not see each other's schema need two registries. Everything a registry owns is already
registry-shaped: the log, the sidecars, the advisory lock. Making it the tenant therefore costs
nothing new and makes the multi-tenant case the same code as the single one, which is the whole
reason this was worth doing before there is a platform to do it for.

**Lazily opened, eagerly locked.** These are opposite and deliberately not done together. Opening a
registry brings its projections to head, which for a fresh set is a replay of its log, so a server
that opened everything at boot would pay every registry's history to answer a request about one of
them — and a data directory is exactly the shape that accumulates registries nobody has touched this
week. Taking the advisory lock is a file write, and not taking it leaves a window in which `borg
set` may walk into a store the server is about to hold. Cheap to take, expensive to be without.

**The gate became per registry.** `borg serve` held one store-wide, which was right when there was
one store. What the gate protects is the read-modify-write on files beside *a store* and that
store's sequencer, so two clients on two registries share none of it and serialising them would be a
limit nothing asks for. Within one registry nothing changed: requests are still answered one at a
time, which is the serialisation process-per-command gave the CLI for free.

**Creating a registry is a server operation.** A directory appearing under a running server's data
dir is a store it has not locked, is not hosting and will not route to — so `registry_create` is a
protocol message and `borg-server create` uses it whenever a server is up. It also creates one
directly when nothing is running, because a data directory has to be fillable before there is a
server to fill it. Both, and the pair is the point.

#### The handshake names a registry, and carries a credential nothing checks

`ClientHello` gains `registry` and `credential`, both `Option` with serde defaults.

**Routing belongs to the connection, not to the message.** The registry is what a connection is
*to*, so it is settled once; repeating it per message would put a tenancy decision on every line a
shell client writes. That is the opposite of the rule for transactions, and for the opposite reason:
a transaction outlives its connection and therefore cannot be implied by one. The single exception
is `repo_push`, which may name another registry, so that a deploy client pushing to three does not
need three connections.

**Absent is the sole registry at n=1 and an error at n≥2.** The convenience is what keeps a local
developer's experience exactly as it was — start a server, connect, name nothing. It must not
survive a second registry, because "the obvious one" stops being obvious and any answer would be a
coin toss over somebody's data. The error names the options rather than merely refusing, and
`registries` — the one message that needs no registry — is what lets a client that guessed wrong
find out what to say.

*Where the routing error is delivered* took a decision. The obvious place is the handshake, and it
is the wrong one: the server does not acknowledge an accepted hello, because it has nothing to say,
so a client is not reading at that point and one that were could not tell a refusal from an answer
(`CLAUDE.md` records that gap). So the failure is remembered and handed to the first request that
needs a registry, which is every request except `registries`. Same sentence, delivered on a channel
the client is definitely reading.

**`credential` is reserved and its existence is the point.** A local server has no one to
authenticate — the socket's file permissions are the boundary — and nothing reads the field. Adding
it once authentication exists would mean moving the wire at exactly the moment there is a deployment
that cannot take a wire change. It costs one `Option<String>` now.

#### `start | stop | status | logs`, backgrounding by default

**`start` backgrounds unless told otherwise**, because that is what a person at a terminal wants:
run it, get the prompt back, have the thing be up. That means a pidfile and a log file beside the
registries, since a backgrounded process with neither cannot be stopped or debugged.
`--foreground` is what systemd, docker and a scenario want: stay in the foreground, log to stdout,
daemonize nothing — a supervisor's whole job is to be the parent process.

**Backgrounding is a re-exec, not a fork.** `fork` without `exec` is a minefield in a process with a
tokio runtime, and re-exec means the backgrounded server is an ordinary `borg-server start
--foreground` — the same code path a supervisor runs, so there is no second lifecycle that exists
only in the background case. The child goes into a process group of its own (`process_group`, which
is safe) rather than a session of its own (`setsid`, which would need `libc` and an `unsafe` block
this workspace forbids).

**`start` waits until the server answers**, because a socket file exists a moment before anything is
listening on it and a `start` that returned with only a pid would make every caller write that loop
— which every scenario and `dev.sh` previously did.

**`stop` is `SIGTERM`, not a protocol message.** A `shutdown` request on §17.5 would be a wire on
which anyone who can connect can stop the server, which is exactly the shape that must not exist on
the day `credential` starts meaning something. A signal is the operating system's own authorisation
model. It is sent by running `kill`, because sending one from Rust means `libc` and an `unsafe`
block; that is a dependency-and-invariant trade taken deliberately on a path a person runs by hand.

`stop` waits for the **process**, not for the socket, and that distinction is a race somebody has to
lose: a server stops accepting the moment its listener drops and *then* stops its workers and
releases its locks, so a `stop` that returned when the socket went quiet would hand control back
with the advisory locks still held. It cost a scenario failure under load to find.

**Every failure says how to start one.** `status`, `stop` and `logs` against nothing are the
commonest confusion there is, and "no server is running" alone sends somebody to read `--help`.

#### One well-known socket for the whole server

`$XDG_RUNTIME_DIR/borg.sock` when that directory exists, `<data-dir>/borg.sock` otherwise — which
with the default data dir is `~/.borg/borg.sock`. The runtime dir is where a per-user socket belongs:
user-private, on tmpfs, cleaned at logout, so a crashed server leaves nothing behind across a reboot.
The fallback is beside the data it serves, which is the only other place a client can find without
being told.

**One socket, not one per registry.** The handshake routes, so a socket per registry would be a
second routing mechanism doing the same job worse — and the moment the transport is a TCP port or a
WebSocket rather than a file, a path per tenant is not a thing that exists. Two servers on two data
dirs with one `$XDG_RUNTIME_DIR` therefore want the same address and the second is refused because
something is already listening, which is the right failure for a well-known address; `--socket`
is how you say otherwise.

#### The server executes the push

`repo_push {registry, branch, path}` — the *server* runs the describe/push against a path on **its**
disk. This retires *"pushing a schema to a served store means stopping the server"*, which was the
flaw `examples/personal-crm/dev.sh` was built around: it pushes before it serves, and changing the
schema means restarting the whole script.

**Why the server and not the client.** A push moves two things. Definitions travel the log; they
must land through the registry the server is holding open, or a second `Registry` over one store
breaks the single-process assumption. Implementations — which file each producer is — are a sidecar
(§9.2), and a client cannot write it because it is not the machine the code runs on. Both point the
same way: the process that already owns the store is the only one that can do this.

**The precondition was not the protocol.** It was the implementation fingerprint. Before it, `repo
push` recomputed every producer's source buffer whether or not anything had moved, so a push against
a live server was not merely unbuilt but unsafe to want. Idempotent and code-change-aware, a push
now costs exactly what the change costs, which is what makes it a thing a dev loop may do against a
running server.

**How the held registry sees it**, which is the question this had to answer:

- **Definitions** go through `ops::open`, which answers the held registry when there is one. The
  projections are maintained on the way in like any other commit, so the instance that answered the
  last read is the instance that has the new defs. Nothing is re-opened and no log is replayed.
- **The worker pool** is the half the log cannot carry. It was built at boot from the producer table
  the push has just rewritten, so `Held::reload_producers` re-registers every producer and **discards
  every idle worker** — the pool is keyed on the command's *path*, and the common case for an edited
  pipeline is the same path with different bytes, so a surviving pool would hand the next invocation
  a process still running the old program. That is precisely the mislabelled output the fingerprint
  work exists to prevent, arriving through the back door.
- **Order matters and is asserted.** The reload happens *before* the catch-up, because the catch-up
  is what runs the new definitions. `crates/borg-server/src/serve.rs`'s live-push test fails without
  it, which was checked by removing it.
- **The gate** is what makes "discard every idle worker" safe: the caller holds the registry's gate
  for the whole push, so no invocation is in flight.
- **Poisonings** need nothing. §14's recovery is a producer's ClientVersion moving, which the def
  layer this push landed already did, and the poison table this process holds is keyed on that
  version — so a fixed producer's record retires itself, without the restart it used to need.

**Local-only, and the payload says so.** `path` is a path on the server; for a remote server that
means nothing, and the answer there is an uploaded artifact. So `path` is optional and the message is
expected to grow a sibling field rather than be replaced — a client that sends `path` today keeps
working against a server that also accepts bytes. What this does **not** do is pre-empt
`ExecutionProvider`'s container future (§17.3): a container reference is a different way for the
*server* to find code, which is the same sentence with a different noun.

#### A server would not stop if its socket had been deleted

Found while building the above, and fixed with it. `serve::run` wakes its blocking accept loop by
connecting to its own socket and then **joins** that thread — but if the socket file has been removed
out from under a running server (a scenario's `rm -rf` on its scratch directory, a `tmpfiles` sweep,
somebody tidying), nothing can reach that `accept` any more and the join waits forever: `SIGTERM`
arrives, the handler runs, and the process hangs holding its advisory locks with nothing left that
could release them. `borg-server stop` then fails its own patience deadline.

It surfaced as wedged `borg-server` processes accumulating from scenario runs that had failed
part-way and taken their socket with them — which is exactly the shape of thing that reads as
"something is slow today" until you count the processes. The fix is to join only when the wake
connection succeeded; skipping it loses orderliness and nothing else, because the thread dies with
the process moments later and `stop` is already set. `scenarios/310` also stops its server in a trap,
since it is the one scenario that restarts servers and can therefore leave one behind.

#### One string configures a client

`borg://localhost/personal-crm`. The two halves a client needs — where the server is, which registry
on it — are one fact, and carrying them separately is what lets them be changed independently into a
client pointed at one deployment's socket with another deployment's registry name. `DATABASE_URL`
is the precedent and it is the shape every deployment system already carries.

**The scheme names the transport, so `borg://` can be redefined and `borg+ws://` cannot be
invented.** `borg://` means *the local transport*, which today resolves to §17.6's well-known
address; a client that wrote it keeps working if that address moves. `borg+unix://` is for saying
the address out loud. `borg+ws://` is parsed and **refused by name** — the browser transport is the
one that arrives next (SDK-DRAFT §5, `serve::Transport` exists for it), and the cost of not naming
it now is that three people invent three spellings and the first real one cannot use any of them.

**An absent registry stays absent**, and this is the rule the whole thing turns on. §17.6 already
decides what no registry means — the sole one at n=1, an error naming the options at n≥2 — so a
client that defaulted to `main` would be re-implementing half of that and disagreeing with the
other half. The parser answers `None` and the handshake carries `None`.

**Where the socket ends and the registry begins** was the one real ambiguity. The divider is the
rule the server already enforces on registry names — letters, digits, `-` and `_` — so the last path
segment is the registry when it could *be* one and part of the path when it could not, which makes
`borg+unix:///tmp/borg.sock` read as the socket it obviously is. A trailing slash always means "no
registry", so both readings of `/run/borg/crm` are sayable. The alternative considered was a query
parameter (`?registry=crm`), rejected because it is a second syntax doing what the path was already
doing, and because the two-form grammar then has three shapes instead of two.

**One parser per language, and one deliberate duplication.** `borg_protocol::url` and
`packages/borg-sdk/src/connection.ts` hold the same table, case for case, because the whole value of
one string is that the *same* string can be pasted into a variable a Rust CLI reads and a variable a
node process reads. What is genuinely duplicated rather than shared is the well-known address —
`$XDG_RUNTIME_DIR` or the data dir — which is `borg_host::host`'s in Rust and cannot be called from
node; `scenarios/310` is what holds the two answers together. Python's SDK is the pipeline half and
has no client, so it has no parser and needs none yet.

**Two CLI commands read a url, and the rest are refused.** `borg generate` and `borg repo push` are
the two that speak to a server rather than opening a store, and `--url`/`$BORG_URL` is theirs.
Everything else is embedded Borg on a `--store`, and an explicit `--url` on one of those is refused
rather than ignored — it says the caller meant a server, and what they would silently have got is an
answer about a file. `$BORG_URL` is ambient and is left alone, for the same reason an exported
variable does not break every unrelated command. `borg generate --socket` is gone: it named half of
what a connection is, and the store's own lock record — which names both halves — is still the
default when no url is given.

#### The SDK reconnects, and never retries

`examples/personal-crm/FRICTION.md` #11: a `borg serve` restart made every later request through an
existing `BorgContext` throw *forever*, and the only recovery was for the application to build a new
context — with no event, no flag, and no way to tell "the server is down" from "this context is
finished". The reconnect story §12.2 was designed for was designed for and not implemented.

**A context is an address, not a socket.** At most one live connection to it; a failed send or read
tears it down; the next operation dials again and repeats the handshake. The handshake is repeated
rather than the socket merely re-opened because the handshake is what settles the registry (§17.6) —
a reconnect that skipped it would be a connection to a server with no idea which store it is for.

**Nothing is retried, and that is the load-bearing half.** `tx_commit` is not idempotent: a commit
that reached the server and lost its answer on the way back is indistinguishable, from the client,
from one that never arrived, and re-sending it either merges a second layer or fails against a
transaction that no longer exists. `tx_create` allocates. So an operation that was in flight fails
with `BorgDisconnectedError`, whose message says the outcome is unknown and that it was not retried;
what to do about that needs the application's knowledge of what it was doing. A reader can still
find out, because a transaction is durable — `bc.transaction(id)` and a read answer whether the
write landed.

**A socket the peer has already closed is dropped before it is used**, which is what makes an
ordinary bounce cost nothing rather than one guaranteed failure per client: the close arrived while
the client was idle, so nothing was in flight and nothing is being retried. It is best-effort by
construction — it depends on the runtime having had a turn to deliver the close — and a client that
was busy right through the outage still discovers the outage by failing. `scenarios/310` asserts
that case deliberately, by bouncing the server from inside a blocking call.

**Transactions survive by construction and needed no code.** A transaction is an id beside the store
(§12.2), so one begun before a bounce commits after it, and `bc.transaction(id)` picks one up in a
process that restarted rather than merely reconnected. What that cost was *not* keeping the
transaction in the connection, which is the tempting shape.

**`connect: "on-demand"`** is the one new knob, and it exists because eager dialling is right for a
script and wrong for a process that legitimately starts before its server — a supervisor bringing
both up, a container, an api whose job is to answer `503` until the backend is there. The default is
unchanged: an error at construction names the address you just configured, and one at first use
names whichever line happened to be first.

#### Nothing listening says how to start one

`no borg server at <addr> — start one with: borg-server start`, from `borg_protocol::url::
unreachable` and from the SDK's `BorgUnreachableError`, word for word. It is a `BorgError` variant
of its own — printed with no prefix — because the whole value of it is the sentence, and a
`storage:` in front of it is noise in front of the only words a reader needs. It also lets a caller
tell *the server is down* from *the server said no*, which is the distinction the SDK's separate
error classes make on the other side.

Anything that is not `ECONNREFUSED`/`ENOENT` is reported as itself. A permission error on a socket
is news, and telling somebody to start a server they already started would be worse than the errno.

#### The example sheds server ownership

`examples/personal-crm/dev.sh` used to *be* the server's supervisor, and had to be: a schema could
only be pushed while nothing was serving, so the script's order was load-bearing and changing the
schema meant re-running the whole thing. Both halves are gone. It **ensures** a server — `status ||
start` — pushes the repo into the running one by url, and leaves it running when you `^C` the
script; only the api and the ui are the script's to kill. `./dev.sh --stop` is the verb that
appears once a server outlives the script that started it.

**`--reset` stops the server, deletes the registry's directory and starts it again**, rather than
going through the socket. A `registry_delete` message would be a destructive operation on a wire
whose `credential` nothing checks yet — which is the same argument that made `stop` a `SIGTERM`
rather than a protocol message. Throwing a store away is a thing you do with filesystem access, on
purpose. Only the registry's directory goes; the server's pidfile and log live beside it and are
where the reason a previous run died is written.

The api moved to `BORG_URL` and `connect: "on-demand"`, which is the FRICTION #11 fix arriving in
the application that reported it: it starts before its server, answers `503` with the sentence
naming the fix, and works the moment a server appears — with no restart and no connection lifecycle
of its own. `smoke.sh` is that observation made repeatable; it is a tool and not a scenario, for the
reason the example's README already gives.

### Export and import

#### Which sidecars are state, and which are residue

Export was easy to specify and hard to *bound*: the log is the data, so walking it is obvious, but
the files beside a store are not log data and each one had to be argued about separately. The rule
that came out of doing it: **a sidecar is exported when losing it would change an answer the restored
registry gives, and skipped when it only describes a process that is over.**

- **The PID counter — exported.** Never in doubt. It is the one sidecar a store cannot recover from
  (`CLAUDE.md`), and this stream is the backup story it never had.
- **The producer implementation table — exported.** A restore without it is a registry holding
  producer definitions it cannot run. The commands are paths on the exporting machine and go back
  verbatim; a restore onto a different machine repairs them with the `repo push` that put them there
  in the first place, and pretending otherwise — rewriting paths, or refusing to carry them — would
  be inventing a deployment opinion inside a backup format.
- **Pause flags — exported.** Tiny, and the failure is silent: a branch somebody paused on purpose
  comes back deriving.
- **Poisonings — exported, and this is the one that could have gone the other way.** A poisoning is
  the engine's judgement about code discovered at runtime (§14) and reads as operational residue. It
  is not, and the test that settles it is an *envelope* comparison rather than a value comparison: a
  poisoned producer's cells read `broken`, and without the table they read `stale` — which is a
  promise of a catch-up that is not coming, the exact lie §14 exists to prevent. `explain` loses the
  reason too. Every test that only compared *values* would have passed either way, which is why the
  round trip compares whole envelopes. The record is keyed on the ClientVersion it was recorded
  against, so carrying it changes nothing about recovery: push fixed code and it retires itself.
- **Open transactions — skipped.** Ephemeral by decree (§12.3), and restore is create-then-import, so
  there is no client holding a handle to a registry that did not exist a moment ago. The *timeout*
  inside the same file is exported, because that is a knob somebody set rather than a transaction
  somebody opened — the two live in one file and are not the same kind of thing.
- **The advisory lock — skipped.** Not state at all: a live claim naming the socket of a process that
  is not this one.

What this leaves visible rather than hidden: a restored registry's transaction branches are still
there, as branch rows with layers and nothing pointing at them, exactly as an abandoned transaction
leaves behind on the store it was exported from (`CLAUDE.md`, *transaction branches are never
reaped*). The stream reproduces the registry, including the parts of it nobody has collected yet.

#### An export is the whole log, and settling it would be data loss

The obvious-looking move is to export at the settled frontier: §10.5 already answers *where can a
coherent snapshot be read*, and a backup wants a coherent snapshot. It is wrong, and the reason is
worth writing down because it will look attractive again. The settled ceiling is a **read** bound over
a branch whose derived data lags its source data; bounding an *export* there drops every source layer
above the watermark. That is not coherence, it is losing writes — and it would lose exactly the most
recent ones, which is the worst possible set to lose from a backup.

So an export is the whole log at head, and the lag comes with it: watermarks, the backlog, and every
label on every derived cell. The restore works the same backlog off and arrives at the same place.
The settled position is *reported* by an export — `the log ends at L47; the default branch is settled
to L44` — so that a backlog captured along with the data is visible rather than a surprise.

There is no torn-read problem to solve either, and that too is worth stating rather than assuming:
embedded `borg` is refused against a served store and is one process besides, and a **served** export
runs under that registry's own gate (§17.6), which serialises it against every other request. Nothing
commits while the walk happens. The honest cost is that a large export holds its registry for its
duration, which is the same gate whose relaxation is already an open question above rather than a new
one this introduces.

#### The header is deliberately almost empty

Two exports of one registry are byte-identical, which makes `export → import → export → cmp` a total
check that covers everything nobody thought to assert. That property is cheap and it is easy to
destroy: a timestamp destroys it, a registry name destroys it, an absolute path destroys it. So the
header carries the stream-format version and the producing binary's version and nothing else. Where a
copy came from and when are facts about the copy rather than about the data, and a filename and an
`ls -l` already carry both.

The same reasoning made the header say `borg <version>` rather than naming the binary that wrote it:
`borg export` and `borg-server export` are two front ends over one module, and a header that told
them apart would make one registry export to two different byte strings depending on who asked.

#### Import adopts event ids, which is a new hole in the provider surface — a deliberate one

`OpenLayer::author_event` takes a *draft* precisely so that a writer cannot name an id or a layer:
`authored` is the layer it is called on, and that is what makes it impossible to author an event
claiming to have been written somewhere it was not. An import is the one writer for which that is
backwards. An event id is *referenced* — by every membership row and by every read-set in the stream
— and `authored` is what distinguishes a merged event from a copied one (§13), so re-minting either
would leave a restore whose lineage is a plausible fiction.

`adopt_event(Event)` is therefore on the trait, and the property is kept rather than abandoned: a
provider **must refuse an event whose `authored` is not the layer it is being replayed into**, so an
event can only be replayed into the place it says it came from. Both backends also advance their id
sequencers past whatever they adopt, or the first ordinary write after a restore would reissue an id
the import had just taken — which the round-trip test checks by writing to both stores and requiring
the same layer id.

#### The server-side pair takes a path, and that is the contract

`export` and `import` are on §17.5, and both name a file **on the server's machine** — the same shape
`repo_push` already has. Here it is forced rather than convenient: the protocol is one request, one
response, in order, and a registry may be enormous. A response carrying the stream would be exactly
the buffering the format exists to avoid, and a multi-message reply would be a change of shape rather
than a field. The remote form is an uploaded artifact and is a field on the message when it arrives.

`import` is answered by the *host* rather than by a registry, like `registry_create`, because it
creates the registry it names. It creates and fills in **one** operation: the two halves apart leave a
window in which the server hosts an empty registry that clients can route to and write into, and
whatever they wrote would then either be refused by the import or silently kept beside it.

#### `borg` addresses a store; `borg-server` addresses a registry

The brief said `borg export [--registry ...]`. It is `borg export [<file>]` on `--store` instead,
because `--registry` is a name under a *data directory* and a data directory is `borg-server`'s
vocabulary — `borg` has addressed a store by path since it existed, and giving it a second addressing
scheme for two commands would be the beginning of a third. So the pair is split the way `create` and
`status` already are: `borg export` / `borg import` for embedded Borg, `borg-server export` /
`borg-server import` for a data directory, whether or not something is serving it.

The consequence is that `borg export --url …` does not exist, unlike `generate` and `repo push`. If a
reason to want it appears, the missing piece is not the flag but somewhere for the bytes to go — see
the paragraph above.

### Performance, tooling and tests

#### The rebuilt indexes are `Projection`s, and a server holds one registry

The abstraction came first and the fix fell out of it, deliberately in that order: the owner's rule
is that performance work lives behind a seam that cannot taint correctness, and the seam here is the
one the spec had already described in prose. §17.1 said the dependency index, the cell-touch index
and the watermarks are "a cache rebuildable by replaying committed layers". That sentence is now a
type. A `Projection` says what it answers, how to fold one committed layer in, and the **position** it
has folded through; `Registry::open` is *bring every projection to head*, which for a fresh set is
today's replay and for a maintained one is nothing at all.

**What was deliberately left out of the trait.** No snapshot hook, no serialisation, no way to fold
backwards, no notion of a summary that might be wrong. Three methods and a position, which is what
the two lifecycles that actually exist genuinely share. `wants(layer)` is in because both of them
already had it — the touch index has never read derived layers (§12.4) and the frontier reads no
events at all — and a fold that needs no membership must not make a replay read one. Everything else
a future implementation might want is an *implementation* of this seam rather than a hook on it:

- A **materialised** projection — one that persists alongside the log and comes back at a position
  near head — needs nothing new. It answers `position()` with what it loaded and folds the tail.
- A **probabilistic** one — a bloom filter over the touch index, graded lineage from per-record to
  per-buffer (`index.rs`) — is legitimate provided its error is one-sided in the direction
  `CellTouchIndex::moved_since` already established: *"a `true` says only check properly. It never
  stands in for a guard failure."* That is the contract an approximation has to meet, and it is a
  precedent rather than a new rule.
- Sharding either index by cell key (§17.2) is orthogonal and already permitted.

**The correctness harness is the point, not the trait shape.** `crates/borg-engine/tests/projections.rs`
folds a real store from zero and compares it against the live-maintained set, question for question,
after derivation with re-runs, a round that merged, a transaction that merged and a fork that did
not. Without it, a divergence is invisible: everything rebuilt from zero answers correctly, so a
served store could quietly answer something else while every existing test passed. It is what makes
future hacks behind this seam safe to try, and it found something on the first run — see below.

**Then the fix.** The server holds one deriving `Registry` per registry for its lifetime and every
request against that registry shares it. The blocker was never the socket: `ops::tx_commit` dropped its registry so `auto_derive` could
open another one *with* an executor, and two live registries over one store are what the
single-process assumption forbids. The long-lived registry carries the executor, so both use the same
instance and the dance is gone. What makes it *safe* is the advisory lock — serve is the only writer,
the CLI is refused by name, and `repo push` is now performed by the server itself (*The server
executes the push*) — so every mutation flows through the instance maintaining the projections. The
lock was built to be honest about a single-process assumption; it turns out to be the precondition
for the cache.

The measurement, `examples/personal-crm/FRICTION.md` #9, reproduced with `examples/personal-crm/bench.sh`
against both binaries on one machine:

| contacts | head | `GET /contacts` before | after | ms/read before | ms/read after |
|---:|---:|---:|---:|---:|---:|
| 45 | L451 | 1 708 ms | 27 ms | 18.8 | 0.3 |
| 100 | L1001 | 8 157 ms | 70 ms | 40.6 | 0.3 |
| 140 | L1401 | 15 556 ms | 160 ms | 55.4 | 0.6 |
| 400 | L4001 | — | 328 ms | — | 0.4 |

The per-read cost stopped tracking the branch head. `POST /contacts` went from 636 ms rising to
891 ms, to flat at ~370 ms. The fan-out benchmark is unchanged, which is the other half of the claim:
this moved a lifecycle, not a hot path.

**What was deliberately not done.** The store-wide gate stays: requests are still answered one at a
time. The replay was the cost and the gate was not, and letting reads overlap is a different change
with a soak of its own — the engine's internals are `Arc`/`Mutex` and were soaked at parallelism 16
in the concurrency milestone, but "the engine tolerates concurrent tasks" is not "two requests may
interleave mid-operation", which is the property process-per-command gave the CLI for free and which
nothing here re-establishes. The CLI still rebuilds per command, because a process that exits has
nothing to keep. And the post-write `catch_up` is still a call rather than a signal (§9.6) — a write
still pays for the derivation it causes, inside the request that caused it, and that is most of what
the residual 370 ms is.

#### The replay keys a round's own derived layers on the round branch

Found by the rebuild-and-diff harness above, on its first run, and **not fixed here**.

`Registry::open`'s replay folds a derived layer's edges under `layer.branch`. For the derived layers a
round merged onto the trunk that is right; for the round's *own* copies of them, still sitting on the
round branch, it keys the same edges a second time under the round branch — which is what §16.3.8 and
`CLAUDE.md` #11 say must never happen. The live index does it correctly, because the engine records
against `at.home`.

On the trunk the two agree exactly, and no read path ever includes a round branch, so the surplus is
unreachable memory rather than a wrong answer — for a round that merged in full. The case that is not
merely wasteful is **partial application** (§16.5): a dropped invocation's derived layer never reaches
the trunk, so a replay files its edges under a branch nobody looks up and the trunk loses them. That
is the exact failure invariant 11 is written to prevent, arriving through the back door of a rebuild
rather than through the keying.

It is left alone because fixing it means telling a round branch from an ordinary one out of the log,
and a round branch is currently identified only by having no name — which is a convention, not a fact.
Giving `Branch` a kind is a durable-format change and belongs with the branch-reaping question already
open under *Concerns carried over from the transactional-model draft*. The harness pins what is true
today: the tests compare on the branches something can name, and assert separately that the live index
holds nothing at all under a round's branch.

The change above reduces the blast radius rather than enlarging it — a served store now answers from
the live index instead of rebuilding one per request — but the CLI still rebuilds per command, so this
is live code and not a curiosity.

#### Workers are a pool per command

`ProcessExecutor` kept one worker behind a mutex, which serialised every invocation through one
subprocess whatever the scheduler decided — the queue, reintroduced below the seam. It now holds a
pool per command, bounded by a permit, with idle workers handed back rather than respawned.

A worker whose conversation ended in anything but a clean `Done` is **discarded rather than
returned**. The protocol is request/response over a pipe, so a failed invocation leaves it at an
unknown offset and reusing it would answer the next invocation with the last one's reply. A worker
that reported `Error` is kept, because it answered: raising on one entity is not a broken process.

#### The def-view fold was O(all layers), and it hid the parallelism

Not planned work, and done because without it the milestone's own measurement said nothing.
`DefRegistry::def_layers` found def layers by filtering every layer the log knew about. Every write
session folds two def-views (§8.0) and every producer run is a write session, so a fan-out of `n`
invocations — each of which commits a layer — cost `O(n²)`, and at 32k it was **44s of the 45s the
fan-out took**. Parallelism against that measured as a slowdown, because the contention it added was
all there was left to add.

`LayerManager` now indexes committed def layers per branch. Def layers are rare by construction: a
schema changes far less often than data does.

`scale.rs`, one shared upstream cell flipped under `n` companies, release build, four cores. `p` is
the degree of parallelism; the first column is the engine as it stood before this milestone.

| `n` | before | `p=1` | `p=2` | `p=4` | `p=8` |
|---|---|---|---|---|---|
| 1,000 | 44.8ms | 11.4ms | 6.9ms | 6.0ms | 5.3ms |
| 8,000 | 2.48s | 112ms | 63.8ms | 60.2ms | 67.4ms |
| 32,000 | 42.4s | 460ms | 298ms | 271ms | 270ms |
| 128,000 | 1278s | 2.00s | 1.25s | 1.17s | 1.16s |

Read it as two separate results. The `before → p=1` column is the def-view fix, and it is where the
order of magnitude is: 640× at 128k, and quadratic to linear — the fan-out was 21µs per invocation
at 1k and 10ms per invocation at 128k, and is now 16µs at both. **Parallelism is the smaller
number**: 1.7× at two, and flat after that. Not a disappointment so much as a measurement of what is
left — with the fold gone, essentially all of the remaining work is under `MemoryStorage`'s single
mutex and `LayerManager`'s, so the second core saturates them and the third and fourth wait. Sharding
either is the next thing worth doing, and both already sit behind an interface that permits it
(§17.2). A deployment whose producers do real work rather than three comparisons will see more, which
is exactly what Amdahl says and not an excuse.

#### The CLI exits quietly when its reader goes away

`borg get … | head -1` closes the pipe as soon as it has its line, Rust turns the `EPIPE` into a
panic, and `println!` crashed the process. It is in three scenarios and it failed about one run in
forty — but only under load, which is why twenty quiet runs of `./check.sh` had never shown it. That
is precisely the frequency the C backfill bug taught us to treat as a bug and not a flake, and it was
found by stress-testing the concurrency work rather than by it.

Not fixed with a signal disposition, which needs `unsafe` and the workspace forbids it. One macro
that writes, and exits `0` if nobody is listening — which is what a Unix tool does. Errors keep going
to stderr, which the pipe did not close.

#### The determinism sweep is a scenario with a knob, and its writes go through transactions

S16 asks for the same workload replayed 50+ times with the settled result byte-identical every run.
Fifty runs is a couple of minutes, which is not a thing to put in `./check.sh` unattended — so
`BORG_DETERMINISM_RUNS` defaults to **5** and the number that counts as evidence is passed in. An
environment variable rather than a flag, for the same reason `BORG_DERIVE_PARALLELISM` is one: it is
not a fact about the store, and the same store swept on a laptop and on a build box wants different
numbers.

**The workload writes through transactions rather than through `borg set`, and that is what makes it
a determinism test at all.** The CLI commits one layer per `set`, so the round settling it discovers
one invocation and the wave has nothing to schedule — a sweep built on `set` would replay a
sequential program fifty times and prove nothing. A transaction commits every cell it wrote as **one**
layer, so a round settling it fans out across every entity, and `borg derive --rebuild` puts the
chain, the fan-out and both migration directions into a single round's waves. That is the widest
thing the CLI can be asked for.

What is compared is the settled state with **every layer id removed**. An id is assigned when a layer
opens (§7.3) and which invocation of a wave opens first is precisely what the scheduler decides, so
`authored at`, `landed at` and `fresh as of` are properties of the schedule; pinning them would pin
the thing the sweep exists to let vary. What is kept is what a client is promised: value, interned
identity, origin, freshness, and which producer said so. The reference run is separately asserted to
have content in it, because a digest that is empty every run is byte-identical every run.

---

## Tests we owe

- ~~**A pool test failed once in 37 runs, under full-suite load only.**~~ **Paid off in Act 2.** It
  was not a timing assumption. Adding the socket-transport tests raised the rate to about 1 in 15 —
  more concurrent spawning — and the captured failure was `the_pool_never_exceeds_its_size` with
  `ETXTBSY`, *"Text file busy"*, on the worker script it had just written. `exec` refuses a file some
  process holds open for writing, and spawning is fork-then-exec: a fork duplicates every open
  descriptor, so one thread writing a script leaves it briefly unrunnable for any *other* thread's
  fork. The window is one fork. Fixed in the provider rather than the harness, because the condition
  is real — editing a pipeline while `borg derive` runs is the same race — with a bounded retry on
  `ETXTBSY` in `Worker::spawn`. 60 consecutive suite runs clean since.

- **Unit coverage generally.** Tests are almost entirely integration-level. `borg-storage`,
  `borg-engine`'s internals and the CLI have essentially none of their own. Landing alongside each
  milestone rather than as a separate push.

Paid off in B: branch visibility of definitions, including the write-rejection half
(`scenarios/070-branch-visibility`), and second-order forks — a fork of a fork, in that scenario and
in `def_events.rs`. Nothing had exercised a branch chain deeper than one fork before.

Paid off in C: both migration directions and their non-interaction (`migration.rs`), a producer
seeded over data that predates it, migration roles resolved from a folded chain, and the two-def-view
write path (`write_validation.rs`). Nothing had run a `down` migration at all before.

Paid off in D: the three read modes distinguished from each other, inline computation recursing
through a chain and terminating on a cyclic read-set, an inline run proved not to advance a
watermark, the settled frontier read against the ragged head, and `reaches` in all three of its
cases (`freshness.rs`) — plus a migration hop computed on demand (`migration.rs`).

Paid off in D's second half (`concurrency.rs`): a three-hop chain racing itself, the same scenario
proved identical at one invocation at a time and at sixteen, two producers on sibling fields of one
object, a migration pair in one wave, a client write landing mid-round, and a constructed interleaving
where the upstream is *held* until its downstream has read past it. Each is replayed 30–120 times,
because a concurrency bug that shows up one run in six reads as a flake and is not one. The first
unit tests in `borg-engine` itself arrived with them, on the round ceiling — the interleavings that
distinguish a prefix from a maximum are exactly the ones an integration test cannot provoke on
purpose.

Paid off in G (`crates/borg-engine/tests/rounds.rs`): S7–S10, plus a deletion landing mid-round. The
last four need two writers overlapping in time, so they hold a round open inside a producer while the
test writes — which turned up a fragility in the *existing* gate in `concurrency.rs` as well.
`tokio::task::yield_now` hands the worker back to its own local run queue, so under load every worker
can spin on a gate only a task in the global queue can open; the upstream-commits-late test failed its
own precondition about one run in forty that way, and both gates poll on a timer now. That frequency
is exactly the one this file says to take seriously, and it was a test bug rather than a product one.

And `borg-exec-process` got its first tests at all (`pool.rs`): concurrent invocations served by
concurrent processes, the pool bounded by its size, and a worker reused when nothing overlaps. They
are not a scenario because the CLI writes one layer per `borg set`, so a wave driven through the CLI
is one invocation wide — the pool only matters where one source layer dirties many, which is a
fan-out flip, a migration backfill, or a producer pushed over existing data.

Paid off in H (`crates/borg-engine/tests/composition.rs`, `scenarios/170`–`200`): the first tests in
which a migration and a concurrent writer are in the same store. A stale *migration* round rejected
in both merge orders — which is S8 with the one producer S8's fixture structurally could not contain,
and is where the guard bug was; a client write to another entity landing mid-migration, checked by
replaying at the stated watermark; a def-only merge landing mid-round, both that it does not mislabel
the round's output and that it does not reject a round it could not have disturbed; and a chained
migration pinned as the limitation it then was rather than left to be rediscovered.

Paid off again in I: that last test flipped from pinning the gap to proving the fix, and
`rounds.rs` gained the three cases a range is judged on — a backlog settling as one round, a pipeline
pushed over already-derived data being discovered at all, and a backlog round still losing its guard
to a genuinely concurrent writer.

Through the binary: a migration's round rejected by its guard, asserted on **layers** rather than on
values, because the value alone cannot distinguish "the stale round was rejected" from "the stale
round landed and was overwritten"; a migration's merge landing under an open transaction and
correctly *not* conflicting, beside another client's write at a different def-version correctly
conflicting; a fork-of-a-fork migrating data inherited through two levels with both ancestors
untouched; and a settled state compared byte-for-byte across runs.

What is still owed here: the **client** half of S13 has no in-process test. `scenarios/170` covers it
end to end and the rule it exercises (a derived write cannot trip a guard) is unit-tested in
`transactions.rs` on a pipeline, but nothing asserts it in Rust with a migration in the picture.

---

## Milestone history

How the system got here, oldest first. Each entry says what the milestone was for, what
landed, and what it measured; the decisions they produced are grouped above.

### A — values become real

Content-addressed interning for `String`, `Binary` and `BigInt`: the hashing, the storage, and
support in the CLI and the wire protocol.

Everything was blocked behind this. No realistic scenario could exist while every field was an
integer — the spec's own motivating example is `company.website.ends_with('.ai')`.

**Done.** Interning existed in storage and nothing called it; it is now wired to the client surface
end to end. The value text form is normative in `SPEC.md` §3.4 and the `BigInt` byte encoding in
§3.1. Three decisions came out of it; they are grouped above. `scenarios/050-values` is the proof, and 030's bash
pipeline now reads a real string and a real number.

### A′ — the PID text form

A lossless, human-usable text form for PIDs, and a cell syntax built on it:

```
Company:o-1234abcd.website     an object property
Company:o-1234abcd             an existence cell
Founder[]:l-5678wxyz           a list's own cell (its length)
Founder[]:l-5678wxyz[0]        a list element
```

The current `Company#100` form is **lossy** — a PID is `(kind, branch, allocator, counter)` and it
carries one of the four. That is why shorthand has to allocate against the root, and why scenario 010
found a real bug. Encoding the whole PID fixes it properly.

Goes with A because both touch the value and PID text layer; doing them together is one round of
scenario churn instead of two.

**Done.** The codec is in `borg-core/src/pid.rs` and the form is normative in `SPEC.md` §3.1 and
§4.1: LEB128 varints for an allocated PID, all 32 bytes for a content hash, Crockford base32, and
the kind letters `o l a j y m s b n`. `Company#100` survives as input-only shorthand.

### B — definitions become load-bearing

Writes validate against the def view: unknown struct or field rejected, type mismatch rejected, and
ownership enforced so a client cannot write a derived field.

This connects the two halves. It broke every existing scenario, because none of them declared
anything — the right kind of breakage, and the scenarios now look like real usage.

**Done.** `FieldDef.origin` became `Ownership::{Source, Derived(ProducerId)}` and `DeclareField`
carries it, so a derived field is declarable at all for the first time;
`DependencyIndexProvider::writer_of` and the state behind it are gone. Enforcement lives in
`borg_engine::write::WriteSession` — one open layer plus the branch's def-view, with
`LayerHandle::put` made crate-private so there is no second door. Parsing is type-directed
(`parse::value_as`), which lifts both reservations §3.4 recorded as temporary. `describe` returns
structs as well as producers, and `borg repo push` folds them into one def layer. §5.1, §5.2, §6.1,
§8, §9.2, §16.1 and §17.4 are updated; §8 is rewritten. `scenarios/060-definitions-enforced` and
`070-branch-visibility` are the proof, and every other scenario now declares a schema first.

### C — migrations end to end, through the CLI

`MutateField` already carries `up`/`down` producer ids, but there is no way to supply an
implementation. With A and B done, a migration is just another script in the repo, and
`borg repo push` already knows how to turn a script into a producer definition.

This unlocks §18's first acceptance scenario, which has **never run**: fork, change a field's type
with a migration, read old data correctly through the new lens on the child while the parent is
untouched, then def-only merge and watch the parent's values migrate.

It is the most valuable demo in the project — the thing no other ORM does, that the whole system was
designed around, and that we have never seen work.

**Done.** `scenarios/080-migration` is the acceptance scenario, running end to end through the real
binary, in both directions and with a no-`down` case that reports `broken`. Five decisions came out
of it, below. The blocker was `WriteSession` validating against the branch rather than the writer —
now split, shape at the ClientVersion and permission at the branch (§8.0). Clients got a real
ClientVersion: the branch's def-version, with `--client-version` to act as an older one and
`borg def version` to see it. `ProducerKind::Migration` lost its version pair, which is now folded
per branch — the thing that makes a def-only merge of a migration work at all. §5.3, §5.4, §5.5,
§8.0, §9.2, §9.3, §9.6, §10.4, §13, §17.3 and §17.4 are updated.

### D — background derivation, and concurrency

Run derivation continuously instead of on demand; make `FreshnessRequirement::Current` actually
compute inline; add `frontier.reaches()`; add the branch-scoped pause switch.

Concurrency is folded in here because it is the same work: a background loop is exactly where you
parallelise, and `settle()`'s round-ceiling is what both need reworked. Deferring it this long is a
deliberate bet that the sequential assumption lives in one function — it should not slide past C.

**The freshness half is done.** `Current` computes inline for real, through a narrow
`InlineDerivation` seam the resolver holds instead of the engine; it follows recorded read-sets so a
cell's inputs are computed before the cell, and it runs migration hops, which closes the gap C left
where a version nothing had materialized read `stale` forever. Derivation now follows every commit
unless the branch is paused, and `borg derive pause | resume | status` is that switch, beside the
store. `frontier.reaches()` and settled-frontier reads exist and are exposed as
`borg frontier reaches <layer>` and `borg get --settled`. §9.6, §10.5, §16.4 and §16.6 are updated,
`scenarios/090-freshness-controls` is the proof, and 040 now demonstrates lag by pausing rather than
by declining to derive. Three decisions came out of it; they are grouped above.

**Concurrency is done too.** `settle()` alternates a sequential *discovery* pass with a concurrent
*execution* pass — one wave of invocations at a time, bounded by `set_parallelism` (default: one per
core, `BORG_DERIVE_PARALLELISM` in the CLI). The ceiling is committed state raised by every layer the
round commits, rather than a value threaded through a loop. `ProcessExecutor` holds a pool per
command instead of one worker behind a lock.

Turning it on found three things that were untrue, and one that cannot be made true without a design
change; all four are grouped above. `crates/borg-engine/tests/concurrency.rs` is the proof — seven scenarios,
each replayed 30–120 times at sixteen-way parallelism, asserting the settled result and never the
schedule. §6.2, §7.3, §16.3, §16.4, §16.5 and §17.3 are updated.

### E — events get identity; layers reference them

Phase 1 of the transactional model. A stored record no longer carries the
layer it lives in: an `Event` carries `authored`, the layer it was *first* committed to, and layers
name their members. Merge stops copying — a parent layer names the child layer's events — and the
lineage the old rewrite destroyed survives, so a read reports both where a value was authored and
where it landed. `StorageProvider` grew `author_event` / `include_event` / `rebuild_read_index`,
`read_layer` yields membership, and both backends keep a materialised
`(branch, cell, version) -> (layer, event)` index maintained as events stream into an open layer.
§4.3, §6.2, §10.4, §11, §13, §16.3 and §17.1 are updated;
`crates/borg-engine/tests/events.rs` is S11 and S12, and the `StorageProvider` contract now runs
against **both** backends rather than SQLite alone.

### F — every client write is a transaction, and its guards are automatic

Phase 2 of the transactional model. A client no longer writes to a
shared branch: `borg tx begin | get | set | delete | commit | abort` forks, writes in isolation and
merges, and a bare `borg set X v` is that same thing done in one process. What a transaction read
becomes what its commit is contingent on, re-evaluated against the parent since the fork point — so
guards stopped being opt-in without the guard machinery changing at all. `Transaction` is a value
type in `borg-core` carrying the fork point, the read-set and the write-set;
`BranchManager::merge_transaction` is the commit; `LayerManager::check_reads` is the automatic half
of `check_guards`. `WriteSession` now reports what it probed and what it wrote, which is what puts
the implied-existence read (§8) into the read-set where it belongs. Transaction state lives beside
the store with the pause flags, and is reaped on an idle timeout swept when a process opens the
store. §12 and §13 are updated; `scenarios/130`, `140` and `150` are S2–S6 plus the surface and the
reaping, and `crates/borg-engine/tests/transactions.rs` has the guard-derivation rules underneath
them.

Four decisions came out of it; they are grouped above. The fan-out curve is unchanged: 128k entities at four cores
derive in 1.61s against 1.56s before, and the shape is still linear.

### G — derivation is a transaction, and the round ceiling is gone

Phase 3 of the transactional model, and the last of it. A round forks
the branch at the source layer it settles, runs every producer on the fork, and merges what settled —
so a producer's read path is `[(round branch, head), (trunk, fork point)]` and there is no
high-water mark anywhere. `reflects` is the fork point by construction; a client write landing
mid-round is above the bound and is simply not in the path.

`RoundCeiling` is deleted, not kept as a fallback, and with it the prefix-versus-filter hole recorded
below, the `ReadPath` that would have had to carry admitted layers, and the `reflects` column the
provider line was never going to get. `borg_core::Round` is `N` invocations sharing one fork point,
sitting beside `Transaction` because the two guard rules have to be read together;
`BranchManager::merge_round` is the merge; `DerivationEngine::settle` is public, because the
interleavings worth testing are statements about *which* layer a round settles.
§12, §13, §16.4 and §16.5 are updated; `crates/borg-engine/tests/rounds.rs` is S7–S10 and
`scenarios/160-rounds-are-transactions` is S7 through the real binary.

Eight decisions came out of it; they are grouped above.

**The fan-out benchmark moved, and this is where it went.** 128k entities at four cores derive in
1.88s against 1.62s before (+16%); the re-derive after flipping the one shared cell is 1.50s against
1.14s (+30%). The shape is unchanged — still linear in `n`, still scaling with parallelism the same
way.

Measured, not guessed, and in two parts. **The merge is ~0.10s of it** at 128k, after three fixes
worth naming because each was a factor of several: the cell-touch index no longer clones a `CellRef`
per probe (it was keyed on a `(BranchId, CellRef)` tuple, and `HashMap::get` wants the whole key); a
merge whose parent has had *nothing* written to it since the fork point skips building the guard set
at all, which is one comparison instead of a million; and the round's `n` per-invocation layers are
regrouped into one layer per producer, so the trunk gains one layer rather than 128k. The naive
version of the merge was 0.48s.

**The remaining ~0.26s is the second read-path segment**, and it is the cost the draft
predicted: every producer read now walks `[(round branch, head), (trunk, fork point)]` instead of one
segment, so it is two index probes rather than one, ~900k extra probes in a 128k round. Halving that
back would mean a read index keyed cell-first rather than branch-first, which makes `scan_buffer`
proportional to the whole store rather than to a branch — a trade worth measuring on its own and not
worth taking blind. What was cheap was removing a `CellRef` clone from the probe itself, which is
done: `MemoryStorage`'s read index is nested now for the same reason the touch index is.

### H — features that had never met

Phase 4a: the composition class (S13–S15, *Acceptance scenarios*) and the determinism sweep (S16). This
class has produced the most bugs in the project and had the least coverage — every major feature
worked alone and several had never been in the same store.

**It found one, and it is the kind this file exists to record.** A migration is the only producer
whose output shares a `CellRef` with a cell clients write, and a round's guard subtraction was keyed
on `CellRef` — so a migration deleted its own guard on the record it migrates from, and a stale
migration round could land over a fresher one with every watermark advanced and nothing outstanding
to correct it.

`crates/borg-engine/tests/composition.rs` is the mid-round interleavings of S13 and S14; `170`, `180`
and `190` are S13, S14 and S15 through the real binary; `200-determinism` is S16. Four decisions came
out of it, below, and one of them is a limitation recorded rather than fixed. §16.5 is updated.

**The fan-out benchmark is unchanged**, measured before and after on the same machine because this
one moves at a different speed from the box the milestone-G table was taken on. 128k entities at four
cores: derive 2.02s after against 2.06s before, re-derive after flipping the one shared cell 1.56s
against 1.66s. At eight, 1.93s against 1.93s and 1.58s against 1.52s. That is noise in both
directions, and it is what the change predicts: a round with no concurrent writer does not build its
guard set at all (`touched_since` answers the whole of it in two map lookups), and where one is built
the difference is a `BTreeSet` of `&CellAt` where there was one of `&CellRef`.

### I — a round settles a range

A round covered one source layer. It now covers `[watermark+1 … head]`, and the two limitations this
file has carried since G — *a backlog of source layers still costs re-runs* and *a chained migration
is not discovered by a catch-up* — are the same change and are both closed.

Neither was a cost entry. A round per source layer **manufactures** the staleness its own guards then
reject: settling `L10` while `L11` is already on the trunk runs work that was guaranteed to lose. And
a producer whose input exists only in a *derived* layer had nothing to trigger it, because derived
layers open no rounds, so a chained migration or a pipeline pushed over already-derived data needed
`borg derive --rebuild` and an operator who knew to run it.

Three things moved together: the opening wave is every layer in the range, derived layers included;
the fork is at the top *layer* while `reflects` stays the top *source* layer; and the buffer scan runs
at the top, where the world is complete. §6.3, §9.6, §16.4 and §16.5 are updated — §6.3's
no-coalescing rule is retired, and what it costs is written down. `scenarios/220-a-backlog-settles-once`
is the new scenario; `rounds.rs` gains the backlog, the pipeline-over-derived-data case and a backlog
under a concurrent writer; `composition.rs`'s chained-migration test flipped from pinning the gap to
proving the fix; `scenarios/180` reaches the second hop of its chain with a plain `borg derive` and
names the top of the range rather than the layer that dirtied it. One decision came out of it; it is grouped above.

**The fan-out benchmark found a quadratic, and it was a real one.** Settling a range puts the seeding
scan (§9.6) where the buffer is *full* — which it never was before, because `catch_up` spent it on a
round forked at the bottom of the log. Two `Vec::contains` dedupes that had only ever seen one or two
candidates suddenly saw 128k, and 128k entities took **88 seconds to derive against 2.6**. Both are
now sets. This is exactly `CLAUDE.md` invariant 5 and exactly the class the benchmark exists for: no
correctness test could see it, the change that exposed it touched neither dedupe, and the curve said
so immediately.

After the fix, 128k entities on this machine: derive 2.66s at one core against 2.62s before, 2.06s at
eight against 2.02s; re-derive after flipping the one shared cell 2.46s against 2.38s at one core and
1.59s against 1.52s at eight. Two to nine percent, consistently in one direction, and it is work the
old schedule was not doing — the seeding scan now runs over a populated buffer on the first derive.
The push-then-derive pattern the benchmark uses has no backlog in it, so the improvement the change is
*for* does not show here; `220` is where it is visible.
