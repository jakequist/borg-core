# Draft: Act 2 — the SDKs

> **Status: draft, not yet normative.** `SPEC.md` describes the engine. This describes the client
> story being built on top of it. Sections get marked **Built** as they land; the document is
> deleted with salvage when wholly absorbed, as `SPEC-DRAFT.md` was.

Two artifacts per language, deliberately distinct:

- **The DSL** (author-side): defines a repo — structs, pipelines, migrations — and emits `describe`.
  The DSL's `Company` is the *source* of definitions.
- **The client** (consumer-side): reads and writes data through transactions. Its `Company` is
  *generated from* the registry's definitions and carries the ClientVersion it was generated at.

Same name, different artifacts, opposite directions. Pipelines use the first; applications use the
second.

---

## 1. What already exists and is merely being wrapped

The SDK is thinner than it looks, because the engine was built mediated-access-first:

- **Tracking ships in no SDK, ever.** Every access is a wire round trip through `ProducerCtx` /
  the transaction, and the engine records read-sets server-side. A Proxy (TS) or `__getattr__`
  (Python) only *translates* property access into `get`/`set` messages. The bash pipeline proved
  this with zero client code.
- **The client surface is the transaction surface.** `fork` = `tx begin`, reads recorded, writes
  isolated, `commitAndMerge` = guarded merge. The CLI proved this shape; the SDK wraps it over a
  socket instead of argv.
- **`describe` is the DSL's compile target.** Structs, ownership, migrations — the repo-push
  contract from milestones B/C. A DSL that emits it needs no new engine surface.
- **Worker execution exists.** TS pipelines run under `borg-exec-process` exactly as bash ones do.

## 2. Decisions taken (from design conversation, reasoning recorded)

1. **Explicit async now, sync-over-async later, preload never (for pipelines).** v1 pipeline and
   client APIs are verbose: `await c.get("headcount")`, `await c.set("isInvestible", true)`.
   Property-access sugar via `worker_thread` + `Atomics.wait` is the later path. Preloading all
   fields is rejected because it collapses the server-recorded read-set to object granularity,
   destroying field-level invalidation (the property scenario 030 proves) and, on the client,
   eroding cell-granular guards (S5).
2. **Ownership is explicit in the DSL.** `borg.bool().derived()` on the field; the pipeline lists
   `writes: ["isInvestible"]`; describe-assembly errors on any mismatch in either direction.
   Inference is later sugar and happens at describe time, never at runtime.
3. **`hasOne` and `borg.list(borg.ref(...))` in v1; `hasMany` deferred.** A derived reverse index
   is per-entity invocations appending to one shared list cell — the recorded caveat on
   single-writer-per-field — and is really the deferred aggregation story. The sugar must not
   imply semantics the engine cannot honor.
4. **ClientVersion is a constructor argument of `BorgContext`.** Codegen emits
   `createBorgContext(...)` with the generation-time def-layer baked in and sends it on connect.
   This is §5.4 made real: old generated clients keep reading through `down` migrations, and the
   engine can name exactly which live clients a def push would break.
5. **Transport: one message protocol, per-transport framing.** Unix socket (length/newline-framed)
   for local; WebSocket for browsers, where framing is native and the layer disappears. Same serde
   messages, same codecs, no protocol forks. A dropped browser connection is an abandoned
   transaction, which the idle reaper already handles.
6. **`borg serve` is expected to be superseded** by remote-connection features (the CLI itself
   connecting to a remote server) — build it as the local instance of that future shape, not as a
   separate species. Keep the serve loop thin over the same command layer the CLI uses.
7. **Python is a scheduled neutrality gate, not a parallel build.** A Python *pipeline* SDK lands
   after the TS one and before the client contract freezes. Python's sync `__getattr__` makes
   mediated access trivial — which is the point: if Python is trivial where TS needs machinery,
   the machinery is ergonomics, not contract, and must not leak into the protocol. **Run, and it
   was**: `__getattr__` is four lines and sends byte-identical messages. §4.2 has the rest.

## 3. The v1 surfaces (verbose on purpose)

### DSL (author-side)

```ts
const Company = borg.struct("Company", {
  name: borg.string(),
  headcount: borg.int(),
  employees: borg.list(borg.ref("Employee")),
  isInvestible: borg.bool().derived(),
});

const isInvestible = borg.pipeline(
  "is_investible", Company, { writes: ["isInvestible"] },
  async (c, world) => {
    const headcount = await c.get("headcount");        // recorded server-side
    await c.set("isInvestible", headcount !== null && headcount < 100);
    // world.get(ref, field) — random access for hops, same recording
  },
);

export default borg.repo({ id: 2, structs: [Company], pipelines: [isInvestible] });
```

The repo module, run with `describe`, prints the describe JSON; run as a worker, it serves
invocations. One artifact, two modes — matching the bash worker's shape.

### Client (consumer-side)

```ts
const bc = createBorgContext({ socket: process.env.BORG_SOCKET });  // version baked in by codegen

const tx = await bc.branch("main").begin();
const c = tx.object(Company, "o-100");          // a handle; no I/O yet
const hc = await c.get("headcount");            // read → recorded → guard
await c.set("headcount", 400);                  // write, isolated on the tx branch
await tx.commit();                              // merge; throws ConflictError on a tripped guard
```

`ConflictError` is contract; the auto-retrying `bc.transact(fn)` wrapper is later sugar.

## 4. Build order and acceptance scenarios (numbering continues from 220)

1. **TS pipeline SDK + worker socket transport** — **Built.** `packages/borg-sdk` (pnpm, tsc,
   vitest; no nx until a second TS package), socket-per-env-var transport in `borg-exec-process`,
   **scenario 230**. What the build settled, beyond what was written above:

   - **The transport is declared in `describe`, not detected at runtime.** Detection would have to
     tell "has not connected yet" from "printed to stdout first", which is the case the socket exists
     to make harmless — the detector is broken by the thing it detects. Absent means stdio, so every
     shell worker is untouched and no socket is created for it. SPEC §17.4 carries the reasoning.
   - **A socket worker's stdout is duplicated onto the engine's stderr.** Inheriting it would put a
     subprocess into `borg get --value`'s output; discarding it would hide a `console.log` from the
     person who wrote it.
   - **`Invoke` names its producer as a string.** A `ProducerId` is a hash past 2⁵³ and JSON has no
     integers, so a JS worker dispatching on a number resolves a producer that does not exist. Free
     until now only because every worker implemented exactly one producer and ignored the field.
   - **`describe` may also state the repo id**, cross-checked against `borg.toml` rather than
     ignored, because the DSL makes an author write it a second time.
   - **Field names are used verbatim**, `isInvestible` in the DSL and `Company#1.isInvestible` at the
     CLI. A silent case conversion is a mapping somebody has to reverse-engineer from an error.
   - **`null` is absence in both directions**, collapsing "never written" and "tombstoned"; the store
     keeps the distinction and a pipeline has nothing different to do with it.
   - **`int()` refuses values past 2⁵³** rather than rounding them, and names `bigint()`.
2. **Python pipeline SDK** — the neutrality gate. **Built.** `packages/borg-sdk-py` (Python ≥3.11,
   zero runtime dependencies, `unittest` cases that pytest also runs), **scenario 240**. It needed
   **no Rust changes at all**, which was the first thing it was there to find out.

   ### The verdict

   **Nothing in the protocol was harder in Python, and nothing was impossible.** The two SDKs emit
   the same `describe` payload *byte for byte* — scenario 240 asserts exactly that, by diffing the
   describe output of the Python pipeline against 230's TypeScript one, which it mirrors field for
   field. Same handshake, same messages, same socket, same stderr duplication. `borg repo push`
   walks a directory of executables and never learns what any of them is written in; the only
   per-language fact in the whole store is a path in `borg.producers.json`.

   **Two things in the TS SDK turned out to be ergonomics wearing contract's clothes.** Neither had
   leaked into the protocol, so neither cost anything — but both read as contract in §4.1 and in the
   TS source, and they are not:

   - **`await` is not the contract; it is JavaScript's.** `c.get("headcount")` here blocks on a
     socket read and returns the value. Every semantic §2.1 defends — one wire message per access,
     nothing preloaded, the read-set recorded server-side, field-granular invalidation — holds
     identically with no `await` anywhere. The verbosity was never buying the semantics; it was
     paying for JavaScript's event loop. *Property access is the same finding twice*: `c.headcount`
     and `c.isInvestible = True` are four lines of `__getattr__`, send byte-identical messages, and
     are tested. §2.1's `worker_thread` + `Atomics.wait` plan is therefore a TypeScript build item,
     not an SDK-surface question — and the fact that Python could ship the sugar today and TS cannot
     is the strongest available evidence that the sugar is not contract.
   - **The request-serialising promise chain is machinery, not protocol.** `connection.ts` chains
     every request behind the last so `Promise.all([c.get("a"), c.get("b")])` cannot cross two
     replies. Nothing in synchronous Python can produce that overlap; the equivalent here is a
     four-line mutex that only matters for a body that deliberately reaches for `threading`. The
     protocol's "one reply per request" is contract. Everything the TS SDK does to survive an author
     writing idiomatic concurrent JavaScript is not.

   **One number in §4.1 above is stated as contract and is not.** "`int()` refuses values past 2⁵³"
   is two rules glued together: *never silently round an integer*, which is contract and language-
   neutral, and *2⁵³*, which is where a JS double stops being exact. The engine's `Int` is an `i64`
   (`borg_core::parse` reads it with `i64::from_str`), so `borg.int_()` here refuses outside `i64`
   and names `bigint()` at the engine's boundary. Copying 2⁵³ would have refused values the store
   holds perfectly well, on the strength of a limitation Python does not have. The rule travelled;
   the threshold did not, and §4.1's wording should not imply it did.

   **Python needs one check TypeScript does not**, and it is this language's problem alone: `bool`
   subclasses `int`, so `int_().encode(True)` would write `1` into an `Int` cell without complaint.
   A silent type change, in the one table whose whole job is preventing them. It is handled in the
   SDK, where a language's hazards belong, and asks nothing of the protocol.

   **`Double` is the one type whose text is not identical across the three languages.** Rust never
   uses exponent notation, Python does past `1e16`, JS uses it differently again. This is harmless
   and worth stating once rather than rediscovering: a `Double` is not content-addressed, so its
   spelling only has to *parse*, and all three read all three. The types where spelling **is**
   identity — `String`, `BigInt`, `Binary` — have exactly one form in both SDKs, which is what
   actually matters and what "canonical text" should be taken to mean.

   ### What had to be reverse-engineered rather than read

   These are documentation bugs, not design ones. Each was recovered from `borg-protocol`'s Rust
   doc comments, `borg-exec-process`, or the bash worker in 030 — which means the second SDK could
   be written from the first, but a third one written from `SPEC.md` alone could not.

   - **§17.4 describes the handshake but never spells the exchange.** "Codecs are negotiated in a
     handshake" does not say that the engine speaks first, that its message is
     `{"version":1,"codecs":[…]}`, or that the worker's reply is the single key `{"codec":"json"}` —
     which is not one of the `FromWorker` variants and so appears nowhere in the message table.
   - **An `Invoke`'s `input` is an entity address the worker concatenates onto.** That
     `Company:o-04068` + `"." + field` is how a cell address is built worker-side is stated in a
     code comment in `borg-exec-process` and demonstrated in the bash worker. It is the single most
     load-bearing fact about writing a worker and it is not in the spec.
   - **Absence has two spellings and the worker must collapse them.** `{"value":null}` (never
     written) and `{"value":"~"}` (tombstoned) both reach a worker; §17.4 mentions the tombstone
     text but not that a `Get` may answer JSON `null`, and never says what a worker should do with
     the difference. Both SDKs collapse it — §4.1 records that decision, but a first-time reader of
     the spec meets it as a surprise.
   - **Whose repo is it — the module or the directory?** §17.4 says a `derived_by` naming a producer
     "the repo does not implement" is a push-time error. The engine means the *directory*: it
     resolves ownership against every producer every executable described. Both SDKs mean the
     *module*, because a module is all its `describe()` can see. So a repo of two files cannot have
     one file declare a field the other file's pipeline owns — the SDK refuses it before the engine,
     which would have accepted it. Scenario 240's mixed repo works around this by reading across the
     boundary through `world`, which is stringly and needs no declaration. **This is a real seam and
     the one thing here worth a design decision**: either the SDKs learn to describe a field owned
     elsewhere (a `derived_by="name"` escape hatch with no local pipeline), or the spec says a
     module is the unit and a repo of one language is one file.
   - **`borg.toml`'s `[[pipelines]] command` entries are decorative.** `repo_push` walks
     `pipelines/` and asks every file; nothing reads the manifest except a line-wise grep for the
     first `id =`. Every scenario's `borg.toml` lists its pipelines and none of it is consulted.
     Harmless today and a trap the moment somebody expects the list to select or exclude something.

   ### What the gate did *not* find

   No protocol element was Python-hostile. No message needed a new shape, no field needed a new
   encoding, and no engine behaviour needed a flag. The `world.get(cell, borg.int_())` shape §5
   guessed at — stringly with an optional field type — transferred without change, which is a second
   language's worth of evidence for it. And the stderr duplication built for 230 is confirmed to be
   transport-level rather than TypeScript-level: scenario 240 asserts a Python `print()` mid-round
   reaches a human on stderr and touches neither the message stream nor the CLI's own stdout, with
   nothing in `borg-exec-process` changed to make it so.
3. **`borg serve` + TS client SDK** — client protocol module in `borg-protocol` (tx ops, resolve
   envelope, describe/def push), unix-socket server, per-connection transaction binding.
   **Scenario 250**: two concurrent SDK clients, S2's conflict through the socket; a client killed
   mid-transaction reaped by the idle timeout.

   **Server half built.** `borg_protocol::client`, `borg serve --socket`, and `scenarios/250-serve`
   (two clients driving the protocol directly, no SDK). Three corrections to the sketch above, all
   recorded in `SPEC.md` §17.5 and `crates/borg-cli/src/serve.rs`:

   - *Not* per-connection transaction binding — **per-store**. The transaction table is already a
     sidecar (§12.2), so a handle outlives its socket for free, and that is the stronger property:
     binding to the connection would make a dropped socket destroy work rather than abandon it,
     which is the opposite of what §2.5's browser story needs.
   - The serve loop being thin required extracting the CLI's command layer (`borg-cli/src/ops.rs`)
     out of `main.rs`, so that `main.rs` is argv-and-printing and `serve.rs` is protocol-and-message
     over the same functions. Two things did not lift and are findings about the CLI, not about
     serve: the `--tx` / `$BORG_TX` / only-one-open defaults are shell affordances a socket client
     cannot use, and `borg set` is an implicit one-shot transaction with no place in a protocol
     whose clients hold transactions explicitly.
   - Not one `Registry` across connections. The store is opened per request and requests are
     serialised, because the ops layer opens and drops registries around derivation; holding one
     open is a change to derivation's lifecycle and is the next real server's first job.

   The TS client SDK and the WebSocket listener are **not** built. The transport trait
   (`serve::Transport` / `serve::Peer`) is the seam the latter slots into, per §5.

   **`def push` and `repo push` are not on the socket, and that leaves a gap worth naming.** Both
   read from a *filesystem* — a JSON file, a directory of pipeline scripts — and `repo push` writes
   the resolution table saying where that code lives (§9.2). A client naming paths on the server's
   disk is not a client operation, it is a deployment one. But the advisory lock means the CLI
   cannot do it either while the store is served, so today **pushing a schema to a served store
   means stopping the server**. That is acceptable for a local dev server and is not acceptable for
   anything else; the answer is a deploy path that ships code rather than names it, which is the
   same question as `ExecutionProvider`'s container future (§17.3) and should not be pre-empted by
   adding a `def_push` message that only works when the client and the server share a disk.
4. **Codegen** — `borg generate --lang ts -o ...` emitting typed structs + `createBorgContext` with
   the ClientVersion stamp; `--watch`. **Scenario 260**: generate, compile a client against it,
   def-push a migration, regenerate, old-versioned client still reads through `down` (the SDK twin
   of scenario 080's second half).
5. **Sugar** (unscheduled): sync-over-async pipelines, `transact` retry, ownership inference,
   `hasMany` (blocked on aggregations).

## 5. Open questions carried

- WebSocket server: in `borg serve` from day one or when the browser client lands? (Lean: the
  transport trait supports it from day one; the actual HTTP listener lands with the browser work.)
- Codegen for list/ref fields: how a `Ref` renders in a typed client (a typed handle, presumably —
  `c.get("employees")` yielding refs the client can `tx.object(...)` through).
- Whether `world` (pipeline random access) takes generated types or stays stringly in v1. **Partly
  answered by the build**: it is stringly, with an optional second argument taking the same
  `FieldType` the DSL already produces (`world.get(cell, borg.int())`). That is where a generated
  type slots in, so only the source of the type changes later, not the shape.
- **`describe` mode is still stdout-sensitive.** A top-level `console.log` in a repo module corrupts
  the describe payload, because that process's whole stdout *is* the payload. It fails immediately
  and loudly (`describe emitted unusable JSON`, quoting the text), which is a far better failure than
  a desynchronised worker — but it is the one place where the socket does not help, and an author who
  hits it is the same author the socket was built for. **The Python SDK has it identically** (a
  `print()` at import time, or a library that logs on import), which confirms it is a property of
  `describe` being a plain stdout invocation rather than of any one runtime.
