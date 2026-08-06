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
const bc = await createBorgContext({ socket: process.env.BORG_SOCKET });  // version baked in by codegen

const tx = await bc.branch("main").begin();
const c = tx.object(Company, "o-100");          // a handle; no I/O yet
const hc = await c.get("headcount");            // read → recorded → guard
const seen = await c.resolve("headcount");      // the same read, with its §10.4 envelope
await c.set("headcount", 400);                  // write, isolated on the tx branch
await tx.commit();                              // merge; throws ConflictError on a tripped guard
```

`ConflictError` is contract; the auto-retrying `bc.transact(fn)` wrapper is later sugar.

*As built*, `createBorgContext` is awaited — it connects and handshakes, and an error at construction
is better than one at first use. The `resolve` line is §4.4's envelope split.

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

   *Since amended in exactly one place*: the implementation fingerprint (§4.6) is a hash of the code,
   so two different programs must describe themselves differently there or it is not a hash of
   anything. Scenario 240 strips that one field before diffing and asserts the two differ. Everything
   else is still byte for byte.

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

   **Server half built.** `borg_protocol::client`, the serve loop, and `scenarios/250-serve` (two
   clients driving the protocol directly, no SDK). It shipped as `borg serve --socket` and is now
   `borg-server` (§2.6, SPEC.md §17.6). Three corrections to the sketch above, all recorded in
   `SPEC.md` §17.5 and `crates/borg-server/src/serve.rs`:

   - *Not* per-connection transaction binding — **per-store**. The transaction table is already a
     sidecar (§12.2), so a handle outlives its socket for free, and that is the stronger property:
     binding to the connection would make a dropped socket destroy work rather than abandon it,
     which is the opposite of what §2.5's browser story needs.
   - The serve loop being thin required extracting the CLI's command layer (now `borg-host::ops`)
     out of `main.rs`, so that `main.rs` is argv-and-printing and `serve.rs` is protocol-and-message
     over the same functions. Two things did not lift and are findings about the CLI, not about
     serve: the `--tx` / `$BORG_TX` / only-one-open defaults are shell affordances a socket client
     cannot use, and `borg set` is an implicit one-shot transaction with no place in a protocol
     whose clients hold transactions explicitly.
   - Not one `Registry` across connections. The store is opened per request and requests are
     serialised, because the ops layer opens and drops registries around derivation; holding one
     open is a change to derivation's lifecycle and is the next real server's first job.

   **Client half now built too** — `packages/borg-sdk/src/client.ts`, exported as `borg-sdk/client`,
   with **scenario 260** driving it against a real server. The WebSocket listener is still not built;
   the transport trait (`serve::Transport` / `serve::Peer`) is the seam it slots into, per §5. What
   the client build settled is in §4.4, because everything interesting about it turned out to be a
   codegen question wearing a client's clothes.

   ~~**`def push` and `repo push` are not on the socket, and that leaves a gap worth naming.**~~
   **Answered by `repo_push`** (SPEC.md §17.6). The gap was real and the reasoning above was half
   right: a client naming paths on the server's disk is indeed a deployment operation and not a
   client one, and the advisory lock meant the CLI could not do it either, so **pushing a schema to
   a served store meant stopping the server**.

   What the original entry got wrong is what should therefore be *on* the socket. It reasoned as if
   the only candidate were a `def_push` the *client* performs, which would need the two to share a
   disk — so it deferred to a deploy path that ships code. The message that landed is the other
   shape: the **server** performs the push, against a path on its own filesystem, and the client
   asks for it. That is local-only and says so, the payload is extensible so that the artifact form
   is a further field rather than a second message, and nothing about it pre-empts
   `ExecutionProvider`'s container future (§17.3) — a container reference is a different way for the
   *server* to find code, which is the same sentence with a different noun.

   The precondition was not the protocol. It was §9.2's implementation fingerprint: a `repo push`
   that recomputed every source buffer whether or not anything had moved is not a thing anyone could
   run against a live server, so this was not merely unbuilt before that landed — it was not safe to
   want. `def push` is still not on the socket, and is the smaller case: it reads one JSON file that
   a repo would emit anyway.
4. **Codegen** — `borg generate --lang ts -o ...` emitting typed structs + `createBorgContext` with
   the ClientVersion stamp; `--watch`. **Built.** `crates/borg-cli/src/generate.rs`, **scenario 260**
   (generate, compile, run — including a deliberately wrong program that must *fail* to compile) and
   **scenario 270**, which is scenario 080's second half arriving in the SDK: generate at v1, push a
   migration, regenerate to v2, and the v1-generated client — unchanged, unrecompiled — still reads
   dates while the v2 one sees years.

   ### What the two builds settled

   - **The value carrier is one table; the *shape* of a struct is not.** Client conversions are the
     same `values.ts` a pipeline uses, and where a client wants a different carrier it is defined in
     terms of the shared one rather than beside it (`refText` is `ref` with the wrapper taken off).
     One entry in that table is per-language ergonomics — see the ref decision below — and none of it
     is protocol.
   - **`get` is the value and `resolve` is the value with its §10.4 envelope, at one round trip
     either way.** §17.5 never answers a read with a bare value, so `get` discards an envelope rather
     than saving a message, and it is defined as `resolve().value` so there is literally one read
     path. This is the split the CLI already has — `borg get --value` against the full print — and it
     is the same split for the same reason: an envelope in the middle of arithmetic is a tax on the
     common case, where the state is `current` and always will be.

     **With one exception, and it is the interesting one.** A `broken` read is not "no value"; it is
     *no value reachable from this version, for a reason* (§9.3, §14). Returning `null` for it would
     collapse it into "nothing was ever written here", which is the exact substitution §9.3 forbids
     the engine from making — so `get` throws `BorgStateError` carrying the envelope, and `resolve`
     returns it labelled. `stale` and `unvalidated` do not throw: those are real values that are
     merely behind, which is what the freshness mode asked for.
   - **A reference reaches a client as a branded string, not as the pipeline SDK's `Ref` object.**
     This answers §5's open question, and the reasoning is that the two sides do different things
     with a reference. A pipeline's next move is `world.get(ref.cell("Employee", "name"))`, so an
     object with a `.cell()` on it earns its keep. A client's next move is always
     `tx.object(Employee, it)`, so an object would exist only to be unwrapped — and, decisively, a
     class cannot carry the *target struct* in its type, because there is one `Ref` class for every
     struct. `Ref<"Employee">` can, which is what makes `tx.object(Company, employeeRef)` a compile
     error. A list field yields `Ref<"Employee[]">`: its value is the handle to the list and not the
     elements (§4.2), so element access is still deferred with the rest of the list story.
   - **Derived fields are emitted `readonly`, which SPEC §15 had deferred "with the SDKs
     themselves".** These are the SDKs themselves. One word in the generated interface plus a
     `WritableKeys` mapped type in the SDK turns a client write to a producer-owned field into a
     compile error, and the generated file stays readable — which matters, because it is the first
     Borg code a user reads.
   - **`borg generate` reads through the socket when the store is served and opens the store when it
     is not**, decided per read rather than by a flag, and it says which it did. A served store
     refuses every other invocation (§17.5), so a generator that could only open a file would fail
     exactly when a developer is most likely to run it. This is §2.6's remote-connection future
     arriving for one read-only command, and deliberately not for the write path: `generate` needs
     none of the answers the general case needs about transactions or about who owns a write. 260
     asserts the two paths emit a byte-identical module.
   - **One message was added to §17.5, and neither half of it could be composed.** `def_show` answers
     about a struct you can already name and codegen's whole job is to not know the names; and the
     stamp a module needs is the branch's *def*-version, which is not `branch_head` — head moves on
     every `borg set`. So `def_view` returns both, in one round trip, because they are one read: a
     generator that took them separately could be handed a schema and a version from either side of a
     push. `DefView::objects()` in the engine is the one supporting addition.

   ### Where the protocol pinched

   Both are reported rather than worked around, and neither is a hack in the code.

   - **There is no server push, so `--watch` polls.** §17.5 is one request, one response, in order,
     with no correlation ids because there is nothing to correlate; a notification would be a change
     of shape and not a field. The loop polls the def view, which is at least the *right* thing to
     poll — head would rewrite the file on every data write. It is cheap and it is honest, and it is
     the first thing a subscription would replace.
   - **A rejected handshake is answered and then hung up on, and whether the client sees the answer
     is a race.** `serve::session` sends the error and returns, which closes the socket; a client that
     writes its first request before reading gets an EPIPE that discards the very answer it was
     racing. The server never acknowledges an *accepted* handshake either — it has nothing to say — so
     a client cannot distinguish "accepted" from "not answered yet" except by asking something. The
     fix is a lingering close on the server (write, shut down the write half, drain), which is work in
     `Peer` and not in the SDK.
5. **The two primitives an application needs and could not say** — `list` and `tx_create`. **Built.**
   `borg_protocol::client`'s `List` / `TxCreate`, `ops::list` / `ops::tx_create`, `borg list <Struct>`
   and `borg tx create <Struct>`, `bc.branch(n).list(Struct)` and `tx.create(Struct)` in the client
   SDK, **scenario 280**. Driven by `/examples/personal-crm`, which cannot express *"list the
   contacts"* or *"create a contact"* with anything §4.3 shipped.

   ### What the build settled

   - **Enumeration is a read, never a guarded one, and that is a boundary rather than a gap.** A
     guard is a question the cell-touch index answers about *a cell* (§12.4); "the set of Contacts"
     is not a cell. The honest guard would be *"no object of this struct was created or deleted since
     the fork"*, which is the absence-guard problem widened from one cell to a whole buffer, and it
     would make every creation conflict with every enumeration. So there is no `tx_list`, and a
     listing buys exactly what a `get` outside a transaction buys. **What that costs is real**: an
     application that lists, decides, and writes based on what the list contained has no protection
     against a contact having appeared in between. Today the answer is "guard something that is a
     cell" — read the specific objects you acted on. §5 carries the rest.
   - **Ids only, and the N+1 is left visible.** `list` answers pids; a name per contact is a read per
     contact. The alternative — one requested field in the reply — answers exactly one shape of
     question and leaves filters, ordering, joins and aggregates where they were, while making the
     first thing anybody builds on §17.5 something that has to be un-built. It is a finding waiting
     for a query layer and is written down as one rather than pre-empted with a field.
   - **The server allocates under an `AllocatorId` of its own, and that is what the week-one
     allocator design was for.** `(branch, allocator, counter)` (§3.1) exists so that allocating
     authorities need not coordinate; there have been two of them since the first scenario — the
     person typing `Company#1`, which is allocator `0`, and now the store. Separate allocators make
     app-created and hand-typed objects disjoint by construction, so `tx create` is safe against a
     store full of fixtures and neither side has to know the other exists.
   - **The counter is persisted in a sidecar, and the honest reason is that the store cannot answer
     the question cheaply.** `InProcessSequencer::resuming_after` is the pattern this wanted: resume
     from what the store already holds, never restart. It does not transfer, because the log answers
     *"the highest layer"* in one read and there is no equivalent for a PID — a counter is
     `(branch, allocator)`-scoped and therefore spans every struct at once, so deriving it means
     scanning every object buffer, which turns creating `n` objects into `O(n²)`. So it goes beside
     the store with the pause flags and the transaction table, **written before the write it names**
     so that a crash burns an id rather than issuing one twice. The residual: deleting that file
     restarts the counter, which is the one sidecar whose loss a store cannot recover from by being
     told again. Recorded in `CLAUDE.md`.
   - **A generated descriptor now states its own name in its type.** `StructDescriptor<Company,
     "Company">`, which is what lets `list` and `create` answer `Ref<"Company">` rather than `string`
     — so an id that came out of the SDK goes back in as a reference with no cast, and one of the
     wrong struct does not compile. The second parameter defaults to `string`, so a hand-written
     descriptor is unaffected and simply gets less. This is the same brand §4.4 gave reference
     *fields*, finally available on the ids the SDK itself produces.
   - **`create` returns a handle, not a bare id**, because the next thing a caller does is always
     write a field on it. `handle.id` is the id, branded; from `tx.object(S, "#5")` it is whatever
     was passed, uncanonicalised, because a handle makes no round trip.
6. **Sugar** (unscheduled): sync-over-async pipelines, `transact` retry, ownership inference,
   `hasMany` (blocked on aggregations). Nothing in §4 is now unbuilt except the WebSocket listener,
   which §5 has always said lands with the browser client rather than with the transport trait.

### 4.6 An SDK says what its code is, and the two SDKs say different amounts

Both SDKs now put a **`fingerprint`** on every producer they describe: an opaque string whose only
contract is that it changes when the code changes (SPEC §9.2, §17.4). It exists because
`borg repo push` is a diff over *shapes* and a pipeline's body is not a shape — so before this, an
edit to a pipeline invalidated nothing and the store served output from two builds at once
(`examples/personal-crm/FRICTION.md` #17).

**This is the first thing in the SDK surface that is deliberately unequal between the two languages**,
and it is worth being precise about why, because "the two SDKs emit the same describe payload byte for
byte" was until now a clean claim and is now a claim with an exception.

- **TypeScript covers the entry module's bytes.** `process.argv[1]` is the file `borg` executed, and
  that file is hashed. **Imports are not covered.** A pipeline that calls into `./scoring.ts` can be
  rewritten entirely without moving its fingerprint. Node's ESM loader exposes no supported way to
  enumerate a module graph from inside it — there is no `require.cache` for ESM — and building one
  means a resolver or a loader hook, which is a dependency and a build step this SDK does not have.
  Recorded as a hole, not as a coverage claim.
- **Python covers the entry module plus every already-imported module beside it** — anything under
  the entry file's directory, which for a repo is `pipelines/` and is the directory Python puts on
  `sys.path` for a script. `sys.modules` already holds the graph by describe time, so this is a
  filter over a dict and a few file reads.
- **Python stops at the repo, on purpose.** Stdlib, installed packages and the SDK itself are
  excluded. Hashing the environment would move a repo's fingerprint on an unrelated upgrade, and
  every push after every install would recompute every source buffer — a mechanism that cries wolf is
  one people route around. The hole this leaves is a pipeline whose logic lives in an installed
  package.
- **A repo that omits it is not opting out.** `borg repo push` hashes the executable it just ran,
  which is what covers a `jq`-and-`bash` worker. An SDK supplies its own only where it covers *more*
  — which is why TypeScript's answer is, by construction, the same information the fallback would
  have produced. It is computed in the SDK anyway, because that is where growing the coverage will
  happen and because an SDK that is silent about its own code is an SDK nobody can reason about.
- **The two digests are not comparable and need not be.** Python folds several files and their
  relative names into one hash; TypeScript hashes one file's bytes. Nothing anywhere compares one
  producer's fingerprint with another's — the only comparison made is a producer's new fingerprint
  against its own previous one — so agreeing would buy nothing and would have cost Python its module
  graph.

**`describe()` stays pure in both.** The fingerprint is a fact about a file, not about the DSL, so it
is attached by `repo()` — already the impure half in both languages, the one that reads argv and opens
sockets. The pure `describe(structs, pipelines)` that both test suites assert to the byte is
unchanged, which is what keeps those assertions worth making.

Scenario 240's byte-for-byte diff now strips the one field and asserts separately that the two
fingerprints **differ** — two different programs describing themselves identically would mean the
hash was not a hash of anything.

## 5. Open questions carried

- **What guards an enumeration?** `list` is a read outside any transaction (§4.5), because "the set
  of Contacts" is not a cell and a guard is a question about a cell. That is correct and it is not
  free: *list, decide, write* is a real application pattern with no protection against an object
  having appeared in between, and the workaround — guard the specific objects you acted on — does not
  cover the decision that depended on the *set*. Three shapes have been considered and none built: a
  **buffer-versioned guard** (the existence buffer gets a version that any creation or deletion
  moves, and a listing guards that — cheap, and coarse enough that every creation conflicts with
  every enumeration); a **predicate guard** re-evaluated at merge, which is where every serializable
  database ends up and is a scheduler's worth of work; or leaving it, which is the current answer and
  is honest as long as it is written down. This is the same family as the absence guard §12.1 already
  solves for one cell, and the fact that the one-cell case *is* solved is the reason to believe the
  set case is a real design question rather than an oversight.
- **`list` has no ordering contract and no cursor**, deliberately. It sorts by PID so that two
  identical reads answer identically, and that is all it promises — allocation order within one
  allocator, nothing across allocators, and nothing about a field. It also materializes the whole
  answer, like `scan_buffer` beneath it, so a struct with millions of instances is a struct this
  should not be pointed at. Paging is not a parameter that can be bolted on: a cursor over a scan
  that re-runs at head is a different consistency story, and it belongs with the query layer or with
  streaming reads (`CLAUDE.md`, things left undone) rather than in front of them.
- WebSocket server: in `borg-server` from day one or when the browser client lands? (Lean: the
  transport trait supports it from day one; the actual HTTP listener lands with the browser work.)
- **How far does the CLI-over-socket wedge go?** `borg generate` now connects to the socket rather
  than being refused by it (§4.4), which is the first instance of §2.6's eventual shape. It was safe
  because generate only reads. The next candidates are `borg get` and `borg explain`, which are also
  pure reads and would remove most of the annoyance of a served store — but the moment a *write*
  command goes this way, `--tx` / `$BORG_TX` / only-one-open have to mean something over a socket,
  and §4.3 already records that they cannot. So the honest boundary may be "reads connect, writes are
  refused", which is a strange thing to have to explain, or the answer may be that the whole CLI
  becomes a client and the shell affordances become client-side state. Not decided.

  **`repo_push` moved the boundary without settling the question**, and it is worth being precise
  about which way. It is not the CLI connecting: `borg repo push` still opens the store directly and
  is still refused while it is served. It is a *client* asking the server to perform a deployment
  operation — so it says nothing about `--tx`, and the wedge is exactly where it was. What it does
  remove is the strongest argument for widening the wedge quickly, which was that a served store was
  unusable in a dev loop. `borg-server` also gives the refused commands somewhere to point: the
  refusal names the socket and the registry, so what a connecting CLI would need is a connection
  string and not a discovery mechanism.
- ~~Codegen for list/ref fields: how a `Ref` renders in a typed client.~~ **Answered by the build**:
  a branded string, `Ref<"Employee">`, which is the PID at runtime and the target struct to the
  compiler. Not a handle and not the pipeline SDK's `Ref` object — §4.4 has the reasoning. What is
  *still* open is the element half: a list field yields the handle to the list, and addressing
  `Employee[]:l-….[3]` through a generated type waits on the rest of the list story (§18).
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
