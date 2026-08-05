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
   the machinery is ergonomics, not contract, and must not leak into the protocol.

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

1. **TS pipeline SDK + worker socket transport** — `packages/borg-sdk` (minimal tooling; nx when a
   second TS package exists), socket-per-env-var transport in `borg-exec-process` so `console.log`
   cannot corrupt the stream. **Scenario 230**: the investing pipeline authored in TS, pushed via
   `borg repo push`, derived, field-granular invalidation asserted — the TS twin of scenario 030,
   plus a pipeline that deliberately `console.log`s mid-invocation and does no harm.
2. **Python pipeline SDK** — the neutrality gate. **Scenario 240**: the same pipeline in Python
   against the same store; a mixed repo (one TS pipeline, one Python) in one push.
3. **`borg serve` + TS client SDK** — client protocol module in `borg-protocol` (tx ops, resolve
   envelope, describe/def push), unix-socket server, per-connection transaction binding.
   **Scenario 250**: two concurrent SDK clients, S2's conflict through the socket; a client killed
   mid-transaction reaped by the idle timeout.
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
- Whether `world` (pipeline random access) takes generated types or stays stringly in v1.
