# FRICTION.md

Everything that was awkward while building `examples/personal-crm`, written as it was hit. The CRM
is the instrument; this is the reading.

Each entry: **what I was trying to do**, **what happened**, **what I expected**, **severity**.
Severity is about building an application, not about the engine's correctness — several of these are
things `SPEC.md` gets *right* and that are nonetheless expensive to build on. Where CLAUDE.md or
SDK-DRAFT already records the cause, I say so; the value of the entry is then what it costs in
practice, which those documents do not say.

Solutions are proposed only where one is obvious. Most of these are observations, on purpose.

---

## The three that matter most

If nothing else here is read: **#9** (a personal CRM is unusable at 140 contacts, and the reason is
not the N+1 — **since fixed**, and the entry keeps both curves because the shape of the before is the
finding), **#17** (two versions of a pipeline's output coexist in one store, both labelled `current`,
and nothing can tell them apart), and **#6/#7** (a broken producer marks 140 innocent rows broken and
leaves the one guilty row reading `current`).

**#17 is fixed**, and #2 with it — see the *Fixed* notes on each. The rest stand as written.

---

## 1. A pipeline file must be executable, and nothing says so

**Trying to** push the repo for the first time.

**What happened**

```
$ borg repo push examples/personal-crm/repo
error: execution: .../repo/pipelines/display_name.ts: Permission denied (os error 13)
```

**Expected** either for `repo push` to say that pipelines are invoked as programs and this one is
not executable, or for it to not care. `Permission denied (os error 13)` on a file I had just
written, whose permissions I had not thought about, sent me looking at directory ownership and
SELinux before `ls -l scenarios/230-.../pipelines/` showed the `x` bit. The fact that a pipeline is
`exec`'d rather than `node`'d is load-bearing (it is what makes the repo language-neutral) and it is
invisible until this error.

**Severity** low, but it is the *first* error a new author sees, and it is in the ten minutes where
they are deciding whether this is a real system.

`dev.sh` chmods before pushing, which is a workaround I would rather not have written.

---

## 2. `borg repo push` is not idempotent — an unchanged repo emits a new def layer

**Trying to** write a `dev.sh` that can be run twice.

**What happened** pushing the identical directory twice moved the branch's def-version from `L1` to
`L2`. Nothing changed, nothing was reported as changed, and the version moved. (It does *not* redo
any derivation — measured: `landed_at` on an existing `displayName` stayed put across an identical
push, and across a push whose pipeline *body* had changed. That second half is #17.)

**Expected** a push whose describe payload equals the current def-view to be a no-op, or at least to
say `no change`. An event log has no obligation to deduplicate, but `repo push` is a *deploy*
command, and deploys are re-run constantly.

**What it costs** `dev.sh` cannot push unconditionally, because the def-version is the `ClientVersion`
every generated client is pinned to (§5.4) — walking it up on every boot means regenerating and
recompiling the api on every boot, for nothing. So `dev.sh` carries its own change detection: a
`cksum` of `repo/**` in `data/.repo-pushed`. That is a piece of deployment logic Borg pushed into
every consumer, and every consumer will implement it slightly differently.

**Severity** medium. The workaround is eight lines, and everyone will write it.

**Fixed**, as a consequence of #17 rather than on its own account. `repo push` is a diff on both
halves now: a definition the branch already holds emits no event, and a producer whose implementation
fingerprint has not moved emits none either — so an unchanged repo lands no def layer and the command
says `unchanged: 7 definitions already in force, nothing pushed`. `dev.sh`'s `cksum` stamp is deleted.
Worth noting what the stamp was actually doing wrong, since it did work: it hashed file contents,
which is what the fingerprint now does, but compared them against a file beside the store rather than
against what the **store believes**. Keeping those two in step was the script's job — `--reset` had to
remember to delete the stamp, and a push to a second branch would have needed a stamp per branch. The
diff has the other side of the comparison already, and does not.

---

## 3. Pushing a schema means stopping the server, and the stop is not the expensive part

Known and recorded (CLAUDE.md; SDK-DRAFT §4.3, with the reasoning for why a `def_push` message would
be the wrong shape). This entry is only about what it costs downstream, which is more than "stop the
server".

**What happened** a schema or pipeline change is not one command. It is: stop vite, stop the api,
stop `borg serve`, `borg repo push`, start `borg serve`, `borg generate`, start the api, start vite.
`dev.sh` *is* that sequence, which is why the sequence is the largest comment in it.

The api has to be restarted rather than merely reconnected because of #11. Two of the four processes
have to come down for a one-line edit to a pipeline body.

**Expected** — I do not expect `def push` on the socket, for exactly the reason SDK-DRAFT gives. What
I expected was that the *client* side of the bounce would be survivable: that a server going away
and coming back is a reconnect, not a redeploy. See #11.

**Severity** medium. It is livable at four processes and would not be at forty.

---

## 4. A contact that does not exist reads exactly like one whose fields are all empty

**Trying to** answer `GET /api/contacts/:id` for an id that was never allocated.

**What happened** `200 OK`, six fields, every one `{"value": null, "state": "current", "origin":
"derived"}`. There is no error and no signal. A typo in a URL is indistinguishable from a real
contact somebody created and never filled in.

**Expected** something. Either an error naming the object, or a way to ask.

**The way to ask exists and I found it by experiment**: the *entity cell*, `Contact:o-040g2` with no
`.field`, reads `"true"` for a live object and `null` for one that was never created (and
`tombstoned` for a deleted one). That is what `tx.create` writes and what `list` scans. But:

- the client SDK has no `exists()`, and `ObjectHandle` — which is the only thing that knows how to
  build a cell address — exists only *inside* a transaction;
- generated code models fields and never the object, so `Contact` the descriptor cannot address
  `Contact:<id>`;
- the client protocol (§17.5) has no request for it; `get` on the entity cell works only because a
  cell address happens to parse that way.

`server.ts` therefore does `branch.get(\`${Contact.name}:${id}\`)` and compares to the string
`"true"`, which is three layers of implementation detail in one line of an application.

**Severity** medium. Every application has this question on its first detail route.

---

## 5. An absent value on a declared *source* field reads `origin: derived`

**Trying to** render the provenance box for a contact that turned out not to exist (#4).

**What happened** `Contact:o-040ga.firstName` — `firstName` is `String`, source, no producer — came
back `origin: "derived"`, `state: "current"`, `fresh_as_of: "L0"`. The UI printed "origin: derived"
next to a field no producer can write.

It is not confined to non-existent objects. A contact that exists and simply never had a phone
number answers the same thing:

```
phone: {"value":null,"state":"current","origin":"derived","by":null,"freshAsOf":"L0","landedAt":"L0"}
```

So *any* absent value claims to have been derived. On a struct with six source fields and one derived
one, five of the six ordinary "this is empty" answers are mislabelled.

**Expected** `origin: source`, or an origin that says "nothing is here". CLAUDE.md records the
neighbouring case (*"Reading a cell of a struct nobody declared answers an absent envelope rather
than an error, and its `origin` reads `derived`"*) and says it is deliberate and asserted. This is
the same answer for a **declared struct's declared source field**, which reads less like a corner
and more like a default that was never chosen.

`origin` is a provenance label, and provenance is the thing this system asks to be trusted about.

**Severity** low in consequence, higher in kind.

---

## 6. A producer that fails on an object's *first* run is invisible — and it is the guilty object

Cause recorded in CLAUDE.md (*"A producer that has never succeeded has no cell to call `broken`"*).
What follows is what that looks like from the browser, because the shape is worse than the sentence.

**Trying to** see how the app behaves when the pipeline throws. I added a line to `display_name.ts`
that throws for one contact, pushed, and created that contact.

**What happened**

| contact | `displayName.value` | `state` | badge in the UI |
|---|---|---|---|
| the 140 that derived fine before | their old name | `broken` | **BROKEN** |
| the one whose invocation threw | `null` | `current` | none |

The only contact whose display name is genuinely unobtainable is the only one the UI shows as fine.
It renders as `(not derived yet)`, which is also the correct rendering for a contact created two
milliseconds ago. There is no timeout after which "not derived yet" becomes suspicious, and nothing
a client can ask.

**Expected** that a value which cannot be produced is labelled, which is invariant 8. It is labelled
everywhere a previous value exists; the first run is the hole, and the first run is exactly when a
new pipeline is wrong.

**Severity** high for an application. This is the one place the system's central promise — *derived
data is never presented as fresh* — has a blind spot, and it is aimed at the newest data.

---

## 7. One bad entity marks every entity's copy of that field `broken`

**What happened** in the same experiment: 140 contacts that had nothing to do with the failure went
`state: broken`. §14 poisons per *producer*, so one malformed row takes the column offline for
everybody, and every one of them keeps serving its last-known value under a red badge.

**Expected** — this is correct per spec and I am not asking for it to change. Recording what it costs:
a personal CRM with one contact whose name breaks the pipeline shows 140 red badges, and the
information "which one is actually broken" is not in any of them. Compare #6: the guilty row is the
unbadged one.

Recovery is at least automatic and complete: pushing the fixed pipeline re-ran every invocation and
all 141 came back `current`, the guilty one included, with `landed_at` moved from `L11` to `L1547`.
No `--retry-broken` needed — which is fortunate, because that flag is CLI-only and the CLI is
refused while the store is served (#3). Note *why* it re-ran: because the invocations were **broken**,
not because the code changed. A push whose producer is healthy re-runs nothing at all, which is #17.

**Severity** medium — accepted design, expensive presentation.

---

## 8. Nothing a client can reach says a producer is broken

**Trying to** show "the display_name pipeline is failing" somewhere in the app.

**What happened** the only notice was one line on the stderr of the process that committed — i.e.
`borg serve`'s log:

```
warning: display_name is now broken: producer P3252117683130161757 failed: BOOM: …
```

No client sees that. §17.5 has no `frontier`, no `derive status`, no `producer list`. `borg derive
status` and `borg producer list` exist and are refused while the store is served. The only
client-visible signal is a cell that *already had a value* flipping to `broken`, plus `explain` on
that cell for the reason — which the api does do (`GET /contacts/:id` calls `explain` when a field
reads `broken`), and which is the only sentence the UI can show. For a producer that has never succeeded (#6), even that is unavailable.

**Expected** a way to ask "is anything wrong" that does not require already knowing which cell to
look at.

**Severity** medium-high. It is the difference between an app that can show a banner and one that
cannot.

---

## 9. The read path is O(log) per read, so the app is O(n²) — and the N+1 is not the reason

This is the big one, and it is a measurement rather than an opinion.

**Trying to** draw the list view.

**What I built** the N+1 the design intends to be visible: `list` answers ids, so the list view is
`1 + 2n` reads (`displayName` and `email` per contact). SDK-DRAFT §4.5 says this is left visible
rather than papered over with a field in the reply, and I agree with that. It is not what hurts.

**What happened** — one process, one socket, localhost, `target/debug`:

| contacts | branch head | `GET /contacts` | `GET /contacts/:id` | ms per read (list) | ms per read (detail) |
|---:|---:|---:|---:|---:|---:|
| 45 | L441 | 1 675 ms | 129 ms | 18.4 | 18.4 |
| 60 | L591 | 2 924 ms | 169 ms | 24.2 | 24.1 |
| 80 | L791 | 4 853 ms | 215 ms | 30.0 | 30.7 |
| 100 | L991 | 7 966 ms | 271 ms | 39.4 | 38.7 |
| 140 | L1391 | 14 925 ms | 365 ms | 53.0 | 52.1 |

Read the last two columns. **The cost of one read is identical on both routes and tracks the branch
head, not the size of the request** — ≈ 0.038 ms × head, on both a 281-read list and a 7-read detail.
The number of contacts in the request is irrelevant; the length of the whole log is everything.

So: each contact costs ~10 layers, each read costs O(log), each list is O(n) reads → **the list view
is O(n²)**. 140 contacts takes fifteen seconds. A thousand contacts extrapolates to about 380 ms per
read and roughly twelve minutes to draw the list. A personal CRM has a thousand contacts.

**The cause is documented and its two halves are documented separately.** CLAUDE.md says
`borg serve` "opens the store per request", and separately that "SQLite `Registry::open` rebuilds
indexes by replaying the log — `O(log)` per CLI invocation". Nothing says what they multiply to,
because the process-per-command CLI paid it once per command and the fan-out benchmark drives the
engine rather than the server. The first application is where they meet: a server is where "per
invocation" becomes "per read".

**Expected** a read to cost something related to the cell being read.

**Severity** blocking for real use. This is not "Borg is slow"; the engine is not what is slow. It is
that the serving lifecycle turns an `O(log)` open into a per-read tax, and CLAUDE.md already names
the fix as a real change ("Making the server hold one [registry] is a change to derivation's
lifecycle — the same change that turns the post-write `catch_up` call into a signal (§9.6) — and
should be made there rather than worked around here"). This is the evidence that it should be made.

Secondary and much smaller: a `POST /contacts` measured 315–760 ms, of which the transaction is a
handful of messages and the rest is the same tax plus the derivation the write pays for (§9.6).

### Fixed — `borg serve` holds one registry

The change was the one CLAUDE.md named, made where it named it. `Registry::open` no longer means
"replay the log"; it means **bring the log's projections to head** (`borg_engine::projection`), and a
projection a process has been maintaining is already there. `borg serve` therefore opens the store
once and every request shares that registry. The executor moved onto it too, which is what removed
the drop-and-reopen dance `tx_commit` needed and was the actual reason a server could not hold a
store open. Requests are still serialised through the same store-wide gate: the replay was the cost,
not the gate.

`./bench.sh` is the measurement, so the table below is reproducible rather than remembered. Same
machine, same `target/debug`, one process, one socket, localhost — the row above and the row below
differ only in the binary:

| contacts | branch head | `POST /contacts` | `GET /contacts` | `GET /contacts/:id` | ms per read (list) | ms per read (detail) |
|---:|---:|---:|---:|---:|---:|---:|
| 45 | L451 | 378 ms | 27 ms | 3 ms | 0.3 | 0.4 |
| 60 | L601 | 375 ms | 35 ms | 3 ms | 0.3 | 0.4 |
| 80 | L801 | 372 ms | 56 ms | 3 ms | 0.3 | 0.4 |
| 100 | L1001 | 371 ms | 70 ms | 3 ms | 0.3 | 0.4 |
| 140 | L1401 | 369 ms | 160 ms | 5 ms | 0.6 | 0.7 |
| 200 | L2001 | 384 ms | 141 ms | 3 ms | 0.4 | 0.4 |
| 300 | L3001 | 374 ms | 295 ms | 4 ms | 0.5 | 0.6 |
| 400 | L4001 | 364 ms | 328 ms | 3 ms | 0.4 | 0.4 |

**Read the last two columns again.** They no longer move: 0.3–0.6 ms per read from L451 to L4001,
where before they went 18.4 → 53.0 ms over a third of that range. **The cost of a read is now a fact
about the read.** The list view is `O(n)` — 801 reads in 328 ms — which is the N+1 the design intends
to be visible and nothing else; 140 contacts went from 14.9 s to 0.16 s, and a thousand contacts is
now under a second rather than the extrapolated twelve minutes.

The write path lost the same tax: `POST /contacts` was 636 ms at 45 contacts rising to 891 ms at 140
on the same machine, and is now flat at ~370 ms. What remains is the transaction's round trips plus
the derivation the write pays for (§9.6) — neither of which grows with the log. Turning that
`catch_up` into a signal is still deferred, and is what the remaining 370 ms is mostly made of.

**What is unchanged, on purpose.** The CLI still rebuilds per command: process-per-command has
nothing to keep, and that is honest rather than lazy. Requests are still one at a time. And a
`--reset` while a server is running still deletes the advisory lock out from under it, which is how
this measurement was first taken twice with two servers on one store — a footgun in `dev.sh`, not in
the engine.

---

## 10. Generated types do not reach the read path

**Trying to** read a contact's fields for a GET — i.e. outside any transaction, because a GET has
nothing to commit and forking a branch per page view would be absurd.

**What happened** the typed surface is `tx.object(Struct, id)`, and it exists **only inside a
transaction**. On a branch there is only:

```ts
branch.get(cell: string, options?: { as?: FieldType<T> }): Promise<Resolved<T | null>>
```

so an application assembles `"Contact:" + id + "." + field` by hand and supplies the conversion
itself. Every compile-time protection scenario 260 demonstrates — a typo'd field name is a compile
error, a value at the wrong type is a compile error — is gone on the path a CRUD app spends most of
its life on. A typo here is an absent envelope at runtime, which is 260's own description of the
failure codegen exists to prevent.

It is partially recoverable, and `server.ts` does recover it, because `StructDescriptor.fields` is
public:

```ts
function read<K extends keyof Contact & string>(branch, id, field: K) {
  return branch.get(`${Contact.name}:${id}.${field}`, { as: Contact.fields[field].type });
}
```

That is five lines every application will write, reaching into the descriptor for two facts the SDK
already knows. The obvious shape — `branch.object(Contact, id)`, a read-only handle with `get` and
`resolve` and no `set` — needs no protocol change at all: it is the same `get` message.

**Expected** the typed handle to be a property of *the struct and the id*, with the transaction being
what adds writes and guards.

**Severity** medium-high. Not blocking, and the least defensible of the entries here, because
nothing in the design requires it.

---

## 11. The SDK never reconnects, and there is no way to notice it has not

**Trying to** keep the api up across a `borg serve` restart — which #3 makes routine.

**What happened** `borg serve` was killed and restarted. Every subsequent request through the
existing `BorgContext` threw:

```
BorgProtocolError: the server hung up in the middle of a request
```

forever. Not once — permanently. The api had to be restarted. `createBorgContext` connects during
construction (deliberately: "an error at construction is better than one at first use"), and there
is no `reconnect()`, no `isOpen`, no close event, and no way for a long-lived process to tell "the
server is down" from "this context is finished". The only recovery is to build a new context, which
means the application has to own connection lifecycle that it has no signal to drive.

The api handles it as well as it can: `BorgProtocolError` becomes a `503` with the sentence
"`borg serve` is not answering — is it still running?", which the UI renders as a red box. That is
honest and it is still a restart.

**Expected** at minimum an event or a flag; ideally that `BorgContext` reconnect on demand, given
that §12.2 already makes transactions survive a dropped socket precisely so that a reconnect is
meaningful. The reconnect story is *designed for* and not *implemented*.

**Severity** medium-high for a server-side client. For the browser client SDK-DRAFT §2.5 anticipates,
it will be blocking — a laptop lid closing is this.

### Fixed — the SDK reconnects, and the api no longer owns a lifecycle

A `BorgContext` is an address now rather than a socket. A failed send or read tears the connection
down and the next operation dials again and repeats the handshake; a socket the peer has already
closed is dropped *before* the next request is written, so an ordinary `borg-server` bounce costs an
idle client nothing at all. **Nothing is retried**, and that half is deliberate: `tx_commit` is not
idempotent, so an operation that was in flight fails with `BorgDisconnectedError` — a class of its
own, saying in words that it was not retried and that its outcome is unknown. Transactions survive
by construction, which is what §12.2 was always for: one begun before a bounce commits after it.

The api is where the difference shows. It connects `on-demand`, so it starts *before* its server
without dying; with nothing listening it answers `503` with `no borg server at <addr> — start one
with: borg-server start`, and the moment a server appears the next request works. No restart, no
`reconnect()` call, no flag for the application to drive. `./smoke.sh` asserts exactly that
sequence, and `scenarios/310-connection-urls` asserts the harder version of it — a client busy right
through the outage, which is the one that discovers the outage by failing.

What this entry asked for was "at minimum an event or a flag; ideally that `BorgContext` reconnect on
demand". There is a `bc.connected` flag, and the reconnect is the ideal one.

---

## 12. The app's only write cannot conflict, so `ConflictError` is written blind

**Trying to** make a `ConflictError` render as something a human understands, as asked.

**What happened** v1 creates, lists and views. `POST /contacts` is `begin → create → set × 5 →
commit`, and it **reads nothing**, so it has no guards and cannot be rejected — scenario 280 asserts
exactly this ("creation is the one write two clients can always both do"). The error path is
therefore unreachable from the app's own routes.

I did not want to ship an untested handler, so I drove a conflict through the same socket the api
uses, with two transactions reading and writing one contact's `notes`:

```
first committed at L1394
ConflictError: { "cell": "Contact:o-040g2.notes", "reason": "guard",
                 "message": "guard on Contact:o-040g2.notes no longer holds against the parent" }
```

That is the shape `server.ts` maps to a `409` and the UI renders as *"Two writes raced. Borg rejected
this one whole rather than letting half of it land — nothing you typed was saved."* Verified against
the real engine, unreachable through the real app.

**Expected** nothing different — this is a consequence of v1's scope, and the first `PATCH
/contacts/:id` brings read-then-write and with it the guard. Recorded because "we handled
ConflictError" would otherwise be a claim nobody had tested.

**Severity** none. Noted so the claim is not overstated.

---

## 13. A listing is not a snapshot, and cannot be made one

**Trying to** understand what the list view actually shows.

**What happened** `GET /contacts` at 140 contacts is 281 independent reads spread over fifteen
seconds. They are not a snapshot: a contact created halfway through can appear with a `displayName`
from after the write and an `email` from before it. `branch.get` takes `settled: true` for "a
coherent snapshot slightly in the past" (§10.5) — but `list` takes **no options at all**, only a
branch. So the *ids* can only be read at the ragged head even when the fields are read settled, and
there is deliberately no `tx_list` (SDK-DRAFT §4.5 gives the reasoning, which I find convincing).

**Expected** `list` to at least accept the same `ReadOptions` `get` does, so that "everything settled"
is expressible end to end.

**Severity** low-medium today, and it is the visible edge of the open question SDK-DRAFT §5 carries
about what guards an enumeration.

---

## 14. `list` orders by PID, and a CRM wants alphabetical

**What happened** contacts come back in allocation order. Sorting by name means fetching every
contact's name first — the full `O(n²)` of #9 — and then sorting in the api. There is no ordering,
no filter, no `limit`, and no cursor, all deliberately (SDK-DRAFT §5, CLAUDE.md).

**Expected** nothing yet; the query layer is out of v1 by decision.

**Severity** low while #9 is unfixed, because sorting is not what makes the list slow. It becomes the
next thing the moment #9 is fixed.

---

## 15. A generated module cannot be committed, so a fresh clone cannot typecheck the api

**Trying to** decide whether `api/gen/borg.generated.ts` belongs in git.

**What happened** it bakes in `CLIENT_VERSION` — the def-layer of the store it was generated from.
On a fresh store that is `L1`; on the store I had been re-pushing to it was `L1405`, a number in the
same sequence as data layers, so it reads like a large arbitrary integer rather than a schema
version. Committing it commits one machine's layer id as if it were source.

So `api/gen/` is gitignored, and the cost is that a fresh clone cannot typecheck or run the api until
`dev.sh` has created a store and pushed to it. The artifact whose purpose is compile-time safety is
the one artifact the repository cannot hold.

**Expected** no strong prior. Naming the tension: codegen output is per-deployment, and the
type-checking it buys is per-repository.

Related, and smaller: `CLIENT_VERSION` being a `LayerId` in the *same counter as data layers* means
the number carries no information a human can use. "Generated at L1405" says nothing about which
schema that is; "generated at L1" and "generated at L1405" may be the identical schema.

**Severity** low-medium.

---

## 16. Engine error text reaches the browser

**What happened** a mistyped id in a URL produced, via the SDK, via the api:

```
cannot parse cell from `Contact:o-99999.firstName`: id has non-zero padding bits
```

`BorgClientError` carries the server's sentence and nothing structured — no code, no field — so an
api can either forward the sentence or replace it with something vaguer. "Non-zero padding bits" is a
fact about base32 that an application cannot translate on the user's behalf.

**Expected** rejections to carry a machine-readable kind alongside the prose, the way `ConflictError`
carries `reason` and `cell`. `ConflictError` is the model here: it is the only Borg error an
application can *act* on, and the reason is that it has fields.

**Severity** low.

---

## 17. Changing a pipeline's code invalidates nothing, so one store holds two versions of a derived field — both labelled `current`

**Trying to** change what `display_name` produces, which is the second thing anybody does to a
pipeline.

**What happened** — measured on a scratch store, one contact per step:

1. contact A exists; its `displayName` derived under a body whose fallback text is `(no name)`;
2. edit the body's fallback to `(nobody)`; `borg repo push`. A's `displayName` **does not move**:
   `landed_at` stays where it was, `state` stays `current`. Only `fresh_as_of` advances, because a
   new def layer landed and was incorporated;
3. create contact B, with no name. B derives under the new body: `(nobody)`;
4. revert the body to `(no name)`; push again. B **keeps** `(nobody)`;
5. create contact C, with no name: `(no name)`.

So the store now holds:

| contact | `displayName` | `state` | produced by |
|---|---|---|---|
| B | `(nobody)` | `current` | the reverted-away body |
| C | `(no name)` | `current` | the current body |

Same struct, same field, same producer id, two different programs, and **nothing in the envelope
distinguishes them**. `state`, `origin` and `by` are identical. `fresh_as_of` says only that both are
caught up with respect to their *inputs*.

**Expected** either invalidation on a producer's code changing, or a label. §10.4's whole premise is
that derived data carries how much it can be trusted, and §5.3/§5.4 do this rigorously for *schema*:
a value is stored at the def-version of its own field, and readers migrate. There is no equivalent
for producer *code*. `borg derive --rebuild` exists and does the right thing — and it is CLI-only, so
it is refused for as long as the store is served (#3).

**Why it is worse than it sounds** the whole point of a derived field is that you do not have to
think about it. The moment its definition changes, an application has a store in which the same
question has two answers, and the only way to know which contacts are on which side is to remember
what you deployed when. During development the body changes constantly; every one of those edits
leaves sediment.

I am not sure invalidating is right — recomputing every invocation on every code push is `O(all
entities)` per edit, which is its own problem — but *silently* not invalidating, while labelling the
result `current`, is the one outcome that contradicts what the envelope is for.

**Severity** high. It is the same class as invariant 8 and #6: derived data presented as fresh when
something about its derivation has moved.

**Fixed.** `describe` now carries an **implementation fingerprint** per producer — an opaque string
whose only contract is that it changes when the code changes — and it travels in the `PushProducer`
def event, because *which program this is* belongs to a producer's definition. A changed fingerprint
therefore re-emits `PushProducer`, which lands a def layer, which moves the producer's ClientVersion,
and a producer standing below its own ClientVersion is handed its whole source buffer as work. The
measurement above now runs the other way: at step 2 A's `displayName` moves, at step 4 B's goes back
with it, and B and C agree at every point. `scenarios/290-a-code-change-invalidates` is that sequence,
asserted.

Three things about the shape of the fix are worth recording. The entry above raised the second of them
and was right to:

- **Nothing in the invalidation machinery was broken.** Poisonings already expired against a
  producer's ClientVersion, and `backfill` already existed for a producer newer than its data. The
  gap was that the *diff* — a repo emits the shape it believes in, `repo push` compares — had no
  surface on which a code edit was visible. It was deaf, not broken. Adding one field to the thing
  being compared was the whole of it; the scheduler learned nothing new, and deliberately still does
  not know what a fingerprint is.
- **The `O(all entities)` worry was the right worry, and it is bounded by the diff.** Recomputing on
  every push would be unusable, which is why the same change makes `repo push` idempotent (#2): the
  cost is paid per *edit*, not per push. It is still `O(that producer's source buffer)` per edit, and
  that is the honest price of the guarantee — a CRM with a million contacts pays for a fallback
  string. Narrowing it further means per-invocation fingerprints of what each *read*, which is a
  different mechanism.
- **A code change is deliberately not a schema change.** No `MutateField`, no migration demanded, no
  field def-version moved: the values are overwritten in place at the version they already had (§5.3
  is untouched). A migration would be the wrong model — it bridges two shapes that coexist, and here
  the old output is simply wrong.

What each fingerprint covers is stated where it is computed, and the limits are real: the TypeScript
SDK hashes the entry module's bytes only, because ESM exposes no way to walk a module graph from
inside — a pipeline whose logic lives in an imported file can change without moving its fingerprint.
The Python SDK covers the entry module plus every already-imported module beside it, and not the
environment. Shell workers get the CLI's fallback, which hashes the command file. A producer that
cannot be fingerprinted at all keeps the old behaviour, documented as such rather than silently.

One migration effect, once: a store that predates this has no fingerprints, so the first push after
it lands reads absent → present as changed and recomputes. The CLI says
`implementation changed (first fingerprint), recomputing N producers` so that it is not mistaken for
a bug.

One unlooked-for consequence, which is an improvement and is now what `scenarios/210` asserts:
**a bad deploy is discovered by the push that deployed it.** Pushing a pipeline that throws used to
commit no data and therefore run nothing, so the failure surfaced on whichever write came next and
was reported to whoever made it. Now the push runs the new build over the existing buffer, and the
person who deployed it is the person who is told. Same for #6's blast radius — nothing about that
changed — but the timing of the report is much better.

---

## 18. Two small ones, for the record

- **`borg.toml`'s `[[pipelines]]` block is decorative.** `repo push` walks `pipelines/` and asks
  every file; nothing reads the list. Already recorded in SDK-DRAFT §4.2. I wrote it anyway because
  scenario 230 does, which is how decorative configuration propagates.
- **`dev.sh` needed the SDK linked by hand** into `repo/` and `api/`, because `borg-sdk` is
  unpublished and the generated module imports `borg-sdk/client` by bare name. Every scenario does
  this (`ts-lib.sh`'s `link_sdk`) and so does `dev.sh`. Fine for a repo-local example; the first
  external user has no `link_sdk`.

---

## What went right, since a friction log that lists only friction is not evidence

Recorded because the absence of an entry is otherwise unreadable.

- **Ownership is enforced three times and I never had to think about it once.** `displayName` is
  `readonly` in the generated interface, so `contact.set("displayName", …)` is a compile error; the
  SDK refuses it before the wire; the engine refuses it after. The UI has no display-name box because
  it *could not* have one.
- **Field-granular invalidation needed no client code at all.** The pipeline body reads `firstName`,
  `lastName` and sometimes `email`, and never `phone` or `notes`. Nothing declares that anywhere. The
  engine watched what crossed the socket.
- **Derivation had already run by the time the commit returned.** Every contact created through the
  form arrived at the detail page with `displayName` already `current`. §9.6 says the write pays for
  the derivation it causes, and for a form submit that is exactly the right place to pay.
- **`borg generate` reading through the socket is the difference between a workflow and a chore.**
  `dev.sh` starts the server and then generates, with no coordination, and the message says which
  way it read.
- **Recovery from a broken producer was automatic and total** (#7).
- **`explain` on a broken cell gave a sentence I could put on screen unedited.**
