/**
 * # borg-sdk/client
 *
 * The consumer-side SDK: read and write data through transactions, over `borg-server`'s socket.
 *
 * ```ts
 * import { Company, createBorgContext } from "./borg.generated.js";
 *
 * // One string says where the server is and which registry on it (§17.7). `$BORG_URL` if omitted.
 * const bc = await createBorgContext({ url: "borg://localhost/personal-crm" });
 * const tx = await bc.branch("main").begin();
 * const c = tx.object(Company, "o-100");   // a handle; no I/O yet
 * const hc = await c.get("headcount");     // read → recorded → guarded at commit
 * await c.set("headcount", (hc ?? 0) + 1);
 * await tx.commit();                       // throws ConflictError on a tripped guard
 * ```
 *
 * This is the *other* artifact from the DSL in `borg-sdk`. Same struct name, opposite direction: the
 * DSL's `Company` is the source of definitions, this one is generated *from* them and carries the
 * ClientVersion it was generated at (SDK-DRAFT §1).
 *
 * ## Three things this SDK deliberately does not do
 *
 * **It caches nothing.** Every `get` and every `set` is a wire message and the engine records the
 * read-set server-side, which is what makes a transaction's guards cell-granular without a line of
 * client code (§12.1). Preloading an object would collapse the guard to object granularity, which is
 * exactly the protection S5 exists to give.
 *
 * **It retries nothing.** [`ConflictError`] is contract (SDK-DRAFT §3): a rejected commit names the
 * cell that moved, and deciding what to do about that is the application's. The auto-retrying
 * `bc.transact(fn)` wrapper is later sugar and is deliberately not here, because a retry loop that
 * ships before anyone has written the non-looping version decides the policy for everybody.
 *
 * **It holds no transaction state.** A transaction is an id. It lives beside the store, not in this
 * connection (§12.2), so [`BorgContext.transaction`] can pick one up after a reconnect and one
 * nobody comes back for is the idle reaper's problem (§12.3).
 *
 * ## …and one thing it now does: reconnect
 *
 * A `BorgContext` outlives its socket. A failed send or read tears the connection down and the
 * *next* operation dials again and re-handshakes; operations that were in flight when it broke fail
 * with [`BorgDisconnectedError`] and are **never** retried. See that class for why the retry is the
 * application's decision and not this library's.
 *
 * Transactions survive a reconnection by construction rather than by machinery: a transaction is an
 * id beside the store (§12.2), so one begun before a server bounce commits after it — the handle
 * outlives the socket that produced it, and [`BorgContext.transaction`] is how a process that lost
 * its context picks one up.
 */

import {
  addressText,
  borgAddress,
  dialBorgServer,
  dialBorgWebSocket,
  parseBorgUrl,
  redactBorgUrl,
  TOKEN_ENV,
  URL_ENV,
  type BorgAddress,
  type BorgUrl,
} from "./connection.js";
import {
  BorgProtocolError,
  LineStream,
  WebSocketStream,
  type MessageStream,
} from "./lines.js";
import {
  CLIENT_PROTOCOL_VERSION,
  type BranchInfo,
  type ClientHello,
  type HelloAck,
  type Request,
  type Response,
  type ServerHello,
  type WireEnvelope,
  type WireLineage,
  type WireSchemaDef,
} from "./client-protocol.js";
import { TOMBSTONE, type AnyFieldType, type FieldType, type RefText } from "./values.js";

// The conversions generated code needs, from the one table this package has. `ref()` and `list()`
// are **not** here: they are the pipeline side's carrier for a reference, and a client's is
// `refText()` — two carriers for one wire form, exported one apiece so that no file has both in
// scope and has to remember which it meant.
export {
  bigint,
  binary,
  bool,
  double,
  int,
  refText,
  string,
  untyped,
  BorgValueError,
  type FieldType,
  type ValueOf,
} from "./values.js";
// Exported as `Ref` because that is what it is called in generated code, where `Ref<"Employee">`
// has to read as one word. See `RefText` in ./values.ts for why a client gets a branded string.
export type { RefText as Ref } from "./values.js";
export { BorgProtocolError } from "./lines.js";
export {
  BorgUnreachableError,
  BorgUrlError,
  parseBorgUrl,
  redactBorgUrl,
  wellKnownSocket,
  type BorgAddress,
  type BorgUrl,
} from "./connection.js";
export type {
  BranchInfo,
  WireEnvelope,
  WireFieldDef,
  WireLineage,
  WireSchemaDef,
  WireStructDef,
} from "./client-protocol.js";

// --- Errors ---------------------------------------------------------------------------------------

/** Anything the server refused that is not a conflict: a bad cell, a rejected write, an expired tx. */
export class BorgClientError extends Error {
  override readonly name = "BorgClientError";
}

/**
 * **The connection broke while this operation was in flight, and it was not retried.**
 * `examples/personal-crm/FRICTION.md` #11.
 *
 * The context is *not* finished: it drops the dead socket and the next operation dials again and
 * re-handshakes. What it will not do is redo this one, and that is a decision rather than an
 * omission — **a retried `tx_commit` can apply twice.** A commit that reached the server and whose
 * answer was lost on the way back is indistinguishable, from here, from one that never arrived; the
 * first is already merged and re-sending it either merges a second layer or fails against a
 * transaction that no longer exists. The same is true of `tx_create`, which allocates. So the
 * outcome of *this* operation is **unknown** and saying so is the only honest thing available; what
 * to do about it needs the application's knowledge of what it was doing, which an SDK does not
 * have. A reader can find out: a transaction is durable (§12.2), so `bc.transaction(id)` and a read
 * answer whether the write landed.
 *
 * It is a [`BorgProtocolError`], so code that already treated "the socket went away" as one kind of
 * thing keeps working; the subclass is what lets code that cares tell it from a malformed message.
 */
export class BorgDisconnectedError extends BorgProtocolError {
  override readonly name = "BorgDisconnectedError";
  /** The address the connection was to. */
  readonly address: string;

  constructor(address: string, why: string) {
    super(
      `${why} (${address}) — this operation was not retried, so whether it took effect is ` +
        `unknown; the next one reconnects`,
    );
    this.address = address;
  }
}

/**
 * A commit the engine rejected whole (§13). **Contract, not an implementation detail.**
 *
 * The cell is the whole point. "Your commit failed" is not something an application can act on;
 * "the cell you read moved, and here it is" is the input to deciding whether to re-read and retry,
 * whether to show the user a diff, or whether to give up.
 *
 * The transaction is **still open** when this is thrown. Its read-set is what a client needs in
 * order to decide, and throwing it away at the rejection would leave the caller holding an error and
 * nothing else. Call [`Tx.abort`] when the decision is "give up".
 */
export class ConflictError extends Error {
  override readonly name = "ConflictError";
  /** The guard cell whose re-evaluation against the parent failed. `null` only for `def_diverged`. */
  readonly cell: string | null;
  /** `guard` | `def_diverged` | `dangling_write`. */
  readonly reason: string;

  constructor(cell: string | null, reason: string, message: string) {
    super(message);
    this.cell = cell;
    this.reason = reason;
  }
}

/**
 * A read whose value cannot honestly be produced at this client's ClientVersion (§9.3, §14).
 *
 * Thrown by [`ObjectHandle.get`] and never by [`ObjectHandle.resolve`], and that split is the whole
 * of how the value-shaped shortcut keeps invariant 8. A `stale` or `unvalidated` value is a real
 * value that is merely behind, and returning it is what the freshness mode asked for. A `broken` one
 * is *absent with a reason* — a value written past a def change with no `down` migration to reach
 * this version, or a producer that failed — and returning `null` for it would collapse "nothing was
 * ever written here" into "there is something here you cannot see". §9.3 is explicit that the system
 * must not serve something plausible; neither may an SDK.
 */
export class BorgStateError extends Error {
  override readonly name = "BorgStateError";
  readonly envelope: Resolved<null>;

  constructor(envelope: Resolved<null>) {
    super(
      `${envelope.cell} reads as broken: no value is reachable here, so there is none to return — ` +
        `resolve() returns the envelope, and explain() says why (§9.3, §14)`,
    );
    this.envelope = envelope;
  }
}

// --- What a read carries --------------------------------------------------------------------------

/**
 * The §10.4 provenance envelope, with the value decoded to the field's type.
 *
 * Field names are the wire's, unconverted — see `client-protocol.ts`. `fresh_as_of` here is the same
 * text `borg get` prints under `fresh as of`, which makes a shell and an SDK comparable without a
 * reformatting step in between.
 */
export interface Resolved<T> {
  /** The cell, canonicalised by the server. You may have asked with `Company#1`. */
  readonly cell: string;
  readonly value: T;
  readonly origin: "source" | "derived";
  readonly state: "current" | "unvalidated" | "stale" | "broken" | "tombstoned";
  /** Which write this is. Absent when nothing is stored here. */
  readonly event: string | null;
  readonly authored_at: string;
  readonly landed_at: string;
  readonly fresh_as_of: string;
  /** The producer that wrote it, for derived data. */
  readonly by: string | null;
}

/** Where a value came from. §11. */
export interface Lineage {
  readonly cell: string;
  readonly produced_by: string | null;
  readonly authored_at: string;
  readonly landed_at: string;
  readonly fresh_as_of: string;
  /** Why this value stopped moving, when its producer is poisoned (§14). */
  readonly broken: string | null;
  readonly from: readonly { cell: string; origin: string; landed_at: string }[];
}

/** What a read is willing to pay for. §10.5. */
export type Freshness = "any" | "validated" | "current";

export interface ReadOptions {
  freshness?: Freshness;
  /** Read at the settled frontier rather than at the ragged head (§10.5). */
  settled?: boolean;
}

// --- Structs ---------------------------------------------------------------------------------------

/** One field, as generated code describes it at runtime. */
export interface FieldDescriptor<T> {
  /** The conversion, from the one table this package has (`./values.ts`). */
  readonly type: FieldType<T>;
  /** Whether a producer owns this field. Clients may not write it (§8). */
  readonly derived: boolean;
  /** The def-version of *this field* (§5.3). */
  readonly version: string;
}

/**
 * A struct, as generated code describes it at runtime: the name the wire uses, and how each field's
 * values convert.
 *
 * The first type parameter is the *shape* — the interface generated beside it — and the mapped
 * `fields` type is what makes a generator bug into a compile error in the generated file: a field in
 * the interface with no descriptor, or a descriptor whose conversion produces the wrong type, will
 * not assemble.
 *
 * **The second is the struct's own name, as a literal type**, and it exists so that ids can be
 * branded on the way *out* as well as on the way in. A reference field already has type
 * `Ref<"Employee">`; without the name in the descriptor's type, everything that answers with an id —
 * [`BranchHandle.list`], [`ObjectHandle.id`] — could only answer `string`, and storing a listed
 * contact in a contact-shaped reference field would need a cast at every call site. Generated code
 * supplies it (`StructDescriptor<Company, "Company">`); hand-written descriptors may leave it out
 * and get `Ref<string>`, which is the honest answer when nothing stated the name in a type.
 */
export interface StructDescriptor<S, N extends string = string> {
  readonly name: N;
  readonly fields: { readonly [K in keyof S]-?: FieldDescriptor<NonNullable<S[K]>> };
}

/**
 * Assemble a struct descriptor. Generated code's only helper.
 *
 * A function rather than an object literal so that the shape parameter is stated once, at the call
 * site, and every field is checked against it. `N` is inferred from the name argument when the
 * result is annotated with it, which is what generated code does.
 */
export function defineStruct<S, N extends string = string>(
  name: N,
  fields: { readonly [K in keyof S]-?: FieldDescriptor<NonNullable<S[K]>> },
): StructDescriptor<S, N> {
  return { name, fields };
}

/**
 * The shape of an object nothing generated: every field is a name and every value is its text.
 *
 * This is the same stringly shape the pipeline SDK's `world` has, and for the same reason — it is
 * what is honestly known about a struct whose definitions this process never read. An un-generated
 * client is not a second-class client; it is the CLI, and the CLI works.
 */
export type Untyped = Record<string, string | null>;

/**
 * The keys of `S` that were not declared `readonly` — i.e. the fields a client may write.
 *
 * Generated code marks a derived field `readonly` and nothing else, which keeps it readable; this is
 * what turns that one word into a compile error on `set`. SPEC.md §15 deferred the static marking
 * "with the SDKs themselves", and this is the SDKs themselves.
 */
export type WritableKeys<S> = {
  [K in keyof S]-?: IfEquals<{ [Q in K]: S[K] }, { -readonly [Q in K]: S[K] }, K, never>;
}[keyof S];

// The standard identity test: two generic function types are assignable to each other only if their
// parameter types are *identical*, which is the only place TypeScript exposes identity rather than
// assignability — and readonly-ness is invisible to assignability.
type IfEquals<X, Y, A, B> =
  (<T>() => T extends X ? 1 : 2) extends <T>() => T extends Y ? 1 : 2 ? A : B;

// --- The surfaces ------------------------------------------------------------------------------------

/** One entity, through one transaction. Constructing it is free — nothing has been read yet. */
export interface ObjectHandle<S, N extends string = string> {
  /** The entity's canonical address: `Company:o-1234abcd`. */
  readonly cell: string;
  /**
   * The entity's id — the PID alone, branded with the struct it belongs to.
   *
   * What everything else is built from: a cell address is `Contact:` + this, and a reference field's
   * value *is* this. From [`Tx.create`] it is always a canonical PID, because the server allocated
   * it. From [`Tx.object`] it is **whatever was passed**, uncanonicalised — a handle makes no round
   * trip, so it has nothing to canonicalise with. Pass the `#5` shorthand and this reads `#5`, which
   * is a thing the server accepts as an *address* and would refuse as a reference *value*; the brand
   * says which struct an id belongs to and cannot say that a shorthand is a PID.
   */
  readonly id: RefText<N>;
  /**
   * Read one field. Recorded server-side, and therefore guarded at commit (§12.1).
   *
   * The value alone. [`resolve`](ObjectHandle.resolve) is the same read with its provenance, at no
   * extra round trip — the envelope is on the wire either way, because §17.5 never answers a read
   * with a bare value. Throws [`BorgStateError`] on a `broken` cell.
   */
  get<K extends keyof S & string>(field: K): Promise<S[K]>;
  /** The same read, with the §10.4 envelope: state, freshness, and who produced it. */
  resolve<K extends keyof S & string>(field: K, options?: ReadOptions): Promise<Resolved<S[K]>>;
  /** Write one field, isolated on the transaction's branch until it merges. `null` deletes it. */
  set<K extends WritableKeys<S> & string>(field: K, value: S[K]): Promise<void>;
}

/** A transaction: fork, write, merge. The only write path there is (§12). */
export interface Tx {
  /** The handle. It names the transaction on every message and outlives this connection (§12.2). */
  readonly id: string;
  /** A handle on one entity. No I/O — the address is built from the struct name and the id. */
  object<S, N extends string>(struct: StructDescriptor<S, N>, id: string): ObjectHandle<S, N>;
  object(struct: string, id: string): ObjectHandle<Untyped>;
  /**
   * Allocate an object and create it, in one round trip. §3.1, §8, §17.5.
   *
   * The one thing a client could not previously say. `object()` names an entity whose id you already
   * had; this is where an id comes from — the server allocates it under an `AllocatorId` of its own,
   * so nothing an application creates can ever collide with a `Contact#5` somebody wrote by hand.
   *
   * The existence cell is written **in this transaction**, so the object appears when the
   * transaction commits and never if it aborts. It joins the write-set and reads nothing, which is
   * why two transactions each creating an object cannot conflict.
   */
  create<S, N extends string>(struct: StructDescriptor<S, N>): Promise<ObjectHandle<S, N>>;
  create(struct: string): Promise<ObjectHandle<Untyped>>;
  /**
   * Merge, guarded by everything this transaction read. Answers the layer it landed in on the
   * parent — what `borg frontier reaches` waits for. Throws [`ConflictError`] if a guard moved.
   */
  commit(): Promise<string>;
  /** Drop it. Nothing it wrote ever left its own branch, which is what makes an abort free. */
  abort(): Promise<void>;
}

/** One branch, as a place to read from and to open transactions on. */
export interface BranchHandle {
  /** The branch's name, or `undefined` for the store's default branch. */
  readonly name: string | undefined;
  /** Fork the branch and open a transaction. */
  begin(): Promise<Tx>;
  /**
   * Read a cell **outside** any transaction, by its text address.
   *
   * Answers the envelope rather than the value, and that is not a stylistic difference from
   * [`ObjectHandle.get`]: a read outside a transaction buys no protection at commit (§12.1), so the
   * only thing telling a caller how much to trust it is the envelope. Values come back as text
   * unless a field type is supplied to convert them — the same stringly-with-an-optional-type shape
   * the pipeline SDK's `world` has, because the same thing is unknown here.
   */
  get(cell: string, options?: ReadOptions): Promise<Resolved<string | null>>;
  get<T>(cell: string, options: ReadOptions & { as: FieldType<T> }): Promise<Resolved<T | null>>;
  /**
   * Every object of one struct, as ids. §9.6, §17.5.
   *
   * **On the branch, not in a transaction, and that is deliberate rather than pending.** A guard is
   * a question about a cell (§12.4), and *"the set of Contacts"* is not one — guarding an
   * enumeration would mean guarding the absence of every object not yet created, which is the
   * absence-guard problem widened from a cell to a whole buffer. So a listing is a read like
   * [`get`] outside a transaction: honest, at head, and buying no protection at commit. SDK-DRAFT §5
   * carries the question.
   *
   * **Ids and nothing else.** Reading a field of each is a read of each — the N+1 an ORM has, made
   * visible rather than hidden behind a reply that would have to grow a query language to stay
   * useful. Deleted objects are not listed (§8.1).
   */
  list<S, N extends string>(struct: StructDescriptor<S, N>): Promise<RefText<N>[]>;
  list(struct: string): Promise<string[]>;
  /** Where a value came from. §11. */
  explain(cell: string): Promise<Lineage>;
  /** This branch's head layer. `L0` where it holds nothing of its own. */
  head(): Promise<string>;
  /** The branch's whole def view, and the def-version it was read at. What codegen reads. */
  defs(): Promise<WireSchemaDef>;
}

export interface BorgContextOptions {
  /**
   * **A connection url: where the server is and which registry on it, in one string** (§17.7).
   *
   * ```ts
   * createBorgContext({ url: "borg://localhost/personal-crm" });
   * createBorgContext({ url: "borg+unix:///tmp/borg.sock/personal-crm" });
   * ```
   *
   * `$BORG_URL` is read when neither this nor [`socket`](BorgContextOptions.socket) is given, which
   * is what makes a deployment configurable without a code change. See `parseBorgUrl` for the
   * grammar and for why an absent registry stays absent.
   */
  url?: string;
  /**
   * The unix socket a `borg-server` is listening on — **the explicit form**, for when the two
   * halves are already separate variables and assembling a url out of them would be theatre.
   * Mutually exclusive with [`url`](BorgContextOptions.url).
   */
  socket?: string;
  /**
   * Which registry on that socket (§17.6). Goes with [`socket`](BorgContextOptions.socket); a url
   * names its own.
   *
   * **Absent is absent in the handshake**, and the server answers with its sole registry when it
   * hosts exactly one and names the options when it hosts more. Nothing is guessed here.
   */
  registry?: string;
  /**
   * **The api key to present** (§17.6). A url may carry its own, in the userinfo:
   *
   * ```ts
   * createBorgContext({ url: "borg://:borgk_A1b2@localhost/personal-crm" });
   * createBorgContext({ socket, credential: process.env.BORG_TOKEN });
   * ```
   *
   * Precedence is explicit, then the url, then `$BORG_TOKEN` — the same order `--url` and
   * `$BORG_URL` relate in, so a process with a token exported can still be pointed elsewhere for
   * one context. **Absent is legitimate and is the local case**: a server with no keys file
   * authenticates nobody, and one that does refuses the handshake saying so.
   *
   * It is **re-presented on every reconnect**, because a reconnect re-runs the handshake — see
   * [`Session`]. A context that authenticated once and then silently stopped would be the shape of
   * bug that only appears after an outage.
   */
  credential?: string;
  /**
   * The def-layer this client's code was generated from — its ClientVersion (§5.4).
   *
   * **Absent means the branch head as it stands**, which is what an un-generated client honestly is:
   * it was authored *now*, against the schema in force. Generated code fills this in and does not
   * offer it as an option, because generation is what decided it.
   */
  clientVersion?: string;
  /**
   * **When to dial.** `"now"` is the default and is right for anything short-lived: an error at
   * construction names the address you just configured, and one at first use names whichever line
   * happened to be first.
   *
   * `"on-demand"` is for a process that legitimately starts *before* its server — a supervisor
   * bringing both up, a container, a dev script, an api whose job is to answer `503` until the
   * backend is there. It is the honest spelling of "I will find out at first use", and it exists so
   * that such a process does not have to own connection lifecycle by hand, which is exactly what
   * `examples/personal-crm/FRICTION.md` #11 said it should not have to. A url that is not a url is
   * still refused here, whichever is chosen: that is a mistake, not an outage.
   */
  connect?: "now" | "on-demand";
  /** The environment `$BORG_URL` and the well-known address are read from. For tests. */
  env?: NodeJS.ProcessEnv;
}

export interface BorgContext {
  /** A branch. `undefined` is the store's default branch, exactly as omitting `--branch` is. */
  branch(name?: string): BranchHandle;
  /** Every branch in the store. */
  branches(): Promise<BranchInfo[]>;
  /**
   * Pick up a transaction by its handle.
   *
   * The reconnect story, and it needs no protocol support because there was never any to need: a
   * transaction binds to the store (§12.2), so a browser tab that reloaded mid-transaction names the
   * same id on a new socket and carries on.
   */
  transaction(id: string): Tx;
  /** The address this context connects to. What an error message or a health check wants. */
  readonly address: string;
  /**
   * Whether a connection is up **right now**, without asking the server anything.
   *
   * `false` means either "not dialled since the last failure" or "torn down", which from here are
   * the same state: the next operation dials. It is deliberately not a liveness probe — a probe
   * that answered `true` would be answering about the moment before the one you care about.
   */
  readonly connected: boolean;
  close(): void;
}

// --- The implementation -------------------------------------------------------------------------------

type Wire = MessageStream<Response | ServerHello | HelloAck, Request | ClientHello>;

/**
 * Connect to a `borg-server` and complete the handshake.
 *
 * The server speaks first and always in JSON — a handshake cannot be encoded in a codec that has not
 * been agreed yet — and JSON is also all this SDK offers back: MessagePack would buy a dependency,
 * and being dependency-free is what lets this package be dropped into anything.
 *
 * **It dials here rather than at first use**, which is the same choice as before: an error at
 * construction names the thing you just configured, and one at first use names whatever line
 * happened to be first. What is new is that this is no longer the *only* dial — see [`Session`].
 */
export async function createBorgContext(options: BorgContextOptions): Promise<BorgContext> {
  // Resolved before anything is dialled, so that a malformed url is refused whichever mode this is
  // in: a url that is not a url is a mistake, not an outage.
  const session = new Session(whereToConnect(options), options.clientVersion);
  if (options.connect !== "on-demand") await session.connect();
  return new Context(session);
}

/** Where a set of options says to connect, which registry to name, and what to present. */
function whereToConnect(options: BorgContextOptions): Endpoint {
  const env = options.env ?? process.env;
  if (options.url !== undefined && options.socket !== undefined) {
    throw new BorgClientError(
      "createBorgContext takes a url or a socket, not both — a url already names the socket",
    );
  }
  // **Explicit, then the url, then the environment.** The same order `--url` and `$BORG_URL` relate
  // in: the thing somebody wrote at this call site beats the thing that was lying around.
  const ambientToken = env[TOKEN_ENV];
  const fallback =
    ambientToken === undefined || ambientToken === "" ? undefined : ambientToken;
  if (options.url !== undefined) {
    if (options.registry !== undefined) {
      throw new BorgClientError(
        // Redacted, because this message quotes the url back and the url may hold a key (§17.6).
        `createBorgContext was given both a url and a registry — ` +
          `\`${redactBorgUrl(options.url)}\` names its own`,
      );
    }
    return connectionOf(parseBorgUrl(options.url), env, options.credential, fallback);
  }
  if (options.socket !== undefined) {
    return {
      address: { kind: "unix", path: options.socket },
      registry: options.registry,
      credential: options.credential ?? fallback,
    };
  }
  const ambient = env[URL_ENV];
  if (ambient === undefined || ambient === "") {
    throw new BorgClientError(
      `createBorgContext needs somewhere to connect: pass { url: "borg://localhost/<registry>" }, ` +
        `or { socket }, or set $${URL_ENV}`,
    );
  }
  return connectionOf(parseBorgUrl(ambient), env, options.credential, fallback);
}

/** Where a context connects, which registry it names, and what it presents. */
interface Endpoint {
  readonly address: BorgAddress;
  readonly registry: string | undefined;
  /** The api key, or `undefined` for an open server. **Never printed** — see [`Session.address`]. */
  readonly credential: string | undefined;
}

function connectionOf(
  url: BorgUrl,
  env: NodeJS.ProcessEnv,
  explicit: string | undefined,
  ambient: string | undefined,
): Endpoint {
  return {
    address: borgAddress(url, env),
    registry: url.registry ?? undefined,
    // Explicit beats the url beats the environment — the last of those is what a deployment sets
    // once, and the first two are what somebody wrote deliberately.
    credential: explicit ?? url.credential ?? ambient,
  };
}

/**
 * **One address, and however many connections it takes.** `examples/personal-crm/FRICTION.md` #11.
 *
 * A `BorgContext` used to *be* a socket, so a server restart made it permanently useless and the
 * only recovery was for the application to build a new one — with no event, no flag and no way to
 * tell "the server is down" from "this context is finished". This is the piece that was missing: an
 * address, at most one live connection to it, and a redial on the next operation after a failure.
 *
 * **Nothing is retried.** A request that was in flight when the socket broke fails with
 * [`BorgDisconnectedError`] and stops there; see that class for why a `tx_commit` in particular
 * must not be re-sent. The redial happens for the *next* operation, which the caller chose to make.
 */
class Session {
  readonly #endpoint: Endpoint;
  readonly #clientVersion: string | undefined;
  #wire: Wire | null = null;
  /** A dial in progress, so two concurrent operations open one socket rather than two. */
  #dialling: Promise<Wire> | null = null;
  #closed = false;

  constructor(endpoint: Endpoint, clientVersion: string | undefined) {
    this.#endpoint = endpoint;
    this.#clientVersion = clientVersion;
  }

  get address(): string {
    return addressText(this.#endpoint.address);
  }

  get connected(): boolean {
    return this.#wire !== null;
  }

  /** Dial now, so that a misconfigured address fails where it was configured. */
  async connect(): Promise<void> {
    await this.#connection();
  }

  #connection(): Promise<Wire> {
    if (this.#closed) {
      throw new BorgClientError("this BorgContext is closed");
    }
    // **A socket the peer has already closed is dropped before it is used, not after.** This is
    // what makes a server bounce cost nothing rather than one guaranteed failure per client: the
    // close arrived while this process was idle, so nothing was in flight and nothing is being
    // retried — the request about to be made has not been sent anywhere yet.
    if (this.#wire !== null && this.#wire.closed) this.#drop(this.#wire);
    if (this.#wire !== null) return Promise.resolve(this.#wire);
    this.#dialling ??= this.#dial().then(
      (wire) => {
        this.#dialling = null;
        // A context closed while the dial was in flight must not adopt the socket it opened.
        if (this.#closed) {
          wire.close();
          throw new BorgClientError("this BorgContext is closed");
        }
        this.#wire = wire;
        return wire;
      },
      (err: unknown) => {
        this.#dialling = null;
        throw err;
      },
    );
    return this.#dialling;
  }

  /**
   * **One connection, over whichever transport the address named**, and the identical handshake on
   * both. A unix socket frames per line; a WebSocket is framed already (`./lines.ts`). Nothing below
   * this method can tell which it got, which is what makes the reconnect story one story.
   */
  async #open(): Promise<Wire> {
    const address = this.#endpoint.address;
    if (address.kind === "ws") {
      const socket = await dialBorgWebSocket(address.url);
      return new WebSocketStream(socket, "the server");
    }
    const socket = await dialBorgServer(address.path);
    return new LineStream(socket, socket, () => socket.end(), "the server");
  }

  async #dial(): Promise<Wire> {
    const wire = await this.#open();

    const hello = await wire.receive();
    if (hello === null) throw new BorgProtocolError("the server hung up before saying hello");
    if (!("version" in hello) || !("codecs" in hello)) {
      throw new BorgProtocolError(
        `the server's opening message was not a hello: ${stringify(hello)}`,
      );
    }
    // **This client's own version, not the server's echoed back** (§17.5). Echoing would make the
    // client claim to speak whatever it was told, which is the one claim a version exists to check.
    const reply: ClientHello = { version: CLIENT_PROTOCOL_VERSION, codec: "json" };
    if (this.#clientVersion !== undefined) reply.client_version = this.#clientVersion;
    // **The registry is settled here, once per connection** (§17.6) — which is exactly why a
    // reconnect has to re-handshake rather than merely re-open: a new socket that skipped this
    // would be a connection to a server with no idea which store it is for.
    if (this.#endpoint.registry !== undefined) reply.registry = this.#endpoint.registry;
    // **Presented on every dial, which is what makes a reconnect re-authenticate** (§17.6). A
    // credential settled once and then not re-sent would work until the first server bounce and
    // fail afterwards, which is the shape of bug that only appears during an outage.
    if (this.#endpoint.credential !== undefined) reply.credential = this.#endpoint.credential;
    wire.send(reply);

    // **And the server answers, before a request goes out.** This is what closes the deviation the
    // SDK used to carry: a registry the server does not host, or a protocol it does not speak, is a
    // refusal *here*, so `createBorgContext` fails where the connection was configured rather than
    // at whichever line happened to make the first call. A reconnect re-runs it, so a context whose
    // registry was deleted under it fails on its next operation and says why.
    const ack = await wire.receive();
    if (ack === null) {
      throw new BorgProtocolError("the server hung up without acknowledging the handshake");
    }
    if (typeof ack === "object" && "refused" in ack) {
      wire.close();
      throw new BorgClientError(ack.refused.reason);
    }
    if (typeof ack !== "object" || !("accepted" in ack)) {
      wire.close();
      throw new BorgProtocolError(
        `the server did not acknowledge the handshake: ${stringify(ack)}`,
      );
    }
    return wire;
  }

  /** One request, one reply, with `error` turned into a throw and `conflict` optionally allowed. */
  async ask(request: Request, options?: { conflicts: boolean }): Promise<Response> {
    const wire = await this.#connection();
    let reply: Response | ServerHello | HelloAck | null;
    try {
      reply = await wire.request(request);
    } catch (err) {
      this.#drop(wire);
      // The framing layer's own words, rewritten to say what a caller has to decide about: the
      // socket is gone, this operation was not retried, and the next one will reconnect.
      throw new BorgDisconnectedError(
        this.address,
        err instanceof Error ? err.message : String(err),
      );
    }
    if (reply === null) {
      this.#drop(wire);
      throw new BorgDisconnectedError(this.address, "the server hung up");
    }
    if ("error" in reply) throw new BorgClientError(reply.error.message);
    if ("conflict" in reply && options?.conflicts !== true) {
      throw new ConflictError(reply.conflict.cell, reply.conflict.reason, reply.conflict.message);
    }
    return reply as Response;
  }

  /**
   * Tear a connection down, if it is still the current one.
   *
   * Guarded on identity because several operations can be in flight over one socket and every one
   * of them fails when it breaks: without the check, the second failure would discard a connection
   * the first had already replaced.
   */
  #drop(wire: Wire): void {
    if (this.#wire !== wire) return;
    this.#wire = null;
    try {
      wire.close();
    } catch {
      // Closing a socket that is already gone is not news.
    }
  }

  close(): void {
    this.#closed = true;
    const wire = this.#wire;
    this.#wire = null;
    if (wire !== null) wire.close();
  }
}

class Context implements BorgContext {
  readonly #session: Session;

  constructor(session: Session) {
    this.#session = session;
  }

  get address(): string {
    return this.#session.address;
  }

  get connected(): boolean {
    return this.#session.connected;
  }

  branch(name?: string): BranchHandle {
    return new Branch(this.#session, name);
  }

  async branches(): Promise<BranchInfo[]> {
    return expect(await this.#session.ask({ branch_list: {} }), "branches");
  }

  transaction(id: string): Tx {
    return new Transaction(this.#session, id);
  }

  close(): void {
    this.#session.close();
  }
}

class Branch implements BranchHandle {
  readonly #session: Session;
  readonly name: string | undefined;

  constructor(session: Session, name: string | undefined) {
    this.#session = session;
    this.name = name;
  }

  async begin(): Promise<Tx> {
    const { tx } = expect(await this.#session.ask({ tx_begin: { branch: this.name } }), "tx");
    return new Transaction(this.#session, tx);
  }

  async get(cell: string, options?: ReadOptions & { as?: AnyFieldType }): Promise<Resolved<never>> {
    const wire = expect(
      await this.#session.ask({
        get: {
          branch: this.name,
          cell,
          freshness: options?.freshness,
          settled: options?.settled ?? false,
        },
      }),
      "cell",
    );
    return envelopeOf(wire, options?.as) as Resolved<never>;
  }

  async list(struct: StructDescriptor<unknown, string> | string): Promise<never[]> {
    const name = typeof struct === "string" ? struct : struct.name;
    const ids = expect(
      await this.#session.ask({ list: { branch: this.name, struct: name } }),
      "ids",
    );
    // Branded, not converted: an id is a PID and a PID is its text (§3.1). The brand is a claim
    // about which struct it belongs to, which the descriptor is what made checkable.
    return ids as never[];
  }

  async explain(cell: string): Promise<Lineage> {
    const wire: WireLineage = expect(
      await this.#session.ask({ explain: { branch: this.name, cell } }),
      "lineage",
    );
    return wire;
  }

  async head(): Promise<string> {
    return expect(await this.#session.ask({ branch_head: { branch: this.name } }), "head").layer;
  }

  async defs(): Promise<WireSchemaDef> {
    return expect(await this.#session.ask({ def_view: { branch: this.name } }), "defs");
  }
}

class Transaction implements Tx {
  readonly #session: Session;
  readonly id: string;

  constructor(session: Session, id: string) {
    this.#session = session;
    this.id = id;
  }

  object(struct: StructDescriptor<unknown> | string, id: string): ObjectHandle<never> {
    const descriptor = typeof struct === "string" ? undefined : struct;
    const name = typeof struct === "string" ? struct : struct.name;
    return new Handle(this.#session, this.id, name, id, descriptor) as ObjectHandle<never>;
  }

  async create(struct: StructDescriptor<unknown> | string): Promise<ObjectHandle<never>> {
    const name = typeof struct === "string" ? struct : struct.name;
    const { id } = expect(
      await this.#session.ask({ tx_create: { tx: this.id, struct: name } }),
      "created",
    );
    // The same handle `object()` builds, on an id the server chose rather than one the caller had.
    // Nothing is cached from the creation: the object exists in the transaction, and the handle
    // reads and writes it like any other.
    return this.object(struct, id);
  }

  async commit(): Promise<string> {
    const reply = await this.#session.ask({ tx_commit: { tx: this.id } }, { conflicts: true });
    if ("conflict" in reply) {
      const { cell, reason, message } = reply.conflict;
      throw new ConflictError(cell, reason, message);
    }
    return expect(reply, "committed").landed;
  }

  async abort(): Promise<void> {
    expect(await this.#session.ask({ tx_abort: { tx: this.id } }), "ok");
  }
}

class Handle implements ObjectHandle<never> {
  readonly #session: Session;
  readonly #tx: string;
  readonly #fields: Record<string, FieldDescriptor<unknown>> | undefined;
  readonly cell: string;
  readonly id: never;

  constructor(
    session: Session,
    tx: string,
    struct: string,
    id: string,
    descriptor: StructDescriptor<unknown> | undefined,
  ) {
    this.#session = session;
    this.#tx = tx;
    this.cell = address(struct, id);
    this.id = id as never;
    this.#fields = descriptor?.fields as Record<string, FieldDescriptor<unknown>> | undefined;
  }

  async get(field: never): Promise<never> {
    const read = await this.resolve(field);
    // The one state where the label is load-bearing to the *value*, and so the one the shortcut
    // cannot silently drop — see `BorgStateError`.
    if (read.state === "broken") throw new BorgStateError(read as unknown as Resolved<null>);
    return read.value;
  }

  async resolve(field: never, options?: ReadOptions): Promise<Resolved<never>> {
    const wire = expect(
      await this.#session.ask({
        tx_get: {
          tx: this.#tx,
          cell: `${this.cell}.${String(field)}`,
          freshness: options?.freshness,
        },
      }),
      "cell",
    );
    return envelopeOf(wire, this.#type(field)) as Resolved<never>;
  }

  async set(field: never, value: never): Promise<void> {
    const descriptor = this.#fields?.[String(field)];
    if (descriptor?.derived === true) {
      // The engine refuses this too, having validated the write against declared ownership (§8) —
      // but it would refuse it as a rejected transaction naming a producer id. This is the same rule
      // stated where the caller can act on it, and it costs no round trip.
      throw new BorgClientError(
        `${this.cell}.${String(field)} is derived and may only be written by the producer that ` +
          `owns it (§8) — a client's writes are to source fields`,
      );
    }
    const type = this.#type(field);
    const text = value === null || value === undefined ? TOMBSTONE : encode(type, value);
    expect(
      await this.#session.ask({
        tx_set: { tx: this.#tx, cell: `${this.cell}.${String(field)}`, value: text },
      }),
      "ok",
    );
  }

  #type(field: unknown): FieldType<unknown> | undefined {
    if (this.#fields === undefined) return undefined;
    const descriptor = this.#fields[String(field)];
    if (descriptor === undefined) {
      // Unreachable from generated code — this is the JavaScript caller, and the untyped path.
      throw new BorgClientError(
        `no field \`${String(field)}\` on ${this.cell.split(":")[0] ?? this.cell}`,
      );
    }
    return descriptor.type;
  }
}

// --- Conversions and plumbing -----------------------------------------------------------------------

/**
 * `Company:o-1234abcd`, or `Company#1` for the shorthand the CLI accepts on input.
 *
 * Both spellings are supported because both are things a caller has in hand: a PID comes back from a
 * reference field, and `#1` is what a fixture or a scenario writes. What the server answers with is
 * always the canonical form, whichever went out.
 */
function address(struct: string, id: string): string {
  return id.startsWith("#") ? `${struct}${id}` : `${struct}:${id}`;
}

/**
 * Absence has two spellings and both mean the same thing here.
 *
 * A cell never written answers `null`; one explicitly deleted answers `~` (§8.1). The distinction is
 * real in the store and survives on the wire; it is collapsed here for the same reason the pipeline
 * SDK collapses it — both mean "there is no value at this cell", and writing `null` back is a
 * tombstone, so the round trip closes. What the two *were* is still in the envelope's `state`.
 */
function envelopeOf(wire: WireEnvelope, type: FieldType<unknown> | undefined): Resolved<unknown> {
  const text = wire.value === null || wire.value === TOMBSTONE ? null : wire.value;
  return {
    cell: wire.cell,
    value: text === null || type === undefined ? text : type.decode(text),
    origin: wire.origin as Resolved<unknown>["origin"],
    state: wire.state as Resolved<unknown>["state"],
    event: wire.event,
    authored_at: wire.authored_at,
    landed_at: wire.landed_at,
    fresh_as_of: wire.fresh_as_of,
    by: wire.by,
  };
}

function encode(type: FieldType<unknown> | undefined, value: unknown): string {
  if (type !== undefined) return type.encode(value);
  if (typeof value === "string") return value;
  throw new BorgClientError(
    `an untyped field takes text, not ${typeof value} — generated code carries the conversion`,
  );
}

/** The payload of the single-key response named `K`. §17.4's single-key rule, as a type. */
type Payload<K extends string> =
  Extract<Response, Record<K, unknown>> extends Record<K, infer V> ? V : never;

/** The reply this request has to have had, or a protocol error naming what came instead. */
function expect<K extends string>(reply: Response | ServerHello | HelloAck, key: K): Payload<K> {
  if (key in reply) {
    return (reply as Record<K, Payload<K>>)[key];
  }
  throw new BorgProtocolError(`expected a \`${key}\` from the server, got ${stringify(reply)}`);
}

function stringify(value: unknown): string {
  try {
    return JSON.stringify(value) ?? String(value);
  } catch {
    return String(value);
  }
}
