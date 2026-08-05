/**
 * # borg-sdk/client
 *
 * The consumer-side SDK: read and write data through transactions, over `borg serve`'s socket.
 *
 * ```ts
 * import { Company, createBorgContext } from "./borg.generated.js";
 *
 * const bc = await createBorgContext({ socket: "/tmp/borg.sock" });
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
 */

import { openUnixSocket } from "./connection.js";
import { BorgProtocolError, LineStream, type MessageStream } from "./lines.js";
import type {
  BranchInfo,
  ClientHello,
  Request,
  Response,
  ServerHello,
  WireEnvelope,
  WireLineage,
  WireSchemaDef,
} from "./client-protocol.js";
import { TOMBSTONE, type AnyFieldType, type FieldType } from "./values.js";

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
 * The type parameter is the *shape* — the interface generated beside it — and the mapped `fields`
 * type is what makes a generator bug into a compile error in the generated file: a field in the
 * interface with no descriptor, or a descriptor whose conversion produces the wrong type, will not
 * assemble.
 */
export interface StructDescriptor<S> {
  readonly name: string;
  readonly fields: { readonly [K in keyof S]-?: FieldDescriptor<NonNullable<S[K]>> };
}

/**
 * Assemble a struct descriptor. Generated code's only helper.
 *
 * A function rather than an object literal so that the shape parameter is stated once, at the call
 * site, and every field is checked against it.
 */
export function defineStruct<S>(
  name: string,
  fields: { readonly [K in keyof S]-?: FieldDescriptor<NonNullable<S[K]>> },
): StructDescriptor<S> {
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
export interface ObjectHandle<S> {
  /** The entity's canonical address: `Company:o-1234abcd`. */
  readonly cell: string;
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
  object<S>(struct: StructDescriptor<S>, id: string): ObjectHandle<S>;
  object(struct: string, id: string): ObjectHandle<Untyped>;
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
  /** Where a value came from. §11. */
  explain(cell: string): Promise<Lineage>;
  /** This branch's head layer. `L0` where it holds nothing of its own. */
  head(): Promise<string>;
  /** The branch's whole def view, and the def-version it was read at. What codegen reads. */
  defs(): Promise<WireSchemaDef>;
}

export interface BorgContextOptions {
  /** The unix socket `borg serve --socket` is listening on. */
  socket: string;
  /**
   * The def-layer this client's code was generated from — its ClientVersion (§5.4).
   *
   * **Absent means the branch head as it stands**, which is what an un-generated client honestly is:
   * it was authored *now*, against the schema in force. Generated code fills this in and does not
   * offer it as an option, because generation is what decided it.
   */
  clientVersion?: string;
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
  close(): void;
}

// --- The implementation -------------------------------------------------------------------------------

type Wire = MessageStream<Response | ServerHello, Request | ClientHello>;

/**
 * Connect to `borg serve` and complete the handshake.
 *
 * The server speaks first and always in JSON — a handshake cannot be encoded in a codec that has not
 * been agreed yet — and JSON is also all this SDK offers back: MessagePack would buy a dependency,
 * and being dependency-free is what lets this package be dropped into anything.
 */
export async function createBorgContext(options: BorgContextOptions): Promise<BorgContext> {
  const socket = await openUnixSocket(options.socket, `borg socket ${options.socket}`);
  const wire: Wire = new LineStream(socket, socket, () => socket.end(), "the server");

  const hello = await wire.receive();
  if (hello === null) throw new BorgProtocolError("the server hung up before saying hello");
  if (!("version" in hello) || !("codecs" in hello)) {
    throw new BorgProtocolError(`the server's opening message was not a hello: ${stringify(hello)}`);
  }
  const reply: ClientHello = { version: hello.version, codec: "json" };
  if (options.clientVersion !== undefined) reply.client_version = options.clientVersion;
  wire.send(reply);

  return new Context(wire);
}

class Context implements BorgContext {
  readonly #wire: Wire;

  constructor(wire: Wire) {
    this.#wire = wire;
  }

  branch(name?: string): BranchHandle {
    return new Branch(this.#wire, name);
  }

  async branches(): Promise<BranchInfo[]> {
    return expect(await ask(this.#wire, { branch_list: {} }), "branches");
  }

  transaction(id: string): Tx {
    return new Transaction(this.#wire, id);
  }

  close(): void {
    this.#wire.close();
  }
}

class Branch implements BranchHandle {
  readonly #wire: Wire;
  readonly name: string | undefined;

  constructor(wire: Wire, name: string | undefined) {
    this.#wire = wire;
    this.name = name;
  }

  async begin(): Promise<Tx> {
    const { tx } = expect(await ask(this.#wire, { tx_begin: { branch: this.name } }), "tx");
    return new Transaction(this.#wire, tx);
  }

  async get(cell: string, options?: ReadOptions & { as?: AnyFieldType }): Promise<Resolved<never>> {
    const wire = expect(
      await ask(this.#wire, {
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

  async explain(cell: string): Promise<Lineage> {
    const wire: WireLineage = expect(
      await ask(this.#wire, { explain: { branch: this.name, cell } }),
      "lineage",
    );
    return wire;
  }

  async head(): Promise<string> {
    return expect(await ask(this.#wire, { branch_head: { branch: this.name } }), "head").layer;
  }

  async defs(): Promise<WireSchemaDef> {
    return expect(await ask(this.#wire, { def_view: { branch: this.name } }), "defs");
  }
}

class Transaction implements Tx {
  readonly #wire: Wire;
  readonly id: string;

  constructor(wire: Wire, id: string) {
    this.#wire = wire;
    this.id = id;
  }

  object(struct: StructDescriptor<unknown> | string, id: string): ObjectHandle<never> {
    const descriptor = typeof struct === "string" ? undefined : struct;
    const name = typeof struct === "string" ? struct : struct.name;
    return new Handle(this.#wire, this.id, address(name, id), descriptor) as ObjectHandle<never>;
  }

  async commit(): Promise<string> {
    const reply = await ask(this.#wire, { tx_commit: { tx: this.id } }, { conflicts: true });
    if ("conflict" in reply) {
      const { cell, reason, message } = reply.conflict;
      throw new ConflictError(cell, reason, message);
    }
    return expect(reply, "committed").landed;
  }

  async abort(): Promise<void> {
    expect(await ask(this.#wire, { tx_abort: { tx: this.id } }), "ok");
  }
}

class Handle implements ObjectHandle<never> {
  readonly #wire: Wire;
  readonly #tx: string;
  readonly #fields: Record<string, FieldDescriptor<unknown>> | undefined;
  readonly cell: string;

  constructor(
    wire: Wire,
    tx: string,
    cell: string,
    descriptor: StructDescriptor<unknown> | undefined,
  ) {
    this.#wire = wire;
    this.#tx = tx;
    this.cell = cell;
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
      await ask(this.#wire, {
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
      await ask(this.#wire, {
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

/** One request, one reply, with `error` turned into a throw and `conflict` optionally allowed. */
async function ask(
  wire: Wire,
  request: Request,
  options?: { conflicts: boolean },
): Promise<Response> {
  const reply = await wire.request(request);
  if (reply === null) throw new BorgProtocolError("the server hung up in the middle of a request");
  if ("error" in reply) throw new BorgClientError(reply.error.message);
  if ("conflict" in reply && options?.conflicts !== true) {
    throw new ConflictError(reply.conflict.cell, reply.conflict.reason, reply.conflict.message);
  }
  return reply as Response;
}

/** The payload of the single-key response named `K`. §17.4's single-key rule, as a type. */
type Payload<K extends string> =
  Extract<Response, Record<K, unknown>> extends Record<K, infer V> ? V : never;

/** The reply this request has to have had, or a protocol error naming what came instead. */
function expect<K extends string>(reply: Response | ServerHello, key: K): Payload<K> {
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
