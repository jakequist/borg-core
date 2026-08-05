/**
 * The two modes of a repo module: describe itself, or serve invocations.
 *
 * One artifact, two modes, matching the bash worker's shape. `describe` stays a plain `argv[1] ===
 * "describe"` invocation printing JSON to stdout — that call has no stream to corrupt, and keeping
 * it plain is what keeps a bash repo one `jq -n`. Everything else is the worker loop.
 *
 * ## The SDK records nothing
 *
 * Every `get` and every `set` below is a wire message, and the engine records the read-set
 * server-side (§9.4). There is no dependency tracking in this file, no cache, and no place to put
 * one: a `Proxy` or a preload would only *translate* accesses, and a preload would translate the
 * wrong ones — an object-granular read-set instead of a field-granular one, which is the difference
 * scenario 030 exists to demonstrate. Tracking ships in no SDK, ever.
 */

import { connect, type Connection } from "./connection.js";
import { producerId, type Description, type ToWorker } from "./protocol.js";
import {
  BorgDefinitionError,
  describe,
  type EntityContext,
  type Fields,
  type PipelineDef,
  type RepoConfig,
  type World,
} from "./dsl.js";
import { TOMBSTONE, type FieldType } from "./values.js";

export interface Repo {
  /** The `describe` payload this repo reports. Pure; useful in tests. */
  describe(): Description;
  /**
   * Run as `borg` invokes it: `describe`, or the worker loop.
   *
   * `argv` defaults to `process.argv.slice(2)`, so `argv[0]` here is what the engine passed as the
   * process's first argument — `describe` or nothing.
   */
  main(argv?: readonly string[]): Promise<void>;
}

/**
 * Define a repo. Validation happens now, at module load, so a mistake fails `describe` as well as
 * the worker loop — one is a push-time error the author sees immediately, the other a mid-round
 * failure they would see much later.
 */
export function repo(config: RepoConfig): Repo {
  const description = describe(config);
  // Producer id → pipeline. The engine invokes by id, and one module may implement several.
  const byId = new Map<string, PipelineDef>();
  for (const p of config.pipelines) byId.set(producerId(p.name), p);

  return {
    describe: () => description,
    async main(argv = process.argv.slice(2)) {
      if (argv[0] === "describe") {
        process.stdout.write(`${JSON.stringify(description)}\n`);
        return;
      }
      await serve(byId);
    },
  };
}

/** The worker loop: handshake, then invocations until the engine says stop. */
async function serve(byId: Map<string, PipelineDef>): Promise<void> {
  const conn = await connect();
  // The handshake is always JSON, because a codec cannot be encoded in one not yet agreed. JSON is
  // also all this SDK offers: MessagePack would buy a dependency, and the framing that makes a
  // shell worker possible is what makes this one dependency-free.
  const hello = await conn.receive();
  if (hello === null) throw new Error("the engine hung up before saying hello");
  conn.send({ codec: "json" });

  for (;;) {
    const message = await conn.receive();
    if (message === null || "shutdown" in message) break;
    if (!("invoke" in message)) continue;

    const { producer, input } = message.invoke;
    try {
      const definition = byId.get(String(producer));
      if (definition === undefined) {
        throw new BorgDefinitionError(
          `the engine invoked producer ${producer}, which this repo does not implement`,
        );
      }
      await invoke(conn, definition, input);
      conn.send({ done: {} });
    } catch (err) {
      // A pipeline that throws on one entity is not a broken process: the engine aborts that
      // invocation's layer and poisons the producer (§14), and the conversation is still in step
      // because every request above completed its reply before this could be reached.
      conn.send({ error: { message: explain(err) } });
    }
  }
  conn.close();
}

async function invoke(conn: Connection, definition: PipelineDef, input: string): Promise<void> {
  // Anything the body kept a reference to stops working the moment the invocation is over. Without
  // this, a forgotten `await` would send a `get` in the middle of the *next* invocation and take the
  // reply belonging to it — one entity's value quietly written to another.
  let live = true;
  const guard = (): void => {
    if (!live) {
      throw new Error(
        `this invocation of \`${definition.name}\` has already finished — an unawaited get() or ` +
          `set() would read another entity's answer`,
      );
    }
  };

  const entity: EntityContext<Fields> = {
    ref: input,
    async get(field) {
      guard();
      const type = fieldType(definition, field);
      const text = present(value(await conn.request({ get: `${input}.${field}` })));
      return text === null ? null : type.decode(text);
    },
    async set(field, next) {
      guard();
      const type = fieldType(definition, field);
      if (!definition.writes.includes(field)) {
        // The engine would refuse this too, having validated the write against declared ownership
        // (§8) — but it would refuse it in the middle of a round, naming a producer rather than a
        // line of code. This is the same rule stated where the author can act on it.
        throw new BorgDefinitionError(
          `\`${definition.name}\` writes ${definition.writes.join(", ")} and does not declare ` +
            `\`${field}\` — add it to \`writes\` and mark the field derived()`,
        );
      }
      acknowledged(
        await conn.request({
          set: { cell: `${input}.${field}`, value: next === null ? "~" : type.encode(next) },
        }),
      );
    },
  };

  const world: World = {
    async get(cell: string, as?: FieldType<unknown>) {
      guard();
      const text = present(value(await conn.request({ get: cell })));
      return text === null || as === undefined ? text : as.decode(text);
    },
    async set(cell: string, next: unknown, as?: FieldType<unknown>) {
      guard();
      let text: string;
      if (next === null) text = "~";
      else if (as !== undefined) text = as.encode(next);
      else if (typeof next === "string") text = next;
      else {
        throw new TypeError(
          `world.set(${cell}, …) takes text, or a value plus the field type to convert it with`,
        );
      }
      acknowledged(await conn.request({ set: { cell, value: text } }));
    },
  } as World;

  try {
    await definition.body(entity, world);
  } finally {
    live = false;
  }
}

function fieldType(definition: PipelineDef, field: string): FieldType<unknown> {
  const type = definition.source.fields[field];
  if (type === undefined) {
    throw new BorgDefinitionError(`\`${definition.source.name}\` declares no field \`${field}\``);
  }
  return type;
}

/**
 * Absence, however the engine spelled it.
 *
 * A cell that has never been written answers `null`; one explicitly deleted answers `~` (§8.1). The
 * distinction is real in the store and survives on the wire, and it is collapsed here because a
 * pipeline has nothing different to do with the two: both mean "there is no value at this cell", and
 * `decode` would have to grow a tombstone case for every type to say so a second way. Writing `null`
 * back is a tombstone, so the round trip is closed.
 */
function present(text: string | null): string | null {
  return text === null || text === TOMBSTONE ? null : text;
}

/** A `Value` reply, or a protocol error naming what came instead. */
function value(reply: ToWorker): string | null {
  if ("value" in reply) return reply.value;
  throw new Error(`expected a value from the engine, got ${JSON.stringify(reply)}`);
}

function acknowledged(reply: ToWorker): void {
  if ("ok" in reply) return;
  throw new Error(`expected an acknowledgement from the engine, got ${JSON.stringify(reply)}`);
}

/** Whatever was thrown, as one line the engine can report. */
function explain(err: unknown): string {
  if (err instanceof Error) return err.message;
  return String(err);
}
