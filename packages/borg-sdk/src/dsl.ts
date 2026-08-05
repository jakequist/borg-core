/**
 * The author-side DSL: structs, pipelines, and the repo they make up.
 *
 * This half is pure declaration — nothing here opens a connection or reads a cell. What it produces
 * is a `describe` payload, which is the DSL's compile target and the whole of the contract between
 * an SDK and `borg repo push` (§17.4). A repo that describes itself needs no new engine surface.
 *
 * ## Ownership is explicit, and checked in both directions
 *
 * `borg.bool().derived()` says a pipeline owns the field; `{ writes: ["isInvestible"] }` says which
 * one. Neither is inferred from the other, and assembling the description errors if they disagree —
 * a `derived()` field nobody writes is a field no client may write either and no pipeline ever will,
 * and a `writes` naming a field that is not `derived()` is a write the engine will refuse at the
 * first invocation. Both are static facts, so both are push-time errors rather than runtime ones.
 *
 * Inference is later sugar, and would happen here rather than at runtime.
 */

import type { AnyFieldType, FieldType, ValueOf } from "./values.js";
import type { Description, FieldSpec, StructSpec } from "./protocol.js";

/** A repo the DSL rejected before it ever reached the engine. */
export class BorgDefinitionError extends Error {
  override readonly name = "BorgDefinitionError";
}

export type Fields = Record<string, AnyFieldType>;

export interface StructDef<F extends Fields = Fields> {
  readonly name: string;
  readonly fields: F;
}

/**
 * Declare a struct. Names are used exactly as written, here and on the wire: a `headcount` in the
 * DSL is `Company#1.headcount` at the CLI. No case is converted, because a mapping that is invisible
 * in both directions is a mapping somebody eventually has to reverse-engineer from an error message.
 */
export function struct<F extends Fields>(name: string, fields: F): StructDef<F> {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
    throw new BorgDefinitionError(`\`${name}\` is not a usable struct name`);
  }
  if (Object.keys(fields).length === 0) {
    throw new BorgDefinitionError(`struct \`${name}\` declares no fields`);
  }
  return { name, fields };
}

/** The input entity of one invocation. Every access is a wire message; nothing is cached. */
export interface EntityContext<F extends Fields> {
  /** The entity's cell address, as the engine named it: `Company:o-1234abcd`. */
  readonly ref: string;
  /** Read one of the entity's fields. Recorded server-side whether or not it exists. */
  get<K extends keyof F & string>(field: K): Promise<ValueOf<F[K]> | null>;
  /** Write one of the entity's fields. `null` deletes it. */
  set<K extends keyof F & string>(field: K, value: ValueOf<F[K]> | null): Promise<void>;
}

/**
 * Random access to anything else, for the hops a pipeline makes beyond its own entity.
 *
 * Stringly in v1: a cell is named by its text address, and a value by its text form unless a field
 * type is supplied to convert it. Generated types are what will make this typed — the second
 * overload is where they slot in, taking the same `FieldType` the DSL already produces — so the
 * shape is chosen now and only the source of the type changes later.
 */
export interface World {
  get(cell: string): Promise<string | null>;
  get<T>(cell: string, as: FieldType<T>): Promise<T | null>;
  set(cell: string, value: string | null): Promise<void>;
  set<T>(cell: string, value: T | null, as: FieldType<T>): Promise<void>;
}

export type PipelineBody<F extends Fields> = (
  entity: EntityContext<F>,
  world: World,
) => Promise<void>;

export interface PipelineDef<F extends Fields = Fields> {
  readonly name: string;
  readonly source: StructDef<F>;
  /** The fields this pipeline owns. Cross-checked against `derived()` at describe time. */
  readonly writes: readonly string[];
  readonly body: PipelineBody<F>;
}

export interface PipelineOptions<F extends Fields> {
  writes: readonly (keyof F & string)[];
}

/**
 * Declare a pipeline: a producer that maps over one struct, one entity at a time (§4.2).
 *
 * The body is `async` on purpose and verbosely so. Every `get` is a round trip whose result the
 * engine records as a dependency, which is what makes invalidation field-granular; preloading the
 * entity would collapse that read-set to object granularity and cost exactly the property scenario
 * 030 demonstrates. Property-access sugar over `worker_thread` + `Atomics.wait` is the later path,
 * and it does not change any of this.
 */
export function pipeline<F extends Fields>(
  name: string,
  source: StructDef<F>,
  options: PipelineOptions<F>,
  body: PipelineBody<F>,
): PipelineDef<F> {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
    throw new BorgDefinitionError(`\`${name}\` is not a usable pipeline name`);
  }
  return { name, source, writes: [...options.writes], body };
}

export interface RepoConfig {
  /**
   * The repo id. Optional, and cross-checked rather than used: `borg.toml` is authoritative,
   * because a repo is a directory and one directory has one id however many modules it contains.
   * Stating it here gets that copy verified at push time instead of quietly ignored.
   */
  id?: number;
  structs: readonly StructDef[];
  pipelines: readonly PipelineDef[];
}

/**
 * Assemble the `describe` payload, refusing anything the engine would refuse later — or worse,
 * would accept and leave unwritable.
 */
export function describe(config: RepoConfig): Description {
  const structs = new Map<string, StructDef>();
  for (const def of config.structs) {
    if (structs.has(def.name)) {
      throw new BorgDefinitionError(`struct \`${def.name}\` is declared twice in this repo`);
    }
    structs.set(def.name, def);
  }

  // Which pipeline claims which field. Built first, because the two cross-checks below are its two
  // directions and both need the whole map.
  const owners = new Map<string, string>();
  const seen = new Set<string>();
  for (const p of config.pipelines) {
    if (seen.has(p.name)) {
      throw new BorgDefinitionError(`pipeline \`${p.name}\` is declared twice in this repo`);
    }
    seen.add(p.name);

    if (structs.get(p.source.name) !== p.source) {
      throw new BorgDefinitionError(
        `pipeline \`${p.name}\` maps over \`${p.source.name}\`, which this repo does not list in ` +
          `\`structs\` — a producer's source struct has to be declared by the repo that implements it`,
      );
    }
    if (p.writes.length === 0) {
      throw new BorgDefinitionError(
        `pipeline \`${p.name}\` declares no \`writes\`, so nothing could ever invoke it`,
      );
    }

    for (const field of p.writes) {
      const what = `${p.source.name}.${field}`;
      const declared = p.source.fields[field];
      if (declared === undefined) {
        throw new BorgDefinitionError(
          `pipeline \`${p.name}\` writes \`${what}\`, which \`${p.source.name}\` does not declare`,
        );
      }
      // Direction one: a claim on a field nobody marked derived. The engine validates every write
      // against declared ownership (§8), so this pipeline would be refused at its first invocation.
      if (!declared.derivedField) {
        throw new BorgDefinitionError(
          `pipeline \`${p.name}\` writes \`${what}\`, which is not declared derived() — ` +
            `add .derived() to the field, or drop it from \`writes\``,
        );
      }
      const already = owners.get(what);
      if (already !== undefined) {
        // Single writer per field is what lets derived layers commit concurrently without
        // conflicting (§16.3). Two claims is not a merge, it is a design mistake.
        throw new BorgDefinitionError(
          `\`${what}\` is written by both \`${already}\` and \`${p.name}\` — a field has one writer`,
        );
      }
      owners.set(what, p.name);
    }
  }

  const specs: StructSpec[] = [];
  for (const def of structs.values()) {
    const fields: FieldSpec[] = [];
    for (const [field, type] of Object.entries(def.fields)) {
      const what = `${def.name}.${field}`;
      const owner = owners.get(what);
      // Direction two: a field declared derived that nothing implements. Clients may not write it
      // (§8) and no producer ever will, so it is a cell that can only ever be empty.
      if (type.derivedField && owner === undefined) {
        throw new BorgDefinitionError(
          `\`${what}\` is declared derived() but no pipeline in this repo writes it — ` +
            `add it to a pipeline's \`writes\`, or drop .derived()`,
        );
      }
      fields.push(
        owner === undefined
          ? { name: field, type: type.wireType }
          : { name: field, type: type.wireType, derived_by: owner },
      );
    }
    specs.push({ name: def.name, fields });
  }

  const description: Description = {
    structs: specs,
    producers: config.pipelines.map((p) => ({ name: p.name, source: p.source.name })),
    // Declared, so the engine knows before it spawns anything that this worker's stdout is its own.
    transport: "socket",
  };
  if (config.id !== undefined) description.repo = config.id;
  return description;
}
