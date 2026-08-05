/**
 * # borg-sdk
 *
 * Author a Borg repo in TypeScript: declare structs, write pipelines, and serve them to the engine.
 *
 * ```ts
 * #!/usr/bin/env node
 * import { borg } from "borg-sdk";
 *
 * const Company = borg.struct("Company", {
 *   website: borg.string(),
 *   headcount: borg.int(),
 *   isInvestible: borg.bool().derived(),
 * });
 *
 * const isInvestible = borg.pipeline(
 *   "invest", Company, { writes: ["isInvestible"] },
 *   async (c) => {
 *     const website = await c.get("website");
 *     const headcount = await c.get("headcount");
 *     await c.set("isInvestible", website?.endsWith(".ai") === true && (headcount ?? 0) > 10);
 *   },
 * );
 *
 * await borg.repo({ id: 1, structs: [Company], pipelines: [isInvestible] }).main();
 * ```
 *
 * The module is the whole repo: run with `describe` it prints its definitions, run without it
 * serves invocations over the socket the engine offers in `BORG_WORKER_SOCKET`.
 *
 * **This SDK records nothing.** Each `get` and `set` is a wire message and the engine records the
 * read-set server-side, which is what makes invalidation field-granular without a line of client
 * code. There is no cache to invalidate and no tracking to get wrong.
 */

import { pipeline, struct } from "./dsl.js";
import { repo as makeRepo } from "./repo.js";
import { binary, bigint, bool, double, int, list, ref, string } from "./values.js";

export {
  BorgValueError,
  Ref,
  type AnyFieldType,
  type FieldType,
  type ValueOf,
} from "./values.js";
export {
  BorgDefinitionError,
  describe,
  type EntityContext,
  type Fields,
  type PipelineDef,
  type PipelineOptions,
  type RepoConfig,
  type StructDef,
  type World,
} from "./dsl.js";
export { BorgProtocolError, SOCKET_ENV } from "./connection.js";
export { producerId, type Description } from "./protocol.js";
export type { Repo } from "./repo.js";

/**
 * The DSL, as one namespace.
 *
 * A single import is what a pipeline file wants: it is one screen of code and every symbol in it is
 * from here, so `borg.string()` reads better than eight named imports and stays stable as the
 * surface grows.
 */
export const borg = {
  struct,
  pipeline,
  repo: makeRepo,

  string,
  int,
  double,
  bool,
  binary,
  bigint,
  ref,
  list,
} as const;

// Named exports too, for anyone who prefers them or is tree-shaking.
export { struct, pipeline, binary, bigint, bool, double, int, list, ref, string };
export { makeRepo as defineRepo };
