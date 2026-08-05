/**
 * The wire messages, as this side of the connection sees them. `crates/borg-protocol` is the
 * contract; this is a transcription of it, and the scenarios are what keep the two honest.
 *
 * **Every message is a single-key object**, including the payload-free ones, so dispatching is one
 * property lookup with no special cases (§17.4).
 */

export interface ServerHello {
  version: number;
  codecs: string[];
}

export type ToWorker =
  | { invoke: { producer: string; input: string } }
  | { value: string | null }
  | { ok: Record<string, never> }
  | { shutdown: Record<string, never> };

export type FromWorker =
  | { get: string }
  | { get_input: string }
  | { set: { cell: string; value: string } }
  | { done: Record<string, never> }
  | { error: { message: string } };

/** What a repo reports when asked to `describe` itself (§17.4). */
export interface Description {
  structs: StructSpec[];
  producers: ProducerSpec[];
  migrations?: MigrationSpec[];
  transport: "stdio" | "socket";
  repo?: number;
}

export interface StructSpec {
  name: string;
  fields: FieldSpec[];
}

export interface FieldSpec {
  name: string;
  type: string;
  derived_by?: string;
  up?: string;
  down?: string;
}

export interface ProducerSpec {
  name: string;
  source: string;
}

export interface MigrationSpec {
  name: string;
}

/**
 * The id the engine derives from a producer's name (§9.2), computed here so a repo serving several
 * pipelines can dispatch an `invoke` to the right one.
 *
 * FNV-1a over 64 bits, in `bigint` because that is the only JS type that can hold the answer. The
 * engine sends the id **as a string** for the same reason: read as a JSON number it would round to
 * 53 bits and name a producer that does not exist.
 */
export function producerId(name: string): string {
  const MASK = (1n << 64n) - 1n;
  let hash = 0xcbf29ce484222325n;
  for (const byte of new TextEncoder().encode(name)) {
    hash = (hash ^ BigInt(byte)) & MASK;
    hash = (hash * 0x100000001b3n) & MASK;
  }
  // Kept away from the small ids a human might type into a def file, exactly as the engine does.
  return String(hash | (1n << 32n));
}
