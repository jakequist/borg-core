/**
 * The client wire messages, as this side of the connection sees them. SPEC.md §17.5.
 *
 * `crates/borg-protocol/src/client.rs` is the contract; this is a transcription of it, and the
 * scenarios are what keep the two honest. As with §17.4, **every message is a single-key object**,
 * including the payload-free ones, so dispatching is one property lookup with no special cases.
 *
 * **Nothing here is renamed.** `fresh_as_of` stays `fresh_as_of` and never becomes `freshAsOf`, for
 * the same reason field names are used verbatim in the DSL (SDK-DRAFT §4.1): a case conversion is
 * invisible in both directions and is a mapping somebody has to reverse-engineer from an error. It
 * also means what this SDK hands back is comparable, key for key, with `jq` on the same socket and
 * with what `borg get` prints.
 */

/** The server speaks first, and always in JSON. */
export interface ServerHello {
  version: number;
  codecs: string[];
}

export interface ClientHello {
  version: number;
  codec: "json";
  /** The def-layer this client's generated code was built from. Absent means the branch head. */
  client_version?: string;
  /**
   * **Which registry on this server the connection is for** (§17.6). Settled once, here, because
   * the registry is what a connection is *to* — repeating it per message would put a tenancy
   * decision on every line.
   *
   * **Absent means the server's sole registry, when it has exactly one**, and is refused with the
   * options when it hosts more. That rule lives in the server and is deliberately not mirrored
   * here: a client that guessed would be re-implementing half of it.
   */
  registry?: string;
}

export type Request =
  | { tx_begin: { branch?: string | undefined } }
  | { tx_get: { tx: string; cell: string; freshness?: string | undefined } }
  | { tx_set: { tx: string; cell: string; value: string } }
  | { tx_commit: { tx: string } }
  | { tx_abort: { tx: string } }
  | {
      get: {
        branch?: string | undefined;
        cell: string;
        freshness?: string | undefined;
        settled?: boolean;
      };
    }
  | { explain: { branch?: string | undefined; cell: string } }
  /** Every object of one struct, as ids. Read-only, at head, outside any transaction (§9.6). */
  | { list: { branch?: string | undefined; struct: string } }
  /** Allocate an object and write its existence cell, in the transaction, in one step (§3.1, §8). */
  | { tx_create: { tx: string; struct: string } }
  | { branch_list: Record<string, never> }
  | { branch_head: { branch?: string | undefined } }
  | { def_show: { branch?: string | undefined; struct: string } }
  | { def_view: { branch?: string | undefined } };

export type Response =
  | { tx: { tx: string } }
  | { cell: WireEnvelope }
  | { ok: Record<string, never> }
  | { committed: { landed: string } }
  | { conflict: { cell: string | null; reason: string; message: string } }
  | { branches: BranchInfo[] }
  /** Canonical PID text, sorted. Ids and nothing else — see the client protocol's `List`. */
  | { ids: string[] }
  | { created: { id: string } }
  | { head: { branch: string; layer: string } }
  | { def: WireStructDef }
  | { defs: WireSchemaDef }
  | { lineage: WireLineage }
  | { error: { message: string } };

/**
 * The §10.4 read envelope, exactly as it comes off the wire. A read is **never a bare value**:
 * derived data is never presented as fresh (invariant 8), so what it reflects travels with it.
 */
export interface WireEnvelope {
  cell: string;
  /** Absent means never written; a tombstone arrives as `"~"` with `state: "tombstoned"`. */
  value: string | null;
  origin: string;
  state: string;
  event: string | null;
  authored_at: string;
  landed_at: string;
  fresh_as_of: string;
  by: string | null;
}

export interface BranchInfo {
  id: string;
  name: string | null;
  forked_at: string | null;
}

export interface WireStructDef {
  name: string;
  fields: WireFieldDef[];
}

export interface WireFieldDef {
  name: string;
  type: string;
  /** The producer that owns this field, by id. Absent means source data (§8). */
  derived_by: string | null;
  repo: number;
  /** The def-version of *this field* (§5.3) — not the branch's whole-schema version. */
  version: string;
}

/** The whole def view of a branch, and the ClientVersion a module generated from it would carry. */
export interface WireSchemaDef {
  version: string;
  structs: WireStructDef[];
}

export interface WireLineage {
  cell: string;
  produced_by: string | null;
  authored_at: string;
  landed_at: string;
  fresh_as_of: string;
  broken: string | null;
  from: { cell: string; origin: string; landed_at: string }[];
}
