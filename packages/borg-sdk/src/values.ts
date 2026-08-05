/**
 * Field types, and the text forms their values take on the wire.
 *
 * **Values cross the wire as text** — `42`, `true`, `~`, `acme.ai`, `@o-1234abcd` — the same forms
 * the CLI accepts and `borg_core::parse` reads (SPEC §3.4, §17.4). A worker never sends a JSON
 * number: JSON has one number type and the engine has `Int`, `Double` and `BigInt`, so a number on
 * the wire would need a rule to disambiguate it and every language's rule would be slightly
 * different. Text has no such gap.
 *
 * Each field type is therefore a pair of functions, `decode` and `encode`, plus the name the
 * declared type goes by in a `describe` payload. Nothing here reaches the network, and nothing here
 * remembers anything: conversion is total and local.
 */

/** A reference to another entity. On the wire, `@` and a PID. */
export class Ref {
  readonly pid: string;

  constructor(pid: string) {
    this.pid = pid;
    if (pid.startsWith("@")) {
      throw new BorgValueError(`a Ref holds a PID, not its wire form — drop the @ from \`${pid}\``);
    }
  }

  /** The wire form: `@o-1234abcd`. */
  toString(): string {
    return `@${this.pid}`;
  }

  /** The cell this reference names, so a pipeline can hop to one of its fields. */
  cell(struct: string, field?: string): string {
    return field === undefined ? `${struct}:${this.pid}` : `${struct}:${this.pid}.${field}`;
  }
}

/** Something the SDK refused to convert, named with enough detail to fix it. */
export class BorgValueError extends Error {
  override readonly name = "BorgValueError";
}

/**
 * A declared field type: what it is called in `describe`, and how its values convert.
 *
 * `decode`/`encode` never see absence. A cell that has never been written, and a cell holding a
 * tombstone, both read as `null` and are written back as `null`; that collapse happens one level up,
 * in the pipeline context, because it is the same rule for every type (§8.1).
 */
export interface FieldType<T> {
  /** `Int`, `String`, `Company`, `Employee[]` — what `describe` calls this. */
  readonly wireType: string;
  /** Whether a pipeline owns this field, rather than clients writing it (§8). */
  readonly derivedField: boolean;
  /** Declare that a pipeline writes this field. Returns a new type; the receiver is unchanged. */
  derived(): FieldType<T>;
  decode(text: string): T;
  encode(value: T): string;
}

/** Any field type, for places that hold a heterogeneous collection of them. */
// The value parameter is genuinely unknown at those sites; `any` is what lets a `FieldType<string>`
// and a `FieldType<number>` sit in one record without erasing the type at every *use* site.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type AnyFieldType = FieldType<any>;

/** The JS type a field type carries. */
export type ValueOf<F> = F extends FieldType<infer T> ? T : never;

function make<T>(
  wireType: string,
  decode: (text: string) => T,
  encode: (value: T) => string,
  derivedField = false,
): FieldType<T> {
  return {
    wireType,
    derivedField,
    decode,
    encode,
    derived: () => make(wireType, decode, encode, true),
  };
}

const fail = (reason: string): never => {
  throw new BorgValueError(reason);
};

/**
 * `Int` is an `i64`, and a JS number is a double: everything past 2⁵³ is representable and wrong.
 *
 * Refusing is the only honest answer. Returning the rounded number would be data that looks almost
 * right, which `parse.rs` names as the worst available outcome and refuses in the same place; the
 * field can be declared `bigint()` instead, which is one word away.
 */
const INT = /^[+-]?\d+$/;

export function string(): FieldType<string> {
  return make(
    "String",
    (text) => text,
    (value) => {
      if (typeof value !== "string") {
        return fail(`a String field takes a string, not ${describeJs(value)}`);
      }
      // `~` is a tombstone on every declared type (§8.1), so a String field cannot hold those two
      // characters: the engine would read the write back as a deletion. Refusing beats writing a
      // value that does not read back, and `null` is how a pipeline means deletion here.
      if (value === "~") {
        return fail(
          "`~` is the tombstone form on every field, so it cannot be stored as a string — " +
            "pass null to delete the cell",
        );
      }
      return value;
    },
  );
}

export function int(): FieldType<number> {
  return make(
    "Int",
    (text) => {
      if (!INT.test(text)) {
        return fail(`an Int field answered \`${text}\`, which is not a whole number`);
      }
      const value = Number(text);
      if (!Number.isSafeInteger(value)) {
        return fail(
          `\`${text}\` does not fit a JS number without losing digits — declare the field bigint()`,
        );
      }
      return value;
    },
    (value) => {
      if (typeof value !== "number" || !Number.isInteger(value)) {
        return fail(`an Int field takes a whole number, not ${describeJs(value)}`);
      }
      if (!Number.isSafeInteger(value)) {
        return fail(`${value} has already lost digits as a JS number — use bigint() for this field`);
      }
      return String(value);
    },
  );
}

export function double(): FieldType<number> {
  return make(
    "Double",
    (text) => {
      const value = Number(text);
      if (text.trim() === "" || !Number.isFinite(value)) {
        return fail(`a Double field answered \`${text}\`, which is not a finite number`);
      }
      return value;
    },
    (value) => {
      if (typeof value !== "number" || !Number.isFinite(value)) {
        return fail(`a Double field takes a finite number, not ${describeJs(value)}`);
      }
      // `String(1)` is `"1"`, which the engine reads as the double 1.0 because it knows the field's
      // declared type. Rust renders it back the same way, so the round trip is stable.
      return String(value);
    },
  );
}

export function bool(): FieldType<boolean> {
  return make(
    "Bool",
    (text) => {
      if (text === "true") return true;
      if (text === "false") return false;
      return fail(`a Bool field answered \`${text}\`, which is neither true nor false`);
    },
    (value) => {
      if (typeof value !== "boolean") {
        return fail(`a Bool field takes a boolean, not ${describeJs(value)}`);
      }
      return String(value);
    },
  );
}

export function binary(): FieldType<Uint8Array> {
  return make(
    "Binary",
    (text) => {
      if (!/^0x(?:[0-9a-fA-F]{2})*$/.test(text)) {
        return fail(`a Binary field answered \`${text}\`, which is not \`0x\` and whole octets`);
      }
      const bytes = new Uint8Array((text.length - 2) / 2);
      for (let i = 0; i < bytes.length; i++) {
        bytes[i] = Number.parseInt(text.slice(2 + i * 2, 4 + i * 2), 16);
      }
      return bytes;
    },
    (value) => {
      if (!(value instanceof Uint8Array)) {
        return fail(`a Binary field takes a Uint8Array, not ${describeJs(value)}`);
      }
      let text = "0x";
      for (const byte of value) text += byte.toString(16).padStart(2, "0");
      return text;
    },
  );
}

export function bigint(): FieldType<bigint> {
  return make(
    "BigInt",
    (text) => {
      // The engine renders a BigInt with a trailing `n`, which is what tells an *untyped* parse a
      // BigInt from an Int. Against a declared BigInt it carries nothing, so both spellings read.
      const digits = text.endsWith("n") ? text.slice(0, -1) : text;
      if (!INT.test(digits)) {
        return fail(`a BigInt field answered \`${text}\`, which is not decimal digits`);
      }
      return BigInt(digits);
    },
    (value) => {
      if (typeof value !== "bigint") {
        return fail(`a BigInt field takes a bigint, not ${describeJs(value)}`);
      }
      // Written with the suffix even though a declared BigInt does not need it: the same text then
      // means the same value on an `Any` field, where the suffix is the only thing distinguishing
      // it from an Int.
      return `${value}n`;
    },
  );
}

/** A reference to an entity of a named struct. */
export function ref(struct: string): FieldType<Ref> {
  return make(
    struct,
    (text) => {
      if (!text.startsWith("@")) {
        return fail(`a ${struct} field answered \`${text}\`, which is not a reference`);
      }
      return new Ref(text.slice(1));
    },
    (value) => {
      if (value instanceof Ref) return value.toString();
      return fail(`a ${struct} field takes a Ref, not ${describeJs(value)}`);
    },
  );
}

/**
 * A list field. Its *value* is a reference to a list, not the elements: elements are cells of their
 * own (`Employee[]:l-….[3]`), which is what makes a list appendable without rewriting it (§4.2).
 *
 * v1 goes no further than the handle. Reading through it needs element addressing in the pipeline
 * surface, and `hasMany` — a derived reverse index — needs aggregations, which are deferred (§18).
 */
export function list<T>(element: FieldType<T>): FieldType<Ref> {
  const reference = ref(`${element.wireType}[]`);
  return make(`${element.wireType}[]`, reference.decode, reference.encode);
}

/** What the caller actually passed, for an error message that saves a debugging session. */
function describeJs(value: unknown): string {
  if (value === null) return "null (pass null through set() to delete the cell)";
  if (value === undefined) return "undefined";
  if (typeof value === "object") return `a ${value.constructor?.name ?? "object"}`;
  return `${typeof value} \`${String(value)}\``;
}

/** The tombstone form, reserved on every declared type (§8.1). */
export const TOMBSTONE = "~";
