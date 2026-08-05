/**
 * The conversion table, in both directions.
 *
 * These are the rules `borg_core::parse` applies on the other side of the wire, restated here for a
 * language whose only number is a double. Where the two could disagree, the SDK refuses rather than
 * guesses — a value that looks almost right is the worst available outcome, because nothing
 * complains and the data is wrong.
 */

import { describe, expect, test } from "vitest";
import { bigint, binary, bool, double, int, list, ref, Ref, string } from "../src/values.js";

describe("the text forms values take on the wire", () => {
  test("every type round trips through the text the engine renders", () => {
    const cases: [{ encode: (v: never) => string; decode: (t: string) => unknown }, unknown, string][] = [
      [string(), "acme.ai", "acme.ai"],
      [string(), "", ""],
      // Against a *declared* String there are no reserved spellings, so these are their characters.
      [string(), "true", "true"],
      [string(), "42", "42"],
      [string(), "@jake", "@jake"],
      [int(), 42, "42"],
      [int(), -1, "-1"],
      [int(), 0, "0"],
      [double(), 1.5, "1.5"],
      [double(), -0.25, "-0.25"],
      [bool(), true, "true"],
      [bool(), false, "false"],
      [bigint(), 170141183460469231731687303715884105728n, "170141183460469231731687303715884105728n"],
      [bigint(), -129n, "-129n"],
      [binary(), new Uint8Array([0xde, 0xad, 0xbe, 0xef]), "0xdeadbeef"],
      [binary(), new Uint8Array(), "0x"],
      [ref("Company"), new Ref("o-1234abcd"), "@o-1234abcd"],
      [list(ref("Employee")), new Ref("l-5678wxyz"), "@l-5678wxyz"],
    ];

    for (const [type, value, text] of cases) {
      expect(type.encode(value as never), `encode ${String(value)}`).toBe(text);
      expect(type.decode(text), `decode ${text}`).toEqual(value);
    }
  });

  /** Rust renders `1.0` as `1`, and a Double field must read that back as the number it is. */
  test("a whole double survives the engine rendering it without a point", () => {
    expect(double().decode("1")).toBe(1);
    expect(double().encode(1)).toBe("1");
    expect(double().decode("1e-7")).toBe(1e-7);
    expect(double().encode(1e30)).toBe("1e+30");
  });

  /** The suffix tells an *untyped* parse a BigInt from an Int, and a declared BigInt accepts both. */
  test("a bigint reads with or without its suffix and always writes with it", () => {
    expect(bigint().decode("-129")).toBe(-129n);
    expect(bigint().decode("-129n")).toBe(-129n);
    expect(bigint().encode(-129n)).toBe("-129n");
  });

  test("the declared type is what a describe payload names", () => {
    expect(string().wireType).toBe("String");
    expect(int().wireType).toBe("Int");
    expect(double().wireType).toBe("Double");
    expect(bool().wireType).toBe("Bool");
    expect(binary().wireType).toBe("Binary");
    expect(bigint().wireType).toBe("BigInt");
    expect(ref("Employee").wireType).toBe("Employee");
    expect(list(ref("Employee")).wireType).toBe("Employee[]");
    expect(list(string()).wireType).toBe("String[]");
  });
});

describe("what the SDK refuses rather than corrupts", () => {
  /**
   * `Int` is an `i64` and a JS number is a double. Everything past 2⁵³ is representable and wrong,
   * so both directions refuse it and both name the field type that would work.
   */
  test("an Int past what a JS number can hold is refused in both directions", () => {
    expect(() => int().decode("9007199254740993")).toThrow(/bigint\(\)/);
    expect(() => int().encode(9007199254740993)).toThrow(/bigint\(\)/);
    // The largest value that is still exactly itself is fine.
    expect(int().encode(9007199254740991)).toBe("9007199254740991");
  });

  test("an Int field refuses a value that is not a whole number", () => {
    expect(() => int().encode(1.5)).toThrow(/whole number/);
    expect(() => int().decode("1.5")).toThrow(/whole number/);
    expect(() => int().decode("acme")).toThrow(/whole number/);
  });

  /** The engine refuses non-finite doubles, so writing one has to fail here and not there. */
  test("a Double field refuses infinities and NaN", () => {
    expect(() => double().encode(Number.NaN)).toThrow(/finite/);
    expect(() => double().encode(Number.POSITIVE_INFINITY)).toThrow(/finite/);
    expect(() => double().decode("nan")).toThrow(/finite/);
    expect(() => double().decode("")).toThrow(/finite/);
  });

  /**
   * `~` is the tombstone on every declared type (§8.1), so a String field cannot hold those two
   * characters — the write would read back as a deletion. Refusing beats writing something that
   * does not read back.
   */
  test("a String field refuses the tombstone spelling and points at null", () => {
    expect(() => string().encode("~")).toThrow(/pass null/);
  });

  test("a type refuses a JS value it cannot carry, and says what it got", () => {
    expect(() => bool().encode("true" as never)).toThrow(/string/);
    expect(() => int().encode("42" as never)).toThrow(/string/);
    expect(() => binary().encode([1, 2] as never)).toThrow(/Uint8Array/);
    expect(() => bigint().encode(1 as never)).toThrow(/bigint/);
    expect(() => ref("Company").encode("o-1" as never)).toThrow(/Ref/);
  });

  test("a malformed answer from the engine is an error, not a value that looks almost right", () => {
    expect(() => bool().decode("yes")).toThrow();
    expect(() => binary().decode("deadbeef")).toThrow(/whole octets/);
    expect(() => binary().decode("0xf")).toThrow(/whole octets/);
    expect(() => ref("Company").decode("o-1234abcd")).toThrow(/reference/);
  });

  test("a Ref holds a PID and not its wire form", () => {
    expect(() => new Ref("@o-1234abcd")).toThrow(/drop the @/);
  });
});

describe("derived() marks the field and nothing else", () => {
  test("it returns a new type, so a shared field type is not contaminated", () => {
    const shared = bool();
    const owned = shared.derived();
    expect(owned.derivedField).toBe(true);
    expect(shared.derivedField).toBe(false);
    expect(owned.wireType).toBe(shared.wireType);
    expect(owned.encode(true)).toBe("true");
  });
});

describe("a Ref names cells", () => {
  test("it builds the address of an entity and of one of its fields", () => {
    const employee = new Ref("o-1234abcd");
    expect(employee.cell("Employee")).toBe("Employee:o-1234abcd");
    expect(employee.cell("Employee", "name")).toBe("Employee:o-1234abcd.name");
  });
});
