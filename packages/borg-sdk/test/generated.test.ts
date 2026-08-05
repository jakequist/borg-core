/**
 * What generated code buys, asserted against the real thing.
 *
 * `test/generated/borg.generated.ts` is `borg generate`'s output for a fixture schema, checked in
 * unedited; `crates/borg-cli/src/generate.rs` asserts the emitter still produces exactly it. So this
 * file and that one are the two halves of one claim: *the emitter is stable* and *what it emits is
 * valid, useful TypeScript*. Either alone proves much less — a golden in Rust would be a string that
 * happened not to change, and a hand-written fixture here would be a file nobody generates.
 *
 * **The compile-time assertions are the file compiling at all.** `pnpm run typecheck` covers `test`,
 * so a wrong field name or a wrong value type below fails the build rather than a test. The runtime
 * cases are for what the types cannot say: that the descriptor converts the way the wire needs.
 */

import { describe as suite, expect, test } from "vitest";
import { Company, CLIENT_VERSION, type Employee } from "./generated/borg.generated.js";
import type { Ref } from "../src/client.js";

suite("a generated module", () => {
  test("stamps the def-layer it was generated from, which is what it connects as", () => {
    // The whole reason codegen is not merely a convenience (§5.4, SDK-DRAFT §2.4).
    expect(CLIENT_VERSION).toBe("L4");
  });

  test("carries the wire name and every field's conversion", () => {
    expect(Company.name).toBe("Company");
    expect(Company.fields.headcount.type.encode(42)).toBe("42");
    expect(Company.fields.headcount.type.decode("42")).toBe(42);
    expect(Company.fields.website.type.decode("acme.ai")).toBe("acme.ai");
    expect(Company.fields.founded.type.encode(1999n)).toBe("1999n");
    expect(Company.fields.valuation.type.decode("1")).toBe(1);
    // The conversions are the ones `src/values.ts` already had — a client and a pipeline convert an
    // Int identically, because there is one table (SDK-DRAFT §4.4).
    expect(() => Company.fields.headcount.type.encode(1.5)).toThrow(/whole number/);
  });

  test("says which fields a producer owns, and at what def-version", () => {
    expect(Company.fields.isInvestible.derived).toBe(true);
    expect(Company.fields.headcount.derived).toBe(false);
    // Per *field*, not the branch's whole-schema version (§5.3).
    expect(Company.fields.headcount.version).toBe("L4");
  });

  /**
   * A reference is the PID, branded with what it points at. The brand is a compile-time fact and
   * erases entirely — which is the point: `tx.object(Employee, ref)` takes the string it was given.
   */
  test("a reference field yields the pid a tx.object() can be opened on", () => {
    const decoded = Company.fields["lead-investor"].type.decode("@o-1234abcd");
    expect(decoded).toBe("o-1234abcd");
    expect(Company.fields["lead-investor"].type.encode(decoded)).toBe("@o-1234abcd");
    // A list field's value is a handle to the list, not its elements (§4.2).
    expect(Company.fields.employees.type.decode("@l-5678wxyz")).toBe("l-5678wxyz");
  });

  test("a field name JavaScript cannot spell bare is quoted, never renamed", () => {
    expect(Object.keys(Company.fields)).toContain("lead-investor");
  });
});

/**
 * The type-level assertions. Nothing here runs; it fails at `pnpm run typecheck` or not at all,
 * which is the same assertion `scenarios/260` makes against a real store with a real `tsc`.
 */
function _typeChecks(company: Company, employee: Employee): void {
  const headcount: number | null = company.headcount;
  const investible: boolean | null = company.isInvestible;
  const website: string | null = company.website;
  const lead: Ref<"Employee"> | null = company["lead-investor"];
  const staff: Ref<"Employee[]"> | null = company.employees;
  const name: string | null = employee.name;
  void [headcount, investible, website, lead, staff, name];

  // @ts-expect-error a Company reference is not an Employee reference, which is what makes
  // `tx.object(Employee, …)` checkable at all (SDK-DRAFT §5).
  const wrong: Ref<"Company"> | null = company["lead-investor"];
  void wrong;

  // @ts-expect-error `isInvestible` is derived, and generated code marks it `readonly` (§8, §15).
  company.isInvestible = true;

  // @ts-expect-error a Company has no `revenue`.
  void company.revenue;
}
void _typeChecks;
