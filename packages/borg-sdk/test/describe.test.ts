/**
 * Describe assembly: what a repo reports, and everything it refuses to report.
 *
 * The payload is the whole contract with `borg repo push` (§17.4), and the cross-checks are §2.2 of
 * the SDK draft made real — ownership is stated twice, on the field and on the pipeline, and the two
 * statements have to agree in both directions.
 */

import { describe as suite, expect, test } from "vitest";
import { borg } from "../src/index.js";
import { BorgDefinitionError, describe } from "../src/dsl.js";
import { producerId } from "../src/protocol.js";

const investing = () => {
  const Company = borg.struct("Company", {
    website: borg.string(),
    headcount: borg.int(),
    employees: borg.list(borg.ref("Employee")),
    isInvestible: borg.bool().derived(),
  });
  const invest = borg.pipeline("invest", Company, { writes: ["isInvestible"] }, async (c) => {
    await c.set("isInvestible", (await c.get("headcount")) !== null);
  });
  return { Company, invest };
};

suite("what a repo reports", () => {
  test("it is the shape borg repo push reads, field types and all", () => {
    const { Company, invest } = investing();
    expect(describe({ id: 2, structs: [Company], pipelines: [invest] })).toEqual({
      structs: [
        {
          name: "Company",
          fields: [
            { name: "website", type: "String" },
            { name: "headcount", type: "Int" },
            { name: "employees", type: "Employee[]" },
            { name: "isInvestible", type: "Bool", derived_by: "invest" },
          ],
        },
      ],
      producers: [{ name: "invest", source: "Company" }],
      transport: "socket",
      repo: 2,
    });
  });

  /**
   * A socket is the only arrangement in which a `console.log` is survivable, so every repo written
   * with this SDK asks for one — and asks *in describe*, which is the one thing the engine reads
   * before it decides how to spawn the worker.
   */
  test("it always declares the socket transport", () => {
    const { Company, invest } = investing();
    expect(describe({ structs: [Company], pipelines: [invest] }).transport).toBe("socket");
  });

  /** The repo id is a cross-check, so saying nothing is saying nothing rather than saying zero. */
  test("it omits the repo id when the author did not state one", () => {
    const { Company, invest } = investing();
    expect(describe({ structs: [Company], pipelines: [invest] })).not.toHaveProperty("repo");
  });

  /** `derived_by` names the pipeline by name; the engine resolves it to the same id it assigns. */
  test("ownership names the pipeline the engine will hash", () => {
    const { Company, invest } = investing();
    const [field] = describe({ structs: [Company], pipelines: [invest] }).structs[0]!.fields.filter(
      (f) => f.name === "isInvestible",
    );
    expect(field!.derived_by).toBe("invest");
    // The id the engine derives from that name — the same FNV-1a, past 2⁵³, which is why it
    // crosses the wire as a string.
    expect(producerId("invest")).toBe("12342029420047889112");
  });
});

suite("the cross-checks, in both directions", () => {
  test("a derived field no pipeline writes is refused, because nothing could ever write it", () => {
    const Company = borg.struct("Company", {
      headcount: borg.int(),
      isInvestible: borg.bool().derived(),
      score: borg.int().derived(),
    });
    const invest = borg.pipeline("invest", Company, { writes: ["isInvestible"] }, async () => {});
    expect(() => describe({ structs: [Company], pipelines: [invest] })).toThrow(BorgDefinitionError);
    expect(() => describe({ structs: [Company], pipelines: [invest] })).toThrow(
      /`Company\.score` is declared derived\(\) but no pipeline/,
    );
  });

  test("a pipeline writing a field nobody marked derived is refused too", () => {
    const Company = borg.struct("Company", { headcount: borg.int() });
    const invest = borg.pipeline("invest", Company, { writes: ["headcount"] }, async () => {});
    expect(() => describe({ structs: [Company], pipelines: [invest] })).toThrow(
      /not declared derived\(\)/,
    );
  });

  test("a pipeline writing a field the struct does not have is refused", () => {
    const Company = borg.struct("Company", { headcount: borg.int() });
    const invest = borg.pipeline(
      "invest",
      Company,
      // Only reachable by defeating the types, which is what a JS caller does.
      { writes: ["isInvestible" as never] },
      async () => {},
    );
    expect(() => describe({ structs: [Company], pipelines: [invest] })).toThrow(/does not declare/);
  });

  /** Single writer per field is what lets derived layers commit concurrently (§16.3). */
  test("two pipelines claiming one field is refused, and both are named", () => {
    const Company = borg.struct("Company", { isInvestible: borg.bool().derived() });
    const first = borg.pipeline("invest", Company, { writes: ["isInvestible"] }, async () => {});
    const second = borg.pipeline("score", Company, { writes: ["isInvestible"] }, async () => {});
    expect(() => describe({ structs: [Company], pipelines: [first, second] })).toThrow(
      /both `invest` and `score`/,
    );
  });

  test("a pipeline over a struct the repo does not declare is refused", () => {
    const Company = borg.struct("Company", { isInvestible: borg.bool().derived() });
    const invest = borg.pipeline("invest", Company, { writes: ["isInvestible"] }, async () => {});
    expect(() => describe({ structs: [], pipelines: [invest] })).toThrow(/does not list in/);
  });

  test("a pipeline that writes nothing is refused, because nothing could invoke it", () => {
    const Company = borg.struct("Company", { headcount: borg.int() });
    const idle = borg.pipeline("idle", Company, { writes: [] }, async () => {});
    expect(() => describe({ structs: [Company], pipelines: [idle] })).toThrow(/no `writes`/);
  });

  test("a name declared twice in one repo is refused", () => {
    const Company = borg.struct("Company", { isInvestible: borg.bool().derived() });
    const invest = borg.pipeline("invest", Company, { writes: ["isInvestible"] }, async () => {});
    expect(() => describe({ structs: [Company, Company], pipelines: [invest] })).toThrow(
      /declared twice/,
    );
    expect(() => describe({ structs: [Company], pipelines: [invest, invest] })).toThrow(
      /declared twice/,
    );
  });

  /** Validation runs when the repo is defined, so `describe` fails as loudly as an invocation. */
  test("borg.repo refuses at definition time, not at the first invocation", () => {
    const Company = borg.struct("Company", { score: borg.int().derived() });
    expect(() => borg.repo({ structs: [Company], pipelines: [] })).toThrow(BorgDefinitionError);
  });
});
