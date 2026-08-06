/**
 * Describe assembly: what a repo reports, and everything it refuses to report.
 *
 * The payload is the whole contract with `borg repo push` (§17.4), and the cross-checks are §2.2 of
 * the SDK draft made real — ownership is stated twice, on the field and on the pipeline, and the two
 * statements have to agree in both directions.
 */

import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe as suite, expect, test } from "vitest";
import { borg } from "../src/index.js";
import { BorgDefinitionError, describe } from "../src/dsl.js";
import { producerId } from "../src/protocol.js";
import { fingerprint } from "../src/repo.js";

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

/**
 * §9.2's *pushing new pipeline source moves the producer's ClientVersion* needs something that moves
 * when the source does. The diff compares name, source buffer and writes, and an edited body touches
 * none of them — so without this a code change is invisible to the push and the old output goes on
 * being served labelled `current`.
 */
suite("the implementation fingerprint", () => {
  const scratch = (body: string): string => {
    const path = join(mkdtempSync(join(tmpdir(), "borg-fingerprint-")), "pipeline.ts");
    writeFileSync(path, body);
    return path;
  };

  test("it changes when the entry module's bytes change", () => {
    const before = scratch("// one\n");
    const after = scratch("// two\n");
    expect(fingerprint(before)).not.toEqual(fingerprint(after));
  });

  test("it does not change when the code does not", () => {
    const once = scratch("// same\n");
    const again = scratch("// same\n");
    // Two files, one program. Content and not path, so a repo checked out somewhere else is not a
    // repo whose code changed.
    expect(fingerprint(once)).toEqual(fingerprint(again));
    expect(fingerprint(once)).toEqual(fingerprint(once));
  });

  test("it says what produced it", () => {
    expect(fingerprint(scratch("x"))).toMatch(/^sha256:[0-9a-f]{64}$/);
  });

  /**
   * The push falls back to hashing the command file, so an unreadable entry loses nothing — and
   * refusing to describe over it would make an SDK repo un-pushable for a reason that does not
   * matter.
   */
  test("an entry module that cannot be read yields no fingerprint at all", () => {
    expect(fingerprint(join(tmpdir(), "borg-no-such-file-ever"))).toBeUndefined();
  });

  test("every producer a repo describes carries it", () => {
    const { Company, invest } = investing();
    const described = borg.repo({ structs: [Company], pipelines: [invest] }).describe();
    expect(described.producers[0]!.fingerprint).toMatch(/^sha256:[0-9a-f]{64}$/);
  });

  /**
   * `describe` is a pure function of the definitions and stays one: the fingerprint is a fact about
   * a file, so it is attached by `repo()`, which is already the half that reads argv.
   */
  test("the pure describe payload carries none, because it is not a fact about the DSL", () => {
    const { Company, invest } = investing();
    expect(describe({ structs: [Company], pipelines: [invest] }).producers[0]).not.toHaveProperty(
      "fingerprint",
    );
  });
});
