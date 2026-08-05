#!/usr/bin/env node
// A Borg pipeline, in TypeScript. The twin of `030-shell-pipeline`'s bash worker.
//
// Node runs this file directly: since 22.18 it strips the types rather than compiling them, so the
// authored artifact and the pushed artifact are the same file. There is no build step, no `dist/`,
// and nothing between what you read and what runs.
//
// Note what is *not* here. No tracking, no cache, no dependency declaration — every `get` below is a
// wire message and the engine records the read-set server-side, which is why writing a field this
// pipeline never read re-runs nothing at all.

import { borg } from "borg-sdk";

const Employee = borg.struct("Employee", {
  name: borg.string(),
});

const Company = borg.struct("Company", {
  website: borg.string(),
  headcount: borg.int(),
  // Declared and never read: what makes field-granular invalidation observable rather than asserted.
  foundedYear: borg.int(),
  // A list field's value is a handle to the list, not its elements (§4.2). Declared here so the
  // `Employee[]` type survives the round trip through `describe` and `borg def show`.
  employees: borg.list(borg.ref("Employee")),
  // Ownership, stated on the field. The pipeline states it again in `writes`, and the two are
  // cross-checked in both directions when this module assembles its description.
  isInvestible: borg.bool().derived(),
});

const invest = borg.pipeline(
  "invest",
  Company,
  { writes: ["isInvestible"] },
  async (c) => {
    // stdout belongs to the author now. Over stdio this line would corrupt the message stream
    // permanently and the failure would surface somewhere else entirely; over the socket the engine
    // offers, it is just a line on the terminal.
    console.log(`[invest] scoring ${c.ref}`);

    let score = 0;

    // A string arrives as its content — `acme.ai`, not the `@s-…` that is physically stored. This
    // pipeline never learns that interning exists.
    const website = await c.get("website");
    if (website !== null && website.endsWith(".ai")) score += 6;

    // …and a number arrives as a number. One text form on the wire, several types behind it.
    const headcount = await c.get("headcount");
    if (headcount !== null && headcount > 10) score += 2;

    console.log(`[invest] ${c.ref} scored ${score}`);

    // Deliberately needs both, so either field moving can flip the answer.
    await c.set("isInvestible", score >= 7);
  },
);

await borg
  .repo({ id: 1, structs: [Company, Employee], pipelines: [invest] })
  .main();
