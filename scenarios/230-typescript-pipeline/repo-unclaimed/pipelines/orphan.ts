#!/usr/bin/env node
// A repo that cannot be pushed, and says why before the engine ever sees it.
//
// `score` is declared `derived()`, which means clients may not write it (§8) — and no pipeline here
// claims it, so no producer ever will either. That is a cell which could only ever be empty. The
// SDK refuses to describe such a repo at all, so the push fails at `describe` with a message naming
// the field and both ways out.

import { borg } from "borg-sdk";

const Company = borg.struct("Company", {
  headcount: borg.int(),
  isInvestible: borg.bool().derived(),
  score: borg.int().derived(),
});

const invest = borg.pipeline(
  "invest",
  Company,
  { writes: ["isInvestible"] },
  async (c) => {
    await c.set("isInvestible", (await c.get("headcount")) !== null);
  },
);

await borg.repo({ id: 7, structs: [Company], pipelines: [invest] }).main();
