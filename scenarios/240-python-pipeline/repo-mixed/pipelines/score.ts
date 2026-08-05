#!/usr/bin/env node
// Half of a repo. The other half is `summarise.py`, and the engine is never told which is which.
//
// One directory, one `borg.toml`, one `borg repo push`, one def layer. `borg repo push` asks every
// file in `pipelines/` to describe itself and folds all the answers together, so what a repo is
// written in is a fact about a file rather than about the repo.

import { borg } from "borg-sdk";

const Startup = borg.struct("Startup", {
  domain: borg.string(),
  staff: borg.int(),
  promising: borg.bool().derived(),
});

const score = borg.pipeline(
  "score",
  Startup,
  { writes: ["promising"] },
  async (c) => {
    console.log(`[score] ${c.ref}`);
    const domain = await c.get("domain");
    const staff = await c.get("staff");
    await c.set("promising", domain?.endsWith(".ai") === true && (staff ?? 0) > 10);
  },
);

await borg.repo({ id: 2, structs: [Startup], pipelines: [score] }).main();
