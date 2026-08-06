#!/usr/bin/env node
// The personal CRM's `displayName`, reduced to the one thing FRICTION #17 measured: a pipeline whose
// body has a literal in it that an author will change on the second day.
//
// The scenario edits the fallback text below with `sed`, and **nothing else in this file moves** —
// no field, no name, no `writes`, no struct. That is the whole point. Every other kind of change a
// repo can make is visible in the shape it describes, and this one is not; before implementation
// fingerprints existed it therefore diffed as "unchanged" and invalidated nothing (§9.2).

import { borg } from "borg-sdk";

const Contact = borg.struct("Contact", {
  firstName: borg.string(),
  lastName: borg.string(),
  // Declared and never read by the pipeline, so a write to it can be shown to run nothing.
  notes: borg.string(),
  displayName: borg.string().derived(),
});

const displayName = borg.pipeline(
  "display_name",
  Contact,
  { writes: ["displayName"] },
  async (c) => {
    const first = await c.get("firstName");
    const last = await c.get("lastName");
    const parts = [first, last].filter((part) => part !== null && part !== "");
    // THE FALLBACK. The scenario rewrites this string and pushes again.
    await c.set("displayName", parts.length > 0 ? parts.join(" ") : "(no name)");
  },
);

await borg.repo({ id: 1, structs: [Contact], pipelines: [displayName] }).main();
