#!/usr/bin/env node
// The CRM's one pipeline: `displayName`, derived from the name parts.
//
// This is the whole reason the app is built on Borg rather than on a table. Nothing in `api/` ever
// computes a display name, and nothing in `ui/` ever concatenates one — the field is owned by this
// producer (§8), so a client write to it is refused by the engine and does not even compile against
// the generated types. Change a contact's `lastName` and this re-runs, for that contact only,
// because the engine recorded which cells this body read.
//
// Node runs this file directly; since 22.18 it strips types rather than compiling them, so the file
// that is read is the file that runs.

import { borg } from "borg-sdk";

const Contact = borg.struct("Contact", {
  firstName: borg.string(),
  lastName: borg.string(),
  email: borg.string(),
  phone: borg.string(),
  notes: borg.string(),
  // Owned by `display_name` below. Stated on the field *and* in the pipeline's `writes`, and the
  // two are cross-checked in both directions when this module describes itself.
  displayName: borg.string().derived(),
});

const displayName = borg.pipeline(
  "display_name",
  Contact,
  { writes: ["displayName"] },
  async (c) => {
    // Every one of these is a wire message the engine records as a dependency of this invocation.
    // `phone` and `notes` are never read here, which is why editing them re-derives nothing.
    const first = (await c.get("firstName"))?.trim() ?? "";
    const last = (await c.get("lastName"))?.trim() ?? "";

    const named = [first, last].filter((part) => part !== "").join(" ");
    if (named !== "") {
      await c.set("displayName", named);
      return;
    }

    // A contact with no name at all is a real state: the create form allows it, and an import from
    // a mail client produces it constantly. Falling back to the email address is a *read* of
    // `email`, so a contact that later gains a first name re-derives — and so does one that only
    // ever had an email and then changes it.
    const email = (await c.get("email"))?.trim() ?? "";
    await c.set("displayName", email !== "" ? email : "(no name)");
  },
);

await borg.repo({ id: 7, structs: [Contact], pipelines: [displayName] }).main();
