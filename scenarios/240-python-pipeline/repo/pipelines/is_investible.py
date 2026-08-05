#!/usr/bin/env python3
"""
A Borg pipeline, in Python. The twin of 230's TypeScript worker and 030's bash one.

This file is a deliberate line-for-line mirror of `230-typescript-pipeline/repo/pipelines/
is_investible.ts` — same structs, same field names, same producer name, same repo id — so that the
two `describe` payloads are byte-identical. `run.sh` asserts exactly that. Two SDKs describing one
repo have to describe it the same way, or the engine is being handed two dialects of one contract.

Note what is *not* here. No tracking, no cache, no dependency declaration — every `get` below is a
wire message and the engine records the read-set server-side, which is why writing a field this
pipeline never read re-runs nothing at all.

And note what is not here that *is* in the TypeScript twin: `await`. A synchronous `c.get` blocks on
a socket read and returns the value. Nothing on the wire is different.
"""

import borg

Employee = borg.struct(
    "Employee",
    {
        "name": borg.string(),
    },
)

Company = borg.struct(
    "Company",
    {
        "website": borg.string(),
        "headcount": borg.int_(),
        # Declared and never read: what makes field-granular invalidation observable rather than
        # asserted.
        "foundedYear": borg.int_(),
        # A list field's value is a handle to the list, not its elements (§4.2). Declared here so the
        # `Employee[]` type survives the round trip through `describe` and `borg def show`.
        "employees": borg.list_(borg.ref("Employee")),
        # Ownership, stated on the field. The pipeline states it again in `writes`, and the two are
        # cross-checked in both directions when this module assembles its description.
        "isInvestible": borg.bool_().derived(),
    },
)


@borg.pipeline("invest", Company, writes=["isInvestible"])
def invest(c):
    # stdout belongs to the author. Over stdio this line would corrupt the message stream permanently
    # and the failure would surface somewhere else entirely; over the socket the engine offers, it is
    # just a line on the terminal.
    print(f"[invest] scoring {c.ref}")

    score = 0

    # A string arrives as its content — `acme.ai`, not the `@s-…` that is physically stored. This
    # pipeline never learns that interning exists.
    website = c.get("website")
    if website is not None and website.endswith(".ai"):
        score += 6

    # …and a number arrives as a number. One text form on the wire, several types behind it.
    headcount = c.get("headcount")
    if headcount is not None and headcount > 10:
        score += 2

    print(f"[invest] {c.ref} scored {score}")

    # Deliberately needs both, so either field moving can flip the answer.
    c.set("isInvestible", score >= 7)


borg.repo(id=1, structs=[Company, Employee], pipelines=[invest]).main()
