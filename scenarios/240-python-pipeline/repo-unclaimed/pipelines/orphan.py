#!/usr/bin/env python3
"""
A repo that cannot be pushed, and says why before the engine ever sees it.

`score` is declared `derived()`, which means clients may not write it (§8) — and no pipeline here
claims it, so no producer ever will either. That is a cell which could only ever be empty. The SDK
refuses to describe such a repo at all, so the push fails at `describe` with a message naming the
field and both ways out.

The check runs at import, not at the first invocation: `borg.repo(...)` assembles the description
eagerly, so `describe` mode fails just as loudly as the worker loop would have.
"""

import borg

Company = borg.struct(
    "Company",
    {
        "headcount": borg.int_(),
        "isInvestible": borg.bool_().derived(),
        "score": borg.int_().derived(),
    },
)


@borg.pipeline("invest", Company, writes=["isInvestible"])
def invest(c):
    c.set("isInvestible", c.get("headcount") is not None)


borg.repo(id=7, structs=[Company], pipelines=[invest]).main()
