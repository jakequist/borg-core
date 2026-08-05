#!/usr/bin/env python3
"""
The other half of the repo. `score.ts` is the first, and neither knows the other exists.

This one reads what the TypeScript pipeline wrote and writes a field of its own, so the round has a
chain across a language boundary in it. Nothing in the engine, the def layer or the dependency index
records which language anything came from — the only per-language fact in the whole store is the
`command` in `borg.producers.json`, which is a path.

## Why `promising` is read through `world`

`Startup.promising` is owned by `score`, which lives in the *other* file. An SDK's describe-assembly
runs per module, so this module cannot declare a `derived()` field whose pipeline it does not
implement — it would be refused as a field nothing could ever write. The engine has no such
restriction: `derived_by` is resolved against every producer the whole repo describes.

So a cross-module read goes through `world`, which is stringly and needs no declaration. That is a
real seam and it is recorded in `SDK-DRAFT.md` §4.2: the SDKs model a repo as one module, and the
engine models it as one directory.
"""

import borg

Startup = borg.struct(
    "Startup",
    {
        "domain": borg.string(),
        "headline": borg.string().derived(),
    },
)


@borg.pipeline("summarise", Startup, writes=["headline"])
def summarise(c, world):
    print(f"[summarise] {c.ref}")
    domain = c.get("domain")
    # The hop across the language boundary. Typed by handing `world` the same field type the DSL
    # produces, which is where a generated type will slot in later.
    promising = world.get(f"{c.ref}.promising", borg.bool_())
    c.set("headline", f"{domain}: {'invest' if promising else 'pass'}")


borg.repo(id=2, structs=[Startup], pipelines=[summarise]).main()
