// A client program that is **wrong**, and must not compile.
//
// This is the assertion the whole generate step exists to make. Each line below is a mistake that
// costs nothing to make and, without generated types, would show up as an absent envelope or a
// rejected transaction somewhere else entirely — a typo'd field name reads as a cell nobody has
// written, which is a legitimate answer to a question you did not mean to ask.
//
// `run.sh` compiles this and asserts that `tsc` refuses it, quoting what it said.

import { Company, createBorgContext } from "./gen/borg.generated.ts";

const bc = await createBorgContext({ socket: "/dev/null" });
const tx = await bc.branch("main").begin();
const c = tx.object(Company, "#1");

// 1. A field that does not exist. The schema says `headcount`.
await c.get("headcont");

// 2. A field that exists, at the wrong type. `headcount` is an `Int`.
await c.set("headcount", "forty");

// 3. A field a producer owns. `is_investible` is `derived_by: "invest"`, so no client may write it
//    (§8) — generated code marks it `readonly` and the SDK's `set` takes only writable keys.
await c.set("is_investible", true);
