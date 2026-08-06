// A client that is **configured by one string** and **outlives the server it is talking to**.
// SPEC.md §17.7, `examples/personal-crm/FRICTION.md` #11.
//
// Compare with `scenarios/260`'s client, which takes a socket path and lives inside one server's
// lifetime. Two things are different here and they are the two the scenario exists for:
//
//   * the address and the registry arrive as **one url**, the way a `DATABASE_URL` does — which is
//     what lets `dev.sh`, an api and a CLI all be pointed at the same store by copying a variable;
//   * the server is **stopped and started underneath this process**, mid-session, and everything
//     carries on. A transaction opened before the bounce commits after it, because a transaction is
//     an id beside the store (§12.2) and never state in a socket.
//
// Everything it observes it prints as `key=value`, one per line, for `run.sh` to assert on.

import { execFileSync } from "node:child_process";
import { BorgUnreachableError, createBorgContext } from "borg-sdk/client";

const [url, bounce, absent] = process.argv.slice(2);
if (url === undefined || bounce === undefined || absent === undefined) {
  throw new Error("usage: client.ts <url> <bounce-script> <absent-url>");
}

const say = (key: string, value: string): void => {
  process.stdout.write(`${key}=${value}\n`);
};

// ── One string, and it names the registry ─────────────────────────────────────────────────────────

const bc = await createBorgContext({ url });
say("address", bc.address);
say("connected", String(bc.connected));
// Which store this is: the two registries hold different schemas, so the def view is the proof that
// the *name* in the url routed rather than the socket.
say("structs", (await bc.branch("main").defs()).structs.map((s) => s.name).join(","));

// ── A transaction, opened before the server goes away ─────────────────────────────────────────────

const tx = await bc.branch("main").begin();
const company = await tx.create("Company");
// An un-generated client: the struct is a string and the value is its text, which is what is
// honestly known about a schema this process never read.
await company.set("website", "acme.ai");
say("tx", tx.id);

// `borg-server stop` and `borg-server start`, run from inside the session. The stop waits for the
// *process*, not for the socket, so the restart cannot race a predecessor that still holds the
// advisory locks — which is exactly the race `borg-server stop` was written to lose deliberately.
//
// **`execFileSync` blocks the event loop**, which is not incidental: it means node cannot deliver
// the socket's `close` while the bounce is happening, so the next request below is written to a
// socket this process still believes is live. That is the *hard* case, deliberately — an idle
// client notices the close and reconnects with nothing in flight, and only a client that was busy
// through the whole outage discovers it by failing.
execFileSync("bash", [bounce], { stdio: "pipe" });
say("bounced", "ok");

// So this one fails, and the failure is the point: it is a distinguishable error saying the
// operation was **not** retried. A read is the right thing to discover it with, because a read is
// the thing you are allowed to simply do again.
const stale = await bc
  .branch("main")
  .head()
  .then(
    () => null,
    (err: unknown) => err,
  );
say("stale_socket", stale instanceof Error ? stale.constructor.name : "none");
say("stale_says", stale instanceof Error ? stale.message : "none");
say("dropped", String(bc.connected));

// **The same handle, over a connection that did not exist when it was opened.** Nothing was
// retried and nothing was resumed by hand: the dead socket has been dropped and this dials.
say("committed", await tx.commit());
say(
  "website",
  String(await (await bc.branch("main").begin()).object("Company", company.id).get("website")),
);
say("listed", String((await bc.branch("main").list("Company")).includes(company.id)));
say("reconnected", String(bc.connected));

// ── And an address with nothing on it says how to fix that ────────────────────────────────────────

const refused = await createBorgContext({ url: absent }).then(
  () => null,
  (err: unknown) => err,
);
say("unreachable_kind", refused instanceof BorgUnreachableError ? "BorgUnreachableError" : "other");
say("unreachable", refused instanceof Error ? refused.message : "none");

bc.close();
