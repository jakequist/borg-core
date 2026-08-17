// A client **over a WebSocket**, sharing one registry with a client over a unix socket.
// SPEC.md §17.5, §17.6.
//
// 310 established that a client is configured by one string and survives its server. This is the
// same client over the transport a browser can open, and the two things that are new are both about
// there being *two* transports at once:
//
//   * the unix client's transaction and this one's are open at the same moment, over two different
//     kinds of connection to the same registry — and the S2 conflict between them is decided by the
//     engine, which cannot tell them apart and must not be able to;
//   * a registry the server does not host is refused **at construction**, naming it. That is the
//     protocol-2 acknowledgement doing its job (`ROADMAP.md`, *The handshake names a registry*),
//     and it is asserted here over the transport where a client is most likely to be pointed at the
//     wrong deployment.
//
// The unix half is `scenarios/250-serve`'s eighty-line `client.py`, run with `execFileSync` — so
// this process is holding its WebSocket open the whole time the other connection exists.
//
// Everything it observes it prints as `key=value`, one per line, for `run.sh` to assert on.

import { execFileSync } from "node:child_process";
import { BorgClientError, createBorgContext } from "borg-sdk/client";

const [url, missingUrl, unixClient, socket, bounce] = process.argv.slice(2);
if (
  url === undefined ||
  missingUrl === undefined ||
  unixClient === undefined ||
  socket === undefined ||
  bounce === undefined
) {
  throw new Error("usage: client.ts <ws-url> <missing-ws-url> <client.py> <socket> <bounce.sh>");
}

const say = (key: string, value: string): void => {
  process.stdout.write(`${key}=${value}\n`);
};

/** One `client.py` invocation over the unix socket. Its answers, one JSON object per line. */
const overUnix = (...requests: string[]): Record<string, never>[] =>
  execFileSync("python3", [unixClient, socket, "--registry", "crm", ...requests], {
    encoding: "utf8",
  })
    .split("\n")
    .filter((line) => line.length > 0)
    .map((line) => JSON.parse(line) as Record<string, never>);

// ── A registry the server does not host, refused where it was configured ──────────────────────────
//
// Not at the first request. The whole cost of the old deferral was that this line used to resolve.

const missing = await createBorgContext({ url: missingUrl }).then(
  () => null,
  (err: unknown) => err,
);
say("missing_kind", missing instanceof BorgClientError ? "BorgClientError" : String(missing));
say("missing_says", missing instanceof Error ? missing.message : "none");

// ── One registry, two transports, at the same time ────────────────────────────────────────────────

const bc = await createBorgContext({ url });
say("address", bc.address);
say("structs", (await bc.branch("main").defs()).structs.map((s) => s.name).join(","));

// A starting value, so that both transactions below have something to read and to increment.
const seed = await bc.branch("main").begin();
await seed.object("Company", "#1").set("headcount", "10");
await seed.commit();

// **The unix client opens its transaction and reads**, while this process's WebSocket is open. The
// read is what makes the guard: a transaction is guarded by what it read (§12.1), and a commit is
// rejected when a guard no longer holds against the parent (§13).
const begun = overUnix('{"tx_begin":{}}', '{"tx_get":{"tx":"%TX%","cell":"Company#1.headcount"}}');
const unixTx = (begun[0] as unknown as { tx: { tx: string } }).tx.tx;
const unixSaw = (begun[1] as unknown as { cell: { value: string } }).cell.value;
say("unix_saw", unixSaw);

// **And this one does the same over the WebSocket, and commits first.**
const ws = await bc.branch("main").begin();
const wsSaw = await ws.object("Company", "#1").get("headcount");
say("ws_saw", String(wsSaw));
await ws.object("Company", "#1").set("headcount", String(Number(wsSaw) + 1));
say("ws_committed", await ws.commit());

// The unix client's commit is now guarded on a cell that moved — and it moved over the *other*
// transport, which nothing in the engine knows or could know.
const refused = overUnix(
  `{"tx_set":{"tx":"${unixTx}","cell":"Company#1.headcount","value":"99"}}`,
  `{"tx_commit":{"tx":"${unixTx}"}}`,
);
const outcome = refused[1] as unknown as {
  conflict?: { cell: string; reason: string };
  committed?: unknown;
};
say("unix_outcome", outcome.conflict === undefined ? "committed" : "conflict");
say("unix_conflict_cell", outcome.conflict?.cell ?? "none");
say("unix_conflict_reason", outcome.conflict?.reason ?? "none");

// The increment happened exactly once, not twice with one silently lost.
say("headcount", String(await (await bc.branch("main").begin()).object("Company", "#1").get("headcount")));

// ── A bounce, over the WebSocket ──────────────────────────────────────────────────────────────────
//
// The same claim 310 makes over a unix socket, made over this one: a transaction is an id beside the
// store (§12.2), so one begun before a restart commits after it — and the reconnect that makes that
// possible is the SDK's, over a transport it had never spoken when that behaviour was written.

const across = await bc.branch("main").begin();
const contact = await across.create("Contact");
await contact.set("name", "Grace");

// **`execFileSync` blocks the event loop**, which is not incidental — it means node cannot deliver
// the WebSocket's `close` while the bounce is happening, so the next request goes out on a socket
// this process still believes is live. That is the *hard* case, and 310 asserts it over a unix
// socket for exactly the same reason: an idle client notices the close and reconnects with nothing
// in flight, and only a client that was busy right through the outage discovers it by failing.
execFileSync("bash", [bounce], { stdio: "pipe" });
say("bounced", "ok");

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

say("resumed", await across.commit());
say("listed", String((await bc.branch("main").list("Contact")).includes(contact.id)));
say("reconnected", String(bc.connected));

bc.close();
