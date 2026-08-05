// A borg client, written against generated types.
//
// Compare with `scenarios/250-serve/client.py`, which speaks the same protocol with a socket and
// `json` and eighty lines. Both are legitimate; the difference this file exists to show is that a
// wrong field name here is a *compile* error rather than an empty envelope at runtime, and that a
// rejected commit arrives as an exception carrying the cell that moved.
//
// Everything it observes it prints as `key=value`, one per line, for `run.sh` to assert on. Reading
// assertions out of a program's stdout keeps the claims in the scenario, where the reasoning is.

import { ConflictError } from "borg-sdk/client";
import { CLIENT_VERSION, Company, createBorgContext } from "./gen/borg.generated.ts";

const socket = process.argv[2];
if (socket === undefined) throw new Error("usage: client.ts <socket>");

const say = (key: string, value: string): void => {
  process.stdout.write(`${key}=${value}\n`);
};

// No version argument: generation baked it in, and this is the only client that could honestly
// state one (§5.4).
const bc = await createBorgContext({ socket });
say("client_version", CLIENT_VERSION);

// ── S2, through the SDK ──────────────────────────────────────────────────────────────────────────
//
// Two transactions read the same cell and both write it. The read precedes the write, so it observed
// the parent and is guarded — compare-and-swap falls out of reading a cell first (§12.1).

const a = await bc.branch("main").begin();
const b = await bc.branch("main").begin();
for (const tx of [a, b]) {
  const c = tx.object(Company, "#1");
  // Typed: `headcount` is `number | null` because the schema says `Int`, and the SDK converted it.
  const seen: number | null = await c.get("headcount");
  await c.set("headcount", (seen ?? 0) + 1);
}

say("first", await a.commit());
try {
  await b.commit();
  say("second", "committed");
} catch (err) {
  if (!(err instanceof ConflictError)) throw err;
  say("conflict.reason", err.reason);
  say("conflict.cell", err.cell ?? "");
}
// Still open after the rejection: its read-set is what a client needs in order to decide about
// retrying, so the SDK does not throw it away (§12, and SDK-DRAFT §3 — no retry sugar in v1).
await b.abort();
say("aborted", "ok");

// ── The envelope on a read that is behind ───────────────────────────────────────────────────────
//
// `is_investible` is derived from `headcount`, which the commit above moved. Auto-derivation is
// paused on this branch, so nothing has recomputed it — and the read says so rather than serving it
// as though it were current (invariant 8).

const derived = await bc.branch("main").get("Company#1.is_investible");
say("stale.state", derived.state);
say("stale.origin", derived.origin);
say("stale.value", derived.value ?? "");
say("stale.produced_by", derived.by === null ? "" : derived.by.slice(0, 1));
say("stale.fresh_as_of", derived.fresh_as_of);

// ── Values, at the types the schema declared ────────────────────────────────────────────────────

const tx = await bc.branch("main").begin();
const c = tx.object(Company, "#1");
const headcount: number | null = await c.get("headcount");
const website: string | null = await c.get("website");
say("headcount", String(headcount));
say("website", website ?? "");

// The same read, with its provenance. One round trip either way — §17.5 never answers with a bare
// value, so `get` is discarding an envelope rather than saving a message.
const envelope = await c.resolve("website");
say("website.origin", envelope.origin);
say("website.state", envelope.state);
say("website.cell", envelope.cell);

await tx.abort();
bc.close();
