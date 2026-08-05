// A client generated **before** the schema moved, and never regenerated.
//
// It is the whole point of §5.4. `Company.founded` is a `String` here, holding an ISO date, because
// that is what the branch declared when `borg generate` ran. The schema has since moved on and this
// file has not, which is a supported state and not a broken one: the ClientVersion its module baked
// in travels on every connection, so what it writes is stored in the shape it knows and what it
// reads comes back through `down`.
//
//     client-v1.ts <socket> write <date>
//     client-v1.ts <socket> read

import { CLIENT_VERSION, Company, createBorgContext } from "./gen/v1/borg.generated.ts";

const [, , socket, verb, argument] = process.argv;
if (socket === undefined || verb === undefined) {
  throw new Error("usage: client-v1.ts <socket> write <date> | read");
}

const say = (key: string, value: string): void => {
  process.stdout.write(`${key}=${value}\n`);
};

const bc = await createBorgContext({ socket });
say("client_version", CLIENT_VERSION);

if (verb === "write") {
  const tx = await bc.branch("main").begin();
  // A `string`, because at this version `founded` is declared `String`. The v2 module's `set` would
  // refuse this at compile time — see `crossed.ts`.
  const founded: string = argument ?? "";
  await tx.object(Company, "#1").set("founded", founded);
  say("landed", await tx.commit());
} else {
  const one = await bc.branch("main").get("Company#1.founded");
  say("one.value", one.value ?? "");
  say("one.origin", one.origin);

  // Written by a *newer* client, in a shape this one has never heard of. There is a path back to
  // this version, so the system meets this client where it is (§9.3) — and says plainly that what
  // it is looking at was computed.
  const two = await bc.branch("main").get("Company#2.founded");
  say("two.value", two.value ?? "");
  say("two.origin", two.origin);
  say("two.state", two.state);
  say("two.produced_by", two.by === null ? "" : two.by.slice(0, 1));
}

bc.close();
