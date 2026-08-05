// The same client, regenerated after the schema moved. `founded` is an `Int` holding the year.
//
//     client-v2.ts <socket> write <year>
//     client-v2.ts <socket> read

import { CLIENT_VERSION, Company, createBorgContext } from "./gen/v2/borg.generated.ts";

const [, , socket, verb, argument] = process.argv;
if (socket === undefined || verb === undefined) {
  throw new Error("usage: client-v2.ts <socket> write <year> | read");
}

const say = (key: string, value: string): void => {
  process.stdout.write(`${key}=${value}\n`);
};

const bc = await createBorgContext({ socket });
say("client_version", CLIENT_VERSION);

if (verb === "write") {
  const tx = await bc.branch("main").begin();
  // A `number`, because at this version `founded` is declared `Int`. Same field, same store, same
  // line of code in the previous file — and a different type, checked by the compiler.
  const founded: number = Number(argument);
  await tx.object(Company, "#2").set("founded", founded);
  say("landed", await tx.commit());
} else {
  const tx = await bc.branch("main").begin();
  const one = tx.object(Company, "#1");
  // The value a *v1* client wrote, seen through the new lens: `up` ran, so the date reads as a year.
  const founded: number | null = await one.get("founded");
  say("one.value", founded === null ? "" : String(founded));
  const envelope = await one.resolve("founded");
  say("one.origin", envelope.origin);
  say("one.state", envelope.state);
  await tx.abort();
}

bc.close();
