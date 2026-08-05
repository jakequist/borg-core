// The two generated modules crossed over, which must not compile.
//
// Without this the scenario would only be asserting that two files compile — and two *identical*
// generated modules would satisfy that too. What has to be true is that regenerating after a def
// push changed the types, and this is the file that says so: it reads the v1 module expecting v2's
// shape and writes the v2 module expecting v1's.

import { Company as CompanyV1 } from "./gen/v1/borg.generated.ts";
import { Company as CompanyV2, createBorgContext } from "./gen/v2/borg.generated.ts";

const bc = await createBorgContext({ socket: "/dev/null" });
const tx = await bc.branch("main").begin();

// At v1, `founded` is a String. Reading it as a number is the mistake a developer makes by not
// regenerating and assuming they did.
const wrong: number | null = await tx.object(CompanyV1, "#1").get("founded");
void wrong;

// And at v2 it is an Int, so the old shape no longer goes in.
await tx.object(CompanyV2, "#1").set("founded", "1999-06-01");
