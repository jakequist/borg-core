/**
 * The client SDK, against a real `borg serve`.
 *
 * Not a stand-in engine. The worker tests in `worker.test.ts` fake the engine because what they
 * assert is *the conversation* — which cells crossed the wire, in what order — and a real engine
 * would make that harder to read without making it truer. This file asserts the opposite kind of
 * thing: that a guard trips, that a conflict names a cell, that an envelope says `source`. None of
 * that is a property of the SDK at all; it is the engine's, reached through the SDK, and a fake
 * server would let every one of these pass while the real thing was broken.
 *
 * So: the real binary, a real store, a real socket. If the binary is not built, the suite skips
 * loudly — `check.sh` builds it in an earlier step, and a developer running vitest alone should be
 * told what to build rather than watch nine tests fail on ENOENT.
 */

import { afterAll, beforeAll, describe as suite, expect, test } from "vitest";
import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import { connect } from "node:net";
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve as resolvePath } from "node:path";
import { fileURLToPath } from "node:url";
import {
  BorgClientError,
  ConflictError,
  createBorgContext,
  defineStruct,
  int,
  refText,
  string,
  type BorgContext,
  type Ref,
} from "../src/client.js";

const BORG =
  process.env["BORG_BIN"] ??
  resolvePath(fileURLToPath(new URL("../../../target/debug/borg", import.meta.url)));

const available = existsSync(BORG);
if (!available) {
  console.warn(
    `⚠ skipping the client SDK suite: no borg binary at ${BORG}. ` +
      `Run \`cargo build -p borg-cli\` (or set BORG_BIN).`,
  );
}

/**
 * The structs, written the way codegen writes them. Hand-maintained here on purpose: this suite is
 * about the *runtime*, and importing the golden module would make a change to the emitter able to
 * break tests that are not about the emitter.
 */
interface Company {
  headcount: number | null;
  website: string | null;
  ceo: Ref<"Employee"> | null;
}
const Company = defineStruct<Company>("Company", {
  headcount: { type: int(), derived: false, version: "L1" },
  website: { type: string(), derived: false, version: "L1" },
  ceo: { type: refText("Employee"), derived: false, version: "L1" },
});

interface Employee {
  name: string | null;
}
const Employee = defineStruct<Employee>("Employee", {
  name: { type: string(), derived: false, version: "L1" },
});

let dir: string;
let socket: string;
let server: ChildProcess;

async function untilListening(path: string): Promise<void> {
  // A socket file exists a moment before anything is listening on it, and a test that races that is
  // a test that fails one run in forty — `scenarios/250-serve` records the same lesson.
  for (let attempt = 0; attempt < 200; attempt++) {
    const open = await new Promise<boolean>((done) => {
      const probe = connect(path);
      probe.once("connect", () => {
        probe.destroy();
        done(true);
      });
      probe.once("error", () => done(false));
    });
    if (open) return;
    await new Promise((done) => setTimeout(done, 50));
  }
  throw new Error(`nothing came up on ${path}`);
}

beforeAll(async () => {
  if (!available) return;
  dir = mkdtempSync(join(tmpdir(), "borg-client-"));
  socket = join(dir, "borg.sock");
  const store = join(dir, "borg.db");
  const borg = (...args: string[]): void => {
    execFileSync(BORG, ["--store", store, ...args], { stdio: "pipe" });
  };

  borg("init");
  const schema = join(dir, "schema.json");
  writeFileSync(
    schema,
    JSON.stringify({
      repo: 1,
      events: [
        { DeclareField: { struct_name: "Company", field: "headcount", ty: "Int" } },
        { DeclareField: { struct_name: "Company", field: "website", ty: "String" } },
        { DeclareField: { struct_name: "Company", field: "ceo", ty: "Employee" } },
        { DeclareField: { struct_name: "Employee", field: "name", ty: "String" } },
      ],
    }),
  );
  borg("def", "push", schema);

  server = spawn(BORG, ["--store", store, "serve", "--socket", socket], { stdio: "pipe" });
  await untilListening(socket);
}, 60_000);

afterAll(() => {
  if (!available) return;
  server.kill("SIGTERM");
  rmSync(dir, { recursive: true, force: true });
});

async function context(clientVersion?: string): Promise<BorgContext> {
  return clientVersion === undefined
    ? createBorgContext({ socket })
    : createBorgContext({ socket, clientVersion });
}

suite.skipIf(!available)("the client SDK, over borg serve", () => {
  test("connects, handshakes, and can be asked about the store", async () => {
    const bc = await context();
    const branches = await bc.branches();
    expect(branches.map((b) => b.name)).toContain("main");
    expect(await bc.branch("main").head()).toMatch(/^L\d+$/);
    bc.close();
  });

  test("a transaction writes, commits, and the write is there afterwards", async () => {
    const bc = await context();
    const tx = await bc.branch("main").begin();
    // A handle is free: the address is built from the struct name and the id, and nothing has been
    // read yet.
    const c = tx.object(Company, "#1");
    await c.set("headcount", 40);
    await c.set("website", "acme.ai");
    const landed = await tx.commit();
    expect(landed).toMatch(/^L\d+$/);

    const after = await bc.branch("main").get("Company#1.headcount");
    expect(after.value).toBe("40");
    bc.close();
  });

  /**
   * The envelope split, which is the one interesting decision in the read surface (SDK-DRAFT §4.4):
   * `get` is the value and `resolve` is the value *and its provenance*, at the same one round trip,
   * because §17.5 never answers a read with a bare value.
   */
  test("get is the value; resolve is the same read with its §10.4 envelope", async () => {
    const bc = await context();
    const tx = await bc.branch("main").begin();
    const c = tx.object(Company, "#2");
    await c.set("headcount", 7);
    await tx.commit();

    const read = await bc.branch("main").begin();
    const handle = read.object(Company, "#2");
    expect(await handle.get("headcount")).toBe(7);

    const resolved = await handle.resolve("headcount");
    expect(resolved.value).toBe(7);
    expect(resolved.origin).toBe("source");
    expect(resolved.state).toBe("current");
    // The canonical cell, whatever shorthand went out.
    expect(resolved.cell).toMatch(/^Company:o-[0-9a-z]+\.headcount$/);
    // Layer ids are the text `borg get` prints, not JSON numbers — see the client protocol header.
    expect(resolved.fresh_as_of).toMatch(/^L\d+$/);
    expect(resolved.landed_at).toMatch(/^L\d+$/);
    await read.abort();
    bc.close();
  });

  test("a cell nobody has written reads as null, and so does one that was deleted", async () => {
    const bc = await context();
    const tx = await bc.branch("main").begin();
    const c = tx.object(Company, "#3");
    expect(await c.get("headcount")).toBeNull();

    await c.set("headcount", 1);
    await c.set("headcount", null);
    // Absence has two spellings on the wire and one meaning here (§8.1); the envelope keeps the
    // distinction for anyone who needs it.
    expect(await c.get("headcount")).toBeNull();
    expect((await c.resolve("headcount")).state).toBe("tombstoned");
    await tx.abort();
    bc.close();
  });

  /**
   * S2, through the SDK. The same shape as `scenarios/140` and `250-serve`, except that what a
   * caller gets is a typed exception carrying the cell — which is the whole of SDK-DRAFT §3's claim
   * that `ConflictError` is contract.
   */
  test("two transactions that read the same cell: the second commit throws, naming the cell", async () => {
    const bc = await context();
    const tx = await bc.branch("main").begin();
    await tx.object(Company, "#10").set("headcount", 100);
    await tx.commit();

    const a = await bc.branch("main").begin();
    const b = await bc.branch("main").begin();
    // Read-modify-write, both. The read precedes the write, so it observed the parent and is
    // guarded — which is how compare-and-swap falls out of reading a cell first (§12.1).
    for (const tx of [a, b]) {
      const c = tx.object(Company, "#10");
      await c.set("headcount", (await c.get("headcount")) ?? 0);
    }
    await expect(a.commit()).resolves.toMatch(/^L\d+$/);

    const rejected = await b.commit().then(
      () => null,
      (err: unknown) => err,
    );
    expect(rejected).toBeInstanceOf(ConflictError);
    const conflict = rejected as ConflictError;
    expect(conflict.reason).toBe("guard");
    expect(conflict.cell).toContain("headcount");

    // **Still open.** Its read-set is what a client needs in order to decide about retrying, so the
    // rejection does not throw it away (§12, `ops::tx_commit`).
    await expect(b.abort()).resolves.toBeUndefined();
    bc.close();
  });

  /**
   * A transaction is an id, and it lives beside the store rather than in this socket (§12.2). Two
   * contexts here are two connections; nothing between them and the transaction table can tell.
   */
  test("a transaction outlives the connection that opened it", async () => {
    const first = await context();
    const tx = await first.branch("main").begin();
    await tx.object(Company, "#20").set("headcount", 5);
    first.close();

    const second = await context();
    const resumed = second.transaction(tx.id);
    await resumed.object(Company, "#20").set("headcount", 6);
    await resumed.commit();

    expect((await second.branch("main").get("Company#20.headcount")).value).toBe("6");
    second.close();
  });

  test("values convert through the same table a pipeline uses, references included", async () => {
    const bc = await context();
    const tx = await bc.branch("main").begin();
    const employee = tx.object(Employee, "#1");
    await employee.set("name", "Ada");
    // The canonical address is what the server answered with, and its PID is what a reference
    // holds: `Employee:o-1234abcd.name` → `o-1234abcd`.
    const canonical = (await employee.resolve("name")).cell;
    const pid = canonical.slice(canonical.indexOf(":") + 1, canonical.lastIndexOf("."));

    const company = tx.object(Company, "#30");
    await company.set("ceo", pid as Ref<"Employee">);
    expect(await company.get("ceo")).toBe(pid);
    // What is stored is the wire form, `@` and the PID — the same text `borg set` accepts.
    expect((await company.resolve("ceo")).value).toBe(pid);
    await tx.abort();
    bc.close();
  });

  /**
   * The un-generated client. It is not a second-class one: it is the CLI, and the CLI works — so the
   * SDK offers the same stringly shape the pipeline SDK's `world` has, for the same reason. What is
   * honestly known about a struct whose definitions this process never read is its name.
   */
  test("a struct named rather than generated works, with values as their text", async () => {
    const bc = await context();
    const tx = await bc.branch("main").begin();
    const c = tx.object("Company", "#50");
    await c.set("headcount", "12");
    expect(await c.get("headcount")).toBe("12");
    // No conversion to reach for, so a number has nowhere to go — said rather than coerced.
    await expect(
      (c as unknown as { set: (f: string, v: unknown) => Promise<void> }).set("headcount", 12),
    ).rejects.toThrow(/untyped/);
    await tx.abort();
    bc.close();
  });

  test("a value the field cannot hold is refused before it reaches the wire", async () => {
    const bc = await context();
    const tx = await bc.branch("main").begin();
    const c = tx.object(Company, "#40");
    await expect(c.set("headcount", 1.5 as number)).rejects.toThrow(/whole number/);
    await tx.abort();
    bc.close();
  });

  test("a derived field is refused by the SDK, where the caller can act on it", async () => {
    const bc = await context();
    const tx = await bc.branch("main").begin();
    interface Scored {
      score: number | null;
    }
    const Scored = defineStruct<Scored>("Scored", {
      score: { type: int(), derived: true, version: "L1" },
    });
    // The engine would refuse it too, as a rejected transaction naming a producer id (§8). This
    // names the field, costs no round trip, and is the same rule.
    await expect(
      (tx.object(Scored, "#1") as { set: (f: "score", v: number) => Promise<void> }).set("score", 1),
    ).rejects.toThrow(/derived/);
    await tx.abort();
    bc.close();
  });

  test("what the server refuses arrives as an error with the sentence borg would have printed", async () => {
    const bc = await context();
    await expect(bc.branch("nope").head()).rejects.toBeInstanceOf(BorgClientError);
    await expect(bc.branch("main").get("not a cell")).rejects.toBeInstanceOf(BorgClientError);
    // §12.3's promise, over a socket: a handle that never existed says so by name, and one that was
    // reaped says *expired after N idle* — never "unknown transaction", which sends a client
    // hunting a bug in its own bookkeeping.
    await expect(bc.transaction("tx-9999").commit()).rejects.toThrow(/tx-9999/);
    bc.close();
  });

  /**
   * **Reading an undeclared cell is not an error, and that is the engine's answer rather than the
   * SDK's.** `borg get Wombat#1.nose` prints exactly this envelope: absent, at `L0`. Asserted here
   * because it is surprising enough that somebody will otherwise "fix" it in the SDK, where it would
   * become a second read path that disagrees with the CLI.
   */
  test("a cell of a struct nobody declared answers an absent envelope, as borg get does", async () => {
    const bc = await context();
    const read = await bc.branch("main").get("Wombat#1.nose");
    expect(read.value).toBeNull();
    expect(read.fresh_as_of).toBe("L0");
    bc.close();
  });

  /**
   * The ClientVersion is the one thing the handshake carries beyond the codec, and a client that
   * states one the server cannot read is told so rather than left with a socket that went quiet.
   * (`scenarios/270` is where a *valid* one earns its keep, reading through a `down` migration.)
   *
   * **The settle is not decoration.** A rejected handshake is answered *and then hung up on*, and
   * the server never acknowledges an accepted one — it has nothing to say — so a client cannot know
   * it was accepted except by asking something. Ask too fast and the write lands on a socket the
   * peer has already closed, which is an EPIPE that discards the very answer it was racing. That is
   * a property of `borg serve`'s refusal path and is recorded in SDK-DRAFT §4.4 as a gap rather
   * than papered over here: the fix is a lingering close on the server, not a retry in the SDK.
   */
  test("a client version that is not a layer id is refused by name", async () => {
    const bc = await context("not-a-layer");
    await new Promise((done) => setTimeout(done, 100));
    await expect(bc.branches()).rejects.toThrow(/client_version/);
    bc.close();
  });
});
