/**
 * The client SDK, against a real `borg-server`.
 *
 * Not a stand-in engine. The worker tests in `worker.test.ts` fake the engine because what they
 * assert is *the conversation* — which cells crossed the wire, in what order — and a real engine
 * would make that harder to read without making it truer. This file asserts the opposite kind of
 * thing: that a guard trips, that a conflict names a cell, that an envelope says `source`. None of
 * that is a property of the SDK at all; it is the engine's, reached through the SDK, and a fake
 * server would let every one of these pass while the real thing was broken.
 *
 * So: the real binaries, a real store, a real socket. If either is not built, the suite skips
 * loudly — `check.sh` builds them in an earlier step, and a developer running vitest alone should be
 * told what to build rather than watch nine tests fail on ENOENT.
 *
 * The store is a **registry under a data directory**, because that is what a server hosts (SPEC.md
 * §17.6). There is exactly one here, which is the case a client names no registry for — so nothing
 * in the SDK below has to say which store it means.
 */

import { afterAll, beforeAll, describe as suite, expect, test } from "vitest";
import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import { connect, createServer, type Server } from "node:net";
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve as resolvePath } from "node:path";
import { fileURLToPath } from "node:url";
import {
  BorgClientError,
  BorgDisconnectedError,
  BorgUnreachableError,
  ConflictError,
  createBorgContext,
  defineStruct,
  int,
  refText,
  string,
  type BorgContext,
  type Ref,
  type StructDescriptor,
} from "../src/client.js";

const BORG =
  process.env["BORG_BIN"] ??
  resolvePath(fileURLToPath(new URL("../../../target/debug/borg", import.meta.url)));

const BORG_SERVER =
  process.env["BORG_SERVER_BIN"] ??
  resolvePath(fileURLToPath(new URL("../../../target/debug/borg-server", import.meta.url)));

const available = existsSync(BORG) && existsSync(BORG_SERVER);
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
// The second type argument is the struct's own name, which is what brands the ids `list` and
// `create` answer with — codegen emits `StructDescriptor<Company, "Company">` for the same reason.
const Company: StructDescriptor<Company, "Company"> = defineStruct("Company", {
  headcount: { type: int(), derived: false, version: "L1" },
  website: { type: string(), derived: false, version: "L1" },
  ceo: { type: refText("Employee"), derived: false, version: "L1" },
});

interface Employee {
  name: string | null;
}
const Employee: StructDescriptor<Employee, "Employee"> = defineStruct("Employee", {
  name: { type: string(), derived: false, version: "L1" },
});

/** A struct of its own for the enumeration tests, so that what they list is only what they made. */
interface Contact {
  name: string | null;
}
const Contact: StructDescriptor<Contact, "Contact"> = defineStruct("Contact", {
  name: { type: string(), derived: false, version: "L1" },
});

let dir: string;
let data: string;
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
  // One registry under a data directory — what `borg-server` hosts. `main` is the only one, so the
  // handshake below names none and gets it.
  data = join(dir, "data");
  const store = join(data, "main", "borg.db");
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
        { DeclareField: { struct_name: "Contact", field: "name", ty: "String" } },
      ],
    }),
  );
  borg("def", "push", schema);

  server = startServer();
  await untilListening(socket);
}, 60_000);

/**
 * `--foreground`, because this test *is* the supervisor: it holds the child and kills it in
 * `afterAll`, which is exactly what backgrounding would take away from it.
 */
function startServer(): ChildProcess {
  return spawn(BORG_SERVER, ["start", "--foreground", "--data-dir", data, "--socket", socket], {
    stdio: "pipe",
  });
}

/**
 * Wait for the **process**, not for the socket, and for the same reason `borg-server stop` does: a
 * server stops accepting the moment its listener drops and *then* releases its advisory locks, so
 * restarting on a quiet socket would race a predecessor that still holds them.
 */
async function stopServer(): Promise<void> {
  if (server.exitCode !== null || server.signalCode !== null) return;
  const gone = new Promise<void>((done) => server.once("exit", () => done()));
  server.kill("SIGTERM");
  await gone;
}

/** Stop the server and start another on the same data directory and address. */
async function bounce(): Promise<void> {
  await stopServer();
  server = startServer();
  await untilListening(socket);
}

afterAll(async () => {
  if (!available) return;
  await stopServer();
  rmSync(dir, { recursive: true, force: true });
});

async function context(clientVersion?: string): Promise<BorgContext> {
  return clientVersion === undefined
    ? createBorgContext({ socket })
    : createBorgContext({ socket, clientVersion });
}

suite.skipIf(!available)("the client SDK, over borg-server", () => {
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
   * The two primitives an application needs and could not previously say: *make me one of these*,
   * and *which of these are there* (§9.6, §17.5). Together they are the difference between a store
   * you can address and one you can write an application against.
   */
  test("create allocates ids the client never chose, and list finds them all", async () => {
    const bc = await context();
    const tx = await bc.branch("main").begin();
    const made = [];
    for (const name of ["Ada", "Grace", "Barbara"]) {
      const contact = await tx.create(Contact);
      await contact.set("name", name);
      made.push(contact.id);
    }
    // Distinct by construction — the server allocates, so nothing here had to choose an id or check
    // one, which is what makes creating an object safe without reading anything first.
    expect(new Set(made).size).toBe(3);
    for (const id of made) expect(id).toMatch(/^o-[0-9a-z]+$/);

    // Not yet: the creations are on the transaction's own branch until it merges (§12).
    expect(await bc.branch("main").list(Contact)).toEqual([]);
    await tx.commit();

    const listed = await bc.branch("main").list(Contact);
    expect([...listed].sort()).toEqual([...made].sort());
    // Ids and nothing else. A name costs a read per contact, which is the N+1 this deliberately
    // does not hide (SDK-DRAFT §4.5).
    const reading = await bc.branch("main").begin();
    const names = [];
    for (const id of listed) names.push(await reading.object(Contact, id).get("name"));
    expect(names.sort()).toEqual(["Ada", "Barbara", "Grace"]);
    await reading.abort();
    bc.close();
  });

  /**
   * The typing the descriptor's name literal buys: an id that came out of the SDK goes back in as a
   * reference **without a cast**, and one belonging to the wrong struct does not compile.
   */
  test("an id from create is a reference to its own struct, and only to that", async () => {
    const bc = await context();
    const tx = await bc.branch("main").begin();
    const employee = await tx.create(Employee);
    await employee.set("name", "Ada");

    const company = tx.object(Company, "#60");
    // No `as Ref<"Employee">` anywhere: `Employee`'s descriptor states its own name in its type, so
    // `employee.id` already *is* an `Employee` reference to the compiler.
    await company.set("ceo", employee.id);
    expect(await company.get("ceo")).toBe(employee.id);

    // @ts-expect-error a Company id is not an Employee reference, which is the whole point.
    await company.set("ceo", (await tx.create(Contact)).id);
    await tx.abort();
    bc.close();
  });

  test("two transactions each creating an object both commit, with different ids", async () => {
    const bc = await context();
    const a = await bc.branch("main").begin();
    const b = await bc.branch("main").begin();
    const first = await a.create(Employee);
    const second = await b.create(Employee);
    expect(first.id).not.toBe(second.id);

    // Neither read anything and the cells they wrote are distinct by construction, so there is no
    // guard to trip — creation is the one write two clients can always both do.
    await expect(a.commit()).resolves.toMatch(/^L\d+$/);
    await expect(b.commit()).resolves.toMatch(/^L\d+$/);
    bc.close();
  });

  test("listing or creating a struct nobody declared is refused by name", async () => {
    const bc = await context();
    await expect(bc.branch("main").list("Wombat")).rejects.toThrow(/Wombat/);
    const tx = await bc.branch("main").begin();
    await expect(tx.create("Wombat")).rejects.toThrow(/Wombat/);
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
   * a property of the server's refusal path and is recorded in SDK-DRAFT §4.4 as a gap rather
   * than papered over here: the fix is a lingering close on the server, not a retry in the SDK.
   */
  test("a client version that is not a layer id is refused by name", async () => {
    const bc = await context("not-a-layer");
    await new Promise((done) => setTimeout(done, 100));
    await expect(bc.branches()).rejects.toThrow(/client_version/);
    bc.close();
  });
});

/**
 * **Connection urls, and reconnection.** SPEC.md §17.7, `examples/personal-crm/FRICTION.md` #11.
 *
 * The suite above is about what the engine answers. This one is about the connection itself: how a
 * client says where to connect, and what happens to a long-lived one when the server it is talking
 * to goes away and comes back. Both were things an application had to own by hand, and #11 recorded
 * what that cost — a `borg serve` restart made every later request throw *forever*, and the only
 * recovery was restarting the api.
 *
 * These bounce a **real** server rather than a stand-in, because the claim is about surviving a real
 * restart: locks released and retaken, a registry reopened from its log, a transaction picked up out
 * of the sidecar it lives in. One test below does use a stand-in, and says why.
 */
suite.skipIf(!available)("connection urls and reconnection", () => {
  /** How many times the soak below bounces. See `scenarios/200-determinism` on why 5 is a smoke test. */
  const BOUNCES = Number(process.env["BORG_RECONNECT_BOUNCES"] ?? 5);

  test("a url names the socket and the registry, and reaches the same store as the explicit form", async () => {
    // The registry is named here, where the suite above leaves it out — one store is hosted, so
    // both are correct, and asserting they agree is what makes the routing a fact rather than a
    // coincidence of there being nothing else to route to.
    const byUrl = await createBorgContext({ url: `borg+unix://${socket}/main` });
    const tx = await byUrl.branch("main").begin();
    await tx.object(Company, "#70").set("headcount", 3);
    await tx.commit();
    expect(byUrl.address).toBe(socket);
    byUrl.close();

    const explicit = await createBorgContext({ socket, registry: "main" });
    expect((await explicit.branch("main").get("Company#70.headcount")).value).toBe("3");
    explicit.close();

    // A trailing slash names no registry, which the server answers with its sole one (§17.6).
    const unnamed = await createBorgContext({ url: `borg+unix://${socket}/` });
    expect((await unnamed.branch("main").get("Company#70.headcount")).value).toBe("3");
    unnamed.close();
  });

  test("$BORG_URL configures a client that was given nothing", async () => {
    const bc = await createBorgContext({ env: { BORG_URL: `borg+unix://${socket}/main` } });
    expect((await bc.branches()).map((b) => b.name)).toContain("main");
    bc.close();

    // …and a client given nothing at all, with nothing in the environment, is told both ways to
    // say it rather than left to read a stack trace about `undefined`.
    await expect(createBorgContext({ env: {} })).rejects.toThrow(/BORG_URL/);
  });

  test("a registry the server does not host is refused by name, with the ones it does", async () => {
    // **The refusal arrives at the first request, not at the handshake**, which is the server's
    // recorded deviation (`ROADMAP.md`, *The handshake names a registry*): the server does not
    // acknowledge an accepted hello, so there is nowhere to put a refusal at connect time. The cost
    // is exactly this — `createBorgContext` resolves for a registry that does not exist.
    const bc = await createBorgContext({ url: `borg+unix://${socket}/nope` });
    await expect(bc.branches()).rejects.toThrow(/nope/);
    await expect(bc.branches()).rejects.toThrow(/main/);
    bc.close();
  });

  /**
   * **The exact complaint the owner hit**, and the sentence that answers it. `ECONNREFUSED` and
   * `ENOENT` on a borg address mean one thing, and reporting the errno reports the symptom to
   * somebody who needs the cause.
   */
  test("no server at the address says so, and says how to start one", async () => {
    const nowhere = join(dir, "nothing-here.sock");
    const refused = await createBorgContext({ url: `borg+unix://${nowhere}/main` }).then(
      () => null,
      (err: unknown) => err,
    );
    expect(refused).toBeInstanceOf(BorgUnreachableError);
    expect((refused as Error).message).toBe(
      `no borg server at ${nowhere} — start one with: borg-server start`,
    );
  });

  /**
   * **FRICTION #11.** One context, a server restarted underneath it, and no new context anywhere.
   *
   * The close arrives while this process is idle, so the dead socket is dropped *before* the next
   * request is written rather than after it fails — which is what makes a bounce cost nothing at
   * all rather than one guaranteed error per client. Nothing is retried either way: nothing had
   * been sent.
   */
  test("a context survives the server being stopped and started under it", async () => {
    const bc = await createBorgContext({ url: `borg+unix://${socket}/main` });
    const tx = await bc.branch("main").begin();
    await tx.object(Company, "#71").set("headcount", 1);
    await tx.commit();
    expect(bc.connected).toBe(true);

    await bounce();

    expect((await bc.branch("main").get("Company#71.headcount")).value).toBe("1");
    expect(bc.connected).toBe(true);

    // And it can still write, which is the half a read-only reconnect would not prove.
    const after = await bc.branch("main").begin();
    await after.object(Company, "#71").set("headcount", 2);
    await after.commit();
    expect((await bc.branch("main").get("Company#71.headcount")).value).toBe("2");
    bc.close();
  });

  /**
   * **Transactions survive reconnection by construction**, which is the whole reason §12.2 puts a
   * transaction beside the store rather than in the connection: the handle is an id, the state is a
   * sidecar, and a socket is not part of either.
   *
   * *Failing means the reconnect story §12.2 was designed for does not actually work, and a client
   * that loses its connection mid-transaction loses the transaction with it.*
   */
  test("a transaction begun before a bounce commits after it", async () => {
    const bc = await createBorgContext({ url: `borg+unix://${socket}/main` });
    const tx = await bc.branch("main").begin();
    const contact = await tx.create(Contact);
    await contact.set("name", "Grace");
    const id = contact.id;

    await bounce();

    // The same handle, over a connection that did not exist when it was opened.
    const landed = await tx.commit();
    expect(landed).toMatch(/^L\d+$/);
    expect(await bc.branch("main").list(Contact)).toContain(id);

    // And the other half of §12.2: a *new* context can pick one up by id, which is what a process
    // that restarted rather than merely reconnected has to do.
    const second = await bc.branch("main").begin();
    await second.object(Contact, id).set("name", "Grace Hopper");
    await bounce();
    const resumed = (await createBorgContext({ url: `borg+unix://${socket}/main` })).transaction(
      second.id,
    );
    await resumed.commit();
    expect(await bc.branch("main").begin().then((t) => t.object(Contact, id).get("name"))).toBe(
      "Grace Hopper",
    );
    bc.close();
  });

  /**
   * **An operation that was in flight when the socket died fails, and is never retried.**
   *
   * A stand-in server, and the one in this file — because what is under test is the SDK's own
   * behaviour when a reply never comes, and making a *real* server drop a connection at exactly
   * that instant is a race rather than a test. This one accepts, says hello, and then destroys the
   * socket the moment a request arrives, which is the failure a `kill -9` mid-request produces.
   *
   * A retry here is the thing that must not happen: `tx_commit` is not idempotent, and a commit
   * whose answer was lost is indistinguishable from one that never arrived (`BorgDisconnectedError`).
   */
  test("a request whose answer never comes fails as disconnected, and the context recovers", async () => {
    const path = join(dir, "rude.sock");
    let requests = 0;
    const rude: Server = createServer((peer) => {
      peer.write(`${JSON.stringify({ version: 1, codecs: ["json"] })}\n`);
      // Counted by *lines* rather than by `data` events: the hello and the first request are two
      // writes that the kernel is free to deliver as one chunk, and a stand-in that assumed
      // otherwise would pass or hang depending on timing.
      let seen = 0;
      let pending = "";
      peer.on("data", (chunk: Buffer) => {
        pending += chunk.toString("utf8");
        for (let nl = pending.indexOf("\n"); nl >= 0; nl = pending.indexOf("\n")) {
          pending = pending.slice(nl + 1);
          seen += 1;
          // Line 1 is the client's hello. Line 2 is a request, and this server answers none.
          if (seen >= 2) {
            requests += 1;
            peer.destroy();
            return;
          }
        }
      });
    });
    await new Promise<void>((done) => rude.listen(path, done));

    const bc = await createBorgContext({ url: `borg+unix://${path}/main` });
    const failed = await bc.branches().then(
      () => null,
      (err: unknown) => err,
    );
    expect(failed).toBeInstanceOf(BorgDisconnectedError);
    expect((failed as Error).message).toContain("not retried");
    expect((failed as BorgDisconnectedError).address).toBe(path);
    expect(bc.connected).toBe(false);

    // The next operation dials again — one connection per failure, and exactly one request sent per
    // operation. Two requests for two operations is the assertion that nothing was resent.
    await expect(bc.branches()).rejects.toBeInstanceOf(BorgDisconnectedError);
    expect(requests).toBe(2);

    bc.close();
    await new Promise<void>((done) => rude.close(() => done()));
  });

  /**
   * A process that starts *before* its server — a supervisor bringing both up, a container, a dev
   * script — and answers honestly until the server arrives. This is the shape
   * `examples/personal-crm`'s api takes, and the regression test for the original complaint.
   */
  test("a context can be built with no server running, and works once one appears", async () => {
    // Its own data directory: the one the suite's server is holding is locked by it, which is the
    // *point* of the lock and would make this a test of that instead.
    const home = join(dir, "late");
    const path = join(home, "borg.sock");
    execFileSync(BORG, ["--store", join(home, "main", "borg.db"), "init"], { stdio: "pipe" });

    const later = await createBorgContext({
      url: `borg+unix://${path}/main`,
      connect: "on-demand",
    });
    expect(later.connected).toBe(false);
    // Nothing there yet, and it says the same sentence a failed dial says.
    await expect(later.branches()).rejects.toBeInstanceOf(BorgUnreachableError);
    await expect(later.branches()).rejects.toThrow(/borg-server start/);

    const late = spawn(BORG_SERVER, ["start", "--foreground", "--data-dir", home, "--socket", path], {
      stdio: "pipe",
    });
    const gone = new Promise<void>((done) => {
      if (late.exitCode !== null || late.signalCode !== null) done();
      else late.once("exit", () => done());
    });
    try {
      await untilListening(path);
      // No new context, no restart: the same object that was failing a moment ago now works.
      expect((await later.branches()).map((b) => b.name)).toContain("main");
      expect(later.connected).toBe(true);
    } finally {
      late.kill("SIGTERM");
      await gone;
      later.close();
    }
  }, 60_000);

  /**
   * **The soak.** Bounce timing is racy territory and this project's history is explicit about the
   * frequency that counts: milestone C's ordering bug appeared one run in six and an EPIPE panic one
   * in forty, and both read as flakes. So this loops, and the count is
   * `$BORG_RECONNECT_BOUNCES` — 5 by default so the suite stays quick, and turned up to 25+ by hand.
   */
  test(
    "reconnection survives being done over and over",
    async () => {
      const bc = await createBorgContext({ url: `borg+unix://${socket}/main` });
      for (let round = 0; round < BOUNCES; round++) {
        await bounce();
        const tx = await bc.branch("main").begin();
        await tx.object(Company, "#72").set("headcount", round);
        await tx.commit();
        expect((await bc.branch("main").get("Company#72.headcount")).value).toBe(String(round));
      }
      bc.close();
    },
    300_000,
  );
});
