/**
 * The worker loop, against a stand-in engine on a real unix socket.
 *
 * A fake socket would test the code and not the contract; this one accepts a connection, performs
 * the handshake the engine performs, and answers the messages the engine answers. What it asserts is
 * the conversation — which cells were asked for, in which order, and with what text — because that
 * conversation *is* the dependency capture. The SDK records nothing, so if the right `get` does not
 * cross the wire, the right invalidation does not happen.
 */

import { afterEach, beforeEach, describe as suite, expect, test } from "vitest";
import { createServer, type Server, type Socket } from "node:net";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { borg } from "../src/index.js";
import { producerId } from "../src/protocol.js";
import { SOCKET_ENV } from "../src/connection.js";
import type { Repo } from "../src/repo.js";

/** The engine's half of the connection, in the shape a test wants to write. */
class Engine {
  #lines: string[] = [];
  #waiting: ((line: string) => void)[] = [];
  #pending = "";

  readonly #socket: Socket;

  constructor(socket: Socket) {
    this.#socket = socket;
    socket.on("data", (chunk) => {
      this.#pending += chunk.toString("utf8");
      let at = this.#pending.indexOf("\n");
      while (at >= 0) {
        const line = this.#pending.slice(0, at).trim();
        this.#pending = this.#pending.slice(at + 1);
        if (line) {
          const waiter = this.#waiting.shift();
          if (waiter) waiter(line);
          else this.#lines.push(line);
        }
        at = this.#pending.indexOf("\n");
      }
    });
  }

  send(message: unknown): void {
    this.#socket.write(`${JSON.stringify(message)}\n`);
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  receive(): Promise<any> {
    const ready = this.#lines.shift();
    const line =
      ready !== undefined
        ? Promise.resolve(ready)
        : new Promise<string>((resolve) => this.#waiting.push(resolve));
    return line.then((text) => JSON.parse(text));
  }

  end(): void {
    this.#socket.end();
  }
}

let server: Server;
let dir: string;
let path: string;
let previous: string | undefined;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "borg-sdk-"));
  path = join(dir, "worker.sock");
  previous = process.env[SOCKET_ENV];
  process.env[SOCKET_ENV] = path;
});

afterEach(() => {
  server?.close();
  if (previous === undefined) delete process.env[SOCKET_ENV];
  else process.env[SOCKET_ENV] = previous;
  rmSync(dir, { recursive: true, force: true });
});

/**
 * Listen, start the repo's worker loop against it, and complete the handshake.
 *
 * The listener has to exist before the worker does — which is what the engine itself does, and for
 * the same reason: a worker that starts first finds nothing to connect to.
 */
async function start(repo: Repo): Promise<{ side: Engine; running: Promise<void> }> {
  const connected = new Promise<Socket>((resolve) => {
    server = createServer(resolve);
  });
  await new Promise<void>((resolve) => server.listen(path, resolve));

  const running = repo.main([]);
  const side = new Engine(await connected);
  side.send({ version: 1, codecs: ["msgpack", "json"] });
  expect(await side.receive()).toEqual({ codec: "json" });
  return { side, running };
}

const Company = borg.struct("Company", {
  website: borg.string(),
  headcount: borg.int(),
  founded: borg.int(),
  isInvestible: borg.bool().derived(),
});

function investing(body: Parameters<typeof borg.pipeline<typeof Company.fields>>[3]) {
  const invest = borg.pipeline("invest", Company, { writes: ["isInvestible"] }, body);
  return borg.repo({ id: 1, structs: [Company], pipelines: [invest] });
}

const INVOKE = {
  invoke: { producer: producerId("invest"), input: "Company:o-04068" },
};

suite("serving invocations", () => {
  test("a get is a wire message, and a set carries the canonical text", async () => {
    const repo = investing(async (c) => {
      const website = await c.get("website");
      const headcount = await c.get("headcount");
      await c.set("isInvestible", website?.endsWith(".ai") === true && (headcount ?? 0) > 10);
    });
    const { side, running } = await start(repo);

    side.send(INVOKE);
    expect(await side.receive()).toEqual({ get: "Company:o-04068.website" });
    side.send({ value: "acme.ai" });
    expect(await side.receive()).toEqual({ get: "Company:o-04068.headcount" });
    side.send({ value: "40" });
    expect(await side.receive()).toEqual({
      set: { cell: "Company:o-04068.isInvestible", value: "true" },
    });
    side.send({ ok: {} });
    expect(await side.receive()).toEqual({ done: {} });

    side.send({ shutdown: {} });
    await running;
  });

  /**
   * The point of the socket, from this side: the protocol is on a file descriptor of its own, so
   * stdout is the author's and a `console.log` costs nothing.
   */
  test("a pipeline may print to stdout mid-invocation without touching the protocol", async () => {
    const repo = investing(async (c) => {
      console.log("about to read the website");
      const website = await c.get("website");
      process.stdout.write("no newline, either\n");
      await c.set("isInvestible", website !== null);
    });
    const { side, running } = await start(repo);

    side.send(INVOKE);
    expect(await side.receive()).toEqual({ get: "Company:o-04068.website" });
    side.send({ value: "acme.ai" });
    expect(await side.receive()).toEqual({
      set: { cell: "Company:o-04068.isInvestible", value: "true" },
    });
    side.send({ ok: {} });
    expect(await side.receive()).toEqual({ done: {} });

    side.send({ shutdown: {} });
    await running;
  });

  /** `Value(None)` is a cell that has never been written; `~` is one explicitly deleted (§8.1). */
  test("an absent cell and a tombstone both read as null, and null writes a tombstone", async () => {
    const seen: (number | null)[] = [];
    const repo = investing(async (c) => {
      seen.push(await c.get("headcount"));
      seen.push(await c.get("founded"));
      await c.set("isInvestible", null);
    });
    const { side, running } = await start(repo);

    side.send(INVOKE);
    await side.receive();
    side.send({ value: null });
    await side.receive();
    side.send({ value: "~" });
    expect(await side.receive()).toEqual({
      set: { cell: "Company:o-04068.isInvestible", value: "~" },
    });
    side.send({ ok: {} });
    expect(await side.receive()).toEqual({ done: {} });
    expect(seen).toEqual([null, null]);

    side.send({ shutdown: {} });
    await running;
  });

  /**
   * One reply per request on one stream, so two requests in flight would read each other's answers.
   * `Promise.all` is a thing an author writes on the first day, so it has to be correct.
   */
  test("concurrent reads are serialised rather than crossed", async () => {
    const repo = investing(async (c) => {
      const [website, headcount] = await Promise.all([c.get("website"), c.get("headcount")]);
      expect(website).toBe("acme.ai");
      expect(headcount).toBe(40);
      await c.set("isInvestible", true);
    });
    const { side, running } = await start(repo);

    side.send(INVOKE);
    expect(await side.receive()).toEqual({ get: "Company:o-04068.website" });
    side.send({ value: "acme.ai" });
    expect(await side.receive()).toEqual({ get: "Company:o-04068.headcount" });
    side.send({ value: "40" });
    expect(await side.receive()).toEqual({
      set: { cell: "Company:o-04068.isInvestible", value: "true" },
    });
    side.send({ ok: {} });
    expect(await side.receive()).toEqual({ done: {} });

    side.send({ shutdown: {} });
    await running;
  });

  test("the world is random access to any cell, stringly or converted", async () => {
    const repo = investing(async (c, world) => {
      expect(await world.get("Company:o-99999.website")).toBe("rival.ai");
      expect(await world.get("Company:o-99999.headcount", borg.int())).toBe(7);
      await world.set("Note:o-12345.body", "seen");
      await c.set("isInvestible", false);
    });
    const { side, running } = await start(repo);

    side.send(INVOKE);
    expect(await side.receive()).toEqual({ get: "Company:o-99999.website" });
    side.send({ value: "rival.ai" });
    expect(await side.receive()).toEqual({ get: "Company:o-99999.headcount" });
    side.send({ value: "7" });
    expect(await side.receive()).toEqual({ set: { cell: "Note:o-12345.body", value: "seen" } });
    side.send({ ok: {} });
    await side.receive();
    side.send({ ok: {} });
    expect(await side.receive()).toEqual({ done: {} });

    side.send({ shutdown: {} });
    await running;
  });
});

suite("failure stays inside one invocation", () => {
  test("a pipeline that throws reports the failure and the worker keeps serving", async () => {
    let attempt = 0;
    const repo = investing(async (c) => {
      attempt += 1;
      if (attempt === 1) throw new Error("no website to speak of");
      await c.set("isInvestible", false);
    });
    const { side, running } = await start(repo);

    side.send(INVOKE);
    expect(await side.receive()).toEqual({ error: { message: "no website to speak of" } });

    // The stream is still in step, which is what makes a failed invocation cost one entity and not
    // the process.
    side.send(INVOKE);
    expect(await side.receive()).toEqual({
      set: { cell: "Company:o-04068.isInvestible", value: "false" },
    });
    side.send({ ok: {} });
    expect(await side.receive()).toEqual({ done: {} });

    side.send({ shutdown: {} });
    await running;
  });

  test("writing a field the pipeline did not declare fails before it reaches the engine", async () => {
    const repo = investing(async (c) => {
      // Reachable from JS, and from TS by defeating the types — either way it is the engine's
      // ownership rule, restated where the author can act on it.
      await c.set("website" as never, "acme.ai" as never);
    });
    const { side, running } = await start(repo);

    side.send(INVOKE);
    const reply = await side.receive();
    expect(reply.error.message).toMatch(/does not declare `website`/);

    side.send({ shutdown: {} });
    await running;
  });

  test("an invocation the engine does not implement is reported rather than ignored", async () => {
    const repo = investing(async () => {});
    const { side, running } = await start(repo);

    side.send({ invoke: { producer: producerId("unknown"), input: "Company:o-04068" } });
    const reply = await side.receive();
    expect(reply.error.message).toMatch(/does not implement/);

    side.send({ shutdown: {} });
    await running;
  });

  /**
   * A forgotten `await` would otherwise send a `get` during the *next* invocation and take the reply
   * belonging to it — one entity's value written to another, silently.
   */
  test("a leaked context stops working the moment its invocation ends", async () => {
    let leaked: { get: (field: "website") => Promise<string | null> } | undefined;
    const repo = investing(async (c) => {
      leaked = c;
      await c.set("isInvestible", true);
    });
    const { side, running } = await start(repo);

    side.send(INVOKE);
    await side.receive();
    side.send({ ok: {} });
    expect(await side.receive()).toEqual({ done: {} });

    await expect(leaked!.get("website")).rejects.toThrow(/already finished/);

    side.send({ shutdown: {} });
    await running;
  });
});

suite("describe mode", () => {
  test("`describe` prints the payload and serves nothing", async () => {
    const repo = investing(async () => {});
    const written: string[] = [];
    const original = process.stdout.write.bind(process.stdout);
    // The one place the SDK does use stdout, and the reason `describe` is a separate mode.
    process.stdout.write = ((chunk: string) => {
      written.push(String(chunk));
      return true;
    }) as typeof process.stdout.write;
    try {
      await repo.main(["describe"]);
    } finally {
      process.stdout.write = original;
    }
    expect(JSON.parse(written.join(""))).toEqual(repo.describe());
  });
});
