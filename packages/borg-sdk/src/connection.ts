/**
 * The connection to the engine: newline-delimited JSON, in strict request/response order.
 *
 * ## Why a socket
 *
 * A worker may be spoken to over its own stdio, and the shell pipelines are. That is not survivable
 * in a language with a `console.log`: one stray line anywhere — in the pipeline, in a dependency, in
 * a warning printed by the runtime — desynchronises the stream permanently, and the failure surfaces
 * far from its cause. So this SDK declares `"transport": "socket"`, the engine listens on a unix
 * socket and passes its path in `BORG_WORKER_SOCKET`, and **stdout is left entirely to the author**
 * (§17.4).
 *
 * The stdio path is still implemented, because the transport is the engine's choice and not this
 * library's, and because a worker started by hand has no socket to connect to.
 *
 * ## Requests are serialised
 *
 * The protocol is one reply per request on one stream, so two requests in flight would read each
 * other's answers. `await Promise.all([c.get("a"), c.get("b")])` is a thing an author will write on
 * the first day, so it has to be correct rather than merely discouraged: every request queues behind
 * the last, and the concurrency simply does not buy anything.
 */

import { createConnection, type Socket } from "node:net";
import { StringDecoder } from "node:string_decoder";
import type { Readable, Writable } from "node:stream";
import type { FromWorker, ToWorker } from "./protocol.js";

/** The environment variable the engine names the socket in. */
export const SOCKET_ENV = "BORG_WORKER_SOCKET";

export class BorgProtocolError extends Error {
  override readonly name = "BorgProtocolError";
}

export interface Connection {
  /** The next message from the engine, or `null` once it has hung up. */
  receive(): Promise<ToWorker | null>;
  send(message: FromWorker | { codec: string }): void;
  /** Send one request and read its reply, with nothing else able to interleave. */
  request(message: FromWorker): Promise<ToWorker>;
  close(): void;
}

/**
 * A newline-delimited message stream over any duplex pair.
 *
 * The decoder is `StringDecoder` rather than `chunk.toString()` because a multi-byte character can
 * straddle two chunks, and a repo whose strings are ASCII today will not be forever.
 */
class LineConnection implements Connection {
  #decoder = new StringDecoder("utf8");
  #pending = "";
  #ready: string[] = [];
  #waiting: ((line: string | null) => void)[] = [];
  #ended = false;
  #turn: Promise<unknown> = Promise.resolve();

  readonly #output: Writable;
  readonly #closer: () => void;

  constructor(input: Readable, output: Writable, closer: () => void) {
    this.#output = output;
    this.#closer = closer;
    input.on("data", (chunk: Buffer) => this.#absorb(this.#decoder.write(chunk)));
    input.on("end", () => this.#finish());
    input.on("close", () => this.#finish());
    input.on("error", () => this.#finish());
  }

  #absorb(text: string): void {
    this.#pending += text;
    let newline = this.#pending.indexOf("\n");
    while (newline >= 0) {
      const line = this.#pending.slice(0, newline).trim();
      this.#pending = this.#pending.slice(newline + 1);
      // Blank lines are ignored rather than fatal, matching the engine's own reader.
      if (line.length > 0) this.#deliver(line);
      newline = this.#pending.indexOf("\n");
    }
  }

  #deliver(line: string): void {
    const waiter = this.#waiting.shift();
    if (waiter) waiter(line);
    else this.#ready.push(line);
  }

  #finish(): void {
    if (this.#ended) return;
    this.#ended = true;
    for (const waiter of this.#waiting.splice(0)) waiter(null);
  }

  #line(): Promise<string | null> {
    const ready = this.#ready.shift();
    if (ready !== undefined) return Promise.resolve(ready);
    if (this.#ended) return Promise.resolve(null);
    return new Promise((resolve) => this.#waiting.push(resolve));
  }

  async receive(): Promise<ToWorker | null> {
    const line = await this.#line();
    if (line === null) return null;
    try {
      return JSON.parse(line) as ToWorker;
    } catch {
      throw new BorgProtocolError(`the engine sent something that is not JSON: ${line}`);
    }
  }

  send(message: FromWorker | { codec: string }): void {
    this.#output.write(`${JSON.stringify(message)}\n`);
  }

  request(message: FromWorker): Promise<ToWorker> {
    // Chained on the previous request rather than run beside it — see the header. The chain is kept
    // whatever happens, so one rejected request does not let the next one jump the queue.
    const turn = this.#turn.then(
      () => this.#exchange(message),
      () => this.#exchange(message),
    );
    this.#turn = turn.catch(() => undefined);
    return turn;
  }

  async #exchange(message: FromWorker): Promise<ToWorker> {
    this.send(message);
    const reply = await this.receive();
    if (reply === null) {
      throw new BorgProtocolError("the engine hung up in the middle of an invocation");
    }
    return reply;
  }

  close(): void {
    this.#closer();
  }
}

/** Connect however the engine asked to be spoken to. */
export async function connect(env: NodeJS.ProcessEnv = process.env): Promise<Connection> {
  const path = env[SOCKET_ENV];
  if (path === undefined || path === "") {
    // No socket on offer: the engine is speaking over this process's own pipes, and everything
    // written to stdout from here on is a protocol message.
    return new LineConnection(process.stdin, process.stdout, () => process.stdin.pause());
  }
  const socket = await new Promise<Socket>((resolve, reject) => {
    const attempt = createConnection(path);
    attempt.once("connect", () => resolve(attempt));
    attempt.once("error", (err: Error) =>
      reject(new BorgProtocolError(`${SOCKET_ENV}=${path}: ${err.message}`)),
    );
  });
  socket.setNoDelay(true);
  return new LineConnection(socket, socket, () => socket.end());
}
