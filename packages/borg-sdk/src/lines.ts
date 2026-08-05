/**
 * Newline-delimited JSON messages over a duplex pair, in strict request/response order.
 *
 * **Shared by both protocols this SDK speaks**, and that is the point of it being here: §17.4 (the
 * engine talking to a worker) and §17.5 (a client talking to `borg serve`) are two message sets over
 * *one* framing, and the Rust side says so by putting `read_message`/`write_message` in one crate.
 * A second copy of the buffering below would be a second place for a multi-byte character split
 * across two chunks to be handled slightly differently.
 *
 * ## Requests are serialised
 *
 * One reply per request on one stream, so two requests in flight would read each other's answers.
 * `await Promise.all([c.get("a"), c.get("b")])` is a thing an author writes on the first day, so it
 * has to be correct rather than merely discouraged: every request queues behind the last, and the
 * concurrency simply does not buy anything. (The Python SDK needs four lines of mutex for the same
 * property and only for a body that reaches for `threading` — SDK-DRAFT §4.2 records that this
 * machinery is JavaScript's problem and not the protocol's.)
 */

import { StringDecoder } from "node:string_decoder";
import type { Readable, Writable } from "node:stream";

export class BorgProtocolError extends Error {
  override readonly name = "BorgProtocolError";
}

/** A message stream. `In` is what arrives, `Out` is what may be sent. */
export interface MessageStream<In, Out> {
  /** The next message from the other end, or `null` once it has hung up. */
  receive(): Promise<In | null>;
  send(message: Out): void;
  /** Send one request and read its reply, with nothing else able to interleave. */
  request(message: Out): Promise<In>;
  close(): void;
}

/**
 * The decoder is `StringDecoder` rather than `chunk.toString()` because a multi-byte character can
 * straddle two chunks, and a repo whose strings are ASCII today will not be forever.
 */
export class LineStream<In, Out> implements MessageStream<In, Out> {
  #decoder = new StringDecoder("utf8");
  #pending = "";
  #ready: string[] = [];
  #waiting: ((line: string | null) => void)[] = [];
  #ended = false;
  #turn: Promise<unknown> = Promise.resolve();

  readonly #output: Writable;
  readonly #closer: () => void;
  /** Named in errors, so "the engine hung up" and "the server hung up" are the same code path. */
  readonly #peer: string;

  constructor(input: Readable, output: Writable, closer: () => void, peer = "the engine") {
    this.#output = output;
    this.#closer = closer;
    this.#peer = peer;
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

  async receive(): Promise<In | null> {
    const line = await this.#line();
    if (line === null) return null;
    try {
      return JSON.parse(line) as In;
    } catch {
      throw new BorgProtocolError(`${this.#peer} sent something that is not JSON: ${line}`);
    }
  }

  send(message: Out): void {
    this.#output.write(`${JSON.stringify(message)}\n`);
  }

  request(message: Out): Promise<In> {
    // Chained on the previous request rather than run beside it — see the header. The chain is kept
    // whatever happens, so one rejected request does not let the next one jump the queue.
    const turn = this.#turn.then(
      () => this.#exchange(message),
      () => this.#exchange(message),
    );
    this.#turn = turn.catch(() => undefined);
    return turn;
  }

  async #exchange(message: Out): Promise<In> {
    this.send(message);
    const reply = await this.receive();
    if (reply === null) {
      throw new BorgProtocolError(`${this.#peer} hung up in the middle of a request`);
    }
    return reply;
  }

  close(): void {
    this.#closer();
  }
}
