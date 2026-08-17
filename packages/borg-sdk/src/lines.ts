/**
 * JSON messages over a duplex pair, in strict request/response order.
 *
 * **Shared by both protocols this SDK speaks**, and that is the point of it being here: §17.4 (the
 * engine talking to a worker) and §17.5 (a client talking to `borg-server`) are two message sets over
 * *one* framing, and the Rust side says so by putting `read_message`/`write_message` in one crate.
 * A second copy of the buffering below would be a second place for a multi-byte character split
 * across two chunks to be handled slightly differently.
 *
 * ## Two transports, and the framing is the only difference
 *
 * [`LineStream`] is a byte stream — a pipe or a unix socket — where a newline is what makes a
 * message. [`WebSocketStream`] is a message stream already, so it has no delimiter at all. What they
 * share, in [`Framed`], is everything that is *not* framing: the queue, the ordering, and what
 * happens to a waiting reader when the transport ends. That last one is what a reconnect is built
 * on, and it has to behave identically over both or the guarantee is per-transport (SPEC.md §17.7).
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

/** A queued arrival, resolved as a value or rethrown as the failure it turned out to be. */
function settle<In>(arrived: In | BorgProtocolError | null): Promise<In | null> {
  return arrived instanceof BorgProtocolError
    ? Promise.reject(arrived)
    : Promise.resolve(arrived);
}

export class BorgProtocolError extends Error {
  // Annotated `string` rather than left to infer the literal, so that the subclasses in
  // `./client.ts` can name themselves. A `name` narrowed to one literal makes every specialisation
  // of an error a compile error, which is the opposite of what naming an error class is for.
  override readonly name: string = "BorgProtocolError";
}

/** A message stream. `In` is what arrives, `Out` is what may be sent. */
export interface MessageStream<In, Out> {
  /**
   * Whether the other end has already gone away.
   *
   * What it buys is a *transparent* reconnect: a client SDK that finds its socket in this state
   * before it has written anything can dial a fresh one and nothing has been sent twice. Without
   * it, the first operation after a server restart is always a failure — the connection is dead and
   * only writing to it discovers that — which turns a bounce into one guaranteed error per client
   * rather than none.
   */
  readonly closed: boolean;
  /** The next message from the other end, or `null` once it has hung up. */
  receive(): Promise<In | null>;
  send(message: Out): void;
  /** Send one request and read its reply, with nothing else able to interleave. */
  request(message: Out): Promise<In>;
  close(): void;
}

/**
 * The half of a message stream that has nothing to do with framing: a queue of arrived messages, a
 * queue of readers waiting for one, an end-of-stream flag, and the turn-taking that keeps requests
 * serialised.
 *
 * **Extracted when the WebSocket transport arrived**, and extracted rather than copied for the
 * reason this file already gives about the decoder: two implementations of "one reply per request,
 * in order, and a socket that ends fails whatever was waiting" would be two places for a reconnect
 * to behave slightly differently — and the reconnect semantics are exactly what has to hold
 * *identically* over both transports (SPEC.md §17.7). A subclass supplies only how a message
 * becomes bytes and how bytes become a message.
 */
abstract class Framed<In, Out> implements MessageStream<In, Out> {
  #ready: (In | BorgProtocolError)[] = [];
  #waiting: ((message: In | BorgProtocolError | null) => void)[] = [];
  #ended = false;
  #turn: Promise<unknown> = Promise.resolve();

  /** Named in errors, so "the engine hung up" and "the server hung up" are the same code path. */
  protected readonly peer: string;

  protected constructor(peer: string) {
    this.peer = peer;
  }

  /** Put a message on the wire. */
  abstract send(message: Out): void;
  /** Let go of the underlying transport. */
  abstract close(): void;

  /**
   * A subclass calls this once per message that arrived — or with an error, for one that arrived
   * and was not a message.
   *
   * **A decoding failure is delivered rather than thrown**, because both subclasses decode inside a
   * transport callback where nothing is waiting to catch it. Queued in order, it comes out of
   * [`receive`] as a rejection at exactly the point the caller was waiting, which is where a caller
   * can do something about it.
   */
  protected deliver(message: In | BorgProtocolError): void {
    const waiter = this.#waiting.shift();
    if (waiter) waiter(message);
    else this.#ready.push(message);
  }

  /** A subclass calls this when the transport has ended, however it ended. */
  protected finish(): void {
    if (this.#ended) return;
    this.#ended = true;
    for (const waiter of this.#waiting.splice(0)) waiter(null);
  }

  /**
   * See [`MessageStream.closed`]. Buffered messages still count as arrived, so this is about the
   * *stream*: once it has ended, nothing more will ever arrive on it.
   */
  get closed(): boolean {
    return this.#ended;
  }

  receive(): Promise<In | null> {
    const ready = this.#ready.shift();
    if (ready !== undefined) return settle(ready);
    if (this.#ended) return Promise.resolve(null);
    return new Promise<In | BorgProtocolError | null>((resolve) =>
      this.#waiting.push(resolve),
    ).then(settle);
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
      throw new BorgProtocolError(`${this.peer} hung up in the middle of a request`);
    }
    return reply;
  }
}

/**
 * Newline-delimited JSON over a byte stream: a pipe, or a unix socket.
 *
 * The decoder is `StringDecoder` rather than `chunk.toString()` because a multi-byte character can
 * straddle two chunks, and a repo whose strings are ASCII today will not be forever.
 */
export class LineStream<In, Out> extends Framed<In, Out> {
  #decoder = new StringDecoder("utf8");
  #pending = "";

  readonly #output: Writable;
  readonly #closer: () => void;

  constructor(input: Readable, output: Writable, closer: () => void, peer = "the engine") {
    super(peer);
    this.#output = output;
    this.#closer = closer;
    input.on("data", (chunk: Buffer) => this.#absorb(this.#decoder.write(chunk)));
    input.on("end", () => this.finish());
    input.on("close", () => this.finish());
    input.on("error", () => this.finish());
  }

  #absorb(text: string): void {
    this.#pending += text;
    let newline = this.#pending.indexOf("\n");
    while (newline >= 0) {
      const line = this.#pending.slice(0, newline).trim();
      this.#pending = this.#pending.slice(newline + 1);
      // Blank lines are ignored rather than fatal, matching the engine's own reader.
      if (line.length > 0) this.deliver(this.#parse(line));
      newline = this.#pending.indexOf("\n");
    }
  }

  #parse(line: string): In | BorgProtocolError {
    try {
      return JSON.parse(line) as In;
    } catch {
      return new BorgProtocolError(`${this.peer} sent something that is not JSON: ${line}`);
    }
  }

  send(message: Out): void {
    this.#output.write(`${JSON.stringify(message)}\n`);
  }

  close(): void {
    this.#closer();
  }
}

/**
 * **The same messages over a WebSocket, with the framing taken out.** SPEC.md §17.4, §17.6.
 *
 * A WebSocket is already a stream of messages, so the newline above has nothing left to do — and
 * putting one in anyway would be a delimiter inside a delimiter, with the outer one carrying a
 * character no reader looks for. So `send` writes one text frame per message and every arriving
 * frame is one message, and everything above this line — the ordering, the queue, the end-of-stream
 * behaviour that a reconnect depends on — is the shared half and is *not* reimplemented here.
 *
 * **The runtime's own `WebSocket`, and no dependency.** Node has had a global one since 21 and this
 * package's whole shape is that it can be dropped into anything; a browser has had one forever,
 * which is the client this transport exists for. That is also why the events are the browser's
 * (`addEventListener`) rather than node's `ws`-style emitter: one code path, both runtimes.
 */
export class WebSocketStream<In, Out> extends Framed<In, Out> {
  readonly #socket: WebSocket;

  constructor(socket: WebSocket, peer = "the server") {
    super(peer);
    this.#socket = socket;
    socket.addEventListener("message", (event: MessageEvent) => {
      // A text frame is JSON, which is the only codec this SDK offers (see `client.ts`). A binary
      // frame would be MessagePack and there is nothing here that could decode it, so it is
      // reported rather than silently dropped.
      if (typeof event.data !== "string") {
        this.deliver(
          new BorgProtocolError(
            `${this.peer} sent a binary frame, and this client only speaks json`,
          ),
        );
        return;
      }
      try {
        this.deliver(JSON.parse(event.data) as In);
      } catch {
        this.deliver(
          new BorgProtocolError(`${this.peer} sent something that is not JSON: ${event.data}`),
        );
      }
    });
    socket.addEventListener("close", () => this.finish());
    // An error on a WebSocket is always followed by a close, but a listener has to exist or the
    // runtime treats it as unhandled.
    socket.addEventListener("error", () => this.finish());
  }

  send(message: Out): void {
    this.#socket.send(JSON.stringify(message));
  }

  close(): void {
    try {
      this.#socket.close();
    } catch {
      // Closing one that is already closing is not news.
    }
  }
}
