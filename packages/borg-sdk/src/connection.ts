/**
 * The worker's connection to the engine. §17.4.
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
 * The framing itself lives in [`./lines.js`](./lines.js), because the client protocol (§17.5) is the
 * same framing carrying a different message set.
 */

import { createConnection, type Socket } from "node:net";
import { BorgProtocolError, LineStream, type MessageStream } from "./lines.js";
import type { FromWorker, ToWorker } from "./protocol.js";

/** The environment variable the engine names the socket in. */
export const SOCKET_ENV = "BORG_WORKER_SOCKET";

export { BorgProtocolError } from "./lines.js";

/**
 * The worker half of §17.4. `{ codec: string }` is in the send type because the handshake reply is
 * not one of the `FromWorker` variants — it is a single key `codec` and appears in no message table,
 * which SDK-DRAFT §4.2 records as one of the things a second SDK had to reverse-engineer.
 */
export type Connection = MessageStream<ToWorker, FromWorker | { codec: string }>;

/** Connect however the engine asked to be spoken to. */
export async function connect(env: NodeJS.ProcessEnv = process.env): Promise<Connection> {
  const path = env[SOCKET_ENV];
  if (path === undefined || path === "") {
    // No socket on offer: the engine is speaking over this process's own pipes, and everything
    // written to stdout from here on is a protocol message.
    return new LineStream(process.stdin, process.stdout, () => process.stdin.pause());
  }
  const socket = await openUnixSocket(path, `${SOCKET_ENV}=${path}`);
  return new LineStream(socket, socket, () => socket.end());
}

/**
 * A connected unix socket, or an error naming where it was asked to connect.
 *
 * Shared with the client SDK: "connection refused" without an address is the least useful error a
 * socket can produce, and both halves of this package have exactly one address to name.
 */
export async function openUnixSocket(path: string, what: string): Promise<Socket> {
  const socket = await new Promise<Socket>((resolve, reject) => {
    const attempt = createConnection(path);
    attempt.once("connect", () => resolve(attempt));
    attempt.once("error", (err: Error) =>
      reject(new BorgProtocolError(`${what}: ${err.message}`)),
    );
  });
  socket.setNoDelay(true);
  return socket;
}
