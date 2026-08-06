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

import { existsSync } from "node:fs";
import { createConnection, type Socket } from "node:net";
import { join } from "node:path";
import { BorgProtocolError, LineStream, type MessageStream } from "./lines.js";
import type { FromWorker, ToWorker } from "./protocol.js";

/** The environment variable the engine names the socket in. */
export const SOCKET_ENV = "BORG_WORKER_SOCKET";

/** The environment variable a *client* is configured with. See [`parseBorgUrl`]. */
export const URL_ENV = "BORG_URL";

/** The socket's name, wherever it is put — `borg_host::host::SOCKET_FILE`. */
const SOCKET_FILE = "borg.sock";

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

// --- Connection URLs. SPEC.md §17.7. --------------------------------------------------------------

/**
 * **A connection URL: one string that configures a client**, the way `DATABASE_URL` does.
 *
 * ```text
 * borg://localhost/personal-crm               the well-known local address, registry personal-crm
 * borg://localhost                            the well-known local address, no registry named
 * borg+unix:///run/user/1000/borg.sock/crm    an explicit socket, registry crm
 * borg+unix:///tmp/borg.sock                  an explicit socket, no registry named
 * borg+ws://borg.example/crm                  reserved; parsed and refused
 * ```
 *
 * Everything a client needs is *where the server is* and *which registry on it* (§17.6), and those
 * two travel together or they get separated — which is how a staging client ends up pointed at
 * production's socket with production's registry name still in the other variable.
 *
 * **An absent registry stays absent.** It is not defaulted here: the server's rule is that a
 * handshake naming no registry gets the sole registry at n=1 and is refused with the options at
 * n≥2, and a client that filled in a guess would be re-implementing half of that rule and
 * disagreeing with the other half.
 *
 * **`borg://` is *the* local transport**, whatever that turns out to be — today the well-known unix
 * socket, resolved by [`wellKnownSocket`]. `borg+unix://` is the escape hatch for when the address
 * has to be said out loud: a scenario, a second server, a container mount. `borg+ws://` is reserved
 * for the browser transport a unix socket cannot serve, and is refused *by name* so that nobody
 * invents a spelling for it in the meantime.
 *
 * **Where the socket ends and the registry begins**, for `borg+unix://`: the last path segment is
 * the registry when it could *be* a registry name — letters, digits, `-` and `_`, which is the rule
 * the server itself enforces — and part of the socket path when it could not. That is what makes
 * `borg+unix:///tmp/borg.sock` read as the socket it obviously is, because `borg.sock` has a dot in
 * it and no registry ever can. A trailing slash always means "no registry", so both readings of an
 * ambiguous path are sayable: `/run/borg/crm` is the socket `/run/borg` and the registry `crm`, and
 * `/run/borg/crm/` is the socket `/run/borg/crm`.
 */
export interface BorgUrl {
  /** `local` is the well-known address; `unix` is [`BorgUrl.path`]. */
  readonly transport: "local" | "unix";
  /** The socket, for `unix`. `null` for `local`, which [`borgSocket`] resolves. */
  readonly path: string | null;
  /** The registry named in the url, or `null` — which is `null` in the handshake too. */
  readonly registry: string | null;
}

/** A url that is not one, named with enough detail to fix it. */
export class BorgUrlError extends Error {
  override readonly name = "BorgUrlError";
}

/** Nothing is listening where a client was told to connect. See [`dialBorgServer`]. */
export class BorgUnreachableError extends BorgProtocolError {
  override readonly name = "BorgUnreachableError";
  /** The address that answered nothing. */
  readonly address: string;

  constructor(address: string) {
    super(`no borg server at ${address} — start one with: borg-server start`);
    this.address = address;
  }
}

/**
 * The server's own rule for what may be a registry (`borg_host::host`), restated where a client can
 * apply it before spending a connection on it.
 */
function nameIsValid(name: string): boolean {
  return name.length > 0 && /^[A-Za-z0-9_-]+$/.test(name);
}

function malformed(text: string, why: string): BorgUrlError {
  return new BorgUrlError(`\`${text}\` is not a borg url: ${why}`);
}

/** Parse a connection url. Every refusal quotes it back — see [`BorgUrl`]. */
export function parseBorgUrl(text: string): BorgUrl {
  const mark = text.indexOf("://");
  if (mark < 0) {
    throw malformed(
      text,
      "it needs a scheme — borg://localhost/<registry> or " +
        "borg+unix:///path/to/borg.sock/<registry>",
    );
  }
  const scheme = text.slice(0, mark);
  const rest = text.slice(mark + 3);
  const stray = /[?#]/.exec(rest);
  if (stray !== null) {
    throw malformed(
      text,
      `\`${stray[0]}\` has no meaning here — a borg url is a transport, an address and a ` +
        `registry, and nothing else`,
    );
  }
  switch (scheme) {
    case "borg":
      return local(text, rest);
    case "borg+unix":
      return unix(text, rest);
    // Named rather than lumped in with the unknown schemes, because this one *will* exist and the
    // sentence a user gets should say so.
    case "borg+ws":
    case "borg+wss":
      throw malformed(
        text,
        "`borg+ws://` is reserved for the browser transport and is not yet supported — " +
          "today's transports are borg:// and borg+unix://",
      );
    default:
      throw malformed(
        text,
        `\`${scheme}\` is not a borg transport — try borg://, borg+unix:// or the reserved ` +
          `borg+ws://`,
      );
  }
}

/** `borg://<host>[/<registry>]`. */
function local(text: string, rest: string): BorgUrl {
  const slash = rest.indexOf("/");
  const host = slash < 0 ? rest : rest.slice(0, slash);
  const tail = slash < 0 ? "" : rest.slice(slash + 1);
  // An empty host is accepted (`borg:///crm`) because it is what a URL library produces when the
  // authority is omitted, and because there is exactly one thing it could mean.
  if (host !== "" && host !== "localhost") {
    throw malformed(
      text,
      `\`${host}\` is not reachable over the local transport — borg:// is this machine's ` +
        `well-known socket, and a remote server is the reserved borg+ws://`,
    );
  }
  return { transport: "local", path: null, registry: registrySegment(text, tail) };
}

/** `borg+unix://<socket-path>[/<registry>]`. See [`BorgUrl`] for where the two divide. */
function unix(text: string, rest: string): BorgUrl {
  if (!rest.startsWith("/")) {
    throw malformed(
      text,
      "borg+unix names an absolute socket path, so it takes three slashes — " +
        "borg+unix:///tmp/borg.sock",
    );
  }
  if (rest.endsWith("/")) {
    const socket = rest.slice(0, -1);
    if (socket === "") throw malformed(text, "it names no socket path");
    return { transport: "unix", path: socket, registry: null };
  }
  const split = rest.lastIndexOf("/");
  const head = rest.slice(0, split);
  const last = rest.slice(split + 1);
  if (head !== "" && nameIsValid(last)) {
    return { transport: "unix", path: head, registry: last };
  }
  return { transport: "unix", path: rest, registry: null };
}

/** The one path segment a `borg://` url may carry after the host. */
function registrySegment(text: string, tail: string): string | null {
  const trimmed = tail.replace(/\/+$/, "");
  if (trimmed === "") return null;
  if (trimmed.includes("/")) {
    throw malformed(
      text,
      `\`${trimmed}\` is more than one path segment — a borg url names one registry, as ` +
        `borg://localhost/<registry>`,
    );
  }
  if (!nameIsValid(trimmed)) {
    throw malformed(text, `\`${trimmed}\` is not a registry name — letters, digits, \`-\` and \`_\``);
  }
  return trimmed;
}

/**
 * **The address `borg://localhost` resolves to**, and the one rule in this file that is genuinely
 * reimplemented rather than shared: `borg_host::host::default_socket` is Rust and this is not.
 *
 * `$XDG_RUNTIME_DIR/borg.sock` when that directory *exists*, and `<data-dir>/borg.sock` when it
 * does not — which with the default data directory is `~/.borg/borg.sock`, and `.borg/borg.sock`
 * for a process with no `$HOME`, which is precisely a container. The existence check matters: an
 * exported `XDG_RUNTIME_DIR` naming a directory that is not there is what a login shell leaves
 * behind, and a socket cannot be created in it.
 */
export function wellKnownSocket(env: NodeJS.ProcessEnv = process.env): string {
  const runtime = env["XDG_RUNTIME_DIR"];
  if (runtime !== undefined && runtime !== "" && existsSync(runtime)) {
    return join(runtime, SOCKET_FILE);
  }
  const home = env["HOME"];
  const dataDir = home === undefined || home === "" ? ".borg" : join(home, ".borg");
  return join(dataDir, SOCKET_FILE);
}

/** The socket a parsed url says to dial. */
export function borgSocket(url: BorgUrl, env: NodeJS.ProcessEnv = process.env): string {
  return url.path ?? wellKnownSocket(env);
}

/**
 * Connect to a `borg-server`, saying something useful when there is none.
 *
 * `ECONNREFUSED` and `ENOENT` on a borg address mean one thing — nothing is serving there — and
 * reporting the errno is reporting the symptom to somebody who needs the cause. This is the
 * sentence `borg_protocol::url::unreachable` produces on the Rust side, in the same words, because
 * a message a user learns to recognise has to be the same message everywhere.
 *
 * Anything else is reported as itself: a permission error, or a path that is not a socket, is news.
 */
export async function dialBorgServer(path: string): Promise<Socket> {
  return new Promise<Socket>((resolve, reject) => {
    const attempt = createConnection(path);
    attempt.once("connect", () => {
      attempt.setNoDelay(true);
      resolve(attempt);
    });
    attempt.once("error", (err: NodeJS.ErrnoException) => {
      reject(
        err.code === "ENOENT" || err.code === "ECONNREFUSED"
          ? new BorgUnreachableError(path)
          : new BorgProtocolError(`borg socket ${path}: ${err.message}`),
      );
    });
  });
}
