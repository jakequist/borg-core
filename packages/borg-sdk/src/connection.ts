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

/**
 * The environment variable an api key travels in when the url does not carry one (§17.6).
 *
 * The same name as `borg_host::keys::TOKEN_ENV` and as the CLI's, because a token with three
 * spellings gets configured in the wrong one.
 */
export const TOKEN_ENV = "BORG_TOKEN";

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
 * borg+ws://borg.example:7717/crm             a websocket, registry crm
 * borg+wss://borg.example/crm                 the same, through a TLS-terminating proxy
 * borg+wss://:borgk_A1b2@borg.example/crm     the same, presenting an api key (§17.6)
 * ```
 *
 * Everything a client needs is *where the server is* and *which registry on it* (§17.6), and those
 * two travel together or they get separated — which is how a staging client ends up pointed at
 * production's socket with production's registry name still in the other variable.
 *
 * **The credential rides in the userinfo**, `borg://:<key>@host/<registry>`, where `DATABASE_URL`
 * puts a password and where every deployment system already knows not to log it. There is no
 * username — a borg server authenticates a key and not a person — so the leading colon is optional
 * and a userinfo holding a second colon is refused rather than silently split. Every refusal here
 * quotes the url back, so [`redactBorgUrl`] takes the key out first: a connection string in an
 * environment variable, an error, and a log file is exactly how a secret escapes.
 *
 * **An absent registry stays absent.** It is not defaulted here: the server's rule is that a
 * handshake naming no registry gets the sole registry at n=1 and is refused with the options at
 * n≥2, and a client that filled in a guess would be re-implementing half of that rule and
 * disagreeing with the other half.
 *
 * **`borg://` is *the* local transport**, whatever that turns out to be — today the well-known unix
 * socket, resolved by [`wellKnownSocket`]. `borg+unix://` is the escape hatch for when the address
 * has to be said out loud: a scenario, a second server, a container mount.
 *
 * **`borg+ws://` is the transport a browser can open**, and the one a unix socket cannot serve. It
 * is a host and a port, and the port defaults the way `ws://`'s does — 80 plain, 443 secure —
 * because the whole argument for a WebSocket is that it rides infrastructure that already exists,
 * and that infrastructure listens on those two. `borg+wss://` is the same address behind a proxy
 * that terminates TLS; the runtime's own `WebSocket` does the TLS, so it costs this package
 * nothing. (`borg` the CLI refuses `borg+wss://`, because a Rust client would have to grow a
 * certificate store to speak it — `borg_protocol::url` says so where it says no.)
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
  /** `local` is the well-known address; `unix` is [`BorgUrl.path`]; `ws` is host and port. */
  readonly transport: "local" | "unix" | "ws";
  /** The socket, for `unix`. `null` for `local`, which [`borgSocket`] resolves, and for `ws`. */
  readonly path: string | null;
  /** For `ws`: the host, the port, and whether TLS. `null` otherwise. */
  readonly ws: { readonly secure: boolean; readonly host: string; readonly port: number } | null;
  /** The registry named in the url, or `null` — which is `null` in the handshake too. */
  readonly registry: string | null;
  /**
   * The api key from the userinfo, for `ClientHello.credential` (§17.6), or `null`.
   *
   * `null` is what a client of an open server carries — a server with no keys file authenticates
   * nobody — and what `$BORG_TOKEN` then fills in.
   */
  readonly credential: string | null;
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
  return new BorgUrlError(`\`${redactBorgUrl(text)}\` is not a borg url: ${why}`);
}

/**
 * **A url with its credential taken out**, for anything a human or a log will see (§17.6).
 *
 * `borg://:borgk_A1b2@host/crm` becomes `borg://:***@host/crm`: enough to see that a key was
 * supplied, and none of it. The same function exists as `borg_protocol::url::redacted` in Rust,
 * because both clients quote urls back in errors and one of them redacting is no protection.
 *
 * Exported because it is not only this file's problem — anything that reports a connection url has
 * the same secret in the same place.
 */
export function redactBorgUrl(text: string): string {
  const mark = text.indexOf("://");
  if (mark < 0) return text;
  const rest = text.slice(mark + 3);
  const slash = rest.indexOf("/");
  const authority = slash < 0 ? rest : rest.slice(0, slash);
  const at = authority.indexOf("@");
  return at < 0 ? text : `${text.slice(0, mark)}://:***@${rest.slice(at + 1)}`;
}

/**
 * **The credential, and the rest of the url without it.** §17.6, §17.7.
 *
 * The `@` is looked for before the first `/` so that a unix socket path containing one — legal, if
 * unusual — is not read as an authority. `:<key>@` and `<key>@` mean the same thing, because there
 * is no username for the colon to be separating from; a userinfo with a second colon is refused
 * rather than truncated, which would fail much later as `credential not valid`.
 */
function userinfo(text: string, rest: string): { credential: string | null; rest: string } {
  const slash = rest.indexOf("/");
  const authority = slash < 0 ? rest : rest.slice(0, slash);
  const at = authority.indexOf("@");
  if (at < 0) return { credential: null, rest };
  const written = authority.slice(0, at);
  const key = written.startsWith(":") ? written.slice(1) : written;
  if (key.includes(":")) {
    throw malformed(
      text,
      "a borg url has no username — the credential is the whole userinfo, as " +
        "borg://:<key>@host/<registry>",
    );
  }
  if (key === "") {
    throw malformed(
      text,
      "it has an empty credential — leave the `@` out to present none, or write " +
        "borg://:<key>@host/<registry>",
    );
  }
  return { credential: key, rest: rest.slice(at + 1) };
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
  const whole = text.slice(mark + 3);
  const stray = /[?#]/.exec(whole);
  if (stray !== null) {
    throw malformed(
      text,
      `\`${stray[0]}\` has no meaning here — a borg url is a transport, an address and a ` +
        `registry, and nothing else`,
    );
  }
  // **The credential comes off first, whatever the transport.** It is a property of the connection
  // rather than of the address, so every scheme takes it in the same place and no parser below has
  // to know it was ever there.
  const { credential, rest } = userinfo(text, whole);
  const parsed = ((): BorgUrl => {
    switch (scheme) {
      case "borg":
        return local(text, rest);
      case "borg+unix":
        return unix(text, rest);
      case "borg+ws":
        return websocket(text, rest, false);
      case "borg+wss":
        return websocket(text, rest, true);
      default:
        throw malformed(
          text,
          `\`${scheme}\` is not a borg transport — try borg://, borg+unix://, borg+ws:// or ` +
            `borg+wss://`,
        );
    }
  })();
  return { ...parsed, credential };
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
  return {
    transport: "local",
    path: null,
    ws: null,
    registry: registrySegment(text, tail),
    credential: null,
  };
}

/**
 * `borg+ws://<host>[:<port>][/<registry>]`, and the same for `borg+wss://`. See [`BorgUrl`] for why
 * the port defaults to 80 and 443 rather than to a number of borg's own.
 */
function websocket(text: string, rest: string, secure: boolean): BorgUrl {
  const slash = rest.indexOf("/");
  const authority = slash < 0 ? rest : rest.slice(0, slash);
  const tail = slash < 0 ? "" : rest.slice(slash + 1);
  const noHost = (): never => {
    throw malformed(text, "it names no host — borg+ws://<host>[:<port>]/<registry>");
  };
  if (authority === "") noHost();
  const colon = authority.lastIndexOf(":");
  const host = colon < 0 ? authority : authority.slice(0, colon);
  const written = colon < 0 ? null : authority.slice(colon + 1);
  if (host === "") noHost();
  if (written !== null && !/^\d{1,5}$/.test(written)) {
    throw malformed(text, `\`${written}\` is not a port — borg+ws://<host>:<port>/<registry>`);
  }
  const port = written === null ? (secure ? 443 : 80) : Number(written);
  if (port > 65535) {
    throw malformed(text, `\`${written}\` is not a port — borg+ws://<host>:<port>/<registry>`);
  }
  return {
    transport: "ws",
    path: null,
    ws: { secure, host, port },
    registry: registrySegment(text, tail),
    credential: null,
  };
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
    return { transport: "unix", path: socket, ws: null, registry: null, credential: null };
  }
  const split = rest.lastIndexOf("/");
  const head = rest.slice(0, split);
  const last = rest.slice(split + 1);
  if (head !== "" && nameIsValid(last)) {
    return { transport: "unix", path: head, ws: null, registry: last, credential: null };
  }
  return { transport: "unix", path: rest, ws: null, registry: null, credential: null };
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

/**
 * **Where a parsed url says to dial**, with `borg://` resolved against the well-known address.
 *
 * A `unix` address is a path and a `ws` address is a url; both print as themselves, which is what
 * `BorgContext.address` hands back and what every error message names.
 */
export type BorgAddress =
  | { readonly kind: "unix"; readonly path: string }
  | { readonly kind: "ws"; readonly url: string };

/** The address a parsed url says to dial. */
export function borgAddress(url: BorgUrl, env: NodeJS.ProcessEnv = process.env): BorgAddress {
  if (url.transport === "ws") {
    const ws = url.ws;
    if (ws === null) throw new BorgUrlError("a ws url with no host — this is a parser bug");
    // **The request path is `/` and carries no registry.** The registry travels in `ClientHello`
    // and nowhere else (§17.6); a second place to say it is a second thing to disagree.
    return { kind: "ws", url: `${ws.secure ? "wss" : "ws"}://${ws.host}:${ws.port}/` };
  }
  return { kind: "unix", path: url.path ?? wellKnownSocket(env) };
}

/** What an address is called, in an error or in `BorgContext.address`. */
export function addressText(address: BorgAddress): string {
  // A trailing slash on a url is what a dial needs and noise in a message, so it goes here.
  return address.kind === "unix" ? address.path : address.url.replace(/\/$/, "");
}

/** The socket a parsed url says to dial, for the `unix` case. Kept for callers that only have one. */
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

/**
 * The same dial over a WebSocket, with the same two outcomes.
 *
 * **The runtime's own `WebSocket`**, global since Node 21 and forever in a browser — no dependency,
 * which is what lets this package be dropped into anything, and no TLS code of its own, which is
 * what makes `borg+wss://` free here and expensive in Rust.
 *
 * The failure classes are the ones a unix socket has: nothing listening is
 * [`BorgUnreachableError`] with the same sentence, and anything else is reported as itself. A
 * browser's `WebSocket` deliberately does not tell a page *why* a connection failed, so the
 * distinction is one the environment sometimes cannot make and this reports what it was given.
 */
export async function dialBorgWebSocket(url: string): Promise<WebSocket> {
  if (typeof WebSocket === "undefined") {
    throw new BorgProtocolError(
      `${url}: this runtime has no WebSocket — node 21+ has one globally, and a browser has ` +
        `always had one`,
    );
  }
  return new Promise<WebSocket>((resolve, reject) => {
    const socket = new WebSocket(url);
    const opened = (): void => {
      socket.removeEventListener("error", failed);
      resolve(socket);
    };
    const failed = (): void => {
      socket.removeEventListener("open", opened);
      // A WebSocket error event carries no errno anywhere, and in a browser carries nothing at all
      // by design. "Nothing is listening" is overwhelmingly what it means, and it is the sentence
      // that tells somebody what to do — so it is the one reported, with the address in it.
      reject(new BorgUnreachableError(url.replace(/\/$/, "")));
    };
    socket.addEventListener("open", opened, { once: true });
    socket.addEventListener("error", failed, { once: true });
  });
}
