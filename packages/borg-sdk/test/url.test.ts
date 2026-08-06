/**
 * Connection URLs. SPEC.md §17.7.
 *
 * **The same table as `crates/borg-protocol/src/url.rs`**, deliberately case for case. One string
 * configures a client, and the whole value of that is that the *same* string can be pasted into a
 * `BORG_URL` a Rust CLI reads and a `BORG_URL` a node process reads — so two parsers that disagreed
 * about where a socket path ends would be worse than having only one of them.
 */

import { describe as suite, expect, test } from "vitest";
import {
  BorgUrlError,
  borgSocket,
  parseBorgUrl,
  wellKnownSocket,
} from "../src/connection.js";

/** The well-known address these cases resolve `borg://` against. */
const WELL_KNOWN = "/run/user/1000/borg.sock";
const ENV = { XDG_RUNTIME_DIR: undefined, HOME: undefined } as NodeJS.ProcessEnv;

/** What the Rust table asserts: the socket to dial, and the registry to name. */
function parsed(text: string): [string, string | null] {
  const url = parseBorgUrl(text);
  return [
    url.transport === "local" ? WELL_KNOWN : borgSocket(url, ENV),
    url.registry,
  ];
}

suite("connection urls", () => {
  test("a url names a socket and a registry", () => {
    const table: [string, string, string | null][] = [
      // The two forms in the documentation, which are the two anybody writes.
      ["borg://localhost/personal-crm", WELL_KNOWN, "personal-crm"],
      ["borg+unix:///path/to/borg.sock/personal-crm", "/path/to/borg.sock", "personal-crm"],
      // No registry: absent here is absent in the handshake, and the server decides.
      ["borg://localhost", WELL_KNOWN, null],
      ["borg://localhost/", WELL_KNOWN, null],
      ["borg:///crm", WELL_KNOWN, "crm"],
      // A socket whose last segment cannot be a registry name is all socket — which is the case
      // that makes the obvious spelling do the obvious thing.
      ["borg+unix:///tmp/borg.sock", "/tmp/borg.sock", null],
      ["borg+unix:///tmp/borg.sock/", "/tmp/borg.sock", null],
      // Both readings of an ambiguous path, said two ways.
      ["borg+unix:///run/borg/crm", "/run/borg", "crm"],
      ["borg+unix:///run/borg/crm/", "/run/borg/crm", null],
      // Registry names take the characters the server accepts, and no others.
      ["borg://localhost/my_app-2", WELL_KNOWN, "my_app-2"],
    ];
    for (const [text, socket, registry] of table) {
      expect(parsed(text), text).toEqual([socket, registry]);
    }
  });

  test("a url that is not one says so and quotes itself", () => {
    const table: [string, string][] = [
      ["/tmp/borg.sock", "it needs a scheme"],
      ["borg.sock", "it needs a scheme"],
      ["", "it needs a scheme"],
      ["postgres://localhost/crm", "`postgres` is not a borg transport"],
      ["borg://example.com/crm", "`example.com` is not reachable"],
      ["borg://localhost/a/b", "more than one path segment"],
      ["borg://localhost/has.dot", "is not a registry name"],
      ["borg+unix://tmp/borg.sock", "three slashes"],
      ["borg+unix:///", "it names no socket path"],
      ["borg://localhost/crm?tls=1", "`?` has no meaning here"],
    ];
    for (const [text, needle] of table) {
      let refusal: unknown;
      try {
        parseBorgUrl(text);
      } catch (err) {
        refusal = err;
      }
      expect(refusal, `parsing \`${text}\` should have been refused`).toBeInstanceOf(BorgUrlError);
      const said = (refusal as Error).message;
      expect(said, text).toContain(needle);
      // A refusal quotes the url back, because a url is usually in a variable somebody has to go
      // and find rather than on the command line they are looking at.
      expect(said, text).toContain(`\`${text}\``);
    }
  });

  /**
   * The transport that does not exist yet is named rather than left to be invented. A client that
   * guessed a spelling today would be wrong on the day the real one ships.
   */
  test("the websocket transport is reserved and refused by name", () => {
    for (const text of ["borg+ws://borg.example/crm", "borg+wss://borg.example/crm"]) {
      expect(() => parseBorgUrl(text)).toThrow(/not yet supported/);
      expect(() => parseBorgUrl(text)).toThrow(/borg\+ws:\/\//);
    }
  });

  test("an absent registry is absent rather than guessed", () => {
    for (const text of ["borg://localhost", "borg+unix:///tmp/borg.sock"]) {
      expect(parseBorgUrl(text).registry, text).toBeNull();
    }
  });

  /**
   * The well-known address, which is the whole of what `borg://localhost` means — and the one rule
   * in this file that is genuinely reimplemented rather than shared, because `borg_host::host` is
   * Rust and this is not. `scenarios/310` is what holds the two answers together.
   */
  test("borg:// resolves to $XDG_RUNTIME_DIR/borg.sock, and to the data dir when there is none", () => {
    // The runtime dir has to *exist*: an exported variable naming a directory that is not there is
    // what a login shell on a container leaves behind, and a socket cannot be created in it.
    expect(wellKnownSocket({ XDG_RUNTIME_DIR: "/", HOME: "/home/ada" })).toBe("/borg.sock");
    expect(
      wellKnownSocket({ XDG_RUNTIME_DIR: "/no/such/runtime/dir", HOME: "/home/ada" }),
    ).toBe("/home/ada/.borg/borg.sock");
    expect(wellKnownSocket({ HOME: "/home/ada" })).toBe("/home/ada/.borg/borg.sock");
    // Homeless: a container, which is exactly where a server runs.
    expect(wellKnownSocket({})).toBe(".borg/borg.sock");
  });
});
