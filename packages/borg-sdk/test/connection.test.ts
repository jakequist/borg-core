/**
 * Choosing a transport, from the worker's side.
 *
 * The engine decides; this side only reads what it was told. `BORG_WORKER_SOCKET` means a socket,
 * and nothing means the process's own pipes — which is where every byte written to stdout from then
 * on is a protocol message.
 */

import { afterEach, expect, test } from "vitest";
import { connect, SOCKET_ENV } from "../src/connection.js";

const original = process.stdout.write.bind(process.stdout);

afterEach(() => {
  process.stdout.write = original;
});

test("with no socket on offer, the connection is this process's own pipes", async () => {
  const written: string[] = [];
  process.stdout.write = ((chunk: string) => {
    written.push(String(chunk));
    return true;
  }) as typeof process.stdout.write;

  const conn = await connect({});
  conn.send({ done: {} });
  conn.close();

  expect(written).toEqual(['{"done":{}}\n']);
});

test("a socket that is not there is refused by name, rather than hanging", async () => {
  await expect(connect({ [SOCKET_ENV]: "/nonexistent/borg.sock" })).rejects.toThrow(
    /BORG_WORKER_SOCKET=\/nonexistent\/borg\.sock/,
  );
});
