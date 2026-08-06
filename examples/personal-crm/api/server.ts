// The CRM's API: `node:http` in front of `borg-sdk/client`. No framework, no ORM, no cache.
//
// Every route below is a shape of Borg operation:
//
//   POST /api/contacts      a transaction: begin → create → set each field → commit
//   GET  /api/contacts      an enumeration, then a read per contact per field (the N+1, visible)
//   GET  /api/contacts/:id  reads with their §10.4 envelopes, which is what the detail view shows
//
// It compiles against `gen/borg.generated.ts`, which `borg generate` wrote from the *store's* own
// definitions — so `firstName` is a field the compiler knows about, and `displayName` is one the
// compiler knows this process may not write.

import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import {
  BorgClientError,
  BorgProtocolError,
  BorgStateError,
  ConflictError,
  type BorgContext,
  type BranchHandle,
  type Resolved,
} from "borg-sdk/client";
import { Contact, CLIENT_VERSION, createBorgContext } from "./gen/borg.generated.ts";

const SOCKET = process.env.BORG_SOCKET;
const PORT = Number(process.env.PORT ?? 8787);
const BRANCH = process.env.BORG_BRANCH ?? "main";

if (SOCKET === undefined) {
  throw new Error("BORG_SOCKET is not set — start `borg serve --socket <path>` first (see dev.sh)");
}

// ── The fields this app knows about ───────────────────────────────────────────────────────────────
//
// Spelled out rather than taken from `Object.keys(Contact.fields)` because the *order* is a UI
// decision and the split is a semantic one: `EDITABLE` is what a form may submit, and it is exactly
// the set of fields no producer owns. Getting that wrong is a compile error — `WritableKeys` is what
// `handle.set` takes — which is the whole reason the api is written against generated types.

const EDITABLE = ["firstName", "lastName", "email", "phone", "notes"] as const;
type Editable = (typeof EDITABLE)[number];

/** Every field, in the order the detail view shows them. `displayName` last, because it is the derived one. */
const ALL = [...EDITABLE, "displayName"] as const;
type Any = (typeof ALL)[number];

// ── Reading outside a transaction ─────────────────────────────────────────────────────────────────

/**
 * One field of one contact, read on the branch rather than through a transaction.
 *
 * A read outside a transaction buys no protection at commit (§12.1), which for a GET is exactly
 * right — nothing is going to commit. What it costs is that there is **no object handle out here**:
 * `tx.object(Contact, id)` exists only inside a transaction, so the cell address is assembled by
 * hand from `Contact.name` and the id. The conversion is at least recoverable from the descriptor,
 * which is what keeps this generic helper typed: `Contact.fields[field].type` is
 * `FieldType<Contact[K]>`, so the envelope comes back at the field's declared type and not as text.
 */
function read<K extends Any>(
  branch: BranchHandle,
  id: string,
  field: K,
): Promise<Resolved<Contact[K] | null>> {
  return branch.get(`${Contact.name}:${id}.${field}`, { as: Contact.fields[field].type });
}

/** The envelope, flattened to what a browser needs to render honesty. */
interface FieldView {
  value: string | null;
  /** `current` | `unvalidated` | `stale` | `broken` | `tombstoned` — §10.4. */
  state: string;
  origin: string;
  /** The producer that computed it, for derived fields. */
  by: string | null;
  /** How far behind what you are looking at may be (§10.5). */
  freshAsOf: string;
  landedAt: string;
  /** Why this value stopped moving, when its producer is poisoned (§14). Only ever set on `broken`. */
  broken?: string | null;
}

function view(resolved: Resolved<string | null>): FieldView {
  return {
    value: resolved.value,
    state: resolved.state,
    origin: resolved.origin,
    by: resolved.by,
    freshAsOf: resolved.fresh_as_of,
    landedAt: resolved.landed_at,
  };
}

/**
 * The listing. One `list`, then **two reads per contact** — this is the N+1, and it is deliberately
 * not hidden.
 *
 * `list` answers ids and nothing else (§9.6, SDK-DRAFT §4.5): a name per contact is a read per
 * contact, because there is no query layer and a "give me one field too" parameter would answer
 * exactly one shape of question. At the scale of a personal CRM this is invisible — 40 contacts is
 * 81 messages down a unix socket. It is also the single clearest place this app would stop scaling,
 * and the reads are issued serially because the SDK's connection chains requests, so the cost is
 * 81 × round-trip and not 81 × in-flight. FRICTION.md has the measurement.
 */
async function listContacts(branch: BranchHandle): Promise<unknown[]> {
  const ids = await branch.list(Contact);
  const rows = [];
  for (const id of ids) {
    const displayName = await read(branch, id, "displayName");
    const email = await read(branch, id, "email");
    rows.push({ id, displayName: view(displayName), email: email.value });
  }
  return rows;
}

/**
 * Does this contact exist?
 *
 * `Contact:<id>` with no `.field` is the object's **existence cell** — `tx.create` writes `true`
 * there and `borg delete` tombstones it, and it is what `list` scans. Reading it is the only way to
 * tell a contact whose fields are all empty from one that was never created: every field of a
 * non-existent object reads back as a perfectly ordinary absent value, so without this check
 * `GET /api/contacts/<anything-well-formed>` answers `200` with six nulls. FRICTION.md #4.
 *
 * The address is built by hand because the SDK models fields and not entities out here; the
 * generated `Contact` descriptor has no notion of the object cell.
 */
async function exists(branch: BranchHandle, id: string): Promise<boolean> {
  const cell = await branch.get(`${Contact.name}:${id}`);
  return cell.value === "true";
}

/** One contact: every field, each with the envelope it was read under. */
async function getContact(branch: BranchHandle, id: string): Promise<Record<string, FieldView>> {
  const fields: Record<string, FieldView> = {};
  for (const field of ALL) {
    const resolved = await read(branch, id, field);
    const entry = view(resolved);
    // A `broken` value is absent *for a reason*, and §11 is where the reason lives. Fetched only
    // when there is one to fetch, because it is an extra round trip on a path that is normally fine.
    if (resolved.state === "broken") {
      const lineage = await branch.explain(resolved.cell);
      entry.broken = lineage.broken;
    }
    fields[field] = entry;
  }
  return fields;
}

/**
 * Create: begin, allocate, write each field, merge. §12.
 *
 * The transaction reads nothing, so there is nothing to guard and this cannot conflict — two people
 * adding a contact at the same moment are two objects, because neither client chose an id. What it
 * *does* buy is atomicity: a contact appears with all of its fields or with none of them, and never
 * half-written, because everything below lands on the transaction's own branch until `commit`.
 *
 * `displayName` is absent from this function and cannot be added to it: it is `readonly` in the
 * generated interface, so `handle.set("displayName", …)` does not compile.
 */
async function createContact(bc: BorgContext, body: Partial<Record<Editable, string>>): Promise<string> {
  const tx = await bc.branch(BRANCH).begin();
  try {
    const contact = await tx.create(Contact);
    for (const field of EDITABLE) {
      const value = body[field];
      if (value !== undefined && value.trim() !== "") await contact.set(field, value.trim());
    }
    await tx.commit();
    return contact.id;
  } catch (err) {
    // A create cannot conflict, but an expired transaction or a rejected value can still land here,
    // and a transaction left open is one the reaper has to clean up (§12.3).
    await tx.abort().catch(() => {});
    throw err;
  }
}

// ── HTTP ──────────────────────────────────────────────────────────────────────────────────────────

/**
 * What the browser is told when something failed.
 *
 * The point of this function is that Borg's failures are *specific*, and flattening them to "500"
 * throws away the only part a human can act on. A `ConflictError` names the cell that moved; a
 * `BorgStateError` says a value is unreachable rather than absent. The UI renders both as sentences.
 */
function failure(err: unknown): { status: number; body: Record<string, unknown> } {
  if (err instanceof ConflictError) {
    return {
      status: 409,
      body: {
        kind: "conflict",
        message: err.message,
        cell: err.cell,
        reason: err.reason,
        hint: "someone else wrote a cell this request had read; re-read and decide",
      },
    };
  }
  if (err instanceof BorgStateError) {
    return {
      status: 502,
      body: {
        kind: "broken",
        message: err.message,
        cell: err.envelope.cell,
        hint: "the producer that owns this field failed — `borg explain` says why",
      },
    };
  }
  if (err instanceof BorgClientError) {
    return { status: 400, body: { kind: "rejected", message: err.message } };
  }
  if (err instanceof BorgProtocolError) {
    return {
      status: 503,
      body: {
        kind: "disconnected",
        message: err.message,
        hint: "`borg serve` is not answering — is it still running?",
      },
    };
  }
  return { status: 500, body: { kind: "error", message: String(err) } };
}

function send(res: ServerResponse, status: number, body: unknown): void {
  const text = JSON.stringify(body, null, 2);
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(text),
    // The UI is served by vite on another port in development.
    "access-control-allow-origin": "*",
    "access-control-allow-headers": "content-type",
  });
  res.end(text);
}

async function readBody(req: IncomingMessage): Promise<Record<string, string>> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  if (chunks.length === 0) return {};
  const parsed: unknown = JSON.parse(Buffer.concat(chunks).toString("utf8"));
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new BorgClientError("the request body must be a JSON object");
  }
  const out: Record<string, string> = {};
  for (const [key, value] of Object.entries(parsed)) {
    if (typeof value === "string") out[key] = value;
  }
  return out;
}

const bc = await createBorgContext({ socket: SOCKET });
const branch = bc.branch(BRANCH);

const server = createServer((req, res) => {
  void (async () => {
    const url = new URL(req.url ?? "/", "http://localhost");
    const path = url.pathname;
    try {
      if (req.method === "OPTIONS") return send(res, 204, {});

      if (path === "/api/health" && req.method === "GET") {
        return send(res, 200, {
          ok: true,
          // The def-version this code was generated at (§5.4). Shown in the UI's footer, because
          // "which schema is this client speaking?" is otherwise invisible until it matters.
          clientVersion: CLIENT_VERSION,
          branch: BRANCH,
          head: await branch.head(),
        });
      }

      if (path === "/api/contacts" && req.method === "GET") {
        return send(res, 200, await listContacts(branch));
      }

      if (path === "/api/contacts" && req.method === "POST") {
        const id = await createContact(bc, await readBody(req));
        return send(res, 201, { id });
      }

      const match = /^\/api\/contacts\/([^/]+)$/.exec(path);
      if (match?.[1] !== undefined && req.method === "GET") {
        const id = decodeURIComponent(match[1]);
        if (!(await exists(branch, id))) {
          return send(res, 404, { kind: "not_found", message: `no contact ${id}` });
        }
        return send(res, 200, { id, cell: `${Contact.name}:${id}`, fields: await getContact(branch, id) });
      }

      send(res, 404, { kind: "not_found", message: `no route ${req.method} ${path}` });
    } catch (err) {
      const { status, body } = failure(err);
      // Loudly, on the server's own stderr: the browser gets the sentence, the operator gets the
      // stack. Neither is a substitute for the other.
      console.error(`${req.method} ${path} → ${status}`, err);
      send(res, status, body);
    }
  })();
});

server.listen(PORT, () => {
  console.log(`crm api on http://localhost:${PORT} — borg ${SOCKET}, branch ${BRANCH}, client ${CLIENT_VERSION}`);
});

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => {
    server.close();
    bc.close();
    process.exit(0);
  });
}
