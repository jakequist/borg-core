// Three views — list, detail, create — and no state library. `useState` and the hash is the whole
// of the routing, because the app is three views and a router would be the largest thing in it.
//
// The part worth reading is `<Failure>` and `<Badge>`. Borg's answers are not "the value"; they are
// the value *and how far you may trust it* (§10.4, invariant 8), and its refusals are specific
// enough to act on. Rendering either of those as a blank screen would be throwing away the only
// thing this backend does that a table does not.

import { useCallback, useEffect, useState } from "react";

const API = import.meta.env.VITE_API ?? "http://localhost:8787";

// ── Talking to the api ────────────────────────────────────────────────────────────────────────────

/**
 * Every failure reaches the UI as an object, never as a thrown string.
 *
 * `kind` comes from the api, which got it from the SDK's error classes: `conflict` carries the cell
 * that moved, `broken` carries the cell whose producer failed. `unreachable` is the one the api
 * cannot report, because it is the api that is missing.
 */
async function call(path, options) {
  let response;
  try {
    response = await fetch(`${API}${path}`, options);
  } catch (err) {
    throw {
      kind: "unreachable",
      message: `the api at ${API} did not answer (${err.message})`,
      hint: "is dev.sh still running?",
    };
  }
  const body = await response.json().catch(() => ({
    kind: "error",
    message: `${response.status} ${response.statusText}, and the body was not JSON`,
  }));
  if (!response.ok) throw body;
  return body;
}

const listContacts = () => call("/api/contacts");
const getContact = (id) => call(`/api/contacts/${encodeURIComponent(id)}`);
const createContact = (body) =>
  call("/api/contacts", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });

// ── Honesty ───────────────────────────────────────────────────────────────────────────────────────

const EXPLAIN = {
  conflict:
    "Two writes raced. Borg rejected this one whole rather than letting half of it land — nothing " +
    "you typed was saved, and re-reading is safe.",
  broken:
    "The pipeline that computes this field failed, so there is no value to show. Borg will not " +
    "invent one: an absent value and an unreachable one are different, and this is the second.",
  rejected: "Borg refused the write. The message is the engine's own.",
  disconnected: "The api is up but `borg serve` is not answering it.",
  unreachable: "Nothing answered at all.",
  not_found: "No such route.",
};

function Failure({ error, onDismiss }) {
  if (!error) return null;
  return (
    <div className="error" role="alert">
      <h3>{error.kind ?? "error"}</h3>
      <p>{EXPLAIN[error.kind] ?? ""}</p>
      <p>{error.message}</p>
      {error.cell && (
        <p>
          cell: <code>{error.cell}</code>
          {error.reason ? ` (${error.reason})` : ""}
        </p>
      )}
      {error.hint && <p className="muted">{error.hint}</p>}
      {onDismiss && <button onClick={onDismiss}>dismiss</button>}
    </div>
  );
}

/**
 * The §10.4 state, as something a person can see at a glance.
 *
 * `current` is not badged — badging the normal case teaches people to ignore badges. Everything
 * else is, because everything else means the value on screen is not what a recomputation would
 * produce right now.
 */
function Badge({ field }) {
  if (!field || field.state === "current") return null;
  const says = {
    stale: "behind — an input moved and this has not been recomputed yet",
    unvalidated: "not revalidated since its inputs last moved",
    broken: "unreachable — the producer that owns this failed",
    tombstoned: "deleted",
  };
  return (
    <span className={`badge ${field.state}`} title={says[field.state] ?? field.state}>
      {field.state}
    </span>
  );
}

// ── List ──────────────────────────────────────────────────────────────────────────────────────────

function ContactList({ go }) {
  const [rows, setRows] = useState(null);
  const [error, setError] = useState(null);

  const load = useCallback(() => {
    setError(null);
    listContacts().then(setRows, setError);
  }, []);
  useEffect(load, [load]);

  return (
    <>
      <h2>contacts</h2>
      <Failure error={error} onDismiss={load} />
      {rows === null && !error && <p className="muted">reading…</p>}
      {rows?.length === 0 && <p className="muted">No contacts yet. Add one.</p>}
      {rows?.length > 0 && (
        <ul className="contacts">
          {rows.map((row) => (
            <li key={row.id}>
              <a href={`#/contacts/${row.id}`} onClick={() => go(["detail", row.id])}>
                {/* A derived field that is absent is not the same as a contact with no name: the
                    pipeline always writes something, so `null` here means it has not run yet. */}
                {row.displayName.value ?? <span className="muted">(not derived yet)</span>}
              </a>
              <Badge field={row.displayName} />
              <span className="email">{row.email ?? ""}</span>
            </li>
          ))}
        </ul>
      )}
    </>
  );
}

// ── Detail ────────────────────────────────────────────────────────────────────────────────────────

const LABELS = {
  firstName: "first name",
  lastName: "last name",
  email: "email",
  phone: "phone",
  notes: "notes",
};

function ContactDetail({ id, go }) {
  const [contact, setContact] = useState(null);
  const [error, setError] = useState(null);

  const load = useCallback(() => {
    setError(null);
    getContact(id).then(setContact, setError);
  }, [id]);
  useEffect(load, [load]);

  const display = contact?.fields.displayName;

  return (
    <>
      <p>
        <a className="link" href="#/" onClick={() => go(["list"])}>
          ← all contacts
        </a>
      </p>
      <Failure error={error} onDismiss={load} />
      {contact && (
        <>
          <h2>
            {display.value ?? <span className="muted">(not derived yet)</span>} <Badge field={display} />
          </h2>
          <dl className="fields">
            {Object.keys(LABELS).map((field) => (
              <Fragmentish key={field} label={LABELS[field]} field={contact.fields[field]} />
            ))}
          </dl>

          {/* The envelope, shown rather than summarised. This is the app's whole reason for being on
              Borg: `displayName` is not a column, it is a value with a provenance, and a detail view
              is exactly where a person can be told what it is. */}
          <div className="provenance">
            <strong>displayName</strong> — this field is computed, not typed
            <table>
              <tbody>
                <tr>
                  <td>state</td>
                  <td>
                    {display.state} <Badge field={display} />
                  </td>
                </tr>
                <tr>
                  <td>origin</td>
                  <td>{display.origin}</td>
                </tr>
                {display.broken && (
                  <tr>
                    {/* §11's answer to "why did this stop moving". The api fetches it only when
                        there is one, because it is a second round trip. */}
                    <td>broken because</td>
                    <td>{display.broken}</td>
                  </tr>
                )}
                <tr>
                  <td>produced by</td>
                  <td>
                    <code>{display.by ?? "—"}</code>
                  </td>
                </tr>
                <tr>
                  <td>fresh as of</td>
                  <td>
                    <code>{display.freshAsOf}</code>
                    <span className="muted">
                      {" "}
                      — everything up to this layer is incorporated in what you are reading
                    </span>
                  </td>
                </tr>
                <tr>
                  <td>landed at</td>
                  <td>
                    <code>{display.landedAt}</code>
                  </td>
                </tr>
                <tr>
                  <td>cell</td>
                  <td>
                    <code>{contact.cell}.displayName</code>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </>
      )}
    </>
  );
}

/** One `dt`/`dd` pair. React fragments in a `<dl>`, without importing `Fragment` twice. */
function Fragmentish({ label, field }) {
  return (
    <>
      <dt>{label}</dt>
      <dd className={field.value === null ? "absent" : undefined}>
        {field.value ?? "—"} <Badge field={field} />
      </dd>
    </>
  );
}

// ── Create ────────────────────────────────────────────────────────────────────────────────────────

const EMPTY = { firstName: "", lastName: "", email: "", phone: "", notes: "" };

function CreateContact({ go }) {
  const [form, setForm] = useState(EMPTY);
  const [error, setError] = useState(null);
  const [busy, setBusy] = useState(false);

  const submit = async (event) => {
    event.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const { id } = await createContact(form);
      go(["detail", id]);
    } catch (err) {
      // Nothing is cleared. A rejected commit wrote nothing at all — the transaction's whole point —
      // so what the person typed is still the truth and is still in the boxes.
      setError(err);
    } finally {
      setBusy(false);
    }
  };

  const field = (name, Tag = "input") => (
    <label>
      <span>{LABELS[name]}</span>
      <Tag value={form[name]} onChange={(e) => setForm({ ...form, [name]: e.target.value })} />
    </label>
  );

  return (
    <>
      <h2>new contact</h2>
      <Failure error={error} onDismiss={() => setError(null)} />
      <form onSubmit={submit}>
        {field("firstName")}
        {field("lastName")}
        {field("email")}
        {field("phone")}
        {field("notes", "textarea")}
        {/* No `displayName` box, and that is the point: the field exists, it is on every contact,
            and no client may write it (§8). The generated types make an attempt not compile. */}
        <p className="muted">
          <code>displayName</code> is derived by the <code>display_name</code> pipeline from the name
          parts. It appears a moment after this commits.
        </p>
        <p>
          <button className="primary" type="submit" disabled={busy}>
            {busy ? "committing…" : "create"}
          </button>{" "}
          <button type="button" onClick={() => go(["list"])}>
            cancel
          </button>
        </p>
      </form>
    </>
  );
}

// ── The shell ─────────────────────────────────────────────────────────────────────────────────────

function route() {
  const match = /^#\/contacts\/(.+)$/.exec(window.location.hash);
  if (match) return ["detail", match[1]];
  if (window.location.hash === "#/new") return ["new"];
  return ["list"];
}

export function App() {
  const [view, setView] = useState(route);
  const [health, setHealth] = useState(null);

  useEffect(() => {
    const onHash = () => setView(route());
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  useEffect(() => {
    call("/api/health").then(setHealth, () => setHealth(null));
  }, [view]);

  const go = (next) => {
    window.location.hash = next[0] === "detail" ? `#/contacts/${next[1]}` : next[0] === "new" ? "#/new" : "#/";
    setView(next);
  };

  return (
    <main>
      <header>
        <h1>contacts</h1>
        <nav>
          <a className="link" href="#/" onClick={() => go(["list"])}>
            list
          </a>
          <a className="link" href="#/new" onClick={() => go(["new"])}>
            new
          </a>
        </nav>
      </header>

      {view[0] === "list" && <ContactList go={go} />}
      {view[0] === "detail" && <ContactDetail id={view[1]} go={go} />}
      {view[0] === "new" && <CreateContact go={go} />}

      <footer>
        {health ? (
          <>
            branch <code>{health.branch}</code> · head <code>{health.head}</code> · this client was
            generated at <code>{health.clientVersion}</code>
          </>
        ) : (
          "the api is not answering"
        )}
      </footer>
    </main>
  );
}
