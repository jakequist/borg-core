# Borg

An event-sourced data backend where **schema changes are data changes**, **derived data is honest
about its freshness**, and **every write is a transaction**.

Borg stores cells — one field of one object — in an append-only log of layers on branches. From
that, four properties most stacks bolt on afterward fall out of the core model:

- **Branch your data like code.** Fork a branch, change a schema *with a migration attached*, read
  old data through the new lens, merge back — while clients built against the old schema keep
  working through `down` migrations. A stale build of your app survives a schema change, and there
  is a test that proves it.
- **Pipelines with field-level dependency tracking.** A pipeline is ordinary code (TypeScript,
  Python, even bash) that reads and writes cells through the engine. The engine records exactly what
  each invocation read — through any number of hops — so a write re-runs precisely the work that
  depended on it, and nothing else. No dependency declarations, in any language, ever.
- **Freshness you can trust.** Every read returns a provenance envelope: where the value came from,
  which producer wrote it, and the exact layer it reflects. Derived data is served *and labelled*
  when stale, never silently served, never withheld. A standing property test forks the log at any
  derived value's stated watermark, recomputes from scratch, and demands the same answer.
- **Optimistic transactions with automatic guards.** A transaction records what it read; at commit,
  those reads become guards evaluated against everything that landed since. Read-modify-write is a
  compare-and-swap without anyone writing one.

## Status: early, honest, moving fast

This is pre-1.0 and the on-disk formats are **not stable**. The compatibility promise is the data,
not the bytes: every release can export a registry as a canonical event stream and import streams
from prior releases *(export/import is the current work — see `ROADMAP.md`)*. Run it for real
projects with the same care you'd give any young database.

## Quick look

```bash
./check.sh                    # build everything, run every test and end-to-end scenario
borg-server start             # a local server hosting a directory of registries
borg-server create crm
cd examples/personal-crm && ./dev.sh    # a small real app: React + API + a Borg repo
```

A repo defines structs and pipelines in your language:

```ts
const Contact = borg.struct("Contact", {
  firstName: borg.string(),
  lastName:  borg.string(),
  displayName: borg.string().derived(),
});

const displayName = borg.pipeline("display_name", Contact,
  { writes: ["displayName"] },
  async (c) => {
    const first = await c.get("firstName"), last = await c.get("lastName");
    await c.set("displayName", [first, last].filter(Boolean).join(" ") || "(no name)");
  });
```

Push it, and the engine runs it — invalidating per field, labelling every output with what it
reflects, and re-running everything if (and only if) the code or its inputs change.

## Reading order

- **`SPEC.md`** — normative: what the system is and why. Code comments cite its sections.
- **`ROADMAP.md`** — where this is going, and a log of every design decision with its reasoning.
- **`scenarios/`** — 30+ end-to-end scenarios driving the real binaries; each one is a claim about
  the system stated in prose. If a scenario passes, that devex works.
- **`examples/personal-crm/`** — a real application, including its unvarnished `FRICTION.md`.

## License

Apache-2.0. The hosted platform (borg-hq.com) is developed separately; this repository is the
entire engine, server, CLI, and SDKs — the parts that sit next to your data.
