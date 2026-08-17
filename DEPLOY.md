# Deploying `borg-server`

The deployment artifact is one container image holding two binaries: `borg-server`, which stays up
and hosts a directory of registries (SPEC.md §17.6), and `borg`, the client, on `PATH` so that
`docker exec` can administer the thing.

This document is the operational half. `SPEC.md` says what the server *is*; this says how to run it,
back it up and upgrade it without losing data.

---

## The image

```bash
docker build -t borg-server .
docker run -d --name borg -p 127.0.0.1:7411:7411 -v borg-data:/data borg-server
```

Published from `main` as `ghcr.io/<owner>/borg-server`, tagged `latest` and `sha-<short>`.
**Pin the sha tag anywhere it matters.** `latest` is for a human typing `docker pull`; a deployment
that floats cannot answer "what is running right now" and has no way back.

| | |
|---|---|
| Base | `debian:bookworm-slim` |
| Size | ~400 MB |
| User | `borg`, uid/gid **10001**, never root |
| Volume | `/data` |
| Port | `7411`, plaintext `ws://` |
| Health | `GET /health` on the same port |
| Entrypoint | `borg-server start --foreground --data-dir /data` |

### Why the image is 400 MB and not 90 MB

Because **this server runs user code**. A pipeline is not a plugin loaded into the process — it is
an executable file the server spawns (§17.4), so the languages a pipeline may be written in are
exactly the interpreters in this image. It ships `node` (≥22.18, which is what runs a `.ts` pipeline
directly), `python3`, `bash` and `jq`, plus `curl`, `ca-certificates` and `procps`.

The lean alternative — interpreters supplied by whoever deploys — was rejected because it turns a
correctness property into a base-image choice made elsewhere, and the symptom is a broken producer
(§14) discovered at derivation time rather than a build error. Tenant isolation is the VM boundary
(`ROADMAP.md`, *The production arc*), not this image's package list, so a shorter list buys no
safety — only fewer languages. A deployment that knows it runs only one language should build a
derived image that removes the rest, rather than starting leaner and repairing.

`procps` is not optional: `borg-server stop` runs `kill` as a *program*, and without `/bin/kill` its
liveness probe reports a running server as dead rather than erroring.

---

## The volume

`/data` holds one directory per registry, the unix socket, and nothing else that matters.

```
/data/
  borg.sock          the local transport; always bound, whatever --listen says
  <registry>/        borg.db and its sidecars, one directory per registry
```

Use a **named volume**. Docker applies the image's ownership when it creates one, so uid 10001 owns
it from the start. A **bind mount inherits nothing** — if you must use one, `chown -R 10001:10001`
it on the host first, or the server cannot write and will not start.

`XDG_RUNTIME_DIR=/data` is set in the image deliberately. Without it the well-known address
`borg://localhost/<registry>` (§17.7) resolves to `/data/.borg/borg.sock` while the server listens
on `/data/borg.sock` — one directory apart, and a client typing the documented address misses it.

### Creating a registry

```bash
docker exec borg borg-server --data-dir /data create crm
```

`--data-dir /data` is required on every `borg-server` subcommand run via `docker exec`: the default
is `$HOME/.borg`, and `HOME` is `/data`, so a bare invocation looks in `/data/.borg` and finds
nothing. The entrypoint passes it explicitly for the same reason.

---

## Pipeline code

Mount it, read-only, at a path that **never changes**:

```yaml
volumes:
  - ./repos:/repos:ro
```

This is the sharpest edge in the whole deployment. `borg repo push` records, per producer, the path
of the executable on the server's disk (§9.2), and that path is carried in an export:

```json
{"producer":{"name":"invest","command":"/repos/shell/pipelines/is_investible.sh","transport":"stdio"}}
```

Restore that stream into a container that mounts the repo somewhere else and every producer points
at nothing. Fix the mount path once and treat it as part of the deployment's contract.

Push through the running server — it no longer requires downtime (§17.6):

```bash
docker exec borg borg repo push /repos/crm --url borg://localhost/crm
```

The path is a path **in the container**, because the server is the machine the code runs on. A repo
whose pipelines import an SDK needs its dependencies mounted with it (`node_modules` beside a
TypeScript repo); the server spawns the file, it does not install anything.

---

## TLS, and the proxy in front

**`borg-server` terminates no TLS and there is no TLS backend compiled into the binary** (§17.6).
`tungstenite` is taken with `default-features = false`, which makes that a property of the artifact
rather than a line in a document. `wss://` as a *listen* address is refused by name rather than
quietly served in plaintext.

So the deployment is always: proxy terminates `wss://`, forwards plaintext `ws://` to 7411.

```nginx
location / {
    proxy_pass http://127.0.0.1:7411;
    proxy_http_version 1.1;
    proxy_set_header Upgrade    $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host       $host;

    # A borg connection is long-lived and mostly idle. The default 60s read timeout closes
    # transactions out from under clients.
    proxy_read_timeout  1h;
    proxy_send_timeout  1h;
}
```

Two things follow, and both are deliberate:

- **Publish 7411 on loopback only.** The compose file binds `127.0.0.1:7411:7411`. Exposing it
  publicly exposes an unauthenticated, unencrypted database — `ClientHello` carries a `credential`
  field and *nothing checks it yet* (§17.6). Until it does, the network boundary is the boundary.
- **The server reads no forwarded header.** Not `X-Forwarded-For`, not `X-Forwarded-Proto`, not
  `X-Real-IP`. Nothing in the protocol is a function of the client's address, and trusting one would
  add a spoofable identity to answer a question nobody asks. Do not expect them to do anything.

`GET /health` is unauthenticated and reports the version and a registry **count**, never names. It
is answered on the accept thread, so it stays green while a registry is busy — it asks "is the
process up and listening", which is the question a load balancer should ask.

---

## Backup

**The compatibility promise is the data, not the bytes** (§19). Pre-1.0 the on-disk format may
change; the event stream is what every release can write and read. So a backup is an *export*, not a
copy of `borg.db`.

```bash
# Through the running server — no downtime. The export runs under the registry's own gate, so it is
# a coherent snapshot of the whole log at one instant.
docker exec borg borg-server --data-dir /data export crm /data/backup/crm-$(date -u +%F).borgstream
```

`<file>` is a path **in the container**, because the server is what writes it. Mount a backup
directory, or write into `/data` and copy out with `docker cp`.

Then get the file off the host — the stream is the backup, and a backup on the same disk as the
thing it is backing up is not one.

Three things worth knowing:

- **An export is at head, never at the settled frontier.** It includes source layers above the
  watermark, because settling would drop the most recent writes out of a backup. The settled
  position is reported so a captured backlog is visible.
- **It holds the registry for its duration.** Requests to *that* registry queue behind it; other
  registries are unaffected. For a large store, back up on a schedule that tolerates that.
- **It does not carry your pipeline code.** It records which program each producer *is*, and where
  — never the program. Back up the repo directory with git, like code.

Restoring creates the registry and fills it in one operation, and refuses a name that already
exists:

```bash
docker exec borg borg-server --data-dir /data import crm-restored /data/backup/crm-2026-08-17.borgstream
```

---

## Upgrading

Default to exporting first. It is cheap, and it is the only thing that makes the rest reversible.

### When the on-disk format did not change — in place

The common case. Release notes say so.

```bash
docker exec borg borg-server --data-dir /data export crm /data/backup/pre-upgrade.borgstream
docker compose pull                # or: edit the pinned sha
docker compose up -d
docker exec borg borg-server --data-dir /data status
curl -fsS http://127.0.0.1:7411/health
```

The volume is untouched; the new binary opens the same stores. `docker stop` sends SIGTERM, which
the server handles itself — it releases every advisory lock and unlinks the socket on the way out,
so this is a clean shutdown in well under a second and needs no init shim.

### When it did change — export, replace, import

```bash
# 1. Export every registry with the OLD image still running.
docker exec borg borg-server --data-dir /data export crm /data/backup/crm.borgstream

# 2. Copy the streams off the volume, so that replacing it cannot take them with it.
docker cp borg:/data/backup ./backup

# 3. Stop, and replace the volume.
docker compose down
docker volume rm borg-data

# 4. Start the NEW image on an empty volume, then import.
docker compose up -d
docker cp ./backup borg:/data/backup
docker exec borg borg-server --data-dir /data import crm /data/backup/crm.borgstream
```

Import creates the registry it names, so step 4 needs no `create`. Verify before deleting anything:
`status` lists the registries, and a `get` of a known cell through the socket is the real check.

Registry names are `[A-Za-z0-9_-]+`. A restore into a *different* name is how you rehearse this
without touching production.

---

## Operating notes

- **A write pays for the derivation it causes**, inside the request that caused it (§9.6). A push
  that invalidates a large fan-out makes that one request slow. `BORG_DERIVE_PARALLELISM` bounds how
  many invocations run at once; unset means one per core, which on a shared VM is usually more than
  you want to hand a single tenant.
- **Requests are serialised per registry.** One gate per registry, not per server, so two registries
  proceed independently and one registry's slow request queues its own.
- **A served store locks every other `borg` invocation out**, by design — one process serves a
  store. `generate` and `repo push` speak to the socket instead; the rest will refuse and name the
  socket to talk to.
- **Transaction and round branches are never reaped.** They are cheap rows, and nothing collects
  them yet. Expect the log to grow with write volume.
- **Logs go to stdout** in the foreground, which is where Docker wants them. The compose file caps
  them at 5 × 10 MB; a server that logs forever fills the disk the registries are on.
- **`BORG_LISTEN`** sets the listen addresses (comma- or space-separated). Empty turns the WebSocket
  off and leaves the unix socket, which is always bound. Any `--listen` on the command line wins
  outright. If you move the port, override the `HEALTHCHECK` too — it is baked to 7411, because
  `HEALTHCHECK` in exec form is not shell-expanded and parsing a listen list inside a probe would be
  a second copy of that parser.
