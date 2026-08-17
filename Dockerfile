# `borg-server`, as a deployment artifact. SPEC.md §17.6.
#
# ## The one decision worth reading before you change anything here
#
# **This image runs user code.** A pipeline is not a plugin loaded into the server — it is an
# executable file the server *spawns*, over stdio or a unix socket (§17.4,
# `crates/borg-exec-process`), and `Command::new(command)` runs that file directly with no shell in
# between. So what a pipeline may be written in is exactly the set of interpreters present in this
# image, and nothing else. A repo whose `borg.toml` names `pipelines/display_name.ts` needs `node`
# here or the producer does not run; the same for `python3` and for a `#!/usr/bin/env bash` worker,
# which also wants `jq`, because that is what a shell pipeline parses the protocol with.
#
# The alternative — a lean image, interpreters supplied by whoever deploys a repo — was rejected.
# It moves a *correctness* property (does this producer run at all?) into a base-image choice made
# somewhere else, and the failure it produces is a broken producer (§14) discovered at derivation
# time rather than a build error. The honest trade is stated rather than hidden: this image is
# roughly 400 MB instead of roughly 90 MB, and every byte of that is an interpreter that exists to
# be handed a file the operator wrote. Tenant isolation is the VM boundary (ROADMAP.md, *The
# production arc*), not this image's package list, so a smaller list buys no safety — only a
# narrower set of languages.
#
# If a deployment knows it only ever runs, say, TypeScript pipelines, the right move is a derived
# image that removes what it does not need, not a leaner base that every deployment then has to
# repair.
#
# ## Layout
#
#   /data          the data directory: one subdirectory per registry, plus borg.sock. A volume.
#   /repos         where pipeline code is expected to be mounted. Not a volume — see DEPLOY.md.
#   borg-server    the server. `start --foreground` is the entrypoint.
#   borg           the client, on PATH for `docker exec` — export, import, create, status.

# ---------------------------------------------------------------------------------------------
# Build. Debian rather than Alpine because `rusqlite` is taken `bundled` (Cargo.toml), which
# compiles SQLite from source against the platform libc; musl would mean a second toolchain
# decision for a dependency that already builds.
# ---------------------------------------------------------------------------------------------
FROM rust:1-bookworm AS build

WORKDIR /build

# The whole workspace in one copy, and one `cargo build`. A manifest-first layer that pre-builds
# dependencies is the usual trick and it is not taken here: eleven path crates all naming each
# other means the manifest shuffle is most of a `Cargo.toml` reimplementation, and it goes stale
# silently the first time a crate is added. The BuildKit cache mounts below do the same job with
# none of that, and CI's layer cache does the rest.
COPY . .

# `--locked` so the image is built from `Cargo.lock` and a build that would need to resolve a new
# version fails here rather than shipping something no test ran against.
#
# The target directory is a cache mount, so the binaries do not survive the RUN — they are copied
# to /out inside the same layer, which is what the runtime stage takes.
RUN --mount=type=cache,target=/build/target,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    cargo build --release --locked -p borg-cli -p borg-server \
    && mkdir -p /out \
    && cp target/release/borg target/release/borg-server /out/

# ---------------------------------------------------------------------------------------------
# Runtime.
# ---------------------------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# Node is not installed from Debian, which ships 18 on bookworm. It has to be 22.18 or newer:
# pipelines are written as `.ts` files with a `#!/usr/bin/env node` shebang and are executed
# directly (see examples/personal-crm/repo/pipelines/display_name.ts), which needs the type
# stripping Node enabled by default in 22.18. `scenarios/ts-lib.sh` asserts the same floor for the
# same reason, and the RUN below asserts it a third time so a base-image bump cannot quietly
# reintroduce a Node that cannot run a pipeline.
COPY --from=node:22-bookworm-slim /usr/local/bin/node /usr/local/bin/node
COPY --from=node:22-bookworm-slim /usr/local/lib/node_modules /usr/local/lib/node_modules

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
    # Interpreters a producer may be written in (§17.4). See the header.
    bash \
    python3 \
    # What a `#!/usr/bin/env bash` worker parses the protocol with — scenarios/030's pipeline is
    # the reference shell producer and it is `jq` from top to bottom.
    jq \
    # `borg-server stop` shells out to `kill` (crates/borg-server/src/lifecycle.rs), and its
    # liveness probe reads `kill -0`. Without /bin/kill that probe answers "dead" rather than
    # erroring, so a running server would be reported stopped. procps is not optional.
    procps \
    # TLS roots, so a pipeline that fetches something reaches it. The server itself terminates no
    # TLS and links no TLS backend (§17.6) — this is for user code only.
    ca-certificates \
    # The HEALTHCHECK below, and the one thing an operator always wants inside a container.
    curl \
    ; \
    rm -rf /var/lib/apt/lists/*; \
    ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm; \
    ln -s ../lib/node_modules/corepack/dist/corepack.js /usr/local/bin/corepack; \
    # Assert the floor rather than trust the tag. Same predicate as scenarios/ts-lib.sh.
    node -e 'const [a,b]=process.versions.node.split(".").map(Number); if(!(a>22||(a===22&&b>=18))) throw new Error(`node ${process.version} cannot run a .ts pipeline directly (needs 22.18+)`)'; \
    python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)'; \
    bash --version >/dev/null; jq --version >/dev/null

COPY --from=build /out/borg-server /out/borg /usr/local/bin/

# A fixed uid, because a bind-mounted data directory has to be chown'd to *something* on the host
# and "whatever the base image happened to allocate" is not a number anybody can write down.
# DEPLOY.md states it.
RUN set -eux; \
    groupadd --gid 10001 borg; \
    useradd --uid 10001 --gid 10001 --home-dir /data --no-create-home --shell /usr/sbin/nologin borg; \
    mkdir -p /data /repos; \
    chown borg:borg /data /repos

# Declared after the chown so a *named* volume created from this image inherits the ownership.
# A bind mount inherits nothing and must be chown'd on the host — DEPLOY.md, "The volume".
VOLUME /data

USER borg:borg

# `$HOME` is set because `host::default_data_dir()` falls back to a cwd-relative `./.borg` when it
# is unset, and a relative data directory in a container is a store that moves when the working
# directory does.
ENV HOME=/data

# **This is what makes `borg://localhost/<registry>` work inside the container**, and it is not
# cosmetic. §17.7's well-known address resolves to `default_socket(default_data_dir())`, which with
# `HOME=/data` and no `XDG_RUNTIME_DIR` is `/data/.borg/borg.sock` — while the server, told
# `--data-dir /data`, is actually listening on `/data/borg.sock`. A client typing the one address
# the spec calls well-known would miss the server by one directory.
#
# `default_socket` prefers `$XDG_RUNTIME_DIR/borg.sock` whenever that names an existing directory
# (crates/borg-host/src/host.rs), so pointing it at /data makes both sides agree on /data/borg.sock:
# the entrypoint listens there and `borg://localhost` resolves there. The documented hazard of a
# shared XDG_RUNTIME_DIR — two data directories colliding on one socket — does not apply, because a
# container runs one server, which is the whole shape of this image.
ENV XDG_RUNTIME_DIR=/data

# The listen address as configuration rather than as part of the command, so `docker run -e
# BORG_LISTEN=…` and a compose `environment:` key both work without restating the entrypoint.
# `BORG_LISTEN=` (empty) turns the WebSocket off and leaves the unix socket, which is always bound.
ENV BORG_LISTEN=ws://0.0.0.0:7411
EXPOSE 7411

# `GET /health` is the one HTTP endpoint and it lives on the WebSocket's port (§17.6). It is
# answered on the accept thread, so it stays green while a registry is busy — this probe asks "is
# the process up and listening", which is the question a supervisor should be asking, and
# deliberately not "is every registry responsive".
#
# Hardcoded to 7411 rather than derived from $BORG_LISTEN: HEALTHCHECK is not shell-expanded at
# runtime in exec form, and parsing a multi-address list in a probe would be a second listen-address
# parser. A deployment that moves the port overrides HEALTHCHECK too — DEPLOY.md says so.
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -fsS http://127.0.0.1:7411/health || exit 1

# `--foreground` is not optional under a supervisor: without it `start` re-execs itself with
# `--foreground`, writes a pidfile and returns, and the container's pid 1 exits immediately. In the
# foreground the server is pid 1, handles SIGTERM and SIGINT itself (crates/borg-server/src/serve.rs),
# releases every advisory lock and unlinks the socket on the way out — so `docker stop` is a clean
# shutdown and needs no init shim.
#
# ENTRYPOINT/CMD are split so that `docker run <image> --listen ws://0.0.0.0:9000` appends flags,
# while `docker run --entrypoint borg-server <image> status --data-dir /data` still works.
ENTRYPOINT ["borg-server", "start", "--foreground", "--data-dir", "/data"]
CMD []
