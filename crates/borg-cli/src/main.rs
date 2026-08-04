//! `borg` — the command line client.
//!
//! This is the testbed for what a client is like to use. Every command goes through the same engine
//! an SDK eventually will, so if the CLI is awkward the design is awkward.
//!
//! Each invocation opens the store, does one thing, and exits. Layers and branches are durable; the
//! indexes are rebuilt from the log on open (see `Registry`).

use borg_core::{
    BorgError, BranchId, CellRecord, ClientVersion, DefEvent, Freshness, FreshnessRequirement,
    LayerAuthor, LayerId, LayerKind, MergeMode, ObjectTypeName, Origin, ProducerId, RepoId, Result,
    ValueType, parse,
};
use borg_engine::Registry;
use borg_exec_native::NativeExecutor;
use borg_storage_sqlite::SqliteStorage;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const ALLOCATOR: borg_core::AllocatorId = borg_core::AllocatorId(0);
/// Everything the CLI writes is authored against the store's initial def-view. Real clients carry
/// the layer their generated code was built from (§5.4); the CLI has no generated code yet.
const CLIENT_VERSION: ClientVersion = ClientVersion(LayerId(1));

struct Args {
    store: PathBuf,
    branch: Option<String>,
    value_only: bool,
    count_only: bool,
    rest: Vec<String>,
}

fn usage() -> ! {
    eprintln!(
        "\
borg — an event-sourced data backend

  borg init                            create a store
  borg set <cell> <value>              write a source cell
  borg delete <cell>                   tombstone a cell
  borg get <cell> [--value]            read a cell, with provenance
  borg explain <cell>                  show where a value came from

  borg branch list
  borg branch fork <parent> [--at <layer>] [--name <name>]
  borg branch merge <child> [--defs-only]

  borg def push <file.json>            push definition mutations
  borg def show <Struct>               show a struct's definition

  borg repo push <dir>                 push a repo: defs and pipelines
  borg producer list                   registered producers
  borg derive [--count]                run producers until caught up

  borg layer list | borg layer head
  borg frontier                        how far each producer has caught up

Cells are written Struct:pid.field, Struct:pid, Element[]:pid or Element[]:pid[n], where a pid
looks like o-1234abcd and names the whole identity. Struct#100 is accepted on input as a
shorthand for counter 100 on the root branch; what borg prints is always the canonical form.

Values are 42, 1.5, true, false, ~ (tombstone), @o-1234abcd (a reference), 0xdeadbeef (binary),
-129n (a bigint), and anything else is a string. Strings, binary and bigints are interned by
content, which borg does for you — you write the text and read the text back. Note the corollary:
the forms above win, so a string field cannot yet hold the text `true` or `42`.

Options:
  --store <path>   store file (default ./borg.db)
  --branch <name>  branch to operate on (default: the root branch)
  --value          print only the value
  --count          print only a count"
    );
    std::process::exit(2)
}

fn parse_args() -> Args {
    let mut args = Args {
        store: PathBuf::from("borg.db"),
        branch: None,
        value_only: false,
        count_only: false,
        rest: Vec::new(),
    };
    let mut raw = std::env::args().skip(1);
    while let Some(arg) = raw.next() {
        match arg.as_str() {
            "--store" => args.store = raw.next().unwrap_or_else(|| usage()).into(),
            "--branch" => args.branch = raw.next(),
            "--value" => args.value_only = true,
            "--count" => args.count_only = true,
            "-h" | "--help" => usage(),
            _ => args.rest.push(arg),
        }
    }
    args
}

#[tokio::main]
async fn main() {
    let args = parse_args();
    if let Err(err) = run(args).await {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

async fn open(args: &Args) -> Result<Registry> {
    let storage = Arc::new(SqliteStorage::open(&args.store)?);
    Registry::open(storage, Arc::new(NativeExecutor::new())).await
}

/// The branch the `Struct#100` shorthand names ids on.
///
/// A PID's branch component records where an object was *allocated*, not where it lives — the whole
/// point of `(branch, allocator, counter)` is that ids never collide, so a fork can inherit an
/// object without renaming it. The shorthand therefore always resolves against the root, or
/// `Company#1` would mean a different object on every branch and a fork could never read what its
/// parent wrote. A canonical address needs none of this: it carries its own branch.
fn allocation_branch(registry: &Registry) -> Result<BranchId> {
    registry
        .default_branch()
        .ok_or_else(|| BorgError::Storage("store has no branches — run `borg init`".into()))
}

/// Resolve `--branch`, falling back to the root. This selects the *timeline*, which is a separate
/// question from which object a shorthand names.
fn branch_of(registry: &Registry, name: Option<&str>) -> Result<BranchId> {
    match name {
        Some(name) => registry
            .branches
            .all()
            .into_iter()
            .find(|b| b.name.as_deref() == Some(name))
            .map(|b| b.id)
            .ok_or_else(|| BorgError::Storage(format!("no branch named `{name}`"))),
        None => registry
            .default_branch()
            .ok_or_else(|| BorgError::Storage("store has no branches — run `borg init`".into())),
    }
}

async fn run(args: Args) -> Result<()> {
    let verb = args.rest.first().map(String::as_str).unwrap_or("");
    let rest: Vec<&str> = args.rest.iter().skip(1).map(String::as_str).collect();

    match (verb, rest.as_slice()) {
        ("init", _) => init(&args).await,
        ("set", [cell, value]) => set(&args, cell, value).await,
        ("delete", [cell]) => set(&args, cell, "~").await,
        ("get", [cell]) => get(&args, cell).await,
        ("explain", [cell]) => explain(&args, cell).await,
        ("branch", ["list"]) => branch_list(&args).await,
        ("branch", ["fork", parent, tail @ ..]) => branch_fork(&args, parent, tail).await,
        ("branch", ["merge", child, tail @ ..]) => branch_merge(&args, child, tail).await,
        ("def", ["push", file]) => def_push(&args, file).await,
        ("def", ["show", name]) => def_show(&args, name).await,
        ("layer", ["list"]) => layer_list(&args).await,
        ("layer", ["head"]) => layer_head(&args).await,
        ("repo", ["push", dir]) => repo_push(&args, dir).await,
        ("producer", ["list"]) => producer_list(&args).await,
        ("derive", _) => derive(&args).await,
        ("frontier", _) => frontier(&args).await,
        _ => usage(),
    }
}

async fn init(args: &Args) -> Result<()> {
    if args.store.exists() {
        return Err(BorgError::Storage(format!(
            "{} already exists",
            args.store.display()
        )));
    }
    let registry = open(args).await?;
    let id = registry.branches.create_root(Some("main".into())).await?;
    println!("initialised {} (branch main = {id})", args.store.display());
    Ok(())
}

/// Write one source cell as its own layer.
async fn set(args: &Args, cell: &str, value: &str) -> Result<()> {
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;
    let cell = parse::cell_ref(cell, allocation_branch(&registry)?, ALLOCATOR)?;
    // A string, blob or bigint is content-addressed and has no identity until the store has seen it
    // (§3.1). Interning here, rather than asking the caller to do it, is what makes `borg set
    // Company#1.website acme.ai` the whole story — there is no PID to create first and none to
    // quote back (§3.4).
    let value = parse::value(value, allocation_branch(&registry)?, ALLOCATOR)?;
    let value = registry.values.intern(value).await?;

    let mut layer = registry
        .layers
        .open(branch, LayerKind::Value, LayerAuthor::Source)
        .await?;

    // Writing a property implies the object exists. Producers map over a struct's `ObjectBuffer`,
    // which holds existence cells (§4.2), so without this a `Company` whose fields were set but
    // which was never explicitly created would be invisible to every pipeline.
    //
    // Only when absent, never on every write: the existence cell lives in the buffer producers
    // subscribe to, so rewriting it would make *any* property write look like a new entity and
    // re-run pipelines that read nothing of the sort.
    if let Some(existence) = implied_existence(&cell)
        && registry
            .resolver
            .resolve(
                branch,
                &existence,
                None,
                CLIENT_VERSION,
                FreshnessRequirement::Any,
            )
            .await?
            .value
            .is_none()
    {
        layer
            .put(
                &existence,
                CellRecord {
                    value: borg_core::Value::Bool(true),
                    version: CLIENT_VERSION,
                    written_at: layer.id(),
                    origin: Origin::Source,
                    derivation: None,
                },
            )
            .await?;
    }

    layer
        .put(
            &cell,
            CellRecord {
                value,
                version: CLIENT_VERSION,
                written_at: layer.id(),
                origin: Origin::Source,
                derivation: None,
            },
        )
        .await?;
    let id = registry.layers.commit(layer).await?;
    println!("{id}");
    Ok(())
}

/// The existence cell a property write implies, if this is a property write.
fn implied_existence(cell: &borg_core::CellRef) -> Option<borg_core::CellRef> {
    match &cell.buffer {
        borg_core::BufferId::ObjectProp(struct_name, _) => Some(borg_core::CellRef::existence(
            struct_name.clone(),
            *cell.pid(),
        )),
        _ => None,
    }
}

async fn get(args: &Args, cell: &str) -> Result<()> {
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;
    let cell = parse::cell_ref(cell, allocation_branch(&registry)?, ALLOCATOR)?;

    let resolved = registry
        .resolver
        .resolve(
            branch,
            &cell,
            None,
            CLIENT_VERSION,
            FreshnessRequirement::Validated,
        )
        .await?;

    // Interned content reads back as content, so a string field prints `acme.ai` rather than the
    // `@s-…` that is physically stored (§3.4). What `borg get --value` prints is what `borg set`
    // accepts, which is the property a shell pipeline actually relies on.
    let rendered = match &resolved.value {
        Some(value) => Some(registry.values.render(value).await?),
        None => None,
    };

    if args.value_only {
        if let Some(value) = rendered {
            println!("{value}");
        }
        return Ok(());
    }

    println!("{cell}");
    println!(
        "  value:       {}",
        rendered.as_deref().unwrap_or("<absent>")
    );
    // Shown only when there is one: it is the proof that equal content is stored once, registry-wide
    // and branch-independently (§3.1), and there is nothing to show for a primitive.
    if let Some(pid) = resolved
        .value
        .as_ref()
        .and_then(|v| registry.values.content_pid(v))
    {
        println!("  interned:    @{pid}");
    }
    println!(
        "  origin:      {}",
        match resolved.origin {
            Origin::Source => "source",
            Origin::Derived => "derived",
        }
    );
    println!("  state:       {}", state_name(resolved.state));
    println!("  written at:  {}", resolved.written_at);
    println!("  fresh as of: {}", resolved.fresh_as_of);
    if let Some(producer) = resolved.by {
        println!("  produced by: {producer}");
    }
    Ok(())
}

const fn state_name(state: Freshness) -> &'static str {
    match state {
        Freshness::Current => "current",
        Freshness::Unvalidated => "unvalidated",
        Freshness::Stale => "stale",
        Freshness::Broken => "broken",
        Freshness::Tombstoned => "tombstoned",
    }
}

async fn explain(args: &Args, cell: &str) -> Result<()> {
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;
    let cell = parse::cell_ref(cell, allocation_branch(&registry)?, ALLOCATOR)?;

    let Some(lineage) = registry
        .resolver
        .explain(branch, &cell, None, CLIENT_VERSION)
        .await?
    else {
        println!("{cell}: nothing stored");
        return Ok(());
    };

    println!("{cell}");
    match lineage.produced_by {
        Some(producer) => println!("  produced by {producer} at {}", lineage.written_at),
        None => println!("  source, written at {}", lineage.written_at),
    }
    if !lineage.from.is_empty() {
        println!("  from");
        for edge in lineage.from {
            println!(
                "    {}  {}  @{}",
                edge.cell.cell,
                match edge.origin {
                    Origin::Source => "source ",
                    Origin::Derived => "derived",
                },
                edge.written_at
            );
        }
    }
    Ok(())
}

async fn branch_list(args: &Args) -> Result<()> {
    let registry = open(args).await?;
    let mut branches = registry.branches.all();
    branches.sort_by_key(|b| b.id.0);
    for branch in branches {
        let name = branch.name.unwrap_or_else(|| "<unnamed>".into());
        match branch.origin {
            Some(origin) => println!("{:<6} {name:<16} forked at {origin}", branch.id.to_string()),
            None => println!("{:<6} {name:<16} root", branch.id.to_string()),
        }
    }
    Ok(())
}

async fn branch_fork(args: &Args, parent: &str, tail: &[&str]) -> Result<()> {
    let registry = open(args).await?;
    let parent_id = branch_of(&registry, Some(parent))?;
    let at = flag(tail, "--at")
        .and_then(|v| v.trim_start_matches('L').parse::<u64>().ok())
        .map(LayerId)
        .or_else(|| registry.layers.head(parent_id))
        .ok_or_else(|| BorgError::Storage("nothing to fork from — the branch is empty".into()))?;
    let name = flag(tail, "--name").map(str::to_string);

    let id = registry.branches.fork(parent_id, at, name).await?;
    println!("{id}");
    Ok(())
}

async fn branch_merge(args: &Args, child: &str, tail: &[&str]) -> Result<()> {
    let registry = open(args).await?;
    let child_id = branch_of(&registry, Some(child))?;
    let mode = if tail.contains(&"--defs-only") {
        MergeMode::DefOnly
    } else {
        MergeMode::DefAndData
    };
    let replayed = registry.branches.merge(child_id, mode).await?;
    println!("{} layer(s) replayed", replayed.len());
    Ok(())
}

/// The JSON a `borg def push` file holds.
///
/// The repo is stated once at the top rather than repeated on every event, because a push comes from
/// exactly one repo.
#[derive(serde::Deserialize)]
struct DefFile {
    repo: u32,
    events: Vec<DefEventSpec>,
}

/// Matches the variant names of `DefEvent` so the file format reads like the log.
#[allow(clippy::enum_variant_names)]
#[derive(serde::Deserialize)]
enum DefEventSpec {
    DeclareField {
        struct_name: String,
        field: String,
        ty: String,
    },
    MutateField {
        struct_name: String,
        field: String,
        ty: String,
        up: u64,
        #[serde(default)]
        down: Option<u64>,
    },
    DeleteField {
        struct_name: String,
        field: String,
    },
}

fn value_type(name: &str) -> ValueType {
    match name {
        "Int" => ValueType::Int,
        "Bool" => ValueType::Bool,
        "Double" => ValueType::Double,
        "String" => ValueType::String,
        "Binary" => ValueType::Binary,
        "BigInt" => ValueType::BigInt,
        "Any" => ValueType::Any,
        other => ValueType::Object(other.into()),
    }
}

async fn def_push(args: &Args, file: &str) -> Result<()> {
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;

    let raw = std::fs::read_to_string(file)
        .map_err(|err| BorgError::Storage(format!("{file}: {err}")))?;
    let spec: DefFile =
        serde_json::from_str(&raw).map_err(|err| BorgError::Storage(format!("{file}: {err}")))?;
    let repo = RepoId(spec.repo);

    let events = spec
        .events
        .into_iter()
        .map(|event| match event {
            DefEventSpec::DeclareField {
                struct_name,
                field,
                ty,
            } => DefEvent::DeclareField {
                struct_name: struct_name.into(),
                field: field.into(),
                ty: value_type(&ty),
                repo,
            },
            DefEventSpec::MutateField {
                struct_name,
                field,
                ty,
                up,
                down,
            } => DefEvent::MutateField {
                struct_name: struct_name.into(),
                field: field.into(),
                ty: value_type(&ty),
                repo,
                up: ProducerId(up),
                down: down.map(ProducerId),
            },
            DefEventSpec::DeleteField { struct_name, field } => DefEvent::DeleteField {
                struct_name: struct_name.into(),
                field: field.into(),
                repo,
            },
        })
        .collect();

    let layer = registry.defs.push(branch, events).await?;
    println!("{layer}");
    Ok(())
}

async fn def_show(args: &Args, name: &str) -> Result<()> {
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;
    let path = registry.branches.read_path(branch, None)?;
    let view = registry.defs.view(&path).await?;

    let name: ObjectTypeName = name.into();
    let Some(def) = view.object(&name) else {
        return Err(BorgError::Storage(format!("no struct named `{name}`")));
    };
    println!("{name}");
    for (field, def) in &def.fields {
        println!(
            "  {field:<16} {:<10} repo {:<4} v{}",
            format!("{:?}", def.ty),
            def.declaring_repo.0,
            def.version
        );
    }
    Ok(())
}

async fn layer_list(args: &Args) -> Result<()> {
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;
    let mut layers = registry.layers.layers_of(branch);
    layers.sort_by_key(|l| l.id.0);
    for layer in layers {
        let author = match layer.author {
            LayerAuthor::Source => "source".to_string(),
            LayerAuthor::Derived { producer, reflects } => {
                format!("derived by {producer}, reflects {reflects}")
            }
        };
        println!(
            "{:<6} {:<6} {author}",
            layer.id.to_string(),
            match layer.kind {
                LayerKind::Value => "value",
                LayerKind::Def => "def",
            }
        );
    }
    Ok(())
}

async fn layer_head(args: &Args) -> Result<()> {
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;
    println!("{}", registry.layers.head(branch).unwrap_or(LayerId(0)));
    Ok(())
}

async fn frontier(args: &Args) -> Result<()> {
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;
    let path = registry.branches.read_path(branch, None)?;
    let view = registry.defs.view(&path).await?;

    // The log knows producer ids; only the implementation table knows what a human called them.
    // Joining the two here is the CLI's job precisely because the log must not hold either.
    let names = load_impls(args);
    let head = registry.layers.head(branch).unwrap_or(LayerId(0));
    let mut any = false;
    for producer in view.producers() {
        any = true;
        let name = names
            .producers
            .iter()
            .find(|impl_| impl_.id == producer.id.0)
            .map_or_else(|| producer.id.to_string(), |impl_| impl_.name.clone());
        println!(
            "{name:<16} caught up through {} (head {head})",
            registry.frontier.watermark(branch, producer.id)
        );
    }
    if !any {
        println!("no producers registered");
    }
    Ok(())
}

fn flag<'a>(tail: &[&'a str], name: &str) -> Option<&'a str> {
    tail.iter()
        .position(|arg| *arg == name)
        .and_then(|i| tail.get(i + 1))
        .copied()
}

// --- Repos, producers and derivation ---

/// Where a producer's implementation lives.
///
/// Deliberately *not* in the log. §9.2 separates a producer's definition from its implementation:
/// the log records that producer P exists at some ClientVersion, and the `ExecutionProvider`
/// resolves that id to code. Writing a local file path into the log would tie the data model to one
/// machine's filesystem. This sidecar is the CLI's own resolution table, and a container-backed
/// runtime would keep an image reference in exactly the same place.
#[derive(Default, serde::Serialize, serde::Deserialize)]
struct Implementations {
    producers: Vec<Implementation>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Implementation {
    id: u64,
    name: String,
    source: String,
    command: PathBuf,
}

fn impls_path(args: &Args) -> PathBuf {
    args.store.with_extension("producers.json")
}

fn load_impls(args: &Args) -> Implementations {
    std::fs::read_to_string(impls_path(args))
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_default()
}

fn save_impls(args: &Args, impls: &Implementations) -> Result<()> {
    let raw =
        serde_json::to_string_pretty(impls).map_err(|err| BorgError::Storage(err.to_string()))?;
    std::fs::write(impls_path(args), raw).map_err(|err| BorgError::Storage(err.to_string()))
}

/// Push a repo: ask each pipeline to describe itself, record the definitions in the log, and
/// remember where the code lives.
async fn repo_push(args: &Args, dir: &str) -> Result<()> {
    let dir = PathBuf::from(dir);
    let repo = read_repo_id(&dir)?;
    let registry = open(args).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;

    let mut pipelines: Vec<PathBuf> = std::fs::read_dir(dir.join("pipelines"))
        .map_err(|err| BorgError::Storage(format!("{}/pipelines: {err}", dir.display())))?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| path.is_file())
        .collect();
    pipelines.sort();

    let mut impls = load_impls(args);
    let mut events = Vec::new();
    for command in pipelines {
        // The script is the source of truth for what it implements, so a producer definition cannot
        // exist without the code that satisfies it.
        let described = borg_exec_process::describe(&command)?;
        for spec in described.producers {
            let id = ProducerId(spec.id());
            events.push(DefEvent::PushProducer(borg_core::ProducerDef {
                id,
                kind: borg_core::ProducerKind::Pipeline,
                source: borg_core::BufferId::Object(spec.source.as_str().into()),
                version: LayerId(1),
                declaring_repo: repo,
            }));
            impls.producers.retain(|p| p.id != id.0);
            impls.producers.push(Implementation {
                id: id.0,
                name: spec.name.clone(),
                source: spec.source.clone(),
                command: command.canonicalize().unwrap_or_else(|_| command.clone()),
            });
            println!("{} -> {id}", spec.name);
        }
    }

    if !events.is_empty() {
        registry.defs.push(branch, events).await?;
    }
    save_impls(args, &impls)
}

/// Read the repo id out of `borg.toml`.
fn read_repo_id(dir: &Path) -> Result<RepoId> {
    let manifest = dir.join("borg.toml");
    let raw = std::fs::read_to_string(&manifest)
        .map_err(|err| BorgError::Storage(format!("{}: {err}", manifest.display())))?;
    for line in raw.lines() {
        if let Some((key, value)) = line.split_once('=')
            && key.trim() == "id"
            && let Ok(id) = value.trim().parse::<u32>()
        {
            return Ok(RepoId(id));
        }
    }
    Err(BorgError::Storage(format!(
        "{}: no `id` under [repo]",
        manifest.display()
    )))
}

async fn producer_list(args: &Args) -> Result<()> {
    for producer in load_impls(args).producers {
        println!(
            "{:<20} {:<10} maps {:<12} {}",
            producer.name,
            format!("P{}", producer.id),
            producer.source,
            producer.command.display()
        );
    }
    Ok(())
}

/// Run every producer forward to head.
async fn derive(args: &Args) -> Result<()> {
    let storage = Arc::new(SqliteStorage::open(&args.store)?);
    let impls = load_impls(args);

    let probe = Registry::open(
        Arc::clone(&storage) as Arc<_>,
        Arc::new(NativeExecutor::new()),
    )
    .await?;
    let allocation = allocation_branch(&probe)?;
    let executor = borg_exec_process::from_registrations(
        allocation,
        impls
            .producers
            .iter()
            .map(|p| (p.id, p.command.clone(), p.source.clone())),
    );
    drop(probe);

    let registry = Registry::open(storage, Arc::clone(&executor) as Arc<_>).await?;
    let branch = branch_of(&registry, args.branch.as_deref())?;

    // Producer definitions live in the log; this makes the engine aware of the ones on this branch.
    let path = registry.branches.read_path(branch, None)?;
    let view = registry.defs.view(&path).await?;
    for def in view.producers() {
        registry.engine.register(def.clone());
    }

    let executed = registry.engine.catch_up(branch).await?;
    executor.shutdown().await;

    if args.count_only {
        println!("{executed}");
    } else {
        println!("{executed} invocation(s)");
    }
    Ok(())
}
