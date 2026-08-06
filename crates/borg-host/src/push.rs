//! `repo push` — a repo's whole schema, diffed against what the branch believes. SPEC.md §5.2, §9.2.
//!
//! Its own module rather than a function in [`crate::ops`] for two reasons. It is the only operation
//! that reads a *directory* rather than a store, so it is the seam where "local" stops being an
//! implementation detail and becomes part of the contract (§17.6): the path is a path on the machine
//! running this code, and when that machine is a server the path is the server's. And it is a diff
//! rather than a command — a repo emits the shape it believes in now and this decides what that
//! *means* against the definitions in force — so the interesting part is a comparison, not a write.
//!
//! **Three front ends, one implementation.** `borg repo push <dir>` runs it in the CLI's process;
//! `repo_push` over the socket runs it in the server's; both render [`Push`] their own way. Before
//! this module the second did not exist at all, and pushing a schema to a served store meant
//! stopping the server — see [`repo_push`] for what makes it safe now.

use crate::ops::{self, Ops};
use borg_core::{BorgError, DefEvent, LayerId, ObjectTypeName, ProducerId, RepoId, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// What one push turned out to be, in the words both front ends print.
pub struct Push {
    /// The def layer it landed, if it landed one. **`None` is the ordinary case in a dev loop**: a
    /// repo describing exactly what is already in force has nothing to say, and a def layer saying
    /// nothing is still a def layer (§9.2).
    pub layer: Option<LayerId>,
    /// One line per definition or producer that moved, then the consequences. Rendered rather than
    /// structured because every consumer prints it: the CLI to stdout, the server into the response
    /// and thence to whoever asked.
    pub report: Vec<String>,
}

/// Push a repo at `dir` into the branch `args` names, and catch that branch up. SPEC.md §9.2.
///
/// **The registry this runs against may be one a server is holding open, and that is the point.**
/// Until the fingerprint work (§9.2) a push against a live store was not merely unimplemented but
/// unsafe to want: `repo push` recomputed every producer's source buffer whether or not anything had
/// changed, so a dev loop that pushed on every boot would have re-derived the world on every boot.
/// It is idempotent now — an unchanged repo emits no event and lands no layer — and code-change
/// aware, so a push is exactly as expensive as the change it carries.
///
/// What a caller holding a `Registry` open must know: this moves **two** things, and they are moved
/// through different doors.
///
/// * **Definitions** travel the log, so they go through the registry in `args` — the held one, when
///   there is one ([`ops::open`]). Its projections are maintained on the way in like any other
///   commit, so the instance that answered the last read is the instance that has the new defs.
/// * **Implementations** — where each producer's code lives — are a sidecar (§9.2), and the worker
///   pool a server built at boot was built from the old copy. So this calls
///   [`ops::Held::reload_producers`] when there is a held registry, **before** it catches the branch
///   up: the catch-up is what runs the new definitions, and running them against the old pool would
///   reproduce the exact mislabelling the fingerprint work exists to prevent.
///
/// What the caller must supply is the **gate**: `borg-server` holds this registry's gate for the
/// whole call (`crate::host`), which is what makes "no invocation is in flight while the pool is
/// discarded" true rather than hoped for.
pub async fn repo_push(args: &Ops, dir: &Path) -> Result<Push> {
    let repo = read_repo_id(dir)?;
    let registry = ops::open(args).await?;
    let branch = ops::branch_of(&registry, args.branch.as_deref())?;

    let mut scripts: Vec<PathBuf> = std::fs::read_dir(dir.join("pipelines"))
        .map_err(|err| BorgError::Storage(format!("{}/pipelines: {err}", dir.display())))?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| path.is_file())
        .collect();
    scripts.sort();

    // Everything the repo describes, gathered before anything is emitted: a `derived_by` may name a
    // producer implemented by a different script in the same repo, so ownership can only be resolved
    // once the whole repo has spoken.
    let mut described = Vec::new();
    for command in scripts {
        // The script is the source of truth for what it implements, so a producer definition cannot
        // exist without the code that satisfies it.
        let description = borg_exec_process::describe(&command)?;
        // An SDK whose author writes the repo id in code as well as in `borg.toml` has two copies
        // of one fact. `borg.toml` is the authoritative one — a repo is a directory, and one
        // directory has one id however many executables it holds — so the other is checked rather
        // than ignored. A repo that says nothing (every shell worker) skips this.
        if let Some(claimed) = description.repo
            && claimed != repo.0
        {
            return Err(BorgError::Storage(format!(
                "{} describes itself as repo {claimed}, but {}/borg.toml says {}",
                command.display(),
                dir.display(),
                repo.0
            )));
        }
        described.push((command.clone(), description));
    }

    // The definitions this push is a *diff against*. A repo emits its whole schema every time
    // (§5.2), so what it means by a field depends on what is already declared: nothing yet is a
    // declaration, a different type is a mutation, the same type is a repeat. The same question is
    // asked of producers, where the answer turns on the implementation fingerprint (§9.2).
    //
    // Read before a single event is built, because *every* arm below needs it — a repeat is only
    // recognisable against what is in force.
    let path = registry.branches.read_path(branch, None)?;
    let view = registry.defs.view(&path).await?;

    let mut impls = ops::load_impls(args);
    let mut events = Vec::new();
    // Held back until the push is accepted — see [`Push`].
    let mut report: Vec<String> = Vec::new();
    // What this push turned out to be, counted rather than listed: the number is the interesting
    // part, because it is how many source buffers are about to be recomputed. Also what lets a
    // no-op push say so instead of saying nothing at all.
    let mut tally = PushTally::default();
    // One digest per file, not per producer: several producers may be described by one command, and
    // hashing a file once per pipeline it declares is work for nothing.
    let mut fallbacks: HashMap<PathBuf, Option<String>> = HashMap::new();

    for (command, description) in &described {
        for spec in &description.producers {
            let id = ProducerId(spec.id());
            let def = borg_core::ProducerDef {
                id,
                kind: borg_core::ProducerKind::Pipeline,
                source: borg_core::BufferId::Object(spec.source.as_str().into()),
                version: LayerId(0),
                declaring_repo: repo,
                fingerprint: fingerprint_of(spec.fingerprint.as_ref(), command, &mut fallbacks),
            };
            land(
                producer_change(view.producer(id), &def),
                def,
                &spec.name,
                &mut events,
                &mut report,
                &mut tally,
            );
            // Remembered on **every** push, whether or not the definition moved. Where the code
            // lives is a fact about this machine (§9.2) and can change while the program does not —
            // a repo checked out to a new path is the same producer at a new file.
            ops::remember(
                &mut impls,
                id,
                &spec.name,
                &spec.source,
                command,
                description.transport,
            );
        }
    }

    let known: Vec<&str> = described
        .iter()
        .flat_map(|(_, d)| {
            let pipelines = d.producers.iter().map(|p| p.name.as_str());
            pipelines.chain(d.migrations.iter().map(|m| m.name.as_str()))
        })
        .collect();
    let resolve = |owner: &str, what: &str| -> Result<ProducerId> {
        if known.contains(&owner) {
            return Ok(ProducerId(borg_protocol::producer_id(owner)));
        }
        Err(BorgError::Storage(format!(
            "{what} names `{owner}`, which this repo does not implement (it implements: {})",
            known.join(", ")
        )))
    };

    for (_, description) in &described {
        for spec in &description.structs {
            let struct_name: ObjectTypeName = spec.name.as_str().into();
            for field in &spec.fields {
                let ty = ops::value_type(&field.ty);
                let what = format!("{}.{}", spec.name, field.name);

                // A migration's definition names the field buffer it maps over (§9.3) and its
                // direction; which two versions it bridges is folded from the `MutateField` below,
                // on whichever branch that event ends up on.
                let source = borg_core::BufferId::ObjectProp(
                    struct_name.clone(),
                    field.name.as_str().into(),
                );
                let mut migration = |name: &Option<String>,
                                     direction|
                 -> Result<Option<ProducerId>> {
                    let Some(name) = name else { return Ok(None) };
                    let id = resolve(name, &what)?;
                    let (command, transport, supplied) = described
                        .iter()
                        .find_map(|(command, d)| {
                            let spec = d.migrations.iter().find(|m| m.name == *name)?;
                            Some((command.clone(), d.transport, spec.fingerprint.clone()))
                        })
                        .expect("resolve() accepted the name, so some script described it");
                    let def = borg_core::ProducerDef {
                        id,
                        kind: borg_core::ProducerKind::Migration { direction },
                        source: source.clone(),
                        version: LayerId(0),
                        declaring_repo: repo,
                        // A migration is a producer and its code moves the same way (§9.1).
                        // Note the *role* it plays is not in here: which two versions it bridges
                        // is folded from the `MutateField` below (§9.3), so a repeat push of the
                        // same migration code is a repeat however many steps it has bridged.
                        fingerprint: fingerprint_of(supplied.as_ref(), &command, &mut fallbacks),
                    };
                    land(
                        producer_change(view.producer(id), &def),
                        def,
                        name,
                        &mut events,
                        &mut report,
                        &mut tally,
                    );
                    ops::remember(&mut impls, id, name, &spec.name, &command, transport);
                    Ok(Some(id))
                };
                let up = migration(&field.up, borg_core::MigrationDirection::Up)?;
                let down = migration(&field.down, borg_core::MigrationDirection::Down)?;

                let name: borg_core::FieldName = field.name.as_str().into();
                let declared = view
                    .object(&struct_name)
                    .and_then(|object| object.fields.get(&name));
                match declared {
                    // The type moved. §6.1 says that needs migrations, and the field is where they
                    // are named — a repo cannot say "mutate from String" because it does not know
                    // what it is mutating from, and on another branch the answer differs.
                    Some(existing) if existing.ty != ty => {
                        // Asked before the missing-`up` question, because for a derived field the
                        // answer is not "name a migration" — no migration can be appointed for it at
                        // all, so advising one would send the author to write code the next push
                        // would reject.
                        if let Some(owner) = existing.ownership.producer() {
                            return Err(BorgError::MigrationOnDerivedField {
                                struct_name: struct_name.clone(),
                                field: field.name.clone(),
                                owner,
                            });
                        }
                        let Some(up) = up else {
                            return Err(BorgError::Storage(format!(
                                "{what} changes from {} to {ty}, which needs an `up` migration to \
                                 carry the existing values forward",
                                existing.ty
                            )));
                        };
                        events.push(DefEvent::MutateField {
                            struct_name: struct_name.clone(),
                            field: field.name.as_str().into(),
                            ty,
                            repo,
                            up,
                            down,
                        });
                        report.push(format!("{what} {} -> {}", existing.ty, field.ty));
                    }
                    _ => {
                        let owner = match &field.derived_by {
                            // A field owned by a producer this repo does not implement would be a
                            // field nothing can ever write. Caught here rather than at the first
                            // write attempt.
                            Some(name) => Some(resolve(name, &what)?),
                            None => None,
                        };
                        let ownership = ops::ownership(owner);
                        // **The same repo redeclaring the same shape emits nothing.** The fold
                        // already treats it as a repeat rather than a collision (`DefView::apply`),
                        // so this changes no definition — what it changes is whether a *layer*
                        // exists to hold it. A repo emits its whole schema every push (§5.2), so
                        // without this every push of an unchanged repo would land a def layer, walk
                        // the branch's def-version up by one, and make `repo push` something a dev
                        // loop has to guard against running (FRICTION #2).
                        //
                        // A field whose *ownership* moved is deliberately not a repeat: the fold
                        // rejects that as a collision, and it has to be emitted to be rejected.
                        if declared.is_some_and(|existing| {
                            existing.ty == ty
                                && existing.declaring_repo == repo
                                && existing.ownership == ownership
                        }) {
                            tally.unchanged += 1;
                            continue;
                        }
                        events.push(DefEvent::DeclareField {
                            struct_name: struct_name.clone(),
                            field: field.name.as_str().into(),
                            ty,
                            repo,
                            ownership,
                        });
                        report.push(format!("{what} {}", field.ty));
                    }
                }
            }
        }
    }

    // A migration nothing names bridges nothing. It would be registered, implemented and never
    // reachable, which is worth a push-time error rather than a puzzle later.
    for (_, description) in &described {
        for spec in &description.migrations {
            if !impls.producers.iter().any(|p| p.name == spec.name) {
                return Err(BorgError::Storage(format!(
                    "`{}` is implemented but no field names it as its `up` or `down`",
                    spec.name
                )));
            }
        }
    }

    // **No events, no layer.** A repo describing exactly what is already in force has nothing to
    // say, and a def layer saying nothing is still a def layer: it moves the branch's def-version,
    // regenerates every client built from it, and costs a round settling a change nobody made.
    let layer = if events.is_empty() {
        None
    } else {
        Some(registry.defs.push(branch, events).await?)
    };
    drop(registry);
    ops::save_impls(args, &impls)?;
    // **Before the catch-up below, not after it.** The push has just rewritten the table the worker
    // pool was built from, and the very next thing that happens is a round running the producers it
    // names — so a reload after derivation would run the new definitions against the old code, which
    // is exactly the mislabelled output the fingerprint work exists to prevent. Nothing here for the
    // CLI, which has no pool to correct.
    if let Some(held) = &args.held {
        held.reload_producers(args).await;
    }

    // The consequence, spelled out, because the next thing that happens is a round that recomputes
    // whole source buffers and the author should not have to infer that from a def-version moving.
    if tally.recomputing > 0 {
        report.push(format!(
            "implementation changed, recomputing {}",
            plural(tally.recomputing, "producer")
        ));
    }
    if tally.first_seen > 0 {
        // The one-time cost of this mechanism arriving. Nothing recorded which program produced the
        // values already in the store, so absent → present reads as changed and buys certainty for
        // one recompute. Said out loud so it is not mistaken for a bug the first time.
        report.push(format!(
            "implementation changed (first fingerprint), recomputing {}",
            plural(tally.first_seen, "producer")
        ));
    }
    if report.is_empty() {
        report.push(format!(
            "unchanged: {} already in force, nothing pushed",
            plural(tally.unchanged, "definition")
        ));
    }

    ops::auto_derive(args, branch).await?;
    Ok(Push { layer, report })
}

fn plural(count: usize, noun: &str) -> String {
    if count == 1 {
        format!("{count} {noun}")
    } else {
        format!("{count} {noun}s")
    }
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

/// What this push does to one producer's definition. SPEC.md §9.2.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ProducerChange {
    /// Nothing in force names it.
    New,
    /// The definition and the implementation are both exactly what is already in force, so the push
    /// is a repeat and emits nothing.
    Unchanged,
    /// The code moved. The definition re-lands, its ClientVersion moves to the new def-layer, and
    /// every value it has already written is invalid because a different program produced it.
    Implementation {
        /// Nothing was in force to compare against — the store predates fingerprints. One recompute
        /// buys certainty about values whose provenance nothing recorded.
        first: bool,
    },
    /// Something structural moved: the buffer it maps over, its kind, the repo declaring it.
    Definition,
}

/// Diff one producer against what is in force. SPEC.md §9.2.
///
/// **The implementation is part of the definition here, and that is the whole point.** A repo emits
/// the shape it believes in and `repo push` compares it with the branch's — but until the
/// fingerprint existed, the comparable surface was name, source buffer and kind, none of which an
/// edit to a pipeline's body touches. So an edited pipeline diffed as *unchanged*, no event was
/// emitted, the producer's ClientVersion never moved, and its old output went on being served
/// labelled `current` beside output from the new code. The invalidation machinery was never broken;
/// nothing was telling it anything had happened.
fn producer_change(
    existing: Option<&borg_core::ProducerDef>,
    next: &borg_core::ProducerDef,
) -> ProducerChange {
    let Some(existing) = existing else {
        return ProducerChange::New;
    };
    if existing.kind != next.kind
        || existing.source != next.source
        || existing.declaring_repo != next.declaring_repo
    {
        return ProducerChange::Definition;
    }
    match (&existing.fingerprint, &next.fingerprint) {
        (Some(before), Some(after)) if before == after => ProducerChange::Unchanged,
        (Some(_), Some(_)) => ProducerChange::Implementation { first: false },
        (None, Some(_)) => ProducerChange::Implementation { first: true },
        // **Absent means never invalidate on a code change**, and it is a documented status quo
        // rather than a failure. A producer nothing can fingerprint — no digest from `describe` and
        // a command file that cannot be read — is exactly as invisible to a code edit as every
        // producer was before this existed. Treating it as changed instead would recompute its whole
        // source buffer on every push, for ever, on no evidence at all.
        (_, None) => ProducerChange::Unchanged,
    }
}

/// Add one producer's definition to the push, or account for its absence. See [`ProducerChange`].
fn land(
    change: ProducerChange,
    def: borg_core::ProducerDef,
    name: &str,
    events: &mut Vec<DefEvent>,
    report: &mut Vec<String>,
    tally: &mut PushTally,
) {
    let id = def.id;
    match change {
        ProducerChange::Unchanged => {
            tally.unchanged += 1;
            return;
        }
        ProducerChange::New => report.push(format!("{name} -> {id}")),
        ProducerChange::Definition => {
            report.push(format!("{name} -> {id} (definition changed)"));
            tally.recomputing += 1;
        }
        ProducerChange::Implementation { first: false } => {
            report.push(format!("{name} -> {id} (implementation changed)"));
            tally.recomputing += 1;
        }
        ProducerChange::Implementation { first: true } => {
            report.push(format!(
                "{name} -> {id} (implementation changed, first fingerprint)"
            ));
            tally.first_seen += 1;
        }
    }
    events.push(DefEvent::PushProducer(def));
}

/// What one `repo push` turned out to be.
#[derive(Default)]
struct PushTally {
    /// Producers whose code moved and whose source buffers are therefore about to be recomputed.
    recomputing: usize,
    /// Producers that had no fingerprint to move from — the one-time migration effect.
    first_seen: usize,
    /// Definitions the push had nothing to say about.
    unchanged: usize,
}

/// The fingerprint to record for one producer: what `describe` said, or the command file's bytes.
///
/// **The fallback is what gives a `bash`-and-`jq` repo coverage.** A shell worker cannot reasonably
/// compute a digest of itself in `jq`, and requiring one would make this mechanism a feature of
/// repos written in a language with an SDK — which is the opposite of what §17.4 is for. The engine
/// already has the file open in the sense that matters: it just executed it.
///
/// An SDK supplies its own only when it can cover *more* than the one file. Where it does, its
/// answer wins: the fallback would be a strict subset of what it already accounted for.
///
/// `None` — a file that cannot be read at all — is the documented status quo, see
/// [`producer_change`]. Deliberately not an error: `describe` has already run this command
/// successfully, so a read failure here says something has changed underfoot, and refusing the whole
/// push over a hash is a worse answer than pushing without one.
fn fingerprint_of(
    supplied: Option<&String>,
    command: &PathBuf,
    cache: &mut HashMap<PathBuf, Option<String>>,
) -> Option<String> {
    if let Some(supplied) = supplied {
        return Some(supplied.clone());
    }
    cache
        .entry(command.clone())
        .or_insert_with(|| {
            std::fs::read(command)
                .ok()
                .map(|bytes| borg_protocol::fingerprint(&bytes))
        })
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use borg_core::{BufferId, MigrationDirection, ProducerKind};

    fn def(fingerprint: Option<&str>) -> borg_core::ProducerDef {
        borg_core::ProducerDef {
            id: ProducerId(7),
            kind: ProducerKind::Pipeline,
            source: BufferId::Object("Company".into()),
            version: LayerId(3),
            declaring_repo: RepoId(1),
            fingerprint: fingerprint.map(str::to_string),
        }
    }

    /// The bug FRICTION #17 measured, as one assertion. Everything the diff could compare before
    /// fingerprints existed is equal here, and the code is not.
    #[test]
    fn an_edited_body_is_a_change_even_though_nothing_about_the_shape_moved() {
        let before = def(Some("sha256:one"));
        let after = def(Some("sha256:two"));
        assert_eq!(before.id, after.id);
        assert_eq!(before.source, after.source);
        assert_eq!(
            producer_change(Some(&before), &after),
            ProducerChange::Implementation { first: false }
        );
    }

    /// The half that makes it affordable. A dev loop pushes constantly, and a mechanism that
    /// recomputed every source buffer on every push is one nobody would leave switched on. It is
    /// also the precondition for pushing against a *live* server (§17.6).
    #[test]
    fn the_same_code_pushed_again_emits_nothing() {
        assert_eq!(
            producer_change(Some(&def(Some("sha256:one"))), &def(Some("sha256:one"))),
            ProducerChange::Unchanged
        );
    }

    #[test]
    fn a_producer_nothing_has_ever_defined_is_new() {
        assert_eq!(
            producer_change(None, &def(Some("sha256:one"))),
            ProducerChange::New
        );
    }

    /// The one-time migration effect. A store written before this existed says nothing about what
    /// produced its values, so the only honest reading of absent → present is "changed" — and it
    /// costs exactly one recompute, once, which the CLI says out loud.
    #[test]
    fn a_store_that_has_never_seen_a_fingerprint_recomputes_once() {
        assert_eq!(
            producer_change(Some(&def(None)), &def(Some("sha256:one"))),
            ProducerChange::Implementation { first: true }
        );
    }

    /// **Absent means never invalidate on a code change**, documented rather than accidental. A
    /// producer nothing can fingerprint is exactly as invisible to an edit as every producer was
    /// before this existed; treating it as changed would recompute its buffer on every push for ever
    /// on no evidence at all.
    #[test]
    fn a_producer_that_cannot_be_fingerprinted_keeps_the_status_quo() {
        assert_eq!(
            producer_change(Some(&def(None)), &def(None)),
            ProducerChange::Unchanged
        );
        assert_eq!(
            producer_change(Some(&def(Some("sha256:one"))), &def(None)),
            ProducerChange::Unchanged
        );
    }

    /// Structural moves are still moves, and are diffed the same way they always were — the
    /// fingerprint is an addition to the comparable surface, not a replacement for it.
    #[test]
    fn a_producer_that_maps_over_a_different_buffer_is_a_change() {
        let before = def(Some("sha256:one"));
        let mut after = def(Some("sha256:one"));
        after.source = BufferId::Object("Contact".into());
        assert_eq!(
            producer_change(Some(&before), &after),
            ProducerChange::Definition
        );

        let mut migration = def(Some("sha256:one"));
        migration.kind = ProducerKind::Migration {
            direction: MigrationDirection::Up,
        };
        assert_eq!(
            producer_change(Some(&before), &migration),
            ProducerChange::Definition,
            "a pipeline that became a migration is not the same producer doing the same job"
        );
    }

    /// A ClientVersion is stamped by the fold and is never authored (§9.2), so the version a repo
    /// happens to be carrying must not be part of the comparison — every event a push builds has the
    /// same placeholder in it.
    #[test]
    fn the_placeholder_client_version_is_not_part_of_the_diff() {
        let mut before = def(Some("sha256:one"));
        before.version = LayerId(99);
        assert_eq!(
            producer_change(Some(&before), &def(Some("sha256:one"))),
            ProducerChange::Unchanged
        );
    }
}
