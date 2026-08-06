//! `borg generate` — the typed client, emitted from a branch's definitions. SPEC.md §15,
//! SDK-DRAFT.md §4.4.
//!
//! One module out, containing an interface and a runtime descriptor per struct plus a
//! `createBorgContext` with the **generation-time def-layer baked in as the ClientVersion**. That
//! stamp is the whole reason codegen is not merely a convenience: §5.4 says an actor's def-view is
//! the one its code was authored against, and generated code is the only actor that can honestly
//! state one. The CLI cannot — it has no generated code, so every invocation is authored *now* — and
//! an un-generated SDK client cannot either, which is why its hello omits the field.
//!
//! ## Where the definitions come from
//!
//! **From the socket if the store is served, from the store if it is not**, decided per read and not
//! by a flag. A served store refuses every other `borg` invocation (§17.5), so a `generate` that
//! only knew how to open a file would fail exactly when a developer is most likely to run it — with
//! their server up — and the fix would be to stop the server, regenerate, and start it again.
//!
//! This is the first place the CLI *connects to* the socket instead of being turned away by it,
//! which SDK-DRAFT §2.6 names as the remote-connection future. It
//! is scoped to this one command on purpose: `generate` only reads, so it needs none of the answers
//! the general case needs about transactions, `$BORG_TX`, or which process owns a write.
//!
//! ## `--watch`
//!
//! Polls. There is no server push in §17.5 and adding one would be a change of shape — the protocol
//! is one request, one response, in order, with no correlation ids because there is nothing to
//! correlate. So the loop asks for the def view and rewrites the file when the answer changed;
//! SDK-DRAFT §4.4 records this as a real gap rather than as a thing that was hacked around.
//!
//! It polls the **def view** rather than `branch_head`, and the difference is not an optimisation:
//! head moves on every data write, and a generated module changes only when a *def* layer lands
//! (§5.3). Watching head would rewrite the file on every `borg set`.

use borg_core::{BorgError, Result};
use borg_host::ops::{self, Ops};
use borg_host::render::struct_def;
use borg_host::serving;
use borg_protocol::client::{ClientHello, Request, Response, SchemaDef, StructDef};
use borg_protocol::{Codec, ServerHello};
use std::io::BufReader;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

/// The file a generated module lands in. Named rather than derived from the branch or the schema
/// because it is imported by hand, and an import path that moved when the schema did would be the
/// one thing regeneration must never break.
pub const MODULE: &str = "borg.generated.ts";

/// How often `--watch` asks. A def push is a human action, so this is tuned for "notices before you
/// have alt-tabbed" rather than for latency.
const POLL: std::time::Duration = std::time::Duration::from_millis(400);

pub struct Generate {
    /// `--lang`. One today; named rather than assumed, because the second one is the point of §15.
    pub lang: String,
    /// `-o` / `--out`.
    pub out: PathBuf,
    pub watch: bool,
    /// `--socket`, to override the socket the store's own lock record names.
    pub socket: Option<PathBuf>,
}

pub async fn run(args: &Ops, options: &Generate) -> Result<()> {
    if options.lang != "ts" && options.lang != "typescript" {
        return Err(BorgError::Storage(format!(
            "`{}` is not a language borg generates — try ts",
            options.lang
        )));
    }

    if !options.watch {
        let (schema, mode) = read_schema(args, options).await?;
        let path = write(&options.out, &module(&schema))?;
        eprintln!("{}", wrote(&path, &schema, &mode));
        return Ok(());
    }

    // Reported once and then only when it changes, so a watch that silently switched from the
    // socket to the store — because someone stopped the server — says so.
    let mut announced: Option<String> = None;
    let mut written: Option<String> = None;
    loop {
        match read_schema(args, options).await {
            Ok((schema, mode)) => {
                if announced.as_deref() != Some(mode.as_str()) {
                    eprintln!("watching {}, reading {mode}", args.store.display());
                    announced = Some(mode.clone());
                }
                let source = module(&schema);
                if written.as_deref() != Some(source.as_str()) {
                    let path = write(&options.out, &source)?;
                    eprintln!("{}", wrote(&path, &schema, &mode));
                    written = Some(source);
                }
            }
            // A server stopping mid-watch, a store momentarily locked: reported and retried, never
            // fatal. A watch that exited the first time the thing it watches was busy would be a
            // watch nobody trusts to still be running.
            Err(err) => {
                if announced.is_some() {
                    eprintln!("warning: {err}");
                    announced = None;
                }
            }
        }
        tokio::time::sleep(POLL).await;
    }
}

/// What was written, at what version, and **how it was read** — the last of those because "am I
/// looking at the store or at the server?" is the one question a developer with both running has,
/// and a tool that answers it only when something goes wrong answers it too late.
fn wrote(path: &Path, schema: &SchemaDef, mode: &str) -> String {
    let fields: usize = schema.structs.iter().map(|s| s.fields.len()).sum();
    format!(
        "{} — {} struct{}, {fields} field{} at {}, read {mode}",
        path.display(),
        schema.structs.len(),
        if schema.structs.len() == 1 { "" } else { "s" },
        if fields == 1 { "" } else { "s" },
        schema.version,
    )
}

fn write(out: &Path, source: &str) -> Result<PathBuf> {
    std::fs::create_dir_all(out)
        .map_err(|err| BorgError::Storage(format!("{}: {err}", out.display())))?;
    let path = out.join(MODULE);
    std::fs::write(&path, source)
        .map_err(|err| BorgError::Storage(format!("{}: {err}", path.display())))?;
    Ok(path)
}

// --- Where the definitions come from ---------------------------------------------------------------

/// The schema, and a phrase saying how it was obtained. See the module header.
async fn read_schema(args: &Ops, options: &Generate) -> Result<(SchemaDef, String)> {
    // The store's own lock record says both halves of what a connection needs: where the server is
    // listening, and **what it calls this store** — one socket serves a directory of registries and
    // the handshake is what routes (§17.6), so a generator that knew only the address would be
    // asking an arbitrary registry for its schema.
    let served = serving::served_on(&args.store);
    let socket = options
        .socket
        .clone()
        .or_else(|| served.as_ref().map(|served| served.socket.clone()));
    match socket {
        Some(socket) => {
            let registry = served.and_then(|served| served.registry);
            let schema = over_socket(&socket, registry.as_deref(), args.branch.as_deref())?;
            Ok((schema, format!("through {}", socket.display())))
        }
        None => {
            let (version, structs) = ops::def_view(args).await?;
            let schema = SchemaDef {
                version: version.to_string(),
                structs: structs.iter().map(struct_def).collect(),
            };
            Ok((schema, "directly".to_string()))
        }
    }
}

/// The first Rust client of §17.5, and it is deliberately small: connect, hello, one request, one
/// response. If this needed a client library, the protocol would have the hidden complexity
/// `scenarios/250-serve` exists to disprove.
fn over_socket(socket: &Path, registry: Option<&str>, branch: Option<&str>) -> Result<SchemaDef> {
    let refused = |what: &str, err: &dyn std::fmt::Display| {
        BorgError::Storage(format!("{}: {what}: {err}", socket.display()))
    };
    let stream = UnixStream::connect(socket).map_err(|err| refused("cannot connect", &err))?;
    let mut reader = BufReader::new(
        stream
            .try_clone()
            .map_err(|err| refused("cannot read", &err))?,
    );
    let mut writer = stream;

    let _: ServerHello = borg_protocol::read_message(&mut reader, Codec::Json)
        .map_err(|err| refused("no hello", &err))?;
    // **No `client_version`.** Generation is asking what the schema *is* right now; stating a
    // version would be asking what it looked like to somebody else.
    let hello = ClientHello {
        version: borg_protocol::client::VERSION,
        client_version: None,
        codec: "json".to_string(),
        // From the store's lock record, so that a server hosting several registries is asked about
        // *this* one. Absent where the record predates named registries, which the server reads as
        // "the sole registry" — the same default a hand-written client takes (§17.6).
        registry: registry.map(str::to_string),
        // Nothing to present, and nothing checks it yet (§17.6).
        credential: None,
    };
    borg_protocol::write_message(&mut writer, Codec::Json, &hello)
        .map_err(|err| refused("cannot greet", &err))?;
    borg_protocol::write_message(
        &mut writer,
        Codec::Json,
        &Request::DefView {
            branch: branch.map(str::to_string),
        },
    )
    .map_err(|err| refused("cannot ask", &err))?;

    let response: Response = borg_protocol::read_message(&mut reader, Codec::Json)
        .map_err(|err| refused("no answer", &err))?;
    match response {
        Response::Defs(schema) => Ok(schema),
        Response::Error { message } => Err(BorgError::Storage(message)),
        other => Err(BorgError::Storage(format!(
            "{}: expected definitions, got {other:?}",
            socket.display()
        ))),
    }
}

// --- The emitter ------------------------------------------------------------------------------------

/// What a declared type becomes on the client: the TypeScript type of its values, and the
/// conversion that produces them.
///
/// **The one place this knowledge is duplicated**, and it is duplicated because the emitter is Rust
/// and the table is TypeScript (`packages/borg-sdk/src/values.ts`). The `ctor` column names the
/// exported function rather than restating its rules, so a change to how a `Double` is spelled
/// changes one file and this stays right. What would drift is a *new* declared type, and
/// `scenarios/260` compiles the output, which is what catches it.
struct Mapped {
    ts: String,
    ctor: String,
    /// The `borg-sdk/client` value import this needs.
    import: &'static str,
    /// Whether it also needs the `Ref` type import.
    reference: bool,
}

fn map_type(ty: &str) -> Mapped {
    let primitive = |ts: &str, ctor: &str, import: &'static str| Mapped {
        ts: ts.to_string(),
        ctor: ctor.to_string(),
        import,
        reference: false,
    };
    match ty {
        "Int" => primitive("number", "int()", "int"),
        "Double" => primitive("number", "double()", "double"),
        "Bool" => primitive("boolean", "bool()", "bool"),
        "String" => primitive("string", "string()", "string"),
        "Binary" => primitive("Uint8Array", "binary()", "binary"),
        "BigInt" => primitive("bigint", "bigint()", "bigint"),
        // A field the schema left open. Its text is its value, because nothing declared a shape —
        // guessing one would be the SDK inventing a contract the log does not have.
        "Any" | "AnyObject" | "AnyArray" | "AnyNumber" => {
            primitive("string", "untyped()", "untyped")
        }
        // Everything else names a struct, `Employee` or `Employee[]`. A reference's value is the
        // PID, branded with what it points at, so that `tx.object(Company, employeeRef)` does not
        // compile (SDK-DRAFT §5).
        other => Mapped {
            ts: format!("Ref<{}>", quoted(other)),
            ctor: format!("refText({})", quoted(other)),
            import: "refText",
            reference: true,
        },
    }
}

/// The whole module. Deterministic: same schema in, byte-identical file out.
pub fn module(schema: &SchemaDef) -> String {
    let mut structs = schema.structs.clone();
    structs.sort_by(|a, b| a.name.cmp(&b.name));

    let mut values: Vec<&str> = Vec::new();
    let mut reference = false;
    for object in &structs {
        for field in &object.fields {
            let mapped = map_type(&field.ty);
            if !values.contains(&mapped.import) {
                values.push(mapped.import);
            }
            reference |= mapped.reference;
        }
    }
    values.sort_unstable();

    let mut out = String::new();
    out.push_str(&header(&schema.version));
    out.push_str(&imports(&values, reference));
    out.push_str(&preamble(&schema.version));
    for object in &structs {
        out.push_str(&emit_struct(object));
    }
    out
}

fn header(version: &str) -> String {
    format!(
        r#"// Generated by `borg generate --lang ts`. Do not edit — regenerate.
//
// **This module is pinned to a schema.** `{version}` is the branch's def-version at the moment it
// was generated, and `createBorgContext` sends it as this client's ClientVersion on every connection
// (SPEC.md §5.4, SDK-DRAFT §2.4). The pin is what makes *not* regenerating a supported state rather
// than a broken one: values written in a newer shape reach this code through `down` migrations, and
// what it writes is stored in the shape it knows. Regenerating is how you adopt a new schema, and it
// is a decision rather than an obligation.
//
// Every field is nullable. A cell that was never written and a tombstoned one both read as `null`
// (§8.1), and no declaration can promise otherwise.

"#
    )
}

fn imports(values: &[&str], reference: bool) -> String {
    let mut lines = vec![
        "  createBorgContext as connect,".to_string(),
        "  defineStruct,".to_string(),
    ];
    for value in values {
        lines.push(format!("  {value},"));
    }
    lines.push("  type BorgContext,".into());
    lines.push("  type BorgContextOptions,".into());
    if reference {
        lines.push("  type Ref,".into());
    }
    lines.push("  type StructDescriptor,".into());
    format!(
        "import {{\n{}\n}} from \"borg-sdk/client\";\n\n",
        lines.join("\n")
    )
}

fn preamble(version: &str) -> String {
    format!(
        r#"/** The def-layer this module was generated from — its ClientVersion (SPEC.md §5.4). */
export const CLIENT_VERSION = "{version}";

/**
 * Connect to `borg serve`, as a client authored against `{version}`.
 *
 * Every option the SDK's own `createBorgContext` takes except the version, which is not an option
 * here: generation decided it.
 */
export function createBorgContext(
  options: Omit<BorgContextOptions, "clientVersion">,
): Promise<BorgContext> {{
  return connect({{ ...options, clientVersion: CLIENT_VERSION }});
}}
"#
    )
}

fn emit_struct(object: &StructDef) -> String {
    let name = &object.name;
    let mut out = format!(
        "\n// ── {name} {}\n\n",
        "─".repeat(72usize.saturating_sub(name.len() + 6))
    );

    out.push_str(&format!("export interface {name} {{\n"));
    for field in &object.fields {
        let mapped = map_type(&field.ty);
        out.push_str(&format!("  /** {} */\n", describe_field(field)));
        // `readonly` is the entire static marking of ownership, and it is one word so that the
        // generated file stays readable; `WritableKeys` in the SDK is what turns it into a compile
        // error on `set`. SPEC.md §15 deferred this "with the SDKs themselves".
        let modifier = if field.derived_by.is_some() {
            "readonly "
        } else {
            ""
        };
        out.push_str(&format!(
            "  {modifier}{}: {} | null;\n",
            member(&field.name),
            mapped.ts
        ));
    }
    out.push_str("}\n\n");

    // The name appears in the *type* as well as in the value, and it is not redundant: it is what
    // brands the ids `tx.create` and `branch.list` answer with, so a listed `Company` can be stored
    // in a `Ref<"Company">` field without a cast. `StructDescriptor`'s second parameter defaults to
    // `string`, so a hand-written descriptor that omits it still compiles and simply gets less.
    out.push_str(&format!(
        "/** The runtime half of {{@link {name}}}: what `tx.object({name}, id)` converts values with. */\n\
         export const {name}: StructDescriptor<{name}, {n}> = defineStruct(\"{name}\", {{\n",
        n = quoted(name)
    ));
    for field in &object.fields {
        out.push_str(&format!(
            "  {}: {{ type: {}, derived: {}, version: \"{}\" }},\n",
            member(&field.name),
            map_type(&field.ty).ctor,
            field.derived_by.is_some(),
            field.version
        ));
    }
    out.push_str("});\n");
    out
}

fn describe_field(field: &borg_protocol::client::FieldDef) -> String {
    match &field.derived_by {
        // Named by id, because an id is all the log holds — only the implementation table knows
        // what a human called it (§9.2), and that table is not a fact about the schema.
        Some(producer) => format!(
            "`{}` — derived by {producer}, so client writes are refused (§8). Repo {}, v{}.",
            field.ty, field.repo, field.version
        ),
        None => format!(
            "`{}` — source. Repo {}, v{}.",
            field.ty, field.repo, field.version
        ),
    }
}

/// A property name, quoted when it is not a bare JavaScript identifier.
///
/// Field names are used **verbatim** — `isInvestible` in the DSL is `isInvestible` here and
/// `Company#1.isInvestible` at the CLI (SDK-DRAFT §4.1). A name that JavaScript cannot spell bare is
/// quoted rather than mangled, because a mangling is a mapping somebody has to reverse-engineer from
/// an error.
fn member(name: &str) -> String {
    let identifier = !name.is_empty()
        && name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_' || c == '$')
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$');
    if identifier {
        name.to_string()
    } else {
        quoted(name)
    }
}

fn quoted(text: &str) -> String {
    format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use borg_protocol::client::FieldDef;

    fn field(name: &str, ty: &str, derived_by: Option<&str>) -> FieldDef {
        FieldDef {
            name: name.into(),
            ty: ty.into(),
            derived_by: derived_by.map(str::to_string),
            repo: 1,
            version: "L4".into(),
        }
    }

    /// The fixture behind the golden file. Deliberately covers one of everything the emitter has a
    /// branch for: a primitive, a derived field, a reference, a list and a name JavaScript cannot
    /// spell bare.
    fn fixture() -> SchemaDef {
        SchemaDef {
            version: "L4".into(),
            structs: vec![
                StructDef {
                    name: "Employee".into(),
                    fields: vec![field("name", "String", None)],
                },
                StructDef {
                    name: "Company".into(),
                    fields: vec![
                        field("employees", "Employee[]", None),
                        field("founded", "BigInt", None),
                        field("headcount", "Int", None),
                        field("isInvestible", "Bool", Some("P13897230598270219100")),
                        field("lead-investor", "Employee", None),
                        field("notes", "Any", None),
                        field("valuation", "Double", None),
                        field("website", "String", None),
                    ],
                },
            ],
        }
    }

    /// **The golden file is the one that is compiled**, not a copy of it.
    ///
    /// It lives in the TypeScript package's test tree, where `pnpm run typecheck` compiles it and a
    /// vitest case exercises the types it declares. A golden held in `crates/` would prove the
    /// emitter is stable and nothing about whether what it emits is valid TypeScript — which is the
    /// only property that matters.
    #[test]
    fn the_emitted_module_is_the_golden_file() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../packages/borg-sdk/test/generated/borg.generated.ts"
        );
        let got = module(&fixture());
        // `BORG_UPDATE_GOLDEN=1 cargo test -p borg-cli` rewrites it. A golden nobody can regenerate
        // is a golden that gets deleted the first time it is legitimately wrong.
        if std::env::var_os("BORG_UPDATE_GOLDEN").is_some() {
            std::fs::write(path, &got).unwrap();
        }
        assert_eq!(
            got,
            std::fs::read_to_string(path).unwrap(),
            "the generated module changed — rerun with BORG_UPDATE_GOLDEN=1 if that is intended, \
             and read the diff, because it is the diff a user's `git status` will show"
        );
    }

    /// Regenerating an unchanged schema must produce an unchanged file, or every diff carries noise
    /// and "did the schema move?" stops being answerable by looking.
    #[test]
    fn a_schema_that_did_not_move_emits_a_byte_identical_module() {
        let mut shuffled = fixture();
        shuffled.structs.reverse();
        assert_eq!(module(&fixture()), module(&shuffled));
    }

    /// The stamp is the reason codegen exists (§5.4). It has to be *in* the module, and it has to be
    /// what the module sends.
    #[test]
    fn the_module_bakes_in_the_def_layer_it_was_generated_at() {
        let source = module(&fixture());
        assert!(
            source.contains("export const CLIENT_VERSION = \"L4\";"),
            "{source}"
        );
        assert!(
            source.contains("clientVersion: CLIENT_VERSION"),
            "the generated createBorgContext must send the stamp, not merely hold it"
        );
    }

    /// Ownership is static now that it is declared (§8), so the generator marks it rather than
    /// leaving every field writable and waiting for a runtime rejection (SPEC.md §15).
    #[test]
    fn a_derived_field_is_emitted_readonly_and_a_source_field_is_not() {
        let source = module(&fixture());
        assert!(
            source.contains("  readonly isInvestible: boolean | null;"),
            "{source}"
        );
        assert!(source.contains("  headcount: number | null;"), "{source}");
        assert!(!source.contains("readonly headcount"), "{source}");
    }

    /// A reference carries what it points at in its *type*, which is what makes passing an
    /// `Employee` reference to `tx.object(Company, …)` a compile error (SDK-DRAFT §5).
    #[test]
    fn a_reference_field_is_branded_with_the_struct_it_names() {
        let source = module(&fixture());
        assert!(source.contains(r#"Ref<"Employee"> | null"#), "{source}");
        assert!(
            source.contains(r#"Ref<"Employee[]"> | null"#),
            "a list is a reference to the list"
        );
        assert!(source.contains(r#"refText("Employee")"#), "{source}");
    }

    /// A descriptor states its own name **in its type**, which is what lets the ids `list` and
    /// `create` answer with be branded with the struct they belong to rather than being bare
    /// strings (SDK-DRAFT §4.5). Without it a listed `Employee` could not be stored in an
    /// `Employee` reference field without a cast.
    #[test]
    fn a_descriptor_carries_its_struct_name_as_a_literal_type() {
        let source = module(&fixture());
        assert!(
            source.contains(r#"export const Employee: StructDescriptor<Employee, "Employee"> ="#),
            "{source}"
        );
    }

    /// Names are verbatim, and one JavaScript cannot spell bare is quoted rather than mangled.
    #[test]
    fn a_field_name_that_is_not_an_identifier_is_quoted_not_renamed() {
        let source = module(&fixture());
        assert!(
            source.contains(r#"  "lead-investor": Ref<"Employee"> | null;"#),
            "{source}"
        );
        assert!(
            !source.contains("leadInvestor"),
            "a rename is a mapping nobody can reverse"
        );
    }

    /// A declared type nobody taught the table about is a *struct*, not an error: the log's type
    /// vocabulary is open (`ValueType::Object` takes any name), so the fallback has to be the one
    /// that is right for a name and not the one that stops the build.
    #[test]
    fn an_unrecognised_declared_type_is_read_as_a_struct_reference() {
        assert_eq!(map_type("Int").ts, "number");
        assert_eq!(map_type("Employee").ts, r#"Ref<"Employee">"#);
        assert_eq!(map_type("Wombat").ts, r#"Ref<"Wombat">"#);
        assert_eq!(map_type("Any").ctor, "untyped()");
    }
}
