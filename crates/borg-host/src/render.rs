//! The renderings that more than one front end needs.
//!
//! Most rendering belongs to whoever is doing it — the CLI prints lines, the server writes
//! `Response`s — and lives there. This holds the exceptions: shapes that go out over the wire *and*
//! are produced without a wire, where two implementations would be a bug that only shows up on one
//! of the two paths.

use borg_protocol::client::{FieldDef, StructDef};

/// A struct definition as the wire carries it.
///
/// One renderer for `def_show`, `def_view` and `borg generate`'s direct-store path, because a struct
/// is a struct — and codegen reading a different shape depending on whether it went through a socket
/// would be the one bug that only shows up on a served store.
#[must_use]
pub fn struct_def(object: &borg_core::ObjectDef) -> StructDef {
    StructDef {
        name: object.name.to_string(),
        fields: object
            .fields
            .values()
            .map(|def| FieldDef {
                name: def.name.to_string(),
                ty: def.ty.to_string(),
                // By id, because an id is all the log holds — only the implementation table knows
                // what a human called it (§9.2).
                derived_by: def.ownership.producer().map(|p| p.to_string()),
                repo: def.declaring_repo.0,
                version: def.version.to_string(),
            })
            .collect(),
    }
}
