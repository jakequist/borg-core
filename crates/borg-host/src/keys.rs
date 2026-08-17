//! **Static API keys: what a server checks a `ClientHello`'s credential against.** SPEC.md §17.6.
//!
//! The smallest thing that puts a `borg-server` on the internet, shaped so that the platform's
//! signed tokens replace it without a wire change. `ClientHello::credential` was reserved two
//! milestones ago and nothing checked it; this is the thing that checks it, and the field did not
//! move.
//!
//! ## No keys file means an open server
//!
//! `borg-server start` on a laptop authenticates nobody, because there is nobody to authenticate: a
//! unix socket's file permissions are the boundary and always were. The *first* `keygen` writes the
//! file, and the file's existence is what flips the server to enforcing. That is deliberately the
//! one bit of configuration there is — no flag, no environment variable, no "auth: on" in a config
//! nobody would find. An operator asks `borg-server status`, which says which mode it is in.
//!
//! ## …but an *unreadable* keys file does not
//!
//! This is the one file beside a store that is **not** a [`crate::sidecar::Sidecar`], and the
//! difference is the whole reason it is written out here. A sidecar's rule is that a missing or
//! corrupt file reads as the default, and it is justified by "a sidecar holds nothing that cannot be
//! recreated by doing the thing again". A keys file holds exactly that, in the direction that
//! matters: if an unparsable file read as *no keys*, then truncating it — a full disk, a half-written
//! copy, a `>` in the wrong shell — would silently turn authentication off. So a file that is there
//! and cannot be read is a **refusal**, and one that is not there at all is an open server. Absent
//! and broken are different answers here, and everywhere else in this crate they are the same one.
//!
//! ## The keys are hashed, and only hashed
//!
//! An entry is `sha256:<hex>` of the key's bytes, a label, and a scope. Plaintext exists in exactly
//! one place — the line `borg-server keygen` prints — and nowhere else, ever: not in the file, not
//! in `status`, not in a log, and not in an error message. [`ConnectionUrl`][url] redacts a
//! credential out of the url it quotes back for the same reason.
//!
//! The lookup compares hashes rather than secrets, which is what makes a non-constant-time
//! comparison sound: an attacker supplies a key and cannot iterate over hash prefixes without
//! inverting SHA-256, so there is no timing oracle to walk.
//!
//! ## Scope is a list of registry names, or `*`
//!
//! One server is one org (`ROADMAP.md`, *The production arc*), so most keys are `*` and scoping is
//! for the deploy key that should only reach staging. What a scope buys beyond refusing a
//! connection is **that a credential cannot see what it cannot reach**: `registries` is filtered to
//! the scope, and so is the list of options in a routing refusal. An unauthenticated caller learns
//! no registry name it did not itself supply; an authenticated one learns exactly its own scope.
//!
//! ## The admin token, and why the unix socket is not exempt
//!
//! `borg-server status`, `create`, `export` and `import` are clients of the server they administer —
//! they speak §17.5 over the socket. Enforcement would lock them out, and the tempting fix is to
//! exempt the unix transport. That is refused here: an exemption would make unix and WebSocket
//! semantically different, so every claim about authentication would grow "…over the network", and
//! the local case — the one everybody develops against — would be the one nothing tests.
//!
//! So instead a server **mints a token at boot** into [`admin_path`], mode `0600`, and removes it on
//! the way out beside the socket and the advisory locks. It is scoped `*`, it is presented by the
//! lifecycle commands like any other credential, and it is checked by the same code on the same
//! field. The boundary it draws is the filesystem's: whoever can read the data directory can read
//! this, and could already read the stores themselves. `$BORG_TOKEN` overrides it, which is what a
//! lifecycle command run against a server on another machine needs.
//!
//! [url]: borg_protocol::url::ConnectionUrl

use borg_core::{BorgError, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// The keys file, in the data directory beside the registries.
///
/// Named like the server's other files (`borg-server.pid`, `borg-server.log`) rather than like a
/// sidecar, because it belongs to the *server* and not to any one store — and because a dot in the
/// name is what keeps `Host::open` from mistaking it for a registry (`crate::host::name_is_valid`).
pub const KEYS_FILE: &str = "borg-server.keys.json";

/// Where a running server leaves the credential its own CLI clients use. See the module header.
pub const ADMIN_FILE: &str = "borg-server.admin";

/// The environment variable a client presents a key through when the url does not carry one.
///
/// The same name on every client — the CLI, `borg-server`'s lifecycle commands and the TypeScript
/// SDK — because a token that had three spellings would be configured in the wrong one.
pub const TOKEN_ENV: &str = "BORG_TOKEN";

/// What a key may reach.
///
/// `*` or a list of registry names, and nothing in between — no globs, no negations. A scope is
/// read by whoever is deciding whether to keep a deployment's key, and a pattern language is how
/// that stops being answerable by looking.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Scope {
    /// Written `"*"`. Every registry this server hosts, including ones created later.
    All(Star),
    /// Written `["crm", "analytics"]`. A name that is not hosted is not an error — a key may
    /// legitimately be written before the registry it is for.
    Only(Vec<String>),
}

/// The literal `"*"`, as a type, so that `Scope`'s untagged serde cannot read any other string as
/// unrestricted access.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Star;

impl Serialize for Star {
    fn serialize<S: serde::Serializer>(&self, out: S) -> std::result::Result<S::Ok, S::Error> {
        out.serialize_str("*")
    }
}

impl<'de> Deserialize<'de> for Star {
    fn deserialize<D: serde::Deserializer<'de>>(input: D) -> std::result::Result<Self, D::Error> {
        let text = String::deserialize(input)?;
        if text == "*" {
            return Ok(Self);
        }
        Err(serde::de::Error::custom(format!(
            "`{text}` is not a scope — write \"*\" or a list of registry names"
        )))
    }
}

impl Scope {
    /// Unrestricted. What a key gets when `--registries` is not given, and what the admin token has.
    #[must_use]
    pub fn all() -> Self {
        Self::All(Star)
    }

    /// Whether this scope reaches a registry by that name.
    #[must_use]
    pub fn allows(&self, name: &str) -> bool {
        match self {
            Self::All(_) => true,
            Self::Only(names) => names.iter().any(|allowed| allowed == name),
        }
    }

    /// What of `names` this scope can see. The filter applied to `registries` and to the options in
    /// a routing refusal — see the module header.
    #[must_use]
    pub fn visible(&self, names: Vec<String>) -> Vec<String> {
        match self {
            Self::All(_) => names,
            Self::Only(_) => names.into_iter().filter(|name| self.allows(name)).collect(),
        }
    }

    /// How `keys list` prints it.
    #[must_use]
    pub fn written(&self) -> String {
        match self {
            Self::All(_) => "*".to_string(),
            Self::Only(names) => names.join(","),
        }
    }
}

/// One key, as the file holds it. **The key itself is not here** — only its digest.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Key {
    /// What an operator calls it: `ci`, `staging-web`. The handle `keys revoke` takes, so it is
    /// unique — issuing a second key under a live label would make revocation ambiguous.
    pub label: String,
    /// `sha256:<hex>` of the key's bytes, in `borg_protocol::fingerprint`'s form — one tagged-digest
    /// spelling in the workspace, so a string says what produced it.
    pub hash: String,
    pub registries: Scope,
    /// Unix seconds, for `keys list`. Nothing decides anything on it: a static key does not expire,
    /// and a key with an expiry would be the platform's signed token wearing a worse disguise.
    #[serde(default)]
    pub created: u64,
}

/// The whole file.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Keys {
    pub keys: Vec<Key>,
}

impl Keys {
    /// The entry a presented key matches, by digest. See the module header for why comparing digests
    /// rather than secrets is what makes this comparison's timing uninteresting.
    #[must_use]
    pub fn matching(&self, presented: &str) -> Option<&Key> {
        let digest = digest(presented);
        self.keys.iter().find(|key| key.hash == digest)
    }

    #[must_use]
    pub fn labelled(&self, label: &str) -> Option<&Key> {
        self.keys.iter().find(|key| key.label == label)
    }
}

#[must_use]
pub fn keys_path(data_dir: &Path) -> PathBuf {
    data_dir.join(KEYS_FILE)
}

#[must_use]
pub fn admin_path(data_dir: &Path) -> PathBuf {
    data_dir.join(ADMIN_FILE)
}

/// The keys this server enforces, or `None` when there is no file and the server is open.
///
/// **A file that exists and cannot be read is an error**, never an empty set. See the module header:
/// this is the one place in the crate where absent and corrupt are different answers, and the reason
/// is that reading corrupt as absent would turn authentication off by accident.
pub fn load(data_dir: &Path) -> Result<Option<Keys>> {
    let path = keys_path(data_dir);
    let raw = match std::fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(BorgError::Storage(format!(
                "{}: {err} — this server has a keys file it cannot read, and refuses connections \
                 rather than serving them unauthenticated",
                path.display()
            )));
        }
    };
    serde_json::from_str(&raw).map(Some).map_err(|err| {
        BorgError::Storage(format!(
            "{}: {err} — this server has a keys file it cannot parse, and refuses connections \
             rather than serving them unauthenticated",
            path.display()
        ))
    })
}

/// Write the keys file back, atomically and `0600`.
///
/// Atomically because the server re-reads this file on **every** handshake (see [`authorize`]), so a
/// reader that caught a half-written file would refuse every connection for as long as the write
/// took. A rename is what makes the two states the only two states.
pub fn save(data_dir: &Path, keys: &Keys) -> Result<()> {
    let path = keys_path(data_dir);
    let raw =
        serde_json::to_string_pretty(keys).map_err(|err| BorgError::Storage(err.to_string()))?;
    write_private(&path, &format!("{raw}\n"))
}

/// A secret written where only its owner can read it, and put in place in one step.
fn write_private(path: &Path, contents: &str) -> Result<()> {
    let named = |err: std::io::Error| BorgError::Storage(format!("{}: {err}", path.display()));
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(named)?;
    }
    let staged = path.with_extension("tmp");
    std::fs::write(&staged, contents).map_err(named)?;
    // Before the rename, so the file is never briefly world-readable at its real name.
    let mut mode = std::fs::metadata(&staged).map_err(named)?.permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut mode, 0o600);
    std::fs::set_permissions(&staged, mode).map_err(named)?;
    std::fs::rename(&staged, path).map_err(named)
}

/// **A fresh key, and the only time it exists in plaintext.** See [`digest`] for what is stored.
///
/// `borgk_` and 256 bits of `/dev/urandom`, base64url so that it needs no escaping in a connection
/// url, in an environment variable or in a shell. The prefix is for a human reading a config file
/// and for a secret scanner: a string that says what it is is a string somebody notices in a diff.
///
/// `/dev/urandom` rather than a crate, because the workspace is deliberately thin and this is the
/// one random number it needs — read directly, so there is no generator to seed, no state to fork
/// and nothing to get wrong at process start.
pub fn generate() -> Result<String> {
    use std::io::Read;
    let mut source = std::fs::File::open("/dev/urandom").map_err(|err| {
        BorgError::Storage(format!(
            "/dev/urandom: {err} — a key cannot be generated without randomness, and this refuses \
             rather than inventing some"
        ))
    })?;
    let mut bytes = [0u8; 32];
    source.read_exact(&mut bytes).map_err(|err| {
        BorgError::Storage(format!("/dev/urandom: {err} — a short read of randomness"))
    })?;
    Ok(format!("borgk_{}", base64url(&bytes)))
}

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// Unpadded base64url. Written out rather than taken as a dependency: it is twelve lines, it is used
/// once, and the alphabet is the point — `-` and `_` are exactly the characters a registry name may
/// hold, so a key never needs escaping anywhere a url goes.
fn base64url(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let packed = chunk.iter().enumerate().fold(0u32, |packed, (at, byte)| {
            packed | (u32::from(*byte) << (16 - 8 * at))
        });
        for at in 0..=chunk.len() {
            out.push(char::from(
                ALPHABET[((packed >> (18 - 6 * at)) & 0x3f) as usize],
            ));
        }
    }
    out
}

/// What is stored for a key: `sha256:<hex>`, in the workspace's one tagged-digest form.
#[must_use]
pub fn digest(key: &str) -> String {
    borg_protocol::fingerprint(key.as_bytes())
}

/// Mint the local admin credential for a server that is starting. See the module header.
pub fn mint_admin(data_dir: &Path) -> Result<String> {
    let token = generate()?;
    write_private(&admin_path(data_dir), &token)?;
    Ok(token)
}

/// The admin credential a running server left here, if this process may read it.
///
/// `None` covers every failure — no server, no permission, no file — because all of them mean the
/// same thing to a caller: there is no local admin path, so present `$BORG_TOKEN` or nothing and
/// let the server say what it thinks.
#[must_use]
pub fn read_admin(data_dir: &Path) -> Option<String> {
    std::fs::read_to_string(admin_path(data_dir))
        .ok()
        .map(|raw| raw.trim().to_string())
        .filter(|token| !token.is_empty())
}

/// Remove it, on the way out. Best-effort, like the socket and the advisory locks: a `kill -9`
/// leaves one behind and the next `start` overwrites it.
pub fn clear_admin(data_dir: &Path) {
    let _ = std::fs::remove_file(admin_path(data_dir));
}

/// **Why a handshake was refused, in the words a stranger should read.** SPEC.md §17.6.
///
/// A newtype over prose rather than a [`BorgError`], and the distinction is load-bearing twice
/// over. A `BorgError`'s `Display` carries a category prefix meant for an operator at a terminal,
/// and the client that receives a refusal wraps it in one *again* — so the sentence a rejected
/// caller would read is `storage: storage: that credential is not valid`. And the type is what says
/// these particular strings are read by somebody who has **not** authenticated, which is the whole
/// reason they name no registry, no path and no key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refusal(pub String);

impl std::fmt::Display for Refusal {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.write_str(&self.0)
    }
}

fn refused(why: &str) -> Refusal {
    Refusal(why.to_string())
}

/// **What a client presenting this credential may do.** The answer [`authorize`] gives.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Access {
    /// No keys file: this server authenticates nobody, and every connection reaches everything.
    Open,
    /// A key, and what it may reach.
    Granted { label: String, scope: Scope },
}

impl Access {
    /// The scope to route, filter and check within. Open is unrestricted, which is what makes every
    /// scenario that predates authentication carry on unchanged.
    #[must_use]
    pub fn scope(&self) -> Scope {
        match self {
            Self::Open => Scope::all(),
            Self::Granted { scope, .. } => scope.clone(),
        }
    }

    /// What `HelloAck` reports and `borg-server status` prints: `open` or `required`.
    #[must_use]
    pub fn mode(&self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Granted { .. } => "required",
        }
    }
}

/// **Whether this credential gets in.** The one place the rule lives, for both transports.
///
/// Read fresh every time, because that is what makes revocation take effect immediately and because
/// the file is written by a *different process* — `borg-server keygen` runs beside a live server, it
/// does not speak to it. The file is one small read and the alternative is a cache with no
/// invalidation, which is the thing the server's sidecar audit exists to refuse.
///
/// **The refusals disclose nothing.** A caller that cannot authenticate learns that a credential is
/// required, or that the one it holds is not valid, and never which registries exist — the routing
/// refusal that names them happens afterwards and only inside a credential's own scope.
pub fn authorize(data_dir: &Path, presented: Option<&str>) -> std::result::Result<Access, Refusal> {
    // **The detail stays on this side.** A caller that has not authenticated is told that the
    // server cannot read its credential store and nothing else — not the path, not the parse error,
    // both of which are facts about a filesystem a stranger has no business learning. The operator's
    // copy of that error is on `borg-server start`, which refuses to boot on it, and in
    // `borg-server keys`, which prints it in full.
    let file = load(data_dir).map_err(|_| {
        refused(
            "this server cannot read its credential store, and refuses every connection until an \
             operator has fixed it",
        )
    })?;
    let Some(keys) = file else {
        return Ok(Access::Open);
    };
    let Some(presented) = presented.filter(|key| !key.is_empty()) else {
        return Err(refused(
            "this server requires a credential — put a key in the url as \
             borg://:<key>@host/<registry>, or set $BORG_TOKEN",
        ));
    };
    // The admin token first, and it is not in the keys file: it is minted per boot, lives beside the
    // pidfile, and exists so that `borg-server status` can ask a server it administers without the
    // unix socket being exempt from the rule. See the module header.
    if read_admin(data_dir).is_some_and(|admin| digest(&admin) == digest(presented)) {
        return Ok(Access::Granted {
            label: "<local admin>".to_string(),
            scope: Scope::all(),
        });
    }
    let Some(key) = keys.matching(presented) else {
        // Deliberately the same sentence for a key that was never issued and one that was revoked.
        // Which of the two it is, is a fact about this server's key list, and the caller has just
        // failed to prove it may know anything about that.
        return Err(refused("that credential is not valid for this server"));
    };
    Ok(Access::Granted {
        label: key.label.clone(),
        scope: key.registries.clone(),
    })
}

/// **A registry this credential may not reach, refused without saying whether it exists.**
///
/// Separate from [`authorize`] because it needs the name the handshake asked for, and it comes
/// *before* routing for the reason the module header gives: routing's refusal names what is hosted,
/// and a key scoped to `staging` must not be able to enumerate production by guessing.
pub fn permit(scope: &Scope, registry: &str) -> std::result::Result<(), Refusal> {
    if scope.allows(registry) {
        return Ok(());
    }
    Err(Refusal(format!(
        "that credential is not valid for registry `{registry}`"
    )))
}

/// `borg-server keygen`: issue a key, store its digest, hand back the plaintext **once**.
///
/// The key is returned rather than printed, because printing is the caller's — the same rule every
/// operation in this crate follows. It is the only time the plaintext exists.
pub fn issue(data_dir: &Path, label: &str, scope: Scope) -> Result<String> {
    check_label(label)?;
    let mut keys = load(data_dir)?.unwrap_or_default();
    if keys.labelled(label).is_some() {
        return Err(BorgError::Storage(format!(
            "a key labelled `{label}` already exists — revoke it first, or use another label; two \
             live keys under one label would make `keys revoke {label}` ambiguous"
        )));
    }
    let key = generate()?;
    keys.keys.push(Key {
        label: label.to_string(),
        hash: digest(&key),
        registries: scope,
        created: now(),
    });
    save(data_dir, &keys)?;
    Ok(key)
}

/// `borg-server keys revoke`. Takes effect for the **next handshake**; see [`authorize`].
pub fn revoke(data_dir: &Path, label: &str) -> Result<()> {
    let Some(mut keys) = load(data_dir)? else {
        return Err(BorgError::Storage(format!(
            "there are no keys to revoke — {} does not exist, and this server is open",
            keys_path(data_dir).display()
        )));
    };
    let before = keys.keys.len();
    keys.keys.retain(|key| key.label != label);
    if keys.keys.len() == before {
        return Err(BorgError::Storage(format!("no key labelled `{label}`")));
    }
    // **The file stays, even at zero keys**, and that is the whole difference between revoking the
    // last key and deleting the file. A server whose keys were all revoked is a server nobody can
    // reach, which is a locked door; one whose file is gone is open to everybody, which is an open
    // door. Removing it here would turn the strictest possible operation into the loosest.
    save(data_dir, &keys)
}

fn check_label(label: &str) -> Result<()> {
    let usable = !label.is_empty()
        && label
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if usable {
        return Ok(());
    }
    Err(BorgError::Storage(format!(
        "`{label}` is not a key label — letters, digits, `-` and `_`"
    )))
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

/// How long ago something happened, coarsely. `keys list` is read to answer "is this the key we
/// issued last week?", which needs no precision and no date library.
#[must_use]
pub fn ago(created: u64) -> String {
    if created == 0 {
        return "unknown".to_string();
    }
    let seconds = now().saturating_sub(created);
    match seconds {
        0..=59 => "just now".to_string(),
        60..=3599 => format!("{}m ago", seconds / 60),
        3600..=86_399 => format!("{}h ago", seconds / 3600),
        _ => format!("{}d ago", seconds / 86_400),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "borg-keys-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// **The zero-ceremony case, and it is the one that must not regress.** A laptop's server has no
    /// keys file, so every connection is open — which is what keeps every scenario written before
    /// authentication existed passing unchanged.
    #[test]
    fn a_server_with_no_keys_file_is_open_to_everybody() {
        let dir = temp_dir("open");
        assert_eq!(authorize(&dir, None).unwrap(), Access::Open);
        assert_eq!(authorize(&dir, Some("anything")).unwrap(), Access::Open);
        assert_eq!(Access::Open.mode(), "open");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// **The first key flips the server**, and the plaintext is answered once and stored never.
    #[test]
    fn the_first_key_flips_the_server_to_enforcing_and_is_never_stored_in_the_clear() {
        let dir = temp_dir("first");
        let key = issue(&dir, "ci", Scope::all()).unwrap();
        assert!(key.starts_with("borgk_"), "{key}");

        let raw = std::fs::read_to_string(keys_path(&dir)).unwrap();
        assert!(
            !raw.contains(&key),
            "the plaintext key must never reach the file: {raw}"
        );
        assert!(raw.contains(&digest(&key)), "{raw}");

        let Access::Granted { label, scope } = authorize(&dir, Some(&key)).unwrap() else {
            panic!("an issued key must get in")
        };
        assert_eq!(label, "ci");
        assert_eq!(scope, Scope::all());

        // …and the same server now refuses a caller with nothing.
        let refusal = authorize(&dir, None).unwrap_err().to_string();
        assert!(refusal.contains("requires a credential"), "{refusal}");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Two keys are two keys: one revoked leaves the other working, and the revoked one is refused
    /// in the same words as a key that never existed.
    #[test]
    fn a_revoked_key_is_refused_in_the_same_words_as_one_that_never_existed() {
        let dir = temp_dir("revoke");
        let ci = issue(&dir, "ci", Scope::all()).unwrap();
        let web = issue(&dir, "web", Scope::all()).unwrap();

        revoke(&dir, "ci").unwrap();
        let revoked = authorize(&dir, Some(&ci)).unwrap_err().to_string();
        let never = authorize(&dir, Some("borgk_nope")).unwrap_err().to_string();
        assert_eq!(
            revoked, never,
            "distinguishing the two would answer a question about the key list to somebody who \
             just failed to prove they may ask one"
        );
        assert!(authorize(&dir, Some(&web)).is_ok(), "the other key stands");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// **Revoking the last key leaves a locked door, not an open one.** The file stays at zero keys,
    /// because deleting it is the loosest operation there is and revocation is the strictest.
    #[test]
    fn revoking_the_last_key_leaves_the_server_enforcing() {
        let dir = temp_dir("last");
        let only = issue(&dir, "only", Scope::all()).unwrap();
        revoke(&dir, "only").unwrap();

        assert!(keys_path(&dir).is_file(), "the file must survive");
        assert!(authorize(&dir, Some(&only)).is_err());
        let refusal = authorize(&dir, None).unwrap_err().to_string();
        assert!(refusal.contains("requires a credential"), "{refusal}");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A scope is a list of registries, and what it buys is that a key cannot reach — or see —
    /// anything else.
    #[test]
    fn a_scoped_key_reaches_its_registries_and_no_others() {
        let dir = temp_dir("scope");
        let key = issue(&dir, "staging", Scope::Only(vec!["staging".into()])).unwrap();
        let access = authorize(&dir, Some(&key)).unwrap();

        permit(&access.scope(), "staging").unwrap();
        let refusal = permit(&access.scope(), "production")
            .unwrap_err()
            .to_string();
        assert!(refusal.contains("`production`"), "{refusal}");
        assert!(
            !refusal.contains("staging"),
            "a refusal must not name what else exists or what else the key reaches: {refusal}"
        );

        assert_eq!(
            access
                .scope()
                .visible(vec!["production".into(), "staging".into()]),
            vec!["staging".to_string()],
            "a scoped credential sees only its own registries"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// **A keys file that cannot be read is a refusal, not an open server.** The one place in this
    /// crate where corrupt and absent are different answers — see the module header.
    #[test]
    fn a_corrupt_keys_file_refuses_rather_than_opening_the_server() {
        let dir = temp_dir("corrupt");
        issue(&dir, "ci", Scope::all()).unwrap();
        std::fs::write(keys_path(&dir), "{\"keys\": [truncated").unwrap();

        let err = authorize(&dir, Some("borgk_whatever"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("cannot read its credential store"), "{err}");
        assert!(
            !err.contains(&dir.display().to_string()),
            "a caller that has not authenticated learns no filesystem path: {err}"
        );
        assert!(
            load(&dir).is_err(),
            "a half-written file must never read as no keys"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// The local admin path: minted at boot, `*`-scoped, and checked on the same field by the same
    /// code as any other credential — which is what keeps the unix socket from being exempt.
    #[test]
    fn the_admin_token_gets_in_without_being_in_the_keys_file() {
        let dir = temp_dir("admin");
        issue(&dir, "ci", Scope::Only(vec!["crm".into()])).unwrap();
        let admin = mint_admin(&dir).unwrap();

        let raw = std::fs::read_to_string(keys_path(&dir)).unwrap();
        assert!(!raw.contains(&admin), "the admin token is not a stored key");

        let Access::Granted { label, scope } = authorize(&dir, Some(&admin)).unwrap() else {
            panic!("the admin token must get in")
        };
        assert_eq!(label, "<local admin>");
        assert_eq!(scope, Scope::all(), "administration is unscoped");
        assert_eq!(read_admin(&dir).as_deref(), Some(admin.as_str()));

        clear_admin(&dir);
        assert!(read_admin(&dir).is_none());
        assert!(
            authorize(&dir, Some(&admin)).is_err(),
            "a token whose server has stopped is not a key"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Both secrets are `0600`, because a data directory is not always private and a credential a
    /// second user can read is not a credential.
    #[test]
    fn the_files_holding_secrets_are_readable_only_by_their_owner() {
        let dir = temp_dir("modes");
        issue(&dir, "ci", Scope::all()).unwrap();
        mint_admin(&dir).unwrap();
        for path in [keys_path(&dir), admin_path(&dir)] {
            let mode = std::os::unix::fs::PermissionsExt::mode(
                &std::fs::metadata(&path).unwrap().permissions(),
            );
            assert_eq!(
                mode & 0o077,
                0,
                "{} is group- or world-readable",
                path.display()
            );
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A key needs no escaping in a url, an environment variable or a shell — which is the whole
    /// reason the alphabet is base64url and not base64.
    #[test]
    fn a_generated_key_is_url_safe_and_does_not_repeat() {
        let first = generate().unwrap();
        let second = generate().unwrap();
        assert_ne!(first, second);
        assert_eq!(first.len(), "borgk_".len() + 43, "{first}");
        assert!(
            first
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
            "{first}"
        );
    }

    /// The scope's two spellings, and nothing else. A string that is not `"*"` is a mistake and is
    /// refused rather than read as some third meaning.
    #[test]
    fn a_scope_is_a_star_or_a_list_and_nothing_else() {
        let all: Scope = serde_json::from_str(r#""*""#).unwrap();
        assert_eq!(all, Scope::all());
        assert_eq!(serde_json::to_string(&all).unwrap(), r#""*""#);

        let some: Scope = serde_json::from_str(r#"["crm","analytics"]"#).unwrap();
        assert!(some.allows("crm") && !some.allows("other"));

        assert!(
            serde_json::from_str::<Scope>(r#""all""#).is_err(),
            "only `*` means everything"
        );
    }

    /// Two live keys under one label would make revocation ambiguous, so the second is refused.
    #[test]
    fn a_label_may_name_only_one_live_key() {
        let dir = temp_dir("label");
        issue(&dir, "ci", Scope::all()).unwrap();
        let again = issue(&dir, "ci", Scope::all()).unwrap_err().to_string();
        assert!(again.contains("already exists"), "{again}");

        // …and it is reusable once revoked, which is what makes rotation `revoke` then `keygen`.
        revoke(&dir, "ci").unwrap();
        assert!(issue(&dir, "ci", Scope::all()).is_ok());

        assert!(issue(&dir, "has space", Scope::all()).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
