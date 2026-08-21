// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// Derived in part from MinIO (https://github.com/minio/minio), AGPL-3.0.
// SPDX-License-Identifier: AGPL-3.0-only

//! Mapping between S3 object keys and on-disk relative paths.
//!
//! An S3 key is an arbitrary UTF-8 string. A filesystem path is not: separators,
//! `.` and `..`, control bytes and (on Windows) `\` all mean something. This
//! module defines a total, reversible encoding from keys to paths so that any
//! key the S3 API accepts can be stored, and the original key recovered byte for
//! byte.
//!
//! # Encoding rules
//!
//! 1. Split the key on `/`. Each component is encoded independently and the
//!    encoded components are joined with the platform separator.
//! 2. Inside a component, these bytes are percent-encoded with uppercase hex
//!    (`%XX`): `%` itself, the C0 control bytes `0x00` through `0x1F`, `0x7F`,
//!    and `\`. Everything else, including non-ASCII UTF-8, is left alone.
//! 3. Three components are special-cased whole, because the generic rule would
//!    leave them meaningful to the filesystem:
//!    - `.` encodes to `%2E`
//!    - `..` encodes to `%2E%2E`
//!    - the empty component encodes to `%2F`. An encoded `/` can never come out
//!      of rule 2 (a real component cannot contain `/`, that is what we split
//!      on), so `%2F` unambiguously marks "empty" and the decoder maps it back.
//! 4. A component starting with [`RESERVED_PREFIX`] has its first byte encoded
//!    (`_` becomes `%5F`). Object directories hold bookkeeping files named
//!    `__aks3.*` ([`META_FILE`], [`data_file_name`]); this rule guarantees no
//!    user key can ever encode to one of those names.
//!
//! Decoding reverses the rules. Malformed escapes yield [`PathError::BadEscape`]
//! and bytes that do not reassemble into UTF-8 yield [`PathError::NonUtf8`]. A
//! name that decodes to anything else containing a `/` is rejected rather than
//! spliced into the key, since encoding never produces one.
//!
//! # Version IDs
//!
//! Version IDs also reach the filesystem, via [`data_file_name`], and arrive
//! from the client as `?versionId=`. They get their own narrower escaping: any
//! byte outside `[A-Za-z0-9._-]` is percent-escaped, so the name is always a
//! single component no matter what the client sent. [`is_valid_version_id`]
//! reports whether an ID is well-formed, for callers that would rather reject
//! it at the API edge.

use std::path::{Component, Path, PathBuf};

/// Name of the per-object metadata file inside an object directory.
pub const META_FILE: &str = "__aks3.meta.json";

/// Component prefix reserved for aks3 bookkeeping files.
///
/// Encoding escapes the first byte of any component starting with this, so a
/// user key can never collide with a reserved name.
pub const RESERVED_PREFIX: &str = "__aks3";

/// Errors produced when decoding an on-disk path back into an S3 key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PathError {
    /// A `%` escape was truncated or contained a non-hex digit, or the path
    /// held a component that encoding never produces (root, `.`, `..`, a
    /// Windows drive prefix).
    #[error("malformed percent-escape in encoded path component")]
    BadEscape,
    /// The decoded bytes are not valid UTF-8, so they cannot be an S3 key.
    #[error("encoded path component does not decode to valid UTF-8")]
    NonUtf8,
}

/// Encode one S3 key component (a `/`-free slice of a key) as a file name.
#[must_use]
pub fn encode_component(c: &str) -> String {
    match c {
        "" => return "%2F".to_owned(),
        "." => return "%2E".to_owned(),
        ".." => return "%2E%2E".to_owned(),
        _ => {}
    }

    // Rule 4: escape the leading `_` of a reserved-prefix component. Slicing at
    // 1 is safe because the prefix starts with the ASCII byte `_`.
    let (mut out, rest) = if c.starts_with(RESERVED_PREFIX) {
        (String::from("%5F"), &c[1..])
    } else {
        (String::new(), c)
    };
    out.reserve(rest.len());

    for ch in rest.chars() {
        // Every byte rule 2 escapes is ASCII, and a non-ASCII char's UTF-8
        // bytes are all >= 0x80, so working per char matches working per byte.
        if ch.is_ascii() && needs_escape(ch as u8) {
            push_escape(&mut out, ch as u8);
        } else {
            out.push(ch);
        }
    }
    out
}

/// Decode one encoded file name back into an S3 key component.
///
/// # Errors
///
/// [`PathError::BadEscape`] if a `%` escape is truncated or not hex, or if the
/// component decodes to something containing a `/` other than the lone `/` that
/// marks an empty component; [`PathError::NonUtf8`] if the decoded bytes are not
/// valid UTF-8.
pub fn decode_component(c: &str) -> Result<String, PathError> {
    let bytes = c.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return Err(PathError::BadEscape);
            }
            let hi = hex_val(bytes[i + 1]).ok_or(PathError::BadEscape)?;
            let lo = hex_val(bytes[i + 2]).ok_or(PathError::BadEscape)?;
            out.push((hi << 4) | lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    let decoded = String::from_utf8(out).map_err(|_| PathError::NonUtf8)?;

    // Rule 3: `%2F` is the empty-component marker. Encoding never emits a `/`
    // inside a component by any other route, so anything else that decodes to
    // one is a corrupt name, and letting it through would splice an extra
    // separator into the reconstructed key.
    if decoded == "/" {
        return Ok(String::new());
    }
    if decoded.contains('/') {
        return Err(PathError::BadEscape);
    }
    Ok(decoded)
}

/// Encode a full S3 key as a relative path under the bucket directory.
#[must_use]
pub fn key_to_rel_path(key: &str) -> PathBuf {
    let mut path = PathBuf::new();
    for component in key.split('/') {
        path.push(encode_component(component));
    }
    path
}

/// Recover the S3 key from a relative path produced by [`key_to_rel_path`].
///
/// # Errors
///
/// [`PathError::BadEscape`] if any component is malformed or is not a plain
/// file name; [`PathError::NonUtf8`] if a component is not valid UTF-8 either
/// on disk or after decoding.
pub fn rel_path_to_key(p: &Path) -> Result<String, PathError> {
    let mut parts = Vec::new();
    for component in p.components() {
        let Component::Normal(name) = component else {
            // Encoding never emits root, `.`, `..` or a drive prefix.
            return Err(PathError::BadEscape);
        };
        let name = name.to_str().ok_or(PathError::NonUtf8)?;
        parts.push(decode_component(name)?);
    }
    Ok(parts.join("/"))
}

/// Whether `version_id` is well-formed: `[A-Za-z0-9._-]+` containing no `..`
/// and at least one alphanumeric character.
///
/// Every version ID aks3 mints passes (UUIDs, and the literal `"null"` for the
/// unversioned placeholder). A client-supplied `?versionId=` is untrusted, so
/// callers should reject one that fails this check at the API edge with an
/// `InvalidArgument` rather than looking it up. [`data_file_name`] is safe
/// either way; rejecting early just gives a clearer error than a lookup miss on
/// an escaped name.
#[must_use]
pub fn is_valid_version_id(version_id: &str) -> bool {
    !version_id.contains("..")
        && version_id.bytes().any(|b| b.is_ascii_alphanumeric())
        && version_id.bytes().all(is_safe_version_byte)
}

/// Name of the data file holding the bytes of one object version.
///
/// The result is always exactly one relative path component: any byte outside
/// `[A-Za-z0-9._-]` is percent-escaped, so `/`, `\` and control bytes cannot
/// survive into the name and a hostile `version_id` cannot climb out of the
/// object directory it is joined onto. The escaping is injective, so two
/// distinct version IDs never name the same file. A `version_id` satisfying
/// [`is_valid_version_id`] is emitted verbatim.
#[must_use]
pub fn data_file_name(version_id: &str) -> String {
    const PREFIX: &str = "__aks3.v.";
    const SUFFIX: &str = ".data";

    let mut out = String::with_capacity(PREFIX.len() + version_id.len() + SUFFIX.len());
    out.push_str(PREFIX);
    for &b in version_id.as_bytes() {
        if is_safe_version_byte(b) {
            out.push(char::from(b));
        } else {
            push_escape(&mut out, b);
        }
    }
    out.push_str(SUFFIX);
    out
}

/// Bytes that may appear unescaped in a data file name.
fn is_safe_version_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-')
}

/// Whether a byte must be percent-encoded under rule 2.
fn needs_escape(b: u8) -> bool {
    b == b'%' || b < 0x20 || b == 0x7F || b == b'\\'
}

fn push_escape(out: &mut String, b: u8) {
    const HEX: [char; 16] = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F',
    ];
    out.push('%');
    out.push(HEX[(b >> 4) as usize]);
    out.push(HEX[(b & 0x0F) as usize]);
}

/// Value of a single hex digit, accepting either case.
fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn roundtrip_plain() {
        for k in [
            "a",
            "a/b/c",
            "photos/2026/cat.jpg",
            "with space",
            "uni\u{e9}",
        ] {
            let p = key_to_rel_path(k);
            assert_eq!(rel_path_to_key(&p).unwrap(), k);
        }
    }

    #[test]
    fn special_components() {
        assert_eq!(encode_component("."), "%2E");
        assert_eq!(encode_component(".."), "%2E%2E");
        assert_eq!(encode_component(""), "%2F");
        assert_eq!(encode_component("__aks3.meta.json"), "%5F_aks3.meta.json");
        assert_eq!(
            decode_component("%5F_aks3.meta.json").unwrap(),
            "__aks3.meta.json"
        );
        assert_eq!(rel_path_to_key(&key_to_rel_path("a//b")).unwrap(), "a//b");
        assert_eq!(
            rel_path_to_key(&key_to_rel_path("a/./..")).unwrap(),
            "a/./.."
        );
    }

    #[test]
    fn hostile_bytes() {
        let k = "bad\u{1}name/pct%25";
        assert_eq!(rel_path_to_key(&key_to_rel_path(k)).unwrap(), k);
        assert!(decode_component("%GZ").is_err());
        assert!(decode_component("%2").is_err());
    }

    #[test]
    fn data_file_names() {
        assert_eq!(data_file_name("null"), "__aks3.v.null.data");
    }

    #[test]
    fn data_file_name_escapes_path_hostile_ids() {
        assert_eq!(data_file_name("null"), "__aks3.v.null.data");
        assert_eq!(
            data_file_name("../../etc/passwd"),
            "__aks3.v...%2F..%2Fetc%2Fpasswd.data"
        );
        assert_eq!(data_file_name("a/b"), "__aks3.v.a%2Fb.data");
        assert_eq!(data_file_name("a\\b"), "__aks3.v.a%5Cb.data");
        assert_eq!(data_file_name("a b"), "__aks3.v.a%20b.data");
        assert_eq!(data_file_name("a%b"), "__aks3.v.a%25b.data");
        assert_eq!(data_file_name("\u{0}"), "__aks3.v.%00.data");
    }

    #[test]
    fn data_file_name_is_always_one_safe_component() {
        for id in [
            "null",
            "",
            ".",
            "..",
            "../../etc/passwd",
            "/",
            "//",
            "/etc/passwd",
            "a/b",
            "a\\b",
            "..\\..\\windows",
            "a%2Fb",
            "\u{0}\u{1f}\u{7f}",
            "caf\u{e9}",
            "\u{1f600}",
        ] {
            let name = data_file_name(id);
            assert!(!name.contains('/'), "{id:?} produced {name:?} with a slash");
            assert!(
                !name.contains('\\'),
                "{id:?} produced {name:?} with a backslash"
            );
            assert!(name.starts_with(RESERVED_PREFIX), "{id:?} lost its prefix");
            // strip_suffix, not ends_with: the suffix we emit is always exactly
            // ".data", and clippy pushes ends_with toward a case-insensitive
            // Path::extension check that would weaken the assertion.
            assert!(
                name.strip_suffix(".data").is_some(),
                "{id:?} lost its suffix"
            );
            let p = Path::new(&name);
            assert!(p.is_relative(), "{id:?} produced an absolute path");
            let comps: Vec<_> = p.components().collect();
            assert_eq!(comps.len(), 1, "{id:?} produced {comps:?}");
            assert!(
                matches!(comps[0], Component::Normal(_)),
                "{id:?} produced {comps:?}"
            );
        }
    }

    #[test]
    fn data_file_name_is_injective() {
        let ids = ["a/b", "a%2Fb", "a%252Fb", "a\\b", "ab", "a.b", "a b"];
        let names: BTreeSet<String> = ids.iter().map(|i| data_file_name(i)).collect();
        assert_eq!(names.len(), ids.len(), "two version IDs share a file name");
    }

    #[test]
    fn version_id_validation() {
        for ok in [
            "null",
            "0198f0c1-7b3a-7e2d-9f01-2c3d4e5f6a7b",
            "abc",
            "A1",
            "a.b",
            "a_b",
            "a-b",
            "9",
        ] {
            assert!(is_valid_version_id(ok), "{ok:?} should be valid");
        }
        for bad in [
            "",
            ".",
            "..",
            "...",
            "../x",
            "a/b",
            "a\\b",
            "a b",
            "a%b",
            "..a",
            "a..b",
            "a..",
            "\u{0}",
            "caf\u{e9}",
            "a\nb",
        ] {
            assert!(!is_valid_version_id(bad), "{bad:?} should be rejected");
        }
    }

    #[test]
    fn valid_version_ids_are_never_escaped() {
        for id in [
            "null",
            "0198f0c1-7b3a-7e2d-9f01-2c3d4e5f6a7b",
            "a.b",
            "a_b-1",
        ] {
            assert_eq!(data_file_name(id), format!("__aks3.v.{id}.data"));
        }
    }

    // --- additional cases beyond the brief ---

    #[test]
    fn roundtrip_hostile_keys() {
        for k in [
            "",
            "/",
            "//",
            "a/",
            "/a",
            ".",
            "..",
            "../../etc/passwd",
            "a/../../b",
            "back\\slash",
            "pct%",
            "%2E",
            "%2F",
            "%5F_aks3.meta.json",
            META_FILE,
            "dir/__aks3.v.null.data",
            "\u{7f}del",
            "tab\tnew\nline",
            "\u{1f600}/emoji",
            "\u{e9}\u{301}combining",
            "trailing.",
            "trailing..",
        ] {
            let p = key_to_rel_path(k);
            assert_eq!(
                rel_path_to_key(&p).unwrap(),
                k,
                "roundtrip failed for {k:?}"
            );
        }
    }

    #[test]
    fn escape_set_is_exactly_rule_two() {
        assert_eq!(encode_component("\u{0}"), "%00");
        assert_eq!(encode_component("\u{1f}"), "%1F");
        assert_eq!(encode_component("\u{7f}"), "%7F");
        assert_eq!(encode_component("\\"), "%5C");
        assert_eq!(encode_component("%"), "%25");
        // Space (0x20) and other printable ASCII are left alone.
        assert_eq!(encode_component("a b:c*d?e\"f<g>h|i"), "a b:c*d?e\"f<g>h|i");
        // Non-ASCII passes through untouched.
        assert_eq!(
            encode_component("caf\u{e9} \u{1f600}"),
            "caf\u{e9} \u{1f600}"
        );
    }

    #[test]
    fn reserved_prefix_escaping() {
        assert_eq!(encode_component(META_FILE), "%5F_aks3.meta.json");
        assert_eq!(
            encode_component(&data_file_name("v1")),
            "%5F_aks3.v.v1.data"
        );
        assert_eq!(encode_component("__aks3"), "%5F_aks3");
        assert_eq!(encode_component("__aks3suffix"), "%5F_aks3suffix");
        // Near misses are not escaped.
        assert_eq!(encode_component("__aks"), "__aks");
        assert_eq!(encode_component("_aks3"), "_aks3");
        assert_eq!(encode_component("x__aks3"), "x__aks3");
    }

    #[test]
    fn no_user_key_encodes_to_a_reserved_name() {
        for k in [META_FILE, "__aks3.v.null.data", RESERVED_PREFIX] {
            assert!(!encode_component(k).starts_with(RESERVED_PREFIX));
        }
    }

    #[test]
    fn decode_rejects_malformed_escapes() {
        for bad in ["%", "%A", "%GG", "%2G", "%G2", "a%", "a%1", "%%25"] {
            assert_eq!(
                decode_component(bad),
                Err(PathError::BadEscape),
                "expected BadEscape for {bad:?}"
            );
        }
    }

    #[test]
    fn decode_rejects_non_utf8() {
        assert_eq!(decode_component("%FF"), Err(PathError::NonUtf8));
        assert_eq!(decode_component("%C3%28"), Err(PathError::NonUtf8));
    }

    #[test]
    fn decode_accepts_lowercase_hex() {
        assert_eq!(decode_component("%2e").unwrap(), ".");
        assert_eq!(decode_component("%5f_aks3").unwrap(), "__aks3");
    }

    #[test]
    fn decode_never_yields_a_separator() {
        // Both spellings of the empty marker decode to empty, not to `/`.
        assert_eq!(decode_component("%2F").unwrap(), "");
        assert_eq!(decode_component("%2f").unwrap(), "");
        // A corrupt name that would splice a separator into the key is rejected.
        assert_eq!(decode_component("%2Fa"), Err(PathError::BadEscape));
        assert_eq!(decode_component("a%2Fb"), Err(PathError::BadEscape));
        assert_eq!(decode_component("%2F%2F"), Err(PathError::BadEscape));
    }

    #[test]
    fn rel_path_to_key_rejects_traversal_components() {
        assert_eq!(rel_path_to_key(Path::new("..")), Err(PathError::BadEscape));
        assert_eq!(
            rel_path_to_key(Path::new("a/../b")),
            Err(PathError::BadEscape)
        );
        assert_eq!(
            rel_path_to_key(Path::new("/abs")),
            Err(PathError::BadEscape)
        );
    }

    #[test]
    fn encoded_paths_have_only_normal_components() {
        for k in ["", ".", "..", "a/./..", "//", "a/../b"] {
            let p = key_to_rel_path(k);
            assert!(
                p.components().all(|c| matches!(c, Component::Normal(_))),
                "{k:?} encoded to a path with a non-normal component: {p:?}"
            );
            assert!(p.is_relative(), "{k:?} encoded to a non-relative path");
        }
    }

    #[test]
    fn component_count_matches_key() {
        let p = key_to_rel_path("a//b/c");
        assert_eq!(p.components().count(), 4);
    }
}
