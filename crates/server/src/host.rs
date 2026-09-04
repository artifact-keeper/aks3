// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// Derived in part from MinIO (https://github.com/minio/minio), AGPL-3.0.
// SPDX-License-Identifier: AGPL-3.0-only

//! Virtual-hosted-style addressing: which `Host` headers name a bucket.
//!
//! An S3 client can put the bucket in the path (`/bucket/key`, path style) or
//! in the hostname (`bucket.s3.example.com/key`, virtual-hosted style). AWS
//! made the second the default years ago and some clients no longer offer the
//! first at all — the AWS SDK for Java v2 and everything built on it, Unity
//! Catalog included — so a store that cannot read a bucket out of the `Host`
//! header is a store those clients cannot talk to.
//!
//! Reading it is only safe once an operator has said which domains are theirs.
//! Without one, every hostname the store is reached through is
//! indistinguishable from a bucket: `aks3.aks3.svc`, a tailnet name, a
//! CNAME in front of a load balancer. Guessing there would break path-style
//! requests through those very names, which is why this is off until
//! [`Config::virtual_host_domains`] is set, and why a host that is *not* under
//! one of those domains is still read as path style rather than guessed at.
//! That is the one place this parser deliberately differs from
//! [`s3s::host::SingleDomain`], which treats any unmatched host as a bucket.
//!
//! [`Config::virtual_host_domains`]: crate::config::Config::virtual_host_domains

use std::net::IpAddr;

use s3s::host::{S3Host, VirtualHost};
use s3s::S3Result;

/// The domains under which a subdomain names a bucket.
///
/// Construct this from [`Config::virtual_host_domains`], which is where the
/// domains are normalized (lowercased, port-free) and checked. Passing
/// anything else risks a domain that can never match; nothing here validates,
/// because a request is the wrong time to discover a bad setting.
///
/// [`Config::virtual_host_domains`]: crate::config::Config::virtual_host_domains
#[derive(Debug, Clone)]
pub struct VirtualHostDomains {
    domains: Vec<String>,
}

impl VirtualHostDomains {
    /// Wraps already-normalized domains.
    #[must_use]
    pub fn new(domains: Vec<String>) -> Self {
        Self { domains }
    }

    /// The bucket `host` names, if it names one.
    ///
    /// `None` means path style: the host is the endpoint itself, is an address
    /// rather than a name, or is not under any configured domain.
    fn bucket_of(&self, host: &str) -> Option<String> {
        let name = host_name(host)?;
        self.domains
            .iter()
            .find_map(|domain| strip_domain_suffix(name, domain))
            .map(str::to_ascii_lowercase)
    }
}

impl S3Host for VirtualHostDomains {
    fn parse_host_header<'a>(&'a self, host: &'a str) -> S3Result<VirtualHost<'a>> {
        let vh = VirtualHost::new(host);
        // An unparseable or unmatched host is not an error: it is a path-style
        // request, and the path parser is about to have its say. Answering
        // `InvalidRequest` here would reject every client that reaches the
        // store through a name the operator did not list.
        Ok(match self.bucket_of(host) {
            Some(bucket) => vh.with_bucket(bucket),
            None => vh,
        })
    }
}

/// The name half of a `Host` header: the port dropped, and addresses refused.
///
/// A bracketed IPv6 literal and a bare IPv4 address are both rejected because
/// neither can carry a bucket, and an IPv4 address's dots would otherwise look
/// like labels. s3s skips this parser for hosts it recognizes as addresses;
/// this repeats the check so the parser is safe to call on its own.
fn host_name(host: &str) -> Option<&str> {
    let name = match host.rsplit_once(':') {
        // A port is digits, and the part before it must not be the tail of an
        // IPv6 literal ("[::1]:9000" splits on the wrong colon otherwise).
        Some((head, port))
            if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) && !head.is_empty() =>
        {
            head
        }
        _ => host,
    };
    if name.starts_with('[') || name.parse::<IpAddr>().is_ok() {
        return None;
    }
    Some(name)
}

/// `name` with `domain` and the dot before it removed, when `name` sits under
/// `domain` as a subdomain.
///
/// `None` when `name` *is* the domain (a path-style request to the endpoint),
/// when it is under a different domain, or when the label before the domain is
/// empty. The comparison is case-insensitive because a hostname is; the caller
/// lowercases what comes back, since a bucket name is not.
fn strip_domain_suffix<'a>(name: &'a str, domain: &str) -> Option<&'a str> {
    let split = name.len().checked_sub(domain.len())?;
    // A multi-byte `Host` header could otherwise put the split mid-character,
    // which would panic. Domains are ASCII, so a boundary here is also proof
    // the tail could match at all.
    if split == 0 || !name.is_char_boundary(split) {
        return None;
    }
    let (prefix, tail) = name.split_at(split);
    if !tail.eq_ignore_ascii_case(domain) {
        return None;
    }
    let bucket = prefix.strip_suffix('.')?;
    (!bucket.is_empty()).then_some(bucket)
}

#[cfg(test)]
mod tests {
    use super::{host_name, strip_domain_suffix, VirtualHostDomains};
    use s3s::host::S3Host;

    fn domains(list: &[&str]) -> VirtualHostDomains {
        VirtualHostDomains::new(list.iter().map(|d| (*d).to_owned()).collect())
    }

    fn bucket(list: &[&str], host: &str) -> Option<String> {
        domains(list)
            .parse_host_header(host)
            .unwrap()
            .bucket()
            .map(str::to_owned)
    }

    #[test]
    fn a_subdomain_of_a_configured_domain_names_the_bucket() {
        assert_eq!(
            bucket(&["s3.example.com"], "demo.s3.example.com"),
            Some("demo".to_owned())
        );
    }

    #[test]
    fn the_endpoint_itself_names_no_bucket() {
        assert_eq!(bucket(&["s3.example.com"], "s3.example.com"), None);
    }

    #[test]
    fn a_port_is_not_part_of_the_name() {
        assert_eq!(
            bucket(&["s3.example.com"], "demo.s3.example.com:9000"),
            Some("demo".to_owned())
        );
        assert_eq!(bucket(&["s3.example.com"], "s3.example.com:9000"), None);
    }

    #[test]
    fn a_host_case_is_folded_but_the_bucket_is_lowercased() {
        assert_eq!(
            bucket(&["s3.example.com"], "Demo.S3.Example.COM"),
            Some("demo".to_owned())
        );
    }

    /// The regression this parser exists to avoid: with a domain configured,
    /// a request through any other name stays path style. s3s's own
    /// `SingleDomain` would read the whole host as a bucket here, which turns
    /// every in-cluster path-style request into a request for a bucket nobody
    /// created.
    #[test]
    fn an_unrelated_host_stays_path_style() {
        for host in [
            "aks3.aks3.svc",
            "aks3.aks3.svc.cluster.local:9000",
            "grace.possum-fujita.ts.net",
            "localhost:9000",
            "s3.example.com.evil.test",
        ] {
            assert_eq!(bucket(&["s3.example.com"], host), None, "host {host}");
        }
    }

    #[test]
    fn an_address_names_no_bucket() {
        for host in ["127.0.0.1", "127.0.0.1:9000", "[::1]:9000", "[::1]", "::1"] {
            assert_eq!(bucket(&["s3.example.com"], host), None, "host {host}");
        }
    }

    #[test]
    fn several_domains_are_matched_in_turn() {
        let list = &["s3.example.com", "objects.example.net"];
        assert_eq!(bucket(list, "a.s3.example.com"), Some("a".to_owned()));
        assert_eq!(bucket(list, "b.objects.example.net"), Some("b".to_owned()));
        assert_eq!(bucket(list, "c.other.example.org"), None);
    }

    /// A bucket name may contain dots, so everything before the domain is the
    /// bucket, not just the last label.
    #[test]
    fn a_dotted_bucket_survives() {
        assert_eq!(
            bucket(&["s3.example.com"], "my.data.s3.example.com"),
            Some("my.data".to_owned())
        );
    }

    #[test]
    fn an_empty_label_names_no_bucket() {
        assert_eq!(bucket(&["s3.example.com"], ".s3.example.com"), None);
    }

    #[test]
    fn no_domains_means_every_host_is_path_style() {
        assert_eq!(bucket(&[], "demo.s3.example.com"), None);
    }

    #[test]
    fn a_multibyte_host_does_not_split_mid_character() {
        // The suffix length lands inside the 'é' when compared naively.
        assert_eq!(strip_domain_suffix("café.test", "e.test"), None);
        assert_eq!(
            bucket(&["s3.example.com"], "demö.s3.example.com"),
            Some("demö".to_owned())
        );
    }

    #[test]
    fn host_name_drops_only_a_real_port() {
        assert_eq!(host_name("s3.example.com:9000"), Some("s3.example.com"));
        assert_eq!(host_name("s3.example.com:"), Some("s3.example.com:"));
        assert_eq!(
            host_name("s3.example.com:port"),
            Some("s3.example.com:port")
        );
        assert_eq!(host_name("127.0.0.1:9000"), None);
    }
}
