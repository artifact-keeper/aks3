// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// Derived in part from MinIO (https://github.com/minio/minio), AGPL-3.0.
// SPDX-License-Identifier: AGPL-3.0-only

//! Server settings: what to listen on, where to keep data, who the root user is.
//!
//! Settings come from two places, in order: an optional TOML file, then the
//! environment. The environment wins, so a container image can ship a file and
//! still have its credentials injected at run time.
//!
//! Only the root credential pair is required. Everything else has a default,
//! which is what makes an environment-only start (no file at all) work.

use std::fmt;
use std::path::{Path, PathBuf};

/// Address the server listens on when nothing says otherwise. Loopback, not
/// `0.0.0.0`: a store that reached the network by default would be one
/// forgotten setting away from being served to it.
const DEFAULT_LISTEN: &str = "127.0.0.1:9000";

/// Directory the store lives in when nothing says otherwise, relative to the
/// working directory.
const DEFAULT_DATA_DIR: &str = "./data";

/// How long a shutdown waits for connections that are still in flight before
/// dropping them, when nothing says otherwise.
///
/// A `GET` of a large object is a long-lived connection, so this is generous
/// enough to let one finish rather than tuned for a fast exit. It has to stay
/// under the timeout of whatever sent the signal, because that timeout ends in
/// SIGKILL. `docker stop` waits ten seconds by default, so a ten second window
/// here would be a tie, and a tie is lost: the drain is still deciding when the
/// process is killed under it, which is the outcome the drain exists to avoid.
/// Eight seconds leaves the margin that makes the common case resolve on the
/// server's terms under a supervisor nobody has configured.
const DEFAULT_SHUTDOWN_GRACE_SECONDS: u64 = 8;

/// The longest drain window this server will start with.
///
/// Ten minutes is past anything a supervisor allows without being told to (a
/// systemd unit stops at ninety seconds, Kubernetes at thirty), so a value
/// above it is far more likely to be a typo (an extra zero, or milliseconds
/// written where seconds were asked for) than a request. Left unchecked, such a
/// value costs nothing at startup and everything at the stop weeks later, where
/// it turns into a drain that never finishes and a SIGKILL every time. The
/// bound is arbitrary in the sense that no mechanism breaks at 601; it is there
/// so the mistake is reported by the process that will suffer from it, while
/// somebody is still watching.
const MAX_SHUTDOWN_GRACE_SECONDS: u64 = 600;

/// Environment variable naming the listen address.
pub const ENV_LISTEN: &str = "AKS3_LISTEN";
/// Environment variable naming the data directory.
pub const ENV_DATA_DIR: &str = "AKS3_DATA_DIR";
/// Environment variable naming the root access key id.
pub const ENV_ROOT_USER: &str = "AKS3_ROOT_USER";
/// Environment variable naming the root secret key.
pub const ENV_ROOT_PASSWORD: &str = "AKS3_ROOT_PASSWORD";
/// Environment variable naming the shutdown grace period, in seconds.
pub const ENV_SHUTDOWN_GRACE: &str = "AKS3_SHUTDOWN_GRACE";
/// Environment variable naming the domains under which a subdomain is a
/// bucket, comma-separated. Empty or unset leaves virtual-hosted-style
/// addressing off.
pub const ENV_VIRTUAL_HOST_DOMAINS: &str = "AKS3_VIRTUAL_HOST_DOMAINS";

/// Reasons the server cannot work out what to start as.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The config file could not be read.
    #[error("cannot read config file {path}")]
    Read {
        /// The file that was asked for.
        path: PathBuf,
        /// Why the read failed.
        source: std::io::Error,
    },
    /// The config file is not the TOML this server understands.
    #[error("cannot parse config file {path}")]
    Parse {
        /// The file that was read.
        path: PathBuf,
        /// Where and how the parse failed.
        source: toml::de::Error,
    },
    /// A setting with no default was given neither in the file nor the
    /// environment. Carries the environment variable that would supply it.
    #[error("missing required setting; set it in the config file or as {0}")]
    Missing(&'static str),
    /// A setting was given a value the server cannot start with. Separate from
    /// [`ConfigError::Parse`] because the file parser only knows the TOML
    /// types: a whole number where a whole number belongs is a valid file and
    /// still an unusable setting, and the environment carries strings, so it
    /// gets no type check at all.
    ///
    /// The value is quoted in the message, so that a variable set to nothing at
    /// all reads as `""` rather than as a gap the reader has to guess the width
    /// of. That is not a rare case: Compose passes an empty string for
    /// `VAR=${UNSET}`.
    #[error("{setting} = {value:?} is not usable; it has to be {expected}")]
    Invalid {
        /// The setting's name in the config file.
        setting: &'static str,
        /// The value as it was written.
        value: String,
        /// What would have been accepted, as a phrase completing the message.
        expected: &'static str,
    },
}

/// TLS material, as paths to PEM files on disk.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// Certificate chain, leaf first.
    pub cert_pem: PathBuf,
    /// Private key for the leaf certificate.
    pub key_pem: PathBuf,
}

/// Everything the server needs to start.
#[derive(Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Host and port to bind, as `tokio::net::TcpListener` takes it. Port 0
    /// asks the operating system to choose, which is how tests get a free one.
    pub listen: String,
    /// Root of the on-disk store, created if it does not exist.
    pub data_dir: PathBuf,
    /// Root access key id. Not a secret; it travels in the clear.
    pub root_access_key: String,
    /// Root secret key. Never log this; see the [`fmt::Debug`] impl.
    pub root_secret_key: String,
    /// How many seconds a shutdown gives the connections still in flight to
    /// finish before dropping them. Zero drops them at once.
    pub shutdown_grace_seconds: u64,
    /// Domains under which a subdomain of the host names a bucket
    /// (virtual-hosted-style addressing). Empty leaves it off, and every
    /// request is read as path style.
    ///
    /// Normalized on load: lowercased, without a port, and checked so that no
    /// domain is a suffix of another. See [`crate::host`] for what matching
    /// one does and, more to the point, what not matching one does not do.
    pub virtual_host_domains: Vec<String>,
    /// TLS material, or `None` to serve plain HTTP.
    pub tls: Option<TlsConfig>,
}

/// A config file, which may leave anything out.
///
/// This is separate from [`Config`] because the two answer different questions.
/// A file is allowed to be partial (the environment may fill the rest in), so
/// every field here is optional; a [`Config`] is what is left once both sources
/// have been read and the result is known to be complete.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    listen: Option<String>,
    data_dir: Option<PathBuf>,
    root_access_key: Option<String>,
    root_secret_key: Option<String>,
    shutdown_grace_seconds: Option<u64>,
    virtual_host_domains: Option<Vec<String>>,
    tls: Option<TlsConfig>,
}

impl Config {
    /// Reads the config file at `path` if given, then lets the environment
    /// override it.
    ///
    /// Errors if the file is unreadable or malformed, or if the root
    /// credentials are absent from both sources.
    pub fn load(path: Option<&Path>) -> Result<Self, ConfigError> {
        Self::load_from(path, |name| std::env::var(name).ok())
    }

    /// [`Config::load`] against an arbitrary environment.
    ///
    /// Taking the lookup as an argument is what lets the override rules be
    /// tested in parallel: the real environment is process-wide, so tests that
    /// set variables in it have to take turns.
    fn load_from(
        path: Option<&Path>,
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<Self, ConfigError> {
        let file = match path {
            Some(path) => {
                let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
                    path: path.to_path_buf(),
                    source,
                })?;
                toml::from_str::<FileConfig>(&text).map_err(|source| ConfigError::Parse {
                    path: path.to_path_buf(),
                    source,
                })?
            }
            None => FileConfig::default(),
        };

        Ok(Self {
            listen: env(ENV_LISTEN)
                .or(file.listen)
                .unwrap_or_else(|| DEFAULT_LISTEN.to_owned()),
            data_dir: env(ENV_DATA_DIR)
                .map(PathBuf::from)
                .or(file.data_dir)
                .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIR)),
            root_access_key: env(ENV_ROOT_USER)
                .or(file.root_access_key)
                .ok_or(ConfigError::Missing(ENV_ROOT_USER))?,
            root_secret_key: env(ENV_ROOT_PASSWORD)
                .or(file.root_secret_key)
                .ok_or(ConfigError::Missing(ENV_ROOT_PASSWORD))?,
            shutdown_grace_seconds: shutdown_grace_seconds(
                env(ENV_SHUTDOWN_GRACE).as_deref(),
                file.shutdown_grace_seconds,
            )?,
            virtual_host_domains: virtual_host_domains(
                env(ENV_VIRTUAL_HOST_DOMAINS).as_deref(),
                file.virtual_host_domains,
            )?,
            tls: file.tls,
        })
    }
}

/// Settles the virtual-host domains from the two sources and checks each one
/// can actually match a `Host` header.
///
/// The environment form is comma-separated, because that is the only list an
/// environment variable can carry; blanks around the commas are trimmed and an
/// empty element is dropped, so `AKS3_VIRTUAL_HOST_DOMAINS=` and
/// `AKS3_VIRTUAL_HOST_DOMAINS=" "` both mean "off" rather than "one nameless
/// domain". A set variable always replaces the file's list rather than adding
/// to it, which is the rule every other setting here follows.
///
/// What is rejected is anything that could never match. A port is the likeliest
/// mistake (`s3.example.com:9000`, copied from an endpoint URL): the port is
/// not part of a hostname, so the domain would match nothing and
/// virtual-hosted-style requests would silently stay path style. Overlapping
/// domains are rejected too, since a host under both has two answers and the
/// order of a config file should not decide which.
fn virtual_host_domains(
    from_env: Option<&str>,
    from_file: Option<Vec<String>>,
) -> Result<Vec<String>, ConfigError> {
    /// The name to report either source under, which is the file's key.
    const SETTING: &str = "virtual_host_domains";

    let listed: Vec<String> = match from_env {
        Some(text) => text
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .map(str::to_ascii_lowercase)
            .collect(),
        None => from_file
            .unwrap_or_default()
            .iter()
            .map(|domain| domain.trim().to_ascii_lowercase())
            .filter(|domain| !domain.is_empty())
            .collect(),
    };

    let mut domains: Vec<String> = Vec::with_capacity(listed.len());
    for domain in listed {
        let invalid = |expected: &'static str| ConfigError::Invalid {
            setting: SETTING,
            value: domain.clone(),
            expected,
        };
        if domain.contains(':') {
            return Err(invalid(
                "a hostname without a port, such as \"s3.example.com\" (the port a client \
                 connects to is not part of the name it sends)",
            ));
        }
        if !is_hostname(&domain) {
            return Err(invalid(
                "a hostname of dot-separated letters, digits and hyphens, such as \
                 \"s3.example.com\"",
            ));
        }
        if let Some(other) = domains
            .iter()
            .find(|other| domain.ends_with(other.as_str()) || other.ends_with(&domain))
        {
            return Err(ConfigError::Invalid {
                setting: SETTING,
                value: format!("{domain} and {other}"),
                expected: "domains that do not overlap, so that a host under one of them \
                           names one bucket",
            });
        }
        domains.push(domain);
    }
    Ok(domains)
}

/// Whether `s` is a hostname: dot-separated labels of letters, digits and
/// hyphens, none of them empty.
///
/// Deliberately narrow. This is a domain an operator typed into a config file,
/// not a name resolved from the wire, so the cost of refusing an exotic-but-legal
/// name is a clear error at startup, while the cost of accepting a malformed one
/// is a matching rule that never fires.
fn is_hostname(s: &str) -> bool {
    !s.is_empty()
        && s.split('.').all(|label| {
            !label.is_empty()
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
}

/// Settles the drain window from the two sources and checks it is one the
/// server can start with.
///
/// The environment arrives as text because that is all an environment holds, so
/// this is where a value that is not a number is caught; the file is already
/// known to hold a whole number, having parsed as one, and only the bound is
/// left to check.
fn shutdown_grace_seconds(
    from_env: Option<&str>,
    from_file: Option<u64>,
) -> Result<u64, ConfigError> {
    /// The name to report either source under, which is the file's key: an
    /// operator who set the environment variable knows which one they set,
    /// and one who set neither is being pointed at the file.
    const SETTING: &str = "shutdown_grace_seconds";
    /// Completes "it has to be ..." in the error.
    const EXPECTED: &str = "a whole number of seconds, at most 600";

    let seconds = match from_env {
        Some(text) => text.parse::<u64>().map_err(|_| ConfigError::Invalid {
            setting: SETTING,
            value: text.to_owned(),
            expected: EXPECTED,
        })?,
        None => from_file.unwrap_or(DEFAULT_SHUTDOWN_GRACE_SECONDS),
    };

    if seconds > MAX_SHUTDOWN_GRACE_SECONDS {
        return Err(ConfigError::Invalid {
            setting: SETTING,
            value: seconds.to_string(),
            expected: EXPECTED,
        });
    }
    Ok(seconds)
}

/// Written by hand so the root secret cannot reach a log through a `{:?}` on
/// the config, which is the one struct every startup path holds.
impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("listen", &self.listen)
            .field("data_dir", &self.data_dir)
            .field("root_access_key", &self.root_access_key)
            .field("root_secret_key", &"[REDACTED]")
            .field("shutdown_grace_seconds", &self.shutdown_grace_seconds)
            .field("virtual_host_domains", &self.virtual_host_domains)
            .field("tls", &self.tls)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Config, ConfigError, ENV_DATA_DIR, ENV_LISTEN, ENV_ROOT_PASSWORD, ENV_ROOT_USER,
        ENV_SHUTDOWN_GRACE, ENV_VIRTUAL_HOST_DOMAINS,
    };

    /// A config file with every setting the server needs.
    const FULL_TOML: &str = r#"
        listen = "127.0.0.1:0"
        data_dir = "/tmp/aks3-test"
        root_access_key = "admin"
        root_secret_key = "secretpassword"
    "#;

    /// Writes `body` to a scratch file and returns it with the directory that
    /// owns it, which must outlive the path.
    fn config_file(body: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.toml");
        std::fs::write(&path, body).unwrap();
        (dir, path)
    }

    #[test]
    fn env_overrides_and_required_creds() {
        let _env = ScrubbedEnv::new();
        let (_dir, path) = config_file(FULL_TOML);
        let c = Config::load(Some(&path)).unwrap();
        assert_eq!(c.root_access_key, "admin");
        assert!(Config::load(None).is_err()); // no file, no env: missing creds
    }

    #[test]
    fn env_wins_over_the_file() {
        let (_dir, path) = config_file(FULL_TOML);
        let env = [
            (ENV_LISTEN, "0.0.0.0:9999"),
            (ENV_DATA_DIR, "/var/lib/aks3"),
            (ENV_ROOT_USER, "operator"),
            (ENV_ROOT_PASSWORD, "anotherpassword"),
        ];
        let c = Config::load_from(Some(&path), stub_env(&env)).unwrap();
        assert_eq!(c.listen, "0.0.0.0:9999");
        assert_eq!(c.data_dir, std::path::Path::new("/var/lib/aks3"));
        assert_eq!(c.root_access_key, "operator");
        assert_eq!(c.root_secret_key, "anotherpassword");
    }

    #[test]
    fn env_alone_is_enough_to_start() {
        let env = [
            (ENV_ROOT_USER, "admin"),
            (ENV_ROOT_PASSWORD, "secretpassword"),
        ];
        let c = Config::load_from(None, stub_env(&env)).unwrap();
        assert_eq!(c.root_access_key, "admin");
        assert_eq!(c.listen, super::DEFAULT_LISTEN);
        assert_eq!(c.data_dir, std::path::Path::new(super::DEFAULT_DATA_DIR));
        assert!(c.tls.is_none());
    }

    #[test]
    fn half_a_credential_pair_is_not_enough() {
        let env = [(ENV_ROOT_USER, "admin")];
        assert!(matches!(
            Config::load_from(None, stub_env(&env)),
            Err(ConfigError::Missing(ENV_ROOT_PASSWORD))
        ));
    }

    #[test]
    fn a_config_path_that_does_not_exist_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("absent.toml");
        let env = [
            (ENV_ROOT_USER, "admin"),
            (ENV_ROOT_PASSWORD, "secretpassword"),
        ];
        assert!(matches!(
            Config::load_from(Some(&missing), stub_env(&env)),
            Err(ConfigError::Read { .. })
        ));
    }

    #[test]
    fn malformed_toml_names_the_file() {
        let (_dir, path) = config_file("listen = ");
        let err = Config::load_from(Some(&path), stub_env(&[])).unwrap_err();
        assert!(matches!(err, ConfigError::Parse { .. }));
        assert!(err.to_string().contains("c.toml"));
    }

    #[test]
    fn an_unknown_setting_is_rejected_rather_than_ignored() {
        let (_dir, path) = config_file(
            r#"
            root_access_key = "admin"
            root_secret_key = "secretpassword"
            listne = "127.0.0.1:9000"
            "#,
        );
        assert!(matches!(
            Config::load_from(Some(&path), stub_env(&[])),
            Err(ConfigError::Parse { .. })
        ));
    }

    /// The drain window has a default, so a config written before the setting
    /// existed keeps the behaviour it had.
    #[test]
    fn the_grace_period_defaults_to_eight_seconds() {
        let env = [
            (ENV_ROOT_USER, "admin"),
            (ENV_ROOT_PASSWORD, "secretpassword"),
        ];
        let c = Config::load_from(None, stub_env(&env)).unwrap();
        assert_eq!(c.shutdown_grace_seconds, 8);
    }

    #[test]
    fn the_grace_period_comes_from_the_file() {
        let (_dir, path) = config_file(
            r#"
            root_access_key = "admin"
            root_secret_key = "secretpassword"
            shutdown_grace_seconds = 25
            "#,
        );
        let c = Config::load_from(Some(&path), stub_env(&[])).unwrap();
        assert_eq!(c.shutdown_grace_seconds, 25);
    }

    #[test]
    fn the_grace_period_in_the_environment_wins_over_the_file() {
        let (_dir, path) = config_file(
            r#"
            root_access_key = "admin"
            root_secret_key = "secretpassword"
            shutdown_grace_seconds = 25
            "#,
        );
        let env = [(ENV_SHUTDOWN_GRACE, "40")];
        let c = Config::load_from(Some(&path), stub_env(&env)).unwrap();
        assert_eq!(c.shutdown_grace_seconds, 40);
    }

    /// Zero is a real answer, not a mistake: it asks for the connections still
    /// open at a stop to be dropped rather than waited for.
    #[test]
    fn a_zero_grace_period_is_accepted() {
        let env = [
            (ENV_ROOT_USER, "admin"),
            (ENV_ROOT_PASSWORD, "secretpassword"),
            (ENV_SHUTDOWN_GRACE, "0"),
        ];
        let c = Config::load_from(None, stub_env(&env)).unwrap();
        assert_eq!(c.shutdown_grace_seconds, 0);
    }

    /// An environment variable is a string, so this is the one source that can
    /// carry something that is not a number at all.
    #[test]
    fn a_grace_period_that_is_not_a_number_is_an_error() {
        let env = [
            (ENV_ROOT_USER, "admin"),
            (ENV_ROOT_PASSWORD, "secretpassword"),
            (ENV_SHUTDOWN_GRACE, "eight"),
        ];
        let err = Config::load_from(None, stub_env(&env)).unwrap_err();
        assert!(matches!(err, ConfigError::Invalid { .. }));
        assert!(err.to_string().contains("eight"), "{err}");
    }

    /// A variable set to nothing at all is the case Compose produces from
    /// `VAR=${UNSET}`, and the message has to show the emptiness rather than
    /// trail off into a blank.
    #[test]
    fn an_empty_grace_period_is_an_error_that_shows_it_was_empty() {
        let env = [
            (ENV_ROOT_USER, "admin"),
            (ENV_ROOT_PASSWORD, "secretpassword"),
            (ENV_SHUTDOWN_GRACE, ""),
        ];
        let err = Config::load_from(None, stub_env(&env)).unwrap_err();
        assert!(matches!(err, ConfigError::Invalid { .. }));
        assert!(err.to_string().contains(r#"= """#), "{err}");
    }

    /// A negative number is not a parse error the caller can be left to guess
    /// at either: it is rejected the same way, naming what was written.
    #[test]
    fn a_negative_grace_period_is_an_error() {
        let env = [
            (ENV_ROOT_USER, "admin"),
            (ENV_ROOT_PASSWORD, "secretpassword"),
            (ENV_SHUTDOWN_GRACE, "-1"),
        ];
        assert!(matches!(
            Config::load_from(None, stub_env(&env)),
            Err(ConfigError::Invalid { .. })
        ));
    }

    /// The cap catches the mistyped value (a millisecond figure, an extra
    /// zero) that would otherwise turn every stop into the supervisor's
    /// SIGKILL. It applies wherever the value came from.
    #[test]
    fn a_grace_period_above_the_cap_is_an_error_from_either_source() {
        let env = [
            (ENV_ROOT_USER, "admin"),
            (ENV_ROOT_PASSWORD, "secretpassword"),
            (ENV_SHUTDOWN_GRACE, "601"),
        ];
        assert!(matches!(
            Config::load_from(None, stub_env(&env)),
            Err(ConfigError::Invalid { .. })
        ));

        let (_dir, path) = config_file(
            r#"
            root_access_key = "admin"
            root_secret_key = "secretpassword"
            shutdown_grace_seconds = 8000
            "#,
        );
        assert!(matches!(
            Config::load_from(Some(&path), stub_env(&[])),
            Err(ConfigError::Invalid { .. })
        ));
    }

    /// The bound itself is allowed; only what is past it is not.
    #[test]
    fn the_cap_itself_is_accepted() {
        let env = [
            (ENV_ROOT_USER, "admin"),
            (ENV_ROOT_PASSWORD, "secretpassword"),
            (ENV_SHUTDOWN_GRACE, "600"),
        ];
        let c = Config::load_from(None, stub_env(&env)).unwrap();
        assert_eq!(c.shutdown_grace_seconds, 600);
    }

    #[test]
    fn tls_paths_come_from_the_file() {
        let (_dir, path) = config_file(
            r#"
            root_access_key = "admin"
            root_secret_key = "secretpassword"
            [tls]
            cert_pem = "/etc/aks3/cert.pem"
            key_pem = "/etc/aks3/key.pem"
            "#,
        );
        let tls = Config::load_from(Some(&path), stub_env(&[]))
            .unwrap()
            .tls
            .unwrap();
        assert_eq!(tls.cert_pem, std::path::Path::new("/etc/aks3/cert.pem"));
        assert_eq!(tls.key_pem, std::path::Path::new("/etc/aks3/key.pem"));
    }

    #[test]
    fn debug_output_hides_the_root_secret() {
        let (_dir, path) = config_file(FULL_TOML);
        let c = Config::load_from(Some(&path), stub_env(&[])).unwrap();
        let rendered = format!("{c:?}");
        assert!(rendered.contains("admin"));
        assert!(!rendered.contains("secretpassword"));
    }

    /// An environment made of a fixed list of pairs, for tests that must not
    /// touch the real one.
    fn stub_env<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |key| {
            pairs
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| (*v).to_owned())
        }
    }

    /// The process environment with every `AKS3_*` setting removed, restored
    /// when this guard drops.
    ///
    /// Only tests that go through [`Config::load`] itself need this. The
    /// environment is process-wide, so the guard also holds a lock: two of
    /// these running at once on different test threads would restore each
    /// other's saved values.
    #[test]
    fn virtual_hosted_style_is_off_by_default() {
        let env = [
            (ENV_ROOT_USER, "admin"),
            (ENV_ROOT_PASSWORD, "secretpassword"),
        ];
        let c = Config::load_from(None, stub_env(&env)).unwrap();
        assert!(c.virtual_host_domains.is_empty());
    }

    #[test]
    fn virtual_host_domains_come_from_the_file() {
        let (_dir, path) = config_file(
            r#"
            root_access_key = "admin"
            root_secret_key = "secretpassword"
            virtual_host_domains = ["S3.Example.com", "objects.example.net"]
        "#,
        );
        let c = Config::load_from(Some(&path), stub_env(&[])).unwrap();
        assert_eq!(
            c.virtual_host_domains,
            ["s3.example.com", "objects.example.net"]
        );
    }

    #[test]
    fn virtual_host_domains_in_the_environment_are_comma_separated_and_win() {
        let (_dir, path) = config_file(
            r#"
            root_access_key = "admin"
            root_secret_key = "secretpassword"
            virtual_host_domains = ["from.the.file"]
        "#,
        );
        let env = [(
            ENV_VIRTUAL_HOST_DOMAINS,
            " s3.example.com , objects.example.net ,",
        )];
        let c = Config::load_from(Some(&path), stub_env(&env)).unwrap();
        assert_eq!(
            c.virtual_host_domains,
            ["s3.example.com", "objects.example.net"]
        );
    }

    /// Compose passes an empty string for an unset variable, and an empty
    /// string here has to mean "off", not "one nameless domain".
    #[test]
    fn an_empty_virtual_host_domains_variable_means_off() {
        let env = [
            (ENV_ROOT_USER, "admin"),
            (ENV_ROOT_PASSWORD, "secretpassword"),
            (ENV_VIRTUAL_HOST_DOMAINS, " , "),
        ];
        let c = Config::load_from(None, stub_env(&env)).unwrap();
        assert!(c.virtual_host_domains.is_empty());
    }

    /// The likeliest mistake: pasting the port from an endpoint URL. A domain
    /// with a port matches no `Host` header, so it is caught at startup
    /// instead of quietly leaving every request path style.
    #[test]
    fn a_virtual_host_domain_with_a_port_is_rejected() {
        let env = [
            (ENV_ROOT_USER, "admin"),
            (ENV_ROOT_PASSWORD, "secretpassword"),
            (ENV_VIRTUAL_HOST_DOMAINS, "s3.example.com:9000"),
        ];
        let err = Config::load_from(None, stub_env(&env)).unwrap_err();
        assert!(
            matches!(&err, ConfigError::Invalid { setting, value, .. }
                if *setting == "virtual_host_domains" && value == "s3.example.com:9000"),
            "{err}"
        );
        assert!(err.to_string().contains("without a port"), "{err}");
    }

    #[test]
    fn a_malformed_virtual_host_domain_is_rejected() {
        for bad in [
            "s3..example.com",
            "s3.example.com/",
            "under_score.example.com",
        ] {
            let env = [
                (ENV_ROOT_USER, "admin"),
                (ENV_ROOT_PASSWORD, "secretpassword"),
                (ENV_VIRTUAL_HOST_DOMAINS, bad),
            ];
            let err = Config::load_from(None, stub_env(&env)).unwrap_err();
            assert!(
                matches!(&err, ConfigError::Invalid { setting, .. } if *setting == "virtual_host_domains"),
                "{bad} was accepted: {err}"
            );
        }
    }

    /// A host under both domains would name two different buckets, and which
    /// one it got would depend on the order they were written in.
    #[test]
    fn overlapping_virtual_host_domains_are_rejected() {
        let env = [
            (ENV_ROOT_USER, "admin"),
            (ENV_ROOT_PASSWORD, "secretpassword"),
            (ENV_VIRTUAL_HOST_DOMAINS, "example.com,s3.example.com"),
        ];
        let err = Config::load_from(None, stub_env(&env)).unwrap_err();
        assert!(err.to_string().contains("do not overlap"), "{err}");
    }

    struct ScrubbedEnv {
        _guard: std::sync::MutexGuard<'static, ()>,
        saved: Vec<(&'static str, Option<String>)>,
    }

    impl ScrubbedEnv {
        fn new() -> Self {
            static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
            // A test that panicked mid-scrub poisons the lock. The value is
            // `()`, so there is no corrupt state to protect against here.
            let guard = LOCK
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let names = [
                ENV_LISTEN,
                ENV_DATA_DIR,
                ENV_ROOT_USER,
                ENV_ROOT_PASSWORD,
                ENV_SHUTDOWN_GRACE,
                ENV_VIRTUAL_HOST_DOMAINS,
            ];
            let saved = names
                .iter()
                .map(|name| (*name, std::env::var(name).ok()))
                .collect();
            for name in names {
                std::env::remove_var(name);
            }
            Self {
                _guard: guard,
                saved,
            }
        }
    }

    impl Drop for ScrubbedEnv {
        fn drop(&mut self) {
            for (name, value) in &self.saved {
                match value {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        }
    }
}
