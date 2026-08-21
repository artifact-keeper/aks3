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

/// Environment variable naming the listen address.
pub const ENV_LISTEN: &str = "AKS3_LISTEN";
/// Environment variable naming the data directory.
pub const ENV_DATA_DIR: &str = "AKS3_DATA_DIR";
/// Environment variable naming the root access key id.
pub const ENV_ROOT_USER: &str = "AKS3_ROOT_USER";
/// Environment variable naming the root secret key.
pub const ENV_ROOT_PASSWORD: &str = "AKS3_ROOT_PASSWORD";

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
            tls: file.tls,
        })
    }
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
            .field("tls", &self.tls)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, ConfigError, ENV_DATA_DIR, ENV_LISTEN, ENV_ROOT_PASSWORD, ENV_ROOT_USER};

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
            let names = [ENV_LISTEN, ENV_DATA_DIR, ENV_ROOT_USER, ENV_ROOT_PASSWORD];
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
