// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// Derived in part from MinIO (https://github.com/minio/minio), AGPL-3.0.
// SPDX-License-Identifier: AGPL-3.0-only

//! The `aks3` binary: read the settings, start the server, log why not.

use aks3_server::config::Config;
use aks3_server::serve::{self, ShutdownSignals};

/// The only flag the binary takes.
const CONFIG_FLAG: &str = "--config";

/// Usage line printed when the arguments do not parse.
const USAGE: &str = "usage: aks3 [--config <path>]";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // First, before anything that can be slow or can fail. A container is
    // stoppable from the moment it starts, and reading a config file or
    // sweeping a large store's temp directory is time in which a SIGTERM with
    // no handler installed is discarded rather than held: the process would
    // keep running until its supervisor gave up and sent SIGKILL. Registering
    // here costs nothing if the process goes on to exit on a bad config.
    let signals = ShutdownSignals::install()?;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config_path = config_path_from_args(std::env::args().skip(1))?;
    let config = Config::load(config_path.as_deref())?;

    // Nobody is waiting to be told the address; the log line in `run` is the
    // announcement.
    let (bound, _) = tokio::sync::oneshot::channel();
    serve::run(config, bound, signals.recv()).await
}

/// Pulls `--config <path>` out of the command line.
///
/// Anything else is an error rather than something to ignore: a mistyped flag
/// that started a server on defaults would be worse than one that did not
/// start at all.
fn config_path_from_args(
    mut args: impl Iterator<Item = String>,
) -> anyhow::Result<Option<std::path::PathBuf>> {
    let Some(flag) = args.next() else {
        return Ok(None);
    };
    anyhow::ensure!(flag == CONFIG_FLAG, "unexpected argument {flag}\n{USAGE}");
    let path = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("{CONFIG_FLAG} needs a path\n{USAGE}"))?;
    anyhow::ensure!(args.next().is_none(), "too many arguments\n{USAGE}");
    Ok(Some(path.into()))
}

#[cfg(test)]
mod tests {
    use super::config_path_from_args;

    /// The command line as `std::env::args().skip(1)` yields it.
    fn args(items: &[&str]) -> impl Iterator<Item = String> {
        items
            .iter()
            .map(|item| (*item).to_owned())
            .collect::<Vec<_>>()
            .into_iter()
    }

    #[test]
    fn no_arguments_means_no_config_file() {
        assert!(config_path_from_args(args(&[])).unwrap().is_none());
    }

    #[test]
    fn the_config_flag_yields_its_path() {
        let path = config_path_from_args(args(&["--config", "/etc/aks3.toml"]))
            .unwrap()
            .unwrap();
        assert_eq!(path, std::path::Path::new("/etc/aks3.toml"));
    }

    #[test]
    fn a_flag_without_a_path_is_an_error() {
        assert!(config_path_from_args(args(&["--config"])).is_err());
    }

    #[test]
    fn an_unknown_argument_is_an_error() {
        assert!(config_path_from_args(args(&["--verbose"])).is_err());
    }

    #[test]
    fn trailing_arguments_are_an_error() {
        assert!(config_path_from_args(args(&["--config", "a.toml", "b.toml"])).is_err());
    }
}
