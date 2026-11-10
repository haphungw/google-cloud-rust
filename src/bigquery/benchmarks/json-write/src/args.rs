// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use clap::{Parser, Subcommand};

#[derive(Parser, Debug, Clone)]
#[command(
    author,
    version,
    about = "BigQuery Storage Write API JSON Encoding & Ingestion Benchmark",
    long_about = None
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Command {
    /// Offline CPU and wire-size benchmark (no network or GCP credentials required).
    Local(LocalConfig),
    /// Live end-to-end throughput benchmark against BigQuery Storage Write API.
    Live(LiveConfig),
}

#[derive(Parser, Debug, Clone)]
pub struct LocalConfig {
    /// Number of timed measurement samples per batch size and encoder.
    #[arg(long, default_value_t = 9)]
    pub samples: usize,

    /// Total rows encoded per sample for the 2-column `json_default.rs` schema.
    #[arg(long, default_value_t = 60_000)]
    pub simple_rows: usize,

    /// Total rows encoded per sample for the 11-column realistic BigQuery schema.
    #[arg(long, default_value_t = 40_000)]
    pub realistic_rows: usize,
}

#[derive(Parser, Debug, Clone)]
pub struct LiveConfig {
    /// Google Cloud project ID for temporary benchmark tables.
    #[arg(long, default_value = "", env = "GOOGLE_CLOUD_PROJECT")]
    pub project: String,

    /// Number of interleaved fresh-connection rounds per scenario.
    #[arg(long, default_value_t = 5)]
    pub rounds: usize,

    /// Existing dataset ID to use; if empty, creates and deletes a temporary dataset.
    #[arg(long, default_value = "")]
    pub dataset_id: String,
}

pub fn parse_args() -> Cli {
    Cli::parse()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_local_defaults() -> anyhow::Result<()> {
        let cli = Cli::try_parse_from(["cmd", "local"])?;
        let Command::Local(cfg) = cli.command else {
            anyhow::bail!("expected local command");
        };
        assert_eq!(cfg.samples, 9);
        assert_eq!(cfg.simple_rows, 60_000);
        assert_eq!(cfg.realistic_rows, 40_000);
        Ok(())
    }

    #[test]
    fn parse_local_custom() -> anyhow::Result<()> {
        let cli = Cli::try_parse_from([
            "cmd",
            "local",
            "--samples",
            "5",
            "--simple-rows",
            "10000",
            "--realistic-rows",
            "8000",
        ])?;
        let Command::Local(cfg) = cli.command else {
            anyhow::bail!("expected local command");
        };
        assert_eq!(cfg.samples, 5);
        assert_eq!(cfg.simple_rows, 10_000);
        assert_eq!(cfg.realistic_rows, 8_000);
        Ok(())
    }

    #[test]
    fn parse_live_custom() -> anyhow::Result<()> {
        let cli = Cli::try_parse_from([
            "cmd",
            "live",
            "--project",
            "test-project",
            "--rounds",
            "3",
            "--dataset-id",
            "custom_ds",
        ])?;
        let Command::Live(cfg) = cli.command else {
            anyhow::bail!("expected live command");
        };
        assert_eq!(cfg.project, "test-project");
        assert_eq!(cfg.rounds, 3);
        assert_eq!(cfg.dataset_id, "custom_ds");
        Ok(())
    }
}
