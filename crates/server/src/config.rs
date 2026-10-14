//! Конфигурация сервера: CLI → TOML → встроенные значения.

use clap::Parser;
use replication::{NodeConfig, Role};
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use types::{DbError, Result};

#[derive(Debug, Parser)]
#[command(
    name = "miniclick",
    version,
    about = "Учебная колоночная СУБД MiniClick"
)]
pub struct Cli {
    #[arg(
        long,
        help = "Путь к TOML-конфигурации; по умолчанию ./config.toml, если он существует"
    )]
    pub config: Option<PathBuf>,
    #[arg(long)]
    pub listen_addr: Option<String>,
    #[arg(long)]
    pub data_dir: Option<PathBuf>,
    #[arg(long)]
    pub node_id: Option<String>,
    #[arg(long, help = "В этой лабораторной поддерживается только Leader")]
    pub role: Option<String>,
    #[arg(long)]
    pub max_body_bytes: Option<usize>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    listen_addr: Option<String>,
    data_dir: Option<PathBuf>,
    node_id: Option<String>,
    role: Option<String>,
    max_body_bytes: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub node: NodeConfig,
    pub data_dir: PathBuf,
    pub max_body_bytes: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node: NodeConfig {
                node_id: "node-1".into(),
                listen_addr: "127.0.0.1:8123".into(),
                role: Role::Leader,
                sync_interval_ms: 1000,
            },
            data_dir: PathBuf::from("./data"),
            max_body_bytes: 256 * 1024 * 1024,
        }
    }
}

impl Config {
    /// Относительные пути разрешаются от рабочего каталога, включая путь из TOML.
    pub fn load(cli: Cli, working_dir: &Path) -> Result<Self> {
        let explicit = cli.config.is_some();
        let path = working_dir.join(cli.config.unwrap_or_else(|| PathBuf::from("config.toml")));
        let file = if explicit || path.exists() {
            let text = std::fs::read_to_string(path)?;
            toml::from_str::<FileConfig>(&text)
                .map_err(|error| DbError::Syntax(format!("invalid config: {error}")))?
        } else {
            FileConfig::default()
        };
        let mut config = Self::default();
        if let Some(addr) = cli.listen_addr.or(file.listen_addr) {
            config.node.listen_addr = addr;
        }
        if let Some(id) = cli.node_id.or(file.node_id) {
            config.node.node_id = id;
        }
        let data_dir = cli.data_dir.or(file.data_dir).unwrap_or(config.data_dir);
        if data_dir.as_os_str().is_empty() {
            return Err(DbError::Syntax("data_dir must not be empty".into()));
        }
        config.data_dir = working_dir.join(data_dir);
        if let Some(limit) = cli.max_body_bytes.or(file.max_body_bytes) {
            config.max_body_bytes = limit;
        }
        if let Some(role) = cli.role.or(file.role) {
            if !role.eq_ignore_ascii_case("leader") {
                return Err(DbError::Unsupported(
                    "only role Leader is supported in this lab".into(),
                ));
            }
        }
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        self.node
            .listen_addr
            .parse::<SocketAddr>()
            .map_err(|_| DbError::Syntax("listen_addr must be an IP address and port".into()))?;
        if self.node.node_id.trim().is_empty()
            || self.data_dir.as_os_str().is_empty()
            || self.max_body_bytes == 0
        {
            return Err(DbError::Syntax(
                "node_id, data_dir and max_body_bytes must be nonempty/positive".into(),
            ));
        }
        if !matches!(self.node.role, Role::Leader) {
            return Err(DbError::Unsupported(
                "only role Leader is supported in this lab".into(),
            ));
        }
        Ok(())
    }
}
