//! Конфигурация сервера: CLI → TOML → встроенные значения.

use clap::Parser;
use replication::{NodeConfig, Role};
use serde::Deserialize;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
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
    #[arg(long, help = "Идентификатор кластера; общий у лидера и реплик")]
    pub cluster_id: Option<String>,
    #[arg(long, help = "Роль узла: Leader или Replica")]
    pub role: Option<String>,
    #[arg(long, help = "Адрес лидера для Replica: IP:порт или имя:порт")]
    pub leader_addr: Option<String>,
    #[arg(long, help = "Ожидаемый node_id лидера для Replica")]
    pub leader_id: Option<String>,
    #[arg(long, help = "Интервал синхронизации реплики в миллисекундах")]
    pub sync_interval_ms: Option<u64>,
    #[arg(long)]
    pub max_body_bytes: Option<usize>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    listen_addr: Option<String>,
    data_dir: Option<PathBuf>,
    node_id: Option<String>,
    cluster_id: Option<String>,
    role: Option<String>,
    leader_addr: Option<String>,
    leader_id: Option<String>,
    sync_interval_ms: Option<u64>,
    max_body_bytes: Option<usize>,
    cluster: Option<ClusterConfig>,
}

/// Фиксированный состав Raft; DNS разрешается только при сетевом соединении.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    pub id: u64,
    #[serde(deserialize_with = "deserialize_members")]
    pub members: BTreeMap<u64, String>,
}

fn deserialize_members<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<BTreeMap<u64, String>, D::Error> {
    let members = BTreeMap::<String, String>::deserialize(deserializer)?;
    members
        .into_iter()
        .map(|(key, address)| {
            let id = key.parse::<u64>().map_err(serde::de::Error::custom)?;
            if id.to_string() != key {
                return Err(serde::de::Error::custom("member id must be canonical"));
            }
            Ok((id, address))
        })
        .collect()
}

#[derive(Debug, Clone)]
pub struct Config {
    pub node: NodeConfig,
    pub data_dir: PathBuf,
    pub max_body_bytes: usize,
    pub cluster: Option<ClusterConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node: NodeConfig {
                node_id: "node-1".into(),
                cluster_id: "miniclick".into(),
                listen_addr: "127.0.0.1:8123".into(),
                role: Role::Leader,
                sync_interval_ms: 1000,
            },
            data_dir: PathBuf::from("./data"),
            max_body_bytes: 256 * 1024 * 1024,
            cluster: None,
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
        let mut config = Self {
            cluster: file.cluster,
            ..Self::default()
        };
        if let Some(addr) = cli.listen_addr.or(file.listen_addr) {
            config.node.listen_addr = addr;
        }
        if let Some(id) = cli.node_id.or(file.node_id) {
            config.node.node_id = id;
        }
        if let Some(cluster) = cli.cluster_id.or(file.cluster_id) {
            config.node.cluster_id = cluster;
        }
        let data_dir = cli.data_dir.or(file.data_dir).unwrap_or(config.data_dir);
        if data_dir.as_os_str().is_empty() {
            return Err(DbError::Syntax("data_dir must not be empty".into()));
        }
        config.data_dir = working_dir.join(data_dir);
        if let Some(limit) = cli.max_body_bytes.or(file.max_body_bytes) {
            config.max_body_bytes = limit;
        }
        if let Some(interval) = cli.sync_interval_ms.or(file.sync_interval_ms) {
            config.node.sync_interval_ms = interval;
        }
        if let Some(role) = cli.role.or(file.role) {
            config.node.role = parse_role(
                &role,
                cli.leader_addr.or(file.leader_addr),
                cli.leader_id.or(file.leader_id),
            )?;
        }
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if let Some(cluster) = &self.cluster {
            if cluster.members.len() != 3
                || !cluster.members.contains_key(&cluster.id)
                || cluster.members.contains_key(&0)
                || cluster.members.values().collect::<BTreeSet<_>>().len() != 3
                || matches!(self.node.role, Role::Replica { .. })
            {
                return Err(DbError::Syntax("cluster requires three distinct members, a local id and no static Replica role".into()));
            }
            for address in cluster.members.values() {
                validate_peer_address(address)?;
            }
        }
        self.node
            .listen_addr
            .parse::<SocketAddr>()
            .map_err(|_| DbError::Syntax("listen_addr must be an IP address and port".into()))?;
        if self.node.node_id.trim().is_empty()
            || self.node.cluster_id.trim().is_empty()
            || self.data_dir.as_os_str().is_empty()
            || self.max_body_bytes == 0
            || self.node.sync_interval_ms == 0
        {
            return Err(DbError::Syntax(
                "node_id, cluster_id, data_dir, max_body_bytes and sync_interval_ms must be nonempty/positive"
                    .into(),
            ));
        }
        if let Role::Replica {
            leader_addr,
            leader_id,
        } = &self.node.role
        {
            // Имя разрешается HTTP-клиентом при соединении: узел может появиться позже.
            validate_peer_address(leader_addr)?;
            if leader_id.trim().is_empty() {
                return Err(DbError::Syntax("leader_id must not be empty".into()));
            }
        }
        Ok(())
    }
}

fn validate_peer_address(address: &str) -> Result<()> {
    let authority = address
        .parse::<axum::http::uri::Authority>()
        .map_err(|_| DbError::Syntax("peer address must be a host and port".into()))?;
    if authority.host().is_empty() || authority.port_u16().is_none() || address.contains('@') {
        return Err(DbError::Syntax(
            "peer address must be a host and port".into(),
        ));
    }
    Ok(())
}

fn parse_role(role: &str, leader_addr: Option<String>, leader_id: Option<String>) -> Result<Role> {
    if role.eq_ignore_ascii_case("leader") {
        return Ok(Role::Leader);
    }
    if role.eq_ignore_ascii_case("replica") {
        let leader_addr = leader_addr
            .ok_or_else(|| DbError::Syntax("role Replica requires leader_addr".into()))?;
        let leader_id =
            leader_id.ok_or_else(|| DbError::Syntax("role Replica requires leader_id".into()))?;
        return Ok(Role::Replica {
            leader_addr,
            leader_id,
        });
    }
    Err(DbError::Syntax(format!("unknown role: {role}")))
}
