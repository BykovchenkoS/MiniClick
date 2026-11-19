//! Конфигурация сервера: CLI → TOML → встроенные значения.

use clap::Parser;
use replication::{NodeConfig, Role};
use serde::Deserialize;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use types::{DataType, DbError, Result, TableSchema};

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
    #[serde(default)]
    sharding: BTreeMap<String, ShardedTableConfig>,
    #[serde(default)]
    local_shards: BTreeMap<String, LocalShardConfig>,
}

/// Фиксированный состав Raft; DNS разрешается только при сетевом соединении.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    pub id: u64,
    #[serde(deserialize_with = "deserialize_addresses")]
    pub members: BTreeMap<u64, String>,
}

/// Размещение одной логической таблицы на независимых узлах.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardedTableConfig {
    pub local_table: String,
    pub key: String,
    /// Схема координатора позволяет выбрать шард до обращения к сети.
    pub schema: Option<TableSchema>,
    #[serde(deserialize_with = "deserialize_addresses")]
    pub shards: BTreeMap<u64, String>,
    #[serde(default, deserialize_with = "deserialize_id_map")]
    pub raft_shards: BTreeMap<u64, RaftShardConfig>,
}

/// Адреса участников Raft-группы одного шарда и её ожидаемая идентичность.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftShardConfig {
    pub cluster_id: String,
    pub members: Vec<String>,
}

impl ShardedTableConfig {
    /// Ключ UInt64 задаёт индекс в порядке возрастания ID шардов.
    pub fn shard_for_key(&self, key: u64) -> Option<(u64, &str)> {
        let index = self.shard_index(key)?;
        self.shards
            .iter()
            .nth(index)
            .map(|(&id, address)| (id, address.as_str()))
    }

    pub(crate) fn shard_index(&self, key: u64) -> Option<usize> {
        shard_index(key, self.shards.len())
    }
}

/// Размещение физической таблицы на этом узле. Порядок ID в TOML произвольный.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalShardConfig {
    pub key: String,
    pub shard_id: u64,
    pub shard_ids: Vec<u64>,
}

impl LocalShardConfig {
    /// Позиция локального шарда в том же порядке ID, который использует координатор.
    pub(crate) fn index(&self) -> usize {
        self.shard_ids
            .iter()
            .filter(|&&id| id < self.shard_id)
            .count()
    }

    pub(crate) fn shard_index(&self, key: u64) -> Option<usize> {
        shard_index(key, self.shard_ids.len())
    }
}

fn shard_index(key: u64, count: usize) -> Option<usize> {
    key.checked_rem(count as u64).map(|index| index as usize)
}

fn deserialize_addresses<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<BTreeMap<u64, String>, D::Error> {
    deserialize_id_map(deserializer)
}

fn deserialize_id_map<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> std::result::Result<BTreeMap<u64, T>, D::Error> {
    let members = BTreeMap::<String, T>::deserialize(deserializer)?;
    members
        .into_iter()
        .map(|(key, address)| {
            let id = key.parse::<u64>().map_err(serde::de::Error::custom)?;
            if id.to_string() != key {
                return Err(serde::de::Error::custom("id must be canonical"));
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
    /// Карта логических таблиц по регистрозависимым именам.
    pub sharding: BTreeMap<String, ShardedTableConfig>,
    /// Правила размещения по именам физических таблиц.
    pub local_shards: BTreeMap<String, LocalShardConfig>,
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
            sharding: BTreeMap::new(),
            local_shards: BTreeMap::new(),
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
            sharding: file.sharding,
            local_shards: file.local_shards,
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
        if !self.local_shards.is_empty() && matches!(self.node.role, Role::Replica { .. }) {
            return Err(DbError::Unsupported(
                "local shard placement requires a writable node".into(),
            ));
        }
        for (name, table) in &self.local_shards {
            let ids: BTreeSet<_> = table.shard_ids.iter().copied().collect();
            if name.trim().is_empty()
                || table.key.trim().is_empty()
                || ids.contains(&0)
                || !ids.contains(&table.shard_id)
                || ids.len() != table.shard_ids.len()
                || self.sharding.contains_key(name)
            {
                return Err(DbError::Syntax(format!(
                    "local shard table {name:?} requires a key, distinct positive shard IDs including its own, and a name distinct from logical tables"
                )));
            }
        }
        if !self.sharding.is_empty()
            && (self.cluster.is_some() || matches!(self.node.role, Role::Replica { .. }))
        {
            return Err(DbError::Unsupported(
                "sharding coordinator requires a standalone leader".into(),
            ));
        }
        for (name, table) in &self.sharding {
            if name.trim().is_empty()
                || table.local_table.trim().is_empty()
                || table.key.trim().is_empty()
                || table.shards.is_empty()
                || table.shards.contains_key(&0)
                || table.shards.values().collect::<BTreeSet<_>>().len() != table.shards.len()
            {
                return Err(DbError::Syntax(format!(
                    "sharding table {name:?} requires nonempty names and shards with positive IDs and distinct addresses"
                )));
            }
            if let Some(schema) = &table.schema {
                let mut columns = BTreeSet::new();
                if schema
                    .columns
                    .iter()
                    .any(|column| column.name.trim().is_empty() || !columns.insert(&column.name))
                    || schema.column(&table.key).map(|column| column.data_type)
                        != Some(DataType::UInt64)
                {
                    return Err(DbError::Syntax(format!(
                        "sharding table {name:?} requires distinct nonempty column names and a UInt64 shard key"
                    )));
                }
            }
            for (id, address) in &table.shards {
                validate_peer_address(address).map_err(|error| {
                    DbError::Syntax(format!("sharding table {name:?}, shard {id}: {error}"))
                })?;
            }
            let mut cluster_ids = BTreeSet::new();
            let mut addresses: BTreeSet<_> = table.shards.values().collect();
            for (id, group) in &table.raft_shards {
                if group.cluster_id.trim().is_empty()
                    || group.members.len() != 3
                    || group.members.iter().collect::<BTreeSet<_>>().len() != 3
                    || !table
                        .shards
                        .get(id)
                        .is_some_and(|address| group.members.contains(address))
                {
                    return Err(DbError::Syntax(format!(
                        "sharding table {name:?}, shard {id}: Raft requires a cluster ID and three distinct members including the configured shard address"
                    )));
                }
                if !cluster_ids.insert(&group.cluster_id) {
                    return Err(DbError::Syntax(format!(
                        "sharding table {name:?}, shard {id}: Raft cluster ID is already used by another shard"
                    )));
                }
                for address in &group.members {
                    validate_peer_address(address)?;
                    if table.shards.get(id) != Some(address) && !addresses.insert(address) {
                        return Err(DbError::Syntax(format!(
                            "sharding table {name:?}, shard {id}: Raft address {address} is already used by another shard"
                        )));
                    }
                }
            }
        }
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
