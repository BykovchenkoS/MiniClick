//! Неизменяемая карта размещения в каталоге данных координатора или обычного шарда.

use crate::config::{Config, LocalShardConfig};
use serde::Serialize;
use std::{collections::BTreeMap, fs, io::Write};
use types::{Catalog, ColumnData, DbError, Engine, Result, ScanRequest};

pub(crate) const FILE_NAME: &str = "sharding-topology.json";

#[derive(Serialize)]
struct Topology<'a> {
    version: u32,
    tables: BTreeMap<&'a str, LogicalPlacement<'a>>,
    local_shards: BTreeMap<String, LocalShardConfig>,
}

#[derive(Serialize)]
struct LogicalPlacement<'a> {
    local_table: &'a str,
    key: &'a str,
    shards: BTreeMap<u64, Destination<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum Destination<'a> {
    Standalone(&'a str),
    Raft {
        cluster_id: &'a str,
        members: Vec<&'a str>,
    },
}

/// Вызывается до запуска HTTP под блокировкой открытого DiskCatalog.
pub(crate) fn check(config: &Config, catalog: &dyn Catalog) -> Result<()> {
    let path = config.data_dir.join(FILE_NAME);
    let stored = match fs::read(&path) {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    if stored.is_none() && config.sharding.is_empty() && config.local_shards.is_empty() {
        return Ok(());
    }

    let topology = document(config)?;
    let upgrading = stored.is_some();
    if let Some(stored) = stored {
        let mut stored: serde_json::Value = serde_json::from_slice(&stored).map_err(|error| {
            DbError::Syntax(format!(
                "invalid saved sharding topology {}: {error}",
                path.display()
            ))
        })?;
        if stored == topology {
            return Ok(());
        }
        if stored.get("version").and_then(serde_json::Value::as_u64) == Some(1) {
            stored["version"] = 2.into();
        }
        if stored != topology {
            return Err(DbError::Syntax(format!(
                "sharding topology differs from saved placement at {}; restore the previous topology or migrate the data",
                path.display()
            )));
        }
    }

    validate_existing_rows(config, catalog)?;
    let mut file = tempfile::NamedTempFile::new_in(&config.data_dir)?;
    serde_json::to_writer_pretty(&mut file, &topology).map_err(std::io::Error::other)?;
    file.write_all(b"\n")?;
    file.as_file().sync_all()?;
    if upgrading {
        file.persist(path).map_err(|error| error.error)?;
    } else {
        file.persist_noclobber(path).map_err(|error| error.error)?;
    }
    #[cfg(unix)]
    fs::File::open(&config.data_dir)?.sync_all()?;
    Ok(())
}

pub(super) fn document(config: &Config) -> Result<serde_json::Value> {
    let tables = config
        .sharding
        .iter()
        .map(|(name, table)| {
            let shards = table.shards.iter().map(|(&id, address)| {
                let destination = match table.raft_shards.get(&id) {
                    Some(group) => {
                        let mut members: Vec<_> =
                            group.members.iter().map(String::as_str).collect();
                        members.sort_unstable();
                        Destination::Raft {
                            cluster_id: &group.cluster_id,
                            members,
                        }
                    }
                    None => Destination::Standalone(address),
                };
                (id, destination)
            });
            (
                name.as_str(),
                LogicalPlacement {
                    local_table: &table.local_table,
                    key: &table.key,
                    shards: shards.collect(),
                },
            )
        })
        .collect();
    let mut local_shards = config.local_shards.clone();
    for placement in local_shards.values_mut() {
        placement.shard_ids.sort_unstable();
    }
    serde_json::to_value(Topology {
        version: 2,
        tables,
        local_shards,
    })
    .map_err(|error| DbError::Internal(format!("sharding topology serialization: {error}")))
}

fn validate_existing_rows(config: &Config, catalog: &dyn Catalog) -> Result<()> {
    for (name, placement) in &config.local_shards {
        let table = match catalog.get_table(name) {
            Ok(table) => table,
            Err(DbError::UnknownTable(_)) => continue,
            Err(error) => return Err(error),
        };
        let request = ScanRequest {
            columns: vec![placement.key.clone()],
            predicate: None,
        };
        if table.definition().engine != Engine::MergeTree {
            return Err(DbError::TypeMismatch(format!(
                "shard table {name:?} must use MergeTree"
            )));
        }
        for block in table.scan(&request)? {
            let block = block?;
            let Some(ColumnData::UInt64(keys)) = block.column(&placement.key) else {
                return Err(DbError::TypeMismatch(format!(
                    "shard key {} of table {name:?} must be UInt64",
                    placement.key
                )));
            };
            let own = Some(placement.index());
            if let Some(key) = keys.iter().find(|&&key| placement.shard_index(key) != own) {
                return Err(DbError::Syntax(format!(
                    "existing rows of shard table {name:?} require migration: key {}={key} does not belong to shard {}",
                    placement.key, placement.shard_id
                )));
            }
        }
    }
    Ok(())
}
