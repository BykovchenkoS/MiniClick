//! Распределение INSERT по физическим шардам с атомарностью на каждом шарде.

pub(crate) mod read;
pub(crate) mod topology;

use crate::{
    config::{LocalShardConfig, RaftShardConfig, ShardedTableConfig},
    error::{blocking, ApiError},
    formats::{format_insert_tsv, parse_input, InputFormat},
    insert::{insert_block, InsertParams},
    AppState,
};
use axum::{
    body::Bytes,
    extract::{
        rejection::{BytesRejection, PathRejection, QueryRejection},
        Path, Query, State,
    },
    http::StatusCode,
    Json,
};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, time::Duration};
use types::{ColumnData, DataType, DbError, Engine, TableEntry, TableId, TableSchema};

pub(crate) fn client() -> types::Result<Client> {
    Client::builder()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .no_proxy()
        .build()
        .map_err(|error| DbError::Internal(format!("shard HTTP client: {error}")))
}

pub(crate) async fn insert(
    client: &Client,
    table: &ShardedTableConfig,
    format: InputFormat,
    body: Bytes,
) -> Result<(), ApiError> {
    if body.is_empty() {
        return Ok(());
    }
    let mut entries = if table.schema.is_some() {
        BTreeMap::new()
    } else {
        legacy_insert_entries(client, table).await?
    };
    let schema = table
        .schema
        .as_ref()
        .or_else(|| {
            entries
                .values()
                .next()
                .map(|(_, entry)| &entry.definition.schema)
        })
        .ok_or_else(|| DbError::Internal("sharded table has no destinations".into()))?;
    let (format, batches) = split_insert(table, schema, format, body).await?;
    let mut requests = Vec::with_capacity(batches.len());
    for (id, body) in batches {
        let (url, entry) = match entries.remove(&id) {
            Some(entry) => entry,
            None => load_shard(client, table, id).await?,
        };
        let url = table_url(url, "insert", &entry.id).map_err(|error| shard_error(id, error))?;
        requests.push((id, client.post(url).query(&[("format", format)]).body(body)));
    }

    let mut confirmed = Vec::with_capacity(requests.len());
    for (id, request) in requests {
        let outcome = match request.send().await {
            Ok(response) if response.status() == StatusCode::OK => {
                confirmed.push(id);
                continue;
            }
            Ok(response) if response.status() == StatusCode::PAYLOAD_TOO_LARGE => {
                let message = format!("shard {id} rejected the INSERT body size");
                return Err(ApiError::new(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    if confirmed.is_empty() {
                        message
                    } else {
                        format!("{message}; partial INSERT, confirmed shards: {confirmed:?}")
                    },
                ));
            }
            Ok(response) => format!("HTTP {}", response.status()),
            Err(error) => error.to_string(),
        };
        return Err(shard_error(
            id,
            format!("INSERT outcome unknown ({outcome}); confirmed shards: {confirmed:?}"),
        ));
    }
    Ok(())
}

async fn legacy_insert_entries(
    client: &Client,
    table: &ShardedTableConfig,
) -> Result<BTreeMap<u64, (Url, TableEntry)>, ApiError> {
    let mut entries = BTreeMap::new();
    for &id in table.shards.keys() {
        entries.insert(id, load_shard(client, table, id).await?);
    }
    let (&first_id, (_, first)) = entries
        .first_key_value()
        .ok_or_else(|| DbError::Internal("sharded table has no destinations".into()))?;
    for (&id, (_, entry)) in entries.iter().skip(1) {
        if first.definition.schema != entry.definition.schema {
            return Err(shard_error(
                id,
                format!("schema differs from shard {first_id}"),
            ));
        }
    }

    Ok(entries)
}

async fn load_shard(
    client: &Client,
    table: &ShardedTableConfig,
    id: u64,
) -> Result<(Url, TableEntry), ApiError> {
    let group = table.raft_shards.get(&id);
    let base = match group {
        Some(group) => leader(client, group)
            .await
            .map_err(|error| shard_error(id, error))?,
        None => {
            let address = table
                .shards
                .get(&id)
                .ok_or_else(|| DbError::Internal("destination shard address is missing".into()))?;
            shard_url(address).map_err(|error| shard_error(id, error))?
        }
    };
    let metadata = client
        .get(
            base.join("shard/tables")
                .map_err(|error| shard_error(id, error))?,
        )
        .send()
        .await
        .map_err(|error| shard_error(id, error))?;
    if metadata.status() != StatusCode::OK {
        return Err(shard_error(
            id,
            format!("metadata returned {}", metadata.status()),
        ));
    }
    let tables = metadata
        .json::<Vec<ShardTableEntry>>()
        .await
        .map_err(|error| shard_error(id, error))?;
    let entry = physical_entry(tables, table, id).map_err(|error| shard_error(id, error))?;
    Ok((base, entry))
}

async fn split_insert(
    table: &ShardedTableConfig,
    schema: &TableSchema,
    format: InputFormat,
    body: Bytes,
) -> Result<(InputFormat, Vec<(u64, Bytes)>), ApiError> {
    let schema = schema.clone();
    let table = table.clone();
    blocking(move || {
        let block = parse_input(format, &body, &schema)?;
        let Some(ColumnData::UInt64(keys)) = block.column(&table.key) else {
            return Err(DbError::Internal(
                "validated sharding key is missing".into(),
            ));
        };
        let shard_ids: Vec<_> = table.shards.keys().copied().collect();
        let mut rows_by_shard = BTreeMap::<u64, Vec<usize>>::new();
        for (row, &key) in keys.iter().enumerate() {
            let index = table
                .shard_index(key)
                .ok_or_else(|| DbError::Internal("sharded table has no destinations".into()))?;
            rows_by_shard.entry(shard_ids[index]).or_default().push(row);
        }
        if let Some((&id, _)) = rows_by_shard
            .first_key_value()
            .filter(|(_, rows)| rows.len() == block.rows())
        {
            return Ok((format, vec![(id, body)]));
        }
        let batches = rows_by_shard
            .into_iter()
            .map(|(id, rows)| (id, Bytes::from(format_insert_tsv(&block.take(&rows)))))
            .collect();
        Ok((InputFormat::Tsv, batches))
    })
    .await
}

#[derive(Deserialize)]
struct MemberStatus {
    id: u64,
    cluster_id: String,
    role: String,
    leader: Option<u64>,
    healthy: bool,
}

impl MemberStatus {
    fn is_leader_of(&self, group: &RaftShardConfig) -> bool {
        self.healthy
            && self.cluster_id == group.cluster_id
            && self.role == "Leader"
            && self.leader == Some(self.id)
    }
}

async fn leader(client: &Client, group: &RaftShardConfig) -> types::Result<Url> {
    for address in &group.members {
        let base = shard_url(address)?;
        let status = base
            .join("cluster/status")
            .map_err(|error| DbError::Internal(error.to_string()))?;
        if let Ok(response) = client
            .get(status)
            .timeout(Duration::from_secs(1))
            .send()
            .await
        {
            if response.status() == StatusCode::OK
                && response
                    .json::<MemberStatus>()
                    .await
                    .is_ok_and(|status| status.is_leader_of(group))
            {
                return Ok(base);
            }
        }
    }
    Err(no_leader(group))
}

fn no_leader(group: &RaftShardConfig) -> DbError {
    DbError::Io(std::io::Error::other(format!(
        "no healthy leader for Raft group {}",
        group.cluster_id
    )))
}

fn shard_url(address: &str) -> types::Result<Url> {
    Url::parse(&format!("http://{address}/"))
        .map_err(|error| DbError::Syntax(format!("invalid shard URL: {error}")))
}

fn table_url(mut base: Url, operation: &str, id: &TableId) -> types::Result<Url> {
    base.path_segments_mut()
        .map_err(|_| DbError::Internal("invalid shard URL".into()))?
        .pop_if_empty()
        .extend(["shard", operation, id.as_str()]);
    Ok(base)
}

#[derive(Deserialize, Serialize)]
pub(crate) struct ShardTableEntry {
    #[serde(flatten)]
    entry: TableEntry,
    placement: LocalShardConfig,
}

fn physical_entry(
    tables: Vec<ShardTableEntry>,
    table: &ShardedTableConfig,
    shard_id: u64,
) -> types::Result<TableEntry> {
    let ShardTableEntry {
        entry,
        mut placement,
    } = tables
        .into_iter()
        .find(|metadata| metadata.entry.definition.name == table.local_table)
        .ok_or_else(|| {
            DbError::TypeMismatch(format!(
                "physical table {:?} is missing or has no shard placement",
                table.local_table
            ))
        })?;
    placement.shard_ids.sort_unstable();
    if placement.key != table.key
        || placement.shard_id != shard_id
        || !placement.shard_ids.iter().eq(table.shards.keys())
    {
        return Err(DbError::TypeMismatch(format!(
            "shard placement mismatch for {:?}: expected key {:?}, shard ID {shard_id} and shard IDs {:?}; got {placement:?}",
            table.local_table, table.key, table.shards.keys().collect::<Vec<_>>(),
        )));
    }
    if entry.definition.engine != Engine::MergeTree
        || entry
            .definition
            .schema
            .column(&table.key)
            .map(|col| col.data_type)
            != Some(DataType::UInt64)
    {
        return Err(DbError::TypeMismatch(format!(
            "expected MergeTree with UInt64 key {}",
            table.key
        )));
    }
    if table
        .schema
        .as_ref()
        .is_some_and(|schema| *schema != entry.definition.schema)
    {
        return Err(DbError::TypeMismatch(
            "schema differs from the coordinator configuration".into(),
        ));
    }
    Ok(entry)
}

fn shard_error(id: u64, error: impl std::fmt::Display) -> ApiError {
    let message = format!("shard {id}: {error}");
    tracing::warn!(%message, "Ошибка обращения к шарду");
    ApiError::new(StatusCode::BAD_GATEWAY, message)
}

pub(crate) async fn insert_local(
    State(app): State<AppState>,
    path: Result<Path<String>, PathRejection>,
    params: Result<Query<InsertParams>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<&'static str, ApiError> {
    let Path(id) = path?;
    let Query(params) = params?;
    let body = body?;
    if let Some(cluster) = &app.cluster {
        cluster
            .write(crate::cluster::types::Command::ShardInsert {
                table: TableId::new(id),
                format: params.format,
                body: body.to_vec(),
            })
            .await?;
        return Ok("Ok.\n");
    }
    blocking(move || {
        // ID связывает запись с физической таблицей, схему которой сверил координатор.
        let table = app.catalog.get_table_by_id(&TableId::new(id))?;
        if table.definition().engine != Engine::MergeTree {
            return Err(DbError::Unsupported(
                "shard table must use MergeTree".into(),
            ));
        }
        insert_block(&app, table.as_ref(), params.format, &body)
    })
    .await?;
    Ok("Ok.\n")
}

pub(crate) async fn tables(
    State(app): State<AppState>,
) -> Result<Json<Vec<ShardTableEntry>>, ApiError> {
    read_barrier(&app).await?;
    blocking(move || {
        let _view = app
            .view_gate
            .read()
            .map_err(|_| DbError::Internal("view lock poisoned".into()))?;
        app.check_view()?;
        Ok(Json(
            app.catalog
                .table_entries()
                .into_iter()
                .filter_map(|entry| {
                    let placement = app.config.local_shards.get(&entry.definition.name)?.clone();
                    Some(ShardTableEntry { entry, placement })
                })
                .collect(),
        ))
    })
    .await
}

async fn read_barrier(app: &AppState) -> Result<(), ApiError> {
    if let Some(cluster) = &app.cluster {
        cluster.read_barrier().await?;
    }
    Ok(())
}
