//! Маршрутизация одной атомарной вставки на физический шард.

pub(crate) mod read;

use crate::{
    config::{RaftShardConfig, ShardedTableConfig},
    error::{blocking, ApiError},
    formats::{parse_input, InputFormat},
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
use serde::Deserialize;
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
    let target = if let Some(schema) = &table.schema {
        match insert_shard(table, schema, format, &body).await? {
            Some(id) => {
                let (url, entry) = load_shard(client, table, id).await?;
                Some((id, url, entry))
            }
            None => None,
        }
    } else {
        legacy_insert_target(client, table, format, &body).await?
    };
    let Some((id, url, entry)) = target else {
        return Ok(());
    };
    let response = client
        .post(table_url(url, "insert", &entry.id)?)
        .query(&[("format", format)])
        .body(body)
        .send()
        .await
        .map_err(|error| shard_error(id, format!("INSERT outcome unknown: {error}")))?;
    match response.status() {
        StatusCode::OK => Ok(()),
        StatusCode::PAYLOAD_TOO_LARGE => Err(ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!("shard {id} rejected the INSERT body size"),
        )),
        status => Err(shard_error(id, format!("INSERT returned {status}"))),
    }
}

async fn legacy_insert_target(
    client: &Client,
    table: &ShardedTableConfig,
    format: InputFormat,
    body: &Bytes,
) -> Result<Option<(u64, Url, TableEntry)>, ApiError> {
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

    let Some(id) = insert_shard(table, &first.definition.schema, format, body).await? else {
        return Ok(None);
    };
    let (url, entry) = entries
        .remove(&id)
        .ok_or_else(|| DbError::Internal("destination shard metadata is missing".into()))?;
    Ok(Some((id, url, entry)))
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
            base.join(metadata_path(group.is_some()))
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
        .json::<Vec<TableEntry>>()
        .await
        .map_err(|error| shard_error(id, error))?;
    let entry = physical_entry(tables, table).map_err(|error| shard_error(id, error))?;
    Ok((base, entry))
}

async fn insert_shard(
    table: &ShardedTableConfig,
    schema: &TableSchema,
    format: InputFormat,
    body: &Bytes,
) -> Result<Option<u64>, ApiError> {
    let schema = schema.clone();
    let table = table.clone();
    let input = body.clone();
    blocking(move || {
        let block = parse_input(format, &input, &schema)?;
        let Some(ColumnData::UInt64(keys)) = block.column(&table.key) else {
            return Err(DbError::Internal(
                "validated sharding key is missing".into(),
            ));
        };
        let Some(first) = keys.first() else {
            return Ok(None);
        };
        let (id, _) = table
            .shard_for_key(*first)
            .ok_or_else(|| DbError::Internal("sharded table has no destinations".into()))?;
        let first_index = table.shard_index(*first);
        if keys
            .iter()
            .skip(1)
            .any(|&key| table.shard_index(key) != first_index)
        {
            return Err(DbError::Unsupported(
                "INSERT must target a single shard".into(),
            ));
        }
        Ok(Some(id))
    })
    .await
}

fn metadata_path(replicated: bool) -> &'static str {
    if replicated {
        "shard/tables"
    } else {
        "replica/tables"
    }
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

fn physical_entry(
    tables: Vec<TableEntry>,
    table: &ShardedTableConfig,
) -> types::Result<TableEntry> {
    let entry = tables
        .into_iter()
        .find(|entry| entry.definition.name == table.local_table)
        .ok_or_else(|| DbError::UnknownTable(table.local_table.clone()))?;
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
        // ID связывает запись со схемой, прочитанной координатором до разбора тела.
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

pub(crate) async fn tables(State(app): State<AppState>) -> Result<Json<Vec<TableEntry>>, ApiError> {
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
                .filter(|entry| app.config.local_shards.contains_key(&entry.definition.name))
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
