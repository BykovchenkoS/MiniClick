//! Маршрутизация одной атомарной вставки на физический шард.

use crate::{
    config::ShardedTableConfig,
    error::{blocking, ApiError},
    formats::{parse_input, InputFormat},
    insert::InsertParams,
    AppState,
};
use axum::{
    body::Bytes,
    extract::{
        rejection::{BytesRejection, PathRejection, QueryRejection},
        Path, Query, State,
    },
    http::StatusCode,
};
use reqwest::{Client, Url};
use std::{collections::BTreeMap, time::Duration};
use types::{ColumnData, DataType, DbError, Engine, TableEntry, TableId};

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
    let mut entries = BTreeMap::new();
    for (&id, address) in &table.shards {
        let base =
            Url::parse(&format!("http://{address}/")).map_err(|error| shard_error(id, error))?;
        let metadata = client
            .get(
                base.join("replica/tables")
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
        let entry = tables
            .into_iter()
            .find(|entry| entry.definition.name == table.local_table)
            .ok_or_else(|| shard_error(id, format!("missing table {}", table.local_table)))?;
        if entry.definition.engine != Engine::MergeTree
            || entry
                .definition
                .schema
                .column(&table.key)
                .map(|col| col.data_type)
                != Some(DataType::UInt64)
        {
            return Err(shard_error(
                id,
                format!("expected MergeTree with UInt64 key {}", table.key),
            ));
        }
        entries.insert(id, (base, entry));
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

    let schema = first.definition.schema.clone();
    let table = table.clone();
    let input = body.clone();
    let shard = blocking(move || {
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
    .await?;
    let Some(id) = shard else {
        return Ok(());
    };

    let (mut url, entry) = entries
        .remove(&id)
        .ok_or_else(|| DbError::Internal("destination shard metadata is missing".into()))?;
    url.path_segments_mut()
        .map_err(|_| DbError::Internal("invalid shard URL".into()))?
        .pop_if_empty()
        .extend(["shard", "insert", entry.id.as_str()]);
    let response = client
        .post(url)
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
    blocking(move || {
        // ID связывает запись со схемой, прочитанной координатором до разбора тела.
        let table = app.catalog.get_table_by_id(&TableId::new(id))?;
        if table.definition().engine != Engine::MergeTree {
            return Err(DbError::Unsupported(
                "shard table must use MergeTree".into(),
            ));
        }
        let block = parse_input(params.format, &body, &table.definition().schema)?;
        table.insert(block)
    })
    .await?;
    Ok("Ok.\n")
}
