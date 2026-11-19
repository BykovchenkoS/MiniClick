//! Адаптер распределённой таблицы для синхронного планировщика и исполнителя.

mod wire;

use super::{physical_entry, shard_url, table_url};
use crate::{
    config::ShardedTableConfig,
    error::{blocking, ApiError},
    meta::ScanInfo,
    AppState,
};
use axum::{
    body::Body,
    extract::{rejection::JsonRejection, Path, State},
    http::header,
    response::{IntoResponse, Response},
    Json,
};
use reqwest::{blocking::Client, Url};
use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Lines},
    sync::Arc,
    time::Duration,
};
use types::{
    Block, BlockStream, Catalog, ColumnDef, DbError, Engine, Result, ScanRequest, ScanStats, Table,
    TableDefinition, TableEntry, TableId,
};
use wire::Frame;

pub(crate) struct ShardedCatalog {
    local: Arc<dyn Catalog>,
    tables: BTreeMap<String, ShardedTableConfig>,
}

impl ShardedCatalog {
    pub(crate) fn new(
        local: Arc<dyn Catalog>,
        tables: BTreeMap<String, ShardedTableConfig>,
    ) -> Self {
        Self { local, tables }
    }
}

impl Catalog for ShardedCatalog {
    fn create_table(&self, definition: TableDefinition, if_not_exists: bool) -> Result<()> {
        if self.tables.contains_key(&definition.name) {
            return Err(sharded_ddl());
        }
        self.local.create_table(definition, if_not_exists)
    }

    fn drop_table(&self, name: &str, if_exists: bool) -> Result<()> {
        if self.tables.contains_key(name) {
            return Err(sharded_ddl());
        }
        self.local.drop_table(name, if_exists)
    }

    fn get_table(&self, name: &str) -> Result<Arc<dyn Table>> {
        match self.tables.get(name) {
            Some(config) => Ok(Arc::new(ShardedTable::open(name, config)?)),
            None => self.local.get_table(name),
        }
    }

    fn list_tables(&self) -> Vec<TableDefinition> {
        self.local.list_tables()
    }
}

fn sharded_ddl() -> DbError {
    DbError::Unsupported("DDL for sharded tables".into())
}

struct ShardedTable {
    definition: TableDefinition,
    endpoints: Vec<Shard>,
    client: Client,
}

#[derive(Clone)]
struct Shard {
    id: u64,
    base: Url,
    table: TableId,
}

impl ShardedTable {
    fn open(name: &str, config: &ShardedTableConfig) -> Result<Self> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(60))
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .map_err(|error| DbError::Internal(format!("shard HTTP client: {error}")))?;
        let mut definition: Option<TableDefinition> = None;
        let mut endpoints = Vec::with_capacity(config.shards.len());
        for (&id, address) in &config.shards {
            let base = shard_url(address)?;
            let metadata = base
                .join("replica/tables")
                .map_err(|error| shard_error(id, error))?;
            let tables = client
                .get(metadata)
                .send()
                .and_then(|response| response.error_for_status()?.json::<Vec<TableEntry>>())
                .map_err(|error| shard_error(id, error))?;
            let entry = physical_entry(tables, config).map_err(|error| shard_error(id, error))?;
            if let Some(first) = &definition {
                if first.schema != entry.definition.schema {
                    return Err(shard_error(id, "schema differs across shards"));
                }
            } else {
                definition = Some(entry.definition);
            }
            endpoints.push(Shard {
                id,
                base,
                table: entry.id,
            });
        }
        let mut definition = definition
            .ok_or_else(|| DbError::Internal("sharded table has no destinations".into()))?;
        name.clone_into(&mut definition.name);
        Ok(Self {
            definition,
            endpoints,
            client,
        })
    }

    fn request(
        client: &Client,
        shard: &Shard,
        operation: &str,
        columns: Option<&[String]>,
    ) -> Result<reqwest::blocking::Response> {
        let url = table_url(shard.base.clone(), operation, &shard.table)?;
        let request = match columns {
            Some(columns) => client.post(url).json(columns),
            None => client.get(url),
        };
        request
            .send()
            .and_then(reqwest::blocking::Response::error_for_status)
            .map_err(|error| shard_error(shard.id, error))
    }
}

impl Table for ShardedTable {
    fn definition(&self) -> &TableDefinition {
        &self.definition
    }

    fn scan(&self, request: &ScanRequest) -> Result<BlockStream> {
        let header = request
            .columns
            .iter()
            .map(|name| {
                self.definition
                    .schema
                    .column(name)
                    .cloned()
                    .ok_or_else(|| DbError::UnknownColumn(name.clone()))
            })
            .collect::<Result<Vec<_>>>()?;
        let columns = request.columns.clone();
        let client = self.client.clone();
        let mut endpoints = self.endpoints.clone().into_iter();
        let mut active = None;
        let mut done = false;
        Ok(Box::new(std::iter::from_fn(move || {
            if done {
                return None;
            }
            let next = next_block(&client, &mut endpoints, &mut active, &columns, &header);
            done = !matches!(next, Ok(Some(_)));
            next.transpose()
        })))
    }

    fn explain_scan(&self, _: &ScanRequest) -> Result<ScanStats> {
        let mut total = ScanStats::default();
        for shard in &self.endpoints {
            let stats: ScanInfo = Self::request(&self.client, shard, "stats", None)?
                .json()
                .map_err(|error| shard_error(shard.id, error))?;
            macro_rules! add {
                ($($field:ident),+ $(,)?) => {
                    $(total.$field = total.$field.checked_add(stats.$field)
                        .ok_or_else(|| shard_error(shard.id, "scan statistics overflow"))?;)+
                };
            }
            add!(
                parts_total,
                parts_selected,
                granules_total,
                granules_selected,
                rows_total,
                rows_selected
            );
        }
        Ok(total)
    }

    fn insert(&self, _: Block) -> Result<()> {
        Err(DbError::Unsupported(
            "use HTTP INSERT for sharded tables".into(),
        ))
    }

    fn flush(&self) -> Result<()> {
        Err(sharded_ddl())
    }

    fn optimize(&self) -> Result<()> {
        Err(sharded_ddl())
    }

    fn drop_data(&self) -> Result<()> {
        Err(sharded_ddl())
    }
}

type ScanLines = Lines<BufReader<reqwest::blocking::Response>>;

fn next_block(
    client: &Client,
    endpoints: &mut impl Iterator<Item = Shard>,
    active: &mut Option<(u64, ScanLines)>,
    columns: &[String],
    header: &[ColumnDef],
) -> Result<Option<Block>> {
    loop {
        if active.is_none() {
            let Some(shard) = endpoints.next() else {
                return Ok(None);
            };
            let response = ShardedTable::request(client, &shard, "scan", Some(columns))?;
            *active = Some((shard.id, BufReader::new(response).lines()));
        }
        if let Some((id, lines)) = active {
            let line = lines
                .next()
                .ok_or_else(|| shard_error(*id, "scan ended without a completion frame"))?
                .map_err(|error| shard_error(*id, error))?;
            match serde_json::from_str::<Frame>(&line).map_err(|error| shard_error(*id, error))? {
                Frame::End => *active = None,
                Frame::Error(error) => return Err(shard_error(*id, error)),
                Frame::Block { rows, columns } => {
                    return wire::decode(rows, columns, header)
                        .map(Some)
                        .map_err(|error| shard_error(*id, error));
                }
            }
        }
    }
}

fn shard_error(id: u64, error: impl std::fmt::Display) -> DbError {
    DbError::Io(std::io::Error::other(format!("shard {id}: {error}")))
}

fn physical_table(app: &AppState, id: String) -> Result<Arc<dyn Table>> {
    let table = app.catalog.get_table_by_id(&TableId::new(id))?;
    if table.definition().engine != Engine::MergeTree {
        return Err(DbError::Unsupported(
            "shard table must use MergeTree".into(),
        ));
    }
    Ok(table)
}

pub(crate) async fn scan_local(
    State(app): State<AppState>,
    Path(id): Path<String>,
    columns: std::result::Result<Json<Vec<String>>, JsonRejection>,
) -> std::result::Result<Response, ApiError> {
    let Json(columns) =
        columns.map_err(|error| ApiError::new(error.status(), error.body_text()))?;
    let scan = blocking(move || {
        physical_table(&app, id)?.scan(&ScanRequest {
            columns,
            predicate: None,
        })
    })
    .await?;
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    tokio::task::spawn_blocking(move || {
        for block in scan {
            let (frame, failed) = match block {
                Ok(block) => (Frame::from(block), false),
                Err(error) => (Frame::Error(error.to_string()), true),
            };
            if !wire::send(&sender, &frame) || failed {
                return;
            }
        }
        wire::send(&sender, &Frame::End);
    });
    let body = Body::from_stream(futures_util::stream::unfold(
        receiver,
        |mut receiver| async move {
            receiver
                .recv()
                .await
                .map(|bytes| (Ok::<_, std::convert::Infallible>(bytes), receiver))
        },
    ));
    Ok(([(header::CONTENT_TYPE, "application/x-ndjson")], body).into_response())
}

pub(crate) async fn stats_local(
    State(app): State<AppState>,
    Path(id): Path<String>,
) -> std::result::Result<Json<ScanInfo>, ApiError> {
    let stats =
        blocking(move || physical_table(&app, id)?.explain_scan(&ScanRequest::default())).await?;
    Ok(Json(stats.into()))
}
