//! Адаптер распределённой таблицы для синхронного планировщика и исполнителя.

mod wire;

use super::{
    metadata_path, no_leader, physical_entry, read_barrier, shard_url, table_url, MemberStatus,
};
use crate::{
    config::{RaftShardConfig, ShardedTableConfig},
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
    TableDefinition, TableEntry, TableId, TableSchema,
};
use wire::{Frame, Predicate, ReadRequest};

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
    config: ShardedTableConfig,
    client: Client,
}

#[derive(Clone)]
struct Shard {
    id: u64,
    base: Url,
    table: Option<TableId>,
}

impl Shard {
    fn entry(&mut self, client: &Client, config: &ShardedTableConfig) -> Result<TableEntry> {
        let group = config.raft_shards.get(&self.id);
        if let Some(group) = group {
            self.base = leader(client, group).map_err(|error| shard_error(self.id, error))?;
        }
        let metadata = self
            .base
            .join(metadata_path(group.is_some()))
            .map_err(|error| shard_error(self.id, error))?;
        let tables = client
            .get(metadata)
            .send()
            .and_then(|response| response.error_for_status()?.json::<Vec<TableEntry>>())
            .map_err(|error| shard_error(self.id, error))?;
        physical_entry(tables, config).map_err(|error| shard_error(self.id, error))
    }

    fn resolve(
        mut self,
        client: &Client,
        config: &ShardedTableConfig,
        schema: &TableSchema,
    ) -> Result<Self> {
        if self.table.is_none() {
            let entry = self.entry(client, config)?;
            if entry.definition.schema != *schema {
                return Err(shard_error(self.id, "schema differs across shards"));
            }
            self.table = Some(entry.id);
        }
        Ok(self)
    }
}

fn leader(client: &Client, group: &RaftShardConfig) -> Result<Url> {
    for address in &group.members {
        let base = shard_url(address)?;
        let status = base
            .join("cluster/status")
            .map_err(|error| DbError::Internal(error.to_string()))?;
        let response = client.get(status).timeout(Duration::from_secs(1)).send();
        if let Ok(response) = response {
            if response.status().is_success()
                && response
                    .json::<MemberStatus>()
                    .is_ok_and(|status| status.is_leader_of(group))
            {
                return Ok(base);
            }
        }
    }
    Err(no_leader(group))
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
        let mut endpoints = config
            .shards
            .iter()
            .map(|(&id, address)| {
                Ok(Shard {
                    id,
                    base: shard_url(address)?,
                    table: None,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        // Для связывания WHERE нужна одна схема. Остальные шарды разрешаются по мере чтения.
        let mut first_error = None;
        let mut definition = None;
        for shard in &mut endpoints {
            match shard.entry(&client, config) {
                Ok(entry) => {
                    shard.table = Some(entry.id);
                    definition = Some(entry.definition);
                    break;
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        let mut definition = definition.ok_or_else(|| {
            first_error
                .unwrap_or_else(|| DbError::Internal("sharded table has no destinations".into()))
        })?;
        name.clone_into(&mut definition.name);
        Ok(Self {
            definition,
            endpoints,
            config: config.clone(),
            client,
        })
    }

    fn selected_shards(&self, predicate: Option<&Predicate>) -> Vec<Shard> {
        let selected: Vec<_> = self
            .endpoints
            .iter()
            .enumerate()
            .filter(|(index, _)| {
                predicate.is_none_or(|predicate| predicate.may_match_shard(&self.config, *index))
            })
            .map(|(_, shard)| shard.clone())
            .collect();
        tracing::debug!(table = %self.definition.name, shards = ?selected.iter().map(|shard| shard.id).collect::<Vec<_>>(), "Выбраны шарды");
        selected
    }

    fn request(
        client: &Client,
        shard: &Shard,
        operation: &str,
        request: &ReadRequest,
    ) -> Result<reqwest::blocking::Response> {
        let table = shard
            .table
            .as_ref()
            .ok_or_else(|| DbError::Internal("shard table ID is missing".into()))?;
        let url = table_url(shard.base.clone(), operation, table)?;
        client
            .post(url)
            .json(request)
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
        let request = ReadRequest::from(request);
        let client = self.client.clone();
        let config = self.config.clone();
        let schema = self.definition.schema.clone();
        let mut endpoints = self
            .selected_shards(request.predicate.as_ref())
            .into_iter()
            .map({
                let client = client.clone();
                move |shard| shard.resolve(&client, &config, &schema)
            });
        let mut active = None;
        let mut done = false;
        Ok(Box::new(std::iter::from_fn(move || {
            if done {
                return None;
            }
            let next = next_block(&client, &mut endpoints, &mut active, &request, &header);
            done = !matches!(next, Ok(Some(_)));
            next.transpose()
        })))
    }

    fn explain_scan(&self, request: &ScanRequest) -> Result<ScanStats> {
        let request = ReadRequest::from(request);
        let mut total = ScanStats::default();
        for shard in self.selected_shards(request.predicate.as_ref()) {
            let shard = shard.resolve(&self.client, &self.config, &self.definition.schema)?;
            let stats: ScanInfo = Self::request(&self.client, &shard, "stats", &request)?
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
    endpoints: &mut impl Iterator<Item = Result<Shard>>,
    active: &mut Option<(u64, ScanLines)>,
    request: &ReadRequest,
    header: &[ColumnDef],
) -> Result<Option<Block>> {
    loop {
        if active.is_none() {
            let Some(shard) = endpoints.next() else {
                return Ok(None);
            };
            let shard = shard?;
            let response = ShardedTable::request(client, &shard, "scan", request)?;
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
    if app.cluster.is_some()
        && !app
            .config
            .local_shards
            .contains_key(&table.definition().name)
    {
        return Err(DbError::Unsupported(
            "Raft table has no shard placement".into(),
        ));
    }
    Ok(table)
}

pub(crate) async fn scan_local(
    State(app): State<AppState>,
    Path(id): Path<String>,
    request: std::result::Result<Json<ReadRequest>, JsonRejection>,
) -> std::result::Result<Response, ApiError> {
    let Json(request) =
        request.map_err(|error| ApiError::new(error.status(), error.body_text()))?;
    read_barrier(&app).await?;
    let scan = blocking(move || {
        let _view = app
            .view_gate
            .read()
            .map_err(|_| DbError::Internal("view lock poisoned".into()))?;
        app.check_view()?;
        physical_table(&app, id)?.scan(&request.into())
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
    request: std::result::Result<Json<ReadRequest>, JsonRejection>,
) -> std::result::Result<Json<ScanInfo>, ApiError> {
    let Json(request) =
        request.map_err(|error| ApiError::new(error.status(), error.body_text()))?;
    read_barrier(&app).await?;
    let stats = blocking(move || {
        let _view = app
            .view_gate
            .read()
            .map_err(|_| DbError::Internal("view lock poisoned".into()))?;
        app.check_view()?;
        physical_table(&app, id)?.explain_scan(&request.into())
    })
    .await?;
    Ok(Json(stats.into()))
}
