//! Метаданные таблиц и статистика полного сканирования без чтения колонок.

use crate::{
    error::{blocking, ApiError, ErrorResponse},
    sharding::read::ShardedTable,
    AppState,
};
use axum::extract::{rejection::PathRejection, Path, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use types::{Codec, DbError, Engine, ScanRequest, ScanStats, Table, TableDefinition, TableSchema};
use utoipa::ToSchema;

#[derive(Serialize, ToSchema)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
}
#[derive(Serialize, ToSchema)]
pub struct SchemaInfo {
    pub columns: Vec<ColumnInfo>,
}
impl From<TableSchema> for SchemaInfo {
    fn from(schema: TableSchema) -> Self {
        Self {
            columns: schema
                .columns
                .into_iter()
                .map(|column| ColumnInfo {
                    name: column.name,
                    data_type: column.data_type.to_string(),
                })
                .collect(),
        }
    }
}
#[derive(Serialize, ToSchema)]
pub struct SettingsInfo {
    pub index_granularity: usize,
    pub codec: String,
}
#[derive(Serialize, ToSchema)]
pub struct TableInfo {
    pub name: String,
    pub schema: SchemaInfo,
    pub engine: String,
    /// Ключ сортировки локальной таблицы.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_by: Option<Vec<String>>,
    /// Параметры хранения локальной таблицы.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub settings: Option<SettingsInfo>,
}
impl TableInfo {
    fn distributed(name: String, schema: TableSchema) -> Self {
        Self {
            name,
            schema: schema.into(),
            engine: "Distributed".into(),
            order_by: None,
            settings: None,
        }
    }
}
impl From<TableDefinition> for TableInfo {
    fn from(definition: TableDefinition) -> Self {
        Self {
            name: definition.name,
            schema: definition.schema.into(),
            engine: match definition.engine {
                Engine::Memory => "Memory",
                Engine::MergeTree => "MergeTree",
            }
            .into(),
            order_by: Some(definition.order_by),
            settings: Some(SettingsInfo {
                index_granularity: definition.settings.index_granularity,
                codec: match definition.settings.codec {
                    Codec::None => "None",
                    Codec::Lz4 => "LZ4",
                    Codec::Zstd => "ZSTD",
                }
                .into(),
            }),
        }
    }
}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct ScanInfo {
    pub parts_total: usize,
    pub parts_selected: usize,
    pub granules_total: usize,
    pub granules_selected: usize,
    pub rows_total: u64,
    pub rows_selected: u64,
}
impl From<ScanStats> for ScanInfo {
    fn from(stats: ScanStats) -> Self {
        Self {
            parts_total: stats.parts_total,
            parts_selected: stats.parts_selected,
            granules_total: stats.granules_total,
            granules_selected: stats.granules_selected,
            rows_total: stats.rows_total,
            rows_selected: stats.rows_selected,
        }
    }
}
#[derive(Serialize, ToSchema)]
pub struct TableDescription {
    #[serde(flatten)]
    pub definition: TableInfo,
    pub stats: ScanInfo,
}

#[utoipa::path(get, path = "/tables", tag = "metadata", responses(
    (status = 200, description = "Локальные и логические таблицы по имени", body = [TableInfo]),
    (status = 500, description = "Ошибка сервера или не удалось получить схему логической таблицы", body = ErrorResponse)
))]
pub async fn tables(State(app): State<AppState>) -> Result<Json<Vec<TableInfo>>, ApiError> {
    blocking(move || {
        let _view = app
            .view_gate
            .read()
            .map_err(|_| DbError::Internal("view lock poisoned".into()))?;
        app.check_view()?;
        let mut tables: Vec<_> = app
            .catalog
            .list_tables()
            .into_iter()
            .map(TableInfo::from)
            .collect();
        for (name, config) in &app.config.sharding {
            let schema = match &config.schema {
                Some(schema) => schema.clone(),
                None => ShardedTable::open(name, config)?
                    .definition()
                    .schema
                    .clone(),
            };
            tables.push(TableInfo::distributed(name.clone(), schema));
        }
        tables.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        Ok(Json(tables))
    })
    .await
}

#[utoipa::path(get, path = "/tables/{name}", tag = "metadata", params(("name" = String, Path, description = "Регистрозависимое имя таблицы")), responses(
    (status = 200, description = "Описание и статистика полного скана, суммарная для всех шардов логической таблицы", body = TableDescription),
    (status = 400, description = "Неверное имя", body = ErrorResponse),
    (status = 404, description = "Таблица не найдена", body = ErrorResponse),
    (status = 500, description = "Ошибка сервера или не удалось получить данные требуемого шарда", body = ErrorResponse)
))]
pub async fn describe(
    State(app): State<AppState>,
    path: Result<Path<String>, PathRejection>,
) -> Result<Json<TableDescription>, ApiError> {
    let Path(name) = path?;
    blocking(move || {
        let _view = app
            .view_gate
            .read()
            .map_err(|_| DbError::Internal("view lock poisoned".into()))?;
        app.check_view()?;
        if let Some(config) = app.config.sharding.get(&name) {
            let table = ShardedTable::open(&name, config)?;
            return Ok(Json(TableDescription {
                definition: TableInfo::distributed(name, table.definition().schema.clone()),
                stats: table.explain_scan(&ScanRequest::default())?.into(),
            }));
        }
        let table = app.catalog.get_table(&name)?;
        Ok(Json(TableDescription {
            definition: table.definition().clone().into(),
            stats: table.explain_scan(&ScanRequest::default())?.into(),
        }))
    })
    .await
}
