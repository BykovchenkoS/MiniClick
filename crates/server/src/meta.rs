//! Метаданные таблиц и статистика полного сканирования без чтения колонок.

use crate::{
    error::{blocking, ApiError, ErrorResponse},
    AppState,
};
use axum::extract::{rejection::PathRejection, Path, State};
use axum::Json;
use serde::Serialize;
use types::{Codec, Engine, ScanRequest, ScanStats, TableDefinition};
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
    pub order_by: Vec<String>,
    pub settings: SettingsInfo,
}
impl From<TableDefinition> for TableInfo {
    fn from(definition: TableDefinition) -> Self {
        Self {
            name: definition.name,
            schema: SchemaInfo {
                columns: definition
                    .schema
                    .columns
                    .into_iter()
                    .map(|column| ColumnInfo {
                        name: column.name,
                        data_type: column.data_type.to_string(),
                    })
                    .collect(),
            },
            engine: match definition.engine {
                Engine::Memory => "Memory",
                Engine::MergeTree => "MergeTree",
            }
            .into(),
            order_by: definition.order_by,
            settings: SettingsInfo {
                index_granularity: definition.settings.index_granularity,
                codec: match definition.settings.codec {
                    Codec::None => "None",
                    Codec::Lz4 => "LZ4",
                    Codec::Zstd => "ZSTD",
                }
                .into(),
            },
        }
    }
}

#[derive(Serialize, ToSchema)]
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
    (status = 200, description = "Таблицы по имени", body = [TableInfo]),
    (status = 500, description = "Ошибка сервера", body = ErrorResponse)
))]
pub async fn tables(State(app): State<AppState>) -> Result<Json<Vec<TableInfo>>, ApiError> {
    blocking(move || {
        Ok(Json(
            app.catalog
                .list_tables()
                .into_iter()
                .map(TableInfo::from)
                .collect(),
        ))
    })
    .await
}

#[utoipa::path(get, path = "/tables/{name}", tag = "metadata", params(("name" = String, Path, description = "Регистрозависимое имя таблицы")), responses(
    (status = 200, description = "Описание и статистика таблицы", body = TableDescription),
    (status = 400, description = "Неверное имя", body = ErrorResponse),
    (status = 404, description = "Таблица не найдена", body = ErrorResponse),
    (status = 500, description = "Ошибка сервера", body = ErrorResponse)
))]
pub async fn describe(
    State(app): State<AppState>,
    path: Result<Path<String>, PathRejection>,
) -> Result<Json<TableDescription>, ApiError> {
    let Path(name) = path?;
    blocking(move || {
        let table = app.catalog.get_table(&name)?;
        Ok(Json(TableDescription {
            definition: table.definition().clone().into(),
            stats: table.explain_scan(&ScanRequest::default())?.into(),
        }))
    })
    .await
}
