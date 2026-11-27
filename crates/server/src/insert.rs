//! INSERT минует QueryEngine. Перед записью тело проверяется целиком.

use crate::{
    error::{blocking, ApiError, ErrorResponse},
    formats::{parse_input, InputFormat},
    AppState,
};
use axum::extract::{
    rejection::{BytesRejection, PathRejection, QueryRejection},
    Path, Query, State,
};
use axum::{
    body::Bytes,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use types::{ColumnData, DbError, Engine, Table};
use utoipa::{IntoParams, ToSchema};

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(deny_unknown_fields)]
pub struct InsertParams {
    #[serde(default)]
    #[param(default = "CSV", required = false)]
    pub format: InputFormat,
    #[param(value_type = String, required = false)]
    pub request_id: Option<String>,
}

impl InsertParams {
    pub(crate) fn parsed_request_id(&self) -> Result<Option<uuid::Uuid>, ApiError> {
        self.request_id
            .as_deref()
            .map(uuid::Uuid::parse_str)
            .transpose()
            .map_err(|error| DbError::Syntax(format!("invalid request_id: {error}")).into())
    }
}

pub struct BinaryBody;

impl utoipa::PartialSchema for BinaryBody {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::{KnownFormat, ObjectBuilder, SchemaFormat, Type};
        ObjectBuilder::new()
            .schema_type(Type::String)
            .format(Some(SchemaFormat::KnownFormat(KnownFormat::Binary)))
            .into()
    }
}
impl ToSchema for BinaryBody {}

#[utoipa::path(post, path = "/insert/{table}", tag = "insert", params(
    InsertParams, ("table" = String, Path, description = "Имя существующей таблицы")
), request_body(content = BinaryBody, content_type = "application/octet-stream", description = "Файл без заголовка; JSONEachRow — объекты с именами колонок"), responses(
    (status = 200, description = "Вставка завершена", body = String, content_type = "text/plain"),
    (status = 202, description = "Распределённая вставка принята и ждёт завершения", body = String, content_type = "text/plain"),
    (status = 400, description = "Ошибочные данные, формат или ключ чужого шарда", body = ErrorResponse),
    (status = 404, description = "Таблица не найдена", body = ErrorResponse),
    (status = 409, description = "Запись на реплику или конфликт request_id", body = ErrorResponse),
    (status = 413, description = "Превышен лимит тела", body = ErrorResponse),
    (status = 502, description = "Сбой предварительной проверки шарда", body = ErrorResponse),
    (status = 500, description = "Ошибка сервера", body = ErrorResponse),
    (status = 503, description = "Ожидается завершение предыдущей распределённой вставки", body = ErrorResponse)
))]
pub async fn insert(
    State(app): State<AppState>,
    path: Result<Path<String>, PathRejection>,
    params: Result<Query<InsertParams>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Response, ApiError> {
    let Path(name) = path?;
    let Query(params) = params?;
    let request_id = params.parsed_request_id()?;
    if let Some(table) = app.config.sharding.get(&name) {
        return match crate::sharding::insert(&app, &name, table, params.format, body?, request_id)
            .await?
        {
            Some(id) => Ok((StatusCode::ACCEPTED, format!("Pending {id}\n")).into_response()),
            None => Ok("Ok.\n".into_response()),
        };
    }
    if request_id.is_some() {
        return Err(
            DbError::Unsupported("request_id requires a logical sharded table".into()).into(),
        );
    }
    if let Some(cluster) = &app.cluster {
        cluster
            .write(crate::cluster::types::Command::Insert {
                table: name,
                format: params.format,
                body: body?.to_vec(),
            })
            .await?;
        return Ok("Ok.\n".into_response());
    }
    app.check_writable()?;
    let body = body?;
    blocking(move || {
        let table = app.catalog.get_table(&name)?;
        insert_block(&app, table.as_ref(), params.format, &body)
    })
    .await?;
    Ok("Ok.\n".into_response())
}

/// Проверяет размещение всех строк при записи как по имени, так и по ID.
pub(crate) fn insert_block(
    app: &AppState,
    table: &dyn Table,
    format: InputFormat,
    body: &[u8],
) -> types::Result<()> {
    insert_block_with_id(app, table, format, body, None)
}

pub(crate) fn insert_block_with_id(
    app: &AppState,
    table: &dyn Table,
    format: InputFormat,
    body: &[u8],
    request_id: Option<uuid::Uuid>,
) -> types::Result<()> {
    let definition = table.definition();
    let block = parse_input(format, body, &definition.schema)?;
    if let Some(placement) = app.config.local_shards.get(&definition.name) {
        let Some(ColumnData::UInt64(keys)) = block.column(&placement.key) else {
            return Err(DbError::TypeMismatch(format!(
                "shard key {} must be UInt64",
                placement.key
            )));
        };
        if definition.engine != Engine::MergeTree {
            return Err(DbError::Unsupported(
                "shard table must use MergeTree".into(),
            ));
        }
        let local_index = Some(placement.index());
        for (index, &key) in keys.iter().enumerate() {
            if placement.shard_index(key) != local_index {
                return Err(DbError::BadInput {
                    row: index + 1,
                    message: format!(
                        "key {}={key} does not belong to shard {}",
                        placement.key, placement.shard_id
                    ),
                });
            }
        }
    }
    match request_id {
        Some(id) => {
            let checksum = insert_checksum(&definition.name, format, body);
            table.insert_once(block, &id.to_string(), &checksum)
        }
        None => table.insert(block),
    }
}

pub(crate) fn insert_checksum(table: &str, format: InputFormat, body: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update((table.len() as u64).to_le_bytes());
    hash.update(table.as_bytes());
    hash.update([format as u8]);
    hash.update(body);
    hash.finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
