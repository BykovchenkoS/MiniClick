//! INSERT минует QueryEngine. Перед записью тело проверяется целиком.

use crate::{
    error::{blocking, ApiError, ErrorResponse},
    formats::{parse_input, InputFormat},
    AppState,
};
use axum::body::Bytes;
use axum::extract::{
    rejection::{BytesRejection, PathRejection, QueryRejection},
    Path, Query, State,
};
use serde::Deserialize;
use types::{ColumnData, DbError, Engine, Table};
use utoipa::{IntoParams, ToSchema};

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(deny_unknown_fields)]
pub struct InsertParams {
    #[serde(default)]
    #[param(default = "CSV", required = false)]
    pub format: InputFormat,
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
    (status = 400, description = "Ошибочные данные, формат или ключ чужого шарда", body = ErrorResponse),
    (status = 404, description = "Таблица не найдена", body = ErrorResponse),
    (status = 409, description = "Запись на реплику", body = ErrorResponse),
    (status = 413, description = "Превышен лимит тела. При записи на несколько шардов часть уже могла подтвердить запись", body = ErrorResponse),
    (status = 502, description = "Ошибка проверки шарда или записи. После начала отправки результат может быть частичным или неизвестным", body = ErrorResponse),
    (status = 500, description = "Ошибка сервера", body = ErrorResponse)
))]
pub async fn insert(
    State(app): State<AppState>,
    path: Result<Path<String>, PathRejection>,
    params: Result<Query<InsertParams>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<&'static str, ApiError> {
    let Path(name) = path?;
    let Query(params) = params?;
    if let Some(table) = app.config.sharding.get(&name) {
        let client = app
            .shard_client
            .as_ref()
            .ok_or_else(|| DbError::Internal("shard HTTP client is missing".into()))?;
        crate::sharding::insert(client, table, params.format, body?).await?;
        return Ok("Ok.\n");
    }
    if let Some(cluster) = &app.cluster {
        cluster
            .write(crate::cluster::types::Command::Insert {
                table: name,
                format: params.format,
                body: body?.to_vec(),
            })
            .await?;
        return Ok("Ok.\n");
    }
    app.check_writable()?;
    let body = body?;
    blocking(move || {
        let table = app.catalog.get_table(&name)?;
        insert_block(&app, table.as_ref(), params.format, &body)
    })
    .await?;
    Ok("Ok.\n")
}

/// Проверяет размещение всех строк при записи как по имени, так и по ID.
pub(crate) fn insert_block(
    app: &AppState,
    table: &dyn Table,
    format: InputFormat,
    body: &[u8],
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
    table.insert(block)
}
