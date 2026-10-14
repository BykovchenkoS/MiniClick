//! INSERT минует QueryEngine: тело целиком превращается в один проверенный Block.

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
    (status = 400, description = "Ошибочные данные или формат", body = ErrorResponse),
    (status = 404, description = "Таблица не найдена", body = ErrorResponse),
    (status = 409, description = "Запись на реплику", body = ErrorResponse),
    (status = 413, description = "Превышен лимит тела", body = ErrorResponse),
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
    let body = body?;
    blocking(move || {
        let table = app.catalog.get_table(&name)?;
        let block = parse_input(params.format, &body, &table.definition().schema)?;
        table.insert(block)
    })
    .await?;
    Ok("Ok.\n")
}
