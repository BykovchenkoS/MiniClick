//! SQL и полностью сформированный результат, включая ошибки ленивого потока.

use crate::{
    error::{blocking, ApiError, ErrorResponse},
    formats::{format_output, OutputFormat},
    AppState,
};
use axum::body::Bytes;
use axum::extract::{
    rejection::{BytesRejection, QueryRejection},
    Query, State,
};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use types::{DbError, QueryOutput};
use utoipa::IntoParams;

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(deny_unknown_fields)]
pub struct QueryParams {
    #[serde(default)]
    #[param(default = "TSV", required = false)]
    pub format: OutputFormat,
}

#[utoipa::path(post, path = "/query", tag = "query", params(QueryParams),
    request_body(content = String, content_type = "text/plain", example = "SELECT * FROM hits WHERE user_id = 42 LIMIT 10"),
    responses(
        (status = 200, description = "Результат запроса или Ok.", content(
            (String = "text/tab-separated-values"), (String = "text/csv"),
            (String = "application/x-ndjson"), (String = "text/plain")
        )),
        (status = 400, description = "Ошибка SQL, формата или типов", body = ErrorResponse),
        (status = 404, description = "Таблица не найдена", body = ErrorResponse),
        (status = 409, description = "Запись на реплику", body = ErrorResponse),
        (status = 413, description = "Превышен лимит тела", body = ErrorResponse),
        (status = 500, description = "Ошибка сервера", body = ErrorResponse)
    )
)]
pub async fn query(
    State(app): State<AppState>,
    params: Result<Query<QueryParams>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Response, ApiError> {
    let Query(params) = params?;
    let body = body?;
    let (content_type, output) = blocking(move || {
        let sql = std::str::from_utf8(&body)
            .map_err(|_| DbError::Syntax("SQL body must be UTF-8".into()))?;
        match app.query.execute(sql)? {
            QueryOutput::Ok => Ok(("text/plain; charset=utf-8", b"Ok.\n".to_vec())),
            QueryOutput::Rows { header, stream } => Ok((
                params.format.content_type(),
                format_output(params.format, &header, stream)?,
            )),
        }
    })
    .await?;
    Ok(([(header::CONTENT_TYPE, content_type)], output).into_response())
}
