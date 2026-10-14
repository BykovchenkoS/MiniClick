//! Единое представление ошибок ядра и HTTP-экстракторов.

use axum::extract::rejection::{BytesRejection, PathRejection, QueryRejection};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
use types::DbError;
use utoipa::ToSchema;

#[derive(Serialize, ToSchema)]
pub struct ErrorResponse {
    pub error: String,
}

pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

impl From<DbError> for ApiError {
    fn from(error: DbError) -> Self {
        let status = match &error {
            DbError::UnknownTable(_) => StatusCode::NOT_FOUND,
            DbError::NotLeader { .. } => StatusCode::CONFLICT,
            DbError::Io(_) | DbError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::BAD_REQUEST,
        };
        if status.is_server_error() {
            tracing::error!(%error, "Ошибка обработки HTTP-запроса");
        }
        Self::new(status, error.to_string())
    }
}

impl From<QueryRejection> for ApiError {
    fn from(error: QueryRejection) -> Self {
        Self::new(StatusCode::BAD_REQUEST, error.body_text())
    }
}
impl From<PathRejection> for ApiError {
    fn from(error: PathRejection) -> Self {
        Self::new(StatusCode::BAD_REQUEST, error.body_text())
    }
}
impl From<BytesRejection> for ApiError {
    fn from(error: BytesRejection) -> Self {
        Self::new(error.status(), error.body_text())
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorResponse {
                error: self.message,
            }),
        )
            .into_response()
    }
}

pub(crate) async fn blocking<T, F>(operation: F) -> std::result::Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce() -> types::Result<T> + Send + 'static,
{
    let span = tracing::Span::current();
    tokio::task::spawn_blocking(move || span.in_scope(operation))
        .await
        .map_err(|error| DbError::Internal(format!("blocking task failed: {error}")))?
        .map_err(ApiError::from)
}
