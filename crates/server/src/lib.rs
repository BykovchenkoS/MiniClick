//! Сборка ядра и HTTP-приложения. Публичный роутер используется интеграционными тестами.

mod admin;
pub mod config;
pub mod error;
pub mod formats;
pub mod generator;
mod insert;
mod meta;
mod query;

use axum::{
    extract::DefaultBodyLimit,
    http::StatusCode,
    routing::{get, post},
    Router,
};
use catalog::DiskCatalog;
use config::Config;
use std::sync::Arc;
use types::{Catalog, Engine, QueryEngine, Result};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

#[derive(Clone)]
pub struct AppState {
    pub catalog: Arc<dyn Catalog>,
    pub query: Arc<dyn QueryEngine>,
    pub config: Config,
}

impl AppState {
    /// Только сервер знает одновременно о постоянном Catalog, Query и Storage.
    pub fn open(config: Config) -> Result<Self> {
        config.validate()?;
        let catalog: Arc<dyn Catalog> = Arc::new(DiskCatalog::open(
            &config.data_dir,
            vec![
                (Engine::Memory, Arc::new(types::testing::MemTableFactory)),
                (Engine::MergeTree, Arc::new(storage::MergeTreeFactory)),
            ],
        )?);
        let query = Arc::new(::query::SqlQueryEngine::new(catalog.clone()));
        Ok(Self {
            catalog,
            query,
            config,
        })
    }
}

#[derive(OpenApi)]
#[openapi(
    paths(
        admin::ping,
        admin::help,
        meta::tables,
        meta::describe,
        insert::insert,
        query::query
    ),
    components(schemas(formats::InputFormat, formats::OutputFormat)),
    info(
        title = "MiniClick",
        description = "CSV/TSV без заголовка. Текущая ветка: SELECT/WHERE/LIMIT, CREATE/DROP/OPTIMIZE; агрегаты и EXPLAIN ожидают блок B."
    )
)]
struct ApiDoc;

/// Не выполняет I/O: состояние можно подменить MemCatalog или тестовым QueryEngine.
pub fn router(state: AppState) -> Router {
    let limit = state.config.max_body_bytes;
    Router::new()
        .route("/ping", get(admin::ping))
        .route("/help", get(admin::help))
        .route("/tables", get(meta::tables))
        .route("/tables/{name}", get(meta::describe))
        .route("/insert/{table}", post(insert::insert))
        .route("/query", post(query::query))
        .fallback(|| async { error::ApiError::new(StatusCode::NOT_FOUND, "unknown endpoint") })
        .method_not_allowed_fallback(|| async {
            error::ApiError::new(StatusCode::METHOD_NOT_ALLOWED, "method not allowed")
        })
        .layer(DefaultBodyLimit::max(limit))
        .merge(SwaggerUi::new("/swagger-ui").url("/api-doc/openapi.json", ApiDoc::openapi()))
        .with_state(state)
}
