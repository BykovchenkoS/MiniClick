//! Сборка ядра и HTTP-приложения. Публичный роутер используется интеграционными тестами.

mod admin;
pub mod cluster;
pub mod config;
pub mod error;
pub mod formats;
pub mod generator;
mod insert;
mod meta;
mod query;
pub mod replica;

use axum::{
    extract::DefaultBodyLimit,
    http::StatusCode,
    routing::{get, post},
    Router,
};
use catalog::DiskCatalog;
use config::Config;
use replication::Role;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, RwLock,
};
use types::{Catalog, DbError, Engine, QueryEngine, Result};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

#[derive(Clone)]
pub struct AppState {
    pub catalog: Arc<dyn Catalog>,
    pub query: Arc<dyn QueryEngine>,
    pub config: Config,
    pub cluster: Option<Arc<cluster::Cluster>>,
    pub(crate) view_gate: Arc<RwLock<()>>,
    pub(crate) view_healthy: Arc<AtomicBool>,
    _cluster_lease: Option<Arc<std::fs::File>>,
}

impl AppState {
    /// Только сервер знает одновременно о постоянном Catalog, Query и Storage.
    pub fn open(config: Config) -> Result<Self> {
        config.validate()?;
        let (directory, lease) = if config.cluster.is_some() {
            let (path, lease) = cluster::prepare_view(&config)?;
            (path, Some(lease))
        } else {
            (config.data_dir.clone(), None)
        };
        let catalog: Arc<dyn Catalog> = Arc::new(DiskCatalog::open(
            &directory,
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
            cluster: None,
            view_gate: Arc::new(RwLock::new(())),
            view_healthy: Arc::new(AtomicBool::new(true)),
            _cluster_lease: lease,
        })
    }

    pub fn is_replica(&self) -> bool {
        if let Some(cluster) = &self.cluster {
            return !cluster.is_leader();
        }
        matches!(self.config.node.role, Role::Replica { .. })
    }

    pub(crate) fn check_view(&self) -> Result<()> {
        if !self.view_healthy.load(Ordering::Acquire)
            || self
                .cluster
                .as_ref()
                .is_some_and(|cluster| cluster.raft.metrics().borrow().running_state.is_err())
        {
            return Err(DbError::Internal(
                "cluster view is unavailable after a storage failure; restart required".into(),
            ));
        }
        Ok(())
    }

    /// Реплика не принимает запись: клиента отправляем к лидеру.
    pub fn check_writable(&self) -> Result<()> {
        match &self.config.node.role {
            Role::Leader => Ok(()),
            Role::Replica { leader_addr, .. } => Err(DbError::NotLeader {
                leader: leader_addr.clone(),
            }),
        }
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
        query::query,
        replica::ping,
        replica::tables,
        replica::parts,
        replica::part,
        cluster::status,
        cluster::initialize
    ),
    components(schemas(formats::InputFormat, formats::OutputFormat)),
    info(
        title = "MiniClick",
        description = "CSV/TSV без заголовка. SQL: CREATE/DROP/OPTIMIZE, SELECT с WHERE, GROUP BY, ORDER BY, LIMIT, агрегатами count/sum/min/max и EXPLAIN SELECT."
    )
)]
struct ApiDoc;

/// Не выполняет I/O: состояние можно подменить MemCatalog или тестовым QueryEngine.
pub fn router(state: AppState) -> Router {
    let limit = state.config.max_body_bytes;
    let mut router = Router::new()
        .route("/ping", get(admin::ping))
        .route("/help", get(admin::help))
        .route("/tables", get(meta::tables))
        .route("/tables/{name}", get(meta::describe))
        .route("/insert/{table}", post(insert::insert))
        .route("/query", post(query::query))
        .layer(DefaultBodyLimit::max(limit));
    // Источником для других реплик служит только лидер.
    if state.config.cluster.is_none() && !state.is_replica() {
        router = router
            .route("/replica/ping", get(replica::ping))
            .route("/replica/tables", get(replica::tables))
            .route("/replica/tables/{id}/parts", get(replica::parts))
            .route("/replica/tables/{id}/parts/{part}", get(replica::part));
    }
    if state.config.cluster.is_some() {
        // JSON экранирует SQL и расширяет байты INSERT; снимок идёт отдельными чанками.
        let rpc_limit = limit
            .saturating_mul(6)
            .saturating_add(64 * 1024)
            .max(1024 * 1024);
        router = router
            .route("/cluster/status", get(cluster::status))
            .route("/cluster/init", post(cluster::initialize));
        let rpc = Router::new()
            .route("/cluster/append", post(cluster::append))
            .route("/cluster/vote", post(cluster::vote))
            .route("/cluster/snapshot", post(cluster::snapshot))
            .layer(DefaultBodyLimit::max(rpc_limit));
        router = router.merge(rpc);
    }
    router
        .fallback(|| async { error::ApiError::new(StatusCode::NOT_FOUND, "unknown endpoint") })
        .method_not_allowed_fallback(|| async {
            error::ApiError::new(StatusCode::METHOD_NOT_ALLOWED, "method not allowed")
        })
        .merge(SwaggerUi::new("/swagger-ui").url("/api-doc/openapi.json", ApiDoc::openapi()))
        .with_state(state)
}
