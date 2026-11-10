//! Кластер с фиксированным составом: выборы и запись большинством через OpenRaft.

mod log_store;
mod network;
mod state_machine;
pub(crate) mod types;

use self::types::{ClusterRaft, Command, TypeConfig};
use crate::{config::Config, error::ApiError, AppState};
use ::types::{DbError, Result};
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    Json,
};
use openraft::error::{ClientWriteError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, ServerState};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub struct Cluster {
    pub raft: ClusterRaft,
    config: crate::config::ClusterConfig,
}

#[derive(Default, Serialize, Deserialize, PartialEq, Eq)]
struct Identity {
    cluster_id: String,
    config: Option<crate::config::ClusterConfig>,
}

/// view целиком производен от истории; чужой каталог и символьные ссылки не удаляем.
pub(crate) fn prepare_view(config: &Config) -> Result<(PathBuf, Arc<File>)> {
    fs::create_dir_all(&config.data_dir)?;
    let lease = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(config.data_dir.join(".cluster.lock"))?;
    lease.try_lock().map_err(|error| {
        DbError::Internal(format!("cluster directory is already open: {error}"))
    })?;
    let identity = Identity {
        cluster_id: config.node.cluster_id.clone(),
        config: config.cluster.clone(),
    };
    let identity_path = config.data_dir.join("cluster-identity.json");
    let view = config.data_dir.join("view");
    state_machine::StateMachine::validate_file(&config.data_dir.join("raft-state.json"))?;
    if identity_path.exists() {
        let stored: Identity = log_store::read(&identity_path)?;
        if stored != identity {
            return Err(DbError::Syntax(
                "cluster identity differs from saved configuration".into(),
            ));
        }
    } else {
        if view.exists() || config.data_dir.join("catalog.json").exists() {
            return Err(DbError::Syntax(
                "cluster mode requires a separate data directory".into(),
            ));
        }
        log_store::persist(&identity_path, &identity)?;
    }
    if let Ok(metadata) = fs::symlink_metadata(&view) {
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(DbError::Internal(
                "cluster view is not a regular directory".into(),
            ));
        }
        fs::remove_dir_all(&view)?;
    }
    Ok((view, Arc::new(lease)))
}

impl Cluster {
    pub async fn start(app: &AppState) -> Result<Arc<Self>> {
        let config = app
            .config
            .cluster
            .clone()
            .ok_or_else(|| DbError::Internal("cluster configuration missing".into()))?;
        let opened = app.clone();
        let (log, state_machine) = tokio::task::spawn_blocking(move || {
            let log = log_store::LogStore::open(opened.config.data_dir.join("raft-log.json"))?;
            let sm = state_machine::StateMachine::open(
                opened.config.data_dir.join("raft-state.json"),
                opened,
            )?;
            Ok::<_, DbError>((log, sm))
        })
        .await
        .map_err(|error| DbError::Internal(error.to_string()))??;
        let settings = openraft::Config {
            cluster_name: app.config.node.cluster_id.clone(),
            heartbeat_interval: 150,
            election_timeout_min: 600,
            election_timeout_max: 1000,
            max_payload_entries: 1,
            snapshot_max_chunk_size: 64 * 1024,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(100),
            max_in_snapshot_log_to_keep: 20,
            ..Default::default()
        }
        .validate()
        .map_err(|error| DbError::Syntax(error.to_string()))?;
        let network = network::Network {
            client: reqwest::Client::new(),
            cluster_id: app.config.node.cluster_id.clone(),
        };
        let raft = ClusterRaft::new(config.id, Arc::new(settings), network, log, state_machine)
            .await
            .map_err(|error| DbError::Internal(error.to_string()))?;
        Ok(Arc::new(Self { raft, config }))
    }

    pub fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.running_state.is_ok() && metrics.state == ServerState::Leader
    }

    fn not_leader(&self) -> ApiError {
        let leader = self.raft.metrics().borrow().current_leader;
        match leader.and_then(|id| self.config.members.get(&id)) {
            Some(address) => ApiError::new(
                StatusCode::CONFLICT,
                format!("NotLeader: leader is {address}"),
            ),
            None => ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "cluster has no elected leader",
            ),
        }
    }

    pub async fn write(&self, command: Command) -> std::result::Result<(), ApiError> {
        if !self.is_leader() {
            return Err(self.not_leader());
        }
        let result = tokio::time::timeout(Duration::from_secs(3), self.raft.client_write(command))
            .await
            .map_err(|_| {
                ApiError::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "quorum timeout; write outcome is unknown",
                )
            })?;
        let response = result.map_err(|error| match error {
            RaftError::APIError(ClientWriteError::ForwardToLeader(_)) => self.not_leader(),
            _ => ApiError::new(StatusCode::SERVICE_UNAVAILABLE, error.to_string()),
        })?;
        if let Some(error) = response.data.error {
            return Err(ApiError::new(
                StatusCode::from_u16(response.data.status)
                    .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                error,
            ));
        }
        Ok(())
    }
}

fn cluster(app: &AppState) -> std::result::Result<&Arc<Cluster>, ApiError> {
    app.cluster
        .as_ref()
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "cluster mode is disabled"))
}

fn check_cluster(app: &AppState, headers: &HeaderMap) -> std::result::Result<(), ApiError> {
    cluster(app)?;
    if headers
        .get("x-miniclick-cluster")
        .and_then(|value| value.to_str().ok())
        != Some(app.config.node.cluster_id.as_str())
    {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "cluster identity mismatch",
        ));
    }
    Ok(())
}

#[utoipa::path(get, path = "/cluster/status", tag = "cluster", responses((status = 200, description = "Роль, лидер, term и индексы Raft")))]
pub async fn status(
    State(app): State<AppState>,
) -> std::result::Result<Json<serde_json::Value>, ApiError> {
    let cluster = cluster(&app)?;
    let metrics = cluster.raft.metrics().borrow().clone();
    Ok(Json(serde_json::json!({
        "id": metrics.id,
        "role": format!("{:?}", metrics.state),
        "leader": metrics.current_leader,
        "leader_addr": metrics.current_leader.and_then(|id| cluster.config.members.get(&id)),
        "term": metrics.current_term,
        "last_log_index": metrics.last_log_index,
        "last_applied": metrics.last_applied.map(|id| id.index),
        "snapshot": metrics.snapshot.map(|id| id.index),
        "purged": metrics.purged.map(|id| id.index),
        "healthy": metrics.running_state.is_ok() && app.view_healthy.load(std::sync::atomic::Ordering::Acquire),
    })))
}

#[utoipa::path(post, path = "/cluster/init", tag = "cluster", responses((status = 200, description = "Однократная инициализация фиксированного состава")))]
pub async fn initialize(
    State(app): State<AppState>,
) -> std::result::Result<&'static str, ApiError> {
    let cluster = cluster(&app)?;
    let members: BTreeMap<_, _> = cluster
        .config
        .members
        .iter()
        .map(|(id, addr)| (*id, BasicNode::new(addr.clone())))
        .collect();
    cluster
        .raft
        .initialize(members)
        .await
        .map_err(|error| ApiError::new(StatusCode::CONFLICT, error.to_string()))?;
    Ok("Ok.\n")
}

pub async fn append(
    State(app): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<AppendEntriesRequest<TypeConfig>>,
) -> std::result::Result<
    Json<std::result::Result<AppendEntriesResponse<u64>, RaftError<u64>>>,
    ApiError,
> {
    check_cluster(&app, &headers)?;
    Ok(Json(cluster(&app)?.raft.append_entries(request).await))
}

pub async fn vote(
    State(app): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<VoteRequest<u64>>,
) -> std::result::Result<Json<std::result::Result<VoteResponse<u64>, RaftError<u64>>>, ApiError> {
    check_cluster(&app, &headers)?;
    Ok(Json(cluster(&app)?.raft.vote(request).await))
}

pub async fn snapshot(
    State(app): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<InstallSnapshotRequest<TypeConfig>>,
) -> std::result::Result<
    Json<
        std::result::Result<
            InstallSnapshotResponse<u64>,
            RaftError<u64, openraft::error::InstallSnapshotError>,
        >,
    >,
    ApiError,
> {
    check_cluster(&app, &headers)?;
    Ok(Json(cluster(&app)?.raft.install_snapshot(request).await))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn configuration(root: &std::path::Path) -> Config {
        Config {
            data_dir: root.to_path_buf(),
            cluster: Some(crate::config::ClusterConfig {
                id: 1,
                members: [
                    (1, "node1:8123".into()),
                    (2, "node2:8123".into()),
                    (3, "node3:8123".into()),
                ]
                .into_iter()
                .collect(),
            }),
            ..Config::default()
        }
    }

    #[test]
    fn active_lease_and_identity_mismatch_do_not_remove_existing_view() {
        let root = tempfile::tempdir().unwrap();
        let config = configuration(root.path());
        let app = AppState::open(config.clone()).unwrap();
        let marker = root.path().join("view/marker");
        fs::write(&marker, b"kept").unwrap();
        assert!(AppState::open(config.clone()).is_err());
        assert_eq!(fs::read(&marker).unwrap(), b"kept");
        drop(app);
        let mut other = config.clone();
        other.cluster.as_mut().unwrap().id = 2;
        assert!(AppState::open(other).is_err());
        assert_eq!(fs::read(&marker).unwrap(), b"kept");
        fs::write(root.path().join("raft-state.json"), b"{truncated").unwrap();
        assert!(AppState::open(config).is_err());
        assert_eq!(fs::read(marker).unwrap(), b"kept");
    }
}
