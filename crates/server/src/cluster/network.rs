//! RPC OpenRaft поверх HTTP; идентичность кластера проверяется до обработки.

use super::types::TypeConfig;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use serde::{de::DeserializeOwned, Serialize};

pub struct Network {
    pub client: reqwest::Client,
    pub cluster_id: String,
}

pub struct Connection {
    client: reqwest::Client,
    cluster_id: String,
    target: u64,
    node: BasicNode,
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = Connection;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        Connection {
            client: self.client.clone(),
            cluster_id: self.cluster_id.clone(),
            target,
            node: node.clone(),
        }
    }
}

impl Connection {
    async fn send<
        Q: Serialize + Sync,
        R: DeserializeOwned,
        E: std::error::Error + DeserializeOwned,
    >(
        &self,
        path: &str,
        request: &Q,
        option: RPCOption,
    ) -> Result<R, RPCError<u64, BasicNode, E>> {
        let response = self
            .client
            .post(format!("http://{}/cluster/{path}", self.node.addr))
            .header("x-miniclick-cluster", &self.cluster_id)
            .timeout(option.hard_ttl())
            .json(request)
            .send()
            .await
            .map_err(|error| RPCError::Network(NetworkError::new(&error)))?
            .error_for_status()
            .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
        let result: Result<R, E> = response
            .json()
            .await
            .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
        result.map_err(|error| {
            RPCError::RemoteError(RemoteError::new_with_node(
                self.target,
                self.node.clone(),
                error,
            ))
        })
    }
}

impl RaftNetwork<TypeConfig> for Connection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.send("append", &rpc, option).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.send("vote", &rpc, option).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        self.send("snapshot", &rpc, option).await
    }
}
