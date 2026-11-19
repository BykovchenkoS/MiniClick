use reqwest::{Client, StatusCode};
use serde_json::Value;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Cluster {
    root: tempfile::TempDir,
    addresses: Vec<String>,
    processes: Vec<Option<Child>>,
    client: Client,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for child in self.processes.iter_mut().flatten() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Cluster {
    async fn start() -> Self {
        Self::start_with_limit(256 * 1024 * 1024).await
    }

    async fn start_with_limit(limit: usize) -> Self {
        Self::start_with_config(limit, "process-test", "").await
    }

    async fn start_with_config(limit: usize, cluster_id: &str, placement: &str) -> Self {
        let listeners: Vec<_> = (0..3)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let addresses: Vec<_> = listeners
            .iter()
            .map(|listener| listener.local_addr().unwrap().to_string())
            .collect();
        let mut cluster = Self {
            root: tempfile::tempdir().unwrap(),
            addresses,
            processes: vec![None, None, None],
            client: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
        };
        drop(listeners);
        for node in 0..3 {
            let config = format!("max_body_bytes = {limit}\nlisten_addr = '{}'\ndata_dir = '{}'\ncluster_id = '{cluster_id}'\nnode_id = 'node{}'\n[cluster]\nid = {}\nmembers = {{ '1' = '{}', '2' = '{}', '3' = '{}' }}\n{placement}", cluster.addresses[node], cluster.root.path().join(format!("data{node}")).display(), node+1, node+1, cluster.addresses[0], cluster.addresses[1], cluster.addresses[2]);
            std::fs::write(cluster.config(node), config).unwrap();
            cluster.restart(node).await;
        }
        cluster
    }

    fn config(&self, node: usize) -> PathBuf {
        self.root.path().join(format!("node{node}.toml"))
    }

    fn url(&self, node: usize, path: &str) -> String {
        format!("http://{}{}", self.addresses[node], path)
    }

    fn logs(&self) -> String {
        (0..3)
            .map(|node| {
                std::fs::read_to_string(self.root.path().join(format!("node{node}.log")))
                    .unwrap_or_default()
                    .lines()
                    .rev()
                    .take(25)
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    async fn restart(&mut self, node: usize) {
        assert!(self.processes[node].is_none());
        self.processes[node] = Some(
            Command::new(env!("CARGO_BIN_EXE_miniclick"))
                .arg("--config")
                .arg(self.config(node))
                .stdout(Stdio::null())
                .stderr(
                    std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(self.root.path().join(format!("node{node}.log")))
                        .unwrap(),
                )
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Ok(response) = self.client.get(self.url(node, "/ping")).send().await {
                if response.status().is_success() {
                    return;
                }
            }
            assert!(
                self.processes[node]
                    .as_mut()
                    .unwrap()
                    .try_wait()
                    .unwrap()
                    .is_none(),
                "{}",
                self.logs()
            );
            assert!(Instant::now() < deadline, "{}", self.logs());
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn kill(&mut self, node: usize) {
        let mut child = self.processes[node].take().unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
    }

    async fn status(&self, node: usize) -> Value {
        let response = self
            .client
            .get(self.url(node, "/cluster/status"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{}", self.logs());
        response.json().await.unwrap()
    }

    async fn leader(&self) -> usize {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            for node in 0..3 {
                if self.processes[node].is_some() {
                    let status = self.status(node).await;
                    if status["role"] == "Leader" && status["leader"] == (node + 1) as u64 {
                        return node;
                    }
                }
            }
            assert!(Instant::now() < deadline, "leader timeout: {}", self.logs());
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    }

    async fn sql(&self, node: usize, sql: &str) -> String {
        let response = self
            .client
            .post(self.url(node, "/query"))
            .body(sql.to_owned())
            .send()
            .await
            .unwrap();
        let status = response.status();
        let text = response.text().await.unwrap();
        assert_eq!(status, StatusCode::OK, "{text}\n{}", self.logs());
        text
    }

    async fn insert(&self, node: usize, body: &str) -> StatusCode {
        self.client
            .post(self.url(node, "/insert/hits?format=CSV"))
            .body(body.to_owned())
            .send()
            .await
            .unwrap()
            .status()
    }

    async fn await_rows(&self, node: usize, expected: &str) {
        self.await_query(node, "SELECT id, value FROM hits ORDER BY id", expected)
            .await;
    }

    async fn await_query(&self, node: usize, query: &str, expected: &str) {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let response = self
                .client
                .post(self.url(node, "/query"))
                .body(query.to_owned())
                .send()
                .await
                .unwrap();
            let status = response.status();
            let text = response.text().await.unwrap();
            if status == StatusCode::OK && text == expected {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "node {}: {status} {text}\n{}",
                node + 1,
                self.logs()
            );
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    }
}

#[tokio::test]
async fn coordinator_reads_committed_shard_rows_and_inserts_after_raft_leader_failure() {
    let mut cluster = Cluster::start_with_config(
        256 * 1024 * 1024,
        "process-test",
        "\n[local_shards.hits]\nkey = 'id'\nshard_id = 1\nshard_ids = [1, 2]\n",
    )
    .await;
    let response = cluster
        .client
        .post(cluster.url(0, "/cluster/init"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let first = cluster.leader().await;
    let listeners: Vec<_> = (0..2)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    cluster.addresses.extend(
        listeners
            .iter()
            .map(|listener| listener.local_addr().unwrap().to_string()),
    );
    cluster.processes.extend([None, None]);
    let (odd, coordinator) = (3, 4);
    let odd_config = format!(
        "listen_addr = '{}'\ndata_dir = '{}'\n[local_shards.hits]\nkey = 'id'\nshard_id = 2\nshard_ids = [1, 2]\n",
        cluster.addresses[odd], cluster.root.path().join("odd").display()
    );
    let coordinator_config = format!(
        "listen_addr = '{}'\ndata_dir = '{}'\n[sharding.hits]\nlocal_table = 'hits'\nkey = 'id'\nshards = {{ '1' = '{}', '2' = '{}' }}\n[sharding.hits.raft_shards.'1']\ncluster_id = 'process-test'\nmembers = {:?}\n",
        cluster.addresses[coordinator], cluster.root.path().join("coordinator").display(),
        cluster.addresses[first], cluster.addresses[odd], &cluster.addresses[..3]
    );
    std::fs::write(cluster.config(odd), odd_config).unwrap();
    std::fs::write(cluster.config(coordinator), coordinator_config).unwrap();
    drop(listeners);
    cluster.restart(odd).await;
    cluster.restart(coordinator).await;
    let ddl = "CREATE TABLE hits (id UInt64, value String) ENGINE = MergeTree ORDER BY id";
    cluster.sql(first, ddl).await;
    cluster.sql(odd, ddl).await;
    assert_eq!(
        cluster.insert(coordinator, "2,before\n").await,
        StatusCode::OK
    );
    assert_eq!(
        cluster.insert(coordinator, "1,ordinary\n").await,
        StatusCode::OK
    );

    cluster.kill(first);
    let second = cluster.leader().await;
    assert_ne!(first, second);
    let query = "SELECT id, value FROM hits ORDER BY id";
    assert_eq!(
        cluster.sql(coordinator, query).await,
        "1\tordinary\n2\tbefore\n"
    );
    assert_eq!(
        cluster.insert(coordinator, "4,after\n").await,
        StatusCode::OK
    );
    assert_eq!(
        cluster.insert(second, "6,allowed\n7,wrong-shard\n").await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        cluster.sql(coordinator, query).await,
        "1\tordinary\n2\tbefore\n4\tafter\n"
    );

    cluster.restart(first).await;
    cluster.await_rows(first, "2\tbefore\n4\tafter\n").await;
}

#[tokio::test]
async fn coordinator_uses_independent_raft_groups_when_each_shard_elects_a_new_leader() {
    let (mut even, mut odd) = tokio::join!(
        Cluster::start_with_config(
            256 * 1024 * 1024,
            "even-group",
            "\n[local_shards.hits]\nkey = 'id'\nshard_id = 1\nshard_ids = [1, 2]\n",
        ),
        Cluster::start_with_config(
            256 * 1024 * 1024,
            "odd-group",
            "\n[local_shards.hits]\nkey = 'id'\nshard_id = 2\nshard_ids = [1, 2]\n",
        )
    );
    for group in [&even, &odd] {
        let response = group
            .client
            .post(group.url(0, "/cluster/init"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    let (even_first, odd_first) = tokio::join!(even.leader(), odd.leader());
    let ddl = "CREATE TABLE hits (id UInt64, value String) ENGINE = MergeTree ORDER BY id";
    even.sql(even_first, ddl).await;
    odd.sql(
        odd_first,
        "CREATE TABLE hits (value String, id UInt64) ENGINE = MergeTree ORDER BY id",
    )
    .await;

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let coordinator = even.addresses.len();
    even.addresses
        .push(listener.local_addr().unwrap().to_string());
    even.processes.push(None);
    let config = format!(
        "listen_addr = '{}'\ndata_dir = '{}'\n[sharding.hits]\nlocal_table = 'hits'\nkey = 'id'\nshards = {{ '1' = '{}', '2' = '{}' }}\n[sharding.hits.schema]\ncolumns = [{{ name = 'id', data_type = 'UInt64' }}, {{ name = 'value', data_type = 'String' }}]\n[sharding.hits.raft_shards.'1']\ncluster_id = 'even-group'\nmembers = {:?}\n[sharding.hits.raft_shards.'2']\ncluster_id = 'odd-group'\nmembers = {:?}\n",
        even.addresses[coordinator], even.root.path().join("coordinator").display(),
        even.addresses[even_first], odd.addresses[odd_first], &even.addresses[..3], &odd.addresses,
    );
    std::fs::write(even.config(coordinator), config).unwrap();
    drop(listener);
    even.restart(coordinator).await;

    assert_eq!(
        even.insert(coordinator, "1,99\n").await,
        StatusCode::BAD_GATEWAY
    );
    assert_eq!(odd.sql(odd_first, "SELECT count() FROM hits").await, "0\n");
    odd.sql(odd_first, "DROP TABLE hits").await;
    odd.sql(odd_first, ddl).await;

    assert_eq!(
        even.insert(coordinator, "1,odd-before\n").await,
        StatusCode::OK
    );
    assert_eq!(
        even.insert(coordinator, "2,even-before\n").await,
        StatusCode::OK
    );
    let even_term = even.status(even_first).await["term"].clone();
    let query = "SELECT id, value FROM hits ORDER BY id";
    odd.kill(odd_first);
    assert_ne!(odd.leader().await, odd_first);
    let even_status = even.status(even_first).await;
    assert_eq!(even_status["role"], "Leader");
    assert_eq!(even_status["term"], even_term);
    assert_eq!(
        even.sql(coordinator, query).await,
        "1\todd-before\n2\teven-before\n"
    );
    assert_eq!(
        even.insert(coordinator, "3,odd-after\n").await,
        StatusCode::OK
    );

    even.kill(even_first);
    let (even_second, odd_second) = tokio::join!(even.leader(), odd.leader());
    assert_ne!(even_second, even_first);
    assert_eq!(
        even.insert(coordinator, "4,even-after\n").await,
        StatusCode::OK
    );
    assert_eq!(
        even.sql(coordinator, query).await,
        "1\todd-before\n2\teven-before\n3\todd-after\n4\teven-after\n"
    );
    assert_eq!(
        even.sql(even_second, query).await,
        "2\teven-before\n4\teven-after\n"
    );
    assert_eq!(
        odd.sql(odd_second, query).await,
        "1\todd-before\n3\todd-after\n"
    );

    // Первый нечётный лидер всё ещё остановлен. Остановка второго оставляет один голос.
    odd.kill(odd_second);
    assert_eq!(
        even.insert(coordinator, "6,with-odd-down\n").await,
        StatusCode::OK
    );
    assert_eq!(
        even.insert(coordinator, "5,rejected\n7,rejected\n").await,
        StatusCode::BAD_GATEWAY
    );
    assert_eq!(
        even.insert(coordinator, "8,allowed\n9,mixed\n").await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        even.sql(even_second, query).await,
        "2\teven-before\n4\teven-after\n6\twith-odd-down\n"
    );
    odd.restart(odd_first).await;
    // Сетевой ping возвращается раньше, чем новый лидер подтвердит чтение кворумом.
    even.await_query(
        coordinator,
        query,
        "1\todd-before\n2\teven-before\n3\todd-after\n4\teven-after\n6\twith-odd-down\n",
    )
    .await;
    odd.await_rows(odd_first, "1\todd-before\n3\todd-after\n")
        .await;
}

#[tokio::test]
async fn committed_rows_survive_two_leader_crashes_and_full_cluster_restart() {
    let mut cluster = Cluster::start().await;
    // До init все узлы доступны, но не принимают кластерную запись.
    assert_eq!(
        cluster.insert(0, "1,alpha\n").await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    let response = cluster
        .client
        .post(cluster.url(0, "/cluster/init"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK, "{}", cluster.logs());
    let first = cluster.leader().await;
    cluster
        .sql(
            first,
            "CREATE TABLE hits (id UInt64, value String) ENGINE = MergeTree ORDER BY id",
        )
        .await;
    assert_eq!(
        cluster.insert(first, "3,gamma\n1,alpha\n2,beta\n").await,
        StatusCode::OK
    );
    for node in 0..3 {
        cluster
            .await_rows(node, "1\talpha\n2\tbeta\n3\tgamma\n")
            .await;
    }
    let follower = (first + 1) % 3;
    assert_eq!(
        cluster.insert(follower, "99,forbidden\n").await,
        StatusCode::CONFLICT
    );
    assert_eq!(
        cluster.insert(first, "4,delta\nbroken\n").await,
        StatusCode::BAD_REQUEST
    );
    cluster.kill(first);
    let second = cluster.leader().await;
    assert_ne!(first, second);
    cluster
        .await_rows(second, "1\talpha\n2\tbeta\n3\tgamma\n")
        .await;
    assert_eq!(
        cluster.insert(second, "5,epsilon\n4,delta\n").await,
        StatusCode::OK
    );
    cluster.restart(first).await;
    cluster
        .await_rows(first, "1\talpha\n2\tbeta\n3\tgamma\n4\tdelta\n5\tepsilon\n")
        .await;
    cluster.kill(second);
    let third = cluster.leader().await;
    assert_ne!(second, third);
    assert_eq!(cluster.insert(third, "6,zeta\n").await, StatusCode::OK);
    cluster.restart(second).await;
    let expected = "1\talpha\n2\tbeta\n3\tgamma\n4\tdelta\n5\tepsilon\n6\tzeta\n";
    for node in 0..3 {
        cluster.await_rows(node, expected).await;
    }
    for node in 0..3 {
        cluster.kill(node);
    }
    for node in 0..3 {
        cluster.restart(node).await;
    }
    let leader = cluster.leader().await;
    for node in 0..3 {
        cluster.await_rows(node, expected).await;
    }
    // Один оставшийся процесс не может подтвердить новую запись.
    for node in 0..3 {
        if node != leader {
            cluster.kill(node);
        }
    }
    assert_eq!(
        cluster.insert(leader, "7,unconfirmed\n").await,
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn returning_follower_installs_snapshot_after_missing_logs_were_purged() {
    let mut cluster = Cluster::start_with_limit(1024).await;
    assert_eq!(
        cluster
            .client
            .post(cluster.url(0, "/cluster/init"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let leader = cluster.leader().await;
    cluster
        .sql(
            leader,
            "CREATE TABLE hits (id UInt64, value String) ENGINE = MergeTree ORDER BY id",
        )
        .await;
    assert_eq!(
        cluster.insert(leader, "1,alpha\n2,beta\n").await,
        StatusCode::OK
    );
    let follower = (leader + 1) % 3;
    cluster.await_rows(follower, "1\talpha\n2\tbeta\n").await;
    let previous_applied = cluster.status(follower).await["last_applied"]
        .as_u64()
        .unwrap();
    cluster.kill(follower);
    for _ in 0..110 {
        cluster.sql(leader, "OPTIMIZE TABLE hits").await;
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    let snapshot_index = loop {
        let status = cluster.status(leader).await;
        if status["purged"]
            .as_u64()
            .is_some_and(|index| index > previous_applied)
        {
            break status["snapshot"].as_u64().unwrap();
        }
        assert!(
            Instant::now() < deadline,
            "snapshot timeout: {}",
            cluster.logs()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(cluster.insert(leader, "3,gamma\n").await, StatusCode::OK);
    cluster.restart(follower).await;
    cluster
        .await_rows(follower, "1\talpha\n2\tbeta\n3\tgamma\n")
        .await;
    let status = cluster.status(follower).await;
    assert!(
        status["snapshot"]
            .as_u64()
            .is_some_and(|index| index >= snapshot_index),
        "{status}"
    );
    cluster.kill(follower);
    cluster.restart(follower).await;
    cluster
        .await_rows(follower, "1\talpha\n2\tbeta\n3\tgamma\n")
        .await;
}

#[tokio::test]
async fn client_limit_allows_raft_encoding_overhead_and_rejects_oversized_client_body() {
    let cluster = Cluster::start_with_limit(1024).await;
    assert_eq!(
        cluster
            .client
            .post(cluster.url(0, "/cluster/init"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let leader = cluster.leader().await;
    cluster
        .sql(
            leader,
            "CREATE TABLE hits (id UInt64, value String) ENGINE = MergeTree ORDER BY id",
        )
        .await;
    let value = "x".repeat(850);
    assert_eq!(
        cluster.insert(leader, &format!("1,{value}\n")).await,
        StatusCode::OK
    );
    assert_eq!(
        cluster
            .insert(leader, &format!("2,{}\n", "y".repeat(1024)))
            .await,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    assert_eq!(cluster.insert(leader, "2,beta\n").await, StatusCode::OK);
    for node in 0..3 {
        cluster
            .await_rows(node, &format!("1\t{value}\n2\tbeta\n"))
            .await;
    }
}
