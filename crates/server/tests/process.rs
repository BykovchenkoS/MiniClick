#![cfg(unix)]

use reqwest::{Client, StatusCode};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Process {
    child: Child,
    url: String,
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Process {
    async fn start(root: &Path) -> Self {
        Self::start_with(root, &[], "server.log").await
    }

    async fn start_with(root: &Path, extra: &[String], log_name: &str) -> Self {
        let log = root.join(log_name);
        let child = Command::new(env!("CARGO_BIN_EXE_miniclick"))
            .args(["--listen-addr", "127.0.0.1:0", "--data-dir"])
            .arg(root.join("data"))
            .args(extra.iter().map(String::as_str))
            .current_dir(root)
            .env("RUST_LOG", "info")
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(&log).unwrap())
            .spawn()
            .unwrap();
        let mut process = Self {
            child,
            url: String::new(),
        };
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let text = std::fs::read_to_string(&log).unwrap();
            if let Some(address) = text
                .lines()
                .find(|line| line.contains("MiniClick запущен"))
                .and_then(|line| line.split("address=").nth(1))
                .and_then(|tail| tail.split_whitespace().next())
            {
                process.url = format!("http://{address}");
                return process;
            }
            assert!(
                process.child.try_wait().unwrap().is_none(),
                "server exited: {text}"
            );
            assert!(
                Instant::now() < deadline,
                "server readiness timeout: {text}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    async fn stop(mut self) {
        assert!(Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .unwrap()
            .success());
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(status.success());
                return;
            }
            assert!(Instant::now() < deadline, "server shutdown timeout");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}
fn client() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(300))
        .build()
        .unwrap()
}
async fn sql(client: &Client, process: &Process, query: &str) -> String {
    let response = client
        .post(format!("{}/query", process.url))
        .body(query.to_owned())
        .send()
        .await
        .unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{text}");
    text
}
async fn stats(client: &Client, process: &Process) -> serde_json::Value {
    client
        .get(format!("{}/tables/hits", process.url))
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap()["stats"]
        .clone()
}

async fn assert_query_results(client: &Client, process: &Process, parts: usize, granules: usize) {
    assert_eq!(
        sql(
            client,
            process,
            "SELECT count() AS n, sum(id) AS total, min(id) AS lo, max(id) AS hi FROM hits"
        )
        .await,
        "4\t7\t1\t3\n"
    );
    assert_eq!(sql(client, process, "SELECT text, count() AS n, sum(id) AS total FROM hits GROUP BY text ORDER BY n DESC, text ASC").await,
        "a\t2\t2\nb\t1\t2\nc\t1\t3\n");
    assert_eq!(
        sql(
            client,
            process,
            "SELECT text FROM hits WHERE id >= 2 ORDER BY id DESC LIMIT 1"
        )
        .await,
        "c\n"
    );
    let explain = sql(client, process, "EXPLAIN SELECT text, count() AS n FROM hits WHERE id >= 1 GROUP BY text ORDER BY n DESC, text ASC LIMIT 2").await;
    assert!(explain.contains("Limit 2"));
    assert!(explain.contains("Sort [count() DESC"));
    assert!(explain.contains("Aggregate by [`text`]"));
    assert!(
        explain.contains(&format!(
            "parts: {parts} / {parts}, granules: {granules} / {granules}, rows: 4 / 4"
        )),
        "{explain}"
    );
}

#[tokio::test]
async fn separate_process_restarts_preserve_rows_and_optimize_without_duplicates() {
    let root = tempfile::tempdir().unwrap();
    let process = Process::start(root.path()).await;
    let client = client();
    sql(&client, &process, "CREATE TABLE hits (id UInt64, text String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2").await;
    sql(
        &client,
        &process,
        "CREATE TABLE ephemeral (id UInt64) ENGINE = Memory",
    )
    .await;
    for (name, body) in [
        ("hits", "3,c\n1,a\n1,a\n"),
        ("hits", "2,b\n"),
        ("ephemeral", "99\n"),
    ] {
        let response = client
            .post(format!("{}/insert/{name}", process.url))
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    assert_eq!(stats(&client, &process).await["parts_total"], 2);
    assert_query_results(&client, &process, 2, 3).await;
    process.stop().await;
    let process = Process::start(root.path()).await;
    assert_eq!(sql(&client, &process, "SELECT * FROM ephemeral").await, "");
    assert_eq!(
        sql(
            &client,
            &process,
            "SELECT count(), sum(id), min(id), max(id) FROM ephemeral"
        )
        .await,
        "0\t0\t0\t0\n"
    );
    assert_eq!(
        sql(
            &client,
            &process,
            "SELECT id, count() AS n FROM ephemeral GROUP BY id ORDER BY id"
        )
        .await,
        ""
    );
    assert_query_results(&client, &process, 2, 3).await;
    let before = sql(&client, &process, "SELECT * FROM hits").await;
    let mut rows: Vec<_> = before.lines().collect();
    rows.sort();
    assert_eq!(rows, ["1\ta", "1\ta", "2\tb", "3\tc"]);
    assert_eq!(stats(&client, &process).await["parts_total"], 2);
    sql(&client, &process, "OPTIMIZE TABLE hits").await;
    assert_eq!(stats(&client, &process).await["parts_total"], 1);
    assert_query_results(&client, &process, 1, 2).await;
    process.stop().await;
    let process = Process::start(root.path()).await;
    assert_eq!(
        sql(&client, &process, "SELECT * FROM hits").await,
        "1\ta\n1\ta\n2\tb\n3\tc\n"
    );
    assert_eq!(stats(&client, &process).await["rows_total"], 4);
    assert_query_results(&client, &process, 1, 2).await;
    sql(&client, &process, "DROP TABLE hits").await;
    sql(
        &client,
        &process,
        "CREATE TABLE hits (id UInt64) ENGINE = MergeTree ORDER BY id",
    )
    .await;
    assert_eq!(sql(&client, &process, "SELECT * FROM hits").await, "");
    process.stop().await;
}

#[tokio::test]
#[ignore = "Миллион строк через HTTP; запускать отдельно в release"]
async fn million_rows_over_http_survive_optimize_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let process = Process::start(root.path()).await;
    let client = client();
    sql(&client, &process, server::generator::CREATE_SQL).await;
    for (rows, offset) in [(1_000_000, 0), (1000, 1_000_000)] {
        let mut body = Vec::new();
        server::generator::generate_csv(&mut body, rows, offset).unwrap();
        let response = client
            .post(format!("{}/insert/hits", process.url))
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "{}",
            response.text().await.unwrap()
        );
    }
    let indexed = sql(
        &client,
        &process,
        "SELECT user_id, delta FROM hits WHERE event_date = '2026-10-01'",
    )
    .await;
    let full = sql(
        &client,
        &process,
        "SELECT user_id, delta FROM hits WHERE event_date_text = '2026-10-01'",
    )
    .await;
    assert_eq!(indexed, full);
    assert_eq!(indexed.lines().count(), 33367);
    let explain = sql(
        &client,
        &process,
        "EXPLAIN SELECT user_id, delta FROM hits WHERE event_date = '2026-10-01'",
    )
    .await;
    assert!(
        explain.contains("parts: 2 / 2, granules: 6 / 124, rows: 41960 / 1001000"),
        "{explain}"
    );
    let log = std::fs::read_to_string(root.path().join("server.log")).unwrap();
    let scans: Vec<_> = log
        .lines()
        .filter(|line| line.contains("Выбраны parts и гранулы"))
        .collect();
    assert_eq!(scans.len(), 2);
    let counter = |line: &str, name: &str| -> usize {
        line.split(&format!("{name}="))
            .nth(1)
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse()
            .unwrap()
    };
    assert!(counter(scans[0], "granules_selected") < counter(scans[0], "granules_total"));
    assert_eq!(
        counter(scans[1], "granules_selected"),
        counter(scans[1], "granules_total")
    );
    assert_eq!(stats(&client, &process).await["rows_total"], 1_001_000);
    assert_eq!(stats(&client, &process).await["parts_total"], 2);
    let global_query = "SELECT count() AS n, sum(delta) AS total, min(event_date) AS first_day, max(event_date) AS last_day FROM hits";
    let global = sql(&client, &process, global_query).await;
    assert_eq!(global, "1001000\t-1810\t2026-10-01\t2026-10-30\n");
    let grouped_query = "SELECT event_date, count() AS n, sum(delta) AS total FROM hits GROUP BY event_date ORDER BY event_date";
    let grouped = sql(&client, &process, grouped_query).await;
    assert_eq!(grouped.lines().count(), 30);
    sql(&client, &process, "OPTIMIZE TABLE hits").await;
    assert_eq!(stats(&client, &process).await["parts_total"], 1);
    assert_eq!(sql(&client, &process, global_query).await, global);
    assert_eq!(sql(&client, &process, grouped_query).await, grouped);
    process.stop().await;
    let process = Process::start(root.path()).await;
    assert_eq!(stats(&client, &process).await["rows_total"], 1_001_000);
    assert_eq!(sql(&client, &process, global_query).await, global);
    assert_eq!(sql(&client, &process, grouped_query).await, grouped);
    let after = sql(
        &client,
        &process,
        "SELECT user_id, delta FROM hits WHERE event_date = '2026-10-01'",
    )
    .await;
    let mut expected: Vec<_> = indexed.lines().collect();
    expected.sort();
    let mut actual: Vec<_> = after.lines().collect();
    actual.sort();
    assert_eq!(actual, expected);
    process.stop().await;
}

#[test]
fn missing_registered_mergetree_manifest_fails_without_recreating_it() {
    let root = tempfile::tempdir().unwrap();
    let config = server::config::Config {
        data_dir: root.path().to_owned(),
        ..Default::default()
    };
    let app = server::AppState::open(config.clone()).unwrap();
    app.query
        .execute("CREATE TABLE hits (id UInt64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    drop(app);
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.path().join("catalog.json")).unwrap()).unwrap();
    let path = root
        .path()
        .join("tables")
        .join(manifest["tables"][0]["id"].as_str().unwrap())
        .join("table.json");
    std::fs::remove_file(&path).unwrap();
    assert!(server::AppState::open(config).is_err());
    assert!(!path.exists());
}

async fn try_sql(client: &Client, process: &Process, query: &str) -> Option<String> {
    let response = client
        .post(format!("{}/query", process.url))
        .body(query.to_owned())
        .send()
        .await
        .ok()?;
    if response.status() != StatusCode::OK {
        return None;
    }
    response.text().await.ok()
}

async fn wait_sql(client: &Client, process: &Process, query: &str, expected: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut last = None;
    loop {
        if let Some(text) = try_sql(client, process, query).await {
            if text == expected {
                return;
            }
            last = Some(text);
        }
        assert!(
            Instant::now() < deadline,
            "wait timeout for {query}: {last:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn try_parts(client: &Client, process: &Process) -> Option<u64> {
    let response = client
        .get(format!("{}/tables/hits", process.url))
        .send()
        .await
        .ok()?;
    if response.status() != StatusCode::OK {
        return None;
    }
    let value: serde_json::Value = response.json().await.ok()?;
    value["stats"]["parts_total"].as_u64()
}

async fn wait_parts(client: &Client, process: &Process, expected: u64) {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut last = None;
    loop {
        if let Some(parts) = try_parts(client, process).await {
            if parts == expected {
                return;
            }
            last = Some(parts);
        }
        assert!(
            Instant::now() < deadline,
            "wait timeout for parts {expected}: {last:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn replica_follows_leader_after_optimize_restart_and_recreate() {
    let leader_root = tempfile::tempdir().unwrap();
    let replica_root = tempfile::tempdir().unwrap();
    let client = client();

    let leader = Process::start(leader_root.path()).await;
    sql(
        &client,
        &leader,
        "CREATE TABLE hits (id UInt64, text String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2",
    )
    .await;
    for body in ["3,c\n", "1,a\n2,b\n"] {
        let response = client
            .post(format!("{}/insert/hits", leader.url))
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    assert_eq!(stats(&client, &leader).await["parts_total"], 2);

    let replica_args = vec![
        "--cluster-id".to_owned(),
        "miniclick".to_owned(),
        "--role".to_owned(),
        "Replica".to_owned(),
        "--leader-addr".to_owned(),
        leader.url.trim_start_matches("http://").to_owned(),
        "--leader-id".to_owned(),
        "node-1".to_owned(),
        "--sync-interval-ms".to_owned(),
        "50".to_owned(),
    ];
    let replica = Process::start_with(replica_root.path(), &replica_args, "replica.log").await;
    wait_sql(
        &client,
        &replica,
        "SELECT id, text FROM hits ORDER BY id",
        "1\ta\n2\tb\n3\tc\n",
    )
    .await;
    wait_parts(&client, &replica, 2).await;

    let write = client
        .post(format!("{}/insert/hits", replica.url))
        .body("9,z\n")
        .send()
        .await
        .unwrap();
    assert_eq!(write.status(), StatusCode::CONFLICT);

    sql(&client, &leader, "OPTIMIZE TABLE hits").await;
    wait_parts(&client, &replica, 1).await;
    assert_eq!(
        sql(&client, &replica, "SELECT count() FROM hits").await,
        "3\n"
    );

    sql(&client, &leader, "DROP TABLE hits").await;
    sql(
        &client,
        &leader,
        "CREATE TABLE hits (id UInt64, label String) ENGINE = MergeTree ORDER BY id",
    )
    .await;
    let response = client
        .post(format!("{}/insert/hits", leader.url))
        .body("7,z\n")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    wait_sql(
        &client,
        &replica,
        "SELECT id, label FROM hits ORDER BY id",
        "7\tz\n",
    )
    .await;
    assert_eq!(
        sql(&client, &replica, "SELECT count() FROM hits").await,
        "1\n"
    );

    replica.stop().await;
    let replica = Process::start_with(replica_root.path(), &replica_args, "replica.log").await;
    wait_sql(
        &client,
        &replica,
        "SELECT id, label FROM hits ORDER BY id",
        "7\tz\n",
    )
    .await;

    leader.stop().await;
    assert_eq!(
        sql(&client, &replica, "SELECT id, label FROM hits ORDER BY id").await,
        "7\tz\n"
    );
    replica.stop().await;
}

#[tokio::test]
async fn replica_keeps_data_when_leader_identity_does_not_match() {
    let leader_root = tempfile::tempdir().unwrap();
    let replica_root = tempfile::tempdir().unwrap();
    let foreign_root = tempfile::tempdir().unwrap();
    let client = client();

    let leader = Process::start(leader_root.path()).await;
    sql(
        &client,
        &leader,
        "CREATE TABLE hits (id UInt64) ENGINE = MergeTree ORDER BY id",
    )
    .await;
    let response = client
        .post(format!("{}/insert/hits", leader.url))
        .body("1\n")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let replica_args = vec![
        "--cluster-id".to_owned(),
        "miniclick".to_owned(),
        "--role".to_owned(),
        "Replica".to_owned(),
        "--leader-addr".to_owned(),
        leader.url.trim_start_matches("http://").to_owned(),
        "--leader-id".to_owned(),
        "node-1".to_owned(),
        "--sync-interval-ms".to_owned(),
        "50".to_owned(),
    ];
    let replica = Process::start_with(replica_root.path(), &replica_args, "replica.log").await;
    wait_sql(&client, &replica, "SELECT id FROM hits", "1\n").await;
    replica.stop().await;
    leader.stop().await;

    let foreign = Process::start_with(
        foreign_root.path(),
        &[
            "--cluster-id".to_owned(),
            "other".to_owned(),
            "--node-id".to_owned(),
            "node-2".to_owned(),
        ],
        "foreign.log",
    )
    .await;
    let foreign_args = vec![
        "--cluster-id".to_owned(),
        "miniclick".to_owned(),
        "--role".to_owned(),
        "Replica".to_owned(),
        "--leader-addr".to_owned(),
        foreign.url.trim_start_matches("http://").to_owned(),
        "--leader-id".to_owned(),
        "node-1".to_owned(),
        "--sync-interval-ms".to_owned(),
        "50".to_owned(),
    ];
    let replica = Process::start_with(replica_root.path(), &foreign_args, "replica.log").await;

    // Чужой лидер пуст; при неверной идентичности реплика не должна ничего менять.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(sql(&client, &replica, "SELECT id FROM hits").await, "1\n");
    assert_eq!(try_parts(&client, &replica).await, Some(1));

    replica.stop().await;
    foreign.stop().await;
}

#[tokio::test]
async fn replica_catches_up_after_downtime_and_optimize() {
    let leader_root = tempfile::tempdir().unwrap();
    let replica_root = tempfile::tempdir().unwrap();
    let client = client();

    let leader = Process::start(leader_root.path()).await;
    sql(
        &client,
        &leader,
        "CREATE TABLE hits (id UInt64, text String) ENGINE = MergeTree ORDER BY id",
    )
    .await;
    let response = client
        .post(format!("{}/insert/hits", leader.url))
        .body("1,a\n")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let replica_args = vec![
        "--cluster-id".to_owned(),
        "miniclick".to_owned(),
        "--role".to_owned(),
        "Replica".to_owned(),
        "--leader-addr".to_owned(),
        leader.url.trim_start_matches("http://").to_owned(),
        "--leader-id".to_owned(),
        "node-1".to_owned(),
        "--sync-interval-ms".to_owned(),
        "50".to_owned(),
    ];
    let replica = Process::start_with(replica_root.path(), &replica_args, "replica.log").await;
    wait_sql(
        &client,
        &replica,
        "SELECT id, text FROM hits ORDER BY id",
        "1\ta\n",
    )
    .await;
    replica.stop().await;

    // Пока реплика выключена, лидер получает новые строки и объединяет parts.
    for body in ["2,b\n", "3,c\n"] {
        let response = client
            .post(format!("{}/insert/hits", leader.url))
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    sql(&client, &leader, "OPTIMIZE TABLE hits").await;
    assert_eq!(stats(&client, &leader).await["parts_total"], 1);

    let replica = Process::start_with(replica_root.path(), &replica_args, "replica.log").await;
    wait_sql(
        &client,
        &replica,
        "SELECT id, text FROM hits ORDER BY id",
        "1\ta\n2\tb\n3\tc\n",
    )
    .await;
    assert_eq!(
        sql(&client, &replica, "SELECT count() FROM hits").await,
        "3\n"
    );

    replica.stop().await;
    leader.stop().await;
}

#[tokio::test]
async fn replica_sigterm_cancels_a_leader_that_never_answers() {
    let root = tempfile::tempdir().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (accepted, connected) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel();
    let blackhole = std::thread::spawn(move || {
        let (connection, _) = listener.accept().unwrap();
        accepted.send(()).unwrap();
        let _ = released.recv_timeout(Duration::from_secs(20));
        drop(connection);
    });
    let args = vec![
        "--role".into(),
        "Replica".into(),
        "--leader-addr".into(),
        address,
        "--leader-id".into(),
        "node-1".into(),
    ];
    let replica = Process::start_with(root.path(), &args, "replica.log").await;
    connected.recv_timeout(Duration::from_secs(5)).unwrap();
    let start = Instant::now();
    replica.stop().await;
    let elapsed = start.elapsed();
    let _ = release.send(());
    blackhole.join().unwrap();
    assert!(
        elapsed < Duration::from_secs(5),
        "SIGTERM занял {elapsed:?}"
    );
}
