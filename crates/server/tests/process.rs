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
        let log = root.join("server.log");
        let child = Command::new(env!("CARGO_BIN_EXE_miniclick"))
            .args(["--listen-addr", "127.0.0.1:0", "--data-dir"])
            .arg(root.join("data"))
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
