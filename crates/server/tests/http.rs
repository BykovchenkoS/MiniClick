use reqwest::{Client, StatusCode};
use server::{config::Config, AppState};
use std::sync::Arc;
use tokio::{sync::oneshot, task::JoinHandle};
use types::{Block, Column, ColumnData, ColumnDef, DataType, DbError, QueryEngine, QueryOutput};

struct TestServer {
    url: String,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}
impl TestServer {
    async fn start(state: AppState) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, server::router(state))
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        Self {
            url,
            stop: Some(stop),
            task: Some(task),
        }
    }
    async fn close(mut self) {
        let _ = self.stop.take().unwrap().send(());
        self.task.take().unwrap().await.unwrap();
    }
}
impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
fn client() -> Client {
    Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap()
}
fn state(root: &std::path::Path) -> AppState {
    AppState::open(Config {
        data_dir: root.to_owned(),
        ..Config::default()
    })
    .unwrap()
}
async fn sql(client: &Client, server: &TestServer, query: &str) -> String {
    let response = client
        .post(format!("{}/query", server.url))
        .body(query.to_owned())
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

#[tokio::test]
async fn real_http_create_insert_select_and_atomic_bad_input_for_both_engines() {
    for engine in ["Memory", "MergeTree"] {
        let root = tempfile::tempdir().unwrap();
        let server = TestServer::start(state(root.path())).await;
        let client = client();
        assert_eq!(
            client
                .get(format!("{}/ping", server.url))
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
            "Ok.\n"
        );
        assert_eq!(sql(&client, &server, &format!("CREATE TABLE hits (id UInt64, text String) ENGINE = {engine} ORDER BY id SETTINGS index_granularity = 2")).await, "Ok.\n");
        let response = client
            .post(format!("{}/insert/hits", server.url))
            .body("3,c\n1,a\n2,b\n")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.text().await.unwrap(), "Ok.\n");
        assert_eq!(
            sql(
                &client,
                &server,
                "SELECT text FROM hits WHERE id = 2 LIMIT 1"
            )
            .await,
            "b\n"
        );
        let bad = client
            .post(format!("{}/insert/hits", server.url))
            .body("4,d\nbroken,e\n")
            .send()
            .await
            .unwrap();
        assert_eq!(bad.status(), StatusCode::BAD_REQUEST);
        assert!(bad.json::<serde_json::Value>().await.unwrap()["error"]
            .as_str()
            .unwrap()
            .contains("row 2"));
        let describe: serde_json::Value = client
            .get(format!("{}/tables/hits", server.url))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(describe["stats"]["rows_total"], 3);
        assert_eq!(describe["engine"], engine);
        assert_eq!(describe["schema"]["columns"][0]["data_type"], "UInt64");
        let response = client
            .post(format!("{}/insert/hits?format=JSONEachRow", server.url))
            .body("{\"text\":\"Юникод\",\"id\":\"4\"}\n")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let response = client
            .post(format!("{}/query?format=JSONEachRow", server.url))
            .body("SELECT id, text FROM hits WHERE id = 4")
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.headers()["content-type"],
            "application/x-ndjson; charset=utf-8"
        );
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap(),
            serde_json::json!({"id":4,"text":"Юникод"})
        );
        assert_eq!(sql(&client, &server, "OPTIMIZE TABLE hits").await, "Ok.\n");
        assert_eq!(sql(&client, &server, "DROP TABLE hits").await, "Ok.\n");
        let tables: serde_json::Value = client
            .get(format!("{}/tables", server.url))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(tables, serde_json::json!([]));
        server.close().await;
    }
}

#[tokio::test]
async fn input_formats_errors_and_unknown_format_have_no_side_effects() {
    let root = tempfile::tempdir().unwrap();
    let server = TestServer::start(state(root.path())).await;
    let client = client();
    let response = client
        .post(format!("{}/query?format=Unknown", server.url))
        .body("CREATE TABLE unwanted (id UInt64) ENGINE = Memory")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    sql(
        &client,
        &server,
        "CREATE TABLE hits (id UInt64, text String) ENGINE = Memory",
    )
    .await;
    let response = client
        .post(format!("{}/insert/hits?format=TSV", server.url))
        .body("1\tfirst\\tline\n")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        sql(&client, &server, "SELECT text FROM hits").await,
        "first\\tline\n"
    );
    let cases = [
        ("/query", "SELECT * FROM missing", StatusCode::NOT_FOUND),
        ("/query", "SELECT absent FROM hits", StatusCode::BAD_REQUEST),
        (
            "/query",
            "SELECT * FROM hits ORDER BY id + 1",
            StatusCode::BAD_REQUEST,
        ),
        (
            "/query",
            "CREATE TABLE hits (id UInt64) ENGINE = Memory",
            StatusCode::BAD_REQUEST,
        ),
        ("/query?format=", "DROP TABLE hits", StatusCode::BAD_REQUEST),
        (
            "/query?format=CSV&format=TSV",
            "DROP TABLE hits",
            StatusCode::BAD_REQUEST,
        ),
        ("/insert/hits?format=Pretty", "2,x", StatusCode::BAD_REQUEST),
        (
            "/insert/hits?format=JSONEachRow",
            "{\"id\":2,\"id\":3,\"text\":\"bad\"}",
            StatusCode::BAD_REQUEST,
        ),
        (
            "/insert/hits?format=JSONEachRow",
            "{\"id\":2,\"text\":null}",
            StatusCode::BAD_REQUEST,
        ),
    ];
    for (path, body, expected) in cases {
        let response = client
            .post(format!("{}{path}", server.url))
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), expected, "{path}: {body}");
        assert!(response.json::<serde_json::Value>().await.unwrap()["error"].is_string());
    }
    let tables: serde_json::Value = client
        .get(format!("{}/tables", server.url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(tables.as_array().unwrap().len(), 1);
    assert_eq!(tables[0]["name"], "hits");
    assert_eq!(sql(&client, &server, "SELECT id FROM hits").await, "1\n");
    let bad_path = client
        .get(format!("{}/tables/%FF", server.url))
        .send()
        .await
        .unwrap();
    assert_eq!(bad_path.status(), StatusCode::BAD_REQUEST);
    assert!(bad_path.json::<serde_json::Value>().await.unwrap()["error"].is_string());
    server.close().await;
}

#[tokio::test]
async fn body_limit_applies_to_both_endpoints_with_json_errors() {
    let root = tempfile::tempdir().unwrap();
    let mut app = state(root.path());
    app.catalog
        .create_table(
            types::TableDefinition {
                name: "hits".into(),
                schema: types::TableSchema {
                    columns: vec![ColumnDef {
                        name: "text".into(),
                        data_type: DataType::String,
                    }],
                },
                engine: types::Engine::Memory,
                order_by: vec![],
                settings: Default::default(),
            },
            false,
        )
        .unwrap();
    app.config.max_body_bytes = 8;
    let server = TestServer::start(app).await;
    let client = client();
    for path in ["/insert/hits", "/query"] {
        let response = client
            .post(format!("{}{path}", server.url))
            .body("x".repeat(9))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(response.json::<serde_json::Value>().await.unwrap()["error"].is_string());
    }
    let response = client
        .post(format!("{}/insert/hits", server.url))
        .body("x".repeat(8))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    server.close().await;
}

struct BrokenEngine;
impl QueryEngine for BrokenEngine {
    fn execute(&self, sql: &str) -> types::Result<QueryOutput> {
        if sql == "replica" {
            return Err(DbError::NotLeader {
                leader: "http://leader:8123".into(),
            });
        }
        let block = Block::new(vec![Column {
            name: "id".into(),
            data: ColumnData::UInt64(vec![42]),
        }])?;
        Ok(QueryOutput::Rows {
            header: block.header(),
            stream: Box::new(
                vec![
                    Ok(block),
                    Err(DbError::Internal("late scan failure".into())),
                ]
                .into_iter(),
            ),
        })
    }
}

#[tokio::test]
async fn late_scan_error_returns_only_error_and_not_a_partial_success() {
    let root = tempfile::tempdir().unwrap();
    let mut app = state(root.path());
    app.query = Arc::new(BrokenEngine);
    let server = TestServer::start(app).await;
    let client = client();
    let response = client
        .post(format!("{}/query", server.url))
        .body("scan")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        response.json::<serde_json::Value>().await.unwrap(),
        serde_json::json!({"error":"internal error: late scan failure"})
    );
    let response = client
        .post(format!("{}/query", server.url))
        .body("replica")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert!(response.json::<serde_json::Value>().await.unwrap()["error"]
        .as_str()
        .unwrap()
        .contains("http://leader:8123"));
    server.close().await;
}

#[tokio::test]
async fn openapi_matches_routes_formats_binary_upload_and_vendored_assets() {
    let root = tempfile::tempdir().unwrap();
    let server = TestServer::start(state(root.path())).await;
    let client = client();
    let doc: serde_json::Value = client
        .get(format!("{}/api-doc/openapi.json", server.url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    for (path, method) in [
        ("/ping", "get"),
        ("/help", "get"),
        ("/tables", "get"),
        ("/tables/{name}", "get"),
        ("/insert/{table}", "post"),
        ("/query", "post"),
    ] {
        assert!(doc["paths"][path][method].is_object(), "{path}");
    }
    assert_eq!(doc["components"]["schemas"]["BinaryBody"]["type"], "string");
    assert_eq!(
        doc["components"]["schemas"]["BinaryBody"]["format"],
        "binary"
    );
    assert_eq!(
        doc["components"]["schemas"]["InputFormat"]["enum"],
        serde_json::json!(["CSV", "TSV", "JSONEachRow"])
    );
    assert!(doc["paths"]["/query"]["post"]["requestBody"]["content"]["text/plain"].is_object());
    let examples =
        &doc["paths"]["/query"]["post"]["requestBody"]["content"]["text/plain"]["examples"];
    assert!(examples["group_by"]["value"]
        .as_str()
        .unwrap()
        .contains("GROUP BY"));
    assert!(examples["explain"]["value"]
        .as_str()
        .unwrap()
        .starts_with("EXPLAIN SELECT"));
    let html = client
        .get(format!("{}/swagger-ui", server.url))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(html.contains("swagger-ui-bundle.js"));
    let javascript = client
        .get(format!("{}/swagger-ui/swagger-ui-bundle.js", server.url))
        .send()
        .await
        .unwrap();
    assert_eq!(javascript.status(), StatusCode::OK);
    assert!(javascript.bytes().await.unwrap().len() > 100_000);
    server.close().await;
}

async fn json_rows(client: &Client, server: &TestServer, query: &str) -> Vec<serde_json::Value> {
    let response = client
        .post(format!("{}/query?format=JSONEachRow", server.url))
        .body(query.to_owned())
        .send()
        .await
        .unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{text}");
    text.lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

async fn seed_queries(client: &Client, server: &TestServer, engine: &str) {
    sql(client, server, &format!("CREATE TABLE events (id UInt64, label String, amount Int64, ratio Float64) ENGINE = {engine} ORDER BY id SETTINGS index_granularity = 2")).await;
    for body in [
        "4,b,40,0.1\n1,a,10,0.2\n3,b,30,0.3\n",
        "2,a,20,0.4\n2,a,5,0.5\n",
    ] {
        let response = client
            .post(format!("{}/insert/events", server.url))
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}

#[tokio::test]
async fn aggregates_grouping_and_hidden_sort_keys_work_over_http_for_both_engines() {
    for engine in ["Memory", "MergeTree"] {
        let root = tempfile::tempdir().unwrap();
        let server = TestServer::start(state(root.path())).await;
        let client = client();
        seed_queries(&client, &server, engine).await;
        assert_eq!(json_rows(&client, &server, "SELECT count() AS n, sum(amount) AS total, min(amount) AS lo, max(amount) AS hi FROM events").await,
            [serde_json::json!({"n":5,"total":105,"lo":5,"hi":40})]);
        let floats = json_rows(&client, &server, "SELECT sum(ratio) AS total FROM events").await;
        assert!((floats[0]["total"].as_f64().unwrap() - 1.5).abs() < 1e-12);
        assert_eq!(json_rows(&client, &server, "SELECT label, count() AS n, sum(amount) AS total FROM events WHERE id >= 2 GROUP BY label ORDER BY total DESC, label ASC LIMIT 2").await,
            [serde_json::json!({"label":"b","n":2,"total":70}), serde_json::json!({"label":"a","n":2,"total":25})]);
        assert_eq!(
            json_rows(
                &client,
                &server,
                "SELECT amount FROM events ORDER BY id DESC, amount ASC"
            )
            .await,
            [40, 30, 5, 20, 10].map(|amount| serde_json::json!({"amount":amount}))
        );
        let csv = client.post(format!("{}/query?format=CSV", server.url))
            .body("SELECT label, count() AS n FROM events GROUP BY label ORDER BY n DESC, label LIMIT 1")
            .send().await.unwrap();
        assert_eq!(csv.status(), StatusCode::OK);
        assert_eq!(csv.text().await.unwrap(), "a,3\n");
        let pretty = client
            .post(format!("{}/query?format=Pretty", server.url))
            .body("SELECT sum(amount) AS total FROM events")
            .send()
            .await
            .unwrap();
        assert_eq!(pretty.status(), StatusCode::OK);
        assert_eq!(
            pretty.text().await.unwrap(),
            "+-------+\n| total |\n+-------+\n| 105   |\n+-------+\n"
        );
        sql(&client, &server, "DROP TABLE events").await;
        sql(
            &client,
            &server,
            &format!("CREATE TABLE events (id UInt64, amount Int64) ENGINE = {engine} ORDER BY id"),
        )
        .await;
        assert_eq!(json_rows(&client, &server, "SELECT count() AS n, sum(amount) AS total, min(amount) AS lo, max(amount) AS hi FROM events").await,
            [serde_json::json!({"n":0,"total":0,"lo":0,"hi":0})]);
        assert!(json_rows(
            &client,
            &server,
            "SELECT id, count() AS n FROM events GROUP BY id ORDER BY id"
        )
        .await
        .is_empty());
        assert_eq!(
            sql(&client, &server, "SELECT count() FROM events LIMIT 0").await,
            ""
        );
        server.close().await;
    }
}

#[tokio::test]
async fn explain_uses_the_normal_query_formats_and_counts_pruned_granules() {
    let root = tempfile::tempdir().unwrap();
    let server = TestServer::start(state(root.path())).await;
    let client = client();
    seed_queries(&client, &server, "MergeTree").await;
    let query = "EXPLAIN SELECT label FROM events WHERE id > 3 ORDER BY id DESC LIMIT 1";
    let tsv = sql(&client, &server, query).await;
    assert!(tsv.contains("Sort [`id` DESC]"));
    assert!(tsv.contains("parts: 1 / 2, granules: 2 / 3, rows: 3 / 5"));
    assert_eq!(
        sql(
            &client,
            &server,
            "SELECT label FROM events WHERE id > 3 ORDER BY id DESC LIMIT 1"
        )
        .await,
        "b\n"
    );
    assert_eq!(
        sql(&client, &server, "SELECT count() FROM events WHERE id > 3").await,
        "1\n"
    );
    let json = json_rows(&client, &server, query).await;
    let lines: Vec<_> = json
        .iter()
        .map(|row| {
            assert_eq!(row.as_object().unwrap().len(), 1);
            row["explain"].as_str().unwrap()
        })
        .collect();
    assert_eq!(lines, tsv.lines().collect::<Vec<_>>());
    let csv = client
        .post(format!("{}/query?format=CSV", server.url))
        .body(query)
        .send()
        .await
        .unwrap();
    assert_eq!(csv.status(), StatusCode::OK);
    let body = csv.bytes().await.unwrap();
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(body.as_ref());
    let records: Vec<_> = reader
        .records()
        .map(|record| record.unwrap().get(0).unwrap().to_owned())
        .collect();
    assert_eq!(records, lines);
    let pretty = client
        .post(format!("{}/query?format=Pretty", server.url))
        .body(query)
        .send()
        .await
        .unwrap();
    assert_eq!(pretty.status(), StatusCode::OK);
    let pretty = pretty.text().await.unwrap();
    assert!(pretty.contains("| explain"));
    assert!(pretty.contains("parts: 1 / 2, granules: 2 / 3, rows: 3 / 5"));
    server.close().await;
}

#[tokio::test]
async fn actual_late_storage_errors_do_not_leak_sorted_or_aggregated_results() {
    let root = tempfile::tempdir().unwrap();
    let app = state(root.path());
    let server = TestServer::start(app.clone()).await;
    let client = client();
    seed_queries(&client, &server, "MergeTree").await;
    let table = app.catalog.get_table("events").unwrap();
    let part = table.parts().unwrap().remove(1);
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.path().join("catalog.json")).unwrap()).unwrap();
    let path = root
        .path()
        .join("tables")
        .join(manifest["tables"][0]["id"].as_str().unwrap())
        .join(part)
        .join("amount.bin");
    std::fs::write(path, b"broken second part").unwrap();
    let explain = sql(
        &client,
        &server,
        "EXPLAIN SELECT sum(amount) AS total FROM events",
    )
    .await;
    assert!(explain.contains("parts: 2 / 2, granules: 3 / 3, rows: 5 / 5"));
    for query in [
        "SELECT sum(amount) AS total FROM events",
        "SELECT label, sum(amount) AS total FROM events GROUP BY label LIMIT 1",
        "SELECT amount FROM events ORDER BY id DESC LIMIT 1",
    ] {
        let response = client
            .post(format!("{}/query", server.url))
            .body(query)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "{query}"
        );
        let error: serde_json::Value = response.json().await.unwrap();
        assert_eq!(error.as_object().unwrap().len(), 1);
        assert!(error["error"].is_string());
    }
    assert_eq!(
        sql(&client, &server, "SELECT count() FROM events").await,
        "5\n"
    );
    assert_eq!(
        sql(
            &client,
            &server,
            "SELECT sum(amount) FROM events WHERE id > 3"
        )
        .await,
        "40\n"
    );
    server.close().await;
}
