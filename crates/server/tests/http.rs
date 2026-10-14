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
            "SELECT * FROM hits ORDER BY id",
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
