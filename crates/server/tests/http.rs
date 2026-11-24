use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use server::{config::Config, AppState};
use std::sync::Arc;
use tokio::{sync::oneshot, task::JoinHandle};
use types::{
    Block, Column, ColumnData, ColumnDef, DataType, DbError, Peer, QueryEngine, QueryOutput,
    TableSchema,
};

struct TestServer {
    url: String,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}
impl TestServer {
    async fn start(state: AppState) -> Self {
        Self::start_router(server::router(state)).await
    }
    async fn start_router(router: axum::Router) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
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
fn shard_config(root: &std::path::Path, shard_id: u64) -> Config {
    Config {
        data_dir: root.to_owned(),
        local_shards: [(
            "hits_local".into(),
            server::config::LocalShardConfig {
                key: "id".into(),
                shard_id,
                shard_ids: vec![10, 2],
            },
        )]
        .into(),
        ..Config::default()
    }
}
fn shard_state(root: &std::path::Path, shard_id: u64) -> AppState {
    AppState::open(shard_config(root, shard_id)).unwrap()
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

fn coordinator_config(root: &std::path::Path, even: &TestServer, odd: &TestServer) -> Config {
    Config {
        data_dir: root.join("coordinator"),
        sharding: [(
            "hits".into(),
            server::config::ShardedTableConfig {
                local_table: "hits_local".into(),
                key: "id".into(),
                schema: None,
                shards: [
                    (10, odd.url.trim_start_matches("http://").into()),
                    (2, even.url.trim_start_matches("http://").into()),
                ]
                .into(),
                raft_shards: Default::default(),
            },
        )]
        .into(),
        ..Config::default()
    }
}

async fn coordinator(root: &std::path::Path, even: &TestServer, odd: &TestServer) -> TestServer {
    TestServer::start(AppState::open(coordinator_config(root, even, odd)).unwrap()).await
}

async fn table_metadata(
    client: &Client,
    server: &TestServer,
    path: &str,
    expected_status: StatusCode,
) -> Value {
    let response = client
        .get(format!("{}{path}", server.url))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body: Value = response.json().await.unwrap();
    assert_eq!(status, expected_status, "{body}");
    body
}

#[tokio::test]
async fn coordinator_metadata_lists_logical_and_local_tables_and_sums_all_shards() {
    for configured_schema in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let even = TestServer::start(shard_state(&root.path().join("even"), 2)).await;
        let odd = TestServer::start(shard_state(&root.path().join("odd"), 10)).await;
        let client = client();
        for (shard, settings) in [
            (&even, "id SETTINGS index_granularity = 1, codec = 'LZ4'"),
            (&odd, "text SETTINGS index_granularity = 2, codec = 'ZSTD'"),
        ] {
            sql(
                &client,
                shard,
                &format!(
                    "CREATE TABLE hits_local (id UInt64, text String) ENGINE = MergeTree ORDER BY {settings}"
                ),
            )
            .await;
        }
        let columns = json!([
            {"name": "id", "data_type": "UInt64"},
            {"name": "text", "data_type": "String"}
        ]);
        let mut config = coordinator_config(root.path(), &even, &odd);
        if configured_schema {
            config.sharding.get_mut("hits").unwrap().schema = Some(TableSchema {
                columns: serde_json::from_value(columns.clone()).unwrap(),
            });
        }
        let coordinator = TestServer::start(AppState::open(config).unwrap()).await;
        sql(
            &client,
            &coordinator,
            "CREATE TABLE local_hits (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 17, codec = 'LZ4'",
        )
        .await;
        client
            .post(format!("{}/insert/hits", coordinator.url))
            .body("1,one\n2,two\n3,three\n4,four\n5,five\n")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        let logical =
            json!({"name": "hits", "engine": "Distributed", "schema": {"columns": columns}});
        let local = json!({
            "name": "local_hits",
            "engine": "MergeTree",
            "schema": {"columns": [{"name": "id", "data_type": "UInt64"}]},
            "order_by": ["id"],
            "settings": {"index_granularity": 17, "codec": "LZ4"},
        });
        let listed = json!([logical, local]);
        assert_eq!(
            table_metadata(&client, &coordinator, "/tables", StatusCode::OK).await,
            listed
        );
        let mut description =
            table_metadata(&client, &coordinator, "/tables/hits", StatusCode::OK).await;
        let stats = description
            .as_object_mut()
            .unwrap()
            .remove("stats")
            .unwrap();
        assert_eq!(description, logical);
        assert_eq!(
            stats,
            json!({
                "parts_total": 2, "parts_selected": 2,
                "granules_total": 4, "granules_selected": 4,
                "rows_total": 5, "rows_selected": 5,
            })
        );
        let count = sql(&client, &coordinator, "SELECT count() FROM hits").await;
        assert_eq!(
            count.trim().parse::<u64>().unwrap(),
            stats["rows_total"].as_u64().unwrap()
        );

        odd.close().await;
        assert_eq!(
            table_metadata(&client, &coordinator, "/tables", StatusCode::OK).await,
            listed
        );
        let error = table_metadata(
            &client,
            &coordinator,
            "/tables/hits",
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
        assert!(
            error["error"].as_str().unwrap().contains("shard 10"),
            "{error}"
        );
        assert!(error.get("stats").is_none(), "{error}");

        even.close().await;
        if configured_schema {
            assert_eq!(
                table_metadata(&client, &coordinator, "/tables", StatusCode::OK).await,
                listed
            );
        } else {
            let error = table_metadata(
                &client,
                &coordinator,
                "/tables",
                StatusCode::INTERNAL_SERVER_ERROR,
            )
            .await;
            assert!(
                error["error"].as_str().unwrap().contains("shard 2"),
                "{error}"
            );
        }
        let mut description =
            table_metadata(&client, &coordinator, "/tables/local_hits", StatusCode::OK).await;
        assert_eq!(
            description
                .as_object_mut()
                .unwrap()
                .remove("stats")
                .unwrap()["rows_total"],
            0
        );
        assert_eq!(description, local);
    }
}

#[tokio::test]
async fn coordinator_reserves_logical_names_and_rejects_local_name_conflicts_on_startup() {
    let root = tempfile::tempdir().unwrap();
    let even = TestServer::start(state(&root.path().join("even"))).await;
    let odd = TestServer::start(state(&root.path().join("odd"))).await;
    let mut config = coordinator_config(root.path(), &even, &odd);
    let coordinator = TestServer::start(AppState::open(config.clone()).unwrap()).await;
    let client = client();
    let ddl = "CREATE TABLE hits (id UInt64) ENGINE = MergeTree ORDER BY id";
    let response = client
        .post(format!("{}/query", coordinator.url))
        .body(ddl)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(response
        .text()
        .await
        .unwrap()
        .contains("DDL for sharded tables"));
    coordinator.close().await;

    config.data_dir = root.path().join("existing-local-tables");
    let standalone = TestServer::start(state(&config.data_dir)).await;
    sql(&client, &standalone, ddl).await;
    standalone.close().await;
    assert!(
        matches!(AppState::open(config), Err(DbError::Syntax(message))
            if message.contains("\"hits\"") && message.contains("existing local table"))
    );
}

#[tokio::test]
async fn sharded_http_insert_routes_whole_batches_and_rejects_invalid_input_atomically() {
    let root = tempfile::tempdir().unwrap();
    let even_state = shard_state(&root.path().join("even"), 2);
    let even = TestServer::start(even_state.clone()).await;
    let odd = TestServer::start(shard_state(&root.path().join("odd"), 10)).await;
    let client = client();
    let ddl = "CREATE TABLE hits_local (id UInt64, text String) ENGINE = MergeTree ORDER BY id";
    for shard in [&even, &odd] {
        sql(&client, shard, ddl).await;
    }
    let coordinator = coordinator(root.path(), &even, &odd).await;
    for (format, input) in [
        (
            "CSV",
            "1,\"кавычки \"\"да\"\",\r\nещё строка\"\r\n2,two\r\n7,seven\r\n0,zero\r\n",
        ),
        ("TSV", "4\tfour\\\\with\\ttab\n3\tthree\\nстроки\n"),
        (
            "JSONEachRow",
            "{\"text\":\"пять\",\"id\":5}\n{\"id\":6,\"text\":\"six\\u0000\"}\n",
        ),
        ("CSV", "10,ten\n8,eight\n"),
        ("CSV", ""),
        ("TSV", ""),
        ("JSONEachRow", ""),
    ] {
        let response = client
            .post(format!("{}/insert/hits?format={format}", coordinator.url))
            .body(input)
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
    let response = client
        .post(format!("{}/insert/hits", coordinator.url))
        .body("12,twelve\n13,thirteen\ninvalid,broken\n")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(response.text().await.unwrap().contains("row 3"));

    let select = "SELECT id, text FROM hits_local ORDER BY id";
    let even_rows = "0\tzero\n2\ttwo\n4\tfour\\\\with\\ttab\n6\tsix\\0\n8\teight\n10\tten\n";
    let odd_rows = "1\tкавычки \"да\",\\r\\nещё строка\n3\tthree\\nстроки\n5\tпять\n7\tseven\n";
    assert_eq!(sql(&client, &even, select).await, even_rows);
    assert_eq!(sql(&client, &odd, select).await, odd_rows);

    let entry = even_state.catalog.table_entries().pop().unwrap();
    for path in [
        "/insert/hits_local".to_owned(),
        format!("/shard/insert/{}", entry.id),
    ] {
        let response = client
            .post(format!("{}{path}", even.url))
            .body("12,twelve\n9,nine\n")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let error = response.text().await.unwrap();
        assert!(
            error.contains("row 2") && error.contains("id=9") && error.contains("shard 2"),
            "{error}"
        );
        assert_eq!(sql(&client, &even, select).await, even_rows);
        assert_eq!(sql(&client, &odd, select).await, odd_rows);
    }
    let direct = client
        .post(format!("{}/insert/hits_local", even.url))
        .body("12,twelve\n")
        .send()
        .await
        .unwrap();
    assert_eq!(direct.status(), StatusCode::OK);
    assert_eq!(
        sql(&client, &coordinator, "SELECT text FROM hits WHERE id = 12").await,
        "twelve\n"
    );

    sql(&client, &odd, "DROP TABLE hits_local").await;
    sql(
        &client,
        &odd,
        "CREATE TABLE hits_local (text String, id UInt64) ENGINE = MergeTree ORDER BY id",
    )
    .await;
    let response = client
        .post(format!("{}/insert/hits", coordinator.url))
        .body("14,fourteen\n15,fifteen\n")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert!(response.text().await.unwrap().contains("schema differs"));
    assert_eq!(
        sql(&client, &even, select).await,
        format!("{even_rows}12\ttwelve\n")
    );
    assert_eq!(sql(&client, &odd, select).await, "");
    let response = client
        .post(format!("{}/query", coordinator.url))
        .body("SELECT id, text FROM hits ORDER BY id")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let error: Value = response.json().await.unwrap();
    assert!(error["error"].as_str().unwrap().contains("schema differs"));
    assert_eq!(error.as_object().unwrap().len(), 1);
    let error = table_metadata(
        &client,
        &coordinator,
        "/tables/hits",
        StatusCode::INTERNAL_SERVER_ERROR,
    )
    .await;
    assert!(error["error"].as_str().unwrap().contains("schema differs"));
    assert!(error.get("stats").is_none());
}

#[tokio::test]
async fn restarting_with_changed_sharding_topology_leaves_existing_rows_intact() {
    let root = tempfile::tempdir().unwrap();
    let even_config = shard_config(&root.path().join("even"), 2);
    let even = TestServer::start(AppState::open(even_config.clone()).unwrap()).await;
    let odd = TestServer::start(shard_state(&root.path().join("odd"), 10)).await;
    let config = coordinator_config(root.path(), &even, &odd);
    let coordinator = TestServer::start(AppState::open(config.clone()).unwrap()).await;
    let client = client();
    let ddl = "CREATE TABLE hits_local (id UInt64) ENGINE = MergeTree ORDER BY id";
    for shard in [&even, &odd] {
        sql(&client, shard, ddl).await;
    }
    client
        .post(format!("{}/insert/hits", coordinator.url))
        .body("3\n4\n")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    coordinator.close().await;

    let mut changed = config.clone();
    changed
        .sharding
        .get_mut("hits")
        .unwrap()
        .shards
        .insert(20, "new-shard:8123".into());
    assert!(
        matches!(AppState::open(changed), Err(DbError::Syntax(message))
        if message.contains("sharding topology differs"))
    );

    let mut changed = config.clone();
    let addresses = &mut changed.sharding.get_mut("hits").unwrap().shards;
    let first = addresses[&2].clone();
    let second = addresses[&10].clone();
    addresses.insert(2, second);
    addresses.insert(10, first);
    assert!(
        matches!(AppState::open(changed), Err(DbError::Syntax(message))
        if message.contains("sharding topology differs"))
    );

    let coordinator = TestServer::start(AppState::open(config).unwrap()).await;
    assert_eq!(
        sql(&client, &coordinator, "SELECT id FROM hits WHERE id = 4").await,
        "4\n"
    );
    assert_eq!(
        sql(&client, &coordinator, "SELECT id FROM hits ORDER BY id").await,
        "3\n4\n"
    );
    even.close().await;

    let mut changed = even_config.clone();
    changed
        .local_shards
        .get_mut("hits_local")
        .unwrap()
        .shard_ids
        .push(20);
    assert!(
        matches!(AppState::open(changed), Err(DbError::Syntax(message))
        if message.contains("sharding topology differs"))
    );
    let mut reordered = even_config;
    reordered
        .local_shards
        .get_mut("hits_local")
        .unwrap()
        .shard_ids
        .reverse();
    let even = TestServer::start(AppState::open(reordered).unwrap()).await;
    assert_eq!(
        sql(&client, &even, "SELECT id FROM hits_local").await,
        "4\n"
    );
    assert_eq!(sql(&client, &odd, "SELECT id FROM hits_local").await, "3\n");
}

#[tokio::test]
async fn coordinator_rejects_incompatible_physical_placement_before_reading_or_writing() {
    let root = tempfile::tempdir().unwrap();
    let even = TestServer::start(shard_state(&root.path().join("even"), 2)).await;
    let mut odd_config = shard_config(&root.path().join("odd"), 10);
    // Индекс нечётного шарда совпадает, но состав шардов отличается.
    odd_config
        .local_shards
        .get_mut("hits_local")
        .unwrap()
        .shard_ids = vec![3, 10];
    let odd = TestServer::start(AppState::open(odd_config).unwrap()).await;
    let client = client();
    for shard in [&even, &odd] {
        sql(
            &client,
            shard,
            "CREATE TABLE hits_local (id UInt64) ENGINE = MergeTree ORDER BY id",
        )
        .await;
    }
    let mut config = coordinator_config(root.path(), &even, &odd);
    config.sharding.get_mut("hits").unwrap().schema = Some(TableSchema {
        columns: vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::UInt64,
        }],
    });
    let coordinator = TestServer::start(AppState::open(config).unwrap()).await;
    for (path, body, status) in [
        (
            "/query",
            "SELECT id FROM hits WHERE id = 3",
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
        ("/insert/hits", "4\n3\n", StatusCode::BAD_GATEWAY),
    ] {
        let response = client
            .post(format!("{}{path}", coordinator.url))
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        let error = response.text().await.unwrap();
        assert!(
            error.contains("shard 10") && error.contains("shard placement mismatch"),
            "{error}"
        );
    }
    for shard in [&even, &odd] {
        assert_eq!(
            sql(&client, shard, "SELECT count() FROM hits_local").await,
            "0\n"
        );
    }
}

#[tokio::test]
async fn sharded_insert_reports_confirmed_and_unknown_outcomes_without_retrying() {
    use axum::{extract::Request, middleware, response::IntoResponse};

    let root = tempfile::tempdir().unwrap();
    let even = TestServer::start(shard_state(&root.path().join("even"), 2)).await;
    let odd = TestServer::start_router(
        server::router(shard_state(&root.path().join("odd"), 10)).layer(middleware::from_fn(
            |request: Request, next: middleware::Next| async move {
                let insert = request.uri().path().starts_with("/shard/insert/");
                let response = next.run(request).await;
                if insert && response.status() == StatusCode::OK {
                    // Запись уже применена, но координатор не получает подтверждение.
                    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
                response
            },
        )),
    )
    .await;
    let client = client();
    let ddl = "CREATE TABLE hits_local (id UInt64, text String) ENGINE = MergeTree ORDER BY id";
    for shard in [&even, &odd] {
        sql(&client, shard, ddl).await;
    }
    let coordinator = coordinator(root.path(), &even, &odd).await;
    let response = client
        .post(format!("{}/insert/hits", coordinator.url))
        .body("1,odd\n2,even\n")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let error = response.text().await.unwrap();
    assert!(
        error.contains("shard 10: INSERT outcome unknown"),
        "{error}"
    );
    assert!(error.contains("confirmed shards: [2]"), "{error}");
    let select = "SELECT id, text FROM hits_local ORDER BY id";
    assert_eq!(sql(&client, &even, select).await, "2\teven\n");
    assert_eq!(sql(&client, &odd, select).await, "1\todd\n");
}

#[tokio::test]
async fn sharded_insert_reports_partial_write_when_a_shard_rejects_the_body_size() {
    let root = tempfile::tempdir().unwrap();
    let even = TestServer::start(shard_state(&root.path().join("even"), 2)).await;
    let odd = TestServer::start(
        AppState::open(Config {
            max_body_bytes: 256,
            ..shard_config(&root.path().join("odd"), 10)
        })
        .unwrap(),
    )
    .await;
    let client = client();
    let ddl = "CREATE TABLE hits_local (id UInt64, text String) ENGINE = MergeTree ORDER BY id";
    for shard in [&even, &odd] {
        sql(&client, shard, ddl).await;
    }
    let coordinator = coordinator(root.path(), &even, &odd).await;
    let response = client
        .post(format!("{}/insert/hits", coordinator.url))
        .body(format!("2,even\n1,odd\n3,{}\n", "x".repeat(256)))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let error = response.text().await.unwrap();
    assert!(error.contains("shard 10 rejected"), "{error}");
    assert!(
        error.contains("partial INSERT, confirmed shards: [2]"),
        "{error}"
    );
    let select = "SELECT id, text FROM hits_local ORDER BY id";
    assert_eq!(sql(&client, &even, select).await, "2\teven\n");
    assert_eq!(sql(&client, &odd, select).await, "");
}

#[tokio::test]
async fn sharded_select_groups_and_sorts_globally_and_fails_on_a_late_shard_error() {
    let root = tempfile::tempdir().unwrap();
    let even = TestServer::start(shard_state(&root.path().join("even"), 2)).await;
    let odd_root = root.path().join("odd");
    let odd_state = shard_state(&odd_root, 10);
    let odd = TestServer::start(odd_state.clone()).await;
    let coordinator = coordinator(root.path(), &even, &odd).await;
    let client = client();
    for shard in [&even, &odd] {
        sql(&client, shard, "CREATE TABLE hits_local (id UInt64, label String, amount Int64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1").await;
    }
    for input in [
        "2,shared,20\n4,even,50\n6,shared,1000\n",
        "1,shared,40\n3,odd,55\n5,odd,1\n",
    ] {
        let response = client
            .post(format!("{}/insert/hits", coordinator.url))
            .body(input)
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
    let grouped = "SELECT label, sum(amount) AS total, count() AS n FROM hits WHERE id < 6 GROUP BY label ORDER BY total DESC, label ASC LIMIT 2";
    assert_eq!(
        sql(&client, &coordinator, grouped).await,
        "shared\t60\t2\nodd\t56\t2\n"
    );
    assert_eq!(
        sql(
            &client,
            &coordinator,
            "SELECT min(amount), max(amount) FROM hits WHERE id > 1"
        )
        .await,
        "1\t1000\n"
    );
    assert_eq!(
        sql(
            &client,
            &coordinator,
            "SELECT label FROM hits ORDER BY id DESC LIMIT 2"
        )
        .await,
        "shared\nodd\n"
    );
    assert_eq!(
        sql(&client, &coordinator, "SELECT count() FROM hits").await,
        "6\n"
    );

    let entry = odd_state.catalog.table_entries().pop().unwrap();
    let part = odd_state
        .catalog
        .get_table_by_id(&entry.id)
        .unwrap()
        .parts()
        .unwrap()
        .pop()
        .unwrap();
    std::fs::write(
        odd_root
            .join("tables")
            .join(entry.id.as_str())
            .join(part)
            .join("amount.bin"),
        b"broken column",
    )
    .unwrap();
    let explain = sql(&client, &coordinator, &format!("EXPLAIN {grouped}")).await;
    assert!(explain.lines().any(|line| line.trim() == "shards: [2, 10]"));
    assert!(explain.contains("parts: 2 / 2, granules: 5 / 6, rows: 5 / 6"));
    let response = client
        .post(format!("{}/query", coordinator.url))
        .body(grouped)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let error = response.json::<serde_json::Value>().await.unwrap();
    assert_eq!(error.as_object().unwrap().len(), 1);
    assert!(error["error"].as_str().unwrap().contains("shard 10"));
}

#[tokio::test]
async fn sharded_select_skips_unneeded_shards_and_damaged_granules() {
    let root = tempfile::tempdir().unwrap();
    let even_root = root.path().join("even");
    let even_state = AppState::open(Config {
        max_body_bytes: 160,
        ..shard_config(&even_root, 2)
    })
    .unwrap();
    let even = TestServer::start(even_state.clone()).await;
    let odd = TestServer::start(shard_state(&root.path().join("odd"), 10)).await;
    let coordinator = coordinator(root.path(), &even, &odd).await;
    let client = client();
    for shard in [&even, &odd] {
        sql(&client, shard, "CREATE TABLE hits_local (id UInt64, label String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1").await;
    }
    for input in ["2,no\n4,hit\n6,other\n", "1,odd-one\n3,odd-three\n"] {
        let response = client
            .post(format!("{}/insert/hits", coordinator.url))
            .body(input)
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
    assert_eq!(
        sql(
            &client,
            &coordinator,
            "SELECT id FROM hits WHERE id = 4 OR id + 0 = 3 ORDER BY id"
        )
        .await,
        "3\n4\n"
    );
    let plan = sql(
        &client,
        &coordinator,
        "EXPLAIN SELECT id FROM hits WHERE id = 4 OR id + 0 = 3",
    )
    .await;
    assert!(plan.lines().any(|line| line.trim() == "shards: [2, 10]"));
    assert_eq!(
        sql(
            &client,
            &coordinator,
            "SELECT count() FROM hits WHERE id = 2 AND id = 3"
        )
        .await,
        "0\n"
    );

    let entry = even_state.catalog.table_entries().pop().unwrap();
    let part = even_state
        .catalog
        .get_table_by_id(&entry.id)
        .unwrap()
        .parts()
        .unwrap()
        .pop()
        .unwrap();
    let column = std::fs::OpenOptions::new()
        .write(true)
        .open(
            even_root
                .join("tables")
                .join(entry.id.as_str())
                .join(part)
                .join("label.bin"),
        )
        .unwrap();
    column
        .set_len(column.metadata().unwrap().len() - 1)
        .unwrap();
    drop(column);
    odd.close().await;

    let query =
        "SELECT label FROM hits WHERE (4 = id OR id = 2) AND NOT (label = 'no') ORDER BY id";
    assert_eq!(sql(&client, &coordinator, query).await, "hit\n");
    let explain = sql(&client, &coordinator, &format!("EXPLAIN {query}")).await;
    assert!(explain.lines().any(|line| line.trim() == "shards: [2]"));
    assert!(
        explain.contains("parts: 1 / 1, granules: 2 / 3, rows: 2 / 3"),
        "{explain}"
    );
    let empty = sql(
        &client,
        &coordinator,
        "EXPLAIN SELECT count() FROM hits WHERE id = 2 AND id = 3",
    )
    .await;
    assert!(empty.lines().any(|line| line.trim() == "shards: []"));
}

#[tokio::test]
async fn shard_insert_cannot_write_to_a_recreated_table_using_the_old_id() {
    let root = tempfile::tempdir().unwrap();
    let app = state(root.path());
    let catalog = app.catalog.clone();
    let shard = TestServer::start(app).await;
    let client = client();
    let ddl = "CREATE TABLE hits (id UInt64) ENGINE = MergeTree ORDER BY id";
    sql(&client, &shard, ddl).await;
    let old_id = catalog.table_entries().pop().unwrap().id;
    sql(&client, &shard, "DROP TABLE hits").await;
    sql(&client, &shard, ddl).await;
    let response = client
        .post(format!("{}/shard/insert/{old_id}", shard.url))
        .body("42\n")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        sql(&client, &shard, "SELECT count() FROM hits").await,
        "0\n"
    );
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
        ("/replica/ping", "get"),
        ("/replica/tables", "get"),
        ("/replica/tables/{id}/parts", "get"),
        ("/replica/tables/{id}/parts/{part}", "get"),
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

#[tokio::test]
async fn replica_endpoints_and_http_peer_preserve_parts() {
    let root = tempfile::tempdir().unwrap();
    let app = state(root.path());
    let catalog = app.catalog.clone();
    let server = TestServer::start(app).await;
    let client = client();
    sql(
        &client,
        &server,
        "CREATE TABLE hits (id UInt64, text String) ENGINE = MergeTree ORDER BY id",
    )
    .await;
    let response = client
        .post(format!("{}/insert/hits", server.url))
        .body("3,c\n1,a\n2,b\n")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let id = catalog
        .table_entries()
        .into_iter()
        .find(|entry| entry.definition.name == "hits")
        .unwrap()
        .id;
    let table = catalog.get_table("hits").unwrap();
    let expected = table.export_part("all_1_1_0").unwrap();

    let url = server.url.clone();
    let peer_id = id.clone();
    let (tables, parts, fetched) = tokio::task::spawn_blocking(move || {
        let peer = server::replica::HttpPeer::new(&url).unwrap();
        assert_eq!(
            peer.ping().unwrap(),
            types::NodeIdentity {
                cluster_id: "miniclick".into(),
                node_id: "node-1".into(),
            }
        );
        let tables = peer.list_tables().unwrap();
        let parts = peer.list_parts(&peer_id).unwrap();
        let fetched = peer.fetch_part(&peer_id, &parts[0]).unwrap();
        (tables, parts, fetched)
    })
    .await
    .unwrap();
    assert!(tables.iter().any(|entry| entry.definition.name == "hits"));
    assert_eq!(parts, ["all_1_1_0"]);
    assert_eq!(fetched, expected);

    let raw = client
        .get(format!(
            "{}/replica/tables/{}/parts/all_1_1_0",
            server.url, id
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(raw.status(), StatusCode::OK);
    assert_eq!(raw.headers()["content-type"], "application/octet-stream");
    assert_eq!(&raw.bytes().await.unwrap()[..4], b"MCP1");

    let missing = client
        .get(format!("{}/replica/tables/missing/parts", server.url))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    server.close().await;
}

#[tokio::test]
async fn replica_rejects_writes_but_allows_reads() {
    let root = tempfile::tempdir().unwrap();
    let mut config = Config {
        data_dir: root.path().to_owned(),
        ..Config::default()
    };
    config.node.role = replication::Role::Replica {
        leader_addr: "127.0.0.1:18123".into(),
        leader_id: "leader-1".into(),
    };
    let app = AppState::open(config).unwrap();
    app.query
        .execute("CREATE TABLE hits (id UInt64) ENGINE = Memory")
        .unwrap();
    let server = TestServer::start(app).await;
    let client = client();

    let replica_endpoint = client
        .get(format!("{}/replica/ping", server.url))
        .send()
        .await
        .unwrap();
    assert_eq!(replica_endpoint.status(), StatusCode::NOT_FOUND);

    let response = client
        .post(format!("{}/insert/hits", server.url))
        .body("1\n")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let error: serde_json::Value = response.json().await.unwrap();
    assert!(error["error"].as_str().unwrap().contains("127.0.0.1:18123"));

    for query in [
        "CREATE TABLE other (id UInt64) ENGINE = Memory",
        "DROP TABLE hits",
        "OPTIMIZE TABLE hits",
    ] {
        let response = client
            .post(format!("{}/query", server.url))
            .body(query)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT, "{query}");
        assert!(response.json::<serde_json::Value>().await.unwrap()["error"]
            .as_str()
            .unwrap()
            .contains("127.0.0.1:18123"));
    }

    assert_eq!(sql(&client, &server, "SELECT * FROM hits").await, "");
    let explain = sql(&client, &server, "EXPLAIN SELECT * FROM hits").await;
    assert!(explain.contains("Scan"));
    server.close().await;
}

#[tokio::test]
async fn replica_sync_loop_pulls_table_and_parts_from_leader() {
    let leader_root = tempfile::tempdir().unwrap();
    let leader = TestServer::start(state(leader_root.path())).await;
    let client = client();
    sql(
        &client,
        &leader,
        "CREATE TABLE hits (id UInt64, text String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2",
    )
    .await;
    let response = client
        .post(format!("{}/insert/hits", leader.url))
        .body("2,b\n1,a\n")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let replica_root = tempfile::tempdir().unwrap();
    let mut config = Config {
        data_dir: replica_root.path().to_owned(),
        ..Config::default()
    };
    config.node.role = replication::Role::Replica {
        leader_addr: leader.url.trim_start_matches("http://").to_owned(),
        leader_id: "node-1".into(),
    };
    let replica = AppState::open(config).unwrap();
    let catalog = replica.catalog.clone();
    let leader_url = leader.url.clone();
    let (stop, stopped) = std::sync::mpsc::channel();
    let sync = tokio::task::spawn_blocking(move || {
        server::replica::sync_loop(
            catalog,
            leader_url,
            types::NodeIdentity {
                cluster_id: "miniclick".into(),
                node_id: "node-1".into(),
            },
            std::time::Duration::from_millis(20),
            stopped,
        );
    });

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if let Ok(table) = replica.catalog.get_table("hits") {
            if !table.parts().unwrap().is_empty() {
                break;
            }
        }
        assert!(tokio::time::Instant::now() < deadline, "sync timeout");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let _ = stop.send(());
    sync.await.unwrap();

    let QueryOutput::Rows { stream, .. } = replica
        .query
        .execute("SELECT id, text FROM hits ORDER BY id")
        .unwrap()
    else {
        panic!("expected rows");
    };
    let blocks: Vec<Block> = stream.collect::<types::Result<_>>().unwrap();
    let block = Block::concat(&blocks).unwrap();
    assert_eq!(block.column("id"), Some(&ColumnData::UInt64(vec![1, 2])));
    leader.close().await;
}
