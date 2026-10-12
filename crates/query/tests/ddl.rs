use query::SqlQueryEngine;
use std::sync::Arc;
use types::testing::MemCatalog;
use types::*;

fn setup() -> (SqlQueryEngine, Arc<MemCatalog>) {
    let catalog = Arc::new(MemCatalog::new());
    (SqlQueryEngine::new(catalog.clone()), catalog)
}
fn ok(engine: &SqlQueryEngine, sql: &str) {
    assert!(matches!(engine.execute(sql), Ok(QueryOutput::Ok)), "{sql}");
}
fn ids(engine: &SqlQueryEngine) -> Vec<u64> {
    let QueryOutput::Rows { stream, .. } = engine.execute("SELECT id FROM events").unwrap() else {
        panic!()
    };
    let mut result = Vec::new();
    for block in stream {
        let block = block.unwrap();
        let Some(ColumnData::UInt64(values)) = block.column("id") else {
            panic!()
        };
        result.extend_from_slice(values);
    }
    result
}
fn insert(catalog: &dyn Catalog, values: Vec<u64>) {
    catalog
        .get_table("events")
        .unwrap()
        .insert(
            Block::new(vec![Column {
                name: "id".into(),
                data: ColumnData::UInt64(values),
            }])
            .unwrap(),
        )
        .unwrap();
}

#[test]
fn create_memory_supports_all_six_types_and_defaults() {
    let (engine, catalog) = setup();
    ok(&engine,"CREATE TABLE events (id UInt64, signed Int64, score Float64, event String, day Date, time DateTime) ENGINE = Memory");
    let table = catalog.get_table("events").unwrap();
    assert_eq!(table.definition().engine, Engine::Memory);
    assert!(table.definition().order_by.is_empty());
    assert_eq!(table.definition().settings, TableSettings::default());
    assert_eq!(
        table
            .definition()
            .schema
            .columns
            .iter()
            .map(|c| c.data_type)
            .collect::<Vec<_>>(),
        vec![
            DataType::UInt64,
            DataType::Int64,
            DataType::Float64,
            DataType::String,
            DataType::Date,
            DataType::DateTime
        ]
    );
    let QueryOutput::Rows { header, mut stream } = engine.execute("SELECT * FROM events").unwrap()
    else {
        panic!()
    };
    assert_eq!(header.len(), 6);
    assert!(stream.next().is_none());
}

#[test]
fn create_mergetree_preserves_key_and_settings() {
    let (engine, catalog) = setup();
    ok(&engine,"CREATE TABLE events (id UInt64, day Date) ENGINE = MergeTree ORDER BY (day, id) SETTINGS index_granularity = 64, codec = 'None'");
    let table = catalog.get_table("events").unwrap();
    let def = table.definition();
    assert_eq!(def.engine, Engine::MergeTree);
    assert_eq!(def.order_by, vec!["day", "id"]);
    assert_eq!(def.settings.index_granularity, 64);
    assert_eq!(def.settings.codec, Codec::None);
}

#[test]
fn create_single_key_and_codec_variants() {
    for (text, expected) in [
        ("LZ4", Codec::Lz4),
        ("ZSTD", Codec::Zstd),
        ("none", Codec::None),
    ] {
        let (engine, catalog) = setup();
        ok(&engine,&format!("CREATE TABLE events (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS codec = '{text}'"));
        assert_eq!(
            catalog
                .get_table("events")
                .unwrap()
                .definition()
                .settings
                .codec,
            expected
        );
    }
}

#[test]
fn quoted_identifiers_comments_and_trailing_semicolon() {
    let (engine, catalog) = setup();
    ok(&engine,"-- note\nCREATE TABLE `Event Logs` (`User ID` UInt64) ENGINE = Memory SETTINGS /* note */ codec = 'LZ4'; -- end");
    let table = catalog.get_table("Event Logs").unwrap();
    assert_eq!(table.definition().schema.names(), vec!["User ID"]);
    ok(&engine, "DROP TABLE `Event Logs`;");
    assert!(catalog.list_tables().is_empty());
}

#[test]
fn if_not_exists_preserves_existing_table_and_data() {
    let (engine, catalog) = setup();
    ok(&engine, "CREATE TABLE events (id UInt64) ENGINE = Memory");
    insert(catalog.as_ref(), vec![1, 2]);
    let before = catalog.get_table("events").unwrap();
    ok(
        &engine,
        "CREATE TABLE IF NOT EXISTS events (other String) ENGINE = Memory",
    );
    assert!(Arc::ptr_eq(&before, &catalog.get_table("events").unwrap()));
    assert_eq!(ids(&engine), vec![1, 2]);
    assert!(matches!(
        engine.execute("CREATE TABLE events (id UInt64) ENGINE = Memory"),
        Err(DbError::TableExists(_))
    ));
}

#[test]
fn invalid_schema_is_rejected_before_catalog_change() {
    let (engine, catalog) = setup();
    for sql in [
        "CREATE TABLE events () ENGINE = Memory",
        "CREATE TABLE events (id UInt64, id UInt64) ENGINE = Memory",
        "CREATE TABLE events (id UInt64) ENGINE = MergeTree",
        "CREATE TABLE events (id UInt64) ENGINE = MergeTree ORDER BY ()",
        "CREATE TABLE events (id UInt64) ENGINE = MergeTree ORDER BY (id, id)",
        "CREATE TABLE events (id UInt64)",
    ] {
        assert!(
            matches!(engine.execute(sql), Err(DbError::Syntax(_))),
            "{sql}"
        );
        assert!(catalog.list_tables().is_empty());
    }
    assert!(matches!(
        engine.execute("CREATE TABLE events (id UInt64) ENGINE = MergeTree ORDER BY missing"),
        Err(DbError::UnknownColumn(_))
    ));
    assert!(catalog.list_tables().is_empty());
}

#[test]
fn unsupported_create_features_are_not_silently_ignored() {
    let (engine, catalog) = setup();
    for sql in [
        "CREATE OR REPLACE TABLE events (id UInt64) ENGINE = Memory",
        "CREATE TEMPORARY TABLE events (id UInt64) ENGINE = Memory",
        "CREATE TABLE events (id UInt64 DEFAULT 7) ENGINE = Memory",
        "CREATE TABLE events (id UInt64 NULL) ENGINE = Memory",
        "CREATE TABLE events (id UInt64 NOT NULL) ENGINE = Memory",
        "CREATE TABLE events (id Int32) ENGINE = Memory",
        "CREATE TABLE events (id UInt64) ENGINE = UnknownEngine",
        "CREATE TABLE events (id UInt64) ENGINE = MergeTree ORDER BY (id + 1)",
        "CREATE TABLE events (id UInt64) ENGINE = MergeTree PRIMARY KEY id ORDER BY id",
        "CREATE TABLE events ON CLUSTER cluster (id UInt64) ENGINE = Memory",
        "CREATE TABLE db.events (id UInt64) ENGINE = Memory",
        "CREATE TABLE events (id UInt64) ENGINE = Memory AS SELECT id FROM other",
    ] {
        assert!(engine.execute(sql).is_err(), "accepted: {sql}");
        assert!(catalog.list_tables().is_empty(), "{sql}");
    }
}

#[test]
fn invalid_unknown_and_duplicate_settings_have_no_effect() {
    let (engine, catalog) = setup();
    for settings in [
        "index_granularity = 0",
        "index_granularity = -1",
        "index_granularity = 1.5",
        "index_granularity = '8'",
        "index_granularity = 18446744073709551616",
        "index_granularity = 8, index_granularity = 16",
        "codec = 'LZ4', codec = 'ZSTD'",
        "codec = 'unknown'",
        "codec = LZ4",
        "unknown = 1",
        "codec = 'LZ4',",
        "",
    ] {
        let sql = format!("CREATE TABLE events (id UInt64) ENGINE = Memory SETTINGS {settings}");
        assert!(engine.execute(&sql).is_err(), "{sql}");
        assert!(catalog.list_tables().is_empty());
    }
}

#[test]
fn drop_and_if_exists_behave_as_expected() {
    let (engine, catalog) = setup();
    ok(&engine, "CREATE TABLE events (id UInt64) ENGINE = Memory");
    ok(&engine, "DROP TABLE events");
    assert!(catalog.list_tables().is_empty());
    assert!(matches!(
        engine.execute("DROP TABLE events"),
        Err(DbError::UnknownTable(_))
    ));
    ok(&engine, "DROP TABLE IF EXISTS events");
}

#[test]
fn optimize_delegates_to_table_and_preserves_data() {
    let (engine, catalog) = setup();
    ok(&engine, "CREATE TABLE events (id UInt64) ENGINE = Memory");
    insert(catalog.as_ref(), vec![1, 2]);
    insert(catalog.as_ref(), vec![3]);
    let table = catalog.get_table("events").unwrap();
    assert_eq!(
        table
            .explain_scan(&ScanRequest::default())
            .unwrap()
            .parts_total,
        2
    );
    ok(&engine, "OPTIMIZE TABLE events");
    assert_eq!(
        table
            .explain_scan(&ScanRequest::default())
            .unwrap()
            .parts_total,
        1
    );
    assert_eq!(ids(&engine), vec![1, 2, 3]);
    assert!(matches!(
        engine.execute("OPTIMIZE TABLE missing"),
        Err(DbError::UnknownTable(_))
    ));
}

#[test]
fn unsupported_drop_and_optimize_do_not_mutate_table() {
    let (engine, catalog) = setup();
    ok(&engine, "CREATE TABLE events (id UInt64) ENGINE = Memory");
    insert(catalog.as_ref(), vec![1]);
    insert(catalog.as_ref(), vec![2]);
    for sql in [
        "DROP TABLE events, other",
        "DROP TABLE events CASCADE",
        "DROP VIEW events",
        "OPTIMIZE TABLE events FINAL",
        "OPTIMIZE TABLE events DEDUPLICATE",
        "OPTIMIZE TABLE events ON CLUSTER cluster",
        "OPTIMIZE TABLE events PARTITION 1",
    ] {
        assert!(engine.execute(sql).is_err(), "{sql}");
        assert_eq!(ids(&engine), vec![1, 2]);
        assert_eq!(
            catalog
                .get_table("events")
                .unwrap()
                .explain_scan(&ScanRequest::default())
                .unwrap()
                .parts_total,
            2
        );
    }
}

#[test]
fn multiple_statements_are_rejected_before_any_action() {
    let (engine, catalog) = setup();
    for sql in [
        "CREATE TABLE events (id UInt64) ENGINE = Memory; DROP TABLE events",
        "CREATE TABLE events (id UInt64) ENGINE = Memory SETTINGS codec = 'LZ4'; SELECT * FROM events",
    ] {
        assert!(matches!(engine.execute(sql),Err(DbError::Unsupported(_))),"{sql}");
        assert!(catalog.list_tables().is_empty());
    }
    ok(&engine, "CREATE TABLE events (id UInt64) ENGINE = Memory");
    assert!(matches!(
        engine.execute("DROP TABLE events; SELECT * FROM events"),
        Err(DbError::Unsupported(_))
    ));
    assert!(catalog.get_table("events").is_ok());
}

#[test]
fn settings_tokens_inside_literals_are_not_statement_separators() {
    let (engine, catalog) = setup();
    assert!(engine.execute("CREATE TABLE events (id UInt64) ENGINE = Memory SETTINGS codec = 'LZ4; DROP TABLE other'").is_err());
    assert!(catalog.list_tables().is_empty());
    ok(
        &engine,
        "CREATE TABLE `SETTINGS; DROP TABLE other` (id UInt64) ENGINE = Memory",
    );
    assert!(catalog.get_table("SETTINGS; DROP TABLE other").is_ok());
}

struct RejectCatalog;
impl Catalog for RejectCatalog {
    fn create_table(&self, _: TableDefinition, _: bool) -> Result<()> {
        Err(DbError::NotLeader {
            leader: "leader".into(),
        })
    }
    fn drop_table(&self, _: &str, _: bool) -> Result<()> {
        Err(DbError::NotLeader {
            leader: "leader".into(),
        })
    }
    fn get_table(&self, _: &str) -> Result<Arc<dyn Table>> {
        Err(DbError::Io(std::io::Error::other("catalog failure")))
    }
    fn list_tables(&self) -> Vec<TableDefinition> {
        vec![]
    }
}
#[test]
fn errors_from_catalog_are_propagated() {
    let engine = SqlQueryEngine::new(Arc::new(RejectCatalog));
    for sql in [
        "CREATE TABLE events (id UInt64) ENGINE = Memory",
        "DROP TABLE events",
    ] {
        assert!(matches!(
            engine.execute(sql),
            Err(DbError::NotLeader { .. })
        ));
    }
    assert!(matches!(
        engine.execute("OPTIMIZE TABLE events"),
        Err(DbError::Io(_))
    ));
}
