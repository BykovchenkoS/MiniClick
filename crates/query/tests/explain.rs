use query::SqlQueryEngine;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use types::testing::MemCatalog;
use types::*;

fn definition() -> TableDefinition {
    TableDefinition {
        name: "events".into(),
        schema: TableSchema {
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    data_type: DataType::UInt64,
                },
                ColumnDef {
                    name: "event".into(),
                    data_type: DataType::String,
                },
                ColumnDef {
                    name: "value".into(),
                    data_type: DataType::Int64,
                },
                ColumnDef {
                    name: "day".into(),
                    data_type: DataType::Date,
                },
                ColumnDef {
                    name: "time".into(),
                    data_type: DataType::DateTime,
                },
            ],
        },
        engine: Engine::Memory,
        order_by: vec![],
        settings: TableSettings::default(),
    }
}

fn fixture() -> Block {
    let day = |s| match Value::parse(s, DataType::Date).unwrap() {
        Value::Date(d) => d,
        _ => unreachable!(),
    };
    Block::new(vec![
        Column {
            name: "id".into(),
            data: ColumnData::UInt64(vec![1, 2, 3, 4, 5]),
        },
        Column {
            name: "event".into(),
            data: ColumnData::String(
                ["click", "view", "click", "view", "click"]
                    .map(str::to_owned)
                    .to_vec(),
            ),
        },
        Column {
            name: "value".into(),
            data: ColumnData::Int64(vec![-2, 0, 2, 3, 10]),
        },
        Column {
            name: "day".into(),
            data: ColumnData::Date(vec![
                day("2026-10-01"),
                day("2026-10-02"),
                day("2026-10-01"),
                day("2026-10-03"),
                day("2026-10-03"),
            ]),
        },
        Column {
            name: "time".into(),
            data: ColumnData::DateTime(vec![0, 1, 2, 86400, 86401]),
        },
    ])
    .unwrap()
}

fn engine(blocks: Vec<Block>) -> SqlQueryEngine {
    let catalog = Arc::new(MemCatalog::new());
    catalog.create_table(definition(), false).unwrap();
    let table = catalog.get_table("events").unwrap();
    for block in blocks {
        table.insert(block).unwrap();
    }
    SqlQueryEngine::new(catalog)
}

// Таблица намеренно игнорирует predicate: проверяем точную фильтрацию в Query.
struct SpyTable {
    definition: TableDefinition,
    request: Mutex<Option<ScanRequest>>,
    blocks: Vec<Block>,
    reads: Arc<AtomicUsize>,
    scans: AtomicUsize,
    explains: AtomicUsize,
    fail_explain: bool,
}
impl Table for SpyTable {
    fn definition(&self) -> &TableDefinition {
        &self.definition
    }
    fn insert(&self, _: Block) -> Result<()> {
        Err(DbError::Unsupported("spy".into()))
    }
    fn scan(&self, req: &ScanRequest) -> Result<BlockStream> {
        self.scans.fetch_add(1, Ordering::SeqCst);
        *self.request.lock().unwrap() = Some(req.clone());
        let mut input = self
            .blocks
            .iter()
            .map(|b| b.select(&req.columns))
            .collect::<Vec<_>>();
        input.push(Err(DbError::Io(std::io::Error::other(
            "ошибка следующего блока",
        ))));
        // После ошибки есть ещё данные: исполнитель должен остановиться на ошибке.
        input.push(Ok(fixture().select(&req.columns)?));
        let reads = self.reads.clone();
        Ok(Box::new(input.into_iter().inspect(move |_| {
            reads.fetch_add(1, Ordering::SeqCst);
        })))
    }
    fn explain_scan(&self, req: &ScanRequest) -> Result<ScanStats> {
        self.explains.fetch_add(1, Ordering::SeqCst);
        *self.request.lock().unwrap() = Some(req.clone());
        if self.fail_explain {
            return Err(DbError::Io(std::io::Error::other("stats failure")));
        }
        Ok(ScanStats {
            parts_total: 7,
            parts_selected: 2,
            granules_total: 861,
            granules_selected: 14,
            rows_total: 7_000_000,
            rows_selected: 114_688,
        })
    }
    fn flush(&self) -> Result<()> {
        Ok(())
    }
    fn optimize(&self) -> Result<()> {
        Ok(())
    }
    fn drop_data(&self) -> Result<()> {
        Ok(())
    }
}
struct SpyCatalog(Arc<SpyTable>);
impl Catalog for SpyCatalog {
    fn create_table(&self, _: TableDefinition, _: bool) -> Result<()> {
        Err(DbError::Unsupported("spy".into()))
    }
    fn drop_table(&self, _: &str, _: bool) -> Result<()> {
        Err(DbError::Unsupported("spy".into()))
    }
    fn get_table(&self, _: &str) -> Result<Arc<dyn Table>> {
        Ok(self.0.clone())
    }
    fn list_tables(&self) -> Vec<TableDefinition> {
        vec![self.0.definition.clone()]
    }
}
fn spy_with_error(blocks: Vec<Block>, fail_explain: bool) -> (SqlQueryEngine, Arc<SpyTable>) {
    let table = Arc::new(SpyTable {
        definition: definition(),
        request: Mutex::new(None),
        blocks,
        reads: Arc::new(AtomicUsize::new(0)),
        scans: AtomicUsize::new(0),
        explains: AtomicUsize::new(0),
        fail_explain,
    });
    (
        SqlQueryEngine::new(Arc::new(SpyCatalog(table.clone()))),
        table,
    )
}

fn spy() -> (SqlQueryEngine, Arc<SpyTable>) {
    spy_with_error(vec![fixture()], false)
}
fn lines(e: &SqlQueryEngine, sql: &str) -> Vec<String> {
    let QueryOutput::Rows { header, mut stream } = e.execute(sql).unwrap() else {
        panic!()
    };
    assert_eq!(
        header,
        vec![ColumnDef {
            name: "explain".into(),
            data_type: DataType::String
        }]
    );
    let block = stream.next().unwrap().unwrap();
    assert_eq!(block.header(), header);
    assert!(stream.next().is_none());
    let ColumnData::String(lines) = block.column("explain").unwrap() else {
        panic!()
    };
    lines.clone()
}
#[test]
fn simple_plan_and_exact_stats_without_scan() {
    let (e, t) = spy();
    assert_eq!(lines(&e,"EXPLAIN SELECT id FROM events WHERE value > 0 LIMIT 2"),vec![
        "Project [`id` AS `id`: UInt64]", "  Limit 2", "    Filter (`value` > 0)",
        "      Scan `events` [`id`, `value`]",
        "        parts: 2 / 7, granules: 14 / 861, rows: 114688 / 7000000 (selected / total; before WHERE)",
    ]);
    assert_eq!(t.scans.load(Ordering::SeqCst), 0);
    assert_eq!(t.reads.load(Ordering::SeqCst), 0);
    assert_eq!(t.explains.load(Ordering::SeqCst), 1);
}
#[test]
fn scan_request_matches_select_and_contains_typed_predicate() {
    let (e, t) = spy();
    let sql = "SELECT id FROM events WHERE day = '2026-10-01' ORDER BY event LIMIT 2";
    lines(&e, &format!("EXPLAIN {sql}"));
    let explained = t.request.lock().unwrap().clone().unwrap();
    assert_eq!(explained.columns, vec!["id", "event", "day"]);
    assert_eq!(
        explained.predicate,
        Some(Expr::binary(
            BinaryOp::Eq,
            Expr::col("day"),
            Expr::lit(Value::parse("2026-10-01", DataType::Date).unwrap())
        ))
    );
    e.execute(sql).unwrap();
    assert_eq!(explained, t.request.lock().unwrap().clone().unwrap());
}
#[test]
fn sorted_grouped_plan_shows_aggregate_and_limit_order() {
    let (e, t) = spy();
    let l=lines(&e,"EXPLAIN SELECT event, count() AS n FROM events WHERE id > 0 GROUP BY event ORDER BY n DESC LIMIT 1");
    assert_eq!(l[0], "Limit 1");
    assert_eq!(l[1], "  Sort [count() DESC]");
    assert!(l[2].starts_with("    Aggregate by [`event`]"));
    assert!(l[3].starts_with("      Filter"));
    assert_eq!(t.scans.load(Ordering::SeqCst), 0);
}
#[test]
fn hidden_sort_key_is_shown_with_public_output() {
    let (e, _) = spy();
    let l = lines(
        &e,
        "EXPLAIN SELECT id FROM events ORDER BY event DESC LIMIT 1",
    );
    assert_eq!(l[0], "Output [`id`]");
    assert_eq!(l[1], "  Limit 1");
    assert_eq!(l[2], "    Sort [`event` DESC]");
    assert!(l[3].contains("Project"));
}
#[test]
fn global_count_has_empty_scan_columns() {
    let (e, t) = spy();
    let l = lines(&e, "EXPLAIN SELECT count(*) AS n FROM events");
    assert_eq!(l[0], "Aggregate global [count() AS `n`: UInt64]");
    assert_eq!(l[1], "  Scan `events` []");
    assert!(t
        .request
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .columns
        .is_empty());
}
#[test]
fn limit_zero_validates_but_calls_neither_scan_nor_stats() {
    let (e, t) = spy();
    assert_eq!(
        lines(&e, "EXPLAIN SELECT id FROM events ORDER BY event LIMIT 0"),
        vec!["Empty (LIMIT 0) [`id` AS `id`: UInt64]"]
    );
    assert_eq!(t.scans.load(Ordering::SeqCst), 0);
    assert_eq!(t.explains.load(Ordering::SeqCst), 0);
    assert!(e
        .execute("EXPLAIN SELECT missing FROM events LIMIT 0")
        .is_err());
}
#[test]
fn expressions_are_displayed_without_evaluation() {
    let (e, t) = spy();
    let l = lines(
        &e,
        "EXPLAIN SELECT 1 / 0 AS ratio FROM events WHERE NOT (value < -1 OR value = 0)",
    );
    assert!(l[0].contains("(1 / 0) AS `ratio`: Float64"));
    assert!(l[1].contains("NOT"));
    assert!(l[1].contains(" OR "));
    assert_eq!(t.scans.load(Ordering::SeqCst), 0);
}
#[test]
fn invalid_queries_do_not_request_stats() {
    let (e, t) = spy();
    for sql in [
        "SELECT missing FROM events",
        "SELECT sum(event) FROM events",
        "SELECT id, count() FROM events GROUP BY event",
        "SELECT id FROM events ORDER BY missing",
    ] {
        assert!(e.execute(&format!("EXPLAIN {sql}")).is_err());
    }
    assert_eq!(t.explains.load(Ordering::SeqCst), 0);
    assert_eq!(t.scans.load(Ordering::SeqCst), 0);
}
#[test]
fn unsupported_options_and_non_select_are_rejected() {
    let (e, t) = spy();
    for sql in [
        "EXPLAIN ANALYZE SELECT id FROM events",
        "EXPLAIN VERBOSE SELECT id FROM events",
        "EXPLAIN FORMAT JSON SELECT id FROM events",
        "EXPLAIN (ANALYZE false) SELECT id FROM events",
        "EXPLAIN QUERY PLAN SELECT id FROM events",
        "DESCRIBE SELECT id FROM events",
        "DESC SELECT id FROM events",
        "EXPLAIN DROP TABLE events",
        "EXPLAIN OPTIMIZE TABLE events",
        "EXPLAIN CREATE TABLE t (id UInt64) ENGINE = Memory",
        "EXPLAIN EXPLAIN SELECT id FROM events",
        "EXPLAIN SELECT id FROM events; DROP TABLE events",
    ] {
        assert!(e.execute(sql).is_err(), "{sql}");
    }
    assert_eq!(t.explains.load(Ordering::SeqCst), 0);
    assert_eq!(t.scans.load(Ordering::SeqCst), 0);
}
#[test]
fn stats_errors_propagate_without_scan() {
    let (e, t) = spy_with_error(vec![], true);
    assert!(matches!(
        e.execute("EXPLAIN SELECT id FROM events"),
        Err(DbError::Io(_))
    ));
    assert_eq!(t.scans.load(Ordering::SeqCst), 0);
    assert_eq!(t.explains.load(Ordering::SeqCst), 1);
}
#[test]
fn comments_case_and_empty_memory_table_work() {
    let e = engine(vec![]);
    let l = lines(&e, "/* demo */ explain select id from events;");
    assert!(l.last().unwrap().contains("rows: 0 / 0"));
    assert!(matches!(
        e.execute("EXPLAIN SELECT id FROM absent"),
        Err(DbError::UnknownTable(_))
    ));
}
