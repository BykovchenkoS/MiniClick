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

fn rows(engine: &SqlQueryEngine, sql: &str) -> Vec<u64> {
    let QueryOutput::Rows { header, stream } = engine.execute(sql).unwrap() else {
        panic!("ожидались строки")
    };
    assert_eq!(
        header,
        vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::UInt64
        }]
    );
    let mut result = Vec::new();
    for block in stream {
        let block = block.unwrap();
        assert_eq!(block.header(), header);
        let Some(ColumnData::UInt64(ids)) = block.column("id") else {
            panic!("ожидался id")
        };
        result.extend_from_slice(ids);
    }
    result
}

#[test]
fn all_comparisons_filter_a_column_not_in_output() {
    let engine = engine(vec![fixture()]);
    for (predicate, expected) in [
        ("value = 2", vec![3]),
        ("value != 2", vec![1, 2, 4, 5]),
        ("value <> 2", vec![1, 2, 4, 5]),
        ("value < 2", vec![1, 2]),
        ("value <= 2", vec![1, 2, 3]),
        ("value > 2", vec![4, 5]),
        ("value >= 2", vec![3, 4, 5]),
        ("2 < value", vec![4, 5]),
        ("event = 'click'", vec![1, 3, 5]),
    ] {
        assert_eq!(
            rows(&engine, &format!("SELECT id FROM events WHERE {predicate}")),
            expected,
            "{predicate}"
        );
    }
}

#[test]
fn logical_precedence_parentheses_and_not() {
    let engine = engine(vec![fixture()]);
    for (predicate, expected) in [
        ("id = 1 OR id = 2 AND value > 1", vec![1]),
        ("(id = 1 OR id = 2) AND value > 1", vec![]),
        ("NOT (event = 'view') AND id >= 3", vec![3, 5]),
        ("NOT NOT (id = 2)", vec![2]),
        ("value < 0 OR (event = 'click' AND value > 2)", vec![1, 5]),
    ] {
        assert_eq!(
            rows(&engine, &format!("SELECT id FROM events WHERE {predicate}")),
            expected,
            "{predicate}"
        );
    }
}

#[test]
fn typed_literals_dates_and_datetimes() {
    let engine = engine(vec![fixture()]);
    for (predicate, expected) in [
        ("id = '3'", vec![3]),
        ("'3' = id", vec![3]),
        ("day = '2026-10-01'", vec![1, 3]),
        ("'2026-10-02' <= day", vec![2, 4, 5]),
        ("time >= '1970-01-02'", vec![4, 5]),
        ("time = '1970-01-01T00:00:01'", vec![2]),
    ] {
        assert_eq!(
            rows(&engine, &format!("SELECT id FROM events WHERE {predicate}")),
            expected,
            "{predicate}"
        );
    }
}

#[test]
fn numeric_literals_keep_fraction_and_sign() {
    let engine = engine(vec![fixture()]);
    for (predicate, expected) in [
        ("value > 2.5", vec![4, 5]),
        ("2.5 < value", vec![4, 5]),
        ("id > -1", vec![1, 2, 3, 4, 5]),
        ("id = -1", vec![]),
        ("value < -1.5", vec![1]),
        ("id >= +3", vec![3, 4, 5]),
        ("value > 2e0", vec![4, 5]),
        ("value < id", vec![1, 2, 3, 4]),
    ] {
        assert_eq!(
            rows(&engine, &format!("SELECT id FROM events WHERE {predicate}")),
            expected,
            "{predicate}"
        );
    }
}

#[test]
fn compares_large_literals_without_losing_precision() {
    let b = Block::new(vec![
        Column {
            name: "id".into(),
            data: ColumnData::UInt64(vec![9_007_199_254_740_992, 9_007_199_254_740_993, u64::MAX]),
        },
        Column {
            name: "event".into(),
            data: ColumnData::String(vec![String::new(); 3]),
        },
        Column {
            name: "value".into(),
            data: ColumnData::Int64(vec![i64::MIN, 0, i64::MAX]),
        },
        Column {
            name: "day".into(),
            data: ColumnData::Date(vec![0; 3]),
        },
        Column {
            name: "time".into(),
            data: ColumnData::DateTime(vec![0; 3]),
        },
    ])
    .unwrap();
    let engine = engine(vec![b]);
    assert_eq!(
        rows(&engine, "SELECT id FROM events WHERE id = 9007199254740993"),
        vec![9_007_199_254_740_993]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT id FROM events WHERE id > 9007199254740992.0"
        ),
        vec![9_007_199_254_740_993, u64::MAX]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT id FROM events WHERE id < 18446744073709551616.0"
        ),
        vec![9_007_199_254_740_992, 9_007_199_254_740_993, u64::MAX]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT id FROM events WHERE value = -9223372036854775808"
        ),
        vec![9_007_199_254_740_992]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT id FROM events WHERE id = 18446744073709551615"
        ),
        vec![u64::MAX]
    );
}

#[test]
fn validates_where_even_on_empty_input_and_limit_zero() {
    let engine = engine(vec![]);
    for predicate in [
        "id = 'abc'",
        "day = '2026-02-30'",
        "event = 1",
        "id",
        "1",
        "id AND id = 1",
        "NOT id",
        "id = event",
    ] {
        let sql = format!("SELECT id FROM events WHERE {predicate} LIMIT 0");
        assert!(
            matches!(engine.execute(&sql), Err(DbError::TypeMismatch(_))),
            "{sql}"
        );
    }
    for sql in [
        "SELECT id FROM events WHERE missing = 1",
        "SELECT id FROM events WHERE id = 1 OR missing = 1",
        "SELECT id AS alias FROM events WHERE alias = 1",
        "SELECT id FROM events WHERE missing = 1 LIMIT 0",
    ] {
        assert!(
            matches!(engine.execute(sql), Err(DbError::UnknownColumn(_))),
            "{sql}"
        );
    }
}

#[test]
fn rejects_unimplemented_predicates_and_limit_variants() {
    let engine = engine(vec![]);
    for sql in [
        "SELECT id FROM events WHERE id IN (1, 2)",
        "SELECT id FROM events WHERE event LIKE 'c%'",
        "SELECT id FROM events WHERE id IS NULL",
        "SELECT id FROM events LIMIT 2 OFFSET 1",
        "SELECT id FROM events LIMIT 1 BY id",
    ] {
        assert!(
            matches!(engine.execute(sql), Err(DbError::Unsupported(_))),
            "{sql}"
        );
    }
    for sql in [
        "SELECT id FROM events LIMIT -1",
        "SELECT id FROM events LIMIT 1.5",
        "SELECT id FROM events LIMIT '2'",
        "SELECT id FROM events LIMIT 18446744073709551616",
    ] {
        assert!(
            matches!(engine.execute(sql), Err(DbError::Syntax(_))),
            "{sql}"
        );
    }
}

#[test]
fn limit_counts_filtered_rows_across_blocks_and_skips_empty_blocks() {
    let b = fixture();
    let engine = engine(vec![b.slice(0, 2), b.slice(0, 0), b.slice(2, 3)]);
    assert_eq!(
        rows(
            &engine,
            "SELECT id FROM events WHERE event = 'click' LIMIT 2"
        ),
        vec![1, 3]
    );
    assert_eq!(
        rows(&engine, "SELECT id FROM events LIMIT 3"),
        vec![1, 2, 3]
    );
    assert_eq!(
        rows(&engine, "SELECT id FROM events LIMIT 18446744073709551615"),
        vec![1, 2, 3, 4, 5]
    );
    assert_eq!(rows(&engine, "SELECT id FROM events LIMIT 0"), vec![]);
    assert_eq!(
        rows(&engine, "SELECT id FROM events WHERE id > 99 LIMIT 2"),
        vec![]
    );
}

#[test]
fn result_does_not_depend_on_block_boundaries() {
    let b = fixture();
    for split in 0..=b.rows() {
        let engine = engine(vec![b.slice(0, split), b.slice(split, b.rows() - split)]);
        assert_eq!(
            rows(
                &engine,
                "SELECT id FROM events WHERE id > 1 AND event = 'click' LIMIT 2"
            ),
            vec![3, 5]
        );
    }
}

// Таблица намеренно игнорирует predicate: проверяем точную фильтрацию в Query.
struct SpyTable {
    definition: TableDefinition,
    request: Mutex<Option<ScanRequest>>,
    blocks: Vec<Block>,
    reads: Arc<AtomicUsize>,
}
impl Table for SpyTable {
    fn definition(&self) -> &TableDefinition {
        &self.definition
    }
    fn insert(&self, _: Block) -> Result<()> {
        Err(DbError::Unsupported("spy".into()))
    }
    fn scan(&self, req: &ScanRequest) -> Result<BlockStream> {
        *self.request.lock().unwrap() = Some(req.clone());
        let mut input = self
            .blocks
            .iter()
            .map(|b| b.select(&req.columns))
            .collect::<Vec<_>>();
        input.push(Err(DbError::Io(std::io::Error::other(
            "ошибка следующего блока",
        ))));
        // После ошибки есть ещё данные, исполнитель должен остановиться на ошибке
        input.push(Ok(fixture().select(&req.columns)?));
        let reads = self.reads.clone();
        Ok(Box::new(input.into_iter().inspect(move |_| {
            reads.fetch_add(1, Ordering::SeqCst);
        })))
    }
    fn explain_scan(&self, _: &ScanRequest) -> Result<ScanStats> {
        Ok(ScanStats::default())
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
fn spy(blocks: Vec<Block>) -> (SqlQueryEngine, Arc<SpyTable>) {
    let table = Arc::new(SpyTable {
        definition: definition(),
        request: Mutex::new(None),
        blocks,
        reads: Arc::new(AtomicUsize::new(0)),
    });
    (
        SqlQueryEngine::new(Arc::new(SpyCatalog(table.clone()))),
        table,
    )
}

#[test]
fn passes_typed_predicate_and_hidden_columns_and_stops_reading_at_limit() {
    let b = fixture();
    let (engine, table) = spy(vec![b.slice(0, 2), b.slice(2, 3)]);
    let QueryOutput::Rows { mut stream, .. } = engine
        .execute("SELECT id FROM events WHERE day = '2026-10-01' LIMIT 2")
        .unwrap()
    else {
        panic!()
    };
    let req = table.request.lock().unwrap().clone().unwrap();
    assert_eq!(req.columns, vec!["id", "day"]);
    assert_eq!(
        req.predicate,
        Some(Expr::binary(
            BinaryOp::Eq,
            Expr::col("day"),
            Expr::lit(Value::parse("2026-10-01", DataType::Date).unwrap())
        ))
    );
    assert_eq!(table.reads.load(Ordering::SeqCst), 0);
    assert_eq!(
        stream.next().unwrap().unwrap().column("id"),
        Some(&ColumnData::UInt64(vec![1]))
    );
    assert_eq!(
        stream.next().unwrap().unwrap().column("id"),
        Some(&ColumnData::UInt64(vec![3]))
    );
    assert!(stream.next().is_none());
    assert_eq!(table.reads.load(Ordering::SeqCst), 2);
}

#[test]
fn limit_zero_does_not_call_scan() {
    let (engine, table) = spy(vec![fixture()]);
    assert_eq!(
        rows(&engine, "SELECT id FROM events WHERE id > 0 LIMIT 0"),
        vec![]
    );
    assert!(table.request.lock().unwrap().is_none());
    assert_eq!(table.reads.load(Ordering::SeqCst), 0);
}

#[test]
fn numeric_fallback_is_passed_unchanged_to_storage() {
    let (engine, table) = spy(vec![fixture()]);
    engine
        .execute("SELECT id FROM events WHERE id > -1")
        .unwrap();
    assert_eq!(
        table.request.lock().unwrap().as_ref().unwrap().predicate,
        Some(Expr::binary(
            BinaryOp::Gt,
            Expr::col("id"),
            Expr::lit(Value::Int64(-1))
        ))
    );
}

#[test]
fn error_before_limit_is_returned_once_and_ends_the_stream() {
    let (engine, table) = spy(vec![]);
    let QueryOutput::Rows { mut stream, .. } = engine
        .execute("SELECT id FROM events WHERE id > 0 LIMIT 2")
        .unwrap()
    else {
        panic!()
    };
    assert!(matches!(stream.next(), Some(Err(DbError::Io(_)))));
    assert!(stream.next().is_none());
    assert!(stream.next().is_none());
    assert_eq!(table.reads.load(Ordering::SeqCst), 1);
}
