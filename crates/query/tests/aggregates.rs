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
        // После ошибки есть ещё данные: исполнитель должен остановиться на ошибке.
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

fn output(e: &SqlQueryEngine, sql: &str) -> Result<(Vec<ColumnDef>, Vec<Block>)> {
    let QueryOutput::Rows { header, stream } = e.execute(sql)? else {
        panic!()
    };
    Ok((header, stream.collect::<Result<Vec<_>>>()?))
}
fn row(e: &SqlQueryEngine, sql: &str) -> Vec<Value> {
    let (header, blocks) = output(e, sql).unwrap();
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0].header(), header);
    assert_eq!(blocks[0].rows(), 1);
    blocks[0].columns().iter().map(|c| c.data.get(0)).collect()
}

#[test]
fn all_aggregates_across_blocks_and_empty_blocks() {
    let b = fixture();
    for split in 0..=b.rows() {
        let e = engine(vec![
            b.slice(0, split),
            b.slice(0, 0),
            b.slice(split, b.rows() - split),
        ]);
        assert_eq!(row(&e,"SELECT count() AS n, count(*) AS star, count(event) AS col, sum(value) AS total, min(value) AS lo, max(value) AS hi FROM events"),
        vec![Value::UInt64(5),Value::UInt64(5),Value::UInt64(5),Value::Int64(13),Value::Int64(-2),Value::Int64(10)]);
    }
}

#[test]
fn where_precedes_aggregation_and_limit_follows_it() {
    let b = fixture();
    let e = engine(vec![b.slice(0, 2), b.slice(2, 3)]);
    assert_eq!(
        row(
            &e,
            "SELECT count() AS n, sum(value) AS total FROM events WHERE event = 'click' LIMIT 1"
        ),
        vec![Value::UInt64(3), Value::Int64(10)]
    );
    assert_eq!(
        row(&e, "SELECT sum(value) FROM events LIMIT 100"),
        vec![Value::Int64(13)]
    );
}

#[test]
fn empty_input_and_filtered_out_input_return_defaults() {
    for e in [
        engine(vec![]),
        engine(vec![fixture().slice(0, 0)]),
        engine(vec![fixture()]),
    ] {
        assert_eq!(row(&e,"SELECT count(), sum(value), sum(id), min(value), max(value), min(event), max(day), min(time) FROM events WHERE id > 100"),
        vec![Value::UInt64(0),Value::Int64(0),Value::UInt64(0),Value::Int64(0),Value::Int64(0),Value::String(String::new()),Value::Date(0),Value::DateTime(0)]);
    }
}

#[test]
fn min_max_preserve_string_date_and_datetime_types() {
    let e = engine(vec![fixture()]);
    assert_eq!(
        row(
            &e,
            "SELECT min(event), max(event), min(day), max(day), min(time), max(time) FROM events"
        ),
        vec![
            Value::String("click".into()),
            Value::String("view".into()),
            Value::parse("2026-10-01", DataType::Date).unwrap(),
            Value::parse("2026-10-03", DataType::Date).unwrap(),
            Value::DateTime(0),
            Value::DateTime(86401)
        ]
    );
}

#[test]
fn aggregate_arguments_support_arithmetic_and_constants() {
    let e = engine(vec![fixture()]);
    assert_eq!(row(&e,"SELECT SUM(value * 2) AS doubled, count(42) AS n, sum(1) AS ones, min(-value) AS negative FROM events"),
        vec![Value::Int64(26),Value::UInt64(5),Value::Int64(5),Value::Int64(-10)]);
}

#[test]
fn sum_wraps_signed_and_unsigned_integers() {
    let e = engine(vec![fixture().slice(0, 1), fixture().slice(1, 1)]);
    assert_eq!(
        row(
            &e,
            "SELECT sum(9223372036854775807), sum(18446744073709551615) FROM events"
        ),
        vec![Value::Int64(-2), Value::UInt64(u64::MAX - 1)]
    );
    assert!(output(&e, "SELECT sum(9223372036854775807 + 1) FROM events").is_err());
}

#[test]
fn float_results_and_empty_float_defaults() {
    let e = engine(vec![fixture()]);
    assert_eq!(
        row(
            &e,
            "SELECT sum(value / 2), min(value / 2), max(value / 2) FROM events"
        ),
        vec![
            Value::Float64(6.5),
            Value::Float64(-1.0),
            Value::Float64(5.0)
        ]
    );
    assert_eq!(
        row(
            &e,
            "SELECT sum(value / 2), min(value / 2), max(value / 2) FROM events WHERE id > 99"
        ),
        vec![Value::Float64(0.0); 3]
    );
}

#[test]
fn invalid_aggregates_are_rejected_before_scan_even_at_limit_zero() {
    for sql in [
        "SELECT sum(event) FROM events",
        "SELECT sum(day) FROM events",
        "SELECT sum(time) FROM events",
        "SELECT count(missing) FROM events",
        "SELECT sum(sum(value)) FROM events",
        "SELECT count(), id FROM events",
        "SELECT *, count() FROM events",
        "SELECT count(), 1 FROM events",
        "SELECT sum(value) + 1 FROM events",
        "SELECT count() FROM events WHERE sum(value) > 0",
        "SELECT count() AS n, sum(value) AS n FROM events",
        "SELECT count(value > 0) FROM events",
    ] {
        let (e, t) = spy(vec![]);
        assert!(e.execute(&format!("{sql} LIMIT 0")).is_err(), "{sql}");
        assert!(t.request.lock().unwrap().is_none(), "{sql}");
    }
}

#[test]
fn unsupported_function_modifiers_are_not_silently_ignored() {
    let e = engine(vec![]);
    for sql in [
        "SELECT sum() FROM events",
        "SELECT count(id,value) FROM events",
        "SELECT max(*) FROM events",
        "SELECT avg(value) FROM events",
        "SELECT count(DISTINCT id) FROM events",
        "SELECT count(ALL id) FROM events",
        "SELECT sum(value) FILTER (WHERE id > 1) FROM events",
        "SELECT sum(value) OVER () FROM events",
        "SELECT sum(value ORDER BY id) FROM events",
        "SELECT sum(value) WITHIN GROUP (ORDER BY id) FROM events",
        "SELECT sum(x => value) FROM events",
        "SELECT db.sum(value) FROM events",
        "SELECT count(events.*) FROM events",
        "SELECT sum(1)(value) FROM events",
        "SELECT count() FROM events GROUP BY id",
    ] {
        assert!(e.execute(sql).is_err(), "{sql}");
    }
}

#[test]
fn count_only_requests_no_columns_and_reads_lazily_until_error() {
    let b = fixture();
    let (e, t) = spy(vec![b.slice(0, 2), b.slice(2, 3)]);
    let QueryOutput::Rows { header, mut stream } = e
        .execute("SELECT count(*) AS n FROM events LIMIT 1")
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(header[0].data_type, DataType::UInt64);
    assert!(t
        .request
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .columns
        .is_empty());
    assert_eq!(t.reads.load(Ordering::SeqCst), 0);
    assert!(matches!(stream.next(), Some(Err(DbError::Io(_)))));
    assert!(stream.next().is_none());
    assert!(stream.next().is_none());
    assert_eq!(t.reads.load(Ordering::SeqCst), 3);
}

#[test]
fn limit_zero_returns_header_without_scan() {
    let (e, t) = spy(vec![]);
    let (header, blocks) = output(
        &e,
        "SELECT count() AS n, sum(value) AS total FROM events LIMIT 0",
    )
    .unwrap();
    assert_eq!(
        header.iter().map(|c| c.data_type).collect::<Vec<_>>(),
        vec![DataType::UInt64, DataType::Int64]
    );
    assert!(blocks.is_empty());
    assert!(t.request.lock().unwrap().is_none());
}

#[test]
fn scan_request_contains_aggregate_arguments_and_where_columns() {
    let (e, t) = spy(vec![]);
    e.execute("SELECT sum(value), min(value) FROM events WHERE event = 'click'")
        .unwrap();
    let req = t.request.lock().unwrap().clone().unwrap();
    assert_eq!(req.columns, vec!["value", "event"]);
    assert!(req.predicate.is_some());
}

#[test]
fn argument_error_is_returned_once_without_partial_aggregate() {
    let b = fixture();
    let e = engine(vec![b.slice(0, 1), b.slice(1, 4)]);
    for function in ["sum", "count"] {
        let QueryOutput::Rows { mut stream, .. } = e
            .execute(&format!("SELECT {function}(10 / value) FROM events"))
            .unwrap()
        else {
            panic!()
        };
        assert!(matches!(stream.next(), Some(Err(DbError::TypeMismatch(_)))));
        assert!(stream.next().is_none());
    }
}
