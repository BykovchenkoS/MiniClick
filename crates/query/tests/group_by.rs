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

fn result(e: &SqlQueryEngine, sql: &str) -> (Vec<ColumnDef>, Vec<Vec<Value>>) {
    let QueryOutput::Rows { header, stream } = e.execute(sql).unwrap() else {
        panic!()
    };
    let mut rows = Vec::new();
    for block in stream {
        let block = block.unwrap();
        assert_eq!(header, block.header());
        for i in 0..block.rows() {
            rows.push(block.columns().iter().map(|c| c.data.get(i)).collect());
        }
    }
    (header, rows)
}
fn custom(block: Block) -> SqlQueryEngine {
    let c = Arc::new(MemCatalog::new());
    let mut d = definition();
    d.schema.columns = block.header();
    c.create_table(d, false).unwrap();
    c.get_table("events").unwrap().insert(block).unwrap();
    SqlQueryEngine::new(c)
}

#[test]
fn groups_across_blocks_with_all_aggregates_and_output_order() {
    let b = fixture();
    for split in 0..=b.rows() {
        let e = engine(vec![
            b.slice(0, split),
            b.slice(0, 0),
            b.slice(split, b.rows() - split),
        ]);
        let (header, rows)=result(&e,"SELECT sum(value) AS total, event AS kind, count(*) AS n, min(value) AS lo, max(value) AS hi FROM events GROUP BY event");
        assert_eq!(
            header.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            vec!["total", "kind", "n", "lo", "hi"]
        );
        assert_eq!(
            rows,
            vec![
                vec![
                    Value::Int64(10),
                    Value::String("click".into()),
                    Value::UInt64(3),
                    Value::Int64(-2),
                    Value::Int64(10)
                ],
                vec![
                    Value::Int64(3),
                    Value::String("view".into()),
                    Value::UInt64(2),
                    Value::Int64(0),
                    Value::Int64(3)
                ],
            ]
        );
    }
}

#[test]
fn where_then_grouping_then_limit() {
    let e = engine(vec![fixture().slice(0, 2), fixture().slice(2, 3)]);
    assert_eq!(
        result(
            &e,
            "SELECT event, sum(value) AS total FROM events WHERE id > 1 GROUP BY event LIMIT 1"
        )
        .1,
        vec![vec![Value::String("view".into()), Value::Int64(3)]]
    );
    assert_eq!(
        result(
            &e,
            "SELECT event, sum(value) AS total FROM events GROUP BY event LIMIT 1"
        )
        .1,
        vec![vec![Value::String("click".into()), Value::Int64(10)]]
    );
}

#[test]
fn multiple_keys_and_hidden_keys() {
    let e = engine(vec![fixture()]);
    let (_, rows) = result(
        &e,
        "SELECT event, day, count() AS n FROM events GROUP BY event, day",
    );
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0][2], Value::UInt64(2));
    assert_eq!(
        result(&e, "SELECT count() AS n FROM events GROUP BY event").1,
        vec![vec![Value::UInt64(3)], vec![Value::UInt64(2)]]
    );
}

#[test]
fn grouping_without_aggregates_and_repeated_keys() {
    let e = engine(vec![fixture()]);
    assert_eq!(
        result(
            &e,
            "SELECT event AS a, event AS b FROM events GROUP BY event, event"
        )
        .1,
        vec![
            vec![Value::String("click".into()); 2],
            vec![Value::String("view".into()); 2]
        ]
    );
    assert_eq!(
        result(&e, "SELECT event FROM events GROUP BY event, day")
            .1
            .len(),
        4
    );
}

#[test]
fn empty_input_has_header_and_no_groups() {
    for e in [
        engine(vec![]),
        engine(vec![fixture().slice(0, 0)]),
        engine(vec![fixture()]),
    ] {
        let (header, rows) = result(
            &e,
            "SELECT event, count() AS n FROM events WHERE id > 100 GROUP BY event",
        );
        assert_eq!(
            header.iter().map(|c| c.data_type).collect::<Vec<_>>(),
            vec![DataType::String, DataType::UInt64]
        );
        assert!(rows.is_empty());
    }
}

#[test]
fn arithmetic_arguments_and_count_column() {
    let e = engine(vec![fixture()]);
    assert_eq!(
        result(
            &e,
            "SELECT event, sum(value * 2) AS total, count(value) AS n FROM events GROUP BY event"
        )
        .1,
        vec![
            vec![
                Value::String("click".into()),
                Value::Int64(20),
                Value::UInt64(3)
            ],
            vec![
                Value::String("view".into()),
                Value::Int64(6),
                Value::UInt64(2)
            ]
        ]
    );
}

#[test]
fn invalid_grouping_is_rejected_before_scan_even_with_limit_zero() {
    for sql in [
        "SELECT id, count() FROM events GROUP BY event",
        "SELECT event, count() FROM events GROUP BY missing",
        "SELECT value FROM events GROUP BY missing",
        "SELECT event AS kind, count() FROM events GROUP BY kind",
        "SELECT count(), 42 FROM events GROUP BY event",
        "SELECT *, count() FROM events GROUP BY event",
        "SELECT event + 1 FROM events GROUP BY event",
        "SELECT sum(sum(value)) FROM events GROUP BY event",
        "SELECT sum(value) + 1 FROM events GROUP BY event",
        "SELECT event, count() FROM events WHERE count() > 0 GROUP BY event",
        "SELECT event AS n, count() AS n FROM events GROUP BY event",
    ] {
        let (e, t) = spy(vec![]);
        assert!(e.execute(&format!("{sql} LIMIT 0")).is_err(), "{sql}");
        assert!(t.request.lock().unwrap().is_none(), "{sql}");
    }
}

#[test]
fn unsupported_grouping_forms_are_rejected() {
    let e = engine(vec![]);
    for suffix in [
        "ALL",
        "1",
        "value + 1",
        "ROLLUP(event)",
        "CUBE(event)",
        "GROUPING SETS ((event), ())",
        "event WITH TOTALS",
        "event WITH ROLLUP",
        "event WITH CUBE",
        "()",
        "events.event",
        "event HAVING count() > 1",
        "event ORDER BY event",
    ] {
        assert!(
            e.execute(&format!("SELECT count() FROM events GROUP BY {suffix}"))
                .is_err(),
            "{suffix}"
        );
    }
}

#[test]
fn grouped_scan_is_lazy_and_source_error_prevents_partial_results() {
    let (e, t) = spy(vec![fixture()]);
    let QueryOutput::Rows { mut stream, .. } = e
        .execute("SELECT sum(value) FROM events WHERE id > 0 GROUP BY event LIMIT 1")
        .unwrap()
    else {
        panic!()
    };
    let req = t.request.lock().unwrap().clone().unwrap();
    assert_eq!(req.columns, vec!["value", "event", "id"]);
    assert!(req.predicate.is_some());
    assert_eq!(t.reads.load(Ordering::SeqCst), 0);
    assert!(matches!(stream.next(), Some(Err(DbError::Io(_)))));
    assert!(stream.next().is_none());
    assert!(stream.next().is_none());
    assert_eq!(t.reads.load(Ordering::SeqCst), 2);
}

#[test]
fn limit_zero_does_not_scan() {
    let (e, t) = spy(vec![]);
    let (header, rows) = result(
        &e,
        "SELECT event, sum(value) FROM events GROUP BY event LIMIT 0",
    );
    assert_eq!(header.len(), 2);
    assert!(rows.is_empty());
    assert!(t.request.lock().unwrap().is_none());
}

#[test]
fn runtime_error_in_later_group_is_not_hidden_by_limit() {
    let b = fixture();
    let e = engine(vec![b.slice(0, 1), b.slice(1, 4)]);
    let QueryOutput::Rows { mut stream, .. } = e
        .execute("SELECT event, sum(10 / value) FROM events GROUP BY event LIMIT 1")
        .unwrap()
    else {
        panic!()
    };
    assert!(matches!(stream.next(), Some(Err(DbError::TypeMismatch(_)))));
    assert!(stream.next().is_none());
}

#[test]
fn key_types_and_large_integers_are_preserved() {
    let e = engine(vec![fixture()]);
    for col in ["id", "value", "day", "time"] {
        let (header, rows) = result(
            &e,
            &format!("SELECT {col}, count() FROM events GROUP BY {col}"),
        );
        assert_eq!(
            header[0].data_type,
            definition().schema.column(col).unwrap().data_type
        );
        assert!(!rows.is_empty());
    }
    let e = custom(
        Block::new(vec![Column {
            name: "key".into(),
            data: ColumnData::UInt64(vec![u64::MAX, u64::MAX - 1, u64::MAX]),
        }])
        .unwrap(),
    );
    assert_eq!(
        result(&e, "SELECT key, count() FROM events GROUP BY key").1,
        vec![
            vec![Value::UInt64(u64::MAX), Value::UInt64(2)],
            vec![Value::UInt64(u64::MAX - 1), Value::UInt64(1)]
        ]
    );
}

#[test]
fn composite_string_keys_have_no_separator_collisions() {
    let e = custom(
        Block::new(vec![
            Column {
                name: "a".into(),
                data: ColumnData::String(vec!["a|b".into(), "a".into(), "".into(), "a|b".into()]),
            },
            Column {
                name: "b".into(),
                data: ColumnData::String(vec!["c".into(), "b|c".into(), "".into(), "c".into()]),
            },
        ])
        .unwrap(),
    );
    let (_, rows) = result(&e, "SELECT *, count() AS n FROM events GROUP BY a,b");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][2], Value::UInt64(2));
}

#[test]
fn float_keys_follow_total_order_equality_including_nan_and_signed_zero() {
    let nan = f64::from_bits(0x7ff8000000000001);
    let other_nan = f64::from_bits(0x7ff8000000000002);
    let e = custom(
        Block::new(vec![Column {
            name: "f".into(),
            data: ColumnData::Float64(vec![-0.0, 0.0, -0.0, nan, nan, other_nan, f64::INFINITY]),
        }])
        .unwrap(),
    );
    let (_, rows) = result(&e, "SELECT f, count() FROM events GROUP BY f");
    assert_eq!(rows.len(), 5);
    for (row, (value, count)) in rows.iter().zip([
        (-0.0_f64, 2),
        (0.0, 1),
        (nan, 2),
        (other_nan, 1),
        (f64::INFINITY, 1),
    ]) {
        let Value::Float64(actual) = row[0] else {
            panic!()
        };
        assert_eq!(actual.to_bits(), value.to_bits());
        assert_eq!(row[1], Value::UInt64(count));
    }
}

#[test]
fn quoted_names_and_case_sensitive_group_columns() {
    let e = custom(
        Block::new(vec![Column {
            name: "Event Type".into(),
            data: ColumnData::String(vec!["a".into(), "a".into()]),
        }])
        .unwrap(),
    );
    assert_eq!(
        result(
            &e,
            "SELECT `Event Type` AS kind, COUNT(*) AS n FROM events GROUP BY `Event Type`"
        )
        .1,
        vec![vec![Value::String("a".into()), Value::UInt64(2)]]
    );
    assert!(e
        .execute("SELECT count() FROM events GROUP BY `event type`")
        .is_err());
}
