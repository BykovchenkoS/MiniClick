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
        // После ошибки есть ещё данные, исполнитель должен остановиться на ошибке.
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
fn sorts_across_blocks_then_limits() {
    let b = fixture();
    for split in 0..=b.rows() {
        let e = engine(vec![
            b.slice(0, split),
            b.slice(0, 0),
            b.slice(split, b.rows() - split),
        ]);
        assert_eq!(
            result(&e, "SELECT id FROM events ORDER BY id DESC LIMIT 2").1,
            vec![vec![Value::UInt64(5)], vec![Value::UInt64(4)]]
        );
        assert_eq!(
            result(
                &e,
                "SELECT id FROM events WHERE id > 1 ORDER BY id ASC LIMIT 2"
            )
            .1,
            vec![vec![Value::UInt64(2)], vec![Value::UInt64(3)]]
        );
    }
}
#[test]
fn multiple_keys_and_stable_ties() {
    let e = engine(vec![fixture()]);
    assert_eq!(
        result(&e, "SELECT id FROM events ORDER BY event, id DESC").1,
        [5, 3, 1, 4, 2]
            .into_iter()
            .map(|id| vec![Value::UInt64(id)])
            .collect::<Vec<_>>()
    );
    assert_eq!(
        result(&e, "SELECT id FROM events ORDER BY event").1,
        [1, 3, 5, 2, 4]
            .into_iter()
            .map(|id| vec![Value::UInt64(id)])
            .collect::<Vec<_>>()
    );
}
#[test]
fn alias_priority_and_computed_projection() {
    let e = engine(vec![fixture()]);
    assert_eq!(
        result(&e, "SELECT -value AS id FROM events ORDER BY id").1,
        [-10, -3, -2, 0, 2]
            .into_iter()
            .map(|v| vec![Value::Int64(v)])
            .collect::<Vec<_>>()
    );
    assert_eq!(
        result(
            &e,
            "SELECT id AS renamed FROM events ORDER BY id DESC LIMIT 1"
        )
        .1,
        vec![vec![Value::UInt64(5)]]
    );
}
#[test]
fn hidden_sort_columns_do_not_leak_or_collide() {
    let e = engine(vec![fixture()]);
    let (header, rows) = result(
        &e,
        "SELECT id AS __sort_1 FROM events ORDER BY event, event DESC, id DESC LIMIT 1",
    );
    assert_eq!(header.len(), 1);
    assert_eq!(header[0].name, "__sort_1");
    assert_eq!(rows, vec![vec![Value::UInt64(5)]]);
}
#[test]
fn sorts_grouped_aggregates_and_hidden_group_keys() {
    let e = engine(vec![fixture()]);
    assert_eq!(result(&e,"SELECT event, sum(value) AS total FROM events GROUP BY event ORDER BY total DESC LIMIT 1").1,
        vec![vec![Value::String("click".into()),Value::Int64(10)]]);
    assert_eq!(
        result(
            &e,
            "SELECT count() AS n FROM events GROUP BY event ORDER BY event DESC"
        )
        .1,
        vec![vec![Value::UInt64(2)], vec![Value::UInt64(3)]]
    );
    assert_eq!(
        result(
            &e,
            "SELECT event FROM events GROUP BY event ORDER BY event DESC LIMIT 1"
        )
        .1,
        vec![vec![Value::String("view".into())]]
    );
    assert_eq!(
        result(
            &e,
            "SELECT sum(value) AS total FROM events ORDER BY total DESC"
        )
        .1,
        vec![vec![Value::Int64(13)]]
    );
}
#[test]
fn empty_results_keep_public_header() {
    for e in [engine(vec![]), engine(vec![fixture().slice(0, 0)])] {
        let (header, rows) = result(&e, "SELECT id FROM events ORDER BY event");
        assert_eq!(header.len(), 1);
        assert_eq!(header[0].name, "id");
        assert!(rows.is_empty());
        assert!(result(
            &e,
            "SELECT count() AS n FROM events GROUP BY event ORDER BY event"
        )
        .1
        .is_empty());
        assert_eq!(
            result(&e, "SELECT count() AS n FROM events ORDER BY n").1,
            vec![vec![Value::UInt64(0)]]
        );
    }
}
#[test]
fn invalid_sort_names_rejected_before_scan_and_limit_zero() {
    for sql in [
        "SELECT id FROM events ORDER BY missing",
        "SELECT count() AS n FROM events ORDER BY id",
        "SELECT event, count() FROM events GROUP BY event ORDER BY value",
    ] {
        let (e, t) = spy(vec![]);
        assert!(e.execute(&format!("{sql} LIMIT 0")).is_err(), "{sql}");
        assert!(t.request.lock().unwrap().is_none());
    }
    let (e, t) = spy(vec![]);
    let (header, rows) = result(&e, "SELECT id FROM events ORDER BY event LIMIT 0");
    assert_eq!(header.len(), 1);
    assert!(rows.is_empty());
    assert!(t.request.lock().unwrap().is_none());
}
#[test]
fn unsupported_sort_options_are_not_ignored() {
    let e = engine(vec![]);
    for suffix in [
        "1",
        "id + 1",
        "id NULLS FIRST",
        "id NULLS LAST",
        "id WITH FILL",
        "id INTERPOLATE",
        "events.id",
        "id COLLATE 'en'",
        "count()",
        "ALL",
    ] {
        assert!(
            e.execute(&format!("SELECT id FROM events ORDER BY {suffix}"))
                .is_err(),
            "{suffix}"
        );
    }
}

#[test]
fn sorting_reads_lazily_but_does_not_hide_later_errors() {
    let (e, t) = spy(vec![fixture()]);
    let QueryOutput::Rows { mut stream, header } = e
        .execute("SELECT id FROM events ORDER BY event LIMIT 1")
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(header.len(), 1);
    assert_eq!(t.reads.load(Ordering::SeqCst), 0);
    assert_eq!(
        t.request.lock().unwrap().as_ref().unwrap().columns,
        vec!["id", "event"]
    );
    assert!(matches!(stream.next(), Some(Err(DbError::Io(_)))));
    assert!(stream.next().is_none());
    assert!(stream.next().is_none());
    assert_eq!(t.reads.load(Ordering::SeqCst), 2);
}
#[test]
fn projection_error_beyond_limit_is_not_hidden_by_sort() {
    let e = engine(vec![fixture()]);
    let QueryOutput::Rows { mut stream, .. } = e
        .execute("SELECT 10 / value AS ratio FROM events ORDER BY id LIMIT 1")
        .unwrap()
    else {
        panic!()
    };
    assert!(matches!(stream.next(), Some(Err(DbError::TypeMismatch(_)))));
    assert!(stream.next().is_none());
}
#[test]
fn sorts_dates_datetimes_signed_and_large_unsigned_values() {
    let e = engine(vec![fixture()]);
    assert_eq!(
        result(
            &e,
            "SELECT id FROM events ORDER BY day DESC, time DESC LIMIT 2"
        )
        .1,
        vec![vec![Value::UInt64(5)], vec![Value::UInt64(4)]]
    );
    assert_eq!(
        result(&e, "SELECT value FROM events ORDER BY value LIMIT 1").1,
        vec![vec![Value::Int64(-2)]]
    );
    let e = custom(
        Block::new(vec![Column {
            name: "key".into(),
            data: ColumnData::UInt64(vec![u64::MAX, 0, u64::MAX - 1]),
        }])
        .unwrap(),
    );
    assert_eq!(
        result(&e, "SELECT key FROM events ORDER BY key").1,
        vec![
            vec![Value::UInt64(0)],
            vec![Value::UInt64(u64::MAX - 1)],
            vec![Value::UInt64(u64::MAX)]
        ]
    );
}
#[test]
fn float_sort_uses_total_order() {
    let values = vec![f64::NAN, 0.0, -0.0, f64::NEG_INFINITY, 1.0, f64::INFINITY];
    let e = custom(
        Block::new(vec![Column {
            name: "f".into(),
            data: ColumnData::Float64(values.clone()),
        }])
        .unwrap(),
    );
    let mut expected = values;
    expected.sort_by(f64::total_cmp);
    for (row, expected) in result(&e, "SELECT f FROM events ORDER BY f")
        .1
        .iter()
        .zip(expected)
    {
        let Value::Float64(value) = row[0] else {
            panic!()
        };
        assert_eq!(value.to_bits(), expected.to_bits());
    }
}
#[test]
fn quoted_aliases_and_case_are_preserved() {
    let e = engine(vec![fixture()]);
    let (header, rows) = result(
        &e,
        "SELECT id AS `User ID` FROM events ORDER BY `User ID` DESC LIMIT 1",
    );
    assert_eq!(header[0].name, "User ID");
    assert_eq!(rows, vec![vec![Value::UInt64(5)]]);
    assert!(e
        .execute("SELECT id AS `User ID` FROM events ORDER BY `user id`")
        .is_err());
}
