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
            ],
        },
        engine: Engine::Memory,
        order_by: vec![],
        settings: TableSettings::default(),
    }
}

fn block(ids: Vec<u64>, events: Vec<&str>) -> Block {
    Block::new(vec![
        Column {
            name: "id".into(),
            data: ColumnData::UInt64(ids),
        },
        Column {
            name: "event".into(),
            data: ColumnData::String(events.into_iter().map(str::to_owned).collect()),
        },
    ])
    .unwrap()
}

fn setup(blocks: Vec<Block>) -> SqlQueryEngine {
    let catalog = Arc::new(MemCatalog::new());
    catalog.create_table(definition(), false).unwrap();
    let table = catalog.get_table("events").unwrap();
    for b in blocks {
        table.insert(b).unwrap();
    }
    SqlQueryEngine::new(catalog)
}

fn collect(engine: &SqlQueryEngine, sql: &str) -> (Vec<ColumnDef>, Vec<Block>) {
    let QueryOutput::Rows { header, stream } = engine.execute(sql).unwrap() else {
        panic!("ожидался результат SELECT");
    };
    (header, stream.collect::<Result<Vec<_>>>().unwrap())
}

#[test]
fn selects_all_columns() {
    let b = block(vec![1, 2], vec!["click", "view"]);
    let engine = setup(vec![b.clone()]);
    let (header, blocks) = collect(&engine, "SELECT * FROM events;");
    assert_eq!(header, definition().schema.columns);
    assert_eq!(blocks, vec![b]);
}

#[test]
fn selects_columns_in_requested_order() {
    let b = block(vec![1], vec!["click"]);
    let engine = setup(vec![b.clone()]);
    let (header, blocks) = collect(&engine, "SELECT event, id FROM events");
    assert_eq!(
        header.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
        ["event", "id"]
    );
    assert_eq!(
        blocks,
        vec![b.select(&["event".into(), "id".into()]).unwrap()]
    );
}

#[test]
fn aliases_and_repeated_source_have_distinct_output_names() {
    let engine = setup(vec![block(vec![7], vec!["click"])]);
    let (header, blocks) = collect(&engine, "SELECT id AS first, id AS second FROM events");
    assert_eq!(
        header.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
        ["first", "second"]
    );
    assert_eq!(
        blocks[0].column("first"),
        Some(&ColumnData::UInt64(vec![7]))
    );
    assert_eq!(blocks[0].column("second"), blocks[0].column("first"));
}

#[test]
fn rejects_duplicate_output_names() {
    let engine = setup(vec![]);
    for sql in [
        "SELECT id, id FROM events",
        "SELECT *, id FROM events",
        "SELECT id AS x, event AS x FROM events",
    ] {
        assert!(
            matches!(engine.execute(sql), Err(DbError::Unsupported(_))),
            "{sql}"
        );
    }
}

#[test]
fn empty_table_still_has_header() {
    let engine = setup(vec![]);
    let (header, blocks) = collect(&engine, "SELECT id AS user_id FROM events");
    assert_eq!(
        header,
        vec![ColumnDef {
            name: "user_id".into(),
            data_type: DataType::UInt64
        }]
    );
    assert!(blocks.is_empty());
}

#[test]
fn preserves_multiple_and_empty_blocks() {
    let input = vec![
        block(vec![1], vec!["click"]),
        block(vec![], vec![]),
        block(vec![2, 3], vec!["view", "click"]),
    ];
    let engine = setup(input.clone());
    let (_, blocks) = collect(&engine, "SELECT id FROM events");
    let expected: Vec<_> = input
        .iter()
        .map(|b| b.select(&["id".into()]).unwrap())
        .collect();
    assert_eq!(blocks, expected);
}

#[test]
fn validates_missing_and_case_sensitive_names_on_empty_table() {
    let engine = setup(vec![]);
    assert!(matches!(
        engine.execute("SELECT * FROM absent"),
        Err(DbError::UnknownTable(_))
    ));
    assert!(matches!(
        engine.execute("SELECT * FROM Events"),
        Err(DbError::UnknownTable(_))
    ));
    assert!(matches!(
        engine.execute("SELECT absent FROM events"),
        Err(DbError::UnknownColumn(_))
    ));
    assert!(matches!(
        engine.execute("SELECT ID FROM events"),
        Err(DbError::UnknownColumn(_))
    ));
}

// Проверяем контракт с произвольным Storage, а не только с MemTable.
struct ProbeTable {
    definition: TableDefinition,
    request: Mutex<Option<ScanRequest>>,
    reads: Arc<AtomicUsize>,
}

impl Table for ProbeTable {
    fn definition(&self) -> &TableDefinition {
        &self.definition
    }
    fn insert(&self, _: Block) -> Result<()> {
        Err(DbError::Unsupported("probe".into()))
    }
    fn scan(&self, request: &ScanRequest) -> Result<BlockStream> {
        *self.request.lock().unwrap() = Some(request.clone());
        let reads = self.reads.clone();
        Ok(Box::new(std::iter::once_with(move || {
            reads.fetch_add(1, Ordering::SeqCst);
            Err(DbError::Io(std::io::Error::other("ошибка чтения")))
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

struct ProbeCatalog(Arc<ProbeTable>);
impl Catalog for ProbeCatalog {
    fn create_table(&self, _: TableDefinition, _: bool) -> Result<()> {
        Err(DbError::Unsupported("probe".into()))
    }
    fn drop_table(&self, _: &str, _: bool) -> Result<()> {
        Err(DbError::Unsupported("probe".into()))
    }
    fn get_table(&self, _: &str) -> Result<Arc<dyn Table>> {
        Ok(self.0.clone())
    }
    fn list_tables(&self) -> Vec<TableDefinition> {
        vec![self.0.definition.clone()]
    }
}

#[test]
fn requests_only_needed_columns_and_reads_lazily_and_propagates_error() {
    let table = Arc::new(ProbeTable {
        definition: definition(),
        request: Mutex::new(None),
        reads: Arc::new(AtomicUsize::new(0)),
    });
    let engine = SqlQueryEngine::new(Arc::new(ProbeCatalog(table.clone())));
    let QueryOutput::Rows { mut stream, header } = engine
        .execute("SELECT id AS a, id AS b FROM events")
        .unwrap()
    else {
        panic!("ожидались строки");
    };
    assert_eq!(header.len(), 2);
    assert_eq!(
        *table.request.lock().unwrap(),
        Some(ScanRequest {
            columns: vec!["id".into()],
            predicate: None
        })
    );
    assert_eq!(table.reads.load(Ordering::SeqCst), 0);
    assert!(matches!(stream.next(), Some(Err(DbError::Io(_)))));
    assert_eq!(table.reads.load(Ordering::SeqCst), 1);
    assert!(stream.next().is_none());
}

#[test]
fn invalid_query_does_not_start_scan() {
    let table = Arc::new(ProbeTable {
        definition: definition(),
        request: Mutex::new(None),
        reads: Arc::new(AtomicUsize::new(0)),
    });
    let engine = SqlQueryEngine::new(Arc::new(ProbeCatalog(table.clone())));
    assert!(matches!(
        engine.execute("SELECT missing FROM events"),
        Err(DbError::UnknownColumn(_))
    ));
    assert!(table.request.lock().unwrap().is_none());
}
