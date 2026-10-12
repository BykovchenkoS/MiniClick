//! Query + may_match на тестовом Storage, без зависимости от MergeTree
use query::SqlQueryEngine;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use types::*;

struct RangeTable {
    definition: TableDefinition,
    blocks: Vec<Block>,
    pruning: bool,
    selected: AtomicUsize,
}
impl Table for RangeTable {
    fn definition(&self) -> &TableDefinition {
        &self.definition
    }
    fn insert(&self, _: Block) -> Result<()> {
        Err(DbError::Unsupported("test table".into()))
    }
    fn scan(&self, req: &ScanRequest) -> Result<BlockStream> {
        let mut result = Vec::new();
        let mut selected = 0;
        for block in &self.blocks {
            let id = block.column("id").unwrap();
            let range = KeyRange {
                min: vec![id.get(0)],
                max: vec![id.get(block.rows() - 1)],
            };
            if self.pruning
                && req
                    .predicate
                    .as_ref()
                    .is_some_and(|p| !may_match(p, &self.definition.order_by, &range))
            {
                continue;
            }
            selected += 1;
            result.push(block.select(&req.columns));
        }
        self.selected.store(selected, Ordering::SeqCst);
        Ok(Box::new(result.into_iter()))
    }
    fn explain_scan(&self, _: &ScanRequest) -> Result<ScanStats> {
        Err(DbError::Unsupported("test table".into()))
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
struct RangeCatalog(Arc<RangeTable>);
impl Catalog for RangeCatalog {
    fn get_table(&self, name: &str) -> Result<Arc<dyn Table>> {
        if name == "events" {
            Ok(self.0.clone())
        } else {
            Err(DbError::UnknownTable(name.into()))
        }
    }
    fn create_table(&self, _: TableDefinition, _: bool) -> Result<()> {
        Err(DbError::Unsupported("test catalog".into()))
    }
    fn drop_table(&self, _: &str, _: bool) -> Result<()> {
        Err(DbError::Unsupported("test catalog".into()))
    }
    fn list_tables(&self) -> Vec<TableDefinition> {
        vec![self.0.definition.clone()]
    }
}
fn setup(pruning: bool) -> (SqlQueryEngine, Arc<RangeTable>) {
    let schema = TableSchema {
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
    };
    let blocks = [vec![1, 2, 3], vec![10, 11, 12], vec![20, 21, 22]]
        .into_iter()
        .map(|ids| {
            let event = ids
                .iter()
                .map(|id| {
                    if id % 2 == 0 {
                        "click".into()
                    } else {
                        "view".into()
                    }
                })
                .collect();
            Block::new(vec![
                Column {
                    name: "id".into(),
                    data: ColumnData::UInt64(ids),
                },
                Column {
                    name: "event".into(),
                    data: ColumnData::String(event),
                },
            ])
            .unwrap()
        })
        .collect();
    let table = Arc::new(RangeTable {
        definition: TableDefinition {
            name: "events".into(),
            schema,
            engine: Engine::Memory,
            order_by: vec!["id".into()],
            settings: TableSettings::default(),
        },
        blocks,
        pruning,
        selected: AtomicUsize::new(0),
    });
    (
        SqlQueryEngine::new(Arc::new(RangeCatalog(table.clone()))),
        table,
    )
}
fn ids(engine: &SqlQueryEngine, sql: &str) -> Vec<u64> {
    let QueryOutput::Rows { stream, .. } = engine.execute(sql).unwrap() else {
        panic!()
    };
    let mut ids = Vec::new();
    for block in stream {
        let block = block.unwrap();
        let Some(ColumnData::UInt64(values)) = block.column("id") else {
            panic!()
        };
        ids.extend_from_slice(values);
    }
    ids
}

#[test]
fn pruning_preserves_query_results_and_skips_unneeded_ranges() {
    let (full, _) = setup(false);
    let (pruned, table) = setup(true);
    for predicate in [
        "id + 1 > 10",
        "id * 2 >= 20 AND id < 21",
        "id >= 10",
        "10 <= id",
        "id = 11",
        "id > 22",
        "id < 1",
        "id > -1",
        "id > 10.5",
        "id = '11'",
        "id >= 10 AND event = 'click'",
        "id < 2 OR id > 20",
        "id < 2 OR event = 'click'",
        "NOT (id >= 10)",
        "id != 11",
    ] {
        let sql = format!("SELECT id FROM events WHERE {predicate}");
        assert_eq!(ids(&pruned, &sql), ids(&full, &sql), "{predicate}");
    }
    assert_eq!(
        ids(&pruned, "SELECT id FROM events WHERE id = 11"),
        vec![11]
    );
    assert_eq!(table.selected.load(Ordering::SeqCst), 1);
    assert_eq!(ids(&pruned, "SELECT id FROM events WHERE id > 22"), vec![]);
    assert_eq!(table.selected.load(Ordering::SeqCst), 0);
    assert_eq!(
        ids(
            &pruned,
            "SELECT id FROM events WHERE id >= 10 AND event = 'click' LIMIT 2"
        ),
        vec![10, 12]
    );
}
