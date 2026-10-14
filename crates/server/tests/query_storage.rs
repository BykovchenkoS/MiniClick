//! Проверка Query с дисковым MergeTree без HTTP и рабочего Catalog.

use query::SqlQueryEngine;
use std::sync::Arc;
use storage::MergeTreeTable;
use types::{
    Block, Catalog, Codec, Column, ColumnData, ColumnDef, DataType, DbError, Engine, QueryEngine,
    QueryOutput, Result, Table, TableDefinition, TableSchema, TableSettings, Value,
};

// Каталог только выдаёт заранее открытую таблицу; DDL каталога здесь не проверяется.
struct TestCatalog(Arc<dyn Table>);

impl Catalog for TestCatalog {
    fn create_table(&self, _: TableDefinition, _: bool) -> Result<()> {
        Err(DbError::Unsupported(
            "test catalog does not create tables".into(),
        ))
    }
    fn drop_table(&self, _: &str, _: bool) -> Result<()> {
        Err(DbError::Unsupported(
            "test catalog does not drop tables".into(),
        ))
    }
    fn get_table(&self, name: &str) -> Result<Arc<dyn Table>> {
        if name == self.0.definition().name {
            Ok(self.0.clone())
        } else {
            Err(DbError::UnknownTable(name.into()))
        }
    }
    fn list_tables(&self) -> Vec<TableDefinition> {
        vec![self.0.definition().clone()]
    }
}

fn definition(codec: Codec) -> TableDefinition {
    TableDefinition {
        name: "events".into(),
        schema: TableSchema {
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    data_type: DataType::Int64,
                },
                ColumnDef {
                    name: "event".into(),
                    data_type: DataType::String,
                },
            ],
        },
        engine: Engine::MergeTree,
        order_by: vec!["id".into()],
        settings: TableSettings {
            index_granularity: 2,
            codec,
        },
    }
}

fn block(ids: &[i64], events: &[&str]) -> Block {
    Block::new(vec![
        Column {
            name: "id".into(),
            data: ColumnData::Int64(ids.to_vec()),
        },
        Column {
            name: "event".into(),
            data: ColumnData::String(events.iter().map(|s| (*s).into()).collect()),
        },
    ])
    .unwrap()
}

fn seed(table: &dyn Table) {
    table
        .insert(block(&[6, 1, 4, 2, 5, 3], &["c", "a", "b", "a", "c", "b"]))
        .unwrap();
    table
        .insert(block(&[104, 101, 103, 102], &["b", "a", "b", "a"]))
        .unwrap();
}

fn engine(table: Arc<dyn Table>) -> SqlQueryEngine {
    SqlQueryEngine::new(Arc::new(TestCatalog(table)))
}

fn rows(engine: &SqlQueryEngine, sql: &str) -> (Vec<ColumnDef>, Vec<Vec<Value>>) {
    let QueryOutput::Rows { header, stream } = engine.execute(sql).unwrap() else {
        panic!("expected rows for {sql}");
    };
    let mut rows = Vec::new();
    for block in stream {
        let block = block.unwrap();
        assert_eq!(block.header(), header);
        for row in 0..block.rows() {
            rows.push(block.columns().iter().map(|c| c.data.get(row)).collect());
        }
    }
    (header, rows)
}

fn explain(engine: &SqlQueryEngine, sql: &str) -> String {
    rows(engine, sql)
        .1
        .into_iter()
        .map(|r| match &r[0] {
            Value::String(line) => line.clone(),
            _ => panic!("EXPLAIN must return strings"),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn queries_preserve_results_across_optimize_and_reopen_for_all_codecs() {
    for codec in [Codec::None, Codec::Lz4, Codec::Zstd] {
        let root = tempfile::tempdir().unwrap();
        let def = definition(codec);
        let table = Arc::new(MergeTreeTable::open(&def, root.path()).unwrap());
        seed(table.as_ref());
        let query = engine(table.clone());
        assert_eq!(table.parts().unwrap().len(), 2);
        assert_queries(&query);
        assert!(matches!(
            query.execute("OPTIMIZE TABLE events").unwrap(),
            QueryOutput::Ok
        ));
        assert_eq!(table.parts().unwrap().len(), 1);
        assert_queries(&query);
        table.flush().unwrap();
        // Убираем все ссылки, чтобы повторное открытие действительно читало диск.
        drop(query);
        drop(table);
        let reopened = Arc::new(MergeTreeTable::open(&def, root.path()).unwrap());
        assert_eq!(reopened.parts().unwrap().len(), 1);
        assert_queries(&engine(reopened));
    }
}

fn assert_queries(query: &SqlQueryEngine) {
    assert_eq!(
        rows(query, "SELECT count() AS n FROM events").1,
        vec![vec![Value::UInt64(10)]]
    );
    assert_eq!(
        rows(query, "SELECT id FROM events ORDER BY id").1,
        [1, 2, 3, 4, 5, 6, 101, 102, 103, 104].map(|id| vec![Value::Int64(id)]),
    );
    let (header, result) = rows(
        query,
        "SELECT event FROM events WHERE id > 101 ORDER BY id DESC LIMIT 2",
    );
    assert_eq!(
        header,
        vec![ColumnDef {
            name: "event".into(),
            data_type: DataType::String
        }]
    );
    assert_eq!(
        result,
        vec![
            vec![Value::String("b".into())],
            vec![Value::String("b".into())]
        ]
    );
    assert_eq!(
        rows(
            query,
            "SELECT id * 2 AS doubled FROM events WHERE event = 'a' ORDER BY id DESC LIMIT 2"
        )
        .1,
        vec![vec![Value::Int64(204)], vec![Value::Int64(202)]],
    );
    assert_eq!(
        rows(query, "SELECT event, count() AS n, sum(id) AS total FROM events WHERE id >= 4 GROUP BY event ORDER BY n DESC, event ASC LIMIT 2").1,
        vec![
            vec![Value::String("b".into()), Value::UInt64(3), Value::Int64(211)],
            vec![Value::String("a".into()), Value::UInt64(2), Value::Int64(203)],
        ],
    );
}

#[test]
fn explain_and_query_prune_parts_and_granules_but_filter_exact_rows() {
    let root = tempfile::tempdir().unwrap();
    let table = Arc::new(MergeTreeTable::open(&definition(Codec::None), root.path()).unwrap());
    seed(table.as_ref());
    let query = engine(table.clone());
    // Последняя гранула содержит 103 и 104: Storage вернёт обе строки, Query — одну.
    let plan = explain(
        &query,
        "EXPLAIN SELECT id, event FROM events WHERE id > 103",
    );
    assert!(
        plan.contains("parts: 1 / 2, granules: 1 / 5, rows: 2 / 10"),
        "{plan}"
    );
    assert_eq!(
        rows(&query, "SELECT id, event FROM events WHERE id > 103").1,
        vec![vec![Value::Int64(104), Value::String("b".into())]]
    );
    assert_eq!(
        rows(&query, "SELECT count() FROM events WHERE id > 103").1,
        vec![vec![Value::UInt64(1)]]
    );

    // Повреждаем колонку отсечённого part: выборочный SQL не должен её читать.
    let first = table.parts().unwrap().remove(0);
    std::fs::write(root.path().join(first).join("event.bin"), b"broken").unwrap();
    assert_eq!(
        rows(&query, "SELECT id, event FROM events WHERE id > 103").1,
        vec![vec![Value::Int64(104), Value::String("b".into())]]
    );
    assert_eq!(
        explain(
            &query,
            "EXPLAIN SELECT id, event FROM events WHERE id > 103"
        ),
        plan
    );
    let QueryOutput::Rows { stream, .. } = query.execute("SELECT event FROM events").unwrap()
    else {
        panic!("expected rows")
    };
    assert!(stream.collect::<Result<Vec<_>>>().is_err());
    // count() не запрашивает данные колонок даже при полном чтении таблицы.
    assert_eq!(
        rows(&query, "SELECT count() FROM events").1,
        vec![vec![Value::UInt64(10)]]
    );
}

#[test]
fn empty_disk_table_keeps_header_and_validates_queries() {
    let root = tempfile::tempdir().unwrap();
    let table = Arc::new(MergeTreeTable::open(&definition(Codec::None), root.path()).unwrap());
    let query = engine(table);
    let (header, result) = rows(
        &query,
        "SELECT event FROM events WHERE id > 0 ORDER BY id LIMIT 2",
    );
    assert_eq!(
        header,
        vec![ColumnDef {
            name: "event".into(),
            data_type: DataType::String
        }]
    );
    assert!(result.is_empty());
    assert_eq!(
        rows(&query, "SELECT count() FROM events").1,
        vec![vec![Value::UInt64(0)]]
    );
    assert!(matches!(
        query.execute("SELECT missing FROM events LIMIT 0"),
        Err(DbError::UnknownColumn(_))
    ));
    assert!(matches!(
        query.execute("SELECT id FROM absent"),
        Err(DbError::UnknownTable(_))
    ));
}
