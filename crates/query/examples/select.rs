use query::SqlQueryEngine;
use std::sync::Arc;
use types::testing::MemCatalog;
use types::*;

fn main() -> Result<()> {
    let catalog = Arc::new(MemCatalog::new());
    catalog.create_table(
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
        },
        false,
    )?;
    catalog.get_table("events")?.insert(Block::new(vec![
        Column {
            name: "id".into(),
            data: ColumnData::UInt64(vec![1, 2, 3]),
        },
        Column {
            name: "event".into(),
            data: ColumnData::String(vec!["click".into(), "view".into(), "click".into()]),
        },
    ])?)?;
    let engine = SqlQueryEngine::new(catalog);
    let sql = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "SELECT event, id AS user_id FROM events".into());
    println!("SQL: {sql}");
    if let QueryOutput::Rows { header, stream } = engine.execute(&sql)? {
        println!(
            "{}",
            header
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>()
                .join("\t")
        );
        for block in stream {
            let block = block?;
            for row in 0..block.rows() {
                println!(
                    "{}",
                    block
                        .columns()
                        .iter()
                        .map(|c| c.data.get(row).to_string())
                        .collect::<Vec<_>>()
                        .join("\t")
                );
            }
        }
    }
    Ok(())
}
