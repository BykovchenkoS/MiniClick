use query::SqlQueryEngine;
use std::sync::Arc;
use types::testing::MemCatalog;
use types::*;

fn run(engine: &SqlQueryEngine, sql: &str) -> Result<()> {
    println!("SQL: {sql}");
    match engine.execute(sql)? {
        QueryOutput::Ok => println!("OK"),
        QueryOutput::Rows { header, stream } => {
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
    }
    Ok(())
}
fn main() -> Result<()> {
    let catalog = Arc::new(MemCatalog::new());
    let engine = SqlQueryEngine::new(catalog.clone());
    run(
        &engine,
        "CREATE TABLE events (id UInt64, event String) ENGINE = Memory",
    )?;
    // SQL INSERT не входит в Query: здесь напрямую вызывается Table::insert.
    for (id, event) in [(1, "click"), (2, "view"), (3, "click")] {
        catalog.get_table("events")?.insert(Block::new(vec![
            Column {
                name: "id".into(),
                data: ColumnData::UInt64(vec![id]),
            },
            Column {
                name: "event".into(),
                data: ColumnData::String(vec![event.into()]),
            },
        ])?)?;
    }
    run(
        &engine,
        "SELECT id FROM events WHERE event = 'click' LIMIT 2",
    )?;
    let stats = catalog
        .get_table("events")?
        .explain_scan(&ScanRequest::default())?;
    println!("Memory blocks before OPTIMIZE: {}", stats.parts_total);
    run(&engine, "OPTIMIZE TABLE events")?;
    let stats = catalog
        .get_table("events")?
        .explain_scan(&ScanRequest::default())?;
    println!("Memory blocks after OPTIMIZE: {}", stats.parts_total);
    run(&engine, "SELECT id FROM events")?;
    run(&engine, "DROP TABLE events")?;
    run(&engine, "DROP TABLE IF EXISTS events")?;
    println!("Tables remaining: {}", catalog.list_tables().len());
    Ok(())
}
