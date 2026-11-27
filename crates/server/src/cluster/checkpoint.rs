//! Снимок хранит текущее состояние таблиц и сохраняет их идентичность.

use crate::{formats, AppState};
use serde::{Deserialize, Serialize};
use types::{Engine, PartData, Result, ScanRequest, TableEntry};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct TableCheckpoint {
    entry: TableEntry,
    data: TableData,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    receipts: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum TableData {
    MergeTree(Vec<PartCheckpoint>),
    Memory(Vec<Vec<u8>>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PartCheckpoint {
    name: String,
    files: Vec<(String, Vec<u8>)>,
    checksum: String,
}

pub(super) fn capture(app: &AppState) -> Result<Vec<TableCheckpoint>> {
    app.catalog
        .table_entries()
        .into_iter()
        .map(|entry| {
            let table = app.catalog.get_table_by_id(&entry.id)?;
            let data = match entry.definition.engine {
                Engine::MergeTree => TableData::MergeTree(
                    table
                        .parts()?
                        .into_iter()
                        .map(|name| {
                            let PartData {
                                name,
                                files,
                                checksum,
                            } = table.export_part(&name)?;
                            Ok(PartCheckpoint {
                                name,
                                files,
                                checksum,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                ),
                Engine::Memory => TableData::Memory(
                    table
                        .scan(&ScanRequest {
                            columns: entry.definition.schema.names(),
                            predicate: None,
                        })?
                        .map(|block| block.map(|block| formats::format_insert_tsv(&block)))
                        .collect::<Result<Vec<_>>>()?,
                ),
            };
            let receipts = table.insert_receipts()?;
            Ok(TableCheckpoint {
                entry,
                data,
                receipts,
            })
        })
        .collect()
}

pub(super) fn restore(app: &AppState, tables: &[TableCheckpoint]) -> Result<()> {
    for TableCheckpoint {
        entry,
        data,
        receipts,
    } in tables
    {
        app.catalog
            .create_table_with_id(entry.definition.clone(), entry.id.clone(), false)?;
        let table = app.catalog.get_table_by_id(&entry.id)?;
        match data {
            TableData::MergeTree(parts) => table.sync_parts_stream(
                &mut parts.iter().map(|part| {
                    Ok(PartData {
                        name: part.name.clone(),
                        files: part.files.clone(),
                        checksum: part.checksum.clone(),
                    })
                }),
                &[],
            )?,
            TableData::Memory(blocks) => {
                for body in blocks {
                    table.insert(formats::parse_input(
                        formats::InputFormat::Tsv,
                        body,
                        &entry.definition.schema,
                    )?)?;
                }
            }
        }
        if !receipts.is_empty() {
            table.restore_insert_receipts(receipts.clone())?;
        }
    }
    Ok(())
}
