//! Простые реализации общих интерфейсов, чтобы треки не ждали друг друга.
//! MemTable остаётся и после - это движок Engine::Memory.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

use crate::block::Block;
use crate::error::{DbError, Result};
use crate::interfaces::*;
use crate::schema::TableDefinition;

/// Таблица в памяти: хранит вставленные блоки как есть. predicate игнорирует -
/// это разрешено контрактом ScanRequest.
pub struct MemTable {
    definition: TableDefinition,
    blocks: RwLock<Vec<Block>>,
}

impl MemTable {
    pub fn new(definition: TableDefinition) -> MemTable {
        MemTable {
            definition,
            blocks: RwLock::new(Vec::new()),
        }
    }
}

impl Table for MemTable {
    fn definition(&self) -> &TableDefinition {
        &self.definition
    }

    fn insert(&self, block: Block) -> Result<()> {
        if block.header() != self.definition.schema.columns {
            return Err(DbError::TypeMismatch(format!(
                "block columns do not match table {}",
                self.definition.name
            )));
        }
        self.blocks.write().unwrap().push(block);
        Ok(())
    }

    fn scan(&self, request: &ScanRequest) -> Result<BlockStream> {
        let blocks = self.blocks.read().unwrap();
        let out: Vec<Result<Block>> = blocks.iter().map(|b| b.select(&request.columns)).collect();
        Ok(Box::new(out.into_iter()))
    }

    /// Каждый вставленный блок считается part, гранул нет, отсечения нет
    fn explain_scan(&self, _request: &ScanRequest) -> Result<ScanStats> {
        let blocks = self.blocks.read().unwrap();
        let rows: u64 = blocks.iter().map(|b| b.rows() as u64).sum();
        Ok(ScanStats {
            parts_total: blocks.len(),
            parts_selected: blocks.len(),
            granules_total: 0,
            granules_selected: 0,
            rows_total: rows,
            rows_selected: rows,
        })
    }

    fn flush(&self) -> Result<()> {
        Ok(())
    }

    fn optimize(&self) -> Result<()> {
        let mut blocks = self.blocks.write().unwrap();
        if blocks.len() > 1 {
            let merged = Block::concat(&blocks)?;
            *blocks = vec![merged];
        }
        Ok(())
    }

    fn drop_data(&self) -> Result<()> {
        self.blocks.write().unwrap().clear();
        Ok(())
    }
}

pub struct MemTableFactory;

impl TableFactory for MemTableFactory {
    fn open(&self, definition: &TableDefinition, _data_dir: &Path) -> Result<Arc<dyn Table>> {
        Ok(Arc::new(MemTable::new(definition.clone())))
    }
}

/// Каталог в памяти: любая таблица создаётся как MemTable, независимо от engine
#[derive(Default)]
pub struct MemCatalog {
    tables: RwLock<BTreeMap<String, Arc<dyn Table>>>,
}

impl MemCatalog {
    pub fn new() -> MemCatalog {
        MemCatalog::default()
    }
}

impl Catalog for MemCatalog {
    fn create_table(&self, definition: TableDefinition, if_not_exists: bool) -> Result<()> {
        definition.validate()?;
        let mut tables = self.tables.write().unwrap();
        if tables.contains_key(&definition.name) {
            return if if_not_exists {
                Ok(())
            } else {
                Err(DbError::TableExists(definition.name))
            };
        }
        tables.insert(definition.name.clone(), Arc::new(MemTable::new(definition)));
        Ok(())
    }

    fn drop_table(&self, name: &str, if_exists: bool) -> Result<()> {
        match self.tables.write().unwrap().remove(name) {
            Some(t) => t.drop_data(),
            None if if_exists => Ok(()),
            None => Err(DbError::UnknownTable(name.to_string())),
        }
    }

    fn get_table(&self, name: &str) -> Result<Arc<dyn Table>> {
        self.tables
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| DbError::UnknownTable(name.to_string()))
    }

    fn list_tables(&self) -> Vec<TableDefinition> {
        self.tables
            .read()
            .unwrap()
            .values()
            .map(|t| t.definition().clone())
            .collect()
    }
}

/// Лидер «по сети» без сети: ходит в каталог другой ноды в том же процессе
/// Нужен, чтобы тестировать репликацию двумя каталогами в одном тесте.
pub struct LocalPeer {
    pub catalog: Arc<dyn Catalog>,
}

impl Peer for LocalPeer {
    fn ping(&self) -> Result<()> {
        Ok(())
    }

    fn list_tables(&self) -> Result<Vec<TableDefinition>> {
        Ok(self.catalog.list_tables())
    }

    fn list_parts(&self, table: &str) -> Result<Vec<String>> {
        self.catalog.get_table(table)?.parts()
    }

    fn fetch_part(&self, table: &str, part: &str) -> Result<PartData> {
        self.catalog.get_table(table)?.export_part(part)
    }
}

/// Заглушка движка для сервера: понимает только `SELECT * FROM <table>`
pub struct StubQueryEngine {
    pub catalog: Arc<dyn Catalog>,
}

impl QueryEngine for StubQueryEngine {
    fn execute(&self, sql: &str) -> Result<QueryOutput> {
        let words: Vec<&str> = sql.split_whitespace().collect();
        let table = match words.as_slice() {
            [s, "*", f, t]
                if s.eq_ignore_ascii_case("select") && f.eq_ignore_ascii_case("from") =>
            {
                t.trim_end_matches(';')
            }
            _ => {
                return Err(DbError::Unsupported(
                    "stub engine: only SELECT * FROM t".into(),
                ))
            }
        };
        let table = self.catalog.get_table(table)?;
        let header = table.definition().schema.columns.clone();
        let request = ScanRequest {
            columns: table.definition().schema.names(),
            predicate: None,
        };
        Ok(QueryOutput::Rows {
            header,
            stream: table.scan(&request)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::Column;
    use crate::column::ColumnData;
    use crate::schema::{ColumnDef, Engine, TableSchema, TableSettings};
    use crate::value::DataType;

    #[test]
    fn create_insert_select() {
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new());
        catalog
            .create_table(
                TableDefinition {
                    name: "t".into(),
                    schema: TableSchema {
                        columns: vec![ColumnDef {
                            name: "id".into(),
                            data_type: DataType::Int64,
                        }],
                    },
                    engine: Engine::Memory,
                    order_by: vec![],
                    settings: TableSettings::default(),
                },
                false,
            )
            .unwrap();
        let block = Block::new(vec![Column {
            name: "id".into(),
            data: ColumnData::Int64(vec![1, 2]),
        }])
        .unwrap();
        catalog
            .get_table("t")
            .unwrap()
            .insert(block.clone())
            .unwrap();

        let engine = StubQueryEngine {
            catalog: catalog.clone(),
        };
        let QueryOutput::Rows { stream, .. } = engine.execute("select * from t").unwrap() else {
            panic!("expected rows")
        };
        let blocks: Vec<Block> = stream.collect::<Result<_>>().unwrap();
        assert_eq!(blocks, vec![block]);

        let stats = catalog
            .get_table("t")
            .unwrap()
            .explain_scan(&ScanRequest::default())
            .unwrap();
        assert_eq!((stats.parts_selected, stats.rows_selected), (1, 2));
    }
}
