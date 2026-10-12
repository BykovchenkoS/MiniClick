//! Реализация MergeTree через общий контракт Table.

use crate::part::{internal, sync_directory, validate_block, write_part};
use crate::repository::PartRepository;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};
use types::{
    Block, BlockStream, DbError, Result, ScanRequest, ScanStats, Table, TableDefinition,
    TableFactory,
};

/// Постоянная таблица; изменения сериализуются, читатели держат снимки parts.
pub struct MergeTreeTable {
    pub(crate) definition: TableDefinition,
    pub(crate) repository: PartRepository,
    pub(crate) write_lock: Mutex<()>,
}

impl MergeTreeTable {
    /// data_dir — каталог одной таблицы, заданный Catalog.
    pub fn open(definition: &TableDefinition, data_dir: &Path) -> Result<Self> {
        Ok(Self {
            definition: definition.clone(),
            repository: PartRepository::open(data_dir, definition)?,
            write_lock: Mutex::new(()),
        })
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MergeTreeFactory;

impl TableFactory for MergeTreeFactory {
    fn open(&self, definition: &TableDefinition, data_dir: &Path) -> Result<Arc<dyn Table>> {
        Ok(Arc::new(MergeTreeTable::open(definition, data_dir)?))
    }
}

impl Table for MergeTreeTable {
    fn definition(&self) -> &TableDefinition {
        &self.definition
    }

    fn insert(&self, block: Block) -> Result<()> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| internal("table write lock poisoned"))?;
        self.repository.check_live()?;
        validate_block(&block, &self.definition)?;
        if block.rows() == 0 {
            return Ok(());
        }
        let id = self.repository.reserve_insert()?;
        let result = (|| {
            write_part(&self.repository.root, &id.name(), &self.definition, &block)?;
            let part = self.repository.open_part(id)?;
            self.repository.replace(vec![(id, part)], &[])
        })();
        if result.is_err() {
            let path = self.repository.root.join(id.name());
            if let Err(error) = fs::remove_dir_all(&path) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(?path, %error, "Не удалось удалить неопубликованный part");
                }
            }
        }
        result
    }

    fn scan(&self, _request: &ScanRequest) -> Result<BlockStream> {
        Err(DbError::Unsupported(
            "MergeTree scan is not implemented yet".into(),
        ))
    }

    fn explain_scan(&self, _request: &ScanRequest) -> Result<ScanStats> {
        Err(DbError::Unsupported(
            "MergeTree explain_scan is not implemented yet".into(),
        ))
    }

    fn flush(&self) -> Result<()> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| internal("table write lock poisoned"))?;
        self.repository.check_live()?;
        sync_directory(&self.repository.root)
    }

    fn optimize(&self) -> Result<()> {
        self.repository.check_live()?;
        Err(DbError::Unsupported(
            "MergeTree optimize is not implemented yet".into(),
        ))
    }

    fn drop_data(&self) -> Result<()> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| internal("table write lock poisoned"))?;
        self.repository.drop_data()
    }

    fn parts(&self) -> Result<Vec<String>> {
        let mut names: Vec<_> = self
            .repository
            .snapshot()?
            .iter()
            .map(|p| {
                p.path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(str::to_owned)
                    .ok_or_else(|| internal("invalid active part path"))
            })
            .collect::<Result<_>>()?;
        names.sort();
        Ok(names)
    }
}

#[cfg(test)]
mod tests;
