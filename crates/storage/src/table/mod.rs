//! Реализация MergeTree через общий контракт Table.

use crate::merge;
use crate::part::{internal, part_checksum, sync_directory, validate_block, write_part};
use crate::repository::{PartId, PartRepository};
use crate::scan::{self, ScanStream};
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};
use types::{
    Block, BlockStream, PartData, Result, ScanRequest, ScanStats, Table, TableDefinition,
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

/// Фабрика MergeTree для регистрации в Catalog; data_dir уже задаёт каталог таблицы.
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

    fn scan(&self, request: &ScanRequest) -> Result<BlockStream> {
        let plan = scan::plan(&self.definition, self.repository.snapshot()?, request)?;
        let stats = plan.stats;
        tracing::info!(
            parts_total = stats.parts_total,
            parts_selected = stats.parts_selected,
            granules_total = stats.granules_total,
            granules_selected = stats.granules_selected,
            rows_total = stats.rows_total,
            rows_selected = stats.rows_selected,
            "Выбраны parts и гранулы для сканирования"
        );
        Ok(ScanStream::into_stream(plan, request.columns.clone()))
    }

    fn explain_scan(&self, request: &ScanRequest) -> Result<ScanStats> {
        Ok(scan::plan(&self.definition, self.repository.snapshot()?, request)?.stats)
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
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| internal("table write lock poisoned"))?;
        self.repository.check_live()?;
        loop {
            let selected = merge::select(&self.repository.snapshot()?);
            if selected.is_empty() {
                return Ok(());
            }
            let (id, remove) = merge::merged_id(&selected)?;
            let result = (|| {
                merge::write_merge(&self.repository.root, id, &self.definition, &selected)?;
                self.repository
                    .replace(vec![(id, self.repository.open_part(id)?)], &remove)
            })();
            if result.is_err() {
                let path = self.repository.root.join(id.name());
                if let Err(error) = fs::remove_dir_all(&path) {
                    if error.kind() != std::io::ErrorKind::NotFound {
                        tracing::warn!(?path, %error, "Не удалось удалить неопубликованный merge");
                    }
                }
            }
            result?;
        }
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

    fn export_part(&self, name: &str) -> Result<PartData> {
        PartId::parse(name)?;
        // Снимок удерживает Arc<DataPart>: параллельный OPTIMIZE не удалит файлы,
        // пока они читаются.
        let part = self
            .repository
            .snapshot()?
            .into_iter()
            .find(|part| part.path == self.repository.root.join(name))
            .ok_or_else(|| internal("part is not active"))?;
        let mut files = Vec::with_capacity(2 + self.definition.schema.columns.len() * 2);
        files.push((
            "part.json".to_owned(),
            fs::read(part.path.join("part.json"))?,
        ));
        files.push((
            "primary.idx".to_owned(),
            fs::read(part.path.join("primary.idx"))?,
        ));
        for column in &self.definition.schema.columns {
            let marks = format!("{}.mrk", column.name);
            let data = format!("{}.bin", column.name);
            files.push((marks.clone(), fs::read(part.path.join(marks))?));
            files.push((data.clone(), fs::read(part.path.join(data))?));
        }
        let checksum = part_checksum(&files);
        Ok(PartData {
            name: name.to_owned(),
            files,
            checksum,
        })
    }

    fn sync_parts(&self, add: Vec<PartData>, remove: &[String]) -> Result<()> {
        self.sync_parts_stream(&mut add.into_iter().map(Ok), remove)
    }

    fn sync_parts_stream(
        &self,
        add: &mut dyn Iterator<Item = Result<PartData>>,
        remove: &[String],
    ) -> Result<()> {
        let _guard = self
            .write_lock
            .lock()
            .map_err(|_| internal("table write lock poisoned"))?;
        self.repository.import(add, remove)
    }
}

#[cfg(test)]
mod tests;
