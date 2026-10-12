//! Снимки активных parts и атомарное сохранение набора на диске.

use crate::part::{internal, sync_directory, validate_definition, write_file, DataPart};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, RwLock,
};
use types::{Result, TableDefinition};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct PartId {
    pub min: u64,
    pub max: u64,
    pub level: u64,
}

impl PartId {
    pub fn parse(name: &str) -> Result<Self> {
        let fields: Vec<_> = name.split('_').collect();
        if fields.len() != 4 || fields[0] != "all" {
            return Err(internal("invalid part name"));
        }
        let number = |text: &str| -> Result<u64> {
            if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
                return Err(internal("invalid part number"));
            }
            text.parse().map_err(|_| internal("part number overflow"))
        };
        let id = Self {
            min: number(fields[1])?,
            max: number(fields[2])?,
            level: number(fields[3])?,
        };
        if id.min == 0
            || id.min > id.max
            || id.name() != name
            || (id.level == 0 && id.min != id.max)
        {
            return Err(internal("invalid part range"));
        }
        Ok(id)
    }

    pub fn name(self) -> String {
        format!("all_{}_{}_{}", self.min, self.max, self.level)
    }
}

pub(crate) struct DirectoryLease {
    _file: File,
    dropped: AtomicBool,
}

impl DirectoryLease {
    pub fn is_dropped(&self) -> bool {
        self.dropped.load(Ordering::Acquire)
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Manifest {
    version: u32,
    last_insert: u64,
    active: Vec<String>,
    definition: TableDefinition,
}

struct State {
    manifest: Manifest,
    parts: Vec<Arc<DataPart>>,
}

pub(crate) struct PartRepository {
    pub root: PathBuf,
    definition: TableDefinition,
    lease: Arc<DirectoryLease>,
    state: RwLock<State>,
}

impl PartRepository {
    pub fn open(root: &Path, definition: &TableDefinition) -> Result<Self> {
        validate_definition(definition)?;
        fs::create_dir_all(root)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join(".lock"))?;
        file.try_lock().map_err(|e| {
            internal(format!(
                "table directory is already open or cannot be locked: {e}"
            ))
        })?;
        let lease = Arc::new(DirectoryLease {
            _file: file,
            dropped: AtomicBool::new(false),
        });
        let manifest_path = root.join("table.json");
        let existed = manifest_path.exists();
        let mut manifest = if existed {
            serde_json::from_slice::<Manifest>(&fs::read(&manifest_path)?)
                .map_err(|e| internal(format!("invalid table manifest: {e}")))?
        } else {
            Manifest {
                version: 1,
                last_insert: 0,
                active: Vec::new(),
                definition: definition.clone(),
            }
        };
        if manifest.version != 1
            || manifest.definition.schema != definition.schema
            || manifest.definition.order_by != definition.order_by
            || manifest.definition.settings.index_granularity
                != definition.settings.index_granularity
            || manifest.definition.engine != definition.engine
        {
            return Err(internal("manifest differs from table definition"));
        }
        let mut parts = Vec::new();
        let mut ids = Vec::new();
        for name in &manifest.active {
            let id = PartId::parse(name)?;
            if ids
                .iter()
                .any(|previous: &PartId| previous.min <= id.max && id.min <= previous.max)
            {
                return Err(internal("active part ranges overlap"));
            }
            ids.push(id);
            let part_type = fs::symlink_metadata(root.join(name))?.file_type();
            if !part_type.is_dir() || part_type.is_symlink() {
                return Err(internal("active part is not a directory"));
            }
            let mut part = DataPart::open(&root.join(name), definition)?;
            part.lease = Some(lease.clone());
            parts.push(Arc::new(part));
            manifest.last_insert = manifest.last_insert.max(id.max);
        }
        let entries = fs::read_dir(root)?.collect::<std::io::Result<Vec<_>>>()?;
        // Сначала проверяем состояние, затем удаляем только известные временные файлы.
        for entry in &entries {
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| internal("non-UTF-8 part name"))?;
            if name.starts_with("all_") {
                let id = PartId::parse(&name)?;
                if !entry.file_type()?.is_dir() || entry.file_type()?.is_symlink() {
                    return Err(internal("part is not a directory"));
                }
                if !existed {
                    return Err(internal("part directory exists without table manifest"));
                }
                manifest.last_insert = manifest.last_insert.max(id.max);
            }
        }
        persist(root, &manifest)?;
        for entry in entries {
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| internal("non-UTF-8 file name"))?;
            if name.starts_with("tmp_")
                || (name.starts_with("all_") && !manifest.active.contains(&name))
            {
                if entry.file_type()?.is_dir() {
                    fs::remove_dir_all(entry.path())?;
                } else {
                    fs::remove_file(entry.path())?;
                }
            }
        }
        parts.sort_by_key(|p| {
            PartId::parse(p.path.file_name().and_then(|n| n.to_str()).unwrap_or("")).ok()
        });
        Ok(Self {
            root: root.to_owned(),
            definition: definition.clone(),
            lease,
            state: RwLock::new(State { manifest, parts }),
        })
    }

    pub fn check_live(&self) -> Result<()> {
        if self.lease.is_dropped() {
            return Err(internal("table was dropped"));
        }
        Ok(())
    }

    pub fn snapshot(&self) -> Result<Vec<Arc<DataPart>>> {
        let state = self
            .state
            .read()
            .map_err(|_| internal("repository lock poisoned"))?;
        self.check_live()?;
        Ok(state.parts.clone())
    }

    pub fn reserve_insert(&self) -> Result<PartId> {
        let mut state = self
            .state
            .write()
            .map_err(|_| internal("repository lock poisoned"))?;
        self.check_live()?;
        let mut manifest = state.manifest.clone();
        manifest.last_insert = manifest
            .last_insert
            .checked_add(1)
            .ok_or_else(|| internal("insert counter overflow"))?;
        persist(&self.root, &manifest)?;
        let number = manifest.last_insert;
        state.manifest = manifest;
        Ok(PartId {
            min: number,
            max: number,
            level: 0,
        })
    }

    /// Единственная точка публикации нового набора для INSERT и merge.
    pub fn replace(&self, add: Vec<(PartId, DataPart)>, remove: &[PartId]) -> Result<()> {
        let mut state = self
            .state
            .write()
            .map_err(|_| internal("repository lock poisoned"))?;
        self.check_live()?;
        let mut manifest = state.manifest.clone();
        let removed: Vec<_> = remove.iter().map(|id| id.name()).collect();
        for name in &removed {
            if !manifest.active.contains(name) {
                return Err(internal("removed part is not active"));
            }
        }
        manifest.active.retain(|name| !removed.contains(name));
        let mut additions = Vec::new();
        for (id, mut part) in add {
            let name = id.name();
            if PartId::parse(&name)? != id || part.path != self.root.join(&name) {
                return Err(internal("added part path differs from id"));
            }
            if manifest.active.contains(&name) || removed.contains(&name) {
                return Err(internal("part name is reused"));
            }
            for active in &manifest.active {
                let other = PartId::parse(active)?;
                if other.min <= id.max && id.min <= other.max {
                    return Err(internal("active ranges overlap"));
                }
            }
            manifest.last_insert = manifest.last_insert.max(id.max);
            manifest.active.push(name);
            part.lease = Some(self.lease.clone());
            additions.push(Arc::new(part));
        }
        manifest.active.sort();
        persist(&self.root, &manifest)?;
        // После rename ошибок больше нет: память обязана соответствовать диску.
        state.parts.retain(|part| {
            let name = part.path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if removed.iter().any(|n| n == name) {
                part.retire();
                false
            } else {
                true
            }
        });
        state.parts.extend(additions);
        state.parts.sort_by_key(|p| {
            PartId::parse(p.path.file_name().and_then(|n| n.to_str()).unwrap_or("")).ok()
        });
        state.manifest = manifest;
        Ok(())
    }

    pub fn open_part(&self, id: PartId) -> Result<DataPart> {
        DataPart::open(&self.root.join(id.name()), &self.definition)
    }

    pub fn drop_data(&self) -> Result<()> {
        let mut state = self
            .state
            .write()
            .map_err(|_| internal("repository lock poisoned"))?;
        self.check_live()?;
        // Помечаем весь старый жизненный цикл до удаления каталога.
        self.lease.dropped.store(true, Ordering::Release);
        fs::remove_dir_all(&self.root)?;
        state.parts.clear();
        Ok(())
    }
}

fn persist(root: &Path, manifest: &Manifest) -> Result<()> {
    let bytes =
        serde_json::to_vec(manifest).map_err(|e| internal(format!("manifest JSON: {e}")))?;
    let temporary = root.join("tmp_table.json");
    write_file(&temporary, &bytes)?;
    fs::rename(&temporary, root.join("table.json"))?;
    // Набор уже опубликован; ошибка fsync не должна выглядеть как откат INSERT.
    if let Err(error) = sync_directory(root) {
        tracing::error!(%error, "Набор parts опубликован, но fsync каталога не удался");
    }
    Ok(())
}

#[cfg(test)]
mod tests;
