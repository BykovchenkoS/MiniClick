//! Постоянный каталог: описания таблиц и фабрики движков, без зависимости от Storage.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use types::{Catalog, DbError, Engine, Result, Table, TableDefinition, TableFactory};
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredTable {
    id: String,
    definition: TableDefinition,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u32,
    tables: Vec<StoredTable>,
}

struct Entry {
    stored: StoredTable,
    table: Arc<dyn Table>,
}

/// Фабрики передаёт сервер; каждый жизненный цикл таблицы имеет отдельный каталог.
pub struct DiskCatalog {
    root: PathBuf,
    factories: Vec<(Engine, Arc<dyn TableFactory>)>,
    tables: RwLock<BTreeMap<String, Entry>>,
    _lock: File,
}

fn internal(message: impl Into<String>) -> DbError {
    DbError::Internal(message.into())
}

fn require_directory(path: &Path) -> Result<()> {
    let meta = fs::symlink_metadata(path)?;
    if !meta.is_dir() || meta.file_type().is_symlink() {
        return Err(internal(format!(
            "not a regular directory: {}",
            path.display()
        )));
    }
    Ok(())
}

impl DiskCatalog {
    /// Открывает зарегистрированные таблицы. Повреждённые метаданные не восстанавливаются догадками.
    pub fn open(root: &Path, factories: Vec<(Engine, Arc<dyn TableFactory>)>) -> Result<Self> {
        for (i, (engine, _)) in factories.iter().enumerate() {
            if factories[..i].iter().any(|(other, _)| other == engine) {
                return Err(internal("duplicate engine factory"));
            }
        }
        fs::create_dir_all(root)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join(".catalog.lock"))?;
        lock.try_lock()
            .map_err(|error| internal(format!("catalog is already open: {error}")))?;
        let data = root.join("tables");
        fs::create_dir_all(&data)?;
        require_directory(&data)?;
        let manifest_path = root.join("catalog.json");
        let manifest = if manifest_path.exists() {
            serde_json::from_slice::<Manifest>(&fs::read(&manifest_path)?)
                .map_err(|error| internal(format!("invalid catalog JSON: {error}")))?
        } else {
            if fs::read_dir(&data)?.next().is_some() {
                return Err(internal("table directories exist without catalog.json"));
            }
            Manifest {
                version: 1,
                tables: Vec::new(),
            }
        };
        if manifest.version != 1 {
            return Err(internal("unsupported catalog version"));
        }
        let mut names = BTreeSet::new();
        let mut ids = BTreeSet::new();
        // Проверяем весь список до открытия фабрик: битая запись не запускает очистку.
        for stored in &manifest.tables {
            let id =
                Uuid::parse_str(&stored.id).map_err(|_| internal("invalid table directory id"))?;
            if id.to_string() != stored.id || !ids.insert(stored.id.clone()) {
                return Err(internal("duplicate or noncanonical table directory id"));
            }
            if stored.definition.name.is_empty() || !names.insert(stored.definition.name.clone()) {
                return Err(internal("empty or duplicate table name"));
            }
            stored
                .definition
                .validate()
                .map_err(|error| internal(format!("invalid table definition: {error}")))?;
            let path = data.join(&stored.id);
            require_directory(&path)?;
            if stored.definition.engine == Engine::MergeTree && !path.join("table.json").is_file() {
                return Err(internal("registered MergeTree manifest is missing"));
            }
        }
        let mut tables = BTreeMap::new();
        for stored in manifest.tables {
            let factory = factories
                .iter()
                .find(|(engine, _)| *engine == stored.definition.engine)
                .ok_or_else(|| internal("missing factory for registered engine"))?;
            let table = factory.1.open(&stored.definition, &data.join(&stored.id))?;
            tables.insert(stored.definition.name.clone(), Entry { stored, table });
        }
        let catalog = Self {
            root: root.to_owned(),
            factories,
            tables: RwLock::new(tables),
            _lock: lock,
        };
        if !manifest_path.exists() {
            catalog.persist(&[])?;
        }
        catalog.cleanup(&ids)?;
        Ok(catalog)
    }

    fn persist(&self, tables: &[StoredTable]) -> Result<()> {
        let bytes = serde_json::to_vec_pretty(&Manifest {
            version: 1,
            tables: tables.to_vec(),
        })
        .map_err(|error| internal(format!("catalog serialization: {error}")))?;
        let temporary = self.root.join("tmp_catalog.json");
        let mut file = File::create(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, self.root.join("catalog.json"))?;
        // После rename изменение опубликовано: ошибка fsync уже не означает откат.
        if let Err(error) = File::open(&self.root).and_then(|file| file.sync_all()) {
            tracing::error!(%error, "Каталог опубликован, но fsync не удался");
        }
        Ok(())
    }

    fn cleanup(&self, active: &BTreeSet<String>) -> Result<()> {
        let data = self.root.join("tables");
        for entry in fs::read_dir(&data)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if Uuid::parse_str(&name).is_ok_and(|id| id.to_string() == name)
                && !active.contains(&name)
            {
                require_directory(&entry.path())?;
                fs::remove_dir_all(entry.path())?;
            }
        }
        match fs::remove_file(self.root.join("tmp_catalog.json")) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

impl Catalog for DiskCatalog {
    fn create_table(&self, definition: TableDefinition, if_not_exists: bool) -> Result<()> {
        definition.validate()?;
        if definition.name.is_empty() {
            return Err(DbError::Syntax("empty table name".into()));
        }
        let mut tables = self
            .tables
            .write()
            .map_err(|_| internal("catalog lock poisoned"))?;
        if tables.contains_key(&definition.name) {
            return if if_not_exists {
                Ok(())
            } else {
                Err(DbError::TableExists(definition.name))
            };
        }
        let factory = self
            .factories
            .iter()
            .find(|(engine, _)| *engine == definition.engine)
            .ok_or_else(|| DbError::Unsupported("engine factory is not registered".into()))?;
        let stored = StoredTable {
            id: Uuid::new_v4().to_string(),
            definition,
        };
        let path = self.root.join("tables").join(&stored.id);
        fs::create_dir(&path)?;
        let result = (|| {
            let table = factory.1.open(&stored.definition, &path)?;
            File::open(self.root.join("tables"))?.sync_all()?;
            let mut entries: Vec<_> = tables.values().map(|entry| entry.stored.clone()).collect();
            entries.push(stored.clone());
            entries.sort_by(|left, right| left.definition.name.cmp(&right.definition.name));
            self.persist(&entries)?;
            Ok(table)
        })();
        match result {
            Ok(table) => {
                tables.insert(stored.definition.name.clone(), Entry { stored, table });
                Ok(())
            }
            Err(error) => {
                if let Err(cleanup) = fs::remove_dir_all(&path) {
                    tracing::warn!(%cleanup, ?path, "Не удалось удалить неопубликованную таблицу");
                }
                Err(error)
            }
        }
    }

    fn drop_table(&self, name: &str, if_exists: bool) -> Result<()> {
        let mut tables = self
            .tables
            .write()
            .map_err(|_| internal("catalog lock poisoned"))?;
        if !tables.contains_key(name) {
            return if if_exists {
                Ok(())
            } else {
                Err(DbError::UnknownTable(name.into()))
            };
        }
        let entries: Vec<_> = tables
            .iter()
            .filter(|(key, _)| key.as_str() != name)
            .map(|(_, entry)| entry.stored.clone())
            .collect();
        self.persist(&entries)?;
        if let Some(entry) = tables.remove(name) {
            if let Err(error) = entry.table.drop_data() {
                tracing::error!(%error, table = name, "DROP опубликован, очистка данных отложена до рестарта");
            }
            let path = self.root.join("tables").join(entry.stored.id);
            if path.exists() {
                if let Err(error) = fs::remove_dir_all(&path) {
                    tracing::warn!(%error, ?path, "Остатки DROP будут удалены при рестарте");
                }
            }
        }
        Ok(())
    }

    fn get_table(&self, name: &str) -> Result<Arc<dyn Table>> {
        self.tables
            .read()
            .map_err(|_| internal("catalog lock poisoned"))?
            .get(name)
            .map(|entry| entry.table.clone())
            .ok_or_else(|| DbError::UnknownTable(name.into()))
    }

    fn list_tables(&self) -> Vec<TableDefinition> {
        // Трейт не позволяет вернуть ошибку; опубликованные записи остаются пригодны для чтения.
        self.tables
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .map(|entry| entry.stored.definition.clone())
            .collect()
    }
}
