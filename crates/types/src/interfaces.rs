//! Интерфейсы на границах между треками. Только они общие
//! внутренние интерфейсы каждой подсистемы живут в её крейте

use std::fmt;
use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::block::Block;
use crate::error::{DbError, Result};
use crate::expr::Expr;
use crate::schema::{ColumnDef, TableDefinition};

/// Поток блоков. Синхронный: ядро не async, сервер зовёт его через spawn_blocking
pub type BlockStream = Box<dyn Iterator<Item = Result<Block>> + Send>;

/// Что Query просит у Storage
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ScanRequest {
    /// Какие колонки вернуть, в этом порядке. Может быть пустым (count())
    pub columns: Vec<String>,
    /// Подсказка для отсечения parts и гранул. Storage может вернуть лишние
    /// строки (проверка грубая, по min/max и индексу), но не может потерять
    /// строку, которая подходит под условие. Точную фильтрацию всегда делает
    /// Query после scan
    pub predicate: Option<Expr>,
}

/// Сколько данных выбрал бы scan после отсечения. Для EXPLAIN и логов
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ScanStats {
    pub parts_total: usize,
    pub parts_selected: usize,
    pub granules_total: usize,
    pub granules_selected: usize,
    pub rows_total: u64,
    /// Строк в выбранных гранулах (до фильтрации в Query)
    pub rows_selected: u64,
}

/// Таблица. Реализации: MemTable (types::testing) и MergeTree (storage).
/// Все методы берут &self: одну таблицу читают и пишут из нескольких потоков.
pub trait Table: Send + Sync {
    fn definition(&self) -> &TableDefinition;

    /// Колонки блока совпадают со схемой по именам и типам, порядок как в схеме.
    /// Проверку и приведение делает тот, кто зовёт (сервер при INSERT).
    fn insert(&self, block: Block) -> Result<()>;

    fn scan(&self, request: &ScanRequest) -> Result<BlockStream>;

    /// Что прочитал бы scan с этим запросом: только отсечение, данные не читаются.
    /// Должен использовать то же отсечение, что и scan, чтобы EXPLAIN не расходился
    /// с реальным чтением.
    fn explain_scan(&self, request: &ScanRequest) -> Result<ScanStats>;

    /// Сбросить буферы на диск. Если буферов нет — ничего не делать.
    fn flush(&self) -> Result<()>;

    /// OPTIMIZE TABLE: слить parts. Для Memory — ничего не делать.
    fn optimize(&self) -> Result<()>;

    /// DROP TABLE: удалить данные. После вызова таблицей не пользуются.
    fn drop_data(&self) -> Result<()>;

    // --- Репликация. Реализует только MergeTree; MemTable не реплицируется. ---

    /// Имена активных parts, отсортированные.
    fn parts(&self) -> Result<Vec<String>> {
        Err(DbError::Unsupported("table is not replicable".into()))
    }

    /// Все файлы одного part, чтобы отдать его реплике.
    fn export_part(&self, _name: &str) -> Result<PartData> {
        Err(DbError::Unsupported("table is not replicable".into()))
    }

    /// Атомарно добавить parts и убрать другие. Читающий scan видит
    /// либо набор до, либо набор после, но не смесь.
    fn sync_parts(&self, _add: Vec<PartData>, _remove: &[String]) -> Result<()> {
        Err(DbError::Unsupported("table is not replicable".into()))
    }

    /// Установить лениво получаемые parts одним атомарным изменением набора.
    /// MergeTree пишет каждую часть на диск до получения следующей; ошибка
    /// итератора отменяет весь импорт. Базовая реализация сохраняет совместимость.
    fn sync_parts_stream(
        &self,
        add: &mut dyn Iterator<Item = Result<PartData>>,
        remove: &[String],
    ) -> Result<()> {
        self.sync_parts(add.collect::<Result<Vec<_>>>()?, remove)
    }
}

/// Part целиком: имя и файлы из его каталога (part.json, *.bin, *.mrk, primary.idx).
/// Parts неизменяемы, поэтому имя однозначно определяет содержимое.
/// `checksum` — SHA-256 файлов в порядке `files`; защищает передачу от порчи.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartData {
    pub name: String,
    pub files: Vec<(String, Vec<u8>)>,
    pub checksum: String,
}

/// Идентификатор таблицы, выданный каталогом при CREATE.
/// Для остальных подсистем непрозрачен; формат проверяет каталог.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TableId(String);

impl TableId {
    pub fn new(value: impl Into<String>) -> TableId {
        TableId(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TableId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Таблица с идентичностью: используется каталогом и репликацией.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableEntry {
    pub id: TableId,
    pub definition: TableDefinition,
}

/// Идентичность узла: реплика сверяет её, прежде чем менять локальные данные.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeIdentity {
    pub cluster_id: String,
    pub node_id: String,
}

/// Другая нода, к которой реплика ходит за состоянием лидера.
/// Реализация по сети — трек C; в памяти — types::testing::LocalPeer
pub trait Peer: Send + Sync {
    /// Проверить доступность и вернуть идентичность узла.
    fn ping(&self) -> Result<NodeIdentity>;
    fn list_tables(&self) -> Result<Vec<TableEntry>>;
    fn list_parts(&self, table: &TableId) -> Result<Vec<String>>;
    fn fetch_part(&self, table: &TableId, part: &str) -> Result<PartData>;
}

/// Создаёт объект таблицы по описанию. Catalog держит по фабрике на каждый Engine.
pub trait TableFactory: Send + Sync {
    /// Открыть таблицу в data_dir; если данных там нет — создать пустую.
    /// Одна функция и для CREATE TABLE, и для загрузки при старте.
    fn open(&self, definition: &TableDefinition, data_dir: &Path) -> Result<Arc<dyn Table>>;
}

/// Каталог таблиц. Реализация с диском — трек C, в памяти — types::testing.
pub trait Catalog: Send + Sync {
    fn create_table(&self, definition: TableDefinition, if_not_exists: bool) -> Result<()>;
    fn drop_table(&self, name: &str, if_exists: bool) -> Result<()>;
    fn get_table(&self, name: &str) -> Result<Arc<dyn Table>>;
    /// Отсортировано по имени.
    fn list_tables(&self) -> Vec<TableDefinition>;

    // --- Идентичность для репликации ---

    /// Все таблицы с идентичностями, отсортированы по имени.
    /// Каталог без идентичности возвращает пустой список.
    fn table_entries(&self) -> Vec<TableEntry> {
        Vec::new()
    }

    /// Таблица по идентичности. Каталог без идентичности не поддерживает вызов.
    fn get_table_by_id(&self, _id: &TableId) -> Result<Arc<dyn Table>> {
        Err(DbError::Unsupported("catalog has no table identity".into()))
    }

    /// Создать таблицу с заданной идентичностью; нужно только репликации.
    fn create_table_with_id(
        &self,
        _definition: TableDefinition,
        _id: TableId,
        _if_not_exists: bool,
    ) -> Result<()> {
        Err(DbError::Unsupported(
            "catalog does not support explicit table identity".into(),
        ))
    }
}

/// Результат SQL-запроса.
pub enum QueryOutput {
    /// SELECT: заголовок известен до чтения потока (нужен форматам вывода).
    Rows {
        header: Vec<ColumnDef>,
        stream: BlockStream,
    },
    /// CREATE, DROP, OPTIMIZE.
    Ok,
}

/// Вход в Query: SQL → результат. Реализация — трек B.
pub trait QueryEngine: Send + Sync {
    fn execute(&self, sql: &str) -> Result<QueryOutput>;
}
