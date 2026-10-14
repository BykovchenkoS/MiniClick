//! Запись неизменяемого part через временный каталог.

use std::cmp::Ordering;
use std::fs::{self, File};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering as AtomicOrdering},
    Arc,
};
use types::{Block, Column, ColumnData, DbError, Engine, Result, TableDefinition, Value};

use crate::format::{self, Mark, PartMeta, FORMAT_VERSION};

pub(crate) fn internal(message: impl Into<String>) -> DbError {
    DbError::Internal(message.into())
}

pub(crate) fn validate_definition(definition: &TableDefinition) -> Result<()> {
    definition.validate()?;
    if definition.engine != Engine::MergeTree {
        return Err(DbError::Unsupported("storage requires MergeTree".into()));
    }
    if definition.settings.index_granularity > u32::MAX as usize {
        return Err(DbError::Syntax("index_granularity exceeds u32".into()));
    }
    for column in &definition.schema.columns {
        if column.name.is_empty() || column.name.contains(['/', '\\', '\0']) {
            return Err(DbError::Syntax(
                "column name is not a valid file name".into(),
            ));
        }
    }
    for (i, key) in definition.order_by.iter().enumerate() {
        if definition.order_by[..i].contains(key) {
            return Err(DbError::Syntax("duplicate ORDER BY column".into()));
        }
    }
    Ok(())
}

pub(crate) fn validate_block(block: &Block, definition: &TableDefinition) -> Result<()> {
    if block.header() != definition.schema.columns {
        return Err(DbError::TypeMismatch(
            "block does not match table schema".into(),
        ));
    }
    Ok(())
}

pub(crate) fn compare_keys(left: &[Value], right: &[Value]) -> Result<Ordering> {
    if left.len() != right.len() {
        return Err(internal("key dimensions differ"));
    }
    for (left, right) in left.iter().zip(right) {
        let order = left
            .compare(right)
            .ok_or_else(|| internal("key types differ"))?;
        if order != Ordering::Equal {
            return Ok(order);
        }
    }
    Ok(Ordering::Equal)
}

fn key_columns<'a>(block: &'a Block, definition: &TableDefinition) -> Result<Vec<&'a ColumnData>> {
    definition
        .order_by
        .iter()
        .map(|name| {
            block
                .column(name)
                .ok_or_else(|| DbError::UnknownColumn(name.clone()))
        })
        .collect()
}

pub(crate) fn row_key(
    block: &Block,
    definition: &TableDefinition,
    row: usize,
) -> Result<Vec<Value>> {
    if row >= block.rows() {
        return Err(internal("key row is out of bounds"));
    }
    Ok(key_columns(block, definition)?
        .iter()
        .map(|c| c.get(row))
        .collect())
}

pub(crate) fn sort_block(block: &Block, definition: &TableDefinition) -> Result<Block> {
    validate_block(block, definition)?;
    let keys = key_columns(block, definition)?;
    let mut indices: Vec<_> = (0..block.rows()).collect();
    indices.sort_by(|&a, &b| {
        for column in &keys {
            // Колонка сравнивается сама с собой, типы всегда совпадают.
            let order = column
                .get(a)
                .compare(&column.get(b))
                .unwrap_or(Ordering::Equal);
            if order != Ordering::Equal {
                return order;
            }
        }
        Ordering::Equal
    });
    Ok(block.take(&indices))
}

#[cfg(not(windows))]
pub(crate) fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(windows)]
pub(crate) fn sync_directory(path: &Path) -> Result<()> {
    // Windows не поддерживает этот способ fsync каталога; файлы синхронизируются
    // отдельно. Сохранность переименований при отключении питания не гарантируется.
    if !fs::metadata(path)?.is_dir() {
        return Err(
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "directory expected").into(),
        );
    }
    Ok(())
}

pub(crate) fn write_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

struct ColumnWriter {
    file: BufWriter<File>,
    marks: Vec<Mark>,
    offset: u64,
}

/// Получает уже отсортированные гранулы; используется INSERT и merge.
pub(crate) struct PartWriter {
    temporary: PathBuf,
    target: PathBuf,
    definition: TableDefinition,
    columns: Vec<ColumnWriter>,
    index: Vec<ColumnData>,
    rows: u64,
    min_key: Vec<Value>,
    max_key: Vec<Value>,
    published: bool,
}

impl PartWriter {
    pub fn new(root: &Path, name: &str, definition: &TableDefinition) -> Result<Self> {
        validate_definition(definition)?;
        if name.is_empty() || name.contains(['/', '\\', '\0']) || name == "." || name == ".." {
            return Err(internal("invalid part name"));
        }
        let target = root.join(name);
        if target.exists() {
            return Err(internal("part already exists"));
        }
        let temporary = root.join(format!("tmp_{name}"));
        fs::create_dir(&temporary)?;
        let mut writer = Self {
            temporary,
            target,
            definition: definition.clone(),
            columns: Vec::new(),
            index: Vec::new(),
            rows: 0,
            min_key: Vec::new(),
            max_key: Vec::new(),
            published: false,
        };
        for column in &definition.schema.columns {
            writer.columns.push(ColumnWriter {
                file: BufWriter::new(File::create(
                    writer.temporary.join(format!("{}.bin", column.name)),
                )?),
                marks: Vec::new(),
                offset: 0,
            });
        }
        for name in &definition.order_by {
            let column = definition
                .schema
                .column(name)
                .ok_or_else(|| DbError::UnknownColumn(name.clone()))?;
            writer.index.push(ColumnData::empty(column.data_type));
        }
        Ok(writer)
    }

    pub fn write_granule(&mut self, block: &Block) -> Result<()> {
        validate_block(block, &self.definition)?;
        if block.rows() == 0 || block.rows() > self.definition.settings.index_granularity {
            return Err(internal("invalid granule row count"));
        }
        let first = row_key(block, &self.definition, 0)?;
        let mut previous = self.max_key.clone();
        for row in 0..block.rows() {
            let key = row_key(block, &self.definition, row)?;
            if !previous.is_empty() && compare_keys(&previous, &key)? == Ordering::Greater {
                return Err(internal("granules are not sorted"));
            }
            previous = key;
        }
        for (column, writer) in block.columns().iter().zip(&mut self.columns) {
            let frame = format::encode_frame(
                &format::encode_column(&column.data)?,
                self.definition.settings.codec,
            )?;
            writer.file.write_all(&frame)?;
            writer.marks.push(Mark {
                offset: writer.offset,
                rows: block.rows() as u32,
            });
            writer.offset = writer
                .offset
                .checked_add(frame.len() as u64)
                .ok_or_else(|| internal("column offset overflow"))?;
        }
        for (index, value) in self.index.iter_mut().zip(&first) {
            index.push(value.clone())?;
        }
        if self.rows == 0 {
            self.min_key = first;
        }
        self.max_key = previous;
        self.rows = self
            .rows
            .checked_add(block.rows() as u64)
            .ok_or_else(|| internal("part row count overflow"))?;
        Ok(())
    }

    pub fn finish(mut self) -> Result<PartMeta> {
        let granules = self.index.first().map_or(0, ColumnData::len);
        let meta = PartMeta {
            format_version: FORMAT_VERSION,
            rows: self.rows,
            granules: u32::try_from(granules).map_err(|_| internal("too many granules"))?,
            columns: self.definition.schema.columns.clone(),
            codec: self.definition.settings.codec,
            min_key: self.min_key.clone(),
            max_key: self.max_key.clone(),
        };
        let json = format::encode_meta(&meta)?;
        for (writer, column) in self.columns.iter_mut().zip(&self.definition.schema.columns) {
            writer.file.flush()?;
            writer.file.get_ref().sync_all()?;
            write_file(
                &self.temporary.join(format!("{}.mrk", column.name)),
                &format::encode_marks(&writer.marks)?,
            )?;
        }
        write_file(
            &self.temporary.join("primary.idx"),
            &format::encode_index(&self.index, granules)?,
        )?;
        write_file(&self.temporary.join("part.json"), &json)?;
        // Закрываем файлы до переименования каталога: Windows может запрещать
        // переименование родительского каталога при открытых файлах внутри него.
        self.columns.clear();
        sync_directory(&self.temporary)?;
        fs::rename(&self.temporary, &self.target).map_err(|error| {
            std::io::Error::new(
                error.kind(),
                format!(
                    "rename part directory {} -> {}: {error}",
                    self.temporary.display(),
                    self.target.display(),
                ),
            )
        })?;
        // После публикации каталог принадлежит репозиторию, даже если fsync не удался.
        self.published = true;
        sync_directory(
            self.target
                .parent()
                .ok_or_else(|| internal("missing part parent"))?,
        )?;
        Ok(meta)
    }
}

impl Drop for PartWriter {
    fn drop(&mut self) {
        if !self.published {
            // Освобождаем файловые дескрипторы до удаления незавершённого part.
            self.columns.clear();
            let _ = fs::remove_dir_all(&self.temporary);
        }
    }
}

pub(crate) fn write_part(
    root: &Path,
    name: &str,
    definition: &TableDefinition,
    block: &Block,
) -> Result<PartMeta> {
    validate_definition(definition)?;
    let sorted = sort_block(block, definition)?;
    let mut writer = PartWriter::new(root, name, definition)?;
    for offset in (0..sorted.rows()).step_by(definition.settings.index_granularity) {
        writer.write_granule(&sorted.slice(
            offset,
            (sorted.rows() - offset).min(definition.settings.index_granularity),
        ))?;
    }
    writer.finish()
}

/// Метаданные и индекс находятся в памяти; колонки открываются при чтении.
pub(crate) struct DataPart {
    pub path: PathBuf,
    pub meta: PartMeta,
    pub marks: Vec<Vec<Mark>>,
    pub index: Vec<ColumnData>,
    pub lease: Option<Arc<crate::repository::DirectoryLease>>,
    retired: AtomicBool,
}

impl DataPart {
    pub fn open(path: &Path, definition: &TableDefinition) -> Result<Self> {
        validate_definition(definition)?;
        let meta = format::decode_meta(&fs::read(path.join("part.json"))?)?;
        if meta.columns != definition.schema.columns {
            return Err(internal("part schema differs from table schema"));
        }
        let mut marks: Vec<Vec<Mark>> = Vec::new();
        for column in &meta.columns {
            let column_marks =
                format::decode_marks(&fs::read(path.join(format!("{}.mrk", column.name)))?)?;
            if column_marks.len() != meta.granules as usize
                || column_marks.iter().map(|m| u64::from(m.rows)).sum::<u64>() != meta.rows
            {
                return Err(internal("marks differ from metadata counts"));
            }
            for (i, mark) in column_marks.iter().enumerate() {
                if mark.rows as usize > definition.settings.index_granularity
                    || (i + 1 < column_marks.len()
                        && mark.rows as usize != definition.settings.index_granularity)
                {
                    return Err(internal("invalid granule size"));
                }
            }
            if let Some(first) = marks.first() {
                if first
                    .iter()
                    .map(|m| m.rows)
                    .ne(column_marks.iter().map(|m| m.rows))
                {
                    return Err(internal("column granule counts differ"));
                }
            }
            marks.push(column_marks);
        }
        let key_types = definition
            .order_by
            .iter()
            .map(|name| {
                definition
                    .schema
                    .column(name)
                    .map(|c| c.data_type)
                    .ok_or_else(|| DbError::UnknownColumn(name.clone()))
            })
            .collect::<Result<Vec<_>>>()?;
        if meta.min_key.len() != key_types.len()
            || meta
                .min_key
                .iter()
                .zip(&key_types)
                .any(|(v, t)| v.data_type() != *t)
        {
            return Err(internal("part key differs from table key"));
        }
        let index = format::decode_index(
            &fs::read(path.join("primary.idx"))?,
            &key_types,
            meta.granules as usize,
        )?;
        let part = Self {
            path: path.to_owned(),
            meta,
            marks,
            index,
            lease: None,
            retired: AtomicBool::new(false),
        };
        if compare_keys(&part.key(0), &part.meta.min_key)? != Ordering::Equal {
            return Err(internal("first index key differs from minimum"));
        }
        for granule in 0..part.meta.granules as usize {
            let key = part.key(granule);
            let upper = if granule + 1 < part.meta.granules as usize {
                part.key(granule + 1)
            } else {
                part.meta.max_key.clone()
            };
            if compare_keys(&key, &upper)? == Ordering::Greater {
                return Err(internal("index keys are not sorted"));
            }
        }
        Ok(part)
    }

    pub fn check_live(&self) -> Result<()> {
        if self.lease.as_ref().is_some_and(|lease| lease.is_dropped()) {
            return Err(internal("table was dropped"));
        }
        Ok(())
    }

    pub fn retire(&self) {
        self.retired.store(true, AtomicOrdering::Release);
    }

    pub fn key(&self, granule: usize) -> Vec<Value> {
        self.index.iter().map(|c| c.get(granule)).collect()
    }

    pub fn granule_rows(&self, granule: usize) -> u32 {
        self.marks[0][granule].rows
    }
}

impl Drop for DataPart {
    fn drop(&mut self) {
        if self.retired.load(AtomicOrdering::Acquire)
            && !self.lease.as_ref().is_some_and(|lease| lease.is_dropped())
        {
            if let Err(error) = fs::remove_dir_all(&self.path) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(path = ?self.path, %error, "Не удалось удалить устаревший part");
                }
            }
        }
    }
}

/// Файлы сохраняются между чтениями гранул; чтение начинается по меткам.
pub(crate) struct PartReader {
    part: Arc<DataPart>,
    columns: Vec<usize>,
    files: Vec<File>,
}

impl PartReader {
    pub fn new(part: &Arc<DataPart>, names: &[String]) -> Result<Self> {
        part.check_live()?;
        let mut columns = Vec::new();
        let mut files = Vec::new();
        for name in names {
            let position = part
                .meta
                .columns
                .iter()
                .position(|c| &c.name == name)
                .ok_or_else(|| DbError::UnknownColumn(name.clone()))?;
            if columns.contains(&position) {
                return Err(internal("duplicate scan column"));
            }
            columns.push(position);
            files.push(File::open(part.path.join(format!("{name}.bin")))?);
        }
        Ok(Self {
            part: part.clone(),
            columns,
            files,
        })
    }

    pub fn read_granule(&mut self, granule: usize) -> Result<Block> {
        self.part.check_live()?;
        if granule >= self.part.meta.granules as usize {
            return Err(internal("granule is out of bounds"));
        }
        let mut columns = Vec::new();
        for (&position, file) in self.columns.iter().zip(&mut self.files) {
            let marks = &self.part.marks[position];
            let mark = marks[granule];
            let end = if let Some(next) = marks.get(granule + 1) {
                next.offset
            } else {
                file.metadata()?.len()
            };
            let size = end
                .checked_sub(mark.offset)
                .ok_or_else(|| internal("mark exceeds column size"))?;
            let max_size = format::MAX_FRAME_SIZE
                + format::MAX_FRAME_SIZE / 255
                + 1024
                + format::FRAME_HEADER_SIZE;
            if size < format::FRAME_HEADER_SIZE as u64 || size > max_size as u64 {
                return Err(internal("invalid frame range"));
            }
            if mark.offset != file.stream_position()? {
                file.seek(SeekFrom::Start(mark.offset))?;
            }
            let mut header = [0; format::FRAME_HEADER_SIZE];
            read_exact(file, &mut header)?;
            let compressed = u32::from_le_bytes(
                header[1..5]
                    .try_into()
                    .map_err(|_| internal("invalid header"))?,
            ) as u64;
            if compressed + format::FRAME_HEADER_SIZE as u64 != size {
                return Err(internal("frame header differs from marks"));
            }
            let mut frame = Vec::new();
            frame
                .try_reserve_exact(size as usize)
                .map_err(|_| internal("frame allocation failed"))?;
            frame.extend_from_slice(&header);
            frame.resize(size as usize, 0);
            read_exact(file, &mut frame[format::FRAME_HEADER_SIZE..])?;
            let definition = &self.part.meta.columns[position];
            columns.push(Column {
                name: definition.name.clone(),
                data: format::decode_column(
                    &format::decode_frame(&frame)?,
                    definition.data_type,
                    mark.rows as usize,
                )?,
            });
        }
        Block::with_rows(columns, self.part.granule_rows(granule) as usize)
    }
}

fn read_exact(file: &mut File, bytes: &mut [u8]) -> Result<()> {
    file.read_exact(bytes).map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            internal("truncated column file")
        } else {
            DbError::Io(e)
        }
    })
}

#[cfg(test)]
mod tests;
