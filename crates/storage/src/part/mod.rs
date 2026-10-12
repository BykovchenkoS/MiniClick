//! Запись неизменяемого part через временный каталог.

use std::cmp::Ordering;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use types::{Block, ColumnData, DbError, Engine, Result, TableDefinition, Value};

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

pub(crate) fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
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
        sync_directory(&self.temporary)?;
        fs::rename(&self.temporary, &self.target)?;
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

#[cfg(test)]
mod tests;
