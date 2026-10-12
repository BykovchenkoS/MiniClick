//! Слияние отсортированных потоков с ограниченной памятью.

use crate::part::{compare_keys, internal, row_key, DataPart, PartReader, PartWriter};
use crate::repository::PartId;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::path::Path;
use std::sync::Arc;
use types::{Block, Result, TableDefinition, Value};

/// Не более четырёх входных гранул одновременно; выбираем соседние малые parts.
pub(crate) fn select(parts: &[Arc<DataPart>]) -> Vec<Arc<DataPart>> {
    if parts.len() < 2 {
        return Vec::new();
    }
    let count = parts.len().min(4);
    let start = parts
        .windows(count)
        .enumerate()
        .min_by_key(|(_, window)| {
            window
                .iter()
                .map(|part| u128::from(part.meta.rows))
                .sum::<u128>()
        })
        .map_or(0, |(start, _)| start);
    parts[start..start + count].to_vec()
}

pub(crate) fn merged_id(parts: &[Arc<DataPart>]) -> Result<(PartId, Vec<PartId>)> {
    let ids = parts
        .iter()
        .map(|part| {
            PartId::parse(
                part.path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or(""),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let first = ids.first().ok_or_else(|| internal("empty merge"))?;
    let last = ids.last().ok_or_else(|| internal("empty merge"))?;
    let level = ids
        .iter()
        .map(|id| id.level)
        .max()
        .ok_or_else(|| internal("empty merge"))?
        .checked_add(1)
        .ok_or_else(|| internal("merge level overflow"))?;
    Ok((
        PartId {
            min: first.min,
            max: last.max,
            level,
        },
        ids,
    ))
}

struct Cursor {
    reader: PartReader,
    block: Block,
    row: usize,
    granule: usize,
    granules: usize,
}

impl Cursor {
    fn new(part: &Arc<DataPart>, definition: &TableDefinition) -> Result<Self> {
        let mut reader = PartReader::new(part, &definition.schema.names())?;
        let block = reader.read_granule(0)?;
        Ok(Self {
            reader,
            block,
            row: 0,
            granule: 0,
            granules: part.meta.granules as usize,
        })
    }

    fn advance(&mut self) -> Result<bool> {
        self.row += 1;
        if self.row < self.block.rows() {
            return Ok(true);
        }
        self.granule += 1;
        if self.granule == self.granules {
            return Ok(false);
        }
        self.block = self.reader.read_granule(self.granule)?;
        self.row = 0;
        Ok(true)
    }
}

struct Head {
    key: Vec<Value>,
    cursor: usize,
}

impl Ord for Head {
    fn cmp(&self, other: &Self) -> Ordering {
        // Все ключи построены по одной схеме; очередь извлекает минимальный.
        compare_keys(&other.key, &self.key)
            .unwrap_or(Ordering::Equal)
            .then_with(|| other.cursor.cmp(&self.cursor))
    }
}

impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Head {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Head {}

pub(crate) fn write_merge(
    root: &Path,
    id: PartId,
    definition: &TableDefinition,
    parts: &[Arc<DataPart>],
) -> Result<()> {
    let mut writer = PartWriter::new(root, &id.name(), definition)?;
    let mut cursors = Vec::new();
    let mut heap = BinaryHeap::new();
    for (position, part) in parts.iter().enumerate() {
        let cursor = Cursor::new(part, definition)?;
        heap.push(Head {
            key: row_key(&cursor.block, definition, 0)?,
            cursor: position,
        });
        cursors.push(cursor);
    }
    let mut columns = Block::empty(&definition.schema).into_columns();
    let mut rows = 0;
    while let Some(head) = heap.pop() {
        let cursor = &mut cursors[head.cursor];
        for (column, input) in columns.iter_mut().zip(cursor.block.columns()) {
            column.data.push(input.data.get(cursor.row))?;
        }
        rows += 1;
        if rows == definition.settings.index_granularity {
            let block = Block::new(std::mem::replace(
                &mut columns,
                Block::empty(&definition.schema).into_columns(),
            ))?;
            writer.write_granule(&block)?;
            rows = 0;
        }
        if cursor.advance()? {
            heap.push(Head {
                key: row_key(&cursor.block, definition, cursor.row)?,
                cursor: head.cursor,
            });
        }
    }
    if rows != 0 {
        writer.write_granule(&Block::new(columns)?)?;
    }
    writer.finish()?;
    Ok(())
}

#[cfg(test)]
mod tests;
