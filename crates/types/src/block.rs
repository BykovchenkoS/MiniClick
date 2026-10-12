use crate::column::ColumnData;
use crate::error::{DbError, Result};
use crate::schema::{ColumnDef, TableSchema};

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub name: String,
    pub data: ColumnData,
}

/// Кусок таблицы: несколько колонок одной длины
///
/// Инварианты (проверяются в Block::new):
/// - у всех колонок длина == rows
/// - имена колонок уникальны
///
/// rows хранится отдельно, потому что блок может быть без колонок:
/// `SELECT count() FROM t` читает ноль колонок, но строки посчитать надо
#[derive(Debug, Clone, PartialEq)]
pub struct Block {
    columns: Vec<Column>,
    rows: usize,
}

impl Block {
    pub fn new(columns: Vec<Column>) -> Result<Block> {
        let rows = columns.first().map_or(0, |c| c.data.len());
        Self::with_rows(columns, rows)
    }

    /// Нужен только для блоков без колонок.
    pub fn with_rows(columns: Vec<Column>, rows: usize) -> Result<Block> {
        for (i, c) in columns.iter().enumerate() {
            if c.data.len() != rows {
                return Err(DbError::Internal(format!(
                    "column {} has {} rows, expected {}",
                    c.name,
                    c.data.len(),
                    rows
                )));
            }
            if columns[..i].iter().any(|p| p.name == c.name) {
                return Err(DbError::Internal(format!("duplicate column {}", c.name)));
            }
        }
        Ok(Block { columns, rows })
    }

    /// Пустой блок с колонками по схеме.
    pub fn empty(schema: &TableSchema) -> Block {
        let columns = schema
            .columns
            .iter()
            .map(|c| Column {
                name: c.name.clone(),
                data: ColumnData::empty(c.data_type),
            })
            .collect();
        Block { columns, rows: 0 }
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn into_columns(self) -> Vec<Column> {
        self.columns
    }

    pub fn column(&self, name: &str) -> Option<&ColumnData> {
        self.columns
            .iter()
            .find(|c| c.name == name)
            .map(|c| &c.data)
    }

    /// Описание колонок блока (имя + тип) — для форматов вывода.
    pub fn header(&self) -> Vec<ColumnDef> {
        self.columns
            .iter()
            .map(|c| ColumnDef {
                name: c.name.clone(),
                data_type: c.data.data_type(),
            })
            .collect()
    }

    /// Оставить только перечисленные колонки, в этом порядке.
    pub fn select(&self, names: &[String]) -> Result<Block> {
        let mut out = Vec::with_capacity(names.len());
        for n in names {
            let data = self
                .column(n)
                .ok_or_else(|| DbError::UnknownColumn(n.clone()))?;
            out.push(Column {
                name: n.clone(),
                data: data.clone(),
            });
        }
        Block::with_rows(out, self.rows)
    }

    pub fn filter(&self, mask: &[bool]) -> Block {
        assert_eq!(mask.len(), self.rows, "mask length must equal block rows");
        let rows = mask.iter().filter(|&&m| m).count();
        let columns = self
            .columns
            .iter()
            .map(|c| Column {
                name: c.name.clone(),
                data: c.data.filter(mask),
            })
            .collect();
        Block { columns, rows }
    }

    pub fn take(&self, indices: &[usize]) -> Block {
        let columns = self
            .columns
            .iter()
            .map(|c| Column {
                name: c.name.clone(),
                data: c.data.take(indices),
            })
            .collect();
        Block {
            columns,
            rows: indices.len(),
        }
    }

    pub fn slice(&self, offset: usize, len: usize) -> Block {
        let columns = self
            .columns
            .iter()
            .map(|c| Column {
                name: c.name.clone(),
                data: c.data.slice(offset, len),
            })
            .collect();
        Block { columns, rows: len }
    }

    /// Склеить блоки с одинаковым набором колонок (имена и порядок).
    pub fn concat(blocks: &[Block]) -> Result<Block> {
        let Some(first) = blocks.first() else {
            return Block::new(Vec::new());
        };
        let mut out = first.clone();
        for b in &blocks[1..] {
            if b.columns.len() != out.columns.len() {
                return Err(DbError::Internal("concat: different column count".into()));
            }
            for (dst, src) in out.columns.iter_mut().zip(&b.columns) {
                if dst.name != src.name {
                    return Err(DbError::Internal(format!(
                        "concat: column {} vs {}",
                        dst.name, src.name
                    )));
                }
                dst.data.extend(&src.data)?;
            }
            out.rows += b.rows;
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block() -> Block {
        Block::new(vec![
            Column {
                name: "id".into(),
                data: ColumnData::Int64(vec![1, 2, 3]),
            },
            Column {
                name: "s".into(),
                data: ColumnData::String(vec!["a".into(), "b".into(), "c".into()]),
            },
        ])
        .unwrap()
    }

    #[test]
    fn rejects_different_lengths() {
        let r = Block::new(vec![
            Column {
                name: "a".into(),
                data: ColumnData::Int64(vec![1]),
            },
            Column {
                name: "b".into(),
                data: ColumnData::Int64(vec![1, 2]),
            },
        ]);
        assert!(r.is_err());
    }

    #[test]
    fn filter_select_concat() {
        let b = block();
        let f = b.filter(&[true, false, true]);
        assert_eq!(f.rows(), 2);
        assert_eq!(f.column("id"), Some(&ColumnData::Int64(vec![1, 3])));

        let s = b.select(&["s".into()]).unwrap();
        assert_eq!(s.columns().len(), 1);

        let c = Block::concat(&[b.clone(), f]).unwrap();
        assert_eq!(c.rows(), 5);
    }

    #[test]
    fn zero_column_block_keeps_rows() {
        let b = block().select(&[]).unwrap();
        assert_eq!(b.rows(), 3);
        assert_eq!(b.filter(&[true, true, false]).rows(), 2);
    }
}
