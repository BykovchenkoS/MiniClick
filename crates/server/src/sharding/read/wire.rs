//! Внутренний поток JSON-кадров. Завершение чтения подтверждается отдельным кадром.

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use types::{Block, Column, ColumnData, ColumnDef, DbError, Result};

#[derive(Serialize, Deserialize)]
pub(super) enum Frame {
    Block {
        rows: usize,
        columns: Vec<WireColumn>,
    },
    End,
    Error(String),
}

#[derive(Serialize, Deserialize)]
pub(super) struct WireColumn {
    name: String,
    data: WireData,
}

#[derive(Serialize, Deserialize)]
enum WireData {
    Int64(Vec<i64>),
    UInt64(Vec<u64>),
    // Передача битов сохраняет NaN, бесконечности и знак нуля.
    Float64Bits(Vec<u64>),
    String(Vec<String>),
    Date(Vec<i32>),
    DateTime(Vec<i64>),
}

impl From<Block> for Frame {
    fn from(block: Block) -> Self {
        let rows = block.rows();
        let columns = block
            .into_columns()
            .into_iter()
            .map(|Column { name, data }| {
                let data = match data {
                    ColumnData::Int64(values) => WireData::Int64(values),
                    ColumnData::UInt64(values) => WireData::UInt64(values),
                    ColumnData::Float64(values) => {
                        WireData::Float64Bits(values.into_iter().map(f64::to_bits).collect())
                    }
                    ColumnData::String(values) => WireData::String(values),
                    ColumnData::Date(values) => WireData::Date(values),
                    ColumnData::DateTime(values) => WireData::DateTime(values),
                };
                WireColumn { name, data }
            })
            .collect();
        Self::Block { rows, columns }
    }
}

pub(super) fn decode(rows: usize, columns: Vec<WireColumn>, header: &[ColumnDef]) -> Result<Block> {
    let columns = columns
        .into_iter()
        .map(|WireColumn { name, data }| {
            let data = match data {
                WireData::Int64(values) => ColumnData::Int64(values),
                WireData::UInt64(values) => ColumnData::UInt64(values),
                WireData::Float64Bits(values) => {
                    ColumnData::Float64(values.into_iter().map(f64::from_bits).collect())
                }
                WireData::String(values) => ColumnData::String(values),
                WireData::Date(values) => ColumnData::Date(values),
                WireData::DateTime(values) => ColumnData::DateTime(values),
            };
            Column { name, data }
        })
        .collect();
    let block = Block::with_rows(columns, rows)?;
    if block.header() != header {
        return Err(DbError::Internal("unexpected shard scan columns".into()));
    }
    Ok(block)
}

pub(super) fn send(sender: &mpsc::Sender<Vec<u8>>, frame: &Frame) -> bool {
    let Ok(mut line) = serde_json::to_vec(frame) else {
        return false;
    };
    line.push(b'\n');
    sender.blocking_send(line).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_frame_preserves_float_bits() {
        let bits = [
            0x8000_0000_0000_0000,
            0x7ff8_0000_0000_0042,
            0x7ff0_0000_0000_0000,
        ];
        let block = Block::new(vec![Column {
            name: "reading".into(),
            data: ColumnData::Float64(bits.into_iter().map(f64::from_bits).collect()),
        }])
        .unwrap();
        let header = block.header();
        let bytes = serde_json::to_vec(&Frame::from(block)).unwrap();
        let Frame::Block { rows, columns } = serde_json::from_slice(&bytes).unwrap() else {
            panic!("expected block frame");
        };
        let block = decode(rows, columns, &header).unwrap();
        let Some(ColumnData::Float64(values)) = block.column("reading") else {
            panic!("expected Float64 column");
        };
        assert!(values.iter().map(|value| value.to_bits()).eq(bits));
    }
}
