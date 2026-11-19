//! Внутренний поток JSON-кадров. Завершение чтения подтверждается отдельным кадром.

use crate::config::ShardedTableConfig;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use types::{
    BinaryOp, Block, Column, ColumnData, ColumnDef, DbError, Expr, Result, ScanRequest, Value,
};

#[derive(Serialize, Deserialize)]
pub(crate) struct ReadRequest {
    pub(super) columns: Vec<String>,
    pub(super) predicate: Option<Predicate>,
}

impl From<&ScanRequest> for ReadRequest {
    fn from(request: &ScanRequest) -> Self {
        Self {
            columns: request.columns.clone(),
            predicate: request.predicate.as_ref().and_then(Predicate::from_expr),
        }
    }
}

impl From<ReadRequest> for ScanRequest {
    fn from(request: ReadRequest) -> Self {
        Self {
            columns: request.columns,
            predicate: request.predicate.map(Predicate::into_expr),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub(super) enum Predicate {
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Comparison {
        column: String,
        op: Comparison,
        literal: Literal,
    },
}

#[derive(Serialize, Deserialize)]
pub(super) enum Comparison {
    Eq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

#[derive(Serialize, Deserialize)]
pub(super) enum Literal {
    Float64Bits(u64),
    Other(Value),
}

impl Predicate {
    /// Условие для отсечения допускает лишние строки. В AND можно сохранить одну ветвь,
    /// а для OR нужны обе. Точное условие остаётся в плане координатора.
    fn from_expr(expr: &Expr) -> Option<Self> {
        // Каждый уровень AND и OR вкладывает в JSON объект и массив.
        Self::from_expr_with_depth(expr, 32)
    }

    fn from_expr_with_depth(expr: &Expr, depth: usize) -> Option<Self> {
        let depth = depth.checked_sub(1)?;
        let Expr::Binary { op, left, right } = expr else {
            return None;
        };
        match op {
            BinaryOp::And => match (
                Self::from_expr_with_depth(left, depth),
                Self::from_expr_with_depth(right, depth),
            ) {
                (Some(left), Some(right)) => Some(Self::And(Box::new(left), Box::new(right))),
                (left, right) => left.or(right),
            },
            BinaryOp::Or => Some(Self::Or(
                Box::new(Self::from_expr_with_depth(left, depth)?),
                Box::new(Self::from_expr_with_depth(right, depth)?),
            )),
            op => {
                let (column, literal, reversed) = match (&**left, &**right) {
                    (Expr::Column(column), Expr::Literal(literal)) => (column, literal, false),
                    (Expr::Literal(literal), Expr::Column(column)) => (column, literal, true),
                    _ => return None,
                };
                let op = match (op, reversed) {
                    (BinaryOp::Eq, _) => Comparison::Eq,
                    (BinaryOp::Lt, false) | (BinaryOp::Gt, true) => Comparison::Lt,
                    (BinaryOp::LtEq, false) | (BinaryOp::GtEq, true) => Comparison::LtEq,
                    (BinaryOp::Gt, false) | (BinaryOp::Lt, true) => Comparison::Gt,
                    (BinaryOp::GtEq, false) | (BinaryOp::LtEq, true) => Comparison::GtEq,
                    _ => return None,
                };
                let literal = match literal {
                    Value::Float64(value) => Literal::Float64Bits(value.to_bits()),
                    value => Literal::Other(value.clone()),
                };
                Some(Self::Comparison {
                    column: column.clone(),
                    op,
                    literal,
                })
            }
        }
    }

    pub fn may_match_shard(&self, config: &ShardedTableConfig, index: usize) -> bool {
        match self {
            Self::And(left, right) => {
                left.may_match_shard(config, index) && right.may_match_shard(config, index)
            }
            Self::Or(left, right) => {
                left.may_match_shard(config, index) || right.may_match_shard(config, index)
            }
            Self::Comparison {
                column,
                op: Comparison::Eq,
                literal: Literal::Other(Value::UInt64(value)),
            } if column == &config.key => config.shard_index(*value) == Some(index),
            _ => true,
        }
    }

    fn into_expr(self) -> Expr {
        match self {
            Self::And(left, right) => {
                Expr::binary(BinaryOp::And, left.into_expr(), right.into_expr())
            }
            Self::Or(left, right) => {
                Expr::binary(BinaryOp::Or, left.into_expr(), right.into_expr())
            }
            Self::Comparison {
                column,
                op,
                literal,
            } => {
                let op = match op {
                    Comparison::Eq => BinaryOp::Eq,
                    Comparison::Lt => BinaryOp::Lt,
                    Comparison::LtEq => BinaryOp::LtEq,
                    Comparison::Gt => BinaryOp::Gt,
                    Comparison::GtEq => BinaryOp::GtEq,
                };
                let literal = match literal {
                    Literal::Float64Bits(bits) => Value::Float64(f64::from_bits(bits)),
                    Literal::Other(value) => value,
                };
                Expr::binary(op, Expr::Column(column), Expr::Literal(literal))
            }
        }
    }
}

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
