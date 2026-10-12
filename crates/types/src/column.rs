// Макросы ниже одинаково работают для String и для Copy-типов, поэтому clone().
#![allow(clippy::clone_on_copy)]

use crate::error::{DbError, Result};
use crate::value::{DataType, Value};

/// Данные одной колонки. Вариант совпадает с DataType колонки
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnData {
    Int64(Vec<i64>),
    UInt64(Vec<u64>),
    Float64(Vec<f64>),
    /// Пока просто Vec<String>; offsets + bytes — если упрёмся в скорость
    String(Vec<String>),
    Date(Vec<i32>),
    DateTime(Vec<i64>),
}

/// Применяет одно и то же выражение к вектору внутри любого варианта.
macro_rules! map_variant {
    ($self:expr, $v:ident => $body:expr) => {
        match $self {
            ColumnData::Int64($v) => ColumnData::Int64($body),
            ColumnData::UInt64($v) => ColumnData::UInt64($body),
            ColumnData::Float64($v) => ColumnData::Float64($body),
            ColumnData::String($v) => ColumnData::String($body),
            ColumnData::Date($v) => ColumnData::Date($body),
            ColumnData::DateTime($v) => ColumnData::DateTime($body),
        }
    };
}

macro_rules! with_variant {
    ($self:expr, $v:ident => $body:expr) => {
        match $self {
            ColumnData::Int64($v) => $body,
            ColumnData::UInt64($v) => $body,
            ColumnData::Float64($v) => $body,
            ColumnData::String($v) => $body,
            ColumnData::Date($v) => $body,
            ColumnData::DateTime($v) => $body,
        }
    };
}

impl ColumnData {
    pub fn empty(t: DataType) -> ColumnData {
        match t {
            DataType::Int64 => ColumnData::Int64(Vec::new()),
            DataType::UInt64 => ColumnData::UInt64(Vec::new()),
            DataType::Float64 => ColumnData::Float64(Vec::new()),
            DataType::String => ColumnData::String(Vec::new()),
            DataType::Date => ColumnData::Date(Vec::new()),
            DataType::DateTime => ColumnData::DateTime(Vec::new()),
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            ColumnData::Int64(_) => DataType::Int64,
            ColumnData::UInt64(_) => DataType::UInt64,
            ColumnData::Float64(_) => DataType::Float64,
            ColumnData::String(_) => DataType::String,
            ColumnData::Date(_) => DataType::Date,
            ColumnData::DateTime(_) => DataType::DateTime,
        }
    }

    pub fn len(&self) -> usize {
        with_variant!(self, v => v.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Значение строки i. Медленно, для min/max, индекса и тестов, не для горячих циклов
    pub fn get(&self, i: usize) -> Value {
        match self {
            ColumnData::Int64(v) => Value::Int64(v[i]),
            ColumnData::UInt64(v) => Value::UInt64(v[i]),
            ColumnData::Float64(v) => Value::Float64(v[i]),
            ColumnData::String(v) => Value::String(v[i].clone()),
            ColumnData::Date(v) => Value::Date(v[i]),
            ColumnData::DateTime(v) => Value::DateTime(v[i]),
        }
    }

    pub fn push(&mut self, value: Value) -> Result<()> {
        match (self, value) {
            (ColumnData::Int64(v), Value::Int64(x)) => v.push(x),
            (ColumnData::UInt64(v), Value::UInt64(x)) => v.push(x),
            (ColumnData::Float64(v), Value::Float64(x)) => v.push(x),
            (ColumnData::String(v), Value::String(x)) => v.push(x),
            (ColumnData::Date(v), Value::Date(x)) => v.push(x),
            (ColumnData::DateTime(v), Value::DateTime(x)) => v.push(x),
            (col, value) => {
                return Err(DbError::TypeMismatch(format!(
                    "cannot push {} into {} column",
                    value.data_type(),
                    col.data_type()
                )))
            }
        }
        Ok(())
    }

    /// Оставить строки, где mask[i] == true. mask.len() == self.len()
    pub fn filter(&self, mask: &[bool]) -> ColumnData {
        debug_assert_eq!(mask.len(), self.len());
        map_variant!(self, v => v.iter().zip(mask).filter(|(_, &m)| m).map(|(x, _)| x.clone()).collect())
    }

    /// Строки в порядке indices (для сортировки и merge)
    pub fn take(&self, indices: &[usize]) -> ColumnData {
        map_variant!(self, v => indices.iter().map(|&i| v[i].clone()).collect())
    }

    /// Строки [offset, offset + len)
    pub fn slice(&self, offset: usize, len: usize) -> ColumnData {
        map_variant!(self, v => v[offset..offset + len].to_vec())
    }

    /// Дописать в конец данные другой колонки того же типа
    pub fn extend(&mut self, other: &ColumnData) -> Result<()> {
        match (self, other) {
            (ColumnData::Int64(a), ColumnData::Int64(b)) => a.extend_from_slice(b),
            (ColumnData::UInt64(a), ColumnData::UInt64(b)) => a.extend_from_slice(b),
            (ColumnData::Float64(a), ColumnData::Float64(b)) => a.extend_from_slice(b),
            (ColumnData::String(a), ColumnData::String(b)) => a.extend_from_slice(b),
            (ColumnData::Date(a), ColumnData::Date(b)) => a.extend_from_slice(b),
            (ColumnData::DateTime(a), ColumnData::DateTime(b)) => a.extend_from_slice(b),
            (a, b) => {
                return Err(DbError::TypeMismatch(format!(
                    "cannot concat {} and {} columns",
                    a.data_type(),
                    b.data_type()
                )))
            }
        }
        Ok(())
    }
}
