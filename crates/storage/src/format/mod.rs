//! Форматы колонок, рамок, меток, первичного индекса и метаданных part.

use serde::{Deserialize, Serialize};
use types::{Codec, ColumnData, ColumnDef, DataType, DbError, Result, Value};

pub(crate) const FORMAT_VERSION: u32 = 1;
pub(crate) const MARK_SIZE: usize = 12;
pub(crate) const FRAME_HEADER_SIZE: usize = 9;
// Ограничение одной гранулы защищает распаковку от испорченных размеров.
pub(crate) const MAX_FRAME_SIZE: usize = 256 * 1024 * 1024;

fn corrupt(message: impl Into<String>) -> DbError {
    DbError::Internal(format!("invalid part format: {}", message.into()))
}

/// Чтение ограничено переданным срезом; усечение возвращает ошибку.
struct Reader<'a> {
    remaining: &'a [u8],
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    fn take(&mut self, size: usize) -> Result<&'a [u8]> {
        if size > self.remaining.len() {
            return Err(corrupt("truncated data"));
        }
        let (bytes, rest) = self.remaining.split_at(size);
        self.remaining = rest;
        Ok(bytes)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.take(N)?
            .try_into()
            .map_err(|_| corrupt("invalid fixed-size value"))
    }

    fn finish(self) -> Result<()> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(corrupt("trailing bytes"))
        }
    }
}

pub(crate) fn encode_column(column: &ColumnData) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    match column {
        ColumnData::Int64(values) | ColumnData::DateTime(values) => {
            for value in values {
                bytes.extend_from_slice(&value.to_le_bytes());
            }
        }
        ColumnData::UInt64(values) => {
            for value in values {
                bytes.extend_from_slice(&value.to_le_bytes());
            }
        }
        ColumnData::Float64(values) => {
            for value in values {
                bytes.extend_from_slice(&value.to_le_bytes());
            }
        }
        ColumnData::Date(values) => {
            for value in values {
                bytes.extend_from_slice(&value.to_le_bytes());
            }
        }
        ColumnData::String(values) => {
            for value in values {
                let len =
                    u32::try_from(value.len()).map_err(|_| corrupt("string length exceeds u32"))?;
                bytes.extend_from_slice(&len.to_le_bytes());
                bytes.extend_from_slice(value.as_bytes());
            }
        }
    }
    Ok(bytes)
}

fn read_column(reader: &mut Reader<'_>, data_type: DataType, rows: usize) -> Result<ColumnData> {
    let width = match data_type {
        DataType::Date | DataType::String => 4,
        _ => 8,
    };
    // Проверяем минимум до выделения памяти по числу строк из файла.
    if rows > reader.remaining.len() / width {
        return Err(corrupt("row count exceeds available data"));
    }
    macro_rules! read_numbers {
        ($variant:ident, $number:ty) => {{
            let mut values = Vec::with_capacity(rows);
            for _ in 0..rows {
                values.push(<$number>::from_le_bytes(reader.array()?));
            }
            ColumnData::$variant(values)
        }};
    }
    Ok(match data_type {
        DataType::Int64 => read_numbers!(Int64, i64),
        DataType::UInt64 => read_numbers!(UInt64, u64),
        DataType::Float64 => read_numbers!(Float64, f64),
        DataType::Date => read_numbers!(Date, i32),
        DataType::DateTime => read_numbers!(DateTime, i64),
        DataType::String => {
            let mut values = Vec::with_capacity(rows);
            for _ in 0..rows {
                let len = u32::from_le_bytes(reader.array()?) as usize;
                let value = std::str::from_utf8(reader.take(len)?)
                    .map_err(|_| corrupt("invalid UTF-8 string"))?;
                values.push(value.to_owned());
            }
            ColumnData::String(values)
        }
    })
}

pub(crate) fn decode_column(bytes: &[u8], data_type: DataType, rows: usize) -> Result<ColumnData> {
    let mut reader = Reader::new(bytes);
    let column = read_column(&mut reader, data_type, rows)?;
    reader.finish()?;
    Ok(column)
}

/// Идентификаторы кодеков на диске не зависят от порядка вариантов enum.
fn codec_id(codec: Codec) -> u8 {
    match codec {
        Codec::None => 0,
        Codec::Lz4 => 1,
        Codec::Zstd => 2,
    }
}

fn read_codec(id: u8) -> Result<Codec> {
    match id {
        0 => Ok(Codec::None),
        1 => Ok(Codec::Lz4),
        2 => Ok(Codec::Zstd),
        _ => Err(corrupt(format!("unknown codec {id}"))),
    }
}

pub(crate) fn encode_frame(bytes: &[u8], codec: Codec) -> Result<Vec<u8>> {
    if bytes.len() > MAX_FRAME_SIZE {
        return Err(corrupt("frame exceeds 256 MiB limit"));
    }
    let size = u32::try_from(bytes.len()).map_err(|_| corrupt("frame size exceeds u32"))?;
    let payload = match codec {
        Codec::None => bytes.to_vec(),
        Codec::Lz4 => lz4_flex::block::compress(bytes),
        Codec::Zstd => zstd::bulk::compress(bytes, 3).map_err(DbError::Io)?,
    };
    let compressed =
        u32::try_from(payload.len()).map_err(|_| corrupt("compressed frame size exceeds u32"))?;
    let mut frame = Vec::new();
    frame.push(codec_id(codec));
    frame.extend_from_slice(&compressed.to_le_bytes());
    frame.extend_from_slice(&size.to_le_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

/// Принимает ровно одну рамку; соседние рамки читатель отделяет по заголовку.
pub(crate) fn decode_frame(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut reader = Reader::new(bytes);
    let codec = read_codec(reader.array::<1>()?[0])?;
    let compressed = u32::from_le_bytes(reader.array()?) as usize;
    let original = u32::from_le_bytes(reader.array()?) as usize;
    if original > MAX_FRAME_SIZE {
        return Err(corrupt("frame exceeds 256 MiB limit"));
    }
    let payload = reader.take(compressed)?;
    reader.finish()?;
    let decoded = match codec {
        Codec::None => payload.to_vec(),
        Codec::Lz4 => {
            // Поток ограничен ожидаемой длиной даже при испорченном заголовке.
            let mut output = Vec::new();
            output
                .try_reserve_exact(original)
                .map_err(|_| corrupt("frame allocation failed"))?;
            output.resize(original, 0);
            let size = lz4_flex::block::decompress_into(payload, &mut output)
                .map_err(|e| corrupt(format!("LZ4: {e}")))?;
            output.truncate(size);
            output
        }
        Codec::Zstd => {
            zstd::bulk::decompress(payload, original).map_err(|e| corrupt(format!("ZSTD: {e}")))?
        }
    };
    if decoded.len() != original {
        return Err(corrupt("decoded frame size differs from header"));
    }
    Ok(decoded)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Mark {
    pub offset: u64,
    pub rows: u32,
}

fn validate_marks(marks: &[Mark]) -> Result<()> {
    for (i, mark) in marks.iter().enumerate() {
        if mark.rows == 0 || (i == 0 && mark.offset != 0) {
            return Err(corrupt("empty granule or nonzero first offset"));
        }
        if i > 0 && mark.offset <= marks[i - 1].offset {
            return Err(corrupt("mark offsets are not increasing"));
        }
    }
    Ok(())
}

pub(crate) fn encode_marks(marks: &[Mark]) -> Result<Vec<u8>> {
    validate_marks(marks)?;
    let mut bytes = Vec::new();
    for mark in marks {
        bytes.extend_from_slice(&mark.offset.to_le_bytes());
        bytes.extend_from_slice(&mark.rows.to_le_bytes());
    }
    Ok(bytes)
}

pub(crate) fn decode_marks(bytes: &[u8]) -> Result<Vec<Mark>> {
    if !bytes.len().is_multiple_of(MARK_SIZE) {
        return Err(corrupt("incomplete mark"));
    }
    let mut reader = Reader::new(bytes);
    let mut marks = Vec::with_capacity(bytes.len() / MARK_SIZE);
    while !reader.remaining.is_empty() {
        marks.push(Mark {
            offset: u64::from_le_bytes(reader.array()?),
            rows: u32::from_le_bytes(reader.array()?),
        });
    }
    validate_marks(&marks)?;
    Ok(marks)
}

/// Индекс хранит колонки ключа подряд, по одному значению на гранулу.
pub(crate) fn encode_index(columns: &[ColumnData], granules: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    for column in columns {
        if column.len() != granules {
            return Err(corrupt("index column length differs from granule count"));
        }
        bytes.extend_from_slice(&encode_column(column)?);
    }
    Ok(bytes)
}

pub(crate) fn decode_index(
    bytes: &[u8],
    key_types: &[DataType],
    granules: usize,
) -> Result<Vec<ColumnData>> {
    let mut reader = Reader::new(bytes);
    let mut columns = Vec::new();
    for data_type in key_types {
        columns.push(read_column(&mut reader, *data_type, granules)?);
    }
    reader.finish()?;
    Ok(columns)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct PartMeta {
    pub format_version: u32,
    pub rows: u64,
    pub granules: u32,
    pub columns: Vec<ColumnDef>,
    pub codec: Codec,
    #[serde(with = "key_values")]
    pub min_key: Vec<Value>,
    #[serde(with = "key_values")]
    pub max_key: Vec<Value>,
}

impl PartMeta {
    fn validate(&self) -> Result<()> {
        if self.format_version != FORMAT_VERSION {
            return Err(corrupt(format!(
                "unsupported format version {}",
                self.format_version
            )));
        }
        if self.rows == 0 || self.granules == 0 || u64::from(self.granules) > self.rows {
            return Err(corrupt("invalid row or granule count"));
        }
        if self.columns.is_empty() {
            return Err(corrupt("missing columns"));
        }
        for (i, column) in self.columns.iter().enumerate() {
            if column.name.is_empty() || self.columns[..i].iter().any(|c| c.name == column.name) {
                return Err(corrupt("empty or duplicate column name"));
            }
        }
        if self.min_key.is_empty() || self.min_key.len() != self.max_key.len() {
            return Err(corrupt("invalid key dimensions"));
        }
        let mut order = std::cmp::Ordering::Equal;
        for (min, max) in self.min_key.iter().zip(&self.max_key) {
            let comparison = min
                .compare(max)
                .ok_or_else(|| corrupt("key types differ"))?;
            if order == std::cmp::Ordering::Equal {
                order = comparison;
            }
        }
        if order == std::cmp::Ordering::Greater {
            return Err(corrupt("reversed key bounds"));
        }
        Ok(())
    }
}

pub(crate) fn encode_meta(meta: &PartMeta) -> Result<Vec<u8>> {
    meta.validate()?;
    serde_json::to_vec(meta).map_err(|e| corrupt(format!("metadata JSON: {e}")))
}

pub(crate) fn decode_meta(bytes: &[u8]) -> Result<PartMeta> {
    let meta: PartMeta =
        serde_json::from_slice(bytes).map_err(|e| corrupt(format!("metadata JSON: {e}")))?;
    meta.validate()?;
    Ok(meta)
}

#[cfg(test)]
mod tests;

/// JSON-числа не сохраняют NaN и все битовые особенности Float64.
mod key_values {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use types::Value;

    #[derive(Serialize, Deserialize)]
    #[serde(untagged)]
    enum StoredValue {
        FloatBits { float64_bits: u64 },
        Plain(Value),
    }

    pub fn serialize<S: Serializer>(values: &[Value], serializer: S) -> Result<S::Ok, S::Error> {
        values
            .iter()
            .map(|value| match value {
                Value::Float64(number) => StoredValue::FloatBits {
                    float64_bits: number.to_bits(),
                },
                other => StoredValue::Plain(other.clone()),
            })
            .collect::<Vec<_>>()
            .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<Value>, D::Error> {
        Ok(Vec::<StoredValue>::deserialize(deserializer)?
            .into_iter()
            .map(|value| match value {
                StoredValue::FloatBits { float64_bits } => {
                    Value::Float64(f64::from_bits(float64_bits))
                }
                StoredValue::Plain(value) => value,
            })
            .collect())
    }
}
