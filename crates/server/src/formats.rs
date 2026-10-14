//! Форматы HTTP-данных. Весь ввод проверяется до атомарной вставки одного блока.

use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use types::{Block, BlockStream, ColumnDef, DataType, DbError, Result, TableSchema, Value};
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, ToSchema)]
pub enum InputFormat {
    #[default]
    #[serde(rename = "CSV")]
    Csv,
    #[serde(rename = "TSV")]
    Tsv,
    #[serde(rename = "JSONEachRow")]
    JsonEachRow,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, ToSchema)]
pub enum OutputFormat {
    #[serde(rename = "CSV")]
    Csv,
    #[default]
    #[serde(rename = "TSV")]
    Tsv,
    #[serde(rename = "JSONEachRow")]
    JsonEachRow,
    Pretty,
}

impl OutputFormat {
    pub fn content_type(self) -> &'static str {
        match self {
            Self::Csv => "text/csv; charset=utf-8",
            Self::JsonEachRow => "application/x-ndjson; charset=utf-8",
            Self::Tsv => "text/tab-separated-values; charset=utf-8",
            Self::Pretty => "text/plain; charset=utf-8",
        }
    }
}

fn bad(row: usize, message: impl Into<String>) -> DbError {
    DbError::BadInput {
        row,
        message: message.into(),
    }
}

/// CSV и TSV не имеют заголовка; номер ошибки — номер записи с единицы.
pub fn parse_input(format: InputFormat, body: &[u8], schema: &TableSchema) -> Result<Block> {
    let mut columns = Block::empty(schema).into_columns();
    if body.is_empty() {
        return Block::new(columns);
    }
    let mut append = |row: usize, fields: Vec<String>| -> Result<()> {
        if fields.len() != columns.len() {
            return Err(bad(
                row,
                format!("expected {} fields, got {}", columns.len(), fields.len()),
            ));
        }
        for ((column, definition), field) in columns.iter_mut().zip(&schema.columns).zip(fields) {
            let value = Value::parse(&field, definition.data_type)
                .map_err(|error| bad(row, format!("column {}: {error}", definition.name)))?;
            column.data.push(value)?;
        }
        Ok(())
    };
    match format {
        InputFormat::Csv => {
            let mut reader = csv::ReaderBuilder::new()
                .has_headers(false)
                .flexible(true)
                .from_reader(body);
            for (index, record) in reader.records().enumerate() {
                let record = record.map_err(|error| bad(index + 1, error.to_string()))?;
                append(index + 1, record.iter().map(str::to_owned).collect())?;
            }
        }
        InputFormat::Tsv | InputFormat::JsonEachRow => {
            let text = std::str::from_utf8(body).map_err(|error| {
                let row = body[..error.valid_up_to()]
                    .iter()
                    .filter(|&&byte| byte == b'\n')
                    .count()
                    + 1;
                bad(row, "input is not UTF-8")
            })?;
            for (index, line) in text
                .strip_suffix('\n')
                .unwrap_or(text)
                .split('\n')
                .enumerate()
            {
                let row = index + 1;
                let line = line.strip_suffix('\r').unwrap_or(line);
                let fields = match format {
                    InputFormat::Tsv => line
                        .split('\t')
                        .map(|field| unescape(field, row))
                        .collect::<Result<Vec<_>>>()?,
                    InputFormat::JsonEachRow => json_fields(line, schema, row)?,
                    InputFormat::Csv => unreachable!(),
                };
                append(row, fields)?;
            }
        }
    }
    Block::new(columns)
}

struct JsonRow(BTreeMap<String, serde_json::Value>);

impl<'de> Deserialize<'de> for JsonRow {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct RowVisitor;
        impl<'de> Visitor<'de> for RowVisitor {
            type Value = JsonRow;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an object with unique column names")
            }
            fn visit_map<M: MapAccess<'de>>(
                self,
                mut access: M,
            ) -> std::result::Result<JsonRow, M::Error> {
                let mut fields = BTreeMap::new();
                while let Some((key, value)) = access.next_entry::<String, serde_json::Value>()? {
                    if fields.insert(key.clone(), value).is_some() {
                        return Err(serde::de::Error::custom(format!("duplicate column {key}")));
                    }
                }
                Ok(JsonRow(fields))
            }
        }
        deserializer.deserialize_map(RowVisitor)
    }
}

fn json_fields(line: &str, schema: &TableSchema, row: usize) -> Result<Vec<String>> {
    let JsonRow(mut values) =
        serde_json::from_str(line).map_err(|error| bad(row, error.to_string()))?;
    if values.len() != schema.columns.len()
        || values.keys().any(|name| schema.column(name).is_none())
    {
        return Err(bad(row, "JSON keys must match the table schema"));
    }
    schema
        .columns
        .iter()
        .map(|column| match values.remove(&column.name) {
            Some(serde_json::Value::String(text)) => Ok(text),
            Some(serde_json::Value::Number(number))
                if matches!(
                    column.data_type,
                    DataType::Int64 | DataType::UInt64 | DataType::Float64
                ) =>
            {
                Ok(number.to_string())
            }
            _ => Err(bad(
                row,
                format!(
                    "column {}: expected a string or a compatible number, NULL is not supported",
                    column.name
                ),
            )),
        })
        .collect()
}

fn unescape(field: &str, row: usize) -> Result<String> {
    let mut text = String::new();
    let mut chars = field.chars();
    while let Some(character) = chars.next() {
        text.push(if character == '\\' {
            match chars.next() {
                Some('\\') => '\\',
                Some('t') => '\t',
                Some('n') => '\n',
                Some('r') => '\r',
                Some('0') => '\0',
                _ => return Err(bad(row, "unknown or incomplete TSV escape")),
            }
        } else {
            character
        });
    }
    Ok(text)
}

fn escape(text: &str) -> String {
    let mut output = String::new();
    for character in text.chars() {
        match character {
            '\\' => output.push_str("\\\\"),
            '\t' => output.push_str("\\t"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\0' => output.push_str("\\0"),
            other => output.push(other),
        }
    }
    output
}

fn json_value(value: Value) -> serde_json::Value {
    match value {
        Value::Int64(number) => number.into(),
        Value::UInt64(number) => number.into(),
        Value::Float64(number) if number.is_finite() => serde_json::json!(number),
        other => other.to_string().into(),
    }
}

/// Обходит ленивый поток до отправки HTTP-заголовков: ошибки не дают частичного ответа.
pub fn format_output(
    format: OutputFormat,
    header: &[ColumnDef],
    stream: BlockStream,
) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut pretty = Vec::new();
    for block in stream {
        let block = block?;
        if block.header() != header {
            return Err(DbError::Internal(
                "query block differs from output header".into(),
            ));
        }
        match format {
            OutputFormat::Csv => {
                let mut writer = csv::WriterBuilder::new()
                    .has_headers(false)
                    .from_writer(&mut output);
                for row in 0..block.rows() {
                    writer
                        .write_record(
                            block
                                .columns()
                                .iter()
                                .map(|column| column.data.get(row).to_string()),
                        )
                        .map_err(|error| DbError::Internal(format!("CSV output: {error}")))?;
                }
                writer.flush()?;
            }
            OutputFormat::Tsv => {
                for row in 0..block.rows() {
                    let fields: Vec<_> = block
                        .columns()
                        .iter()
                        .map(|column| escape(&column.data.get(row).to_string()))
                        .collect();
                    output.extend_from_slice(fields.join("\t").as_bytes());
                    output.push(b'\n');
                }
            }
            OutputFormat::JsonEachRow => {
                for row in 0..block.rows() {
                    let object: serde_json::Map<_, _> = block
                        .columns()
                        .iter()
                        .map(|column| (column.name.clone(), json_value(column.data.get(row))))
                        .collect();
                    serde_json::to_writer(&mut output, &object)
                        .map_err(|error| DbError::Internal(error.to_string()))?;
                    output.push(b'\n');
                }
            }
            OutputFormat::Pretty => {
                for row in 0..block.rows() {
                    pretty.push(
                        block
                            .columns()
                            .iter()
                            .map(|column| escape(&column.data.get(row).to_string()))
                            .collect::<Vec<_>>(),
                    );
                }
            }
        }
    }
    if matches!(format, OutputFormat::Pretty) {
        let mut widths: Vec<_> = header
            .iter()
            .map(|column| escape(&column.name).chars().count())
            .collect();
        for row in &pretty {
            for (width, field) in widths.iter_mut().zip(row) {
                *width = (*width).max(field.chars().count());
            }
        }
        let border = format!(
            "+{}+\n",
            widths
                .iter()
                .map(|width| "-".repeat(width + 2))
                .collect::<Vec<_>>()
                .join("+")
        );
        output.extend_from_slice(border.as_bytes());
        let names: Vec<_> = header.iter().map(|column| escape(&column.name)).collect();
        for (index, row) in std::iter::once(&names).chain(&pretty).enumerate() {
            let fields: Vec<_> = row
                .iter()
                .zip(&widths)
                .map(|(field, width)| {
                    format!(" {field}{} ", " ".repeat(width - field.chars().count()))
                })
                .collect();
            output.extend_from_slice(format!("|{}|\n", fields.join("|")).as_bytes());
            if index == 0 {
                output.extend_from_slice(border.as_bytes());
            }
        }
        if !pretty.is_empty() {
            output.extend_from_slice(border.as_bytes());
        }
    }
    Ok(output)
}
