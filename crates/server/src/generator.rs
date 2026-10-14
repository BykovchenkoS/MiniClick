//! Воспроизводимые данные всех шести типов для проверки индекса и форматов.

use std::io::Write;
use types::{DataType, DbError, Result, Value};

pub const CREATE_SQL: &str = "CREATE TABLE hits (event_date Date, user_id UInt64, delta Int64, amount Float64, url String, event_time DateTime, event_date_text String) ENGINE = MergeTree ORDER BY (event_date, user_id)";

/// Текстовая копия даты позволяет сравнить одинаковые выборки с индексом и без него.
pub fn generate_csv(writer: impl Write, rows: u64, start_row: u64) -> Result<()> {
    let Value::Date(base) = Value::parse("2026-10-01", DataType::Date)? else {
        return Err(DbError::Internal("invalid generator date".into()));
    };
    let end = start_row
        .checked_add(rows)
        .ok_or_else(|| DbError::Syntax("generator row range overflows u64".into()))?;
    let mut writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(writer);
    for index in start_row..end {
        let day = base + (index % 30) as i32;
        writer
            .write_record([
                Value::Date(day).to_string(),
                (index % 100_000).to_string(),
                ((index % 201) as i64 - 100).to_string(),
                ((index % 10_000) as f64 / 100.0).to_string(),
                format!("/page/{}", index % 1000),
                Value::DateTime(i64::from(day) * 86_400 + (index % 86_400) as i64).to_string(),
                Value::Date(day).to_string(),
            ])
            .map_err(|error| DbError::Internal(format!("generator CSV: {error}")))?;
    }
    writer.flush()?;
    Ok(())
}
