use crate::error::{DbError, Result};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

/// Типы колонок. Nullable, Array, Decimal — позже
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Int64,
    UInt64,
    Float64,
    String,
    /// Дни с 1970-01-01
    Date,
    /// Секунды с 1970-01-01 00:00:00 UTC
    DateTime,
}

impl DataType {
    /// Имя как в SQL: CREATE TABLE t (x Int64, ...)
    pub fn name(&self) -> &'static str {
        match self {
            DataType::Int64 => "Int64",
            DataType::UInt64 => "UInt64",
            DataType::Float64 => "Float64",
            DataType::String => "String",
            DataType::Date => "Date",
            DataType::DateTime => "DateTime",
        }
    }

    pub fn from_name(s: &str) -> Option<DataType> {
        Some(match s {
            "Int64" => DataType::Int64,
            "UInt64" => DataType::UInt64,
            "Float64" => DataType::Float64,
            "String" => DataType::String,
            "Date" => DataType::Date,
            "DateTime" => DataType::DateTime,
            _ => return None,
        })
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Одно значение: литерал в выражении, min/max part, значение ключа в индексе
/// NULL пока нет
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    String(String),
    Date(i32),
    DateTime(i64),
}

impl Value {
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Int64(_) => DataType::Int64,
            Value::UInt64(_) => DataType::UInt64,
            Value::Float64(_) => DataType::Float64,
            Value::String(_) => DataType::String,
            Value::Date(_) => DataType::Date,
            Value::DateTime(_) => DataType::DateTime,
        }
    }

    /// Сравнение значений одного типа. Для разных типов — None
    /// приведение типов делает Query до того, как значения сюда попадут
    /// Float сравнивается через total_cmp, чтобы сортировка не ломалась на NaN
    pub fn compare(&self, other: &Value) -> Option<Ordering> {
        use Value::*;
        Some(match (self, other) {
            (Int64(a), Int64(b)) => a.cmp(b),
            (UInt64(a), UInt64(b)) => a.cmp(b),
            (Float64(a), Float64(b)) => a.total_cmp(b),
            (String(a), String(b)) => a.cmp(b),
            (Date(a), Date(b)) => a.cmp(b),
            (DateTime(a), DateTime(b)) => a.cmp(b),
            _ => return None,
        })
    }

    /// Разбор текста в значение заданного типа. Один и тот же для литералов SQL
    /// (блок B) и для полей CSV/TSV/JSON (блок C), чтобы даты везде читались одинаково
    ///
    /// Date: `YYYY-MM-DD`. DateTime: `YYYY-MM-DD hh:mm:ss` (или с `T`), либо `YYYY-MM-DD`
    /// (полночь). Время в UTC. Пробелы по краям игнорируются, кроме String
    pub fn parse(text: &str, t: DataType) -> Result<Value> {
        let bad = || DbError::TypeMismatch(format!("cannot parse '{text}' as {t}"));
        let s = text.trim();
        Ok(match t {
            DataType::Int64 => Value::Int64(s.parse().map_err(|_| bad())?),
            DataType::UInt64 => Value::UInt64(s.parse().map_err(|_| bad())?),
            DataType::Float64 => Value::Float64(s.parse().map_err(|_| bad())?),
            DataType::String => Value::String(text.to_string()),
            DataType::Date => Value::Date(parse_date(s).ok_or_else(bad)?),
            DataType::DateTime => {
                let (date, time) = match s.split_once([' ', 'T']) {
                    Some((d, t)) => (d, Some(t)),
                    None => (s, None),
                };
                let days = parse_date(date).ok_or_else(bad)? as i64;
                let secs = match time {
                    Some(t) => parse_time(t).ok_or_else(bad)?,
                    None => 0,
                };
                Value::DateTime(days * 86_400 + secs)
            }
        })
    }

    /// Приведение литерала к типу колонки без потери смысла
    /// None привести нельзя (например, 2.5 к Int64, -1 к UInt64, 'abc' к Date)
    pub fn cast(&self, t: DataType) -> Option<Value> {
        const MAX_EXACT_F64: u64 = 1 << 53;
        if self.data_type() == t {
            return Some(self.clone());
        }
        match (self, t) {
            (Value::String(s), _) => Value::parse(s, t).ok(),
            (Value::Int64(x), DataType::UInt64) => u64::try_from(*x).ok().map(Value::UInt64),
            (Value::Int64(x), DataType::Float64) if x.unsigned_abs() <= MAX_EXACT_F64 => {
                Some(Value::Float64(*x as f64))
            }
            (Value::UInt64(x), DataType::Int64) => i64::try_from(*x).ok().map(Value::Int64),
            (Value::UInt64(x), DataType::Float64) if *x <= MAX_EXACT_F64 => {
                Some(Value::Float64(*x as f64))
            }
            (Value::Float64(x), DataType::Int64)
                if x.fract() == 0.0
                    && *x >= -(MAX_EXACT_F64 as f64)
                    && *x <= MAX_EXACT_F64 as f64 =>
            {
                Some(Value::Int64(*x as i64))
            }
            (Value::Float64(x), DataType::UInt64)
                if x.fract() == 0.0 && *x >= 0.0 && *x <= MAX_EXACT_F64 as f64 =>
            {
                Some(Value::UInt64(*x as u64))
            }
            (Value::Date(d), DataType::DateTime) => Some(Value::DateTime(*d as i64 * 86_400)),
            _ => None,
        }
    }
}

/// Текстовый вид значения для форматов вывода
/// даты как `YYYY-MM-DD` и `YYYY-MM-DD hh:mm:ss`, строки без кавычек
impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Int64(x) => write!(f, "{x}"),
            Value::UInt64(x) => write!(f, "{x}"),
            Value::Float64(x) => write!(f, "{x}"),
            Value::String(s) => f.write_str(s),
            Value::Date(d) => {
                let (y, m, d) = civil_from_days(*d as i64);
                write!(f, "{y:04}-{m:02}-{d:02}")
            }
            Value::DateTime(t) => {
                let (days, secs) = (t.div_euclid(86_400), t.rem_euclid(86_400));
                let (y, m, d) = civil_from_days(days);
                let (h, mi, s) = (secs / 3600, secs / 60 % 60, secs % 60);
                write!(f, "{y:04}-{m:02}-{d:02} {h:02}:{mi:02}:{s:02}")
            }
        }
    }
}

/// `YYYY-MM-DD` дни с 1970-01-01. Несуществующая дата (2023-02-29) None
fn parse_date(s: &str) -> Option<i32> {
    let mut it = s.splitn(3, '-');
    let y: i64 = it.next()?.parse().ok()?;
    let m: i64 = it.next()?.parse().ok()?;
    let d: i64 = it.next()?.parse().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    let days = days_from_civil(y, m, d);
    // Обратное преобразование отсекает 31 апреля и 29 февраля в невисокосный год.
    (civil_from_days(days) == (y, m, d))
        .then_some(days)
        .and_then(|x| i32::try_from(x).ok())
}

/// `hh:mm:ss` секунды от начала суток.
fn parse_time(s: &str) -> Option<i64> {
    let mut it = s.splitn(3, ':');
    let h: i64 = it.next()?.parse().ok()?;
    let m: i64 = it.next()?.parse().ok()?;
    let sec: i64 = it.next()?.parse().ok()?;
    ((0..24).contains(&h) && (0..60).contains(&m) && (0..60).contains(&sec))
        .then_some(h * 3600 + m * 60 + sec)
}

// Алгоритмы Говарда Хиннанта (days_from_civil / civil_from_days), пролептический григорианский календарь
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (
        if m <= 2 {
            yoe + era * 400 + 1
        } else {
            yoe + era * 400
        },
        m,
        d,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates() {
        assert_eq!(
            Value::parse("1970-01-01", DataType::Date).unwrap(),
            Value::Date(0)
        );
        assert_eq!(
            Value::parse("1969-12-31", DataType::Date).unwrap(),
            Value::Date(-1)
        );
        assert_eq!(
            Value::parse("2000-03-01", DataType::Date).unwrap(),
            Value::Date(11017)
        );
        assert!(Value::parse("2000-02-29", DataType::Date).is_ok());
        assert!(Value::parse("2023-02-29", DataType::Date).is_err());
        assert!(Value::parse("2023-04-31", DataType::Date).is_err());
        assert!(Value::parse("2023-13-01", DataType::Date).is_err());
        for s in ["1970-01-01", "2026-10-11", "1900-03-01", "2400-02-29"] {
            assert_eq!(Value::parse(s, DataType::Date).unwrap().to_string(), s);
        }
    }

    #[test]
    fn datetimes() {
        let v = Value::parse("2026-10-11 13:05:09", DataType::DateTime).unwrap();
        assert_eq!(v.to_string(), "2026-10-11 13:05:09");
        assert_eq!(
            Value::parse("2026-10-11T13:05:09", DataType::DateTime).unwrap(),
            v
        );
        assert_eq!(
            Value::parse("1970-01-02", DataType::DateTime).unwrap(),
            Value::DateTime(86_400)
        );
        assert_eq!(Value::DateTime(-1).to_string(), "1969-12-31 23:59:59");
        assert!(Value::parse("2026-10-11 24:00:00", DataType::DateTime).is_err());
    }

    #[test]
    fn casts() {
        let s = |x: &str| Value::String(x.into());
        assert_eq!(
            s("2026-10-01").cast(DataType::Date),
            Value::parse("2026-10-01", DataType::Date).ok()
        );
        assert_eq!(s("abc").cast(DataType::Date), None);
        assert_eq!(
            Value::Int64(5).cast(DataType::Float64),
            Some(Value::Float64(5.0))
        );
        assert_eq!(Value::Int64(-1).cast(DataType::UInt64), None);
        assert_eq!(
            Value::Float64(2.0).cast(DataType::Int64),
            Some(Value::Int64(2))
        );
        assert_eq!(Value::Float64(2.5).cast(DataType::Int64), None);
        assert_eq!(Value::UInt64(u64::MAX).cast(DataType::Int64), None);
        assert_eq!(
            Value::Date(1).cast(DataType::DateTime),
            Some(Value::DateTime(86_400))
        );
        assert_eq!(Value::Int64(1).cast(DataType::String), None);
        assert_eq!(Value::Int64(1).cast(DataType::Date), None);
    }
}
