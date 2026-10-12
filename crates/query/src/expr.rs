//! Проверка типов выражений, арифметика, проекция и маска WHERE
//! Bool нужен только внутри Query: общий DataType не меняем
use std::cmp::Ordering;
use types::{
    BinaryOp, Block, ColumnData, DataType, DbError, Expr, Result, TableSchema, UnaryOp, Value,
};

#[derive(Debug, PartialEq, Eq)]
enum ExprType {
    Scalar(DataType),
    Bool,
}

fn numeric(t: DataType) -> bool {
    matches!(t, DataType::Int64 | DataType::UInt64 | DataType::Float64)
}

fn mismatch(message: &str) -> DbError {
    DbError::TypeMismatch(message.into())
}

pub(crate) fn bind_predicate(mut expr: Expr, schema: &TableSchema) -> Result<Expr> {
    if bind(&mut expr, schema)? != ExprType::Bool {
        return Err(mismatch(
            "WHERE должен содержать логическое условие, например id > 0",
        ));
    }
    Ok(expr)
}

pub(crate) fn bind_scalar(mut expr: Expr, schema: &TableSchema) -> Result<(Expr, DataType)> {
    match bind(&mut expr, schema)? {
        ExprType::Scalar(t) => Ok((expr, t)),
        ExprType::Bool => Err(mismatch(
            "логическое выражение в SELECT пока не поддерживается: в общих типах нет Bool",
        )),
    }
}

fn arithmetic_type(op: BinaryOp, left: DataType, right: DataType) -> Result<DataType> {
    if !numeric(left) || !numeric(right) {
        return Err(mismatch(
            "арифметика поддерживает только Int64, UInt64 и Float64",
        ));
    }
    Ok(
        if op == BinaryOp::Div || left == DataType::Float64 || right == DataType::Float64 {
            DataType::Float64
        } else if left == DataType::UInt64 && right == DataType::UInt64 {
            DataType::UInt64
        } else {
            DataType::Int64
        },
    )
}

fn bind(expr: &mut Expr, schema: &TableSchema) -> Result<ExprType> {
    use BinaryOp::*;
    match expr {
        Expr::Column(name) => schema
            .column(name)
            .map(|c| ExprType::Scalar(c.data_type))
            .ok_or_else(|| DbError::UnknownColumn(name.clone())),
        Expr::Literal(value) => Ok(ExprType::Scalar(value.data_type())),
        Expr::Unary {
            op: UnaryOp::Neg,
            expr,
        } => match bind(expr, schema)? {
            ExprType::Scalar(DataType::Float64) => Ok(ExprType::Scalar(DataType::Float64)),
            ExprType::Scalar(DataType::Int64 | DataType::UInt64) => {
                Ok(ExprType::Scalar(DataType::Int64))
            }
            _ => Err(mismatch("унарный минус ожидает число")),
        },
        Expr::Binary {
            op: op @ (Add | Sub | Mul | Div),
            left,
            right,
        } => {
            let lt = bind(left, schema)?;
            let rt = bind(right, schema)?;
            match (lt, rt) {
                (ExprType::Scalar(a), ExprType::Scalar(b)) => {
                    Ok(ExprType::Scalar(arithmetic_type(*op, a, b)?))
                }
                _ => Err(mismatch("арифметика не принимает логические значения")),
            }
        }
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => {
            if bind(expr, schema)? != ExprType::Bool {
                return Err(mismatch("NOT ожидает логическое условие"));
            }
            Ok(ExprType::Bool)
        }
        Expr::Binary {
            op: And | Or,
            left,
            right,
        } => {
            // Проверяем обе стороны до чтения, короткое замыкание не скрывает ошибки типов/имён
            let left_type = bind(left, schema)?;
            let right_type = bind(right, schema)?;
            if left_type != ExprType::Bool || right_type != ExprType::Bool {
                return Err(mismatch("AND и OR ожидают два логических условия"));
            }
            Ok(ExprType::Bool)
        }
        Expr::Binary {
            op: Eq | NotEq | Lt | LtEq | Gt | GtEq,
            left,
            right,
        } => {
            let mut lt = bind(left, schema)?;
            let mut rt = bind(right, schema)?;
            if let (Expr::Literal(value), ExprType::Scalar(target)) = (&mut **right, &lt) {
                cast_literal(value, *target)?;
                rt = ExprType::Scalar(value.data_type());
            } else if let (Expr::Literal(value), ExprType::Scalar(target)) = (&mut **left, &rt) {
                cast_literal(value, *target)?;
                lt = ExprType::Scalar(value.data_type());
            }
            match (lt, rt) {
                (ExprType::Scalar(a), ExprType::Scalar(b))
                    if a == b || (numeric(a) && numeric(b)) =>
                {
                    Ok(ExprType::Bool)
                }
                _ => Err(mismatch("сравнение требует совместимых скалярных типов")),
            }
        }
        _ => Err(DbError::Unsupported(
            "выражение WHERE пока не поддерживается".into(),
        )),
    }
}

fn cast_literal(value: &mut Value, target: DataType) -> Result<()> {
    if let Some(cast) = value.cast(target) {
        *value = cast;
        Ok(())
    } else if numeric(value.data_type()) && numeric(target) {
        Ok(())
    } else {
        Err(mismatch(&format!(
            "литерал '{value}' нельзя привести к {target}"
        )))
    }
}

pub(crate) fn filter(block: &Block, predicate: &Expr) -> Result<Block> {
    let mask = (0..block.rows())
        .map(|row| eval_bool(predicate, block, row))
        .collect::<Result<Vec<_>>>()?;
    Ok(block.filter(&mask))
}

fn eval_bool(expr: &Expr, block: &Block, row: usize) -> Result<bool> {
    use BinaryOp::*;
    match expr {
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => Ok(!eval_bool(expr, block, row)?),
        Expr::Binary {
            op: And,
            left,
            right,
        } => Ok(eval_bool(left, block, row)? && eval_bool(right, block, row)?),
        Expr::Binary {
            op: Or,
            left,
            right,
        } => Ok(eval_bool(left, block, row)? || eval_bool(right, block, row)?),
        Expr::Binary { op, left, right } => {
            let order = compare(&scalar(left, block, row)?, &scalar(right, block, row)?)?;
            Ok(match op {
                Eq => order == Ordering::Equal,
                NotEq => order != Ordering::Equal,
                Lt => order == Ordering::Less,
                LtEq => order != Ordering::Greater,
                Gt => order == Ordering::Greater,
                GtEq => order != Ordering::Less,
                _ => return Err(DbError::Internal("непроверенный оператор WHERE".into())),
            })
        }
        _ => Err(DbError::Internal(
            "непроверенное логическое выражение".into(),
        )),
    }
}

fn scalar(expr: &Expr, block: &Block, row: usize) -> Result<Value> {
    match expr {
        Expr::Literal(value) => Ok(value.clone()),
        Expr::Column(name) => block
            .column(name)
            .map(|column| column.get(row))
            .ok_or_else(|| DbError::Internal(format!("scan не вернул колонку выражения '{name}'"))),
        Expr::Unary {
            op: UnaryOp::Neg,
            expr,
        } => negate(scalar(expr, block, row)?),
        Expr::Binary {
            op: op @ (BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div),
            left,
            right,
        } => arithmetic(*op, scalar(left, block, row)?, scalar(right, block, row)?),
        _ => Err(DbError::Internal(
            "непроверенное скалярное выражение".into(),
        )),
    }
}

/// Вычисление скалярной проекции. Для простой колонки сохраняем быстрый путь
pub(crate) fn project(block: &Block, expr: &Expr, data_type: DataType) -> Result<ColumnData> {
    if let Expr::Column(name) = expr {
        return block
            .column(name)
            .cloned()
            .ok_or_else(|| DbError::Internal(format!("scan не вернул колонку '{name}'")));
    }
    let mut result = ColumnData::empty(data_type);
    for row in 0..block.rows() {
        result.push(scalar(expr, block, row)?)?;
    }
    Ok(result)
}

fn negate(value: Value) -> Result<Value> {
    match value {
        Value::Int64(x) => x
            .checked_neg()
            .map(Value::Int64)
            .ok_or_else(|| mismatch("переполнение Int64 при унарном минусе")),
        Value::UInt64(x) => i64::try_from(-(x as i128))
            .map(Value::Int64)
            .map_err(|_| mismatch("результат унарного минуса не помещается в Int64")),
        Value::Float64(x) if x.is_finite() => Ok(Value::Float64(-x)),
        _ => Err(mismatch("унарный минус ожидает конечное число")),
    }
}

fn as_float(value: &Value) -> Result<f64> {
    match value.cast(DataType::Float64) {
        Some(Value::Float64(x)) if x.is_finite() => Ok(x),
        _ => Err(mismatch(
            "арифметика Float64 требует конечного числа и точного приведения целого (до 2^53)",
        )),
    }
}

fn as_integer(value: &Value) -> Result<i128> {
    match value {
        Value::Int64(x) => Ok(*x as i128),
        Value::UInt64(x) => Ok(*x as i128),
        _ => Err(mismatch("ожидалось целое число")),
    }
}

fn arithmetic(op: BinaryOp, left: Value, right: Value) -> Result<Value> {
    use BinaryOp::*;
    let target = arithmetic_type(op, left.data_type(), right.data_type())?;
    let overflow = || mismatch("переполнение при вычислении арифметического выражения");
    match target {
        DataType::Float64 => {
            let (a, b) = (as_float(&left)?, as_float(&right)?);
            if op == Div && b == 0.0 {
                return Err(mismatch("деление на ноль"));
            }
            let value = match op {
                Add => a + b,
                Sub => a - b,
                Mul => a * b,
                Div => a / b,
                _ => {
                    return Err(DbError::Internal(
                        "неизвестная арифметическая операция".into(),
                    ))
                }
            };
            if !value.is_finite() {
                return Err(overflow());
            }
            Ok(Value::Float64(value))
        }
        DataType::UInt64 => {
            let (Value::UInt64(a), Value::UInt64(b)) = (left, right) else {
                return Err(DbError::Internal("непроверенные типы UInt64".into()));
            };
            let value = match op {
                Add => a.checked_add(b),
                Sub => a.checked_sub(b),
                Mul => a.checked_mul(b),
                _ => None,
            };
            value.map(Value::UInt64).ok_or_else(overflow)
        }
        DataType::Int64 => {
            let (a, b) = (as_integer(&left)?, as_integer(&right)?);
            let value = match op {
                Add => a.checked_add(b),
                Sub => a.checked_sub(b),
                Mul => a.checked_mul(b),
                _ => None,
            };
            value
                .and_then(|v| i64::try_from(v).ok())
                .map(Value::Int64)
                .ok_or_else(overflow)
        }
        _ => Err(DbError::Internal("непроверенный тип арифметики".into())),
    }
}

fn compare(left: &Value, right: &Value) -> Result<Ordering> {
    if let Some(order) = left.compare(right) {
        return Ok(order);
    }
    use Value::*;
    let order = match (left, right) {
        (Int64(a), UInt64(b)) => (*a as i128).cmp(&(*b as i128)),
        (UInt64(a), Int64(b)) => (*a as i128).cmp(&(*b as i128)),
        (Int64(a), Float64(b)) => compare_int_float(*a as i128, *b),
        (UInt64(a), Float64(b)) => compare_int_float(*a as i128, *b),
        (Float64(a), Int64(b)) => compare_int_float(*b as i128, *a).reverse(),
        (Float64(a), UInt64(b)) => compare_int_float(*b as i128, *a).reverse(),
        _ => return Err(mismatch("несовместимые типы сравнения")),
    };
    Ok(order)
}

/// Точное сравнение i64/u64 с f64 без преобразования большого целого в f64
/// После проверки границ дробь безопасно усекается до i128, остаток определяет порядок
fn compare_int_float(integer: i128, float: f64) -> Ordering {
    if float.is_nan() {
        return if float.is_sign_negative() {
            Ordering::Greater
        } else {
            Ordering::Less
        };
    }
    if float < -9_223_372_036_854_775_808.0 {
        return Ordering::Greater;
    }
    if float >= 18_446_744_073_709_551_616.0 {
        return Ordering::Less;
    }
    match integer.cmp(&(float.trunc() as i128)) {
        Ordering::Equal if float.fract() > 0.0 => Ordering::Less,
        Ordering::Equal if float.fract() < 0.0 => Ordering::Greater,
        order => order,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compares_large_numbers_without_rounding_integer_to_float() {
        let cases = [
            (
                Value::UInt64(9_007_199_254_740_993),
                Value::Float64(9_007_199_254_740_992.0),
                Ordering::Greater,
            ),
            (
                Value::UInt64(u64::MAX),
                Value::Float64(18_446_744_073_709_551_616.0),
                Ordering::Less,
            ),
            (
                Value::Int64(i64::MAX),
                Value::Float64(9_223_372_036_854_775_808.0),
                Ordering::Less,
            ),
            (
                Value::Int64(i64::MIN),
                Value::Float64(-9_223_372_036_854_775_808.0),
                Ordering::Equal,
            ),
            (Value::UInt64(0), Value::Int64(-1), Ordering::Greater),
            (Value::Int64(-2), Value::Float64(-2.5), Ordering::Greater),
            (Value::UInt64(2), Value::Float64(2.5), Ordering::Less),
            (
                Value::Int64(0),
                Value::Float64(f64::INFINITY),
                Ordering::Less,
            ),
            (
                Value::Int64(0),
                Value::Float64(f64::NEG_INFINITY),
                Ordering::Greater,
            ),
            (Value::Int64(0), Value::Float64(f64::NAN), Ordering::Less),
        ];
        for (a, b, expected) in cases {
            assert_eq!(compare(&a, &b).unwrap(), expected, "{a:?} vs {b:?}");
            assert_eq!(compare(&b, &a).unwrap(), expected.reverse());
        }
    }

    #[test]
    fn fractional_comparisons_match_small_exact_numbers() {
        for i in -20..=20 {
            for numerator in -80..=80 {
                let f = numerator as f64 / 4.0;
                let expected = (i as f64).partial_cmp(&f).unwrap();
                assert_eq!(compare_int_float(i, f), expected);
            }
        }
    }
}
