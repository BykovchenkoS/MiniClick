//! Анализ условия WHERE для отсечения parts и гранул. Владелец — блок B,
//! вызывает storage. Лежит в types, потому что storage не зависит от query.

use crate::expr::{BinaryOp, Expr};
use crate::value::Value;
use std::cmp::Ordering;

/// Диапазон ключа ORDER BY, границы включительно. Значения по колонкам ключа
/// в их порядке. Для part — его min_key/max_key; для гранулы i —
/// ключ первой строки гранулы i и ключ первой строки гранулы i + 1
/// (для последней гранулы — max_key part).
#[derive(Debug, Clone, PartialEq)]
pub struct KeyRange {
    pub min: Vec<Value>,
    pub max: Vec<Value>,
}

/// Может ли условие быть истинным хотя бы для одной строки, ключ которой лежит в range.
///
/// Ответ консервативный: `true` значит «может быть», `false` — «точно нет».
/// Ошибиться в сторону `true` можно всегда (лишние гранулы прочитаются и
/// отфильтруются в Query), в сторону `false` — никогда.
///
/// Минимум для нашей реализации: сравнения первой колонки ключа с литералом
/// (=, <, <=, >, >=), AND и OR. Всё остальное — `true`.
pub fn may_match(predicate: &Expr, key_columns: &[String], range: &KeyRange) -> bool {
    if !valid_range(key_columns, range) {
        return true;
    }
    check(predicate, &key_columns[0], &range.min[0], &range.max[0])
}

/// Границы — кортежи, упорядоченные лексикографически, а не независимые
/// min/max каждого поля. Например, [(1, 100), (2, 0)] — корректный диапазон.
fn valid_range(columns: &[String], range: &KeyRange) -> bool {
    if columns.is_empty() || range.min.len() != columns.len() || range.max.len() != columns.len() {
        return false;
    }
    for (i, name) in columns.iter().enumerate() {
        if name.is_empty() || columns[..i].contains(name) {
            return false;
        }
    }
    let mut lex_order = Ordering::Equal;
    for (min, max) in range.min.iter().zip(&range.max) {
        if !finite(min) || !finite(max) {
            return false;
        }
        let Some(order) = min.compare(max) else {
            return false;
        };
        if lex_order == Ordering::Equal {
            lex_order = order;
        }
    }
    lex_order != Ordering::Greater
}

fn finite(value: &Value) -> bool {
    !matches!(value, Value::Float64(x) if !x.is_finite())
}

fn check(predicate: &Expr, key: &str, min: &Value, max: &Value) -> bool {
    use BinaryOp::*;
    match predicate {
        Expr::Binary {
            op: And,
            left,
            right,
        } => check(left, key, min, max) && check(right, key, min, max),
        Expr::Binary {
            op: Or,
            left,
            right,
        } => check(left, key, min, max) || check(right, key, min, max),
        Expr::Binary {
            op: op @ (Eq | Lt | LtEq | Gt | GtEq),
            left,
            right,
        } => {
            let (op, literal) = match (&**left, &**right) {
                (Expr::Column(name), Expr::Literal(value)) if name == key => (*op, value),
                (Expr::Literal(value), Expr::Column(name)) if name == key => {
                    let reversed = match op {
                        Lt => Gt,
                        LtEq => GtEq,
                        Gt => Lt,
                        GtEq => LtEq,
                        _ => Eq,
                    };
                    (reversed, value)
                }
                _ => return true,
            };
            if !finite(literal) {
                return true;
            }

            let (Some(lo), Some(hi)) = (min.compare(literal), max.compare(literal)) else {
                return true;
            };
            match op {
                Eq => lo != Ordering::Greater && hi != Ordering::Less,
                Lt => lo == Ordering::Less,
                LtEq => lo != Ordering::Greater,
                Gt => hi == Ordering::Greater,
                GtEq => hi != Ordering::Less,
                _ => true,
            }
        }
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::UnaryOp;

    fn range(lo: i64, hi: i64) -> KeyRange {
        KeyRange {
            min: vec![Value::Int64(lo)],
            max: vec![Value::Int64(hi)],
        }
    }
    fn condition(op: BinaryOp, value: i64) -> Expr {
        Expr::binary(op, Expr::col("id"), Expr::lit(Value::Int64(value)))
    }
    fn matches(expr: &Expr, range: &KeyRange) -> bool {
        may_match(expr, &["id".into()], range)
    }

    #[test]
    fn inclusive_boundaries_for_all_supported_comparisons() {
        use BinaryOp::*;
        let r = range(10, 20);
        for (op, value, expected) in [
            (Eq, 9, false),
            (Eq, 10, true),
            (Eq, 15, true),
            (Eq, 20, true),
            (Eq, 21, false),
            (Lt, 10, false),
            (Lt, 11, true),
            (LtEq, 9, false),
            (LtEq, 10, true),
            (Gt, 20, false),
            (Gt, 19, true),
            (GtEq, 21, false),
            (GtEq, 20, true),
        ] {
            assert_eq!(
                matches(&condition(op, value), &r),
                expected,
                "{op:?} {value}"
            );
        }
    }

    #[test]
    fn literal_on_left_reverses_the_comparison() {
        use BinaryOp::*;
        for (left_op, right_op) in [(Eq, Eq), (Lt, Gt), (LtEq, GtEq), (Gt, Lt), (GtEq, LtEq)] {
            for value in 9..=21 {
                let expr = Expr::binary(left_op, Expr::lit(Value::Int64(value)), Expr::col("id"));
                assert_eq!(
                    matches(&expr, &range(10, 20)),
                    matches(&condition(right_op, value), &range(10, 20))
                );
            }
        }
    }

    #[test]
    fn and_or_with_supported_and_unknown_branches() {
        let no = condition(BinaryOp::Gt, 30);
        let yes = condition(BinaryOp::Eq, 15);
        let unknown = Expr::binary(
            BinaryOp::Eq,
            Expr::col("event"),
            Expr::lit(Value::String("click".into())),
        );
        for (op, a, b, expected) in [
            (BinaryOp::And, no.clone(), unknown.clone(), false),
            (BinaryOp::Or, no.clone(), unknown, true),
            (BinaryOp::And, yes.clone(), no.clone(), false),
            (BinaryOp::Or, yes, no.clone(), true),
            (BinaryOp::Or, no.clone(), no, false),
        ] {
            assert_eq!(matches(&Expr::binary(op, a, b), &range(10, 20)), expected);
        }

        assert!(matches(
            &Expr::binary(
                BinaryOp::And,
                condition(BinaryOp::Lt, 12),
                condition(BinaryOp::Gt, 18)
            ),
            &range(10, 20)
        ));
    }

    #[test]
    fn unsupported_expressions_remain_conservative() {
        let r = range(10, 20);
        for expr in [
            Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(condition(BinaryOp::Eq, 15)),
            },
            condition(BinaryOp::NotEq, 15),
            Expr::binary(BinaryOp::Eq, Expr::col("id"), Expr::col("id")),
            Expr::binary(BinaryOp::Eq, Expr::col("ID"), Expr::lit(Value::Int64(100))),
            Expr::binary(
                BinaryOp::Eq,
                Expr::binary(BinaryOp::Add, Expr::col("id"), Expr::lit(Value::Int64(1))),
                Expr::lit(Value::Int64(100)),
            ),
            Expr::Function {
                name: "unknown".into(),
                args: vec![],
            },
            Expr::lit(Value::Int64(0)),
        ] {
            assert!(matches(&expr, &r), "{expr:?}");
        }
    }

    #[test]
    fn different_literal_types_are_not_cast_by_pruner() {
        for v in [
            Value::UInt64(100),
            Value::Float64(100.0),
            Value::String("100".into()),
        ] {
            assert!(matches(
                &Expr::binary(BinaryOp::Eq, Expr::col("id"), Expr::lit(v)),
                &range(10, 20)
            ));
        }
        let r = KeyRange {
            min: vec![Value::UInt64(0)],
            max: vec![Value::UInt64(20)],
        };
        assert!(matches(&condition(BinaryOp::Gt, -1), &r));
    }

    #[test]
    fn invalid_or_unknown_ranges_are_not_pruned() {
        let no = condition(BinaryOp::Gt, 100);
        for (keys, r) in [
            (vec![], range(1, 2)),
            (
                vec!["id".into()],
                KeyRange {
                    min: vec![],
                    max: vec![],
                },
            ),
            (vec!["id".into()], range(20, 10)),
            (
                vec!["id".into()],
                KeyRange {
                    min: vec![Value::Int64(1)],
                    max: vec![Value::UInt64(2)],
                },
            ),
            (vec!["id".into(), "x".into()], range(1, 2)),
            (
                vec!["id".into(), "id".into()],
                KeyRange {
                    min: vec![Value::Int64(1); 2],
                    max: vec![Value::Int64(2); 2],
                },
            ),
        ] {
            assert!(may_match(&no, &keys, &r));
        }
    }

    #[test]
    fn composite_bounds_are_lexicographic_not_per_column_min_max() {
        let keys = vec!["id".into(), "suffix".into()];
        let r = KeyRange {
            min: vec![Value::Int64(1), Value::Int64(100)],
            max: vec![Value::Int64(2), Value::Int64(0)],
        };
        assert!(!may_match(&condition(BinaryOp::Gt, 2), &keys, &r));
        assert!(may_match(&condition(BinaryOp::Eq, 1), &keys, &r));
        // (1, 500) находится между границами, хотя 500 больше обеих вторых компонент.
        let suffix = Expr::binary(
            BinaryOp::Eq,
            Expr::col("suffix"),
            Expr::lit(Value::Int64(500)),
        );
        assert!(may_match(&suffix, &keys, &r));
    }

    #[test]
    fn supports_string_date_datetime_unsigned_and_float_ranges() {
        for (min, max, outside, boundary) in [
            (
                Value::String("b".into()),
                Value::String("d".into()),
                Value::String("z".into()),
                Value::String("d".into()),
            ),
            (
                Value::Date(10),
                Value::Date(20),
                Value::Date(21),
                Value::Date(20),
            ),
            (
                Value::DateTime(10),
                Value::DateTime(20),
                Value::DateTime(21),
                Value::DateTime(20),
            ),
            (
                Value::UInt64(u64::MAX - 1),
                Value::UInt64(u64::MAX),
                Value::UInt64(0),
                Value::UInt64(u64::MAX),
            ),
            (
                Value::Float64(1.5),
                Value::Float64(2.5),
                Value::Float64(3.0),
                Value::Float64(2.5),
            ),
        ] {
            let r = KeyRange {
                min: vec![min],
                max: vec![max],
            };
            assert!(!matches(
                &Expr::binary(BinaryOp::Eq, Expr::col("id"), Expr::lit(outside)),
                &r
            ));
            assert!(matches(
                &Expr::binary(BinaryOp::Eq, Expr::col("id"), Expr::lit(boundary)),
                &r
            ));
        }
    }

    #[test]
    fn non_finite_floats_disable_pruning() {
        for v in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let r = KeyRange {
                min: vec![Value::Float64(0.0)],
                max: vec![Value::Float64(v)],
            };
            let expr = Expr::binary(
                BinaryOp::Eq,
                Expr::col("id"),
                Expr::lit(Value::Float64(100.0)),
            );
            assert!(matches(&expr, &r));
            let r = KeyRange {
                min: vec![Value::Float64(0.0)],
                max: vec![Value::Float64(1.0)],
            };
            let expr = Expr::binary(BinaryOp::Eq, Expr::col("id"), Expr::lit(Value::Float64(v)));
            assert!(matches(&expr, &r));
        }
    }

    #[test]
    fn signed_zero_uses_the_same_total_order_as_value_compare() {
        let r = KeyRange {
            min: vec![Value::Float64(-0.0)],
            max: vec![Value::Float64(-0.0)],
        };
        let plus = Expr::binary(
            BinaryOp::Eq,
            Expr::col("id"),
            Expr::lit(Value::Float64(0.0)),
        );
        assert!(!matches(&plus, &r));
        let minus = Expr::binary(
            BinaryOp::Eq,
            Expr::col("id"),
            Expr::lit(Value::Float64(-0.0)),
        );
        assert!(matches(&minus, &r));
    }

    fn eval(expr: &Expr, id: i64) -> bool {
        use BinaryOp::*;
        let Expr::Binary { op, left, right } = expr else {
            panic!("test AST")
        };
        if *op == And {
            return eval(left, id) && eval(right, id);
        }
        if *op == Or {
            return eval(left, id) || eval(right, id);
        }
        let value = |e: &Expr| match e {
            Expr::Column(_) => id,
            Expr::Literal(Value::Int64(v)) => *v,
            _ => panic!("test AST"),
        };
        let (a, b) = (value(left), value(right));
        match op {
            Eq => a == b,
            Lt => a < b,
            LtEq => a <= b,
            Gt => a > b,
            GtEq => a >= b,
            _ => panic!("test op"),
        }
    }

    #[test]
    fn exhaustive_small_integer_ranges_never_lose_a_matching_value() {
        use BinaryOp::*;
        let mut leaves = Vec::new();
        for op in [Eq, Lt, LtEq, Gt, GtEq] {
            for n in -3..=3 {
                leaves.push(condition(op, n));
                leaves.push(Expr::binary(
                    op,
                    Expr::lit(Value::Int64(n)),
                    Expr::col("id"),
                ));
            }
        }
        for lo in -3..=3 {
            for hi in lo..=3 {
                let r = range(lo, hi);
                for a in &leaves {
                    let exists = (lo..=hi).any(|id| eval(a, id));
                    assert_eq!(matches(a, &r), exists, "{a:?}, {lo}..={hi}");
                    for b in &leaves {
                        for op in [And, Or] {
                            let expr = Expr::binary(op, a.clone(), b.clone());
                            if !matches(&expr, &r) {
                                assert!(
                                    !(lo..=hi).any(|id| eval(&expr, id)),
                                    "{expr:?}, {lo}..={hi}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}
