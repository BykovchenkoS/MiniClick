use sqlparser::ast::{
    Expr, GroupByExpr, Query, Select, SelectItem as SqlSelectItem, SetExpr, Statement, TableFactor,
    WildcardAdditionalOptions,
};
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser;
use types::{DbError, Result};

use crate::ast::{Command, SelectItem, SelectQuery};
use sqlparser::{keywords::Keyword, tokenizer::Token};

fn unsupported(feature: &str) -> DbError {
    DbError::Unsupported(format!("Query: {feature} пока не поддерживается"))
}

pub(crate) fn parse(sql: &str) -> Result<Command> {
    let dialect = ClickHouseDialect {};
    let mut parser = Parser::new(&dialect).try_with_sql(sql).map_err(syntax)?;
    while parser.consume_token(&Token::SemiColon) {}
    if parser.peek_token().token == Token::EOF {
        return Err(DbError::Syntax("пустой SQL-запрос".into()));
    }
    let statement = parser.parse_statement().map_err(syntax)?;
    let settings = if matches!(statement, Statement::CreateTable(_))
        && parser.parse_keyword(Keyword::SETTINGS)
    {
        crate::ddl::parse_settings(&mut parser)?
    } else {
        types::TableSettings::default()
    };
    let terminated = parser.consume_token(&Token::SemiColon);
    while parser.consume_token(&Token::SemiColon) {}
    if parser.peek_token().token != Token::EOF {
        return Err(if terminated {
            unsupported("несколько SQL-команд за один вызов")
        } else {
            DbError::Syntax(format!(
                "неожиданное продолжение SQL: {}",
                parser.peek_token()
            ))
        });
    }
    match statement {
        Statement::Query(query) => Ok(Command::Select(translate(*query)?)),
        Statement::CreateTable(create) => crate::ddl::create(create, settings),
        Statement::Drop {
            object_type,
            if_exists,
            names,
            cascade,
            restrict,
            purge,
            temporary,
        } => {
            if object_type != sqlparser::ast::ObjectType::Table
                || cascade
                || restrict
                || purge
                || temporary
            {
                return Err(unsupported("вариант DROP"));
            }
            if names.len() != 1 {
                return Err(unsupported("удаление нескольких таблиц за один вызов"));
            }
            Ok(Command::Drop {
                name: crate::ddl::table_name(names.into_iter().next().expect("одно имя"))?,
                if_exists,
            })
        }
        Statement::OptimizeTable {
            name,
            on_cluster,
            partition,
            include_final,
            deduplicate,
        } => {
            if on_cluster.is_some() || partition.is_some() || include_final || deduplicate.is_some()
            {
                return Err(unsupported(
                    "опция OPTIMIZE (ON CLUSTER / PARTITION / FINAL / DEDUPLICATE)",
                ));
            }
            Ok(Command::Optimize {
                name: crate::ddl::table_name(name)?,
            })
        }
        _ => Err(unsupported("SQL-команда")),
    }
}

pub(crate) fn syntax(error: sqlparser::parser::ParserError) -> DbError {
    DbError::Syntax(error.to_string())
}

fn translate(query: Query) -> Result<SelectQuery> {
    // Перечисляем ВСЕ поля: обновление sqlparser потребует пересмотра адаптера.
    // Нельзя молча проигнорировать часть запроса и вернуть неверные данные.
    let Query {
        with,
        body,
        order_by,
        limit,
        limit_by,
        offset,
        fetch,
        locks,
        for_clause,
        settings,
        format_clause,
    } = query;
    for (present, name) in [
        (with.is_some(), "WITH"),
        (order_by.is_some(), "ORDER BY"),
        (!limit_by.is_empty(), "LIMIT BY"),
        (offset.is_some(), "OFFSET"),
        (fetch.is_some(), "FETCH"),
        (!locks.is_empty(), "блокировки SELECT"),
        (for_clause.is_some(), "FOR"),
        (settings.is_some(), "SETTINGS"),
        (format_clause.is_some(), "FORMAT в SQL"),
    ] {
        if present {
            return Err(unsupported(name));
        }
    }
    let SetExpr::Select(select) = *body else {
        return Err(unsupported("UNION, подзапрос или другая форма SELECT"));
    };
    let Select {
        select_token: _,
        distinct,
        top,
        top_before_distinct: _,
        projection,
        into,
        from,
        lateral_views,
        prewhere,
        selection,
        group_by,
        cluster_by,
        distribute_by,
        sort_by,
        having,
        named_window,
        qualify,
        window_before_qualify: _,
        value_table_mode,
        connect_by,
    } = *select;
    let has_group_by = !matches!(group_by,
        GroupByExpr::Expressions(ref exprs, ref modifiers)
            if exprs.is_empty() && modifiers.is_empty());
    for (present, name) in [
        (distinct.is_some(), "DISTINCT"),
        (top.is_some(), "TOP"),
        (into.is_some(), "SELECT INTO"),
        (!lateral_views.is_empty(), "LATERAL VIEW"),
        (prewhere.is_some(), "PREWHERE"),
        (has_group_by, "GROUP BY"),
        (!cluster_by.is_empty(), "CLUSTER BY"),
        (!distribute_by.is_empty(), "DISTRIBUTE BY"),
        (!sort_by.is_empty(), "SORT BY"),
        (having.is_some(), "HAVING"),
        (
            !named_window.is_empty() || qualify.is_some(),
            "оконные выражения",
        ),
        (value_table_mode.is_some(), "AS VALUE / AS STRUCT"),
        (connect_by.is_some(), "CONNECT BY"),
    ] {
        if present {
            return Err(unsupported(name));
        }
    }
    if from.len() != 1 {
        return Err(unsupported("SELECT без одной таблицы в FROM"));
    }
    let source = from.into_iter().next().expect("проверена одна таблица");
    if !source.joins.is_empty() {
        return Err(unsupported("JOIN"));
    }
    let table = match source.relation {
        TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
        } => {
            if alias.is_some()
                || args.is_some()
                || !with_hints.is_empty()
                || version.is_some()
                || with_ordinality
                || !partitions.is_empty()
                || json_path.is_some()
            {
                return Err(unsupported(
                    "псевдоним таблицы, табличная функция или модификатор FROM",
                ));
            }
            if name.0.len() != 1 {
                return Err(unsupported("составное имя таблицы"));
            }
            name.0.into_iter().next().expect("проверено одно имя").value
        }
        _ => return Err(unsupported("подзапрос или табличное выражение в FROM")),
    };
    let projection = projection
        .into_iter()
        .map(|item| match item {
            SqlSelectItem::Wildcard(options) => {
                let WildcardAdditionalOptions {
                    wildcard_token: _,
                    opt_ilike,
                    opt_exclude,
                    opt_except,
                    opt_replace,
                    opt_rename,
                } = options;
                if opt_ilike.is_some()
                    || opt_exclude.is_some()
                    || opt_except.is_some()
                    || opt_replace.is_some()
                    || opt_rename.is_some()
                {
                    return Err(unsupported("модификатор SELECT *"));
                }
                Ok(SelectItem::Wildcard)
            }
            SqlSelectItem::UnnamedExpr(Expr::Identifier(id)) => Ok(SelectItem::Column {
                name: id.value,
                alias: None,
            }),
            SqlSelectItem::ExprWithAlias {
                expr: Expr::Identifier(id),
                alias,
            } => Ok(SelectItem::Column {
                name: id.value,
                alias: Some(alias.value),
            }),
            SqlSelectItem::UnnamedExpr(expr) => {
                let label = expr.to_string();
                Ok(SelectItem::Expression {
                    expr: translate_expr(expr)?,
                    alias: None,
                    label,
                })
            }
            SqlSelectItem::ExprWithAlias { expr, alias } => {
                let label = expr.to_string();
                Ok(SelectItem::Expression {
                    expr: translate_expr(expr)?,
                    alias: Some(alias.value),
                    label,
                })
            }
            _ => Err(unsupported("квалифицированная звёздочка в SELECT")),
        })
        .collect::<Result<Vec<_>>>()?;
    if projection.is_empty() {
        return Err(DbError::Syntax("пустой список SELECT".into()));
    }
    Ok(SelectQuery {
        table,
        projection,
        predicate: selection.map(translate_expr).transpose()?,
        limit: limit.map(translate_limit).transpose()?,
    })
}

// Переводим только поддерживаемые выражения, типы проверит планировщик
fn translate_expr(expr: Expr) -> Result<types::Expr> {
    use sqlparser::ast::{BinaryOperator as B, UnaryOperator as U, Value as V};
    use types::{BinaryOp as Op, Expr as E, UnaryOp};
    match expr {
        Expr::Identifier(id) => Ok(E::Column(id.value)),
        Expr::Nested(expr) => translate_expr(*expr),
        Expr::Value(V::SingleQuotedString(text)) => Ok(E::Literal(types::Value::String(text))),
        Expr::Value(V::Number(text, _)) => Ok(E::Literal(parse_number(&text)?)),
        Expr::UnaryOp { op: U::Not, expr } => Ok(E::Unary {
            op: UnaryOp::Not,
            expr: Box::new(translate_expr(*expr)?),
        }),
        Expr::UnaryOp { op: U::Minus, expr } => {
            // Сначала разбираем знак, чтобы принять i64::MIN без переполнения.
            match *expr {
                Expr::Value(V::Number(text, _)) => {
                    Ok(E::Literal(parse_number(&format!("-{text}"))?))
                }
                expr => Ok(E::Unary {
                    op: UnaryOp::Neg,
                    expr: Box::new(translate_expr(expr)?),
                }),
            }
        }
        Expr::UnaryOp { op: U::Plus, expr } => match *expr {
            Expr::Value(V::Number(text, _)) => Ok(E::Literal(parse_number(&text)?)),
            _ => Err(unsupported("унарный плюс вне числового литерала")),
        },
        Expr::BinaryOp { left, op, right } => {
            let op = match op {
                B::Plus => Op::Add,
                B::Minus => Op::Sub,
                B::Multiply => Op::Mul,
                B::Divide => Op::Div,
                B::Eq => Op::Eq,
                B::NotEq => Op::NotEq,
                B::Lt => Op::Lt,
                B::LtEq => Op::LtEq,
                B::Gt => Op::Gt,
                B::GtEq => Op::GtEq,
                B::And => Op::And,
                B::Or => Op::Or,
                _ => return Err(unsupported("оператор выражения")),
            };
            Ok(E::binary(
                op,
                translate_expr(*left)?,
                translate_expr(*right)?,
            ))
        }
        _ => Err(unsupported(
            "выражение: NULL, IN, LIKE, функция или другая конструкция",
        )),
    }
}

fn parse_number(text: &str) -> Result<types::Value> {
    use types::Value;
    let bad =
        || DbError::TypeMismatch(format!("число '{text}' выходит за поддерживаемый диапазон"));
    if text.contains(['.', 'e', 'E']) {
        let x: f64 = text.parse().map_err(|_| bad())?;
        if !x.is_finite() {
            return Err(bad());
        }
        Ok(Value::Float64(x))
    } else if let Ok(x) = text.parse::<i64>() {
        Ok(Value::Int64(x))
    } else {
        text.parse::<u64>().map(Value::UInt64).map_err(|_| bad())
    }
}

fn translate_limit(expr: Expr) -> Result<u64> {
    if let Expr::Value(sqlparser::ast::Value::Number(text, _)) = expr {
        if !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) {
            return text
                .parse()
                .map_err(|_| DbError::Syntax("LIMIT превышает UInt64".into()));
        }
    }
    Err(DbError::Syntax(
        "LIMIT должен быть неотрицательным целым литералом".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Существующие тесты SELECT продолжают проверять его собственное AST.
    fn parse(sql: &str) -> Result<SelectQuery> {
        match super::parse(sql)? {
            Command::Select(query) => Ok(query),
            _ => Err(unsupported("не SELECT")),
        }
    }

    #[test]
    fn translates_into_own_ast() {
        assert_eq!(
            parse("SELECT name AS label, id FROM events;").unwrap(),
            SelectQuery {
                table: "events".into(),
                predicate: None,
                limit: None,
                projection: vec![
                    SelectItem::Column {
                        name: "name".into(),
                        alias: Some("label".into())
                    },
                    SelectItem::Column {
                        name: "id".into(),
                        alias: None
                    },
                ],
            }
        );
    }

    #[test]
    fn preserves_quoted_names_and_case() {
        assert_eq!(
            parse("select `User ID` from `Events`;").unwrap(),
            SelectQuery {
                table: "Events".into(),
                predicate: None,
                limit: None,
                projection: vec![SelectItem::Column {
                    name: "User ID".into(),
                    alias: None
                }],
            }
        );
    }

    #[test]
    fn ignores_comments_and_keyword_case() {
        assert_eq!(
            parse("-- demo\nsElEcT /* comment */ * FrOm events;").unwrap(),
            SelectQuery {
                table: "events".into(),
                predicate: None,
                limit: None,
                projection: vec![SelectItem::Wildcard]
            }
        );
    }

    #[test]
    fn rejects_empty_and_malformed_sql() {
        for sql in [
            "",
            "-- only comment",
            "SELECT * FROM",
            "SELECT id AS FROM events",
        ] {
            assert!(matches!(parse(sql), Err(DbError::Syntax(_))), "{sql}");
        }
    }

    #[test]
    fn rejects_unsupported_ast_without_ignoring_clauses() {
        for sql in [
            "SELECT * FROM events PREWHERE id = 1",
            "SELECT * FROM events ORDER BY id",
            "SELECT DISTINCT id FROM events",
            "SELECT id FROM events GROUP BY id",
            "SELECT count() FROM events",
            "SELECT 1",
            "SELECT * FROM events, other",
            "SELECT * FROM events JOIN other ON events.id = other.id",
            "SELECT * FROM (SELECT * FROM events)",
            "SELECT * FROM events UNION ALL SELECT * FROM events",
            "SELECT * FROM events e",
            "SELECT events.id FROM events",
            "SELECT * FROM db.events",
            "SELECT * FROM events FORMAT JSON",
            "SELECT * FROM events SETTINGS max_threads = 1",
            "SELECT * EXCEPT (id) FROM events",
            "SELECT * FROM events; SELECT * FROM other",
            "DROP TABLE events",
            "WITH t AS (SELECT * FROM events) SELECT * FROM t",
        ] {
            let result = parse(sql);
            assert!(
                matches!(result, Err(DbError::Unsupported(_))),
                "{sql}: {result:?}"
            );
        }
    }
}
