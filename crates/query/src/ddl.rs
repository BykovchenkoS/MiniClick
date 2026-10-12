//! Адаптер DDL: библиотечное AST → общие определения таблиц.
use crate::{ast::Command, parser::syntax};
use sqlparser::{
    ast::{self, CreateTable, Expr, HiveDistributionStyle, HiveFormat, ObjectName, Value},
    parser::Parser,
    tokenizer::Token,
};
use std::collections::HashSet;
use types::{
    Codec, ColumnDef, DataType, DbError, Engine, Result, TableDefinition, TableSchema,
    TableSettings,
};

fn unsupported(message: &str) -> DbError {
    DbError::Unsupported(message.into())
}

pub(crate) fn table_name(name: ObjectName) -> Result<String> {
    if name.0.len() != 1 {
        return Err(unsupported(
            "поддерживается только простое имя таблицы без базы/схемы",
        ));
    }
    let name = name.0.into_iter().next().expect("одно имя").value;
    if name.is_empty() {
        return Err(DbError::Syntax("имя таблицы не может быть пустым".into()));
    }
    Ok(name)
}

pub(crate) fn create(create: CreateTable, settings: TableSettings) -> Result<Command> {
    // Явно перечислены все поля AST: неподдерживаемые опции не игнорируются.
    let CreateTable {
        or_replace,
        temporary,
        external,
        global,
        if_not_exists,
        transient,
        volatile,
        name,
        columns,
        constraints,
        hive_distribution,
        hive_formats,
        table_properties,
        with_options,
        file_format,
        location,
        query,
        without_rowid,
        like,
        clone,
        engine,
        comment,
        auto_increment_offset,
        default_charset,
        collation,
        on_commit,
        on_cluster,
        primary_key,
        order_by,
        partition_by,
        cluster_by,
        clustered_by,
        options,
        strict,
        copy_grants,
        enable_schema_evolution,
        change_tracking,
        data_retention_time_in_days,
        max_data_extension_time_in_days,
        default_ddl_collation,
        with_aggregation_policy,
        with_row_access_policy,
        with_tags,
    } = create;
    let hive_options = hive_formats.is_some_and(|h| h != HiveFormat::default());
    for (present, label) in [
        (or_replace, "OR REPLACE"),
        (
            temporary || external || global.is_some() || transient || volatile,
            "специальный вид таблицы",
        ),
        (!constraints.is_empty(), "ограничения таблицы"),
        (
            !matches!(hive_distribution, HiveDistributionStyle::NONE) || hive_options,
            "опции Hive",
        ),
        (
            !table_properties.is_empty() || !with_options.is_empty() || options.is_some(),
            "параметры вне SETTINGS",
        ),
        (
            file_format.is_some() || location.is_some(),
            "внешнее хранение",
        ),
        (
            query.is_some() || like.is_some() || clone.is_some(),
            "CREATE AS / LIKE / CLONE",
        ),
        (without_rowid || strict, "опции SQLite"),
        (comment.is_some(), "COMMENT"),
        (auto_increment_offset.is_some(), "AUTO_INCREMENT"),
        (
            default_charset.is_some() || collation.is_some(),
            "CHARSET / COLLATE",
        ),
        (on_commit.is_some(), "ON COMMIT"),
        (on_cluster.is_some(), "ON CLUSTER"),
        (
            primary_key.is_some(),
            "отдельный PRIMARY KEY (используйте ORDER BY)",
        ),
        (
            partition_by.is_some() || cluster_by.is_some() || clustered_by.is_some(),
            "разбиение таблицы",
        ),
        (
            copy_grants
                || enable_schema_evolution.is_some()
                || change_tracking.is_some()
                || data_retention_time_in_days.is_some()
                || max_data_extension_time_in_days.is_some()
                || default_ddl_collation.is_some()
                || with_aggregation_policy.is_some()
                || with_row_access_policy.is_some()
                || with_tags.is_some(),
            "дополнительные опции CREATE",
        ),
    ] {
        if present {
            return Err(unsupported(&format!(
                "CREATE TABLE: {label} пока не поддерживается"
            )));
        }
    }
    let name = table_name(name)?;
    let engine = engine
        .ok_or_else(|| DbError::Syntax("укажите ENGINE = Memory или ENGINE = MergeTree".into()))?;
    if engine.parameters.as_ref().is_some_and(|p| !p.is_empty()) {
        return Err(unsupported("параметры движка в скобках не поддерживаются"));
    }
    let engine = match engine.name.as_str() {
        "Memory" => Engine::Memory,
        "MergeTree" => Engine::MergeTree,
        _ => {
            return Err(unsupported(&format!(
                "неизвестный движок '{}'",
                engine.name
            )))
        }
    };
    let columns = columns
        .into_iter()
        .map(|column| {
            let ast::ColumnDef {
                name,
                data_type,
                collation,
                options,
            } = column;
            if collation.is_some() || !options.is_empty() {
                return Err(unsupported(
                    "опции колонок (NULL, DEFAULT, CODEC, ограничения) пока не поддерживаются",
                ));
            }
            if name.value.is_empty() {
                return Err(DbError::Syntax("имя колонки не может быть пустым".into()));
            }
            let data_type = match data_type {
                ast::DataType::Int64 => DataType::Int64,
                ast::DataType::UInt64 => DataType::UInt64,
                ast::DataType::Float64 => DataType::Float64,
                ast::DataType::String(None) => DataType::String,
                ast::DataType::Date => DataType::Date,
                ast::DataType::Datetime(None) => DataType::DateTime,
                other => {
                    return Err(unsupported(&format!(
                        "тип колонки {other} не поддерживается"
                    )))
                }
            };
            Ok(ColumnDef {
                name: name.value,
                data_type,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut keys = Vec::new();
    if let Some(order_by) = order_by {
        for expr in order_by {
            let Expr::Identifier(id) = expr else {
                return Err(unsupported(
                    "ORDER BY при CREATE принимает только имена колонок",
                ));
            };
            if keys.contains(&id.value) {
                return Err(DbError::Syntax(format!(
                    "повтор колонки '{}' в ORDER BY",
                    id.value
                )));
            }
            keys.push(id.value);
        }
    }
    let definition = TableDefinition {
        name,
        schema: TableSchema { columns },
        engine,
        order_by: keys,
        settings,
    };
    definition.validate()?;
    Ok(Command::Create {
        definition,
        if_not_exists,
    })
}

/// Минимальный хвост SETTINGS для MiniClick. Значения читает парсер выражений
/// sqlparser, поэтому комментарии, кавычки и точки с запятой не ломают разбор.
pub(crate) fn parse_settings(parser: &mut Parser<'_>) -> Result<TableSettings> {
    let mut settings = TableSettings::default();
    let mut seen = HashSet::new();
    loop {
        let key = parser.parse_identifier(false).map_err(syntax)?.value;
        if !seen.insert(key.clone()) {
            return Err(DbError::Syntax(format!("повтор настройки '{key}'")));
        }
        parser.expect_token(&Token::Eq).map_err(syntax)?;
        let value = parser.parse_expr().map_err(syntax)?;
        match key.as_str() {
            "index_granularity" => {
                let Expr::Value(Value::Number(text, _)) = value else {
                    return Err(DbError::Syntax(
                        "index_granularity должна быть положительным целым числом".into(),
                    ));
                };
                if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(DbError::Syntax(
                        "index_granularity должна быть положительным целым числом".into(),
                    ));
                }
                settings.index_granularity = text
                    .parse::<usize>()
                    .ok()
                    .filter(|n| *n > 0)
                    .ok_or_else(|| {
                        DbError::Syntax(
                            "index_granularity вне диапазона положительного usize".into(),
                        )
                    })?;
            }
            "codec" => {
                let Expr::Value(Value::SingleQuotedString(text)) = value else {
                    return Err(DbError::Syntax(
                        "codec задаётся строкой: 'None', 'LZ4' или 'ZSTD'".into(),
                    ));
                };
                settings.codec = match text.to_ascii_uppercase().as_str() {
                    "NONE" => Codec::None,
                    "LZ4" => Codec::Lz4,
                    "ZSTD" => Codec::Zstd,
                    _ => return Err(unsupported(&format!("неизвестный кодек '{text}'"))),
                };
            }
            _ => return Err(unsupported(&format!("неизвестная настройка '{key}'"))),
        }
        if !parser.consume_token(&Token::Comma) {
            break;
        }
    }
    Ok(settings)
}
