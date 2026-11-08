//! Выполнение SQL через общие интерфейсы Catalog и Table.
//!
//! SELECT с WHERE, GROUP BY, ORDER BY, LIMIT и агрегатами count/sum/min/max.
//! Также поддерживаются EXPLAIN SELECT и команды CREATE / DROP / OPTIMIZE TABLE.
//! Типы sqlparser остаются внутри parser и не пересекают границы крейта.

mod aggregate;
mod ast;
mod ddl;
mod engine;
mod executor;
mod explain;
mod expr;
mod parser;
mod plan;
mod planner;
mod sort;

pub use engine::SqlQueryEngine;

/// Классифицирует поддерживаемый SQL как запись или чтение.
/// Нужна серверу, чтобы отклонять запись на реплике до выполнения.
pub fn is_write(sql: &str) -> types::Result<bool> {
    use crate::ast::Command;
    Ok(match parser::parse(sql)? {
        Command::Select(_) | Command::Explain(_) => false,
        Command::Create { .. } | Command::Drop { .. } | Command::Optimize { .. } => true,
    })
}
