//! Выполнение SQL через общие интерфейсы Catalog и Table.
//!
//! SELECT с выражениями, WHERE, LIMIT и команды CREATE / DROP / OPTIMIZE TABLE.
//! Типы sqlparser остаются внутри parser и не пересекают границы крейта.

mod aggregate;
mod ast;
mod ddl;
mod engine;
mod executor;
mod expr;
mod parser;
mod plan;
mod planner;

pub use engine::SqlQueryEngine;
