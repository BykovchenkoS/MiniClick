//! Выполнение SQL через общие интерфейсы Catalog и Table.
//!
//! SELECT, WHERE, LIMIT и команды CREATE / DROP / OPTIMIZE TABLE.
//! Типы sqlparser остаются внутри parser и не пересекают границы крейта.

mod ast;
mod ddl;
mod engine;
mod executor;
mod expr;
mod parser;
mod plan;
mod planner;

pub use engine::SqlQueryEngine;
