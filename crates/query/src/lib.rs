//! Выполнение SQL через общие интерфейсы Catalog и Table.
//!
//! SELECT из одной таблицы, псевдонимы AS, WHERE и LIMIT.
//! Типы sqlparser остаются внутри parser и не пересекают границы крейта.

mod ast;
mod engine;
mod executor;
mod expr;
mod parser;
mod plan;
mod planner;

pub use engine::SqlQueryEngine;
