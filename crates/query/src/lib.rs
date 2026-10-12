//! Выполнение SQL через общие интерфейсы Catalog и Table.
//!
//! Первый этап: SELECT * / список колонок / псевдонимы AS из одной таблицы.
//! Типы sqlparser остаются внутри parser и не пересекают границы крейта.

mod ast;
mod engine;
mod executor;
mod parser;
mod plan;
mod planner;

pub use engine::SqlQueryEngine;
