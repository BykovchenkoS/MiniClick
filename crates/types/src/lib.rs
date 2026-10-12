//! Общая часть: типы данных и интерфейсы между треками

pub mod block;
pub mod column;
pub mod error;
pub mod expr;
pub mod interfaces;
pub mod key_condition;
pub mod schema;
pub mod testing;
pub mod value;

pub use block::{Block, Column};
pub use column::ColumnData;
pub use error::{DbError, Result};
pub use expr::{BinaryOp, Expr, UnaryOp};
pub use interfaces::{
    BlockStream, Catalog, PartData, Peer, QueryEngine, QueryOutput, ScanRequest, ScanStats, Table,
    TableFactory,
};
pub use key_condition::{may_match, KeyRange};
pub use schema::{Codec, ColumnDef, Engine, TableDefinition, TableSchema, TableSettings};
pub use value::{DataType, Value};
