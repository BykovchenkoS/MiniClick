use std::fmt;

#[derive(Debug)]
pub enum DbError {
    /// Ошибка разбора SQL
    Syntax(String),
    UnknownTable(String),
    TableExists(String),
    UnknownColumn(String),
    TypeMismatch(String),
    /// Не разобралась строка входных данных (CSV/TSV/JSON). row считается с 1.
    BadInput {
        row: usize,
        message: String,
    },
    /// Корректный запрос, но мы это пока не умеем
    Unsupported(String),
    /// Запись пришла на реплику, а писать можно только на лидера
    NotLeader {
        leader: String,
    },
    Io(std::io::Error),
    /// Нарушен инвариант внутри системы — это баг
    Internal(String),
}

pub type Result<T> = std::result::Result<T, DbError>;

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::Syntax(m) => write!(f, "syntax error: {m}"),
            DbError::UnknownTable(t) => write!(f, "unknown table: {t}"),
            DbError::TableExists(t) => write!(f, "table already exists: {t}"),
            DbError::UnknownColumn(c) => write!(f, "unknown column: {c}"),
            DbError::TypeMismatch(m) => write!(f, "type mismatch: {m}"),
            DbError::BadInput { row, message } => write!(f, "bad input at row {row}: {message}"),
            DbError::Unsupported(m) => write!(f, "not supported: {m}"),
            DbError::NotLeader { leader } => {
                write!(f, "this node is a replica, send writes to leader {leader}")
            }
            DbError::Io(e) => write!(f, "io error: {e}"),
            DbError::Internal(m) => write!(f, "internal error: {m}"),
        }
    }
}

impl std::error::Error for DbError {}

impl From<std::io::Error> for DbError {
    fn from(e: std::io::Error) -> Self {
        DbError::Io(e)
    }
}
