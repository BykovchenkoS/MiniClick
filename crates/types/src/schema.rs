use crate::error::{DbError, Result};
use crate::value::DataType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
}

/// Колонки таблицы в порядке объявления. Имена регистрозависимые.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TableSchema {
    pub columns: Vec<ColumnDef>,
}

impl TableSchema {
    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Engine {
    MergeTree,
    /// Таблица в памяти. Для тестов и для первого этапа.
    Memory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Codec {
    None,
    Lz4,
    Zstd,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSettings {
    /// Строк в грануле. В тестах ставим маленькое значение.
    pub index_granularity: usize,
    pub codec: Codec,
}

impl Default for TableSettings {
    fn default() -> Self {
        TableSettings {
            index_granularity: 8192,
            codec: Codec::Lz4,
        }
    }
}

/// Всё, что известно о таблице из CREATE TABLE. Catalog хранит это на диске
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDefinition {
    pub name: String,
    pub schema: TableSchema,
    pub engine: Engine,
    /// Ключ сортировки = первичный ключ. Для Memory может быть пустым
    pub order_by: Vec<String>,
    pub settings: TableSettings,
}

impl TableDefinition {
    /// Проверки, общие для всех, зовёт Catalog перед созданием таблицы
    pub fn validate(&self) -> Result<()> {
        if self.schema.columns.is_empty() {
            return Err(DbError::Syntax(format!(
                "table {} has no columns",
                self.name
            )));
        }
        for (i, c) in self.schema.columns.iter().enumerate() {
            if self.schema.columns[..i].iter().any(|p| p.name == c.name) {
                return Err(DbError::Syntax(format!("duplicate column {}", c.name)));
            }
        }
        for k in &self.order_by {
            if self.schema.column(k).is_none() {
                return Err(DbError::UnknownColumn(k.clone()));
            }
        }
        if self.engine == Engine::MergeTree && self.order_by.is_empty() {
            return Err(DbError::Syntax("MergeTree requires ORDER BY".into()));
        }
        if self.settings.index_granularity == 0 {
            return Err(DbError::Syntax("index_granularity must be > 0".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::DataType;

    #[test]
    fn definition_json_roundtrip() {
        let def = TableDefinition {
            name: "hits".into(),
            schema: TableSchema {
                columns: vec![ColumnDef {
                    name: "id".into(),
                    data_type: DataType::UInt64,
                }],
            },
            engine: Engine::MergeTree,
            order_by: vec!["id".into()],
            settings: TableSettings::default(),
        };
        let json = serde_json::to_string(&def).unwrap();
        assert_eq!(serde_json::from_str::<TableDefinition>(&json).unwrap(), def);
    }
}
