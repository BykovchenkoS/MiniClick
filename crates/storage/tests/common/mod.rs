use types::{
    Block, Codec, Column, ColumnData, ColumnDef, DataType, Engine, TableDefinition, TableSchema,
    TableSettings,
};

pub fn definition(codec: Codec) -> TableDefinition {
    TableDefinition {
        name: "hits".into(),
        schema: TableSchema {
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    data_type: DataType::Int64,
                },
                ColumnDef {
                    name: "text".into(),
                    data_type: DataType::String,
                },
            ],
        },
        engine: Engine::MergeTree,
        order_by: vec!["id".into(), "text".into()],
        settings: TableSettings {
            index_granularity: 2,
            codec,
        },
    }
}

pub fn block(ids: &[i64], texts: &[&str]) -> Block {
    Block::new(vec![
        Column {
            name: "id".into(),
            data: ColumnData::Int64(ids.to_vec()),
        },
        Column {
            name: "text".into(),
            data: ColumnData::String(texts.iter().map(|s| (*s).into()).collect()),
        },
    ])
    .unwrap()
}
