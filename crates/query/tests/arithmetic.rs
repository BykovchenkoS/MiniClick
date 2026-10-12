use query::SqlQueryEngine;
use std::sync::Arc;
use types::testing::MemCatalog;
use types::*;

fn fixture() -> Block {
    Block::new(vec![
        Column {
            name: "id".into(),
            data: ColumnData::UInt64(vec![1, 2, 3]),
        },
        Column {
            name: "value".into(),
            data: ColumnData::Int64(vec![10, -3, 0]),
        },
        Column {
            name: "factor".into(),
            data: ColumnData::Float64(vec![1.5, 2.0, 0.5]),
        },
        Column {
            name: "text".into(),
            data: ColumnData::String(vec!["a".into(), "b".into(), "c".into()]),
        },
    ])
    .unwrap()
}
fn engine(blocks: Vec<Block>, schema: TableSchema) -> SqlQueryEngine {
    let catalog = Arc::new(MemCatalog::new());
    catalog
        .create_table(
            TableDefinition {
                name: "events".into(),
                schema,
                engine: Engine::Memory,
                order_by: vec![],
                settings: TableSettings::default(),
            },
            false,
        )
        .unwrap();
    for block in blocks {
        catalog.get_table("events").unwrap().insert(block).unwrap();
    }
    SqlQueryEngine::new(catalog)
}
fn standard() -> SqlQueryEngine {
    let b = fixture();
    let schema = TableSchema {
        columns: b.header(),
    };
    engine(vec![b], schema)
}
fn collect(engine: &SqlQueryEngine, sql: &str) -> Result<(Vec<ColumnDef>, Vec<Block>)> {
    let QueryOutput::Rows { header, stream } = engine.execute(sql)? else {
        panic!("SELECT expected")
    };
    let blocks = stream.collect::<Result<Vec<_>>>()?;
    for b in &blocks {
        assert_eq!(header, b.header());
    }
    Ok((header, blocks))
}
fn values(engine: &SqlQueryEngine, sql: &str) -> Vec<Value> {
    let (_, blocks) = collect(engine, sql).unwrap();
    blocks
        .iter()
        .flat_map(|b| (0..b.rows()).map(|r| b.columns()[0].data.get(r)))
        .collect()
}

#[test]
fn precedence_parentheses_and_unary_minus() {
    let e = standard();
    assert_eq!(
        values(&e, "SELECT value + 2 * 3 AS result FROM events"),
        vec![Value::Int64(16), Value::Int64(3), Value::Int64(6)]
    );
    assert_eq!(
        values(&e, "SELECT (value + 2) * 3 AS result FROM events"),
        vec![Value::Int64(36), Value::Int64(-3), Value::Int64(6)]
    );
    assert_eq!(
        values(&e, "SELECT -(value + 2) AS result FROM events"),
        vec![Value::Int64(-12), Value::Int64(1), Value::Int64(-2)]
    );
}

#[test]
fn expressions_constants_and_columns_can_share_projection() {
    let e = standard();
    let (header, blocks) = collect(
        &e,
        "SELECT id, value * 2 AS doubled, 'ok' AS label FROM events",
    )
    .unwrap();
    assert_eq!(
        header.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
        vec!["id", "doubled", "label"]
    );
    assert_eq!(
        blocks[0].column("doubled"),
        Some(&ColumnData::Int64(vec![20, -6, 0]))
    );
    assert_eq!(
        blocks[0].column("label"),
        Some(&ColumnData::String(vec!["ok".into(); 3]))
    );
}

#[test]
fn constants_preserve_row_count_without_reading_any_columns() {
    let e = standard();
    assert_eq!(
        values(&e, "SELECT 2 + 3 AS five FROM events"),
        vec![Value::Int64(5); 3]
    );
}

#[test]
fn result_types_follow_the_documented_numeric_rules() {
    let e = standard();
    let (header,blocks)=collect(&e,"SELECT id + id AS a, id + value AS b, id * 2 AS c, value + factor AS d, value / 2 AS e, -id AS f FROM events").unwrap();
    assert_eq!(
        header.iter().map(|c| c.data_type).collect::<Vec<_>>(),
        vec![
            DataType::UInt64,
            DataType::Int64,
            DataType::Int64,
            DataType::Float64,
            DataType::Float64,
            DataType::Int64
        ]
    );
    assert_eq!(
        blocks[0].column("a"),
        Some(&ColumnData::UInt64(vec![2, 4, 6]))
    );
    assert_eq!(
        blocks[0].column("b"),
        Some(&ColumnData::Int64(vec![11, -1, 3]))
    );
    assert_eq!(
        blocks[0].column("d"),
        Some(&ColumnData::Float64(vec![11.5, -1.0, 0.5]))
    );
    assert_eq!(
        blocks[0].column("e"),
        Some(&ColumnData::Float64(vec![5.0, -1.5, 0.0]))
    );
}

#[test]
fn arithmetic_where_and_literal_cast_to_expression_type() {
    let e = standard();
    assert_eq!(
        values(&e, "SELECT id FROM events WHERE value * 2 >= 10"),
        vec![Value::UInt64(1)]
    );
    assert_eq!(
        values(&e, "SELECT id FROM events WHERE value + 1 > '5'"),
        vec![Value::UInt64(1)]
    );
    assert_eq!(
        values(&e, "SELECT id FROM events WHERE -value > 0"),
        vec![Value::UInt64(2)]
    );
    assert_eq!(
        values(
            &e,
            "SELECT value * 2 AS doubled FROM events WHERE id > 1 LIMIT 1"
        ),
        vec![Value::Int64(-6)]
    );
}

#[test]
fn automatic_expression_names_and_duplicate_alias_checks() {
    let e = standard();
    let (header, _) = collect(&e, "SELECT value + 1 FROM events").unwrap();
    assert_eq!(header[0].name, "value + 1");
    for sql in [
        "SELECT value + 1, value + 1 FROM events",
        "SELECT id, value + 1 AS id FROM events",
    ] {
        assert!(matches!(e.execute(sql), Err(DbError::Unsupported(_))));
    }
}

#[test]
fn invalid_types_and_unknown_columns_fail_before_reading_empty_table() {
    let e = engine(
        vec![],
        TableSchema {
            columns: fixture().header(),
        },
    );
    for sql in [
        "SELECT text + 1 FROM events",
        "SELECT -text FROM events",
        "SELECT (id = 1) + 1 FROM events",
        "SELECT id > 1 FROM events",
        "SELECT value FROM events WHERE text * 2 > 1",
        "SELECT value + '2' FROM events",
        "SELECT id * 2 FROM events WHERE value + 1 > 'abc'",
    ] {
        assert!(
            matches!(e.execute(sql), Err(DbError::TypeMismatch(_))),
            "{sql}"
        );
    }
    assert!(matches!(
        e.execute("SELECT missing + 1 FROM events LIMIT 0"),
        Err(DbError::UnknownColumn(_))
    ));
    assert!(matches!(
        e.execute("SELECT value + 1 AS a, a + 1 FROM events"),
        Err(DbError::UnknownColumn(_))
    ));
}

#[test]
fn empty_result_has_inferred_header_and_does_not_evaluate_rows() {
    let e = engine(
        vec![],
        TableSchema {
            columns: fixture().header(),
        },
    );
    let (header, blocks) = collect(&e, "SELECT value / 2 AS half FROM events").unwrap();
    assert_eq!(
        header,
        vec![ColumnDef {
            name: "half".into(),
            data_type: DataType::Float64
        }]
    );
    assert!(blocks.is_empty());
    assert!(values(&standard(), "SELECT 1 / 0 AS bad FROM events LIMIT 0").is_empty());
    assert!(values(&standard(), "SELECT 1 / 0 AS bad FROM events WHERE id < 0").is_empty());
}

#[test]
fn division_by_zero_returns_stream_error_then_stops() {
    let e = standard();
    for sql in [
        "SELECT value / 0 AS bad FROM events",
        "SELECT value / -0.0 AS bad FROM events",
        "SELECT id FROM events WHERE value / 0 > 0",
    ] {
        let QueryOutput::Rows { mut stream, .. } = e.execute(sql).unwrap() else {
            panic!()
        };
        assert!(
            matches!(stream.next(), Some(Err(DbError::TypeMismatch(_)))),
            "{sql}"
        );
        assert!(stream.next().is_none());
    }
}

#[test]
fn logical_short_circuit_and_limit_avoid_unneeded_projection_errors() {
    let e = standard();
    assert_eq!(
        values(
            &e,
            "SELECT id FROM events WHERE value != 0 AND 10 / value > 0"
        ),
        vec![Value::UInt64(1)]
    );
    assert_eq!(
        values(&e, "SELECT 10 / value AS ratio FROM events LIMIT 1"),
        vec![Value::Float64(1.0)]
    );
    assert!(collect(&e, "SELECT 10 / value AS ratio FROM events").is_err());
}

#[test]
fn integer_overflow_underflow_and_float_overflow_are_errors() {
    let e = standard();
    for expr in [
        "9223372036854775807 + 1",
        "-9223372036854775808 - 1",
        "9223372036854775807 * 2",
        "18446744073709551615 + 18446744073709551615",
        "9223372036854775808 - 18446744073709551615",
        "-(-9223372036854775808)",
        "1e308 * 1e308",
    ] {
        assert!(
            matches!(
                collect(&e, &format!("SELECT {expr} AS result FROM events")),
                Err(DbError::TypeMismatch(_))
            ),
            "{expr}"
        );
    }
}

#[test]
fn mixed_integer_operations_check_result_not_individual_operand_range() {
    let e = standard();
    assert_eq!(
        values(&e, "SELECT 9223372036854775808 + -1 AS result FROM events"),
        vec![Value::Int64(i64::MAX); 3]
    );
    assert_eq!(
        values(&e, "SELECT -(9223372036854775808) AS result FROM events"),
        vec![Value::Int64(i64::MIN); 3]
    );
    assert_eq!(
        values(&e, "SELECT 9007199254740993 + 1 AS result FROM events"),
        vec![Value::Int64(9_007_199_254_740_994); 3]
    );
}

#[test]
fn arithmetic_float_conversion_rejects_precision_loss() {
    let e = standard();
    for expr in [
        "9007199254740993 + 0.0",
        "9007199254740993 / 1",
        "18446744073709551615 * 1.0",
    ] {
        assert!(
            matches!(
                collect(&e, &format!("SELECT {expr} AS result FROM events")),
                Err(DbError::TypeMismatch(_))
            ),
            "{expr}"
        );
    }
    assert_eq!(
        values(&e, "SELECT 9007199254740992 + 0.0 AS result FROM events"),
        vec![Value::Float64(9_007_199_254_740_992.0); 3]
    );
}

#[test]
fn expressions_are_independent_of_block_boundaries() {
    let b = fixture();
    for split in 0..=b.rows() {
        let e = engine(
            vec![b.slice(0, split), b.slice(split, b.rows() - split)],
            TableSchema {
                columns: b.header(),
            },
        );
        assert_eq!(
            values(
                &e,
                "SELECT value * 2 AS doubled FROM events WHERE id > 1 LIMIT 2"
            ),
            vec![Value::Int64(-6), Value::Int64(0)]
        );
    }
}
