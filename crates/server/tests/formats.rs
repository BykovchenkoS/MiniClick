use server::formats::{format_output, parse_input, InputFormat, OutputFormat};
use types::{Block, ColumnDef, DataType, DbError, TableSchema, Value};

fn schema() -> TableSchema {
    TableSchema {
        columns: [
            ("i", DataType::Int64),
            ("u", DataType::UInt64),
            ("f", DataType::Float64),
            ("s", DataType::String),
            ("d", DataType::Date),
            ("t", DataType::DateTime),
        ]
        .into_iter()
        .map(|(name, data_type)| ColumnDef {
            name: name.into(),
            data_type,
        })
        .collect(),
    }
}
fn output(format: OutputFormat, block: Block) -> Vec<u8> {
    format_output(
        format,
        &block.header(),
        Box::new(std::iter::once(Ok(block))),
    )
    .unwrap()
}

#[test]
fn csv_parses_quoted_unicode_and_all_six_types() {
    let block = parse_input(InputFormat::Csv, "-7,18446744073709551615,1.25,\"Привет, \"\"мир\"\"\nстрока\",1970-01-02,1970-01-02T01:02:03\r\n".as_bytes(), &schema()).unwrap();
    assert_eq!(block.rows(), 1);
    let actual: Vec<_> = block
        .columns()
        .iter()
        .map(|column| column.data.get(0))
        .collect();
    assert_eq!(
        actual,
        [
            Value::Int64(-7),
            Value::UInt64(u64::MAX),
            Value::Float64(1.25),
            Value::String("Привет, \"мир\"\nстрока".into()),
            Value::Date(1),
            Value::DateTime(90123)
        ]
    );
    assert_eq!(String::from_utf8(output(OutputFormat::Csv, block)).unwrap(), "-7,18446744073709551615,1.25,\"Привет, \"\"мир\"\"\nстрока\",1970-01-02,1970-01-02 01:02:03\n");
}

#[test]
fn tsv_escapes_roundtrip_without_headers() {
    let text = "-7\t42\t1.25\tЮникод\\t\\n\\r\\0\\\\\t1970-01-02\t1970-01-02 01:02:03\n";
    let block = parse_input(InputFormat::Tsv, text.as_bytes(), &schema()).unwrap();
    assert_eq!(
        block.columns()[3].data.get(0),
        Value::String("Юникод\t\n\r\0\\".into())
    );
    assert_eq!(output(OutputFormat::Tsv, block), text.as_bytes());
}

#[test]
fn json_reorders_keys_and_preserves_numeric_types_and_large_unsigned() {
    let text = r#"{"t":"1970-01-02","d":"1970-01-02","s":"a\nb","f":1.25,"u":18446744073709551615,"i":"-7"}"#;
    let block = parse_input(InputFormat::JsonEachRow, text.as_bytes(), &schema()).unwrap();
    assert_eq!(block.columns()[1].data.get(0), Value::UInt64(u64::MAX));
    let json: serde_json::Value =
        serde_json::from_slice(&output(OutputFormat::JsonEachRow, block)).unwrap();
    assert_eq!(
        json,
        serde_json::json!({"i":-7,"u":u64::MAX,"f":1.25,"s":"a\nb","d":"1970-01-02","t":"1970-01-02 00:00:00"})
    );
}

#[test]
fn invalid_records_report_the_record_number() {
    let valid = "1,2,3,a,1970-01-01,1970-01-01\n";
    for invalid in [
        "4,5,6,b,2023-02-29,1970-01-01\n",
        "4,5\n",
        "4,5,6,b,1970-01-01,1970-01-01,extra\n",
    ] {
        assert!(matches!(
            parse_input(
                InputFormat::Csv,
                format!("{valid}{invalid}").as_bytes(),
                &schema()
            ),
            Err(DbError::BadInput { row: 2, .. })
        ));
    }
    for text in [
        r#"{"i":1,"i":2}"#,
        r#"{"i":null}"#,
        "[]",
        "",
        r#"{"unknown":1}"#,
    ] {
        let data = format!("{{\"i\":1}}\n{text}\n");
        let one = TableSchema {
            columns: vec![ColumnDef {
                name: "i".into(),
                data_type: DataType::Int64,
            }],
        };
        assert!(
            matches!(
                parse_input(InputFormat::JsonEachRow, data.as_bytes(), &one),
                Err(DbError::BadInput { row: 2, .. })
            ),
            "{text}"
        );
    }
    assert!(matches!(
        parse_input(
            InputFormat::Tsv,
            b"1\t2\t3\ta\\x\t1970-01-01\t1970-01-01",
            &schema()
        ),
        Err(DbError::BadInput { row: 1, .. })
    ));
}

#[test]
fn empty_inputs_keep_schema_and_pretty_has_a_header() {
    for format in [InputFormat::Csv, InputFormat::Tsv, InputFormat::JsonEachRow] {
        let block = parse_input(format, b"", &schema()).unwrap();
        assert_eq!(block.rows(), 0);
        assert_eq!(block.header(), schema().columns);
    }
    let header = vec![ColumnDef {
        name: "имя".into(),
        data_type: DataType::String,
    }];
    let pretty =
        format_output(OutputFormat::Pretty, &header, Box::new(std::iter::empty())).unwrap();
    assert_eq!(
        String::from_utf8(pretty).unwrap(),
        "+-----+\n| имя |\n+-----+\n"
    );
}

#[test]
fn nonfinite_float_json_is_text_and_readable_again() {
    let schema = TableSchema {
        columns: vec![ColumnDef {
            name: "f".into(),
            data_type: DataType::Float64,
        }],
    };
    let block = parse_input(InputFormat::Csv, b"NaN\ninf\n-inf\n", &schema).unwrap();
    let json = output(OutputFormat::JsonEachRow, block);
    assert_eq!(
        json,
        b"{\"f\":\"NaN\"}\n{\"f\":\"inf\"}\n{\"f\":\"-inf\"}\n"
    );
    let read = parse_input(InputFormat::JsonEachRow, &json, &schema).unwrap();
    assert!(matches!(read.columns()[0].data.get(0), Value::Float64(value) if value.is_nan()));
    assert_eq!(read.columns()[0].data.get(1), Value::Float64(f64::INFINITY));
}

#[test]
fn lazy_stream_failure_and_header_mismatch_are_errors() {
    let block = Block::empty(&schema());
    let stream = Box::new(
        vec![
            Ok(block.clone()),
            Err(DbError::Internal("broken granule".into())),
        ]
        .into_iter(),
    );
    assert!(
        matches!(format_output(OutputFormat::Tsv, &schema().columns, stream), Err(DbError::Internal(message)) if message == "broken granule")
    );
    assert!(format_output(OutputFormat::Csv, &[], Box::new(std::iter::once(Ok(block)))).is_err());
}
