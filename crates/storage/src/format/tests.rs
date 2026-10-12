use super::*;

fn assert_corrupt<T: std::fmt::Debug>(result: Result<T>) {
    assert!(matches!(result, Err(DbError::Internal(_))), "{result:?}");
}

#[test]
fn columns_match_explicit_little_endian_bytes() {
    let cases = [
        (
            ColumnData::Int64(vec![-2, 1]),
            vec![
                254, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 0, 0, 0, 0, 0,
            ],
        ),
        (
            ColumnData::UInt64(vec![0x0102030405060708]),
            vec![8, 7, 6, 5, 4, 3, 2, 1],
        ),
        (
            ColumnData::Float64(vec![1.0, -0.0]),
            vec![0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 128],
        ),
        (
            ColumnData::Date(vec![-1, 1]),
            vec![255, 255, 255, 255, 1, 0, 0, 0],
        ),
        (ColumnData::DateTime(vec![-1]), vec![255; 8]),
        (
            ColumnData::String(vec!["".into(), "Я".into()]),
            vec![0, 0, 0, 0, 2, 0, 0, 0, 208, 175],
        ),
    ];
    for (column, expected) in cases {
        assert_eq!(encode_column(&column).unwrap(), expected);
        let decoded = decode_column(&expected, column.data_type(), column.len()).unwrap();
        assert_eq!(decoded, column);
        // Равенство f64 не различает знаки нуля, формат обязан их сохранять.
        if let ColumnData::Float64(values) = decoded {
            assert_eq!(values[1].to_bits(), (-0.0f64).to_bits());
        }
    }
}

#[test]
fn columns_preserve_extremes_and_empty_inputs() {
    for column in [
        ColumnData::Int64(vec![i64::MIN, i64::MAX]),
        ColumnData::UInt64(vec![0, u64::MAX]),
        ColumnData::Date(vec![i32::MIN, i32::MAX]),
        ColumnData::DateTime(vec![i64::MIN, i64::MAX]),
        ColumnData::String(vec!["a\0b".into(), "Привет 🦀".into()]),
    ] {
        let bytes = encode_column(&column).unwrap();
        assert_eq!(
            decode_column(&bytes, column.data_type(), column.len()).unwrap(),
            column
        );
    }
    for data_type in [
        DataType::Int64,
        DataType::UInt64,
        DataType::Float64,
        DataType::String,
        DataType::Date,
        DataType::DateTime,
    ] {
        assert_eq!(
            encode_column(&ColumnData::empty(data_type)).unwrap(),
            Vec::<u8>::new()
        );
        assert_eq!(
            decode_column(&[], data_type, 0).unwrap(),
            ColumnData::empty(data_type)
        );
        assert_corrupt(decode_column(&[0], data_type, 0));
    }
    let bits = [
        0x7ff8000000000042u64,
        f64::INFINITY.to_bits(),
        f64::NEG_INFINITY.to_bits(),
    ];
    let column = ColumnData::Float64(bits.iter().map(|b| f64::from_bits(*b)).collect());
    let decoded = decode_column(&encode_column(&column).unwrap(), DataType::Float64, 3).unwrap();
    let ColumnData::Float64(values) = decoded else {
        panic!("wrong column type")
    };
    assert_eq!(values.iter().map(|v| v.to_bits()).collect::<Vec<_>>(), bits);
}

#[test]
fn column_decoding_rejects_wrong_counts_truncation_and_trailing_bytes() {
    for data_type in [
        DataType::Int64,
        DataType::UInt64,
        DataType::Float64,
        DataType::Date,
        DataType::DateTime,
    ] {
        let width = if data_type == DataType::Date { 4 } else { 8 };
        assert_corrupt(decode_column(&vec![0; width - 1], data_type, 1));
        assert_corrupt(decode_column(&vec![0; width + 1], data_type, 1));
        assert_corrupt(decode_column(&vec![0; width], data_type, 2));
        assert_corrupt(decode_column(&[], data_type, usize::MAX));
    }
    for bytes in [
        vec![1, 0, 0],
        vec![2, 0, 0, 0, b'a'],
        vec![1, 0, 0, 0, 255],
        vec![0, 0, 0, 0, 42],
        vec![255; 4],
    ] {
        assert_corrupt(decode_column(&bytes, DataType::String, 1));
    }
    assert_corrupt(decode_column(&[0; 4], DataType::String, 2));
    assert_corrupt(decode_column(&[], DataType::String, usize::MAX));
}

#[test]
fn none_frame_has_explicit_header_and_exact_payload() {
    let expected = [0, 3, 0, 0, 0, 3, 0, 0, 0, 10, 20, 30];
    assert_eq!(FRAME_HEADER_SIZE, 9);
    assert_eq!(encode_frame(&[10, 20, 30], Codec::None).unwrap(), expected);
    assert_eq!(decode_frame(&expected).unwrap(), [10, 20, 30]);
    assert_eq!(encode_frame(&[], Codec::None).unwrap(), [0; 9]);
    assert_eq!(decode_frame(&[0; 9]).unwrap(), Vec::<u8>::new());
}

#[test]
fn frames_reject_invalid_sizes_unknown_codecs_and_partial_headers() {
    let valid = [0, 3, 0, 0, 0, 3, 0, 0, 0, 10, 20, 30];
    for size in 0..valid.len() {
        assert_corrupt(decode_frame(&valid[..size]));
    }
    let mut bytes = valid.to_vec();
    bytes.push(99);
    assert_corrupt(decode_frame(&bytes));
    let mut bytes = valid;
    bytes[5] = 4;
    assert_corrupt(decode_frame(&bytes));
    bytes = valid;
    bytes[1] = 255;
    assert_corrupt(decode_frame(&bytes));
    bytes = valid;
    bytes[0] = 3;
    assert_corrupt(decode_frame(&bytes));
}

#[test]
fn marks_are_twelve_bytes_each_with_row_counts() {
    let marks = [
        Mark { offset: 0, rows: 2 },
        Mark {
            offset: 0x0102030405060708,
            rows: 1,
        },
    ];
    let expected = [
        0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 8, 7, 6, 5, 4, 3, 2, 1, 1, 0, 0, 0,
    ];
    assert_eq!(encode_marks(&marks).unwrap(), expected);
    assert_eq!(decode_marks(&expected).unwrap(), marks);
    assert_eq!(decode_marks(&[]).unwrap(), Vec::<Mark>::new());
    assert_eq!(encode_marks(&[]).unwrap(), Vec::<u8>::new());
    for size in 1..MARK_SIZE {
        assert_corrupt(decode_marks(&expected[..size]));
    }
    for marks in [
        vec![Mark { offset: 0, rows: 0 }],
        vec![Mark { offset: 1, rows: 1 }],
        vec![Mark { offset: 0, rows: 1 }, Mark { offset: 0, rows: 1 }],
        vec![
            Mark { offset: 0, rows: 1 },
            Mark {
                offset: 20,
                rows: 1,
            },
            Mark {
                offset: 10,
                rows: 1,
            },
        ],
    ] {
        assert_corrupt(encode_marks(&marks));
        let bytes: Vec<u8> = marks
            .iter()
            .flat_map(|m| {
                m.offset
                    .to_le_bytes()
                    .into_iter()
                    .chain(m.rows.to_le_bytes())
            })
            .collect();
        assert_corrupt(decode_marks(&bytes));
    }
}

#[test]
fn index_is_column_major_and_supports_variable_length_keys() {
    let columns = [
        ColumnData::Date(vec![1, 2]),
        ColumnData::String(vec!["b".into(), "a".into()]),
    ];
    let expected = [1, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, b'b', 1, 0, 0, 0, b'a'];
    assert_eq!(encode_index(&columns, 2).unwrap(), expected);
    assert_eq!(
        decode_index(&expected, &[DataType::Date, DataType::String], 2).unwrap(),
        columns
    );
    assert_corrupt(encode_index(&columns, 3));
    for size in 0..expected.len() {
        assert_corrupt(decode_index(
            &expected[..size],
            &[DataType::Date, DataType::String],
            2,
        ));
    }
    let mut extra = expected.to_vec();
    extra.push(0);
    assert_corrupt(decode_index(&extra, &[DataType::Date, DataType::String], 2));
    assert_eq!(
        decode_index(&[], &[DataType::Date, DataType::String], 0).unwrap(),
        [ColumnData::Date(vec![]), ColumnData::String(vec![])]
    );
}

fn meta() -> PartMeta {
    PartMeta {
        format_version: FORMAT_VERSION,
        rows: 3,
        granules: 2,
        columns: vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int64,
        }],
        codec: Codec::None,
        min_key: vec![Value::Int64(1)],
        max_key: vec![Value::Int64(3)],
    }
}

#[test]
fn metadata_reads_explicit_json_and_preserves_fields() {
    let json = br#"{"format_version":1,"rows":3,"granules":2,"columns":[{"name":"id","data_type":"Int64"}],"codec":"None","min_key":[{"Int64":1}],"max_key":[{"Int64":3}]}"#;
    assert_eq!(decode_meta(json).unwrap(), meta());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&encode_meta(&meta()).unwrap()).unwrap(),
        serde_json::from_slice::<serde_json::Value>(json).unwrap()
    );
    let mut composite = meta();
    composite.columns.push(ColumnDef {
        name: "suffix".into(),
        data_type: DataType::Int64,
    });
    composite.min_key = vec![Value::Int64(1), Value::Int64(100)];
    composite.max_key = vec![Value::Int64(2), Value::Int64(0)];
    assert_eq!(
        decode_meta(&encode_meta(&composite).unwrap()).unwrap(),
        composite
    );
}

#[test]
fn metadata_rejects_versions_invalid_counts_and_bounds() {
    let mut invalid = Vec::new();
    let mut m = meta();
    m.format_version = 2;
    invalid.push(m);
    let mut m = meta();
    m.rows = 0;
    invalid.push(m);
    let mut m = meta();
    m.granules = 0;
    invalid.push(m);
    let mut m = meta();
    m.granules = 4;
    invalid.push(m);
    let mut m = meta();
    m.columns.clear();
    invalid.push(m);
    let mut m = meta();
    m.columns[0].name.clear();
    invalid.push(m);
    let mut m = meta();
    m.columns.push(m.columns[0].clone());
    invalid.push(m);
    let mut m = meta();
    m.min_key.clear();
    invalid.push(m);
    let mut m = meta();
    m.max_key.push(Value::Int64(4));
    invalid.push(m);
    let mut m = meta();
    m.max_key[0] = Value::UInt64(3);
    invalid.push(m);
    let mut m = meta();
    m.min_key[0] = Value::Int64(4);
    invalid.push(m);
    for m in invalid {
        assert_corrupt(decode_meta(&serde_json::to_vec(&m).unwrap()));
        assert_corrupt(encode_meta(&m));
    }
    for bytes in [b"{".as_slice(), b"{}", b"null"] {
        assert_corrupt(decode_meta(bytes));
    }
    let mut m = meta();
    m.min_key[0] = Value::Float64(f64::NAN);
    m.max_key[0] = Value::Float64(1.0);
    assert_corrupt(encode_meta(&m));
}

#[test]
fn compressed_frames_preserve_data_and_reject_invalid_streams() {
    for codec in [Codec::Lz4, Codec::Zstd] {
        for payload in [vec![], vec![42], vec![7; 65536], (0..=255).collect()] {
            let frame = encode_frame(&payload, codec).unwrap();
            assert_eq!(frame[0], if codec == Codec::Lz4 { 1 } else { 2 });
            assert_eq!(decode_frame(&frame).unwrap(), payload);
            let mut wrong_size = frame.clone();
            wrong_size[5..9].copy_from_slice(&((payload.len() + 1) as u32).to_le_bytes());
            assert_corrupt(decode_frame(&wrong_size));
            assert_corrupt(decode_frame(&frame[..frame.len() - 1]));
        }
        let mut invalid = vec![if codec == Codec::Lz4 { 1 } else { 2 }];
        invalid.extend_from_slice(&3u32.to_le_bytes());
        invalid.extend_from_slice(&8u32.to_le_bytes());
        invalid.extend_from_slice(&[255; 3]);
        assert_corrupt(decode_frame(&invalid));
        assert!(encode_frame(&[0; 65536], codec).unwrap().len() < 65536);
    }
}

#[test]
fn untrusted_frame_size_is_rejected_before_allocation() {
    for codec in [Codec::None, Codec::Lz4, Codec::Zstd] {
        let mut frame = encode_frame(&[], codec).unwrap();
        frame[5..9].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_corrupt(decode_frame(&frame));
    }
}
