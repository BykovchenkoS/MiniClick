use super::*;
use crate::test_support::{block, definition};
use types::{Codec, DataType};

#[test]
fn writer_sorts_composite_keys_and_records_partial_last_granule() {
    let root = tempfile::tempdir().unwrap();
    let def = definition(Codec::None);
    let meta = write_part(
        root.path(),
        "all_1_1_0",
        &def,
        &block(&[2, 1, 1, 3, 2], &["b", "z", "a", "c", "a"]),
    )
    .unwrap();
    let path = root.path().join("all_1_1_0");
    assert!(!root.path().join("tmp_all_1_1_0").exists());
    assert_eq!(meta.rows, 5);
    assert_eq!(meta.granules, 3);
    assert_eq!(meta.min_key, [Value::Int64(1), Value::String("a".into())]);
    assert_eq!(meta.max_key, [Value::Int64(3), Value::String("c".into())]);
    let marks = format::decode_marks(&fs::read(path.join("id.mrk")).unwrap()).unwrap();
    assert_eq!(marks.iter().map(|m| m.rows).collect::<Vec<_>>(), [2, 2, 1]);
    assert_eq!(
        marks.iter().map(|m| m.offset).collect::<Vec<_>>(),
        [0, 25, 50]
    );
    let bytes = fs::read(path.join("id.bin")).unwrap();
    assert_eq!(
        format::decode_column(
            &format::decode_frame(&bytes[..25]).unwrap(),
            DataType::Int64,
            2
        )
        .unwrap(),
        ColumnData::Int64(vec![1, 1])
    );
    assert_eq!(
        format::decode_column(
            &format::decode_frame(&bytes[25..50]).unwrap(),
            DataType::Int64,
            2
        )
        .unwrap(),
        ColumnData::Int64(vec![2, 2])
    );
    assert_eq!(
        format::decode_index(
            &fs::read(path.join("primary.idx")).unwrap(),
            &[DataType::Int64, DataType::String],
            3
        )
        .unwrap(),
        [
            ColumnData::Int64(vec![1, 2, 3]),
            ColumnData::String(vec!["a".into(), "a".into(), "c".into()])
        ]
    );
    assert_eq!(
        format::decode_meta(&fs::read(path.join("part.json")).unwrap()).unwrap(),
        meta
    );
}

#[test]
fn invalid_or_abandoned_writes_do_not_publish_parts() {
    let root = tempfile::tempdir().unwrap();
    let def = definition(Codec::Lz4);
    let bad = Block::with_rows(vec![], 1).unwrap();
    assert!(matches!(
        write_part(root.path(), "all_1_1_0", &def, &bad),
        Err(DbError::TypeMismatch(_))
    ));
    assert!(write_part(root.path(), "all_1_1_0", &def, &block(&[], &[])).is_err());
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
    {
        let mut writer = PartWriter::new(root.path(), "all_1_1_0", &def).unwrap();
        assert!(writer.write_granule(&block(&[2, 1], &["b", "a"])).is_err());
    }
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
    let mut unsafe_def = def.clone();
    unsafe_def.schema.columns[0].name = "../id".into();
    unsafe_def.order_by[0] = "../id".into();
    assert!(PartWriter::new(root.path(), "all_1_1_0", &unsafe_def).is_err());
    assert!(PartWriter::new(root.path(), "../escape", &def).is_err());
}

#[test]
fn existing_part_is_never_overwritten() {
    let root = tempfile::tempdir().unwrap();
    let def = definition(Codec::Zstd);
    write_part(root.path(), "all_1_1_0", &def, &block(&[1], &["first"])).unwrap();
    let original = fs::read(root.path().join("all_1_1_0/text.bin")).unwrap();
    assert!(write_part(root.path(), "all_1_1_0", &def, &block(&[2], &["second"])).is_err());
    assert_eq!(
        fs::read(root.path().join("all_1_1_0/text.bin")).unwrap(),
        original
    );
}

#[test]
fn reader_returns_requested_columns_and_exact_granules_for_all_codecs() {
    for codec in [Codec::None, Codec::Lz4, Codec::Zstd] {
        let root = tempfile::tempdir().unwrap();
        let def = definition(codec);
        write_part(
            root.path(),
            "all_1_1_0",
            &def,
            &block(&[5, 2, 4, 1, 3], &["e", "b", "d", "a", "c"]),
        )
        .unwrap();
        let part = Arc::new(DataPart::open(&root.path().join("all_1_1_0"), &def).unwrap());
        let mut reader = PartReader::new(&part, &["text".into(), "id".into()]).unwrap();
        assert_eq!(
            reader.read_granule(1).unwrap(),
            block(&[3, 4], &["c", "d"])
                .select(&["text".into(), "id".into()])
                .unwrap()
        );
        assert_eq!(
            reader.read_granule(2).unwrap(),
            block(&[5], &["e"])
                .select(&["text".into(), "id".into()])
                .unwrap()
        );
        assert_eq!(
            reader.read_granule(0).unwrap(),
            block(&[1, 2], &["a", "b"])
                .select(&["text".into(), "id".into()])
                .unwrap()
        );
        assert!(reader.read_granule(3).is_err());
        assert!(matches!(
            PartReader::new(&part, &["ID".into()]),
            Err(DbError::UnknownColumn(_))
        ));
        fs::remove_file(part.path.join("text.bin")).unwrap();
        assert_eq!(
            PartReader::new(&part, &["id".into()])
                .unwrap()
                .read_granule(1)
                .unwrap()
                .column("id"),
            Some(&ColumnData::Int64(vec![3, 4]))
        );
        fs::remove_file(part.path.join("id.bin")).unwrap();
        let mut count_reader = PartReader::new(&part, &[]).unwrap();
        assert_eq!(
            count_reader.read_granule(2).unwrap(),
            Block::with_rows(vec![], 1).unwrap()
        );
    }
}

#[test]
fn reader_rejects_inconsistent_metadata_marks_index_and_column_frames() {
    for damaged in ["part.json", "id.mrk", "primary.idx", "id.bin"] {
        let root = tempfile::tempdir().unwrap();
        let def = definition(Codec::None);
        write_part(
            root.path(),
            "all_1_1_0",
            &def,
            &block(&[1, 2, 3], &["a", "b", "c"]),
        )
        .unwrap();
        let path = root.path().join("all_1_1_0");
        fs::write(path.join(damaged), [0]).unwrap();
        if damaged == "id.bin" {
            let part = Arc::new(DataPart::open(&path, &def).unwrap());
            assert!(PartReader::new(&part, &["id".into()])
                .unwrap()
                .read_granule(0)
                .is_err());
        } else {
            assert!(matches!(
                DataPart::open(&path, &def),
                Err(DbError::Internal(_))
            ));
        }
    }
    let root = tempfile::tempdir().unwrap();
    let def = definition(Codec::None);
    write_part(
        root.path(),
        "all_1_1_0",
        &def,
        &block(&[1, 2, 3], &["a", "b", "c"]),
    )
    .unwrap();
    let path = root.path().join("all_1_1_0");
    let marks = [
        Mark { offset: 0, rows: 1 },
        Mark {
            offset: 25,
            rows: 2,
        },
    ];
    fs::write(path.join("id.mrk"), format::encode_marks(&marks).unwrap()).unwrap();
    assert!(matches!(
        DataPart::open(&path, &def),
        Err(DbError::Internal(_))
    ));
}
