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
