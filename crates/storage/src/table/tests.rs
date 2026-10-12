use super::*;
use crate::{
    part::PartReader,
    test_support::{block, definition},
};
use types::{Codec, ColumnData, DbError, Engine};

#[test]
fn insert_preserves_schema_atomicity_and_unique_names_after_restart() {
    let root = tempfile::tempdir().unwrap();
    let def = definition(Codec::Lz4);
    let table = MergeTreeTable::open(&def, root.path()).unwrap();
    table.insert(block(&[3, 1], &["c", "a"])).unwrap();
    let before = table.parts().unwrap();
    let wrong = block(&[2], &["b"])
        .select(&["text".into(), "id".into()])
        .unwrap();
    assert!(matches!(table.insert(wrong), Err(DbError::TypeMismatch(_))));
    assert_eq!(table.parts().unwrap(), before);
    table.insert(block(&[], &[])).unwrap();
    assert_eq!(table.parts().unwrap(), before);
    table.flush().unwrap();
    drop(table);
    let table = MergeTreeFactory.open(&def, root.path()).unwrap();
    assert_eq!(table.parts().unwrap(), ["all_1_1_0"]);
    table.insert(block(&[2], &["b"])).unwrap();
    assert_eq!(table.parts().unwrap(), ["all_1_1_0", "all_2_2_0"]);
    assert_eq!(table.definition(), &def);
}

#[test]
fn failed_disk_write_does_not_publish_data() {
    let root = tempfile::tempdir().unwrap();
    let def = definition(Codec::None);
    let table = MergeTreeTable::open(&def, root.path()).unwrap();
    table.insert(block(&[1], &["a"])).unwrap();
    fs::create_dir(root.path().join("tmp_all_2_2_0")).unwrap();
    assert!(table.insert(block(&[2], &["b"])).is_err());
    assert_eq!(table.parts().unwrap(), ["all_1_1_0"]);
    let parts = table.repository.snapshot().unwrap();
    assert_eq!(
        PartReader::new(&parts[0], &["id".into()])
            .unwrap()
            .read_granule(0)
            .unwrap()
            .column("id"),
        Some(&ColumnData::Int64(vec![1]))
    );
    fs::remove_dir(root.path().join("tmp_all_2_2_0")).unwrap();
    table.insert(block(&[3], &["c"])).unwrap();
    assert_eq!(table.parts().unwrap(), ["all_1_1_0", "all_3_3_0"]);
}

#[test]
fn drop_invalidates_old_readers_without_deleting_recreated_table() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("table");
    let def = definition(Codec::None);
    let table = MergeTreeTable::open(&def, &root).unwrap();
    table.insert(block(&[1], &["old"])).unwrap();
    let old = table.repository.snapshot().unwrap();
    old[0].retire();
    table.drop_data().unwrap();
    assert!(table.insert(block(&[2], &["bad"])).is_err());
    assert!(table.parts().is_err());
    assert!(PartReader::new(&old[0], &["id".into()]).is_err());
    let new = MergeTreeTable::open(&def, &root).unwrap();
    new.insert(block(&[3], &["new"])).unwrap();
    drop(old);
    drop(table);
    assert!(root.join("all_1_1_0").exists());
    assert_eq!(new.parts().unwrap(), ["all_1_1_0"]);
    let mut memory = def;
    memory.engine = Engine::Memory;
    assert!(MergeTreeTable::open(&memory, parent.path()).is_err());
}
