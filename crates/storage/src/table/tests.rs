use super::*;
use crate::{
    part::PartReader,
    test_support::{block, definition},
};
use types::{Block, Codec, ColumnData, DbError, Engine, ScanRequest, TableDefinition};

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

fn rows(table: &MergeTreeTable, def: &TableDefinition) -> Block {
    let request = ScanRequest {
        columns: def.schema.names(),
        predicate: None,
    };
    let blocks: Vec<Block> = table
        .scan(&request)
        .unwrap()
        .collect::<Result<_>>()
        .unwrap();
    Block::concat(&blocks).unwrap()
}

#[test]
fn export_and_import_preserve_all_rows() {
    let source_root = tempfile::tempdir().unwrap();
    let def = definition(Codec::Lz4);
    let source = MergeTreeTable::open(&def, source_root.path()).unwrap();
    source.insert(block(&[3, 1], &["c", "a"])).unwrap();
    source.insert(block(&[2], &["b"])).unwrap();
    let names = source.parts().unwrap();
    let exported: Vec<_> = names
        .iter()
        .map(|name| source.export_part(name).unwrap())
        .collect();

    let dest_root = tempfile::tempdir().unwrap();
    let dest = MergeTreeTable::open(&def, dest_root.path()).unwrap();
    dest.sync_parts(exported, &[]).unwrap();

    assert_eq!(dest.parts().unwrap(), names);
    assert_eq!(rows(&dest, &def), rows(&source, &def));
}

#[test]
fn sync_parts_replaces_merged_parts_without_loss_or_duplication() {
    let source_root = tempfile::tempdir().unwrap();
    let def = definition(Codec::None);
    let source = MergeTreeTable::open(&def, source_root.path()).unwrap();
    source.insert(block(&[1, 3], &["a", "c"])).unwrap();
    source.insert(block(&[2], &["b"])).unwrap();
    let old_names = source.parts().unwrap();
    let old: Vec<_> = old_names
        .iter()
        .map(|name| source.export_part(name).unwrap())
        .collect();

    let dest_root = tempfile::tempdir().unwrap();
    let dest = MergeTreeTable::open(&def, dest_root.path()).unwrap();
    dest.sync_parts(old, &[]).unwrap();
    assert_eq!(rows(&dest, &def), rows(&source, &def));

    source.optimize().unwrap();
    let merged_names = source.parts().unwrap();
    assert_eq!(merged_names.len(), 1);
    let merged = source.export_part(&merged_names[0]).unwrap();
    dest.sync_parts(vec![merged], &old_names).unwrap();

    assert_eq!(dest.parts().unwrap(), merged_names);
    assert_eq!(rows(&dest, &def), rows(&source, &def));
}

#[test]
fn failed_import_keeps_active_parts_unchanged() {
    let source_root = tempfile::tempdir().unwrap();
    let def = definition(Codec::Lz4);
    let source = MergeTreeTable::open(&def, source_root.path()).unwrap();
    source.insert(block(&[7], &["g"])).unwrap();
    let valid = source.export_part("all_1_1_0").unwrap();

    let dest_root = tempfile::tempdir().unwrap();
    let dest = MergeTreeTable::open(&def, dest_root.path()).unwrap();
    dest.insert(block(&[1], &["a"])).unwrap();
    let before_parts = dest.parts().unwrap();
    let before_rows = rows(&dest, &def);

    // Порча part.json обнаруживается по контрольной сумме до публикации.
    let mut corrupted = valid.clone();
    corrupted.name = "all_2_2_0".to_owned();
    corrupted.files[0].1 = b"not a part manifest".to_vec();
    assert!(dest.sync_parts(vec![corrupted], &before_parts).is_err());
    assert_eq!(dest.parts().unwrap(), before_parts);
    assert_eq!(rows(&dest, &def), before_rows);
    assert!(!dest_root.path().join("all_2_2_0").exists());

    // Небезопасное имя файла отклоняется до записи на диск.
    let mut unsafe_part = valid.clone();
    unsafe_part.name = "all_2_2_0".to_owned();
    unsafe_part
        .files
        .push(("../escape".to_owned(), b"x".to_vec()));
    assert!(dest.sync_parts(vec![unsafe_part], &[]).is_err());
    assert_eq!(dest.parts().unwrap(), before_parts);
    assert!(!dest_root.path().join("escape").exists());
    assert!(!dest_root.path().join("all_2_2_0").exists());

    // Порча содержимого .bin тоже обнаруживается по контрольной сумме.
    let mut corrupted_data = valid;
    corrupted_data.name = "all_3_3_0".to_owned();
    let (_, bytes) = corrupted_data
        .files
        .iter_mut()
        .find(|(name, _)| name.ends_with(".bin"))
        .unwrap();
    bytes[0] ^= 0xff;
    assert!(dest.sync_parts(vec![corrupted_data], &[]).is_err());
    assert_eq!(dest.parts().unwrap(), before_parts);
    assert!(!dest_root.path().join("all_3_3_0").exists());
}
