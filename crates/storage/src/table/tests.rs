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

#[test]
fn streamed_import_stages_before_next_fetch_and_rolls_back_on_network_error() {
    let source_root = tempfile::tempdir().unwrap();
    let dest_root = tempfile::tempdir().unwrap();
    let def = definition(Codec::Lz4);
    let source = MergeTreeTable::open(&def, source_root.path()).unwrap();
    source.insert(block(&[1], &["old"])).unwrap();
    source.insert(block(&[2], &["two"])).unwrap();
    source.insert(block(&[3], &["three"])).unwrap();
    let dest = MergeTreeTable::open(&def, dest_root.path()).unwrap();
    dest.insert(block(&[1], &["old"])).unwrap();
    let before = dest.parts().unwrap();
    let mut step = 0;
    let mut interrupted = std::iter::from_fn(|| {
        assert_eq!(dest.parts().unwrap(), before);
        step += 1;
        match step {
            1 => Some(source.export_part("all_2_2_0")),
            2 => {
                // Первая часть уже на диске, но ещё не видна читателям.
                assert!(dest_root.path().join("all_2_2_0").is_dir());
                Some(Err(std::io::Error::other("connection lost").into()))
            }
            _ => None,
        }
    });
    assert!(dest.sync_parts_stream(&mut interrupted, &before).is_err());
    assert_eq!(dest.parts().unwrap(), before);
    assert_eq!(rows(&dest, &def), block(&[1], &["old"]));
    assert!(!dest_root.path().join("all_2_2_0").exists());
    assert!(!dest_root.path().join("tmp_all_2_2_0").exists());

    let old_reader = dest
        .scan(&ScanRequest {
            columns: def.schema.names(),
            predicate: None,
        })
        .unwrap();
    let mut retry = ["all_2_2_0", "all_3_3_0"]
        .into_iter()
        .map(|name| source.export_part(name));
    dest.sync_parts_stream(&mut retry, &before).unwrap();
    assert_eq!(rows(&dest, &def), block(&[2, 3], &["two", "three"]));
    let old: Vec<_> = old_reader.collect::<Result<_>>().unwrap();
    assert_eq!(Block::concat(&old).unwrap(), block(&[1], &["old"]));
    drop(dest);
    let reopened = MergeTreeTable::open(&def, dest_root.path()).unwrap();
    assert_eq!(rows(&reopened, &def), block(&[2, 3], &["two", "three"]));
}

#[test]
fn crash_during_streamed_import_process() {
    let Ok(root) = std::env::var("MINICLICK_IMPORT_CRASH_ROOT") else {
        return;
    };
    let root = Path::new(&root);
    let def = definition(Codec::Lz4);
    let source = MergeTreeTable::open(&def, &root.join("source")).unwrap();
    source.insert(block(&[1], &["old"])).unwrap();
    source.insert(block(&[2], &["new"])).unwrap();
    let dest = MergeTreeTable::open(&def, &root.join("replica")).unwrap();
    dest.insert(block(&[1], &["old"])).unwrap();
    let mut fetched = false;
    let mut download = std::iter::from_fn(|| {
        if fetched {
            assert!(root.join("replica/all_2_2_0").is_dir());
            // Имитируем падение после записи part, но до публикации manifest.
            std::process::exit(0);
        }
        fetched = true;
        Some(source.export_part("all_2_2_0"))
    });
    dest.sync_parts_stream(&mut download, &["all_1_1_0".into()])
        .unwrap();
    panic!("Дочерний процесс не достиг точки падения");
}

#[test]
fn restart_discards_staged_import_after_abrupt_process_exit_and_allows_retry() {
    let root = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "table::tests::crash_during_streamed_import_process",
            "--nocapture",
        ])
        .env("MINICLICK_IMPORT_CRASH_ROOT", root.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(root.path().join("replica/all_2_2_0").exists());
    let def = definition(Codec::Lz4);
    let dest = MergeTreeTable::open(&def, &root.path().join("replica")).unwrap();
    assert_eq!(rows(&dest, &def), block(&[1], &["old"]));
    assert!(!root.path().join("replica/all_2_2_0").exists());
    let source = MergeTreeTable::open(&def, &root.path().join("source")).unwrap();
    let mut retry = std::iter::once_with(|| source.export_part("all_2_2_0"));
    dest.sync_parts_stream(&mut retry, &["all_1_1_0".into()])
        .unwrap();
    assert_eq!(rows(&dest, &def), block(&[2], &["new"]));
}
