use catalog::DiskCatalog;
use std::sync::Arc;
use types::{
    testing::MemTableFactory, Catalog, ColumnDef, DataType, DbError, Engine, TableDefinition,
    TableSchema, TableSettings,
};

fn definition(name: &str) -> TableDefinition {
    TableDefinition {
        name: name.into(),
        engine: Engine::Memory,
        schema: TableSchema {
            columns: vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::UInt64,
            }],
        },
        order_by: vec![],
        settings: TableSettings::default(),
    }
}
fn open(path: &std::path::Path) -> types::Result<DiskCatalog> {
    DiskCatalog::open(path, vec![(Engine::Memory, Arc::new(MemTableFactory))])
}
fn manifest(path: &std::path::Path) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(path.join("catalog.json")).unwrap()).unwrap()
}

#[test]
fn names_flags_schema_and_memory_restart() {
    let root = tempfile::tempdir().unwrap();
    let catalog = open(root.path()).unwrap();
    catalog.create_table(definition("z"), false).unwrap();
    catalog.create_table(definition("a"), false).unwrap();
    assert!(matches!(
        catalog.create_table(definition("a"), false),
        Err(DbError::TableExists(_))
    ));
    catalog.create_table(definition("a"), true).unwrap();
    let table = catalog.get_table("a").unwrap();
    table
        .insert(
            types::Block::new(vec![types::Column {
                name: "id".into(),
                data: types::ColumnData::UInt64(vec![42]),
            }])
            .unwrap(),
        )
        .unwrap();
    assert_eq!(
        catalog
            .list_tables()
            .iter()
            .map(|table| table.name.as_str())
            .collect::<Vec<_>>(),
        ["a", "z"]
    );
    assert!(matches!(
        catalog.get_table("A"),
        Err(DbError::UnknownTable(_))
    ));
    assert!(open(root.path()).is_err());
    drop(catalog);
    let catalog = open(root.path()).unwrap();
    assert_eq!(
        catalog.get_table("a").unwrap().definition(),
        &definition("a")
    );
    assert_eq!(
        catalog
            .get_table("a")
            .unwrap()
            .explain_scan(&Default::default())
            .unwrap()
            .rows_total,
        0
    );
    catalog.drop_table("a", false).unwrap();
    assert!(matches!(
        catalog.drop_table("a", false),
        Err(DbError::UnknownTable(_))
    ));
    catalog.drop_table("a", true).unwrap();
    drop(catalog);
    assert_eq!(open(root.path()).unwrap().list_tables(), [definition("z")]);
}

#[test]
fn recreated_names_have_distinct_directories_and_sql_names_are_not_paths() {
    let root = tempfile::tempdir().unwrap();
    let catalog = open(root.path()).unwrap();
    let name = "../Таблица/данные";
    catalog.create_table(definition(name), false).unwrap();
    let first = manifest(root.path())["tables"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let old = catalog.get_table(name).unwrap();
    catalog.drop_table(name, false).unwrap();
    catalog.create_table(definition(name), false).unwrap();
    let second = manifest(root.path())["tables"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_ne!(first, second);
    assert!(!root.path().join("tables").join(first).exists());
    drop(old);
    assert!(root.path().join("tables").join(second).is_dir());
}

#[test]
fn failed_metadata_publication_preserves_previous_state() {
    let root = tempfile::tempdir().unwrap();
    let catalog = open(root.path()).unwrap();
    catalog.create_table(definition("kept"), false).unwrap();
    let before = std::fs::read(root.path().join("catalog.json")).unwrap();
    std::fs::create_dir(root.path().join("tmp_catalog.json")).unwrap();
    assert!(catalog.create_table(definition("lost"), false).is_err());
    assert!(catalog.drop_table("kept", false).is_err());
    assert_eq!(catalog.list_tables(), [definition("kept")]);
    assert_eq!(
        std::fs::read(root.path().join("catalog.json")).unwrap(),
        before
    );
    assert_eq!(
        std::fs::read_dir(root.path().join("tables"))
            .unwrap()
            .count(),
        1
    );
    std::fs::remove_dir(root.path().join("tmp_catalog.json")).unwrap();
    drop(catalog);
    assert_eq!(
        open(root.path()).unwrap().list_tables(),
        [definition("kept")]
    );
}

#[test]
fn startup_cleans_only_managed_orphans_after_validation() {
    let root = tempfile::tempdir().unwrap();
    let catalog = open(root.path()).unwrap();
    catalog.create_table(definition("kept"), false).unwrap();
    drop(catalog);
    let orphan = root
        .path()
        .join("tables")
        .join(uuid::Uuid::new_v4().to_string());
    std::fs::create_dir(&orphan).unwrap();
    std::fs::write(orphan.join("partial.bin"), b"partial").unwrap();
    let unknown = root.path().join("tables/user-files");
    std::fs::create_dir(&unknown).unwrap();
    std::fs::write(root.path().join("tmp_catalog.json"), b"incomplete").unwrap();
    let catalog = open(root.path()).unwrap();
    assert!(!orphan.exists());
    assert!(unknown.exists());
    assert!(!root.path().join("tmp_catalog.json").exists());
    assert_eq!(catalog.list_tables(), [definition("kept")]);
}

#[test]
fn corrupt_or_missing_registered_state_is_never_recreated() {
    let root = tempfile::tempdir().unwrap();
    let catalog = open(root.path()).unwrap();
    catalog.create_table(definition("kept"), false).unwrap();
    drop(catalog);
    let original = manifest(root.path());
    for change in ["version", "duplicate", "path", "schema"] {
        let mut invalid = original.clone();
        match change {
            "version" => invalid["version"] = 2.into(),
            "duplicate" => {
                let entry = invalid["tables"][0].clone();
                invalid["tables"].as_array_mut().unwrap().push(entry);
            }
            "path" => invalid["tables"][0]["id"] = "../../outside".into(),
            "schema" => {
                invalid["tables"][0]["definition"]["schema"]["columns"] = serde_json::json!([])
            }
            _ => unreachable!(),
        }
        std::fs::write(root.path().join("catalog.json"), invalid.to_string()).unwrap();
        assert!(open(root.path()).is_err(), "{change}");
    }
    std::fs::write(root.path().join("catalog.json"), original.to_string()).unwrap();
    let path = root
        .path()
        .join("tables")
        .join(original["tables"][0]["id"].as_str().unwrap());
    std::fs::remove_dir(&path).unwrap();
    assert!(open(root.path()).is_err());
    assert!(!path.exists());
}

#[test]
fn concurrent_creates_publish_all_names_once() {
    let root = tempfile::tempdir().unwrap();
    let catalog = Arc::new(open(root.path()).unwrap());
    std::thread::scope(|scope| {
        for index in 0..12 {
            let catalog = catalog.clone();
            scope.spawn(move || {
                catalog
                    .create_table(definition(&format!("table_{index:02}")), false)
                    .unwrap();
                catalog.create_table(definition("shared"), true).unwrap();
            });
        }
    });
    assert_eq!(catalog.list_tables().len(), 13);
    drop(catalog);
    assert_eq!(open(root.path()).unwrap().list_tables().len(), 13);
}

#[test]
fn missing_manifest_does_not_import_existing_directories() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(root.path().join("tables/standalone-storage")).unwrap();
    assert!(open(root.path()).is_err());
    assert!(!root.path().join("catalog.json").exists());
}
