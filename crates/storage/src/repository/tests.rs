use super::*;
use crate::{
    part::write_part,
    test_support::{block, definition},
};
use types::Codec;

fn insert(repo: &PartRepository, def: &TableDefinition, value: i64) -> PartId {
    let id = repo.reserve_insert().unwrap();
    write_part(&repo.root, &id.name(), def, &block(&[value], &["x"])).unwrap();
    repo.replace(vec![(id, repo.open_part(id).unwrap())], &[])
        .unwrap();
    id
}

#[test]
fn names_are_canonical_and_ranges_are_valid() {
    for name in [
        "all_0_1_0",
        "all_2_1_1",
        "all_01_1_0",
        "all_1_2_0",
        "all_+1_1_0",
        "../all_1_1_0",
        "all_1_1_0_extra",
    ] {
        assert!(PartId::parse(name).is_err(), "{name}");
    }
    assert_eq!(
        PartId::parse("all_1_2_1").unwrap(),
        PartId {
            min: 1,
            max: 2,
            level: 1
        }
    );
}

#[test]
fn restart_restores_committed_set_and_discards_orphans_without_reusing_ids() {
    let root = tempfile::tempdir().unwrap();
    let def = definition(Codec::Lz4);
    let repo = PartRepository::open(root.path(), &def).unwrap();
    let active = insert(&repo, &def, 1);
    let orphan = repo.reserve_insert().unwrap();
    write_part(root.path(), &orphan.name(), &def, &block(&[2], &["orphan"])).unwrap();
    fs::create_dir(root.path().join("tmp_all_3_3_0")).unwrap();
    drop(repo);
    let repo = PartRepository::open(root.path(), &def).unwrap();
    assert_eq!(repo.snapshot().unwrap().len(), 1);
    assert_eq!(
        repo.snapshot().unwrap()[0].path,
        root.path().join(active.name())
    );
    assert!(!root.path().join(orphan.name()).exists());
    assert!(!root.path().join("tmp_all_3_3_0").exists());
    assert_eq!(repo.reserve_insert().unwrap().min, 3);
}

#[test]
fn replacement_survives_restart_and_old_parts_live_until_last_reader() {
    let root = tempfile::tempdir().unwrap();
    let def = definition(Codec::None);
    let repo = PartRepository::open(root.path(), &def).unwrap();
    let a = insert(&repo, &def, 1);
    let b = insert(&repo, &def, 2);
    let old = repo.snapshot().unwrap();
    let merged = PartId {
        min: a.min,
        max: b.max,
        level: 1,
    };
    write_part(
        root.path(),
        &merged.name(),
        &def,
        &block(&[1, 2], &["x", "x"]),
    )
    .unwrap();
    repo.replace(vec![(merged, repo.open_part(merged).unwrap())], &[a, b])
        .unwrap();
    assert_eq!(repo.snapshot().unwrap().len(), 1);
    assert!(root.path().join(a.name()).exists());
    drop(repo);
    assert!(PartRepository::open(root.path(), &def).is_err());
    drop(old);
    assert!(!root.path().join(a.name()).exists());
    let repo = PartRepository::open(root.path(), &def).unwrap();
    assert_eq!(repo.snapshot().unwrap()[0].meta.rows, 2);
    assert_eq!(repo.reserve_insert().unwrap().min, 3);
}

#[test]
fn interrupted_publication_keeps_previous_set_and_failed_manifest_does_not_change_memory() {
    let root = tempfile::tempdir().unwrap();
    let def = definition(Codec::None);
    let repo = PartRepository::open(root.path(), &def).unwrap();
    let a = insert(&repo, &def, 1);
    let b = insert(&repo, &def, 2);
    let merged = PartId {
        min: 1,
        max: 2,
        level: 1,
    };
    write_part(
        root.path(),
        &merged.name(),
        &def,
        &block(&[1, 2], &["x", "x"]),
    )
    .unwrap();
    fs::create_dir(root.path().join("tmp_table.json")).unwrap();
    assert!(repo
        .replace(vec![(merged, repo.open_part(merged).unwrap())], &[a, b])
        .is_err());
    assert_eq!(repo.snapshot().unwrap().len(), 2);
    fs::remove_dir(root.path().join("tmp_table.json")).unwrap();
    drop(repo);
    let repo = PartRepository::open(root.path(), &def).unwrap();
    assert_eq!(
        repo.snapshot()
            .unwrap()
            .iter()
            .map(|p| p.meta.rows)
            .sum::<u64>(),
        2
    );
    assert!(!root.path().join(merged.name()).exists());
}

#[test]
fn crash_writer_process() {
    let Ok(root) = std::env::var("MINICLICK_CRASH_ROOT") else {
        return;
    };
    let mode = std::env::var("MINICLICK_CRASH_MODE").unwrap();
    let def = definition(Codec::Lz4);
    let table = crate::MergeTreeTable::open(&def, Path::new(&root)).unwrap();
    use types::Table;
    table.insert(block(&[3, 1], &["c", "a"])).unwrap();
    table.insert(block(&[2], &["b"])).unwrap();
    if mode == "before" {
        let parts = table.repository.snapshot().unwrap();
        let (id, _) = crate::merge::merged_id(&parts).unwrap();
        crate::merge::write_merge(Path::new(&root), id, &def, &parts).unwrap();
    } else {
        let _old_reader = table.scan(&types::ScanRequest::default()).unwrap();
        table.optimize().unwrap();
        assert!(Path::new(&root).join("all_1_1_0").exists());
        // exit намеренно пропускает Drop: исходные каталоги остаются на диске.
        std::process::exit(0);
    }
    std::process::exit(0);
}

#[test]
fn abrupt_process_exit_before_and_after_merge_commit_recovers_without_duplicates() {
    use types::{ColumnData, ScanRequest, Table};
    for mode in ["before", "after"] {
        let root = tempfile::tempdir().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "repository::tests::crash_writer_process",
                "--nocapture",
            ])
            .env("MINICLICK_CRASH_ROOT", root.path())
            .env("MINICLICK_CRASH_MODE", mode)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(root.path().join("all_1_1_0").exists());
        assert!(root.path().join("all_1_2_1").exists());
        let table = crate::MergeTreeTable::open(&definition(Codec::Lz4), root.path()).unwrap();
        assert_eq!(
            table.parts().unwrap().len(),
            if mode == "before" { 2 } else { 1 }
        );
        let request = ScanRequest {
            columns: vec!["id".into()],
            predicate: None,
        };
        let mut ids: Vec<i64> = Vec::new();
        for block in table.scan(&request).unwrap() {
            let block = block.unwrap();
            let ColumnData::Int64(values) = block.column("id").unwrap() else {
                panic!("id type")
            };
            ids.extend_from_slice(values);
        }
        ids.sort();
        assert_eq!(ids, [1, 2, 3]);
        if mode == "before" {
            assert!(!root.path().join("all_1_2_1").exists());
        } else {
            assert!(!root.path().join("all_1_1_0").exists());
        }
        table.insert(block(&[4], &["d"])).unwrap();
        assert!(table.parts().unwrap().contains(&"all_3_3_0".into()));
    }
}

#[test]
fn corrupted_manifest_or_index_is_rejected_instead_of_silently_dropping_rows() {
    for damaged in [
        "table.json",
        "all_1_1_0/primary.idx",
        "all_1_1_0/part.json",
        "all_1_1_0/text.mrk",
    ] {
        let root = tempfile::tempdir().unwrap();
        let def = definition(Codec::None);
        let repo = PartRepository::open(root.path(), &def).unwrap();
        insert(&repo, &def, 1);
        drop(repo);
        fs::write(root.path().join(damaged), [0]).unwrap();
        assert!(
            PartRepository::open(root.path(), &def).is_err(),
            "{damaged}"
        );
        assert!(root.path().join("all_1_1_0").exists());
    }
}
