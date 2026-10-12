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
