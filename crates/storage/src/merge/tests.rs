use super::*;
use crate::{
    part::write_part,
    repository::PartRepository,
    test_support::{block, definition},
};
use types::Codec;

#[test]
fn policy_selects_neighboring_small_parts_and_advances_merge_level() {
    let root = tempfile::tempdir().unwrap();
    let def = definition(Codec::None);
    let repo = PartRepository::open(root.path(), &def).unwrap();
    for rows in [8, 1, 1, 1, 1] {
        let id = repo.reserve_insert().unwrap();
        write_part(
            root.path(),
            &id.name(),
            &def,
            &block(&vec![1; rows], &vec!["x"; rows]),
        )
        .unwrap();
        repo.replace(vec![(id, repo.open_part(id).unwrap())], &[])
            .unwrap();
    }
    let chosen = select(&repo.snapshot().unwrap());
    let (id, originals) = merged_id(&chosen).unwrap();
    assert_eq!(
        id,
        PartId {
            min: 2,
            max: 5,
            level: 1
        }
    );
    assert_eq!(
        originals.iter().map(|id| id.min).collect::<Vec<_>>(),
        [2, 3, 4, 5]
    );
    assert!(select(&chosen[..1]).is_empty());
}
