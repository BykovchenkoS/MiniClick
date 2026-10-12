use super::*;
use crate::{
    part::write_part,
    repository::PartRepository,
    test_support::{block, definition},
};
use types::{Codec, Value};

#[test]
fn custom_matcher_selects_ranges_and_counts_only_their_rows() {
    let root = tempfile::tempdir().unwrap();
    let def = definition(Codec::None);
    let repo = PartRepository::open(root.path(), &def).unwrap();
    let id = repo.reserve_insert().unwrap();
    write_part(
        root.path(),
        &id.name(),
        &def,
        &block(&[1, 2, 3, 4, 5], &["a", "b", "c", "d", "e"]),
    )
    .unwrap();
    repo.replace(vec![(id, repo.open_part(id).unwrap())], &[])
        .unwrap();
    let request = ScanRequest {
        columns: vec![],
        predicate: Some(Expr::lit(Value::Int64(0))),
    };
    let plan = plan_with(&def, repo.snapshot().unwrap(), &request, |_, _, range| {
        range.max[0].compare(&Value::Int64(3)) == Some(std::cmp::Ordering::Greater)
    })
    .unwrap();
    assert_eq!(plan.stats.granules_selected, 2);
    assert_eq!(plan.stats.rows_selected, 3);
    assert_eq!(
        plan.parts[0].ranges,
        std::iter::once(1..3).collect::<Vec<_>>()
    );
}
