mod common;

use common::{block, definition};
use storage::MergeTreeTable;
use types::{testing::MemTable, Block, Codec, ColumnData, DbError, ScanRequest, Table};
use types::{BinaryOp, Expr, Value};

fn read(table: &dyn Table, request: &ScanRequest) -> Vec<Block> {
    table
        .scan(request)
        .unwrap()
        .collect::<types::Result<Vec<_>>>()
        .unwrap()
}

fn rows(blocks: &[Block]) -> Vec<(i64, String)> {
    let mut rows = Vec::new();
    for block in blocks {
        let ColumnData::Int64(ids) = block.column("id").unwrap() else {
            panic!("id type")
        };
        let ColumnData::String(texts) = block.column("text").unwrap() else {
            panic!("text type")
        };
        rows.extend(ids.iter().copied().zip(texts.iter().cloned()));
    }
    rows.sort();
    rows
}

#[test]
fn full_scan_matches_memory_and_preserves_column_order_and_counts() {
    let root = tempfile::tempdir().unwrap();
    let def = definition(Codec::Lz4);
    let table = MergeTreeTable::open(&def, root.path()).unwrap();
    let memory = MemTable::new(def);
    for input in [
        block(&[3, 1, 2], &["c", "a", "b"]),
        block(&[1, 4], &["z", "d"]),
    ] {
        table.insert(input.clone()).unwrap();
        memory.insert(input).unwrap();
    }
    let request = ScanRequest {
        columns: vec!["text".into(), "id".into()],
        predicate: None,
    };
    let actual = read(&table, &request);
    assert_eq!(rows(&actual), rows(&read(&memory, &request)));
    assert_eq!(
        rows(&actual),
        [
            (1, "a".into()),
            (1, "z".into()),
            (2, "b".into()),
            (3, "c".into()),
            (4, "d".into())
        ]
    );
    assert!(actual.iter().all(|b| b
        .header()
        .iter()
        .map(|c| c.name.as_str())
        .collect::<Vec<_>>()
        == ["text", "id"]));
    let stats = table.explain_scan(&request).unwrap();
    assert_eq!(
        (
            stats.parts_total,
            stats.parts_selected,
            stats.granules_total,
            stats.granules_selected,
            stats.rows_total,
            stats.rows_selected
        ),
        (2, 2, 3, 3, 5, 5)
    );
    let counts = read(&table, &ScanRequest::default());
    assert_eq!(counts.iter().map(Block::rows).sum::<usize>(), 5);
    assert!(counts.iter().all(|b| b.columns().is_empty()));
}

#[test]
fn empty_table_validates_columns_and_scan_keeps_insertion_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let table = MergeTreeTable::open(&definition(Codec::None), root.path()).unwrap();
    assert!(read(&table, &ScanRequest::default()).is_empty());
    assert_eq!(
        table.explain_scan(&ScanRequest::default()).unwrap(),
        types::ScanStats::default()
    );
    assert!(matches!(
        table.scan(&ScanRequest {
            columns: vec!["missing".into()],
            predicate: None
        }),
        Err(DbError::UnknownColumn(_))
    ));
    table.insert(block(&[1, 2], &["a", "b"])).unwrap();
    let request = ScanRequest {
        columns: vec!["id".into(), "text".into()],
        predicate: None,
    };
    let old = table.scan(&request).unwrap();
    table.insert(block(&[3], &["c"])).unwrap();
    assert_eq!(
        rows(&old.collect::<types::Result<Vec<_>>>().unwrap()),
        [(1, "a".into()), (2, "b".into())]
    );
    assert_eq!(
        read(&table, &request)
            .iter()
            .map(Block::rows)
            .sum::<usize>(),
        3
    );
}

#[test]
fn scan_reads_lazily_and_returns_one_error_before_ending() {
    let root = tempfile::tempdir().unwrap();
    let table = MergeTreeTable::open(&definition(Codec::None), root.path()).unwrap();
    table.insert(block(&[1, 2, 3], &["a", "b", "c"])).unwrap();
    let request = ScanRequest {
        columns: vec!["id".into()],
        predicate: None,
    };
    let mut stream = table.scan(&request).unwrap();
    // Первая рамка цела, вторая усечена: ошибка возникает после первого блока.
    std::fs::OpenOptions::new()
        .write(true)
        .open(root.path().join("all_1_1_0/id.bin"))
        .unwrap()
        .set_len(26)
        .unwrap();
    assert_eq!(
        stream.next().unwrap().unwrap().column("id"),
        Some(&ColumnData::Int64(vec![1, 2]))
    );
    assert!(stream.next().unwrap().is_err());
    assert!(stream.next().is_none());
    assert!(stream.next().is_none());
    std::fs::remove_file(root.path().join("all_1_1_0/id.bin")).unwrap();
    assert_eq!(table.explain_scan(&request).unwrap().rows_selected, 3);
    assert_eq!(
        read(&table, &ScanRequest::default())
            .iter()
            .map(Block::rows)
            .sum::<usize>(),
        3
    );
}

#[test]
fn pruning_skips_damaged_granules_and_parts_and_explain_matches_scan() {
    let root = tempfile::tempdir().unwrap();
    let table = MergeTreeTable::open(&definition(Codec::None), root.path()).unwrap();
    table
        .insert(block(&[1, 2, 3, 4, 5, 6], &["a", "b", "c", "d", "e", "f"]))
        .unwrap();
    table.insert(block(&[10, 11], &["j", "k"])).unwrap();
    let path = root.path().join("all_1_1_0/id.bin");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[0] = 255;
    bytes[50] = 255;
    std::fs::write(&path, bytes).unwrap();
    std::fs::remove_file(root.path().join("all_2_2_0/id.bin")).unwrap();
    let request = ScanRequest {
        columns: vec!["id".into(), "text".into()],
        predicate: Some(Expr::binary(
            BinaryOp::Eq,
            Expr::col("id"),
            Expr::lit(Value::Int64(4)),
        )),
    };
    let stats = table.explain_scan(&request).unwrap();
    assert_eq!(
        (
            stats.parts_total,
            stats.parts_selected,
            stats.granules_total,
            stats.granules_selected,
            stats.rows_total,
            stats.rows_selected
        ),
        (2, 1, 4, 1, 8, 2)
    );
    assert_eq!(
        rows(&read(&table, &request)),
        [(3, "c".into()), (4, "d".into())]
    );
    let mut empty = request;
    empty.predicate = Some(Expr::binary(
        BinaryOp::Gt,
        Expr::col("id"),
        Expr::lit(Value::Int64(100)),
    ));
    assert_eq!(table.explain_scan(&empty).unwrap().parts_selected, 0);
    assert!(read(&table, &empty).is_empty());
}

#[test]
fn pruning_never_loses_matches_at_duplicate_keys_or_inclusive_boundaries() {
    let root = tempfile::tempdir().unwrap();
    let table = MergeTreeTable::open(&definition(Codec::Zstd), root.path()).unwrap();
    table
        .insert(block(&[1, 2, 2, 2, 3, 4], &["a", "a", "b", "c", "a", "a"]))
        .unwrap();
    table.insert(block(&[0, 2, 5], &["a", "d", "a"])).unwrap();
    let full = rows(&read(
        &table,
        &ScanRequest {
            columns: vec!["id".into(), "text".into()],
            predicate: None,
        },
    ));
    for bound in -1..=6 {
        for op in [
            BinaryOp::Eq,
            BinaryOp::Lt,
            BinaryOp::LtEq,
            BinaryOp::Gt,
            BinaryOp::GtEq,
        ] {
            let matches = |id: i64| match op {
                BinaryOp::Eq => id == bound,
                BinaryOp::Lt => id < bound,
                BinaryOp::LtEq => id <= bound,
                BinaryOp::Gt => id > bound,
                BinaryOp::GtEq => id >= bound,
                _ => unreachable!(),
            };
            let request = ScanRequest {
                columns: vec!["id".into(), "text".into()],
                predicate: Some(Expr::binary(
                    op,
                    Expr::col("id"),
                    Expr::lit(Value::Int64(bound)),
                )),
            };
            let blocks = read(&table, &request);
            let actual: Vec<_> = rows(&blocks)
                .into_iter()
                .filter(|(id, _)| matches(*id))
                .collect();
            let expected: Vec<_> = full
                .iter()
                .filter(|(id, _)| matches(*id))
                .cloned()
                .collect();
            assert_eq!(actual, expected, "{op:?} {bound}");
            let stats = table.explain_scan(&request).unwrap();
            assert_eq!(blocks.len(), stats.granules_selected);
            assert_eq!(
                blocks.iter().map(Block::rows).sum::<usize>() as u64,
                stats.rows_selected
            );
        }
    }
    let unsupported = ScanRequest {
        columns: vec![],
        predicate: Some(Expr::binary(
            BinaryOp::Eq,
            Expr::col("text"),
            Expr::lit(Value::String("no".into())),
        )),
    };
    assert_eq!(table.explain_scan(&unsupported).unwrap().rows_selected, 9);
}

#[test]
fn optimize_preserves_duplicates_old_readers_and_data_after_restart() {
    for codec in [Codec::None, Codec::Lz4, Codec::Zstd] {
        let root = tempfile::tempdir().unwrap();
        let def = definition(codec);
        let table = MergeTreeTable::open(&def, root.path()).unwrap();
        table.optimize().unwrap();
        for _ in 0..9 {
            table.insert(block(&[3, 1, 1], &["c", "b", "a"])).unwrap();
        }
        let request = ScanRequest {
            columns: vec!["id".into(), "text".into()],
            predicate: None,
        };
        let before = rows(&read(&table, &request));
        let old = table.scan(&request).unwrap();
        table.optimize().unwrap();
        assert_eq!(table.parts().unwrap().len(), 1);
        assert_eq!(rows(&read(&table, &request)), before);
        assert_eq!(table.explain_scan(&request).unwrap().rows_total, 27);
        assert!(root.path().join("all_1_1_0").exists());
        assert_eq!(
            rows(&old.collect::<types::Result<Vec<_>>>().unwrap()),
            before
        );
        assert!(!root.path().join("all_1_1_0").exists());
        let merged = table.parts().unwrap();
        table.optimize().unwrap();
        assert_eq!(table.parts().unwrap(), merged);
        drop(table);
        let table = MergeTreeTable::open(&def, root.path()).unwrap();
        assert_eq!(rows(&read(&table, &request)), before);
        table.insert(block(&[0], &["new"])).unwrap();
        assert!(table.parts().unwrap().contains(&"all_10_10_0".into()));
    }
}

#[test]
fn optimize_errors_leave_original_parts_active_and_allow_retry() {
    let root = tempfile::tempdir().unwrap();
    let table = MergeTreeTable::open(&definition(Codec::None), root.path()).unwrap();
    table.insert(block(&[2, 1], &["b", "a"])).unwrap();
    table.insert(block(&[3], &["c"])).unwrap();
    let original = table.parts().unwrap();
    let path = root.path().join("all_2_2_0/id.bin");
    let bytes = std::fs::read(&path).unwrap();
    std::fs::write(&path, [255]).unwrap();
    assert!(table.optimize().is_err());
    assert_eq!(table.parts().unwrap(), original);
    assert!(!root.path().join("all_1_2_1").exists());
    assert!(!root.path().join("tmp_all_1_2_1").exists());
    std::fs::write(&path, bytes).unwrap();
    std::fs::create_dir(root.path().join("tmp_table.json")).unwrap();
    assert!(table.optimize().is_err());
    assert_eq!(table.parts().unwrap(), original);
    assert!(!root.path().join("all_1_2_1").exists());
    std::fs::remove_dir(root.path().join("tmp_table.json")).unwrap();
    table.optimize().unwrap();
    assert_eq!(table.parts().unwrap(), ["all_1_2_1"]);
    let request = ScanRequest {
        columns: vec!["id".into(), "text".into()],
        predicate: None,
    };
    assert_eq!(
        rows(&read(&table, &request)),
        [(1, "a".into()), (2, "b".into()), (3, "c".into())]
    );
}
