mod common;

use common::{block, definition};
use storage::MergeTreeTable;
use types::{testing::MemTable, Block, Codec, ColumnData, DbError, ScanRequest, Table};

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
