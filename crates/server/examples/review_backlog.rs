//! Воспроизведение расхода памяти при начальной синхронизации большой таблицы.

use server::{config::Config, AppState};
use types::{testing::LocalPeer, Block, Column, ColumnData, NodeIdentity, ScanRequest};

fn main() {
    let root = tempfile::tempdir().unwrap();
    let leader = AppState::open(Config {
        data_dir: root.path().join("leader"),
        ..Config::default()
    })
    .unwrap();
    leader.query.execute("CREATE TABLE backlog (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS codec = 'None'").unwrap();
    let table = leader.catalog.get_table("backlog").unwrap();
    for _ in 0..80 {
        table
            .insert(
                Block::new(vec![Column {
                    name: "id".into(),
                    data: ColumnData::UInt64(vec![1; 262_144]),
                }])
                .unwrap(),
            )
            .unwrap();
    }
    println!("Leader prepared: 80 parts, about 160 MiB; each part about 2 MiB");
    let local = AppState::open(Config {
        data_dir: root.path().join("replica"),
        ..Config::default()
    })
    .unwrap();
    let expected = NodeIdentity {
        cluster_id: "miniclick".into(),
        node_id: "leader".into(),
    };
    let peer = LocalPeer {
        catalog: leader.catalog,
        identity: expected.clone(),
    };
    let report = replication::sync_once(local.catalog.as_ref(), &peer, &expected).unwrap();
    println!("Replica completed: {report:?}");
    assert_eq!(report.parts_fetched, 80);
    assert_eq!(report.failures, 0);
    let table = local.catalog.get_table("backlog").unwrap();
    let mut rows = 0u64;
    let mut sum = 0u64;
    for block in table
        .scan(&ScanRequest {
            columns: vec!["id".into()],
            predicate: None,
        })
        .unwrap()
    {
        let block = block.unwrap();
        let Some(ColumnData::UInt64(values)) = block.column("id") else {
            panic!("Ожидалась колонка UInt64");
        };
        rows += values.len() as u64;
        sum += values.iter().sum::<u64>();
    }
    assert_eq!(rows, 20_971_520);
    assert_eq!(sum, 20_971_520);
    println!("Verified rows={rows}, sum={sum}");
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        if let Some(peak) = status.lines().find(|line| line.starts_with("VmHWM:")) {
            println!("{peak}");
        }
    }
}
