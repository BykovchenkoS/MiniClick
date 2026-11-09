//! Регрессионные проверки репликации: смена движка и отмена сетевых ожиданий.

use server::{config::Config, AppState};
use std::io::{Read, Write};
use std::sync::mpsc;
use std::time::Duration;
use types::{testing::LocalPeer, Engine, NodeIdentity};

fn open(root: &std::path::Path) -> AppState {
    AppState::open(Config {
        data_dir: root.to_owned(),
        ..Config::default()
    })
    .unwrap()
}

fn identity() -> NodeIdentity {
    NodeIdentity {
        cluster_id: "miniclick".into(),
        node_id: "leader".into(),
    }
}

#[test]
fn existing_memory_table_does_not_permanently_block_remote_mergetree() {
    let leader_root = tempfile::tempdir().unwrap();
    let replica_root = tempfile::tempdir().unwrap();
    let leader = open(leader_root.path());
    leader
        .query
        .execute("CREATE TABLE hits (id UInt64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    let local = open(replica_root.path());
    local
        .query
        .execute("CREATE TABLE hits (id UInt64) ENGINE = Memory")
        .unwrap();
    // Имитируем перезапуск существующего каталога в роли Replica.
    drop(local);
    let local = open(replica_root.path());
    let peer = LocalPeer {
        catalog: leader.catalog,
        identity: identity(),
    };
    let mut reports = Vec::new();
    for _ in 0..3 {
        reports.push(replication::sync_once(local.catalog.as_ref(), &peer, &identity()).unwrap());
    }
    assert_eq!(
        local.catalog.get_table("hits").unwrap().definition().engine,
        Engine::MergeTree,
        "После трёх циклов таблица осталась Memory: {reports:?}"
    );
}

#[test]
fn stop_interrupts_sync_when_leader_accepts_but_does_not_respond() {
    for partial_body in [false, true] {
        assert_stop_interrupts_request(partial_body);
    }
}

fn assert_stop_interrupts_request(partial_body: bool) {
    let root = tempfile::tempdir().unwrap();
    let local = open(root.path());
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (accepted, got_connection) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let blackhole = std::thread::spawn(move || {
        let (mut connection, _) = listener.accept().unwrap();
        if partial_body {
            let mut request = [0; 1];
            connection.read_exact(&mut request).unwrap();
            connection.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n{\"cluster_id\":").unwrap();
        }
        accepted.send(()).unwrap();
        let _ = released.recv_timeout(Duration::from_secs(10));
        drop(connection);
    });
    let (stop, stopped) = mpsc::channel();
    let (done, finished) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        server::replica::sync_loop(
            local.catalog,
            address,
            identity(),
            Duration::from_millis(20),
            stopped,
        );
        done.send(()).unwrap();
    });
    got_connection.recv_timeout(Duration::from_secs(5)).unwrap();
    stop.send(()).unwrap();
    let quick_stop = finished.recv_timeout(Duration::from_secs(1));
    // Освобождаем запрос даже при провале проверки, чтобы не оставлять потоков.
    release.send(()).unwrap();
    worker.join().unwrap();
    blackhole.join().unwrap();
    assert!(
        quick_stop.is_ok(),
        "Остановка не прервала зависший HTTP-запрос, partial_body={partial_body}"
    );
}

#[test]
fn cancellation_between_parts_keeps_previous_committed_rows() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use types::{
        Block, Column, ColumnData, PartData, Peer, Result, ScanRequest, TableEntry, TableId,
    };

    struct CancelOnFetch {
        inner: LocalPeer,
        cancelled: AtomicBool,
    }
    impl Peer for CancelOnFetch {
        fn ping(&self) -> Result<NodeIdentity> {
            self.inner.ping()
        }
        fn list_tables(&self) -> Result<Vec<TableEntry>> {
            self.inner.list_tables()
        }
        fn list_parts(&self, table: &TableId) -> Result<Vec<String>> {
            self.inner.list_parts(table)
        }
        fn fetch_part(&self, table: &TableId, part: &str) -> Result<PartData> {
            let data = self.inner.fetch_part(table, part)?;
            self.cancelled.store(true, Ordering::Release);
            Ok(data)
        }
    }

    let leader_root = tempfile::tempdir().unwrap();
    let local_root = tempfile::tempdir().unwrap();
    let leader = open(leader_root.path());
    leader
        .query
        .execute("CREATE TABLE hits (id UInt64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    let local = open(local_root.path());
    let peer = LocalPeer {
        catalog: leader.catalog.clone(),
        identity: identity(),
    };
    let table = leader.catalog.get_table("hits").unwrap();
    let insert = |id| {
        table
            .insert(
                Block::new(vec![Column {
                    name: "id".into(),
                    data: ColumnData::UInt64(vec![id]),
                }])
                .unwrap(),
            )
            .unwrap()
    };
    insert(1);
    replication::sync_once(local.catalog.as_ref(), &peer, &identity()).unwrap();
    insert(2);
    insert(3);
    let peer = CancelOnFetch {
        inner: peer,
        cancelled: AtomicBool::new(false),
    };
    let error =
        replication::sync_once_with_cancel(local.catalog.as_ref(), &peer, &identity(), &|| {
            peer.cancelled.load(Ordering::Acquire)
        })
        .unwrap_err();
    assert!(
        matches!(error, types::DbError::Io(ref error) if error.kind() == std::io::ErrorKind::Interrupted)
    );
    let table = local.catalog.get_table("hits").unwrap();
    assert_eq!(table.parts().unwrap(), ["all_1_1_0"]);
    let blocks = table
        .scan(&ScanRequest {
            columns: vec!["id".into()],
            predicate: None,
        })
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        Block::concat(&blocks).unwrap().column("id"),
        Some(&ColumnData::UInt64(vec![1]))
    );
    let entry = local.catalog.table_entries().remove(0);
    let path = local_root.path().join("tables").join(entry.id.as_str());
    assert!(!path.join("all_2_2_0").exists());
}

#[test]
fn mergetree_to_memory_replicates_an_empty_definition_and_later_drop() {
    use types::{Block, Column, ColumnData, DbError, ScanRequest};

    let leader_root = tempfile::tempdir().unwrap();
    let local_root = tempfile::tempdir().unwrap();
    let leader = open(leader_root.path());
    let local = open(local_root.path());
    let peer = LocalPeer {
        catalog: leader.catalog.clone(),
        identity: identity(),
    };
    let insert = |value| {
        leader
            .catalog
            .get_table("hits")
            .unwrap()
            .insert(
                Block::new(vec![Column {
                    name: "id".into(),
                    data: ColumnData::UInt64(vec![value]),
                }])
                .unwrap(),
            )
            .unwrap()
    };
    leader
        .query
        .execute("CREATE TABLE hits (id UInt64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    insert(42);
    replication::sync_once(local.catalog.as_ref(), &peer, &identity()).unwrap();
    let old_id = local.catalog.table_entries().remove(0).id;
    leader.query.execute("DROP TABLE hits").unwrap();
    leader
        .query
        .execute("CREATE TABLE hits (id UInt64) ENGINE = Memory")
        .unwrap();
    insert(99);
    let report = replication::sync_once(local.catalog.as_ref(), &peer, &identity()).unwrap();
    assert_eq!(report.failures, 0);
    assert_eq!(report.parts_fetched, 0);
    let entry = local.catalog.table_entries().remove(0);
    assert_ne!(entry.id, old_id);
    assert_eq!(entry.id, leader.catalog.table_entries().remove(0).id);
    let table = local.catalog.get_table("hits").unwrap();
    assert_eq!(table.definition().engine, Engine::Memory);
    assert_eq!(
        table
            .explain_scan(&ScanRequest::default())
            .unwrap()
            .rows_total,
        0
    );
    assert_eq!(
        leader
            .catalog
            .get_table("hits")
            .unwrap()
            .explain_scan(&ScanRequest::default())
            .unwrap()
            .rows_total,
        1
    );
    assert_eq!(
        replication::sync_once(local.catalog.as_ref(), &peer, &identity()).unwrap(),
        replication::SyncReport::default()
    );
    leader.query.execute("DROP TABLE hits").unwrap();
    replication::sync_once(local.catalog.as_ref(), &peer, &identity()).unwrap();
    assert!(matches!(
        local.catalog.get_table("hits"),
        Err(DbError::UnknownTable(_))
    ));
}
