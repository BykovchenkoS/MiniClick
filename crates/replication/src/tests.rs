use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use types::testing::LocalPeer;
use types::{
    Block, BlockStream, Catalog, Codec, ColumnDef, DataType, DbError, Engine, NodeIdentity,
    PartData, Peer, Result, ScanRequest, ScanStats, Table, TableDefinition, TableEntry, TableId,
    TableSchema, TableSettings,
};

use super::{sync_once, SyncReport};

/// Таблица с управляемым набором parts: проверяет сверку без Storage.
struct FakeTable {
    definition: TableDefinition,
    parts: RwLock<BTreeMap<String, PartData>>,
    fail_export: Option<String>,
}

impl FakeTable {
    fn new(
        definition: TableDefinition,
        parts: &[(&str, &str)],
        fail_export: Option<&str>,
    ) -> FakeTable {
        let parts: BTreeMap<String, PartData> = parts
            .iter()
            .map(|(name, payload)| {
                (
                    (*name).to_owned(),
                    PartData {
                        name: (*name).to_owned(),
                        files: vec![(format!("{name}.bin"), payload.as_bytes().to_vec())],
                        checksum: String::new(),
                    },
                )
            })
            .collect();
        FakeTable {
            definition,
            parts: RwLock::new(parts),
            fail_export: fail_export.map(str::to_owned),
        }
    }
}

impl Table for FakeTable {
    fn definition(&self) -> &TableDefinition {
        &self.definition
    }

    fn insert(&self, _block: Block) -> Result<()> {
        Err(DbError::Unsupported(
            "fake table does not accept inserts".into(),
        ))
    }

    fn scan(&self, _request: &ScanRequest) -> Result<BlockStream> {
        Ok(Box::new(std::iter::empty::<Result<Block>>()))
    }

    fn explain_scan(&self, _request: &ScanRequest) -> Result<ScanStats> {
        Ok(ScanStats::default())
    }

    fn flush(&self) -> Result<()> {
        Ok(())
    }

    fn optimize(&self) -> Result<()> {
        Ok(())
    }

    fn drop_data(&self) -> Result<()> {
        self.parts.write().unwrap().clear();
        Ok(())
    }

    fn parts(&self) -> Result<Vec<String>> {
        Ok(self.parts.read().unwrap().keys().cloned().collect())
    }

    fn export_part(&self, name: &str) -> Result<PartData> {
        if self.fail_export.as_deref() == Some(name) {
            return Err(DbError::Internal(format!("fake export failure: {name}")));
        }
        self.parts
            .read()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| DbError::UnknownTable(name.into()))
    }

    fn sync_parts(&self, add: Vec<PartData>, remove: &[String]) -> Result<()> {
        let mut parts = self.parts.write().unwrap();
        let mut next = parts.clone();
        for name in remove {
            next.remove(name);
        }
        for part in add {
            if next.contains_key(&part.name) {
                return Err(DbError::Internal(format!(
                    "part already exists: {}",
                    part.name
                )));
            }
            next.insert(part.name.clone(), part);
        }
        *parts = next;
        Ok(())
    }
}

type TestTableMap = BTreeMap<String, (TableId, Arc<dyn Table>)>;

/// Каталог для тестов: создаёт FakeTable и хранит их в памяти.
#[derive(Default)]
struct TestCatalog {
    tables: RwLock<TestTableMap>,
    next_id: AtomicU64,
}

impl TestCatalog {
    fn new() -> TestCatalog {
        TestCatalog::default()
    }

    fn next_id(&self) -> TableId {
        TableId::new(format!(
            "test-{}",
            self.next_id.fetch_add(1, Ordering::Relaxed)
        ))
    }

    /// Кладёт таблицу с новым id; повторная установка имитирует DROP + CREATE.
    fn install(&self, table: FakeTable) {
        let id = self.next_id();
        self.install_with_id(id, table);
    }

    fn install_with_id(&self, id: TableId, table: FakeTable) {
        let name = table.definition.name.clone();
        self.tables
            .write()
            .unwrap()
            .insert(name, (id, Arc::new(table)));
    }
}

impl Catalog for TestCatalog {
    fn create_table(&self, definition: TableDefinition, if_not_exists: bool) -> Result<()> {
        self.create_table_with_id(definition, self.next_id(), if_not_exists)
    }

    fn create_table_with_id(
        &self,
        definition: TableDefinition,
        id: TableId,
        if_not_exists: bool,
    ) -> Result<()> {
        let mut tables = self.tables.write().unwrap();
        if tables.contains_key(&definition.name) {
            return if if_not_exists {
                Ok(())
            } else {
                Err(DbError::TableExists(definition.name))
            };
        }
        if tables.values().any(|(existing, _)| existing == &id) {
            return Err(DbError::Internal("duplicate table id".into()));
        }
        let table = Arc::new(FakeTable::new(definition.clone(), &[], None));
        tables.insert(definition.name, (id, table));
        Ok(())
    }

    fn drop_table(&self, name: &str, if_exists: bool) -> Result<()> {
        match self.tables.write().unwrap().remove(name) {
            Some((_, table)) => table.drop_data(),
            None if if_exists => Ok(()),
            None => Err(DbError::UnknownTable(name.into())),
        }
    }

    fn get_table(&self, name: &str) -> Result<Arc<dyn Table>> {
        self.tables
            .read()
            .unwrap()
            .get(name)
            .map(|(_, table)| table.clone())
            .ok_or_else(|| DbError::UnknownTable(name.into()))
    }

    fn list_tables(&self) -> Vec<TableDefinition> {
        self.tables
            .read()
            .unwrap()
            .values()
            .map(|(_, table)| table.definition().clone())
            .collect()
    }

    fn table_entries(&self) -> Vec<TableEntry> {
        self.tables
            .read()
            .unwrap()
            .values()
            .map(|(id, table)| TableEntry {
                id: id.clone(),
                definition: table.definition().clone(),
            })
            .collect()
    }

    fn get_table_by_id(&self, id: &TableId) -> Result<Arc<dyn Table>> {
        self.tables
            .read()
            .unwrap()
            .values()
            .find(|(entry_id, _)| entry_id == id)
            .map(|(_, table)| table.clone())
            .ok_or_else(|| DbError::UnknownTable(id.to_string()))
    }
}

/// Лидер, который не отвечает: проверяет сохранность реплики при сбое.
struct UnreachablePeer;

impl Peer for UnreachablePeer {
    fn ping(&self) -> Result<NodeIdentity> {
        Err(DbError::Internal("unreachable".into()))
    }

    fn list_tables(&self) -> Result<Vec<TableEntry>> {
        Err(DbError::Internal("unreachable".into()))
    }

    fn list_parts(&self, _table: &TableId) -> Result<Vec<String>> {
        Err(DbError::Internal("unreachable".into()))
    }

    fn fetch_part(&self, _table: &TableId, _part: &str) -> Result<PartData> {
        Err(DbError::Internal("unreachable".into()))
    }
}

/// Лидер, который возвращает part с чужим именем.
struct WrongPartPeer {
    inner: LocalPeer,
}

impl Peer for WrongPartPeer {
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
        let mut data = self.inner.fetch_part(table, part)?;
        data.name = "all_3_3_0".to_owned();
        Ok(data)
    }
}

/// Лидер, у которого OPTIMIZE меняет parts между list_parts и fetch_part.
struct OptimizingPeer {
    catalog: Arc<TestCatalog>,
    id: TableId,
    pending: AtomicBool,
}

impl Peer for OptimizingPeer {
    fn ping(&self) -> Result<NodeIdentity> {
        Ok(leader_identity())
    }

    fn list_tables(&self) -> Result<Vec<TableEntry>> {
        Ok(self.catalog.table_entries())
    }

    fn list_parts(&self, table: &TableId) -> Result<Vec<String>> {
        self.catalog.get_table_by_id(table)?.parts()
    }

    fn fetch_part(&self, table: &TableId, part: &str) -> Result<PartData> {
        if self.pending.swap(false, Ordering::SeqCst) {
            // OPTIMIZE уже заменил активный набор на merged part.
            self.catalog.install_with_id(
                self.id.clone(),
                mergetree("hits", &[("all_1_2_1", "merged")], None),
            );
            return Err(DbError::Internal("part is not active".into()));
        }
        self.catalog.get_table_by_id(table)?.export_part(part)
    }
}

fn definition(name: &str, engine: Engine) -> TableDefinition {
    TableDefinition {
        name: name.to_owned(),
        schema: TableSchema {
            columns: vec![ColumnDef {
                name: "id".to_owned(),
                data_type: DataType::UInt64,
            }],
        },
        engine,
        order_by: if engine == Engine::MergeTree {
            vec!["id".to_owned()]
        } else {
            Vec::new()
        },
        settings: TableSettings {
            index_granularity: 2,
            codec: Codec::Lz4,
        },
    }
}

fn identity(cluster: &str, node: &str) -> NodeIdentity {
    NodeIdentity {
        cluster_id: cluster.to_owned(),
        node_id: node.to_owned(),
    }
}

fn leader_identity() -> NodeIdentity {
    identity("test-cluster", "leader-1")
}

fn leader_peer(leader: TestCatalog) -> LocalPeer {
    LocalPeer {
        catalog: Arc::new(leader),
        identity: leader_identity(),
    }
}

fn mergetree(name: &str, parts: &[(&str, &str)], fail_export: Option<&str>) -> FakeTable {
    FakeTable::new(definition(name, Engine::MergeTree), parts, fail_export)
}

#[test]
fn creates_missing_table_and_fetches_all_parts() {
    let leader = TestCatalog::new();
    leader.install(mergetree(
        "hits",
        &[("all_1_1_0", "v1"), ("all_2_2_0", "v2")],
        None,
    ));
    let leader = leader_peer(leader);
    let local = TestCatalog::new();

    let report = sync_once(&local, &leader, &leader_identity()).unwrap();

    assert_eq!(report.tables_created, 1);
    assert_eq!(report.tables_dropped, 0);
    assert_eq!(report.parts_fetched, 2);
    assert_eq!(report.parts_removed, 0);
    assert_eq!(report.failures, 0);
    let table = local.get_table("hits").unwrap();
    assert_eq!(
        table.parts().unwrap(),
        vec!["all_1_1_0".to_owned(), "all_2_2_0".to_owned()]
    );
    assert_eq!(
        table.export_part("all_2_2_0").unwrap(),
        PartData {
            name: "all_2_2_0".into(),
            files: vec![("all_2_2_0.bin".into(), b"v2".to_vec())],
            checksum: String::new(),
        }
    );
}

#[test]
fn drops_local_tables_absent_on_leader() {
    let leader = leader_peer(TestCatalog::new());
    let local = TestCatalog::new();
    local.install(mergetree("extra", &[("all_1_1_0", "x")], None));

    let report = sync_once(&local, &leader, &leader_identity()).unwrap();

    assert_eq!(report.tables_dropped, 1);
    assert_eq!(report.tables_created, 0);
    assert_eq!(report.failures, 0);
    assert!(local.list_tables().is_empty());
}

#[test]
fn keeps_common_parts_and_replaces_stale_ones() {
    let id = TableId::new("shared-lifecycle");
    let leader = TestCatalog::new();
    leader.install_with_id(
        id.clone(),
        mergetree(
            "hits",
            &[("all_2_2_0", "new"), ("all_3_3_0", "shared")],
            None,
        ),
    );
    let leader = leader_peer(leader);
    let local = TestCatalog::new();
    local.install_with_id(
        id,
        mergetree(
            "hits",
            &[("all_1_1_0", "old"), ("all_3_3_0", "shared")],
            None,
        ),
    );

    let report = sync_once(&local, &leader, &leader_identity()).unwrap();

    assert_eq!(report.tables_created, 0);
    assert_eq!(report.tables_dropped, 0);
    assert_eq!(report.parts_fetched, 1);
    assert_eq!(report.parts_removed, 1);
    assert_eq!(report.failures, 0);
    let table = local.get_table("hits").unwrap();
    assert_eq!(
        table.parts().unwrap(),
        vec!["all_2_2_0".to_owned(), "all_3_3_0".to_owned()]
    );
    assert!(table.export_part("all_1_1_0").is_err());
}

#[test]
fn repeated_sync_makes_no_changes() {
    let leader = TestCatalog::new();
    leader.install(mergetree("hits", &[("all_1_1_0", "v1")], None));
    let leader = leader_peer(leader);
    let local = TestCatalog::new();
    sync_once(&local, &leader, &leader_identity()).unwrap();

    let report = sync_once(&local, &leader, &leader_identity()).unwrap();

    assert_eq!(report, SyncReport::default());
}

#[test]
fn failure_in_one_table_does_not_stop_others() {
    let leader = TestCatalog::new();
    leader.install(mergetree("good", &[("all_1_1_0", "g")], None));
    leader.install(mergetree("bad", &[("all_1_1_0", "b")], Some("all_1_1_0")));
    let leader = leader_peer(leader);
    let local = TestCatalog::new();

    let report = sync_once(&local, &leader, &leader_identity()).unwrap();

    assert_eq!(report.tables_created, 2);
    assert_eq!(report.parts_fetched, 1);
    assert_eq!(report.failures, 1);
    assert_eq!(
        local.get_table("good").unwrap().parts().unwrap(),
        vec!["all_1_1_0".to_owned()]
    );
    assert!(local.get_table("bad").unwrap().parts().unwrap().is_empty());
}

#[test]
fn failed_fetch_does_not_publish_partial_parts() {
    let leader = TestCatalog::new();
    leader.install(mergetree(
        "hits",
        &[("all_1_1_0", "ok"), ("all_2_2_0", "bad")],
        Some("all_2_2_0"),
    ));
    let leader = leader_peer(leader);
    let local = TestCatalog::new();

    let report = sync_once(&local, &leader, &leader_identity()).unwrap();

    assert_eq!(report.tables_created, 1);
    assert_eq!(report.parts_fetched, 0);
    assert_eq!(report.parts_removed, 0);
    assert_eq!(report.failures, 1);
    assert!(local.get_table("hits").unwrap().parts().unwrap().is_empty());
}

#[test]
fn unreachable_leader_leaves_replica_unchanged() {
    let local = TestCatalog::new();
    local.install(mergetree("extra", &[("all_1_1_0", "keep")], None));

    let error = sync_once(&local, &UnreachablePeer, &leader_identity()).unwrap_err();

    assert!(matches!(error, DbError::Internal(_)));
    assert_eq!(local.list_tables().len(), 1);
    assert_eq!(
        local.get_table("extra").unwrap().parts().unwrap(),
        vec!["all_1_1_0".to_owned()]
    );
}

#[test]
fn memory_definitions_are_replicated_without_fetching_parts() {
    let leader = TestCatalog::new();
    leader.install(FakeTable::new(
        definition("memo", Engine::Memory),
        &[],
        None,
    ));
    let leader = leader_peer(leader);
    let local = TestCatalog::new();

    let report = sync_once(&local, &leader, &leader_identity()).unwrap();

    assert_eq!(report.tables_created, 1);
    assert_eq!(report.failures, 0);
    assert_eq!(report.parts_fetched, 0);
    let table = local.get_table("memo").unwrap();
    assert_eq!(table.definition().engine, Engine::Memory);
    assert!(table.parts().unwrap().is_empty());
}

#[test]
fn recreates_table_when_id_changes_without_keeping_old_parts() {
    let old_id = TableId::new("life-old");
    let leader = Arc::new(TestCatalog::new());
    leader.install_with_id(
        old_id.clone(),
        mergetree("hits", &[("all_1_1_0", "old")], None),
    );
    let peer = LocalPeer {
        catalog: leader.clone(),
        identity: leader_identity(),
    };
    let local = TestCatalog::new();
    local.install_with_id(old_id, mergetree("hits", &[("all_1_1_0", "old")], None));

    assert_eq!(
        sync_once(&local, &peer, &leader_identity()).unwrap(),
        SyncReport::default()
    );

    let new_id = TableId::new("life-new");
    leader.install_with_id(
        new_id.clone(),
        mergetree("hits", &[("all_1_1_0", "new")], None),
    );
    let report = sync_once(&local, &peer, &leader_identity()).unwrap();

    assert_eq!(report.tables_created, 1);
    assert_eq!(report.tables_dropped, 0);
    assert_eq!(report.failures, 0);
    let entries = local.table_entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].id, new_id);
    let table = local.get_table("hits").unwrap();
    assert_eq!(table.parts().unwrap(), vec!["all_1_1_0".to_owned()]);
    assert_eq!(
        table.export_part("all_1_1_0").unwrap().files[0].1,
        b"new".to_vec()
    );
}

#[test]
fn same_id_with_different_schema_is_reported_and_not_touched() {
    let id = TableId::new("same-life");
    let leader = TestCatalog::new();
    leader.install_with_id(
        id.clone(),
        mergetree("hits", &[("all_1_1_0", "leader")], None),
    );
    let leader = leader_peer(leader);
    let mut local_definition = definition("hits", Engine::MergeTree);
    local_definition.settings.index_granularity = 64;
    let local = TestCatalog::new();
    local.install_with_id(
        id,
        FakeTable::new(local_definition, &[("all_1_1_0", "local")], None),
    );

    let report = sync_once(&local, &leader, &leader_identity()).unwrap();

    assert_eq!(report.failures, 1);
    assert_eq!(report.parts_fetched, 0);
    let table = local.get_table("hits").unwrap();
    assert_eq!(table.parts().unwrap(), vec!["all_1_1_0".to_owned()]);
    assert_eq!(
        table.export_part("all_1_1_0").unwrap().files[0].1,
        b"local".to_vec()
    );
}

#[test]
fn mismatched_cluster_or_leader_id_does_not_change_local_data() {
    for wrong_identity in [
        identity("other-cluster", "leader-1"),
        identity("test-cluster", "other-node"),
    ] {
        let local = TestCatalog::new();
        local.install(mergetree("hits", &[("all_1_1_0", "keep")], None));
        let wrong = LocalPeer {
            catalog: Arc::new(TestCatalog::new()),
            identity: wrong_identity,
        };

        let error = sync_once(&local, &wrong, &leader_identity()).unwrap_err();

        assert!(matches!(error, DbError::WrongLeader { .. }));
        assert_eq!(local.list_tables().len(), 1);
        let table = local.get_table("hits").unwrap();
        assert_eq!(table.parts().unwrap(), vec!["all_1_1_0".to_owned()]);
        assert_eq!(
            table.export_part("all_1_1_0").unwrap().files[0].1,
            b"keep".to_vec()
        );
    }
}

#[test]
fn optimize_between_list_and_fetch_recovers_on_next_cycle() {
    let id = TableId::new("optimize-life");
    let leader = Arc::new(TestCatalog::new());
    leader.install_with_id(
        id.clone(),
        mergetree(
            "hits",
            &[("all_1_1_0", "old"), ("all_2_2_0", "second")],
            None,
        ),
    );
    let peer = OptimizingPeer {
        catalog: leader,
        id: id.clone(),
        pending: AtomicBool::new(true),
    };
    let local = TestCatalog::new();
    local.install_with_id(id, mergetree("hits", &[("all_1_1_0", "old")], None));

    let report = sync_once(&local, &peer, &leader_identity()).unwrap();

    assert_eq!(report.failures, 1);
    assert_eq!(report.parts_fetched, 0);
    assert_eq!(report.parts_removed, 0);
    assert_eq!(
        local.get_table("hits").unwrap().parts().unwrap(),
        vec!["all_1_1_0".to_owned()]
    );

    let report = sync_once(&local, &peer, &leader_identity()).unwrap();

    assert_eq!(report.failures, 0);
    assert_eq!(report.parts_fetched, 1);
    assert_eq!(report.parts_removed, 1);
    assert_eq!(
        local.get_table("hits").unwrap().parts().unwrap(),
        vec!["all_1_2_1".to_owned()]
    );
    assert_eq!(
        local
            .get_table("hits")
            .unwrap()
            .export_part("all_1_2_1")
            .unwrap()
            .files[0]
            .1,
        b"merged".to_vec()
    );
}

#[test]
fn mismatched_fetched_part_name_aborts_without_changes() {
    let id = TableId::new("same-life");
    let leader = TestCatalog::new();
    leader.install_with_id(id.clone(), mergetree("hits", &[("all_2_2_0", "new")], None));
    let peer = WrongPartPeer {
        inner: leader_peer(leader),
    };
    let local = TestCatalog::new();
    local.install_with_id(id, mergetree("hits", &[("all_1_1_0", "old")], None));

    // Запрошен all_2_2_0, но лидер возвращает PartData с именем all_3_3_0.
    let report = sync_once(&local, &peer, &leader_identity()).unwrap();

    assert_eq!(report.failures, 1);
    assert_eq!(report.parts_fetched, 0);
    assert_eq!(report.parts_removed, 0);
    let table = local.get_table("hits").unwrap();
    assert_eq!(table.parts().unwrap(), vec!["all_1_1_0".to_owned()]);
    assert_eq!(
        table.export_part("all_1_1_0").unwrap().files[0].1,
        b"old".to_vec()
    );
}
