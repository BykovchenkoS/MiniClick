//! Каталог восстанавливается из контрольного снимка и хвоста команд.

use super::checkpoint::{self, TableCheckpoint};
use super::log_store::{persist, read, storage_error};
use super::types::{Command, CommandResult, Entry, TypeConfig};
use crate::{
    insert::{insert_block, insert_block_with_id},
    AppState,
};
use openraft::storage::RaftStateMachine;
use openraft::{
    EntryPayload, LogId, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError,
    StoredMembership,
};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{self, Cursor};
use std::path::{Path, PathBuf};
use std::sync::{atomic::Ordering, Arc, Mutex};
use types::DbError;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Applied {
    last_applied: Option<LogId<u64>>,
    membership: StoredMembership<u64, openraft::BasicNode>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    commands: Vec<Command>,
    #[serde(default)]
    command_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tables: Option<Vec<TableCheckpoint>>,
}

#[derive(Clone)]
pub struct StateMachine {
    path: Arc<PathBuf>,
    tail: Arc<PathBuf>,
    state: Arc<Mutex<Applied>>,
    app: AppState,
}

fn execute(app: &AppState, command: &Command, ordinal: usize) -> types::Result<CommandResult> {
    let result = match command {
        Command::Sql(sql) if !app.config.local_shards.is_empty() => {
            // Порядок команд входит в снимок и даёт один ID на всех участниках.
            let id = uuid::Uuid::from_u128(ordinal as u128).to_string();
            query::SqlQueryEngine::new(app.catalog.clone())
                .execute_with_table_id(sql, Some(types::TableId::new(id)))
                .map(|_| ())
        }
        Command::Sql(sql) => app.query.execute(sql).map(|_| ()),
        Command::Insert {
            table,
            format,
            body,
        } => (|| {
            let table = app.catalog.get_table(table)?;
            insert_block(app, table.as_ref(), *format, body)
        })(),
        Command::ShardInsert {
            table,
            format,
            body,
            request_id,
        } => (|| {
            let table = app.catalog.get_table_by_id(table)?;
            if !app
                .config
                .local_shards
                .contains_key(&table.definition().name)
            {
                return Err(DbError::Unsupported(
                    "Raft table has no shard placement".into(),
                ));
            }
            let request_id = request_id
                .as_deref()
                .map(uuid::Uuid::parse_str)
                .transpose()
                .map_err(|error| DbError::Syntax(format!("invalid insert request ID: {error}")))?;
            insert_block_with_id(app, table.as_ref(), *format, body, request_id)
        })(),
    };
    match result {
        Ok(()) => Ok(CommandResult {
            error: None,
            status: 200,
        }),
        Err(error @ (DbError::Io(_) | DbError::Internal(_))) => Err(error),
        Err(error) => Ok(CommandResult {
            status: if matches!(error, DbError::UnknownTable(_)) {
                404
            } else {
                400
            },
            error: Some(error.to_string()),
        }),
    }
}

fn rebuild(app: &AppState, state: &Applied) -> types::Result<()> {
    for table in app.catalog.list_tables() {
        app.catalog.drop_table(&table.name, false)?;
    }
    if let Some(tables) = &state.tables {
        checkpoint::restore(app, tables)?;
    }
    for (index, command) in state.commands.iter().enumerate() {
        execute(app, command, index + 1)?;
    }
    Ok(())
}

fn entry_files(tail: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut entries = Vec::new();
    for file in fs::read_dir(tail)? {
        let path = file?.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let index = path
            .file_stem()
            .and_then(|name| name.to_str())
            .and_then(|name| name.parse::<u64>().ok())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid raft tail entry name")
            })?;
        entries.push((index, path));
    }
    entries.sort_unstable_by_key(|(index, _)| *index);
    Ok(entries)
}

fn apply_entry(app: &AppState, state: &mut Applied, entry: Entry) -> types::Result<CommandResult> {
    if let Some(last) = state.last_applied {
        if last.index.checked_add(1) != Some(entry.log_id.index) {
            return Err(DbError::Internal("nonconsecutive applied log index".into()));
        }
    }
    let response = match entry.payload {
        EntryPayload::Normal(command) => {
            state.command_count = state
                .command_count
                .checked_add(1)
                .ok_or_else(|| DbError::Internal("raft command ordinal overflow".into()))?;
            execute(app, &command, state.command_count)?
        }
        EntryPayload::Membership(membership) => {
            state.membership = StoredMembership::new(Some(entry.log_id), membership);
            CommandResult {
                error: None,
                status: 200,
            }
        }
        EntryPayload::Blank => CommandResult {
            error: None,
            status: 200,
        },
    };
    state.last_applied = Some(entry.log_id);
    Ok(response)
}

fn compact_tail(tail: &Path, last: Option<LogId<u64>>) -> io::Result<()> {
    if let Some(last) = last {
        for (_, path) in entry_files(tail)?
            .into_iter()
            .take_while(|(i, _)| *i <= last.index)
        {
            fs::remove_file(path)?;
        }
    }
    Ok(())
}

impl StateMachine {
    pub(super) fn validate_file(path: &std::path::Path) -> types::Result<()> {
        let _: Applied = read(path)?;
        Ok(())
    }

    /// Вызывать до приёма HTTP-запросов и только после создания пустого view.
    pub fn open(path: PathBuf, app: AppState) -> types::Result<Self> {
        let mut state: Applied = read(&path)?;
        let tail = path.with_extension("entries");
        fs::create_dir_all(&tail)?;
        if let Some(parent) = tail.parent() {
            fs::File::open(parent)?.sync_all()?;
        }
        rebuild(&app, &state)?;
        if !state.commands.is_empty() {
            state.command_count = state.commands.len();
        }
        state.commands.clear();
        state.tables = None;
        let checkpoint_index = state.last_applied.map(|id| id.index);
        for (index, file) in entry_files(&tail)? {
            if checkpoint_index.is_some_and(|last| index <= last) {
                continue;
            }
            let entry: Entry =
                serde_json::from_reader(fs::File::open(file)?).map_err(io::Error::other)?;
            if entry.log_id.index != index {
                return Err(DbError::Internal("raft tail entry index mismatch".into()));
            }
            apply_entry(&app, &mut state, entry)?;
        }
        Ok(Self {
            path: Arc::new(path),
            tail: Arc::new(tail),
            state: Arc::new(Mutex::new(state)),
            app,
        })
    }

    fn snapshot(&self) -> io::Result<Snapshot<TypeConfig>> {
        let _view = self
            .app
            .view_gate
            .read()
            .map_err(|error| io::Error::other(error.to_string()))?;
        let state = self
            .state
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        let checkpoint = Applied {
            tables: Some(checkpoint::capture(&self.app).map_err(io::Error::other)?),
            ..state.clone()
        };
        persist(&self.path, &checkpoint)?;
        compact_tail(&self.tail, state.last_applied)?;
        Self::encode_snapshot(&checkpoint)
    }

    fn encode_snapshot(state: &Applied) -> io::Result<Snapshot<TypeConfig>> {
        let data = serde_json::to_vec(state).map_err(io::Error::other)?;
        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: state.last_applied,
                last_membership: state.membership.clone(),
                snapshot_id: format!("applied-{:?}", state.last_applied),
            },
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.snapshot())
            .await
            .map_err(storage_error)?
            .map_err(storage_error)
    }
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<u64>>,
            StoredMembership<u64, openraft::BasicNode>,
        ),
        StorageError<u64>,
    > {
        let state = self
            .state
            .lock()
            .map_err(|error| storage_error(io::Error::other(error.to_string())))?;
        Ok((state.last_applied, state.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<CommandResult>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        let this = self.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _view = this
                .app
                .view_gate
                .write()
                .map_err(|_| DbError::Internal("view lock poisoned".into()))?;
            let result = (|| {
                let mut state = this
                    .state
                    .lock()
                    .map_err(|_| DbError::Internal("state lock poisoned".into()))?;
                let mut results = Vec::new();
                for entry in entries {
                    if state
                        .last_applied
                        .is_some_and(|last| last.index.checked_add(1) != Some(entry.log_id.index))
                    {
                        return Err(DbError::Internal("nonconsecutive applied log index".into()));
                    }
                    // Каждый новый индекс закрепляется до изменения восстанавливаемого view.
                    let path = this.tail.join(format!("{}.json", entry.log_id.index));
                    persist(&path, &entry)?;
                    results.push(apply_entry(&this.app, &mut state, entry)?);
                }
                Ok::<_, DbError>(results)
            })();
            if result.is_err() {
                this.app.view_healthy.store(false, Ordering::Release);
            }
            result
        })
        .await
        .map_err(storage_error)?;
        result.map_err(|error| {
            self.app.view_healthy.store(false, Ordering::Release);
            storage_error(error)
        })
    }

    async fn get_snapshot_builder(&mut self) -> Self {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let next: Applied = serde_json::from_slice(snapshot.get_ref()).map_err(storage_error)?;
        if next.last_applied != meta.last_log_id || next.membership != meta.last_membership {
            return Err(storage_error(io::Error::other(
                "snapshot metadata mismatch",
            )));
        }
        let this = self.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _view = this
                .app
                .view_gate
                .write()
                .map_err(|_| DbError::Internal("view lock poisoned".into()))?;
            let result = (|| {
                let mut state = this
                    .state
                    .lock()
                    .map_err(|_| DbError::Internal("state lock poisoned".into()))?;
                if state
                    .last_applied
                    .is_some_and(|last| next.last_applied.is_none_or(|id| id.index < last.index))
                {
                    return Err(DbError::Internal("snapshot predates applied index".into()));
                }
                persist(&this.path, &next)?;
                rebuild(&this.app, &next)?;
                compact_tail(&this.tail, next.last_applied)?;
                *state = Applied {
                    command_count: next.command_count.max(next.commands.len()),
                    commands: Vec::new(),
                    tables: None,
                    ..next
                };
                Ok::<_, DbError>(())
            })();
            if result.is_err() {
                this.app.view_healthy.store(false, Ordering::Release);
            }
            result
        })
        .await
        .map_err(storage_error)?;
        result.map_err(|error| {
            self.app.view_healthy.store(false, Ordering::Release);
            storage_error(error)
        })
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let saved: Applied = read(&path)?;
            if saved.last_applied.is_none() {
                return Ok(None);
            }
            Self::encode_snapshot(&saved).map(Some)
        })
        .await
        .map_err(storage_error)?
        .map_err(storage_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::Config, formats::InputFormat};
    use openraft::CommittedLeaderId;
    use types::{Block, ColumnData, QueryOutput};

    fn app(root: &std::path::Path) -> AppState {
        AppState::open(Config {
            data_dir: root.join("view"),
            ..Config::default()
        })
        .unwrap()
    }

    fn entry(index: u64, command: Command) -> Entry {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(command),
        }
    }

    fn insert(body: &[u8]) -> Command {
        Command::Insert {
            table: "hits".into(),
            format: InputFormat::Csv,
            body: body.to_vec(),
        }
    }

    fn ids(app: &AppState) -> Vec<u64> {
        let QueryOutput::Rows { stream, .. } = app
            .query
            .execute("SELECT id FROM hits ORDER BY id")
            .unwrap()
        else {
            panic!("expected rows")
        };
        let blocks = stream.collect::<types::Result<Vec<_>>>().unwrap();
        let block = Block::concat(&blocks).unwrap();
        let ColumnData::UInt64(ids) = block.column("id").unwrap() else {
            panic!("expected UInt64")
        };
        ids.clone()
    }

    #[tokio::test]
    async fn replay_rebuilds_view_without_repeating_insert_or_preserving_unrecorded_rows() {
        let root = tempfile::tempdir().unwrap();
        let app = app(root.path());
        let path = root.path().join("state.json");
        let mut machine = StateMachine::open(path.clone(), app.clone()).unwrap();
        machine
            .apply([
                entry(
                    0,
                    Command::Sql(
                        "CREATE TABLE hits (id UInt64) ENGINE = MergeTree ORDER BY id".into(),
                    ),
                ),
                entry(1, insert(b"2\n1\n")),
            ])
            .await
            .unwrap();
        // Имитируем view после незавершённого применения, не вошедшего в историю.
        execute(&app, &insert(b"99\n"), 1).unwrap();
        assert_eq!(ids(&app), [1, 2, 99]);
        let reopened = StateMachine::open(path, app.clone()).unwrap();
        assert_eq!(ids(&app), [1, 2]);
        assert_eq!(
            reopened.state.lock().unwrap().last_applied.unwrap().index,
            1
        );
        // Имитируем обратное окно: история устойчива, но view ещё не обновлён.
        persist(&reopened.tail.join("2.json"), &entry(2, insert(b"3\n"))).unwrap();
        StateMachine::open(reopened.path.as_ref().clone(), app.clone()).unwrap();
        assert_eq!(ids(&app), [1, 2, 3]);
    }

    #[tokio::test]
    async fn snapshot_replaces_view_and_survives_reopen_with_membership() {
        let source_root = tempfile::tempdir().unwrap();
        let source = app(source_root.path());
        let mut machine =
            StateMachine::open(source_root.path().join("state.json"), source).unwrap();
        let membership = openraft::Membership::new(vec![[1, 2, 3].into_iter().collect()], None);
        machine
            .apply([
                Entry {
                    log_id: LogId::new(CommittedLeaderId::new(1, 1), 0),
                    payload: EntryPayload::Membership(membership),
                },
                entry(
                    1,
                    Command::Sql(
                        "CREATE TABLE hits (id UInt64) ENGINE = MergeTree ORDER BY id".into(),
                    ),
                ),
                entry(2, insert(b"7\n3\n")),
            ])
            .await
            .unwrap();
        let snapshot = machine.build_snapshot().await.unwrap();
        assert!(entry_files(&machine.tail).unwrap().is_empty());
        let saved: Applied = read(&machine.path).unwrap();
        assert!(saved.commands.is_empty());
        assert!(saved.tables.is_some());
        machine.apply([entry(3, insert(b"11\n"))]).await.unwrap();
        let source = machine.app.clone();
        StateMachine::open(machine.path.as_ref().clone(), source.clone()).unwrap();
        assert_eq!(ids(&source), [3, 7, 11]);
        let target_root = tempfile::tempdir().unwrap();
        let target = app(target_root.path());
        let path = target_root.path().join("state.json");
        let mut target_machine = StateMachine::open(path.clone(), target.clone()).unwrap();
        execute(
            &target,
            &Command::Sql("CREATE TABLE hits (id UInt64) ENGINE = MergeTree ORDER BY id".into()),
            1,
        )
        .unwrap();
        execute(&target, &insert(b"99\n"), 1).unwrap();
        let mut bad_meta = snapshot.meta.clone();
        bad_meta.last_log_id = Some(LogId::new(CommittedLeaderId::new(1, 1), 99));
        assert!(target_machine
            .install_snapshot(
                &bad_meta,
                Box::new(Cursor::new(snapshot.snapshot.get_ref().clone()))
            )
            .await
            .is_err());
        assert_eq!(ids(&target), [99]);
        let expected_membership = snapshot.meta.last_membership.clone();
        target_machine
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .unwrap();
        assert_eq!(ids(&target), [3, 7]);
        let mut reopened = StateMachine::open(path, target.clone()).unwrap();
        let (last, membership) = reopened.applied_state().await.unwrap();
        assert_eq!(last.unwrap().index, 2);
        assert_eq!(membership, expected_membership);
        assert_eq!(ids(&target), [3, 7]);
    }

    #[tokio::test]
    async fn checkpoint_restores_table_identity_and_insert_receipts_after_merge() {
        let root = tempfile::tempdir().unwrap();
        let app = AppState::open(Config {
            data_dir: root.path().join("view"),
            local_shards: [(
                "hits".into(),
                crate::config::LocalShardConfig {
                    key: "id".into(),
                    shard_id: 1,
                    shard_ids: vec![1, 2],
                },
            )]
            .into(),
            ..Config::default()
        })
        .unwrap();
        let path = root.path().join("state.json");
        let mut machine = StateMachine::open(path.clone(), app.clone()).unwrap();
        let command = Command::ShardInsert {
            table: types::TableId::new(uuid::Uuid::from_u128(1).to_string()),
            format: InputFormat::Csv,
            body: b"2\n".to_vec(),
            request_id: Some(uuid::Uuid::from_u128(15).to_string()),
        };
        let responses = machine
            .apply([
                entry(
                    0,
                    Command::Sql(
                        "CREATE TABLE hits (id UInt64) ENGINE = MergeTree ORDER BY id".into(),
                    ),
                ),
                entry(1, command.clone()),
                entry(2, insert(b"4\n")),
                entry(3, Command::Sql("OPTIMIZE TABLE hits".into())),
            ])
            .await
            .unwrap();
        assert!(responses.iter().all(|response| response.status == 200));
        machine.build_snapshot().await.unwrap();
        drop(machine);

        let mut machine = StateMachine::open(path, app.clone()).unwrap();
        let response = machine.apply([entry(4, command)]).await.unwrap();
        assert_eq!(response[0].status, 200);
        assert_eq!(ids(&app), [2, 4]);
    }

    #[tokio::test]
    async fn invalid_insert_records_error_without_changing_rows() {
        let root = tempfile::tempdir().unwrap();
        let app = app(root.path());
        let path = root.path().join("state.json");
        let mut machine = StateMachine::open(path.clone(), app.clone()).unwrap();
        let responses = machine
            .apply([
                entry(
                    0,
                    Command::Sql(
                        "CREATE TABLE hits (id UInt64) ENGINE = MergeTree ORDER BY id".into(),
                    ),
                ),
                entry(1, insert(b"1\n")),
                entry(2, insert(b"2\nbroken\n")),
            ])
            .await
            .unwrap();
        assert_eq!(responses[2].status, 400);
        assert!(responses[2].error.is_some());
        assert_eq!(ids(&app), [1]);
        StateMachine::open(path, app.clone()).unwrap();
        assert_eq!(ids(&app), [1]);
    }

    #[tokio::test]
    async fn failed_history_publication_disables_serving_view() {
        let root = tempfile::tempdir().unwrap();
        let app = app(root.path());
        let path = root.path().join("state.json");
        let mut machine = StateMachine::open(path.clone(), app.clone()).unwrap();
        machine
            .apply([entry(
                0,
                Command::Sql("CREATE TABLE hits (id UInt64) ENGINE = MergeTree ORDER BY id".into()),
            )])
            .await
            .unwrap();
        std::fs::create_dir(machine.tail.join("1.tmp")).unwrap();
        assert!(machine.apply([entry(1, insert(b"1\n"))]).await.is_err());
        assert!(app.check_view().is_err());
    }
}
