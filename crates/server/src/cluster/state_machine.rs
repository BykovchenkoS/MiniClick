//! История применения — источник истины; каталог является восстанавливаемым представлением.

use super::log_store::{persist, read, storage_error};
use super::types::{Command, CommandResult, Entry, TypeConfig};
use crate::{formats::parse_input, AppState};
use openraft::storage::RaftStateMachine;
use openraft::{
    EntryPayload, LogId, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError,
    StoredMembership,
};
use serde::{Deserialize, Serialize};
use std::io::{self, Cursor};
use std::path::PathBuf;
use std::sync::{atomic::Ordering, Arc, Mutex};
use types::DbError;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Applied {
    last_applied: Option<LogId<u64>>,
    membership: StoredMembership<u64, openraft::BasicNode>,
    commands: Vec<Command>,
}

#[derive(Clone)]
pub struct StateMachine {
    path: Arc<PathBuf>,
    state: Arc<Mutex<Applied>>,
    app: AppState,
}

fn execute(app: &AppState, command: &Command) -> types::Result<CommandResult> {
    let result = match command {
        Command::Sql(sql) => app.query.execute(sql).map(|_| ()),
        Command::Insert {
            table,
            format,
            body,
        } => (|| {
            let table = app.catalog.get_table(table)?;
            let block = parse_input(*format, body, &table.definition().schema)?;
            table.insert(block)
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
    for command in &state.commands {
        execute(app, command)?;
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
        let state: Applied = read(&path)?;
        rebuild(&app, &state)?;
        Ok(Self {
            path: Arc::new(path),
            state: Arc::new(Mutex::new(state)),
            app,
        })
    }

    fn snapshot(&self) -> io::Result<Snapshot<TypeConfig>> {
        let state = self
            .state
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        let data = serde_json::to_vec(&*state).map_err(io::Error::other)?;
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
                        .is_some_and(|last| entry.log_id.index <= last.index)
                    {
                        return Err(DbError::Internal(
                            "applied log index is not increasing".into(),
                        ));
                    }
                    let mut next = state.clone();
                    next.last_applied = Some(entry.log_id);
                    match &entry.payload {
                        EntryPayload::Normal(command) => next.commands.push(command.clone()),
                        EntryPayload::Membership(membership) => {
                            next.membership =
                                StoredMembership::new(Some(entry.log_id), membership.clone())
                        }
                        EntryPayload::Blank => {}
                    }
                    // История фиксируется раньше view: после сбоя view строится заново.
                    persist(&this.path, &next)?;
                    let response = match &entry.payload {
                        EntryPayload::Normal(command) => execute(&this.app, command)?,
                        _ => CommandResult {
                            error: None,
                            status: 200,
                        },
                    };
                    *state = next;
                    results.push(response);
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
                persist(&this.path, &next)?;
                rebuild(&this.app, &next)?;
                *state = next;
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
        let snapshot = self.build_snapshot().await?;
        Ok(snapshot.meta.last_log_id.map(|_| snapshot))
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
        execute(&app, &insert(b"99\n")).unwrap();
        assert_eq!(ids(&app), [1, 2, 99]);
        let reopened = StateMachine::open(path, app.clone()).unwrap();
        assert_eq!(ids(&app), [1, 2]);
        assert_eq!(
            reopened.state.lock().unwrap().last_applied.unwrap().index,
            1
        );
        // Имитируем обратное окно: история устойчива, но view ещё не обновлён.
        let mut durable = reopened.state.lock().unwrap().clone();
        durable.commands.push(insert(b"3\n"));
        durable.last_applied = Some(LogId::new(CommittedLeaderId::new(1, 1), 2));
        persist(&reopened.path, &durable).unwrap();
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
        let target_root = tempfile::tempdir().unwrap();
        let target = app(target_root.path());
        let path = target_root.path().join("state.json");
        let mut target_machine = StateMachine::open(path.clone(), target.clone()).unwrap();
        execute(
            &target,
            &Command::Sql("CREATE TABLE hits (id UInt64) ENGINE = MergeTree ORDER BY id".into()),
        )
        .unwrap();
        execute(&target, &insert(b"99\n")).unwrap();
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
        std::fs::create_dir(path.with_extension("tmp")).unwrap();
        assert!(machine.apply([entry(1, insert(b"1\n"))]).await.is_err());
        assert!(app.check_view().is_err());
    }
}
