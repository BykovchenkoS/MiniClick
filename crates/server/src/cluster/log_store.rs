//! Устойчивые голоса и журнал; операции сериализованы общей блокировкой.

use super::types::{Entry, TypeConfig};
use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{LogId, LogState, RaftLogReader, StorageError, StorageIOError, Vote};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::{self, File};
use std::io::{self, Write};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct LogData {
    vote: Option<Vote<u64>>,
    committed: Option<LogId<u64>>,
    purged: Option<LogId<u64>>,
    entries: BTreeMap<u64, Entry>,
}

#[derive(Clone)]
pub struct LogStore {
    path: Arc<PathBuf>,
    data: Arc<Mutex<LogData>>,
}

/// rename публикует единый документ; fsync каталога закрепляет новое имя.
pub(super) fn persist<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    let temporary = path.with_extension("tmp");
    let bytes = serde_json::to_vec(value).map_err(io::Error::other)?;
    let mut file = File::create(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::other("missing storage directory"))?;
    File::open(parent)?.sync_all()
}

pub(super) fn read<T: serde::de::DeserializeOwned + Default>(path: &Path) -> io::Result<T> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(T::default()),
        Err(error) => Err(error),
    }
}

pub(super) fn storage_error(error: impl std::error::Error + 'static) -> StorageError<u64> {
    StorageIOError::write(&error).into()
}

impl LogStore {
    pub fn open(path: PathBuf) -> io::Result<Self> {
        let data: LogData = read(&path)?;
        let mut expected = match data.purged {
            Some(id) => id.index.checked_add(1),
            None => Some(0),
        };
        for (index, entry) in &data.entries {
            if expected != Some(*index) || entry.log_id.index != *index {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "nonconsecutive or misnumbered raft log",
                ));
            }
            expected = index.checked_add(1);
        }
        let last = data
            .entries
            .last_key_value()
            .map(|(index, _)| *index)
            .or(data.purged.map(|id| id.index));
        if data
            .committed
            .is_some_and(|id| last.is_none_or(|last| id.index > last))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "committed index exceeds raft log",
            ));
        }
        Ok(Self {
            path: Arc::new(path),
            data: Arc::new(Mutex::new(data)),
        })
    }

    async fn update(
        &self,
        operation: impl FnOnce(&mut LogData) + Send,
    ) -> Result<(), StorageError<u64>> {
        let mut guard = self.data.lock().await;
        let mut next = guard.clone();
        operation(&mut next);
        let path = self.path.clone();
        let saved = next.clone();
        tokio::task::spawn_blocking(move || persist(&path, &saved))
            .await
            .map_err(storage_error)?
            .map_err(storage_error)?;
        *guard = next;
        Ok(())
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError<u64>> {
        Ok(self
            .data
            .lock()
            .await
            .entries
            .iter()
            .filter(|(index, _)| range.contains(*index))
            .map(|(_, entry)| entry.clone())
            .collect())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let state = self.data.lock().await;
        Ok(LogState {
            last_purged_log_id: state.purged,
            last_log_id: state
                .entries
                .last_key_value()
                .map(|(_, entry)| entry.log_id)
                .or(state.purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.update(|state| state.vote = Some(*vote)).await
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.data.lock().await.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        self.update(|state| state.committed = committed).await
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.data.lock().await.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        self.update(move |state| {
            for entry in entries {
                state.entries.insert(entry.log_id.index, entry);
            }
        })
        .await?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.update(|state| state.entries.retain(|index, _| *index < log_id.index))
            .await
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.update(|state| {
            state.entries.retain(|index, _| *index > log_id.index);
            state.purged = Some(log_id);
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::storage::RaftLogStorageExt;
    use openraft::{CommittedLeaderId, EntryPayload};

    #[tokio::test]
    async fn vote_commit_truncation_and_purge_survive_reopen() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("log.json");
        let mut store = LogStore::open(path.clone()).unwrap();
        let id = |index| LogId::new(CommittedLeaderId::new(3, 2), index);
        store.save_vote(&Vote::new(3, 2)).await.unwrap();
        store
            .blocking_append((0..4).map(|index| Entry {
                log_id: id(index),
                payload: EntryPayload::Blank,
            }))
            .await
            .unwrap();
        store.save_committed(Some(id(2))).await.unwrap();
        store.truncate(id(3)).await.unwrap();
        store.purge(id(1)).await.unwrap();
        drop(store);
        let mut reopened = LogStore::open(path).unwrap();
        assert_eq!(reopened.read_vote().await.unwrap(), Some(Vote::new(3, 2)));
        assert_eq!(reopened.read_committed().await.unwrap(), Some(id(2)));
        let state = reopened.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(id(1)));
        assert_eq!(state.last_log_id, Some(id(2)));
        let entries = reopened.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_id, id(2));
    }

    #[test]
    fn corrupt_log_is_an_error_instead_of_an_empty_cluster() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("log.json");
        fs::write(&path, b"{truncated").unwrap();
        assert!(LogStore::open(path).is_err());
    }

    #[test]
    fn valid_json_with_missing_or_misnumbered_log_entries_is_rejected() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("log.json");
        let entry = |index| Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Blank,
        };
        for entries in [
            [(0, entry(0)), (2, entry(2))].into_iter().collect(),
            [(0, entry(1))].into_iter().collect(),
        ] {
            persist(
                &path,
                &LogData {
                    entries,
                    ..LogData::default()
                },
            )
            .unwrap();
            assert!(LogStore::open(path.clone()).is_err());
        }
    }
}
