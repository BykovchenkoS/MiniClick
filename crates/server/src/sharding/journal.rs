//! Решение о вставке записывается до отправки первого блока.

use super::{load_shard, shard_error, table_url};
use crate::{
    error::{blocking, ApiError},
    formats::InputFormat,
    AppState,
};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
};
use types::{DbError, TableId};

pub(crate) struct Journal {
    path: PathBuf,
    _lock: File,
}

#[derive(Deserialize, Serialize)]
pub(super) struct Decision {
    pub(super) id: String,
    pub(super) table: String,
    pub(super) checksum: String,
    pub(super) format: InputFormat,
    pub(super) batches: Vec<Batch>,
}

#[derive(Deserialize, Serialize)]
pub(super) struct Batch {
    pub(super) shard: u64,
    pub(super) table: TableId,
    pub(super) body: Vec<u8>,
}

pub(crate) fn initialize(config: &crate::config::Config) -> types::Result<()> {
    if config.sharding.is_empty() {
        return Ok(());
    }
    let path = path_for(config);
    fs::create_dir_all(&path)?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    let _journal = Journal::open(path.clone(), true)?;
    let marker = path.join("topology.json");
    let document = super::topology::document(config)?;
    let tables = &document["tables"];
    match File::open(&marker) {
        Ok(file) => {
            let saved: serde_json::Value =
                serde_json::from_reader(io::BufReader::new(file)).map_err(io::Error::other)?;
            if saved != *tables {
                return Err(DbError::Syntax(
                    "shard journal belongs to a different topology".into(),
                ));
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let mut staged = tempfile::NamedTempFile::new_in(&path)?;
            serde_json::to_writer(&mut staged, tables).map_err(io::Error::other)?;
            staged.as_file().sync_all()?;
            staged
                .persist_noclobber(marker)
                .map_err(|error| error.error)?;
            sync_dir(&path)?;
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn path_for(config: &crate::config::Config) -> PathBuf {
    config
        .shard_journal_dir
        .clone()
        .unwrap_or_else(|| config.data_dir.join("shard-journal"))
}

impl Journal {
    fn open(path: PathBuf, exclusive: bool) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.join("lock"))?;
        if exclusive {
            file.lock()?;
        } else {
            file.lock_shared()?;
        }
        Ok(Self { path, _lock: file })
    }

    pub(super) async fn lock(app: &AppState, exclusive: bool) -> Result<Self, ApiError> {
        let path = path_for(&app.config);
        blocking(move || Ok(Self::open(path, exclusive)?)).await
    }

    pub(super) fn pending(&self) -> io::Result<bool> {
        self.path.join("pending.json").try_exists()
    }

    pub(super) fn receipt(&self, id: &str) -> io::Result<Option<String>> {
        match fs::read_to_string(self.receipt_path(id)) {
            Ok(checksum) => Ok(Some(checksum)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn receipt_path(&self, id: &str) -> PathBuf {
        self.path.join(format!("{id}.receipt"))
    }

    pub(super) async fn record(
        self,
        decision: Decision,
    ) -> Result<(Self, Decision, bool), ApiError> {
        blocking(move || {
            let durable = match self.decide(&decision) {
                Ok(()) => true,
                Err(error) if self.pending()? => {
                    tracing::error!(%error, "Не удалось подтвердить запись решения");
                    false
                }
                Err(error) => return Err(error),
            };
            Ok((self, decision, durable))
        })
        .await
    }

    fn decide(&self, decision: &Decision) -> types::Result<()> {
        let mut staged = tempfile::NamedTempFile::new_in(&self.path)?;
        use std::io::Write;
        {
            let mut output = io::BufWriter::new(staged.as_file_mut());
            serde_json::to_writer(&mut output, decision).map_err(io::Error::other)?;
            output.flush()?;
        }
        staged.as_file().sync_all()?;
        staged
            .persist_noclobber(self.path.join("pending.json"))
            .map_err(|error| error.error)?;
        sync_dir(&self.path)?;
        Ok(())
    }

    pub(super) async fn load(&self) -> Result<Option<Decision>, ApiError> {
        let pending = self.path.join("pending.json");
        let pending_file = match File::open(&pending) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(DbError::from(error).into()),
        };
        blocking(move || {
            let decision: Decision = serde_json::from_reader(io::BufReader::new(pending_file))
                .map_err(io::Error::other)?;
            if !uuid::Uuid::parse_str(&decision.id).is_ok_and(|id| id.to_string() == decision.id) {
                return Err(DbError::Internal("invalid ID in shard journal".into()));
            }
            Ok(Some(decision))
        })
        .await
    }

    pub(super) async fn settle(&self, app: &AppState, decision: &Decision) -> Result<(), ApiError> {
        sync_dir(&self.path).map_err(DbError::from)?;
        if self.receipt(&decision.id).map_err(DbError::from)?.is_none() {
            finish_shards(app, decision).await?;
            let mut receipt = tempfile::NamedTempFile::new_in(&self.path).map_err(DbError::from)?;
            use std::io::Write;
            receipt
                .write_all(decision.checksum.as_bytes())
                .map_err(DbError::from)?;
            receipt.as_file().sync_all().map_err(DbError::from)?;
            receipt
                .persist_noclobber(self.receipt_path(&decision.id))
                .map_err(|error| DbError::from(error.error))?;
            sync_dir(&self.path).map_err(DbError::from)?;
        }
        fs::remove_file(self.path.join("pending.json")).map_err(DbError::from)?;
        sync_dir(&self.path).map_err(DbError::from)?;
        Ok(())
    }
}

pub(crate) async fn read_guard(app: &AppState) -> Result<Option<Journal>, ApiError> {
    if app.config.sharding.is_empty() {
        return Ok(None);
    }
    loop {
        let shared = Journal::lock(app, false).await?;
        if !shared.pending().map_err(DbError::from)? {
            return Ok(Some(shared));
        }
        drop(shared);
        let exclusive = Journal::lock(app, true).await?;
        let Some(decision) = exclusive.load().await? else {
            continue;
        };
        exclusive.settle(app, &decision).await.map_err(|_| {
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "a sharded INSERT is pending; retry after shard recovery",
            )
        })?;
    }
}

async fn finish_shards(app: &AppState, decision: &Decision) -> Result<(), ApiError> {
    let client = app
        .shard_client
        .as_ref()
        .ok_or_else(|| DbError::Internal("shard HTTP client is missing".into()))?;
    let table = app
        .config
        .sharding
        .get(&decision.table)
        .ok_or_else(|| DbError::UnknownTable(decision.table.clone()))?;
    for batch in &decision.batches {
        let (base, entry) = load_shard(client, table, batch.shard).await?;
        if entry.entry.id != batch.table {
            return Err(shard_error(
                batch.shard,
                "physical table changed during pending INSERT",
            ));
        }
        let url = table_url(base, "insert", &batch.table)?;
        let response = client
            .post(url)
            .query(&[("format", decision.format)])
            .query(&[("request_id", decision.id.as_str())])
            .body(batch.body.clone())
            .send()
            .await
            .map_err(|error| shard_error(batch.shard, error))?;
        if response.status() != StatusCode::OK {
            return Err(shard_error(
                batch.shard,
                format!("INSERT returned HTTP {}", response.status()),
            ));
        }
    }
    Ok(())
}

fn sync_dir(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    let _ = path;
    Ok(())
}
