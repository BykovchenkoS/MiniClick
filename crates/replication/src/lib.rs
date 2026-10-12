//! Репликация. Основная работа в следующей лабе
//! Сейчас здесь только конфигурация ноды и сигнатура синхронизации

use types::{Catalog, DbError, Peer, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    /// Принимает запись (INSERT, DDL, OPTIMIZE) и делает merge
    Leader,
    /// Только читает и подтягивает состояние лидера
    Replica { leader_addr: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeConfig {
    pub node_id: String,
    pub listen_addr: String,
    pub role: Role,
    /// Как часто реплика сверяется с лидером
    pub sync_interval_ms: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SyncReport {
    pub tables_created: usize,
    pub tables_dropped: usize,
    pub parts_fetched: usize,
    pub parts_removed: usize,
}

/// Один проход синхронизации реплики с лидером
///
/// Алгоритм:
/// 1. Таблицы: создать те, что есть у лидера и нет локально, удалить лишние
/// 2. Для каждой MergeTree-таблицы: target = leader.list_parts, local = table.parts()
///    Скачать parts из target, которых нет локально, затем одним вызовом
///    table.sync_parts(скачанные, local - target)
/// 3. Ошибка по одной таблице не останавливает остальные, записать и идти дальше
pub fn sync_once(_local: &dyn Catalog, _leader: &dyn Peer) -> Result<SyncReport> {
    Err(DbError::Unsupported("replication: next lab".into()))
}
