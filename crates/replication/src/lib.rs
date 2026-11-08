//! Репликация Leader–Replica: периодическая сверка реплики с лидером.
//! Сетевой `Peer` и фоновый цикл живут в `server`.

use std::collections::{BTreeMap, BTreeSet};

use types::{Catalog, DbError, Engine, NodeIdentity, Peer, Result, TableEntry};

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    /// Принимает запись (INSERT, DDL, OPTIMIZE) и делает merge
    Leader,
    /// Только читает и подтягивает состояние лидера
    Replica {
        leader_addr: String,
        /// Ожидаемый node_id лидера; защищает от подключения к чужому узлу
        leader_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeConfig {
    pub node_id: String,
    /// Общий идентификатор кластера; должен совпадать у лидера и реплик
    pub cluster_id: String,
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
    /// Сколько таблиц не удалось обработать; остальные при этом синхронизируются.
    pub failures: usize,
}

/// Один проход синхронизации реплики с лидером.
///
/// Алгоритм:
/// 1. Таблицы сверяются по `(имя, TableId)`: недостающие создаются с id лидера,
///    лишние удаляются, при смене id таблица пересоздаётся.
/// 2. Для каждой MergeTree-таблицы: target = leader.list_parts(id),
///    local = table.parts(). Скачать недостающие parts и одним вызовом
///    table.sync_parts(скачанные, local - target).
/// 3. Ошибка по одной таблице не останавливает остальные: она учитывается
///    в failures и логируется.
///
/// Ошибки `ping` и `list_tables` фатальны: без снимка лидера локальные данные
/// не меняются. Идентичность лидера проверяется до любых изменений.
/// `Memory`-таблицы не реплицируются.
pub fn sync_once(
    local: &dyn Catalog,
    leader: &dyn Peer,
    expected: &NodeIdentity,
) -> Result<SyncReport> {
    let actual = leader.ping()?;
    if &actual != expected {
        return Err(DbError::WrongLeader {
            expected: format!("{}@{}", expected.cluster_id, expected.node_id),
            actual: format!("{}@{}", actual.cluster_id, actual.node_id),
        });
    }
    let remote = leader.list_tables()?;
    let local_before = local.table_entries();
    let mut report = SyncReport::default();
    // Таблицы, которые не удалось создать или подготовить: parts не трогаем.
    let mut unavailable = BTreeSet::new();

    let remote_tables: BTreeMap<&str, &TableEntry> = remote
        .iter()
        .filter(|entry| entry.definition.engine == Engine::MergeTree)
        .map(|entry| (entry.definition.name.as_str(), entry))
        .collect();
    let local_tables: BTreeMap<&str, &TableEntry> = local_before
        .iter()
        .filter(|entry| entry.definition.engine == Engine::MergeTree)
        .map(|entry| (entry.definition.name.as_str(), entry))
        .collect();

    for (name, remote_entry) in &remote_tables {
        let name = *name;
        let remote_entry = *remote_entry;
        match local_tables.get(name) {
            Some(local_entry) if local_entry.id == remote_entry.id => {
                if local_entry.definition != remote_entry.definition {
                    report.failures += 1;
                    unavailable.insert(name);
                    tracing::warn!(table = %name, "Схема таблицы отличается от лидера");
                }
            }
            Some(_) => {
                if let Err(error) = local.drop_table(name, true) {
                    report.failures += 1;
                    unavailable.insert(name);
                    tracing::warn!(table = %name, %error, "Не удалось удалить таблицу перед пересозданием");
                    continue;
                }
                match local.create_table_with_id(
                    remote_entry.definition.clone(),
                    remote_entry.id.clone(),
                    false,
                ) {
                    Ok(()) => report.tables_created += 1,
                    Err(error) => {
                        report.failures += 1;
                        unavailable.insert(name);
                        tracing::warn!(table = %name, %error, "Не удалось создать таблицу на реплике");
                    }
                }
            }
            None => match local.create_table_with_id(
                remote_entry.definition.clone(),
                remote_entry.id.clone(),
                false,
            ) {
                Ok(()) => report.tables_created += 1,
                Err(error) => {
                    report.failures += 1;
                    unavailable.insert(name);
                    tracing::warn!(table = %name, %error, "Не удалось создать таблицу на реплике");
                }
            },
        }
    }

    for name in local_tables.keys() {
        let name = *name;
        if remote_tables.contains_key(name) {
            continue;
        }
        match local.drop_table(name, true) {
            Ok(()) => report.tables_dropped += 1,
            Err(error) => {
                report.failures += 1;
                tracing::warn!(table = %name, %error, "Не удалось удалить лишнюю таблицу на реплике");
            }
        }
    }

    for (name, remote_entry) in &remote_tables {
        let name = *name;
        if unavailable.contains(name) {
            continue;
        }
        if let Err(error) = sync_table(local, leader, remote_entry, &mut report) {
            report.failures += 1;
            tracing::warn!(table = %name, %error, "Не удалось синхронизировать parts");
        }
    }

    Ok(report)
}

/// Приводит набор активных parts одной таблицы к набору лидера.
fn sync_table(
    local: &dyn Catalog,
    leader: &dyn Peer,
    entry: &TableEntry,
    report: &mut SyncReport,
) -> Result<()> {
    let table = local.get_table_by_id(&entry.id)?;
    let target: BTreeSet<String> = leader.list_parts(&entry.id)?.into_iter().collect();
    let have: BTreeSet<String> = table.parts()?.into_iter().collect();
    let missing: Vec<String> = target.difference(&have).cloned().collect();
    let remove: Vec<String> = have.difference(&target).cloned().collect();
    if missing.is_empty() && remove.is_empty() {
        return Ok(());
    }
    // Сначала скачиваем всё, затем публикуем набор одним вызовом:
    // при ошибке скачивания локальные parts не меняются.
    let mut fetched = Vec::with_capacity(missing.len());
    for part in &missing {
        let data = leader.fetch_part(&entry.id, part)?;
        if &data.name != part {
            return Err(DbError::Internal(format!(
                "leader returned part {} instead of {part}",
                data.name
            )));
        }
        fetched.push(data);
    }
    table.sync_parts(fetched, &remove)?;
    report.parts_fetched += missing.len();
    report.parts_removed += remove.len();
    Ok(())
}
