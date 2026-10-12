//! Трек A. MergeTree: реализует types::Table и types::TableFactory
//! Внутренние интерфейсы (PartRepository, DataPart, Codec, PrimaryIndex
//! PartPruner, MergePolicy) и типы PartId, PartMeta, Mark, GranuleRange живут здесь

// Формат подключается к записи и чтению parts в следующих этапах.
#[allow(dead_code)]
mod format;

// До подключения MergeTree модуль проверяется изолированно.
#[allow(dead_code)]
mod part;
#[cfg(test)]
mod test_support;

// Репозиторий подключается к таблице в следующем этапе.
#[allow(dead_code)]
mod repository;
