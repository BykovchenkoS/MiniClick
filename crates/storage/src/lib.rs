//! Трек A. MergeTree: реализует types::Table и types::TableFactory
//! Внутренние интерфейсы (PartRepository, DataPart, Codec, PrimaryIndex
//! PartPruner, MergePolicy) и типы PartId, PartMeta, Mark, GranuleRange живут здесь

mod format;

mod part;
#[cfg(test)]
mod test_support;

mod repository;
mod table;

pub use table::{MergeTreeFactory, MergeTreeTable};

mod merge;
mod scan;
