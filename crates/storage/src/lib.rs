//! MergeTree: хранение parts, отсечение гранул и слияние через OPTIMIZE.
//! Общие контракты Table и TableFactory находятся в types.

mod format;
mod merge;
mod part;
mod repository;
mod scan;
mod table;

pub use table::{MergeTreeFactory, MergeTreeTable};

#[cfg(test)]
mod test_support;
