//! Команды и ответы журнала Raft; SQL и форматы разбираются при применении.

use crate::formats::InputFormat;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    Sql(String),
    Insert {
        table: String,
        format: InputFormat,
        body: Vec<u8>,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CommandResult {
    pub error: Option<String>,
    pub status: u16,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Command,
        R = CommandResult,
);

pub type Entry = openraft::Entry<TypeConfig>;
pub type ClusterRaft = openraft::Raft<TypeConfig>;
