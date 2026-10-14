use std::sync::Arc;
use types::{ColumnDef, ScanRequest, Table};

/// Проверенный план. Хэндл удерживает ту таблицу, по схеме которой строился план.
pub(crate) struct QueryPlan {
    pub order_by: Vec<SortKey>,
    pub output_width: usize,
    pub table: Arc<dyn Table>,
    pub scan: ScanRequest,
    pub projection: Vec<ProjectionColumn>,
    pub header: Vec<ColumnDef>,
    pub limit: Option<u64>,
    pub grouping: Option<GroupPlan>,
    pub aggregates: Option<Vec<crate::aggregate::AggregateSpec>>,
}

pub(crate) struct ProjectionColumn {
    pub expr: types::Expr,
    pub data_type: types::DataType,
    pub output: String,
}

pub(crate) struct GroupPlan {
    pub keys: Vec<ColumnDef>,
    pub outputs: Vec<GroupOutput>,
}
pub(crate) enum GroupOutput {
    Key(usize),
    Aggregate(usize),
}

pub(crate) struct SortKey {
    pub index: usize,
    pub ascending: bool,
}
