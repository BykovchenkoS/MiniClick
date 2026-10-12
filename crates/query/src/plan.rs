use std::sync::Arc;
use types::{ColumnDef, ScanRequest, Table};

/// Проверенный план. Хэндл удерживает ту таблицу, по схеме которой строился план.
pub(crate) struct QueryPlan {
    pub table: Arc<dyn Table>,
    pub scan: ScanRequest,
    pub projection: Vec<ProjectionColumn>,
    pub header: Vec<ColumnDef>,
}

pub(crate) struct ProjectionColumn {
    pub source: String,
    pub output: String,
}
