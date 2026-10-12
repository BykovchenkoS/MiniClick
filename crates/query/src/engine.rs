use crate::{executor, parser, planner};
use std::sync::Arc;
use types::{Catalog, QueryEngine, QueryOutput, Result};

pub struct SqlQueryEngine {
    catalog: Arc<dyn Catalog>,
}

impl SqlQueryEngine {
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }
}

impl QueryEngine for SqlQueryEngine {
    fn execute(&self, sql: &str) -> Result<QueryOutput> {
        let query = parser::parse(sql)?;
        let plan = planner::plan(query, self.catalog.as_ref())?;
        executor::execute(plan)
    }
}
