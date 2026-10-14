use crate::{ast::Command, executor, parser, planner};
use std::sync::Arc;
use types::{Catalog, QueryEngine, QueryOutput, Result};

/// Фасад Query: SQL → собственное AST → проверенный план → поток блоков.
/// Catalog передаётся извне; подходит и MemCatalog, и постоянный каталог.
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
        match parser::parse(sql)? {
            Command::Explain(query) => {
                let plan = planner::plan(query, self.catalog.as_ref())?;
                crate::explain::execute(&plan)
            }
            Command::Select(query) => {
                let plan = planner::plan(query, self.catalog.as_ref())?;
                executor::execute(plan)
            }
            Command::Create {
                definition,
                if_not_exists,
            } => {
                self.catalog.create_table(definition, if_not_exists)?;
                Ok(QueryOutput::Ok)
            }
            Command::Drop { name, if_exists } => {
                self.catalog.drop_table(&name, if_exists)?;
                Ok(QueryOutput::Ok)
            }
            Command::Optimize { name } => {
                self.catalog.get_table(&name)?.optimize()?;
                Ok(QueryOutput::Ok)
            }
        }
    }
}
