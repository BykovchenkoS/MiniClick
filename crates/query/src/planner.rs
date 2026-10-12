use crate::ast::{SelectItem, SelectQuery};
use crate::plan::{ProjectionColumn, QueryPlan};
use std::collections::HashSet;
use types::{Catalog, ColumnDef, DbError, Expr, Result, ScanRequest};

pub(crate) fn plan(query: SelectQuery, catalog: &dyn Catalog) -> Result<QueryPlan> {
    let table = catalog.get_table(&query.table)?;
    let schema = &table.definition().schema;
    let mut expressions = Vec::new();
    for item in query.projection {
        match item {
            SelectItem::Wildcard => {
                for column in &schema.columns {
                    expressions.push((Expr::col(&column.name), column.name.clone()));
                }
            }
            SelectItem::Column { name, alias } => {
                let output = alias.unwrap_or_else(|| name.clone());
                expressions.push((Expr::Column(name), output));
            }
            SelectItem::Expression { expr, alias, label } => {
                expressions.push((expr, alias.unwrap_or(label)))
            }
        }
    }
    let mut output_names = HashSet::new();
    let mut columns = Vec::new();
    let mut header = Vec::new();
    let mut projection = Vec::new();
    for (expr, output) in expressions {
        let (expr, data_type) = crate::expr::bind_scalar(expr, schema)?;
        if !output_names.insert(output.clone()) {
            return Err(DbError::Unsupported(format!(
                "повторяющееся имя результата '{output}': укажите разные псевдонимы AS"
            )));
        }
        for name in expr.columns() {
            if !columns.contains(&name) {
                columns.push(name);
            }
        }
        header.push(ColumnDef {
            name: output.clone(),
            data_type,
        });
        projection.push(ProjectionColumn {
            expr,
            data_type,
            output,
        });
    }
    let predicate = query
        .predicate
        .map(|p| crate::expr::bind_predicate(p, schema))
        .transpose()?;
    if let Some(predicate) = &predicate {
        for name in predicate.columns() {
            if !columns.contains(&name) {
                columns.push(name);
            }
        }
    }
    Ok(QueryPlan {
        table,
        scan: ScanRequest { columns, predicate },
        projection,
        header,
        limit: query.limit,
    })
}
