use crate::ast::{SelectItem, SelectQuery};
use crate::plan::{ProjectionColumn, QueryPlan};
use std::collections::HashSet;
use types::{Catalog, ColumnDef, DbError, Result, ScanRequest};

pub(crate) fn plan(query: SelectQuery, catalog: &dyn Catalog) -> Result<QueryPlan> {
    let table = catalog.get_table(&query.table)?;
    let schema = &table.definition().schema;
    let mut projection = Vec::new();
    for item in query.projection {
        match item {
            SelectItem::Wildcard => {
                for column in &schema.columns {
                    projection.push(ProjectionColumn {
                        source: column.name.clone(),
                        output: column.name.clone(),
                    });
                }
            }
            SelectItem::Column { name, alias } => {
                projection.push(ProjectionColumn {
                    output: alias.unwrap_or_else(|| name.clone()),
                    source: name,
                });
            }
        }
    }
    let mut output_names = HashSet::new();
    let mut columns = Vec::new();
    let mut header = Vec::new();
    for column in &projection {
        let definition = schema
            .column(&column.source)
            .ok_or_else(|| DbError::UnknownColumn(column.source.clone()))?;
        if !output_names.insert(column.output.clone()) {
            return Err(DbError::Unsupported(format!(
                "повторяющееся имя результата '{}': укажите разные псевдонимы AS",
                column.output
            )));
        }
        if !columns.contains(&column.source) {
            columns.push(column.source.clone());
        }
        header.push(ColumnDef {
            name: column.output.clone(),
            data_type: definition.data_type,
        });
    }
    Ok(QueryPlan {
        table,
        scan: ScanRequest {
            columns,
            predicate: None,
        },
        projection,
        header,
    })
}
