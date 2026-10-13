use crate::ast::{SelectItem, SelectQuery};
use crate::plan::{GroupOutput, GroupPlan, ProjectionColumn, QueryPlan};
use std::collections::HashSet;
use types::{Catalog, ColumnDef, DbError, Expr, Result, ScanRequest};

pub(crate) fn plan(query: SelectQuery, catalog: &dyn Catalog) -> Result<QueryPlan> {
    let table = catalog.get_table(&query.table)?;
    let schema = &table.definition().schema;
    let mut keys = Vec::new();
    for name in query.group_by {
        let column = schema
            .column(&name)
            .ok_or_else(|| DbError::UnknownColumn(name.clone()))?;
        if !keys.iter().any(|c: &ColumnDef| c.name == name) {
            keys.push(column.clone());
        }
    }
    let grouped = !keys.is_empty();
    let mut group_outputs = Vec::new();
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
    let is_aggregate = expressions
        .iter()
        .any(|(e, _)| matches!(e, Expr::Function { .. }));
    let mut aggregates = Vec::new();
    let mut output_names = HashSet::new();
    let mut columns = Vec::new();
    let mut header = Vec::new();
    let mut projection = Vec::new();
    for (expr, output) in expressions {
        let (expr, data_type) = if grouped && !matches!(&expr, Expr::Function { .. }) {
            let (expr, data_type) = crate::expr::bind_scalar(expr, schema)?;
            let Expr::Column(name) = &expr else {
                return Err(DbError::Unsupported(
                    "SELECT с GROUP BY: ожидается ключ группы или агрегат".into(),
                ));
            };
            let index = keys.iter().position(|c| &c.name == name).ok_or_else(|| {
                DbError::Unsupported(format!("колонка '{name}' не входит в GROUP BY"))
            })?;
            group_outputs.push(GroupOutput::Key(index));
            (expr, data_type)
        } else if is_aggregate || grouped {
            let spec = crate::aggregate::AggregateSpec::bind(expr.clone(), schema)?;
            let data_type = spec.data_type;
            group_outputs.push(GroupOutput::Aggregate(aggregates.len()));
            aggregates.push(spec);
            (expr, data_type)
        } else {
            crate::expr::bind_scalar(expr, schema)?
        };
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
    for key in &keys {
        if !columns.contains(&key.name) {
            columns.push(key.name.clone());
        }
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
        aggregates: (is_aggregate || grouped).then_some(aggregates),
        grouping: grouped.then_some(GroupPlan {
            keys,
            outputs: group_outputs,
        }),
    })
}
