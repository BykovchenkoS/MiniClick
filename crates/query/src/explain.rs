//! EXPLAIN использует тот же проверенный план, что и SELECT.
use crate::plan::QueryPlan;
use types::*;

pub(crate) fn execute(plan: &QueryPlan) -> Result<QueryOutput> {
    let mut nodes = Vec::new();
    let outputs = |width: usize| {
        plan.projection[..width]
            .iter()
            .map(|p| {
                format!(
                    "{} AS {}: {}",
                    expression(&p.expr),
                    identifier(&p.output),
                    p.data_type
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    };
    if plan.limit == Some(0) {
        nodes.push(format!("Empty (LIMIT 0) [{}]", outputs(plan.output_width)));
    } else {
        let sorted = !plan.order_by.is_empty();
        if sorted && plan.output_width < plan.header.len() {
            nodes.push(format!(
                "Output [{}]",
                plan.header[..plan.output_width]
                    .iter()
                    .map(|c| identifier(&c.name))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if sorted {
            if let Some(limit) = plan.limit {
                nodes.push(format!("Limit {limit}"));
            }
            nodes.push(format!(
                "Sort [{}]",
                plan.order_by
                    .iter()
                    .map(|key| {
                        let p = &plan.projection[key.index];
                        format!(
                            "{} {}",
                            expression(&p.expr),
                            if key.ascending { "ASC" } else { "DESC" }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if plan.aggregates.is_some() {
            if !sorted {
                if let Some(limit) = plan.limit {
                    nodes.push(format!("Limit {limit}"));
                }
            }
            let keys = plan.grouping.as_ref().map_or_else(
                || "global".into(),
                |g| {
                    format!(
                        "by [{}]",
                        g.keys
                            .iter()
                            .map(|c| identifier(&c.name))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                },
            );
            nodes.push(format!(
                "Aggregate {keys} [{}]",
                outputs(plan.projection.len())
            ));
        } else {
            nodes.push(format!("Project [{}]", outputs(plan.projection.len())));
            if !sorted {
                if let Some(limit) = plan.limit {
                    nodes.push(format!("Limit {limit}"));
                }
            }
        }
        if let Some(predicate) = &plan.scan.predicate {
            nodes.push(format!("Filter {}", expression(predicate)));
        }
        nodes.push(format!(
            "Scan {} [{}]",
            identifier(&plan.table.definition().name),
            plan.scan
                .columns
                .iter()
                .map(|name| identifier(name))
                .collect::<Vec<_>>()
                .join(", ")
        ));
        // Статистика отсечения: scan не вызывается.
        let stats = plan.table.explain_scan(&plan.scan)?;
        nodes.push(format!(
            "parts: {} / {}, granules: {} / {}, rows: {} / {} (selected / total; before WHERE)",
            stats.parts_selected,
            stats.parts_total,
            stats.granules_selected,
            stats.granules_total,
            stats.rows_selected,
            stats.rows_total
        ));
    }
    let lines = nodes
        .into_iter()
        .enumerate()
        .map(|(depth, text)| format!("{}{text}", "  ".repeat(depth)))
        .collect();
    let block = Block::new(vec![Column {
        name: "explain".into(),
        data: ColumnData::String(lines),
    }])?;
    Ok(QueryOutput::Rows {
        header: block.header(),
        stream: Box::new(std::iter::once(Ok(block))),
    })
}
fn identifier(name: &str) -> String {
    format!(
        "`{}`",
        name.replace('`', "``")
            .replace('\n', "\\n")
            .replace('\r', "\\r")
            .replace('\t', "\\t")
    )
}
fn expression(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) => identifier(name),
        Expr::Literal(value) => match value {
            Value::String(s) => format!(
                "'{}'",
                s.replace('\\', "\\\\")
                    .replace('\'', "''")
                    .replace('\n', "\\n")
                    .replace('\r', "\\r")
                    .replace('\t', "\\t")
            ),
            Value::Date(_) | Value::DateTime(_) => format!("{} '{value}'", value.data_type()),
            _ => value.to_string(),
        },
        Expr::Unary { op, expr } => format!(
            "({} {})",
            match op {
                UnaryOp::Not => "NOT",
                UnaryOp::Neg => "-",
            },
            expression(expr)
        ),
        Expr::Binary { op, left, right } => {
            use BinaryOp::*;
            let op = match op {
                Add => "+",
                Sub => "-",
                Mul => "*",
                Div => "/",
                Eq => "=",
                NotEq => "!=",
                Lt => "<",
                LtEq => "<=",
                Gt => ">",
                GtEq => ">=",
                And => "AND",
                Or => "OR",
            };
            format!("({} {op} {})", expression(left), expression(right))
        }
        Expr::Function { name, args } => format!(
            "{name}({})",
            args.iter().map(expression).collect::<Vec<_>>().join(", ")
        ),
    }
}
