use crate::plan::QueryPlan;
use types::{Block, Column, DbError, QueryOutput, Result};

pub(crate) fn execute(plan: QueryPlan) -> Result<QueryOutput> {
    let input = plan.table.scan(&plan.scan)?;
    let projection = plan.projection;

    // map не собирает весь результат, обрабатывается один входной блок за раз
    // ошибки чтения остаются элементами потока Result<Block>

    let stream = input.map(move |item| {
        let block = item?;
        let columns = projection
            .iter()
            .map(|column| {
                let data = block.column(&column.source).ok_or_else(|| {
                    DbError::Internal(format!(
                        "scan не вернул запрошенную колонку '{}'",
                        column.source
                    ))
                })?;
                Ok(Column {
                    name: column.output.clone(),
                    data: data.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Block::with_rows(columns, block.rows())
    });

    Ok(QueryOutput::Rows {
        header: plan.header,
        stream: Box::new(stream),
    })
}
