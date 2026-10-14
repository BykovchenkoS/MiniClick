use crate::plan::{ProjectionColumn, QueryPlan};
use types::{Block, BlockStream, Column, Expr, QueryOutput, Result};

pub(crate) fn execute(plan: QueryPlan) -> Result<QueryOutput> {
    if !plan.order_by.is_empty() {
        return crate::sort::execute(plan);
    }
    if plan.limit == Some(0) {
        return Ok(QueryOutput::Rows {
            header: plan.header,
            stream: Box::new(std::iter::empty()),
        });
    }
    let input = plan.table.scan(&plan.scan)?;
    if let Some(specs) = plan.aggregates {
        let header = plan.header.clone();
        let mut pending = Some((
            input,
            specs,
            plan.scan.predicate,
            header,
            plan.grouping,
            plan.limit,
        ));
        return Ok(QueryOutput::Rows {
            header: plan.header,
            stream: Box::new(std::iter::from_fn(move || {
                let (input, specs, predicate, header, grouping, limit) = pending.take()?;
                if let Some(grouping) = grouping {
                    return Some(crate::aggregate::execute_grouped(
                        input,
                        &specs,
                        predicate.as_ref(),
                        &header,
                        &grouping,
                        limit,
                    ));
                }
                Some(crate::aggregate::execute(
                    input,
                    &specs,
                    predicate.as_ref(),
                    &header,
                ))
            })),
        });
    }
    Ok(QueryOutput::Rows {
        header: plan.header,
        stream: Box::new(SelectStream {
            input,
            projection: plan.projection,
            predicate: plan.scan.predicate,
            remaining: plan.limit,
            done: false,
        }),
    })
}

struct SelectStream {
    input: BlockStream,
    projection: Vec<ProjectionColumn>,
    predicate: Option<Expr>,
    remaining: Option<u64>,
    done: bool,
}

impl Iterator for SelectStream {
    type Item = Result<Block>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done || self.remaining == Some(0) {
            return None;
        }
        let item = match self.input.next() {
            Some(item) => item,
            None => {
                self.done = true;
                return None;
            }
        };
        let result = item.and_then(|block| self.process(block));

        // После ошибки не выдаём частичный остаток результата
        if result.is_err() {
            self.done = true;
        }
        Some(result)
    }
}

impl std::iter::FusedIterator for SelectStream {}

impl SelectStream {
    fn process(&mut self, block: Block) -> Result<Block> {
        let mut block = match &self.predicate {
            Some(predicate) => crate::expr::filter(&block, predicate)?,
            None => block,
        };
        // LIMIT считает подходящие строки, а не исходные строки или блоки
        if let Some(remaining) = &mut self.remaining {
            let keep = (*remaining).min(block.rows() as u64) as usize;
            if keep < block.rows() {
                block = block.slice(0, keep);
            }
            *remaining -= keep as u64;
        }
        let columns = self
            .projection
            .iter()
            .map(|column| {
                Ok(Column {
                    name: column.output.clone(),
                    data: crate::expr::project(&block, &column.expr, column.data_type)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Block::with_rows(columns, block.rows())
    }
}
