//! Полная сортировка результата, LIMIT применяется после неё
use crate::plan::QueryPlan;
use std::cmp::Ordering;
use types::*;

pub(crate) fn execute(mut plan: QueryPlan) -> Result<QueryOutput> {
    let order = std::mem::take(&mut plan.order_by);
    let limit = plan.limit.take();
    let header = plan.header[..plan.output_width].to_vec();
    if limit == Some(0) {
        return Ok(QueryOutput::Rows {
            header,
            stream: Box::new(std::iter::empty()),
        });
    }
    let QueryOutput::Rows {
        header: internal_header,
        stream,
    } = crate::executor::execute(plan)?
    else {
        return Err(DbError::Internal("сортировка ожидает строки".into()));
    };
    let names: Vec<_> = header.iter().map(|c| c.name.clone()).collect();
    let mut pending = Some(stream);
    Ok(QueryOutput::Rows {
        header,
        stream: Box::new(std::iter::from_fn(move || {
            let stream = pending.take()?;
            Some((|| {
                let mut blocks = vec![Block::new(
                    internal_header
                        .iter()
                        .map(|c| Column {
                            name: c.name.clone(),
                            data: ColumnData::empty(c.data_type),
                        })
                        .collect(),
                )?];
                for block in stream {
                    blocks.push(block?);
                }
                let block = Block::concat(&blocks)?;
                let mut indices: Vec<_> = (0..block.rows()).collect();
                indices.sort_by(|&a, &b| {
                    for key in &order {
                        let column = &block.columns()[key.index].data;
                        let cmp = compare(column, a, b);
                        if cmp != Ordering::Equal {
                            return if key.ascending { cmp } else { cmp.reverse() };
                        }
                    }
                    Ordering::Equal
                });
                if let Some(limit) = limit {
                    indices.truncate(limit.min(indices.len() as u64) as usize);
                }
                block.take(&indices).select(&names)
            })())
        })),
    })
}

fn compare(column: &ColumnData, a: usize, b: usize) -> Ordering {
    match column {
        ColumnData::Int64(v) => v[a].cmp(&v[b]),
        ColumnData::UInt64(v) => v[a].cmp(&v[b]),
        ColumnData::Float64(v) => v[a].total_cmp(&v[b]),
        ColumnData::String(v) => v[a].cmp(&v[b]),
        ColumnData::Date(v) => v[a].cmp(&v[b]),
        ColumnData::DateTime(v) => v[a].cmp(&v[b]),
    }
}
