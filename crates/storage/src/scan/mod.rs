//! План чтения удерживает снимок parts, поток выдаёт по одной грануле.

use crate::part::{internal, DataPart, PartReader};
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;
use types::{
    Block, BlockStream, DbError, Expr, KeyRange, Result, ScanRequest, ScanStats, TableDefinition,
};

pub(crate) struct SelectedPart {
    pub part: Arc<DataPart>,
    pub ranges: Vec<Range<usize>>,
}

pub(crate) struct ScanPlan {
    pub parts: Vec<SelectedPart>,
    pub stats: ScanStats,
}

pub(crate) fn plan(
    definition: &TableDefinition,
    parts: Vec<Arc<DataPart>>,
    request: &ScanRequest,
) -> Result<ScanPlan> {
    plan_with(definition, parts, request, types::may_match)
}

pub(crate) fn plan_with(
    definition: &TableDefinition,
    parts: Vec<Arc<DataPart>>,
    request: &ScanRequest,
    may_match: impl Fn(&Expr, &[String], &KeyRange) -> bool,
) -> Result<ScanPlan> {
    for (i, name) in request.columns.iter().enumerate() {
        if definition.schema.column(name).is_none() {
            return Err(DbError::UnknownColumn(name.clone()));
        }
        if request.columns[..i].contains(name) {
            return Err(internal("duplicate scan column"));
        }
    }
    let mut stats = ScanStats {
        parts_total: parts.len(),
        ..ScanStats::default()
    };
    let mut selected = Vec::new();
    for part in parts {
        let granules = part.meta.granules as usize;
        stats.granules_total = stats
            .granules_total
            .checked_add(granules)
            .ok_or_else(|| internal("granule count overflow"))?;
        stats.rows_total = stats
            .rows_total
            .checked_add(part.meta.rows)
            .ok_or_else(|| internal("row count overflow"))?;
        let matches = |range: KeyRange| {
            request
                .predicate
                .as_ref()
                .is_none_or(|predicate| may_match(predicate, &definition.order_by, &range))
        };
        if !matches(KeyRange {
            min: part.meta.min_key.clone(),
            max: part.meta.max_key.clone(),
        }) {
            continue;
        }
        let mut ranges: Vec<Range<usize>> = Vec::new();
        for granule in 0..granules {
            let max = if granule + 1 < granules {
                part.key(granule + 1)
            } else {
                part.meta.max_key.clone()
            };
            if !matches(KeyRange {
                min: part.key(granule),
                max,
            }) {
                continue;
            }
            stats.granules_selected += 1;
            stats.rows_selected = stats
                .rows_selected
                .checked_add(u64::from(part.granule_rows(granule)))
                .ok_or_else(|| internal("selected row count overflow"))?;
            if let Some(range) = ranges.last_mut().filter(|range| range.end == granule) {
                range.end += 1;
            } else {
                ranges.push(granule..granule + 1);
            }
        }
        if !ranges.is_empty() {
            stats.parts_selected += 1;
            selected.push(SelectedPart { part, ranges });
        }
    }

    Ok(ScanPlan {
        parts: selected,
        stats,
    })
}

struct Cursor {
    reader: PartReader,
    granules: Box<dyn Iterator<Item = usize> + Send>,
}

pub(crate) struct ScanStream {
    parts: VecDeque<SelectedPart>,
    current: Option<Cursor>,
    columns: Vec<String>,
    failed: bool,
}

impl ScanStream {
    pub fn into_stream(plan: ScanPlan, columns: Vec<String>) -> BlockStream {
        Box::new(Self {
            parts: plan.parts.into(),
            current: None,
            columns,
            failed: false,
        })
    }

    fn read_next(&mut self) -> Result<Option<Block>> {
        loop {
            if let Some(cursor) = &mut self.current {
                if let Some(granule) = cursor.granules.next() {
                    return cursor.reader.read_granule(granule).map(Some);
                }
                self.current = None;
            }
            let Some(selected) = self.parts.pop_front() else {
                return Ok(None);
            };
            self.current = Some(Cursor {
                reader: PartReader::new(&selected.part, &self.columns)?,
                granules: Box::new(selected.ranges.into_iter().flatten()),
            });
        }
    }
}

impl Iterator for ScanStream {
    type Item = Result<Block>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        match self.read_next() {
            Ok(Some(block)) => Some(Ok(block)),
            Ok(None) => None,
            Err(error) => {
                self.failed = true;
                self.parts.clear();
                self.current = None;
                Some(Err(error))
            }
        }
    }
}

#[cfg(test)]
mod tests;
