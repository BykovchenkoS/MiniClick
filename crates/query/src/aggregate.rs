//! Состояния глобальных агрегатов: обновление блоком, слияние, завершение.
use std::cmp::Ordering;
use types::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Count,
    Sum,
    Min,
    Max,
}

pub(crate) struct AggregateSpec {
    kind: Kind,
    argument: Option<(Expr, DataType)>,
    pub data_type: DataType,
}

impl AggregateSpec {
    pub fn bind(expr: Expr, schema: &TableSchema) -> Result<Self> {
        let Expr::Function { name, mut args } = expr else {
            return Err(DbError::Unsupported(
                "нельзя смешивать агрегаты и обычные выражения без GROUP BY".into(),
            ));
        };
        let kind = match name.as_str() {
            "count" => Kind::Count,
            "sum" => Kind::Sum,
            "min" => Kind::Min,
            "max" => Kind::Max,
            _ => return Err(DbError::Unsupported(format!("агрегат {name}"))),
        };
        if args.len() > 1 || (kind != Kind::Count && args.len() != 1) {
            return Err(DbError::Unsupported(
                "неверное число аргументов агрегата".into(),
            ));
        }
        let bound = args
            .pop()
            .map(|e| crate::expr::bind_scalar(e, schema))
            .transpose()?;
        let data_type = if kind == Kind::Count {
            DataType::UInt64
        } else {
            bound.as_ref().unwrap().1
        };
        if kind == Kind::Sum
            && !matches!(
                data_type,
                DataType::Int64 | DataType::UInt64 | DataType::Float64
            )
        {
            return Err(DbError::TypeMismatch(
                "sum ожидает числовой аргумент".into(),
            ));
        }
        Ok(Self {
            kind,
            argument: bound,
            data_type,
        })
    }

    fn empty(&self) -> State {
        State {
            kind: self.kind,
            data_type: self.data_type,
            value: default_value(self.data_type),
            seen: false,
        }
    }

    fn update(&self, state: &mut State, block: &Block) -> Result<()> {
        // Аргумент count проверяется и вычисляется, хотя NULL пока отсутствует.
        let values = self
            .argument
            .as_ref()
            .map(|(expr, data_type)| crate::expr::project(block, expr, *data_type))
            .transpose()?;
        if self.kind == Kind::Count {
            if let Value::UInt64(value) = &mut state.value {
                *value = value
                    .checked_add(block.rows() as u64)
                    .ok_or_else(|| DbError::TypeMismatch("переполнение count".into()))?;
            }
        } else if let Some(values) = values {
            for row in 0..block.rows() {
                state.accept(values.get(row))?;
            }
        }
        Ok(())
    }
}

fn default_value(t: DataType) -> Value {
    match t {
        DataType::Int64 => Value::Int64(0),
        DataType::UInt64 => Value::UInt64(0),
        DataType::Float64 => Value::Float64(0.0),
        DataType::String => Value::String(String::new()),
        DataType::Date => Value::Date(0),
        DataType::DateTime => Value::DateTime(0),
    }
}

struct State {
    kind: Kind,
    data_type: DataType,
    value: Value,
    seen: bool,
}
impl State {
    fn accept(&mut self, value: Value) -> Result<()> {
        match self.kind {
            Kind::Count => return Err(DbError::Internal("count обновляется числом строк".into())),
            Kind::Sum => match (&mut self.value, value) {
                (Value::Int64(a), Value::Int64(b)) => *a = a.wrapping_add(b),
                (Value::UInt64(a), Value::UInt64(b)) => *a = a.wrapping_add(b),
                (Value::Float64(a), Value::Float64(b)) => *a += b,
                _ => return Err(DbError::Internal("тип состояния sum".into())),
            },
            Kind::Min | Kind::Max => {
                let order = value
                    .compare(&self.value)
                    .ok_or_else(|| DbError::Internal("тип состояния min/max".into()))?;
                if !self.seen
                    || (self.kind == Kind::Min && order == Ordering::Less)
                    || (self.kind == Kind::Max && order == Ordering::Greater)
                {
                    self.value = value;
                }
            }
        }
        self.seen = true;
        Ok(())
    }

    fn merge(&mut self, other: Self) -> Result<()> {
        if self.kind != other.kind || self.data_type != other.data_type {
            return Err(DbError::Internal(
                "несовместимые состояния агрегатов".into(),
            ));
        }
        if self.kind == Kind::Count {
            if let (Value::UInt64(a), Value::UInt64(b)) = (&mut self.value, other.value) {
                *a = a
                    .checked_add(b)
                    .ok_or_else(|| DbError::TypeMismatch("переполнение count".into()))?;
            }
        } else if other.seen {
            self.accept(other.value)?;
        }
        Ok(())
    }

    fn finish(self) -> Value {
        self.value
    }
}

pub(crate) fn execute(
    input: BlockStream,
    specs: &[AggregateSpec],
    predicate: Option<&Expr>,
    header: &[ColumnDef],
) -> Result<Block> {
    let mut states: Vec<_> = specs.iter().map(AggregateSpec::empty).collect();
    for block in input {
        let block = block?;
        let block = match predicate {
            Some(p) => crate::expr::filter(&block, p)?,
            None => block,
        };
        for (spec, state) in specs.iter().zip(&mut states) {
            let mut partial = spec.empty();
            spec.update(&mut partial, &block)?;
            state.merge(partial)?;
        }
    }
    let columns = states
        .into_iter()
        .zip(header)
        .map(|(state, def)| {
            let mut data = ColumnData::empty(def.data_type);
            data.push(state.finish())?;
            Ok(Column {
                name: def.name.clone(),
                data,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Block::with_rows(columns, 1)
}
