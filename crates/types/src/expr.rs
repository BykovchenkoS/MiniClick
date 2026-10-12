use crate::value::Value;

/// Выражение. Лежит в types, потому что через ScanRequest уходит из Query в Storage.
/// Литералы уже разобраны в Value. SelectQuery и QueryPlan живут в query.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(String),
    Literal(Value),
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// Обычные функции и агрегаты (count, sum, min, max, avg)
    /// Имя в нижнем регистре. count() — пустой args
    Function {
        name: String,
        args: Vec<Expr>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}

impl Expr {
    pub fn col(name: &str) -> Expr {
        Expr::Column(name.to_string())
    }

    pub fn lit(v: Value) -> Expr {
        Expr::Literal(v)
    }

    pub fn binary(op: BinaryOp, left: Expr, right: Expr) -> Expr {
        Expr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Все колонки, на которые ссылается выражение, без повторов
    pub fn columns(&self) -> Vec<String> {
        let mut out = Vec::new();
        self.collect_columns(&mut out);
        out
    }

    fn collect_columns(&self, out: &mut Vec<String>) {
        match self {
            Expr::Column(c) => {
                if !out.contains(c) {
                    out.push(c.clone());
                }
            }
            Expr::Literal(_) => {}
            Expr::Unary { expr, .. } => expr.collect_columns(out),
            Expr::Binary { left, right, .. } => {
                left.collect_columns(out);
                right.collect_columns(out);
            }
            Expr::Function { args, .. } => args.iter().for_each(|a| a.collect_columns(out)),
        }
    }
}
