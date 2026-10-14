/// Наше представление SELECT, не зависящее от sqlparser.
#[derive(Debug, PartialEq)]
pub(crate) struct SelectQuery {
    pub table: String,
    pub order_by: Vec<OrderItem>,
    pub group_by: Vec<String>,
    pub projection: Vec<SelectItem>,
    pub predicate: Option<types::Expr>,
    pub limit: Option<u64>,
}

#[derive(Debug, PartialEq)]
pub(crate) enum SelectItem {
    Wildcard,
    Column {
        name: String,
        alias: Option<String>,
    },
    Expression {
        expr: types::Expr,
        alias: Option<String>,
        label: String,
    },
}

#[derive(Debug, PartialEq)]
pub(crate) enum Command {
    Select(SelectQuery),
    Explain(SelectQuery),
    Create {
        definition: types::TableDefinition,
        if_not_exists: bool,
    },
    Drop {
        name: String,
        if_exists: bool,
    },
    Optimize {
        name: String,
    },
}

#[derive(Debug, PartialEq)]
pub(crate) struct OrderItem {
    pub name: String,
    pub ascending: bool,
}
