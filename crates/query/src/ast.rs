/// наше представление SELECT, не зависящее от sqlparser
#[derive(Debug, PartialEq)]
pub(crate) struct SelectQuery {
    pub table: String,
    pub projection: Vec<SelectItem>,
    pub predicate: Option<types::Expr>,
    pub limit: Option<u64>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SelectItem {
    Wildcard,
    Column { name: String, alias: Option<String> },
}
