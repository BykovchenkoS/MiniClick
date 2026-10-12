/// наше представление SELECT, не зависящее от sqlparser

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SelectQuery {
    pub table: String,
    pub projection: Vec<SelectItem>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SelectItem {
    Wildcard,
    Column { name: String, alias: Option<String> },
}
