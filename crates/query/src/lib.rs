//! Трек B. Реализует types::QueryEngine
//! SQL разбирает sqlparser-rs (диалект ClickHouse), его типы не выходят за пределы этого крейта
//! Внутри: перевод AST sqlparser в SelectQuery / TableDefinition, планировщик, исполнитель, трансформы
