use query::is_write;

#[test]
fn classifies_supported_sql_as_read_or_write() {
    for sql in [
        "SELECT * FROM t",
        "-- comment\nsElEcT id FROM t WHERE id = 1",
        "EXPLAIN SELECT * FROM t",
    ] {
        assert!(!is_write(sql).unwrap(), "{sql}");
    }
    for sql in [
        "CREATE TABLE t (id UInt64) ENGINE = Memory",
        "DROP TABLE t",
        "OPTIMIZE TABLE t",
    ] {
        assert!(is_write(sql).unwrap(), "{sql}");
    }
}

#[test]
fn invalid_or_unsupported_sql_is_an_error_not_a_write() {
    for sql in [
        "",
        "not sql",
        "INSERT INTO t VALUES (1)",
        "DROP TABLE a; DROP TABLE b",
    ] {
        assert!(is_write(sql).is_err(), "{sql}");
    }
}
