//! Проверка доступности и примеры HTTP-запросов.

#[utoipa::path(get, path = "/ping", tag = "admin", responses((status = 200, description = "Сервер доступен", body = String, content_type = "text/plain")))]
pub async fn ping() -> &'static str {
    "Ok.\n"
}

#[utoipa::path(get, path = "/help", tag = "admin", responses((status = 200, description = "Примеры операций", body = String, content_type = "text/plain")))]
pub async fn help() -> &'static str {
    concat!(
        "MiniClick\nGET /tables\nGET /tables/{name}\n",
        "POST /query?format=TSV: CREATE TABLE hits (id UInt64) ENGINE = MergeTree ORDER BY id\n",
        "POST /insert/hits?format=CSV: 42\\n\n",
        "POST /query: SELECT id FROM hits WHERE id > 0 ORDER BY id DESC LIMIT 10\n",
        "POST /query: SELECT count() AS n, sum(id) AS total, min(id) AS lo, max(id) AS hi FROM hits\n",
        "POST /query: SELECT id, count() AS n FROM hits GROUP BY id ORDER BY n DESC, id ASC\n",
        "POST /query: EXPLAIN SELECT id FROM hits WHERE id = 42\n",
        "POST /query: OPTIMIZE TABLE hits\nPOST /query: DROP TABLE hits\n",
        "Swagger: /swagger-ui\nCSV/TSV: без заголовка, порядок полей как в схеме.\n",
        "GROUP BY: имена колонок; ORDER BY: имена или алиасы SELECT.\n",
        "JOIN, HAVING, DISTINCT и EXPLAIN ANALYZE не поддерживаются.\n"
    )
}
