//! Проверка доступности и примеры HTTP-запросов.

#[utoipa::path(get, path = "/ping", tag = "admin", responses((status = 200, description = "Сервер доступен", body = String, content_type = "text/plain")))]
pub async fn ping() -> &'static str {
    "Ok.\n"
}

#[utoipa::path(get, path = "/help", tag = "admin", responses((status = 200, description = "Примеры операций", body = String, content_type = "text/plain")))]
pub async fn help() -> &'static str {
    "MiniClick\nGET /tables\nGET /tables/{name}\nPOST /query?format=TSV: CREATE TABLE hits (id UInt64) ENGINE = MergeTree ORDER BY id\nPOST /insert/hits?format=CSV: 42\\n\nPOST /query?format=TSV: SELECT * FROM hits WHERE id = 42 LIMIT 10\nPOST /query: OPTIMIZE TABLE hits\nPOST /query: DROP TABLE hits\nSwagger: /swagger-ui\nCSV/TSV: без заголовка, порядок полей как в схеме.\nGROUP BY, ORDER BY, агрегаты и EXPLAIN пока отсутствуют в этой ветке.\n"
}
