# Ручная проверка MiniClick

Открой [Swagger UI](http://localhost:8123/swagger-ui/). Сервер запущен в Docker,
контейнер — `miniclick-manual`, адрес — `http://localhost:8123`.
В таблицу `manual_events` уже загружены **8 строк двумя INSERT**: это 2 parts,
4 гранулы, кодек LZ4. Данные лежат в постоянном Docker-томе `miniclick-manual-data`.
Перезапуск контейнера сохраняет MergeTree. Docker Desktop должен работать.

## Как отправлять запросы через Swagger

1. Раскрой `POST /query`, нажми **Try it out**.
2. Выбери `format`: `Pretty` для таблицы с заголовком или `TSV` для сравнения с примерами ниже.
3. Замени содержимое тела запроса на SQL и нажми **Execute**.
4. Посмотри HTTP-код и **Response body**. Отправляй одну SQL-команду за запрос.

SQL-файлы находятся в [queries](queries). Их содержимое можно копировать в тело запроса.
`01-create.sql` уже выполнен; начни с `02-select.sql`.
В таблице представлены все шесть типов: Date, UInt64, DateTime, String, Int64, Float64.
Полная схема и статистика доступны через [GET /tables/manual_events](http://localhost:8123/tables/manual_events).

## Запросы и ожидаемые результаты

Начни с исходных восьми строк, до дополнительных вставок.
Порядок колонок в результатах соответствует SELECT. В примерах TSV табуляции заменены пробелами.

### 1. Чтение и сортировка — `02-select.sql`

```sql
SELECT id, category, amount FROM manual_events ORDER BY id
```

```text
1 books 100
2 games 200
3 books -50
4 tools 300
5 games 150
6 books 75
7 tools 25
8 games 0
```

Для проверки дат и вещественных чисел отправь `SELECT * FROM manual_events ORDER BY id`
с форматом `JSONEachRow`: даты будут строками, `id`, `amount`, `ratio` — числами.

### 2. Агрегаты — `03-aggregates.sql`

```sql
SELECT count() AS n, sum(amount) AS total, min(amount) AS lo,
       max(amount) AS hi, sum(ratio) AS ratio_total
FROM manual_events
```

Ожидается одна строка: `8 800 -50 300 9`.
Попробуй `TSV`, `CSV`, `JSONEachRow` и `Pretty` для одного запроса.
CSV и TSV не содержат заголовка; Pretty содержит заголовок.

### 3. GROUP BY — `04-group-by.sql`

```sql
SELECT category, count() AS n, sum(amount) AS total
FROM manual_events GROUP BY category ORDER BY total DESC, category ASC
```

```text
games 3 350
tools 2 325
books 3 125
```

### 4. WHERE + GROUP BY + LIMIT — `05-where-group-limit.sql`

```sql
SELECT category, count() AS n, sum(amount) AS total
FROM manual_events WHERE event_date = '2026-10-01'
GROUP BY category ORDER BY total DESC, category ASC LIMIT 2
```

```text
tools 1 300
games 1 200
```

### 5. ORDER BY + LIMIT — `06-sort-limit.sql`

```sql
SELECT id, category, amount FROM manual_events
ORDER BY amount DESC, id ASC LIMIT 3
```

```text
4 tools 300
2 games 200
5 games 150
```

Можно убрать `category, amount` из SELECT: порядок id должен остаться `4, 2, 5`.
Это проверяет сортировку по колонке, которой нет в выводе.

### 6. Отсечение гранул — `07-explain-key.sql` и `08-explain-full.sql`

```sql
EXPLAIN SELECT id, category FROM manual_events WHERE event_date = '2026-10-01'
```

В плане до OPTIMIZE ожидается:

```text
parts: 1 / 2, granules: 2 / 4, rows: 4 / 8
```

Для неключевой колонки:

```sql
EXPLAIN SELECT id, category FROM manual_events WHERE category = 'books'
```

```text
parts: 2 / 2, granules: 4 / 4, rows: 8 / 8
```

Числа показывают выбранные/общие объёмы чтения **до WHERE**.
Сам SELECT с условием `category = 'books'` возвращает только id `1, 3, 6`
(добавь `ORDER BY id` для фиксированного порядка).

### 7. Ошибочные запросы и атомарность INSERT

Через `POST /query` проверь:

| SQL | HTTP |
|---|---|
| `SELECT missing_column FROM manual_events` | 400 |
| `SELECT * FROM table_that_does_not_exist` | 404 |
| `SELECT sum(category) FROM manual_events` | 400 |
| `SELECT id FROM manual_events ORDER BY id + 1` | 400 |
| `SELECT DISTINCT category FROM manual_events` | 400 |

Тело ошибки должно быть JSON с полем `error`.

Для загрузки файла раскрой `POST /insert/{table}`, нажми **Try it out**,
укажи `table = manual_events`, выбери `format` и файл в теле `application/octet-stream`.
Файлы находятся в `E:\PythonProjects\ItmoProjects\MiniClick\docs\manual-testing`.

| Файл | format | Ожидается |
|---|---|---|
| [bad-input.csv](bad-input.csv) | CSV | 400, ошибка `row 2` |
| [bad-duplicate.jsonl](bad-duplicate.jsonl) | JSONEachRow | 400, дублирующийся ключ |
| [bad-null.jsonl](bad-null.jsonl) | JSONEachRow | 400, NULL запрещён |

После каждого ошибочного файла `SELECT count() FROM manual_events` должен вернуть `8`.
`SELECT id FROM manual_events WHERE id = 99` должен вернуть пустой результат:
правильная первая строка `bad-input.csv` тоже не должна вставиться.

### 8. OPTIMIZE и рестарт — `09-optimize.sql`

Посмотри `GET /tables/manual_events`: `stats.parts_total = 2`, `rows_total = 8`.
Выполни:

```sql
OPTIMIZE TABLE manual_events
```

Ответ — `Ok.`. Повтори GET: ожидается **1 part, 4 гранулы, 8 строк**.
Повтори сортировку и агрегаты: строки и суммы должны совпасть с исходными.

В PowerShell перезапусти сервер:

```powershell
docker restart miniclick-manual
```

После запуска `/ping` снова отвечает `Ok.`; все 8 строк, сумма 800 и один part сохраняются.

### 9. Успешные вставки трёх форматов

Выполни этот сценарий после проверок исходного набора.
Отправляй файлы по одному через `POST /insert/{table}`:

| Файл | format | Ожидаемые count() / sum(amount) после вставки |
|---|---|---|
| [extra.csv](extra.csv) | CSV | 9 / 820 |
| [extra.tsv](extra.tsv) | TSV | 10 / 850 |
| [extra.jsonl](extra.jsonl) | JSONEachRow | 11 / 890 |

Каждый файл содержит одну строку. Успешный ответ — `Ok.`.
Повторная загрузка снова добавит строку: `id` не имеет ограничения уникальности.
CSV/TSV — без заголовка, в порядке колонок схемы.
В JSONEachRow ключи переставлены, `id` передан строкой — это допустимо.

### 10. Memory и DROP

Через `POST /query` создай отдельную таблицу:

```sql
CREATE TABLE manual_memory (
    event_date Date, id UInt64, event_time DateTime,
    category String, amount Int64, ratio Float64
) ENGINE = Memory
```

Через `POST /insert/{table}` загрузи `initial-a.csv`, указав `table = manual_memory`, `format = CSV`.
Запрос `SELECT count(), sum(amount) FROM manual_memory` должен вернуть `4 550`.
После `docker restart miniclick-manual` таблица Memory останется, но
`SELECT count() FROM manual_memory` вернёт `0`; MergeTree сохранит строки.
Удали тестовую Memory: `DROP TABLE manual_memory`, затем повтори
`DROP TABLE IF EXISTS manual_memory` — оба запроса успешны.

## Автоматическая проверка исходного набора

Из корня проекта:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\docs\manual-testing\check.ps1
```

Скрипт выполняет 23 проверки результатов, форматов, кодов ошибок и атомарности INSERT.
Он рассчитан на исходные **8 строк**; успешных вставок и DROP не выполняет.
Его можно повторять, в том числе после OPTIMIZE и рестарта, до вставки дополнительных строк.
При первой ошибке скрипт останавливается с ненулевым кодом.
Для другого адреса добавь `-BaseUrl http://localhost:8124`.

Чтобы вручную восстановить исходный набор, выполни `DROP TABLE IF EXISTS manual_events`,
затем SQL из [01-create.sql](queries/01-create.sql) и загрузи
[initial-a.csv](initial-a.csv), [initial-b.csv](initial-b.csv) двумя отдельными INSERT.
Это удаляет изменения именно тестовой таблицы `manual_events`.

## Управление запущенным сервером

```powershell
docker logs --tail 100 miniclick-manual
docker stop miniclick-manual
docker start miniclick-manual
docker restart miniclick-manual
```

Сервер работает в Linux-контейнере из образа `rust:1.90-bookworm`.
Текущий release-бинарник подключён из тома сборки `miniclick-c-target` только для чтения;
данные — из отдельного тома `miniclick-manual-data`. Эти тома нужны для повторного запуска.
Порт опубликован на `127.0.0.1:8123`. Лимит тела запроса — 256 МиБ.
Управлять данными можно через Swagger и HTTP; SQL INSERT не поддерживается.
