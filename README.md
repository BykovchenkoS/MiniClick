# MiniClick

Колоночная СУБД на Rust по мотивам ClickHouse. Ветка `feat/storage` содержит
MergeTree, базовый Query и блок C: постоянный Catalog, HTTP, Swagger и форматы.

## Сборка и запуск

Целевая среда — Linux (в том числе WSL), Rust 1.90.0 с rustfmt и clippy.
Storage использует файловые блокировки Rust 1.89+ и синхронизацию каталогов;
Windows не является средой приёмки. Для замеров в WSL держите проект и данные
в Linux-файловой системе, например `~/miniclick`, а не в `/mnt/c` или `/mnt/e`.

```bash
cargo build --locked --release --bin miniclick
./target/release/miniclick
```

Swagger: <http://localhost:8123/swagger-ui>.
OpenAPI: <http://localhost:8123/api-doc/openapi.json>.
Swagger UI встроен в бинарник через `vendored` и не требует внешних ресурсов
при запуске. Загрузка зависимостей Cargo при первой сборке требует сети.

```bash
cp config.example.toml config.toml
./target/release/miniclick --config config.toml --listen-addr 127.0.0.1:8123 --data-dir ./data
./target/release/miniclick --help
```

Приоритет настроек: явно заданные CLI-флаги → TOML → встроенные значения.
Без `--config` необязательный `config.toml` ищется в рабочем каталоге.
Явно заданный отсутствующий файл, неизвестное поле или неверное значение
останавливают запуск. Все относительные пути считаются от рабочего каталога,
включая `data_dir` из TOML. По умолчанию: `127.0.0.1:8123`, `./data`,
`node-1`, роль `Leader`, лимит тела 256 МиБ (`--max-body-bytes`).
Только роль Leader доступна в этой лабораторной. SIGINT/SIGTERM прекращают
приём новых запросов и дают текущим завершиться. `RUST_LOG=info` включает
рабочие логи; статистика Storage содержит выбранные parts, гранулы и строки.

## HTTP

| Маршрут | Назначение |
|---|---|
| `GET /ping` | Проверка доступности: `Ok.` |
| `GET /help` | Примеры операций |
| `GET /tables` | Описания таблиц по имени, JSON |
| `GET /tables/{name}` | Описание и объект `stats` с числом parts, гранул и строк |
| `POST /query?format=TSV` | SQL в теле `text/plain` |
| `POST /insert/{table}?format=CSV` | Данные в теле `application/octet-stream`, без multipart |

```bash
curl -fsS localhost:8123/query --data-binary \
  "CREATE TABLE hits (event_date Date, user_id UInt64, url String) ENGINE = MergeTree ORDER BY (event_date, user_id) SETTINGS index_granularity = 8192, codec = 'LZ4'"
printf '2026-10-01,42,/home\n2026-10-02,7,/about\n' > hits.csv
curl -fsS 'localhost:8123/insert/hits?format=CSV' --data-binary @hits.csv
curl -fsS 'localhost:8123/query?format=JSONEachRow' --data-binary \
  "SELECT user_id, url FROM hits WHERE event_date = '2026-10-01' LIMIT 10"
curl -fsS localhost:8123/tables/hits
curl -fsS localhost:8123/query --data-binary 'OPTIMIZE TABLE hits'
```

INSERT не проходит через QueryEngine. Всё тело проверяется до одного вызова
`Table::insert`; ошибка поздней записи не оставляет частичных данных.
Результат SELECT также собирается целиком до отправки HTTP-ответа, включая
обход ленивого потока. Чтение файлов и обработка данных выполняются через
`spawn_blocking`. Ввод и вывод пока не потоковые: учитывайте память для тела,
разобранных колонок, сортировки INSERT и готового ответа.

Ошибки имеют вид `{"error":"..."}`. Коды: неизвестная таблица — 404;
ошибки SQL, данных, типов и форматов — 400; `NotLeader` — 409 с адресом лидера
в сообщении; превышение лимита тела — 413; I/O и внутренние ошибки — 500.
Имена таблиц и колонок регистрозависимы. SQL INSERT, несколько SQL-команд
за один запрос, JOIN, подзапросы и UNION не поддерживаются.

## Форматы

- Ввод: `CSV` (по умолчанию), `TSV`, `JSONEachRow`. CSV/TSV без заголовка,
  поля строго в порядке схемы. CSV поддерживает кавычки, удвоенные кавычки
  и переводы строк внутри поля. Номер ошибки CSV — номер записи, не физической строки.
- TSV использует табуляцию и экранирование `\\`, `\t`, `\n`, `\r`, `\0`.
  Неизвестные escape-последовательности отклоняются.
- JSONEachRow: один объект на строку, точный набор колонок, произвольный
  порядок ключей, без повторяющихся ключей и NULL. Числовые колонки принимают
  число или строку; String, Date и DateTime требуют строку.
- Вывод: `TSV` (по умолчанию), `CSV`, `JSONEachRow`, `Pretty`.
  CSV/TSV без заголовка, Pretty с заголовком. Пустой SELECT даёт пустое тело,
  кроме Pretty, где остаётся заголовок. DDL и INSERT возвращают `Ok.\n`.
- Date: `YYYY-MM-DD`; DateTime: `YYYY-MM-DD hh:mm:ss` в UTC.
  На вводе DateTime также принимает `T` между датой и временем или только дату.
  В JSON числа сохраняют числовой тип; NaN и бесконечности выводятся строками.
- Пустое тело INSERT означает ноль строк. Пустые строки внутри TSV/JSONEachRow
  считаются записями и должны соответствовать схеме; CSV-библиотека пропускает пустые строки.

## Каталог и восстановление

```text
data/
  .catalog.lock
  catalog.json                # версия, описания таблиц и уникальные id
  tables/<uuid>/
    table.json                # манифест Storage для MergeTree
    all_1_1_0/                # part
```

Catalog получает фабрики движков от сервера и не зависит от Storage.
CREATE/DROP публикуются атомарной заменой `catalog.json`. Каждый CREATE
получает новый UUID-каталог, поэтому SQL-имя не используется как путь.
После публикации DROP ошибка очистки журналируется; остатки удаляются
при следующем запуске. Временные файлы и неактивные UUID-каталоги очищаются
только после успешного восстановления зарегистрированных таблиц.

Повреждённые метаданные, отсутствующий каталог или манифест MergeTree
останавливают запуск, не создавая пустую замену. Неизвестные каталоги
не импортируются автоматически. Memory сохраняет описание и после
рестарта остаётся пустой; MergeTree сохраняет данные. Не редактируйте
метаданные вручную и не запускайте два сервера на одном каталоге данных.
Подробнее о файловом формате и восстановлении: [Storage](crates/storage/README.md).

## Генератор, демо и проверки

```bash
cargo run --release -p server --example generate_data -- --rows 1000000 --output hits.csv
# Схема выводится в stderr; CSV без заголовка — в файл или stdout.
cargo run --release -p server --example generate_data -- --rows 1000 --start-row 1000000 --output extra.csv

bash scripts/demo.sh                         # миллион строк + две дополнительные вставки
ROWS=120 EXTRA_ROWS=10 bash scripts/demo.sh   # короткая проверка сценария
```

Генератор воспроизводим, покрывает все шесть типов и 30 дат; ключ —
`(event_date, user_id)`. Неключевая `event_date_text` содержит ту же дату.
Демо сравнивает одинаковые выборки по ключу и по этой колонке, прогревает
оба запроса и записывает по три замера. Время берётся из curl, выбранные
гранулы — из логов Storage. Разница времени зависит от диска, кэша и объёма
данных; сценарий проверяет равенство результатов, не обещает ускорение
на маленькой таблице.

Демо использует отдельный временный каталог и адрес `127.0.0.1:18123`
(`MINICLICK_ADDR` переопределяет его), выполняет CREATE, три INSERT, SELECT,
два рестарта, OPTIMIZE и DROP. Сравнивает все строки с учётом дубликатов,
проверяет уменьшение parts с трёх до одного. CSV, результаты, времена
и логи сохраняются по напечатанному пути. Нужны Bash, Cargo, curl и обычные
Linux-утилиты. Скрипт отказывается работать, если по адресу уже доступен сервер.

```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
cargo build --locked --release --bin miniclick
cargo test --locked --release -p server --test process million_rows_over_http -- --ignored --nocapture
```

CI выполняет форматирование, clippy, тесты, release-сборку и короткое демо
на Linux с Rust 1.90.0. Миллион строк запускается отдельно.

## Границы текущей ветки

Существующий Query поддерживает CREATE/DROP/OPTIMIZE, SELECT с выражениями,
WHERE и LIMIT. Агрегаты, GROUP BY, ORDER BY в SELECT и SQL EXPLAIN
в этой ветке отсутствуют: полная демонстрация лабораторной ожидает блока B.
`GET /tables/{name}` показывает статистику полного скана; статистика
отсечения конкретного запроса сейчас доступна в логах Storage.

Размер гранулы и кодек задаются только через SQL SETTINGS; стандартные
значения — 8192 и LZ4. Соответствующих серверных флагов пока нет, чтобы
не менять Query и не затирать явно заданные SQL-настройки. Репликация,
проверка роли SQL-записи, `/replica/*` и потоковые ответы отложены.

## Архитектура

[Схема архитектуры](docs/architecture/architecture.drawio)

Схема подготовлена для первой лабораторной работы и описывает общую структуру системы и интерфейсы подсистем.
Детали реализации могут уточняться в ходе следующих лабораторных.
