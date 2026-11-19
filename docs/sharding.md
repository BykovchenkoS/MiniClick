# Демо шардирования через Docker и Swagger

Два шарда хранят свою часть таблицы `hits_local` в MergeTree. Координатор
предоставляет логическую таблицу `hits`. Ключ `user_id` направляет чётные ID
на `shard-even`, а нечётные на `shard-odd`. Карта задана в
[`deploy/sharding-coordinator.toml`](../deploy/sharding-coordinator.toml).
Каждый шард сверяет ключи со своим правилом размещения.
[`sharding-even.toml`](../deploy/sharding-even.toml) · [`sharding-odd.toml`](../deploy/sharding-odd.toml)

## 1. Запуск

Запустите стенд из корня репозитория.

```bash
docker compose -f compose.sharding.yaml up -d --build --wait
```

| Узел | Swagger UI |
|---|---|
| Координатор | <http://localhost:8130/swagger-ui/> |
| Шард для чётных ID | <http://localhost:8131/swagger-ui/> |
| Шард для нечётных ID | <http://localhost:8132/swagger-ui/> |

Каждый шаг в Swagger выполняется через **Try it out** и **Execute**. Команды SQL
вставляются в тело `POST /query` с форматом ответа `TSV`.

## 2. Создание физической таблицы

Выполните запрос на **обоих шардах**. Ответ каждого узла будет `200` и `Ok.`.

```sql
CREATE TABLE IF NOT EXISTS hits_local (
    user_id UInt64,
    category String,
    amount Int64
) ENGINE = MergeTree
ORDER BY user_id
SETTINGS index_granularity = 1
```

## 3. Загрузка двух блоков через координатор

Откройте `POST /insert/{table}` на **координаторе**, укажите `table = hits`
и `format = CSV`. Загрузите файлы по очереди отдельными запросами.

- [`sharding-even.csv`](../deploy/sharding-even.csv) содержит чётные ID.
- [`sharding-odd.csv`](../deploy/sharding-odd.csv) содержит нечётные ID.

Каждая загрузка возвращает `200` и `Ok.`. Один запрос принимает строки
для одного шарда. Повторная загрузка файла добавит те же строки ещё раз.

Локальное размещение можно проверить через `POST /query` на каждом шарде.

```sql
SELECT user_id FROM hits_local ORDER BY user_id
```

Ожидаемые ID на чётном шарде — `2, 4, 6`, на нечётном — `1, 3, 5`.

Чтобы проверить защиту от записи на чужой шард, загрузите
[`sharding-odd.csv`](../deploy/sharding-odd.csv) через `POST /insert/{table}`
на чётном шарде с `table = hits_local`. Запрос вернёт `400`. Повторный
SELECT на чётном шарде вернёт прежние строки.

## 4. Распределённый SELECT

Отправьте запрос на **координатор**, чтобы получить две группы с наибольшей суммой.

```sql
SELECT category, sum(amount) AS total, count() AS n
FROM hits
WHERE user_id < 6
GROUP BY category
ORDER BY total DESC, category ASC
LIMIT 2
```

При однократной загрузке каждого файла результат содержит две строки.

```text
news    60    2
blog    56    2
```

Группа `news` объединяет строки с ID 1 и 2 из разных шардов. Строка с ID 6
отфильтровывается до агрегации.

## 5. EXPLAIN и точная фильтрация

Проверьте отсечение через `EXPLAIN` на **координаторе**.

```sql
EXPLAIN SELECT user_id, category, amount FROM hits WHERE user_id = 4
```

Последняя строка плана содержит статистику чётного шарда.

```text
parts: 1 / 1, granules: 2 / 3, rows: 2 / 3 (selected / total; before WHERE)
```

Координатор выбирает чётный шард, и статистика относится к его трём гранулам.
Разреженный индекс оставляет две гранулы, чьи диапазоны могут содержать ключ 4.
Выполните тот же запрос без `EXPLAIN`. Точный `WHERE` вернёт одну строку.

```text
4    shop    50
```

## Остановка

```bash
docker compose -f compose.sharding.yaml down
```

Данные в томах сохраняются. После повторного запуска можно сразу выполнить
SELECT и EXPLAIN над уже загруженными строками.

## Демо смены лидера Raft на одном шарде

Этот стенд использует отдельный проект Docker и отдельные тома. Чётный шард
состоит из трёх участников одной Raft-группы, нечётный работает на одном узле.
Координатор ищет лидера среди адресов из `raft_shards` и читает с него после
подтверждения кворумом. Все участники группы применяют одну проверку ключа.

```bash
docker compose -f compose.sharding-raft.yaml up -d --build --wait
curl -fsS -X POST http://localhost:8141/cluster/init
```

Инициализация выполняется один раз для новых томов. Ниже адреса Swagger и
соответствующие имена сервисов.

| Узел | Сервис | Swagger UI |
|---|---|---|
| Координатор | `coordinator` | <http://localhost:8140/swagger-ui/> |
| Участник Raft 1 | `shard-even-1` | <http://localhost:8141/swagger-ui/> |
| Участник Raft 2 | `shard-even-2` | <http://localhost:8142/swagger-ui/> |
| Участник Raft 3 | `shard-even-3` | <http://localhost:8143/swagger-ui/> |
| Нечётный шард | `shard-odd` | <http://localhost:8144/swagger-ui/> |

1. Через `GET /cluster/status` на портах 8141–8143 найдите участника с
   `role = "Leader"`. Выполните DDL из шага 2 на лидере и на нечётном шарде.
2. На координаторе загрузите оба CSV из шага 3. Убедитесь, что
   `SELECT count() FROM hits` возвращает `6`.
3. Остановите сервис текущего лидера. В примере это первый участник.

   ```bash
   docker compose -f compose.sharding-raft.yaml stop shard-even-1
   ```

4. По `GET /cluster/status` найдите нового лидера среди двух оставшихся узлов.
   Повторите `SELECT count() FROM hits` через координатор. Ответ останется `6`.
5. Загрузите через координатор новый CSV с одной строкой `8,shop,70`.
   `INSERT` вернёт `200`, а запрос `SELECT amount FROM hits WHERE user_id = 8`
   через координатор вернёт `70`. Общий `count()` станет `7`.

Для восстановления остановленного узла выполните `docker compose -f
compose.sharding-raft.yaml start shard-even-1`, подставив имя сервиса из шага 3.
Координатор заново ищет лидера для каждого запроса. Если соединение оборвалось
после отправки INSERT, результат может быть неизвестен. Автоматического повтора
такой записи нет.
