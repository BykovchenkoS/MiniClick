Ниже запросы для уже заполненной таблицы `manual_events`. Отправляй каждый отдельно через  **POST /query** , формат `Pretty`.

**JOIN пока не реализован** — пример с ним привёл отдельно в конце.

**1. Группировка, несколько агрегатов и сортировка по сумме:**

```
SELECT
    category,
    count() AS events,
    sum(amount) AS total,
    min(amount) AS smallest,
    max(amount) AS largest
FROM manual_events
GROUP BY category
ORDER BY total DESC, category ASC
```

Ожидаемые суммы: `games = 350`, `tools = 325`, `books = 125`.

**2. Фильтрация перед группировкой и выбор двух лучших категорий:**

```
SELECT
    category,
    count() AS events,
    sum(amount) AS total
FROM manual_events
WHERE event_date = '2026-10-01' AND amount > 0
GROUP BY category
ORDER BY total DESC, category ASC
LIMIT 2
```

Результат: `tools — 300`, `games — 200`.

**3. Группировка сразу по двум колонкам:**

```
SELECT
    event_date,
    category,
    count() AS events,
    sum(amount) AS total
FROM manual_events
GROUP BY event_date, category
ORDER BY event_date ASC, total DESC, category ASC
```

Получится статистика каждой категории за каждый день.

**4. Сложное условие и сортировка по двум колонкам:**

```
SELECT id, event_date, category, amount
FROM manual_events
WHERE (category = 'books' OR category = 'games') AND amount > 0
ORDER BY amount DESC, id ASC
LIMIT 3
```

Ожидаемые `id`:  **2, 5, 1** .

**5. Проверка использования индекса для диапазона дат:**

```
EXPLAIN
SELECT id, category, amount
FROM manual_events
WHERE event_date >= '2026-10-01'
  AND event_date < '2026-10-02'
ORDER BY amount DESC, id ASC
LIMIT 3
```

План покажет этапы выполнения и число выбранных parts, гранул и строк. Убери `EXPLAIN`, чтобы получить сам результат.


Создадим таблицу `events` и добавим в неё 5 строк.

**1. В Swagger → POST /query** отправь:

```sql
CREATE TABLE events (
    event_date Date,
    user_id UInt64,
    url String,
    duration_ms Int64
)
ENGINE = MergeTree
ORDER BY (event_date, user_id)
```

**2. Сохрани эти данные в файл `events.csv` без заголовка:**

```csv
2026-10-01,101,/home,120
2026-10-01,102,/catalog,350
2026-10-01,101,/checkout,800
2026-10-02,103,/home,90
2026-10-02,102,/catalog,250
```

В **POST /insert/{table}** укажи:

- `table`: `events`;
- `format`: `CSV`;
- файл: `events.csv`.

Оба запроса должны вернуть `Ok.`.

**3. Проверь через POST /query с форматом `Pretty`:**

```sql
SELECT
    user_id,
    count() AS visits,
    sum(duration_ms) AS total_duration
FROM events
GROUP BY user_id
ORDER BY total_duration DESC
```


**6. Пример JOIN для будущей реализации — сейчас не выполнится:**

При наличии таблиц `events(user_id, event_date, url)` и `users(user_id, name)`:

```
SELECT
    u.name,
    count() AS visits
FROM events AS e
JOIN users AS u ON e.user_id = u.user_id
WHERE e.event_date = '2026-10-01'
GROUP BY u.name
ORDER BY visits DESC, u.name ASC
LIMIT 10
```

Такой запрос объединяет события с пользователями и выбирает десять самых активных пользователей за день. Для него нужно добавить поддержку JOIN в Query.
