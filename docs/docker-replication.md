# Ручная демонстрация смены лидера MiniClick

## Устройство и гарантии

Три узла используют OpenRaft 0.9.25: библиотека выполняет выборы, согласует
журнал большинством и восстанавливает отставшие узлы. Отдельного координатора
нет. Состав фиксирован секцией [cluster] в конфигурациях.

INSERT, CREATE/DROP/OPTIMIZE реплицируются как команды. Ответ 200 означает
устойчивую запись на большинстве (два из трёх) и применение на лидере.
Follower применяет тот же журнал. Один оставшийся узел не подтверждает запись.
Старый лидер не может обойти кворум. Выбор победителя не привязан к порядку
node1 → node2 → node3. SELECT читает локальную копию и может отставать;
линейризуемого чтения здесь нет.

Запись на follower возвращает 409 с адресом известного лидера. До выборов
или при таймауте большинства — 503. После таймаута исход записи неизвестен:
она может позже примениться. Автоматических повторов и дедупликации клиентских
повторов нет, повтор INSERT без проверки может создать дубли.

Межузловые RPC идут по HTTP с JSON, не по gRPC. Каждый узел создаёт свои parts
из команд. Копирование parts через /replica/* в кластерном режиме не работает.
Без [cluster] прежние фиксированные Leader/Replica и асинхронная передача
parts сохранены. В кластерном режиме журналируются также строки Memory-таблиц,
поэтому они восстанавливаются из журнала после рестарта.

У каждого узла собственный volume. cluster-identity.json сохраняет состав
и идентичность; raft-log.json — голоса, журнал и индексы; raft-state.json —
историю применения, last_applied и membership. Файлы публикуются через
временный файл, fsync, rename и fsync каталога. view/ — производный каталог:
при старте он создаётся заново из истории до открытия HTTP. Падение между
применением INSERT и фиксацией состояния не приводит к повторной вставке
поверх старых parts. Снимки содержат историю; установка блокирует читателей.

Это учебное хранилище журнала: история растёт, JSON переписывается, запуск
и установка снимка повторно исполняют команды. Память и время восстановления
растут с историей. Нет динамического состава, миграции старых данных, TLS
и авторизации. Порты Compose опубликованы только на loopback хоста.
Не меняйте id/members у существующего volume и не редактируйте view вручную.
Общие контракты types не изменены.

## Подготовка

Нужны Docker Engine, Compose v2 или новее и curl; Rust на хосте не нужен.
Образ собирается на Rust 1.96 / Debian Bookworm, первая сборка требует сети.
Соберите до защиты:

```bash
docker compose build
```

Если работают прежние контейнеры leader/replica, остановите их командой ниже.
Volumes сохраняются. Новый Compose использует отдельные node1-data/node2-data/
node3-data; старые leader-data/replica-data автоматически не импортируются.

```bash
docker compose down --remove-orphans
```

| Узел | id | Порт хоста | Конфигурация |
|---|---:|---:|---|
| node1 | 1 | 8123 | deploy/node1.toml |
| node2 | 2 | 8124 | deploy/node2.toml |
| node3 | 3 | 8125 | deploy/node3.toml |

При занятых портах задайте MINICLICK_NODE1_PORT, MINICLICK_NODE2_PORT,
MINICLICK_NODE3_PORT для команд Compose и используйте их в curl. Внутренние
адреса остаются node1:8123, node2:8123, node3:8123.

Далее отдельные команды для копирования; автоматического демо-скрипта нет.
Для повторного показа используйте новое имя таблицы либо явно удалите только
election_demo на текущем лидере.

## 1. Запуск и однократная инициализация

```bash
docker compose up -d --wait
docker compose ps
curl -sS http://localhost:8123/cluster/status
curl -sS -X POST http://localhost:8123/cluster/init
curl -sS http://localhost:8123/cluster/status
curl -sS http://localhost:8124/cluster/status
curl -sS http://localhost:8125/cluster/status
```

До init роль Learner, лидера нет. После init дождитесь Leader. Статус показывает
id, role, leader, leader_addr, term, last_log_index, last_applied, snapshot,
purged и healthy. leader_addr — внутренний адрес Docker; с хоста используйте
порт из таблицы. После рестартов init повторять не нужно; повтор вернёт 409.

## 2. Первая запись

По статусу выберите лидера. Например, победил node1:

```bash
LEADER=node1
URL=http://localhost:8123
curl -fsS "$URL/query" --data-binary 'CREATE TABLE election_demo (id UInt64, value String) ENGINE = MergeTree ORDER BY id'
curl -fsS "$URL/insert/election_demo?format=CSV" --data-binary $'1,alpha\n2,beta\n3,gamma\n'
curl -fsS "$URL/query" --data-binary 'SELECT id, value FROM election_demo ORDER BY id'
```

Покажите те же три строки SELECT на остальных узлах. При задержке повторите
запрос и статус: last_applied должен догнать запись лидера.

## 3. Аварийное завершение первого лидера

```bash
FIRST=$LEADER
docker compose kill "$FIRST"
docker compose ps --all
```

Покажите /cluster/status оставшихся узлов. После выборов сменится leader
и вырастет term. Обновите переменные по фактическому победителю, например node2:

```bash
LEADER=node2
URL=http://localhost:8124
curl -fsS "$URL/query" --data-binary 'SELECT id, value FROM election_demo ORDER BY id'
curl -fsS "$URL/insert/election_demo?format=CSV" --data-binary $'4,delta\n5,epsilon\n'
curl -fsS "$URL/query" --data-binary 'SELECT id, value FROM election_demo ORDER BY id'
```

## 4. Возврат первого узла

```bash
docker compose start "$FIRST"
docker compose ps
```

На вернувшемся узле проверьте статус и все пять строк. Например, для node1:

```bash
curl -sS http://localhost:8123/cluster/status
curl -fsS http://localhost:8123/query --data-binary 'SELECT id, value FROM election_demo ORDER BY id'
```

Перед следующим отказом дождитесь всех пяти строк и актуального last_applied
на вернувшемся узле и второй реплике. Старый лидер подключается к текущему
кластеру; конфигурации при переключениях не редактируются.

## 5. Повторный отказ

```bash
SECOND=$LEADER
docker compose kill "$SECOND"
```

Покажите статусы двух работающих узлов и обновите переменные. Например,
победил node3; если другой — используйте его имя и порт:

```bash
LEADER=node3
URL=http://localhost:8125
curl -fsS "$URL/query" --data-binary 'SELECT id, value FROM election_demo ORDER BY id'
curl -fsS "$URL/insert/election_demo?format=CSV" --data-binary $'6,zeta\n'
docker compose start "$SECOND"
```

После догоняния проверьте все три копии:

```bash
curl -fsS http://localhost:8123/query --data-binary 'SELECT id, value FROM election_demo ORDER BY id'
curl -fsS http://localhost:8124/query --data-binary 'SELECT id, value FROM election_demo ORDER BY id'
curl -fsS http://localhost:8125/query --data-binary 'SELECT id, value FROM election_demo ORDER BY id'
```

В каждом ответе ровно шесть строк:

```text
1   alpha
2   beta
3   gamma
4   delta
5   epsilon
6   zeta
```

Проверяется точный состав строк, а не только count(). Для полного рестарта
выполните docker compose restart, дождитесь выборов и повторите SELECT.

## Остановка и дополнительные наблюдения

```bash
docker compose logs --tail 30 node1 node2 node3
docker compose down
```

down сохраняет данные. down -v уничтожает все три хранилища и для рестартов
не нужен. При остановке двух узлов оставшийся обслуживает локальное чтение,
но не подтверждает запись — вернёт 503. После такого ответа не повторяйте
INSERT вслепую: исход мог остаться неопределённым.

Swagger доступен на /swagger-ui всех трёх портов; там описаны /cluster/status
и /cluster/init. Все контейнеры находятся на одном хосте, его отказ не моделируется.
