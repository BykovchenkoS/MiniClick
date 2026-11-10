#!/usr/bin/env bash
# Проверяет две независимые копии данных, догоняющую синхронизацию и перезапуск.
set -euo pipefail

project_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$project_dir"
table="replication_demo_$(date +%s)_$$"
compose() { docker compose "$@"; }
sql() {
    local node=$1 query=$2
    compose exec -T "$node" curl --silent --show-error --fail --max-time 5 \
        --data-binary "$query" 'http://127.0.0.1:8123/query?format=TSV'
}
insert() {
    compose exec -T leader curl --silent --show-error --fail --max-time 5 \
        --data-binary "$1" "http://127.0.0.1:8123/insert/$table?format=CSV"
}
assert_rows() {
    local node=$1 expected=$2 actual
    actual=$(sql "$node" "SELECT id, value FROM $table ORDER BY id")
    if [[ $actual != "$expected" ]]; then
        printf 'Неверные строки на %s.\nОжидались:\n%s\nПолучены:\n%s\n' "$node" "$expected" "$actual" >&2
        return 1
    fi
    printf '%s:\n%s\n' "$node" "$actual"
}
await_replica() {
    local expected=$1 actual=''
    for ((attempt=0; attempt<60; attempt++)); do
        if actual=$(sql replica "SELECT id, value FROM $table ORDER BY id" 2>/dev/null) \
            && [[ $actual == "$expected" ]]; then
            assert_rows replica "$expected"
            return
        fi
        sleep 0.5
    done
    printf 'Реплика не догнала лидера. Последний ответ:\n%s\n' "$actual" >&2
    compose logs --tail 30 replica >&2
    return 1
}
restore_nodes() {
    local status=$?
    if ((status != 0)); then
        compose logs --tail 30 >&2 || true
    fi
    compose start leader replica >/dev/null || true
}
trap restore_nodes EXIT

echo '1. Сборка и запуск двух узлов с отдельными volumes.'
compose up -d --build --wait --wait-timeout 90
sql leader "CREATE TABLE $table (id UInt64, value String) ENGINE = MergeTree ORDER BY (id)" >/dev/null
insert $'1,alpha\n2,beta\n' >/dev/null
first=$'1\talpha\n2\tbeta'
assert_rows leader "$first"
await_replica "$first"

echo '2. Запись на реплику должна вернуть HTTP 409.'
status=$(compose exec -T replica curl --silent --show-error --max-time 5 \
    --output /dev/null --write-out '%{http_code}' --data-binary $'99,forbidden\n' \
    "http://127.0.0.1:8123/insert/$table?format=CSV")
[[ $status == 409 ]] || { echo "Ожидался 409, получен $status" >&2; exit 1; }
echo "HTTP $status"
assert_rows replica "$first"

echo '3. Останавливаем реплику: лидер подтверждает INSERT без неё.'
compose stop replica
insert $'3,gamma\n' >/dev/null
all=$'1\talpha\n2\tbeta\n3\tgamma'
assert_rows leader "$all"
compose up -d --no-build --wait --wait-timeout 90 replica
await_replica "$all"

echo '4. Останавливаем лидера: реплика продолжает читать свою копию.'
compose stop leader
assert_rows replica "$all"

echo '5. Перезапускаем реплику при выключенном лидере: данные сохранены.'
compose restart replica
compose up -d --no-build --wait --wait-timeout 90 replica
assert_rows replica "$all"

echo '6. Возвращаем лидера и проверяем обе копии.'
compose up -d --no-build --wait --wait-timeout 90 leader
assert_rows leader "$all"
await_replica "$all"
echo "Демо пройдено. Оба узла работают; таблица $table оставлена для проверки."
