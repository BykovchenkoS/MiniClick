#!/usr/bin/env bash
# Полное SQL-демо: Query, постоянный Catalog, HTTP и MergeTree.
set -euo pipefail

project_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$project_dir"
rows=${ROWS:-1000000}
extra_rows=${EXTRA_ROWS:-1000}
address=${MINICLICK_ADDR:-127.0.0.1:18123}
base_url="http://$address"
for count in "$rows" "$extra_rows"; do
    if [[ ! $count =~ ^[0-9]+$ ]] || (( count < 1 )); then
        echo 'ROWS и EXTRA_ROWS должны быть положительными целыми числами.' >&2
        exit 1
    fi
done
for command in curl awk sort cmp wc mktemp sed tee; do
    command -v "$command" >/dev/null || { echo "Нет команды: $command" >&2; exit 1; }
done
if curl --silent --fail --max-time 1 "$base_url/ping" >/dev/null; then
    echo "По адресу $address уже работает сервер; выберите другой MINICLICK_ADDR." >&2
    exit 1
fi

cargo build --locked --release --bin miniclick --example generate_data
target_dir=$(cargo metadata --no-deps --format-version 1 | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')
binary="$target_dir/release/miniclick"
generator="$target_dir/release/examples/generate_data"
run_dir=$(mktemp -d "${TMPDIR:-/tmp}/miniclick-demo.XXXXXXXX")
server_pid=''
stop_server() {
    if [[ -n $server_pid ]]; then
        kill -TERM "$server_pid" 2>/dev/null || true
        wait "$server_pid" || true
        server_pid=''
    fi
}
trap stop_server EXIT
start_server() {
    (
        cd "$run_dir"
        export RUST_LOG=info
        exec "$binary" --listen-addr "$address" --data-dir "$run_dir/data"
    ) >>"$run_dir/server.log" 2>&1 &
    server_pid=$!
    for ((attempt=0; attempt<150; attempt++)); do
        if ! kill -0 "$server_pid" 2>/dev/null; then
            cat "$run_dir/server.log" >&2
            echo 'Сервер завершился до готовности.' >&2
            exit 1
        fi
        if curl --silent --fail --max-time 1 "$base_url/ping" >/dev/null; then return; fi
        sleep 0.1
    done
    echo 'Истекло время ожидания сервера.' >&2
    exit 1
}
sql() {
    curl --silent --show-error --fail -H 'Content-Type: text/plain' --data-binary "$1" "$base_url/query?format=TSV"
}
timed_sql() {
    local label=$1 query=$2 file=$3 before elapsed
    before=$(wc -l <"$run_dir/server.log")
    elapsed=$(curl --silent --show-error --fail -H 'Content-Type: text/plain' --data-binary "$query" \
        --output "$file" --write-out '%{time_total}' "$base_url/query?format=TSV")
    printf '%s\t%s\n' "$label" "$elapsed" | tee -a "$run_dir/timings.tsv"
    awk -v start="$before" 'NR > start && /Выбраны parts и гранулы/ {print}' "$run_dir/server.log"
}
describe() {
    curl --silent --show-error --fail "$base_url/tables/hits"
}
stat() {
    describe | sed -n "s/.*\"$1\":\([0-9]*\).*/\1/p"
}
plan_stats() {
    awk '/parts: / && /granules: / && /rows: / {
        gsub(",", "")
        printf "%s\t%s\t%s\t%s\t%s\t%s\n", $2, $4, $6, $8, $10, $12
        found++
    } END { if (found != 1) exit 1 }' "$1"
}
global_query='SELECT count() AS n, sum(delta) AS total, min(event_date) AS first_day, max(event_date) AS last_day FROM hits'
groups_query='SELECT event_date, count() AS n, sum(delta) AS total FROM hits WHERE delta >= 0 GROUP BY event_date ORDER BY n DESC, event_date ASC LIMIT 30'
summaries() {
    local prefix=$1
    sql "$global_query" >"$run_dir/$prefix-global.tsv"
    sql "$groups_query" >"$run_dir/$prefix-groups.tsv"
}

echo "Артефакты демо: $run_dir"
printf 'operation\tseconds\n' >"$run_dir/timings.tsv"
start_server
echo "Swagger: $base_url/swagger-ui"
sql 'CREATE TABLE hits (event_date Date, user_id UInt64, delta Int64, amount Float64, url String, event_time DateTime, event_date_text String) ENGINE = MergeTree ORDER BY (event_date, user_id)' >/dev/null

for ((batch=0; batch<3; batch++)); do
    if (( batch == 0 )); then count=$rows; offset=0; else count=$extra_rows; offset=$((rows + (batch-1)*extra_rows)); fi
    "$generator" --rows "$count" --start-row "$offset" --output "$run_dir/batch-$batch.csv" 2>"$run_dir/schema.sql"
    elapsed=$(curl --silent --show-error --fail -H 'Content-Type: application/octet-stream' \
        --data-binary "@$run_dir/batch-$batch.csv" --output "$run_dir/insert-$batch.txt" --write-out '%{time_total}' "$base_url/insert/hits?format=CSV")
    printf 'insert-%s\t%s\n' "$batch" "$elapsed" | tee -a "$run_dir/timings.tsv"
done
expected_rows=$((rows + 2*extra_rows))
[[ $(stat rows_total) == "$expected_rows" ]]
[[ $(stat parts_total) == 3 ]]
sql 'SELECT user_id, url FROM hits WHERE event_date = '\''2026-10-01'\'' LIMIT 5'

indexed="SELECT user_id, delta FROM hits WHERE event_date = '2026-10-01'"
full="SELECT user_id, delta FROM hits WHERE event_date_text = '2026-10-01'"
sql "EXPLAIN $indexed" >"$run_dir/indexed-explain.tsv"
sql "EXPLAIN $full" >"$run_dir/full-explain.tsv"
indexed_stats=$(plan_stats "$run_dir/indexed-explain.tsv")
full_stats=$(plan_stats "$run_dir/full-explain.tsv")
printf 'query\tparts_selected\tparts_total\tgranules_selected\tgranules_total\trows_selected\trows_total\n' >"$run_dir/explain-stats.tsv"
printf 'indexed\t%s\nfull-scan\t%s\n' "$indexed_stats" "$full_stats" | tee -a "$run_dir/explain-stats.tsv"
IFS=$'\t' read -r indexed_parts indexed_parts_total indexed_granules indexed_granules_total indexed_rows indexed_rows_total <<<"$indexed_stats"
IFS=$'\t' read -r full_parts full_parts_total full_granules full_granules_total full_rows full_rows_total <<<"$full_stats"
[[ $indexed_parts -le $indexed_parts_total && $full_parts -eq $full_parts_total ]]
[[ $indexed_granules_total -eq $full_granules_total && $full_granules -eq $full_granules_total ]]
[[ $indexed_rows_total -eq $expected_rows && $full_rows_total -eq $expected_rows && $full_rows -eq $expected_rows ]]
[[ $indexed_rows -le $full_rows && $indexed_granules -le $full_granules ]]
if (( rows >= 8192 )); then [[ $indexed_granules -lt $full_granules ]]; fi
summaries before
# Генератор выдаёт последовательный диапазон: сумма полного цикла delta равна нулю.
remainder=$((expected_rows % 201))
expected_sum=$((remainder * (remainder - 201) / 2))
last_day=$((expected_rows < 30 ? expected_rows : 30))
[[ $(cat "$run_dir/before-global.tsv") == "$(printf '%s\t%s\t2026-10-01\t2026-10-%02d' "$expected_rows" "$expected_sum" "$last_day")" ]]
positive_remainder=$((remainder > 100 ? remainder - 100 : 0))
expected_positive_count=$((expected_rows / 201 * 101 + positive_remainder))
expected_positive_sum=$((expected_rows / 201 * 5050 + positive_remainder * (positive_remainder - 1) / 2))
group_totals=$(awk -F '\t' '{n += $2; total += $3} END {printf "%.0f\t%.0f", n, total}' "$run_dir/before-groups.tsv")
[[ $group_totals == "$(printf '%s\t%s' "$expected_positive_count" "$expected_positive_sum")" ]]
echo 'Глобальные агрегаты:'
cat "$run_dir/before-global.tsv"
echo 'WHERE → GROUP BY → ORDER BY → LIMIT:'
cat "$run_dir/before-groups.tsv"
# Оба запроса прогреваются; повторные замеры имеют одинаковую проекцию и результат.
sql "$indexed" >/dev/null
sql "$full" >/dev/null
for ((iteration=1; iteration<=3; iteration++)); do
    timed_sql "indexed-$iteration" "$indexed" "$run_dir/indexed.tsv"
    timed_sql "full-scan-$iteration" "$full" "$run_dir/full.tsv"
done
LC_ALL=C sort "$run_dir/indexed.tsv" >"$run_dir/indexed.sorted.tsv"
LC_ALL=C sort "$run_dir/full.tsv" >"$run_dir/full.sorted.tsv"
cmp "$run_dir/indexed.sorted.tsv" "$run_dir/full.sorted.tsv"
sql 'SELECT * FROM hits' | LC_ALL=C sort >"$run_dir/before.tsv"
[[ $(wc -l <"$run_dir/before.tsv") -eq $expected_rows ]]
stop_server
start_server
sql 'SELECT * FROM hits' | LC_ALL=C sort >"$run_dir/restarted.tsv"
cmp "$run_dir/before.tsv" "$run_dir/restarted.tsv"
summaries restarted
cmp "$run_dir/before-global.tsv" "$run_dir/restarted-global.tsv"
cmp "$run_dir/before-groups.tsv" "$run_dir/restarted-groups.tsv"
timed_sql optimize 'OPTIMIZE TABLE hits' "$run_dir/optimize.txt"
[[ $(stat parts_total) == 1 ]]
[[ $(stat rows_total) == "$expected_rows" ]]
stop_server
start_server
sql 'SELECT * FROM hits' | LC_ALL=C sort >"$run_dir/optimized.tsv"
cmp "$run_dir/before.tsv" "$run_dir/optimized.tsv"
summaries optimized
cmp "$run_dir/before-global.tsv" "$run_dir/optimized-global.tsv"
cmp "$run_dir/before-groups.tsv" "$run_dir/optimized-groups.tsv"
sql "EXPLAIN $indexed" >"$run_dir/optimized-explain.tsv"
sql "EXPLAIN $groups_query" >"$run_dir/grouped-explain.tsv"
describe | tee "$run_dir/description.json"
printf '\n'
sql 'DROP TABLE hits' >/dev/null
stop_server
echo "MiniClick проверен: $expected_rows строк, 3 → 1 part, два рестарта, без потерь и дублей."
echo 'Агрегаты, группировка, сортировка и EXPLAIN проверены через HTTP.'
echo "Время и статистика гранул: $run_dir/timings.tsv, $run_dir/explain-stats.tsv"
