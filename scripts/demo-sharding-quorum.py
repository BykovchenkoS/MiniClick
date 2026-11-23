"""Сквозная проверка потери и восстановления кворума на чистых томах."""

import json
import os
from pathlib import Path
import subprocess
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import uuid


ROOT = Path(__file__).resolve().parents[1]
COORDINATOR = "coordinator"
EVEN = ("shard-even-1", "shard-even-2", "shard-even-3")
ODD = ("shard-odd", "shard-odd-2", "shard-odd-3")
PORT_NAMES = ("COORDINATOR", "EVEN_1", "EVEN_2", "EVEN_3", "ODD_1", "ODD_2", "ODD_3")
DDL = (
    "CREATE TABLE hits_local (user_id UInt64, category String, amount Int64) "
    "ENGINE = MergeTree ORDER BY user_id SETTINGS index_granularity = 1"
)
FULL_QUERY = "SELECT user_id, category, amount FROM hits ORDER BY user_id"
INITIAL_ROWS = "1\tnews\t40\n2\tnews\t20\n3\tblog\t55\n4\tshop\t50\n5\tblog\t1\n6\tnews\t1000\n"


def expect(actual, expected, label):
    if actual != expected:
        raise AssertionError(f"{label}: ожидалось {expected!r}, получено {actual!r}")


def wait_for(action, label, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = action()
            if result:
                return result
        except (URLError, TimeoutError):
            pass
        time.sleep(0.25)
    raise RuntimeError(f"Истекло время ожидания: {label}")


def run_demo(compose):
    def published_port(service):
        address = compose("port", service, "8123", capture=True).strip()
        return int(address.rsplit(":", 1)[1])

    compose("up", "-d", "--build", "--wait")
    ports = {service: published_port(service) for service in (COORDINATOR, *EVEN, *ODD)}

    def request(service, path, body=None):
        if isinstance(body, str):
            body = body.encode("utf-8")
        url = f"http://127.0.0.1:{ports[service]}{path}"
        try:
            with urlopen(Request(url, data=body), timeout=10) as response:
                return response.status, response.read().decode("utf-8")
        except HTTPError as error:
            return error.code, error.read().decode("utf-8")

    def leader(group):
        for service in group:
            code, body = request(service, "/cluster/status")
            if code == 200:
                status = json.loads(body)
                if status["healthy"] and status["role"] == "Leader" and status["id"] == status["leader"]:
                    return service
        return None

    def sql(query):
        return request(COORDINATOR, "/query", query)

    def insert(body):
        return request(COORDINATOR, "/insert/hits?format=CSV", body)

    for group in (EVEN, ODD):
        expect(request(group[0], "/cluster/init", b""), (200, "Ok.\n"), "Инициализация Raft")
        elected = wait_for(lambda: leader(group), "избрание лидера")
        expect(request(elected, "/query", DDL), (200, "Ok.\n"), "Создание таблицы")
    for suffix in ("even", "odd"):
        data = (ROOT / "deploy" / f"sharding-{suffix}.csv").read_bytes()
        expect(insert(data), (200, "Ok.\n"), f"Загрузка CSV {suffix}")
    expect(sql(FULL_QUERY), (200, INITIAL_ROWS), "Данные перед отказом")
    print("Чистый стенд: загружены и проверены 6 строк.", flush=True)

    odd_leader = wait_for(lambda: leader(ODD), "лидер нечётного шарда")
    stopped = tuple(service for service in ODD if service != odd_leader)
    compose("stop", *stopped)
    expect(request(odd_leader, "/ping"), (200, "Ok.\n"), "Оставшийся участник")

    expect(sql("SELECT amount FROM hits WHERE user_id = 4"), (200, "50\n"), "Точечный SELECT")
    code, plan = sql("EXPLAIN SELECT amount FROM hits WHERE user_id = 4")
    expect(code, 200, "EXPLAIN")
    expect(
        [line.strip() for line in plan.splitlines() if line.strip().startswith("shards:")],
        ["shards: [1]"],
        "Выбранные шарды",
    )
    for query in (FULL_QUERY, "SELECT user_id FROM hits LIMIT 1"):
        code, body = sql(query)
        expect(code, 500, "SELECT к обоим шардам")
        error = json.loads(body)
        if set(error) != {"error"} or "shard 2" not in error["error"]:
            raise AssertionError(f"Ожидалась ошибка нечётного шарда без строк: {body}")
    expect(insert("8,shop,70\n"), (200, "Ok.\n"), "INSERT на чётный шард")
    code, body = insert("7,shop,71\n9,shop,90\n")
    expect(code, 502, "INSERT на нечётный шард")
    if "shard 2" not in json.loads(body)["error"]:
        raise AssertionError(f"Ожидалась ошибка нечётного шарда: {body}")
    print("Без кворума: SELECT и INSERT на чётном шарде работают, запросы к нечётному отклонены.", flush=True)

    expected = (200, INITIAL_ROWS + "8\tshop\t70\n")
    compose("start", *stopped)
    wait_for(lambda: sql(FULL_QUERY) == expected, "восстановление точного набора строк")
    compose("restart", COORDINATOR)
    # Docker может назначить другой динамический порт после рестарта.
    ports[COORDINATOR] = published_port(COORDINATOR)
    wait_for(lambda: sql(FULL_QUERY) == expected, "чтение после рестарта координатора")
    print("После восстановления и рестарта: 7 строк, включая подтверждённый ключ 8; ключей 7 и 9 нет.", flush=True)


def main():
    project = f"miniclick-quorum-{uuid.uuid4().hex[:12]}"
    env = {**os.environ, **{f"MINICLICK_RAFT_{name}_PORT": "0" for name in PORT_NAMES}}

    def compose(*args, capture=False, check=True):
        result = subprocess.run(
            ["docker", "compose", "-p", project, "-f", "compose.sharding-raft.yaml", *args],
            cwd=ROOT, env=env, check=check, text=True, stdout=subprocess.PIPE if capture else None,
        )
        return result.stdout

    print(f"Временный проект Compose: {project}", flush=True)
    try:
        run_demo(compose)
    except Exception:
        compose("logs", "--tail", "20", check=False)
        raise
    finally:
        # Удаляются только временные тома с демоданными этого запуска.
        compose("down", "--volumes")


if __name__ == "__main__":
    main()
