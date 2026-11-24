"""Сквозные проверки отказов шардов и координаторов на чистых томах."""

import argparse
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
BACKUP = "coordinator-backup"
PROXY = "coordinator-proxy"
EVEN = ("shard-even-1", "shard-even-2", "shard-even-3")
ODD = ("shard-odd", "shard-odd-2", "shard-odd-3")
PORT_NAMES = (
    "COORDINATOR", "COORDINATOR_BACKUP", "PROXY",
    "EVEN_1", "EVEN_2", "EVEN_3", "ODD_1", "ODD_2", "ODD_3",
)
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


def run_demo(compose, coordinator_failover):
    def published_port(service):
        address = compose("port", service, "8123", capture=True).strip()
        return int(address.rsplit(":", 1)[1])

    compose("up", "-d", "--build", "--wait")
    ports = {
        service: published_port(service)
        for service in (COORDINATOR, BACKUP, PROXY, *EVEN, *ODD)
    }

    def request(service, path, body=None, *, include_backend=False):
        if isinstance(body, str):
            body = body.encode("utf-8")
        url = f"http://127.0.0.1:{ports[service]}{path}"
        try:
            response = urlopen(Request(url, data=body), timeout=10)
        except HTTPError as error:
            response = error
        with response:
            result = response.status, response.read().decode("utf-8")
            if include_backend:
                return (*result, response.headers.get("X-MiniClick-Coordinator"))
            return result

    def leader(group, excluded=None):
        for service in group:
            if service == excluded:
                continue
            try:
                code, body = request(service, "/cluster/status")
            except (URLError, TimeoutError):
                continue
            if code == 200:
                status = json.loads(body)
                if status["healthy"] and status["role"] == "Leader" and status["id"] == status["leader"]:
                    return service
        return None

    route = PROXY if coordinator_failover else COORDINATOR

    def sql(query):
        return request(route, "/query", query)

    def insert(body):
        return request(route, "/insert/hits?format=CSV", body)

    for group in (EVEN, ODD):
        expect(request(group[0], "/cluster/init", b""), (200, "Ok.\n"), "Инициализация Raft")
        elected = wait_for(lambda: leader(group), "избрание лидера")
        expect(request(elected, "/query", DDL), (200, "Ok.\n"), "Создание таблицы")
    for suffix in ("even", "odd"):
        data = (ROOT / "deploy" / f"sharding-{suffix}.csv").read_bytes()
        expect(insert(data), (200, "Ok.\n"), f"Загрузка CSV {suffix}")
    expect(sql(FULL_QUERY), (200, INITIAL_ROWS), "Данные перед отказом")
    print("Чистый стенд: загружены и проверены 6 строк.", flush=True)

    if coordinator_failover:
        verify_coordinator_failover(compose, request, leader)
        return

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


def verify_coordinator_failover(compose, request, leader):
    def proxy(path, body=None):
        return request(PROXY, path, body, include_backend=True)

    def wait_for_coordinator(service):
        wait_for(lambda: proxy("/ping") == (200, "Ok.\n", service), f"HAProxy -> {service}")

    wait_for_coordinator(COORDINATOR)
    expect(request(BACKUP, "/query", FULL_QUERY), (200, INITIAL_ROWS), "Начальные данные на резервном")
    compose("kill", COORDINATOR)
    wait_for_coordinator(BACKUP)
    expect(proxy("/query", FULL_QUERY), (200, INITIAL_ROWS, BACKUP), "SELECT после отказа основного")

    old_leaders = [wait_for(lambda: leader(group), "действующий лидер") for group in (EVEN, ODD)]
    compose("kill", *old_leaders)
    for group, old in zip((EVEN, ODD), old_leaders):
        elected = wait_for(lambda: leader(group, excluded=old), f"смена лидера {old}")
        print(f"Raft: {old} -> {elected}", flush=True)

    wait_for(
        lambda: proxy("/query", FULL_QUERY) == (200, INITIAL_ROWS, BACKUP),
        "SELECT через новых лидеров",
    )
    insertion = "/insert/hits?format=CSV"
    expect(proxy(insertion, "7,shop,70\n8,news,80\n"), (200, "Ok.\n", BACKUP), "INSERT через резервный")
    expected = INITIAL_ROWS + "7\tshop\t70\n8\tnews\t80\n"
    expect(proxy("/query", FULL_QUERY), (200, expected, BACKUP), "Запись видна через резервный")
    print("Основной остановлен: резервный обслуживает SELECT и INSERT через новых лидеров.", flush=True)

    compose("start", COORDINATOR)
    wait_for_coordinator(COORDINATOR)
    expect(proxy("/query", FULL_QUERY), (200, expected, COORDINATOR), "Основной видит запись резервного")
    compose("kill", BACKUP)
    expect(proxy(insertion, "9,blog,90\n10,shop,100\n"), (200, "Ok.\n", COORDINATOR), "INSERT через основной")
    expected += "9\tblog\t90\n10\tshop\t100\n"
    expect(proxy("/query", FULL_QUERY), (200, expected, COORDINATOR), "Точный набор строк после отказов")
    print("После возврата основного и остановки резервного проверены все 10 строк.", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coordinator-failover", action="store_true", help="проверить отказ координаторов")
    args = parser.parse_args()
    scenario = "failover" if args.coordinator_failover else "quorum"
    project = f"miniclick-{scenario}-{uuid.uuid4().hex[:12]}"
    env = {**os.environ, **{f"MINICLICK_RAFT_{name}_PORT": "0" for name in PORT_NAMES}}

    def compose(*args, capture=False, check=True):
        result = subprocess.run(
            ["docker", "compose", "-p", project, "-f", "compose.sharding-raft.yaml", *args],
            cwd=ROOT, env=env, check=check, text=True, stdout=subprocess.PIPE if capture else None,
        )
        return result.stdout

    print(f"Временный проект Compose: {project}", flush=True)
    try:
        run_demo(compose, args.coordinator_failover)
    except Exception:
        compose("logs", "--tail", "20", check=False)
        raise
    finally:
        # Удаляются только временные тома с демоданными этого запуска.
        compose("down", "--volumes")


if __name__ == "__main__":
    main()
