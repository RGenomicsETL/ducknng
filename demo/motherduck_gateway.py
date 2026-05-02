#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

import duckdb


BACKEND_USERS = ("alice", "bob")
BACKEND_PREFIX = "subscriber_"
GATEWAY_NAME = "gateway"


def sql_quote(text: str) -> str:
    return text.replace("'", "''")


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def http_request(url: str, method: str, body: bytes | None = None,
    headers: dict[str, str] | None = None) -> tuple[int, dict[str, str], bytes]:
    req = urllib.request.Request(url, data=body, method=method)
    for name, value in (headers or {}).items():
        req.add_header(name, value)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, dict(resp.headers.items()), resp.read()
    except urllib.error.HTTPError as err:
        return err.code, dict(err.headers.items()), err.read()


def wait_healthz(base_url: str, timeout_s: float) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            status, _, body = http_request(base_url + "/healthz", "GET")
            if status == 200 and body == b"ok":
                return
        except OSError:
            pass
        time.sleep(0.1)
    raise RuntimeError("gateway health check did not become ready")


def start_worker(script: pathlib.Path, ready_path: pathlib.Path,
    args: list[str]) -> subprocess.Popen[str]:
    proc = subprocess.Popen(
        [sys.executable, str(script), *args, "--ready-file", str(ready_path)],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    deadline = time.time() + 10
    while time.time() < deadline:
        if ready_path.exists():
            text = ready_path.read_text(encoding="utf-8").strip()
            if text == "ready":
                return proc
            raise RuntimeError(f"worker failed during startup: {text}")
        if proc.poll() is not None:
            out, err = proc.communicate(timeout=1)
            raise RuntimeError(
                f"worker exited during startup with code {proc.returncode}\nstdout:\n{out}\nstderr:\n{err}"
            )
        time.sleep(0.1)
    raise RuntimeError("timed out waiting for worker startup")


def stop_worker(name: str, proc: subprocess.Popen[str]) -> None:
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate(timeout=5)
    if proc.returncode not in (0, -signal.SIGTERM):
        out = proc.stdout.read() if proc.stdout else ""
        err = proc.stderr.read() if proc.stderr else ""
        raise RuntimeError(
            f"{name} exited with code {proc.returncode}\nstdout:\n{out}\nstderr:\n{err}"
        )


def decode_arrow_rows(ext_path: pathlib.Path, body: bytes) -> tuple[list[str], list[tuple[object, ...]]]:
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    try:
        con.execute(f"LOAD '{sql_quote(str(ext_path))}'")
        cur = con.execute(
            "SELECT * FROM ducknng_parse_body(?::BLOB, 'application/vnd.apache.arrow.stream')",
            [body],
        )
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        return columns, rows
    finally:
        con.close()


def post_json(base_url: str, path: str, payload: dict[str, object]) -> tuple[int, dict[str, str], bytes]:
    return http_request(
        base_url + path,
        "POST",
        json.dumps(payload).encode("utf-8"),
        {"Content-Type": "application/json"},
    )


def collect_query(base_url: str, ext_path: pathlib.Path, user_name: str,
    sql: str, batch_rows: int = 1) -> tuple[list[str], list[tuple[object, ...]]]:
    status, headers, body = post_json(
        base_url,
        "/v1/query/start",
        {"user": user_name, "sql": sql, "batch_rows": batch_rows},
    )
    if status != 200:
        raise RuntimeError(f"unexpected start status {status}: {body!r}")
    if headers.get("X-Ducknng-Subscriber") != user_name:
        raise RuntimeError(f"gateway routed query for {user_name!r} to {headers.get('X-Ducknng-Subscriber')!r}")
    columns, rows = decode_arrow_rows(ext_path, body)
    token = headers.get("X-Ducknng-Next-Token")
    all_rows = list(rows)
    while token:
        status, headers, body = post_json(base_url, "/v1/query/fetch", {"token": token})
        if status == 204:
            token = None
            break
        if status != 200:
            raise RuntimeError(f"unexpected fetch status {status}: {body!r}")
        if headers.get("X-Ducknng-Subscriber") != user_name:
            raise RuntimeError(f"gateway continued query for {user_name!r} on {headers.get('X-Ducknng-Subscriber')!r}")
        token = headers.get("X-Ducknng-Next-Token")
        _, rows = decode_arrow_rows(ext_path, body)
        all_rows.extend(rows)
    return columns, all_rows


def build_gateway_route_sqls(backend_urls: dict[str, str]) -> tuple[str, str, str]:
    subscriber_case = "\n      ".join(
        f"WHEN '{user_name}' THEN '{backend_urls[user_name]}'"
        for user_name in BACKEND_USERS
    )
    start_sql = f"""
WITH req AS (
  SELECT
    json_extract_string(body_text::JSON, '$.user') AS user_name,
    json_extract_string(body_text::JSON, '$.sql') AS sql,
    coalesce(TRY_CAST(json_extract_string(body_text::JSON, '$.batch_rows') AS UBIGINT), 0::UBIGINT) AS batch_rows,
    coalesce(TRY_CAST(json_extract_string(body_text::JSON, '$.batch_bytes') AS UBIGINT), 0::UBIGINT) AS batch_bytes
  FROM ducknng_http_request_body()
),
target AS (
  SELECT
    user_name,
    sql,
    batch_rows,
    batch_bytes,
    CASE user_name
      {subscriber_case}
      ELSE NULL
    END AS backend_url
  FROM req
),
opened AS (
  SELECT
    user_name,
    backend_url,
    batch_rows,
    batch_bytes,
    ducknng_open_query_raw(backend_url, sql, batch_rows, batch_bytes, 0::UBIGINT) AS open_frame
  FROM target
  WHERE backend_url IS NOT NULL AND sql IS NOT NULL
),
open_meta AS (
  SELECT
    user_name,
    backend_url,
    batch_rows,
    batch_bytes,
    ducknng_frame_error_text(open_frame) AS open_error,
    ducknng_frame_payload_text(open_frame) AS open_control_json
  FROM opened
),
fetch_inputs AS (
  SELECT
    user_name,
    backend_url,
    batch_rows,
    batch_bytes,
    json_extract(open_control_json::JSON, '$.session_id')::UBIGINT AS session_id,
    json_extract_string(open_control_json::JSON, '$.session_token') AS session_token
  FROM open_meta
  WHERE open_error IS NULL AND open_control_json IS NOT NULL
),
fetched AS (
  SELECT
    user_name,
    backend_url,
    batch_rows,
    batch_bytes,
    session_id,
    session_token,
    ducknng_fetch_query_raw(backend_url, session_id, session_token, batch_rows, batch_bytes, 0::UBIGINT) AS fetch_frame
  FROM fetch_inputs
),
fetch_meta AS (
  SELECT
    user_name,
    backend_url,
    batch_rows,
    batch_bytes,
    session_id,
    session_token,
    ducknng_frame_error_text(fetch_frame) AS fetch_error,
    ducknng_frame_payload(fetch_frame) AS fetch_payload,
    ducknng_frame_payload_text(fetch_frame) AS fetch_control_json
  FROM fetched
),
token AS (
  SELECT
    hex(CAST(CAST(
      to_json(
        struct_pack(
          user_name := user_name,
          backend_url := backend_url,
          session_id := session_id,
          session_token := session_token,
          batch_rows := batch_rows,
          batch_bytes := batch_bytes
        )
      ) AS VARCHAR
    ) AS BLOB)) AS token
  FROM fetch_meta
  WHERE fetch_error IS NULL AND fetch_payload IS NOT NULL AND fetch_control_json IS NULL
),
closed AS (
  SELECT
    ducknng_close_query_raw(backend_url, session_id, session_token, 0::UBIGINT) AS close_frame
  FROM fetch_meta
  WHERE fetch_error IS NULL AND fetch_control_json IS NOT NULL
)
SELECT
  CASE
    WHEN EXISTS (SELECT 1 FROM req WHERE user_name IS NULL OR sql IS NULL) THEN 400
    WHEN EXISTS (SELECT 1 FROM target WHERE backend_url IS NULL) THEN 404
    WHEN EXISTS (SELECT 1 FROM open_meta WHERE open_error IS NOT NULL) THEN 400
    WHEN EXISTS (SELECT 1 FROM fetch_meta WHERE fetch_error IS NOT NULL) THEN 400
    WHEN EXISTS (SELECT 1 FROM fetch_meta WHERE fetch_control_json IS NOT NULL) THEN 204
    ELSE 200
  END AS status,
  CASE
    WHEN EXISTS (SELECT 1 FROM req WHERE user_name IS NULL OR sql IS NULL)
      OR EXISTS (SELECT 1 FROM target WHERE backend_url IS NULL)
      OR EXISTS (SELECT 1 FROM open_meta WHERE open_error IS NOT NULL)
      OR EXISTS (SELECT 1 FROM fetch_meta WHERE fetch_error IS NOT NULL)
      THEN 'application/json; charset=utf-8'
    WHEN EXISTS (SELECT 1 FROM fetch_meta WHERE fetch_control_json IS NOT NULL)
      THEN NULL
    ELSE 'application/vnd.apache.arrow.stream'
  END AS content_type,
  CASE
    WHEN EXISTS (SELECT 1 FROM fetch_meta WHERE fetch_control_json IS NOT NULL)
      THEN CAST(
        to_json(
          list_value(
            struct_pack(name := 'X-Ducknng-End-Of-Stream', value := 'true'),
            struct_pack(name := 'X-Ducknng-Subscriber', value := (SELECT user_name FROM target LIMIT 1))
          )
        ) AS VARCHAR
      )
    WHEN EXISTS (SELECT 1 FROM token)
      THEN CAST(
        to_json(
          list_value(
            struct_pack(name := 'X-Ducknng-End-Of-Stream', value := 'false'),
            struct_pack(name := 'X-Ducknng-Next-Token', value := (SELECT token FROM token)),
            struct_pack(name := 'X-Ducknng-Subscriber', value := (SELECT user_name FROM target LIMIT 1))
          )
        ) AS VARCHAR
      )
    ELSE NULL
  END AS headers_json,
  CASE
    WHEN EXISTS (SELECT 1 FROM req WHERE user_name IS NULL OR sql IS NULL)
      THEN CAST(to_json(struct_pack(error := 'user and sql are required')) AS VARCHAR)
    WHEN EXISTS (SELECT 1 FROM target WHERE backend_url IS NULL)
      THEN CAST(to_json(struct_pack(error := 'unknown subscriber')) AS VARCHAR)
    WHEN EXISTS (SELECT 1 FROM open_meta WHERE open_error IS NOT NULL)
      THEN CAST(to_json(struct_pack(error := (SELECT open_error FROM open_meta WHERE open_error IS NOT NULL LIMIT 1))) AS VARCHAR)
    WHEN EXISTS (SELECT 1 FROM fetch_meta WHERE fetch_error IS NOT NULL)
      THEN CAST(to_json(struct_pack(error := (SELECT fetch_error FROM fetch_meta WHERE fetch_error IS NOT NULL LIMIT 1))) AS VARCHAR)
    ELSE NULL
  END AS body_text,
  CASE
    WHEN EXISTS (SELECT 1 FROM fetch_meta WHERE fetch_error IS NULL AND fetch_payload IS NOT NULL AND fetch_control_json IS NULL)
      THEN (SELECT fetch_payload FROM fetch_meta LIMIT 1)
    ELSE NULL
  END AS body
FROM (SELECT 1) AS keepalive
LEFT JOIN closed ON TRUE
"""
    fetch_sql = """
WITH req AS (
  SELECT json_extract_string(body_text::JSON, '$.token') AS token_hex
  FROM ducknng_http_request_body()
),
tok AS (
  SELECT
    json_extract_string(decode(from_hex(token_hex))::JSON, '$.user_name') AS user_name,
    json_extract_string(decode(from_hex(token_hex))::JSON, '$.backend_url') AS backend_url,
    TRY_CAST(json_extract_string(decode(from_hex(token_hex))::JSON, '$.session_id') AS UBIGINT) AS session_id,
    json_extract_string(decode(from_hex(token_hex))::JSON, '$.session_token') AS session_token,
    coalesce(TRY_CAST(json_extract_string(decode(from_hex(token_hex))::JSON, '$.batch_rows') AS UBIGINT), 0::UBIGINT) AS batch_rows,
    coalesce(TRY_CAST(json_extract_string(decode(from_hex(token_hex))::JSON, '$.batch_bytes') AS UBIGINT), 0::UBIGINT) AS batch_bytes
  FROM req
  WHERE token_hex IS NOT NULL
),
fetched AS (
  SELECT
    user_name,
    backend_url,
    session_id,
    session_token,
    batch_rows,
    batch_bytes,
    ducknng_fetch_query_raw(backend_url, session_id, session_token, batch_rows, batch_bytes, 0::UBIGINT) AS fetch_frame
  FROM tok
  WHERE backend_url IS NOT NULL AND session_id > 0 AND session_token IS NOT NULL
),
fetch_meta AS (
  SELECT
    user_name,
    backend_url,
    session_id,
    session_token,
    batch_rows,
    batch_bytes,
    ducknng_frame_error_text(fetch_frame) AS fetch_error,
    ducknng_frame_payload(fetch_frame) AS fetch_payload,
    ducknng_frame_payload_text(fetch_frame) AS fetch_control_json
  FROM fetched
),
token AS (
  SELECT
    hex(CAST(CAST(
      to_json(
        struct_pack(
          user_name := user_name,
          backend_url := backend_url,
          session_id := session_id,
          session_token := session_token,
          batch_rows := batch_rows,
          batch_bytes := batch_bytes
        )
      ) AS VARCHAR
    ) AS BLOB)) AS token
  FROM fetch_meta
  WHERE fetch_error IS NULL AND fetch_payload IS NOT NULL AND fetch_control_json IS NULL
),
closed AS (
  SELECT
    ducknng_close_query_raw(backend_url, session_id, session_token, 0::UBIGINT) AS close_frame
  FROM fetch_meta
  WHERE fetch_error IS NULL AND fetch_control_json IS NOT NULL
)
SELECT
  CASE
    WHEN EXISTS (SELECT 1 FROM req WHERE token_hex IS NULL) THEN 400
    WHEN EXISTS (SELECT 1 FROM fetch_meta WHERE fetch_error IS NOT NULL) THEN 400
    WHEN EXISTS (SELECT 1 FROM fetch_meta WHERE fetch_control_json IS NOT NULL) THEN 204
    WHEN EXISTS (SELECT 1 FROM token) THEN 200
    ELSE 400
  END AS status,
  CASE
    WHEN EXISTS (SELECT 1 FROM req WHERE token_hex IS NULL)
      OR EXISTS (SELECT 1 FROM fetch_meta WHERE fetch_error IS NOT NULL)
      OR NOT EXISTS (SELECT 1 FROM fetch_meta)
      THEN 'application/json; charset=utf-8'
    WHEN EXISTS (SELECT 1 FROM fetch_meta WHERE fetch_control_json IS NOT NULL)
      THEN NULL
    ELSE 'application/vnd.apache.arrow.stream'
  END AS content_type,
  CASE
    WHEN EXISTS (SELECT 1 FROM fetch_meta WHERE fetch_control_json IS NOT NULL)
      THEN CAST(
        to_json(
          list_value(
            struct_pack(name := 'X-Ducknng-End-Of-Stream', value := 'true'),
            struct_pack(name := 'X-Ducknng-Subscriber', value := (SELECT user_name FROM tok LIMIT 1))
          )
        ) AS VARCHAR
      )
    WHEN EXISTS (SELECT 1 FROM token)
      THEN CAST(
        to_json(
          list_value(
            struct_pack(name := 'X-Ducknng-End-Of-Stream', value := 'false'),
            struct_pack(name := 'X-Ducknng-Next-Token', value := (SELECT token FROM token)),
            struct_pack(name := 'X-Ducknng-Subscriber', value := (SELECT user_name FROM tok LIMIT 1))
          )
        ) AS VARCHAR
      )
    ELSE NULL
  END AS headers_json,
  CASE
    WHEN EXISTS (SELECT 1 FROM req WHERE token_hex IS NULL)
      THEN CAST(to_json(struct_pack(error := 'token is required')) AS VARCHAR)
    WHEN EXISTS (SELECT 1 FROM fetch_meta WHERE fetch_error IS NOT NULL)
      THEN CAST(to_json(struct_pack(error := (SELECT fetch_error FROM fetch_meta WHERE fetch_error IS NOT NULL LIMIT 1))) AS VARCHAR)
    WHEN NOT EXISTS (SELECT 1 FROM fetch_meta)
      THEN CAST(to_json(struct_pack(error := 'invalid or expired token')) AS VARCHAR)
    ELSE NULL
  END AS body_text,
  CASE
    WHEN EXISTS (SELECT 1 FROM fetch_meta WHERE fetch_error IS NULL AND fetch_payload IS NOT NULL AND fetch_control_json IS NULL)
      THEN (SELECT fetch_payload FROM fetch_meta LIMIT 1)
    ELSE NULL
  END AS body
FROM (SELECT 1) AS keepalive
LEFT JOIN closed ON TRUE
"""
    close_sql = """
WITH req AS (
  SELECT json_extract_string(body_text::JSON, '$.token') AS token_hex
  FROM ducknng_http_request_body()
),
tok AS (
  SELECT
    json_extract_string(decode(from_hex(token_hex))::JSON, '$.user_name') AS user_name,
    json_extract_string(decode(from_hex(token_hex))::JSON, '$.backend_url') AS backend_url,
    TRY_CAST(json_extract_string(decode(from_hex(token_hex))::JSON, '$.session_id') AS UBIGINT) AS session_id,
    json_extract_string(decode(from_hex(token_hex))::JSON, '$.session_token') AS session_token
  FROM req
  WHERE token_hex IS NOT NULL
),
closed AS (
  SELECT
    user_name,
    ducknng_close_query_raw(backend_url, session_id, session_token, 0::UBIGINT) AS close_frame
  FROM tok
  WHERE backend_url IS NOT NULL AND session_id > 0 AND session_token IS NOT NULL
),
close_meta AS (
  SELECT
    user_name,
    ducknng_frame_error_text(close_frame) AS close_error
  FROM closed
)
SELECT
  CASE
    WHEN EXISTS (SELECT 1 FROM req WHERE token_hex IS NULL) THEN 400
    WHEN EXISTS (SELECT 1 FROM close_meta WHERE close_error IS NOT NULL) THEN 400
    WHEN EXISTS (SELECT 1 FROM closed) THEN 200
    ELSE 400
  END AS status,
  'application/json; charset=utf-8' AS content_type,
  CASE
    WHEN EXISTS (SELECT 1 FROM req WHERE token_hex IS NULL)
      THEN CAST(to_json(struct_pack(closed := FALSE, error := 'token is required')) AS VARCHAR)
    WHEN EXISTS (SELECT 1 FROM close_meta WHERE close_error IS NOT NULL)
      THEN CAST(to_json(struct_pack(closed := FALSE, error := (SELECT close_error FROM close_meta WHERE close_error IS NOT NULL LIMIT 1))) AS VARCHAR)
    WHEN EXISTS (SELECT 1 FROM closed)
      THEN CAST(to_json(struct_pack(closed := TRUE, subscriber := (SELECT user_name FROM closed LIMIT 1))) AS VARCHAR)
    ELSE CAST(to_json(struct_pack(closed := FALSE, error := 'invalid or expired token')) AS VARCHAR)
  END AS body_text
FROM (SELECT 1)
"""
    return start_sql, fetch_sql, close_sql


def backend_worker(con: duckdb.DuckDBPyConnection, user_name: str, listen_port: int) -> None:
    server_name = BACKEND_PREFIX + user_name
    range_start, range_stop = {
        "alice": (1, 3001),
        "bob": (10001, 13001),
    }[user_name]
    con.execute("DROP TABLE IF EXISTS tenant_numbers")
    con.execute("CREATE TABLE tenant_numbers(owner VARCHAR, i INTEGER, v INTEGER)")
    con.execute(
        f"INSERT INTO tenant_numbers "
        f"SELECT '{user_name}' AS owner, i, i * 10 AS v "
        f"FROM range({range_start}, {range_stop}) AS t(i)"
    )
    con.execute(
        f"SELECT ducknng_start_server('{server_name}', 'tcp://127.0.0.1:{listen_port}', 1, 134217728, 300000, 0::UBIGINT)"
    )


def gateway_worker(con: duckdb.DuckDBPyConnection, gateway_port: int,
    backend_ports: dict[str, int]) -> None:
    backend_urls = {
        user_name: f"tcp://127.0.0.1:{backend_ports[user_name]}"
        for user_name in BACKEND_USERS
    }
    start_sql, fetch_sql, close_sql = build_gateway_route_sqls(backend_urls)
    con.execute(
        f"SELECT ducknng_start_server('{GATEWAY_NAME}', 'http://127.0.0.1:{gateway_port}/_ducknng', 1, 134217728, 300000, 0::UBIGINT)"
    )
    con.execute(
        "SELECT ducknng_register_http_route(?, ?, ?, ?)",
        [GATEWAY_NAME, "GET", "/healthz", "SELECT 200 AS status, 'text/plain; charset=utf-8' AS content_type, 'ok' AS body_text"],
    )
    con.execute(
        "SELECT ducknng_register_http_route(?, ?, ?, ?, CAST(? AS UBIGINT))",
        [GATEWAY_NAME, "POST", "/v1/query/start", start_sql, 1048576],
    )
    con.execute(
        "SELECT ducknng_register_http_route(?, ?, ?, ?, CAST(? AS UBIGINT))",
        [GATEWAY_NAME, "POST", "/v1/query/fetch", fetch_sql, 1048576],
    )
    con.execute(
        "SELECT ducknng_register_http_route(?, ?, ?, ?, CAST(? AS UBIGINT))",
        [GATEWAY_NAME, "POST", "/v1/query/close", close_sql, 1048576],
    )


def run_demo(ext_path: pathlib.Path) -> int:
    backend_ports = {user_name: free_port() for user_name in BACKEND_USERS}
    gateway_port = free_port()
    base_url = f"http://127.0.0.1:{gateway_port}"
    script = pathlib.Path(__file__).resolve()

    with tempfile.TemporaryDirectory(prefix="ducknng-gateway-demo-") as tmpdir:
        tmp = pathlib.Path(tmpdir)
        processes: dict[str, subprocess.Popen[str]] = {}
        try:
            for user_name in BACKEND_USERS:
                ready_path = tmp / f"{user_name}.ready"
                processes[user_name] = start_worker(
                    script,
                    ready_path,
                    [
                        "--role", "backend",
                        "--extension", str(ext_path),
                        "--backend-key", user_name,
                        "--listen-port", str(backend_ports[user_name]),
                    ],
                )
            processes["gateway"] = start_worker(
                script,
                tmp / "gateway.ready",
                [
                    "--role", "gateway",
                    "--extension", str(ext_path),
                    "--gateway-port", str(gateway_port),
                    "--alice-port", str(backend_ports["alice"]),
                    "--bob-port", str(backend_ports["bob"]),
                ],
            )
            wait_healthz(base_url, 10)

            columns, alice_rows = collect_query(
                base_url,
                ext_path,
                "alice",
                "SELECT owner, i, v FROM tenant_numbers ORDER BY i",
                batch_rows=1,
            )
            if columns != ["owner", "i", "v"]:
                raise RuntimeError(f"unexpected alice columns {columns!r}")
            if len(alice_rows) != 3000 or alice_rows[0] != ("alice", 1, 10) or alice_rows[-1] != ("alice", 3000, 30000):
                raise RuntimeError(f"unexpected alice rows summary {len(alice_rows)!r} {alice_rows[:2]!r} {alice_rows[-2:]!r}")

            _, bob_rows = collect_query(
                base_url,
                ext_path,
                "bob",
                "SELECT owner, i, v FROM tenant_numbers ORDER BY i",
                batch_rows=1,
            )
            if len(bob_rows) != 3000 or bob_rows[0] != ("bob", 10001, 100010) or bob_rows[-1] != ("bob", 13000, 130000):
                raise RuntimeError(f"unexpected bob rows summary {len(bob_rows)!r} {bob_rows[:2]!r} {bob_rows[-2:]!r}")

            status, headers, body = post_json(
                base_url,
                "/v1/query/start",
                {
                    "user": "alice",
                    "sql": "SELECT owner, i, v FROM tenant_numbers ORDER BY i",
                    "batch_rows": 1,
                },
            )
            if status != 200:
                raise RuntimeError(f"unexpected explicit close start status {status}: {body!r}")
            if headers.get("X-Ducknng-Subscriber") != "alice":
                raise RuntimeError(f"unexpected explicit close subscriber {headers.get('X-Ducknng-Subscriber')!r}")
            _, first_rows = decode_arrow_rows(ext_path, body)
            if not first_rows or first_rows[0] != ("alice", 1, 10):
                raise RuntimeError(f"unexpected first close-path batch {first_rows[:5]!r}")
            token = headers.get("X-Ducknng-Next-Token")
            if not token:
                raise RuntimeError("missing continuation token for explicit close path")

            close_status, _, close_body = post_json(base_url, "/v1/query/close", {"token": token})
            if close_status != 200:
                raise RuntimeError(f"unexpected close status {close_status}: {close_body!r}")
            close_json = json.loads(close_body.decode("utf-8"))
            if not close_json.get("closed") or close_json.get("subscriber") != "alice":
                raise RuntimeError(f"unexpected close payload {close_json!r}")

            unknown_status, _, unknown_body = post_json(
                base_url,
                "/v1/query/start",
                {"user": "charlie", "sql": "SELECT 1"},
            )
            if unknown_status != 404:
                raise RuntimeError(f"unexpected unknown-subscriber status {unknown_status}: {unknown_body!r}")

            bad_close_status, _, bad_close_body = post_json(base_url, "/v1/query/close", {"token": "7B7D"})
            if bad_close_status != 400:
                raise RuntimeError(f"unexpected invalid close status {bad_close_status}: {bad_close_body!r}")

            print("motherduck gateway demo: ok")
            print("alice rows:", len(alice_rows), alice_rows[0], alice_rows[-1])
            print("bob rows:", len(bob_rows), bob_rows[0], bob_rows[-1])
            return 0
        finally:
            for name in ["gateway", *reversed(BACKEND_USERS)]:
                proc = processes.get(name)
                if proc is not None:
                    stop_worker(name, proc)


def worker_main(args: argparse.Namespace) -> int:
    stop = False

    def handle_signal(_signum: int, _frame: object) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    try:
        con.execute(f"LOAD '{sql_quote(str(args.extension))}'")
        if args.role == "backend":
            if not args.backend_key or args.listen_port is None:
                raise RuntimeError("backend role requires --backend-key and --listen-port")
            backend_worker(con, args.backend_key, args.listen_port)
        elif args.role == "gateway":
            if args.gateway_port is None or args.alice_port is None or args.bob_port is None:
                raise RuntimeError("gateway role requires --gateway-port, --alice-port, and --bob-port")
            gateway_worker(
                con,
                args.gateway_port,
                {"alice": args.alice_port, "bob": args.bob_port},
            )
        else:
            raise RuntimeError(f"unknown role {args.role}")
        args.ready_file.write_text("ready\n", encoding="utf-8")
        while not stop:
            time.sleep(0.1)
        if args.role == "gateway":
            con.execute(f"SELECT ducknng_stop_server('{GATEWAY_NAME}')")
        else:
            con.execute(f"SELECT ducknng_stop_server('{BACKEND_PREFIX}{args.backend_key}')")
        return 0
    except Exception as exc:  # pragma: no cover - best-effort demo diagnostics
        args.ready_file.write_text(f"error: {exc}\n", encoding="utf-8")
        raise
    finally:
        con.close()


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the ducknng MotherDuck-style gateway demo.")
    parser.add_argument("extension_path", nargs="?", help="Path to the built ducknng.duckdb_extension")
    parser.add_argument("--role", choices=("backend", "gateway"))
    parser.add_argument("--extension", type=pathlib.Path)
    parser.add_argument("--ready-file", type=pathlib.Path)
    parser.add_argument("--backend-key")
    parser.add_argument("--listen-port", type=int)
    parser.add_argument("--gateway-port", type=int)
    parser.add_argument("--alice-port", type=int)
    parser.add_argument("--bob-port", type=int)
    return parser.parse_args(argv)


def main() -> int:
    args = parse_args(sys.argv[1:])
    if args.role:
        if args.extension is None or args.ready_file is None:
            raise SystemExit("worker mode requires --extension and --ready-file")
        return worker_main(args)
    if not args.extension_path:
        raise SystemExit("usage: demo/motherduck_gateway.py <extension_path>")
    ext_path = pathlib.Path(args.extension_path).resolve()
    if not ext_path.exists():
        raise SystemExit(f"extension not found: {ext_path}")
    return run_demo(ext_path)


if __name__ == "__main__":
    raise SystemExit(main())
