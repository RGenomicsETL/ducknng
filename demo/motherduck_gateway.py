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


SUBSCRIBER_SPECS = (
    {
        "backend_key": "alice",
        "subscriber_id": "alice_worker",
        "tenant_id": "tenant_alice",
        "principal_id": "demo:alice",
        "api_token": "demo-alice-token",
        "range_start": 1,
        "range_stop": 3001,
    },
    {
        "backend_key": "bob",
        "subscriber_id": "bob_worker",
        "tenant_id": "tenant_bob",
        "principal_id": "demo:bob",
        "api_token": "demo-bob-token",
        "range_start": 10001,
        "range_stop": 13001,
    },
)
ORPHAN_IDENTITY = {
    "principal_id": "demo:orphan",
    "tenant_id": "tenant_orphan",
    "api_token": "demo-orphan-token",
}
BACKEND_PREFIX = "subscriber_"
GATEWAY_NAME = "gateway"


def sql_quote(text: str) -> str:
    return text.replace("'", "''")


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def spec_by_backend_key(backend_key: str) -> dict[str, object]:
    for spec in SUBSCRIBER_SPECS:
        if spec["backend_key"] == backend_key:
            return spec
    raise KeyError(f"unknown backend_key {backend_key!r}")


def spec_by_subscriber_id(subscriber_id: str) -> dict[str, object]:
    for spec in SUBSCRIBER_SPECS:
        if spec["subscriber_id"] == subscriber_id:
            return spec
    raise KeyError(f"unknown subscriber_id {subscriber_id!r}")


def auth_headers(api_token: str, extra_headers: dict[str, str] | None = None) -> dict[str, str]:
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_token}",
    }
    headers.update(extra_headers or {})
    return headers


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


def post_json(base_url: str, path: str, payload: dict[str, object],
    headers: dict[str, str] | None = None) -> tuple[int, dict[str, str], bytes]:
    merged_headers = {"Content-Type": "application/json"}
    if headers:
        merged_headers.update(headers)
    return http_request(
        base_url + path,
        "POST",
        json.dumps(payload).encode("utf-8"),
        merged_headers,
    )


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


def collect_query(base_url: str, ext_path: pathlib.Path, spec: dict[str, object],
    sql: str, batch_rows: int = 1) -> tuple[list[str], list[tuple[object, ...]]]:
    headers = auth_headers(str(spec["api_token"]))
    status, response_headers, body = post_json(
        base_url,
        "/v1/query/start",
        {"sql": sql, "batch_rows": batch_rows},
        headers,
    )
    if status != 200:
        raise RuntimeError(f"unexpected start status {status}: {body!r}")
    if response_headers.get("X-Ducknng-Tenant") != spec["tenant_id"]:
        raise RuntimeError(
            f"gateway routed token for {spec['tenant_id']!r} to tenant {response_headers.get('X-Ducknng-Tenant')!r}"
        )
    if response_headers.get("X-Ducknng-Subscriber") != spec["subscriber_id"]:
        raise RuntimeError(
            f"gateway routed token for {spec['subscriber_id']!r} to subscriber {response_headers.get('X-Ducknng-Subscriber')!r}"
        )
    columns, rows = decode_arrow_rows(ext_path, body)
    token = response_headers.get("X-Ducknng-Next-Token")
    all_rows = list(rows)
    while token:
        status, response_headers, body = post_json(
            base_url,
            "/v1/query/fetch",
            {"token": token},
            headers,
        )
        if status == 204:
            token = None
            break
        if status != 200:
            raise RuntimeError(f"unexpected fetch status {status}: {body!r}")
        if response_headers.get("X-Ducknng-Tenant") != spec["tenant_id"]:
            raise RuntimeError(
                f"gateway continued token for {spec['tenant_id']!r} on tenant {response_headers.get('X-Ducknng-Tenant')!r}"
            )
        if response_headers.get("X-Ducknng-Subscriber") != spec["subscriber_id"]:
            raise RuntimeError(
                f"gateway continued token for {spec['subscriber_id']!r} on subscriber {response_headers.get('X-Ducknng-Subscriber')!r}"
            )
        token = response_headers.get("X-Ducknng-Next-Token")
        _, rows = decode_arrow_rows(ext_path, body)
        all_rows.extend(rows)
    return columns, all_rows


def build_gateway_route_sqls() -> tuple[str, str, str]:
    start_sql = """
WITH req AS (
  SELECT
    r.headers_json,
    r.caller_identity,
    json_extract_string(TRY_CAST(b.body_text AS JSON), '$.sql') AS sql,
    coalesce(TRY_CAST(json_extract_string(TRY_CAST(b.body_text AS JSON), '$.batch_rows') AS UBIGINT), 0::UBIGINT) AS batch_rows,
    coalesce(TRY_CAST(json_extract_string(TRY_CAST(b.body_text AS JSON), '$.batch_bytes') AS UBIGINT), 0::UBIGINT) AS batch_bytes
  FROM ducknng_http_request() AS r, ducknng_http_request_body() AS b
),
header_auth AS (
  SELECT
    max(
      CASE
        WHEN lower(json_extract_string(value, '$.name')) = 'authorization'
          THEN NULLIF(regexp_extract(json_extract_string(value, '$.value'), '^Bearer[ ]+(.+)$', 1), '')
        ELSE NULL
      END
    ) AS bearer_token
  FROM req, json_each(coalesce(req.headers_json, '[]')::JSON)
),
auth AS (
  SELECT req.caller_identity, header_auth.bearer_token
  FROM req, header_auth
),
principal AS (
  SELECT
    i.tenant_id,
    i.principal_id,
    CASE
      WHEN auth.caller_identity IS NOT NULL AND i.caller_identity = auth.caller_identity THEN 'caller_identity'
      ELSE 'bearer'
    END AS auth_source
  FROM auth
  JOIN gateway_identities AS i
    ON i.active AND (
      (auth.caller_identity IS NOT NULL AND i.caller_identity = auth.caller_identity) OR
      (auth.bearer_token IS NOT NULL AND i.api_token = auth.bearer_token)
    )
  ORDER BY
    CASE
      WHEN auth.caller_identity IS NOT NULL AND i.caller_identity = auth.caller_identity THEN 0
      ELSE 1
    END,
    i.principal_id
  LIMIT 1
),
subscriber AS (
  SELECT
    s.tenant_id,
    s.subscriber_id,
    s.backend_url
  FROM principal AS p
  JOIN gateway_subscribers AS s
    ON s.tenant_id = p.tenant_id
  WHERE s.enabled
  ORDER BY s.priority, s.subscriber_id
  LIMIT 1
),
target AS (
  SELECT
    p.tenant_id,
    p.principal_id,
    p.auth_source,
    s.subscriber_id,
    s.backend_url,
    req.sql,
    req.batch_rows,
    req.batch_bytes
  FROM req, principal AS p, subscriber AS s
),
opened AS (
  SELECT
    tenant_id,
    principal_id,
    auth_source,
    subscriber_id,
    backend_url,
    batch_rows,
    batch_bytes,
    ducknng_open_query_raw(backend_url, sql, batch_rows, batch_bytes, 0::UBIGINT) AS open_frame
  FROM target
  WHERE sql IS NOT NULL
),
open_meta AS (
  SELECT
    tenant_id,
    principal_id,
    auth_source,
    subscriber_id,
    backend_url,
    batch_rows,
    batch_bytes,
    ducknng_frame_error_text(open_frame) AS open_error,
    ducknng_frame_payload_text(open_frame) AS open_control_json
  FROM opened
),
fetch_inputs AS (
  SELECT
    tenant_id,
    principal_id,
    auth_source,
    subscriber_id,
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
    tenant_id,
    principal_id,
    auth_source,
    subscriber_id,
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
    tenant_id,
    principal_id,
    auth_source,
    subscriber_id,
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
          tenant_id := tenant_id,
          subscriber_id := subscriber_id,
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
    WHEN NOT EXISTS (SELECT 1 FROM principal) THEN 401
    WHEN EXISTS (SELECT 1 FROM req WHERE sql IS NULL) THEN 400
    WHEN NOT EXISTS (SELECT 1 FROM subscriber) THEN 503
    WHEN EXISTS (SELECT 1 FROM open_meta WHERE open_error IS NOT NULL) THEN 400
    WHEN EXISTS (SELECT 1 FROM fetch_meta WHERE fetch_error IS NOT NULL) THEN 400
    WHEN EXISTS (SELECT 1 FROM fetch_meta WHERE fetch_control_json IS NOT NULL) THEN 204
    ELSE 200
  END AS status,
  CASE
    WHEN NOT EXISTS (SELECT 1 FROM principal)
      OR EXISTS (SELECT 1 FROM req WHERE sql IS NULL)
      OR NOT EXISTS (SELECT 1 FROM subscriber)
      OR EXISTS (SELECT 1 FROM open_meta WHERE open_error IS NOT NULL)
      OR EXISTS (SELECT 1 FROM fetch_meta WHERE fetch_error IS NOT NULL)
      THEN 'application/json; charset=utf-8'
    WHEN EXISTS (SELECT 1 FROM fetch_meta WHERE fetch_control_json IS NOT NULL)
      THEN NULL
    ELSE 'application/vnd.apache.arrow.stream'
  END AS content_type,
  CASE
    WHEN NOT EXISTS (SELECT 1 FROM principal)
      THEN CAST(
        to_json(list_value(struct_pack(name := 'WWW-Authenticate', value := 'Bearer'))) AS VARCHAR
      )
    WHEN EXISTS (SELECT 1 FROM fetch_meta WHERE fetch_control_json IS NOT NULL)
      THEN CAST(
        to_json(
          list_value(
            struct_pack(name := 'X-Ducknng-End-Of-Stream', value := 'true'),
            struct_pack(name := 'X-Ducknng-Tenant', value := (SELECT tenant_id FROM target LIMIT 1)),
            struct_pack(name := 'X-Ducknng-Subscriber', value := (SELECT subscriber_id FROM target LIMIT 1))
          )
        ) AS VARCHAR
      )
    WHEN EXISTS (SELECT 1 FROM token)
      THEN CAST(
        to_json(
          list_value(
            struct_pack(name := 'X-Ducknng-End-Of-Stream', value := 'false'),
            struct_pack(name := 'X-Ducknng-Next-Token', value := (SELECT token FROM token)),
            struct_pack(name := 'X-Ducknng-Tenant', value := (SELECT tenant_id FROM target LIMIT 1)),
            struct_pack(name := 'X-Ducknng-Subscriber', value := (SELECT subscriber_id FROM target LIMIT 1))
          )
        ) AS VARCHAR
      )
    ELSE NULL
  END AS headers_json,
  CASE
    WHEN NOT EXISTS (SELECT 1 FROM principal)
      THEN CAST(to_json(struct_pack(error := 'authorization required')) AS VARCHAR)
    WHEN EXISTS (SELECT 1 FROM req WHERE sql IS NULL)
      THEN CAST(to_json(struct_pack(error := 'sql is required')) AS VARCHAR)
    WHEN NOT EXISTS (SELECT 1 FROM subscriber)
      THEN CAST(to_json(struct_pack(error := 'no subscriber available for tenant')) AS VARCHAR)
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
  SELECT
    r.headers_json,
    r.caller_identity,
    json_extract_string(TRY_CAST(b.body_text AS JSON), '$.token') AS token_hex
  FROM ducknng_http_request() AS r, ducknng_http_request_body() AS b
),
header_auth AS (
  SELECT
    max(
      CASE
        WHEN lower(json_extract_string(value, '$.name')) = 'authorization'
          THEN NULLIF(regexp_extract(json_extract_string(value, '$.value'), '^Bearer[ ]+(.+)$', 1), '')
        ELSE NULL
      END
    ) AS bearer_token
  FROM req, json_each(coalesce(req.headers_json, '[]')::JSON)
),
auth AS (
  SELECT req.caller_identity, header_auth.bearer_token
  FROM req, header_auth
),
principal AS (
  SELECT
    i.tenant_id,
    i.principal_id
  FROM auth
  JOIN gateway_identities AS i
    ON i.active AND (
      (auth.caller_identity IS NOT NULL AND i.caller_identity = auth.caller_identity) OR
      (auth.bearer_token IS NOT NULL AND i.api_token = auth.bearer_token)
    )
  ORDER BY
    CASE
      WHEN auth.caller_identity IS NOT NULL AND i.caller_identity = auth.caller_identity THEN 0
      ELSE 1
    END,
    i.principal_id
  LIMIT 1
),
tok AS (
  SELECT
    json_extract_string(TRY_CAST(decode(from_hex(token_hex)) AS JSON), '$.tenant_id') AS tenant_id,
    json_extract_string(TRY_CAST(decode(from_hex(token_hex)) AS JSON), '$.subscriber_id') AS subscriber_id,
    TRY_CAST(json_extract_string(TRY_CAST(decode(from_hex(token_hex)) AS JSON), '$.session_id') AS UBIGINT) AS session_id,
    json_extract_string(TRY_CAST(decode(from_hex(token_hex)) AS JSON), '$.session_token') AS session_token,
    coalesce(TRY_CAST(json_extract_string(TRY_CAST(decode(from_hex(token_hex)) AS JSON), '$.batch_rows') AS UBIGINT), 0::UBIGINT) AS batch_rows,
    coalesce(TRY_CAST(json_extract_string(TRY_CAST(decode(from_hex(token_hex)) AS JSON), '$.batch_bytes') AS UBIGINT), 0::UBIGINT) AS batch_bytes
  FROM req
  WHERE token_hex IS NOT NULL
),
valid_tok AS (
  SELECT
    tenant_id,
    subscriber_id,
    session_id,
    session_token,
    batch_rows,
    batch_bytes
  FROM tok
  WHERE subscriber_id IS NOT NULL AND session_id > 0 AND session_token IS NOT NULL
),
scoped AS (
  SELECT
    valid_tok.tenant_id,
    valid_tok.subscriber_id,
    valid_tok.session_id,
    valid_tok.session_token,
    valid_tok.batch_rows,
    valid_tok.batch_bytes
  FROM valid_tok
  JOIN principal
    ON principal.tenant_id = valid_tok.tenant_id
),
target AS (
  SELECT
    scoped.tenant_id,
    scoped.subscriber_id,
    scoped.session_id,
    scoped.session_token,
    scoped.batch_rows,
    scoped.batch_bytes,
    s.backend_url
  FROM scoped
  JOIN gateway_subscribers AS s
    ON s.tenant_id = scoped.tenant_id AND s.subscriber_id = scoped.subscriber_id
  WHERE s.enabled
),
fetched AS (
  SELECT
    tenant_id,
    subscriber_id,
    session_id,
    session_token,
    batch_rows,
    batch_bytes,
    ducknng_fetch_query_raw(backend_url, session_id, session_token, batch_rows, batch_bytes, 0::UBIGINT) AS fetch_frame
  FROM target
),
fetch_meta AS (
  SELECT
    tenant_id,
    subscriber_id,
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
          tenant_id := tenant_id,
          subscriber_id := subscriber_id,
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
    tenant_id,
    subscriber_id,
    ducknng_close_query_raw(backend_url, session_id, session_token, 0::UBIGINT) AS close_frame
  FROM target
  JOIN fetch_meta USING(tenant_id, subscriber_id, session_id, session_token, batch_rows, batch_bytes)
  WHERE fetch_error IS NULL AND fetch_control_json IS NOT NULL
)
SELECT
  CASE
    WHEN NOT EXISTS (SELECT 1 FROM principal) THEN 401
    WHEN EXISTS (SELECT 1 FROM req WHERE token_hex IS NULL) THEN 400
    WHEN EXISTS (SELECT 1 FROM tok) AND NOT EXISTS (SELECT 1 FROM valid_tok) THEN 400
    WHEN EXISTS (SELECT 1 FROM valid_tok) AND NOT EXISTS (SELECT 1 FROM scoped) THEN 403
    WHEN EXISTS (SELECT 1 FROM scoped) AND NOT EXISTS (SELECT 1 FROM target) THEN 503
    WHEN EXISTS (SELECT 1 FROM fetch_meta WHERE fetch_error IS NOT NULL) THEN 400
    WHEN EXISTS (SELECT 1 FROM fetch_meta WHERE fetch_control_json IS NOT NULL) THEN 204
    WHEN EXISTS (SELECT 1 FROM token) THEN 200
    ELSE 400
  END AS status,
  CASE
    WHEN NOT EXISTS (SELECT 1 FROM principal)
      OR EXISTS (SELECT 1 FROM req WHERE token_hex IS NULL)
      OR (EXISTS (SELECT 1 FROM tok) AND NOT EXISTS (SELECT 1 FROM valid_tok))
      OR (EXISTS (SELECT 1 FROM valid_tok) AND NOT EXISTS (SELECT 1 FROM scoped))
      OR (EXISTS (SELECT 1 FROM scoped) AND NOT EXISTS (SELECT 1 FROM target))
      OR EXISTS (SELECT 1 FROM fetch_meta WHERE fetch_error IS NOT NULL)
      OR NOT EXISTS (SELECT 1 FROM tok)
      THEN 'application/json; charset=utf-8'
    WHEN EXISTS (SELECT 1 FROM fetch_meta WHERE fetch_control_json IS NOT NULL)
      THEN NULL
    ELSE 'application/vnd.apache.arrow.stream'
  END AS content_type,
  CASE
    WHEN NOT EXISTS (SELECT 1 FROM principal)
      THEN CAST(
        to_json(list_value(struct_pack(name := 'WWW-Authenticate', value := 'Bearer'))) AS VARCHAR
      )
    WHEN EXISTS (SELECT 1 FROM fetch_meta WHERE fetch_control_json IS NOT NULL)
      THEN CAST(
        to_json(
          list_value(
            struct_pack(name := 'X-Ducknng-End-Of-Stream', value := 'true'),
            struct_pack(name := 'X-Ducknng-Tenant', value := (SELECT tenant_id FROM target LIMIT 1)),
            struct_pack(name := 'X-Ducknng-Subscriber', value := (SELECT subscriber_id FROM target LIMIT 1))
          )
        ) AS VARCHAR
      )
    WHEN EXISTS (SELECT 1 FROM token)
      THEN CAST(
        to_json(
          list_value(
            struct_pack(name := 'X-Ducknng-End-Of-Stream', value := 'false'),
            struct_pack(name := 'X-Ducknng-Next-Token', value := (SELECT token FROM token)),
            struct_pack(name := 'X-Ducknng-Tenant', value := (SELECT tenant_id FROM target LIMIT 1)),
            struct_pack(name := 'X-Ducknng-Subscriber', value := (SELECT subscriber_id FROM target LIMIT 1))
          )
        ) AS VARCHAR
      )
    ELSE NULL
  END AS headers_json,
  CASE
    WHEN NOT EXISTS (SELECT 1 FROM principal)
      THEN CAST(to_json(struct_pack(error := 'authorization required')) AS VARCHAR)
    WHEN EXISTS (SELECT 1 FROM req WHERE token_hex IS NULL)
      THEN CAST(to_json(struct_pack(error := 'token is required')) AS VARCHAR)
    WHEN EXISTS (SELECT 1 FROM tok) AND NOT EXISTS (SELECT 1 FROM valid_tok)
      THEN CAST(to_json(struct_pack(error := 'invalid or expired token')) AS VARCHAR)
    WHEN NOT EXISTS (SELECT 1 FROM tok)
      THEN CAST(to_json(struct_pack(error := 'invalid or expired token')) AS VARCHAR)
    WHEN EXISTS (SELECT 1 FROM valid_tok) AND NOT EXISTS (SELECT 1 FROM scoped)
      THEN CAST(to_json(struct_pack(error := 'token is not valid for caller')) AS VARCHAR)
    WHEN EXISTS (SELECT 1 FROM scoped) AND NOT EXISTS (SELECT 1 FROM target)
      THEN CAST(to_json(struct_pack(error := 'no subscriber available for tenant')) AS VARCHAR)
    WHEN EXISTS (SELECT 1 FROM fetch_meta WHERE fetch_error IS NOT NULL)
      THEN CAST(to_json(struct_pack(error := (SELECT fetch_error FROM fetch_meta WHERE fetch_error IS NOT NULL LIMIT 1))) AS VARCHAR)
    ELSE NULL
  END AS body_text,
  CASE
    WHEN EXISTS (SELECT 1 FROM fetch_meta WHERE fetch_error IS NULL AND fetch_payload IS NOT NULL AND fetch_control_json IS NULL)
      THEN (SELECT fetch_payload FROM fetch_meta LIMIT 1)
    ELSE NULL
  END AS body
FROM (SELECT 1)
LEFT JOIN closed ON TRUE
"""
    close_sql = """
WITH req AS (
  SELECT
    r.headers_json,
    r.caller_identity,
    json_extract_string(TRY_CAST(b.body_text AS JSON), '$.token') AS token_hex
  FROM ducknng_http_request() AS r, ducknng_http_request_body() AS b
),
header_auth AS (
  SELECT
    max(
      CASE
        WHEN lower(json_extract_string(value, '$.name')) = 'authorization'
          THEN NULLIF(regexp_extract(json_extract_string(value, '$.value'), '^Bearer[ ]+(.+)$', 1), '')
        ELSE NULL
      END
    ) AS bearer_token
  FROM req, json_each(coalesce(req.headers_json, '[]')::JSON)
),
auth AS (
  SELECT req.caller_identity, header_auth.bearer_token
  FROM req, header_auth
),
principal AS (
  SELECT
    i.tenant_id,
    i.principal_id
  FROM auth
  JOIN gateway_identities AS i
    ON i.active AND (
      (auth.caller_identity IS NOT NULL AND i.caller_identity = auth.caller_identity) OR
      (auth.bearer_token IS NOT NULL AND i.api_token = auth.bearer_token)
    )
  ORDER BY
    CASE
      WHEN auth.caller_identity IS NOT NULL AND i.caller_identity = auth.caller_identity THEN 0
      ELSE 1
    END,
    i.principal_id
  LIMIT 1
),
tok AS (
  SELECT
    json_extract_string(TRY_CAST(decode(from_hex(token_hex)) AS JSON), '$.tenant_id') AS tenant_id,
    json_extract_string(TRY_CAST(decode(from_hex(token_hex)) AS JSON), '$.subscriber_id') AS subscriber_id,
    TRY_CAST(json_extract_string(TRY_CAST(decode(from_hex(token_hex)) AS JSON), '$.session_id') AS UBIGINT) AS session_id,
    json_extract_string(TRY_CAST(decode(from_hex(token_hex)) AS JSON), '$.session_token') AS session_token
  FROM req
  WHERE token_hex IS NOT NULL
),
valid_tok AS (
  SELECT
    tenant_id,
    subscriber_id,
    session_id,
    session_token
  FROM tok
  WHERE subscriber_id IS NOT NULL AND session_id > 0 AND session_token IS NOT NULL
),
scoped AS (
  SELECT
    valid_tok.tenant_id,
    valid_tok.subscriber_id,
    valid_tok.session_id,
    valid_tok.session_token
  FROM valid_tok
  JOIN principal
    ON principal.tenant_id = valid_tok.tenant_id
),
target AS (
  SELECT
    scoped.tenant_id,
    scoped.subscriber_id,
    scoped.session_id,
    scoped.session_token,
    s.backend_url
  FROM scoped
  JOIN gateway_subscribers AS s
    ON s.tenant_id = scoped.tenant_id AND s.subscriber_id = scoped.subscriber_id
  WHERE s.enabled
),
closed AS (
  SELECT
    tenant_id,
    subscriber_id,
    ducknng_close_query_raw(backend_url, session_id, session_token, 0::UBIGINT) AS close_frame
  FROM target
),
close_meta AS (
  SELECT
    tenant_id,
    subscriber_id,
    ducknng_frame_error_text(close_frame) AS close_error
  FROM closed
)
SELECT
  CASE
    WHEN NOT EXISTS (SELECT 1 FROM principal) THEN 401
    WHEN EXISTS (SELECT 1 FROM req WHERE token_hex IS NULL) THEN 400
    WHEN EXISTS (SELECT 1 FROM tok) AND NOT EXISTS (SELECT 1 FROM valid_tok) THEN 400
    WHEN EXISTS (SELECT 1 FROM valid_tok) AND NOT EXISTS (SELECT 1 FROM scoped) THEN 403
    WHEN EXISTS (SELECT 1 FROM scoped) AND NOT EXISTS (SELECT 1 FROM target) THEN 503
    WHEN EXISTS (SELECT 1 FROM close_meta WHERE close_error IS NOT NULL) THEN 400
    WHEN EXISTS (SELECT 1 FROM closed) THEN 200
    ELSE 400
  END AS status,
  'application/json; charset=utf-8' AS content_type,
  CASE
    WHEN NOT EXISTS (SELECT 1 FROM principal)
      THEN CAST(
        to_json(list_value(struct_pack(name := 'WWW-Authenticate', value := 'Bearer'))) AS VARCHAR
      )
    ELSE NULL
  END AS headers_json,
  CASE
    WHEN NOT EXISTS (SELECT 1 FROM principal)
      THEN CAST(to_json(struct_pack(closed := FALSE, error := 'authorization required')) AS VARCHAR)
    WHEN EXISTS (SELECT 1 FROM req WHERE token_hex IS NULL)
      THEN CAST(to_json(struct_pack(closed := FALSE, error := 'token is required')) AS VARCHAR)
    WHEN EXISTS (SELECT 1 FROM tok) AND NOT EXISTS (SELECT 1 FROM valid_tok)
      THEN CAST(to_json(struct_pack(closed := FALSE, error := 'invalid or expired token')) AS VARCHAR)
    WHEN NOT EXISTS (SELECT 1 FROM tok)
      THEN CAST(to_json(struct_pack(closed := FALSE, error := 'invalid or expired token')) AS VARCHAR)
    WHEN EXISTS (SELECT 1 FROM valid_tok) AND NOT EXISTS (SELECT 1 FROM scoped)
      THEN CAST(to_json(struct_pack(closed := FALSE, error := 'token is not valid for caller')) AS VARCHAR)
    WHEN EXISTS (SELECT 1 FROM scoped) AND NOT EXISTS (SELECT 1 FROM target)
      THEN CAST(to_json(struct_pack(closed := FALSE, error := 'no subscriber available for tenant')) AS VARCHAR)
    WHEN EXISTS (SELECT 1 FROM close_meta WHERE close_error IS NOT NULL)
      THEN CAST(to_json(struct_pack(closed := FALSE, error := (SELECT close_error FROM close_meta WHERE close_error IS NOT NULL LIMIT 1))) AS VARCHAR)
    WHEN EXISTS (SELECT 1 FROM closed)
      THEN CAST(
        to_json(
          struct_pack(
            closed := TRUE,
            tenant_id := (SELECT tenant_id FROM closed LIMIT 1),
            subscriber_id := (SELECT subscriber_id FROM closed LIMIT 1)
          )
        ) AS VARCHAR
      )
    ELSE CAST(to_json(struct_pack(closed := FALSE, error := 'invalid or expired token')) AS VARCHAR)
  END AS body_text
FROM (SELECT 1)
"""
    return start_sql, fetch_sql, close_sql


def backend_worker(con: duckdb.DuckDBPyConnection, backend_key: str, listen_port: int) -> None:
    spec = spec_by_backend_key(backend_key)
    server_name = BACKEND_PREFIX + backend_key
    con.execute("DROP TABLE IF EXISTS tenant_numbers")
    con.execute("CREATE TABLE tenant_numbers(owner VARCHAR, i INTEGER, v INTEGER)")
    con.execute(
        f"INSERT INTO tenant_numbers "
        f"SELECT '{backend_key}' AS owner, i, i * 10 AS v "
        f"FROM range({int(spec['range_start'])}, {int(spec['range_stop'])}) AS t(i)"
    )
    con.execute(
        f"SELECT ducknng_start_server('{server_name}', 'tcp://127.0.0.1:{listen_port}', 1, 134217728, 300000, 0::UBIGINT)"
    )


def install_gateway_metadata(con: duckdb.DuckDBPyConnection, backend_urls: dict[str, str]) -> None:
    con.execute("DROP TABLE IF EXISTS gateway_identities")
    con.execute("DROP TABLE IF EXISTS gateway_subscribers")
    con.execute(
        "CREATE TABLE gateway_identities("
        "api_token VARCHAR, caller_identity VARCHAR, tenant_id VARCHAR, principal_id VARCHAR, active BOOLEAN)"
    )
    con.execute(
        "CREATE TABLE gateway_subscribers("
        "subscriber_id VARCHAR, tenant_id VARCHAR, backend_url VARCHAR, priority INTEGER, enabled BOOLEAN)"
    )
    identity_rows = [
        (str(spec["api_token"]), None, str(spec["tenant_id"]), str(spec["principal_id"]), True)
        for spec in SUBSCRIBER_SPECS
    ]
    identity_rows.append(
        (str(ORPHAN_IDENTITY["api_token"]), None, str(ORPHAN_IDENTITY["tenant_id"]), str(ORPHAN_IDENTITY["principal_id"]), True)
    )
    subscriber_rows = [
        (
            str(spec["subscriber_id"]),
            str(spec["tenant_id"]),
            backend_urls[str(spec["backend_key"])],
            1,
            True,
        )
        for spec in SUBSCRIBER_SPECS
    ]
    con.executemany(
        "INSERT INTO gateway_identities VALUES (?, ?, ?, ?, ?)",
        identity_rows,
    )
    con.executemany(
        "INSERT INTO gateway_subscribers VALUES (?, ?, ?, ?, ?)",
        subscriber_rows,
    )


def gateway_worker(con: duckdb.DuckDBPyConnection, gateway_port: int,
    backend_ports: dict[str, int]) -> None:
    backend_urls = {
        backend_key: f"tcp://127.0.0.1:{port}"
        for backend_key, port in backend_ports.items()
    }
    install_gateway_metadata(con, backend_urls)
    start_sql, fetch_sql, close_sql = build_gateway_route_sqls()
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
    backend_ports = {
        str(spec["backend_key"]): free_port()
        for spec in SUBSCRIBER_SPECS
    }
    gateway_port = free_port()
    base_url = f"http://127.0.0.1:{gateway_port}"
    script = pathlib.Path(__file__).resolve()

    with tempfile.TemporaryDirectory(prefix="ducknng-gateway-demo-") as tmpdir:
        tmp = pathlib.Path(tmpdir)
        processes: dict[str, subprocess.Popen[str]] = {}
        try:
            for spec in SUBSCRIBER_SPECS:
                backend_key = str(spec["backend_key"])
                processes[backend_key] = start_worker(
                    script,
                    tmp / f"{backend_key}.ready",
                    [
                        "--role", "backend",
                        "--extension", str(ext_path),
                        "--backend-key", backend_key,
                        "--listen-port", str(backend_ports[backend_key]),
                    ],
                )
            gateway_args = [
                "--role", "gateway",
                "--extension", str(ext_path),
                "--gateway-port", str(gateway_port),
            ]
            for backend_key, port in backend_ports.items():
                gateway_args.extend(["--subscriber", f"{backend_key}={port}"])
            processes["gateway"] = start_worker(
                script,
                tmp / "gateway.ready",
                gateway_args,
            )
            wait_healthz(base_url, 10)

            alice_spec = spec_by_subscriber_id("alice_worker")
            bob_spec = spec_by_subscriber_id("bob_worker")

            columns, alice_rows = collect_query(
                base_url,
                ext_path,
                alice_spec,
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
                bob_spec,
                "SELECT owner, i, v FROM tenant_numbers ORDER BY i",
                batch_rows=1,
            )
            if len(bob_rows) != 3000 or bob_rows[0] != ("bob", 10001, 100010) or bob_rows[-1] != ("bob", 13000, 130000):
                raise RuntimeError(f"unexpected bob rows summary {len(bob_rows)!r} {bob_rows[:2]!r} {bob_rows[-2:]!r}")

            status, headers, body = post_json(
                base_url,
                "/v1/query/start",
                {
                    "sql": "SELECT owner, i, v FROM tenant_numbers ORDER BY i",
                    "batch_rows": 1,
                },
                auth_headers(str(alice_spec["api_token"])),
            )
            if status != 200:
                raise RuntimeError(f"unexpected explicit close start status {status}: {body!r}")
            if headers.get("X-Ducknng-Tenant") != alice_spec["tenant_id"]:
                raise RuntimeError(f"unexpected explicit close tenant {headers.get('X-Ducknng-Tenant')!r}")
            if headers.get("X-Ducknng-Subscriber") != alice_spec["subscriber_id"]:
                raise RuntimeError(f"unexpected explicit close subscriber {headers.get('X-Ducknng-Subscriber')!r}")
            _, first_rows = decode_arrow_rows(ext_path, body)
            if not first_rows or first_rows[0] != ("alice", 1, 10):
                raise RuntimeError(f"unexpected first close-path batch {first_rows[:5]!r}")
            token = headers.get("X-Ducknng-Next-Token")
            if not token:
                raise RuntimeError("missing continuation token for explicit close path")

            cross_close_status, _, cross_close_body = post_json(
                base_url,
                "/v1/query/close",
                {"token": token},
                auth_headers(str(bob_spec["api_token"])),
            )
            if cross_close_status != 403:
                raise RuntimeError(f"unexpected cross-tenant close status {cross_close_status}: {cross_close_body!r}")

            close_status, _, close_body = post_json(
                base_url,
                "/v1/query/close",
                {"token": token},
                auth_headers(str(alice_spec["api_token"])),
            )
            if close_status != 200:
                raise RuntimeError(f"unexpected close status {close_status}: {close_body!r}")
            close_json = json.loads(close_body.decode("utf-8"))
            if not close_json.get("closed") or close_json.get("tenant_id") != alice_spec["tenant_id"]:
                raise RuntimeError(f"unexpected close payload {close_json!r}")

            missing_auth_status, _, missing_auth_body = post_json(
                base_url,
                "/v1/query/start",
                {"sql": "SELECT 1 AS x"},
            )
            if missing_auth_status != 401:
                raise RuntimeError(f"unexpected missing-auth status {missing_auth_status}: {missing_auth_body!r}")

            orphan_status, _, orphan_body = post_json(
                base_url,
                "/v1/query/start",
                {"sql": "SELECT 1 AS x"},
                auth_headers(str(ORPHAN_IDENTITY["api_token"])),
            )
            if orphan_status != 503:
                raise RuntimeError(f"unexpected orphan-tenant status {orphan_status}: {orphan_body!r}")

            bad_close_status, _, bad_close_body = post_json(
                base_url,
                "/v1/query/close",
                {"token": "7B7D"},
                auth_headers(str(alice_spec["api_token"])),
            )
            if bad_close_status != 400:
                raise RuntimeError(f"unexpected invalid close status {bad_close_status}: {bad_close_body!r}")

            print("motherduck gateway demo: ok")
            print("alice rows:", len(alice_rows), alice_rows[0], alice_rows[-1])
            print("bob rows:", len(bob_rows), bob_rows[0], bob_rows[-1])
            return 0
        finally:
            for name in ["gateway", *reversed([str(spec["backend_key"]) for spec in SUBSCRIBER_SPECS])]:
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
            if args.gateway_port is None:
                raise RuntimeError("gateway role requires --gateway-port")
            backend_ports: dict[str, int] = {}
            for item in args.subscriber:
                backend_key, raw_port = item.split("=", 1)
                backend_ports[backend_key] = int(raw_port)
            if not backend_ports:
                raise RuntimeError("gateway role requires at least one --subscriber backend_key=port")
            gateway_worker(con, args.gateway_port, backend_ports)
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
    parser.add_argument("--subscriber", action="append", default=[])
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
