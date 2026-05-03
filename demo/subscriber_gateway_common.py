from __future__ import annotations

import json
import pathlib
import signal
import socket
import subprocess
import sys
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
