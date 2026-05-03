#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
import signal
import sys
import tempfile
import time

import duckdb

from subscriber_gateway_common import (
    BACKEND_PREFIX,
    GATEWAY_NAME,
    ORPHAN_IDENTITY,
    SUBSCRIBER_SPECS,
    auth_headers,
    collect_query,
    decode_arrow_rows,
    free_port,
    post_json,
    spec_by_subscriber_id,
    sql_quote,
    start_worker,
    stop_worker,
    wait_healthz,
)
from subscriber_gateway_workers import backend_worker, gateway_worker


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
        processes: dict[str, object] = {}
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

            print("subscriber gateway demo: ok")
            print("alice rows:", len(alice_rows), alice_rows[0], alice_rows[-1])
            print("bob rows:", len(bob_rows), bob_rows[0], bob_rows[-1])
            return 0
        finally:
            for name in ["gateway", *reversed([str(spec["backend_key"]) for spec in SUBSCRIBER_SPECS])]:
                proc = processes.get(name)
                if proc is not None:
                    stop_worker(name, proc)  # type: ignore[arg-type]


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
    parser = argparse.ArgumentParser(description="Run the ducknng subscriber gateway demo.")
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
        raise SystemExit("usage: demo/subscriber_gateway.py <extension_path>")
    ext_path = pathlib.Path(args.extension_path).resolve()
    if not ext_path.exists():
        raise SystemExit(f"extension not found: {ext_path}")
    return run_demo(ext_path)


if __name__ == "__main__":
    raise SystemExit(main())
