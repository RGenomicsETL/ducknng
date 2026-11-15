#!/usr/bin/env python3
"""Reject ducknng client calls in an authorizer that runs a prepared plan.

The policy is prepared on the server's execution connection outside any
authorizer, so its ducknng calls pass their bind-time guard. The authorizer
then runs it through EXECUTE, where DuckDB may reuse the bound plan and run
the dependent pipeline on worker threads. Every authorized request must be
denied without the ducknng_request table or the ducknng_request_raw scalar
reaching the counting HTTP endpoint. Outside the authorizer the same plans
prepared on the host connection, and a direct host call, must still reach it.

The execution-context tables resolve the current caller at every execution:
a reused plan reports each new request's subject, an authorizer sees its own
callback, and a user SQL body codec inherits the scope of the SQL that
decodes, so it reads the caller's context but cannot contact the network
from inside an authorizer.

usage: test/authorizer_prepared_guard.py <extension_path> [expected_version]
"""
from __future__ import annotations

import http.server
import pathlib
import sys
import threading

import _duckdb
import duckdb

GUARD_ERROR = "cannot run inside a SQL authorizer callback"
AUTHORIZED_REQUESTS = 5


class Counter(http.server.BaseHTTPRequestHandler):
    calls = 0
    lock = threading.Lock()

    def do_POST(self) -> None:
        self.rfile.read(int(self.headers.get("Content-Length", "0")))
        with Counter.lock:
            Counter.calls += 1
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args) -> None:
        pass


def calls() -> int:
    with Counter.lock:
        return Counter.calls


def policy_sql(name: str, port: int, call: str) -> str:
    """A policy whose ducknng call depends on a sorted, materialized CTE, so
    the plan has several pipelines that DuckDB schedules on worker threads."""
    url = f"http://127.0.0.1:{port}/count"
    if call == "table":
        source = (f"x, ducknng_request('{url}', encode('body'), 1000, 0::UBIGINT) r")
        allow = "r.ok IS NOT NULL"
    else:
        source = "x"
        allow = (f"ducknng_request_raw('{url}', encode(x.i::VARCHAR), 1000, "
                 "0::UBIGINT) IS NOT NULL")
    return (f"PREPARE {name} AS "
            "WITH x AS MATERIALIZED ("
            "SELECT i FROM range(1000000) t(i) ORDER BY hash(i)) "
            f"SELECT {allow} AS allow FROM {source} LIMIT 1")


# A sorted, materialized CTE joined to the context table, so the context
# table's init and scan run in a dependent pipeline on worker threads.
WORKER_CTE = ("WITH x AS MATERIALIZED ("
              "SELECT i FROM range(1000000) t(i) ORDER BY hash(i)) ")
ANONYMOUS_SUBJECT = ('{"peer_identity":null,"principal":null,"subject":null,'
                     '"claims_json":null,"authenticated":false}')


def principal_subject(name: str) -> str:
    return ('{"peer_identity":null,"principal":"%s","subject":"%s",'
            '"claims_json":null,"authenticated":false}' % (name, name))


def check_execution_context(con, run_rpc, port: int) -> None:
    url = "inproc://prepared_guard"

    def call(method: str):
        frame = con.execute(
            "SELECT ducknng_request_raw(?, ducknng_encode_rpc_call(?, '{}'), 5000, 0::UBIGINT)",
            [url, method]).fetchone()[0]
        ok, error = con.execute("SELECT ok, error FROM ducknng_decode_frame(?::BLOB)",
                                [frame]).fetchone()
        text = con.execute("SELECT ducknng_frame_payload_text(?::BLOB)", [frame]).fetchone()[0]
        return ok, error, text

    def authorize(sql):
        assert con.execute("SELECT ducknng_set_service_authorizer('prepared_guard', ?)",
                           [sql]).fetchone()[0]

    def manifest():
        return con.execute("SELECT ok, error FROM ducknng_get_rpc_manifest(?, 0::UBIGINT)",
                           [url]).fetchone()

    # One plan, prepared once on the request connection, reused by an
    # anonymous caller and then by two different principals.
    assert run_rpc(f"PREPARE ctx_subject AS {WORKER_CTE}"
                   "SELECT to_json(s) FROM x, ducknng_request_subject() s LIMIT 1") == (True, None)
    assert con.execute("SELECT ducknng_register_sql_method('ctx_who', 'EXECUTE ctx_subject', '{}')"
                       ).fetchone()[0]
    assert call("ctx_who") == (True, None, ANONYMOUS_SUBJECT)
    for name in ("alice", "bob", "alice"):
        authorize(f"SELECT true AS allow, '{name}' AS principal FROM ducknng_auth_context()")
        assert call("ctx_who") == (True, None, principal_subject(name)), name
    authorize(None)
    # The host connection is outside any request.
    assert con.execute("SELECT count(*) FROM ducknng_request_subject()").fetchone()[0] == 0
    assert con.execute("SELECT count(*) FROM ducknng_auth_context()").fetchone()[0] == 0

    # A prepared policy reads the authorizer callback it is evaluated for.
    assert run_rpc(f"PREPARE ctx_auth AS {WORKER_CTE}"
                   "SELECT a.rpc_method = 'manifest' AS allow FROM x, ducknng_auth_context() a "
                   "LIMIT 1") == (True, None)
    authorize("EXECUTE ctx_auth")
    for _ in range(3):
        assert manifest() == (True, None)
        ok, error, _ = call("ctx_who")
        assert ok is False and "SQL authorizer denied request" in error, error
    authorize(None)

    # A user codec inherits the decoding scope: on the host it sees nothing,
    # inside an authorizer it sees the callback (and no request subject, which
    # is not decided yet), inside a request it sees the caller.
    con.execute("CREATE OR REPLACE FUNCTION ctx_codec(b BLOB) AS ("
                "SELECT coalesce(max(rpc_method), 'none') || ':' || "
                "(SELECT count(*) FROM ducknng_request_subject()) || ':' || "
                "coalesce((SELECT max(principal) FROM ducknng_request_subject()), 'none') "
                "FROM ducknng_auth_context())")
    assert con.execute("SELECT ducknng_register_codec('application/x-ctx', 'ctx_codec')").fetchone()[0]
    decode = "ducknng_parse_body('x'::BLOB, 'application/x-ctx')"
    assert con.execute(f"SELECT value FROM {decode}").fetchall() == [("none:0:none",)]
    assert run_rpc(f"PREPARE ctx_codec_policy AS {WORKER_CTE}"
                   f"SELECT v.value = 'manifest:0:none' AS allow FROM x, {decode} v LIMIT 1"
                   ) == (True, None)
    authorize("EXECUTE ctx_codec_policy")
    for _ in range(3):
        assert manifest() == (True, None)
        ok, error, _ = call("ctx_who")
        assert ok is False and "SQL authorizer denied request" in error, error
    authorize("SELECT true AS allow, 'carol' AS principal FROM ducknng_auth_context()")
    assert con.execute("SELECT ducknng_register_sql_method('ctx_decode', ?, '{}')",
                       [f"SELECT value FROM {decode}"]).fetchone()[0]
    assert call("ctx_decode") == (True, None, '"none:1:carol"')
    authorize(None)

    # A codec that issues HTTP: denied from a prepared authorizer policy with
    # no outbound request, and allowed for host SQL.
    con.execute("CREATE OR REPLACE FUNCTION ctx_http_codec(b BLOB) AS "
                f"ducknng_frame_payload_text(ducknng_request_raw('http://127.0.0.1:{port}/count', "
                "b, 1000, 0::UBIGINT))")
    assert con.execute("SELECT ducknng_register_codec('application/x-ctx-http', 'ctx_http_codec')"
                       ).fetchone()[0]
    http_decode = "ducknng_parse_body('x'::BLOB, 'application/x-ctx-http')"
    assert run_rpc(f"PREPARE ctx_http_policy AS {WORKER_CTE}"
                   f"SELECT v.value IS NOT NULL AS allow FROM x, {http_decode} v LIMIT 1"
                   ) == (True, None)
    before = calls()
    authorize("EXECUTE ctx_http_policy")
    for attempt in range(AUTHORIZED_REQUESTS):
        ok, error = manifest()
        print(f"codec authorized request {attempt}: ok={ok} http_calls={calls() - before} "
              f"error={error}", flush=True)
        assert ok is False and error and GUARD_ERROR in error, (attempt, error)
        assert calls() == before, (attempt, calls() - before)
    authorize(None)
    assert con.execute(f"SELECT count(*) FROM {http_decode}").fetchone()[0] == 1
    assert calls() == before + 1

    # Codec SQL that decodes a SQL-backed body would need the codec connection
    # it already holds; it fails instead of waiting forever.
    con.execute("CREATE OR REPLACE FUNCTION ctx_nested_codec(b BLOB) AS (SELECT max(a)::VARCHAR "
                "FROM ducknng_parse_body('[{\"a\":1}]'::BLOB, 'application/json', "
                "schema := NULL::STRUCT(a INTEGER)))")
    assert con.execute("SELECT ducknng_register_codec('application/x-ctx-nested', "
                       "'ctx_nested_codec')").fetchone()[0]
    try:
        con.execute("SELECT value FROM ducknng_parse_body('x'::BLOB, 'application/x-ctx-nested')")
    except duckdb.Error as exc:
        assert "a body codec cannot decode a nested body" in str(exc), exc
    else:
        raise AssertionError("nested SQL-backed decode did not fail")
    print("execution context: ok", flush=True)


def main() -> None:
    if len(sys.argv) not in (2, 3):
        raise SystemExit(__doc__.strip().splitlines()[-1])
    ext_path = pathlib.Path(sys.argv[1]).resolve()
    expected_version = sys.argv[2] if len(sys.argv) == 3 else None

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Counter)
    serving = threading.Thread(target=server.serve_forever, daemon=True)
    serving.start()
    con = duckdb.connect(config={"allow_unsigned_extensions": "true", "threads": "8"})
    started = False
    try:
        version = con.execute("SELECT version()").fetchone()[0]
        runtime = pathlib.Path(_duckdb.__file__).resolve()
        print(f"duckdb {version} from {runtime}", flush=True)
        assert runtime.is_file(), runtime
        if expected_version is not None:
            assert version == expected_version, (version, expected_version)
        con.execute(f"LOAD '{ext_path}'")
        con.execute("SELECT ducknng_register_exec_method(false)")
        assert con.execute(
            "SELECT ducknng_start_server('prepared_guard', 'inproc://prepared_guard', 1, "
            "1048576::UBIGINT, 300000::UBIGINT, 0::UBIGINT)").fetchone()[0]
        started = True
        port = server.server_port

        def run_rpc(sql: str):
            return con.execute(
                "SELECT ok, error FROM ducknng_run_rpc('inproc://prepared_guard', ?, 0::UBIGINT)",
                [sql]).fetchone()

        for name, call in (("table_policy", "table"), ("scalar_policy", "scalar")):
            assert run_rpc(policy_sql(name, port, call)) == (True, None), name
        assert calls() == 0

        for name in ("table_policy", "scalar_policy"):
            assert con.execute(
                "SELECT ducknng_set_service_authorizer('prepared_guard', ?)",
                [f"EXECUTE {name}"]).fetchone()[0]
            for attempt in range(AUTHORIZED_REQUESTS):
                ok, error = con.execute(
                    "SELECT ok, error FROM ducknng_get_rpc_manifest("
                    "'inproc://prepared_guard', 0::UBIGINT)").fetchone()
                print(f"{name} authorized request {attempt}: ok={ok} "
                      f"http_calls={calls()} error={error}", flush=True)
                assert ok is False, (name, attempt)
                assert error and GUARD_ERROR in error, (name, attempt, error)
                assert calls() == 0, (name, attempt, calls())

        assert con.execute(
            "SELECT ducknng_set_service_authorizer('prepared_guard', NULL)").fetchone()[0]
        # The same worker-thread plans still run outside any authorizer.
        for name, call in (("host_table", "table"), ("host_scalar", "scalar")):
            con.execute(policy_sql(name, port, call))
            before = calls()
            assert con.execute(f"EXECUTE {name}").fetchall() == [(True,)], name
            assert calls() > before, name
        before = calls()
        ok = con.execute(
            "SELECT ok FROM ducknng_request(?, encode('host'), 1000, 0::UBIGINT)",
            [f"http://127.0.0.1:{port}/count"]).fetchone()[0]
        assert ok is not None
        assert calls() == before + 1
        print("prepared authorizer guard: ok", flush=True)
        check_execution_context(con, run_rpc, port)
    finally:
        try:
            if started:
                con.execute("SELECT ducknng_stop_server('prepared_guard')")
        finally:
            con.close()
            server.shutdown()
            server.server_close()
            serving.join()


if __name__ == "__main__":
    main()
