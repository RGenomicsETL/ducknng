#!/usr/bin/env python3
"""
Generate description.yml and functions.yaml for DuckDB community-extension submission.
Run from repo root:
  python3 scripts/generate_community_yaml.py
Output: description.yml (in repo root and community-extensions/extensions/ducknng/)
"""

import json, os, subprocess, sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
EXT_BIN = REPO_ROOT / "build" / "release" / "ducknng.duckdb_extension"
COMMUNITY_DIR = REPO_ROOT / "community-extensions" / "extensions" / "ducknng"
GIT_REF = subprocess.run(
    ["git", "rev-parse", "HEAD"], capture_output=True, text=True, cwd=REPO_ROOT
).stdout.strip()

EXTENSION = {
    "name": "ducknng",
    "description": "Pure C DuckDB extension exposing a DuckDB-backed SQL and RPC server over NNG using Arrow IPC — with framed RPC, custom HTTP routes, TLS support, body codec layer, and admission controls",
    "version": "0.1.0",
    "language": "C",
    "build": "cmake",
    "license": "MIT",
    "requires_toolchains": "python3",
    "excluded_platforms": "wasm_mvp;wasm_eh;wasm_threads",
    "maintainers": ["sounkou-bioinfo"],
}

FUNCTIONS = [
    {
        "name": "ducknng_start_server",
        "kind": "scalar",
        "category": "Server",
        "signature": "ducknng_start_server(name, listen_url, rep_contexts, recv_max_bytes, session_idle_ms, tls_config_id)",
        "returns": "BOOLEAN",
        "description": "Start a named NNG REP server. Supports inproc://, ipc://, tcp://, tls+tcp://, ws://, wss://, http://, and https:// URL schemes. TLS config can point to a self-signed or file-based config created via ducknng_self_signed_tls_config.",
        "examples": [
            "SELECT ducknng_start_server('demo', 'tcp://127.0.0.1:0', 1, 134217728, 30000, 0::UBIGINT);"
        ],
    },
    {
        "name": "ducknng_stop_server",
        "kind": "scalar",
        "category": "Server",
        "signature": "ducknng_stop_server(name)",
        "returns": "BOOLEAN",
        "description": "Stop a named server, close all its pipes, and release its resources.",
        "examples": ["SELECT ducknng_stop_server('demo');"],
    },
    {
        "name": "ducknng_list_servers",
        "kind": "table",
        "category": "Server",
        "signature": "ducknng_list_servers()",
        "returns": "table",
        "description": "List all running servers with their listen URL, TLS mode, and pipe counts.",
        "examples": ["SELECT name, listen, tls_enabled FROM ducknng_list_servers();"],
    },
    {
        "name": "ducknng_open_socket",
        "kind": "scalar",
        "category": "NNG Sockets",
        "signature": "ducknng_open_socket(kind)",
        "returns": "STRUCT(ok, error, nng_error, nng_error_message, socket_id UBIGINT, payload, url)",
        "description": "Open a raw NNG socket of the given kind: req, rep, pub, sub, push, pull, surveyor, respondent, bus, or pair.",
        "examples": ["SELECT ducknng_open_socket('req');"],
    },
    {
        "name": "ducknng_dial_socket",
        "kind": "scalar",
        "category": "NNG Sockets",
        "signature": "ducknng_dial_socket(socket_id, url, timeout_ms, tls_config_id)",
        "returns": "STRUCT(ok, error, nng_error, nng_error_message, socket_id, payload, url)",
        "description": "Dial a socket to a remote listener address.",
        "examples": [
            "SELECT (ducknng_dial_socket(getvariable('sid'), 'tcp://127.0.0.1:12345', 1000, 0::UBIGINT)).ok;"
        ],
    },
    {
        "name": "ducknng_listen_socket",
        "kind": "scalar",
        "category": "NNG Sockets",
        "signature": "ducknng_listen_socket(socket_id, url, flags, tls_config_id)",
        "returns": "STRUCT(ok, error, nng_error, nng_error_message, socket_id, payload, url)",
        "description": "Start listening on a socket at the given URL.",
        "examples": [
            "SELECT ducknng_listen_socket(socket_id, 'tcp://127.0.0.1:0', 0, 0::UBIGINT);"
        ],
    },
    {
        "name": "ducknng_send_socket_raw",
        "kind": "scalar",
        "category": "NNG Sockets",
        "signature": "ducknng_send_socket_raw(socket_id, data, timeout_ms)",
        "returns": "STRUCT(ok, error, nng_error, nng_error_message, socket_id, payload, url)",
        "description": "Send raw bytes on a connected socket. If timeout_ms is negative, the send is non-blocking.",
        "examples": [
            "SELECT (ducknng_send_socket_raw(socket_id, 'hello'::BLOB, 1000)).ok;"
        ],
    },
    {
        "name": "ducknng_recv_socket_raw",
        "kind": "scalar",
        "category": "NNG Sockets",
        "signature": "ducknng_recv_socket_raw(socket_id, timeout_ms)",
        "returns": "STRUCT(ok, error, nng_error, nng_error_message, socket_id, payload BLOB, url)",
        "description": "Receive raw bytes from a connected socket.",
        "examples": ["SELECT ducknng_recv_socket_raw(socket_id, 1000);"],
    },
    {
        "name": "ducknng_close_socket",
        "kind": "scalar",
        "category": "NNG Sockets",
        "signature": "ducknng_close_socket(socket_id)",
        "returns": "STRUCT(ok, error, nng_error, nng_error_message, socket_id, payload, url)",
        "description": "Close a socket and release all its resources.",
        "examples": ["SELECT ducknng_close_socket(socket_id);"],
    },
    {
        "name": "ducknng_subscribe_socket",
        "kind": "scalar",
        "category": "NNG Sockets",
        "signature": "ducknng_subscribe_socket(socket_id, prefix)",
        "returns": "STRUCT(ok, error, nng_error, nng_error_message, socket_id, payload, url)",
        "description": "Subscribe a SUB socket to a topic prefix.",
        "examples": ["SELECT ducknng_subscribe_socket(socket_id, 'topic.'::BLOB);"],
    },
    {
        "name": "ducknng_list_sockets",
        "kind": "table",
        "category": "NNG Sockets",
        "signature": "ducknng_list_sockets()",
        "returns": "table",
        "description": "List all open NNG sockets with their kind and state.",
        "examples": ["SELECT socket_id, kind FROM ducknng_list_sockets();"],
    },
    {
        "name": "ducknng_get_rpc_manifest",
        "kind": "table",
        "category": "Framed RPC",
        "signature": "ducknng_get_rpc_manifest(url, tls_config_id)",
        "returns": "table",
        "description": "Request the RPC method manifest from a remote server.",
        "examples": [
            "SELECT name FROM ducknng_get_rpc_manifest('tcp://127.0.0.1:12345', 0::UBIGINT);"
        ],
    },
    {
        "name": "ducknng_query_rpc",
        "kind": "table",
        "category": "Framed RPC",
        "signature": "ducknng_query_rpc(url, sql, tls_config_id)",
        "returns": "table",
        "description": "Execute a SQL statement on a remote server and return the result rows. For SELECT, opens a session, fetches the first batch, and auto-closes. For DML, returns rows_changed.",
        "examples": [
            "SELECT * FROM ducknng_query_rpc('tcp://127.0.0.1:12345', 'SELECT 42 AS answer', 0::UBIGINT);"
        ],
    },
    {
        "name": "ducknng_run_rpc",
        "kind": "table",
        "category": "Framed RPC",
        "signature": "ducknng_run_rpc(url, sql, tls_config_id)",
        "returns": "table",
        "description": "Execute SQL on a remote server using the exec method. Returns rows_changed and statement metadata.",
        "examples": [
            "SELECT * FROM ducknng_run_rpc('tcp://127.0.0.1:12345', 'CREATE TABLE t AS SELECT 1 AS a', 0::UBIGINT);"
        ],
    },
    {
        "name": "ducknng_open_query",
        "kind": "table",
        "category": "Framed RPC",
        "signature": "ducknng_open_query(url, sql, batch_rows, batch_bytes, tls_config_id)",
        "returns": "table",
        "description": "Open a query session on a remote server. Returns the session_id and session_token needed for fetch/close/cancel.",
        "examples": [
            "SELECT session_id, state FROM ducknng_open_query('tcp://127.0.0.1:12345', 'SELECT range AS i FROM range(10)', 0::UBIGINT, 0::UBIGINT, 0::UBIGINT);"
        ],
    },
    {
        "name": "ducknng_fetch_query_table",
        "kind": "table",
        "category": "Framed RPC",
        "signature": "ducknng_fetch_query_table(url, session_id, session_token, batch_rows, batch_bytes, tls_config_id)",
        "returns": "table",
        "description": "Fetch the next batch of rows from an open query session. Returns decoded Arrow IPC rows directly.",
        "examples": [
            "SELECT * FROM ducknng_fetch_query_table('tcp://127.0.0.1:12345', 1, 'abc123', 0::UBIGINT, 0::UBIGINT, 0::UBIGINT);"
        ],
    },
    {
        "name": "ducknng_close_query",
        "kind": "table",
        "category": "Framed RPC",
        "signature": "ducknng_close_query(url, session_id, session_token, tls_config_id)",
        "returns": "table",
        "description": "Close a remote query session and release its server-side resources.",
        "examples": [
            "SELECT * FROM ducknng_close_query('tcp://127.0.0.1:12345', 1, 'abc123', 0::UBIGINT);"
        ],
    },
    {
        "name": "ducknng_cancel_query",
        "kind": "table",
        "category": "Framed RPC",
        "signature": "ducknng_cancel_query(url, session_id, session_token, tls_config_id)",
        "returns": "table",
        "description": "Cancel a running query on a remote session.",
        "examples": [
            "SELECT * FROM ducknng_cancel_query('tcp://127.0.0.1:12345', 1, 'abc123', 0::UBIGINT);"
        ],
    },
    {
        "name": "ducknng_decode_frame",
        "kind": "table",
        "category": "Framed RPC",
        "signature": "ducknng_decode_frame(data)",
        "returns": "table",
        "description": "Decode a ducknng protocol frame into its envelope fields: type, name, payload, payload_text, error_text.",
        "examples": [
            "SELECT type_name, payload_text FROM ducknng_decode_frame(getvariable('frame'));"
        ],
    },
    {
        "name": "ducknng_request_raw",
        "kind": "scalar",
        "category": "Framed RPC",
        "signature": "ducknng_request_raw(url, data, timeout_ms, tls_config_id)",
        "returns": "BLOB",
        "description": "Send a raw NNG request and return the reply as a BLOB frame.",
        "examples": [
            "SELECT ducknng_request_raw('tcp://127.0.0.1:12345', from_hex('01000000...'), 1000, 0::UBIGINT);"
        ],
    },
    {
        "name": "ducknng_ncurl",
        "kind": "table",
        "category": "HTTP Client",
        "signature": "ducknng_ncurl(url, method, headers_json, body, timeout_ms, tls_config_id)",
        "returns": "table",
        "description": "Perform an HTTP request and return the full response including status, headers, body BLOB, and body_text.",
        "examples": [
            "SELECT ok, status, body_text FROM ducknng_ncurl('http://127.0.0.1:8080/healthz', 'GET', NULL, NULL, 2000, 0::UBIGINT);"
        ],
    },
    {
        "name": "ducknng_ncurl_table",
        "kind": "table",
        "category": "HTTP Client",
        "signature": "ducknng_ncurl_table(url, method, headers_json, body, timeout_ms, tls_config_id)",
        "returns": "table",
        "description": "Perform an HTTP request and parse the response body through the built-in codec layer based on Content-Type. Requires a 2xx status.",
        "examples": [
            "SELECT * FROM ducknng_ncurl_table('http://127.0.0.1:8080/data.csv', 'GET', NULL, NULL, 2000, 0::UBIGINT);"
        ],
    },
    {
        "name": "ducknng_parse_body",
        "kind": "table",
        "category": "Body Codecs",
        "signature": "ducknng_parse_body(body, content_type)",
        "returns": "table",
        "description": "Parse a BLOB using the supplied content type. Supports JSON, NDJSON, CSV, TSV, Parquet, Arrow IPC, form-urlencoded, and ducknng frames.",
        "examples": [
            "SELECT * FROM ducknng_parse_body(getvariable('body'), 'application/json');"
        ],
    },
    {
        "name": "ducknng_parse_csv",
        "kind": "table",
        "category": "Body Codecs",
        "signature": "ducknng_parse_csv(body)",
        "returns": "table",
        "description": "Parse a CSV BLOB using DuckDB's read_csv_auto via a tempfile round-trip.",
        "examples": ["SELECT * FROM ducknng_parse_csv(getvariable('csv_body'));"],
    },
    {
        "name": "ducknng_parse_tsv",
        "kind": "table",
        "category": "Body Codecs",
        "signature": "ducknng_parse_tsv(body)",
        "returns": "table",
        "description": "Parse a TSV BLOB using DuckDB's read_csv_auto(delim='\t') via a tempfile round-trip.",
        "examples": ["SELECT * FROM ducknng_parse_tsv(getvariable('tsv_body'));"],
    },
    {
        "name": "ducknng_parse_parquet",
        "kind": "table",
        "category": "Body Codecs",
        "signature": "ducknng_parse_parquet(body)",
        "returns": "table",
        "description": "Parse a Parquet BLOB using DuckDB's read_parquet via a tempfile round-trip.",
        "examples": [
            "SELECT * FROM ducknng_parse_parquet(getvariable('parquet_body'));"
        ],
    },
    {
        "name": "ducknng_list_codecs",
        "kind": "table",
        "category": "Body Codecs",
        "signature": "ducknng_list_codecs()",
        "returns": "table",
        "description": "List all built-in and user-registered body codec providers.",
        "examples": ["SELECT * FROM ducknng_list_codecs();"],
    },
    {
        "name": "ducknng_register_codec",
        "kind": "scalar",
        "category": "Body Codecs",
        "signature": "ducknng_register_codec(content_type, function_name)",
        "returns": "BOOLEAN",
        "description": "Register a scalar SQL function as a custom body codec for the given content type. The function receives a BLOB and returns VARCHAR.",
        "examples": [
            "SELECT ducknng_register_codec('application/x-custom', 'my_codec_fn');"
        ],
    },
    {
        "name": "ducknng_unregister_codec",
        "kind": "scalar",
        "category": "Body Codecs",
        "signature": "ducknng_unregister_codec(content_type)",
        "returns": "BOOLEAN",
        "description": "Unregister a user-registered body codec, restoring the built-in behavior for that content type.",
        "examples": ["SELECT ducknng_unregister_codec('application/x-custom');"],
    },
    {
        "name": "ducknng_register_http_route",
        "kind": "scalar",
        "category": "HTTP Routes",
        "signature": "ducknng_register_http_route(service_name, method, path, handler_sql)",
        "returns": "BOOLEAN",
        "description": "Register an exact-match HTTP route backed by a SQL handler. The handler SQL must return columns: status, content_type, body or body_text.",
        "examples": [
            "SELECT ducknng_register_http_route('svc', 'GET', '/healthz', 'SELECT 200 AS status, ''text/plain'' AS content_type, ''ok'' AS body_text');"
        ],
    },
    {
        "name": "ducknng_register_http_route_pattern",
        "kind": "scalar",
        "category": "HTTP Routes",
        "signature": "ducknng_register_http_route_pattern(service_name, method, match_kind, path, handler_sql)",
        "returns": "BOOLEAN",
        "description": "Register an HTTP route with prefix or template matching. match_kind is 'prefix' or 'template'. Template paths support {param} placeholders accessible via ducknng_http_path_param.",
        "examples": [
            "SELECT ducknng_register_http_route_pattern('svc', 'GET', 'prefix', '/api/', 'SELECT 200 AS status, ''text/plain'' AS content_type, ''prefix hit'' AS body_text');"
        ],
    },
    {
        "name": "ducknng_add_stream_route",
        "kind": "scalar",
        "category": "HTTP Routes",
        "signature": "ducknng_add_stream_route(service_name, method, path, handler_sql, content_type)",
        "returns": "BOOLEAN",
        "description": "Register a chunked-streaming HTTP route. The handler SQL must return a 'chunk' column. Each row is written as a separate HTTP chunk. Default content-type is text/event-stream.",
        "examples": [
            "SELECT ducknng_add_stream_route('svc', 'GET', '/events', 'SELECT ducknng_format_sse(''tick '' || i::VARCHAR) AS chunk FROM range(1,4) t(i)');"
        ],
    },
    {
        "name": "ducknng_format_sse",
        "kind": "scalar_macro",
        "category": "HTTP Routes",
        "signature": "ducknng_format_sse(data, event := NULL, id := NULL, retry := NULL)",
        "returns": "VARCHAR",
        "description": "Format a Server-Sent Events event string with optional event type, id, and retry fields.",
        "examples": ["SELECT ducknng_format_sse('hello', event := 'update');"],
    },
    {
        "name": "ducknng_register_http_static",
        "kind": "scalar",
        "category": "HTTP Routes",
        "signature": "ducknng_register_http_static(service_name, url_prefix, directory_path)",
        "returns": "BOOLEAN",
        "description": "Serve static files from a directory under a URL prefix.",
        "examples": [
            "SELECT ducknng_register_http_static('svc', '/files/', '/var/www');"
        ],
    },
    {
        "name": "ducknng_list_http_routes",
        "kind": "table",
        "category": "HTTP Routes",
        "signature": "ducknng_list_http_routes()",
        "returns": "table",
        "description": "List all registered HTTP routes with their method, path, match kind, auth policy, and streaming mode.",
        "examples": [
            "SELECT service_name, method, path, is_stream FROM ducknng_list_http_routes();"
        ],
    },
    {
        "name": "ducknng_register_http_worker",
        "kind": "scalar",
        "category": "HTTP Workers",
        "signature": "ducknng_register_http_worker(service_name, worker_name, sql, interval_ms)",
        "returns": "BOOLEAN",
        "description": "Register a background worker that executes SQL on a recurring interval (milliseconds).",
        "examples": [
            "SELECT ducknng_register_http_worker('svc', 'cleanup', 'DELETE FROM logs WHERE ts < now() - interval ''1 day''', 3600000);"
        ],
    },
    {
        "name": "ducknng_self_signed_tls_config",
        "kind": "scalar",
        "category": "TLS",
        "signature": "ducknng_self_signed_tls_config(host, days, auth_mode)",
        "returns": "UBIGINT",
        "description": "Generate a self-signed TLS certificate in memory. auth_mode: 0=none, 1=server, 2=mutual. Returns a TLS config handle.",
        "examples": [
            "SELECT ducknng_self_signed_tls_config('127.0.0.1', 365, 0) AS tls_id;"
        ],
    },
    {
        "name": "ducknng_tls_config_from_files",
        "kind": "scalar",
        "category": "TLS",
        "signature": "ducknng_tls_config_from_files(cert_path, key_path, ca_path, auth_mode)",
        "returns": "UBIGINT",
        "description": "Create a TLS config from PEM files.",
        "examples": [
            "SELECT ducknng_tls_config_from_files('cert.pem', 'key.pem', 'ca.pem', 2) AS tls_id;"
        ],
    },
    {
        "name": "ducknng_drop_tls_config",
        "kind": "scalar",
        "category": "TLS",
        "signature": "ducknng_drop_tls_config(tls_config_id)",
        "returns": "BOOLEAN",
        "description": "Drop a TLS config and release its certificate memory.",
        "examples": ["SELECT ducknng_drop_tls_config(getvariable('tls_id'));"],
    },
    {
        "name": "ducknng_read_monitor",
        "kind": "table",
        "category": "Monitoring",
        "signature": "ducknng_read_monitor(name, after_seq, max_events)",
        "returns": "table",
        "description": "Read pipe events from a service's event monitor ring buffer.",
        "examples": [
            "SELECT seq, event_type FROM ducknng_read_monitor('svc', 0, 100);"
        ],
    },
    {
        "name": "ducknng_monitor_status",
        "kind": "table",
        "category": "Monitoring",
        "signature": "ducknng_monitor_status(name)",
        "returns": "table",
        "description": "Return monitor ring buffer capacity, counts, and active pipe metrics.",
        "examples": [
            "SELECT active_pipes, dropped_events FROM ducknng_monitor_status('svc');"
        ],
    },
    {
        "name": "ducknng_list_pipes",
        "kind": "table",
        "category": "Monitoring",
        "signature": "ducknng_list_pipes(name)",
        "returns": "table",
        "description": "List currently open NNG pipes for a service.",
        "examples": ["SELECT pipe_id, remote_addr FROM ducknng_list_pipes('svc');"],
    },
    {
        "name": "ducknng_log_entries",
        "kind": "table",
        "category": "Monitoring",
        "signature": "ducknng_log_entries()",
        "returns": "table",
        "description": "Read the DuckDB log ring buffer captured via the logger API.",
        "examples": [
            "SELECT timestamp, message FROM ducknng_log_entries() ORDER BY timestamp;"
        ],
    },
]


def hello_world():
    """Construct hello_world SQL with optional rendered output."""
    sql = """    -- Load the extension
    LOAD ducknng;

    -- Start an inproc REP server and run remote SQL
    SELECT ducknng_start_server('demo', 'inproc://ducknng_demo', 1, 134217728, 30000, 0::UBIGINT);

    -- ducknng_query_rpc returns the actual result rows
    SELECT *
    FROM ducknng_query_rpc('inproc://ducknng_demo', 'SELECT 42 AS answer', 0::UBIGINT);

    SELECT ducknng_stop_server('demo');"""

    if not EXT_BIN.exists():
        return sql

    try:
        result = subprocess.run(
            [
                "/usr/local/bin/duckdb152",
                "-unsigned",
                "-c",
                EXT_BIN.as_uri()
                if os.name == "nt"
                else f"LOAD '{EXT_BIN}';"
                + "; ".join(
                    [
                        "SELECT ducknng_start_server('demo', 'inproc://ducknng_demo', 1, 134217728, 30000, 0::UBIGINT)",
                        "SELECT * FROM ducknng_query_rpc('inproc://ducknng_demo', 'SELECT 42 AS answer', 0::UBIGINT)",
                        "SELECT ducknng_stop_server('demo')",
                    ]
                ),
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        raw = result.stdout
    except Exception:
        return sql

    # Extract the three box-drawing tables
    tables = []
    current = []
    in_table = False
    for line in raw.splitlines():
        if line.startswith("┌"):
            in_table = True
            current = [line]
        elif line.startswith("└") and in_table:
            current.append(line)
            tables.append("\n".join(current))
            current = []
            in_table = False
        elif in_table:
            current.append(line)

    out = []
    out.append("    -- Load the extension")
    out.append("    LOAD ducknng;")
    out.append("")
    out.append("    -- Start an inproc REP server and run remote SQL")
    out.append(
        "    SELECT ducknng_start_server('demo', 'inproc://ducknng_demo', 1, 134217728, 30000, 0::UBIGINT);"
    )
    out.append("")
    if len(tables) > 0:
        for tline in tables[0].splitlines():
            out.append(f"    {tline}")
    out.append("")
    out.append("    -- ducknng_query_rpc returns the actual result rows")
    out.append("    SELECT *")
    out.append(
        "    FROM ducknng_query_rpc('inproc://ducknng_demo', 'SELECT 42 AS answer', 0::UBIGINT);"
    )
    out.append("")
    if len(tables) > 1:
        for tline in tables[1].splitlines():
            out.append(f"    {tline}")
    out.append("")
    out.append("    SELECT ducknng_stop_server('demo');")
    out.append("")
    if len(tables) > 2:
        for tline in tables[2].splitlines():
            out.append(f"    {tline}")
    return "\n".join(out)


def dump_yaml_key_value(val, indent=0, prefix=""):
    sp = "  " * indent
    if isinstance(val, bool):
        return f"{sp}{prefix}{str(val).lower()}"
    elif isinstance(val, str):
        if "\n" in val:
            return f"{sp}{prefix}|\n" + "\n".join(f"{sp}{l}" for l in val.splitlines())
        elif ":" in val or any(c in val for c in "{}[]&*?|>!%@`,"):
            return f'{sp}{prefix}"{val}"'
        else:
            return f"{sp}{prefix}{val}"
    elif isinstance(val, list):
        items = []
        for v in val:
            items.append(f"{sp}{prefix}- {v}")
        return "\n".join(items)
    elif isinstance(val, dict):
        return dump_yaml_dict(val, indent)
    elif val is None:
        return f"{sp}{prefix}~"
    else:
        return f"{sp}{prefix}{val}"


def dump_yaml_dict(d, indent=0):
    """Format a dict as YAML key: value pairs."""
    sp = "  " * indent
    lines = []
    for k, v in d.items():
        if isinstance(v, dict):
            lines.append(f"{sp}{k}:")
            lines.append(dump_yaml_dict(v, indent + 1))
        elif isinstance(v, list) and v and any(isinstance(x, dict) for x in v):
            lines.append(f"{sp}{k}:")
            for item in v:
                lines.append(f"{sp}-")
                lines.append(dump_yaml_dict(item, indent + 2))
        else:
            lines.append(dump_yaml_key_value(v, indent, f"{k}: "))
    return "\n".join(lines)


def build_description_yml():
    desc = {
        "extension": EXTENSION,
        "repo": {"github": f"{EXTENSION['maintainers'][0]}/ducknng", "ref": GIT_REF},
        "docs": {
            "hello_world": hello_world(),
            "extended_description": """    ducknng is a pure C DuckDB extension that exposes a DuckDB-backed SQL and
    RPC server over NNG (Nanomsg Next Generation) using Arrow IPC with nanoarrow C
    for payload encoding and decoding.

    **Transport layer (NNG)**
    Supports inproc://, ipc://, tcp://, and tls+tcp:// URLs. TLS certificates
    can be loaded from file paths or in-memory PEM content; self-signed dev
    certificates are generated entirely inside the extension (no file I/O).

    **Framed RPC**
    Versioned request/reply envelope with manifest, exec, query session
    (open/fetch/close/cancel), and raw unary operations. All tabular data
    is encoded as Arrow IPC streams.

    **HTTP carrier**
    Start a server on an http:// or https:// URL for the framed RPC mount.
    Register custom HTTP routes (exact, prefix, or template matching) backed
    by SQL queries. Streaming chunked routes for Server-Sent Events are
    supported via ducknng_add_stream_route. Static asset serving, route-local
    auth policies, and background workers are also available.

    **Body codec layer**
    Parse HTTP response bodies by content type: JSON, NDJSON, CSV, TSV,
    Parquet, Arrow IPC, form-urlencoded, and ducknng frames. Standalone
    ducknng_parse_csv(body), ducknng_parse_tsv(body), and
    ducknng_parse_parquet(body) functions use DuckDB's standard readers
    via a tempfile round-trip. User-registered codec hooks extend the set.

    **Admission & security**
    mTLS peer-identity extraction, exact identity allowlists, IP/CIDR
    allowlists, per-service and per-principal resource limits (max memory,
    max sessions, max result bytes), and SQL authorizer callbacks.

    **Development & testing**
    Built against the DuckDB C API (no C++). Uses DuckDB's stable and
    unstable C extensions API for Arrow conversion. 20+ SQL integration
    tests run via sqllogictest. Cross-platform (Linux, macOS, Windows)
    via the extension-ci-tools CMake build system.

    Project details and examples: https://github.com/sounkou-bioinfo/ducknng

    Community package excludes WASM targets (NNG threading requirement).""",
        },
    }
    return dump_yaml_dict(desc) + "\n"


def build_functions_json():
    """functions.yaml is JSON (despite the .yaml extension) as used by duckhts."""
    doc = {
        "manifest_version": 1,
        "community_extension": {
            "extension": EXTENSION,
            "repo": {
                "github": f"{EXTENSION['maintainers'][0]}/ducknng",
                "ref_source": "git_head",
            },
            "docs": {
                "hello_world_lines": [
                    "LOAD ducknng;",
                    "",
                    "SELECT ducknng_start_server('demo', 'inproc://ducknng_demo', 1, 134217728, 30000, 0::UBIGINT);",
                    "",
                    "SELECT * FROM ducknng_query_rpc('inproc://ducknng_demo', 'SELECT 42 AS answer', 0::UBIGINT);",
                    "",
                    "SELECT ducknng_stop_server('demo');",
                ],
                "extended_intro": [
                    "ducknng is a pure C DuckDB extension that exposes a DuckDB-backed SQL and RPC server over NNG (Nanomsg Next Generation) using Arrow IPC with nanoarrow C for payload encoding and decoding.",
                ],
                "feature_notes": [
                    "Only linux_amd64 and osx_amd64/osx_arm64 are supported; WASM targets cannot use NNG threading.",
                    "The extension uses both stable and unstable DuckDB C extension APIs for Arrow conversion.",
                    "TLS configs accept PEM text (in-memory, no file I/O) or file paths.",
                    "HTTP transport does not yet support http/2.",
                ],
            },
        },
        "functions": FUNCTIONS,
    }
    return json.dumps(doc, indent=2) + "\n"


def main():
    # Build description.yml
    desc = build_description_yml()
    (REPO_ROOT / "description.yml").write_text(desc)
    print(f"Wrote description.yml ({len(desc)} bytes)")

    # Build functions.yaml (JSON format per community-extensions convention)
    fn_json = build_functions_json()
    (REPO_ROOT / "functions.yaml").write_text(fn_json)
    print(f"Wrote functions.yaml ({len(fn_json)} bytes)")

    # Write to community-extensions directory too
    COMMUNITY_DIR.mkdir(parents=True, exist_ok=True)
    (COMMUNITY_DIR / "description.yml").write_text(desc)
    print(f"Wrote community-extensions/extensions/ducknng/description.yml")

    # Also write functions.yaml to the pr branch
    pr_functions_dir = Path("/tmp/community-extensions/extensions/ducknng")
    pr_functions_dir.mkdir(parents=True, exist_ok=True)
    (pr_functions_dir / "description.yml").write_text(desc)
    print(
        f"Wrote /tmp/community-extensions/extensions/ducknng/description.yml (for PR)"
    )


if __name__ == "__main__":
    main()
