
<!-- README.md is generated from README.Rmd using [duckknit](https://github.com/rundel/duckknit). Please edit that file -->

# ducknng

`ducknng` is a pure C DuckDB extension that exposes DuckDB SQL and
manifest-declared RPC over [NNG](https://nng.nanomsg.org/) transports
and HTTP/HTTPS carriers. A SQL session can call another DuckDB session
across `inproc://`, `ipc://`, `tcp://`, `tls+tcp://`, `ws://`, `wss://`,
`http://`, or `https://`, and RPC row payloads ride on Arrow IPC.

It draws on two R packages.
[`nanonext`](https://github.com/r-lib/nanonext) supplies the socket and
AIO ergonomics; [`mangoro`](https://github.com/sounkou-bioinfo/mangoro)
supplies the thin versioned envelope. The DuckDB integration keeps
generated SQL injection-safe but assumes the deployment owns the trust
boundary — see **Deployment and admission policy** below.

### How the pieces stack

The extension is one runtime with four layers; later sections walk
through each in turn.

1.  **Transport.** NNG sockets and listeners across the full transport
    family. Synchronous send/recv plus first-class one-shot AIO handles.
    Per-service pipe-event telemetry (`ducknng_read_monitor(...)`,
    `ducknng_list_pipes(...)`).
2.  **Framed RPC.** A versioned envelope carries Arrow IPC table
    payloads or JSON control text. Servers come up with
    `ducknng_start_server(...)`, and the listen URL scheme chooses NNG
    or HTTP/HTTPS. HTTP/HTTPS stays on the same server entrypoint with
    `contexts = 1`. Clients call by URL; synchronous helpers route by
    scheme, AIO helpers expose the same calls as futures.
3.  **Policy.** Manifest-driven method registry with opt-in `exec`. Fast
    C admission for required mTLS, exact peer-identity allowlists,
    IP/CIDR allowlists, and service limits. Optional SQL authorizers run
    at the request boundary and read context through
    `ducknng_auth_context()`. Sessions are bearer-token owned,
    additionally bound to verified mTLS identity when present. The
    default execution model is `shared_serialized_connection`, with
    `service_serialized_connection` and `request_connection` available
    through `ducknng_set_service_execution_model(...)`; the current
    model is exposed in `manifest` and `ducknng_list_servers()`.
4.  **Codecs.** `ducknng_parse_body(...)` and `ducknng_ncurl_table(...)`
    parse content-type-tagged BLOBs. Built-in providers cover JSON,
    Arrow IPC, ducknng frames, and a text/raw fallback. Deployments can
    override or extend the set with
    `ducknng_register_codec(content_type, function_name)`; the function
    takes `BLOB` and returns `VARCHAR`.

`ducknng` is intentionally low-level. Long-lived runtime handles —
servers, sockets, AIO, TLS configs, query sessions — are **manually
managed**. Stop, close, or drop them explicitly; runtime teardown is
fallback cleanup, not the primary lifecycle.

## Quick tour

Build the extension:

``` sh
make configure
make release
```

Load it, start an IPC service, ask the server its name through the
built-in `manifest` method, and stop it again:

``` sql
-- args: name, listen URL, REP-context count, recv_max_bytes,
--       session_idle_ms, tls_config_id (0 = plaintext).
SELECT ducknng_start_server(
  'hello', 'ipc:///tmp/ducknng_hello.ipc',
  1, 134217728, 300000, 0::UBIGINT
);
-- The manifest column is JSON text held as VARCHAR; cast to JSON to drill in.
SELECT json_extract_string(manifest::JSON, '$.server.name') AS server_name,
       json_array_length(json_extract(manifest::JSON, '$.methods')) AS methods
FROM ducknng_get_rpc_manifest('ipc:///tmp/ducknng_hello.ipc', 0::UBIGINT)
WHERE ok;
SELECT ducknng_stop_server('hello');
+-----------------------------------------------------------------------------------------------------------+
| ducknng_start_server('hello', 'ipc:///tmp/ducknng_hello.ipc', 1, 134217728, 300000, CAST(0 AS "UBIGINT")) |
+-----------------------------------------------------------------------------------------------------------+
| true                                                                                                      |
+-----------------------------------------------------------------------------------------------------------+
+-------------+---------+
| server_name | methods |
+-------------+---------+
| ducknng     | 5       |
+-------------+---------+
+------------------------------+
| ducknng_stop_server('hello') |
+------------------------------+
| true                         |
+------------------------------+
```

Point `ducknng_start_server(...)` at an `http://` or `https://` URL with
`contexts = 1` and the same RPC methods are mounted on the HTTP carrier.
The **Examples** section walks through transport patterns, AIO, the
client surface, body codecs, and sessions in that order.

## Lifetime and manual cleanup

DuckDB’s extension API does give `ducknng` destroy callbacks for
internal function-registration, bind, and init state. What DuckDB SQL
does **not** give this project is a general-purpose GC/finalizer model
for long-lived SQL handles like R external pointer finalizers. So at the
public SQL surface, long-lived handles are manually managed.

You should explicitly clean up what you create:

- stop servers with `ducknng_stop_server(...)`
- close sockets with `ducknng_close_socket(...)`
- drop aio handles with `ducknng_aio_drop(...)`
- drop TLS config handles with `ducknng_drop_tls_config(...)`
- close query sessions with `ducknng_close_query(...)`

Important details:

- `ducknng_aio_cancel(...)` is cancellation control, not the destructor
- `ducknng_cancel_query(...)` is best-effort session control, not a
  replacement for the normal explicit close path
- runtime teardown is fallback cleanup, not the primary lifecycle model
  for a long-lived DuckDB process

For the deeper write-ups see `docs/lifetime.md`, `docs/protocol.md`,
`docs/manifest.md`, `docs/security.md`, `docs/registry.md`,
`docs/transports.md`, `docs/http.md`, `docs/http_server_framework.md`,
`docs/codecs.md`, and `docs/types.md`. A multi-process subscriber
gateway sketch now lives in `docs/subscriber_gateway_demo.md`; the older
non-sealed routing note remains in `docs/mesh_routing_demo.md`.
`NEWS.md` summarizes landed changes. TLS supports both file-backed and
in-memory PEM material (`ducknng_tls_config_from_files(...)`,
`ducknng_tls_config_from_pem(...)`,
`ducknng_self_signed_tls_config(...)`); `auth_mode = 2` enables required
mTLS peer verification for `tls+tcp://`, `wss://`, and `https://`
services.

## Function catalog

The generated catalog is the public SQL surface. Any loaded `ducknng__*`
helper names are internal macro implementation details required by
DuckDB’s stable C API path and are not supported user APIs.

<details>
<summary>
<strong>Expand the full generated function catalog</strong>
</summary>

# Function Catalog

This file is generated from `function_catalog/functions.yaml`.

## Service Control

| name                                  | kind   | arguments                                                                                     | returns   | description                                                                      |
|---------------------------------------|--------|-----------------------------------------------------------------------------------------------|-----------|----------------------------------------------------------------------------------|
| `ducknng_start_server`                | scalar | `name, listen, contexts, recv_max_bytes, session_idle_ms, tls_config_id[, ip_allowlist_json]` | `BOOLEAN` | Start a named ducknng service and choose the carrier from the listen URL scheme. |
| `ducknng_stop_server`                 | scalar | `name`                                                                                        | `BOOLEAN` | Stop a named ducknng service.                                                    |
| `ducknng_set_service_execution_model` | scalar | `name, model`                                                                                 | `BOOLEAN` | Set the DuckDB connection execution model used by service-side SQL.              |

## Introspection

| name                     | kind  | arguments                     | returns                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | description                                                                     |
|--------------------------|-------|-------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| `ducknng_list_servers`   | table |                               | `TABLE(service_id UBIGINT, name VARCHAR, listen VARCHAR, contexts INTEGER, running BOOLEAN, execution_model VARCHAR, sessions UBIGINT, active_pipes UBIGINT, max_open_sessions UBIGINT, max_active_pipes UBIGINT, inflight_requests UBIGINT, max_inflight_requests UBIGINT, max_sessions_per_peer_identity UBIGINT, tls_enabled BOOLEAN, tls_auth_mode INTEGER, peer_identity_required BOOLEAN, peer_allowlist_active BOOLEAN, ip_allowlist_active BOOLEAN, sql_authorizer_active BOOLEAN, peer_allowlist_count UBIGINT, ip_allowlist_count UBIGINT)` | List registered ducknng services.                                               |
| `ducknng_read_monitor`   | table | `name, after_seq, max_events` | `TABLE(seq UBIGINT, ts_ms UBIGINT, pipe_id UBIGINT, service_name VARCHAR, listen VARCHAR, transport_family VARCHAR, scheme VARCHAR, event VARCHAR, admitted BOOLEAN, reason VARCHAR, remote_addr VARCHAR, remote_ip VARCHAR, remote_port INTEGER, peer_identity VARCHAR)`                                                                                                                                                                                                                                                                             | Read the bounded per-service NNG pipe monitor event stream.                     |
| `ducknng_monitor_status` | table | `name`                        | `TABLE(service_name VARCHAR, event_capacity UBIGINT, event_count UBIGINT, oldest_seq UBIGINT, newest_seq UBIGINT, dropped_events UBIGINT, active_pipes UBIGINT, max_active_pipes UBIGINT)`                                                                                                                                                                                                                                                                                                                                                            | Return pipe monitor ring status and active-pipe counters for a running service. |
| `ducknng_list_pipes`     | table | `name`                        | `TABLE(pipe_id UBIGINT, opened_ms UBIGINT, service_name VARCHAR, listen VARCHAR, transport_family VARCHAR, scheme VARCHAR, remote_addr VARCHAR, remote_ip VARCHAR, remote_port INTEGER, peer_identity VARCHAR)`                                                                                                                                                                                                                                                                                                                                       | List currently active NNG pipes for a running service.                          |

## Method Registry

| name                           | kind   | arguments             | returns                                                                                                                                                                                                                                                                       | description                                                                            |
|--------------------------------|--------|-----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------|
| `ducknng_register_exec_method` | scalar | `[requires_auth]`     | `BOOLEAN`                                                                                                                                                                                                                                                                     | Register the built-in exec RPC method explicitly.                                      |
| `ducknng_set_method_auth`      | scalar | `name, requires_auth` | `BOOLEAN`                                                                                                                                                                                                                                                                     | Set descriptor-level verified-peer-identity authorization for a registered RPC method. |
| `ducknng_unregister_method`    | scalar | `name`                | `BOOLEAN`                                                                                                                                                                                                                                                                     | Unregister a method from the runtime registry.                                         |
| `ducknng_unregister_family`    | scalar | `family`              | `UBIGINT`                                                                                                                                                                                                                                                                     | Unregister all methods in a family and return the number removed.                      |
| `ducknng_list_methods`         | table  |                       | `TABLE(name VARCHAR, family VARCHAR, summary VARCHAR, transport_pattern VARCHAR, request_payload_format VARCHAR, response_payload_format VARCHAR, response_mode VARCHAR, request_schema_json VARCHAR, response_schema_json VARCHAR, requires_auth BOOLEAN, disabled BOOLEAN)` | List the currently registered RPC methods in the runtime registry.                     |

## Primitive Transport

| name                          | kind   | arguments                                       | returns                                                                                                                                                         | description                                                                                         |
|-------------------------------|--------|-------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| `ducknng_open_socket`         | scalar | `protocol`                                      | `STRUCT(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, socket_id UBIGINT, payload BLOB, url VARCHAR)`                                 | Open a client socket handle for a supported NNG protocol.                                           |
| `ducknng_dial_socket`         | scalar | `socket_id, url, timeout_ms, tls_config_id`     | `STRUCT(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, socket_id UBIGINT, payload BLOB, url VARCHAR)`                                 | Dial a URL using an opened socket handle.                                                           |
| `ducknng_listen_socket`       | scalar | `socket_id, url, recv_max_bytes, tls_config_id` | `STRUCT(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, socket_id UBIGINT, payload BLOB, url VARCHAR)`                                 | Bind a socket handle to a listen URL and start its NNG listener.                                    |
| `ducknng_close_socket`        | scalar | `socket_id`                                     | `STRUCT(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, socket_id UBIGINT, payload BLOB, url VARCHAR)`                                 | Close a client socket handle.                                                                       |
| `ducknng_send_socket_raw`     | scalar | `socket_id, frame, timeout_ms`                  | `STRUCT(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, socket_id UBIGINT, payload BLOB, url VARCHAR)`                                 | Send one raw frame through an active socket handle.                                                 |
| `ducknng_recv_socket_raw`     | scalar | `socket_id, timeout_ms`                         | `STRUCT(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, socket_id UBIGINT, payload BLOB, url VARCHAR)`                                 | Receive one raw frame from an active socket handle.                                                 |
| `ducknng_subscribe_socket`    | scalar | `socket_id, topic`                              | `STRUCT(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, socket_id UBIGINT, payload BLOB, url VARCHAR)`                                 | Register a raw topic prefix on a sub socket.                                                        |
| `ducknng_unsubscribe_socket`  | scalar | `socket_id, topic`                              | `STRUCT(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, socket_id UBIGINT, payload BLOB, url VARCHAR)`                                 | Remove a raw topic prefix from a sub socket.                                                        |
| `ducknng_list_sockets`        | table  |                                                 | `TABLE(socket_id UBIGINT, protocol VARCHAR, url VARCHAR, open BOOLEAN, connected BOOLEAN, listening BOOLEAN, send_timeout_ms INTEGER, recv_timeout_ms INTEGER)` | List client socket handles in the runtime.                                                          |
| `ducknng_request`             | table  | `url, payload, timeout_ms, tls_config_id`       | `TABLE(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, payload BLOB)`                                                                  | Perform a one-shot raw request and return a structured result row.                                  |
| `ducknng_request_socket`      | table  | `socket_id, payload, timeout_ms`                | `TABLE(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, payload BLOB)`                                                                  | Perform a raw request through a previously dialed socket handle and return a structured result row. |
| `ducknng_request_raw`         | scalar | `url, payload, timeout_ms, tls_config_id`       | `BLOB`                                                                                                                                                          | Perform a one-shot raw request and return the raw reply frame bytes.                                |
| `ducknng_request_socket_raw`  | scalar | `socket_id, payload, timeout_ms`                | `BLOB`                                                                                                                                                          | Perform a raw request through a dialed socket handle and return the raw reply frame bytes.          |
| `ducknng_decode_frame`        | table  | `frame`                                         | `TABLE(ok BOOLEAN, error VARCHAR, version UTINYINT, type UTINYINT, flags UINTEGER, type_name VARCHAR, name VARCHAR, payload BLOB, payload_text VARCHAR)`        | Decode a raw ducknng frame into envelope fields and extracted payload columns.                      |
| `ducknng_frame_payload`       | scalar | `frame`                                         | `BLOB`                                                                                                                                                          | Extract the payload bytes from one raw ducknng frame.                                               |
| `ducknng_frame_payload_text`  | scalar | `frame`                                         | `VARCHAR`                                                                                                                                                       | Extract the payload as UTF-8 text when a raw ducknng frame carries a textual payload.               |
| `ducknng_frame_error_text`    | scalar | `frame`                                         | `VARCHAR`                                                                                                                                                       | Extract the protocol-level error text from a raw ducknng error frame.                               |
| `ducknng_frame_version`       | scalar | `frame`                                         | `UTINYINT`                                                                                                                                                      | Extract the protocol version field from one raw ducknng frame.                                      |
| `ducknng_frame_type`          | scalar | `frame`                                         | `UTINYINT`                                                                                                                                                      | Extract the numeric reply type field from one raw ducknng frame.                                    |
| `ducknng_frame_flags`         | scalar | `frame`                                         | `UINTEGER`                                                                                                                                                      | Extract the reply flags bitset from one raw ducknng frame.                                          |
| `ducknng_frame_type_name`     | scalar | `frame`                                         | `VARCHAR`                                                                                                                                                       | Extract the symbolic reply type name from one raw ducknng frame.                                    |
| `ducknng_frame_name`          | scalar | `frame`                                         | `VARCHAR`                                                                                                                                                       | Extract the method or reply name field from one raw ducknng frame.                                  |
| `ducknng_frame_end_of_stream` | scalar | `frame`                                         | `BOOLEAN`                                                                                                                                                       | Report whether one raw ducknng frame carries the end-of-stream reply flag.                          |

## Transport Security

| name                                 | kind   | arguments                                                                                                | returns                                                                                                                                                                                                                                                                                                                                                                                                                                                    | description                                                                                                                     |
|--------------------------------------|--------|----------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------|
| `ducknng_list_tls_configs`           | table  |                                                                                                          | `TABLE(tls_config_id UBIGINT, source VARCHAR, enabled BOOLEAN, has_cert_key_file BOOLEAN, has_ca_file BOOLEAN, has_cert_pem BOOLEAN, has_key_pem BOOLEAN, has_ca_pem BOOLEAN, has_password BOOLEAN, auth_mode INTEGER, peer_allowlist_active BOOLEAN, peer_allowlist_count UBIGINT, peer_allowlist_json VARCHAR)`                                                                                                                                          | List registered TLS config handles and the kinds of material they contain.                                                      |
| `ducknng_drop_tls_config`            | scalar | `tls_config_id`                                                                                          | `BOOLEAN`                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Remove a registered TLS config handle from the runtime.                                                                         |
| `ducknng_set_tls_peer_allowlist`     | scalar | `tls_config_id, identities_json`                                                                         | `BOOLEAN`                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Set the default exact peer-identity allowlist on a TLS config handle.                                                           |
| `ducknng_set_service_peer_allowlist` | scalar | `name, identities_json`                                                                                  | `BOOLEAN`                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Dynamically set the exact peer-identity allowlist for a running service.                                                        |
| `ducknng_set_service_ip_allowlist`   | scalar | `name, cidrs_json`                                                                                       | `BOOLEAN`                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Dynamically set the IP/CIDR remote-address allowlist for a running service.                                                     |
| `ducknng_set_service_limits`         | scalar | `name, max_open_sessions[, max_active_pipes[, max_inflight_requests[, max_sessions_per_peer_identity]]]` | `BOOLEAN`                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Set service resource limits.                                                                                                    |
| `ducknng_auth_context`               | table  |                                                                                                          | `TABLE(phase VARCHAR, service_name VARCHAR, transport_family VARCHAR, scheme VARCHAR, listen VARCHAR, remote_addr VARCHAR, remote_ip VARCHAR, remote_port INTEGER, tls_verified BOOLEAN, peer_identity VARCHAR, peer_allowlist_active BOOLEAN, ip_allowlist_active BOOLEAN, sql_authorizer_active BOOLEAN, http_method VARCHAR, http_path VARCHAR, content_type VARCHAR, body_bytes UBIGINT, rpc_method VARCHAR, rpc_type VARCHAR, payload_bytes UBIGINT)` | Expose the current request context to a SQL authorization callback.                                                             |
| `ducknng_set_service_authorizer`     | scalar | `name, authorizer_sql`                                                                                   | `BOOLEAN`                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Install or clear a service-level SQL authorization callback evaluated uniformly for framed RPC requests before method dispatch. |
| `ducknng_self_signed_tls_config`     | scalar | `common_name, valid_days, auth_mode`                                                                     | `UBIGINT`                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Generate a self-signed development certificate and register it as a TLS config handle.                                          |
| `ducknng_tls_config_from_pem`        | scalar | `cert_pem, key_pem, ca_pem, password, auth_mode`                                                         | `UBIGINT`                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Register a TLS config handle from in-memory PEM material.                                                                       |
| `ducknng_tls_config_from_files`      | scalar | `cert_key_file, ca_file, password, auth_mode`                                                            | `UBIGINT`                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Register a TLS config handle from file-backed certificate material.                                                             |

## HTTP Transport

| name                        | kind   | arguments                                                    | returns                                                                                                                | description                                                                                                                         |
|-----------------------------|--------|--------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| `ducknng_ncurl`             | table  | `url, method, headers_json, body, timeout_ms, tls_config_id` | `TABLE(ok BOOLEAN, status INTEGER, error VARCHAR, headers_json VARCHAR, body BLOB, body_text VARCHAR)`                 | Perform one HTTP or HTTPS request and return an in-band result row.                                                                 |
| `ducknng_ncurl_aio`         | scalar | `url, method, headers_json, body, timeout_ms, tls_config_id` | `UBIGINT`                                                                                                              | Launch one asynchronous HTTP or HTTPS request and return a future-like aio handle id.                                               |
| `ducknng_ncurl_aio_collect` | table  | `aio_ids, wait_ms`                                           | `TABLE(aio_id UBIGINT, ok BOOLEAN, status INTEGER, error VARCHAR, headers_json VARCHAR, body BLOB, body_text VARCHAR)` | Wait for asynchronous ncurl handles and return one raw HTTP result row per newly collected terminal operation.                      |
| `ducknng_ncurl_table`       | table  | `url, method, headers_json, body, timeout_ms, tls_config_id` | `TABLE(dynamic by response Content-Type)`                                                                              | Perform one HTTP or HTTPS request and parse a successful response body into a DuckDB table using the built-in body codec providers. |

## Body Codecs

| name                       | kind   | arguments                     | returns                                                                                                            | description                                                                                                              |
|----------------------------|--------|-------------------------------|--------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| `ducknng_list_codecs`      | table  |                               | `TABLE(provider VARCHAR, media_types VARCHAR, kind VARCHAR, function_name VARCHAR, output VARCHAR, notes VARCHAR)` | List built-in body serialization/deserialization providers and any registered user codec hooks.                          |
| `ducknng_register_codec`   | scalar | `content_type, function_name` | `BOOLEAN`                                                                                                          | Register a user body codec that decodes a BLOB body into a single VARCHAR value through an existing scalar SQL function. |
| `ducknng_unregister_codec` | scalar | `content_type`                | `BOOLEAN`                                                                                                          | Remove a previously registered user body codec for a content type.                                                       |
| `ducknng_parse_body`       | table  | `body, content_type`          | `TABLE(dynamic by provider)`                                                                                       | Parse one response/request body BLOB according to its content type.                                                      |

## HTTP Routes

| name                                    | kind   | arguments                                                                          | returns                                                                                                                                                                                                                                                                                                                                                                                 | description                                                                                                            |
|-----------------------------------------|--------|------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------|
| `ducknng_register_http_route`           | scalar | `service_name, method, path, handler_sql[, request_max_bytes]`                     | `BOOLEAN`                                                                                                                                                                                                                                                                                                                                                                               | Register one exact-path HTTP route beside the framed RPC mount of an existing <http://> or <https://> service.         |
| `ducknng_register_http_route_pattern`   | scalar | `service_name, method, match_kind, path_pattern, handler_sql[, request_max_bytes]` | `BOOLEAN`                                                                                                                                                                                                                                                                                                                                                                               | Register one low-level HTTP route pattern beside the framed RPC mount using exact, prefix, or template matching.       |
| `ducknng_unregister_http_route`         | scalar | `service_name, method, path`                                                       | `BOOLEAN`                                                                                                                                                                                                                                                                                                                                                                               | Remove one previously registered exact-path HTTP route from a service.                                                 |
| `ducknng_unregister_http_route_pattern` | scalar | `service_name, method, match_kind, path_pattern`                                   | `BOOLEAN`                                                                                                                                                                                                                                                                                                                                                                               | Remove one previously registered prefix, template, or explicit exact route pattern from a service.                     |
| `ducknng_list_http_routes`              | table  |                                                                                    | `TABLE(service_id UBIGINT, route_id UBIGINT, request_max_bytes UBIGINT, service_name VARCHAR, method VARCHAR, match_kind VARCHAR, path VARCHAR, handler_sql VARCHAR)`                                                                                                                                                                                                                   | List the currently registered HTTP routes across running services, including their match kind and stored path pattern. |
| `ducknng_http_request`                  | table  |                                                                                    | `TABLE(service_name VARCHAR, listen VARCHAR, scheme VARCHAR, method VARCHAR, path VARCHAR, query_string VARCHAR, content_type VARCHAR, headers_json VARCHAR, caller_identity VARCHAR, remote_addr VARCHAR, remote_ip VARCHAR, route_method VARCHAR, route_match_kind VARCHAR, route_path VARCHAR, path_params_json VARCHAR, body_bytes UBIGINT, route_id UBIGINT, remote_port INTEGER)` | Expose the current HTTP request context while SQL runs inside an active route handler.                                 |
| `ducknng_http_request_body`             | table  |                                                                                    | `TABLE(body BLOB, body_text VARCHAR)`                                                                                                                                                                                                                                                                                                                                                   | Expose the current HTTP request body while SQL runs inside an active route handler.                                    |
| `ducknng_http_headers_get`              | scalar | `headers_json, name`                                                               | `VARCHAR`                                                                                                                                                                                                                                                                                                                                                                               | Return one header value from ducknng’s canonical HTTP header JSON.                                                     |
| `ducknng_http_headers_build`            | scalar | `names, values`                                                                    | `VARCHAR`                                                                                                                                                                                                                                                                                                                                                                               | Build ducknng’s canonical HTTP header JSON from parallel name and value lists.                                         |
| `ducknng_http_query_param_get`          | scalar | `query_string, name`                                                               | `VARCHAR`                                                                                                                                                                                                                                                                                                                                                                               | Return one decoded query-string parameter value.                                                                       |
| `ducknng_http_cookie_get`               | scalar | `cookie_header, name`                                                              | `VARCHAR`                                                                                                                                                                                                                                                                                                                                                                               | Return one cookie value from a Cookie header string.                                                                   |
| `ducknng_http_path_params_get`          | scalar | `path_params_json, name`                                                           | `VARCHAR`                                                                                                                                                                                                                                                                                                                                                                               | Return one template-route path parameter from path_params_json.                                                        |
| `ducknng_http_header`                   | scalar | `name`                                                                             | `VARCHAR`                                                                                                                                                                                                                                                                                                                                                                               | Route-local shortcut for reading one request header by name.                                                           |
| `ducknng_http_query_param`              | scalar | `name`                                                                             | `VARCHAR`                                                                                                                                                                                                                                                                                                                                                                               | Route-local shortcut for reading one decoded query parameter by name.                                                  |
| `ducknng_http_cookie`                   | scalar | `name`                                                                             | `VARCHAR`                                                                                                                                                                                                                                                                                                                                                                               | Route-local shortcut for reading one request cookie by name.                                                           |
| `ducknng_http_path_param`               | scalar | `name`                                                                             | `VARCHAR`                                                                                                                                                                                                                                                                                                                                                                               | Route-local shortcut for reading one template path parameter by name.                                                  |
| `ducknng_http_response`                 | table  | `status, headers_json, content_type, body, body_text`                              | `TABLE(status INTEGER, headers_json VARCHAR, content_type VARCHAR, body BLOB, body_text VARCHAR)`                                                                                                                                                                                                                                                                                       | Build the one-row response shape expected by a route handler.                                                          |
| `ducknng_http_text`                     | table  | `status, body_text`                                                                | `TABLE(status INTEGER, headers_json VARCHAR, content_type VARCHAR, body BLOB, body_text VARCHAR)`                                                                                                                                                                                                                                                                                       | Build a one-row plain-text HTTP route response.                                                                        |
| `ducknng_http_json`                     | table  | `status, body_text`                                                                | `TABLE(status INTEGER, headers_json VARCHAR, content_type VARCHAR, body BLOB, body_text VARCHAR)`                                                                                                                                                                                                                                                                                       | Build a one-row JSON HTTP route response from a text body.                                                             |
| `ducknng_http_binary`                   | table  | `status, body`                                                                     | `TABLE(status INTEGER, headers_json VARCHAR, content_type VARCHAR, body BLOB, body_text VARCHAR)`                                                                                                                                                                                                                                                                                       | Build a one-row binary HTTP route response.                                                                            |

## Async I/O

| name                               | kind   | arguments                                                                            | returns                                                                                                                                                                                                               | description                                                                                                        |
|------------------------------------|--------|--------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| `ducknng_request_raw_aio`          | scalar | `url, frame, timeout_ms, tls_config_id`                                              | `UBIGINT`                                                                                                                                                                                                             | Launch one raw req/rep roundtrip asynchronously and return a future-like aio handle id.                            |
| `ducknng_get_rpc_manifest_raw_aio` | scalar | `url, timeout_ms, tls_config_id`                                                     | `UBIGINT`                                                                                                                                                                                                             | Launch one asynchronous manifest RPC request and return an aio handle id for the raw reply frame.                  |
| `ducknng_run_rpc_raw_aio`          | scalar | `url, sql, timeout_ms, tls_config_id`                                                | `UBIGINT`                                                                                                                                                                                                             | Launch one asynchronous metadata-only exec RPC request and return an aio handle id for the raw reply frame.        |
| `ducknng_open_query_raw_aio`       | scalar | `url, sql, batch_rows, batch_bytes, timeout_ms, tls_config_id`                       | `UBIGINT`                                                                                                                                                                                                             | Launch one asynchronous query_open request and return an aio handle id for the raw reply frame.                    |
| `ducknng_fetch_query_raw_aio`      | scalar | `url, session_id, session_token, batch_rows, batch_bytes, timeout_ms, tls_config_id` | `UBIGINT`                                                                                                                                                                                                             | Launch one asynchronous fetch request and return an aio handle id for the raw reply frame.                         |
| `ducknng_close_query_raw_aio`      | scalar | `url, session_id, session_token, timeout_ms, tls_config_id`                          | `UBIGINT`                                                                                                                                                                                                             | Launch one asynchronous close request and return an aio handle id for the raw reply frame.                         |
| `ducknng_cancel_query_raw_aio`     | scalar | `url, session_id, session_token, timeout_ms, tls_config_id`                          | `UBIGINT`                                                                                                                                                                                                             | Launch one asynchronous cancel request and return an aio handle id for the raw reply frame.                        |
| `ducknng_request_socket_raw_aio`   | scalar | `socket_id, frame, timeout_ms`                                                       | `UBIGINT`                                                                                                                                                                                                             | Launch one raw req/rep roundtrip asynchronously on an existing req socket handle and return an aio handle id.      |
| `ducknng_send_socket_raw_aio`      | scalar | `socket_id, frame, timeout_ms`                                                       | `UBIGINT`                                                                                                                                                                                                             | Launch one raw socket send asynchronously and return an aio handle id.                                             |
| `ducknng_recv_socket_raw_aio`      | scalar | `socket_id, timeout_ms`                                                              | `UBIGINT`                                                                                                                                                                                                             | Launch one raw socket receive asynchronously and return an aio handle id.                                          |
| `ducknng_aio_ready`                | scalar | `aio_id`                                                                             | `BOOLEAN`                                                                                                                                                                                                             | Return whether an aio handle has reached a terminal state.                                                         |
| `ducknng_aio_wait`                 | scalar | `aio_ids, wait_ms`                                                                   | `BOOLEAN`                                                                                                                                                                                                             | Wait until any requested aio handle reaches a terminal state without collecting or dropping it.                    |
| `ducknng_aio_status`               | table  | `aio_id`                                                                             | `TABLE(aio_id UBIGINT, exists BOOLEAN, kind VARCHAR, state VARCHAR, phase VARCHAR, terminal BOOLEAN, send_done BOOLEAN, send_ok BOOLEAN, recv_done BOOLEAN, recv_ok BOOLEAN, has_reply_frame BOOLEAN, error VARCHAR)` | Inspect the current or terminal status of one aio handle, including send-phase and recv-phase completion.          |
| `ducknng_aio_collect`              | table  | `aio_ids, wait_ms`                                                                   | `TABLE(aio_id UBIGINT, ok BOOLEAN, error VARCHAR, frame BLOB)`                                                                                                                                                        | Wait for any requested aio handles to finish and return one row per newly collected terminal result.               |
| `ducknng_aio_collect_decoded`      | table  | `aio_ids, wait_ms`                                                                   | `TABLE(aio_id UBIGINT, ok BOOLEAN, error VARCHAR, frame_ok BOOLEAN, frame_error VARCHAR, version UTINYINT, type UTINYINT, flags UINTEGER, type_name VARCHAR, name VARCHAR, payload BLOB, payload_text VARCHAR)`       | Wait for framed aio handles, collect their terminal frame rows, and project the decoded envelope columns directly. |
| `ducknng_aio_cancel`               | scalar | `aio_id`                                                                             | `BOOLEAN`                                                                                                                                                                                                             | Request cancellation of a pending aio handle.                                                                      |
| `ducknng_aio_drop`                 | scalar | `aio_id`                                                                             | `BOOLEAN`                                                                                                                                                                                                             | Release a terminal aio handle from the runtime registry.                                                           |

## RPC Helper

| name                           | kind   | arguments                 | returns                                                                                               | description                                                                                                                 |
|--------------------------------|--------|---------------------------|-------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| `ducknng_get_rpc_manifest`     | table  | `url, tls_config_id`      | `TABLE(ok BOOLEAN, error VARCHAR, manifest VARCHAR)`                                                  | Request the RPC manifest and return a structured result row.                                                                |
| `ducknng_get_rpc_manifest_raw` | scalar | `url, tls_config_id`      | `BLOB`                                                                                                | Request the RPC manifest and return the raw reply frame as BLOB.                                                            |
| `ducknng_run_rpc`              | table  | `url, sql, tls_config_id` | `TABLE(ok BOOLEAN, error VARCHAR, rows_changed UBIGINT, statement_type INTEGER, result_type INTEGER)` | Execute a metadata-oriented RPC call and return a structured result row.                                                    |
| `ducknng_run_rpc_raw`          | scalar | `url, sql, tls_config_id` | `BLOB`                                                                                                | Execute the exec RPC and return the raw reply frame as BLOB.                                                                |
| `ducknng_query_rpc`            | table  | `url, sql, tls_config_id` | `table`                                                                                               | Execute a row-returning RPC query as a session convenience wrapper and expose the fetched Arrow IPC rows as a DuckDB table. |

## RPC Session

| name                        | kind   | arguments                                                                | returns                                                                                                                                                                                               | description                                                                                                    |
|-----------------------------|--------|--------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| `ducknng_open_query`        | table  | `url, sql, batch_rows, batch_bytes, tls_config_id`                       | `TABLE(ok BOOLEAN, error VARCHAR, session_id UBIGINT, session_token VARCHAR, state VARCHAR, next_method VARCHAR, control_json VARCHAR, idle_timeout_ms UBIGINT)`                                      | Open a server-side query session and return the JSON control metadata as a structured row.                     |
| `ducknng_fetch_query`       | table  | `url, session_id, session_token, batch_rows, batch_bytes, tls_config_id` | `TABLE(ok BOOLEAN, error VARCHAR, session_id UBIGINT, session_token VARCHAR, state VARCHAR, next_method VARCHAR, control_json VARCHAR, idle_timeout_ms UBIGINT, payload BLOB, end_of_stream BOOLEAN)` | Fetch the next session reply and return either JSON control metadata or an Arrow IPC batch payload.            |
| `ducknng_fetch_query_table` | table  | `url, session_id, session_token, batch_rows, batch_bytes, tls_config_id` | `TABLE(dynamic from Arrow IPC batch)`                                                                                                                                                                 | Fetch one session row batch and decode the returned Arrow IPC payload directly into a DuckDB table.            |
| `ducknng_close_query`       | table  | `url, session_id, session_token, tls_config_id`                          | `TABLE(ok BOOLEAN, error VARCHAR, session_id UBIGINT, session_token VARCHAR, state VARCHAR, next_method VARCHAR, control_json VARCHAR, idle_timeout_ms UBIGINT)`                                      | Close a server-side query session and return the JSON control metadata as a structured row.                    |
| `ducknng_cancel_query`      | table  | `url, session_id, session_token, tls_config_id`                          | `TABLE(ok BOOLEAN, error VARCHAR, session_id UBIGINT, session_token VARCHAR, state VARCHAR, next_method VARCHAR, control_json VARCHAR, idle_timeout_ms UBIGINT)`                                      | Request cancellation for a server-side query session and return the JSON control metadata as a structured row. |
| `ducknng_open_query_raw`    | scalar | `url, sql, batch_rows, batch_bytes, tls_config_id`                       | `BLOB`                                                                                                                                                                                                | Open a server-side query session and return the raw reply frame as BLOB.                                       |
| `ducknng_fetch_query_raw`   | scalar | `url, session_id, session_token, batch_rows, batch_bytes, tls_config_id` | `BLOB`                                                                                                                                                                                                | Fetch the next session reply and return the raw reply frame as BLOB.                                           |
| `ducknng_close_query_raw`   | scalar | `url, session_id, session_token, tls_config_id`                          | `BLOB`                                                                                                                                                                                                | Close a server-side query session and return the raw reply frame as BLOB.                                      |
| `ducknng_cancel_query_raw`  | scalar | `url, session_id, session_token, tls_config_id`                          | `BLOB`                                                                                                                                                                                                | Cancel a server-side query session and return the raw reply frame as BLOB.                                     |

</details>

## Examples

### Start an IPC listener and inspect the registry

``` sql
SELECT ducknng_start_server(
  'sql0',                         -- service name
  'ipc:///tmp/ducknng_sql0.ipc', -- listen URL
  1,                              -- REP contexts
  134217728,                      -- recv_max_bytes
  300000,                         -- session_idle_ms
  0                               -- tls_config_id (0 means plaintext)
);
SELECT name, listen, contexts, running, execution_model, sessions
FROM ducknng_list_servers();
SELECT ducknng_stop_server('sql0');
+--------------------------------------------------------------------------------------+
| ducknng_start_server('sql0', 'ipc:///tmp/ducknng_sql0.ipc', 1, 134217728, 300000, 0) |
+--------------------------------------------------------------------------------------+
| true                                                                                 |
+--------------------------------------------------------------------------------------+
+------+-----------------------------+----------+---------+------------------------------+----------+
| name |           listen            | contexts | running |       execution_model        | sessions |
+------+-----------------------------+----------+---------+------------------------------+----------+
| sql0 | ipc:///tmp/ducknng_sql0.ipc | 1        | true    | shared_serialized_connection | 0        |
+------+-----------------------------+----------+---------+------------------------------+----------+
+-----------------------------+
| ducknng_stop_server('sql0') |
+-----------------------------+
| true                        |
+-----------------------------+
```

### Request multiple REP contexts on one REP socket

``` sql
SELECT ducknng_start_server(
  'sql_multi',                    -- service name
  'ipc:///tmp/ducknng_sql_multi.ipc', -- listen URL
  3,                              -- REP contexts
  134217728,                      -- recv_max_bytes
  300000,                         -- session_idle_ms
  0                               -- tls_config_id (0 means plaintext)
);
SELECT name, contexts, running
FROM ducknng_list_servers()
WHERE name = 'sql_multi';
SELECT ducknng_stop_server('sql_multi');
+------------------------------------------------------------------------------------------------+
| ducknng_start_server('sql_multi', 'ipc:///tmp/ducknng_sql_multi.ipc', 3, 134217728, 300000, 0) |
+------------------------------------------------------------------------------------------------+
| true                                                                                           |
+------------------------------------------------------------------------------------------------+
+-----------+----------+---------+
|   name    | contexts | running |
+-----------+----------+---------+
| sql_multi | 3        | true    |
+-----------+----------+---------+
+----------------------------------+
| ducknng_stop_server('sql_multi') |
+----------------------------------+
| true                             |
+----------------------------------+
```

### DuckDB can also act as a client

#### Start the server and inspect the default registry

The default RPC surface only exposes `manifest`. `exec` is opt-in.

``` sql
SELECT ducknng_start_server(
  'sql_client_demo',
  'ipc:///tmp/ducknng_sql_client_demo.ipc',
  1, 134217728, 300000, 0
);
WITH manifest_row AS (
  SELECT manifest
  FROM ducknng_get_rpc_manifest('ipc:///tmp/ducknng_sql_client_demo.ipc', 0::UBIGINT)
  WHERE ok
)
SELECT json_extract_string(manifest::JSON, '$.server.name') AS server_name,
       json_array_length(json_extract(manifest::JSON, '$.methods')) AS method_count,
       position('"name":"exec"' IN manifest) > 0 AS has_exec
FROM manifest_row;
SELECT name, family, response_mode, requires_auth, disabled
FROM ducknng_list_methods()
ORDER BY name;
+------------------------------------------------------------------------------------------------------------+
| ducknng_start_server('sql_client_demo', 'ipc:///tmp/ducknng_sql_client_demo.ipc', 1, 134217728, 300000, 0) |
+------------------------------------------------------------------------------------------------------------+
| true                                                                                                       |
+------------------------------------------------------------------------------------------------------------+
+-------------+--------------+----------+
| server_name | method_count | has_exec |
+-------------+--------------+----------+
| ducknng     | 5            | false    |
+-------------+--------------+----------+
+------------+---------+---------------+---------------+----------+
|    name    | family  | response_mode | requires_auth | disabled |
+------------+---------+---------------+---------------+----------+
| cancel     | query   | metadata_only | false         | false    |
| close      | query   | metadata_only | false         | false    |
| fetch      | query   | rows          | false         | false    |
| manifest   | control | metadata_only | false         | false    |
| query_open | query   | session_open  | false         | false    |
+------------+---------+---------------+---------------+----------+
```

#### Register `exec` and re-inspect the manifest

``` sql
SELECT ducknng_register_exec_method(false) AS registered_exec;
SELECT name, family, response_mode, requires_auth, disabled
FROM ducknng_list_methods()
ORDER BY name;
WITH manifest_row AS (
  SELECT manifest
  FROM ducknng_get_rpc_manifest('ipc:///tmp/ducknng_sql_client_demo.ipc', 0::UBIGINT)
  WHERE ok
)
SELECT json_extract_string(manifest::JSON, '$.server.name') AS server_name,
       json_array_length(json_extract(manifest::JSON, '$.methods')) AS method_count,
       position('"name":"exec"' IN manifest) > 0 AS has_exec
FROM manifest_row;
+-----------------+
| registered_exec |
+-----------------+
| true            |
+-----------------+
+------------+---------+------------------+---------------+----------+
|    name    | family  |  response_mode   | requires_auth | disabled |
+------------+---------+------------------+---------------+----------+
| cancel     | query   | metadata_only    | false         | false    |
| close      | query   | metadata_only    | false         | false    |
| exec       | sql     | metadata_or_rows | false         | false    |
| fetch      | query   | rows             | false         | false    |
| manifest   | control | metadata_only    | false         | false    |
| query_open | query   | session_open     | false         | false    |
+------------+---------+------------------+---------------+----------+
+-------------+--------------+----------+
| server_name | method_count | has_exec |
+-------------+--------------+----------+
| ducknng     | 6            | true     |
+-------------+--------------+----------+
```

#### Run RPC statements and fetch rows

``` sql
SELECT * FROM ducknng_run_rpc(
  'ipc:///tmp/ducknng_sql_client_demo.ipc',
  'CREATE TABLE IF NOT EXISTS client_side_demo(i INTEGER)',
  0::UBIGINT
);
SELECT * FROM ducknng_run_rpc(
  'ipc:///tmp/ducknng_sql_client_demo.ipc',
  'INSERT INTO client_side_demo VALUES (10), (11)',
  0::UBIGINT
);
SELECT *
FROM ducknng_query_rpc(
  'ipc:///tmp/ducknng_sql_client_demo.ipc',
  'SELECT i, i > 10 AS gt_10 FROM client_side_demo ORDER BY i',
  0::UBIGINT
);
+------+-------+--------------+----------------+-------------+
|  ok  | error | rows_changed | statement_type | result_type |
+------+-------+--------------+----------------+-------------+
| true | NULL  | 0            | 7              | 2           |
+------+-------+--------------+----------------+-------------+
+------+-------+--------------+----------------+-------------+
|  ok  | error | rows_changed | statement_type | result_type |
+------+-------+--------------+----------------+-------------+
| true | NULL  | 2            | 2              | 1           |
+------+-------+--------------+----------------+-------------+
+----+-------+
| i  | gt_10 |
+----+-------+
| 10 | false |
| 11 | true  |
+----+-------+
```

#### Arrow IPC type roundtrip

The row path carries temporal, decimal, list, and struct values.

``` sql
SELECT d = DATE '2024-01-02' AS date_ok,
       ts = TIMESTAMP '2024-01-02 03:04:05.123456' AS ts_ok,
       dec = 123.45::DECIMAL(10,2) AS decimal_ok,
       xs[2] IS NULL AND xs[3] = 3 AS list_ok,
       st.a = 7 AND st.b = 'bee' AS struct_ok
FROM ducknng_query_rpc(
  'ipc:///tmp/ducknng_sql_client_demo.ipc',
  'SELECT DATE ''2024-01-02'' AS d,
          TIMESTAMP ''2024-01-02 03:04:05.123456'' AS ts,
          123.45::DECIMAL(10,2) AS dec,
          [1, NULL, 3]::INTEGER[] AS xs,
          {''a'': 7::INTEGER, ''b'': ''bee''} AS st',
  0::UBIGINT
);
+---------+-------+------------+---------+-----------+
| date_ok | ts_ok | decimal_ok | list_ok | struct_ok |
+---------+-------+------------+---------+-----------+
| true    | true  | true       | true    | true      |
+---------+-------+------------+---------+-----------+
```

#### Body codec helpers

``` sql
SELECT provider, output
FROM ducknng_list_codecs()
ORDER BY provider;
SELECT a, b
FROM ducknng_parse_body(
  '[{"a":1,"b":"x"},{"a":2,"b":"y"}]'::BLOB,
  'application/json; charset=utf-8'
)
ORDER BY a;
+---------------+-----------------------------+
|   provider    |           output            |
+---------------+-----------------------------+
| arrow_ipc     | dynamic table               |
| csv           | dynamic table               |
| ducknng_frame | decoded frame columns       |
| form          | name VARCHAR, value VARCHAR |
| json          | dynamic table               |
| ndjson        | dynamic table               |
| parquet       | dynamic table               |
| raw           | body BLOB                   |
| text          | body_text VARCHAR           |
| tsv           | dynamic table               |
+---------------+-----------------------------+
+---+---+
| a | b |
+---+---+
| 1 | x |
| 2 | y |
+---+---+
```

#### Primitive socket layer

Low-level socket operations return a struct-shaped result: `ok`,
`error`, `nng_error`, `nng_error_message`, `socket_id`, `payload`,
`url`.

``` sql
SELECT (ducknng_open_socket('req')).socket_id;
SELECT (ducknng_dial_socket(
  1, 'ipc:///tmp/ducknng_sql_client_demo.ipc', 1000, 0::UBIGINT
)).ok;
SELECT * FROM ducknng_list_sockets();
-- Send the built-in manifest request frame through the open socket.
SELECT ok, error, nng_error_message, octet_length(payload) > 0 AS has_payload
FROM ducknng_request_socket(
  1::UBIGINT,
  from_hex('01000000000000000000000000000000000000000000'),
  1000
);
-- One-shot URL form: no persistent socket handle needed.
SELECT ok, error, nng_error_message, octet_length(payload) > 0 AS has_payload
FROM ducknng_request(
  'ipc:///tmp/ducknng_sql_client_demo.ipc',
  from_hex('01000000000000000000000000000000000000000000'),
  1000,
  0::UBIGINT
);
+----------------------------------------+
| (ducknng_open_socket('req')).socket_id |
+----------------------------------------+
| 1                                      |
+----------------------------------------+
+---------------------------------------------------------------------------------------------------+
| (ducknng_dial_socket(1, 'ipc:///tmp/ducknng_sql_client_demo.ipc', 1000, CAST(0 AS "UBIGINT"))).ok |
+---------------------------------------------------------------------------------------------------+
| true                                                                                              |
+---------------------------------------------------------------------------------------------------+
+-----------+----------+----------------------------------------+------+-----------+-----------+-----------------+-----------------+
| socket_id | protocol |                  url                   | open | connected | listening | send_timeout_ms | recv_timeout_ms |
+-----------+----------+----------------------------------------+------+-----------+-----------+-----------------+-----------------+
| 1         | req      | ipc:///tmp/ducknng_sql_client_demo.ipc | true | true      | false     | 1000            | 1000            |
+-----------+----------+----------------------------------------+------+-----------+-----------+-----------------+-----------------+
+------+-------+-------------------+-------------+
|  ok  | error | nng_error_message | has_payload |
+------+-------+-------------------+-------------+
| true | NULL  | NULL              | true        |
+------+-------+-------------------+-------------+
+------+-------+-------------------+-------------+
|  ok  | error | nng_error_message | has_payload |
+------+-------+-------------------+-------------+
| true | NULL  | NULL              | true        |
+------+-------+-------------------+-------------+
```

#### Raw frame helpers

The raw scalar forms return reply bytes as BLOBs;
`ducknng_decode_frame(...)` projects the envelope fields and text
payload.

``` sql
-- Raw bytes from a socket-scoped request (first 14 octets as hex).
SELECT substr(
  hex(ducknng_request_socket_raw(
    1,
    from_hex('01000000000000000000000000000000000000000000'),
    1000
  )),
  1, 28
);
-- Decode the manifest reply directly.
SELECT ok, version, type_name, name,
       position('"name":"exec"' IN payload_text) > 0 AS has_exec
FROM ducknng_decode_frame(
  ducknng_get_rpc_manifest_raw('ipc:///tmp/ducknng_sql_client_demo.ipc', 0::UBIGINT)
);
-- Decode an exec reply.
SELECT ok, type_name, name
FROM ducknng_decode_frame(
  ducknng_run_rpc_raw(
    'ipc:///tmp/ducknng_sql_client_demo.ipc',
    'CREATE TABLE IF NOT EXISTS client_side_demo(i INTEGER)',
    0::UBIGINT
  )
);
-- The generic raw request helper decodes the same way.
SELECT ok, version, type_name, name,
       position('"name":"exec"' IN payload_text) > 0 AS has_exec
FROM ducknng_decode_frame(
  ducknng_request_raw(
    'ipc:///tmp/ducknng_sql_client_demo.ipc',
    from_hex('01000000000000000000000000000000000000000000'),
    1000, 0::UBIGINT
  )
);
+-------------------------------------------------------------------------------------------------------------------+
| substr(hex(ducknng_request_socket_raw(1, from_hex('01000000000000000000000000000000000000000000'), 1000)), 1, 28) |
+-------------------------------------------------------------------------------------------------------------------+
| 0102040000000800000000000000                                                                                      |
+-------------------------------------------------------------------------------------------------------------------+
+------+---------+-----------+----------+----------+
|  ok  | version | type_name |   name   | has_exec |
+------+---------+-----------+----------+----------+
| true | 1       | result    | manifest | true     |
+------+---------+-----------+----------+----------+
+------+-----------+------+
|  ok  | type_name | name |
+------+-----------+------+
| true | result    | exec |
+------+-----------+------+
+------+---------+-----------+----------+----------+
|  ok  | version | type_name |   name   | has_exec |
+------+---------+-----------+----------+----------+
| true | 1       | result    | manifest | true     |
+------+---------+-----------+----------+----------+
```

#### Cleanup

``` sql
SELECT (ducknng_close_socket(1)).ok;
SELECT ducknng_stop_server('sql_client_demo');
+------------------------------+
| (ducknng_close_socket(1)).ok |
+------------------------------+
| true                         |
+------------------------------+
+----------------------------------------+
| ducknng_stop_server('sql_client_demo') |
+----------------------------------------+
| true                                   |
+----------------------------------------+
```

### Use `ducknng_ncurl()` against a local `nanonext` HTTPS server

A tiny local R `nanonext` HTTPS server is started during README
rendering so this example runs without depending on the public internet.
The core server definition is shown below because the transport helper
and its carrier behavior are part of the example rather than hidden
setup. The hidden setup only launches the script in the background and
manages the temporary PID, log, and CA files needed while rendering.
`ducknng_ncurl(...)` remains the low-level HTTP/HTTPS transport
primitive even though the higher-level synchronous request, RPC, and
session helpers now auto-route over `http://` and `https://` when the
URL scheme selects that carrier.

``` r
library(nanonext)

cert <- write_cert(cn = '127.0.0.1')
writeLines(cert$client[[1]], "/tmp/ducknng_readme_http_demo_ca.pem")

server <- http_server(
  url = 'https://127.0.0.1:18443',
  handlers = list(
    handler('/hello', function(req) {
      list(
        status = 200L,
        headers = c(
          'Content-Type' = 'text/plain',
          'X-Test' = 'hello'
        ),
        body = 'hello from nanonext https server'
      )
    }),
    handler('/echo', function(req) {
      list(
        status = 200L,
        headers = c(
          'Content-Type' = (
            req$headers[['Content-Type']] %||% 'application/octet-stream'
          ),
          'X-Test' = 'echo'
        ),
        body = req$body
      )
    }, method = 'POST')
  ),
  tls = tls_config(server = cert$server)
)
```

``` sql
-- Register a client TLS handle that trusts the self-signed HTTPS demo server.
SET VARIABLE tls_http_id = ducknng_tls_config_from_files(
  NULL,                                     -- cert_key_file
  '/tmp/ducknng_readme_http_demo_ca.pem',   -- ca_file
  NULL,                                     -- password
  2                                         -- auth_mode = require certificate validation
);
SELECT getvariable('tls_http_id') AS tls_config_id;
-- Call /hello through ducknng_ncurl(): NULL method means GET and NULL body means no request body.
SELECT ok, status, error, body_text
FROM ducknng_ncurl(
  'https://127.0.0.1:18443/hello', -- url
  NULL,                            -- method
  NULL,                            -- headers_json
  NULL,                            -- body
  2000,                            -- timeout_ms
  getvariable('tls_http_id')::UBIGINT  -- tls_config_id
);
-- POST can send raw bytes while still exposing HTTP headers and status in-band.
SET VARIABLE echo_headers =
  '[{"name":"Content-Type","value":"application/octet-stream"}]';
SELECT
  ok,
  status,
  error,
  hex(body) AS body_hex,
  position('X-Test' IN headers_json) > 0 AS has_header
FROM ducknng_ncurl(
  'https://127.0.0.1:18443/echo', -- url
  'POST',                         -- method
  getvariable('echo_headers'),    -- headers_json
  from_hex('01020304'),           -- body
  2000,                           -- timeout_ms
  getvariable('tls_http_id')::UBIGINT  -- tls_config_id
);
-- Drop the temporary client TLS handle after the HTTPS checks complete.
SELECT ducknng_drop_tls_config(getvariable('tls_http_id')::UBIGINT);

+---------------+
| tls_config_id |
+---------------+
| 1             |
+---------------+
+------+--------+-------+----------------------------------+
|  ok  | status | error |            body_text             |
+------+--------+-------+----------------------------------+
| true | 200    | NULL  | hello from nanonext https server |
+------+--------+-------+----------------------------------+

+------+--------+-------+----------+------------+
|  ok  | status | error | body_hex | has_header |
+------+--------+-------+----------+------------+
| true | 200    | NULL  | 01020304 | true       |
+------+--------+-------+----------+------------+
+------------------------------------------------------------------------+
| ducknng_drop_tls_config(CAST(getvariable('tls_http_id') AS "UBIGINT")) |
+------------------------------------------------------------------------+
| true                                                                   |
+------------------------------------------------------------------------+
```

### Start `ducknng` on `http://` and use the routed helpers directly

`ducknng_start_server(...)` mounts the existing framed RPC surface at
the exact path encoded in the listen URL, and the higher-level
synchronous request, RPC, and session helpers keep the same names they
already use for NNG URLs while switching carriers automatically from the
scheme. For `http://` and `https://`, use `contexts = 1`.

``` sql
SELECT ducknng_start_server(
  'http_demo',
  'http://127.0.0.1:18444/_ducknng',
  1,
  134217728,
  300000,
  0::UBIGINT
);
WITH manifest_row AS (
  SELECT manifest
  FROM ducknng_get_rpc_manifest('http://127.0.0.1:18444/_ducknng', 0::UBIGINT)
  WHERE ok
)
SELECT json_extract_string(manifest::JSON, '$.server.name') AS server_name,
       json_array_length(json_extract(manifest::JSON, '$.methods')) AS method_count,
       position('"name":"manifest"' IN manifest) > 0 AS has_manifest
FROM manifest_row;
SET VARIABLE http_demo_frame = (
  SELECT body
  FROM ducknng_ncurl(
    'http://127.0.0.1:18444/_ducknng',
    'POST',
    '[{"name":"Content-Type","value":"application/vnd.ducknng.frame"}]',
    from_hex('01000000000000000000000000000000000000000000'),
    2000,
    0::UBIGINT
  )
);
SELECT ok, type_name, name
FROM ducknng_decode_frame(getvariable('http_demo_frame'));
SELECT ducknng_stop_server('http_demo');
+------------------------------------------------------------------------------------------------------------------+
| ducknng_start_server('http_demo', 'http://127.0.0.1:18444/_ducknng', 1, 134217728, 300000, CAST(0 AS "UBIGINT")) |
+------------------------------------------------------------------------------------------------------------------+
| true                                                                                                             |
+------------------------------------------------------------------------------------------------------------------+
+-------------+--------------+--------------+
| server_name | method_count | has_manifest |
+-------------+--------------+--------------+
| ducknng     | 6            | true         |
+-------------+--------------+--------------+

+------+-----------+----------+
|  ok  | type_name |   name   |
+------+-----------+----------+
| true | result    | manifest |
+------+-----------+----------+
+----------------------------------+
| ducknng_stop_server('http_demo') |
+----------------------------------+
| true                             |
+----------------------------------+
```

### Register low-level HTTP routes beside the framed RPC mount

The framed RPC mount stays exactly where the listen URL says it lives,
but HTTP and HTTPS services can now register additional exact, prefix,
or template routes beside that mount. Route handlers are ordinary SQL
queries that return exactly one response row, and they can inspect the
in-flight request through `ducknng_http_request()` and
`ducknng_http_request_body()`.

#### Start the server and register routes

``` sql
SELECT ducknng_start_server(
  'http_route_demo',
  'http://127.0.0.1:18445/_ducknng',
  1, 134217728, 300000, 0::UBIGINT
);
SELECT ducknng_register_http_route(
  'http_route_demo', 'GET', '/healthz',
  'SELECT * FROM ducknng_http_text(200, ''ok'')'
);
SELECT ducknng_register_http_route(
  'http_route_demo', 'POST', '/echo',
  'SELECT * FROM ducknng_http_text(
     201,
     (SELECT method || '' '' || path || ''?'' || coalesce(query_string, '''') ||
             '' x='' || coalesce(ducknng_http_query_param(''x''), '''') ||
             '' '' || coalesce(content_type, '''') ||
             '' '' || coalesce(body_text, '''')
      FROM ducknng_http_request(), ducknng_http_request_body()))'
);
SELECT ducknng_register_http_route_pattern(
  'http_route_demo', 'GET', 'template',
  '/tenant/{tenant_id}/items/{item_id}',
  'SELECT * FROM ducknng_http_text(
     202,
     (SELECT route_match_kind || '' '' ||
             ducknng_http_path_param(''tenant_id'') || '':'' ||
             ducknng_http_path_param(''item_id'')
      FROM ducknng_http_request()))'
);
+------------------------------------------------------------------------------------------------------------------------+
| ducknng_start_server('http_route_demo', 'http://127.0.0.1:18445/_ducknng', 1, 134217728, 300000, CAST(0 AS "UBIGINT")) |
+------------------------------------------------------------------------------------------------------------------------+
| true                                                                                                                   |
+------------------------------------------------------------------------------------------------------------------------+
+-------------------------------------------------------------------------------------------------------------------+
| ducknng_register_http_route('http_route_demo', 'GET', '/healthz', 'SELECT * FROM ducknng_http_text(200, ''ok'')') |
+-------------------------------------------------------------------------------------------------------------------+
| true                                                                                                              |
+-------------------------------------------------------------------------------------------------------------------+
+--------------------------------------------------------------------+
| ducknng_register_http_route('http_route_demo', 'POST', '/echo', 'SELECT * FROM ducknng_http_text(
     201,
     (SELECT method || '' '' || path || ''?'' || coalesce(query_string, '''') ||
             '' x='' || coalesce(ducknng_http_query_param(''x''), '''') ||
             '' '' || coalesce(content_type, '''') ||
             '' '' || coalesce(body_text, '''')
      FROM ducknng_http_request(), ducknng_http_request_body()))') |
+--------------------------------------------------------------------+
| true                                                               |
+--------------------------------------------------------------------+
+---------------------------------------+
| ducknng_register_http_route_pattern('http_route_demo', 'GET', 'template', '/tenant/{tenant_id}/items/{item_id}', 'SELECT * FROM ducknng_http_text(
     202,
     (SELECT route_match_kind || '' '' ||
             ducknng_http_path_param(''tenant_id'') || '':'' ||
             ducknng_http_path_param(''item_id'')
      FROM ducknng_http_request()))') |
+---------------------------------------+
| true                                  |
+---------------------------------------+
```

#### Send test requests

``` sql
SELECT ok, status = 200, body_text
FROM ducknng_ncurl('http://127.0.0.1:18445/healthz', 'GET', NULL, NULL, 2000, 0::UBIGINT);
SELECT ok, status = 201, body_text
FROM ducknng_ncurl(
  'http://127.0.0.1:18445/echo?x=1', 'POST',
  '[{"name":"Content-Type","value":"text/plain"}]',
  'hello'::BLOB, 2000, 0::UBIGINT
);
SELECT ok, status = 202, body_text
FROM ducknng_ncurl(
  'http://127.0.0.1:18445/tenant/alice/items/42', 'GET', NULL, NULL, 2000, 0::UBIGINT
);
+------+----------------+-----------+
|  ok  | (status = 200) | body_text |
+------+----------------+-----------+
| true | true           | ok        |
+------+----------------+-----------+
+------+----------------+-------------------------------------+
|  ok  | (status = 201) |              body_text              |
+------+----------------+-------------------------------------+
| true | true           | POST /echo?x=1 x=1 text/plain hello |
+------+----------------+-------------------------------------+
+------+----------------+-------------------+
|  ok  | (status = 202) |     body_text     |
+------+----------------+-------------------+
| true | true           | template alice:42 |
+------+----------------+-------------------+
```

#### List routes and stop the server

``` sql
SELECT service_name, method, match_kind, path
FROM ducknng_list_http_routes()
WHERE service_name = 'http_route_demo'
ORDER BY match_kind, path;
SELECT ducknng_stop_server('http_route_demo');
+-----------------+--------+------------+-------------------------------------+
|  service_name   | method | match_kind |                path                 |
+-----------------+--------+------------+-------------------------------------+
| http_route_demo | POST   | exact      | /echo                               |
| http_route_demo | GET    | exact      | /healthz                            |
| http_route_demo | GET    | template   | /tenant/{tenant_id}/items/{item_id} |
+-----------------+--------+------------+-------------------------------------+
+----------------------------------------+
| ducknng_stop_server('http_route_demo') |
+----------------------------------------+
| true                                   |
+----------------------------------------+
```

By default these route handlers run on the backward-compatible
`shared_serialized_connection` lane, but services can switch before
traffic to `service_serialized_connection` or `request_connection` with
`ducknng_set_service_execution_model(...)`. Those models use pre-opened
DuckDB execution-pool connections from the same database handle, which
improves same-runtime gateway composition but means handlers should
depend on catalog-visible objects rather than temp tables or temp macros
from the init connection. Prefix and template routes stay low-level on
purpose: the request context remains explicit, while
`ducknng_http_header(...)`, `ducknng_http_query_param(...)`,
`ducknng_http_cookie(...)`, and `ducknng_http_path_param(...)` remove
the repetitive JSON/string parsing from common handlers. The response
helpers such as `ducknng_http_text(...)` and `ducknng_http_json(...)`
only build the existing one-row route response shape. That keeps the
route framework additive instead of turning it into a second RPC
namespace. They are not a safe excuse to expose arbitrary SQL to the
public internet.

A public subscriber gateway is a good fit for this layer, but it should
use separate backend DuckDB processes or runtime boundaries. The main
README still stays single-session, so the multi-process walkthrough
lives separately: `docs/subscriber_gateway_demo.md` explains the
topology, `demo/subscriber_gateway.py` is the live helper,
`make subscriber_gateway_demo` runs the end-to-end check, and
`make subscriber_gateway_rdm` renders a dedicated
`demo/subscriber_gateway.Rmd` walkthrough with hidden worker setup and
live HTTP requests.

### Launch raw socket send/recv airos and inspect send status explicitly

#### Open and connect the socket pair

``` sql
SET VARIABLE pair_a = (ducknng_open_socket('pair')).socket_id;
SET VARIABLE pair_listen_ok = (ducknng_listen_socket(
  getvariable('pair_a')::UBIGINT,
  'ipc:///tmp/ducknng_sql_pair_aio_demo.ipc',
  134217728, 0::UBIGINT
)).ok;
SET VARIABLE pair_b = (ducknng_open_socket('pair')).socket_id;
SET VARIABLE pair_dial_ok = (ducknng_dial_socket(
  getvariable('pair_b')::UBIGINT,
  'ipc:///tmp/ducknng_sql_pair_aio_demo.ipc',
  1000, 0::UBIGINT
)).ok;
```

#### Launch async send and receive, then collect results

``` sql
CREATE TEMP TABLE pair_recv AS
SELECT ducknng_recv_socket_raw_aio(getvariable('pair_a')::UBIGINT, 1000) AS recv_aio;
CREATE TEMP TABLE pair_send AS
SELECT ducknng_send_socket_raw_aio(
  getvariable('pair_b')::UBIGINT,
  from_hex('6173796e632072657175657374'),
  1000
) AS send_aio;
-- Send-only airos succeed with frame = NULL.
SELECT aio_id, ok, frame IS NULL AS no_frame
FROM ducknng_aio_collect((SELECT list_value(send_aio) FROM pair_send), 1000);
SELECT aio_id, ok, hex(frame) = '6173796E632072657175657374' AS got_payload
FROM ducknng_aio_collect((SELECT list_value(recv_aio) FROM pair_recv), 1000);
SELECT kind, state, phase, send_done, send_ok, recv_done, recv_ok IS NULL AS recv_ok_is_null
FROM ducknng_aio_status((SELECT send_aio FROM pair_send));


+--------+------+----------+
| aio_id |  ok  | no_frame |
+--------+------+----------+
| 2      | true | true     |
+--------+------+----------+
+--------+------+-------------+
| aio_id |  ok  | got_payload |
+--------+------+-------------+
| 1      | true | true        |
+--------+------+-------------+
+------+-----------+-------+-----------+---------+-----------+-----------------+
| kind |   state   | phase | send_done | send_ok | recv_done | recv_ok_is_null |
+------+-----------+-------+-----------+---------+-----------+-----------------+
| send | collected | send  | true      | true    | false     | true            |
+------+-----------+-------+-----------+---------+-----------+-----------------+
```

#### Clean up

``` sql
SELECT ducknng_aio_drop((SELECT send_aio FROM pair_send)) AS dropped_send,
       ducknng_aio_drop((SELECT recv_aio FROM pair_recv)) AS dropped_recv;
DROP TABLE pair_send;
DROP TABLE pair_recv;
SELECT (ducknng_close_socket(getvariable('pair_b')::UBIGINT)).ok AS closed_b,
       (ducknng_close_socket(getvariable('pair_a')::UBIGINT)).ok AS closed_a;
+--------------+--------------+
| dropped_send | dropped_recv |
+--------------+--------------+
| true         | true         |
+--------------+--------------+


+----------+----------+
| closed_b | closed_a |
+----------+----------+
| true     | true     |
+----------+----------+
```

### Push one raw message through `push` / `pull`

#### Open and connect

``` sql
SET VARIABLE pull_socket = (ducknng_open_socket('pull')).socket_id;
SET VARIABLE pull_listen_ok = (ducknng_listen_socket(
  getvariable('pull_socket')::UBIGINT,
  'ipc:///tmp/ducknng_sql_pushpull_demo.ipc',
  134217728, 0::UBIGINT
)).ok;
SET VARIABLE push_socket = (ducknng_open_socket('push')).socket_id;
SET VARIABLE push_dial_ok = (ducknng_dial_socket(
  getvariable('push_socket')::UBIGINT,
  'ipc:///tmp/ducknng_sql_pushpull_demo.ipc',
  1000, 0::UBIGINT
)).ok;
```

#### Send and receive

``` sql
CREATE TEMP TABLE pushpull_recv AS
SELECT ducknng_recv_socket_raw_aio(getvariable('pull_socket')::UBIGINT, 1000) AS recv_aio;
SET VARIABLE pushpull_sent = (ducknng_send_socket_raw(
  getvariable('push_socket')::UBIGINT, from_hex('707573682d70756c6c'), 1000
)).ok;
SELECT getvariable('pushpull_sent')::BOOLEAN AS sent,
       ok, hex(frame) = '707573682D70756C6C' AS got_payload
FROM ducknng_aio_collect((SELECT list_value(recv_aio) FROM pushpull_recv), 1000);


+------+------+-------------+
| sent |  ok  | got_payload |
+------+------+-------------+
| true | true | true        |
+------+------+-------------+
```

#### Clean up

``` sql
SELECT ducknng_aio_drop((SELECT recv_aio FROM pushpull_recv)) AS dropped;
DROP TABLE pushpull_recv;
SELECT (ducknng_close_socket(getvariable('push_socket')::UBIGINT)).ok AS closed_push,
       (ducknng_close_socket(getvariable('pull_socket')::UBIGINT)).ok AS closed_pull;
+---------+
| dropped |
+---------+
| true    |
+---------+

+-------------+-------------+
| closed_push | closed_pull |
+-------------+-------------+
| true        | true        |
+-------------+-------------+
```

### Publish one raw message through `pub` / `sub`

#### Open, subscribe, and connect

``` sql
SET VARIABLE pub_socket = (ducknng_open_socket('pub')).socket_id;
SET VARIABLE pub_listen_ok = (ducknng_listen_socket(
  getvariable('pub_socket')::UBIGINT,
  'ipc:///tmp/ducknng_sql_pubsub_demo.ipc',
  134217728, 0::UBIGINT
)).ok;
SET VARIABLE sub_socket = (ducknng_open_socket('sub')).socket_id;
SET VARIABLE sub_subscribe_ok = (ducknng_subscribe_socket(
  getvariable('sub_socket')::UBIGINT, from_hex('')
)).ok;
SET VARIABLE sub_dial_ok = (ducknng_dial_socket(
  getvariable('sub_socket')::UBIGINT,
  'ipc:///tmp/ducknng_sql_pubsub_demo.ipc',
  1000, 0::UBIGINT
)).ok;
```

#### Publish and receive

``` sql
CREATE TEMP TABLE pubsub_recv AS
SELECT ducknng_recv_socket_raw_aio(getvariable('sub_socket')::UBIGINT, 1000) AS recv_aio;
SET VARIABLE pubsub_sent = (ducknng_send_socket_raw(
  getvariable('pub_socket')::UBIGINT, from_hex('7075622d737562'), 1000
)).ok;
SELECT getvariable('pubsub_sent')::BOOLEAN AS sent,
       ok, hex(frame) = '7075622D737562' AS got_payload
FROM ducknng_aio_collect((SELECT list_value(recv_aio) FROM pubsub_recv), 1000);


+------+------+-------------+
| sent |  ok  | got_payload |
+------+------+-------------+
| true | true | true        |
+------+------+-------------+
```

#### Clean up

``` sql
SELECT ducknng_aio_drop((SELECT recv_aio FROM pubsub_recv)) AS dropped;
DROP TABLE pubsub_recv;
SELECT (ducknng_close_socket(getvariable('sub_socket')::UBIGINT)).ok AS closed_sub,
       (ducknng_close_socket(getvariable('pub_socket')::UBIGINT)).ok AS closed_pub;
+---------+
| dropped |
+---------+
| true    |
+---------+

+------------+------------+
| closed_sub | closed_pub |
+------------+------------+
| true       | true       |
+------------+------------+
```

### Exchange one survey and one response through `surveyor` / `respondent`

#### Open respondent listener and surveyor peer

``` sql
SET VARIABLE respondent_socket = (ducknng_open_socket('respondent')).socket_id;
SET VARIABLE respondent_listen_ok = (ducknng_listen_socket(
  getvariable('respondent_socket')::UBIGINT,
  'ipc:///tmp/ducknng_sql_survey_demo.ipc',
  134217728, 0::UBIGINT
)).ok;
SET VARIABLE surveyor_socket = (ducknng_open_socket('surveyor')).socket_id;
SET VARIABLE surveyor_dial_ok = (ducknng_dial_socket(
  getvariable('surveyor_socket')::UBIGINT,
  'ipc:///tmp/ducknng_sql_survey_demo.ipc',
  1000, 0::UBIGINT
)).ok;
```

#### Exchange the survey and the response

``` sql
CREATE TEMP TABLE respondent_recv AS
SELECT ducknng_recv_socket_raw_aio(getvariable('respondent_socket')::UBIGINT, 1000) AS recv_aio;
SET VARIABLE survey_sent = (ducknng_send_socket_raw(
  getvariable('surveyor_socket')::UBIGINT, from_hex('737572766579'), 1000
)).ok;
SELECT getvariable('survey_sent')::BOOLEAN AS sent_survey,
       ok, hex(frame) = '737572766579' AS got_survey
FROM ducknng_aio_collect((SELECT list_value(recv_aio) FROM respondent_recv), 1000);
CREATE TEMP TABLE surveyor_recv AS
SELECT ducknng_recv_socket_raw_aio(getvariable('surveyor_socket')::UBIGINT, 1000) AS recv_aio;
SET VARIABLE response_sent = (ducknng_send_socket_raw(
  getvariable('respondent_socket')::UBIGINT, from_hex('726573706f6e7365'), 1000
)).ok;
SELECT getvariable('response_sent')::BOOLEAN AS sent_response,
       ok, hex(frame) = '726573706F6E7365' AS got_response
FROM ducknng_aio_collect((SELECT list_value(recv_aio) FROM surveyor_recv), 1000);


+-------------+------+------------+
| sent_survey |  ok  | got_survey |
+-------------+------+------------+
| true        | true | true       |
+-------------+------+------------+


+---------------+------+--------------+
| sent_response |  ok  | got_response |
+---------------+------+--------------+
| true          | true | true         |
+---------------+------+--------------+
```

#### Clean up

``` sql
SELECT ducknng_aio_drop((SELECT recv_aio FROM respondent_recv)) AS dropped_respondent,
       ducknng_aio_drop((SELECT recv_aio FROM surveyor_recv)) AS dropped_surveyor;
DROP TABLE respondent_recv;
DROP TABLE surveyor_recv;
SELECT (ducknng_close_socket(getvariable('surveyor_socket')::UBIGINT)).ok AS closed_surveyor,
       (ducknng_close_socket(getvariable('respondent_socket')::UBIGINT)).ok AS closed_respondent;
+--------------------+------------------+
| dropped_respondent | dropped_surveyor |
+--------------------+------------------+
| true               | true             |
+--------------------+------------------+


+-----------------+-------------------+
| closed_surveyor | closed_respondent |
+-----------------+-------------------+
| true            | true              |
+-----------------+-------------------+
```

### Broadcast one raw message through a three-peer `bus` mesh

Every peer in a bus sends to and receives from every other peer, so a
single send from peer A is delivered to both B and C.

#### Open three bus peers and connect

``` sql
SET VARIABLE bus_a = (ducknng_open_socket('bus')).socket_id;
SET VARIABLE bus_b = (ducknng_open_socket('bus')).socket_id;
SET VARIABLE bus_c = (ducknng_open_socket('bus')).socket_id;
-- A listens; B and C dial A for a fully connected mesh.
SET VARIABLE bus_a_listen_ok = (ducknng_listen_socket(
  getvariable('bus_a')::UBIGINT,
  'ipc:///tmp/ducknng_sql_bus_demo.ipc',
  134217728, 0::UBIGINT
)).ok;
SET VARIABLE bus_b_dial_ok = (ducknng_dial_socket(
  getvariable('bus_b')::UBIGINT,
  'ipc:///tmp/ducknng_sql_bus_demo.ipc',
  1000, 0::UBIGINT
)).ok;
SET VARIABLE bus_c_dial_ok = (ducknng_dial_socket(
  getvariable('bus_c')::UBIGINT,
  'ipc:///tmp/ducknng_sql_bus_demo.ipc',
  1000, 0::UBIGINT
)).ok;
```

#### Arm receivers, broadcast from A, and collect at B and C

``` sql
CREATE TEMP TABLE bus_recv_b AS
SELECT ducknng_recv_socket_raw_aio(getvariable('bus_b')::UBIGINT, 1000) AS recv_aio;
CREATE TEMP TABLE bus_recv_c AS
SELECT ducknng_recv_socket_raw_aio(getvariable('bus_c')::UBIGINT, 1000) AS recv_aio;
SET VARIABLE bus_sent = (ducknng_send_socket_raw(
  getvariable('bus_a')::UBIGINT, from_hex('6275732d62726f616463617374'), 1000
)).ok;
SELECT getvariable('bus_sent')::BOOLEAN AS sent,
       ok, hex(frame) = '6275732D62726F616463617374' AS got_payload
FROM ducknng_aio_collect((SELECT list_value(recv_aio) FROM bus_recv_b), 1000);
SELECT ok, hex(frame) = '6275732D62726F616463617374' AS got_payload
FROM ducknng_aio_collect((SELECT list_value(recv_aio) FROM bus_recv_c), 1000);



+------+------+-------------+
| sent |  ok  | got_payload |
+------+------+-------------+
| true | true | true        |
+------+------+-------------+
+------+-------------+
|  ok  | got_payload |
+------+-------------+
| true | true        |
+------+-------------+
```

#### Clean up

``` sql
SELECT ducknng_aio_drop((SELECT recv_aio FROM bus_recv_b)) AS dropped_b,
       ducknng_aio_drop((SELECT recv_aio FROM bus_recv_c)) AS dropped_c;
DROP TABLE bus_recv_b;
DROP TABLE bus_recv_c;
SELECT (ducknng_close_socket(getvariable('bus_c')::UBIGINT)).ok AS closed_c,
       (ducknng_close_socket(getvariable('bus_b')::UBIGINT)).ok AS closed_b,
       (ducknng_close_socket(getvariable('bus_a')::UBIGINT)).ok AS closed_a;
+-----------+-----------+
| dropped_b | dropped_c |
+-----------+-----------+
| true      | true      |
+-----------+-----------+


+----------+----------+----------+
| closed_c | closed_b | closed_a |
+----------+----------+----------+
| true     | true     | true     |
+----------+----------+----------+
```

### Launch raw requests asynchronously and collect the reply frames later

The stable async contract is raw-result-first. Framed RPC aio helpers
collect raw reply frames through `ducknng_aio_collect(...)`, low-level
HTTP aio helpers collect HTTP-shaped rows through
`ducknng_ncurl_aio_collect(...)`, and callers explicitly decode frames
or bodies afterward. Expected aio launch failures, such as unsupported
URL schemes, invalid TLS handles, or missing socket handles, return
immediate terminal error handles so the caller can inspect
`ducknng_aio_status(...)` or collect the error row without turning the
launch itself into a DuckDB exception. `ducknng_aio_wait(...)` is the
wait-without-consuming primitive for lifecycle code that needs to
inspect or drop a terminal handle later.
`ducknng_aio_collect_decoded(...)` projects the decoded envelope columns
directly through the same low-level frame scalar accessors. Aio handles
represent one pending operation, not a background job or streaming
protocol.

#### Start the listener and launch two parallel requests

``` sql
SELECT ducknng_start_server(
  'sql_aio_demo', 'ipc:///tmp/ducknng_sql_aio_demo.ipc',
  1, 134217728, 300000, 0
);
-- timeout_ms bounds the pending network op; wait_ms in collect() is separate.
CREATE TEMP TABLE aio_demo AS
SELECT
  ducknng_request_raw_aio(
    'ipc:///tmp/ducknng_sql_aio_demo.ipc',
    from_hex('01000000000000000000000000000000000000000000'),
    1000, 0::UBIGINT
  ) AS aio1,
  ducknng_request_raw_aio(
    'ipc:///tmp/ducknng_sql_aio_demo.ipc',
    from_hex('01000000000000000000000000000000000000000000'),
    1000, 0::UBIGINT
  ) AS aio2;
SELECT aio1 > 0 AS aio1_started, aio2 > aio1 AS aio2_started_after_aio1
FROM aio_demo;
+------------------------------------------------------------------------------------------------------+
| ducknng_start_server('sql_aio_demo', 'ipc:///tmp/ducknng_sql_aio_demo.ipc', 1, 134217728, 300000, 0) |
+------------------------------------------------------------------------------------------------------+
| true                                                                                                 |
+------------------------------------------------------------------------------------------------------+

+--------------+-------------------------+
| aio1_started | aio2_started_after_aio1 |
+--------------+-------------------------+
| true         | true                    |
+--------------+-------------------------+
```

#### Wait, collect, and inspect terminal handles

``` sql
-- Wait without consuming — status/drop logic can still run after this.
SELECT ducknng_aio_wait((SELECT list_value(aio1, aio2) FROM aio_demo), 1000) AS any_ready;
SELECT aio_id, ok, octet_length(frame) > 0 AS has_frame
FROM ducknng_aio_collect((SELECT list_value(aio1, aio2) FROM aio_demo), 0)
ORDER BY aio_id;
-- A collected aio stays terminal (ready = true) until explicitly dropped.
SELECT ducknng_aio_ready(aio1) AS aio1_ready, ducknng_aio_ready(aio2) AS aio2_ready
FROM aio_demo;
+-----------+
| any_ready |
+-----------+
| true      |
+-----------+
+--------+------+-----------+
| aio_id |  ok  | has_frame |
+--------+------+-----------+
| 9      | true | true      |
| 10     | true | true      |
+--------+------+-----------+
+------------+------------+
| aio1_ready | aio2_ready |
+------------+------------+
| true       | true       |
+------------+------------+
```

#### Drop handles and stop the server

``` sql
SELECT ducknng_aio_drop(aio1) AND ducknng_aio_drop(aio2) AS dropped
FROM aio_demo;
DROP TABLE aio_demo;
SELECT ducknng_stop_server('sql_aio_demo');
+---------+
| dropped |
+---------+
| true    |
+---------+

+-------------------------------------+
| ducknng_stop_server('sql_aio_demo') |
+-------------------------------------+
| true                                |
+-------------------------------------+
```

### Launch unary RPC calls asynchronously and decode the replies later

These helpers build the manifest and exec request frames for you and sit
above the same `ducknng_request_raw_aio(...)` substrate. Decoding
remains explicit.

#### Start the server

`exec` is already in the global registry from the earlier example. A
fresh server inherits the current registry automatically.

``` sql
SELECT ducknng_start_server(
  'sql_rpc_aio_demo', 'ipc:///tmp/ducknng_sql_rpc_aio_demo.ipc',
  1, 134217728, 300000, 0
);
+--------------------------------------------------------------------------------------------------------------+
| ducknng_start_server('sql_rpc_aio_demo', 'ipc:///tmp/ducknng_sql_rpc_aio_demo.ipc', 1, 134217728, 300000, 0) |
+--------------------------------------------------------------------------------------------------------------+
| true                                                                                                         |
+--------------------------------------------------------------------------------------------------------------+
```

#### Launch manifest and exec aio calls

``` sql
SET VARIABLE manifest_aio = ducknng_get_rpc_manifest_raw_aio(
  'ipc:///tmp/ducknng_sql_rpc_aio_demo.ipc', 1000, 0::UBIGINT
);
SET VARIABLE exec_aio = ducknng_run_rpc_raw_aio(
  'ipc:///tmp/ducknng_sql_rpc_aio_demo.ipc',
  'CREATE TABLE IF NOT EXISTS rpc_aio_demo_t(i INTEGER)',
  1000, 0::UBIGINT
);
SELECT getvariable('manifest_aio') > 0 AS manifest_aio_started,
       getvariable('exec_aio') > getvariable('manifest_aio') AS exec_aio_after_manifest;


+----------------------+-------------------------+
| manifest_aio_started | exec_aio_after_manifest |
+----------------------+-------------------------+
| true                 | true                    |
+----------------------+-------------------------+
```

#### Collect and decode the frames

``` sql
CREATE TEMP TABLE rpc_aio_collect AS
SELECT *
FROM ducknng_aio_collect(
  list_value(getvariable('manifest_aio'), getvariable('exec_aio')), 1000
);
SET VARIABLE manifest_frame = (
  SELECT frame FROM rpc_aio_collect WHERE aio_id = getvariable('manifest_aio')
);
SET VARIABLE exec_frame = (
  SELECT frame FROM rpc_aio_collect WHERE aio_id = getvariable('exec_aio')
);
SELECT ok, type_name, name,
       position('"name":"exec"' IN payload_text) > 0 AS has_exec
FROM ducknng_decode_frame(getvariable('manifest_frame'));
SELECT ok, type_name, name
FROM ducknng_decode_frame(getvariable('exec_frame'));



+------+-----------+----------+----------+
|  ok  | type_name |   name   | has_exec |
+------+-----------+----------+----------+
| true | result    | manifest | true     |
+------+-----------+----------+----------+
+------+-----------+------+
|  ok  | type_name | name |
+------+-----------+------+
| true | result    | exec |
+------+-----------+------+
```

#### Convenience decoded collection wrapper

``` sql
SET VARIABLE manifest_aio_decoded = ducknng_get_rpc_manifest_raw_aio(
  'ipc:///tmp/ducknng_sql_rpc_aio_demo.ipc', 1000, 0::UBIGINT
);
SELECT aio_id, ok, frame_ok, type_name, name
FROM ducknng_aio_collect_decoded(list_value(getvariable('manifest_aio_decoded')), 1000);

+--------+------+----------+-----------+----------+
| aio_id |  ok  | frame_ok | type_name |   name   |
+--------+------+----------+-----------+----------+
| 13     | true | true     | result    | manifest |
+--------+------+----------+-----------+----------+
```

#### Clean up

``` sql
SELECT ducknng_aio_drop(getvariable('manifest_aio')) AND
       ducknng_aio_drop(getvariable('exec_aio')) AND
       ducknng_aio_drop(getvariable('manifest_aio_decoded')) AS dropped;
DROP TABLE rpc_aio_collect;
SELECT ducknng_stop_server('sql_rpc_aio_demo');
+---------+
| dropped |
+---------+
| true    |
+---------+

+-----------------------------------------+
| ducknng_stop_server('sql_rpc_aio_demo') |
+-----------------------------------------+
| true                                    |
+-----------------------------------------+
```

### Open, fetch, and close a query session explicitly

#### Start the server and open a session

``` sql
SELECT ducknng_start_server(
  'sql_session_demo', 'ipc:///tmp/ducknng_sql_session_demo.ipc',
  1, 134217728, 300000, 0
);
-- batch_rows = 0 and batch_bytes = 0 use server defaults.
-- Keep both session_id and session_token: the token is the bearer capability
-- required by fetch, close, and cancel.
CREATE TEMP TABLE session_open AS
SELECT *
FROM ducknng_open_query(
  'ipc:///tmp/ducknng_sql_session_demo.ipc',
  'SELECT 1 AS id UNION ALL SELECT 2 AS id ORDER BY id',
  0::UBIGINT, 0::UBIGINT, 0::UBIGINT
);
SET VARIABLE session_id    = (SELECT session_id    FROM session_open);
SET VARIABLE session_token = (SELECT session_token FROM session_open);
SELECT getvariable('session_id') AS opened_session_id,
       length(getvariable('session_token')::VARCHAR) AS session_token_chars,
       idle_timeout_ms AS effective_idle_timeout_ms
FROM session_open;
+--------------------------------------------------------------------------------------------------------------+
| ducknng_start_server('sql_session_demo', 'ipc:///tmp/ducknng_sql_session_demo.ipc', 1, 134217728, 300000, 0) |
+--------------------------------------------------------------------------------------------------------------+
| true                                                                                                         |
+--------------------------------------------------------------------------------------------------------------+



+-------------------+---------------------+---------------------------+
| opened_session_id | session_token_chars | effective_idle_timeout_ms |
+-------------------+---------------------+---------------------------+
| 1                 | 32                  | 300000                    |
+-------------------+---------------------+---------------------------+
```

#### Fetch rows and check the exhausted state

``` sql
-- Table helper decodes the first batch directly as rows.
SELECT *
FROM ducknng_fetch_query_table(
  'ipc:///tmp/ducknng_sql_session_demo.ipc',
  getvariable('session_id')::UBIGINT,
  getvariable('session_token')::VARCHAR,
  0::UBIGINT, 0::UBIGINT, 0::UBIGINT
);
-- After the table helper consumes the batch, the next fetch returns the
-- exhausted control reply (end_of_stream = true, no payload).
SELECT ok, session_id, state, payload IS NULL AS no_payload, end_of_stream
FROM ducknng_fetch_query(
  'ipc:///tmp/ducknng_sql_session_demo.ipc',
  getvariable('session_id')::UBIGINT,
  getvariable('session_token')::VARCHAR,
  0::UBIGINT, 0::UBIGINT, 0::UBIGINT
);
+----+
| id |
+----+
| 1  |
| 2  |
+----+
+------+------------+-----------+------------+---------------+
|  ok  | session_id |   state   | no_payload | end_of_stream |
+------+------------+-----------+------------+---------------+
| true | 1          | exhausted | true       | true          |
+------+------------+-----------+------------+---------------+
```

#### Close the session and stop the server

``` sql
SELECT ok, session_id, state
FROM ducknng_close_query(
  'ipc:///tmp/ducknng_sql_session_demo.ipc',
  getvariable('session_id')::UBIGINT,
  getvariable('session_token')::VARCHAR,
  0::UBIGINT
);
DROP TABLE session_open;
SELECT ducknng_stop_server('sql_session_demo');
+------+------------+--------+
|  ok  | session_id | state  |
+------+------------+--------+
| true | 1          | closed |
+------+------------+--------+

+-----------------------------------------+
| ducknng_stop_server('sql_session_demo') |
+-----------------------------------------+
| true                                    |
+-----------------------------------------+
```

### Drive the same session lifecycle through raw frames

#### Start the server and open a raw session

``` sql
SELECT ducknng_start_server(
  'sql_session_raw_demo', 'ipc:///tmp/ducknng_sql_session_raw_demo.ipc',
  1, 134217728, 300000, 0
);
SET VARIABLE raw_session_open_frame = ducknng_open_query_raw(
  'ipc:///tmp/ducknng_sql_session_raw_demo.ipc',
  'SELECT 1 AS id UNION ALL SELECT 2 AS id ORDER BY id',
  0::UBIGINT, 0::UBIGINT, 0::UBIGINT
);
SELECT ducknng_frame_error_text(getvariable('raw_session_open_frame')::BLOB) AS open_error,
       position('"session_id":' IN ducknng_frame_payload_text(getvariable('raw_session_open_frame')::BLOB)) > 0 AS has_session_id;
SET VARIABLE raw_session_id = (
  SELECT json_extract(
    ducknng_frame_payload_text(getvariable('raw_session_open_frame')::BLOB)::JSON,
    '$.session_id'
  )::UBIGINT
);
SET VARIABLE raw_session_token = (
  SELECT json_extract_string(
    ducknng_frame_payload_text(getvariable('raw_session_open_frame')::BLOB)::JSON,
    '$.session_token'
  )
);
+----------------------------------------------------------------------------------------------------------------------+
| ducknng_start_server('sql_session_raw_demo', 'ipc:///tmp/ducknng_sql_session_raw_demo.ipc', 1, 134217728, 300000, 0) |
+----------------------------------------------------------------------------------------------------------------------+
| true                                                                                                                 |
+----------------------------------------------------------------------------------------------------------------------+

+------------+----------------+
| open_error | has_session_id |
+------------+----------------+
| NULL       | true           |
+------------+----------------+
```

#### Fetch a batch and decode the Arrow payload

``` sql
SET VARIABLE raw_session_fetch_frame = ducknng_fetch_query_raw(
  'ipc:///tmp/ducknng_sql_session_raw_demo.ipc',
  getvariable('raw_session_id')::UBIGINT,
  getvariable('raw_session_token')::VARCHAR,
  0::UBIGINT, 0::UBIGINT, 0::UBIGINT
);
SELECT ducknng_frame_version(getvariable('raw_session_fetch_frame')::BLOB)      AS version,
       ducknng_frame_type_name(getvariable('raw_session_fetch_frame')::BLOB)    AS type_name,
       ducknng_frame_name(getvariable('raw_session_fetch_frame')::BLOB)         AS name,
       ducknng_frame_end_of_stream(getvariable('raw_session_fetch_frame')::BLOB) AS end_of_stream,
       ducknng_frame_flags(getvariable('raw_session_fetch_frame')::BLOB) > 0    AS has_flags;
SELECT *
FROM ducknng_parse_body(
  ducknng_frame_payload(getvariable('raw_session_fetch_frame')::BLOB),
  'application/vnd.apache.arrow.stream'
);

+---------+-----------+-------+---------------+-----------+
| version | type_name | name  | end_of_stream | has_flags |
+---------+-----------+-------+---------------+-----------+
| 1       | result    | fetch | false         | true      |
+---------+-----------+-------+---------------+-----------+
+----+
| id |
+----+
| 1  |
| 2  |
+----+
```

#### Close the raw session and stop the server

``` sql
SET VARIABLE raw_session_close_frame = ducknng_close_query_raw(
  'ipc:///tmp/ducknng_sql_session_raw_demo.ipc',
  getvariable('raw_session_id')::UBIGINT,
  getvariable('raw_session_token')::VARCHAR,
  0::UBIGINT
);
SELECT position('"state":"closed"' IN ducknng_frame_payload_text(
  getvariable('raw_session_close_frame')::BLOB
)) > 0 AS is_closed;
SELECT ducknng_stop_server('sql_session_raw_demo');

+-----------+
| is_closed |
+-----------+
| true      |
+-----------+
+---------------------------------------------+
| ducknng_stop_server('sql_session_raw_demo') |
+---------------------------------------------+
| true                                        |
+---------------------------------------------+
```

### Launch the session lifecycle asynchronously and collect raw frames

#### Start the server

``` sql
SELECT ducknng_start_server(
  'sql_session_aio_demo', 'ipc:///tmp/ducknng_sql_session_aio_demo.ipc',
  1, 134217728, 300000, 0
);
+----------------------------------------------------------------------------------------------------------------------+
| ducknng_start_server('sql_session_aio_demo', 'ipc:///tmp/ducknng_sql_session_aio_demo.ipc', 1, 134217728, 300000, 0) |
+----------------------------------------------------------------------------------------------------------------------+
| true                                                                                                                 |
+----------------------------------------------------------------------------------------------------------------------+
```

#### Open session asynchronously

``` sql
SET VARIABLE session_open_aio = ducknng_open_query_raw_aio(
  'ipc:///tmp/ducknng_sql_session_aio_demo.ipc',
  'SELECT 1 AS id UNION ALL SELECT 2 AS id ORDER BY id',
  0::UBIGINT, 0::UBIGINT, 1000, 0::UBIGINT
);
CREATE TEMP TABLE session_open_aio_collect AS
SELECT * FROM ducknng_aio_collect(list_value(getvariable('session_open_aio')::UBIGINT), 1000);
SET VARIABLE session_open_frame = (SELECT frame FROM session_open_aio_collect);
SELECT ok, type_name, name,
       position('"session_id":' IN payload_text) > 0 AS has_session_id
FROM ducknng_decode_frame(getvariable('session_open_frame')::BLOB);
SET VARIABLE session_aio_id = (
  SELECT json_extract(payload_text::JSON, '$.session_id')::UBIGINT
  FROM ducknng_decode_frame(getvariable('session_open_frame')::BLOB)
);
SET VARIABLE session_aio_token = (
  SELECT json_extract_string(payload_text::JSON, '$.session_token')
  FROM ducknng_decode_frame(getvariable('session_open_frame')::BLOB)
);
SELECT ducknng_aio_drop(getvariable('session_open_aio')::UBIGINT) AS dropped_open_aio;
DROP TABLE session_open_aio_collect;



+------+-----------+------------+----------------+
|  ok  | type_name |    name    | has_session_id |
+------+-----------+------------+----------------+
| true | result    | query_open | true           |
+------+-----------+------------+----------------+


+------------------+
| dropped_open_aio |
+------------------+
| true             |
+------------------+
```

#### Fetch asynchronously

``` sql
SET VARIABLE session_fetch_aio = ducknng_fetch_query_raw_aio(
  'ipc:///tmp/ducknng_sql_session_aio_demo.ipc',
  getvariable('session_aio_id')::UBIGINT,
  getvariable('session_aio_token')::VARCHAR,
  0::UBIGINT, 0::UBIGINT, 1000, 0::UBIGINT
);
CREATE TEMP TABLE session_fetch_aio_collect AS
SELECT * FROM ducknng_aio_collect(list_value(getvariable('session_fetch_aio')::UBIGINT), 1000);
SET VARIABLE session_fetch_frame = (SELECT frame FROM session_fetch_aio_collect);
SELECT ok, type_name, name, octet_length(payload) > 0 AS has_payload
FROM ducknng_decode_frame(getvariable('session_fetch_frame')::BLOB);
SELECT ducknng_aio_drop(getvariable('session_fetch_aio')::UBIGINT) AS dropped_fetch_aio;
DROP TABLE session_fetch_aio_collect;



+------+-----------+-------+-------------+
|  ok  | type_name | name  | has_payload |
+------+-----------+-------+-------------+
| true | result    | fetch | true        |
+------+-----------+-------+-------------+
+-------------------+
| dropped_fetch_aio |
+-------------------+
| true              |
+-------------------+
```

#### Close asynchronously

``` sql
SET VARIABLE session_close_aio = ducknng_close_query_raw_aio(
  'ipc:///tmp/ducknng_sql_session_aio_demo.ipc',
  getvariable('session_aio_id')::UBIGINT,
  getvariable('session_aio_token')::VARCHAR,
  1000, 0::UBIGINT
);
CREATE TEMP TABLE session_close_aio_collect AS
SELECT * FROM ducknng_aio_collect(list_value(getvariable('session_close_aio')::UBIGINT), 1000);
SET VARIABLE session_close_frame = (SELECT frame FROM session_close_aio_collect);
SELECT ok, type_name, name,
       position('"state":"closed"' IN payload_text) > 0 AS is_closed
FROM ducknng_decode_frame(getvariable('session_close_frame')::BLOB);
SELECT ducknng_aio_drop(getvariable('session_close_aio')::UBIGINT) AS dropped_close_aio;
DROP TABLE session_close_aio_collect;



+------+-----------+-------+-----------+
|  ok  | type_name | name  | is_closed |
+------+-----------+-------+-----------+
| true | result    | close | true      |
+------+-----------+-------+-----------+
+-------------------+
| dropped_close_aio |
+-------------------+
| true              |
+-------------------+
```

#### Cancel asynchronously

``` sql
CREATE TEMP TABLE session_cancel_open AS
SELECT * FROM ducknng_open_query(
  'ipc:///tmp/ducknng_sql_session_aio_demo.ipc',
  'SELECT 99 AS id', 0::UBIGINT, 0::UBIGINT, 0::UBIGINT
);
SET VARIABLE session_cancel_id    = (SELECT session_id    FROM session_cancel_open);
SET VARIABLE session_cancel_token = (SELECT session_token FROM session_cancel_open);
SET VARIABLE session_cancel_aio = ducknng_cancel_query_raw_aio(
  'ipc:///tmp/ducknng_sql_session_aio_demo.ipc',
  getvariable('session_cancel_id')::UBIGINT,
  getvariable('session_cancel_token')::VARCHAR,
  1000, 0::UBIGINT
);
CREATE TEMP TABLE session_cancel_aio_collect AS
SELECT * FROM ducknng_aio_collect(list_value(getvariable('session_cancel_aio')::UBIGINT), 1000);
SET VARIABLE session_cancel_frame = (SELECT frame FROM session_cancel_aio_collect);
SELECT ok, type_name, name,
       position('"state":"cancelled"' IN payload_text) > 0 AS is_cancelled
FROM ducknng_decode_frame(getvariable('session_cancel_frame')::BLOB);
SELECT ducknng_aio_drop(getvariable('session_cancel_aio')::UBIGINT) AS dropped_cancel_aio;
DROP TABLE session_cancel_open;
DROP TABLE session_cancel_aio_collect;






+------+-----------+--------+--------------+
|  ok  | type_name |  name  | is_cancelled |
+------+-----------+--------+--------------+
| true | result    | cancel | true         |
+------+-----------+--------+--------------+
+--------------------+
| dropped_cancel_aio |
+--------------------+
| true               |
+--------------------+
```

#### Stop the server

``` sql
SELECT ducknng_stop_server('sql_session_aio_demo');
+---------------------------------------------+
| ducknng_stop_server('sql_session_aio_demo') |
+---------------------------------------------+
| true                                        |
+---------------------------------------------+
```

### `tls+tcp://` with a self-signed development TLS config

``` sql
-- Generate a self-signed loopback certificate and keep it as a runtime TLS handle.
SET VARIABLE tls_self_id = ducknng_self_signed_tls_config('127.0.0.1', 365, 0);
-- Start a TLS listener using the generated config handle.
SELECT ducknng_start_server(
  'tls_demo_self',               -- service name
  'tls+tcp://127.0.0.1:45453',   -- listen URL
  1,                             -- REP contexts
  134217728,                     -- recv_max_bytes
  300000,                        -- session_idle_ms
  getvariable('tls_self_id')::UBIGINT  -- tls_config_id returned above
);
-- Send the built-in manifest request frame over TLS and decode the reply.
SELECT ok, type_name, name, position('"name":"exec"' IN payload_text) > 0
FROM ducknng_decode_frame(
  ducknng_request_raw(
    'tls+tcp://127.0.0.1:45453',                -- url
    from_hex('01000000000000000000000000000000000000000000'), -- manifest request frame
    1000,                                       -- timeout_ms
    getvariable('tls_self_id')::UBIGINT         -- tls_config_id
  )
);
-- Clean up the TLS demo server and config handle.
SELECT ducknng_stop_server('tls_demo_self');
SELECT ducknng_drop_tls_config(getvariable('tls_self_id')::UBIGINT);

+-----------------------------------------------------------------------------------------------------------------------------------------+
| ducknng_start_server('tls_demo_self', 'tls+tcp://127.0.0.1:45453', 1, 134217728, 300000, CAST(getvariable('tls_self_id') AS "UBIGINT")) |
+-----------------------------------------------------------------------------------------------------------------------------------------+
| true                                                                                                                                    |
+-----------------------------------------------------------------------------------------------------------------------------------------+
+------+-----------+----------+------------------------------------------------------+
|  ok  | type_name |   name   | (main."position"(payload_text, '"name":"exec"') > 0) |
+------+-----------+----------+------------------------------------------------------+
| true | result    | manifest | true                                                 |
+------+-----------+----------+------------------------------------------------------+
+--------------------------------------+
| ducknng_stop_server('tls_demo_self') |
+--------------------------------------+
| true                                 |
+--------------------------------------+
+------------------------------------------------------------------------+
| ducknng_drop_tls_config(CAST(getvariable('tls_self_id') AS "UBIGINT")) |
+------------------------------------------------------------------------+
| true                                                                   |
+------------------------------------------------------------------------+
```

For mTLS, create the same kind of TLS handle with `auth_mode = 2`. On
listeners this requires the peer to present a certificate trusted by the
configured CA; on clients it requires server verification. The
dispatcher derives the current caller identity from the first verified
peer certificate SAN as `tls:san:<value>`, falling back to the common
name as `tls:cn:<common-name>`. Sessions opened over mTLS are still
controlled by `session_token`, but they are also bound to that verified
peer identity. `ducknng_set_tls_peer_allowlist(...)` sets an exact
identity allowlist copied into future services, and
`ducknng_set_service_peer_allowlist(...)` changes the allowlist
dynamically for a running service. NNG listeners use NNG’s
`NNG_PIPE_EV_ADD_PRE` pipe notification to close non-admitted new pipes
before they are added to the socket; HTTP/HTTPS returns HTTP `403`
before RPC dispatch. `ducknng_list_servers()` exposes `execution_model`,
`active_pipes`, `max_active_pipes`, `inflight_requests`,
`max_inflight_requests`, `max_sessions_per_peer_identity`,
`tls_enabled`, `tls_auth_mode`, `peer_identity_required`,
`peer_allowlist_active`, `ip_allowlist_active`, `sql_authorizer_active`,
and the corresponding counts so deployments can distinguish the current
execution-lane contract, current NNG membership, TLS without client
verification, mTLS, allowlisted mTLS, IP-gated services, and services
with SQL authorization callbacks. Individual registry-backed methods can
also require verified peer identity; for example,
`ducknng_set_method_auth('manifest', true)` protects manifest discovery
without unregistering the method.

The built-in mTLS, peer-identity allowlist, and IP/CIDR allowlist checks
are the fast C path for common denials. They run before the flexible SQL
callback and, for NNG transports, can reject new pipes at
`NNG_PIPE_EV_ADD_PRE` without entering DuckDB. Basic service resource
limits also start in C:
`ducknng_set_service_limits(name, max_open_sessions)` caps concurrently
open query sessions for a service, the three-argument form also caps
simultaneously active NNG protocol-socket pipes at `ADD_PRE`, and the
four-argument form also caps concurrent in-flight requests before SQL
authorizers and RPC dispatch, and the five-argument form also caps
concurrently open query sessions per verified peer identity; `0` means
unlimited for any cap. Those are the stable built-in quota owners today:
the service itself and, when present, the verified peer identity. The
optional SQL-authorizer `principal` column is deployment policy/audit
metadata, not yet durable session ownership metadata for built-in
quotas. For policy that depends on tables, tenants, method names,
headers, or deployment-specific rules, install a service-level SQL
authorizer with `ducknng_set_service_authorizer(name, authorizer_sql)`.
The callback sees one row from `ducknng_auth_context()` while it runs
and must return exactly one row with a Boolean `allow` column; optional
columns are `status`, `reason`, `principal`, `claims_json`, and
`cache_ttl_ms`. `NULL` or an empty string clears the SQL authorizer.
This callback is intentionally evaluated at the request/dispatch
boundary, not inside NNG’s low-level pipe callback. It runs through the
configured service-owned DuckDB execution model, so keep it short and
side-effect-light: prefer table/view lookups, avoid recursive
`ducknng_*` client calls to the same service, avoid stopping the service
from its own callback, and do not use it for long-running work. That
limitation avoids deadlocks while keeping one uniform policy interface
for NNG, HTTP/HTTPS framed RPC, and HTTP routes.

### `tls+tcp://` from file-backed certificate material

``` sql
-- Register a file-backed TLS config using committed loopback test certificates.
SET VARIABLE tls_files_id = ducknng_tls_config_from_files(
  'test/certs/loopback-cert-key.pem', -- cert_key_file
  'test/certs/loopback-ca.pem',       -- ca_file
  NULL,                               -- password
  0                                   -- auth_mode
);
-- Start a TLS listener with that file-backed config.
SELECT ducknng_start_server(
  'tls_demo_files',              -- service name
  'tls+tcp://127.0.0.1:45454',   -- listen URL
  1,                             -- REP contexts
  134217728,                     -- recv_max_bytes
  300000,                        -- session_idle_ms
  getvariable('tls_files_id')::UBIGINT  -- tls_config_id returned above
);
-- Inspect the manifest reply over TLS using the same config handle on the client side.
SELECT ok, type_name, name, position('"name":"exec"' IN payload_text) > 0
FROM ducknng_decode_frame(
  ducknng_request_raw(
    'tls+tcp://127.0.0.1:45454',                -- url
    from_hex('01000000000000000000000000000000000000000000'), -- manifest request frame
    1000,                                       -- timeout_ms
    getvariable('tls_files_id')::UBIGINT        -- tls_config_id
  )
);
-- Clean up the file-backed TLS demo server and config handle.
SELECT ducknng_stop_server('tls_demo_files');
SELECT ducknng_drop_tls_config(getvariable('tls_files_id')::UBIGINT);

+-------------------------------------------------------------------------------------------------------------------------------------------+
| ducknng_start_server('tls_demo_files', 'tls+tcp://127.0.0.1:45454', 1, 134217728, 300000, CAST(getvariable('tls_files_id') AS "UBIGINT")) |
+-------------------------------------------------------------------------------------------------------------------------------------------+
| true                                                                                                                                      |
+-------------------------------------------------------------------------------------------------------------------------------------------+
+------+-----------+----------+------------------------------------------------------+
|  ok  | type_name |   name   | (main."position"(payload_text, '"name":"exec"') > 0) |
+------+-----------+----------+------------------------------------------------------+
| true | result    | manifest | true                                                 |
+------+-----------+----------+------------------------------------------------------+
+---------------------------------------+
| ducknng_stop_server('tls_demo_files') |
+---------------------------------------+
| true                                  |
+---------------------------------------+
+-------------------------------------------------------------------------+
| ducknng_drop_tls_config(CAST(getvariable('tls_files_id') AS "UBIGINT")) |
+-------------------------------------------------------------------------+
| true                                                                    |
+-------------------------------------------------------------------------+
```

### `ws://` and `wss://` as NNG transports

`ws://` and `wss://` are NNG transport schemes, not the HTTP carrier
described in `docs/http.md`. They use `ducknng_start_server(...)`, the
same framed manifest/RPC/session helpers, and the same TLS handle model
as the other NNG transports.

``` sql
-- Plain WebSocket transport through the NNG service layer.
SELECT ducknng_start_server(
  'ws_demo',
  'ws://127.0.0.1:45455/_ducknng',
  1,
  134217728,
  300000,
  0::UBIGINT
);
SELECT ok, type_name, name
FROM ducknng_decode_frame(
  ducknng_request_raw(
    'ws://127.0.0.1:45455/_ducknng',
    from_hex('01000000000000000000000000000000000000000000'),
    1000,
    0::UBIGINT
  )
);
SELECT ducknng_stop_server('ws_demo');
-- Secure WebSocket transport uses the same reusable TLS handle model.
SET VARIABLE tls_wss_id = ducknng_self_signed_tls_config('127.0.0.1', 365, 0);
SELECT ducknng_start_server(
  'wss_demo',
  'wss://127.0.0.1:45456/_ducknng',
  1,
  134217728,
  300000,
  getvariable('tls_wss_id')::UBIGINT
);
SELECT ok, type_name, name
FROM ducknng_decode_frame(
  ducknng_request_raw(
    'wss://127.0.0.1:45456/_ducknng',
    from_hex('01000000000000000000000000000000000000000000'),
    1000,
    getvariable('tls_wss_id')::UBIGINT
  )
);
SELECT ducknng_stop_server('wss_demo');
SELECT ducknng_drop_tls_config(getvariable('tls_wss_id')::UBIGINT);
+--------------------------------------------------------------------------------------------------------------+
| ducknng_start_server('ws_demo', 'ws://127.0.0.1:45455/_ducknng', 1, 134217728, 300000, CAST(0 AS "UBIGINT")) |
+--------------------------------------------------------------------------------------------------------------+
| true                                                                                                         |
+--------------------------------------------------------------------------------------------------------------+
+------+-----------+----------+
|  ok  | type_name |   name   |
+------+-----------+----------+
| true | result    | manifest |
+------+-----------+----------+
+--------------------------------+
| ducknng_stop_server('ws_demo') |
+--------------------------------+
| true                           |
+--------------------------------+

+----------------------------------------------------------------------------------------------------------------------------------------+
| ducknng_start_server('wss_demo', 'wss://127.0.0.1:45456/_ducknng', 1, 134217728, 300000, CAST(getvariable('tls_wss_id') AS "UBIGINT")) |
+----------------------------------------------------------------------------------------------------------------------------------------+
| true                                                                                                                                   |
+----------------------------------------------------------------------------------------------------------------------------------------+
+------+-----------+----------+
|  ok  | type_name |   name   |
+------+-----------+----------+
| true | result    | manifest |
+------+-----------+----------+
+---------------------------------+
| ducknng_stop_server('wss_demo') |
+---------------------------------+
| true                            |
+---------------------------------+
+-----------------------------------------------------------------------+
| ducknng_drop_tls_config(CAST(getvariable('tls_wss_id') AS "UBIGINT")) |
+-----------------------------------------------------------------------+
| true                                                                  |
+-----------------------------------------------------------------------+
```

### REQ/REP `EXEC` via `nanonext` as an interop example

This example is easier to follow as a short sequence: define the frame
helpers, define the retry helpers, start a local server, run a few
remote `exec` requests, inspect the same table locally, and then clean
up.

#### Frame encoding and decoding helpers

``` r
# Little-endian helpers for the versioned ducknng RPC envelope.
write_u32le <- function(x) {
  writeBin(as.integer(x), raw(), size = 4L, endian = "little")
}

write_u64le <- function(x) {
  x <- as.double(x)
  c(write_u32le(x %% 2^32), write_u32le(floor(x / 2^32)))
}

read_u32le_bytes <- function(x) {
  sum(as.double(as.integer(x)) * 256^(0:3))
}

read_u64le_bytes <- function(x) {
  read_u32le_bytes(x[1:4]) + 2^32 * read_u32le_bytes(x[5:8])
}

read_u32le <- function(buf, offset) {
  read_u32le_bytes(buf[offset + 0:3])
}

read_u64le <- function(buf, offset) {
  read_u64le_bytes(buf[offset + 0:7])
}

# Encode the Arrow IPC payload carried by the built-in exec RPC method.
encode_ducknng_exec_payload <- function(sql, want_result = FALSE) {
  raw_con <- rawConnection(raw(), open = "r+")
  on.exit(close(raw_con), add = TRUE)

  nanoarrow::write_nanoarrow(
    data.frame(sql = sql, want_result = want_result),
    raw_con
  )

  rawConnectionValue(raw_con)
}

# Build a version 1 RPC call frame for the named method.
encode_ducknng_call_frame <- function(method, payload, flags = 0L) {
  method_name <- charToRaw(method)

  c(
    as.raw(1L),
    as.raw(1L),
    write_u32le(flags),
    write_u32le(length(method_name)),
    write_u32le(0),
    write_u64le(length(payload)),
    method_name,
    payload
  )
}

encode_ducknng_exec_request <- function(sql, want_result = FALSE) {
  encode_ducknng_call_frame(
    method = "exec",
    payload = encode_ducknng_exec_payload(sql, want_result = want_result)
  )
}

# Decode a reply envelope and parse its Arrow IPC payload when present.
decode_ducknng_exec_reply <- function(buf) {
  header_size <- 23L

  if (!is.raw(buf) || length(buf) < (header_size - 1L)) {
    stop("ducknng reply frame was empty or truncated", call. = FALSE)
  }

  name_len <- read_u32le(buf, 7)
  error_len <- read_u32le(buf, 11)
  payload_len <- read_u64le(buf, 15)

  name_start <- header_size
  error_start <- name_start + name_len
  payload_start <- error_start + error_len
  payload_end <- payload_start + payload_len - 1L

  payload <- if (payload_len > 0) buf[payload_start:payload_end] else raw()

  list(
    version = as.integer(buf[1]),
    type = as.integer(buf[2]),
    flags = read_u32le(buf, 3),
    method = rawToChar(buf[name_start:(error_start - 1L)]),
    error = if (error_len > 0) rawToChar(buf[error_start:(payload_start - 1L)]) else "",
    payload = payload,
    data = if (payload_len > 0) as.data.frame(nanoarrow::read_nanoarrow(payload)) else NULL
  )
}
```

#### REQ/REP retry helpers

``` r
# Send and receive with the same retry style used in mangoro examples.
send_ducknng_frame <- function(socket, frame, timeout_ms = 1000L, max_attempts = 20L) {
  attempt <- 1L
  send_result <- nanonext::send(socket, frame, mode = "raw", block = timeout_ms)

  while (nanonext::is_error_value(send_result) && attempt < max_attempts) {
    Sys.sleep(0.25)
    send_result <- nanonext::send(socket, frame, mode = "raw", block = timeout_ms)
    attempt <- attempt + 1L
  }

  send_result
}

recv_ducknng_frame <- function(socket, timeout_ms = 1000L, max_attempts = 20L) {
  attempt <- 1L
  response <- nanonext::recv(socket, mode = "raw", block = timeout_ms)

  while (
    (
      nanonext::is_error_value(response) ||
      !is.raw(response) ||
      length(response) < 22L
    ) &&
    attempt < max_attempts
  ) {
    Sys.sleep(0.25)
    response <- nanonext::recv(socket, mode = "raw", block = timeout_ms)
    attempt <- attempt + 1L
  }

  response
}

run_ducknng_req <- function(socket, frame, timeout_ms = 1000L) {
  send_ducknng_frame(socket, frame, timeout_ms = timeout_ms)
  decode_ducknng_exec_reply(
    recv_ducknng_frame(socket, timeout_ms = timeout_ms)
  )
}

# Wait for an IPC listener path to appear before dialing it.
wait_for_ducknng_listener <- function(path, timeout_secs = 10, interval_secs = 0.1) {
  deadline <- Sys.time() + timeout_secs

  while (Sys.time() < deadline) {
    if (file.exists(path)) {
      return(invisible(TRUE))
    }
    Sys.sleep(interval_secs)
  }

  stop("ducknng IPC listener did not become ready in time", call. = FALSE)
}
```

#### Start a local ducknng server and dial a req socket

``` r
# Start a ducknng server in this R session, then talk to it over a req socket.
ext_path <- normalizePath("build/release/ducknng.duckdb_extension")
ipc_path <- "/tmp/ducknng_readme_exec.ipc"
unlink(ipc_path)
ipc_url <- paste0("ipc://", ipc_path)

db_driver <- duckdb::duckdb(
  dbdir = ":memory:",
  config = list(allow_unsigned_extensions = "true")
)
db_con <- DBI::dbConnect(db_driver)

DBI::dbGetQuery(db_con, "SELECT 42 AS ok")
  ok
1 42
DBI::dbExecute(db_con, sprintf("LOAD '%s'", ext_path))
[1] 0
DBI::dbGetQuery(
  db_con,
  sprintf(
    "SELECT ducknng_start_server('sql_exec', '%s', 1, 134217728, 300000, 0::UBIGINT)",
    ipc_url
  )
)
  ducknng_start_server('sql_exec', 'ipc:///tmp/ducknng_readme_exec.ipc', 1, 134217728, 300000, CAST(0 AS "UBIGINT"))
1                                                                                                               TRUE
DBI::dbGetQuery(db_con, "SELECT ducknng_register_exec_method()")
  ducknng_register_exec_method()
1                           TRUE

wait_for_ducknng_listener(ipc_path)
req <- nanonext::socket("req", dial = ipc_url)
```

#### Create and insert rows remotely

``` r
# Create a table remotely and inspect the metadata reply.
create_reply <- run_ducknng_req(
  req,
  encode_ducknng_exec_request("CREATE TABLE ducknng_exec_demo(i INTEGER)")
)
create_reply$data
  rows_changed statement_type result_type
1            0              7           2

# Insert rows remotely and inspect the second metadata reply.
insert_reply <- run_ducknng_req(
  req,
  encode_ducknng_exec_request("INSERT INTO ducknng_exec_demo VALUES (42), (99)")
)
insert_reply$data
  rows_changed statement_type result_type
1            2              2           1
```

#### Query rows remotely with `want_result = TRUE`

``` r
select_reply <- run_ducknng_req(
  req,
  encode_ducknng_exec_request(
    "SELECT i, i > 50 AS gt_50 FROM ducknng_exec_demo ORDER BY i",
    want_result = TRUE
  )
)
select_reply$data
   i gt_50
1 42 FALSE
2 99  TRUE
```

#### Inspect the same table locally through DuckDB

``` r
server_rows <- DBI::dbGetQuery(db_con, "SELECT * FROM ducknng_exec_demo ORDER BY i")
server_rows
   i
1 42
2 99
```

#### Stop the server and clean up

``` r
DBI::dbGetQuery(db_con, "SELECT ducknng_stop_server('sql_exec')")
  ducknng_stop_server('sql_exec')
1                            TRUE
close(req)
DBI::dbDisconnect(db_con)
unlink(ipc_path)
```

## Deployment and admission policy

`ducknng` defaults to low-level, default-open transport behavior. The
application or deployment decides where the trust boundary is. For
anything beyond process-local, filesystem-local, or loopback-only
development, set the boundary deliberately before exposing a service.

A strong direct-exposure baseline is:

1.  use `https://`, `wss://`, or `tls+tcp://` rather than plaintext
    network carriers;
2.  create the TLS handle with `auth_mode = 2` so peers must present a
    verified client certificate;
3.  configure a private CA or otherwise narrow certificate issuance so
    the trust root is not broader than the application;
4.  set exact peer-identity and/or IP/CIDR allowlists before exposure
    with `ducknng_set_tls_peer_allowlist(...)` and the optional
    `ip_allowlist_json` startup argument;
5.  set basic service resource limits such as
    `ducknng_set_service_limits(name, max_open_sessions[, max_active_pipes[, max_inflight_requests[, max_sessions_per_peer_identity]]])`
    before exposing query sessions;
6.  install a service-level SQL authorizer with
    `ducknng_set_service_authorizer(...)` when admission depends on
    deployment tables, tenants, method names, or HTTP metadata;
7.  optionally update a running service dynamically with
    `ducknng_set_service_peer_allowlist(...)`,
    `ducknng_set_service_ip_allowlist(...)`,
    `ducknng_set_service_limits(...)`, and
    `ducknng_set_service_authorizer(...)`.

Free-form SQL execution is intentionally a deployment-owned capability.
`ducknng` keeps its own generated SQL injection-safe, but it does not
make arbitrary user SQL safe: `exec` and query sessions run DuckDB SQL
as the service process on the configured DuckDB connection. Do not
expose `exec` or unrestricted query sessions to callers who are not
already authorized to run that SQL. Use these profiles as a starting
point:

- **Local development / single-user IPC**: `inproc://`, loopback, or
  `ipc://` under a private directory; `exec` may be enabled for
  convenience inside that local boundary.
- **Trusted service mesh**: `tls+tcp://`, `wss://`, or `https://` with
  mTLS, exact peer allowlists, IP/CIDR allowlists where useful, and
  service limits; register `exec` with
  `ducknng_register_exec_method(true)` or leave it disabled.
- **Shared or semi-trusted clients**: prefer query sessions plus a short
  SQL authorizer; use deployment-level DuckDB/process controls for
  filesystem, extension loading, outbound network, and attachments.
- **Public/untrusted internet**: do not expose raw DuckDB SQL directly.
  Put `ducknng` behind a gateway that authenticates, rate-limits, and
  maps users to fixed application operations, or build a separate route
  layer that does not forward arbitrary SQL text.

When writing SQL authorizers, read request fields from
`ducknng_auth_context()` and avoid string-concatenating untrusted HTTP
headers, method names, or payload text into new SQL. Keep the callback
short and side-effect-light.

The relevant identity strings currently come from verified TLS peer
metadata and are SAN-first, CN-fallback: `tls:san:<value>` or
`tls:cn:<common-name>`. Peer identity allowlists are exact-match. IP
allowlists are parsed once into IPv4/IPv6 literal or CIDR rules such as
`127.0.0.1/32`, `10.0.0.0/8`, or `::1/128`. `NULL` or an empty SQL
string clears an allowlist and returns to default-open admission subject
to TLS/method/session policy. An empty JSON array, `[]`, is active
deny-all.

Example shape:

``` sql
-- Set a default allowlist on the TLS handle before starting services with it.
SELECT ducknng_set_tls_peer_allowlist(
  1::UBIGINT,
  '["tls:san:client-a.example","tls:san:client-b.example"]'
);

-- Start with an IP/CIDR allowlist already installed before the listener accepts traffic.
SELECT ducknng_start_server(
  'sql_http',
  'http://127.0.0.1:18444/_ducknng',
  1,
  134217728,
  300000,
  0::UBIGINT,
  '["127.0.0.1/32"]'
);

-- Later, rotate or tighten admission for a running service.
SELECT ducknng_set_service_peer_allowlist(
  'sql_rpc',
  '["tls:san:client-b.example"]'
);
SELECT ducknng_set_service_ip_allowlist(
  'sql_http',
  '["127.0.0.1/32","10.0.0.0/8"]'
);

-- Set a basic service-side resource limit for query sessions.
SELECT ducknng_set_service_limits('sql_http', 16::UBIGINT);

-- Add a flexible SQL callback when policy depends on request context.
SELECT ducknng_set_service_authorizer(
  'sql_http',
  'SELECT remote_ip = ''127.0.0.1'' AND rpc_method = ''manifest'' AS allow,
          403 AS status,
          ''not authorized'' AS reason
   FROM ducknng_auth_context()'
);
```

Transport-specific deployment notes:

| Surface                                                                                          | Accepted schemes                                                                        | TLS handle accepted on             |
|--------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------|------------------------------------|
| `ducknng_start_server(...)`                                                                      | `inproc://`, `ipc://`, `tcp://`, `tls+tcp://`, `ws://`, `wss://`, `http://`, `https://` | `tls+tcp://`, `wss://`, `https://` |
| synchronous RPC/session helpers                                                                  | NNG schemes plus `http://`, `https://`                                                  | `tls+tcp://`, `wss://`, `https://` |
| raw RPC AIO helpers                                                                              | NNG schemes plus `http://`, `https://`                                                  | `tls+tcp://`, `wss://`, `https://` |
| generic socket APIs                                                                              | `inproc://`, `ipc://`, `tcp://`, `tls+tcp://`, `ws://`, `wss://`                        | `tls+tcp://`, `wss://`             |
| HTTP client helpers (`ducknng_ncurl(...)`, `ducknng_ncurl_aio(...)`, `ducknng_ncurl_table(...)`) | `http://`, `https://`                                                                   | `https://`                         |

Supplying a non-zero TLS handle on a non-TLS URL is rejected rather than
silently ignored. HTTP/HTTPS URLs are rejected by the generic NNG socket
layer, and NNG URLs are rejected by HTTP-only helpers.

- `inproc://` is a same-process boundary. It is useful for embedded
  testing/composition, but it is not network authentication.
- `ipc://` relies on filesystem or named-pipe permissions. Avoid
  world-writable shared paths for multi-user deployments.
- plaintext `tcp://`, `ws://`, and `http://` do not provide
  cryptographic peer identity. Use them only on loopback, a private
  network you already trust, or behind a reverse proxy/firewall/VPN that
  performs admission itself.
- `http://` is still useful as a local or reverse-proxy upstream
  carrier. For public HTTP-style deployment, expose `https://` directly
  with mTLS or place `ducknng` behind a proxy such as Caddy, nginx,
  Envoy, or a cloud load balancer that terminates TLS and enforces
  authentication/rate limits before forwarding to loopback `http://` or
  `ipc://`.
- `tls+tcp://` and `wss://` are NNG protocol transports. They use NNG
  socket protocols and NNG pipes, not the HTTP carrier semantics.
- `https://` is the HTTP frame carrier over TLS. It shares the same
  framed RPC methods and admission policy, but HTTP status codes such as
  `403` can appear before RPC dispatch.

IP allowlisting is a useful lighter-weight gate when client certificates
are too much operational overhead, but it is not the same as
cryptographic client identity. HTTPS itself does not standardize an
application IP allowlist; IP admission is normally implemented by the
listener, firewall, reverse proxy, load balancer, or service mesh.
`ducknng` now includes its own service-level IP/CIDR allowlist for
direct listeners. For NNG protocol transports, it uses `NNG_OPT_REMADDR`
from the NNG pipe and checks parsed binary CIDR rules during `ADD_PRE`
and again at dispatch. For HTTP/HTTPS, it reads the connection remote
address from the NNG supplemental HTTP connection and returns HTTP `403`
before RPC dispatch when the address is not admitted. A front door such
as Caddy, nginx, Envoy, cloud firewall/security groups, Kubernetes
network policy, or a similar proxy remains useful for public deployments
because it can combine IP/CIDR policy with rate limits, request logging,
WAF rules, and ordinary web authentication before forwarding to loopback
`http://`, `https://`, `ipc://`, or an internal NNG service.

For NNG transports, admission uses NNG’s own pipe notification
primitive: `nng_pipe_notify(..., NNG_PIPE_EV_ADD_PRE, ...)` lets
`ducknng` inspect verified TLS pipe metadata and remote-address metadata
and close a non-admitted pipe before it is added to the socket. For
HTTP/HTTPS, NNG’s supplemental HTTP API does not expose the same public
protocol-socket pipe admission hook, so `ducknng` checks the same policy
before RPC dispatch and returns HTTP `403` for non-admitted peers or
remote addresses.

The same pipe-event family is useful beyond admission. NNG exposes
`ADD_PRE`, `ADD_POST`, and `REM_POST` pipe events; nanonext surfaces
related tools as `pipe_notify()`, `monitor()`, and `read_monitor()`.
`ducknng` now records a bounded per-service NNG pipe event stream and
exposes it with `ducknng_read_monitor(name, after_seq, max_events)`. It
exposes ring/counter metadata with `ducknng_monitor_status(name)` and
current active NNG pipes with `ducknng_list_pipes(name)`. The event
stream includes event sequence, timestamp, service, transport, pipe id,
admission result for `ADD_PRE`, denial reason when the fast C path has
one, remote address, and verified peer identity when present; the
active-pipe table gives the current pipe set for routing and membership
decisions. These primitives are a natural foundation for telemetry,
connection counts, connection churn, per-service pipe event streams,
presence/worker membership, dynamic routing, backpressure-aware
scheduling, mesh-style DuckDB service discovery, routing/forwarding
demos, and other application-level event streams. HTTP/HTTPS framed RPC
currently uses the same admission policy but does not expose NNG
protocol-socket pipe events; a broader HTTP server framework can reuse
the same request context and monitor concepts without changing the
framed RPC wire protocol.

## Status

What is sealed and runnable in 0.1.0: NNG transports and socket
patterns; first-class one-shot AIO across socket send/recv, ncurl HTTP,
and unary RPC; framed RPC (`manifest`, opt-in `exec`, raw unary, query
sessions); low-level HTTP routes with exact, prefix, or template
matching plus SQL response rows, request-context/body helpers, named
request accessors, and one-row response builders; fast C admission
(mTLS, exact peer-identity allowlists, IP/CIDR allowlists, service
limits); SQL authorizer callbacks; bounded per-service pipe-event
monitor and active-pipe snapshot; body codec layer with built-in
providers and user-registered codec hooks. See
`function_catalog/functions.md` for the exact surface and `NEWS.md` for
landed changes.

Intentionally deferred:

- Route-local auth policies, static asset serving, streaming HTTP route
  helpers, worker lifecycle management, and richer web-toolkit
  conveniences beyond the current low-level route framework.
- CSV/TSV/Parquet body parsers beyond the safe BLOB fallback.
- Full SQL-side decoding of session `fetch` Arrow batch BLOBs into a
  table-function path.
- Routing/forwarding and mesh scheduling as sealed APIs — the pipe
  monitor and active-pipe snapshot are the membership/telemetry
  foundation that comes first.

## References

- [NNG](https://nng.nanomsg.org/) for the underlying messaging library
  and transport family.
- [`nanonext`](https://github.com/r-lib/nanonext) for the main
  client/server ergonomics reference.
- [`mangoro`](https://github.com/sounkou-bioinfo/mangoro) for the
  thin-envelope + Arrow IPC RPC direction.
- [DuckDB C API](https://duckdb.org/docs/stable/clients/c/api) for the
  extension and SQL integration boundary.
- [Apache Arrow
  IPC](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc)
  for the tabular payload format.
