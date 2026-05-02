# ducknng HTTP route framework

This document defines the landed low-level HTTP route layer that lives beside the framed RPC mount described in `docs/http.md`. It is intentionally narrower than a full web toolkit. The purpose of the layer is to let a service expose additive exact, prefix, or template HTTP handlers without minting HTTP-specific copies of `manifest`, `exec`, `query_open`, `fetch`, `close`, or `cancel`.

The route layer is part of the public SQL surface, but it is not part of the manifest-derived RPC surface. A registered HTTP route is a local service configuration entry. It is not a registry method, it does not appear in the `manifest` method list, and it does not change the frame-over-HTTP contract at the RPC mount.

## Public SQL surface

The current route surface is:

```sql
ducknng_register_http_route(service_name, method, path, handler_sql)
ducknng_register_http_route(service_name, method, path, handler_sql, request_max_bytes)
ducknng_register_http_route_pattern(service_name, method, match_kind, path_pattern, handler_sql)
ducknng_register_http_route_pattern(service_name, method, match_kind, path_pattern, handler_sql, request_max_bytes)
ducknng_unregister_http_route(service_name, method, path)
ducknng_unregister_http_route_pattern(service_name, method, match_kind, path_pattern)
ducknng_list_http_routes()
ducknng_http_request()
ducknng_http_request_body()
```

`ducknng_register_http_route(...)` installs one exact method/path match on an existing `http://` or `https://` service. `ducknng_register_http_route_pattern(...)` is the additive generic form for richer low-level routing and currently supports `match_kind = 'exact' | 'prefix' | 'template'`. `ducknng_unregister_http_route(...)` and `ducknng_unregister_http_route_pattern(...)` remove installed routes and return `FALSE` when no matching route exists. `ducknng_list_http_routes()` exposes the current route registry as a table. `ducknng_http_request()` and `ducknng_http_request_body()` are request-context helpers that only emit a row while SQL is running inside an active route handler.

## Route registration rules

Registration is deliberately strict:

- the target service must already exist and must use `http://` or `https://`
- `method` is normalized to uppercase and must be a valid HTTP token
- `path` / `path_pattern` must be an absolute path and must not contain a query string or fragment
- `match_kind = 'template'` requires one or more whole-segment `{name}` captures, where `name` matches `[A-Za-z_][A-Za-z0-9_]*`
- the route path must not conflict with the framed RPC mount path from the service listen URL
- `request_max_bytes`, when non-zero, must be less than or equal to the service `recv_max_bytes`
- one service cannot register the same normalized `method` plus `match_kind` plus `path` twice

Route registration and route listing are rejected inside SQL authorizer callbacks and inside active request handlers. That avoids recursive service-owned SQL entry and lock-order problems on the shared execution lane.

When several registered routes could match one request, the current selection order is deliberate and stable:

- exact beats template
- template beats prefix
- within the same match kind, the longer stored path pattern wins

## Request context

Inside an active route handler, `ducknng_http_request()` returns exactly one row with:

- `service_name`
- `listen`
- `scheme`
- `method`
- `path`
- `query_string`
- `content_type`
- `headers_json`
- `caller_identity`
- `remote_addr`
- `remote_ip`
- `route_method`
- `route_match_kind`
- `route_path`
- `path_params_json`
- `body_bytes`
- `route_id`
- `remote_port`

`ducknng_http_request_body()` returns exactly one row with:

- `body BLOB`
- `body_text VARCHAR`

`body_text` is populated only when the request body looks like valid text under the same UTF-8 check used by the other SQL-visible body helpers. Outside an active route handler, both tables emit zero rows instead of raising an error.

`headers_json` uses the same canonical array-of-objects form as `ducknng_ncurl(...)`, for example `[{"name":"Content-Type","value":"application/json"}]`.

`route_match_kind` and `route_path` identify the matched registered route pattern. For `template` routes, `path_params_json` exposes the extracted captures as a JSON object such as `{"tenant_id":"alice","item_id":"42"}`. Exact and prefix routes leave `path_params_json` as `NULL`.

## Response contract

`handler_sql` is executed as one DuckDB query. It must return exactly one row. The route layer recognizes these columns by name:

- `status INTEGER`
- `headers_json VARCHAR`
- `content_type VARCHAR`
- `body BLOB`
- `body_text VARCHAR`

The rules are:

- `status` is optional and defaults to `200`
- when present, `status` must be between `100` and `599`
- `headers_json` is optional and uses the same canonical JSON form as the client helper layer
- `content_type` is optional
- exactly one of `body` or `body_text` may be non-NULL
- when `body_text` is used without `content_type`, the default content type is `text/plain; charset=utf-8`
- when `body` is used without `content_type`, the default content type is `application/octet-stream`

If the handler returns the wrong shape, returns more than one row, or raises a query error, the adapter fails closed with an HTTP 5xx response.

## Admission and security

Route requests compose the same service-level admission stack as framed RPC:

1. required mTLS when the service TLS policy demands it
2. exact verified-peer allowlists
3. IP/CIDR allowlists
4. service-level inflight limits
5. optional SQL authorizer

The SQL authorizer sees route requests through `ducknng_auth_context()` with `phase = 'http_route'` and HTTP fields populated from the current request. That keeps policy carrier-neutral while still letting deployments write HTTP-specific denials when needed.

Routes do not bypass the guidance in `docs/security.md`. They are a good fit for health endpoints, thin JSON APIs, Arrow-returning gateway operations, and fixed application routes. They are not an automatic sandbox for arbitrary public SQL.

## Execution model

Route handler SQL runs inside the same service-owned DuckDB execution lane exposed elsewhere as:

```text
server.execution.model = "shared_serialized_connection"
```

That is the stable current contract. It means:

- handler SQL is serialized with the service's other DuckDB work
- DuckDB session state on that lane is shared
- handlers should stay short and explicit
- handlers must not assume they can re-enter the same runtime arbitrarily

The most important practical consequence is that a route handler should not synchronously call another `ducknng` service in the same runtime when that backend also needs the shared execution lane. That pattern can block on itself. A MotherDuck-style public gateway should therefore target a separate backend DuckDB process or at least a separate `ducknng` runtime boundary, not a sibling service in the same runtime.

## Example

```sql
SELECT ducknng_register_http_route(
  'api',
  'GET',
  '/healthz',
  'SELECT 200 AS status, ''text/plain; charset=utf-8'' AS content_type, ''ok'' AS body_text'
);

SELECT ducknng_register_http_route(
  'api',
  'POST',
  '/echo',
  'SELECT 201 AS status,
          ''text/plain; charset=utf-8'' AS content_type,
          (
            SELECT method || '' '' || path || '' '' || coalesce(body_text, '''')
            FROM ducknng_http_request(), ducknng_http_request_body()
          ) AS body_text'
);
```

## Explicit non-goals

The landed layer is intentionally low-level. These are still deferred:

- automatic query-parameter parsing helpers
- static asset serving
- HTTP-carrier WebSocket, SSE, or NDJSON streaming
- HTTP-specific copies of manifest-derived RPC methods
- automatic SQL-to-JSON marshalling for arbitrary rowsets

Those may arrive later as additive tooling, but they must stay clearly separate from the framed RPC carrier and from the manifest-derived method surface.
