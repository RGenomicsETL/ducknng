# HTTP server framework design sketch

This is a design document for a future broader HTTP server framework. It is not a sealed API and it does not change the current frame-over-HTTP RPC carrier contract in `docs/http.md`.

## Current invariant

The current HTTP/HTTPS server surface is deliberately narrow:

- `ducknng_start_server(...)` exposes the framed RPC endpoint when the listen URL is `http://` or `https://`.
- The endpoint accepts `POST` only.
- The content type is `application/vnd.ducknng.frame` with optional parameters.
- The body is one complete `ducknng` frame.
- Registry methods are still `manifest`, optional `exec`, `query_open`, `fetch`, and `cancel`/`close` through the same manifest descriptors used by NNG transports.

A broader HTTP server framework must preserve that invariant. It must not create method duplicates such as `http_exec`, `http_query_open`, `http_fetch`, or `http_cancel`. HTTP routes are application routes beside the framed RPC endpoint, not an alternate RPC namespace.

## Desired shape

A future route framework should let deployments attach explicit HTTP routes to a service:

- method match, e.g. `GET`, `POST`, `PUT`, `DELETE`;
- path match, initially exact path or prefix path only;
- optional request size cap;
- optional content-type constraints;
- optional response content type;
- callback SQL or callback method name;
- route-local auth policy that composes with service auth policy.

The first version should prefer a table-driven registry over ad hoc callbacks so route introspection is possible:

```sql
-- sketch only, not implemented
SELECT ducknng_register_http_route(
  service_name := 'api',
  method := 'GET',
  path := '/healthz',
  handler_sql := 'SELECT 200 AS status, ''ok'' AS body_text'
);
```

## Request context

Route callbacks should reuse the same context model as framed RPC authorizers. A future context table could be route-aware while staying compatible with `ducknng_auth_context()` concepts:

- phase;
- service name;
- transport family;
- scheme;
- listen URL;
- remote address/IP/port;
- TLS verification and peer identity;
- peer/IP allowlist state;
- HTTP method;
- HTTP path;
- query string;
- content type;
- body byte count;
- matched route id/name.

The existing `ducknng_auth_context()` can remain the authorizer-only table. A separate route callback context may be cleaner if it needs parsed headers, query parameters, or request bodies.

## Authorization

HTTP routes should compose the same admission stack:

1. fast C denial for required mTLS;
2. exact verified-peer allowlists;
3. IP/CIDR allowlists;
4. service resource limits;
5. optional SQL authorizer at request boundary;
6. route-local policy.

SQL authorizer callbacks remain short, side-effect-light, and fail-closed. Recursive same-service `ducknng_*` client/lifecycle calls remain rejected inside callbacks to avoid deadlocks in the service-owned SQL lane.

## Body handling

Raw request bodies should remain raw by default. Parsed bodies should use the explicit codec/provider layer instead of implicit route magic:

- raw body as `BLOB`;
- optional `body_text` only when the bytes are valid UTF-8 text;
- explicit JSON parsing through the JSON provider;
- Arrow IPC/frame parsing through existing providers;
- CSV/TSV/Parquet only after a memory-backed reader/provider path is real.

The HTTP framework should not reinterpret framed RPC bodies. The framed endpoint remains one complete `ducknng` frame and replies with a complete `ducknng` frame.

## Response model

A route callback should produce a bounded response row, likely with columns like:

- `status INTEGER`;
- `headers_json VARCHAR`;
- `body BLOB` or `body_text VARCHAR`;
- `content_type VARCHAR`.

The callback must return exactly one row. Multiple rows or missing required columns should fail closed with an adapter-level error response.

## Resource limits

Route traffic should share service-level limits with framed RPC where practical:

- maximum request body size;
- maximum in-flight requests;
- future owner/identity quotas;
- future cumulative reply-byte limits.

Route-local caps can add stricter request and reply byte limits. They should not bypass global service limits.

## Non-goals

- no HTTP-specific copies of RPC methods;
- no implicit `exec` exposure through HTTP routes;
- no body parser claims for CSV/TSV/Parquet until those providers really parse in memory;
- no long-running SQL authorizer work;
- no route callbacks inside NNG low-level pipe callbacks;
- no sealed public route API before a prototype and tests prove the shape.

## Open questions

- whether route callbacks should be SQL strings, registered method names, or both;
- whether route matching should initially support path parameters or only exact/prefix matches;
- how to expose request headers without making callback rows huge;
- whether route results should be cached for static health/status endpoints;
- how route-local rate limits should identify owners before SQL authorizer principals become session/request ownership metadata.
