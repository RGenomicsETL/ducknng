# MotherDuck-style gateway demo

This document sketches the clean `ducknng` way to build a MotherDuck-style public gateway without lying about the current execution model.

The key constraint is simple: route handlers run on the service's shared serialized DuckDB execution lane. Because of that, a route handler should not synchronously call another `ducknng` service in the same runtime when that backend also needs that same lane. The honest demo shape is therefore multi-process:

- one DuckDB process runs a private `ducknng` backend service
- a second DuckDB process runs the public HTTP gateway service
- the gateway exposes exact-path HTTP routes
- route SQL talks to the private backend over the ordinary RPC/session helpers

That architecture is close to the important MotherDuck property: a public API edge that fronts a separate query worker plane.

## Topology

```text
HTTP client
    |
    v
public gateway process
  ducknng_start_server('gateway', 'http://127.0.0.1:18080/_ducknng', ...)
  ducknng_register_http_route('gateway', 'POST', '/v1/query', ...)
    |
    v
private backend process
  ducknng_start_server('backend', 'tcp://127.0.0.1:17011', ...)
  ducknng_register_exec_method()
```

The public side is free to authenticate, rate-limit, inspect headers, and narrow the HTTP surface. The private side keeps the real DuckDB query surface off the public network.

## Backend process

Start a backend in one DuckDB process:

```sql
LOAD ducknng;

SELECT ducknng_register_exec_method();

SELECT ducknng_start_server(
  'backend',
  'tcp://127.0.0.1:17011',
  1,
  134217728,
  300000,
  0::UBIGINT
);
```

In a real deployment, put this on `tls+tcp://`, `wss://`, or `https://` with mTLS and allowlists. Loopback TCP is enough for the demo sketch.

## Gateway process

Start the public gateway in a second DuckDB process:

```sql
LOAD ducknng;

SELECT ducknng_start_server(
  'gateway',
  'http://127.0.0.1:18080/_ducknng',
  1,
  134217728,
  300000,
  0::UBIGINT
);
```

Register a simple health route:

```sql
SELECT ducknng_register_http_route(
  'gateway',
  'GET',
  '/healthz',
  'SELECT 200 AS status, ''text/plain; charset=utf-8'' AS content_type, ''ok'' AS body_text'
);
```

Then register a query route that accepts JSON like `{"sql":"SELECT 42 AS answer"}` and returns the first Arrow batch from the backend as the HTTP body:

```sql
SET VARIABLE gateway_query_sql =
'WITH req AS (
   SELECT json_extract_string(body_text::JSON, ''$.sql'') AS sql
   FROM ducknng_http_request_body()
 ),
 opened AS (
   SELECT session_id, session_token
   FROM ducknng_open_query(
     ''tcp://127.0.0.1:17011'',
     (SELECT sql FROM req),
     0::UBIGINT,
     0::UBIGINT,
     0::UBIGINT
   )
   WHERE ok
 ),
 fetched AS (
   SELECT payload, session_id, session_token, end_of_stream
   FROM ducknng_fetch_query(
     ''tcp://127.0.0.1:17011'',
     (SELECT session_id FROM opened),
     (SELECT session_token FROM opened),
     0::UBIGINT,
     0::UBIGINT,
     0::UBIGINT
   )
   WHERE ok
 ),
 closed AS (
   SELECT *
   FROM ducknng_close_query(
     ''tcp://127.0.0.1:17011'',
     (SELECT session_id FROM opened),
     (SELECT session_token FROM opened),
     0::UBIGINT
   )
 )
 SELECT 200 AS status,
        ''application/vnd.apache.arrow.stream'' AS content_type,
        payload AS body
 FROM fetched';

SELECT ducknng_register_http_route(
  'gateway',
  'POST',
  '/v1/query',
  getvariable('gateway_query_sql')::VARCHAR,
  1048576::UBIGINT
);
```

This is intentionally low-level. The route is doing four explicit things:

1. read the inbound JSON request body
2. open a backend query session
3. fetch one Arrow batch
4. close the backend session

That is a good demo shape because it exposes the real control flow instead of pretending the gateway has magical internal shortcuts.

## Client side

From SQL, you can hit the public edge and decode the Arrow reply automatically:

```sql
SELECT *
FROM ducknng_ncurl_table(
  'http://127.0.0.1:18080/v1/query',
  'POST',
  '[{"name":"Content-Type","value":"application/json"}]',
  '{"sql":"SELECT 42 AS answer"}'::BLOB,
  2000,
  0::UBIGINT
);
```

Because the route replies with `application/vnd.apache.arrow.stream`, `ducknng_ncurl_table(...)` can decode the returned batch directly into a DuckDB table.

## What this reproduces well

This demo reproduces the parts of the MotherDuck pattern that matter for `ducknng` right now:

- a public HTTP API edge
- a private query worker plane
- application-controlled routing instead of public raw DuckDB SQL by default
- Arrow over the wire instead of lossy row-string formatting
- explicit service boundaries instead of hidden in-process shortcuts

## What it does not do yet

This is still a low-level demo, not a polished product surface.

- It returns only the first fetched Arrow batch.
- It does not paginate or stream multiple batches over HTTP.
- It does not cache backend sessions between HTTP calls.
- It does not add auth, tenant routing, or rate limiting on its own.
- It assumes the request body is JSON text and the backend query result fits one fetch.

If you want a more complete product slice, the next layer would be:

- a gateway-specific request schema
- route-local auth and tenant resolution
- an HTTP continuation token that carries backend `session_id` plus `session_token`
- explicit close/cancel endpoints or time-bounded automatic cleanup
- HTTPS with mTLS or a real edge auth layer in front of the public route service

That is still compatible with the current `ducknng` contract. It just builds more policy and product shape on top of the low-level route and session primitives that already exist.
