# MotherDuck-style gateway demo

The runnable `ducknng` gateway demo now lives in [demo/motherduck_gateway.py](/root/ducknng/demo/motherduck_gateway.py:1):

```sh
make motherduck_demo
```

This is the honest shape for `ducknng` today:

- one public DuckDB process runs an HTTP gateway service
- several private DuckDB processes run backend subscriber services
- the gateway chooses a backend by user affinity
- backend query sessions stay private to those workers
- the gateway owns the public continuation-token contract

That is close to the important MotherDuck/OpenDuck property: public API edge in front of a worker plane, with query-session affinity staying on the private side.

## Why the gateway uses raw session helpers

HTTP route handlers execute as SQL queries. DuckDB binds table functions eagerly, so the structured client helpers:

- `ducknng_open_query(...)`
- `ducknng_fetch_query(...)`
- `ducknng_close_query(...)`
- `ducknng_cancel_query(...)`

are the ergonomic client surface, but they are the wrong surface for per-request dynamic route SQL where the method inputs come from request-body columns or continuation-token columns.

The route demo therefore uses the raw synchronous session helpers instead:

- `ducknng_open_query_raw(...)`
- `ducknng_fetch_query_raw(...)`
- `ducknng_close_query_raw(...)`
- `ducknng_cancel_query_raw(...)`

and then inspects the returned frames with:

- `ducknng_frame_payload(...)`
- `ducknng_frame_payload_text(...)`
- `ducknng_frame_error_text(...)`

That keeps the gateway route layer generic. The route can carry backend affinity, `session_id`, `session_token`, and fetch hints in a gateway-owned token without depending on bind-time table-function behavior.

## Topology

```text
HTTP client
    |
    v
public gateway process
  ducknng_start_server('gateway', 'http://127.0.0.1:18080/_ducknng', ...)
  ducknng_register_http_route('gateway', 'POST', '/v1/query/start', ...)
  ducknng_register_http_route('gateway', 'POST', '/v1/query/fetch', ...)
  ducknng_register_http_route('gateway', 'POST', '/v1/query/close', ...)
    |
    +--> subscriber backend: alice
    |      ducknng_start_server('subscriber_alice', 'tcp://127.0.0.1:17011', ...)
    |
    +--> subscriber backend: bob
           ducknng_start_server('subscriber_bob', 'tcp://127.0.0.1:17012', ...)
```

The demo uses two dedicated subscribers, `alice` and `bob`, each with its own `tenant_numbers` table. The public route request includes a `user` field, and the gateway routes the request to the matching private backend. The continuation token carries that backend URL plus the backend `session_id`, `session_token`, and fetch hints.

That is already enough to demonstrate:

- user-affine backend selection
- multi-batch fetch continuation
- explicit early close
- Arrow IPC result transport over HTTP

## Why this is multi-process

`ducknng` currently exposes `server.execution.model = "shared_serialized_connection"`. Route handlers therefore run on the same shared DuckDB execution lane as other service-owned SQL in that process.

Because of that, a route handler should not synchronously call another `ducknng` service in the same runtime when that backend also needs that same execution lane. The public gateway demo is intentionally multi-process so the HTTP route can make synchronous backend session calls without deadlocking or timing out on the same shared connection.

This is also why the demo is a better proof than an in-process sqllogictest sketch. It exercises the real network and process boundary the framework is designed for.

## Public contract

The demo exposes three public routes:

- `POST /v1/query/start`
- `POST /v1/query/fetch`
- `POST /v1/query/close`

`/v1/query/start` accepts JSON with at least:

```json
{"user":"alice","sql":"SELECT owner, i, v FROM tenant_numbers ORDER BY i"}
```

It returns the first Arrow batch as the HTTP body when rows are available. If more fetches may be needed, it also returns:

- `X-Ducknng-Next-Token`
- `X-Ducknng-End-Of-Stream: false`
- `X-Ducknng-Subscriber: <user>`

`/v1/query/fetch` accepts `{"token":"..."}` and either returns another Arrow batch with a replacement continuation token or returns `204` with `X-Ducknng-End-Of-Stream: true` after closing the backend session.

`/v1/query/close` accepts `{"token":"..."}` and explicitly closes a live backend session when the client stops early.

## What this demonstrates well

This demo is a good foundation for a real product edge because it proves the pieces that matter:

- exact-path HTTP routes can be the public edge
- the backend query plane can stay private
- route SQL can do dynamic session control without transport-specific RPC copies
- continuation tokens can remain gateway-owned instead of leaking raw backend state as the public API
- Arrow remains the row payload contract

What it does not try to do is reimplement DuckDB catalog/storage extension work such as `ATTACH 'md:...'` or `ATTACH 'openduck:...'`. That is a different layer. Here the goal is the low-level gateway and worker pattern on top of `ducknng`'s current HTTP and session primitives.
