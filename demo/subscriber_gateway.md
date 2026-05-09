
<!-- demo/subscriber_gateway.md is generated from demo/subscriber_gateway.Rmd. Please edit that file -->

# Subscriber gateway demo

This is a rendered walkthrough of the subscriber gateway topology in
`ducknng`. All three services run inside a single in-process DuckDB
connection — no separate worker processes required — because each
service uses `service_serialized_connection`, giving it a dedicated
execution lane.

Render it from the repo root with:

``` sh
make subscriber_gateway_rdm
```

## Topology

One gateway service owns the public HTTP edge, identity resolution, and
subscriber lookup. Two private backend services expose ordinary
`ducknng` query-session services over TCP. All three share one in-memory
DuckDB database, and all three use `service_serialized_connection` so
they each have their own dedicated connection lane.

``` text
HTTP client
    |
    v
gateway service (service_serialized_connection)
  http://127.0.0.1:<auto>/_ducknng
    |
    +--> subscriber_alice  tcp://127.0.0.1:<auto>
    |    (service_serialized_connection)
    |
    +--> subscriber_bob    tcp://127.0.0.1:<auto>
         (service_serialized_connection)
```

Because gateway route SQL calls
`ducknng_open_query_raw(backend_url, ...)` synchronously, the gateway’s
connection blocks while waiting for the backend reply. Since each
backend listens on its own connection, it can respond independently — no
deadlock, even though all three services share one DuckDB runtime. In a
production deployment each subscriber backend would be a separate DuckDB
process with its own isolated database.

The route layer stays generic because the gateway owns two SQL tables:

``` sql
CREATE TABLE gateway_identities(
  api_token VARCHAR,
  caller_identity VARCHAR,
  tenant_id VARCHAR,
  principal_id VARCHAR,
  active BOOLEAN
);

CREATE TABLE gateway_subscribers(
  subscriber_id VARCHAR,
  tenant_id VARCHAR,
  backend_url VARCHAR,
  priority INTEGER,
  enabled BOOLEAN
);
```

`gateway_identities` resolves bearer tokens into a tenant and principal.
`gateway_subscribers` resolves that tenant to one private backend URL.
Nothing in the public route SQL needs to know whether the backend plane
has two workers or two hundred.

## Starting the services

All three services start in one DuckDB connection. Backend services
start first so their assigned TCP ports are available when building the
gateway’s subscriber table.

``` r
# Backend: subscriber_alice (OS assigns the port)
dbExecute(con,
  "SELECT ducknng_start_server('subscriber_alice',
     'tcp://127.0.0.1:0', 1, 134217728, 300000, 0::UBIGINT)"
)
dbExecute(con,
  "SELECT ducknng_set_service_execution_model(
     'subscriber_alice', 'service_serialized_connection')"
)

# Backend: subscriber_bob
dbExecute(con,
  "SELECT ducknng_start_server('subscriber_bob',
     'tcp://127.0.0.1:0', 1, 134217728, 300000, 0::UBIGINT)"
)
dbExecute(con,
  "SELECT ducknng_set_service_execution_model(
     'subscriber_bob', 'service_serialized_connection')"
)

# Gateway HTTP service
dbExecute(con,
  "SELECT ducknng_start_server('gateway',
     'http://127.0.0.1:0/_ducknng', 1, 134217728, 300000, 0::UBIGINT)"
)
dbExecute(con,
  "SELECT ducknng_set_service_execution_model(
     'gateway', 'service_serialized_connection')"
)
```

## Gateway route shape

The public edge is three HTTP routes, all registered with a 1 MiB body
limit:

``` sql
SELECT ducknng_register_http_route('gateway', 'GET',  '/healthz',         ...);
SELECT ducknng_register_http_route('gateway', 'POST', '/v1/query/start',  ..., 1048576::UBIGINT);
SELECT ducknng_register_http_route('gateway', 'POST', '/v1/query/fetch',  ..., 1048576::UBIGINT);
SELECT ducknng_register_http_route('gateway', 'POST', '/v1/query/close',  ..., 1048576::UBIGINT);
```

Inside those routes the gateway uses the raw session helpers so
request-body columns and continuation-token columns stay dynamic:

``` sql
ducknng_open_query_raw(...)
ducknng_fetch_query_raw(...)
ducknng_close_query_raw(...)
ducknng_frame_payload(...)
ducknng_frame_payload_text(...)
ducknng_frame_error_text(...)
```

That keeps the route layer transport-local and gateway-owned. The public
client sees HTTP plus Arrow batches; the private worker plane stays on
the ordinary `ducknng` session contract.

## Alice starts a query through the public gateway

``` r
start <- ncurl(
  paste0(gateway_base_url, "/v1/query/start"),
  convert  = FALSE,
  response = TRUE,
  method   = "POST",
  headers  = c(
    "Content-Type"  = "application/json",
    "Authorization" = "Bearer demo-alice-token"
  ),
  data    = charToRaw(
    '{"sql":"SELECT owner, i, v FROM tenant_numbers WHERE owner = \'alice\' ORDER BY i",
      "batch_rows":4}'
  ),
  timeout = 5000L
)
```

| status | tenant       | subscriber   | end_of_stream | has_next_token |
|-------:|:-------------|:-------------|:--------------|:---------------|
|    200 | tenant_alice | alice_worker | false         | TRUE           |

The response body is an Arrow IPC batch. The gateway resolved the bearer
token, chose `alice_worker` as the backend, and embedded backend session
state in the opaque `X-Ducknng-Next-Token`.

| owner |   i |   v |
|:------|----:|----:|
| alice |   1 |  10 |
| alice |   2 |  20 |
| alice |   3 |  30 |
| alice |   4 |  40 |

## Fetch continues on the same private subscriber

``` r
fetch <- ncurl(
  paste0(gateway_base_url, "/v1/query/fetch"),
  convert  = FALSE,
  response = TRUE,
  method   = "POST",
  headers  = c(
    "Content-Type"  = "application/json",
    "Authorization" = "Bearer demo-alice-token"
  ),
  data    = charToRaw(sprintf('{"token":"%s"}', alice_next_token)),
  timeout = 5000L
)
```

| status | tenant       | subscriber   | end_of_stream | has_next_token |
|-------:|:-------------|:-------------|:--------------|:---------------|
|    200 | tenant_alice | alice_worker | false         | TRUE           |

| owner |    i |     v |
|:------|-----:|------:|
| alice | 2049 | 20490 |
| alice | 2050 | 20500 |
| alice | 2051 | 20510 |
| alice | 2052 | 20520 |

The public continuation token is gateway-owned. It carries tenant and
subscriber affinity plus the private backend `session_id` and
`session_token` without exposing them directly to the client.

## A second identity routes to a different subscriber

``` r
bob_start <- ncurl(
  paste0(gateway_base_url, "/v1/query/start"),
  convert  = FALSE,
  response = TRUE,
  method   = "POST",
  headers  = c(
    "Content-Type"  = "application/json",
    "Authorization" = "Bearer demo-bob-token"
  ),
  data    = charToRaw(
    '{"sql":"SELECT owner, i, v FROM tenant_numbers WHERE owner = \'bob\' ORDER BY i",
      "batch_rows":4}'
  ),
  timeout = 5000L
)
```

| status | tenant     | subscriber | end_of_stream | has_next_token |
|-------:|:-----------|:-----------|:--------------|:---------------|
|    200 | tenant_bob | bob_worker | false         | TRUE           |

| owner |     i |      v |
|:------|------:|-------:|
| bob   | 10001 | 100010 |
| bob   | 10002 | 100020 |
| bob   | 10003 | 100030 |
| bob   | 10004 | 100040 |

The public start route did not change. Only the authenticated identity
changed, and the gateway routed the request to `bob_worker` rather than
`alice_worker`.

## Cross-tenant close is rejected

``` r
wrong_close <- ncurl(
  paste0(gateway_base_url, "/v1/query/close"),
  convert  = FALSE,
  response = TRUE,
  method   = "POST",
  headers  = c(
    "Content-Type"  = "application/json",
    "Authorization" = "Bearer demo-bob-token"
  ),
  data    = charToRaw(sprintf('{"token":"%s"}', alice_next_token)),
  timeout = 5000L
)
```

| status | body                                                                                 |
|-------:|:-------------------------------------------------------------------------------------|
|    403 | {“closed”:false,“error”:“continuation token belongs to another tenant or principal”} |

The `403` is the important ownership boundary: a gateway token is not a
cross-tenant escape hatch.

## The owning tenant can close the session

``` r
close <- ncurl(
  paste0(gateway_base_url, "/v1/query/close"),
  convert  = FALSE,
  response = TRUE,
  method   = "POST",
  headers  = c(
    "Content-Type"  = "application/json",
    "Authorization" = "Bearer demo-alice-token"
  ),
  data    = charToRaw(sprintf('{"token":"%s"}', alice_next_token)),
  timeout = 5000L
)
```

| status | body                                                                      |
|-------:|:--------------------------------------------------------------------------|
|    200 | {“closed”:true,“tenant_id”:“tenant_alice”,“subscriber_id”:“alice_worker”} |

## What this shape buys you

This topology is generic in the right place:

- the public route surface stays fixed
- auth resolution is table-driven
- subscriber discovery is table-driven
- backend session affinity stays private
- Arrow remains the row contract

The `service_serialized_connection` model makes all of this work within
a single DuckDB runtime: each service has its own connection lane, so a
gateway route handler can make synchronous NNG calls to sibling services
without deadlocking.
