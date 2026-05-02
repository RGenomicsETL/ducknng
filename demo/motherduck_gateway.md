
<!-- demo/motherduck_gateway.md is generated from demo/motherduck_gateway.Rmd. Please edit that file -->

# MotherDuck-style gateway demo

This is the dedicated rendered walkthrough for the multi-process
subscriber gateway topology in `ducknng`. Unlike the main `README`, this
document is allowed to boot several private DuckDB workers during
render, so it can show the honest public-gateway shape rather than an
in-process sketch.

Render it from the repo root with:

``` sh
make motherduck_rdm
```

The live helper behind this document is still
[demo/motherduck_gateway.py](motherduck_gateway.py). The rendered
walkthrough keeps the topology generic instead of hardwiring
`user -> backend` logic into the public API.

## Topology

One public DuckDB process owns the HTTP edge, identity resolution,
subscriber lookup, and public continuation tokens. Several private
DuckDB processes expose ordinary `ducknng` query-session services. The
gateway chooses one subscriber per tenant and keeps the backend
`session_id` plus `session_token` private.

``` text
HTTP client
    |
    v
public gateway process
  http://127.0.0.1:<gateway>/_ducknng
    |
    +--> subscriber_alice  tcp://127.0.0.1:<alice>
    |
    +--> subscriber_bob    tcp://127.0.0.1:<bob>
```

The route layer stays generic because the gateway owns two ordinary SQL
tables:

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

`gateway_identities` resolves bearer tokens or caller identities into a
tenant and principal. `gateway_subscribers` resolves that tenant into
one private worker endpoint. Nothing in the public route contract needs
to know whether the backend plane is two workers or two hundred.

## Gateway route shape

The public edge is still just the landed low-level HTTP route surface:

``` sql
SELECT ducknng_register_http_route('gateway', 'POST', '/v1/query/start', ...);
SELECT ducknng_register_http_route('gateway', 'POST', '/v1/query/fetch', ...);
SELECT ducknng_register_http_route('gateway', 'POST', '/v1/query/close', ...);
```

Inside those routes, the gateway uses the raw session helpers so
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
client sees HTTP plus Arrow batches, while the private worker plane
stays on the ordinary `ducknng` session contract.

## Alice starts a query through the public gateway

``` r
start <- ncurl(
  paste0(gateway_base_url, "/v1/query/start"),
  convert = FALSE,
  response = TRUE,
  method = "POST",
  headers = c(
    "Content-Type" = "application/json",
    "Authorization" = "Bearer demo-alice-token"
  ),
  data = charToRaw(
    '{"sql":"SELECT owner, i, v FROM tenant_numbers ORDER BY i"}'
  ),
  timeout = 2000L
)
```

| status | tenant       | subscriber   | end_of_stream | has_next_token |
|-------:|:-------------|:-------------|:--------------|:---------------|
|    200 | tenant_alice | alice_worker | false         | TRUE           |

The response body is an Arrow IPC batch. The gateway already chose the
private subscriber and exposed that choice only through headers. The
effective batch split is still backend-owned, so the public route
contract stays stable even when the worker chooses a larger first chunk.

``` sql
SELECT *
FROM ducknng_parse_body(<first Arrow batch>, 'application/vnd.apache.arrow.stream')
LIMIT 4;
```

``` text
+-------+---+----+
| owner | i | v  |
+-------+---+----+
| alice | 1 | 10 |
| alice | 2 | 20 |
| alice | 3 | 30 |
| alice | 4 | 40 |
+-------+---+----+
```

## Fetch continues on the same private subscriber

``` r
fetch <- ncurl(
  paste0(gateway_base_url, "/v1/query/fetch"),
  convert = FALSE,
  response = TRUE,
  method = "POST",
  headers = c(
    "Content-Type" = "application/json",
    "Authorization" = "Bearer demo-alice-token"
  ),
  data = charToRaw(sprintf('{"token":"%s"}', alice_next_token)),
  timeout = 2000L
)
```

| status | tenant       | subscriber   | end_of_stream | has_next_token |
|-------:|:-------------|:-------------|:--------------|:---------------|
|    200 | tenant_alice | alice_worker | false         | TRUE           |

``` sql
SELECT *
FROM ducknng_parse_body(<next Arrow batch>, 'application/vnd.apache.arrow.stream')
LIMIT 4;
```

``` text
+-------+------+-------+
| owner |  i   |   v   |
+-------+------+-------+
| alice | 2049 | 20490 |
| alice | 2050 | 20500 |
| alice | 2051 | 20510 |
| alice | 2052 | 20520 |
+-------+------+-------+
```

The public continuation token is gateway-owned. It carries tenant
affinity and private backend session state, but it does not turn the
public API into a transport-specific copy of `fetch`.

## A second identity routes to a different subscriber

``` r
bob_start <- ncurl(
  paste0(gateway_base_url, "/v1/query/start"),
  convert = FALSE,
  response = TRUE,
  method = "POST",
  headers = c(
    "Content-Type" = "application/json",
    "Authorization" = "Bearer demo-bob-token"
  ),
  data = charToRaw(
    '{"sql":"SELECT owner, i, v FROM tenant_numbers ORDER BY i"}'
  ),
  timeout = 2000L
)
```

| status | tenant     | subscriber | end_of_stream | has_next_token |
|-------:|:-----------|:-----------|:--------------|:---------------|
|    200 | tenant_bob | bob_worker | false         | TRUE           |

``` sql
SELECT *
FROM ducknng_parse_body(<bob Arrow batch>, 'application/vnd.apache.arrow.stream')
LIMIT 4;
```

``` text
+-------+-------+--------+
| owner |   i   |   v    |
+-------+-------+--------+
| bob   | 10001 | 100010 |
| bob   | 10002 | 100020 |
| bob   | 10003 | 100030 |
| bob   | 10004 | 100040 |
+-------+-------+--------+
```

The public start route did not change. Only the authenticated identity
changed, and the gateway routed the request to a different private
subscriber.

## Cross-tenant close is rejected

``` r
wrong_close <- ncurl(
  paste0(gateway_base_url, "/v1/query/close"),
  convert = FALSE,
  response = TRUE,
  method = "POST",
  headers = c(
    "Content-Type" = "application/json",
    "Authorization" = "Bearer demo-bob-token"
  ),
  data = charToRaw(sprintf('{"token":"%s"}', alice_next_token)),
  timeout = 2000L
)
```

| status | body                                                                                 |
|-------:|:-------------------------------------------------------------------------------------|
|    403 | {“closed”:false,“error”:“continuation token belongs to another tenant or principal”} |

That `403` is the important ownership boundary: a gateway token is not a
cross-tenant escape hatch.

## The owning tenant can close the session

``` r
close <- ncurl(
  paste0(gateway_base_url, "/v1/query/close"),
  convert = FALSE,
  response = TRUE,
  method = "POST",
  headers = c(
    "Content-Type" = "application/json",
    "Authorization" = "Bearer demo-alice-token"
  ),
  data = charToRaw(sprintf('{"token":"%s"}', alice_next_token)),
  timeout = 2000L
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

The current execution model still matters. Route handlers run on
`shared_serialized_connection`, so the public gateway should stay in a
separate DuckDB process from the private workers. That is exactly why
this rendered document boots several processes during render instead of
pretending the pattern is honest inside one runtime.
