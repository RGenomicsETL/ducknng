from __future__ import annotations

import duckdb

from subscriber_gateway_common import (
    BACKEND_PREFIX,
    GATEWAY_NAME,
    ORPHAN_IDENTITY,
    SUBSCRIBER_SPECS,
    spec_by_backend_key,
)


def build_gateway_route_sqls() -> tuple[str, str, str]:
    start_sql = """
WITH req AS (
  SELECT
    r.headers_json,
    r.caller_identity,
    json_extract_string(TRY_CAST(b.body_text AS JSON), '$.sql') AS sql,
    coalesce(TRY_CAST(json_extract_string(TRY_CAST(b.body_text AS JSON), '$.batch_rows') AS UBIGINT), 0::UBIGINT) AS batch_rows,
    coalesce(TRY_CAST(json_extract_string(TRY_CAST(b.body_text AS JSON), '$.batch_bytes') AS UBIGINT), 0::UBIGINT) AS batch_bytes
  FROM ducknng_http_request() AS r, ducknng_http_request_body() AS b
),
header_auth AS (
  SELECT
    max(
      CASE
        WHEN lower(json_extract_string(value, '$.name')) = 'authorization'
          THEN NULLIF(regexp_extract(json_extract_string(value, '$.value'), '^Bearer[ ]+(.+)$', 1), '')
        ELSE NULL
      END
    ) AS bearer_token
  FROM req, json_each(coalesce(req.headers_json, '[]')::JSON)
),
auth AS (
  SELECT req.caller_identity, header_auth.bearer_token
  FROM req, header_auth
),
principal AS (
  SELECT
    i.tenant_id,
    i.principal_id,
    CASE
      WHEN auth.caller_identity IS NOT NULL AND i.caller_identity = auth.caller_identity THEN 'caller_identity'
      ELSE 'bearer'
    END AS auth_source
  FROM auth
  JOIN gateway_identities AS i
    ON i.active AND (
      (auth.caller_identity IS NOT NULL AND i.caller_identity = auth.caller_identity) OR
      (auth.bearer_token IS NOT NULL AND i.api_token = auth.bearer_token)
    )
  ORDER BY CASE
    WHEN auth.caller_identity IS NOT NULL AND i.caller_identity = auth.caller_identity THEN 0
    ELSE 1
  END
  LIMIT 1
),
subscriber AS (
  SELECT
    s.subscriber_id,
    s.backend_url
  FROM principal
  JOIN gateway_subscribers AS s
    ON s.enabled AND s.tenant_id = principal.tenant_id
  ORDER BY s.priority, s.subscriber_id
  LIMIT 1
),
opened AS MATERIALIZED (
  SELECT
    principal.tenant_id,
    principal.principal_id,
    principal.auth_source,
    subscriber.subscriber_id,
    subscriber.backend_url,
    ducknng_open_query_raw(
      subscriber.backend_url,
      req.sql,
      req.batch_rows,
      req.batch_bytes,
      0::UBIGINT
    ) AS frame
  FROM req, principal, subscriber
  WHERE req.sql IS NOT NULL
),
open_decoded AS MATERIALIZED (
  SELECT
    opened.tenant_id,
    opened.principal_id,
    opened.auth_source,
    opened.subscriber_id,
    opened.backend_url,
    TRY_CAST(ducknng_frame_payload_text(opened.frame) AS JSON) AS payload_json,
    CASE
      WHEN ducknng_frame_error_text(opened.frame) IS NOT NULL THEN ducknng_frame_error_text(opened.frame)
      WHEN TRY_CAST(ducknng_frame_payload_text(opened.frame) AS JSON) IS NULL THEN 'ducknng: query_open reply payload was not valid JSON'
      WHEN json_extract(TRY_CAST(ducknng_frame_payload_text(opened.frame) AS JSON), '$.session_id')::UBIGINT IS NULL
        OR json_extract_string(TRY_CAST(ducknng_frame_payload_text(opened.frame) AS JSON), '$.session_token') IS NULL
        THEN 'ducknng: query_open reply did not include session_id and session_token'
      ELSE NULL
    END AS open_error
  FROM opened
),
continuation_seed AS MATERIALIZED (
  SELECT
    tenant_id,
    principal_id,
    auth_source,
    subscriber_id,
    backend_url,
    json_extract(payload_json, '$.session_id')::UBIGINT AS session_id,
    json_extract_string(payload_json, '$.session_token') AS session_token,
    (SELECT batch_rows FROM req LIMIT 1) AS batch_rows,
    (SELECT batch_bytes FROM req LIMIT 1) AS batch_bytes
  FROM open_decoded
  WHERE open_error IS NULL
),
fetched AS MATERIALIZED (
  SELECT
    continuation_seed.tenant_id,
    continuation_seed.principal_id,
    continuation_seed.auth_source,
    continuation_seed.subscriber_id,
    continuation_seed.backend_url,
    continuation_seed.session_id,
    continuation_seed.session_token,
    ducknng_fetch_query_raw(
      continuation_seed.backend_url,
      continuation_seed.session_id,
      continuation_seed.session_token,
      continuation_seed.batch_rows,
      continuation_seed.batch_bytes,
      0::UBIGINT
    ) AS frame
  FROM continuation_seed
),
fetch_decoded AS MATERIALIZED (
  SELECT
    fetched.tenant_id,
    fetched.principal_id,
    fetched.auth_source,
    fetched.subscriber_id,
    fetched.backend_url,
    fetched.session_id,
    fetched.session_token,
    ducknng_frame_error_text(fetched.frame) AS fetch_error,
    TRY_CAST(ducknng_frame_payload_text(fetched.frame) AS JSON) AS payload_json,
    ducknng_frame_payload(fetched.frame) AS payload,
    ducknng_frame_end_of_stream(fetched.frame) AS end_of_stream
  FROM fetched
),
continuation AS MATERIALIZED (
  SELECT
    tenant_id,
    principal_id,
    auth_source,
    subscriber_id,
    CAST(
      to_hex(
        encode(
          CAST(
            to_json(
              struct_pack(
                tenant_id := tenant_id,
                principal_id := principal_id,
                auth_source := auth_source,
                subscriber_id := subscriber_id,
                session_id := session_id,
                session_token := session_token,
                backend_url := backend_url
              )
            ) AS VARCHAR
          )
        )
      ) AS VARCHAR
    ) AS continuation_token,
    payload,
    fetch_error,
    coalesce(json_extract_string(payload_json, '$.state'), '') AS payload_state,
    end_of_stream
  FROM fetch_decoded
),
reply AS (
  SELECT
    CASE
      WHEN NOT EXISTS (SELECT 1 FROM principal) THEN 401
      WHEN EXISTS (SELECT 1 FROM principal) AND NOT EXISTS (SELECT 1 FROM subscriber) THEN 503
      WHEN EXISTS (SELECT 1 FROM req WHERE sql IS NULL) THEN 400
      WHEN EXISTS (SELECT 1 FROM open_decoded WHERE open_error IS NOT NULL) THEN 502
      WHEN EXISTS (SELECT 1 FROM continuation WHERE fetch_error IS NOT NULL) THEN 502
      WHEN EXISTS (SELECT 1 FROM continuation WHERE fetch_error IS NULL AND payload_state = 'exhausted') THEN 204
      ELSE 200
    END AS status,
    CAST(
      to_json(
        list_filter(
          [
            struct_pack(name := 'X-Ducknng-Tenant', value := (SELECT tenant_id FROM principal LIMIT 1)),
            struct_pack(name := 'X-Ducknng-Subscriber', value := (SELECT subscriber_id FROM subscriber LIMIT 1)),
            struct_pack(
              name := 'X-Ducknng-End-Of-Stream',
              value := CASE
                WHEN EXISTS (SELECT 1 FROM continuation WHERE fetch_error IS NULL AND (payload_state = 'exhausted' OR end_of_stream)) THEN 'true'
                WHEN EXISTS (SELECT 1 FROM continuation WHERE fetch_error IS NULL AND payload_state = '') THEN 'false'
                ELSE NULL
              END
            ),
            struct_pack(
              name := 'X-Ducknng-Next-Token',
              value := CASE
                WHEN EXISTS (SELECT 1 FROM continuation WHERE fetch_error IS NULL AND payload_state <> 'exhausted' AND NOT end_of_stream)
                  THEN (SELECT continuation_token FROM continuation LIMIT 1)
                ELSE NULL
              END
            )
          ],
          x -> x.value IS NOT NULL
        )
      ) AS VARCHAR
    ) AS headers_json,
    'application/vnd.apache.arrow.stream' AS content_type,
    CASE
      WHEN EXISTS (SELECT 1 FROM continuation WHERE fetch_error IS NULL AND payload_state = '') THEN
        (SELECT payload FROM continuation WHERE fetch_error IS NULL AND payload_state = '' LIMIT 1)
      ELSE NULL
    END AS body,
    CASE
      WHEN NOT EXISTS (SELECT 1 FROM principal) THEN CAST(to_json(struct_pack(error := 'missing or invalid bearer token')) AS VARCHAR)
      WHEN EXISTS (SELECT 1 FROM principal) AND NOT EXISTS (SELECT 1 FROM subscriber) THEN CAST(to_json(struct_pack(error := 'no subscriber available for tenant')) AS VARCHAR)
      WHEN EXISTS (SELECT 1 FROM req WHERE sql IS NULL) THEN CAST(to_json(struct_pack(error := 'request body must contain sql')) AS VARCHAR)
      WHEN EXISTS (SELECT 1 FROM open_decoded WHERE open_error IS NOT NULL) THEN CAST(to_json(struct_pack(error := (SELECT open_error FROM open_decoded WHERE open_error IS NOT NULL LIMIT 1))) AS VARCHAR)
      WHEN EXISTS (SELECT 1 FROM continuation WHERE fetch_error IS NOT NULL) THEN CAST(to_json(struct_pack(error := (SELECT fetch_error FROM continuation WHERE fetch_error IS NOT NULL LIMIT 1))) AS VARCHAR)
      ELSE NULL
    END AS body_text
)
SELECT status, headers_json, content_type, body, body_text
FROM reply
"""
    fetch_sql = """
WITH req AS (
  SELECT
    r.headers_json,
    r.caller_identity,
    json_extract_string(TRY_CAST(b.body_text AS JSON), '$.token') AS token
  FROM ducknng_http_request() AS r, ducknng_http_request_body() AS b
),
header_auth AS (
  SELECT
    max(
      CASE
        WHEN lower(json_extract_string(value, '$.name')) = 'authorization'
          THEN NULLIF(regexp_extract(json_extract_string(value, '$.value'), '^Bearer[ ]+(.+)$', 1), '')
        ELSE NULL
      END
    ) AS bearer_token
  FROM req, json_each(coalesce(req.headers_json, '[]')::JSON)
),
auth AS (
  SELECT req.caller_identity, header_auth.bearer_token
  FROM req, header_auth
),
principal AS (
  SELECT
    i.tenant_id,
    i.principal_id,
    CASE
      WHEN auth.caller_identity IS NOT NULL AND i.caller_identity = auth.caller_identity THEN 'caller_identity'
      ELSE 'bearer'
    END AS auth_source
  FROM auth
  JOIN gateway_identities AS i
    ON i.active AND (
      (auth.caller_identity IS NOT NULL AND i.caller_identity = auth.caller_identity) OR
      (auth.bearer_token IS NOT NULL AND i.api_token = auth.bearer_token)
    )
  ORDER BY CASE
    WHEN auth.caller_identity IS NOT NULL AND i.caller_identity = auth.caller_identity THEN 0
    ELSE 1
  END
  LIMIT 1
),
token_raw AS MATERIALIZED (
  SELECT TRY_CAST(decode(from_hex(token)) AS VARCHAR) AS token_json
  FROM req
  WHERE token IS NOT NULL
),
token_meta AS MATERIALIZED (
  SELECT
    json_extract_string(token_json::JSON, '$.tenant_id') AS tenant_id,
    json_extract_string(token_json::JSON, '$.principal_id') AS principal_id,
    json_extract_string(token_json::JSON, '$.auth_source') AS auth_source,
    json_extract_string(token_json::JSON, '$.subscriber_id') AS subscriber_id,
    json_extract(token_json::JSON, '$.session_id')::UBIGINT AS session_id,
    json_extract_string(token_json::JSON, '$.session_token') AS session_token,
    json_extract_string(token_json::JSON, '$.backend_url') AS backend_url
  FROM token_raw
  WHERE token_json IS NOT NULL
),
token_ready AS MATERIALIZED (
  SELECT *
  FROM token_meta
  WHERE tenant_id IS NOT NULL
    AND principal_id IS NOT NULL
    AND auth_source IS NOT NULL
    AND subscriber_id IS NOT NULL
    AND session_id IS NOT NULL
    AND session_token IS NOT NULL
    AND backend_url IS NOT NULL
),
authorized AS MATERIALIZED (
  SELECT *
  FROM principal, token_ready
  WHERE principal.tenant_id = token_ready.tenant_id
    AND principal.principal_id = token_ready.principal_id
    AND principal.auth_source = token_ready.auth_source
),
fetched AS MATERIALIZED (
  SELECT
    authorized.tenant_id,
    authorized.subscriber_id,
    ducknng_fetch_query_raw(
      authorized.backend_url,
      authorized.session_id,
      authorized.session_token,
      0::UBIGINT,
      0::UBIGINT,
      0::UBIGINT
    ) AS frame
  FROM authorized
),
decoded AS MATERIALIZED (
  SELECT
    tenant_id,
    subscriber_id,
    ducknng_frame_error_text(frame) AS frame_error,
    TRY_CAST(ducknng_frame_payload_text(fetched.frame) AS JSON) AS payload_json,
    ducknng_frame_payload(fetched.frame) AS payload,
    ducknng_frame_end_of_stream(fetched.frame) AS end_of_stream
  FROM fetched
),
continuation AS MATERIALIZED (
  SELECT
    tenant_id,
    subscriber_id,
    CAST(
      to_hex(
        encode(
          CAST(
            to_json(
              struct_pack(
                tenant_id := (SELECT tenant_id FROM authorized LIMIT 1),
                principal_id := (SELECT principal_id FROM authorized LIMIT 1),
                auth_source := (SELECT auth_source FROM authorized LIMIT 1),
                subscriber_id := subscriber_id,
                session_id := (SELECT session_id FROM authorized LIMIT 1),
                session_token := (SELECT session_token FROM authorized LIMIT 1),
                backend_url := (SELECT backend_url FROM authorized LIMIT 1)
              )
            ) AS VARCHAR
          )
        )
      ) AS VARCHAR
    ) AS continuation_token,
    payload,
    frame_error,
    coalesce(json_extract_string(payload_json, '$.state'), '') AS payload_state,
    end_of_stream
  FROM decoded
),
reply AS (
  SELECT
    CASE
      WHEN NOT EXISTS (SELECT 1 FROM principal) THEN 401
      WHEN NOT EXISTS (SELECT 1 FROM token_ready) THEN 400
      WHEN NOT EXISTS (SELECT 1 FROM authorized) THEN 403
      WHEN EXISTS (SELECT 1 FROM continuation WHERE frame_error IS NOT NULL) THEN 502
      WHEN EXISTS (SELECT 1 FROM continuation WHERE payload_state = 'exhausted') THEN 204
      ELSE 200
    END AS status,
    CAST(
      to_json(
        list_filter(
          [
            struct_pack(name := 'X-Ducknng-Tenant', value := (SELECT tenant_id FROM authorized LIMIT 1)),
            struct_pack(name := 'X-Ducknng-Subscriber', value := (SELECT subscriber_id FROM authorized LIMIT 1)),
            struct_pack(
              name := 'X-Ducknng-End-Of-Stream',
              value := CASE
                WHEN EXISTS (SELECT 1 FROM continuation WHERE frame_error IS NULL AND (payload_state = 'exhausted' OR end_of_stream)) THEN 'true'
                WHEN EXISTS (SELECT 1 FROM continuation WHERE frame_error IS NULL AND payload_state = '') THEN 'false'
                ELSE NULL
              END
            ),
            struct_pack(
              name := 'X-Ducknng-Next-Token',
              value := CASE
                WHEN EXISTS (SELECT 1 FROM continuation WHERE frame_error IS NULL AND payload_state <> 'exhausted' AND NOT end_of_stream)
                  THEN (SELECT continuation_token FROM continuation LIMIT 1)
                ELSE NULL
              END
            )
          ],
          x -> x.value IS NOT NULL
        )
      ) AS VARCHAR
    ) AS headers_json,
    'application/vnd.apache.arrow.stream' AS content_type,
    CASE
      WHEN EXISTS (SELECT 1 FROM continuation WHERE frame_error IS NULL AND payload_state = '') THEN
        (SELECT payload FROM continuation WHERE frame_error IS NULL LIMIT 1)
      ELSE NULL
    END AS body,
    CASE
      WHEN NOT EXISTS (SELECT 1 FROM principal) THEN CAST(to_json(struct_pack(error := 'missing or invalid bearer token')) AS VARCHAR)
      WHEN NOT EXISTS (SELECT 1 FROM token_ready) THEN CAST(to_json(struct_pack(error := 'invalid continuation token')) AS VARCHAR)
      WHEN NOT EXISTS (SELECT 1 FROM authorized) THEN CAST(to_json(struct_pack(error := 'continuation token belongs to another tenant or principal')) AS VARCHAR)
      WHEN EXISTS (SELECT 1 FROM continuation WHERE frame_error IS NOT NULL) THEN CAST(to_json(struct_pack(error := (SELECT frame_error FROM continuation WHERE frame_error IS NOT NULL LIMIT 1))) AS VARCHAR)
      ELSE NULL
    END AS body_text
)
SELECT status, headers_json, content_type, body, body_text
FROM reply
"""
    close_sql = """
DROP TABLE IF EXISTS __ducknng_gateway_close_state;
DROP TABLE IF EXISTS __ducknng_gateway_close_meta;
DROP TABLE IF EXISTS __ducknng_gateway_close_reply;
CREATE TEMP TABLE __ducknng_gateway_close_state AS
WITH req AS (
  SELECT
    r.headers_json,
    r.caller_identity,
    json_extract_string(TRY_CAST(b.body_text AS JSON), '$.token') AS token
  FROM ducknng_http_request() AS r, ducknng_http_request_body() AS b
),
header_auth AS (
  SELECT
    max(
      CASE
        WHEN lower(json_extract_string(value, '$.name')) = 'authorization'
          THEN NULLIF(regexp_extract(json_extract_string(value, '$.value'), '^Bearer[ ]+(.+)$', 1), '')
        ELSE NULL
      END
    ) AS bearer_token
  FROM req, json_each(coalesce(req.headers_json, '[]')::JSON)
),
auth AS (
  SELECT req.caller_identity, header_auth.bearer_token
  FROM req, header_auth
),
principal AS MATERIALIZED (
  SELECT
    i.tenant_id,
    i.principal_id,
    CASE
      WHEN auth.caller_identity IS NOT NULL AND i.caller_identity = auth.caller_identity THEN 'caller_identity'
      ELSE 'bearer'
    END AS auth_source
  FROM auth
  JOIN gateway_identities AS i
    ON i.active AND (
      (auth.caller_identity IS NOT NULL AND i.caller_identity = auth.caller_identity) OR
      (auth.bearer_token IS NOT NULL AND i.api_token = auth.bearer_token)
    )
  ORDER BY CASE
    WHEN auth.caller_identity IS NOT NULL AND i.caller_identity = auth.caller_identity THEN 0
    ELSE 1
  END
  LIMIT 1
),
token_raw AS MATERIALIZED (
  SELECT TRY_CAST(decode(from_hex(token)) AS VARCHAR) AS token_json
  FROM req
  WHERE token IS NOT NULL
),
token_meta AS MATERIALIZED (
  SELECT
    json_extract_string(token_json::JSON, '$.tenant_id') AS tenant_id,
    json_extract_string(token_json::JSON, '$.principal_id') AS principal_id,
    json_extract_string(token_json::JSON, '$.auth_source') AS auth_source,
    json_extract_string(token_json::JSON, '$.subscriber_id') AS subscriber_id,
    json_extract(token_json::JSON, '$.session_id')::UBIGINT AS session_id,
    json_extract_string(token_json::JSON, '$.session_token') AS session_token,
    json_extract_string(token_json::JSON, '$.backend_url') AS backend_url
  FROM token_raw
  WHERE token_json IS NOT NULL
),
token_ready AS MATERIALIZED (
  SELECT *
  FROM token_meta
  WHERE tenant_id IS NOT NULL
    AND principal_id IS NOT NULL
    AND auth_source IS NOT NULL
    AND subscriber_id IS NOT NULL
    AND session_id IS NOT NULL
    AND session_token IS NOT NULL
    AND backend_url IS NOT NULL
),
authorized AS MATERIALIZED (
  SELECT *
  FROM principal, token_ready
  WHERE principal.tenant_id = token_ready.tenant_id
    AND principal.principal_id = token_ready.principal_id
    AND principal.auth_source = token_ready.auth_source
)
SELECT
  EXISTS (SELECT 1 FROM principal) AS has_principal,
  EXISTS (SELECT 1 FROM token_ready) AS has_token_meta,
  EXISTS (SELECT 1 FROM authorized) AS is_authorized,
  (SELECT tenant_id FROM authorized LIMIT 1) AS tenant_id,
  (SELECT subscriber_id FROM authorized LIMIT 1) AS subscriber_id,
  (SELECT backend_url FROM authorized LIMIT 1) AS backend_url,
  (SELECT session_id FROM authorized LIMIT 1) AS session_id,
  (SELECT session_token FROM authorized LIMIT 1) AS session_token;
CREATE TEMP TABLE __ducknng_gateway_close_meta AS
SELECT
  tenant_id,
  subscriber_id,
  ducknng_frame_error_text(
    ducknng_close_query_raw(
      backend_url,
      session_id,
      session_token,
      0::UBIGINT
    )
  ) AS close_error
FROM __ducknng_gateway_close_state
WHERE is_authorized;
CREATE TEMP TABLE __ducknng_gateway_close_reply AS
SELECT
  CASE
    WHEN NOT has_principal THEN 401
    WHEN NOT has_token_meta THEN 400
    WHEN NOT is_authorized THEN 403
    WHEN EXISTS (SELECT 1 FROM __ducknng_gateway_close_meta WHERE close_error IS NOT NULL) THEN 502
    ELSE 200
  END AS status,
  'application/json; charset=utf-8' AS content_type,
  CASE
    WHEN NOT has_principal THEN CAST(to_json(struct_pack(closed := FALSE, error := 'missing or invalid bearer token')) AS VARCHAR)
    WHEN NOT has_token_meta THEN CAST(to_json(struct_pack(closed := FALSE, error := 'invalid continuation token')) AS VARCHAR)
    WHEN NOT is_authorized THEN CAST(to_json(struct_pack(closed := FALSE, error := 'continuation token belongs to another tenant or principal')) AS VARCHAR)
    WHEN EXISTS (SELECT 1 FROM __ducknng_gateway_close_meta WHERE close_error IS NOT NULL)
      THEN CAST(to_json(struct_pack(closed := FALSE, error := (SELECT close_error FROM __ducknng_gateway_close_meta WHERE close_error IS NOT NULL LIMIT 1))) AS VARCHAR)
    ELSE CAST(
      to_json(
        struct_pack(
          closed := TRUE,
          tenant_id := (SELECT tenant_id FROM __ducknng_gateway_close_meta LIMIT 1),
          subscriber_id := (SELECT subscriber_id FROM __ducknng_gateway_close_meta LIMIT 1)
        )
      ) AS VARCHAR
    )
  END AS body_text
FROM __ducknng_gateway_close_state;
SELECT status, content_type, body_text
FROM __ducknng_gateway_close_reply
"""
    return start_sql, fetch_sql, close_sql


def backend_worker(con: duckdb.DuckDBPyConnection, backend_key: str, listen_port: int) -> None:
    spec = spec_by_backend_key(backend_key)
    server_name = BACKEND_PREFIX + backend_key
    con.execute("DROP TABLE IF EXISTS tenant_numbers")
    con.execute("CREATE TABLE tenant_numbers(owner VARCHAR, i INTEGER, v INTEGER)")
    con.execute(
        f"INSERT INTO tenant_numbers "
        f"SELECT '{backend_key}' AS owner, i, i * 10 AS v "
        f"FROM range({int(spec['range_start'])}, {int(spec['range_stop'])}) AS t(i)"
    )
    con.execute(
        f"SELECT ducknng_start_server('{server_name}', 'tcp://127.0.0.1:{listen_port}', 1, 134217728, 300000, 0::UBIGINT)"
    )


def install_gateway_metadata(con: duckdb.DuckDBPyConnection, backend_urls: dict[str, str]) -> None:
    con.execute("DROP TABLE IF EXISTS gateway_identities")
    con.execute("DROP TABLE IF EXISTS gateway_subscribers")
    con.execute(
        "CREATE TABLE gateway_identities("
        "api_token VARCHAR, caller_identity VARCHAR, tenant_id VARCHAR, principal_id VARCHAR, active BOOLEAN)"
    )
    con.execute(
        "CREATE TABLE gateway_subscribers("
        "subscriber_id VARCHAR, tenant_id VARCHAR, backend_url VARCHAR, priority INTEGER, enabled BOOLEAN)"
    )
    identity_rows = [
        (str(spec["api_token"]), None, str(spec["tenant_id"]), str(spec["principal_id"]), True)
        for spec in SUBSCRIBER_SPECS
    ]
    identity_rows.append(
        (str(ORPHAN_IDENTITY["api_token"]), None, str(ORPHAN_IDENTITY["tenant_id"]), str(ORPHAN_IDENTITY["principal_id"]), True)
    )
    subscriber_rows = [
        (
            str(spec["subscriber_id"]),
            str(spec["tenant_id"]),
            backend_urls[str(spec["backend_key"])],
            1,
            True,
        )
        for spec in SUBSCRIBER_SPECS
    ]
    con.executemany(
        "INSERT INTO gateway_identities VALUES (?, ?, ?, ?, ?)",
        identity_rows,
    )
    con.executemany(
        "INSERT INTO gateway_subscribers VALUES (?, ?, ?, ?, ?)",
        subscriber_rows,
    )


def gateway_worker(con: duckdb.DuckDBPyConnection, gateway_port: int,
    backend_ports: dict[str, int]) -> None:
    backend_urls = {
        backend_key: f"tcp://127.0.0.1:{port}"
        for backend_key, port in backend_ports.items()
    }
    install_gateway_metadata(con, backend_urls)
    start_sql, fetch_sql, close_sql = build_gateway_route_sqls()
    con.execute(
        f"SELECT ducknng_start_server('{GATEWAY_NAME}', 'http://127.0.0.1:{gateway_port}/_ducknng', 1, 134217728, 300000, 0::UBIGINT)"
    )
    con.execute(
        "SELECT ducknng_register_http_route(?, ?, ?, ?)",
        [GATEWAY_NAME, "GET", "/healthz", "SELECT 200 AS status, 'text/plain; charset=utf-8' AS content_type, 'ok' AS body_text"],
    )
    con.execute(
        "SELECT ducknng_register_http_route(?, ?, ?, ?, CAST(? AS UBIGINT))",
        [GATEWAY_NAME, "POST", "/v1/query/start", start_sql, 1048576],
    )
    con.execute(
        "SELECT ducknng_register_http_route(?, ?, ?, ?, CAST(? AS UBIGINT))",
        [GATEWAY_NAME, "POST", "/v1/query/fetch", fetch_sql, 1048576],
    )
    con.execute(
        "SELECT ducknng_register_http_route(?, ?, ?, ?, CAST(? AS UBIGINT))",
        [GATEWAY_NAME, "POST", "/v1/query/close", close_sql, 1048576],
    )
