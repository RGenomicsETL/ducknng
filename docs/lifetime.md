# Lifetime and manual resource management

`ducknng` serves DuckDB SQL and manifest-declared RPC methods over byte transports. Arrow IPC carries portable table payloads. Lifetime rules apply inside the extension and to SQL-visible resource handles.

This document separates the two.

## 1. What DuckDB does give the extension

DuckDB's C extension API does provide destroy callbacks for extension-owned state that is tied to function registration or statement execution.

Examples include:

- scalar-function extra info via `duckdb_scalar_function_set_extra_info(..., destroy)`
- scalar-function bind data via `duckdb_scalar_function_set_bind_data(..., destroy)`
- table-function extra info via `duckdb_table_function_set_extra_info(..., destroy)`
- table-function bind data via `duckdb_bind_set_bind_data(..., destroy)`
- table-function init data via `duckdb_init_set_init_data(..., destroy)`
- aggregate destructors and similar callback-driven internal cleanup points

That is enough for extension-internal lifetime management of:

- registered function context objects
- bind-time objects
- init-time objects
- statement-local execution state

`ducknng` already uses those callback hooks for internal cleanup.

### Bind and execution ownership

DuckDB runs a table function's bind callback for `EXPLAIN`, `DESCRIBE`, `PREPARE`, and every plan it builds, sometimes more than once per statement; it runs init and scan only when the plan executes. `ducknng` therefore splits table-function state in two. Bind data is immutable for the life of the plan: it holds copied arguments, the registered function context, and the declared column list. A bind callback validates arguments and declares the schema from local inert information: constant arguments, an explicit declared schema, registry state, or caller-supplied bytes it can decode without running SQL. It never sends a request, executes supplied SQL, advances or fetches a session, consumes or waits on an aio, creates a temporary file, or creates any other external resource.

Init data belongs to one execution. Init or scan performs the request, opens the remote cursor, runs the DuckDB reader, or takes the runtime snapshot. The init-data destructor releases owned replies, results, and cursors. Temporary readers remove their files when the synchronous read finishes. Immutable decoded results from supplied bytes can remain bind-owned. Each `EXECUTE` of a prepared statement gets fresh init data, so repeated executions repeat the effect once each, and a failed execution cannot leave state for the next one. `EXPLAIN ANALYZE` executes the plan and therefore performs the effects.

The guarantee covers callbacks owned by `ducknng`. DuckDB may evaluate caller-supplied argument expressions, such as `nextval(...)` inside an argument, before calling the bind callback; that evaluation belongs to DuckDB, not to the extension.

Table functions whose shape depends on remote or reader output take an explicit `schema` named parameter. It is either a typed NULL STRUCT such as `schema := NULL::STRUCT(id BIGINT, name VARCHAR)`, whose child names and DuckDB logical types are the declared columns, or an Arrow IPC schema descriptor BLOB, which bind decodes locally with nanoarrow and maps through the same Arrow-to-DuckDB type mapping used for received payloads. A descriptor must be a well-formed Arrow IPC stream with a nonempty schema and no record batches; anything else is a bind error. At execution the received Arrow schema must match the declaration in column count, names, and types; a mismatch is an execution error rather than a silent cast. An Arrow descriptor expresses portable Arrow types only, so DuckDB-specific identity that Arrow cannot carry, such as Quack enum or union types, needs a typed STRUCT declaration.

Schema discovery is a separate executed statement. `ducknng_prepare_query(...)` and `ducknng_prepare_query_params(...)` have a fixed one-row result `schema_ipc BLOB`; bind only copies arguments, and each execution sends one `query_prepare` and returns its zero-row Arrow schema stream. A failed `query_prepare` is an execution error. The typical workflow stores the descriptor and passes it to the executor:

```sql
SET VARIABLE s = (SELECT schema_ipc FROM ducknng_prepare_query(url, sql, 0::UBIGINT));
SELECT * FROM ducknng_query_rpc(url, sql, 0::UBIGINT, schema := getvariable('s'));
```

`query_prepare` does not execute the prepared plan, but the remote DuckDB binds the SQL and may evaluate functions while doing so, so discovery is not a purity or security oracle for arbitrary SQL.

`ducknng_query_rpc(...)` owns the whole remote session it opens and closes it on completion, early termination, and failure. `ducknng_fetch_query_table(...)` instead reads from a caller-owned session: bind copies the URL, session id, token, batch limits, TLS id, and declaration without contacting the server, and each execution sends exactly one `fetch` and scans that batch. Repeated executions advance the cursor one batch each. A local failure after a successful fetch, such as a schema mismatch, cannot rewind the cursor, so that batch is consumed. The function never closes the session; the caller closes or cancels it explicitly.

The SQL regressions under `test/sql/` cover inert planning, schema validation,
prepared reuse, and fresh runtime snapshots. The native integration regression
`python test/authorizer_prepared_guard.py build/release/ducknng.duckdb_extension`
checks prepared policy execution, worker-thread context, and SQL-codec admission
against an external HTTP counter. Run it with each supported native DuckDB
runtime; it prints the SQL runtime version and native module path.
`test/body_tempfile_ownership.py` accepts the same extension argument. Its POSIX
fixture verifies exclusive creation, preservation of a pre-existing file,
owned-file cleanup, and denial when external access is disabled.

## 2. What DuckDB does not give us at the SQL-handle level

DuckDB SQL does **not** give `ducknng` a general-purpose GC/finalizer model for user-visible handles like R's external pointer finalizers.

In particular, there is no general SQL-level hook that says:

- this returned `UBIGINT` handle is no longer referenced anywhere
- this handle should be finalized automatically at end of user scope
- this object should be dropped when a variable or temp table cell becomes unreachable

So while DuckDB helps with extension-internal callback-owned state, it does **not** give `ducknng` a `nanonext`-style automatic finalizer story for long-lived SQL-visible runtime handles.

That is why `ducknng` has explicit cleanup functions.

## 3. Manual cleanup the user should do

If you create long-lived runtime objects from SQL, you should clean them up explicitly.

### Servers

If you start a server with `ducknng_start_server(...)`, stop it with:

- `ducknng_stop_server(name)`

Do not rely on database shutdown as the normal cleanup path for long-lived sessions or tests.

### Sockets

If you open a socket with `ducknng_open_socket(...)`, close it with:

- `(ducknng_close_socket(socket_id)).ok`

`ducknng_open_socket(...)` returns the handle as the `socket_id` field of its result struct. This applies to `req`, `rep`, `pair`, `poly`, `push`, `pull`, `pub`, `sub`, `surveyor`, `respondent`, and the other exposed generic protocols. Socket close failures are in-band result rows; they do not normally throw a DuckDB exception.

### AIO handles

If you launch an aio with:

- `ducknng_request_raw_aio(...)`
- `ducknng_request_socket_raw_aio(...)`
- `ducknng_send_socket_raw_aio(...)`
- `ducknng_recv_socket_raw_aio(...)`
- `ducknng_get_rpc_manifest_raw_aio(...)`
- `ducknng_run_rpc_raw_aio(...)`
- `ducknng_open_query_raw_aio(...)`
- `ducknng_fetch_query_raw_aio(...)`
- `ducknng_close_query_raw_aio(...)`
- `ducknng_cancel_query_raw_aio(...)`
- `ducknng_ncurl_aio(...)`
- `ducknng_ncurl_stream_open_aio(...)`
- `ducknng_ncurl_stream_recv_aio(...)`

then you should eventually release the aio handle explicitly with:

- `ducknng_aio_drop(aio_id)`

Important:

- `ducknng_aio_cancel(aio_id)` requests cancellation, but it is **not** the destructor
- the aio collect helpers, including the unary and streaming ncurl collectors, collect terminal results, but they do **not** auto-free the aio slot
- dropping the aio handle is still the explicit cleanup step

### HTTP response streams

A successful `ducknng_ncurl_stream_open_aio_collect(...)` call returns a separate `stream_id`. Close it with:

- `ducknng_ncurl_stream_close(stream_id)`

The open aio and stream are different resources. Before successful open collection, the stream is unclaimed and dropping the open aio closes it. Collection claims the stream, after which dropping the aio does not close the stream. Every claimed stream needs explicit close, including after `end_of_stream = true`. Closing aborts a pending body receive; the receive aio still needs collection or waiting followed by `ducknng_aio_drop(...)`.

### TLS config handles

If you create TLS config handles with:

- `ducknng_tls_config_from_files(...)`
- `ducknng_tls_config_from_pem(...)`
- `ducknng_self_signed_tls_config(...)`

then drop them when done with:

- `ducknng_drop_tls_config(tls_config_id)`

Note that `tls_config_id = 0` is the public sentinel for plaintext/no TLS config. It is not an allocated handle and does not need dropping.

### Query sessions

If you open a query session with:

- `ducknng_open_query(...)`

keep both the returned `session_id` and `session_token`. The `idle_timeout_ms` column is the server-owned effective idle cleanup policy for that session; it is not a client-authoritative lease extension. Close the session explicitly with:

- `ducknng_close_query(...)`

If the session was opened over verified mTLS, the server also records the transport-derived peer identity and requires later `fetch`, `close`, and `cancel` calls to arrive with the same verified identity.

If you need to stop work early, use:

- `ducknng_cancel_query(...)`

but treat `cancel` as best-effort control, not as a universal destructor guarantee. The normal session cleanup path is still explicit close unless the server clearly reports otherwise.

### Execution connection pool

Sessionful query methods and `request_connection` execution scopes use a runtime-owned pool of DuckDB connections. The pool opens a warm minimum eagerly, grows on demand up to a soft ceiling, and waits only a bounded time at the ceiling before returning a visible "pool exhausted" error. The ceiling is controlled through SQL:

- `ducknng_set_execution_pool_max(n)` sets the ceiling, clamped to the compiled range, and returns the effective value.
- `ducknng_execution_pool_max()` returns the current ceiling.

Raising the ceiling lets more concurrent query sessions or nested `query_rpc` table functions hold connections at once. Lowering it caps future growth without dropping connections already in use. A released pooled connection is reset before reuse by dropping connection-local temporary tables and views. This prevents the common temp-object leakage between sessions while keeping the connection handle stable; temp macros and session settings are not reset by this cleanup pass.

Each pooled connection carries at most one runtime-owned execution-context record while it is in use. Request, authorizer, and HTTP route scopes install the record after acquiring the connection and remove it before releasing the connection; session connections install it when the session acquires the connection and remove it when the session releases the connection. The record owns copies of the identity strings. Authorizer and HTTP route records borrow the callback context, which is valid because those scopes run one synchronous `duckdb_query()` and leave only after the result is materialized. Records still present at runtime teardown are freed with the runtime. The runtime's codec connection carries a record only while a body decode runs on it: the decoder takes the codec mutex, copies the record of the connection that requested the decode (if any), runs its prepare, execute, and materialization, then removes the record before releasing the mutex. The copied auth and HTTP pointers stay valid because the requesting connection is blocked in its own synchronous query for that whole window. The codec mutex is not recursive, so codec SQL that itself decodes a SQL-backed body fails with an error instead of waiting on the mutex it holds.

## 4. What runtime teardown still does for you

`ducknng` runtime teardown is still important, and recent hardening work improved it substantially.

When the runtime is destroyed, it now cleans up its owned structures rather than leaving destruction partial. That includes runtime-owned registries and transport/service state.

SQL-defined method registrations are the one registry state that is released only here. Replacing or unregistering a SQL method takes it out of the registry at once, but its handler SQL stays allocated until teardown so that a request already dispatched can finish with it. Repeatedly re-registering methods in a long-lived database therefore retains every past handler text; the cost is bounded by what the host registers.

But that is fallback/runtime teardown behavior. It is **not** a substitute for explicit user-level cleanup in a long-lived database process, test suite, notebook, or interactive session.

## 5. Practical rule of thumb

Treat `ducknng` like a low-level systems binding:

- if you **start** a server, you should **stop** it
- if you **open** a socket, you should **close** it
- if you **launch** an aio, you should **drop** it
- if you **open** an HTTP response stream, you should **close** it
- if you **create** a TLS config, you should **drop** it
- if you **open** a query session, you should **close** it

That explicit lifecycle is the honest current API contract.

## 6. Relation to API hardening

The recent lifetime pass improved internal destruction and ownership correctness, and the session ownership model is now explicit: `session_id` is a lookup key, `session_token` is the bearer capability, and verified mTLS identity is an additional owner constraint when present. Envelope-level RPC authentication may be added later as an application-auth layer, but it is not a prerequisite for the current session lifetime contract.

The main remaining lifetime-adjacent sealing blockers are therefore narrower:

- any remaining borrowed-pointer or concurrent-handle edge cases that should be eliminated before calling the API sealed
- clear documentation of which resources are user-managed and which are statement-local/internal

That is why this document exists and why the README now links to it directly.
