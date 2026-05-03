# ducknng API sealing checklist

This checklist is narrower than `docs/design_review_checklist.md`. It tracks what still blocks calling the current public API sealed or stable enough that public names, contracts, and examples should stop changing casually.

## Current position

`ducknng` already has a real public surface for:

- service lifecycle control
- method registry administration
- raw request/reply helpers
- generic NNG socket patterns
- raw aio handles
- the first raw unary RPC aio wrappers
- query-session control helpers
- TLS config handles
- a scheme-routed server helper plus URL-routed synchronous request/RPC/session helpers
- NNG WebSocket transport schemes through `ws://` and `wss://`
- a low-level HTTP/HTTPS client helper
- a low-level HTTP route framework beside the framed RPC mount
- built-in content-type driven body codec helpers for raw/text/JSON/Arrow IPC/frame bodies, with CSV/TSV/Parquet recognized and using the generic `body BLOB` fallback pending a memory-backed reader path
- user-extensible body codec hooks via `ducknng_register_codec(content_type, function_name)` and `ducknng_unregister_codec(content_type)`, gated to a plain-identifier allowlist and dispatched through a fixed `SELECT <fn>(?::BLOB) AS value` shape

That is enough for serious use and interop work, but it is not yet enough to call the full API sealed.

## Must-resolve before sealing

### 1. Session ownership and execution-lane policy review

The previous bare-`session_id` ownership blocker is addressed and the current query-family ownership model is the stable public contract: `query_open` returns a session-scoped bearer `session_token`, and `fetch`, `close`, and `cancel` must present it with the session id. Transport-derived mTLS identity is an additional owner constraint when present: sessions opened with a verified peer identity are bound to that identity as well as to the token. Exact verified-peer allowlists, IP/CIDR allowlists, descriptor auth, listener-wide mTLS, and SQL authorizers are deployment admission and policy layers around that session contract. Envelope-level RPC authentication may be added later as an additive application-auth layer, but it is not required to seal the current session ownership model and must not weaken token or mTLS owner checks.

The current service execution model is a single serialized DuckDB execution lane backed by the runtime init connection. That is no longer treated as an automatic sealing blocker: deployments can scale or isolate work by using multiple DuckDB contexts, multiple `ducknng` services or instances, or an upstream router. Free-form `exec` and query-session SQL are intentionally deployment-owned capabilities rather than an automatic sandbox; the remaining sealing work is to keep `ducknng`'s own generated SQL injection-safe and document clear deployment profiles for local, trusted-mesh, shared-client, and public/untrusted settings. Per-session or per-request DuckDB connections remain a possible hard-isolation mode for deployments that need it, not the only acceptable sealed posture.

### 2. Resource quotas for multi-client services

The current implementation enforces the stable baseline quotas: listener receive-size limits, descriptor request/reply-size limits, service-level session idle timeout, a service-level `max_open_sessions` cap, an NNG `max_active_pipes` cap, a carrier-neutral `max_inflight_requests` cap, and an mTLS-owner `max_sessions_per_peer_identity` cap set with `ducknng_set_service_limits(...)`; `query_open` replies expose the server-owned effective `idle_timeout_ms`, and the manifest exposes server-owned session policy including `max_open_sessions` and `max_sessions_per_peer_identity`. In the current stable model, quota owners are the service itself and, for the identity cap, the verified peer identity. The SQL-authorizer `principal` column is deployment policy/audit metadata rather than durable session ownership metadata. Future explicit limits for per-pipe/principal in-flight requests, principal-based open-session caps, cumulative reply bytes per owner, and session-open rate are additive hardening work rather than blockers for the current API contract.

### 3. Fetch payload decoding stance

Fetched Arrow payloads are decodable two ways today. `ducknng_fetch_query(...)` keeps the explicit control row and returns the Arrow IPC batch as `payload`, which can still be decoded through `ducknng_parse_body(payload, 'application/vnd.apache.arrow.stream')`. `ducknng_fetch_query_table(...)` is the convenience wrapper for the common one-fetch row path: it expects an Arrow row reply and exposes that batch directly as a DuckDB table. Both surfaces reuse the same shared Arrow IPC decoder and DuckDB vector mapping.

### 4. HTTP async and web-server framework scope

The HTTP transport direction is now in place:

- `ducknng_start_server(...)` now covers `http://` and `https://` listeners with `contexts = 1`
- the existing synchronous request/RPC/session helpers route over `http://` and `https://`
- the raw RPC aio helpers also route over `http://` and `https://`
- `ducknng_ncurl_aio(...)` and `ducknng_ncurl_aio_collect(...)` provide the raw asynchronous HTTP/HTTPS client counterpart to `ducknng_ncurl(...)`

The low-level HTTP route framework is now also part of the public SQL surface: `ducknng_register_http_route(...)`, `ducknng_register_http_route_pattern(...)`, `ducknng_unregister_http_route(...)`, `ducknng_unregister_http_route_pattern(...)`, `ducknng_list_http_routes()`, `ducknng_http_request()`, `ducknng_http_request_body()`, named request accessors, strict header/query/cookie/path parsers, and one-row response-builder macros let an `http://` or `https://` service expose additional exact, prefix, or template routes beside the framed RPC mount. That route layer remains deliberately separate from the manifest-derived RPC surface: registered routes are not manifest methods, they do not mint `http_exec` or `http_fetch`, and they inherit the same service admission stack and `shared_serialized_connection` execution model as the rest of the service.

The remaining HTTP deferments are richer web-toolkit features rather than the existence of a route layer itself: static assets, HTTP-carrier streaming features, route-local authentication policy, worker lifecycle management, and application gateway products remain additive future work.

### 5. Transport matrix stance

The generic socket dial surface accepts an explicit `tls_config_id`, which makes `tls+tcp://` and `wss://` usable through the existing handle model. The stable scheme matrix is documented in `docs/transports.md` and summarized in the README: the shared server surface accepts both NNG and HTTP families, generic socket surfaces remain NNG-only, synchronous RPC/session helpers and raw RPC AIO helpers route across both families, and non-zero TLS handles are accepted only on TLS-capable schemes.

### 6. Final async surface scope

The stable async contract is raw-result-first. NNG/RPC aio helpers collect raw frames through `ducknng_aio_collect(...)`, including the raw session-family launchers `ducknng_open_query_raw_aio(...)`, `ducknng_fetch_query_raw_aio(...)`, `ducknng_close_query_raw_aio(...)`, and `ducknng_cancel_query_raw_aio(...)`. `ducknng_aio_collect_decoded(...)` is the first structured convenience wrapper layered over that same substrate: it still waits on the same terminal frame results, but it projects the decoded envelope columns directly for callers that want a table-shaped result without manual `ducknng_decode_frame(...)` joins. HTTP aio helpers collect raw HTTP status/header/body rows through `ducknng_ncurl_aio_collect(...)`, and callers explicitly decode frames or bodies afterward. The synchronous raw session twins now exist too: `ducknng_open_query_raw(...)`, `ducknng_fetch_query_raw(...)`, `ducknng_close_query_raw(...)`, and `ducknng_cancel_query_raw(...)` return one reply frame directly, while the scalar frame accessors (`ducknng_frame_version(...)`, `ducknng_frame_type(...)`, `ducknng_frame_flags(...)`, `ducknng_frame_type_name(...)`, `ducknng_frame_name(...)`, `ducknng_frame_payload(...)`, `ducknng_frame_payload_text(...)`, `ducknng_frame_error_text(...)`, `ducknng_frame_end_of_stream(...)`) expose the route-safe decode path for dynamic SQL such as HTTP handlers. AIO launch timeout bounds one pending operation; `ducknng_aio_collect(..., wait_ms)` and `ducknng_ncurl_aio_collect(..., wait_ms)` only control how long a later collection call waits.

### 7. Final type contract stance

The unary Arrow row path now covers the next planned type wave for practical DuckDB tabular values: temporal types, decimals, lists, and structs in addition to the original scalar subset. Before sealing, the remaining type question is whether that documented set is the initial public contract or whether any additional Arrow features, such as map values, dictionary-preserving roundtrips, extension types, or broader deep-nesting guarantees, must land first.

## Strongly recommended before sealing

### 8. Representative protocol examples and tests

The repo now demonstrates req-style RPC, `pair`, `push` / `pull`, `pub` / `sub`, raw aio, unary RPC aio, and the current session control flow. Before sealing, the remaining question is not whether the protocol family is entirely undocumented, but whether the current examples and tests are representative enough for the less-common raw socket patterns that remain open/close-oriented rather than tutorial-driven.

At minimum, the project should keep the existing runnable examples green and decide whether additional tutorial coverage for `bus` or any later event-oriented patterns belongs in the sealed public story.

### 9. Explicit scope for `ws://` and `wss://`

`ws://` and `wss://` are now explicit NNG transport schemes in scope for the public API. What still needs to stay explicit is the boundary: they belong to the NNG transport family, not to the HTTP carrier layer. The HTTP transport contract may still defer browser-style or HTTP-carrier WebSocket work even while NNG WebSocket transports are supported.

## Already addressed in the current pass

These items were worth resolving before the API hardens further and should stay fixed:

- runtime teardown now cleans up services, aio handles, client sockets, TLS configs, the runtime mutex, and the global runtime registry entry instead of leaving destruction partial
- the vendored NNG Windows MinGW fallback is now documented through an explicit local patch file and patch ledger under `patches/nng/`
- README examples now include representative raw protocol slices beyond req/pair, including `push` / `pull`, `pub` / `sub`, and `surveyor` / `respondent`
- the README and `docs/lifetime.md` now make the low-level manual-lifecycle contract explicit instead of implying a nanonext-style GC/finalizer model that DuckDB SQL does not actually provide for user-visible handles
- the unary Arrow row path now includes `DATE`, `TIME`, `TIMESTAMP`, `DECIMAL`, `LIST`, and `STRUCT` coverage with SQL-visible regression tests
- query sessions now bind ownership to an explicit `session_token` bearer capability instead of treating `session_id` as a capability
- TLS-authenticated services now attach verified mTLS peer identity to requests and bind sessions to that identity when present
- `ducknng_register_exec_method(true)` can register the opt-in `exec` descriptor with verified peer identity required, while the zero-argument form remains backwards compatible
- `ducknng_set_method_auth(name, requires_auth)` can protect registry-backed methods such as `manifest` using the same descriptor-level auth path
- unregistration now refuses to remove sessionful methods or families while any service has open sessions
- `ducknng_ncurl_aio(...)` now provides the nanonext-style async HTTP client slice and collects through `ducknng_ncurl_aio_collect(...)`
- `ducknng_read_monitor(...)`, `ducknng_monitor_status(...)`, and `ducknng_list_pipes(...)` provide NNG pipe event and active-pipe telemetry
- `src/ducknng_sql_api.c` has been split into focused SQL registration modules and is now only the top-level registration orchestrator
- lifecycle tests cover stopping services with live sessions and rejecting same-service stop while a service-owned SQL authorizer request is active
- unsupported URL schemes and supplied TLS configuration on non-TLS schemes have regression coverage across the major client/helper families
- fetched Arrow IPC session payloads are decoded through `ducknng_parse_body(payload, 'application/vnd.apache.arrow.stream')`, with regression coverage in the session smoke test
- `docs/security.md` now states that arbitrary SQL execution is a deployment-owned capability, not an automatic sandbox, and names recommended exposure profiles plus the internal SQL-injection boundary
- the transport matrix is documented in `docs/transports.md` and summarized in the README, including which schemes accept TLS handles and which surfaces reject the other family
- the async contract is raw-result-first: NNG/RPC aio returns frames, HTTP aio returns HTTP-shaped rows, and any additional structured async wrappers should remain layered conveniences
- the public catalog intentionally excludes `ducknng__*` macro helpers; those names are implementation details for stable-C-API macro plumbing and are not supported user APIs
- user-defined body codec hooks are sealed: `ducknng_register_codec(content_type, function_name)` and `ducknng_unregister_codec(content_type)` install `BLOB → VARCHAR` SQL functions, the registry is gated to plain SQL identifiers spliced into a fixed `SELECT <fn>(?::BLOB) AS value` shape, user hooks take precedence over built-ins for matching content types, and `ducknng_list_codecs()` reports them with `kind = 'user'` alongside built-ins

## Not sealing blockers by themselves

These are still important, but they do not need to be finished before the API can be considered sealed if the above items are settled:

- richer HTTP route-framework features such as static assets, HTTP-carrier streaming, route-local authentication policy, worker lifecycle management, or packaged gateway products, because the sealed current layer is the low-level route surface beside the framed RPC endpoint
- scalarfs-style in-memory filesystem/provider research for CSV/TSV/Parquet body parsing, because the generic `body BLOB` fallback is an acceptable stable behavior until a clean provider exists
- a future DuckDB-native Arrow re-plumb, if one ever becomes viable without unstable or deprecated APIs
