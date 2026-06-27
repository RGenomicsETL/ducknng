# Browser wasm transport checklist

This checklist captures the remaining browser wasm transport matrix for later review. It is about the `duckdb-wasm` side-module runtime unless a row explicitly says webR. Browser support must be proven per transport; do not infer support from native Linux, webR, or another browser carrier.

## Current proven baseline

- [x] Build a `duckdb-wasm` side-module artifact for `ducknng`.
- [x] Load the extension in the local smoke page.
- [x] Load the extension from the GitHub Pages demo.
- [x] Run scalar extension checks and the interactive SQL shell in the Pages demo.
- [x] Prove local/header-hosted `wasm_threads` `inproc://` AIO request/reply and manifest collection.
- [x] Skip full `inproc://` proof on ordinary GitHub Pages service-worker COI, where that path is not reliable enough.
- [x] Keep `DUCKNNG_WASM_TRACE` build-time gated and off by default.

## Explicit browser non-goals unless a new design is approved

- [ ] Keep browser `ipc://` unsupported and documented as unsupported.
- [ ] Keep browser raw `tcp://` unsupported and documented as unsupported.
- [ ] Keep browser native POSIX-style `tls+tcp://` unsupported and documented as unsupported.
- [ ] Treat browser HTTP(S) TLS as browser-managed TLS only; do not silently consume native ducknng TLS config handles.
- [ ] Treat GitHub Pages wasm artifacts as demo provenance, not a stable binary release channel.

## Priority 1: browser Fetch-backed HTTP(S) client

- [x] Choose the implementation route: a JavaScript Fetch bridge for the stock `duckdb-wasm` runtime, or a custom runtime that deliberately exports the needed bridge imports. (Chose a synchronous `XMLHttpRequest` bridge via `EM_ASM`, allowed inside the duckdb-wasm Web Worker — `src/ducknng_wasm_http_fetch.c`, mirroring duckhts's `wasm_http_hfile.c`.)
- [x] Do not rely on C-only `emscripten_fetch()` in the stock runtime unless the missing imports are provided and tested. (The shim uses `EM_ASM` + XHR, requiring no extra runtime imports.)
- [x] Keep the browser HTTP implementation behind the HTTP transport adapter boundary, not in the RPC method layer. (Routed inside `ducknng_http_transact()` under `#ifdef __EMSCRIPTEN__`; scheme/family decisions stay above it.)
- [x] Implement browser `ducknng_ncurl(url, method, headers_json, body, timeout_ms, tls_config_id)` over Fetch for `http://` and `https://`. (`ducknng_ncurl` and `ducknng_ncurl_table` both flow through the rerouted `ducknng_http_transact`.)
- [x] Return the existing in-band result shape: `ok`, `status`, `error`, `headers_json`, `body`, and `body_text`. (Unchanged: the shim returns status + a raw response-header block + body, and the existing native code converts the header block to the canonical JSON.)
- [x] Preserve the canonical response header JSON contract, including duplicate headers where the browser API exposes them. (The shim passes `XMLHttpRequest.getAllResponseHeaders()` back unmodified and reuses `ducknng_http_headers_block_to_json`, so duplicate headers survive exactly as on native.)
- [x] Map CORS, COEP, network, abort, and timeout failures to `ok = false` rows with clear `error` text. (Network/CORS failures, allowlist denial, and setup errors each return a distinct `error` string and `ok = false`.)
- [x] Reject nonzero `tls_config_id` in browser Fetch mode with an explicit error instead of ignoring it. (`ducknng_http_transact` rejects any requested TLS config under `__EMSCRIPTEN__` with "https uses browser-managed TLS".)
- [~] Add timeout and cancellation behavior that does not leave leaked browser requests or stale SQL-visible handles. (Synchronous XHR cannot honor a timeout and exposes no SQL handle to leak; `timeout_ms` is accepted but documented as ignored. Async/cancellable Fetch is Priority 2 with `ducknng_ncurl_aio`.)
- [x] Add browser smoke-page probes for at least one same-origin HTTP request and one HTTPS request where feasible. (`test/browser/run_smoke.mjs` drives a same-origin GET and POST through `ducknng_ncurl`; HTTPS needs an external CORS-permissive origin and stays a follow-up.)

## Priority 2: browser HTTP(S) async and table helpers

- [ ] Implement browser `ducknng_ncurl_aio(...)` over the same Fetch adapter.
- [ ] Implement or adapt `ducknng_ncurl_aio_collect(...)` so terminal Fetch operations return the existing HTTP-shaped result rows.
- [ ] Keep `ducknng_aio_status(...)`, `ducknng_aio_wait(...)`, `ducknng_aio_cancel(...)`, and `ducknng_aio_drop(...)` behavior coherent for browser HTTP handles.
- [ ] Represent expected launch failures as terminal error handles when a runtime exists, matching the native AIO contract.
- [ ] Implement browser `ducknng_ncurl_table(...)` by reusing the Fetch result and the existing body codec layer.
- [ ] Prove JSON and at least one text or CSV response through `ducknng_ncurl_table(...)` in the smoke page.

## Priority 3: framed RPC/session helpers over browser HTTP(S)

- [ ] Route URL-based raw request helpers over the browser Fetch adapter for `http://` and `https://`.
- [ ] Route structured helpers over the same adapter: manifest, exec/query, query open, fetch, close, and cancel.
- [ ] Preserve the existing ducknng frame and method contracts; do not add parallel `http_exec`, `http_fetch`, or similar methods.
- [ ] Preserve Arrow IPC and `ducknng_quack_batch` payload behavior; do not substitute ad hoc JSON row serialization.
- [ ] Preserve session ownership and lifecycle semantics across the HTTP carrier.
- [ ] Add smoke-page probes for HTTP framed manifest and at least one small SQL request.
- [ ] Add a local test endpoint strategy for browser HTTP RPC proof, such as a local header-capable server or proxy, without requiring browser listener support.

## Priority 4: browser WebSocket adapter

- [ ] Decide the first supported browser WebSocket slice: URL-launched request/RPC helpers only, or enough generic socket API behavior to expose sockets.
- [ ] Prefer the smaller URL-launched request/RPC slice unless generic socket compatibility is explicitly required.
- [ ] Implement `ws://` and `wss://` through browser WebSocket APIs or an explicit JavaScript bridge, not by assuming native NNG WebSocket transport works in browser wasm.
- [ ] Define browser WSS TLS behavior as browser-managed TLS; reject unsupported native TLS config handles explicitly.
- [ ] Define how timeouts, close frames, abnormal closes, and backpressure map to SQL-visible errors or AIO status.
- [ ] Add separate smoke-page probes for `ws://` and `wss://`.
- [ ] Revisit docs/transports.md if browser WebSocket support is not equivalent to native NNG `ws://` / `wss://`.

## Browser listener/server matrix

- [ ] Keep browser HTTP listeners unsupported unless a service-worker, proxy, or embedding design is approved.
- [ ] Keep browser WebSocket listeners unsupported unless a browser-compatible server-side design is approved.
- [ ] Avoid implying that `ducknng_start_server('http://...')`, `ducknng_start_server('https://...')`, `ducknng_start_server('ws://...')`, or `ducknng_start_server('wss://...')` can bind inside a normal browser page.

## Hosting and proof requirements

- [ ] Keep GitHub Pages proof limited to extension load, scalars, codecs, and SQL shell unless the host provides real COOP/COEP headers.
- [ ] Use a local no-cache header-serving page or another real-header host for full `wasm_threads` `inproc://` proof.
- [ ] Treat proof PNGs as validation only when the text log says `status=Passed` or the relevant status fields show success.
- [ ] Add separate proof logs for HTTP(S) and WS/WSS rather than reusing the `inproc://` proof.
- [ ] Keep direct browser loading from GitHub release assets out of the required path under COEP; mirror CI-built wasm same-origin when needed.

## webR / Rducknng remains separate

- [ ] Do not infer webR support from the `duckdb-wasm` side-module demo.
- [ ] Design a future `r/Rducknng/` package path separately, using rwasm/webR build and install workflows.
- [ ] Resolve DuckDB version/platform compatibility before attempting direct webR loading.
- [ ] Keep browser network adapters behind transport boundaries so future webR work can reuse or replace them deliberately.

## Documentation and regression checks

- [ ] Update `docs/wasm.md` when a checklist item becomes supported behavior.
- [ ] Update `docs/transports.md` and `docs/http.md` before changing public transport semantics.
- [ ] Add or update README examples for new user-visible wasm workflows only when they are runnable and not misleading.
- [ ] Keep `.github/workflows/MainDistributionPipeline.yml` unchanged unless that workflow change is explicitly requested.
- [ ] Keep wasm-only NNG or dependency patches recorded under `patches/nng/` when vendored NNG changes.
- [ ] Run JavaScript syntax checks for the smoke page after page edits.
- [ ] Run static staging with `DUCKNNG_WASM_SERVE=0 DUCKDB_WASM_PLATFORM=wasm_threads scripts/start_duckdb_wasm_local_test.sh` before publishing Pages-impacting changes.
