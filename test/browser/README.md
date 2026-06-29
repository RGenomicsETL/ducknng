# Browser wasm smoke tests

This directory contains the local Playwright test runner for the `duckdb-wasm` side-module smoke page. It is intentionally separate from the static smoke page itself: the page lives under `scripts/`, while this runner starts a local header-capable HTTP server and drives that page in real Chromium.

The runner proves only the probes requested on the command line. The default `load` probe starts DuckDB wasm, loads the `ducknng` extension, verifies `crossOriginIsolated` under local COOP/COEP headers, and runs one SQL shell query through the loaded extension. Transport probes such as `inproc` and `http-sync` are opt-in so an artifact does not accidentally claim transport support before that path is present.

## Setup

Install the local Node dependencies once:

```sh
cd test/browser
npm ci
```

If Playwright browsers are not already installed on the machine, install Chromium with:

```sh
npx playwright install chromium
```

## Stage a site

The runner consumes a staged static site. Build and stage one from the repository root:

```sh
DUCKNNG_WASM_SERVE=0 DUCKDB_WASM_PLATFORM=wasm_eh \
  scripts/start_duckdb_wasm_local_test.sh
```

or, for the pthread runtime:

```sh
DUCKNNG_WASM_SERVE=0 DUCKDB_WASM_PLATFORM=wasm_threads \
  scripts/start_duckdb_wasm_local_test.sh
```

The staged site is normally written to:

```text
.duckdb-wasm-local-artifacts/site
```

## Run probes

From the repository root, prove extension load and the SQL shell:

```sh
node test/browser/run_smoke.mjs .duckdb-wasm-local-artifacts/site
```

That is equivalent to:

```sh
node test/browser/run_smoke.mjs .duckdb-wasm-local-artifacts/site --probes=load
```

To run the smoke page's scalar/codec/`inproc://` proof as well:

```sh
node test/browser/run_smoke.mjs .duckdb-wasm-local-artifacts/site --probes=load,inproc
```

The `inproc` probe is diagnostic for `wasm_threads` on a real COOP/COEP local server. It has passed individual local proofs, but repeated headless Chromium runs currently expose extension-load and NNG progress flakiness. For non-threaded runtimes, an unavailable `inproc://` result is reported as acceptable rather than as transport support.

To exercise the synchronous browser HTTP client path after building an artifact that includes the browser HTTP bridge:

```sh
node test/browser/run_smoke.mjs .duckdb-wasm-local-artifacts/site --probes=load,http-sync
```

The `http-sync` probe starts same-origin local test endpoints and calls them through `ducknng_ncurl(...)` from the page's SQL shell. It proves GET, POST body round-trip, and an invalid `headers_json` in-band error for the browser HTTP bridge. It does not prove `ducknng_ncurl_aio(...)`, `ducknng_ncurl_table(...)`, framed HTTP RPC/session helpers, HTTPS against a real remote origin, or WebSocket support.

Useful environment variables:

```sh
BROWSER_DEBUG=1
DUCKNNG_BROWSER_PROBES=load,http-sync
```

A successful run prints:

```text
BROWSER SMOKE: PASS
```

Do not treat screenshots or browser images as proof unless the text log contains the passing status.
