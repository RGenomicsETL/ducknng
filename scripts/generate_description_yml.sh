#!/usr/bin/env bash
set -euo pipefail
# Generate description.yml for DuckDB community-extension submission.
# Run from the repo root: bash scripts/generate_description_yml.sh > description.yml

EXTENSION_NAME="ducknng"
GIT_REF=$(git rev-parse HEAD)
DESCRIPTION="Pure C DuckDB extension exposing a DuckDB-backed SQL and RPC server over NNG using Arrow IPC — with framed RPC, custom HTTP routes, TLS support, and a body codec layer"
VERSION="0.1.0"
LANGUAGE="C"
BUILD="cmake"
LICENSE="MIT"
MAINTAINER="sounkou-bioinfo"

cat <<YAML
extension:
  name: ${EXTENSION_NAME}
  description: ${DESCRIPTION}
  version: ${VERSION}
  language: ${LANGUAGE}
  build: ${BUILD}
  license: ${LICENSE}
  requires_toolchains: "python3"
  excluded_platforms: wasm_mvp;wasm_eh;wasm_threads
  maintainers:
    - "${MAINTAINER}"

repo:
  github: ${MAINTAINER}/${EXTENSION_NAME}
  ref: ${GIT_REF}

docs:
  hello_world: |
    -- Load the extension
    LOAD ducknng;

    -- Start an inproc REP server
    SELECT ducknng_start_server('demo', 'inproc://ducknng_demo', 1, 134217728, 30000, 0::UBIGINT);

    -- Run remote SQL via framed RPC
    SELECT ok, rows_changed
    FROM ducknng_query_rpc('inproc://ducknng_demo', 'CREATE TABLE IF NOT EXISTS nums AS SELECT range AS i FROM range(1, 4)', 0::UBIGINT, 0::UBIGINT, 0::UBIGINT);

    -- Open a query session and fetch results
    SELECT session_id, state
    FROM ducknng_open_query('inproc://ducknng_demo', 'SELECT i, i * i AS sq FROM nums', 0::UBIGINT, 0::UBIGINT, 0::UBIGINT);

    SELECT ducknng_stop_server('demo');

  extended_description: |
    ducknng is a pure C DuckDB extension that exposes a DuckDB-backed SQL and
    RPC server over NNG (Nanomsg Next Generation) using Arrow IPC with nanoarrow C
    for payload encoding and decoding.

    **Transport layer (NNG)**
    Supports inproc://, ipc://, tcp://, and tls+tcp:// URLs. TLS certificates
    can be loaded from file paths or in-memory PEM content; self-signed dev
    certificates are generated entirely inside the extension (no file I/O).

    **Framed RPC**
    Versioned request/reply envelope with manifest, exec, query session
    (open/fetch/close/cancel), and raw unary operations. All tabular data
    is encoded as Arrow IPC streams.

    **HTTP carrier**
    Start a server on an http:// or https:// URL for the framed RPC mount.
    Register custom HTTP routes (exact, prefix, or template matching) backed
    by SQL queries. Streaming chunked routes for Server-Sent Events are
    supported via ducknng_add_stream_route. Static asset serving, route-local
    auth policies, and background workers are also available.

    **Body codec layer**
    Parse HTTP response bodies by content type: JSON, NDJSON, CSV, TSV,
    Parquet, Arrow IPC, form-urlencoded, and ducknng frames. Standalone
    ducknng_parse_csv(body), ducknng_parse_tsv(body), and
    ducknng_parse_parquet(body) functions use DuckDB's standard readers
    via a tempfile round-trip. User-registered codec hooks extend the set.

    **Admission & security**
    mTLS peer-identity extraction, exact identity allowlists, IP/CIDR
    allowlists, per-service and per-principal resource limits (max memory,
    max sessions, max result bytes), and SQL authorizer callbacks.

    **Development & testing**
    Built against the DuckDB C API (no C++). Uses DuckDB's stable and
    unstable C extensions API for Arrow conversion. 20+ SQL integration
    tests run via sqllogictest. Cross-platform (Linux, macOS, Windows)
    via the extension-ci-tools CMake build system.

    Project details and examples: https://github.com/sounkou-bioinfo/ducknng

    Community package excludes WASM targets (NNG threading requirement).
YAML
