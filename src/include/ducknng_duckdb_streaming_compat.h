#pragma once
#include "duckdb_extension.h"

/*
 * DuckDB's current C API exposes incremental result delivery for prepared
 * statements through the pending-streaming entrypoint.  Keep that compatibility
 * decision in one small boundary so the session code does not grow scattered
 * version/deprecation branches.
 */
int ducknng_pending_prepared_for_session(duckdb_prepared_statement stmt,
    duckdb_pending_result *out_pending, int *out_result_streaming);
duckdb_data_chunk ducknng_result_fetch_session_chunk(duckdb_result result,
    int result_streaming);
