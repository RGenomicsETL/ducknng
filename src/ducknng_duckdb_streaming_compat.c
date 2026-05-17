#include "ducknng_duckdb_streaming_compat.h"

DUCKDB_EXTENSION_EXTERN

int ducknng_pending_prepared_for_session(duckdb_prepared_statement stmt,
    duckdb_pending_result *out_pending, int *out_result_streaming) {
    if (out_result_streaming) *out_result_streaming = 0;
#if defined(DUCKDB_API_NO_DEPRECATED)
    return duckdb_pending_prepared(stmt, out_pending);
#else
    if (out_result_streaming) *out_result_streaming = 1;
    return duckdb_pending_prepared_streaming(stmt, out_pending);
#endif
}

duckdb_data_chunk ducknng_result_fetch_session_chunk(duckdb_result result,
    int result_streaming) {
#if defined(DUCKDB_API_NO_DEPRECATED)
    (void)result_streaming;
    return duckdb_fetch_chunk(result);
#else
    return result_streaming ? duckdb_stream_fetch_chunk(result) : duckdb_fetch_chunk(result);
#endif
}
