#pragma once
#include "ducknng_duckdb_compat.h"
#include <stddef.h>
#include <stdint.h>

/*
 * A column type node. Scalar columns use only name + logical_type_id (+ decimal
 * width/scale). Nested columns (LIST/ARRAY/STRUCT/UNION/MAP/ENUM) additionally
 * use the recursive fields: a LIST/ARRAY has one child; a STRUCT/UNION has
 * `nchildren` children with `child_names`; a MAP is modeled as a LIST whose
 * single child is a STRUCT(key,value); an ENUM carries `enum_count`/`enum_labels`.
 * Top-level columns live in `ducknng_quack_schema.cols` as a flat array (the
 * public API is unchanged); only child trees are separately heap-allocated.
 */
typedef struct ducknng_quack_column_schema {
    char *name;
    int logical_type_id;
    uint8_t decimal_width;
    uint8_t decimal_scale;
    uint32_t array_size;
    uint32_t enum_count;
    char **enum_labels;
    uint32_t nchildren;
    struct ducknng_quack_column_schema **children;
    char **child_names;
} ducknng_quack_column_schema;

typedef struct ducknng_quack_schema {
    idx_t ncols;
    ducknng_quack_column_schema *cols;
} ducknng_quack_schema;

void ducknng_quack_schema_reset(ducknng_quack_schema *schema);

int ducknng_result_next_chunk_to_quack_payload(duckdb_result result,
    uint8_t **out_bytes, size_t *out_len, int *has_chunk, char **errmsg);
int ducknng_result_next_chunks_to_quack_payload(duckdb_result result,
    uint64_t max_chunks, int include_schema, uint8_t **out_bytes, size_t *out_len,
    int *has_chunk, char **errmsg);
/* Materialized-result variant of ducknng_result_next_chunks_to_quack_payload:
 * encodes up to max_chunks batches starting at *inout_chunk_index (advancing it)
 * using duckdb_result_get_chunk, for results produced by duckdb_query. */
int ducknng_result_materialized_chunks_to_quack_payload(duckdb_result result,
    idx_t *inout_chunk_index, uint64_t max_chunks, int include_schema,
    uint8_t **out_bytes, size_t *out_len, int *has_chunk, char **errmsg);
int ducknng_result_empty_quack_payload(duckdb_result result,
    uint8_t **out_bytes, size_t *out_len, char **errmsg);

/* Parse the self-describing type + name header of a quack batch into a schema
 * without a bind_info. Fills *out_schema (caller frees with
 * ducknng_quack_schema_reset). This is the shared parse used by both the table
 * function bind path and the server-side upload append path, so it must remain
 * fail-closed against arbitrary bytes. */
int ducknng_quack_payload_parse_schema(const uint8_t *payload, size_t payload_len,
    ducknng_quack_schema *out_schema, char **errmsg);
int ducknng_quack_payload_bind_columns(duckdb_bind_info info,
    const uint8_t *payload, size_t payload_len,
    ducknng_quack_schema *out_schema, idx_t *out_row_count, char **errmsg);
/* Decode a quack batch and append its rows to an open appender whose column
 * types must match the batch schema. Adds the appended row count to
 * *inout_rows. Fail-closed: any malformed byte, schema/appender column
 * mismatch, or append error returns -1 with *errmsg and appends nothing
 * further. The appender is neither flushed nor destroyed here. */
/* expected_names (when non-NULL) are the target table's column names in order;
 * the batch column names must match them exactly, since the appender appends
 * by ordinal and would otherwise misassign same-typed columns. */
int ducknng_quack_payload_append_to_appender(duckdb_appender appender,
    const uint8_t *payload, size_t payload_len,
    const char *const *expected_names, idx_t expected_name_count,
    uint64_t *inout_rows, char **errmsg);
int ducknng_quack_payload_read_row_count(const uint8_t *payload, size_t payload_len,
    const ducknng_quack_schema *schema, idx_t *out_row_count, char **errmsg);
int ducknng_quack_payload_scan_begin(const uint8_t *payload, size_t payload_len,
    const ducknng_quack_schema *schema, size_t *inout_offset, uint64_t *out_remaining,
    char **errmsg);
int ducknng_quack_payload_scan_next(duckdb_data_chunk output,
    const uint8_t *payload, size_t payload_len, const ducknng_quack_schema *schema,
    size_t *inout_offset, uint64_t *inout_remaining, char **errmsg);
int ducknng_quack_payload_scan(duckdb_data_chunk output,
    const uint8_t *payload, size_t payload_len,
    const ducknng_quack_schema *schema, idx_t *inout_offset, char **errmsg);
