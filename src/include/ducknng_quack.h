#pragma once
#include "duckdb_extension.h"
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
int ducknng_result_empty_quack_payload(duckdb_result result,
    uint8_t **out_bytes, size_t *out_len, char **errmsg);

int ducknng_quack_payload_bind_columns(duckdb_bind_info info,
    const uint8_t *payload, size_t payload_len,
    ducknng_quack_schema *out_schema, idx_t *out_row_count, char **errmsg);
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
