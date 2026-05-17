#pragma once
#include "duckdb_extension.h"
#include <stddef.h>
#include <stdint.h>

typedef struct ducknng_quack_column_schema {
    char *name;
    int logical_type_id;
    uint8_t decimal_width;
    uint8_t decimal_scale;
} ducknng_quack_column_schema;

typedef struct ducknng_quack_schema {
    idx_t ncols;
    ducknng_quack_column_schema *cols;
} ducknng_quack_schema;

void ducknng_quack_schema_reset(ducknng_quack_schema *schema);

int ducknng_result_next_chunk_to_quack_payload(duckdb_result result,
    uint8_t **out_bytes, size_t *out_len, int *has_chunk, char **errmsg);
int ducknng_result_next_chunks_to_quack_payload(duckdb_result result, int result_streaming,
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
