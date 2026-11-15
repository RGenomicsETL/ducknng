#pragma once

#include "duckdb_extension.h"
#include "nanoarrow/nanoarrow.h"
#include "nanoarrow/nanoarrow_ipc.h"

int ducknng_sql_arrow_schema_to_logical_type(const struct ArrowSchema *schema,
    duckdb_logical_type *out_type, char **errmsg);
int ducknng_sql_arrow_value_at(const struct ArrowSchema *schema,
    struct ArrowArrayView *view, idx_t index, duckdb_value *out_value,
    char **errmsg);
int ducknng_sql_arrow_bind_result_columns(duckdb_bind_info info,
    const struct ArrowSchema *schema, char **errmsg);
/* Require the top-level Arrow schema to match a declared column list exactly:
 * same count, same names in order, and deep-equal converted DuckDB types.
 * Call before writing any output vectors. */
int ducknng_sql_arrow_schema_validate_declared(const struct ArrowSchema *schema,
    idx_t ncols, const char *const *names, const duckdb_logical_type *types,
    char **errmsg);
int ducknng_sql_arrow_assign_column(duckdb_vector vec, struct ArrowArrayView *col_view,
    const struct ArrowSchema *col_schema, idx_t src_offset, idx_t count, char **errmsg);
int ducknng_sql_arrow_scan_table(duckdb_data_chunk output, const struct ArrowSchema *schema,
    const struct ArrowArray *array, idx_t row_count, idx_t *offset, char **errmsg);
