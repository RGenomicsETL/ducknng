#pragma once

#include "duckdb_extension.h"

/* DuckDB 1.4 predates the unchecked length-aware vector assignment helper.
 * Its length-aware assignment function accepts both VARCHAR and BLOB values. */
#ifdef DUCKNNG_DUCKDB_PRE_1_5
#define duckdb_unsafe_vector_assign_string_element_len \
    duckdb_vector_assign_string_element_len
#endif
