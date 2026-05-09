#include "ducknng_ipc_out.h"
#include "ducknng_util.h"
#include "nanoarrow/nanoarrow.h"
#include "nanoarrow/nanoarrow_ipc.h"
#include <stdio.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

static int ducknng_build_exec_request_schema(struct ArrowSchema *schema, struct ArrowError *error) {
    ArrowSchemaInit(schema);
    if (ArrowSchemaSetTypeStruct(schema, 2) != NANOARROW_OK) return -1;
    if (ArrowSchemaSetName(schema->children[0], "sql") != NANOARROW_OK) return -1;
    if (ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_STRING) != NANOARROW_OK) return -1;
    if (ArrowSchemaSetName(schema->children[1], "want_result") != NANOARROW_OK) return -1;
    if (ArrowSchemaSetType(schema->children[1], NANOARROW_TYPE_BOOL) != NANOARROW_OK) return -1;
    (void)error;
    return 0;
}

static int ducknng_build_query_open_request_schema(struct ArrowSchema *schema, struct ArrowError *error) {
    ArrowSchemaInit(schema);
    if (ArrowSchemaSetTypeStruct(schema, 3) != NANOARROW_OK) return -1;
    if (ArrowSchemaSetName(schema->children[0], "sql") != NANOARROW_OK) return -1;
    if (ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_STRING) != NANOARROW_OK) return -1;
    if (ArrowSchemaSetName(schema->children[1], "batch_rows") != NANOARROW_OK) return -1;
    if (ArrowSchemaSetType(schema->children[1], NANOARROW_TYPE_UINT64) != NANOARROW_OK) return -1;
    if (ArrowSchemaSetName(schema->children[2], "batch_bytes") != NANOARROW_OK) return -1;
    if (ArrowSchemaSetType(schema->children[2], NANOARROW_TYPE_UINT64) != NANOARROW_OK) return -1;
    (void)error;
    return 0;
}

static int ducknng_build_metadata_schema(struct ArrowSchema *schema, struct ArrowError *error) {
    ArrowSchemaInit(schema);
    if (ArrowSchemaSetTypeStruct(schema, 3) != NANOARROW_OK) return -1;
    if (ArrowSchemaSetName(schema->children[0], "rows_changed") != NANOARROW_OK) return -1;
    if (ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_UINT64) != NANOARROW_OK) return -1;
    if (ArrowSchemaSetName(schema->children[1], "statement_type") != NANOARROW_OK) return -1;
    if (ArrowSchemaSetType(schema->children[1], NANOARROW_TYPE_INT32) != NANOARROW_OK) return -1;
    if (ArrowSchemaSetName(schema->children[2], "result_type") != NANOARROW_OK) return -1;
    if (ArrowSchemaSetType(schema->children[2], NANOARROW_TYPE_INT32) != NANOARROW_OK) return -1;
    (void)error;
    return 0;
}

static int ducknng_set_arrow_schema_type(duckdb_logical_type logical_type, struct ArrowSchema *schema,
    char **errmsg) {
    duckdb_type type_id;
    if (!logical_type || !schema) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing logical type for Arrow schema mapping");
        return -1;
    }
    type_id = duckdb_get_type_id(logical_type);
    switch (type_id) {
        case DUCKDB_TYPE_BOOLEAN:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_BOOL) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_TINYINT:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_INT8) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_SMALLINT:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_INT16) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_INTEGER:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_INT32) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_BIGINT:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_INT64) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_UTINYINT:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_UINT8) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_USMALLINT:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_UINT16) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_UINTEGER:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_UINT32) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_UBIGINT:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_UINT64) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_FLOAT:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_FLOAT) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_DOUBLE:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_DOUBLE) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_VARCHAR:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_STRING) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_BLOB:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_BINARY) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_DATE:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_DATE32) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_TIME:
            return ArrowSchemaSetTypeDateTime(schema, NANOARROW_TYPE_TIME64, NANOARROW_TIME_UNIT_MICRO, NULL) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_TIME_NS:
            return ArrowSchemaSetTypeDateTime(schema, NANOARROW_TYPE_TIME64, NANOARROW_TIME_UNIT_NANO, NULL) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_TIMESTAMP:
            return ArrowSchemaSetTypeDateTime(schema, NANOARROW_TYPE_TIMESTAMP, NANOARROW_TIME_UNIT_MICRO, NULL) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_TIMESTAMP_S:
            return ArrowSchemaSetTypeDateTime(schema, NANOARROW_TYPE_TIMESTAMP, NANOARROW_TIME_UNIT_SECOND, NULL) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_TIMESTAMP_MS:
            return ArrowSchemaSetTypeDateTime(schema, NANOARROW_TYPE_TIMESTAMP, NANOARROW_TIME_UNIT_MILLI, NULL) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_TIMESTAMP_NS:
            return ArrowSchemaSetTypeDateTime(schema, NANOARROW_TYPE_TIMESTAMP, NANOARROW_TIME_UNIT_NANO, NULL) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_DECIMAL:
            return ArrowSchemaSetTypeDecimal(schema, NANOARROW_TYPE_DECIMAL128,
                (int32_t)duckdb_decimal_width(logical_type), (int32_t)duckdb_decimal_scale(logical_type)) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_HUGEINT:
            return ArrowSchemaSetTypeDecimal(schema, NANOARROW_TYPE_DECIMAL128, 38, 0) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_UUID:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_STRING) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_TIMESTAMP_TZ:
            return ArrowSchemaSetTypeDateTime(schema, NANOARROW_TYPE_TIMESTAMP, NANOARROW_TIME_UNIT_MICRO, "UTC") == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_ENUM:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_STRING) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_MAP: {
            duckdb_logical_type key_type = duckdb_map_type_key_type(logical_type);
            duckdb_logical_type value_type = duckdb_map_type_value_type(logical_type);
            int ok;
            if (ArrowSchemaSetType(schema, NANOARROW_TYPE_MAP) != NANOARROW_OK) {
                if (key_type) duckdb_destroy_logical_type(&key_type);
                if (value_type) duckdb_destroy_logical_type(&value_type);
                return -1;
            }
            ok = key_type && value_type &&
                ducknng_set_arrow_schema_type(key_type, schema->children[0]->children[0], errmsg) == 0 &&
                ducknng_set_arrow_schema_type(value_type, schema->children[0]->children[1], errmsg) == 0;
            if (key_type) duckdb_destroy_logical_type(&key_type);
            if (value_type) duckdb_destroy_logical_type(&value_type);
            return ok ? 0 : -1;
        }
        case DUCKDB_TYPE_UNION: {
            idx_t nmembers = duckdb_union_type_member_count(logical_type);
            idx_t i;
            if (ArrowSchemaSetTypeUnion(schema, NANOARROW_TYPE_DENSE_UNION, (int64_t)nmembers) != NANOARROW_OK) return -1;
            for (i = 0; i < nmembers; i++) {
                duckdb_logical_type member_type = duckdb_union_type_member_type(logical_type, i);
                char *member_name = duckdb_union_type_member_name(logical_type, i);
                int member_ok = member_type &&
                    ArrowSchemaSetName(schema->children[i], member_name ? member_name : "") == NANOARROW_OK &&
                    ducknng_set_arrow_schema_type(member_type, schema->children[i], errmsg) == 0;
                if (member_name) duckdb_free(member_name);
                if (member_type) duckdb_destroy_logical_type(&member_type);
                if (!member_ok) return -1;
            }
            return 0;
        }
        case DUCKDB_TYPE_LIST: {
            duckdb_logical_type child_type = duckdb_list_type_child_type(logical_type);
            int ok;
            if (ArrowSchemaSetType(schema, NANOARROW_TYPE_LIST) != NANOARROW_OK) {
                if (child_type) duckdb_destroy_logical_type(&child_type);
                return -1;
            }
            ok = child_type && ducknng_set_arrow_schema_type(child_type, schema->children[0], errmsg) == 0;
            if (child_type) duckdb_destroy_logical_type(&child_type);
            return ok ? 0 : -1;
        }
        case DUCKDB_TYPE_STRUCT: {
            idx_t nchildren = duckdb_struct_type_child_count(logical_type);
            idx_t i;
            if (ArrowSchemaSetTypeStruct(schema, (int64_t)nchildren) != NANOARROW_OK) return -1;
            for (i = 0; i < nchildren; i++) {
                duckdb_logical_type child_type = duckdb_struct_type_child_type(logical_type, i);
                char *child_name = duckdb_struct_type_child_name(logical_type, i);
                int child_ok = child_type &&
                    ArrowSchemaSetName(schema->children[i], child_name ? child_name : "") == NANOARROW_OK &&
                    ducknng_set_arrow_schema_type(child_type, schema->children[i], errmsg) == 0;
                if (child_name) duckdb_free(child_name);
                if (child_type) duckdb_destroy_logical_type(&child_type);
                if (!child_ok) return -1;
            }
            return 0;
        }
        case DUCKDB_TYPE_INTERVAL:
            return ArrowSchemaSetType(schema, NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO) == NANOARROW_OK ? 0 : -1;
        default:
            if (errmsg) {
                *errmsg = ducknng_strdup(
                    "ducknng: unary exec row replies currently support BOOLEAN, numeric/date/time/timestamp/decimal scalars, "
                    "HUGEINT, UUID, TIMESTAMP_TZ, ENUM, VARCHAR, BLOB, INTERVAL, LIST, MAP, STRUCT, and UNION");
            }
            return -1;
    }
}

static int ducknng_build_result_schema(duckdb_result result, struct ArrowSchema *schema, char **errmsg) {
    idx_t ncols;
    idx_t col;
    ArrowSchemaInit(schema);
    ncols = duckdb_column_count(&result);
    if (ArrowSchemaSetTypeStruct(schema, (int64_t)ncols) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize Arrow result schema");
        return -1;
    }
    for (col = 0; col < ncols; col++) {
        duckdb_logical_type logical_type = duckdb_column_logical_type(&result, col);
        const char *name = duckdb_column_name(&result, col);
        if (ArrowSchemaSetName(schema->children[col], name ? name : "") != NANOARROW_OK) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to set Arrow result column name");
            if (logical_type) duckdb_destroy_logical_type(&logical_type);
            return -1;
        }
        if (ducknng_set_arrow_schema_type(logical_type, schema->children[col], errmsg) != 0) {
            if (logical_type) duckdb_destroy_logical_type(&logical_type);
            return -1;
        }
        if (logical_type) duckdb_destroy_logical_type(&logical_type);
    }
    return 0;
}

static int ducknng_is_valid(uint64_t *validity, idx_t row) {
    return validity ? duckdb_validity_row_is_valid(validity, row) : 1;
}

static void ducknng_format_uuid(duckdb_hugeint stored, char out[37]) {
    /* DuckDB stores UUIDs as int128 with the top bit XOR'd so signed-int sort order
     * matches lexicographic UUID order; flip it back here to recover canonical bytes. */
    uint8_t bytes[16];
    uint64_t hi = (uint64_t)stored.upper ^ 0x8000000000000000ULL;
    uint64_t lo = stored.lower;
    int i;
    for (i = 0; i < 8; i++) bytes[i] = (uint8_t)((hi >> (56 - 8 * i)) & 0xff);
    for (i = 0; i < 8; i++) bytes[8 + i] = (uint8_t)((lo >> (56 - 8 * i)) & 0xff);
    snprintf(out, 37,
        "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5],
        bytes[6], bytes[7],
        bytes[8], bytes[9],
        bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]);
}

static int ducknng_append_decimal_value(duckdb_logical_type logical_type, duckdb_vector vector,
    struct ArrowArray *array, idx_t row) {
    struct ArrowDecimal decimal;
    duckdb_type internal_type = duckdb_decimal_internal_type(logical_type);
    void *data = duckdb_vector_get_data(vector);
    ArrowDecimalInit(&decimal, 128, (int32_t)duckdb_decimal_width(logical_type),
        (int32_t)duckdb_decimal_scale(logical_type));
    switch (internal_type) {
        case DUCKDB_TYPE_SMALLINT:
            ArrowDecimalSetInt(&decimal, ((int16_t *)data)[row]);
            break;
        case DUCKDB_TYPE_INTEGER:
            ArrowDecimalSetInt(&decimal, ((int32_t *)data)[row]);
            break;
        case DUCKDB_TYPE_BIGINT:
            ArrowDecimalSetInt(&decimal, ((int64_t *)data)[row]);
            break;
        case DUCKDB_TYPE_HUGEINT: {
            duckdb_hugeint value = ((duckdb_hugeint *)data)[row];
            decimal.words[decimal.low_word_index] = value.lower;
            decimal.words[decimal.high_word_index] = (uint64_t)value.upper;
            break;
        }
        default:
            return -1;
    }
    return ArrowArrayAppendDecimal(array, &decimal) == NANOARROW_OK ? 0 : -1;
}

static int ducknng_append_vector_value(duckdb_logical_type logical_type, duckdb_type type_id,
    duckdb_vector vector, struct ArrowArray *array, idx_t row, char **errmsg) {
    uint64_t *validity = duckdb_vector_get_validity(vector);
    void *data = duckdb_vector_get_data(vector);
    if (!ducknng_is_valid(validity, row)) {
        return ArrowArrayAppendNull(array, 1) == NANOARROW_OK ? 0 : -1;
    }
    switch (type_id) {
        case DUCKDB_TYPE_BOOLEAN:
            return ArrowArrayAppendInt(array, ((bool *)data)[row] ? 1 : 0) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_TINYINT:
            return ArrowArrayAppendInt(array, ((int8_t *)data)[row]) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_SMALLINT:
            return ArrowArrayAppendInt(array, ((int16_t *)data)[row]) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_INTEGER:
            return ArrowArrayAppendInt(array, ((int32_t *)data)[row]) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_BIGINT:
            return ArrowArrayAppendInt(array, ((int64_t *)data)[row]) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_UTINYINT:
            return ArrowArrayAppendUInt(array, ((uint8_t *)data)[row]) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_USMALLINT:
            return ArrowArrayAppendUInt(array, ((uint16_t *)data)[row]) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_UINTEGER:
            return ArrowArrayAppendUInt(array, ((uint32_t *)data)[row]) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_UBIGINT:
            return ArrowArrayAppendUInt(array, ((uint64_t *)data)[row]) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_FLOAT:
            return ArrowArrayAppendDouble(array, ((float *)data)[row]) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_DOUBLE:
            return ArrowArrayAppendDouble(array, ((double *)data)[row]) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_VARCHAR: {
            duckdb_string_t *strings = (duckdb_string_t *)data;
            struct ArrowStringView value;
            value.data = duckdb_string_t_data(&strings[row]);
            value.size_bytes = (int64_t)duckdb_string_t_length(strings[row]);
            return ArrowArrayAppendString(array, value) == NANOARROW_OK ? 0 : -1;
        }
        case DUCKDB_TYPE_BLOB: {
            duckdb_string_t *strings = (duckdb_string_t *)data;
            struct ArrowBufferView value;
            value.data.data = (const uint8_t *)duckdb_string_t_data(&strings[row]);
            value.size_bytes = (int64_t)duckdb_string_t_length(strings[row]);
            return ArrowArrayAppendBytes(array, value) == NANOARROW_OK ? 0 : -1;
        }
        case DUCKDB_TYPE_DATE:
            return ArrowArrayAppendInt(array, ((duckdb_date *)data)[row].days) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_TIME:
            return ArrowArrayAppendInt(array, ((duckdb_time *)data)[row].micros) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_TIME_NS:
            return ArrowArrayAppendInt(array, ((duckdb_time_ns *)data)[row].nanos) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_TIMESTAMP:
            return ArrowArrayAppendInt(array, ((duckdb_timestamp *)data)[row].micros) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_TIMESTAMP_S:
            return ArrowArrayAppendInt(array, ((duckdb_timestamp_s *)data)[row].seconds) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_TIMESTAMP_MS:
            return ArrowArrayAppendInt(array, ((duckdb_timestamp_ms *)data)[row].millis) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_TIMESTAMP_NS:
            return ArrowArrayAppendInt(array, ((duckdb_timestamp_ns *)data)[row].nanos) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_DECIMAL:
            return ducknng_append_decimal_value(logical_type, vector, array, row);
        case DUCKDB_TYPE_HUGEINT: {
            struct ArrowDecimal decimal;
            duckdb_hugeint value = ((duckdb_hugeint *)data)[row];
            ArrowDecimalInit(&decimal, 128, 38, 0);
            decimal.words[decimal.low_word_index] = value.lower;
            decimal.words[decimal.high_word_index] = (uint64_t)value.upper;
            return ArrowArrayAppendDecimal(array, &decimal) == NANOARROW_OK ? 0 : -1;
        }
        case DUCKDB_TYPE_UUID: {
            duckdb_hugeint stored = ((duckdb_hugeint *)data)[row];
            char buf[37];
            struct ArrowStringView view;
            ducknng_format_uuid(stored, buf);
            view.data = buf;
            view.size_bytes = 36;
            return ArrowArrayAppendString(array, view) == NANOARROW_OK ? 0 : -1;
        }
        case DUCKDB_TYPE_TIMESTAMP_TZ:
            return ArrowArrayAppendInt(array, ((duckdb_timestamp *)data)[row].micros) == NANOARROW_OK ? 0 : -1;
        case DUCKDB_TYPE_ENUM: {
            duckdb_type internal = duckdb_enum_internal_type(logical_type);
            uint64_t idx;
            char *dict;
            struct ArrowStringView view;
            int rc;
            switch (internal) {
                case DUCKDB_TYPE_UTINYINT:  idx = ((uint8_t *)data)[row]; break;
                case DUCKDB_TYPE_USMALLINT: idx = ((uint16_t *)data)[row]; break;
                case DUCKDB_TYPE_UINTEGER:  idx = ((uint32_t *)data)[row]; break;
                default:
                    if (errmsg) *errmsg = ducknng_strdup("ducknng: unsupported ENUM internal type");
                    return -1;
            }
            dict = duckdb_enum_dictionary_value(logical_type, (idx_t)idx);
            if (!dict) {
                if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to resolve ENUM dictionary value");
                return -1;
            }
            view.data = dict;
            view.size_bytes = (int64_t)strlen(dict);
            rc = ArrowArrayAppendString(array, view) == NANOARROW_OK ? 0 : -1;
            duckdb_free(dict);
            return rc;
        }
        case DUCKDB_TYPE_MAP: {
            duckdb_list_entry *entries = (duckdb_list_entry *)data;
            duckdb_logical_type key_type = duckdb_map_type_key_type(logical_type);
            duckdb_logical_type value_type = duckdb_map_type_value_type(logical_type);
            duckdb_type key_type_id = key_type ? duckdb_get_type_id(key_type) : DUCKDB_TYPE_INVALID;
            duckdb_type value_type_id = value_type ? duckdb_get_type_id(value_type) : DUCKDB_TYPE_INVALID;
            duckdb_vector entries_vector = duckdb_list_vector_get_child(vector);
            duckdb_vector key_vector = entries_vector ? duckdb_struct_vector_get_child(entries_vector, 0) : NULL;
            duckdb_vector value_vector = entries_vector ? duckdb_struct_vector_get_child(entries_vector, 1) : NULL;
            uint64_t offset = entries[row].offset;
            uint64_t length = entries[row].length;
            uint64_t j;
            int ok = 1;
            if (!key_type || !value_type || !key_vector || !value_vector) ok = 0;
            for (j = 0; ok && j < length; j++) {
                if (ducknng_append_vector_value(key_type, key_type_id, key_vector,
                        array->children[0]->children[0], (idx_t)(offset + j), errmsg) != 0) ok = 0;
                if (ok && ducknng_append_vector_value(value_type, value_type_id, value_vector,
                        array->children[0]->children[1], (idx_t)(offset + j), errmsg) != 0) ok = 0;
                if (ok && ArrowArrayFinishElement(array->children[0]) != NANOARROW_OK) ok = 0;
            }
            if (key_type) duckdb_destroy_logical_type(&key_type);
            if (value_type) duckdb_destroy_logical_type(&value_type);
            if (!ok) return -1;
            return ArrowArrayFinishElement(array) == NANOARROW_OK ? 0 : -1;
        }
        case DUCKDB_TYPE_UNION: {
            /* DuckDB stores UNION as a struct vector: child 0 is the UTINYINT tag,
             * children 1..N are the variant values. Read the tag, append the active
             * member to the corresponding Arrow dense union child, and finalize. */
            idx_t nmembers = duckdb_union_type_member_count(logical_type);
            duckdb_vector tag_vector = duckdb_struct_vector_get_child(vector, 0);
            uint8_t *tags = tag_vector ? (uint8_t *)duckdb_vector_get_data(tag_vector) : NULL;
            uint8_t tag;
            duckdb_logical_type member_type;
            duckdb_type member_type_id;
            duckdb_vector member_vector;
            idx_t i;
            int member_ok;
            if (!tags || nmembers == 0) {
                if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to read UNION tag vector");
                return -1;
            }
            tag = tags[row];
            if ((idx_t)tag >= nmembers) {
                if (errmsg) *errmsg = ducknng_strdup("ducknng: UNION tag is out of range");
                return -1;
            }
            member_type = duckdb_union_type_member_type(logical_type, (idx_t)tag);
            member_type_id = member_type ? duckdb_get_type_id(member_type) : DUCKDB_TYPE_INVALID;
            member_vector = duckdb_struct_vector_get_child(vector, (idx_t)tag + 1);
            member_ok = member_type && member_vector &&
                ducknng_append_vector_value(member_type, member_type_id, member_vector,
                    array->children[tag], row, errmsg) == 0;
            if (member_type) duckdb_destroy_logical_type(&member_type);
            if (!member_ok) return -1;
            /* Dense union: bump non-active children once so their lengths stay aligned
             * with the union's logical length expectations on the writer side. */
            for (i = 0; i < nmembers; i++) {
                if (i == (idx_t)tag) continue;
                if (ArrowArrayAppendNull(array->children[i], 0) != NANOARROW_OK) {
                    /* Non-fatal: dense unions don't actually require parallel growth. */
                }
            }
            return ArrowArrayFinishUnionElement(array, (int8_t)tag) == NANOARROW_OK ? 0 : -1;
        }
        case DUCKDB_TYPE_LIST: {
            duckdb_list_entry *entries = (duckdb_list_entry *)data;
            duckdb_logical_type child_type = duckdb_list_type_child_type(logical_type);
            duckdb_type child_type_id = child_type ? duckdb_get_type_id(child_type) : DUCKDB_TYPE_INVALID;
            duckdb_vector child_vector = duckdb_list_vector_get_child(vector);
            uint64_t offset = entries[row].offset;
            uint64_t length = entries[row].length;
            uint64_t j;
            if (!child_type) return -1;
            for (j = 0; j < length; j++) {
                if (ducknng_append_vector_value(child_type, child_type_id, child_vector,
                        array->children[0], (idx_t)(offset + j), errmsg) != 0) {
                    duckdb_destroy_logical_type(&child_type);
                    return -1;
                }
            }
            duckdb_destroy_logical_type(&child_type);
            return ArrowArrayFinishElement(array) == NANOARROW_OK ? 0 : -1;
        }
        case DUCKDB_TYPE_STRUCT: {
            idx_t nchildren = duckdb_struct_type_child_count(logical_type);
            idx_t i;
            for (i = 0; i < nchildren; i++) {
                duckdb_logical_type child_type = duckdb_struct_type_child_type(logical_type, i);
                duckdb_type child_type_id = child_type ? duckdb_get_type_id(child_type) : DUCKDB_TYPE_INVALID;
                duckdb_vector child_vector = duckdb_struct_vector_get_child(vector, i);
                int ok = child_type && ducknng_append_vector_value(child_type, child_type_id,
                    child_vector, array->children[i], row, errmsg) == 0;
                if (child_type) duckdb_destroy_logical_type(&child_type);
                if (!ok) return -1;
            }
            return ArrowArrayFinishElement(array) == NANOARROW_OK ? 0 : -1;
        }
        case DUCKDB_TYPE_INTERVAL: {
            duckdb_interval iv = ((duckdb_interval *)data)[row];
            struct ArrowInterval interval;
            interval.type = NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO;
            interval.months = iv.months;
            interval.days   = iv.days;
            interval.ns     = iv.micros * 1000LL;
            interval.ms     = 0;
            return ArrowArrayAppendInterval(array, &interval) == NANOARROW_OK ? 0 : -1;
        }
        default:
            if (errmsg) {
                *errmsg = ducknng_strdup(
                    "ducknng: encountered unsupported DuckDB type while encoding unary row reply "
                    "(BOOLEAN, numeric/date/time/timestamp/decimal scalars, HUGEINT, UUID, "
                    "TIMESTAMP_TZ, ENUM, VARCHAR, BLOB, INTERVAL, LIST, MAP, STRUCT, UNION are supported)");
            }
            return -1;
    }
}

static int ducknng_append_chunk_to_arrow(duckdb_data_chunk chunk, struct ArrowArray *root,
    char **errmsg) {
    idx_t ncols;
    idx_t nrows;
    idx_t col;
    idx_t row;
    duckdb_vector *vectors = NULL;
    duckdb_logical_type *logical_types = NULL;
    duckdb_type *types = NULL;
    ncols = duckdb_data_chunk_get_column_count(chunk);
    nrows = duckdb_data_chunk_get_size(chunk);
    if (ncols == 0) return 0;
    vectors = (duckdb_vector *)duckdb_malloc(sizeof(*vectors) * (size_t)ncols);
    logical_types = (duckdb_logical_type *)duckdb_malloc(sizeof(*logical_types) * (size_t)ncols);
    types = (duckdb_type *)duckdb_malloc(sizeof(*types) * (size_t)ncols);
    if (!vectors || !logical_types || !types) {
        if (vectors) duckdb_free(vectors);
        if (logical_types) duckdb_free(logical_types);
        if (types) duckdb_free(types);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory preparing result chunk encoding");
        return -1;
    }
    memset(logical_types, 0, sizeof(*logical_types) * (size_t)ncols);
    for (col = 0; col < ncols; col++) {
        vectors[col] = duckdb_data_chunk_get_vector(chunk, col);
        logical_types[col] = duckdb_vector_get_column_type(vectors[col]);
        types[col] = logical_types[col] ? duckdb_get_type_id(logical_types[col]) : DUCKDB_TYPE_INVALID;
    }
    for (row = 0; row < nrows; row++) {
        for (col = 0; col < ncols; col++) {
            if (ducknng_append_vector_value(logical_types[col], types[col], vectors[col], root->children[col], row, errmsg) != 0) {
                if ((!errmsg || !*errmsg) && errmsg) {
                    *errmsg = ducknng_strdup("ducknng: failed to append DuckDB result value to Arrow array");
                }
                for (col = 0; col < ncols; col++) if (logical_types[col]) duckdb_destroy_logical_type(&logical_types[col]);
                duckdb_free(vectors);
                duckdb_free(logical_types);
                duckdb_free(types);
                return -1;
            }
        }
        if (ArrowArrayFinishElement(root) != NANOARROW_OK) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to finalize Arrow row element");
            for (col = 0; col < ncols; col++) if (logical_types[col]) duckdb_destroy_logical_type(&logical_types[col]);
            duckdb_free(vectors);
            duckdb_free(logical_types);
            duckdb_free(types);
            return -1;
        }
    }
    for (col = 0; col < ncols; col++) if (logical_types[col]) duckdb_destroy_logical_type(&logical_types[col]);
    duckdb_free(vectors);
    duckdb_free(logical_types);
    duckdb_free(types);
    return 0;
}

static int ducknng_arrow_stream_to_bytes(struct ArrowSchema *schema, struct ArrowArray *array,
    uint8_t **out_bytes, size_t *out_len, char **errmsg) {
    struct ArrowArrayStream stream;
    struct ArrowIpcOutputStream output_stream;
    struct ArrowIpcWriter writer;
    struct ArrowBuffer output_buf;
    struct ArrowError error;
    uint8_t *copy = NULL;
    int rc = -1;

    memset(&stream, 0, sizeof(stream));
    memset(&output_stream, 0, sizeof(output_stream));
    memset(&writer, 0, sizeof(writer));
    memset(&output_buf, 0, sizeof(output_buf));
    memset(&error, 0, sizeof(error));

    if (ArrowBasicArrayStreamInit(&stream, schema, 1) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize Arrow result stream");
        return -1;
    }
    memset(schema, 0, sizeof(*schema));
    ArrowBasicArrayStreamSetArray(&stream, 0, array);
    memset(array, 0, sizeof(*array));

    ArrowBufferInit(&output_buf);
    if (ArrowIpcOutputStreamInitBuffer(&output_stream, &output_buf) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize Arrow result IPC output stream");
        goto cleanup;
    }
    if (ArrowIpcWriterInit(&writer, &output_stream) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize Arrow result IPC writer");
        goto cleanup;
    }
    if (ArrowIpcWriterWriteArrayStream(&writer, &stream, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        goto cleanup;
    }
    copy = (uint8_t *)duckdb_malloc((size_t)output_buf.size_bytes);
    if (!copy) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying Arrow result payload");
        goto cleanup;
    }
    memcpy(copy, output_buf.data, (size_t)output_buf.size_bytes);
    *out_bytes = copy;
    *out_len = (size_t)output_buf.size_bytes;
    copy = NULL;
    rc = 0;

cleanup:
    if (copy) duckdb_free(copy);
    if (writer.private_data) ArrowIpcWriterReset(&writer);
    if (output_stream.release) output_stream.release(&output_stream);
    if (output_buf.data) ArrowBufferReset(&output_buf);
    if (stream.release) ArrowArrayStreamRelease(&stream);
    if (array->release) ArrowArrayRelease(array);
    if (schema->release) ArrowSchemaRelease(schema);
    return rc;
}

int ducknng_exec_request_to_ipc(const char *sql, int want_result,
    uint8_t **out_bytes, size_t *out_len, char **errmsg) {
    struct ArrowSchema schema;
    struct ArrowArray array;
    struct ArrowArrayStream stream;
    struct ArrowIpcOutputStream output_stream;
    struct ArrowIpcWriter writer;
    struct ArrowBuffer output_buf;
    struct ArrowError error;
    struct ArrowStringView sql_view;
    uint8_t *copy = NULL;
    int rc = -1;
    memset(&schema, 0, sizeof(schema));
    memset(&array, 0, sizeof(array));
    memset(&stream, 0, sizeof(stream));
    memset(&output_stream, 0, sizeof(output_stream));
    memset(&writer, 0, sizeof(writer));
    memset(&output_buf, 0, sizeof(output_buf));
    memset(&error, 0, sizeof(error));
    if (!sql || !out_bytes || !out_len) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid exec request arguments");
        return -1;
    }
    if (ducknng_build_exec_request_schema(&schema, &error) != 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to build exec request Arrow schema");
        goto cleanup;
    }
    if (ArrowArrayInitFromSchema(&array, &schema, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        goto cleanup;
    }
    if (ArrowArrayStartAppending(&array) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to start exec request Arrow append");
        goto cleanup;
    }
    sql_view.data = sql;
    sql_view.size_bytes = (int64_t)strlen(sql);
    if (ArrowArrayAppendString(array.children[0], sql_view) != NANOARROW_OK ||
        ArrowArrayAppendInt(array.children[1], want_result ? 1 : 0) != NANOARROW_OK ||
        ArrowArrayFinishElement(&array) != NANOARROW_OK ||
        ArrowArrayFinishBuildingDefault(&array, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message[0] ? error.message :
            "ducknng: failed to build exec request Arrow payload");
        goto cleanup;
    }
    if (ArrowBasicArrayStreamInit(&stream, &schema, 1) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize exec request Arrow stream");
        goto cleanup;
    }
    memset(&schema, 0, sizeof(schema));
    ArrowBasicArrayStreamSetArray(&stream, 0, &array);
    memset(&array, 0, sizeof(array));
    ArrowBufferInit(&output_buf);
    if (ArrowIpcOutputStreamInitBuffer(&output_stream, &output_buf) != NANOARROW_OK ||
        ArrowIpcWriterInit(&writer, &output_stream) != NANOARROW_OK ||
        ArrowIpcWriterWriteArrayStream(&writer, &stream, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message[0] ? error.message :
            "ducknng: failed to encode exec request Arrow IPC payload");
        goto cleanup;
    }
    copy = (uint8_t *)duckdb_malloc((size_t)output_buf.size_bytes);
    if (!copy) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying exec request payload");
        goto cleanup;
    }
    memcpy(copy, output_buf.data, (size_t)output_buf.size_bytes);
    *out_bytes = copy;
    *out_len = (size_t)output_buf.size_bytes;
    copy = NULL;
    rc = 0;
cleanup:
    if (copy) duckdb_free(copy);
    if (writer.private_data) ArrowIpcWriterReset(&writer);
    if (output_stream.release) output_stream.release(&output_stream);
    if (output_buf.data) ArrowBufferReset(&output_buf);
    if (stream.release) ArrowArrayStreamRelease(&stream);
    if (array.release) ArrowArrayRelease(&array);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}

int ducknng_query_open_request_to_ipc(const char *sql, uint64_t batch_rows,
    uint64_t batch_bytes, uint8_t **out_bytes, size_t *out_len, char **errmsg) {
    struct ArrowSchema schema;
    struct ArrowArray array;
    struct ArrowArrayStream stream;
    struct ArrowIpcOutputStream output_stream;
    struct ArrowIpcWriter writer;
    struct ArrowBuffer output_buf;
    struct ArrowError error;
    struct ArrowStringView sql_view;
    uint8_t *copy = NULL;
    int rc = -1;
    memset(&schema, 0, sizeof(schema));
    memset(&array, 0, sizeof(array));
    memset(&stream, 0, sizeof(stream));
    memset(&output_stream, 0, sizeof(output_stream));
    memset(&writer, 0, sizeof(writer));
    memset(&output_buf, 0, sizeof(output_buf));
    memset(&error, 0, sizeof(error));
    if (!sql || !sql[0] || !out_bytes || !out_len) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid query_open request arguments");
        return -1;
    }
    if (ducknng_build_query_open_request_schema(&schema, &error) != 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to build query_open request Arrow schema");
        goto cleanup;
    }
    if (ArrowArrayInitFromSchema(&array, &schema, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        goto cleanup;
    }
    if (ArrowArrayStartAppending(&array) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to start query_open request Arrow append");
        goto cleanup;
    }
    sql_view.data = sql;
    sql_view.size_bytes = (int64_t)strlen(sql);
    if (ArrowArrayAppendString(array.children[0], sql_view) != NANOARROW_OK ||
        (batch_rows > 0 ? ArrowArrayAppendUInt(array.children[1], batch_rows) : ArrowArrayAppendNull(array.children[1], 1)) != NANOARROW_OK ||
        (batch_bytes > 0 ? ArrowArrayAppendUInt(array.children[2], batch_bytes) : ArrowArrayAppendNull(array.children[2], 1)) != NANOARROW_OK ||
        ArrowArrayFinishElement(&array) != NANOARROW_OK ||
        ArrowArrayFinishBuildingDefault(&array, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message[0] ? error.message :
            "ducknng: failed to build query_open request Arrow payload");
        goto cleanup;
    }
    if (ArrowBasicArrayStreamInit(&stream, &schema, 1) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize query_open request Arrow stream");
        goto cleanup;
    }
    memset(&schema, 0, sizeof(schema));
    ArrowBasicArrayStreamSetArray(&stream, 0, &array);
    memset(&array, 0, sizeof(array));
    ArrowBufferInit(&output_buf);
    if (ArrowIpcOutputStreamInitBuffer(&output_stream, &output_buf) != NANOARROW_OK ||
        ArrowIpcWriterInit(&writer, &output_stream) != NANOARROW_OK ||
        ArrowIpcWriterWriteArrayStream(&writer, &stream, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message[0] ? error.message :
            "ducknng: failed to encode query_open request Arrow IPC payload");
        goto cleanup;
    }
    copy = (uint8_t *)duckdb_malloc((size_t)output_buf.size_bytes);
    if (!copy) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying query_open request payload");
        goto cleanup;
    }
    memcpy(copy, output_buf.data, (size_t)output_buf.size_bytes);
    *out_bytes = copy;
    *out_len = (size_t)output_buf.size_bytes;
    copy = NULL;
    rc = 0;
cleanup:
    if (copy) duckdb_free(copy);
    if (writer.private_data) ArrowIpcWriterReset(&writer);
    if (output_stream.release) output_stream.release(&output_stream);
    if (output_buf.data) ArrowBufferReset(&output_buf);
    if (stream.release) ArrowArrayStreamRelease(&stream);
    if (array.release) ArrowArrayRelease(&array);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}

char *ducknng_session_request_json(uint64_t session_id, const char *session_token,
    uint64_t batch_rows, uint64_t batch_bytes) {
    char buf[320];
    int n;
    if (session_id == 0 || !session_token || !session_token[0]) return NULL;
    n = snprintf(buf, sizeof(buf), "{\"session_id\":%llu,\"session_token\":\"%s\"",
        (unsigned long long)session_id, session_token);
    if (n < 0 || (size_t)n >= sizeof(buf)) return NULL;
    if (batch_rows > 0) {
        n += snprintf(buf + n, sizeof(buf) - (size_t)n, ",\"batch_rows\":%llu",
            (unsigned long long)batch_rows);
        if ((size_t)n >= sizeof(buf)) return NULL;
    }
    if (batch_bytes > 0) {
        n += snprintf(buf + n, sizeof(buf) - (size_t)n, ",\"batch_bytes\":%llu",
            (unsigned long long)batch_bytes);
        if ((size_t)n >= sizeof(buf)) return NULL;
    }
    n += snprintf(buf + n, sizeof(buf) - (size_t)n, "}");
    if ((size_t)n >= sizeof(buf)) return NULL;
    return ducknng_strdup(buf);
}

int ducknng_query_to_ipc_stream(duckdb_connection con, const char *sql,
    uint8_t **out_bytes, size_t *out_len, char **errmsg) {
    duckdb_result result;
    int rc;
    memset(&result, 0, sizeof(result));
    if (!con || !sql || !sql[0]) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid query_to_ipc_stream arguments");
        return -1;
    }
    if (duckdb_query(con, sql, &result) == DuckDBError) {
        const char *detail = duckdb_result_error(&result);
        if (errmsg) *errmsg = ducknng_strdup(detail && detail[0] ? detail : "ducknng: query failed");
        duckdb_destroy_result(&result);
        return -1;
    }
    if (duckdb_result_return_type(result) != DUCKDB_RESULT_TYPE_QUERY_RESULT) {
        duckdb_destroy_result(&result);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: query_to_ipc_stream requires a row-returning query");
        return -1;
    }
    rc = ducknng_result_to_ipc_stream(NULL, result, out_bytes, out_len, errmsg);
    duckdb_destroy_result(&result);
    return rc;
}

int ducknng_result_next_chunk_to_ipc(duckdb_result result,
    uint8_t **out_bytes, size_t *out_len, int *has_chunk, char **errmsg) {
    struct ArrowSchema schema;
    struct ArrowArray array;
    struct ArrowError error;
    duckdb_data_chunk chunk = NULL;
    memset(&schema, 0, sizeof(schema));
    memset(&array, 0, sizeof(array));
    memset(&error, 0, sizeof(error));
    if (has_chunk) *has_chunk = 0;
    if (!out_bytes || !out_len) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid next chunk output pointers");
        return -1;
    }
    chunk = duckdb_fetch_chunk(result);
    if (!chunk) {
        if (has_chunk) *has_chunk = 0;
        return 0;
    }
    if (ducknng_build_result_schema(result, &schema, errmsg) != 0) goto cleanup;
    if (ArrowArrayInitFromSchema(&array, &schema, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message[0] ? error.message : "ducknng: failed to initialize Arrow chunk array");
        goto cleanup;
    }
    if (ArrowArrayStartAppending(&array) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to start Arrow chunk append");
        goto cleanup;
    }
    if (ducknng_append_chunk_to_arrow(chunk, &array, errmsg) != 0) goto cleanup;
    if (ArrowArrayFinishBuildingDefault(&array, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message[0] ? error.message : "ducknng: failed to finalize Arrow chunk array");
        goto cleanup;
    }
    if (ducknng_arrow_stream_to_bytes(&schema, &array, out_bytes, out_len, errmsg) != 0) goto cleanup;
    if (has_chunk) *has_chunk = 1;
    duckdb_destroy_data_chunk(&chunk);
    return 0;
cleanup:
    if (chunk) duckdb_destroy_data_chunk(&chunk);
    if (array.release) ArrowArrayRelease(&array);
    if (schema.release) ArrowSchemaRelease(&schema);
    return -1;
}

int ducknng_result_to_ipc_stream(duckdb_prepared_statement stmt, duckdb_result result,
    uint8_t **out_bytes, size_t *out_len, char **errmsg) {
    struct ArrowSchema schema;
    struct ArrowArray array;
    struct ArrowError error;
    duckdb_data_chunk chunk = NULL;
    (void)stmt;

    memset(&schema, 0, sizeof(schema));
    memset(&array, 0, sizeof(array));
    memset(&error, 0, sizeof(error));
    if (!out_bytes || !out_len) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid result output pointers");
        return -1;
    }
    if (ducknng_build_result_schema(result, &schema, errmsg) != 0) goto cleanup;
    if (ArrowArrayInitFromSchema(&array, &schema, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        goto cleanup;
    }
    if (ArrowArrayStartAppending(&array) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to start Arrow result append");
        goto cleanup;
    }
    for (;;) {
        chunk = duckdb_fetch_chunk(result);
        if (!chunk) break;
        if (ducknng_append_chunk_to_arrow(chunk, &array, errmsg) != 0) {
            duckdb_destroy_data_chunk(&chunk);
            goto cleanup;
        }
        duckdb_destroy_data_chunk(&chunk);
    }
    if (ArrowArrayFinishBuildingDefault(&array, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message[0] ? error.message :
            "ducknng: failed to finalize Arrow result array");
        goto cleanup;
    }
    return ducknng_arrow_stream_to_bytes(&schema, &array, out_bytes, out_len, errmsg);

cleanup:
    if (chunk) duckdb_destroy_data_chunk(&chunk);
    if (array.release) ArrowArrayRelease(&array);
    if (schema.release) ArrowSchemaRelease(&schema);
    return -1;
}

int ducknng_exec_metadata_to_ipc(uint64_t rows_changed,
    uint32_t statement_type, uint32_t result_type, uint8_t **out_bytes,
    size_t *out_len, char **errmsg) {
    struct ArrowSchema schema;
    struct ArrowArray array;
    struct ArrowArrayStream stream;
    struct ArrowIpcOutputStream output_stream;
    struct ArrowIpcWriter writer;
    struct ArrowBuffer output_buf;
    struct ArrowError error;
    uint8_t *copy = NULL;
    int rc = -1;

    memset(&schema, 0, sizeof(schema));
    memset(&array, 0, sizeof(array));
    memset(&stream, 0, sizeof(stream));
    memset(&output_stream, 0, sizeof(output_stream));
    memset(&writer, 0, sizeof(writer));
    memset(&output_buf, 0, sizeof(output_buf));
    memset(&error, 0, sizeof(error));

    if (!out_bytes || !out_len) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid metadata output pointers");
        return -1;
    }
    if (ducknng_build_metadata_schema(&schema, &error) != 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to build metadata Arrow schema");
        goto cleanup;
    }
    if (ArrowArrayInitFromSchema(&array, &schema, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        goto cleanup;
    }
    if (ArrowArrayStartAppending(&array) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to start metadata Arrow array append");
        goto cleanup;
    }
    if (ArrowArrayAppendUInt(array.children[0], rows_changed) != NANOARROW_OK ||
        ArrowArrayAppendInt(array.children[1], (int64_t)statement_type) != NANOARROW_OK ||
        ArrowArrayAppendInt(array.children[2], (int64_t)result_type) != NANOARROW_OK ||
        ArrowArrayFinishElement(&array) != NANOARROW_OK ||
        ArrowArrayFinishBuildingDefault(&array, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message[0] ? error.message :
            "ducknng: failed to build metadata Arrow array");
        goto cleanup;
    }
    if (ArrowBasicArrayStreamInit(&stream, &schema, 1) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize metadata Arrow stream");
        goto cleanup;
    }
    memset(&schema, 0, sizeof(schema));
    ArrowBasicArrayStreamSetArray(&stream, 0, &array);
    memset(&array, 0, sizeof(array));
    ArrowBufferInit(&output_buf);
    if (ArrowIpcOutputStreamInitBuffer(&output_stream, &output_buf) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize metadata Arrow IPC output stream");
        goto cleanup;
    }
    if (ArrowIpcWriterInit(&writer, &output_stream) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize metadata Arrow IPC writer");
        goto cleanup;
    }
    if (ArrowIpcWriterWriteArrayStream(&writer, &stream, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        goto cleanup;
    }
    copy = (uint8_t *)duckdb_malloc((size_t)output_buf.size_bytes);
    if (!copy) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying metadata Arrow IPC payload");
        goto cleanup;
    }
    memcpy(copy, output_buf.data, (size_t)output_buf.size_bytes);
    *out_bytes = copy;
    *out_len = (size_t)output_buf.size_bytes;
    copy = NULL;
    rc = 0;

cleanup:
    if (copy) duckdb_free(copy);
    if (writer.private_data) ArrowIpcWriterReset(&writer);
    if (output_stream.release) output_stream.release(&output_stream);
    if (output_buf.data) ArrowBufferReset(&output_buf);
    if (stream.release) ArrowArrayStreamRelease(&stream);
    if (array.release) ArrowArrayRelease(&array);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}
