#include "ducknng_quack.h"
#include "ducknng_duckdb_streaming_compat.h"
#include "ducknng_util.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

#define DUCKNNG_QUACK_FIELD_END 0xffffu
#define DUCKNNG_QUACK_OUTER_RESULT_TYPES 1u
#define DUCKNNG_QUACK_OUTER_RESULT_NAMES 2u
#define DUCKNNG_QUACK_OUTER_RESULTS 4u
#define DUCKNNG_QUACK_TYPE_ID 100u
#define DUCKNNG_QUACK_TYPE_INFO 101u
#define DUCKNNG_QUACK_EXTRA_INFO_KIND 100u
#define DUCKNNG_QUACK_EXTRA_DECIMAL_WIDTH 200u
#define DUCKNNG_QUACK_EXTRA_DECIMAL_SCALE 201u
#define DUCKNNG_QUACK_CHUNK_WRAPPER 300u
#define DUCKNNG_QUACK_CHUNK_ROWS 100u
#define DUCKNNG_QUACK_CHUNK_TYPES 101u
#define DUCKNNG_QUACK_CHUNK_COLUMNS 102u
#define DUCKNNG_QUACK_VECTOR_TYPE 90u
#define DUCKNNG_QUACK_VECTOR_HAS_VALIDITY 100u
#define DUCKNNG_QUACK_VECTOR_VALIDITY 101u
#define DUCKNNG_QUACK_VECTOR_DATA 102u
#define DUCKNNG_QUACK_EXTRA_TYPE_DECIMAL 2u
#define DUCKNNG_QUACK_VECTOR_FLAT 0u

#define DUCKNNG_QUACK_LOGICAL_BOOLEAN 10
#define DUCKNNG_QUACK_LOGICAL_TINYINT 11
#define DUCKNNG_QUACK_LOGICAL_SMALLINT 12
#define DUCKNNG_QUACK_LOGICAL_INTEGER 13
#define DUCKNNG_QUACK_LOGICAL_BIGINT 14
#define DUCKNNG_QUACK_LOGICAL_DATE 15
#define DUCKNNG_QUACK_LOGICAL_TIME 16
#define DUCKNNG_QUACK_LOGICAL_TIMESTAMP_SEC 17
#define DUCKNNG_QUACK_LOGICAL_TIMESTAMP_MS 18
#define DUCKNNG_QUACK_LOGICAL_TIMESTAMP 19
#define DUCKNNG_QUACK_LOGICAL_TIMESTAMP_NS 20
#define DUCKNNG_QUACK_LOGICAL_DECIMAL 21
#define DUCKNNG_QUACK_LOGICAL_FLOAT 22
#define DUCKNNG_QUACK_LOGICAL_DOUBLE 23
#define DUCKNNG_QUACK_LOGICAL_CHAR 24
#define DUCKNNG_QUACK_LOGICAL_VARCHAR 25
#define DUCKNNG_QUACK_LOGICAL_BLOB 26
#define DUCKNNG_QUACK_LOGICAL_INTERVAL 27
#define DUCKNNG_QUACK_LOGICAL_UTINYINT 28
#define DUCKNNG_QUACK_LOGICAL_USMALLINT 29
#define DUCKNNG_QUACK_LOGICAL_UINTEGER 30
#define DUCKNNG_QUACK_LOGICAL_UBIGINT 31
#define DUCKNNG_QUACK_LOGICAL_TIMESTAMP_TZ 32
#define DUCKNNG_QUACK_LOGICAL_TIME_TZ 34
#define DUCKNNG_QUACK_LOGICAL_TIME_NS 35
#define DUCKNNG_QUACK_LOGICAL_UHUGEINT 49
#define DUCKNNG_QUACK_LOGICAL_HUGEINT 50
#define DUCKNNG_QUACK_LOGICAL_UUID 54

typedef struct ducknng_quack_writer {
    uint8_t *data;
    size_t len;
    size_t cap;
} ducknng_quack_writer;

typedef struct ducknng_quack_reader {
    const uint8_t *data;
    size_t len;
    size_t off;
} ducknng_quack_reader;

typedef struct ducknng_quack_type_meta {
    int logical_type_id;
    uint8_t decimal_width;
    uint8_t decimal_scale;
} ducknng_quack_type_meta;

static int ducknng_quack_vector_row_is_valid(const uint64_t *validity, idx_t row) {
    if (!validity) return 1;
    return (validity[row / 64] & (((uint64_t)1) << (row % 64))) != 0;
}

static void ducknng_quack_set_error(char **errmsg, const char *message) {
    if (!errmsg || *errmsg) return;
    *errmsg = ducknng_strdup(message ? message : "ducknng: quack codec error");
}

static int ducknng_quack_writer_reserve(ducknng_quack_writer *w, size_t add, char **errmsg) {
    uint8_t *next;
    size_t want;
    size_t cap;
    if (!w) {
        ducknng_quack_set_error(errmsg, "ducknng: quack writer missing");
        return -1;
    }
    want = w->len + add;
    if (want <= w->cap) return 0;
    cap = w->cap ? w->cap * 2 : 256;
    while (cap < want) cap *= 2;
    next = (uint8_t *)duckdb_malloc(cap);
    if (!next) {
        ducknng_quack_set_error(errmsg, "ducknng: out of memory growing quack payload buffer");
        return -1;
    }
    if (w->data && w->len) memcpy(next, w->data, w->len);
    if (w->data) duckdb_free(w->data);
    w->data = next;
    w->cap = cap;
    return 0;
}

static int ducknng_quack_write_bytes(ducknng_quack_writer *w, const void *data, size_t len, char **errmsg) {
    if (ducknng_quack_writer_reserve(w, len, errmsg) != 0) return -1;
    if (len) memcpy(w->data + w->len, data, len);
    w->len += len;
    return 0;
}

static int ducknng_quack_write_byte(ducknng_quack_writer *w, uint8_t value, char **errmsg) {
    return ducknng_quack_write_bytes(w, &value, 1, errmsg);
}

static int ducknng_quack_write_u16le(ducknng_quack_writer *w, uint16_t value, char **errmsg) {
    uint8_t buf[2];
    buf[0] = (uint8_t)(value & 0xffu);
    buf[1] = (uint8_t)((value >> 8) & 0xffu);
    return ducknng_quack_write_bytes(w, buf, sizeof(buf), errmsg);
}

static int ducknng_quack_write_field_id(ducknng_quack_writer *w, uint16_t field_id, char **errmsg) {
    return ducknng_quack_write_u16le(w, field_id, errmsg);
}

static int ducknng_quack_write_field_end(ducknng_quack_writer *w, char **errmsg) {
    return ducknng_quack_write_field_id(w, DUCKNNG_QUACK_FIELD_END, errmsg);
}

static int ducknng_quack_write_uleb128(ducknng_quack_writer *w, uint64_t value, char **errmsg) {
    do {
        uint8_t byte = (uint8_t)(value & 0x7fu);
        value >>= 7;
        if (value != 0) byte |= 0x80u;
        if (ducknng_quack_write_byte(w, byte, errmsg) != 0) return -1;
    } while (value != 0);
    return 0;
}

static int ducknng_quack_write_sleb128(ducknng_quack_writer *w, int64_t value, char **errmsg) {
    int more = 1;
    while (more) {
        uint8_t byte = (uint8_t)(value & 0x7f);
        int64_t next = value >> 7;
        int sign = (byte & 0x40u) != 0;
        more = !((next == 0 && !sign) || (next == -1 && sign));
        if (more) byte |= 0x80u;
        if (ducknng_quack_write_byte(w, byte, errmsg) != 0) return -1;
        value = next;
    }
    return 0;
}

static int ducknng_quack_write_string_len(ducknng_quack_writer *w, const uint8_t *data, size_t len, char **errmsg) {
    if (ducknng_quack_write_uleb128(w, (uint64_t)len, errmsg) != 0) return -1;
    return ducknng_quack_write_bytes(w, data, len, errmsg);
}

static int ducknng_quack_write_string(ducknng_quack_writer *w, const char *text, char **errmsg) {
    size_t len = text ? strlen(text) : 0;
    return ducknng_quack_write_string_len(w, (const uint8_t *)(text ? text : ""), len, errmsg);
}

static int ducknng_quack_write_blob(ducknng_quack_writer *w, const uint8_t *data, size_t len, char **errmsg) {
    if (ducknng_quack_write_uleb128(w, (uint64_t)len, errmsg) != 0) return -1;
    return ducknng_quack_write_bytes(w, data, len, errmsg);
}

static int ducknng_quack_reader_need(ducknng_quack_reader *r, size_t n, char **errmsg) {
    if (!r || r->off + n > r->len) {
        ducknng_quack_set_error(errmsg, "ducknng: truncated quack payload");
        return -1;
    }
    return 0;
}

static int ducknng_quack_read_byte(ducknng_quack_reader *r, uint8_t *out, char **errmsg) {
    if (ducknng_quack_reader_need(r, 1, errmsg) != 0) return -1;
    if (out) *out = r->data[r->off];
    r->off++;
    return 0;
}

static int ducknng_quack_peek_u16le(ducknng_quack_reader *r, uint16_t *out, char **errmsg) {
    if (ducknng_quack_reader_need(r, 2, errmsg) != 0) return -1;
    if (out) *out = (uint16_t)r->data[r->off] | ((uint16_t)r->data[r->off + 1] << 8);
    return 0;
}

static int ducknng_quack_read_u16le(ducknng_quack_reader *r, uint16_t *out, char **errmsg) {
    if (ducknng_quack_peek_u16le(r, out, errmsg) != 0) return -1;
    r->off += 2;
    return 0;
}

static int ducknng_quack_read_expect_field(ducknng_quack_reader *r, uint16_t expected, char **errmsg) {
    uint16_t field_id = 0;
    if (ducknng_quack_read_u16le(r, &field_id, errmsg) != 0) return -1;
    if (field_id != expected) {
        char buf[128];
        snprintf(buf, sizeof(buf), "ducknng: unexpected quack field id %u (expected %u)",
            (unsigned int)field_id, (unsigned int)expected);
        ducknng_quack_set_error(errmsg, buf);
        return -1;
    }
    return 0;
}

static int ducknng_quack_read_uleb128(ducknng_quack_reader *r, uint64_t *out, char **errmsg) {
    uint64_t value = 0;
    unsigned shift = 0;
    while (1) {
        uint8_t byte = 0;
        if (shift >= 64) {
            ducknng_quack_set_error(errmsg, "ducknng: invalid quack uleb128 integer");
            return -1;
        }
        if (ducknng_quack_read_byte(r, &byte, errmsg) != 0) return -1;
        value |= ((uint64_t)(byte & 0x7fu)) << shift;
        if ((byte & 0x80u) == 0) break;
        shift += 7;
    }
    if (out) *out = value;
    return 0;
}

static int ducknng_quack_read_blob_view(ducknng_quack_reader *r, const uint8_t **out_data,
    size_t *out_len, char **errmsg) {
    uint64_t len = 0;
    if (ducknng_quack_read_uleb128(r, &len, errmsg) != 0) return -1;
    if (ducknng_quack_reader_need(r, (size_t)len, errmsg) != 0) return -1;
    if (out_data) *out_data = r->data + r->off;
    if (out_len) *out_len = (size_t)len;
    r->off += (size_t)len;
    return 0;
}

static int ducknng_quack_read_string_dup(ducknng_quack_reader *r, char **out, char **errmsg) {
    const uint8_t *data = NULL;
    size_t len = 0;
    char *dup;
    if (out) *out = NULL;
    if (ducknng_quack_read_blob_view(r, &data, &len, errmsg) != 0) return -1;
    dup = (char *)duckdb_malloc(len + 1);
    if (!dup) {
        ducknng_quack_set_error(errmsg, "ducknng: out of memory copying quack string");
        return -1;
    }
    if (len) memcpy(dup, data, len);
    dup[len] = '\0';
    if (out) *out = dup;
    else duckdb_free(dup);
    return 0;
}

static int ducknng_quack_skip_string(ducknng_quack_reader *r, char **errmsg) {
    return ducknng_quack_read_blob_view(r, NULL, NULL, errmsg);
}

static int ducknng_quack_duckdb_type_to_meta(duckdb_logical_type type,
    ducknng_quack_type_meta *out, char **errmsg) {
    duckdb_type id;
    if (!type || !out) {
        ducknng_quack_set_error(errmsg, "ducknng: missing DuckDB logical type for quack encoding");
        return -1;
    }
    memset(out, 0, sizeof(*out));
    id = duckdb_get_type_id(type);
    switch (id) {
    case DUCKDB_TYPE_BOOLEAN: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_BOOLEAN; return 0;
    case DUCKDB_TYPE_TINYINT: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_TINYINT; return 0;
    case DUCKDB_TYPE_SMALLINT: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_SMALLINT; return 0;
    case DUCKDB_TYPE_INTEGER: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_INTEGER; return 0;
    case DUCKDB_TYPE_BIGINT: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_BIGINT; return 0;
    case DUCKDB_TYPE_DATE: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_DATE; return 0;
    case DUCKDB_TYPE_TIME: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_TIME; return 0;
    case DUCKDB_TYPE_TIMESTAMP_S: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_TIMESTAMP_SEC; return 0;
    case DUCKDB_TYPE_TIMESTAMP_MS: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_TIMESTAMP_MS; return 0;
    case DUCKDB_TYPE_TIMESTAMP: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_TIMESTAMP; return 0;
    case DUCKDB_TYPE_TIMESTAMP_NS: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_TIMESTAMP_NS; return 0;
    case DUCKDB_TYPE_DECIMAL:
        out->logical_type_id = DUCKNNG_QUACK_LOGICAL_DECIMAL;
        out->decimal_width = duckdb_decimal_width(type);
        out->decimal_scale = duckdb_decimal_scale(type);
        return 0;
    case DUCKDB_TYPE_FLOAT: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_FLOAT; return 0;
    case DUCKDB_TYPE_DOUBLE: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_DOUBLE; return 0;
    case DUCKDB_TYPE_VARCHAR: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_VARCHAR; return 0;
    case DUCKDB_TYPE_BLOB: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_BLOB; return 0;
    case DUCKDB_TYPE_INTERVAL: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_INTERVAL; return 0;
    case DUCKDB_TYPE_UTINYINT: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_UTINYINT; return 0;
    case DUCKDB_TYPE_USMALLINT: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_USMALLINT; return 0;
    case DUCKDB_TYPE_UINTEGER: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_UINTEGER; return 0;
    case DUCKDB_TYPE_UBIGINT: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_UBIGINT; return 0;
    case DUCKDB_TYPE_TIMESTAMP_TZ: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_TIMESTAMP_TZ; return 0;
    case DUCKDB_TYPE_TIME_TZ: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_TIME_TZ; return 0;
    case DUCKDB_TYPE_TIME_NS: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_TIME_NS; return 0;
    case DUCKDB_TYPE_UHUGEINT: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_UHUGEINT; return 0;
    case DUCKDB_TYPE_HUGEINT: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_HUGEINT; return 0;
    case DUCKDB_TYPE_UUID: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_UUID; return 0;
    default:
        ducknng_quack_set_error(errmsg,
            "ducknng: ducknng_quack_batch currently supports scalar bool/integer/float/date/time/timestamp/decimal/varchar/blob/interval/hugeint/uuid columns only");
        return -1;
    }
}

static int ducknng_quack_meta_to_duckdb_type(const ducknng_quack_type_meta *meta,
    duckdb_logical_type *out_type, char **errmsg) {
    if (out_type) *out_type = NULL;
    if (!meta || !out_type) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack logical type metadata");
        return -1;
    }
    switch (meta->logical_type_id) {
    case DUCKNNG_QUACK_LOGICAL_BOOLEAN: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN); break;
    case DUCKNNG_QUACK_LOGICAL_TINYINT: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TINYINT); break;
    case DUCKNNG_QUACK_LOGICAL_SMALLINT: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_SMALLINT); break;
    case DUCKNNG_QUACK_LOGICAL_INTEGER: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER); break;
    case DUCKNNG_QUACK_LOGICAL_BIGINT: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT); break;
    case DUCKNNG_QUACK_LOGICAL_DATE: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_DATE); break;
    case DUCKNNG_QUACK_LOGICAL_TIME: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIME); break;
    case DUCKNNG_QUACK_LOGICAL_TIMESTAMP_SEC: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIMESTAMP_S); break;
    case DUCKNNG_QUACK_LOGICAL_TIMESTAMP_MS: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIMESTAMP_MS); break;
    case DUCKNNG_QUACK_LOGICAL_TIMESTAMP: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIMESTAMP); break;
    case DUCKNNG_QUACK_LOGICAL_TIMESTAMP_NS: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIMESTAMP_NS); break;
    case DUCKNNG_QUACK_LOGICAL_DECIMAL:
        *out_type = duckdb_create_decimal_type(meta->decimal_width, meta->decimal_scale);
        break;
    case DUCKNNG_QUACK_LOGICAL_FLOAT: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_FLOAT); break;
    case DUCKNNG_QUACK_LOGICAL_DOUBLE: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE); break;
    case DUCKNNG_QUACK_LOGICAL_CHAR:
    case DUCKNNG_QUACK_LOGICAL_VARCHAR: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR); break;
    case DUCKNNG_QUACK_LOGICAL_BLOB: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB); break;
    case DUCKNNG_QUACK_LOGICAL_INTERVAL: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_INTERVAL); break;
    case DUCKNNG_QUACK_LOGICAL_UTINYINT: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_UTINYINT); break;
    case DUCKNNG_QUACK_LOGICAL_USMALLINT: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_USMALLINT); break;
    case DUCKNNG_QUACK_LOGICAL_UINTEGER: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_UINTEGER); break;
    case DUCKNNG_QUACK_LOGICAL_UBIGINT: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT); break;
    case DUCKNNG_QUACK_LOGICAL_TIMESTAMP_TZ: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIMESTAMP_TZ); break;
    case DUCKNNG_QUACK_LOGICAL_TIME_TZ: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIME_TZ); break;
    case DUCKNNG_QUACK_LOGICAL_TIME_NS: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIME_NS); break;
    case DUCKNNG_QUACK_LOGICAL_UHUGEINT: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_UHUGEINT); break;
    case DUCKNNG_QUACK_LOGICAL_HUGEINT: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_HUGEINT); break;
    case DUCKNNG_QUACK_LOGICAL_UUID: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_UUID); break;
    default:
        ducknng_quack_set_error(errmsg,
            "ducknng: ducknng_quack_batch decode encountered an unsupported logical type");
        return -1;
    }
    if (!*out_type) {
        ducknng_quack_set_error(errmsg, "ducknng: failed to allocate DuckDB logical type for quack payload");
        return -1;
    }
    return 0;
}

static int ducknng_quack_meta_fixed_size(const ducknng_quack_type_meta *meta, size_t *out_size) {
    size_t size = 0;
    if (!meta || !out_size) return -1;
    switch (meta->logical_type_id) {
    case DUCKNNG_QUACK_LOGICAL_BOOLEAN:
    case DUCKNNG_QUACK_LOGICAL_TINYINT:
    case DUCKNNG_QUACK_LOGICAL_UTINYINT:
        size = 1; break;
    case DUCKNNG_QUACK_LOGICAL_SMALLINT:
    case DUCKNNG_QUACK_LOGICAL_USMALLINT:
        size = 2; break;
    case DUCKNNG_QUACK_LOGICAL_INTEGER:
    case DUCKNNG_QUACK_LOGICAL_UINTEGER:
    case DUCKNNG_QUACK_LOGICAL_DATE:
    case DUCKNNG_QUACK_LOGICAL_FLOAT:
        size = 4; break;
    case DUCKNNG_QUACK_LOGICAL_BIGINT:
    case DUCKNNG_QUACK_LOGICAL_UBIGINT:
    case DUCKNNG_QUACK_LOGICAL_TIME:
    case DUCKNNG_QUACK_LOGICAL_TIMESTAMP_SEC:
    case DUCKNNG_QUACK_LOGICAL_TIMESTAMP_MS:
    case DUCKNNG_QUACK_LOGICAL_TIMESTAMP:
    case DUCKNNG_QUACK_LOGICAL_TIMESTAMP_NS:
    case DUCKNNG_QUACK_LOGICAL_TIMESTAMP_TZ:
    case DUCKNNG_QUACK_LOGICAL_TIME_TZ:
    case DUCKNNG_QUACK_LOGICAL_TIME_NS:
    case DUCKNNG_QUACK_LOGICAL_DOUBLE:
        size = 8; break;
    case DUCKNNG_QUACK_LOGICAL_INTERVAL:
    case DUCKNNG_QUACK_LOGICAL_HUGEINT:
    case DUCKNNG_QUACK_LOGICAL_UHUGEINT:
    case DUCKNNG_QUACK_LOGICAL_UUID:
        size = 16; break;
    case DUCKNNG_QUACK_LOGICAL_DECIMAL:
        if (meta->decimal_width <= 4) size = 2;
        else if (meta->decimal_width <= 9) size = 4;
        else if (meta->decimal_width <= 18) size = 8;
        else if (meta->decimal_width <= 38) size = 16;
        else return -1;
        break;
    default:
        return -1;
    }
    *out_size = size;
    return 0;
}

static int ducknng_quack_meta_is_varlen(const ducknng_quack_type_meta *meta) {
    return meta && (meta->logical_type_id == DUCKNNG_QUACK_LOGICAL_VARCHAR ||
        meta->logical_type_id == DUCKNNG_QUACK_LOGICAL_CHAR ||
        meta->logical_type_id == DUCKNNG_QUACK_LOGICAL_BLOB);
}

static int ducknng_quack_write_type_meta(ducknng_quack_writer *w,
    const ducknng_quack_type_meta *meta, char **errmsg) {
    if (!w || !meta) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack type metadata");
        return -1;
    }
    if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_TYPE_ID, errmsg) != 0 ||
        ducknng_quack_write_uleb128(w, (uint64_t)meta->logical_type_id, errmsg) != 0) return -1;
    if (meta->logical_type_id == DUCKNNG_QUACK_LOGICAL_DECIMAL) {
        if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_TYPE_INFO, errmsg) != 0 ||
            ducknng_quack_write_byte(w, 1, errmsg) != 0 ||
            ducknng_quack_write_field_id(w, DUCKNNG_QUACK_EXTRA_INFO_KIND, errmsg) != 0 ||
            ducknng_quack_write_uleb128(w, DUCKNNG_QUACK_EXTRA_TYPE_DECIMAL, errmsg) != 0 ||
            ducknng_quack_write_field_id(w, DUCKNNG_QUACK_EXTRA_DECIMAL_WIDTH, errmsg) != 0 ||
            ducknng_quack_write_uleb128(w, meta->decimal_width, errmsg) != 0 ||
            ducknng_quack_write_field_id(w, DUCKNNG_QUACK_EXTRA_DECIMAL_SCALE, errmsg) != 0 ||
            ducknng_quack_write_uleb128(w, meta->decimal_scale, errmsg) != 0 ||
            ducknng_quack_write_field_end(w, errmsg) != 0) return -1;
    }
    return ducknng_quack_write_field_end(w, errmsg);
}

static int ducknng_quack_read_type_meta(ducknng_quack_reader *r,
    ducknng_quack_type_meta *out, char **errmsg) {
    uint16_t field_id = 0;
    uint64_t logical_id = 0;
    if (!r || !out) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack logical type decoder state");
        return -1;
    }
    memset(out, 0, sizeof(*out));
    if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_TYPE_ID, errmsg) != 0 ||
        ducknng_quack_read_uleb128(r, &logical_id, errmsg) != 0) return -1;
    out->logical_type_id = (int)logical_id;
    if (ducknng_quack_peek_u16le(r, &field_id, errmsg) != 0) return -1;
    if (field_id == DUCKNNG_QUACK_TYPE_INFO) {
        uint8_t present = 0;
        uint64_t kind = 0;
        if (ducknng_quack_read_u16le(r, &field_id, errmsg) != 0 ||
            ducknng_quack_read_byte(r, &present, errmsg) != 0) return -1;
        if (present) {
            if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_EXTRA_INFO_KIND, errmsg) != 0 ||
                ducknng_quack_read_uleb128(r, &kind, errmsg) != 0) return -1;
            if (kind == DUCKNNG_QUACK_EXTRA_TYPE_DECIMAL) {
                uint64_t width = 0;
                uint64_t scale = 0;
                if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_EXTRA_DECIMAL_WIDTH, errmsg) != 0 ||
                    ducknng_quack_read_uleb128(r, &width, errmsg) != 0 ||
                    ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_EXTRA_DECIMAL_SCALE, errmsg) != 0 ||
                    ducknng_quack_read_uleb128(r, &scale, errmsg) != 0) return -1;
                out->decimal_width = (uint8_t)width;
                out->decimal_scale = (uint8_t)scale;
            }
            if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg) != 0) return -1;
        }
    }
    return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
}

static int ducknng_quack_skip_type_meta(ducknng_quack_reader *r, char **errmsg) {
    ducknng_quack_type_meta meta;
    return ducknng_quack_read_type_meta(r, &meta, errmsg);
}

static int ducknng_quack_skip_string_list(ducknng_quack_reader *r, uint64_t expected, char **errmsg) {
    uint64_t n = 0;
    uint64_t i;
    if (ducknng_quack_read_uleb128(r, &n, errmsg) != 0) return -1;
    if (expected != UINT64_MAX && n != expected) {
        ducknng_quack_set_error(errmsg, "ducknng: quack string list length mismatch");
        return -1;
    }
    for (i = 0; i < n; i++) if (ducknng_quack_skip_string(r, errmsg) != 0) return -1;
    return 0;
}

static int ducknng_quack_skip_type_list(ducknng_quack_reader *r, uint64_t expected, char **errmsg) {
    uint64_t n = 0;
    uint64_t i;
    if (ducknng_quack_read_uleb128(r, &n, errmsg) != 0) return -1;
    if (expected != UINT64_MAX && n != expected) {
        ducknng_quack_set_error(errmsg, "ducknng: quack logical type list length mismatch");
        return -1;
    }
    for (i = 0; i < n; i++) if (ducknng_quack_skip_type_meta(r, errmsg) != 0) return -1;
    return 0;
}

static int ducknng_quack_encode_validity_blob(ducknng_quack_writer *w,
    const uint64_t *validity, idx_t rows, char **errmsg) {
    size_t n_words = (size_t)((rows + 63) / 64);
    return ducknng_quack_write_blob(w, (const uint8_t *)validity, n_words * sizeof(uint64_t), errmsg);
}

static int ducknng_quack_encode_vector(ducknng_quack_writer *w, duckdb_vector vec,
    const ducknng_quack_type_meta *meta, idx_t rows, char **errmsg) {
    size_t width = 0;
    uint64_t *validity = NULL;
    int has_validity = 0;
    if (!w || !vec || !meta) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack vector encoder state");
        return -1;
    }
    validity = duckdb_vector_get_validity(vec);
    has_validity = validity != NULL;
    if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_VECTOR_HAS_VALIDITY, errmsg) != 0 ||
        ducknng_quack_write_byte(w, has_validity ? 1u : 0u, errmsg) != 0) return -1;
    if (has_validity) {
        if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_VECTOR_VALIDITY, errmsg) != 0 ||
            ducknng_quack_encode_validity_blob(w, validity, rows, errmsg) != 0) return -1;
    }
    if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_VECTOR_DATA, errmsg) != 0) return -1;
    if (ducknng_quack_meta_is_varlen(meta)) {
        duckdb_string_t *data = (duckdb_string_t *)duckdb_vector_get_data(vec);
        idx_t row;
        if (ducknng_quack_write_uleb128(w, (uint64_t)rows, errmsg) != 0) return -1;
        for (row = 0; row < rows; row++) {
            const uint8_t *ptr = NULL;
            size_t len = 0;
            if (ducknng_quack_vector_row_is_valid(validity, row)) {
                duckdb_string_t *value = &data[row];
                len = (size_t)duckdb_string_t_length(*value);
                ptr = (const uint8_t *)duckdb_string_t_data(value);
            }
            if (ducknng_quack_write_string_len(w, ptr, len, errmsg) != 0) return -1;
        }
        return ducknng_quack_write_field_end(w, errmsg);
    }
    if (ducknng_quack_meta_fixed_size(meta, &width) != 0) {
        ducknng_quack_set_error(errmsg,
            "ducknng: ducknng_quack_batch currently supports scalar bool/integer/float/date/time/timestamp/decimal/varchar/blob/interval/hugeint/uuid columns only");
        return -1;
    }
    if (ducknng_quack_write_blob(w, (const uint8_t *)duckdb_vector_get_data(vec), width * (size_t)rows, errmsg) != 0) return -1;
    return ducknng_quack_write_field_end(w, errmsg);
}

static int ducknng_quack_encode_one_chunk(ducknng_quack_writer *w, duckdb_data_chunk chunk,
    idx_t ncols, char **errmsg) {
    idx_t rows = duckdb_data_chunk_get_size(chunk);
    idx_t col;
    if (ducknng_quack_write_byte(w, 1, errmsg) != 0 ||
        ducknng_quack_write_field_id(w, DUCKNNG_QUACK_CHUNK_WRAPPER, errmsg) != 0 ||
        ducknng_quack_write_field_id(w, DUCKNNG_QUACK_CHUNK_ROWS, errmsg) != 0 ||
        ducknng_quack_write_uleb128(w, (uint64_t)rows, errmsg) != 0 ||
        ducknng_quack_write_field_id(w, DUCKNNG_QUACK_CHUNK_COLUMNS, errmsg) != 0 ||
        ducknng_quack_write_uleb128(w, (uint64_t)ncols, errmsg) != 0) return -1;
    for (col = 0; col < ncols; col++) {
        duckdb_vector vec = duckdb_data_chunk_get_vector(chunk, col);
        duckdb_logical_type logical_type = duckdb_vector_get_column_type(vec);
        ducknng_quack_type_meta meta;
        int rc = ducknng_quack_duckdb_type_to_meta(logical_type, &meta, errmsg);
        if (logical_type) duckdb_destroy_logical_type(&logical_type);
        if (rc != 0) return -1;
        if (ducknng_quack_encode_vector(w, vec, &meta, rows, errmsg) != 0) return -1;
    }
    return ducknng_quack_write_field_end(w, errmsg);
}

static int ducknng_quack_encode_result_payload(duckdb_result result,
    duckdb_data_chunk *chunks, idx_t chunk_count, int include_schema,
    uint8_t **out_bytes, size_t *out_len, char **errmsg) {
    ducknng_quack_writer w;
    idx_t ncols = duckdb_column_count(&result);
    idx_t col;
    idx_t chunk_idx;
    memset(&w, 0, sizeof(w));
    if (!out_bytes || !out_len) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack payload output pointers");
        return -1;
    }
    if (include_schema) {
        if (ducknng_quack_write_field_id(&w, DUCKNNG_QUACK_OUTER_RESULT_TYPES, errmsg) != 0 ||
            ducknng_quack_write_uleb128(&w, (uint64_t)ncols, errmsg) != 0) goto fail;
        for (col = 0; col < ncols; col++) {
            duckdb_logical_type logical_type = duckdb_column_logical_type(&result, col);
            ducknng_quack_type_meta meta;
            int rc = ducknng_quack_duckdb_type_to_meta(logical_type, &meta, errmsg);
            if (logical_type) duckdb_destroy_logical_type(&logical_type);
            if (rc != 0) goto fail;
            if (ducknng_quack_write_type_meta(&w, &meta, errmsg) != 0) goto fail;
        }
        if (ducknng_quack_write_field_id(&w, DUCKNNG_QUACK_OUTER_RESULT_NAMES, errmsg) != 0 ||
            ducknng_quack_write_uleb128(&w, (uint64_t)ncols, errmsg) != 0) goto fail;
        for (col = 0; col < ncols; col++) {
            if (ducknng_quack_write_string(&w, duckdb_column_name(&result, col), errmsg) != 0) goto fail;
        }
    }
    if (chunk_count > 0) {
        if (ducknng_quack_write_field_id(&w, DUCKNNG_QUACK_OUTER_RESULTS, errmsg) != 0 ||
            ducknng_quack_write_uleb128(&w, (uint64_t)chunk_count, errmsg) != 0) goto fail;
        for (chunk_idx = 0; chunk_idx < chunk_count; chunk_idx++) {
            if (!chunks[chunk_idx]) {
                ducknng_quack_set_error(errmsg, "ducknng: missing quack chunk while encoding");
                goto fail;
            }
            if (ducknng_quack_encode_one_chunk(&w, chunks[chunk_idx], ncols, errmsg) != 0) goto fail;
        }
        if (ducknng_quack_write_field_end(&w, errmsg) != 0) goto fail;
    }
    if (ducknng_quack_write_field_end(&w, errmsg) != 0) goto fail;
    *out_bytes = w.data;
    *out_len = w.len;
    return 0;
fail:
    if (w.data) duckdb_free(w.data);
    return -1;
}

static int ducknng_quack_skip_fixed_vector(ducknng_quack_reader *r,
    const ducknng_quack_type_meta *meta, idx_t rows, char **errmsg) {
    uint16_t field_id = 0;
    uint8_t has_validity = 0;
    const uint8_t *blob = NULL;
    size_t blob_len = 0;
    size_t width = 0;
    if (ducknng_quack_peek_u16le(r, &field_id, errmsg) != 0) return -1;
    if (field_id == DUCKNNG_QUACK_VECTOR_TYPE) {
        uint64_t vector_type = 0;
        if (ducknng_quack_read_u16le(r, &field_id, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &vector_type, errmsg) != 0) return -1;
        if (vector_type != DUCKNNG_QUACK_VECTOR_FLAT) {
            ducknng_quack_set_error(errmsg, "ducknng: ducknng_quack_batch decode currently supports flat vectors only");
            return -1;
        }
    }
    if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_HAS_VALIDITY, errmsg) != 0 ||
        ducknng_quack_read_byte(r, &has_validity, errmsg) != 0) return -1;
    if (has_validity) {
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_VALIDITY, errmsg) != 0 ||
            ducknng_quack_read_blob_view(r, NULL, NULL, errmsg) != 0) return -1;
    }
    if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_DATA, errmsg) != 0 ||
        ducknng_quack_read_blob_view(r, &blob, &blob_len, errmsg) != 0) return -1;
    if (ducknng_quack_meta_fixed_size(meta, &width) != 0 || blob_len != width * (size_t)rows) {
        ducknng_quack_set_error(errmsg, "ducknng: invalid fixed-size quack vector payload");
        return -1;
    }
    return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
}

static int ducknng_quack_skip_varlen_vector(ducknng_quack_reader *r, idx_t rows, char **errmsg) {
    uint16_t field_id = 0;
    uint8_t has_validity = 0;
    uint64_t n_items = 0;
    idx_t row;
    if (ducknng_quack_peek_u16le(r, &field_id, errmsg) != 0) return -1;
    if (field_id == DUCKNNG_QUACK_VECTOR_TYPE) {
        uint64_t vector_type = 0;
        if (ducknng_quack_read_u16le(r, &field_id, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &vector_type, errmsg) != 0) return -1;
        if (vector_type != DUCKNNG_QUACK_VECTOR_FLAT) {
            ducknng_quack_set_error(errmsg, "ducknng: ducknng_quack_batch decode currently supports flat vectors only");
            return -1;
        }
    }
    if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_HAS_VALIDITY, errmsg) != 0 ||
        ducknng_quack_read_byte(r, &has_validity, errmsg) != 0) return -1;
    if (has_validity) {
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_VALIDITY, errmsg) != 0 ||
            ducknng_quack_read_blob_view(r, NULL, NULL, errmsg) != 0) return -1;
    }
    if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_DATA, errmsg) != 0 ||
        ducknng_quack_read_uleb128(r, &n_items, errmsg) != 0) return -1;
    if (n_items != (uint64_t)rows) {
        ducknng_quack_set_error(errmsg, "ducknng: invalid variable-width quack vector payload");
        return -1;
    }
    for (row = 0; row < rows; row++) if (ducknng_quack_skip_string(r, errmsg) != 0) return -1;
    return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
}

static int ducknng_quack_skip_vector(ducknng_quack_reader *r,
    const ducknng_quack_type_meta *meta, idx_t rows, char **errmsg) {
    if (ducknng_quack_meta_is_varlen(meta)) return ducknng_quack_skip_varlen_vector(r, rows, errmsg);
    return ducknng_quack_skip_fixed_vector(r, meta, rows, errmsg);
}

static int ducknng_quack_skip_optional_chunk_types(ducknng_quack_reader *r,
    const ducknng_quack_schema *schema, char **errmsg) {
    uint16_t field_id = 0;
    if (!r || !schema) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack chunk type state");
        return -1;
    }
    if (ducknng_quack_peek_u16le(r, &field_id, errmsg) != 0) return -1;
    if (field_id != DUCKNNG_QUACK_CHUNK_TYPES) return 0;
    if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_CHUNK_TYPES, errmsg) != 0 ||
        ducknng_quack_skip_type_list(r, (uint64_t)schema->ncols, errmsg) != 0) return -1;
    return 0;
}

static int ducknng_quack_skip_data_chunk(ducknng_quack_reader *r,
    const ducknng_quack_schema *schema, idx_t *out_rows, char **errmsg) {
    uint64_t rows = 0;
    uint64_t ncols = 0;
    idx_t col;
    if (!schema) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack schema while skipping data chunk");
        return -1;
    }
    if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_CHUNK_ROWS, errmsg) != 0 ||
        ducknng_quack_read_uleb128(r, &rows, errmsg) != 0 ||
        ducknng_quack_skip_optional_chunk_types(r, schema, errmsg) != 0 ||
        ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_CHUNK_COLUMNS, errmsg) != 0 ||
        ducknng_quack_read_uleb128(r, &ncols, errmsg) != 0) return -1;
    if (ncols != (uint64_t)schema->ncols) {
        ducknng_quack_set_error(errmsg, "ducknng: quack data chunk column count mismatch");
        return -1;
    }
    for (col = 0; col < schema->ncols; col++) {
        ducknng_quack_type_meta meta;
        meta.logical_type_id = schema->cols[col].logical_type_id;
        meta.decimal_width = schema->cols[col].decimal_width;
        meta.decimal_scale = schema->cols[col].decimal_scale;
        if (ducknng_quack_skip_vector(r, &meta, (idx_t)rows, errmsg) != 0) return -1;
    }
    if (out_rows) *out_rows = (idx_t)rows;
    return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
}

static void ducknng_quack_copy_validity_slice(duckdb_vector out_vec,
    const uint64_t *src_validity, idx_t src_offset, idx_t count) {
    uint64_t *dst_validity;
    idx_t row;
    if (!out_vec || !src_validity || count == 0) return;
    duckdb_vector_ensure_validity_writable(out_vec);
    dst_validity = duckdb_vector_get_validity(out_vec);
    for (row = 0; row < count; row++) {
        duckdb_validity_set_row_validity(dst_validity, row,
            ducknng_quack_vector_row_is_valid(src_validity, src_offset + row) ? true : false);
    }
}

static int ducknng_quack_decode_fixed_vector_slice(ducknng_quack_reader *r,
    const ducknng_quack_type_meta *meta, idx_t rows, idx_t offset,
    duckdb_vector out_vec, idx_t out_count, char **errmsg) {
    uint16_t field_id = 0;
    uint8_t has_validity = 0;
    const uint8_t *validity_blob = NULL;
    size_t validity_len = 0;
    const uint8_t *data_blob = NULL;
    size_t data_len = 0;
    size_t width = 0;
    const uint64_t *src_validity = NULL;
    if (ducknng_quack_peek_u16le(r, &field_id, errmsg) != 0) return -1;
    if (field_id == DUCKNNG_QUACK_VECTOR_TYPE) {
        uint64_t vector_type = 0;
        if (ducknng_quack_read_u16le(r, &field_id, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &vector_type, errmsg) != 0) return -1;
        if (vector_type != DUCKNNG_QUACK_VECTOR_FLAT) {
            ducknng_quack_set_error(errmsg, "ducknng: ducknng_quack_batch decode currently supports flat vectors only");
            return -1;
        }
    }
    if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_HAS_VALIDITY, errmsg) != 0 ||
        ducknng_quack_read_byte(r, &has_validity, errmsg) != 0) return -1;
    if (has_validity) {
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_VALIDITY, errmsg) != 0 ||
            ducknng_quack_read_blob_view(r, &validity_blob, &validity_len, errmsg) != 0) return -1;
        src_validity = (const uint64_t *)validity_blob;
    }
    if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_DATA, errmsg) != 0 ||
        ducknng_quack_read_blob_view(r, &data_blob, &data_len, errmsg) != 0) return -1;
    if (ducknng_quack_meta_fixed_size(meta, &width) != 0 || data_len != width * (size_t)rows) {
        ducknng_quack_set_error(errmsg, "ducknng: invalid fixed-size quack vector payload");
        return -1;
    }
    if (out_count > 0) {
        memcpy(duckdb_vector_get_data(out_vec), data_blob + width * (size_t)offset, width * (size_t)out_count);
        if (src_validity) ducknng_quack_copy_validity_slice(out_vec, src_validity, offset, out_count);
    }
    return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
}

static int ducknng_quack_decode_varlen_vector_slice(ducknng_quack_reader *r,
    const ducknng_quack_type_meta *meta, idx_t rows, idx_t offset,
    duckdb_vector out_vec, idx_t out_count, char **errmsg) {
    uint16_t field_id = 0;
    uint8_t has_validity = 0;
    const uint8_t *validity_blob = NULL;
    size_t validity_len = 0;
    const uint64_t *src_validity = NULL;
    uint64_t n_items = 0;
    idx_t row;
    idx_t out_row = 0;
    (void)meta;
    if (ducknng_quack_peek_u16le(r, &field_id, errmsg) != 0) return -1;
    if (field_id == DUCKNNG_QUACK_VECTOR_TYPE) {
        uint64_t vector_type = 0;
        if (ducknng_quack_read_u16le(r, &field_id, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &vector_type, errmsg) != 0) return -1;
        if (vector_type != DUCKNNG_QUACK_VECTOR_FLAT) {
            ducknng_quack_set_error(errmsg, "ducknng: ducknng_quack_batch decode currently supports flat vectors only");
            return -1;
        }
    }
    if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_HAS_VALIDITY, errmsg) != 0 ||
        ducknng_quack_read_byte(r, &has_validity, errmsg) != 0) return -1;
    if (has_validity) {
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_VALIDITY, errmsg) != 0 ||
            ducknng_quack_read_blob_view(r, &validity_blob, &validity_len, errmsg) != 0) return -1;
        src_validity = (const uint64_t *)validity_blob;
    }
    if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_DATA, errmsg) != 0 ||
        ducknng_quack_read_uleb128(r, &n_items, errmsg) != 0) return -1;
    if (n_items != (uint64_t)rows) {
        ducknng_quack_set_error(errmsg, "ducknng: invalid variable-width quack vector payload");
        return -1;
    }
    for (row = 0; row < rows; row++) {
        const uint8_t *data = NULL;
        size_t len = 0;
        if (ducknng_quack_read_blob_view(r, &data, &len, errmsg) != 0) return -1;
        if (row < offset || row >= offset + out_count) continue;
        if (src_validity && !ducknng_quack_vector_row_is_valid(src_validity, row)) {
            duckdb_vector_ensure_validity_writable(out_vec);
            duckdb_validity_set_row_invalid(duckdb_vector_get_validity(out_vec), out_row);
        } else {
            duckdb_unsafe_vector_assign_string_element_len(out_vec, out_row, (const char *)data, (idx_t)len);
        }
        out_row++;
    }
    return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
}

static int ducknng_quack_decode_vector_slice(ducknng_quack_reader *r,
    const ducknng_quack_column_schema *col_schema, idx_t rows, idx_t offset,
    duckdb_vector out_vec, idx_t out_count, char **errmsg) {
    ducknng_quack_type_meta meta;
    meta.logical_type_id = col_schema->logical_type_id;
    meta.decimal_width = col_schema->decimal_width;
    meta.decimal_scale = col_schema->decimal_scale;
    if (ducknng_quack_meta_is_varlen(&meta)) {
        return ducknng_quack_decode_varlen_vector_slice(r, &meta, rows, offset, out_vec, out_count, errmsg);
    }
    return ducknng_quack_decode_fixed_vector_slice(r, &meta, rows, offset, out_vec, out_count, errmsg);
}

void ducknng_quack_schema_reset(ducknng_quack_schema *schema) {
    idx_t i;
    if (!schema) return;
    if (schema->cols) {
        for (i = 0; i < schema->ncols; i++) {
            if (schema->cols[i].name) duckdb_free(schema->cols[i].name);
        }
        duckdb_free(schema->cols);
    }
    memset(schema, 0, sizeof(*schema));
}

int ducknng_result_next_chunks_to_quack_payload(duckdb_result result, int result_streaming,
    uint64_t max_chunks, int include_schema, uint8_t **out_bytes, size_t *out_len,
    int *has_chunk, char **errmsg) {
    duckdb_data_chunk *chunks = NULL;
    idx_t chunk_count = 0;
    uint64_t i;
    int rc;
    if (has_chunk) *has_chunk = 0;
    if (max_chunks == 0) max_chunks = 1;
    chunks = (duckdb_data_chunk *)duckdb_malloc(sizeof(*chunks) * (size_t)max_chunks);
    if (!chunks) {
        ducknng_quack_set_error(errmsg, "ducknng: out of memory collecting quack chunks");
        return -1;
    }
    memset(chunks, 0, sizeof(*chunks) * (size_t)max_chunks);
    for (i = 0; i < max_chunks; i++) {
        chunks[chunk_count] = ducknng_result_fetch_session_chunk(result, result_streaming);
        if (!chunks[chunk_count]) break;
        chunk_count++;
    }
    if (chunk_count == 0) {
        duckdb_free(chunks);
        return 0;
    }
    rc = ducknng_quack_encode_result_payload(result, chunks, chunk_count, include_schema,
        out_bytes, out_len, errmsg);
    for (i = 0; i < (uint64_t)chunk_count; i++) {
        if (chunks[i]) duckdb_destroy_data_chunk(&chunks[i]);
    }
    duckdb_free(chunks);
    if (rc == 0 && has_chunk) *has_chunk = 1;
    return rc;
}

int ducknng_result_next_chunk_to_quack_payload(duckdb_result result,
    uint8_t **out_bytes, size_t *out_len, int *has_chunk, char **errmsg) {
    return ducknng_result_next_chunks_to_quack_payload(result, 0, 1, 1,
        out_bytes, out_len, has_chunk, errmsg);
}

int ducknng_result_empty_quack_payload(duckdb_result result,
    uint8_t **out_bytes, size_t *out_len, char **errmsg) {
    return ducknng_quack_encode_result_payload(result, NULL, 0, 1, out_bytes, out_len, errmsg);
}

int ducknng_quack_payload_bind_columns(duckdb_bind_info info,
    const uint8_t *payload, size_t payload_len,
    ducknng_quack_schema *out_schema, idx_t *out_row_count, char **errmsg) {
    ducknng_quack_reader r;
    uint16_t field_id = 0;
    uint64_t ncols = 0;
    uint64_t i;
    idx_t row_count = 0;
    if (out_row_count) *out_row_count = 0;
    if (!info || !payload || payload_len == 0 || !out_schema) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack payload for bind");
        return -1;
    }
    memset(&r, 0, sizeof(r));
    r.data = payload;
    r.len = payload_len;
    memset(out_schema, 0, sizeof(*out_schema));

    if (ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_OUTER_RESULT_TYPES, errmsg) != 0 ||
        ducknng_quack_read_uleb128(&r, &ncols, errmsg) != 0) goto fail;
    out_schema->ncols = (idx_t)ncols;
    out_schema->cols = (ducknng_quack_column_schema *)duckdb_malloc(sizeof(*out_schema->cols) * (size_t)ncols);
    if (!out_schema->cols) {
        ducknng_quack_set_error(errmsg, "ducknng: out of memory allocating quack schema columns");
        goto fail;
    }
    memset(out_schema->cols, 0, sizeof(*out_schema->cols) * (size_t)ncols);
    for (i = 0; i < ncols; i++) {
        ducknng_quack_type_meta meta;
        if (ducknng_quack_read_type_meta(&r, &meta, errmsg) != 0) goto fail;
        out_schema->cols[i].logical_type_id = meta.logical_type_id;
        out_schema->cols[i].decimal_width = meta.decimal_width;
        out_schema->cols[i].decimal_scale = meta.decimal_scale;
    }
    if (ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_OUTER_RESULT_NAMES, errmsg) != 0 ||
        ducknng_quack_read_uleb128(&r, &ncols, errmsg) != 0) goto fail;
    if (ncols != (uint64_t)out_schema->ncols) {
        ducknng_quack_set_error(errmsg, "ducknng: quack payload name/type count mismatch");
        goto fail;
    }
    for (i = 0; i < ncols; i++) {
        if (ducknng_quack_read_string_dup(&r, &out_schema->cols[i].name, errmsg) != 0) goto fail;
    }
    for (i = 0; i < (uint64_t)out_schema->ncols; i++) {
        duckdb_logical_type bind_type = NULL;
        ducknng_quack_type_meta meta;
        meta.logical_type_id = out_schema->cols[i].logical_type_id;
        meta.decimal_width = out_schema->cols[i].decimal_width;
        meta.decimal_scale = out_schema->cols[i].decimal_scale;
        if (ducknng_quack_meta_to_duckdb_type(&meta, &bind_type, errmsg) != 0) goto fail;
        duckdb_bind_add_result_column(info, out_schema->cols[i].name ? out_schema->cols[i].name : "", bind_type);
        duckdb_destroy_logical_type(&bind_type);
    }
    if (ducknng_quack_peek_u16le(&r, &field_id, errmsg) != 0) goto fail;
    if (field_id == DUCKNNG_QUACK_OUTER_RESULTS) {
        uint64_t n_results = 0;
        uint64_t chunk_idx;
        if (ducknng_quack_read_u16le(&r, &field_id, errmsg) != 0 ||
            ducknng_quack_read_uleb128(&r, &n_results, errmsg) != 0) goto fail;
        for (chunk_idx = 0; chunk_idx < n_results; chunk_idx++) {
            uint8_t present = 0;
            idx_t chunk_rows = 0;
            if (ducknng_quack_read_byte(&r, &present, errmsg) != 0) goto fail;
            if (!present) {
                ducknng_quack_set_error(errmsg, "ducknng: quack payload contained a NULL chunk pointer");
                goto fail;
            }
            if (ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_CHUNK_WRAPPER, errmsg) != 0 ||
                ducknng_quack_skip_data_chunk(&r, out_schema, &chunk_rows, errmsg) != 0) goto fail;
            row_count += chunk_rows;
        }
        if (ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_FIELD_END, errmsg) != 0) goto fail;
    }
    if (ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_FIELD_END, errmsg) != 0) goto fail;
    if (r.off != r.len) {
        ducknng_quack_set_error(errmsg, "ducknng: trailing bytes in quack payload");
        goto fail;
    }
    if (out_row_count) *out_row_count = row_count;
    return 0;
fail:
    ducknng_quack_schema_reset(out_schema);
    return -1;
}

int ducknng_quack_payload_read_row_count(const uint8_t *payload, size_t payload_len,
    const ducknng_quack_schema *schema, idx_t *out_row_count, char **errmsg) {
    ducknng_quack_reader r;
    uint16_t field_id = 0;
    uint64_t n_results = 0;
    uint64_t chunk_idx;
    idx_t row_count = 0;
    if (out_row_count) *out_row_count = 0;
    if (!payload || payload_len == 0 || !schema) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack payload row-count inputs");
        return -1;
    }
    memset(&r, 0, sizeof(r));
    r.data = payload;
    r.len = payload_len;
    if (ducknng_quack_peek_u16le(&r, &field_id, errmsg) != 0) return -1;
    if (field_id == DUCKNNG_QUACK_OUTER_RESULT_TYPES) {
        if (ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_OUTER_RESULT_TYPES, errmsg) != 0 ||
            ducknng_quack_skip_type_list(&r, (uint64_t)schema->ncols, errmsg) != 0 ||
            ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_OUTER_RESULT_NAMES, errmsg) != 0 ||
            ducknng_quack_skip_string_list(&r, (uint64_t)schema->ncols, errmsg) != 0) return -1;
    }
    if (ducknng_quack_peek_u16le(&r, &field_id, errmsg) != 0) return -1;
    if (field_id != DUCKNNG_QUACK_OUTER_RESULTS) {
        if (out_row_count) *out_row_count = 0;
        return 0;
    }
    if (ducknng_quack_read_u16le(&r, &field_id, errmsg) != 0 ||
        ducknng_quack_read_uleb128(&r, &n_results, errmsg) != 0) return -1;
    for (chunk_idx = 0; chunk_idx < n_results; chunk_idx++) {
        uint8_t present = 0;
        idx_t chunk_rows = 0;
        if (ducknng_quack_read_byte(&r, &present, errmsg) != 0 || !present ||
            ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_CHUNK_WRAPPER, errmsg) != 0 ||
            ducknng_quack_skip_data_chunk(&r, schema, &chunk_rows, errmsg) != 0) return -1;
        row_count += chunk_rows;
    }
    if (ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_FIELD_END, errmsg) != 0) return -1;
    if (out_row_count) *out_row_count = row_count;
    return 0;
}

static int ducknng_quack_decode_data_chunk_slice(ducknng_quack_reader *r,
    duckdb_data_chunk output, const ducknng_quack_schema *schema,
    idx_t offset, idx_t *out_rows, char **errmsg) {
    uint64_t rows_u64 = 0;
    uint64_t ncols = 0;
    idx_t rows;
    idx_t copy_count;
    idx_t col;
    if (out_rows) *out_rows = 0;
    if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_CHUNK_ROWS, errmsg) != 0 ||
        ducknng_quack_read_uleb128(r, &rows_u64, errmsg) != 0) return -1;
    rows = (idx_t)rows_u64;
    if (out_rows) *out_rows = rows;
    if (offset >= rows) {
        if (ducknng_quack_skip_optional_chunk_types(r, schema, errmsg) != 0 ||
            ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_CHUNK_COLUMNS, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &ncols, errmsg) != 0) return -1;
        if (ncols != (uint64_t)schema->ncols) {
            ducknng_quack_set_error(errmsg, "ducknng: quack data chunk column count mismatch");
            return -1;
        }
        for (col = 0; col < schema->ncols; col++) {
            ducknng_quack_type_meta meta;
            meta.logical_type_id = schema->cols[col].logical_type_id;
            meta.decimal_width = schema->cols[col].decimal_width;
            meta.decimal_scale = schema->cols[col].decimal_scale;
            if (ducknng_quack_skip_vector(r, &meta, rows, errmsg) != 0) return -1;
        }
        return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
    }
    copy_count = rows - offset;
    if (copy_count > duckdb_vector_size()) copy_count = duckdb_vector_size();
    if (ducknng_quack_skip_optional_chunk_types(r, schema, errmsg) != 0 ||
        ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_CHUNK_COLUMNS, errmsg) != 0 ||
        ducknng_quack_read_uleb128(r, &ncols, errmsg) != 0) return -1;
    if (ncols != (uint64_t)schema->ncols) {
        ducknng_quack_set_error(errmsg, "ducknng: quack data chunk column count mismatch");
        return -1;
    }
    for (col = 0; col < schema->ncols; col++) {
        duckdb_vector out_vec = duckdb_data_chunk_get_vector(output, col);
        if (ducknng_quack_decode_vector_slice(r, &schema->cols[col], rows, offset, out_vec, copy_count, errmsg) != 0) return -1;
    }
    if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg) != 0) return -1;
    duckdb_data_chunk_set_size(output, copy_count);
    return 0;
}

int ducknng_quack_payload_scan_begin(const uint8_t *payload, size_t payload_len,
    const ducknng_quack_schema *schema, size_t *inout_offset, uint64_t *out_remaining,
    char **errmsg) {
    ducknng_quack_reader r;
    uint16_t field_id = 0;
    uint64_t n_results = 0;
    if (inout_offset) *inout_offset = 0;
    if (out_remaining) *out_remaining = 0;
    if (!payload || payload_len == 0 || !schema || !inout_offset || !out_remaining) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack payload scan state");
        return -1;
    }
    memset(&r, 0, sizeof(r));
    r.data = payload;
    r.len = payload_len;
    if (ducknng_quack_peek_u16le(&r, &field_id, errmsg) != 0) return -1;
    if (field_id == DUCKNNG_QUACK_OUTER_RESULT_TYPES) {
        if (ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_OUTER_RESULT_TYPES, errmsg) != 0 ||
            ducknng_quack_skip_type_list(&r, (uint64_t)schema->ncols, errmsg) != 0 ||
            ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_OUTER_RESULT_NAMES, errmsg) != 0 ||
            ducknng_quack_skip_string_list(&r, (uint64_t)schema->ncols, errmsg) != 0) return -1;
    }
    if (ducknng_quack_peek_u16le(&r, &field_id, errmsg) != 0) return -1;
    if (field_id != DUCKNNG_QUACK_OUTER_RESULTS) {
        *inout_offset = r.off;
        *out_remaining = 0;
        return 0;
    }
    if (ducknng_quack_read_u16le(&r, &field_id, errmsg) != 0 ||
        ducknng_quack_read_uleb128(&r, &n_results, errmsg) != 0) return -1;
    *inout_offset = r.off;
    *out_remaining = n_results;
    return 0;
}

int ducknng_quack_payload_scan_next(duckdb_data_chunk output,
    const uint8_t *payload, size_t payload_len, const ducknng_quack_schema *schema,
    size_t *inout_offset, uint64_t *inout_remaining, char **errmsg) {
    ducknng_quack_reader r;
    uint8_t present = 0;
    idx_t chunk_rows = 0;
    if (!output || !payload || payload_len == 0 || !schema || !inout_offset || !inout_remaining) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack payload scan inputs");
        return -1;
    }
    if (*inout_remaining == 0) {
        duckdb_data_chunk_set_size(output, 0);
        return 0;
    }
    if (*inout_offset > payload_len) {
        ducknng_quack_set_error(errmsg, "ducknng: quack payload scan offset is out of range");
        return -1;
    }
    memset(&r, 0, sizeof(r));
    r.data = payload;
    r.len = payload_len;
    r.off = *inout_offset;
    if (ducknng_quack_read_byte(&r, &present, errmsg) != 0 || !present ||
        ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_CHUNK_WRAPPER, errmsg) != 0 ||
        ducknng_quack_decode_data_chunk_slice(&r, output, schema, 0, &chunk_rows, errmsg) != 0) return -1;
    *inout_offset = r.off;
    (*inout_remaining)--;
    if (*inout_remaining == 0) {
        if (ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_FIELD_END, errmsg) != 0) return -1;
        *inout_offset = r.off;
    }
    (void)chunk_rows;
    return 0;
}

int ducknng_quack_payload_scan(duckdb_data_chunk output,
    const uint8_t *payload, size_t payload_len,
    const ducknng_quack_schema *schema, idx_t *inout_offset, char **errmsg) {
    ducknng_quack_reader r;
    uint16_t field_id = 0;
    uint64_t n_results = 0;
    uint64_t chunk_idx;
    idx_t global_offset = inout_offset ? *inout_offset : 0;
    idx_t seen_rows = 0;
    if (!output || !payload || payload_len == 0 || !schema || !inout_offset) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack payload scan inputs");
        return -1;
    }
    memset(&r, 0, sizeof(r));
    r.data = payload;
    r.len = payload_len;
    if (ducknng_quack_peek_u16le(&r, &field_id, errmsg) != 0) return -1;
    if (field_id == DUCKNNG_QUACK_OUTER_RESULT_TYPES) {
        if (ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_OUTER_RESULT_TYPES, errmsg) != 0 ||
            ducknng_quack_skip_type_list(&r, (uint64_t)schema->ncols, errmsg) != 0 ||
            ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_OUTER_RESULT_NAMES, errmsg) != 0 ||
            ducknng_quack_skip_string_list(&r, (uint64_t)schema->ncols, errmsg) != 0) return -1;
    }
    if (ducknng_quack_peek_u16le(&r, &field_id, errmsg) != 0) return -1;
    if (field_id != DUCKNNG_QUACK_OUTER_RESULTS) {
        duckdb_data_chunk_set_size(output, 0);
        return 0;
    }
    if (ducknng_quack_read_u16le(&r, &field_id, errmsg) != 0 ||
        ducknng_quack_read_uleb128(&r, &n_results, errmsg) != 0) return -1;
    for (chunk_idx = 0; chunk_idx < n_results; chunk_idx++) {
        uint8_t present = 0;
        idx_t chunk_rows = 0;
        idx_t local_offset;
        if (ducknng_quack_read_byte(&r, &present, errmsg) != 0 || !present ||
            ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_CHUNK_WRAPPER, errmsg) != 0) return -1;
        if (global_offset >= seen_rows) local_offset = global_offset - seen_rows;
        else local_offset = 0;
        if (ducknng_quack_decode_data_chunk_slice(&r, output, schema, local_offset, &chunk_rows, errmsg) != 0) return -1;
        if (global_offset < seen_rows + chunk_rows) {
            *inout_offset = global_offset + duckdb_data_chunk_get_size(output);
            return 0;
        }
        seen_rows += chunk_rows;
    }
    if (ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_FIELD_END, errmsg) != 0) return -1;
    duckdb_data_chunk_set_size(output, 0);
    return 0;
}
