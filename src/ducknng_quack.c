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
/* Nested logical type ids (DuckDB LogicalTypeId on the wire). */
#define DUCKNNG_QUACK_LOGICAL_STRUCT 100
#define DUCKNNG_QUACK_LOGICAL_LIST 101
#define DUCKNNG_QUACK_LOGICAL_MAP 102
#define DUCKNNG_QUACK_LOGICAL_ENUM 104
#define DUCKNNG_QUACK_LOGICAL_UNION 107
#define DUCKNNG_QUACK_LOGICAL_ARRAY 108
/* ExtraTypeInfo kinds (field 100 of a type-info object). */
#define DUCKNNG_QUACK_EXTRA_TYPE_LIST 4
#define DUCKNNG_QUACK_EXTRA_TYPE_STRUCT 5
#define DUCKNNG_QUACK_EXTRA_TYPE_ENUM 6
#define DUCKNNG_QUACK_EXTRA_TYPE_ARRAY 9
/* ExtraTypeInfo / nested vector field ids and bounds. */
#define DUCKNNG_QUACK_EXTRA_CHILD 200
#define DUCKNNG_QUACK_EXTRA_ARRAY_SIZE 201
#define DUCKNNG_QUACK_EXTRA_STRUCT_COUNT 200
#define DUCKNNG_QUACK_EXTRA_ENUM_COUNT 200
#define DUCKNNG_QUACK_EXTRA_ENUM_VALUES 201
#define DUCKNNG_QUACK_PAIR_FIRST 0
#define DUCKNNG_QUACK_PAIR_SECOND 1
#define DUCKNNG_QUACK_VECTOR_STRUCT_CHILDREN 103
#define DUCKNNG_QUACK_VECTOR_LIST_CHILD_SIZE 104
#define DUCKNNG_QUACK_VECTOR_ARRAY_SIZE 103
#define DUCKNNG_QUACK_VECTOR_LIST_ENTRIES 105
#define DUCKNNG_QUACK_VECTOR_LIST_CHILD 106
#define DUCKNNG_QUACK_VECTOR_ARRAY_CHILD 104
#define DUCKNNG_QUACK_ENTRY_OFFSET 100
#define DUCKNNG_QUACK_ENTRY_LENGTH 101
#define DUCKNNG_QUACK_MAX_NESTING 64u
#define DUCKNNG_QUACK_MAX_STRUCT_MEMBERS 65536u
#define DUCKNNG_QUACK_MAX_ENUM_VALUES (1u << 24)

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

/* Bytes a DuckDB validity mask occupies on the wire: ceil(rows/64) uint64 words. */
static size_t ducknng_quack_validity_bytes(idx_t rows) {
    return (size_t)(((uint64_t)rows + 63u) / 64u) * 8u;
}

/* Read a validity bit byte-wise so the wire blob need not be 8-byte aligned and
 * the result is endianness-independent. On little-endian this is bit-identical
 * to indexing the mask as uint64 words (bit row%64 of word row/64). */
static int ducknng_quack_vector_row_is_valid(const uint8_t *validity, idx_t row) {
    if (!validity) return 1;
    return (validity[row >> 3] >> (row & 7u)) & 1u;
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
    if (ducknng_size_add(w->len, add, &want) != 0) {
        ducknng_quack_set_error(errmsg, "ducknng: quack payload size overflow");
        return -1;
    }
    if (want <= w->cap) return 0;
    if (ducknng_grow_capacity(want, w->cap, 256, &cap) != 0) {
        ducknng_quack_set_error(errmsg, "ducknng: quack payload size overflow");
        return -1;
    }
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
    if (!r || r->off > r->len || n > r->len - r->off) {
        ducknng_quack_set_error(errmsg, "ducknng: truncated quack payload");
        return -1;
    }
    return 0;
}

static int ducknng_quack_uint64_to_idx(uint64_t value, idx_t *out,
    const char *message, char **errmsg) {
    idx_t converted = (idx_t)value;
    if ((uint64_t)converted != value) {
        ducknng_quack_set_error(errmsg, message ? message : "ducknng: quack integer exceeds supported size");
        return -1;
    }
    if (out) *out = converted;
    return 0;
}

static int ducknng_quack_size_to_idx(size_t value, idx_t *out,
    const char *message, char **errmsg) {
    return ducknng_quack_uint64_to_idx((uint64_t)value, out, message, errmsg);
}

static int ducknng_quack_idx_add(idx_t a, idx_t b, idx_t *out,
    const char *message, char **errmsg) {
    uint64_t av = (uint64_t)a;
    uint64_t bv = (uint64_t)b;
    uint64_t sum;
    if (bv > UINT64_MAX - av) {
        ducknng_quack_set_error(errmsg, message ? message : "ducknng: quack row count overflows supported size");
        return -1;
    }
    sum = av + bv;
    if ((uint64_t)(idx_t)sum != sum) {
        ducknng_quack_set_error(errmsg, message ? message : "ducknng: quack row count overflows supported size");
        return -1;
    }
    if (out) *out = (idx_t)sum;
    return 0;
}

static int ducknng_quack_idx_mul(idx_t a, idx_t b, idx_t *out,
    const char *message, char **errmsg) {
    uint64_t av = (uint64_t)a;
    uint64_t bv = (uint64_t)b;
    uint64_t product;
    if (bv != 0 && av > UINT64_MAX / bv) {
        ducknng_quack_set_error(errmsg, message ? message : "ducknng: quack row count overflows supported size");
        return -1;
    }
    product = av * bv;
    if ((uint64_t)(idx_t)product != product) {
        ducknng_quack_set_error(errmsg, message ? message : "ducknng: quack row count overflows supported size");
        return -1;
    }
    if (out) *out = (idx_t)product;
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
        /* The 10th byte (shift==63) may only carry bit 63; bits 0x7e would land
         * past 64 bits and overflow. Reject rather than silently truncate. */
        if (shift == 63 && (byte & 0x7eu) != 0) {
            ducknng_quack_set_error(errmsg, "ducknng: quack uleb128 integer overflows 64 bits");
            return -1;
        }
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
    if (len > (uint64_t)SIZE_MAX) {
        ducknng_quack_set_error(errmsg, "ducknng: quack blob length exceeds supported size");
        return -1;
    }
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

/* ===== Nested type-node codec (recursive; ported from Rducks quack_core.c) ===== */

static void ducknng_quack_node_free(ducknng_quack_column_schema *node);
static int ducknng_quack_node_from_duckdb(duckdb_logical_type type, unsigned depth,
    ducknng_quack_column_schema **out, char **errmsg);
static int ducknng_quack_node_to_duckdb(const ducknng_quack_column_schema *node, unsigned depth,
    duckdb_logical_type *out_type, char **errmsg);
static int ducknng_quack_write_type_node(ducknng_quack_writer *w,
    const ducknng_quack_column_schema *node, unsigned depth, char **errmsg);
static int ducknng_quack_read_type_node(ducknng_quack_reader *r, unsigned depth,
    ducknng_quack_column_schema **out, char **errmsg);

static ducknng_quack_column_schema *ducknng_quack_node_new(int logical_type_id) {
    ducknng_quack_column_schema *n = (ducknng_quack_column_schema *)duckdb_malloc(sizeof(*n));
    if (!n) return NULL;
    memset(n, 0, sizeof(*n));
    n->logical_type_id = logical_type_id;
    return n;
}

/* Append a child node (transfers ownership of `child`) with a member name. */
static int ducknng_quack_node_add_child(ducknng_quack_column_schema *node,
    ducknng_quack_column_schema *child, const char *name) {
    ducknng_quack_column_schema **children;
    char **names;
    char *name_dup;
    uint32_t n;
    if (!node || !child) return -1;
    n = node->nchildren;
    children = (ducknng_quack_column_schema **)duckdb_malloc(sizeof(*children) * ((size_t)n + 1));
    if (!children) return -1;
    names = (char **)duckdb_malloc(sizeof(*names) * ((size_t)n + 1));
    if (!names) { duckdb_free(children); return -1; }
    name_dup = ducknng_strdup(name ? name : "");
    if (!name_dup) { duckdb_free(children); duckdb_free(names); return -1; }
    if (node->children && n) memcpy(children, node->children, sizeof(*children) * n);
    if (node->child_names && n) memcpy(names, node->child_names, sizeof(*names) * n);
    children[n] = child;
    names[n] = name_dup;
    if (node->children) duckdb_free(node->children);
    if (node->child_names) duckdb_free(node->child_names);
    node->children = children;
    node->child_names = names;
    node->nchildren = n + 1;
    return 0;
}

static int ducknng_quack_node_is_nested(const ducknng_quack_column_schema *node) {
    if (!node) return 0;
    switch (node->logical_type_id) {
    case DUCKNNG_QUACK_LOGICAL_LIST:
    case DUCKNNG_QUACK_LOGICAL_MAP:
    case DUCKNNG_QUACK_LOGICAL_STRUCT:
    case DUCKNNG_QUACK_LOGICAL_UNION:
    case DUCKNNG_QUACK_LOGICAL_ARRAY:
        return 1;
    default:
        return 0;
    }
}

static int ducknng_quack_node_is_varlen(const ducknng_quack_column_schema *node) {
    return node && (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_VARCHAR ||
        node->logical_type_id == DUCKNNG_QUACK_LOGICAL_CHAR ||
        node->logical_type_id == DUCKNNG_QUACK_LOGICAL_BLOB);
}

/* Fixed physical width in bytes for a fixed-width leaf (0 if not fixed-width). */
static size_t ducknng_quack_node_fixed_width(const ducknng_quack_column_schema *node) {
    ducknng_quack_type_meta meta;
    size_t size = 0;
    if (!node) return 0;
    if (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_ENUM) {
        if (node->enum_count <= 0xffu) return 1;
        if (node->enum_count <= 0xffffu) return 2;
        return 4;
    }
    memset(&meta, 0, sizeof(meta));
    meta.logical_type_id = node->logical_type_id;
    meta.decimal_width = node->decimal_width;
    if (ducknng_quack_meta_fixed_size(&meta, &size) != 0) return 0;
    return size;
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
    ducknng_quack_column_schema *node = NULL;
    if (ducknng_quack_read_type_node(r, 0, &node, errmsg) != 0) return -1;
    ducknng_quack_node_free(node);
    return 0;
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

static int ducknng_quack_node_from_duckdb(duckdb_logical_type type, unsigned depth,
    ducknng_quack_column_schema **out, char **errmsg) {
    duckdb_type id;
    ducknng_quack_column_schema *node = NULL;
    if (out) *out = NULL;
    if (!type) {
        ducknng_quack_set_error(errmsg, "ducknng: missing DuckDB logical type for quack encoding");
        return -1;
    }
    if (depth > DUCKNNG_QUACK_MAX_NESTING) {
        ducknng_quack_set_error(errmsg, "ducknng: quack type nesting exceeds supported depth");
        return -1;
    }
    id = duckdb_get_type_id(type);
    switch (id) {
    case DUCKDB_TYPE_LIST:
    case DUCKDB_TYPE_ARRAY: {
        duckdb_logical_type child_t = (id == DUCKDB_TYPE_LIST)
            ? duckdb_list_type_child_type(type) : duckdb_array_type_child_type(type);
        ducknng_quack_column_schema *child = NULL;
        int rc;
        node = ducknng_quack_node_new(id == DUCKDB_TYPE_LIST
            ? DUCKNNG_QUACK_LOGICAL_LIST : DUCKNNG_QUACK_LOGICAL_ARRAY);
        if (!node) { if (child_t) duckdb_destroy_logical_type(&child_t); goto oom; }
        if (id == DUCKDB_TYPE_ARRAY) node->array_size = (uint32_t)duckdb_array_type_array_size(type);
        rc = ducknng_quack_node_from_duckdb(child_t, depth + 1, &child, errmsg);
        if (child_t) duckdb_destroy_logical_type(&child_t);
        if (rc != 0) { ducknng_quack_node_free(node); return -1; }
        if (ducknng_quack_node_add_child(node, child, "") != 0) {
            ducknng_quack_node_free(child); ducknng_quack_node_free(node); goto oom;
        }
        break;
    }
    case DUCKDB_TYPE_STRUCT:
    case DUCKDB_TYPE_UNION: {
        idx_t count = (id == DUCKDB_TYPE_STRUCT)
            ? duckdb_struct_type_child_count(type) : duckdb_union_type_member_count(type);
        idx_t i;
        node = ducknng_quack_node_new(id == DUCKDB_TYPE_STRUCT
            ? DUCKNNG_QUACK_LOGICAL_STRUCT : DUCKNNG_QUACK_LOGICAL_UNION);
        if (!node) goto oom;
        if (id == DUCKDB_TYPE_UNION) {
            /* DuckDB UNION physical layout: leading UTINYINT tag (name "") then members. */
            ducknng_quack_column_schema *tag = ducknng_quack_node_new(DUCKNNG_QUACK_LOGICAL_UTINYINT);
            if (!tag) { ducknng_quack_node_free(node); goto oom; }
            if (ducknng_quack_node_add_child(node, tag, "") != 0) {
                ducknng_quack_node_free(tag); ducknng_quack_node_free(node); goto oom;
            }
        }
        for (i = 0; i < count; i++) {
            duckdb_logical_type child_t = (id == DUCKDB_TYPE_STRUCT)
                ? duckdb_struct_type_child_type(type, i) : duckdb_union_type_member_type(type, i);
            char *child_name = (id == DUCKDB_TYPE_STRUCT)
                ? duckdb_struct_type_child_name(type, i) : duckdb_union_type_member_name(type, i);
            ducknng_quack_column_schema *child = NULL;
            int rc = ducknng_quack_node_from_duckdb(child_t, depth + 1, &child, errmsg);
            if (child_t) duckdb_destroy_logical_type(&child_t);
            if (rc != 0) { if (child_name) duckdb_free(child_name); ducknng_quack_node_free(node); return -1; }
            if (ducknng_quack_node_add_child(node, child, child_name ? child_name : "") != 0) {
                if (child_name) duckdb_free(child_name);
                ducknng_quack_node_free(child); ducknng_quack_node_free(node); goto oom;
            }
            if (child_name) duckdb_free(child_name);
        }
        break;
    }
    case DUCKDB_TYPE_MAP: {
        duckdb_logical_type key_t = duckdb_map_type_key_type(type);
        duckdb_logical_type value_t = duckdb_map_type_value_type(type);
        ducknng_quack_column_schema *entry = NULL, *key_node = NULL, *value_node = NULL;
        int rc;
        node = ducknng_quack_node_new(DUCKNNG_QUACK_LOGICAL_MAP);
        entry = ducknng_quack_node_new(DUCKNNG_QUACK_LOGICAL_STRUCT);
        if (!node || !entry) {
            if (key_t) duckdb_destroy_logical_type(&key_t);
            if (value_t) duckdb_destroy_logical_type(&value_t);
            ducknng_quack_node_free(node); ducknng_quack_node_free(entry); goto oom;
        }
        rc = ducknng_quack_node_from_duckdb(key_t, depth + 2, &key_node, errmsg);
        if (key_t) duckdb_destroy_logical_type(&key_t);
        if (rc == 0) rc = ducknng_quack_node_from_duckdb(value_t, depth + 2, &value_node, errmsg);
        if (value_t) duckdb_destroy_logical_type(&value_t);
        if (rc != 0) { ducknng_quack_node_free(key_node); ducknng_quack_node_free(entry); ducknng_quack_node_free(node); return -1; }
        if (ducknng_quack_node_add_child(entry, key_node, "key") != 0) {
            ducknng_quack_node_free(key_node); ducknng_quack_node_free(value_node);
            ducknng_quack_node_free(entry); ducknng_quack_node_free(node); goto oom;
        }
        if (ducknng_quack_node_add_child(entry, value_node, "value") != 0) {
            ducknng_quack_node_free(value_node); ducknng_quack_node_free(entry); ducknng_quack_node_free(node); goto oom;
        }
        if (ducknng_quack_node_add_child(node, entry, "") != 0) {
            ducknng_quack_node_free(entry); ducknng_quack_node_free(node); goto oom;
        }
        break;
    }
    case DUCKDB_TYPE_ENUM: {
        idx_t size = duckdb_enum_dictionary_size(type);
        idx_t i;
        node = ducknng_quack_node_new(DUCKNNG_QUACK_LOGICAL_ENUM);
        if (!node) goto oom;
        node->enum_count = (uint32_t)size;
        node->enum_labels = (char **)duckdb_malloc(sizeof(char *) * (size ? (size_t)size : 1));
        if (!node->enum_labels) { ducknng_quack_node_free(node); goto oom; }
        memset(node->enum_labels, 0, sizeof(char *) * (size ? (size_t)size : 1));
        for (i = 0; i < size; i++) {
            char *label = duckdb_enum_dictionary_value(type, i);
            node->enum_labels[i] = ducknng_strdup(label ? label : "");
            if (label) duckdb_free(label);
            if (!node->enum_labels[i]) { ducknng_quack_node_free(node); goto oom; }
        }
        break;
    }
    default: {
        ducknng_quack_type_meta meta;
        if (ducknng_quack_duckdb_type_to_meta(type, &meta, errmsg) != 0) return -1;
        node = ducknng_quack_node_new(meta.logical_type_id);
        if (!node) goto oom;
        node->decimal_width = meta.decimal_width;
        node->decimal_scale = meta.decimal_scale;
        break;
    }
    }
    *out = node;
    return 0;
oom:
    ducknng_quack_set_error(errmsg, "ducknng: out of memory building quack type node");
    return -1;
}

static int ducknng_quack_node_to_duckdb(const ducknng_quack_column_schema *node, unsigned depth,
    duckdb_logical_type *out_type, char **errmsg) {
    if (out_type) *out_type = NULL;
    if (!node) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack type node");
        return -1;
    }
    if (depth > DUCKNNG_QUACK_MAX_NESTING) {
        ducknng_quack_set_error(errmsg, "ducknng: quack type nesting exceeds supported depth");
        return -1;
    }
    switch (node->logical_type_id) {
    case DUCKNNG_QUACK_LOGICAL_LIST:
    case DUCKNNG_QUACK_LOGICAL_ARRAY: {
        duckdb_logical_type child = NULL;
        if (node->nchildren != 1) {
            ducknng_quack_set_error(errmsg, "ducknng: quack list/array type requires exactly one child");
            return -1;
        }
        if (ducknng_quack_node_to_duckdb(node->children[0], depth + 1, &child, errmsg) != 0) return -1;
        *out_type = (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_LIST)
            ? duckdb_create_list_type(child) : duckdb_create_array_type(child, node->array_size);
        duckdb_destroy_logical_type(&child);
        break;
    }
    case DUCKNNG_QUACK_LOGICAL_STRUCT:
    case DUCKNNG_QUACK_LOGICAL_UNION: {
        uint32_t start = (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_UNION) ? 1u : 0u;
        uint32_t n = (node->nchildren > start) ? node->nchildren - start : 0u;
        duckdb_logical_type *types = NULL;
        const char **names = NULL;
        uint32_t i;
        int ok = 1;
        if (n == 0) { ducknng_quack_set_error(errmsg, "ducknng: quack struct/union type has no members"); return -1; }
        types = (duckdb_logical_type *)duckdb_malloc(sizeof(*types) * n);
        names = (const char **)duckdb_malloc(sizeof(*names) * n);
        if (!types || !names) { if (types) duckdb_free(types); if (names) duckdb_free(names);
            ducknng_quack_set_error(errmsg, "ducknng: out of memory building struct/union type"); return -1; }
        memset(types, 0, sizeof(*types) * n);
        for (i = 0; i < n; i++) {
            names[i] = node->child_names ? node->child_names[start + i] : "";
            if (ducknng_quack_node_to_duckdb(node->children[start + i], depth + 1, &types[i], errmsg) != 0) { ok = 0; break; }
        }
        if (ok) {
            *out_type = (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_STRUCT)
                ? duckdb_create_struct_type(types, names, n)
                : duckdb_create_union_type(types, names, n);
        }
        for (i = 0; i < n; i++) if (types[i]) duckdb_destroy_logical_type(&types[i]);
        duckdb_free(types);
        duckdb_free(names);
        if (!ok) return -1;
        break;
    }
    case DUCKNNG_QUACK_LOGICAL_MAP: {
        duckdb_logical_type key_t = NULL, value_t = NULL;
        const ducknng_quack_column_schema *entry;
        if (node->nchildren != 1 || !node->children[0] ||
            node->children[0]->logical_type_id != DUCKNNG_QUACK_LOGICAL_STRUCT ||
            node->children[0]->nchildren != 2) {
            ducknng_quack_set_error(errmsg, "ducknng: malformed quack map type");
            return -1;
        }
        entry = node->children[0];
        if (ducknng_quack_node_to_duckdb(entry->children[0], depth + 2, &key_t, errmsg) != 0) return -1;
        if (ducknng_quack_node_to_duckdb(entry->children[1], depth + 2, &value_t, errmsg) != 0) {
            duckdb_destroy_logical_type(&key_t); return -1;
        }
        *out_type = duckdb_create_map_type(key_t, value_t);
        duckdb_destroy_logical_type(&key_t);
        duckdb_destroy_logical_type(&value_t);
        break;
    }
    case DUCKNNG_QUACK_LOGICAL_ENUM: {
        *out_type = duckdb_create_enum_type((const char **)node->enum_labels, node->enum_count);
        break;
    }
    default: {
        ducknng_quack_type_meta meta;
        memset(&meta, 0, sizeof(meta));
        meta.logical_type_id = node->logical_type_id;
        meta.decimal_width = node->decimal_width;
        meta.decimal_scale = node->decimal_scale;
        return ducknng_quack_meta_to_duckdb_type(&meta, out_type, errmsg);
    }
    }
    if (!*out_type) {
        ducknng_quack_set_error(errmsg, "ducknng: failed to allocate DuckDB logical type for quack node");
        return -1;
    }
    return 0;
}

static int ducknng_quack_type_needs_info(const ducknng_quack_column_schema *node) {
    switch (node->logical_type_id) {
    case DUCKNNG_QUACK_LOGICAL_DECIMAL:
    case DUCKNNG_QUACK_LOGICAL_LIST:
    case DUCKNNG_QUACK_LOGICAL_MAP:
    case DUCKNNG_QUACK_LOGICAL_STRUCT:
    case DUCKNNG_QUACK_LOGICAL_UNION:
    case DUCKNNG_QUACK_LOGICAL_ENUM:
    case DUCKNNG_QUACK_LOGICAL_ARRAY:
        return 1;
    default:
        return 0;
    }
}

static int ducknng_quack_write_child_pair(ducknng_quack_writer *w, const char *name,
    const ducknng_quack_column_schema *child, unsigned depth, char **errmsg) {
    if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_PAIR_FIRST, errmsg) != 0 ||
        ducknng_quack_write_string(w, name ? name : "", errmsg) != 0 ||
        ducknng_quack_write_field_id(w, DUCKNNG_QUACK_PAIR_SECOND, errmsg) != 0) return -1;
    if (ducknng_quack_write_type_node(w, child, depth, errmsg) != 0) return -1;
    return ducknng_quack_write_field_end(w, errmsg);
}

static int ducknng_quack_write_type_node(ducknng_quack_writer *w,
    const ducknng_quack_column_schema *node, unsigned depth, char **errmsg) {
    uint32_t i;
    if (!w || !node) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack type node");
        return -1;
    }
    if (depth > DUCKNNG_QUACK_MAX_NESTING) {
        ducknng_quack_set_error(errmsg, "ducknng: quack type nesting exceeds supported depth");
        return -1;
    }
    if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_TYPE_ID, errmsg) != 0 ||
        ducknng_quack_write_uleb128(w, (uint64_t)node->logical_type_id, errmsg) != 0) return -1;
    if (ducknng_quack_type_needs_info(node)) {
        if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_TYPE_INFO, errmsg) != 0 ||
            ducknng_quack_write_byte(w, 1, errmsg) != 0 ||
            ducknng_quack_write_field_id(w, DUCKNNG_QUACK_EXTRA_INFO_KIND, errmsg) != 0) return -1;
        switch (node->logical_type_id) {
        case DUCKNNG_QUACK_LOGICAL_DECIMAL:
            if (ducknng_quack_write_uleb128(w, DUCKNNG_QUACK_EXTRA_TYPE_DECIMAL, errmsg) != 0) return -1;
            if (node->decimal_width &&
                (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_EXTRA_DECIMAL_WIDTH, errmsg) != 0 ||
                 ducknng_quack_write_uleb128(w, node->decimal_width, errmsg) != 0)) return -1;
            if (node->decimal_scale &&
                (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_EXTRA_DECIMAL_SCALE, errmsg) != 0 ||
                 ducknng_quack_write_uleb128(w, node->decimal_scale, errmsg) != 0)) return -1;
            break;
        case DUCKNNG_QUACK_LOGICAL_LIST:
        case DUCKNNG_QUACK_LOGICAL_MAP:
            if (node->nchildren != 1) {
                ducknng_quack_set_error(errmsg, "ducknng: quack list/map type requires one child");
                return -1;
            }
            if (ducknng_quack_write_uleb128(w, DUCKNNG_QUACK_EXTRA_TYPE_LIST, errmsg) != 0 ||
                ducknng_quack_write_field_id(w, DUCKNNG_QUACK_EXTRA_CHILD, errmsg) != 0 ||
                ducknng_quack_write_type_node(w, node->children[0], depth + 1, errmsg) != 0) return -1;
            break;
        case DUCKNNG_QUACK_LOGICAL_STRUCT:
        case DUCKNNG_QUACK_LOGICAL_UNION:
            if (ducknng_quack_write_uleb128(w, DUCKNNG_QUACK_EXTRA_TYPE_STRUCT, errmsg) != 0 ||
                ducknng_quack_write_field_id(w, DUCKNNG_QUACK_EXTRA_STRUCT_COUNT, errmsg) != 0 ||
                ducknng_quack_write_uleb128(w, node->nchildren, errmsg) != 0) return -1;
            for (i = 0; i < node->nchildren; i++) {
                const char *nm = (node->child_names && node->child_names[i]) ? node->child_names[i] : "";
                if (ducknng_quack_write_child_pair(w, nm, node->children[i], depth + 1, errmsg) != 0) return -1;
            }
            break;
        case DUCKNNG_QUACK_LOGICAL_ENUM:
            if (ducknng_quack_write_uleb128(w, DUCKNNG_QUACK_EXTRA_TYPE_ENUM, errmsg) != 0 ||
                ducknng_quack_write_field_id(w, DUCKNNG_QUACK_EXTRA_ENUM_COUNT, errmsg) != 0 ||
                ducknng_quack_write_uleb128(w, node->enum_count, errmsg) != 0 ||
                ducknng_quack_write_field_id(w, DUCKNNG_QUACK_EXTRA_ENUM_VALUES, errmsg) != 0 ||
                ducknng_quack_write_uleb128(w, node->enum_count, errmsg) != 0) return -1;
            for (i = 0; i < node->enum_count; i++) {
                const char *label = (node->enum_labels && node->enum_labels[i]) ? node->enum_labels[i] : "";
                if (ducknng_quack_write_string(w, label, errmsg) != 0) return -1;
            }
            break;
        case DUCKNNG_QUACK_LOGICAL_ARRAY:
            if (node->nchildren != 1) {
                ducknng_quack_set_error(errmsg, "ducknng: quack array type requires one child");
                return -1;
            }
            if (ducknng_quack_write_uleb128(w, DUCKNNG_QUACK_EXTRA_TYPE_ARRAY, errmsg) != 0 ||
                ducknng_quack_write_field_id(w, DUCKNNG_QUACK_EXTRA_CHILD, errmsg) != 0 ||
                ducknng_quack_write_type_node(w, node->children[0], depth + 1, errmsg) != 0) return -1;
            if (node->array_size &&
                (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_EXTRA_ARRAY_SIZE, errmsg) != 0 ||
                 ducknng_quack_write_uleb128(w, node->array_size, errmsg) != 0)) return -1;
            break;
        default:
            ducknng_quack_set_error(errmsg, "ducknng: internal error writing quack type info");
            return -1;
        }
        if (ducknng_quack_write_field_end(w, errmsg) != 0) return -1; /* end type_info */
    }
    return ducknng_quack_write_field_end(w, errmsg); /* end LogicalType */
}

static int ducknng_quack_read_child_pair(ducknng_quack_reader *r, char **name_out,
    ducknng_quack_column_schema **type_out, unsigned depth, char **errmsg) {
    uint16_t field = 0;
    int seen_name = 0, seen_type = 0;
    char *name = NULL;
    *name_out = NULL;
    *type_out = NULL;
    for (;;) {
        if (ducknng_quack_read_u16le(r, &field, errmsg) != 0) goto fail;
        if (field == DUCKNNG_QUACK_FIELD_END) break;
        if (field == DUCKNNG_QUACK_PAIR_FIRST) {
            if (seen_name) { ducknng_quack_set_error(errmsg, "ducknng: duplicate name in quack struct member"); goto fail; }
            seen_name = 1;
            if (ducknng_quack_read_string_dup(r, &name, errmsg) != 0) goto fail;
        } else if (field == DUCKNNG_QUACK_PAIR_SECOND) {
            if (seen_type) { ducknng_quack_set_error(errmsg, "ducknng: duplicate type in quack struct member"); goto fail; }
            seen_type = 1;
            if (ducknng_quack_read_type_node(r, depth, type_out, errmsg) != 0) goto fail;
        } else {
            ducknng_quack_set_error(errmsg, "ducknng: unexpected field in quack struct member");
            goto fail;
        }
    }
    if (!*type_out) { ducknng_quack_set_error(errmsg, "ducknng: quack struct member missing its type"); goto fail; }
    if (!name) { name = ducknng_strdup(""); if (!name) goto fail; }
    *name_out = name;
    return 0;
fail:
    if (name) duckdb_free(name);
    if (*type_out) { ducknng_quack_node_free(*type_out); *type_out = NULL; }
    return -1;
}

static int ducknng_quack_read_type_info(ducknng_quack_reader *r, ducknng_quack_column_schema *t,
    unsigned depth, char **errmsg) {
    uint16_t field = 0;
    uint64_t value = 0;
    uint64_t i, count;
    uint32_t seen = 0;
    for (;;) {
        if (ducknng_quack_read_u16le(r, &field, errmsg) != 0) return -1;
        if (field == DUCKNNG_QUACK_FIELD_END) return 0;
        if (field == 100 || field == 101 || field == 103 || field == 200 || field == 201) {
            uint32_t bit = (field < 200) ? (1u << (field - 100)) : (1u << (field - 200 + 5));
            if (seen & bit) { ducknng_quack_set_error(errmsg, "ducknng: duplicate field in quack type info"); return -1; }
            seen |= bit;
        }
        switch (field) {
        case 100: /* extra-info kind (validated by the logical id below) */
            if (ducknng_quack_read_uleb128(r, &value, errmsg) != 0) return -1;
            break;
        case 101: /* alias */
            if (ducknng_quack_skip_string(r, errmsg) != 0) return -1;
            break;
        case 103: { /* extension info pointer */
            uint8_t present = 0;
            if (ducknng_quack_read_byte(r, &present, errmsg) != 0) return -1;
            if (present) { ducknng_quack_set_error(errmsg, "ducknng: extension type info is not supported"); return -1; }
            break;
        }
        case DUCKNNG_QUACK_EXTRA_CHILD: /* 200 */
            switch (t->logical_type_id) {
            case DUCKNNG_QUACK_LOGICAL_DECIMAL:
                if (ducknng_quack_read_uleb128(r, &value, errmsg) != 0) return -1;
                if (value > 38) { ducknng_quack_set_error(errmsg, "ducknng: quack decimal width exceeds 38"); return -1; }
                t->decimal_width = (uint8_t)value;
                break;
            case DUCKNNG_QUACK_LOGICAL_LIST:
            case DUCKNNG_QUACK_LOGICAL_MAP:
            case DUCKNNG_QUACK_LOGICAL_ARRAY: {
                ducknng_quack_column_schema *child = NULL;
                if (ducknng_quack_read_type_node(r, depth + 1, &child, errmsg) != 0) return -1;
                if (ducknng_quack_node_add_child(t, child, "") != 0) {
                    ducknng_quack_node_free(child);
                    ducknng_quack_set_error(errmsg, "ducknng: out of memory decoding nested quack type");
                    return -1;
                }
                break;
            }
            case DUCKNNG_QUACK_LOGICAL_STRUCT:
            case DUCKNNG_QUACK_LOGICAL_UNION:
                if (ducknng_quack_read_uleb128(r, &count, errmsg) != 0) return -1;
                if (count > DUCKNNG_QUACK_MAX_STRUCT_MEMBERS) { ducknng_quack_set_error(errmsg, "ducknng: quack struct member count exceeds limit"); return -1; }
                for (i = 0; i < count; i++) {
                    char *name = NULL;
                    ducknng_quack_column_schema *child = NULL;
                    if (ducknng_quack_read_child_pair(r, &name, &child, depth + 1, errmsg) != 0) return -1;
                    if (ducknng_quack_node_add_child(t, child, name) != 0) {
                        if (name) duckdb_free(name);
                        ducknng_quack_node_free(child);
                        ducknng_quack_set_error(errmsg, "ducknng: out of memory decoding quack struct members");
                        return -1;
                    }
                    if (name) duckdb_free(name);
                }
                break;
            case DUCKNNG_QUACK_LOGICAL_ENUM:
                if (ducknng_quack_read_uleb128(r, &value, errmsg) != 0) return -1;
                if (value > DUCKNNG_QUACK_MAX_ENUM_VALUES) { ducknng_quack_set_error(errmsg, "ducknng: quack enum size exceeds limit"); return -1; }
                t->enum_count = (uint32_t)value;
                break;
            default:
                ducknng_quack_set_error(errmsg, "ducknng: unexpected field 200 in quack type info");
                return -1;
            }
            break;
        case DUCKNNG_QUACK_EXTRA_ARRAY_SIZE: /* 201 */
            switch (t->logical_type_id) {
            case DUCKNNG_QUACK_LOGICAL_DECIMAL:
                if (ducknng_quack_read_uleb128(r, &value, errmsg) != 0) return -1;
                if (value > 38) { ducknng_quack_set_error(errmsg, "ducknng: quack decimal scale exceeds 38"); return -1; }
                t->decimal_scale = (uint8_t)value;
                break;
            case DUCKNNG_QUACK_LOGICAL_ARRAY:
                if (ducknng_quack_read_uleb128(r, &value, errmsg) != 0) return -1;
                if (value > 0xffffffffu) { ducknng_quack_set_error(errmsg, "ducknng: quack array size exceeds limit"); return -1; }
                t->array_size = (uint32_t)value;
                break;
            case DUCKNNG_QUACK_LOGICAL_ENUM: {
                uint64_t n = 0;
                if (ducknng_quack_read_uleb128(r, &n, errmsg) != 0) return -1;
                if (t->enum_count && n != t->enum_count) { ducknng_quack_set_error(errmsg, "ducknng: quack enum values disagree with count"); return -1; }
                if (n > DUCKNNG_QUACK_MAX_ENUM_VALUES) { ducknng_quack_set_error(errmsg, "ducknng: quack enum size exceeds limit"); return -1; }
                t->enum_count = (uint32_t)n;
                t->enum_labels = (char **)duckdb_malloc(sizeof(char *) * (n ? (size_t)n : 1));
                if (!t->enum_labels) { ducknng_quack_set_error(errmsg, "ducknng: out of memory decoding quack enum labels"); return -1; }
                memset(t->enum_labels, 0, sizeof(char *) * (n ? (size_t)n : 1));
                for (i = 0; i < n; i++) {
                    if (ducknng_quack_read_string_dup(r, &t->enum_labels[i], errmsg) != 0) return -1;
                }
                break;
            }
            default:
                ducknng_quack_set_error(errmsg, "ducknng: unexpected field 201 in quack type info");
                return -1;
            }
            break;
        default:
            ducknng_quack_set_error(errmsg, "ducknng: unknown field in quack type info");
            return -1;
        }
    }
}

static int ducknng_quack_read_type_node(ducknng_quack_reader *r, unsigned depth,
    ducknng_quack_column_schema **out, char **errmsg) {
    ducknng_quack_column_schema *t = NULL;
    uint16_t field = 0;
    uint64_t value = 0;
    int seen_id = 0, seen_info = 0;
    if (out) *out = NULL;
    if (depth > DUCKNNG_QUACK_MAX_NESTING) {
        ducknng_quack_set_error(errmsg, "ducknng: quack type nesting exceeds supported depth");
        return -1;
    }
    t = ducknng_quack_node_new(0);
    if (!t) { ducknng_quack_set_error(errmsg, "ducknng: out of memory decoding quack type"); return -1; }
    for (;;) {
        if (ducknng_quack_read_u16le(r, &field, errmsg) != 0) goto fail;
        if (field == DUCKNNG_QUACK_FIELD_END) break;
        switch (field) {
        case DUCKNNG_QUACK_TYPE_ID: /* 100 */
            if (seen_id) { ducknng_quack_set_error(errmsg, "ducknng: duplicate type id in quack logical type"); goto fail; }
            if (ducknng_quack_read_uleb128(r, &value, errmsg) != 0) goto fail;
            t->logical_type_id = (int)value;
            seen_id = 1;
            break;
        case DUCKNNG_QUACK_TYPE_INFO: { /* 101 */
            uint8_t present = 0;
            if (!seen_id) { ducknng_quack_set_error(errmsg, "ducknng: quack type info precedes its id"); goto fail; }
            if (seen_info) { ducknng_quack_set_error(errmsg, "ducknng: duplicate type info in quack logical type"); goto fail; }
            seen_info = 1;
            if (ducknng_quack_read_byte(r, &present, errmsg) != 0) goto fail;
            if (present && ducknng_quack_read_type_info(r, t, depth, errmsg) != 0) goto fail;
            break;
        }
        default:
            ducknng_quack_set_error(errmsg, "ducknng: unknown field in quack logical type");
            goto fail;
        }
    }
    if (!seen_id) { ducknng_quack_set_error(errmsg, "ducknng: quack logical type missing its id"); goto fail; }
    *out = t;
    return 0;
fail:
    ducknng_quack_node_free(t);
    return -1;
}

static int ducknng_quack_encode_validity_blob(ducknng_quack_writer *w,
    const uint64_t *validity, idx_t rows, char **errmsg) {
    size_t n_words = (size_t)((rows + 63) / 64);
    return ducknng_quack_write_blob(w, (const uint8_t *)validity, n_words * sizeof(uint64_t), errmsg);
}

static int ducknng_quack_encode_vector(ducknng_quack_writer *w, duckdb_vector vec,
    const ducknng_quack_column_schema *node, idx_t rows, unsigned depth, char **errmsg) {
    uint64_t *validity = NULL;
    int has_validity = 0;
    size_t width = 0;
    if (!w || !vec || !node) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack vector encoder state");
        return -1;
    }
    if (depth > DUCKNNG_QUACK_MAX_NESTING) {
        ducknng_quack_set_error(errmsg, "ducknng: quack vector nesting exceeds supported depth");
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
    if (ducknng_quack_node_is_varlen(node)) {
        duckdb_string_t *data = (duckdb_string_t *)duckdb_vector_get_data(vec);
        idx_t row;
        if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_VECTOR_DATA, errmsg) != 0 ||
            ducknng_quack_write_uleb128(w, (uint64_t)rows, errmsg) != 0) return -1;
        for (row = 0; row < rows; row++) {
            const uint8_t *ptr = NULL;
            size_t len = 0;
            if (ducknng_quack_vector_row_is_valid((const uint8_t *)validity, row)) {
                duckdb_string_t *value = &data[row];
                len = (size_t)duckdb_string_t_length(*value);
                ptr = (const uint8_t *)duckdb_string_t_data(value);
            }
            if (ducknng_quack_write_string_len(w, ptr, len, errmsg) != 0) return -1;
        }
        return ducknng_quack_write_field_end(w, errmsg);
    }
    if (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_STRUCT ||
        node->logical_type_id == DUCKNNG_QUACK_LOGICAL_UNION) {
        uint32_t i;
        if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_VECTOR_STRUCT_CHILDREN, errmsg) != 0 ||
            ducknng_quack_write_uleb128(w, node->nchildren, errmsg) != 0) return -1;
        for (i = 0; i < node->nchildren; i++) {
            duckdb_vector child = duckdb_struct_vector_get_child(vec, i);
            if (ducknng_quack_encode_vector(w, child, node->children[i], rows, depth + 1, errmsg) != 0) return -1;
        }
        return ducknng_quack_write_field_end(w, errmsg);
    }
    if (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_LIST ||
        node->logical_type_id == DUCKNNG_QUACK_LOGICAL_MAP) {
        idx_t child_size = duckdb_list_vector_get_size(vec);
        duckdb_list_entry *entries = (duckdb_list_entry *)duckdb_vector_get_data(vec);
        duckdb_vector child = duckdb_list_vector_get_child(vec);
        idx_t row;
        if (node->nchildren != 1) {
            ducknng_quack_set_error(errmsg, "ducknng: quack list/map vector has no child type");
            return -1;
        }
        if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_VECTOR_LIST_CHILD_SIZE, errmsg) != 0 ||
            ducknng_quack_write_uleb128(w, (uint64_t)child_size, errmsg) != 0 ||
            ducknng_quack_write_field_id(w, DUCKNNG_QUACK_VECTOR_LIST_ENTRIES, errmsg) != 0 ||
            ducknng_quack_write_uleb128(w, (uint64_t)rows, errmsg) != 0) return -1;
        for (row = 0; row < rows; row++) {
            uint64_t off = 0, len = 0;
            if (!validity || ducknng_quack_vector_row_is_valid((const uint8_t *)validity, row)) {
                off = entries[row].offset;
                len = entries[row].length;
            }
            if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_ENTRY_OFFSET, errmsg) != 0 ||
                ducknng_quack_write_uleb128(w, off, errmsg) != 0 ||
                ducknng_quack_write_field_id(w, DUCKNNG_QUACK_ENTRY_LENGTH, errmsg) != 0 ||
                ducknng_quack_write_uleb128(w, len, errmsg) != 0 ||
                ducknng_quack_write_field_end(w, errmsg) != 0) return -1;
        }
        if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_VECTOR_LIST_CHILD, errmsg) != 0) return -1;
        if (ducknng_quack_encode_vector(w, child, node->children[0], child_size, depth + 1, errmsg) != 0) return -1;
        return ducknng_quack_write_field_end(w, errmsg);
    }
    if (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_ARRAY) {
        duckdb_vector child = duckdb_array_vector_get_child(vec);
        idx_t child_rows;
        if (node->nchildren != 1) {
            ducknng_quack_set_error(errmsg, "ducknng: quack array vector has no child type");
            return -1;
        }
        if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_VECTOR_ARRAY_SIZE, errmsg) != 0 ||
            ducknng_quack_write_uleb128(w, node->array_size, errmsg) != 0 ||
            ducknng_quack_write_field_id(w, DUCKNNG_QUACK_VECTOR_ARRAY_CHILD, errmsg) != 0) return -1;
        if (ducknng_quack_idx_mul(rows, (idx_t)node->array_size, &child_rows,
                "ducknng: quack array row count overflows supported size", errmsg) != 0) return -1;
        if (ducknng_quack_encode_vector(w, child, node->children[0], child_rows, depth + 1, errmsg) != 0) return -1;
        return ducknng_quack_write_field_end(w, errmsg);
    }
    width = ducknng_quack_node_fixed_width(node);
    if (width == 0) {
        ducknng_quack_set_error(errmsg, "ducknng: unsupported column type for ducknng_quack_batch encoding");
        return -1;
    }
    {
        size_t data_len = 0;
        if (ducknng_size_mul(width, (size_t)rows, &data_len) != 0) {
            ducknng_quack_set_error(errmsg, "ducknng: fixed-size quack vector payload overflows supported size");
            return -1;
        }
        if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_VECTOR_DATA, errmsg) != 0 ||
            ducknng_quack_write_blob(w, (const uint8_t *)duckdb_vector_get_data(vec), data_len, errmsg) != 0) return -1;
    }
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
        ducknng_quack_column_schema *node = NULL;
        int rc = ducknng_quack_node_from_duckdb(logical_type, 0, &node, errmsg);
        if (logical_type) duckdb_destroy_logical_type(&logical_type);
        if (rc != 0) return -1;
        rc = ducknng_quack_encode_vector(w, vec, node, rows, 0, errmsg);
        ducknng_quack_node_free(node);
        if (rc != 0) return -1;
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
    if ((uint64_t)ncols > DUCKNNG_QUACK_MAX_STRUCT_MEMBERS) {
        ducknng_quack_set_error(errmsg, "ducknng: quack column count exceeds supported limit");
        return -1;
    }
    if (include_schema) {
        if (ducknng_quack_write_field_id(&w, DUCKNNG_QUACK_OUTER_RESULT_TYPES, errmsg) != 0 ||
            ducknng_quack_write_uleb128(&w, (uint64_t)ncols, errmsg) != 0) goto fail;
        for (col = 0; col < ncols; col++) {
            duckdb_logical_type logical_type = duckdb_column_logical_type(&result, col);
            ducknng_quack_column_schema *node = NULL;
            int rc = ducknng_quack_node_from_duckdb(logical_type, 0, &node, errmsg);
            if (logical_type) duckdb_destroy_logical_type(&logical_type);
            if (rc != 0) goto fail;
            rc = ducknng_quack_write_type_node(&w, node, 0, errmsg);
            ducknng_quack_node_free(node);
            if (rc != 0) goto fail;
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

/* Read a Vector object's leading framing: the optional VECTOR_TYPE marker (only
 * the flat physical layout is supported), the HAS_VALIDITY flag, and, when set,
 * the validity blob. The validity view (into the payload buffer) is returned via
 * out_validity/out_validity_len, or NULL when the vector has no validity mask. */
static int ducknng_quack_read_vector_prologue(ducknng_quack_reader *r,
    const uint8_t **out_validity, size_t *out_validity_len, char **errmsg) {
    uint16_t field_id = 0;
    uint8_t has_validity = 0;
    if (out_validity) *out_validity = NULL;
    if (out_validity_len) *out_validity_len = 0;
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
        const uint8_t *blob = NULL;
        size_t blob_len = 0;
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_VALIDITY, errmsg) != 0 ||
            ducknng_quack_read_blob_view(r, &blob, &blob_len, errmsg) != 0) return -1;
        if (out_validity) *out_validity = blob;
        if (out_validity_len) *out_validity_len = blob_len;
    }
    return 0;
}

/* Recursively skip a Vector object of the given node type without materializing
 * it, so the reader lands exactly past the FIELD_END that closes the vector. */
static int ducknng_quack_skip_vector_node(ducknng_quack_reader *r,
    const ducknng_quack_column_schema *node, idx_t rows, unsigned depth, char **errmsg) {
    if (!node) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack vector type while skipping");
        return -1;
    }
    if (depth > DUCKNNG_QUACK_MAX_NESTING) {
        ducknng_quack_set_error(errmsg, "ducknng: quack vector nesting exceeds supported depth");
        return -1;
    }
    if (ducknng_quack_read_vector_prologue(r, NULL, NULL, errmsg) != 0) return -1;
    if (ducknng_quack_node_is_varlen(node)) {
        uint64_t n_items = 0;
        idx_t row;
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_DATA, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &n_items, errmsg) != 0) return -1;
        if (n_items != (uint64_t)rows) {
            ducknng_quack_set_error(errmsg, "ducknng: invalid variable-width quack vector payload");
            return -1;
        }
        for (row = 0; row < rows; row++) if (ducknng_quack_skip_string(r, errmsg) != 0) return -1;
        return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
    }
    if (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_STRUCT ||
        node->logical_type_id == DUCKNNG_QUACK_LOGICAL_UNION) {
        uint64_t nchild = 0;
        uint32_t i;
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_STRUCT_CHILDREN, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &nchild, errmsg) != 0) return -1;
        if (nchild != node->nchildren || !node->children) {
            ducknng_quack_set_error(errmsg, "ducknng: quack struct vector child count mismatch");
            return -1;
        }
        for (i = 0; i < node->nchildren; i++) {
            if (ducknng_quack_skip_vector_node(r, node->children[i], rows, depth + 1, errmsg) != 0) return -1;
        }
        return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
    }
    if (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_LIST ||
        node->logical_type_id == DUCKNNG_QUACK_LOGICAL_MAP) {
        uint64_t child_size = 0;
        uint64_t n_entries = 0;
        idx_t child_rows = 0;
        uint64_t row;
        if (!node->children || node->nchildren != 1) {
            ducknng_quack_set_error(errmsg, "ducknng: quack list/map vector has no child type");
            return -1;
        }
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_LIST_CHILD_SIZE, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &child_size, errmsg) != 0 ||
            ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_LIST_ENTRIES, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &n_entries, errmsg) != 0) return -1;
        if (n_entries != (uint64_t)rows) {
            ducknng_quack_set_error(errmsg, "ducknng: quack list vector entry count mismatch");
            return -1;
        }
        if (ducknng_quack_uint64_to_idx(child_size, &child_rows,
                "ducknng: quack list child size exceeds supported size", errmsg) != 0) return -1;
        for (row = 0; row < n_entries; row++) {
            uint64_t off = 0, len = 0;
            if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_ENTRY_OFFSET, errmsg) != 0 ||
                ducknng_quack_read_uleb128(r, &off, errmsg) != 0 ||
                ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_ENTRY_LENGTH, errmsg) != 0 ||
                ducknng_quack_read_uleb128(r, &len, errmsg) != 0 ||
                ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg) != 0) return -1;
            if (off > child_size || len > child_size - off) {
                ducknng_quack_set_error(errmsg, "ducknng: quack list entry exceeds child vector size");
                return -1;
            }
        }
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_LIST_CHILD, errmsg) != 0) return -1;
        if (ducknng_quack_skip_vector_node(r, node->children[0], child_rows, depth + 1, errmsg) != 0) return -1;
        return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
    }
    if (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_ARRAY) {
        uint64_t asize = 0;
        idx_t child_rows;
        if (!node->children || node->nchildren != 1) {
            ducknng_quack_set_error(errmsg, "ducknng: quack array vector has no child type");
            return -1;
        }
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_ARRAY_SIZE, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &asize, errmsg) != 0 ||
            ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_ARRAY_CHILD, errmsg) != 0) return -1;
        if (asize != node->array_size) {
            ducknng_quack_set_error(errmsg, "ducknng: quack array vector fixed-size mismatch");
            return -1;
        }
        if (ducknng_quack_idx_mul(rows, (idx_t)asize, &child_rows,
                "ducknng: quack array row count overflows supported size", errmsg) != 0) return -1;
        if (ducknng_quack_skip_vector_node(r, node->children[0], child_rows, depth + 1, errmsg) != 0) return -1;
        return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
    }
    {
        const uint8_t *blob = NULL;
        size_t blob_len = 0;
        size_t width = ducknng_quack_node_fixed_width(node);
        size_t expected_len = 0;
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_DATA, errmsg) != 0 ||
            ducknng_quack_read_blob_view(r, &blob, &blob_len, errmsg) != 0) return -1;
        if (width == 0 || ducknng_size_mul(width, (size_t)rows, &expected_len) != 0 ||
            blob_len != expected_len) {
            ducknng_quack_set_error(errmsg, "ducknng: invalid fixed-size quack vector payload");
            return -1;
        }
        return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
    }
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
    uint64_t rows_u64 = 0;
    uint64_t ncols = 0;
    idx_t rows = 0;
    idx_t col;
    if (!schema) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack schema while skipping data chunk");
        return -1;
    }
    if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_CHUNK_ROWS, errmsg) != 0 ||
        ducknng_quack_read_uleb128(r, &rows_u64, errmsg) != 0) return -1;
    if (ducknng_quack_uint64_to_idx(rows_u64, &rows,
            "ducknng: quack row count exceeds supported size", errmsg) != 0) return -1;
    if (ducknng_quack_skip_optional_chunk_types(r, schema, errmsg) != 0 ||
        ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_CHUNK_COLUMNS, errmsg) != 0 ||
        ducknng_quack_read_uleb128(r, &ncols, errmsg) != 0) return -1;
    if (ncols != (uint64_t)schema->ncols) {
        ducknng_quack_set_error(errmsg, "ducknng: quack data chunk column count mismatch");
        return -1;
    }
    for (col = 0; col < schema->ncols; col++) {
        if (ducknng_quack_skip_vector_node(r, &schema->cols[col], rows, 0, errmsg) != 0) return -1;
    }
    if (out_rows) *out_rows = rows;
    return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
}

static void ducknng_quack_copy_validity_slice(duckdb_vector out_vec,
    const uint8_t *src_validity, idx_t src_offset, idx_t count) {
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

/* Recursively decode a Vector object of the given node type into out_vec. The
 * decoder copies the source rows [offset, offset+out_count) into output rows
 * [0, out_count). Nested vectors (STRUCT/UNION/LIST/MAP/ARRAY) are only ever
 * decoded whole-chunk (offset == 0, out_count == src_rows); their children carry
 * their own row counts and always start at offset 0. */
static int ducknng_quack_decode_vector_into(ducknng_quack_reader *r,
    const ducknng_quack_column_schema *node, idx_t src_rows, idx_t offset,
    duckdb_vector out_vec, idx_t out_count, unsigned depth, char **errmsg) {
    const uint8_t *src_validity = NULL;
    size_t validity_len = 0;
    if (!node || !out_vec) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack vector decoder state");
        return -1;
    }
    if (depth > DUCKNNG_QUACK_MAX_NESTING) {
        ducknng_quack_set_error(errmsg, "ducknng: quack vector nesting exceeds supported depth");
        return -1;
    }
    if (ducknng_quack_read_vector_prologue(r, &src_validity, &validity_len, errmsg) != 0) return -1;

    if (ducknng_quack_node_is_varlen(node)) {
        uint64_t n_items = 0;
        idx_t row;
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_DATA, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &n_items, errmsg) != 0) return -1;
        if (n_items != (uint64_t)src_rows) {
            ducknng_quack_set_error(errmsg, "ducknng: invalid variable-width quack vector payload");
            return -1;
        }
        for (row = 0; row < src_rows; row++) {
            const uint8_t *data = NULL;
            size_t len = 0;
            if (ducknng_quack_read_blob_view(r, &data, &len, errmsg) != 0) return -1;
            if (row < offset || row >= offset + out_count) continue;
            if (src_validity && !ducknng_quack_vector_row_is_valid(src_validity, row)) {
                duckdb_vector_ensure_validity_writable(out_vec);
                duckdb_validity_set_row_invalid(duckdb_vector_get_validity(out_vec), row - offset);
            } else {
                idx_t len_idx = 0;
                if (ducknng_quack_size_to_idx(len, &len_idx,
                        "ducknng: quack string value exceeds supported size", errmsg) != 0) return -1;
                duckdb_unsafe_vector_assign_string_element_len(out_vec, row - offset,
                    (const char *)data, len_idx);
            }
        }
        return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
    }
    if (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_STRUCT ||
        node->logical_type_id == DUCKNNG_QUACK_LOGICAL_UNION) {
        uint64_t nchild = 0;
        uint32_t i;
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_STRUCT_CHILDREN, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &nchild, errmsg) != 0) return -1;
        if (nchild != node->nchildren || !node->children) {
            ducknng_quack_set_error(errmsg, "ducknng: quack struct vector child count mismatch");
            return -1;
        }
        if (src_validity && out_count > 0) ducknng_quack_copy_validity_slice(out_vec, src_validity, offset, out_count);
        for (i = 0; i < node->nchildren; i++) {
            duckdb_vector child = duckdb_struct_vector_get_child(out_vec, i);
            if (ducknng_quack_decode_vector_into(r, node->children[i], src_rows, offset, child, out_count, depth + 1, errmsg) != 0) return -1;
        }
        return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
    }
    if (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_LIST ||
        node->logical_type_id == DUCKNNG_QUACK_LOGICAL_MAP) {
        uint64_t child_size = 0;
        uint64_t n_entries = 0;
        idx_t child_rows = 0;
        uint64_t row;
        duckdb_list_entry *out_entries = NULL;
        duckdb_vector child = NULL;
        if (!node->children || node->nchildren != 1) {
            ducknng_quack_set_error(errmsg, "ducknng: quack list/map vector has no child type");
            return -1;
        }
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_LIST_CHILD_SIZE, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &child_size, errmsg) != 0 ||
            ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_LIST_ENTRIES, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &n_entries, errmsg) != 0) return -1;
        if (n_entries != (uint64_t)src_rows) {
            ducknng_quack_set_error(errmsg, "ducknng: quack list vector entry count mismatch");
            return -1;
        }
        if (ducknng_quack_uint64_to_idx(child_size, &child_rows,
                "ducknng: quack list child size exceeds supported size", errmsg) != 0) return -1;
        if (duckdb_list_vector_reserve(out_vec, child_rows) != DuckDBSuccess) {
            ducknng_quack_set_error(errmsg, "ducknng: failed to reserve quack list child capacity");
            return -1;
        }
        out_entries = (duckdb_list_entry *)duckdb_vector_get_data(out_vec);
        if (src_validity && out_count > 0) ducknng_quack_copy_validity_slice(out_vec, src_validity, offset, out_count);
        for (row = 0; row < n_entries; row++) {
            uint64_t off = 0, len = 0;
            if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_ENTRY_OFFSET, errmsg) != 0 ||
                ducknng_quack_read_uleb128(r, &off, errmsg) != 0 ||
                ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_ENTRY_LENGTH, errmsg) != 0 ||
                ducknng_quack_read_uleb128(r, &len, errmsg) != 0 ||
                ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg) != 0) return -1;
            if (off > child_size || len > child_size - off) {
                ducknng_quack_set_error(errmsg, "ducknng: quack list entry exceeds child vector size");
                return -1;
            }
            if (row >= offset && row < offset + out_count) {
                out_entries[row - offset].offset = off;
                out_entries[row - offset].length = len;
            }
        }
        if (duckdb_list_vector_set_size(out_vec, child_rows) != DuckDBSuccess) {
            ducknng_quack_set_error(errmsg, "ducknng: failed to size quack list child vector");
            return -1;
        }
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_LIST_CHILD, errmsg) != 0) return -1;
        child = duckdb_list_vector_get_child(out_vec);
        if (ducknng_quack_decode_vector_into(r, node->children[0], child_rows, 0, child,
                child_rows, depth + 1, errmsg) != 0) return -1;
        return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
    }
    if (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_ARRAY) {
        uint64_t asize = 0;
        duckdb_vector child = NULL;
        idx_t array_size = 0;
        idx_t child_rows;
        if (!node->children || node->nchildren != 1) {
            ducknng_quack_set_error(errmsg, "ducknng: quack array vector has no child type");
            return -1;
        }
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_ARRAY_SIZE, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &asize, errmsg) != 0) return -1;
        if (asize != node->array_size) {
            ducknng_quack_set_error(errmsg, "ducknng: quack array vector fixed-size mismatch");
            return -1;
        }
        if (ducknng_quack_uint64_to_idx(asize, &array_size,
                "ducknng: quack array size exceeds supported size", errmsg) != 0) return -1;
        if (src_validity && out_count > 0) ducknng_quack_copy_validity_slice(out_vec, src_validity, offset, out_count);
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_ARRAY_CHILD, errmsg) != 0) return -1;
        child = duckdb_array_vector_get_child(out_vec);
        if (ducknng_quack_idx_mul(src_rows, array_size, &child_rows,
                "ducknng: quack array row count overflows supported size", errmsg) != 0) return -1;
        if (ducknng_quack_decode_vector_into(r, node->children[0], child_rows, 0, child,
                child_rows, depth + 1, errmsg) != 0) return -1;
        return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
    }
    {
        const uint8_t *data_blob = NULL;
        size_t data_len = 0;
        size_t width = ducknng_quack_node_fixed_width(node);
        size_t expected_len = 0;
        size_t copy_offset = 0;
        size_t copy_len = 0;
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_DATA, errmsg) != 0 ||
            ducknng_quack_read_blob_view(r, &data_blob, &data_len, errmsg) != 0) return -1;
        if (width == 0 || ducknng_size_mul(width, (size_t)src_rows, &expected_len) != 0 ||
            data_len != expected_len) {
            ducknng_quack_set_error(errmsg, "ducknng: invalid fixed-size quack vector payload");
            return -1;
        }
        if (out_count > 0) {
            if (ducknng_size_mul(width, (size_t)offset, &copy_offset) != 0 ||
                ducknng_size_mul(width, (size_t)out_count, &copy_len) != 0 ||
                copy_offset > data_len || copy_len > data_len - copy_offset) {
                ducknng_quack_set_error(errmsg, "ducknng: invalid fixed-size quack vector slice");
                return -1;
            }
            memcpy(duckdb_vector_get_data(out_vec), data_blob + copy_offset, copy_len);
            if (src_validity) ducknng_quack_copy_validity_slice(out_vec, src_validity, offset, out_count);
        }
        return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
    }
}

/* True when any top-level column is a nested type, in which case the body scan
 * must decode whole source chunks rather than slicing mid-chunk. */
static int ducknng_quack_schema_has_nested(const ducknng_quack_schema *schema) {
    idx_t i;
    if (!schema) return 0;
    for (i = 0; i < schema->ncols; i++) {
        if (ducknng_quack_node_is_nested(&schema->cols[i])) return 1;
    }
    return 0;
}

static void ducknng_quack_node_free(ducknng_quack_column_schema *node);

/* Free a node's owned contents (name, enum labels, child names, and recursively
 * its child node structs) but NOT the node struct itself, so it serves both
 * heap-allocated child nodes and array-embedded top-level columns. */
static void ducknng_quack_node_free_contents(ducknng_quack_column_schema *node) {
    uint32_t i;
    if (!node) return;
    if (node->name) { duckdb_free(node->name); node->name = NULL; }
    if (node->enum_labels) {
        for (i = 0; i < node->enum_count; i++) {
            if (node->enum_labels[i]) duckdb_free(node->enum_labels[i]);
        }
        duckdb_free(node->enum_labels);
        node->enum_labels = NULL;
    }
    if (node->child_names) {
        for (i = 0; i < node->nchildren; i++) {
            if (node->child_names[i]) duckdb_free(node->child_names[i]);
        }
        duckdb_free(node->child_names);
        node->child_names = NULL;
    }
    if (node->children) {
        for (i = 0; i < node->nchildren; i++) ducknng_quack_node_free(node->children[i]);
        duckdb_free(node->children);
        node->children = NULL;
    }
    node->nchildren = 0;
    node->enum_count = 0;
}

static void ducknng_quack_node_free(ducknng_quack_column_schema *node) {
    if (!node) return;
    ducknng_quack_node_free_contents(node);
    duckdb_free(node);
}

void ducknng_quack_schema_reset(ducknng_quack_schema *schema) {
    idx_t i;
    if (!schema) return;
    if (schema->cols) {
        for (i = 0; i < schema->ncols; i++) ducknng_quack_node_free_contents(&schema->cols[i]);
        duckdb_free(schema->cols);
    }
    memset(schema, 0, sizeof(*schema));
}

int ducknng_result_next_chunks_to_quack_payload(duckdb_result result,
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
        chunks[chunk_count] = ducknng_result_fetch_session_chunk(result);
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
    return ducknng_result_next_chunks_to_quack_payload(result, 1, 1,
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
    idx_t ncols_idx = 0;
    uint64_t i;
    idx_t row_count = 0;
    size_t cols_bytes = 0;
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
    if (ncols > DUCKNNG_QUACK_MAX_STRUCT_MEMBERS) {
        ducknng_quack_set_error(errmsg, "ducknng: quack column count exceeds supported limit");
        goto fail;
    }
    if (ducknng_quack_uint64_to_idx(ncols, &ncols_idx,
            "ducknng: quack column count exceeds supported size", errmsg) != 0) goto fail;
    if (ncols_idx > 0) {
        if (ducknng_size_mul(sizeof(*out_schema->cols), (size_t)ncols_idx,
                &cols_bytes) != 0) {
            ducknng_quack_set_error(errmsg, "ducknng: quack schema column allocation overflows supported size");
            goto fail;
        }
        out_schema->cols = (ducknng_quack_column_schema *)duckdb_malloc(cols_bytes);
        if (!out_schema->cols) {
            ducknng_quack_set_error(errmsg, "ducknng: out of memory allocating quack schema columns");
            goto fail;
        }
        memset(out_schema->cols, 0, cols_bytes);
    }
    out_schema->ncols = ncols_idx;
    for (i = 0; i < ncols; i++) {
        ducknng_quack_column_schema *node = NULL;
        if (ducknng_quack_read_type_node(&r, 0, &node, errmsg) != 0) goto fail;
        /* Move the heap node's contents into the flat top-level column slot and
         * free the wrapper struct only (children/labels are now owned by cols[i]).
         * name is set from the separate NAMES section below. */
        out_schema->cols[i] = *node;
        duckdb_free(node);
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
        if (ducknng_quack_node_to_duckdb(&out_schema->cols[i], 0, &bind_type, errmsg) != 0) goto fail;
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
            if (ducknng_quack_idx_add(row_count, chunk_rows, &row_count,
                    "ducknng: quack row count overflows supported size", errmsg) != 0) goto fail;
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
        if (ducknng_quack_idx_add(row_count, chunk_rows, &row_count,
                "ducknng: quack row count overflows supported size", errmsg) != 0) return -1;
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
    int has_nested;
    if (out_rows) *out_rows = 0;
    if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_CHUNK_ROWS, errmsg) != 0 ||
        ducknng_quack_read_uleb128(r, &rows_u64, errmsg) != 0) return -1;
    if (ducknng_quack_uint64_to_idx(rows_u64, &rows,
            "ducknng: quack row count exceeds supported size", errmsg) != 0) return -1;
    if (rows > duckdb_vector_size()) {
        ducknng_quack_set_error(errmsg, "ducknng: quack chunk exceeds output vector capacity");
        return -1;
    }
    if (out_rows) *out_rows = rows;
    has_nested = ducknng_quack_schema_has_nested(schema);
    if (offset >= rows) {
        if (ducknng_quack_skip_optional_chunk_types(r, schema, errmsg) != 0 ||
            ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_CHUNK_COLUMNS, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &ncols, errmsg) != 0) return -1;
        if (ncols != (uint64_t)schema->ncols) {
            ducknng_quack_set_error(errmsg, "ducknng: quack data chunk column count mismatch");
            return -1;
        }
        for (col = 0; col < schema->ncols; col++) {
            if (ducknng_quack_skip_vector_node(r, &schema->cols[col], rows, 0, errmsg) != 0) return -1;
        }
        return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
    }
    copy_count = rows - offset;
    if (copy_count > duckdb_vector_size()) copy_count = duckdb_vector_size();
    if (has_nested) {
        /* Nested vectors cannot be sliced mid-chunk; the scan path advances one
         * whole source chunk per call, so a nested chunk must start at row 0 and
         * fit in one output chunk (source chunks are already <= vector size). */
        if (offset != 0) {
            ducknng_quack_set_error(errmsg, "ducknng: nested quack chunk cannot resume mid-chunk");
            return -1;
        }
        copy_count = rows;
    }
    if (ducknng_quack_skip_optional_chunk_types(r, schema, errmsg) != 0 ||
        ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_CHUNK_COLUMNS, errmsg) != 0 ||
        ducknng_quack_read_uleb128(r, &ncols, errmsg) != 0) return -1;
    if (ncols != (uint64_t)schema->ncols) {
        ducknng_quack_set_error(errmsg, "ducknng: quack data chunk column count mismatch");
        return -1;
    }
    for (col = 0; col < schema->ncols; col++) {
        duckdb_vector out_vec = duckdb_data_chunk_get_vector(output, col);
        if (ducknng_quack_decode_vector_into(r, &schema->cols[col], rows, offset, out_vec, copy_count, 0, errmsg) != 0) return -1;
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
        idx_t next_seen = 0;
        idx_t next_offset = 0;
        if (ducknng_quack_read_byte(&r, &present, errmsg) != 0 || !present ||
            ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_CHUNK_WRAPPER, errmsg) != 0) return -1;
        if (global_offset >= seen_rows) local_offset = global_offset - seen_rows;
        else local_offset = 0;
        if (ducknng_quack_decode_data_chunk_slice(&r, output, schema, local_offset, &chunk_rows, errmsg) != 0) return -1;
        if (ducknng_quack_idx_add(seen_rows, chunk_rows, &next_seen,
                "ducknng: quack row count overflows supported size", errmsg) != 0) return -1;
        if (global_offset < next_seen) {
            if (ducknng_quack_idx_add(global_offset, duckdb_data_chunk_get_size(output), &next_offset,
                    "ducknng: quack scan offset overflows supported size", errmsg) != 0) return -1;
            *inout_offset = next_offset;
            return 0;
        }
        seen_rows = next_seen;
    }
    if (ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_FIELD_END, errmsg) != 0) return -1;
    duckdb_data_chunk_set_size(output, 0);
    return 0;
}
