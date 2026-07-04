#include "duckdb_extension.h"
DUCKDB_EXTENSION_GLOBAL
#undef duckdb_malloc
#undef duckdb_free
#undef duckdb_vector_size

#include "ducknng_quack.h"
#include "ducknng_transport.h"
#include "ducknng_util.h"
#include "ducknng_wire.h"

#include "greatest.h"
#include "theft.h"

#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DUCKNNG_PROP_DEFAULT_TRIALS 1000u
#define DUCKNNG_PROP_DEFAULT_SEED UINT64_C(0xd17c0ffee1234567)
#define DUCKNNG_PROP_MAX_RANDOM_BYTES 512u
#define DUCKNNG_PROP_MAX_RANDOM_URL 256u
#define DUCKNNG_PROP_MAX_FRAME_PAYLOAD 256u
#define DUCKNNG_PROP_MAX_FRAME_ERROR 32u

struct prop_bytes {
    size_t len;
    uint8_t data[];
};

struct prop_bytes_env {
    size_t max_len;
};

struct prop_frame {
    size_t len;
    uint8_t type;
    uint32_t flags;
    uint32_t name_len;
    uint32_t error_len;
    uint64_t payload_len;
    uint8_t data[];
};

static void
prop_init_duckdb_api(void)
{
    duckdb_ext_api.duckdb_malloc = malloc;
    duckdb_ext_api.duckdb_free = free;
}

static size_t
prop_env_size(const char *name, size_t fallback)
{
    const char *value = getenv(name);
    char *end = NULL;
    unsigned long long parsed;

    if (!value || !value[0]) return fallback;
    errno = 0;
    parsed = strtoull(value, &end, 0);
    if (errno != 0 || !end || *end != '\0' || parsed == 0) return fallback;
    return (size_t)parsed;
}

static theft_seed
prop_env_seed(void)
{
    const char *value = getenv("DUCKNNG_PROP_SEED");
    char *end = NULL;
    unsigned long long parsed;

    if (!value || !value[0]) return (theft_seed)DUCKNNG_PROP_DEFAULT_SEED;
    errno = 0;
    parsed = strtoull(value, &end, 0);
    if (errno != 0 || !end || *end != '\0') return (theft_seed)DUCKNNG_PROP_DEFAULT_SEED;
    return (theft_seed)parsed;
}

static int
prop_env_bool(const char *name)
{
    const char *value = getenv(name);

    if (!value || !value[0]) return 0;
    return strcmp(value, "0") != 0 && strcmp(value, "false") != 0 &&
        strcmp(value, "FALSE") != 0 && strcmp(value, "no") != 0 &&
        strcmp(value, "NO") != 0;
}

static void
prop_write_le32(uint8_t *p, uint32_t value)
{
    p[0] = (uint8_t)(value & 0xffu);
    p[1] = (uint8_t)((value >> 8) & 0xffu);
    p[2] = (uint8_t)((value >> 16) & 0xffu);
    p[3] = (uint8_t)((value >> 24) & 0xffu);
}

static void
prop_write_le64(uint8_t *p, uint64_t value)
{
    prop_write_le32(p, (uint32_t)(value & UINT64_C(0xffffffff)));
    prop_write_le32(p + 4, (uint32_t)((value >> 32) & UINT64_C(0xffffffff)));
}

static uint64_t
prop_random_bounded(struct theft *t, uint64_t limit)
{
    uint64_t value;

    if (limit <= 1) return 0;
    value = theft_random_bits(t, 16);
    if (limit > UINT64_C(65536)) {
        value |= theft_random_bits(t, 16) << 16;
    }
    return value % limit;
}

static enum theft_alloc_res
prop_bytes_alloc(struct theft *t, void *env, void **instance)
{
    const struct prop_bytes_env *cfg = (const struct prop_bytes_env *)env;
    size_t max_len = cfg && cfg->max_len ? cfg->max_len : DUCKNNG_PROP_MAX_RANDOM_BYTES;
    size_t len = (size_t)prop_random_bounded(t, (uint64_t)max_len + 1u);
    struct prop_bytes *out;
    size_t i;

    out = (struct prop_bytes *)malloc(sizeof(*out) + len);
    if (!out) return THEFT_ALLOC_ERROR;
    out->len = len;
    for (i = 0; i < len; i++) {
        out->data[i] = (uint8_t)theft_random_bits(t, 8);
    }
    *instance = out;
    return THEFT_ALLOC_OK;
}

static void
prop_bytes_free(void *instance, void *env)
{
    (void)env;
    free(instance);
}

static void
prop_hexdump(FILE *f, const uint8_t *data, size_t len)
{
    size_t row;

    fprintf(f, "len=%zu\n", len);
    for (row = 0; row < len; row += 16) {
        size_t rem = len - row;
        size_t i;

        if (rem > 16) rem = 16;
        fprintf(f, "%04zx:", row);
        for (i = 0; i < rem; i++) fprintf(f, " %02x", data[row + i]);
        fprintf(f, "\n");
    }
}

static void
prop_bytes_print(FILE *f, const void *instance, void *env)
{
    const struct prop_bytes *bytes = (const struct prop_bytes *)instance;

    (void)env;
    prop_hexdump(f, bytes->data, bytes->len);
}

static struct prop_bytes_env prop_random_bytes_env = {
    .max_len = DUCKNNG_PROP_MAX_RANDOM_BYTES,
};

static struct prop_bytes_env prop_random_url_env = {
    .max_len = DUCKNNG_PROP_MAX_RANDOM_URL,
};

static struct theft_type_info prop_random_bytes_info = {
    .alloc = prop_bytes_alloc,
    .free = prop_bytes_free,
    .print = prop_bytes_print,
    .autoshrink_config = {
        .enable = true,
    },
    .env = &prop_random_bytes_env,
};

static struct theft_type_info prop_random_url_info = {
    .alloc = prop_bytes_alloc,
    .free = prop_bytes_free,
    .print = prop_bytes_print,
    .autoshrink_config = {
        .enable = true,
    },
    .env = &prop_random_url_env,
};

static enum theft_alloc_res
prop_frame_alloc(struct theft *t, void *env, void **instance)
{
    uint8_t type = (uint8_t)prop_random_bounded(t, 5);
    uint32_t flags = (uint32_t)theft_random_bits(t, 16);
    uint32_t name_len = (uint32_t)prop_random_bounded(t, DUCKNNG_MAX_METHOD_NAME_LEN + 1u);
    uint32_t error_len = (uint32_t)prop_random_bounded(t, DUCKNNG_PROP_MAX_FRAME_ERROR + 1u);
    uint64_t payload_len = prop_random_bounded(t, DUCKNNG_PROP_MAX_FRAME_PAYLOAD + 1u);
    size_t total;
    struct prop_frame *out;
    size_t i;

    (void)env;
    if (type == DUCKNNG_RPC_CALL) error_len = 0;
    total = DUCKNNG_WIRE_HEADER_LEN + (size_t)name_len + (size_t)error_len + (size_t)payload_len;
    out = (struct prop_frame *)malloc(sizeof(*out) + total);
    if (!out) return THEFT_ALLOC_ERROR;
    out->len = total;
    out->type = type;
    out->flags = flags;
    out->name_len = name_len;
    out->error_len = error_len;
    out->payload_len = payload_len;
    out->data[0] = DUCKNNG_WIRE_VERSION;
    out->data[1] = type;
    prop_write_le32(out->data + 2, flags);
    prop_write_le32(out->data + 6, name_len);
    prop_write_le32(out->data + 10, error_len);
    prop_write_le64(out->data + 14, payload_len);
    for (i = DUCKNNG_WIRE_HEADER_LEN; i < total; i++) {
        out->data[i] = (uint8_t)theft_random_bits(t, 8);
    }
    *instance = out;
    return THEFT_ALLOC_OK;
}

static void
prop_frame_free(void *instance, void *env)
{
    (void)env;
    free(instance);
}

static void
prop_frame_print(FILE *f, const void *instance, void *env)
{
    const struct prop_frame *frame = (const struct prop_frame *)instance;

    (void)env;
    fprintf(f, "type=%u flags=%" PRIu32 " name_len=%" PRIu32
        " error_len=%" PRIu32 " payload_len=%" PRIu64 "\n",
        (unsigned int)frame->type, frame->flags, frame->name_len,
        frame->error_len, frame->payload_len);
    prop_hexdump(f, frame->data, frame->len);
}

static struct theft_type_info prop_frame_info = {
    .alloc = prop_frame_alloc,
    .free = prop_frame_free,
    .print = prop_frame_print,
    .autoshrink_config = {
        .enable = true,
    },
};

struct prop_two_sizes {
    uint64_t a;
    uint64_t b;
};

static enum theft_alloc_res
prop_two_sizes_alloc(struct theft *t, void *env, void **instance)
{
    struct prop_two_sizes *out;

    (void)env;
    out = (struct prop_two_sizes *)malloc(sizeof(*out));
    if (!out) return THEFT_ALLOC_ERROR;
    out->a = ((uint64_t)theft_random_bits(t, 32) << 32) | (uint64_t)theft_random_bits(t, 32);
    out->b = ((uint64_t)theft_random_bits(t, 32) << 32) | (uint64_t)theft_random_bits(t, 32);
    /* Bias toward boundary values so the overflow branches are hit often rather
     * than only on a rare random near-SIZE_MAX draw. */
    switch (theft_random_bits(t, 3)) {
    case 0: out->a = (uint64_t)SIZE_MAX; break;
    case 1: out->b = (uint64_t)SIZE_MAX - (out->a & 0xffu); break;
    case 2: out->a = ((uint64_t)SIZE_MAX / 2u) + (out->a & 0xffffu); break;
    default: break;
    }
    *instance = out;
    return THEFT_ALLOC_OK;
}

static void
prop_two_sizes_free(void *instance, void *env)
{
    (void)env;
    free(instance);
}

static void
prop_two_sizes_print(FILE *f, const void *instance, void *env)
{
    const struct prop_two_sizes *in = (const struct prop_two_sizes *)instance;

    (void)env;
    fprintf(f, "a=%" PRIu64 " b=%" PRIu64 "\n", in->a, in->b);
}

static struct theft_type_info prop_two_sizes_info = {
    .alloc = prop_two_sizes_alloc,
    .free = prop_two_sizes_free,
    .print = prop_two_sizes_print,
    .autoshrink_config = {
        .enable = true,
    },
};

/*
 * Checked size arithmetic must never wrap silently: ducknng_size_add /
 * ducknng_size_mul report overflow as -1 (leaving *out untouched) exactly when
 * the mathematical result exceeds SIZE_MAX, and ducknng_grow_capacity always
 * returns a capacity >= the requested need without overflowing.
 */
static enum theft_trial_res
prop_size_arith_invariants(struct theft *t, void *arg1)
{
    const struct prop_two_sizes *in = (const struct prop_two_sizes *)arg1;
    size_t a = (size_t)in->a;
    size_t b = (size_t)in->b;
    size_t out;
    const size_t sentinel = (size_t)0x5a5a5a5au;

    (void)t;
    out = sentinel;
    if (b > SIZE_MAX - a) {
        if (ducknng_size_add(a, b, &out) != -1) return THEFT_TRIAL_FAIL;
        if (out != sentinel) return THEFT_TRIAL_FAIL;
    } else {
        if (ducknng_size_add(a, b, &out) != 0) return THEFT_TRIAL_FAIL;
        if (out != a + b) return THEFT_TRIAL_FAIL;
    }

    out = sentinel;
    if (a != 0 && b > SIZE_MAX / a) {
        if (ducknng_size_mul(a, b, &out) != -1) return THEFT_TRIAL_FAIL;
        if (out != sentinel) return THEFT_TRIAL_FAIL;
    } else {
        if (ducknng_size_mul(a, b, &out) != 0) return THEFT_TRIAL_FAIL;
        if (out != a * b) return THEFT_TRIAL_FAIL;
    }

    out = 0;
    if (ducknng_grow_capacity(a, b, 256, &out) != 0) return THEFT_TRIAL_FAIL;
    if (out < a) return THEFT_TRIAL_FAIL;

    return THEFT_TRIAL_PASS;
}

#define DUCKNNG_PROP_MAX_PATH_SEG 48u

struct prop_two_strings {
    char a[DUCKNNG_PROP_MAX_PATH_SEG + 1];
    char b[DUCKNNG_PROP_MAX_PATH_SEG + 1];
};

static enum theft_alloc_res
prop_two_strings_alloc(struct theft *t, void *env, void **instance)
{
    struct prop_two_strings *out;
    size_t la, lb, i;

    (void)env;
    out = (struct prop_two_strings *)malloc(sizeof(*out));
    if (!out) return THEFT_ALLOC_ERROR;
    /* Lengths can be 0 so the empty-prefix / empty-name branches are exercised. */
    la = (size_t)prop_random_bounded(t, DUCKNNG_PROP_MAX_PATH_SEG + 1u);
    lb = (size_t)prop_random_bounded(t, DUCKNNG_PROP_MAX_PATH_SEG + 1u);
    /* Fill with printable, never-NUL bytes (incl. '.') so these stay C strings. */
    for (i = 0; i < la; i++) out->a[i] = (char)(0x21u + (theft_random_bits(t, 8) % 0x5eu));
    out->a[la] = '\0';
    for (i = 0; i < lb; i++) out->b[i] = (char)(0x21u + (theft_random_bits(t, 8) % 0x5eu));
    out->b[lb] = '\0';
    *instance = out;
    return THEFT_ALLOC_OK;
}

static void
prop_two_strings_free(void *instance, void *env)
{
    (void)env;
    free(instance);
}

static void
prop_two_strings_print(FILE *f, const void *instance, void *env)
{
    const struct prop_two_strings *in = (const struct prop_two_strings *)instance;

    (void)env;
    fprintf(f, "a=\"%s\" b=\"%s\"\n", in->a, in->b);
}

static struct theft_type_info prop_two_strings_info = {
    .alloc = prop_two_strings_alloc,
    .free = prop_two_strings_free,
    .print = prop_two_strings_print,
    .autoshrink_config = {
        .enable = true,
    },
};

/*
 * ducknng_join_dotted_path(prefix, name) yields a copy of name when prefix is
 * empty, and exactly "prefix.name" (prefix, one '.', name) otherwise — never
 * truncating, overrunning, or losing the NUL terminator.
 */
static enum theft_trial_res
prop_join_dotted_path_invariants(struct theft *t, void *arg1)
{
    const struct prop_two_strings *in = (const struct prop_two_strings *)arg1;
    size_t pa = strlen(in->a);
    size_t pb = strlen(in->b);
    char *r;
    enum theft_trial_res res = THEFT_TRIAL_PASS;

    (void)t;
    r = ducknng_join_dotted_path(in->a, in->b);
    if (!r) return THEFT_TRIAL_FAIL; /* sizes are tiny; allocation must succeed */
    if (pa == 0) {
        if (strcmp(r, in->b) != 0) res = THEFT_TRIAL_FAIL;
    } else if (strlen(r) != pa + 1 + pb) {
        res = THEFT_TRIAL_FAIL;
    } else if (memcmp(r, in->a, pa) != 0 || r[pa] != '.' ||
               (pb && memcmp(r + pa + 1, in->b, pb) != 0)) {
        res = THEFT_TRIAL_FAIL;
    }
    free(r);
    return res;
}

static enum theft_run_res
prop_run_one(const char *name, theft_propfun1 *prop, const struct theft_type_info *info)
{
    struct theft_run_config cfg;

    memset(&cfg, 0, sizeof(cfg));
    cfg.name = name;
    cfg.prop1 = prop;
    cfg.type_info[0] = info;
    cfg.trials = prop_env_size("DUCKNNG_PROP_TRIALS", DUCKNNG_PROP_DEFAULT_TRIALS);
    cfg.seed = prop_env_seed();
    if (prop_env_bool("DUCKNNG_PROP_FORK")) {
        cfg.fork.enable = true;
        cfg.fork.timeout = prop_env_size("DUCKNNG_PROP_TIMEOUT_MS", 1000u);
    }
    return theft_run(&cfg);
}

static enum theft_trial_res
prop_wire_random_bytes(struct theft *t, void *arg1)
{
    const struct prop_bytes *bytes = (const struct prop_bytes *)arg1;
    ducknng_frame frame;
    int rc;
    size_t min_len;

    (void)t;
    rc = ducknng_decode_frame_bytes(bytes->data, bytes->len, &frame);
    if (rc != 0) return THEFT_TRIAL_PASS;

    if (frame.version != DUCKNNG_WIRE_VERSION) return THEFT_TRIAL_FAIL;
    if (frame.name_len > DUCKNNG_MAX_METHOD_NAME_LEN) return THEFT_TRIAL_FAIL;
    if (frame.type == DUCKNNG_RPC_CALL && frame.error_len != 0) return THEFT_TRIAL_FAIL;
    min_len = DUCKNNG_WIRE_HEADER_LEN + (size_t)frame.name_len + (size_t)frame.error_len;
    if (min_len > bytes->len) return THEFT_TRIAL_FAIL;
    if (frame.payload_len > (uint64_t)(bytes->len - min_len)) return THEFT_TRIAL_FAIL;
    if (frame.name != bytes->data + DUCKNNG_WIRE_HEADER_LEN) return THEFT_TRIAL_FAIL;
    if (frame.error != frame.name + frame.name_len) return THEFT_TRIAL_FAIL;
    if (frame.payload != frame.error + frame.error_len) return THEFT_TRIAL_FAIL;
    return THEFT_TRIAL_PASS;
}

static enum theft_trial_res
prop_wire_valid_frame_decodes(struct theft *t, void *arg1)
{
    const struct prop_frame *input = (const struct prop_frame *)arg1;
    ducknng_frame frame;
    size_t name_off = DUCKNNG_WIRE_HEADER_LEN;
    size_t err_off = name_off + input->name_len;
    size_t payload_off = err_off + input->error_len;
    size_t cut;

    (void)t;
    if (ducknng_decode_frame_bytes(input->data, input->len, &frame) != 0) {
        return THEFT_TRIAL_FAIL;
    }
    if (frame.version != DUCKNNG_WIRE_VERSION || frame.type != input->type ||
        frame.flags != input->flags || frame.name_len != input->name_len ||
        frame.error_len != input->error_len || frame.payload_len != input->payload_len) {
        return THEFT_TRIAL_FAIL;
    }
    if (memcmp(frame.name, input->data + name_off, input->name_len) != 0) {
        return THEFT_TRIAL_FAIL;
    }
    if (memcmp(frame.error, input->data + err_off, input->error_len) != 0) {
        return THEFT_TRIAL_FAIL;
    }
    if (memcmp(frame.payload, input->data + payload_off, (size_t)input->payload_len) != 0) {
        return THEFT_TRIAL_FAIL;
    }
    for (cut = 0; cut < input->len; cut++) {
        if (ducknng_decode_frame_bytes(input->data, cut, &frame) == 0) {
            return THEFT_TRIAL_FAIL;
        }
    }
    return THEFT_TRIAL_PASS;
}

static enum theft_trial_res
prop_transport_random_urls(struct theft *t, void *arg1)
{
    const struct prop_bytes *bytes = (const struct prop_bytes *)arg1;
    char *url;
    char *errmsg = NULL;
    ducknng_transport_url parsed;
    int rc;
    enum theft_trial_res result = THEFT_TRIAL_PASS;

    (void)t;
    url = (char *)malloc(bytes->len + 1);
    if (!url) return THEFT_TRIAL_ERROR;
    if (bytes->len) memcpy(url, bytes->data, bytes->len);
    url[bytes->len] = '\0';
    ducknng_transport_url_init(&parsed);
    rc = ducknng_transport_url_parse(url, &parsed, &errmsg);
    if (rc == 0) {
        if (errmsg != NULL) result = THEFT_TRIAL_FAIL;
        if (parsed.family != DUCKNNG_TRANSPORT_FAMILY_NNG &&
            parsed.family != DUCKNNG_TRANSPORT_FAMILY_HTTP) {
            result = THEFT_TRIAL_FAIL;
        }
        if (parsed.uses_tls && parsed.scheme != DUCKNNG_TRANSPORT_SCHEME_TLS_TCP &&
            parsed.scheme != DUCKNNG_TRANSPORT_SCHEME_WSS &&
            parsed.scheme != DUCKNNG_TRANSPORT_SCHEME_HTTPS) {
            result = THEFT_TRIAL_FAIL;
        }
        if (ducknng_transport_url_is_nng(&parsed) && parsed.family != DUCKNNG_TRANSPORT_FAMILY_NNG) {
            result = THEFT_TRIAL_FAIL;
        }
        if (ducknng_transport_url_is_http(&parsed) && parsed.family != DUCKNNG_TRANSPORT_FAMILY_HTTP) {
            result = THEFT_TRIAL_FAIL;
        }
        if (!ducknng_transport_family_name(parsed.family) || !ducknng_transport_scheme_name(parsed.scheme)) {
            result = THEFT_TRIAL_FAIL;
        }
    }
    if (errmsg) free(errmsg);
    free(url);
    return result;
}

static enum theft_trial_res
prop_quack_random_payloads(struct theft *t, void *arg1)
{
    const struct prop_bytes *bytes = (const struct prop_bytes *)arg1;
    ducknng_quack_schema schema;
    idx_t row_count = 0;
    size_t offset = 0;
    uint64_t remaining = 0;
    char *errmsg = NULL;
    int rc;
    enum theft_trial_res result = THEFT_TRIAL_PASS;

    (void)t;
    memset(&schema, 0, sizeof(schema));
    rc = ducknng_quack_payload_read_row_count(bytes->data, bytes->len, &schema,
        &row_count, &errmsg);
    if (rc == 0 && errmsg != NULL) result = THEFT_TRIAL_FAIL;
    if (errmsg) {
        free(errmsg);
        errmsg = NULL;
    }
    rc = ducknng_quack_payload_scan_begin(bytes->data, bytes->len, &schema,
        &offset, &remaining, &errmsg);
    if (rc == 0 && (errmsg != NULL || offset > bytes->len)) result = THEFT_TRIAL_FAIL;
    if (errmsg) free(errmsg);
    return result;
}

/* Wire logical-type ids mirror the TU-private DUCKNNG_QUACK_LOGICAL_* defines in
 * src/ducknng_quack.c; kept local so the property harness can hand-build a nested
 * schema and fuzz the recursive skip path without widening the public header. */
#define PROP_QK_INTEGER 13
#define PROP_QK_BIGINT  14
#define PROP_QK_VARCHAR 25
#define PROP_QK_STRUCT  100
#define PROP_QK_LIST    101

#define PROP_QK_FIELD_END          0xffffu
#define PROP_QK_OUTER_RESULT_TYPES 1u
#define PROP_QK_OUTER_RESULTS      4u
#define PROP_QK_CHUNK_WRAPPER      300u
#define PROP_QK_CHUNK_ROWS         100u
#define PROP_QK_CHUNK_COLUMNS      102u
#define PROP_QK_VECTOR_HAS_VALIDITY 100u
#define PROP_QK_VECTOR_DATA        102u

struct prop_quack_buf {
    uint8_t data[256];
    size_t len;
};

static int
prop_qb_put(struct prop_quack_buf *b, const void *src, size_t len)
{
    if (!b || len > sizeof(b->data) || b->len > sizeof(b->data) - len) return -1;
    if (len) memcpy(b->data + b->len, src, len);
    b->len += len;
    return 0;
}

static int
prop_qb_byte(struct prop_quack_buf *b, uint8_t value)
{
    return prop_qb_put(b, &value, 1);
}

static int
prop_qb_u16(struct prop_quack_buf *b, uint16_t value)
{
    uint8_t tmp[2];

    tmp[0] = (uint8_t)(value & 0xffu);
    tmp[1] = (uint8_t)((value >> 8) & 0xffu);
    return prop_qb_put(b, tmp, sizeof(tmp));
}

static int
prop_qb_uleb(struct prop_quack_buf *b, uint64_t value)
{
    do {
        uint8_t byte = (uint8_t)(value & 0x7fu);
        value >>= 7;
        if (value) byte |= 0x80u;
        if (prop_qb_byte(b, byte) != 0) return -1;
    } while (value);
    return 0;
}

static int
prop_qb_field_end(struct prop_quack_buf *b)
{
    return prop_qb_u16(b, PROP_QK_FIELD_END);
}

static int
prop_quack_build_one_col_schema(ducknng_quack_schema *schema, int type_id)
{
    memset(schema, 0, sizeof(*schema));
    schema->cols = (ducknng_quack_column_schema *)malloc(sizeof(*schema->cols));
    if (!schema->cols) return -1;
    memset(schema->cols, 0, sizeof(*schema->cols));
    schema->ncols = 1;
    schema->cols[0].logical_type_id = type_id;
    return 0;
}

static int
prop_qb_begin_one_col_chunk(struct prop_quack_buf *b, uint64_t rows)
{
    return prop_qb_u16(b, PROP_QK_OUTER_RESULTS) != 0 ||
        prop_qb_uleb(b, 1) != 0 ||
        prop_qb_byte(b, 1) != 0 ||
        prop_qb_u16(b, PROP_QK_CHUNK_WRAPPER) != 0 ||
        prop_qb_u16(b, PROP_QK_CHUNK_ROWS) != 0 ||
        prop_qb_uleb(b, rows) != 0 ||
        prop_qb_u16(b, PROP_QK_CHUNK_COLUMNS) != 0 ||
        prop_qb_uleb(b, 1) != 0 ||
        prop_qb_u16(b, PROP_QK_VECTOR_HAS_VALIDITY) != 0 ||
        prop_qb_byte(b, 0) != 0 ||
        prop_qb_u16(b, PROP_QK_VECTOR_DATA) != 0 ? -1 : 0;
}

static int
prop_quack_payload_fixed_width_overflow(struct prop_quack_buf *b)
{
    memset(b, 0, sizeof(*b));
    if (prop_qb_begin_one_col_chunk(b, UINT64_C(1) << 61) != 0) return -1;
    if (prop_qb_uleb(b, 0) != 0) return -1;
    return prop_qb_field_end(b) != 0 || prop_qb_field_end(b) != 0 ||
        prop_qb_field_end(b) != 0 ? -1 : 0;
}

static int
prop_quack_payload_huge_schema_column_count(struct prop_quack_buf *b)
{
    uint64_t ncols;

    memset(b, 0, sizeof(*b));
    ncols = UINT64_MAX / (uint64_t)sizeof(ducknng_quack_column_schema) + 2u;
    return prop_qb_u16(b, PROP_QK_OUTER_RESULT_TYPES) != 0 ||
        prop_qb_uleb(b, ncols) != 0 ? -1 : 0;
}

static int
prop_quack_payload_varlen_wraparound(struct prop_quack_buf *b)
{
    size_t first_data_off;
    size_t after_huge_len_off;
    uint64_t huge_len;
    uint8_t fake_ends[6] = {0xffu, 0xffu, 0xffu, 0xffu, 0xffu, 0xffu};

    memset(b, 0, sizeof(*b));
    if (prop_qb_begin_one_col_chunk(b, 2) != 0) return -1;
    if (prop_qb_uleb(b, 2) != 0) return -1;
    if (prop_qb_uleb(b, sizeof(fake_ends)) != 0) return -1;
    first_data_off = b->len;
    if (prop_qb_put(b, fake_ends, sizeof(fake_ends)) != 0) return -1;

    /* Choose a second string length that made the old size_t bounds check wrap
     * r->off back to first_data_off, where fake field-end bytes are waiting. */
    after_huge_len_off = b->len + 10;
    huge_len = UINT64_MAX - (uint64_t)after_huge_len_off + 1u +
        (uint64_t)first_data_off;
    if (prop_qb_uleb(b, huge_len) != 0) return -1;
    return b->len == after_huge_len_off ? 0 : -1;
}

static ducknng_quack_column_schema *prop_quack_leaf(int type_id)
{
    ducknng_quack_column_schema *n =
        (ducknng_quack_column_schema *)malloc(sizeof(*n));
    if (!n) return NULL;
    memset(n, 0, sizeof(*n));
    n->logical_type_id = type_id;
    return n;
}

/* Build STRUCT(a INTEGER, b VARCHAR) and LIST(INTEGER) top-level columns. The
 * tree is owned through ducknng_quack_schema_reset, matching the codec's own
 * allocation/ownership rules (duckdb_malloc == malloc in this harness). */
static int prop_quack_build_nested_schema(ducknng_quack_schema *schema)
{
    ducknng_quack_column_schema *st;
    ducknng_quack_column_schema *li;
    memset(schema, 0, sizeof(*schema));
    schema->cols = (ducknng_quack_column_schema *)malloc(sizeof(*schema->cols) * 2);
    if (!schema->cols) return -1;
    memset(schema->cols, 0, sizeof(*schema->cols) * 2);
    schema->ncols = 2;
    st = &schema->cols[0];
    st->logical_type_id = PROP_QK_STRUCT;
    st->nchildren = 2;
    st->children = (ducknng_quack_column_schema **)malloc(sizeof(*st->children) * 2);
    st->child_names = (char **)malloc(sizeof(*st->child_names) * 2);
    if (!st->children || !st->child_names) return -1;
    st->children[0] = prop_quack_leaf(PROP_QK_INTEGER);
    st->children[1] = prop_quack_leaf(PROP_QK_VARCHAR);
    st->child_names[0] = NULL;
    st->child_names[1] = NULL;
    if (!st->children[0] || !st->children[1]) return -1;
    li = &schema->cols[1];
    li->logical_type_id = PROP_QK_LIST;
    li->nchildren = 1;
    li->children = (ducknng_quack_column_schema **)malloc(sizeof(*li->children) * 1);
    if (!li->children) return -1;
    li->children[0] = prop_quack_leaf(PROP_QK_INTEGER);
    if (!li->children[0]) return -1;
    return 0;
}

/* Fuzz the recursive nested skip path: a fixed STRUCT+LIST schema parsed against
 * random bytes must reject cleanly (or parse a row count) without crashing and
 * without claiming success while leaving an error string. */
static enum theft_trial_res
prop_quack_nested_random_payloads(struct theft *t, void *arg1)
{
    const struct prop_bytes *bytes = (const struct prop_bytes *)arg1;
    ducknng_quack_schema schema;
    idx_t row_count = 0;
    char *errmsg = NULL;
    int rc;
    enum theft_trial_res result = THEFT_TRIAL_PASS;

    (void)t;
    if (prop_quack_build_nested_schema(&schema) != 0) {
        ducknng_quack_schema_reset(&schema);
        return THEFT_TRIAL_SKIP;
    }
    rc = ducknng_quack_payload_read_row_count(bytes->data, bytes->len, &schema,
        &row_count, &errmsg);
    if (rc == 0 && errmsg != NULL) result = THEFT_TRIAL_FAIL;
    if (errmsg) free(errmsg);
    ducknng_quack_schema_reset(&schema);
    return result;
}

TEST wire_rejects_or_decodes_random_bytes(void)
{
    ASSERT_EQ(THEFT_RUN_PASS,
        prop_run_one("wire random bytes", prop_wire_random_bytes, &prop_random_bytes_info));
    PASS();
}

TEST wire_decodes_generated_valid_frames(void)
{
    ASSERT_EQ(THEFT_RUN_PASS,
        prop_run_one("wire valid frames", prop_wire_valid_frame_decodes, &prop_frame_info));
    PASS();
}

TEST transport_rejects_or_classifies_random_urls(void)
{
    ASSERT_EQ(THEFT_RUN_PASS,
        prop_run_one("transport random urls", prop_transport_random_urls, &prop_random_url_info));
    PASS();
}

TEST transport_known_schemes(void)
{
    static const struct {
        const char *url;
        ducknng_transport_family family;
        ducknng_transport_scheme scheme;
        int uses_tls;
    } cases[] = {
        {"inproc://prop", DUCKNNG_TRANSPORT_FAMILY_NNG, DUCKNNG_TRANSPORT_SCHEME_INPROC, 0},
        {"ipc:///tmp/ducknng-prop.ipc", DUCKNNG_TRANSPORT_FAMILY_NNG, DUCKNNG_TRANSPORT_SCHEME_IPC, 0},
        {"tcp://127.0.0.1:1234", DUCKNNG_TRANSPORT_FAMILY_NNG, DUCKNNG_TRANSPORT_SCHEME_TCP, 0},
        {"tls+tcp://127.0.0.1:1234", DUCKNNG_TRANSPORT_FAMILY_NNG, DUCKNNG_TRANSPORT_SCHEME_TLS_TCP, 1},
        {"ws://127.0.0.1:1234/rpc", DUCKNNG_TRANSPORT_FAMILY_NNG, DUCKNNG_TRANSPORT_SCHEME_WS, 0},
        {"wss://127.0.0.1:1234/rpc", DUCKNNG_TRANSPORT_FAMILY_NNG, DUCKNNG_TRANSPORT_SCHEME_WSS, 1},
        {"http://127.0.0.1:1234/rpc", DUCKNNG_TRANSPORT_FAMILY_HTTP, DUCKNNG_TRANSPORT_SCHEME_HTTP, 0},
        {"https://127.0.0.1:1234/rpc", DUCKNNG_TRANSPORT_FAMILY_HTTP, DUCKNNG_TRANSPORT_SCHEME_HTTPS, 1},
    };
    size_t i;

    for (i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        ducknng_transport_url parsed;
        char *errmsg = NULL;

        ASSERT_EQ(0, ducknng_transport_url_parse(cases[i].url, &parsed, &errmsg));
        ASSERT_EQ(cases[i].family, parsed.family);
        ASSERT_EQ(cases[i].scheme, parsed.scheme);
        ASSERT_EQ(cases[i].uses_tls, parsed.uses_tls);
        ASSERT_EQ(NULL, errmsg);
    }
    PASS();
}

TEST quack_rejects_or_scans_random_zero_column_payloads(void)
{
    ASSERT_EQ(THEFT_RUN_PASS,
        prop_run_one("quack random zero-column payloads", prop_quack_random_payloads,
            &prop_random_bytes_info));
    PASS();
}

TEST quack_rejects_fixed_width_size_overflow_fixture(void)
{
    struct prop_quack_buf payload;
    ducknng_quack_schema schema;
    idx_t row_count = 0;
    char *errmsg = NULL;
    int rc;

    ASSERT_EQ(0, prop_quack_build_one_col_schema(&schema, PROP_QK_BIGINT));
    ASSERT_EQ(0, prop_quack_payload_fixed_width_overflow(&payload));
    rc = ducknng_quack_payload_read_row_count(payload.data, payload.len, &schema,
        &row_count, &errmsg);
    ASSERT_NEQ(0, rc);
    ASSERT_EQ((idx_t)0, row_count);
    if (errmsg) free(errmsg);
    ducknng_quack_schema_reset(&schema);
    PASS();
}

TEST quack_rejects_blob_length_wraparound_fixture(void)
{
    struct prop_quack_buf payload;
    ducknng_quack_schema schema;
    idx_t row_count = 0;
    char *errmsg = NULL;
    int rc;

    ASSERT_EQ(0, prop_quack_build_one_col_schema(&schema, PROP_QK_VARCHAR));
    ASSERT_EQ(0, prop_quack_payload_varlen_wraparound(&payload));
    rc = ducknng_quack_payload_read_row_count(payload.data, payload.len, &schema,
        &row_count, &errmsg);
    ASSERT_NEQ(0, rc);
    ASSERT_EQ((idx_t)0, row_count);
    if (errmsg) free(errmsg);
    ducknng_quack_schema_reset(&schema);
    PASS();
}

TEST quack_rejects_huge_schema_column_count_fixture(void)
{
    struct prop_quack_buf payload;
    ducknng_quack_schema schema;
    struct _duckdb_bind_info bind_info;
    idx_t row_count = 0;
    char *errmsg = NULL;
    int rc;

    memset(&schema, 0, sizeof(schema));
    memset(&bind_info, 0, sizeof(bind_info));
    ASSERT_EQ(0, prop_quack_payload_huge_schema_column_count(&payload));
    rc = ducknng_quack_payload_bind_columns((duckdb_bind_info)&bind_info,
        payload.data, payload.len, &schema, &row_count, &errmsg);
    ASSERT_NEQ(0, rc);
    ASSERT_EQ((idx_t)0, row_count);
    ASSERT_EQ((idx_t)0, schema.ncols);
    ASSERT_EQ(NULL, schema.cols);
    ASSERT(errmsg != NULL);
    ASSERT(strstr(errmsg, "column count") != NULL);
    if (errmsg) free(errmsg);
    ducknng_quack_schema_reset(&schema);
    PASS();
}

TEST quack_rejects_random_nested_schema_payloads(void)
{
    ASSERT_EQ(THEFT_RUN_PASS,
        prop_run_one("quack random nested-schema payloads", prop_quack_nested_random_payloads,
            &prop_random_bytes_info));
    PASS();
}

TEST size_add_rejects_overflow_keeps_valid_sums(void)
{
    size_t out = 0;

    ASSERT_EQ(0, ducknng_size_add(10, 20, &out));
    ASSERT_EQ((size_t)30, out);

    /* Exact boundary: sum equals SIZE_MAX is representable. */
    out = 0;
    ASSERT_EQ(0, ducknng_size_add(SIZE_MAX - 1, 1, &out));
    ASSERT_EQ(SIZE_MAX, out);

    /* One past the boundary overflows and must not write a wrapped value. */
    out = 0xabcd;
    ASSERT_EQ(-1, ducknng_size_add(SIZE_MAX, 1, &out));
    ASSERT_EQ((size_t)0xabcd, out);

    out = 0xabcd;
    ASSERT_EQ(-1, ducknng_size_add(SIZE_MAX - 4, 5, &out));
    ASSERT_EQ((size_t)0xabcd, out);
    PASS();
}

TEST size_mul_rejects_overflow_keeps_valid_products(void)
{
    size_t out = 0xabcd;

    ASSERT_EQ(0, ducknng_size_mul(6, 7, &out));
    ASSERT_EQ((size_t)42, out);

    /* Zero operand can never overflow. */
    out = 0xabcd;
    ASSERT_EQ(0, ducknng_size_mul(0, SIZE_MAX, &out));
    ASSERT_EQ((size_t)0, out);

    out = 0xabcd;
    ASSERT_EQ(-1, ducknng_size_mul(SIZE_MAX, 2, &out));
    ASSERT_EQ((size_t)0xabcd, out);

    out = 0xabcd;
    ASSERT_EQ(-1, ducknng_size_mul((SIZE_MAX / 4) + 1, 4, &out));
    ASSERT_EQ((size_t)0xabcd, out);
    PASS();
}

TEST grow_capacity_meets_need_without_overflow(void)
{
    size_t cap = 0;

    /* First growth seeds from min_cap, then doubles to cover need. */
    ASSERT_EQ(0, ducknng_grow_capacity(1, 0, 256, &cap));
    ASSERT_EQ((size_t)256, cap);

    ASSERT_EQ(0, ducknng_grow_capacity(300, 256, 256, &cap));
    ASSERT(cap >= 300);
    ASSERT_EQ((size_t)512, cap);

    /* A need already satisfied returns the current capacity unchanged. */
    ASSERT_EQ(0, ducknng_grow_capacity(100, 256, 256, &cap));
    ASSERT_EQ((size_t)256, cap);

    /* Near SIZE_MAX, doubling would overflow; the helper clamps to need
     * instead of wrapping, and never returns a capacity below need. */
    ASSERT_EQ(0, ducknng_grow_capacity(SIZE_MAX, SIZE_MAX / 2 + 8, 256, &cap));
    ASSERT(cap >= SIZE_MAX - 1);
    ASSERT_EQ(SIZE_MAX, cap);
    PASS();
}

TEST size_arith_invariants_hold_for_random_pairs(void)
{
    ASSERT_EQ(THEFT_RUN_PASS,
        prop_run_one("size arithmetic invariants", prop_size_arith_invariants,
            &prop_two_sizes_info));
    PASS();
}

SUITE(size_checked_properties)
{
    RUN_TEST(size_add_rejects_overflow_keeps_valid_sums);
    RUN_TEST(size_mul_rejects_overflow_keeps_valid_products);
    RUN_TEST(grow_capacity_meets_need_without_overflow);
    RUN_TEST(size_arith_invariants_hold_for_random_pairs);
}

TEST join_dotted_path_handles_edges(void)
{
    char *r;

    r = ducknng_join_dotted_path("", "x");   ASSERT(r); ASSERT_STR_EQ("x", r);   free(r);
    r = ducknng_join_dotted_path(NULL, "x"); ASSERT(r); ASSERT_STR_EQ("x", r);   free(r);
    r = ducknng_join_dotted_path("a", "b");  ASSERT(r); ASSERT_STR_EQ("a.b", r); free(r);
    r = ducknng_join_dotted_path("a.b", "c");ASSERT(r); ASSERT_STR_EQ("a.b.c", r); free(r);
    r = ducknng_join_dotted_path("a", "");   ASSERT(r); ASSERT_STR_EQ("a.", r);  free(r);
    r = ducknng_join_dotted_path("a", NULL); ASSERT(r); ASSERT_STR_EQ("a.", r);  free(r);
    r = ducknng_join_dotted_path("", "");    ASSERT(r); ASSERT_STR_EQ("", r);    free(r);
    PASS();
}

TEST join_dotted_path_invariants_hold_for_random_pairs(void)
{
    ASSERT_EQ(THEFT_RUN_PASS,
        prop_run_one("join dotted path invariants", prop_join_dotted_path_invariants,
            &prop_two_strings_info));
    PASS();
}

SUITE(string_path_properties)
{
    RUN_TEST(join_dotted_path_handles_edges);
    RUN_TEST(join_dotted_path_invariants_hold_for_random_pairs);
}

SUITE(wire_properties)
{
    RUN_TEST(wire_rejects_or_decodes_random_bytes);
    RUN_TEST(wire_decodes_generated_valid_frames);
}

SUITE(transport_properties)
{
    RUN_TEST(transport_known_schemes);
    RUN_TEST(transport_rejects_or_classifies_random_urls);
}

SUITE(quack_properties)
{
    RUN_TEST(quack_rejects_or_scans_random_zero_column_payloads);
    RUN_TEST(quack_rejects_fixed_width_size_overflow_fixture);
    RUN_TEST(quack_rejects_blob_length_wraparound_fixture);
    RUN_TEST(quack_rejects_huge_schema_column_count_fixture);
    RUN_TEST(quack_rejects_random_nested_schema_payloads);
}

GREATEST_MAIN_DEFS();

int
main(int argc, char **argv)
{
    prop_init_duckdb_api();
    GREATEST_MAIN_BEGIN();
    RUN_SUITE(size_checked_properties);
    RUN_SUITE(string_path_properties);
    RUN_SUITE(wire_properties);
    RUN_SUITE(transport_properties);
    RUN_SUITE(quack_properties);
    GREATEST_MAIN_END();
}
