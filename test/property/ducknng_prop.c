#include "duckdb_extension.h"
DUCKDB_EXTENSION_GLOBAL
#undef duckdb_malloc
#undef duckdb_free
#undef duckdb_vector_size

#include "ducknng_quack.h"
#include "ducknng_transport.h"
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
}

GREATEST_MAIN_DEFS();

int
main(int argc, char **argv)
{
    prop_init_duckdb_api();
    GREATEST_MAIN_BEGIN();
    RUN_SUITE(wire_properties);
    RUN_SUITE(transport_properties);
    RUN_SUITE(quack_properties);
    GREATEST_MAIN_END();
}
