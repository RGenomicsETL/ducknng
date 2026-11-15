#include "ducknng_sql_shared.h"
#include "ducknng_http_compat.h"
#include "ducknng_ipc_in.h"
#include "ducknng_ipc_out.h"
#include "ducknng_quack.h"
#include "ducknng_sql_arrow.h"
#include "ducknng_runtime.h"
#include "ducknng_util.h"
#include "ducknng_wire.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <inttypes.h>
#ifdef _WIN32
#  include <windows.h>
#else
#  include <unistd.h>
#endif

DUCKDB_EXTENSION_EXTERN

typedef struct {
    bool ok;
    char *error;
    uint8_t version;
    uint8_t type;
    uint8_t status;
    uint32_t flags;
    char *type_name;
    char *name;
    char *payload_text;
    uint8_t *payload;
    idx_t payload_len;
} ducknng_frame_decode_bind_data;

typedef enum ducknng_body_codec_kind {
    DUCKNNG_BODY_CODEC_RAW = 0,
    DUCKNNG_BODY_CODEC_TEXT = 1,
    DUCKNNG_BODY_CODEC_JSON = 2,
    DUCKNNG_BODY_CODEC_CSV = 3,
    DUCKNNG_BODY_CODEC_TSV = 4,
    DUCKNNG_BODY_CODEC_PARQUET = 5,
    DUCKNNG_BODY_CODEC_ARROW_IPC = 6,
    DUCKNNG_BODY_CODEC_DUCKNNG_FRAME = 7,
    DUCKNNG_BODY_CODEC_NDJSON = 8,
    DUCKNNG_BODY_CODEC_FORM = 9,
    DUCKNNG_BODY_CODEC_QUACK_BATCH = 10,
    DUCKNNG_BODY_CODEC_USER = 11
} ducknng_body_codec_kind;

typedef enum ducknng_body_result_kind {
    DUCKNNG_BODY_RESULT_SINGLE = 1,
    DUCKNNG_BODY_RESULT_ARROW = 2,
    DUCKNNG_BODY_RESULT_FRAME = 3,
    DUCKNNG_BODY_RESULT_FORM = 4,
    DUCKNNG_BODY_RESULT_QUACK_BATCH = 5
} ducknng_body_result_kind;

typedef struct {
    int result_kind;
    int codec_kind;
    char *provider;
    char *media_type;
    uint8_t *raw;
    idx_t raw_len;
    char *text;
    int text_is_null;
    struct ArrowSchema schema;
    struct ArrowArray array;
    idx_t row_count;
    ducknng_frame_decode_bind_data frame;
    char *user_function_name; /* set when codec_kind == USER */
    char **form_keys;
    char **form_values;
    idx_t form_count;
    ducknng_quack_schema quack_schema;
} ducknng_body_parse_bind_data;

/* Scan state over one parse result. ducknng_parse_body borrows the result
 * owned by its bind data; ducknng_ncurl_table decodes a fresh result in each
 * init and sets owns_bind so the result is released with the init data. */
typedef struct {
    ducknng_body_parse_bind_data *bind;
    int owns_bind;
    idx_t offset;
    int emitted;
} ducknng_body_parse_init_data;

/* Immutable request arguments of one ducknng_ncurl_table call. client_ctx is
 * an owned wrapper; the statement keeps the underlying context alive. */
typedef struct {
    char *url;
    char *method;
    char *headers_json;
    char *profile_id;
    uint8_t *body;
    size_t body_len;
    int32_t timeout_ms;
    uint64_t tls_config_id;
    duckdb_client_context client_ctx;
    ducknng_sql_declared_schema schema;
} ducknng_ncurl_table_bind_data;

/* Bind plan of ducknng_parse_body and the ducknng_parse_{csv,tsv,parquet}
 * reader aliases. A codec that decodes the supplied bytes in C is parsed at
 * bind into `parsed`, which init borrows. Otherwise `parsed` is NULL and init
 * decodes `body` with the codec selected at bind (codec_kind and
 * user_function_name), so registry changes after bind cannot change dispatch.
 * `reader` marks the aliases, which always use the DuckDB file reader.
 * has_schema says whether `schema` holds declared columns to validate. */
typedef struct {
    uint8_t *body;
    size_t body_len;
    char *media_type;
    int codec_kind;
    int reader;
    char *user_function_name;
    duckdb_client_context client_ctx;
    int has_schema;
    ducknng_sql_declared_schema schema;
    ducknng_body_parse_bind_data *parsed;
} ducknng_body_plan_bind_data;

typedef struct {
    const char *provider;
    const char *media_types;
    const char *output;
    const char *notes;
} ducknng_codec_static_row;

typedef struct {
    char *provider;
    char *media_types;
    char *kind;
    char *function_name;
    char *output;
    char *notes;
} ducknng_codec_dyn_row;

/* One ducknng_list_codecs execution: its own builtin + user codec snapshot
 * taken at init, and the scan offset. */
typedef struct {
    ducknng_codec_dyn_row *rows;
    idx_t row_count;
    idx_t offset;
} ducknng_codecs_init_data;

typedef struct {
    idx_t emitted;
} ducknng_single_row_init_data;

static void destroy_single_row_init_data(void *ptr) {
    ducknng_single_row_init_data *data = (ducknng_single_row_init_data *)ptr;
    if (data) duckdb_free(data);
}

static int execute_sql(duckdb_connection con, const char *sql) {
    duckdb_result result;
    memset(&result, 0, sizeof(result));
    if (duckdb_query(con, sql, &result) == DuckDBError) {
        duckdb_destroy_result(&result);
        return 0;
    }
    duckdb_destroy_result(&result);
    return 1;
}

static char *ducknng_dup_bytes(const uint8_t *data, size_t len) {
    char *out = (char *)duckdb_malloc(len + 1);
    if (!out) return NULL;
    if (len) memcpy(out, data, len);
    out[len] = '\0';
    return out;
}
static const char *ducknng_rpc_type_name(uint8_t type) {
    switch (type) {
    case DUCKNNG_RPC_MANIFEST: return "manifest";
    case DUCKNNG_RPC_CALL: return "call";
    case DUCKNNG_RPC_RESULT: return "result";
    case DUCKNNG_RPC_ERROR: return "error";
    case DUCKNNG_RPC_EVENT: return "event";
    default: return "unknown";
    }
}
static char *ducknng_normalize_media_type(const char *content_type) {
    const char *start;
    const char *end;
    char *out;
    size_t len;
    size_t i;
    if (!content_type) return ducknng_strdup("");
    start = content_type;
    while (*start == ' ' || *start == '\t' || *start == '\r' || *start == '\n') start++;
    end = start;
    while (*end && *end != ';' && *end != ' ' && *end != '\t' && *end != '\r' && *end != '\n') end++;
    while (end > start && (end[-1] == ' ' || end[-1] == '\t')) end--;
    len = (size_t)(end - start);
    out = (char *)duckdb_malloc(len + 1);
    if (!out) return NULL;
    for (i = 0; i < len; i++) out[i] = (char)ducknng_ascii_tolower_int((unsigned char)start[i]);
    out[len] = '\0';
    return out;
}

static const char *ducknng_body_codec_provider_name(int codec_kind) {
    switch (codec_kind) {
        case DUCKNNG_BODY_CODEC_TEXT: return "text";
        case DUCKNNG_BODY_CODEC_JSON: return "json";
        case DUCKNNG_BODY_CODEC_CSV: return "csv";
        case DUCKNNG_BODY_CODEC_TSV: return "tsv";
        case DUCKNNG_BODY_CODEC_PARQUET: return "parquet";
        case DUCKNNG_BODY_CODEC_ARROW_IPC: return "arrow_ipc";
        case DUCKNNG_BODY_CODEC_DUCKNNG_FRAME: return "ducknng_frame";
        case DUCKNNG_BODY_CODEC_NDJSON: return "ndjson";
        case DUCKNNG_BODY_CODEC_FORM: return "form";
        case DUCKNNG_BODY_CODEC_QUACK_BATCH: return "ducknng_quack_batch";
        case DUCKNNG_BODY_CODEC_USER: return "user";
        case DUCKNNG_BODY_CODEC_RAW:
        default: return "raw";
    }
}

static int ducknng_body_codec_for_media_type(const char *media_type) {
    if (!media_type || !media_type[0]) return DUCKNNG_BODY_CODEC_RAW;
    if (ducknng_ascii_ieq(media_type, "application/vnd.ducknng.frame")) return DUCKNNG_BODY_CODEC_DUCKNNG_FRAME;
    if (ducknng_ascii_ieq(media_type, "application/vnd.ducknng.quack-batch")) return DUCKNNG_BODY_CODEC_QUACK_BATCH;
    if (ducknng_ascii_ieq(media_type, "application/vnd.apache.arrow.stream") ||
        ducknng_ascii_ieq(media_type, "application/vnd.apache.arrow.ipc") ||
        ducknng_ascii_ieq(media_type, "application/arrow")) return DUCKNNG_BODY_CODEC_ARROW_IPC;
    if (ducknng_ascii_ieq(media_type, "application/vnd.apache.parquet") ||
        ducknng_ascii_ieq(media_type, "application/parquet") ||
        ducknng_ascii_ieq(media_type, "application/x-parquet")) return DUCKNNG_BODY_CODEC_PARQUET;
    if (ducknng_ascii_ieq(media_type, "text/csv") ||
        ducknng_ascii_ieq(media_type, "application/csv") ||
        ducknng_ascii_ieq(media_type, "text/x-csv")) return DUCKNNG_BODY_CODEC_CSV;
    if (ducknng_ascii_ieq(media_type, "text/tab-separated-values") ||
        ducknng_ascii_ieq(media_type, "application/x-tsv") ||
        ducknng_ascii_ieq(media_type, "text/tsv")) return DUCKNNG_BODY_CODEC_TSV;
    if (ducknng_ascii_ieq(media_type, "application/x-ndjson") ||
        ducknng_ascii_ieq(media_type, "application/ndjson") ||
        ducknng_ascii_ieq(media_type, "application/x-jsonlines") ||
        ducknng_ascii_ieq(media_type, "application/jsonlines")) return DUCKNNG_BODY_CODEC_NDJSON;
    if (ducknng_ascii_ieq(media_type, "application/x-www-form-urlencoded")) return DUCKNNG_BODY_CODEC_FORM;
    if (ducknng_ascii_ieq(media_type, "application/json") ||
        ducknng_ascii_iends_with(media_type, "+json")) return DUCKNNG_BODY_CODEC_JSON;
    if (ducknng_ascii_istarts_with(media_type, "text/")) return DUCKNNG_BODY_CODEC_TEXT;
    return DUCKNNG_BODY_CODEC_RAW;
}

static char *ducknng_sql_quote_literal(const char *s) {
    size_t need = 3;
    size_t i;
    char *out;
    char *p;
    if (!s) return ducknng_strdup("NULL");
    for (i = 0; s[i]; i++) need += s[i] == '\'' ? 2 : 1;
    out = (char *)duckdb_malloc(need);
    if (!out) return NULL;
    p = out;
    *p++ = '\'';
    for (i = 0; s[i]; i++) {
        if (s[i] == '\'') *p++ = '\'';
        *p++ = s[i];
    }
    *p++ = '\'';
    *p = '\0';
    return out;
}

static void ducknng_frame_decode_bind_data_reset(ducknng_frame_decode_bind_data *data) {
    if (!data) return;
    if (data->error) duckdb_free(data->error);
    if (data->type_name) duckdb_free(data->type_name);
    if (data->name) duckdb_free(data->name);
    if (data->payload_text) duckdb_free(data->payload_text);
    if (data->payload) duckdb_free(data->payload);
    memset(data, 0, sizeof(*data));
}

static void destroy_frame_decode_bind_data(void *ptr) {
    ducknng_frame_decode_bind_data *data = (ducknng_frame_decode_bind_data *)ptr;
    if (!data) return;
    ducknng_frame_decode_bind_data_reset(data);
    duckdb_free(data);
}

static void destroy_body_parse_bind_data(void *ptr) {
    ducknng_body_parse_bind_data *data = (ducknng_body_parse_bind_data *)ptr;
    idx_t i;
    if (!data) return;
    if (data->provider) duckdb_free(data->provider);
    if (data->media_type) duckdb_free(data->media_type);
    if (data->raw) duckdb_free(data->raw);
    if (data->text) duckdb_free(data->text);
    if (data->user_function_name) duckdb_free(data->user_function_name);
    if (data->array.release) ArrowArrayRelease(&data->array);
    if (data->schema.release) ArrowSchemaRelease(&data->schema);
    ducknng_quack_schema_reset(&data->quack_schema);
    ducknng_frame_decode_bind_data_reset(&data->frame);
    if (data->form_keys) {
        for (i = 0; i < data->form_count; i++) {
            if (data->form_keys[i]) duckdb_free(data->form_keys[i]);
            if (data->form_values[i]) duckdb_free(data->form_values[i]);
        }
        duckdb_free(data->form_keys);
        duckdb_free(data->form_values);
    }
    duckdb_free(data);
}

static void destroy_body_parse_init_data(void *ptr) {
    ducknng_body_parse_init_data *data = (ducknng_body_parse_init_data *)ptr;
    if (!data) return;
    if (data->owns_bind && data->bind) destroy_body_parse_bind_data(data->bind);
    duckdb_free(data);
}

static void destroy_body_plan_bind_data(void *ptr) {
    ducknng_body_plan_bind_data *plan = (ducknng_body_plan_bind_data *)ptr;
    if (!plan) return;
    if (plan->body) duckdb_free(plan->body);
    if (plan->media_type) duckdb_free(plan->media_type);
    if (plan->user_function_name) duckdb_free(plan->user_function_name);
    if (plan->client_ctx) duckdb_destroy_client_context(&plan->client_ctx);
    ducknng_sql_declared_schema_reset(&plan->schema);
    if (plan->parsed) destroy_body_parse_bind_data(plan->parsed);
    duckdb_free(plan);
}

static void destroy_ncurl_table_bind_data(void *ptr) {
    ducknng_ncurl_table_bind_data *data = (ducknng_ncurl_table_bind_data *)ptr;
    if (!data) return;
    if (data->url) duckdb_free(data->url);
    if (data->method) duckdb_free(data->method);
    if (data->headers_json) duckdb_free(data->headers_json);
    if (data->profile_id) duckdb_free(data->profile_id);
    if (data->body) duckdb_free(data->body);
    if (data->client_ctx) duckdb_destroy_client_context(&data->client_ctx);
    ducknng_sql_declared_schema_reset(&data->schema);
    duckdb_free(data);
}

static void destroy_codecs_init_data(void *ptr) {
    ducknng_codecs_init_data *data = (ducknng_codecs_init_data *)ptr;
    idx_t i;
    if (!data) return;
    if (data->rows) {
        for (i = 0; i < data->row_count; i++) {
            if (data->rows[i].provider) duckdb_free(data->rows[i].provider);
            if (data->rows[i].media_types) duckdb_free(data->rows[i].media_types);
            if (data->rows[i].kind) duckdb_free(data->rows[i].kind);
            if (data->rows[i].function_name) duckdb_free(data->rows[i].function_name);
            if (data->rows[i].output) duckdb_free(data->rows[i].output);
            if (data->rows[i].notes) duckdb_free(data->rows[i].notes);
        }
        duckdb_free(data->rows);
    }
    duckdb_free(data);
}

static int ducknng_lookup_tls_config_copy(ducknng_sql_context *ctx, uint64_t tls_config_id,
    uint64_t *out_id, char **out_source, ducknng_tls_opts *out_opts, char **errmsg) {
    size_t i;
    ducknng_tls_config *cfg = NULL;
    if (out_id) *out_id = 0;
    if (out_source) *out_source = NULL;
    if (out_opts) ducknng_tls_opts_init(out_opts);
    if (tls_config_id == 0) return 0;
    if (!ctx || !ctx->rt) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing runtime for TLS lookup");
        return -1;
    }
    ducknng_mutex_lock(&ctx->rt->mu);
    for (i = 0; i < ctx->rt->tls_config_count; i++) {
        if (ctx->rt->tls_configs[i] && ctx->rt->tls_configs[i]->tls_config_id == tls_config_id) {
            cfg = ctx->rt->tls_configs[i];
            break;
        }
    }
    if (!cfg) {
        ducknng_mutex_unlock(&ctx->rt->mu);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: tls config not found");
        return -1;
    }
    if (out_id) *out_id = cfg->tls_config_id;
    if (out_source && cfg->source) {
        *out_source = ducknng_strdup(cfg->source);
        if (!*out_source) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying TLS source");
            return -1;
        }
    }
    if (out_opts && ducknng_tls_opts_copy(out_opts, &cfg->opts) != 0) {
        if (out_source && *out_source) {
            duckdb_free(*out_source);
            *out_source = NULL;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying TLS options");
        return -1;
    }
    ducknng_mutex_unlock(&ctx->rt->mu);
    return 0;
}

static int ducknng_lookup_tls_opts(ducknng_sql_context *ctx, uint64_t tls_config_id,
    const ducknng_tls_opts **out_opts, char **errmsg) {
    static _Thread_local ducknng_tls_opts tls_copy;
    static _Thread_local int tls_copy_valid = 0;
    if (out_opts) *out_opts = NULL;
    if (tls_copy_valid) {
        ducknng_tls_opts_reset(&tls_copy);
        tls_copy_valid = 0;
    }
    if (tls_config_id == 0) return 0;
    if (ducknng_lookup_tls_config_copy(ctx, tls_config_id, NULL, NULL, &tls_copy, errmsg) != 0) {
        return -1;
    }
    tls_copy_valid = 1;
    if (out_opts) *out_opts = &tls_copy;
    return 0;
}

static void ducknng_single_row_init(duckdb_init_info info) {
    ducknng_single_row_init_data *init = (ducknng_single_row_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->emitted = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_single_row_init_data);
}

/* ducknng_decode_frame() and the application/vnd.ducknng.frame body codec
 * publish the same decoded envelope projection, so both go through one column
 * declaration and one row emitter. */
static void ducknng_body_parse_add_frame_columns(duckdb_bind_info info);
static void ducknng_body_parse_emit_frame_row(duckdb_data_chunk output,
    const ducknng_frame_decode_bind_data *frame);

static void ducknng_decode_frame_bind(duckdb_bind_info info) {
    ducknng_frame_decode_bind_data *bind;
    duckdb_logical_type type;
    duckdb_value frame_val;
    duckdb_blob blob;
    ducknng_frame frame;
    bind = (ducknng_frame_decode_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    frame_val = duckdb_bind_get_parameter(info, 0);
    if (duckdb_is_null_value(frame_val)) {
        memset(&blob, 0, sizeof(blob));
    } else {
        blob = duckdb_get_blob(frame_val);
    }
    duckdb_destroy_value(&frame_val);
    if (!blob.data || blob.size == 0) {
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: frame blob must not be NULL or empty");
    } else if (ducknng_decode_frame_bytes((const uint8_t *)blob.data, (size_t)blob.size, &frame) != 0) {
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: invalid frame envelope");
    } else {
        bind->ok = frame.type != DUCKNNG_RPC_ERROR;
        bind->version = frame.version;
        bind->type = frame.type;
        bind->status = frame.status;
        bind->flags = frame.flags;
        bind->type_name = ducknng_strdup(ducknng_rpc_type_name(frame.type));
        if (!bind->type_name) {
            bind->ok = false;
            bind->error = ducknng_strdup("ducknng: out of memory copying frame type name");
        } else {
            if (frame.error_len > 0) {
                bind->error = ducknng_dup_bytes(frame.error, (size_t)frame.error_len);
                if (!bind->error) {
                    bind->ok = false;
                    bind->error = ducknng_strdup("ducknng: out of memory copying frame error text");
                }
            } else if (frame.type == DUCKNNG_RPC_ERROR) {
                bind->error = ducknng_strdup("ducknng: error frame");
            }
            if (frame.name_len > 0) bind->name = ducknng_dup_bytes(frame.name, (size_t)frame.name_len);
            bind->payload_len = (idx_t)frame.payload_len;
            if (frame.payload_len > 0) {
                bind->payload = (uint8_t *)duckdb_malloc((size_t)frame.payload_len);
                if (!bind->payload) {
                    bind->ok = false;
                    bind->error = ducknng_strdup("ducknng: out of memory copying frame payload");
                } else {
                    memcpy(bind->payload, frame.payload, (size_t)frame.payload_len);
                    if (ducknng_sql_bytes_look_text(frame.payload, (size_t)frame.payload_len)) {
                        bind->payload_text = ducknng_dup_bytes(frame.payload, (size_t)frame.payload_len);
                    }
                }
            }
        }
    }
    if (blob.data) duckdb_free(blob.data);
    ducknng_body_parse_add_frame_columns(info);
    duckdb_bind_set_bind_data(info, bind, destroy_frame_decode_bind_data);
    duckdb_bind_set_cardinality(info, 1, true);
}

static void ducknng_decode_frame_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_single_row_init_data *init = (ducknng_single_row_init_data *)duckdb_function_get_init_data(info);
    ducknng_frame_decode_bind_data *bind = (ducknng_frame_decode_bind_data *)duckdb_function_get_bind_data(info);
    if (!init || !bind || init->emitted) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    ducknng_body_parse_emit_frame_row(output, bind);
    duckdb_data_chunk_set_size(output, 1);
    init->emitted = 1;
}

static int ducknng_body_parse_frame_copy(const uint8_t *data, size_t len,
    ducknng_frame_decode_bind_data *out) {
    ducknng_frame frame;
    if (!out) return -1;
    memset(out, 0, sizeof(*out));
    if (!data || len == 0) {
        out->ok = false;
        out->error = ducknng_strdup("ducknng: frame blob must not be NULL or empty");
        return 0;
    }
    if (ducknng_decode_frame_bytes(data, len, &frame) != 0) {
        out->ok = false;
        out->error = ducknng_strdup("ducknng: invalid frame envelope");
        return 0;
    }
    out->ok = frame.type != DUCKNNG_RPC_ERROR;
    out->version = frame.version;
    out->type = frame.type;
    out->status = frame.status;
    out->flags = frame.flags;
    out->type_name = ducknng_strdup(ducknng_rpc_type_name(frame.type));
    if (!out->type_name) {
        out->ok = false;
        out->error = ducknng_strdup("ducknng: out of memory copying frame type name");
        return -1;
    }
    if (frame.error_len > 0) {
        out->error = ducknng_dup_bytes(frame.error, (size_t)frame.error_len);
        if (!out->error) {
            out->ok = false;
            out->error = ducknng_strdup("ducknng: out of memory copying frame error text");
            return -1;
        }
    } else if (frame.type == DUCKNNG_RPC_ERROR) {
        out->error = ducknng_strdup("ducknng: error frame");
    }
    if (frame.name_len > 0) out->name = ducknng_dup_bytes(frame.name, (size_t)frame.name_len);
    out->payload_len = (idx_t)frame.payload_len;
    if (frame.payload_len > 0) {
        out->payload = (uint8_t *)duckdb_malloc((size_t)frame.payload_len);
        if (!out->payload) {
            out->ok = false;
            out->error = ducknng_strdup("ducknng: out of memory copying frame payload");
            return -1;
        }
        memcpy(out->payload, frame.payload, (size_t)frame.payload_len);
        if (ducknng_sql_bytes_look_text(frame.payload, (size_t)frame.payload_len)) {
            out->payload_text = ducknng_dup_bytes(frame.payload, (size_t)frame.payload_len);
        }
    }
    return 0;
}

static int ducknng_decode_frame_scalar_value(duckdb_vector input, idx_t row, ducknng_frame *frame,
    uint8_t **frame_bytes, idx_t *frame_len) {
    *frame_bytes = arg_blob_dup(input, row, frame_len);
    if (!*frame_bytes && *frame_len > 0) return -1;
    if (!*frame_bytes || *frame_len == 0) return -1;
    return ducknng_decode_frame_bytes(*frame_bytes, (size_t)*frame_len, frame);
}

static void ducknng_frame_payload_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    duckdb_vector frame_vec = duckdb_data_chunk_get_vector(input, 0);
    (void)info;
    for (row = 0; row < count; row++) {
        ducknng_frame frame;
        idx_t frame_len = 0;
        uint8_t *frame_bytes = NULL;
        if (ducknng_decode_frame_scalar_value(frame_vec, row, &frame, &frame_bytes, &frame_len) != 0 ||
            frame.payload_len == 0 || !frame.payload) {
            if (frame_bytes) duckdb_free(frame_bytes);
            set_null(output, row);
            continue;
        }
        assign_blob(output, row, frame.payload, (idx_t)frame.payload_len);
        duckdb_free(frame_bytes);
    }
}

static void ducknng_frame_payload_text_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    duckdb_vector frame_vec = duckdb_data_chunk_get_vector(input, 0);
    (void)info;
    for (row = 0; row < count; row++) {
        ducknng_frame frame;
        idx_t frame_len = 0;
        uint8_t *frame_bytes = NULL;
        char *text = NULL;
        if (ducknng_decode_frame_scalar_value(frame_vec, row, &frame, &frame_bytes, &frame_len) != 0 ||
            frame.payload_len == 0 || !frame.payload ||
            !ducknng_sql_bytes_look_text(frame.payload, (size_t)frame.payload_len)) {
            if (frame_bytes) duckdb_free(frame_bytes);
            set_null(output, row);
            continue;
        }
        text = ducknng_dup_bytes(frame.payload, (size_t)frame.payload_len);
        if (!text) {
            if (frame_bytes) duckdb_free(frame_bytes);
            duckdb_scalar_function_set_error(info, "ducknng: out of memory copying frame payload text");
            return;
        }
        duckdb_unsafe_vector_assign_string_element_len(output, row, text, (idx_t)strlen(text));
        duckdb_free(text);
        duckdb_free(frame_bytes);
    }
}

static void ducknng_frame_error_text_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    duckdb_vector frame_vec = duckdb_data_chunk_get_vector(input, 0);
    (void)info;
    for (row = 0; row < count; row++) {
        ducknng_frame frame;
        idx_t frame_len = 0;
        uint8_t *frame_bytes = NULL;
        char *text = NULL;
        if (ducknng_decode_frame_scalar_value(frame_vec, row, &frame, &frame_bytes, &frame_len) != 0 ||
            frame.error_len == 0 || !frame.error) {
            if (frame_bytes) duckdb_free(frame_bytes);
            set_null(output, row);
            continue;
        }
        text = ducknng_dup_bytes(frame.error, (size_t)frame.error_len);
        if (!text) {
            if (frame_bytes) duckdb_free(frame_bytes);
            duckdb_scalar_function_set_error(info, "ducknng: out of memory copying frame error text");
            return;
        }
        duckdb_unsafe_vector_assign_string_element_len(output, row, text, (idx_t)strlen(text));
        duckdb_free(text);
        duckdb_free(frame_bytes);
    }
}

static void ducknng_frame_version_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    duckdb_vector frame_vec = duckdb_data_chunk_get_vector(input, 0);
    uint8_t *out = (uint8_t *)duckdb_vector_get_data(output);
    (void)info;
    for (row = 0; row < count; row++) {
        ducknng_frame frame;
        idx_t frame_len = 0;
        uint8_t *frame_bytes = NULL;
        if (ducknng_decode_frame_scalar_value(frame_vec, row, &frame, &frame_bytes, &frame_len) != 0) {
            if (frame_bytes) duckdb_free(frame_bytes);
            set_null(output, row);
            continue;
        }
        out[row] = frame.version;
        duckdb_free(frame_bytes);
    }
}

static void ducknng_frame_type_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    duckdb_vector frame_vec = duckdb_data_chunk_get_vector(input, 0);
    uint8_t *out = (uint8_t *)duckdb_vector_get_data(output);
    (void)info;
    for (row = 0; row < count; row++) {
        ducknng_frame frame;
        idx_t frame_len = 0;
        uint8_t *frame_bytes = NULL;
        if (ducknng_decode_frame_scalar_value(frame_vec, row, &frame, &frame_bytes, &frame_len) != 0) {
            if (frame_bytes) duckdb_free(frame_bytes);
            set_null(output, row);
            continue;
        }
        out[row] = frame.type;
        duckdb_free(frame_bytes);
    }
}

static void ducknng_frame_status_scalar(duckdb_function_info info,
    duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    duckdb_vector frame_vec = duckdb_data_chunk_get_vector(input, 0);
    uint8_t *out = (uint8_t *)duckdb_vector_get_data(output);
    (void)info;
    for (row = 0; row < count; row++) {
        ducknng_frame frame;
        idx_t frame_len = 0;
        uint8_t *frame_bytes = NULL;
        if (ducknng_decode_frame_scalar_value(frame_vec, row, &frame,
                &frame_bytes, &frame_len) != 0) {
            if (frame_bytes) duckdb_free(frame_bytes);
            set_null(output, row);
            continue;
        }
        out[row] = frame.status;
        duckdb_free(frame_bytes);
    }
}

static void ducknng_frame_flags_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    duckdb_vector frame_vec = duckdb_data_chunk_get_vector(input, 0);
    uint32_t *out = (uint32_t *)duckdb_vector_get_data(output);
    (void)info;
    for (row = 0; row < count; row++) {
        ducknng_frame frame;
        idx_t frame_len = 0;
        uint8_t *frame_bytes = NULL;
        if (ducknng_decode_frame_scalar_value(frame_vec, row, &frame, &frame_bytes, &frame_len) != 0) {
            if (frame_bytes) duckdb_free(frame_bytes);
            set_null(output, row);
            continue;
        }
        out[row] = frame.flags;
        duckdb_free(frame_bytes);
    }
}

static void ducknng_frame_type_name_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    duckdb_vector frame_vec = duckdb_data_chunk_get_vector(input, 0);
    (void)info;
    for (row = 0; row < count; row++) {
        ducknng_frame frame;
        idx_t frame_len = 0;
        uint8_t *frame_bytes = NULL;
        const char *type_name = NULL;
        if (ducknng_decode_frame_scalar_value(frame_vec, row, &frame, &frame_bytes, &frame_len) != 0) {
            if (frame_bytes) duckdb_free(frame_bytes);
            set_null(output, row);
            continue;
        }
        type_name = ducknng_rpc_type_name(frame.type);
        if (!type_name || !type_name[0]) {
            duckdb_free(frame_bytes);
            set_null(output, row);
            continue;
        }
        duckdb_unsafe_vector_assign_string_element_len(output, row, type_name, (idx_t)strlen(type_name));
        duckdb_free(frame_bytes);
    }
}

static void ducknng_frame_name_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    duckdb_vector frame_vec = duckdb_data_chunk_get_vector(input, 0);
    (void)info;
    for (row = 0; row < count; row++) {
        ducknng_frame frame;
        idx_t frame_len = 0;
        uint8_t *frame_bytes = NULL;
        char *name = NULL;
        if (ducknng_decode_frame_scalar_value(frame_vec, row, &frame, &frame_bytes, &frame_len) != 0 ||
            frame.name_len == 0 || !frame.name) {
            if (frame_bytes) duckdb_free(frame_bytes);
            set_null(output, row);
            continue;
        }
        name = ducknng_dup_bytes(frame.name, (size_t)frame.name_len);
        if (!name) {
            if (frame_bytes) duckdb_free(frame_bytes);
            duckdb_scalar_function_set_error(info, "ducknng: out of memory copying frame name");
            return;
        }
        duckdb_unsafe_vector_assign_string_element_len(output, row, name, (idx_t)strlen(name));
        duckdb_free(name);
        duckdb_free(frame_bytes);
    }
}

static void ducknng_frame_end_of_stream_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    duckdb_vector frame_vec = duckdb_data_chunk_get_vector(input, 0);
    bool *out = (bool *)duckdb_vector_get_data(output);
    (void)info;
    for (row = 0; row < count; row++) {
        ducknng_frame frame;
        idx_t frame_len = 0;
        uint8_t *frame_bytes = NULL;
        if (ducknng_decode_frame_scalar_value(frame_vec, row, &frame, &frame_bytes, &frame_len) != 0) {
            if (frame_bytes) duckdb_free(frame_bytes);
            set_null(output, row);
            continue;
        }
        out[row] = (frame.flags & DUCKNNG_RPC_FLAG_END_OF_STREAM) != 0;
        duckdb_free(frame_bytes);
    }
}

/* Fixed body result layouts. Envelope columns of the frame layout follow the
 * wire order: version, type, status, flags. ducknng_aio_collect_decoded()
 * emits the same order, so one client tuple-unpacking helper works against
 * every decoded-frame projection. */
#define DUCKNNG_BODY_FRAME_NCOLS 10
static const char *const ducknng_body_frame_names[DUCKNNG_BODY_FRAME_NCOLS] = {
    "ok", "error", "version", "type", "status", "flags",
    "type_name", "name", "payload", "payload_text"
};
static const duckdb_type ducknng_body_frame_types[DUCKNNG_BODY_FRAME_NCOLS] = {
    DUCKDB_TYPE_BOOLEAN, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UTINYINT,
    DUCKDB_TYPE_UTINYINT, DUCKDB_TYPE_UTINYINT, DUCKDB_TYPE_UINTEGER,
    DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_BLOB, DUCKDB_TYPE_VARCHAR
};
static const char *const ducknng_body_value_names[1] = { "value" };
static const char *const ducknng_body_text_names[1] = { "body_text" };
static const char *const ducknng_body_raw_names[1] = { "body" };
static const duckdb_type ducknng_body_varchar_types[2] = {
    DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR
};
static const duckdb_type ducknng_body_blob_types[1] = { DUCKDB_TYPE_BLOB };
static const char *const ducknng_body_form_names[2] = { "name", "value" };

static int ducknng_body_add_fixed_columns(duckdb_bind_info info, idx_t ncols,
    const char *const *names, const duckdb_type *types) {
    idx_t i;
    for (i = 0; i < ncols; i++) {
        duckdb_logical_type type = duckdb_create_logical_type(types[i]);
        if (!type) return -1;
        duckdb_bind_add_result_column(info, names[i], type);
        duckdb_destroy_logical_type(&type);
    }
    return 0;
}

static void ducknng_body_parse_add_frame_columns(duckdb_bind_info info) {
    (void)ducknng_body_add_fixed_columns(info, DUCKNNG_BODY_FRAME_NCOLS,
        ducknng_body_frame_names, ducknng_body_frame_types);
}

static void ducknng_body_parse_emit_frame_row(duckdb_data_chunk output, const ducknng_frame_decode_bind_data *frame) {
    bool *ok_data = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0));
    uint8_t *version_data = (uint8_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 2));
    uint8_t *type_data = (uint8_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 3));
    uint8_t *status_data = (uint8_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 4));
    uint32_t *flags_data = (uint32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 5));
    ok_data[0] = frame ? frame->ok : false;
    if (frame && frame->error) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 1), 0, frame->error, (idx_t)strlen(frame->error));
    else set_null(duckdb_data_chunk_get_vector(output, 1), 0);
    version_data[0] = frame ? frame->version : 0;
    type_data[0] = frame ? frame->type : 0;
    status_data[0] = frame ? frame->status : 0;
    flags_data[0] = frame ? frame->flags : 0;
    if (frame && frame->type_name) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 6), 0, frame->type_name, (idx_t)strlen(frame->type_name));
    else set_null(duckdb_data_chunk_get_vector(output, 6), 0);
    if (frame && frame->name) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 7), 0, frame->name, (idx_t)strlen(frame->name));
    else set_null(duckdb_data_chunk_get_vector(output, 7), 0);
    if (frame && frame->payload) assign_blob(duckdb_data_chunk_get_vector(output, 8), 0, frame->payload, frame->payload_len);
    else set_null(duckdb_data_chunk_get_vector(output, 8), 0);
    if (frame && frame->payload_text) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 9), 0, frame->payload_text, (idx_t)strlen(frame->payload_text));
    else set_null(duckdb_data_chunk_get_vector(output, 9), 0);
}

/* Body decoding runs codec SQL through the runtime codec scope on behalf of
 * the parent SQL connection. A parent that is codec_con itself is codec SQL
 * decoding a nested body, which fails instead of deadlocking. */
static int ducknng_body_codec_enter(ducknng_sql_context *ctx,
    duckdb_client_context parent, ducknng_codec_scope *scope, char **errmsg) {
    memset(scope, 0, sizeof(*scope));
    if (!parent) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing DuckDB client context for body codec");
        return -1;
    }
    return ducknng_runtime_codec_enter(ctx->rt,
        (uint64_t)duckdb_client_context_get_connection_id(parent),
        "ducknng: a body codec cannot decode a nested body", scope, errmsg);
}

static void ducknng_body_codec_leave(ducknng_sql_context *ctx, ducknng_codec_scope *scope) {
    ducknng_runtime_codec_leave(ctx->rt, scope);
}

static int ducknng_body_parse_run_duckdb_reader(ducknng_sql_context *ctx,
    duckdb_client_context parent, ducknng_body_parse_bind_data *bind,
    const uint8_t *body, size_t body_len, int codec_kind, char **errmsg) {
    ducknng_codec_scope codec_scope;
    char *json_text = NULL;
    char *quoted = NULL;
    char *sql = NULL;
    uint8_t *payload = NULL;
    size_t payload_len = 0;
    size_t sql_len;
    const char *template_array_struct =
        "WITH _parsed(v) AS (SELECT from_json(%s::JSON, json_structure(%s::JSON))) "
        "SELECT s.* FROM _parsed, unnest(v) AS t(s)";
    const char *template_array_value =
        "WITH _parsed(v) AS (SELECT from_json(%s::JSON, json_structure(%s::JSON))) "
        "SELECT unnest(v) AS value FROM _parsed";
    const char *template_object =
        "WITH _parsed(v) AS (SELECT from_json(%s::JSON, json_structure(%s::JSON))) "
        "SELECT v.* FROM _parsed";
    const char *template_scalar =
        "SELECT from_json(%s::JSON, json_structure(%s::JSON)) AS value";
    const char *chosen_template = NULL;
    const uint8_t *p;
    const uint8_t *end;
    int tried_array_struct = 0;
    int rc = -1;
    if (errmsg) *errmsg = NULL;
    if (!ctx || !ctx->rt || !ducknng_runtime_codec_connection(ctx->rt) || !bind) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing runtime for body parser");
        return -1;
    }
    if (codec_kind != DUCKNNG_BODY_CODEC_JSON) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: this body codec needs memory-backed DuckDB reader support and is not enabled");
        return -1;
    }
    if (!body || body_len == 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: JSON body must not be empty");
        return -1;
    }
    if (!ducknng_sql_bytes_look_text(body, body_len)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: JSON body is not valid UTF-8 text");
        return -1;
    }
    json_text = ducknng_dup_bytes(body, body_len);
    quoted = ducknng_sql_quote_literal(json_text ? json_text : "");
    if (!json_text || !quoted) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory preparing JSON body");
        goto cleanup;
    }
    p = body;
    end = body + body_len;
    while (p < end && (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n')) p++;
    if (p < end && *p == '[') {
        chosen_template = template_array_struct;
        tried_array_struct = 1;
    } else if (p < end && *p == '{') {
        chosen_template = template_object;
    } else {
        chosen_template = template_scalar;
    }
retry_build:
    sql_len = snprintf(NULL, 0, chosen_template, quoted, quoted) + 1;
    sql = (char *)duckdb_malloc(sql_len);
    if (!sql) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory building JSON parser query");
        goto cleanup;
    }
    snprintf(sql, sql_len, chosen_template, quoted, quoted);
    if (ducknng_body_codec_enter(ctx, parent, &codec_scope, errmsg) != 0) goto cleanup;
    if (ducknng_query_to_ipc_stream(ducknng_runtime_codec_connection(ctx->rt),
            sql, &payload, &payload_len, errmsg) != 0) {
        ducknng_body_codec_leave(ctx, &codec_scope);
        if (tried_array_struct) {
            if (errmsg && *errmsg) { duckdb_free(*errmsg); *errmsg = NULL; }
            tried_array_struct = 0;
            chosen_template = template_array_value;
            duckdb_free(sql);
            sql = NULL;
            goto retry_build;
        }
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: JSON body parser query failed");
        goto cleanup;
    }
    ducknng_body_codec_leave(ctx, &codec_scope);
    if (ducknng_decode_ipc_table_payload(payload, payload_len, &bind->schema, &bind->array, errmsg) != 0) goto cleanup;
    bind->result_kind = DUCKNNG_BODY_RESULT_ARROW;
    bind->row_count = (idx_t)bind->array.length;
    rc = 0;
cleanup:
    if (payload) duckdb_free(payload);
    if (sql) duckdb_free(sql);
    if (quoted) duckdb_free(quoted);
    if (json_text) duckdb_free(json_text);
    return rc;
}

static int ducknng_is_sql_identifier(const char *s) {
    /* Tight allowlist for codec scalar function names so register/unregister cannot
     * splice arbitrary SQL into the dispatcher. ASCII letter/underscore start, then
     * letters/digits/underscores/dots (for schema-qualified names). */
    size_t i;
    if (!s || !*s) return 0;
    if (!((s[0] >= 'a' && s[0] <= 'z') || (s[0] >= 'A' && s[0] <= 'Z') || s[0] == '_')) return 0;
    for (i = 1; s[i]; i++) {
        char c = s[i];
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || c == '_' || c == '.')) return 0;
    }
    return 1;
}

static int ducknng_body_parse_run_user_codec(ducknng_sql_context *ctx,
    duckdb_client_context parent, ducknng_body_parse_bind_data *bind,
    const uint8_t *body, size_t body_len, const char *function_name, char **errmsg) {
    ducknng_codec_scope codec_scope;
    duckdb_prepared_statement stmt = NULL;
    duckdb_result result;
    char *sql = NULL;
    size_t sql_len;
    const char *prep_err;
    int rc = -1;
    memset(&result, 0, sizeof(result));
    if (errmsg) *errmsg = NULL;
    if (!ctx || !ctx->rt || !ducknng_runtime_codec_connection(ctx->rt)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing runtime for user codec");
        return -1;
    }
    if (!ducknng_is_sql_identifier(function_name)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: codec function name must be a plain SQL identifier");
        return -1;
    }
    /* Build "SELECT <fn>(?::BLOB) AS value" and prepare it against the codec
     * connection. The body BLOB binds as parameter 1; identifier name was
     * validated above so direct splicing is safe. */
    sql_len = strlen(function_name) + 32;
    sql = (char *)duckdb_malloc(sql_len);
    if (!sql) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory building user codec query");
        return -1;
    }
    snprintf(sql, sql_len, "SELECT %s(?::BLOB) AS value", function_name);
    if (ducknng_body_codec_enter(ctx, parent, &codec_scope, errmsg) != 0) {
        duckdb_free(sql);
        return -1;
    }
    if (duckdb_prepare(ducknng_runtime_codec_connection(ctx->rt), sql, &stmt) == DuckDBError) {
        prep_err = stmt ? duckdb_prepare_error(stmt) : NULL;
        if (errmsg) *errmsg = ducknng_strdup(prep_err && prep_err[0] ? prep_err : "ducknng: failed to prepare user codec query");
        goto cleanup;
    }
    if (duckdb_bind_blob(stmt, 1, body ? body : (const void *)"", (idx_t)body_len) == DuckDBError) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to bind body to user codec");
        goto cleanup;
    }
    if (duckdb_execute_prepared(stmt, &result) == DuckDBError) {
        const char *exec_err = duckdb_result_error(&result);
        if (errmsg) *errmsg = ducknng_strdup(exec_err && exec_err[0] ? exec_err : "ducknng: user codec execution failed");
        goto cleanup;
    }
    {
        duckdb_data_chunk chunk = duckdb_fetch_chunk(result);
        idx_t chunk_size;
        duckdb_vector vec;
        duckdb_logical_type col_type;
        duckdb_type col_type_id;
        if (!chunk) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: user codec returned no rows");
            goto cleanup;
        }
        chunk_size = duckdb_data_chunk_get_size(chunk);
        if (chunk_size < 1 || duckdb_data_chunk_get_column_count(chunk) < 1) {
            duckdb_destroy_data_chunk(&chunk);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: user codec returned no rows");
            goto cleanup;
        }
        vec = duckdb_data_chunk_get_vector(chunk, 0);
        col_type = duckdb_vector_get_column_type(vec);
        col_type_id = duckdb_get_type_id(col_type);
        duckdb_destroy_logical_type(&col_type);
        if (col_type_id != DUCKDB_TYPE_VARCHAR) {
            duckdb_destroy_data_chunk(&chunk);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: user codec must return VARCHAR (cast inside the function if needed)");
            goto cleanup;
        }
        if (arg_is_null(vec, 0)) {
            bind->text_is_null = 1;
            bind->text = NULL;
        } else {
            duckdb_string_t *strs = (duckdb_string_t *)duckdb_vector_get_data(vec);
            const char *src = duckdb_string_t_data(&strs[0]);
            uint32_t len = duckdb_string_t_length(strs[0]);
            bind->text = (char *)duckdb_malloc((size_t)len + 1);
            if (!bind->text) {
                duckdb_destroy_data_chunk(&chunk);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying user codec result");
                goto cleanup;
            }
            if (len) memcpy(bind->text, src, len);
            bind->text[len] = '\0';
        }
        duckdb_destroy_data_chunk(&chunk);
    }
    rc = 0;
cleanup:
    if (result.internal_data) duckdb_destroy_result(&result);
    if (stmt) duckdb_destroy_prepare(&stmt);
    ducknng_body_codec_leave(ctx, &codec_scope);
    if (sql) duckdb_free(sql);
    return rc;
}

/* ---------------------------------------------------------------------------
 * NDJSON: wrap newline-delimited JSON lines into a JSON array in memory so
 * the existing JSON reader path can parse it.  Returns a duckdb_malloc'd
 * NUL-terminated string that the caller must duckdb_free, or NULL on error.
 * Empty / whitespace-only lines are skipped.
 * --------------------------------------------------------------------------- */
static char *ducknng_ndjson_to_json_array(const uint8_t *body, size_t body_len, char **errmsg) {
    const char *src = (const char *)body;
    const char *end = src + body_len;
    const char *p;
    /* Two passes: first count total chars needed, then fill. */
    size_t total = 3; /* '[' + ']' + '\0' */
    int first = 1;
    char *out;
    char *q;
    p = src;
    while (p < end) {
        const char *line_start = p;
        const char *line_end;
        while (p < end && *p != '\n') p++;
        line_end = p;
        if (p < end) p++; /* skip '\n' */
        /* Trim trailing CR */
        while (line_end > line_start && line_end[-1] == '\r') line_end--;
        /* Skip leading whitespace */
        while (line_start < line_end && ((unsigned char)*line_start == ' ' || (unsigned char)*line_start == '\t')) line_start++;
        if (line_start >= line_end) continue; /* blank line */
        if (!first) total += 1; /* ',' */
        total += (size_t)(line_end - line_start);
        first = 0;
    }
    if (first) {
        /* No non-blank lines — return empty array */
        out = (char *)duckdb_malloc(3);
        if (!out) { if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory building NDJSON array"); return NULL; }
        out[0] = '['; out[1] = ']'; out[2] = '\0';
        return out;
    }
    out = (char *)duckdb_malloc(total);
    if (!out) { if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory building NDJSON array"); return NULL; }
    q = out;
    *q++ = '[';
    first = 1;
    p = src;
    while (p < end) {
        const char *line_start = p;
        const char *line_end;
        while (p < end && *p != '\n') p++;
        line_end = p;
        if (p < end) p++;
        while (line_end > line_start && line_end[-1] == '\r') line_end--;
        while (line_start < line_end && ((unsigned char)*line_start == ' ' || (unsigned char)*line_start == '\t')) line_start++;
        if (line_start >= line_end) continue;
        if (!first) *q++ = ',';
        memcpy(q, line_start, (size_t)(line_end - line_start));
        q += (size_t)(line_end - line_start);
        first = 0;
    }
    *q++ = ']';
    *q = '\0';
    return out;
}

/* ---------------------------------------------------------------------------
 * URL decode helpers for application/x-www-form-urlencoded
 * --------------------------------------------------------------------------- */
static int ducknng_hex_val(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return 10 + (c - 'a');
    if (c >= 'A' && c <= 'F') return 10 + (c - 'A');
    return -1;
}

/* Decode a percent-encoded string of 'len' bytes into a NUL-terminated
 * duckdb_malloc'd string.  '+' is decoded as space. */
static char *ducknng_url_decode(const char *src, size_t len) {
    char *out = (char *)duckdb_malloc(len + 1);
    char *q;
    const char *end;
    if (!out) return NULL;
    q = out;
    end = src + len;
    while (src < end) {
        if (*src == '%' && src + 2 < end) {
            int hi = ducknng_hex_val(src[1]);
            int lo = ducknng_hex_val(src[2]);
            if (hi >= 0 && lo >= 0) {
                *q++ = (char)((hi << 4) | lo);
                src += 3;
                continue;
            }
        }
        if (*src == '+') { *q++ = ' '; src++; continue; }
        *q++ = *src++;
    }
    *q = '\0';
    return out;
}

/* Parse application/x-www-form-urlencoded body into parallel key/value arrays.
 * Returns 0 on success, -1 on allocation failure. */
static int ducknng_parse_form_urlencoded(const uint8_t *body, size_t body_len,
    char ***out_keys, char ***out_values, idx_t *out_count, char **errmsg) {
    const char *src = (const char *)body;
    const char *end = src + body_len;
    const char *p;
    idx_t capacity = 8;
    idx_t count = 0;
    char **keys;
    char **values;
    *out_keys = NULL; *out_values = NULL; *out_count = 0;
    keys = (char **)duckdb_malloc(sizeof(char *) * capacity);
    values = (char **)duckdb_malloc(sizeof(char *) * capacity);
    if (!keys || !values) {
        if (keys) duckdb_free(keys);
        if (values) duckdb_free(values);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory parsing form body");
        return -1;
    }
    p = src;
    for (;;) {
        const char *pair_start = p;
        const char *pair_end;
        const char *eq;
        const char *key_start;
        size_t key_len;
        size_t val_len;
        char *k;
        char *v;
        int at_end;
        /* Find end of pair */
        while (p < end && *p != '&') p++;
        pair_end = p;
        at_end = (p >= end);
        if (p < end) p++; /* skip '&' */
        if (pair_start < pair_end) {
            /* Split on first '=' */
            eq = pair_start;
            while (eq < pair_end && *eq != '=') eq++;
            key_start = pair_start;
            key_len = (size_t)(eq - key_start);
            val_len = eq < pair_end ? (size_t)(pair_end - (eq + 1)) : 0;
            k = ducknng_url_decode(key_start, key_len);
            v = ducknng_url_decode(eq < pair_end ? eq + 1 : pair_end, val_len);
            if (!k || !v) {
                if (k) duckdb_free(k);
                if (v) duckdb_free(v);
                /* free already allocated pairs */
                { idx_t i; for (i = 0; i < count; i++) { duckdb_free(keys[i]); duckdb_free(values[i]); } }
                duckdb_free(keys); duckdb_free(values);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory decoding form pair");
                return -1;
            }
            if (count >= capacity) {
                idx_t new_cap = capacity * 2;
                char **nk = (char **)duckdb_malloc(sizeof(char *) * new_cap);
                char **nv = (char **)duckdb_malloc(sizeof(char *) * new_cap);
                if (!nk || !nv) {
                    if (nk) duckdb_free(nk);
                    if (nv) duckdb_free(nv);
                    duckdb_free(k); duckdb_free(v);
                    { idx_t i; for (i = 0; i < count; i++) { duckdb_free(keys[i]); duckdb_free(values[i]); } }
                    duckdb_free(keys); duckdb_free(values);
                    if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory growing form pairs buffer");
                    return -1;
                }
                memcpy(nk, keys, sizeof(char *) * count);
                memcpy(nv, values, sizeof(char *) * count);
                duckdb_free(keys); duckdb_free(values);
                keys = nk; values = nv; capacity = new_cap;
            }
            keys[count] = k;
            values[count] = v;
            count++;
        }
        if (at_end) break;
    }
    *out_keys = keys;
    *out_values = values;
    *out_count = count;
    return 0;
}

/* ---------------------------------------------------------------------------
 * Direct C CSV / TSV parser producing ArrowSchema + ArrowArray via nanoarrow,
 * without temp files or DuckDB execution.
 * --------------------------------------------------------------------------- */

#define DUCKNNG_CSV_TYPE_INT64  0
#define DUCKNNG_CSV_TYPE_DOUBLE 1
#define DUCKNNG_CSV_TYPE_STRING 2
#define DUCKNNG_CSV_MAX_COLS    1024

/* Parse one delimited field from `*pp` up to `end`.
 * Advances *pp past the trailing delimiter (or newline / end of input).
 * Returns a duckdb_malloc'd NUL-terminated string for non-empty unquoted /
 * quoted fields, or NULL if the field is empty (caller treats as NULL). */
static char *ducknng_csv_next_field(const char **pp, const char *end, char delim) {
    const char *p = *pp;
    char *buf;
    char *q;
    char *result = NULL;

    if (p >= end) {
        *pp = end;
        return NULL;
    }
    if (*p == '"') {
        /* RFC 4180 quoted field */
        p++; /* skip opening quote */
        buf = (char *)duckdb_malloc((size_t)(end - p) + 1);
        if (!buf) { *pp = end; return NULL; }
        q = buf;
        while (p < end) {
            if (*p == '"') {
                p++;
                if (p < end && *p == '"') { *q++ = '"'; p++; continue; }
                break; /* closing quote */
            }
            *q++ = *p++;
        }
        *q = '\0';
        result = (q > buf) ? buf : (duckdb_free(buf), NULL);
        /* skip to delimiter / newline */
        while (p < end && *p != delim && *p != '\n' && *p != '\r') p++;
    } else {
        const char *start = p;
        while (p < end && *p != delim && *p != '\n' && *p != '\r') p++;
        if (p > start) {
            buf = (char *)duckdb_malloc((size_t)(p - start) + 1);
            if (buf) { memcpy(buf, start, (size_t)(p - start)); buf[p - start] = '\0'; result = buf; }
        }
    }
    /* consume delimiter */
    if (p < end && *p == delim) p++;
    *pp = p;
    return result;
}

/* Skip to the start of the next logical line (past \r\n or \n). */
static const char *ducknng_csv_skip_line(const char *p, const char *end) {
    while (p < end && *p != '\n' && *p != '\r') p++;
    if (p < end && *p == '\r') p++;
    if (p < end && *p == '\n') p++;
    return p;
}

/* Count the number of delimiter-separated fields in one line (for column count). */
static idx_t ducknng_csv_count_fields(const char *p, const char *end, char delim) {
    idx_t n = 0;
    const char *line_end = p;
    /* find line end first */
    while (line_end < end && *line_end != '\n' && *line_end != '\r') line_end++;
    if (p == line_end) return 0;
    while (p <= line_end) {
        n++;
        /* scan past this field */
        if (p < line_end && *p == '"') {
            p++;
            while (p < line_end) {
                if (*p == '"') { p++; if (p < line_end && *p == '"') { p++; continue; } break; }
                p++;
            }
            while (p < line_end && *p != delim) p++;
        } else {
            while (p < line_end && *p != delim) p++;
        }
        if (p >= line_end) break;
        p++; /* skip delimiter */
    }
    return n;
}

/* Try to parse `s` as int64.  Returns 1 and sets *v on success, 0 on failure. */
static int ducknng_csv_try_int64(const char *s, int64_t *v) {
    char *endp;
    int64_t val;
    if (!s || !s[0]) return 0;
    errno = 0;
    val = (int64_t)strtoll(s, &endp, 10);
    if (errno != 0 || endp == s || *endp != '\0') return 0;
    *v = val;
    return 1;
}

/* Try to parse `s` as double.  Returns 1 and sets *v on success, 0 on failure. */
static int ducknng_csv_try_double(const char *s, double *v) {
    char *endp;
    double val;
    if (!s || !s[0]) return 0;
    errno = 0;
    val = strtod(s, &endp);
    if (errno != 0 || endp == s || *endp != '\0') return 0;
    *v = val;
    return 1;
}

/* Build ArrowSchema + ArrowArray from in-memory CSV/TSV bytes.
 * `delimiter` is ',' for CSV or '\t' for TSV.
 * Populates *out_schema and *out_array on success; caller owns them and must
 * call ArrowSchemaRelease / ArrowArrayRelease.
 * Returns 0 on success, -1 on error (sets *errmsg). */
static int ducknng_csv_parse_body(const uint8_t *body, size_t body_len, char delimiter,
    idx_t max_cols,
    struct ArrowSchema *out_schema, struct ArrowArray *out_array, char **errmsg) {
    const char *src = (const char *)body;
    const char *end = src + body_len;
    const char *p = src;
    struct ArrowError aerr;
    idx_t col_count = 0;
    idx_t row_count = 0;
    idx_t row_capacity = 64;
    char **headers = NULL;
    /* cells: flat [row_capacity * col_count] char* (NULL means SQL NULL) */
    char **cells = NULL;
    int *col_types = NULL; /* DUCKNNG_CSV_TYPE_* per column */
    idx_t col, row;
    int rc = -1;

    memset(&aerr, 0, sizeof(aerr));
    if (errmsg) *errmsg = NULL;

    /* --- Parse header row --- */
    col_count = ducknng_csv_count_fields(p, end, delimiter);
    if (col_count == 0 || col_count > max_cols) {
        if (errmsg) *errmsg = ducknng_strdup(col_count == 0
            ? "ducknng: CSV body has no header row"
            : "ducknng: CSV body exceeds ducknng.csv_max_columns limit");
        return -1;
    }
    headers = (char **)duckdb_malloc(sizeof(char *) * col_count);
    if (!headers) { if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory for CSV headers"); return -1; }
    memset(headers, 0, sizeof(char *) * col_count);

    for (col = 0; col < col_count; col++) {
        char *h = ducknng_csv_next_field(&p, end, delimiter);
        headers[col] = h ? h : ducknng_strdup(""); /* empty header becomes "" */
        if (!headers[col]) { if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory for CSV header name"); goto cleanup; }
    }
    /* skip rest of header line (trailing delimiter already consumed; skip \r\n) */
    if (p < end && *p == '\r') p++;
    if (p < end && *p == '\n') p++;

    /* --- Allocate initial cell table --- */
    cells = (char **)duckdb_malloc(sizeof(char *) * row_capacity * col_count);
    if (!cells) { if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory for CSV cells"); goto cleanup; }
    memset(cells, 0, sizeof(char *) * row_capacity * col_count);

    /* --- Parse data rows --- */
    while (p < end) {
        /* skip completely blank lines */
        if (*p == '\r' || *p == '\n') {
            if (*p == '\r') p++;
            if (p < end && *p == '\n') p++;
            continue;
        }
        /* grow cell table if needed */
        if (row_count >= row_capacity) {
            idx_t new_cap = row_capacity * 2;
            char **new_cells = (char **)duckdb_malloc(sizeof(char *) * new_cap * col_count);
            if (!new_cells) { if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory growing CSV table"); goto cleanup; }
            memcpy(new_cells, cells, sizeof(char *) * row_count * col_count);
            memset(new_cells + row_count * col_count, 0, sizeof(char *) * (new_cap - row_capacity) * col_count);
            duckdb_free(cells);
            cells = new_cells;
            row_capacity = new_cap;
        }
        {
            char **row_cells = cells + row_count * col_count;
            const char *line_end = p;
            while (line_end < end && *line_end != '\n' && *line_end != '\r') line_end++;
            for (col = 0; col < col_count; col++) {
                if (p > line_end) {
                    row_cells[col] = NULL; /* missing trailing columns = NULL */
                } else {
                    row_cells[col] = ducknng_csv_next_field(&p, line_end, delimiter);
                }
            }
            /* skip to next line */
            p = ducknng_csv_skip_line(line_end, end);
            row_count++;
        }
    }

    /* --- Type inference per column --- */
    col_types = (int *)duckdb_malloc(sizeof(int) * col_count);
    if (!col_types) { if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory for CSV type inference"); goto cleanup; }
    for (col = 0; col < col_count; col++) {
        int inferred = DUCKNNG_CSV_TYPE_INT64;
        for (row = 0; row < row_count; row++) {
            const char *v = cells[row * col_count + col];
            if (!v) continue; /* NULL stays compatible with any type */
            if (inferred == DUCKNNG_CSV_TYPE_INT64) {
                int64_t dummy;
                if (!ducknng_csv_try_int64(v, &dummy)) inferred = DUCKNNG_CSV_TYPE_DOUBLE;
            }
            if (inferred == DUCKNNG_CSV_TYPE_DOUBLE) {
                double dummy;
                if (!ducknng_csv_try_double(v, &dummy)) { inferred = DUCKNNG_CSV_TYPE_STRING; break; }
            }
        }
        col_types[col] = inferred;
    }

    /* --- Build ArrowSchema --- */
    ArrowSchemaInit(out_schema);
    if (ArrowSchemaSetTypeStruct(out_schema, (int64_t)col_count) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to init CSV Arrow schema struct");
        goto cleanup;
    }
    for (col = 0; col < col_count; col++) {
        struct ArrowSchema *child = out_schema->children[col];
        enum ArrowType atype = (col_types[col] == DUCKNNG_CSV_TYPE_INT64)  ? NANOARROW_TYPE_INT64
                             : (col_types[col] == DUCKNNG_CSV_TYPE_DOUBLE) ? NANOARROW_TYPE_DOUBLE
                             :                                                NANOARROW_TYPE_STRING;
        if (ArrowSchemaSetName(child, headers[col]) != NANOARROW_OK ||
            ArrowSchemaSetType(child, atype) != NANOARROW_OK) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to set CSV Arrow column schema");
            goto cleanup;
        }
    }

    /* --- Build ArrowArray --- */
    if (ArrowArrayInitFromSchema(out_array, out_schema, &aerr) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(aerr.message[0] ? aerr.message : "ducknng: failed to init CSV Arrow array");
        goto cleanup;
    }
    if (ArrowArrayStartAppending(out_array) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to start CSV Arrow array appending");
        goto cleanup;
    }
    for (row = 0; row < row_count; row++) {
        for (col = 0; col < col_count; col++) {
            const char *v = cells[row * col_count + col];
            struct ArrowArray *child = out_array->children[col];
            if (!v) {
                if (ArrowArrayAppendNull(child, 1) != NANOARROW_OK) {
                    if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to append NULL to CSV Arrow array");
                    ArrowArrayRelease(out_array);
                    goto cleanup;
                }
            } else if (col_types[col] == DUCKNNG_CSV_TYPE_INT64) {
                int64_t iv = 0;
                ducknng_csv_try_int64(v, &iv);
                if (ArrowArrayAppendInt(child, iv) != NANOARROW_OK) {
                    if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to append int to CSV Arrow array");
                    ArrowArrayRelease(out_array);
                    goto cleanup;
                }
            } else if (col_types[col] == DUCKNNG_CSV_TYPE_DOUBLE) {
                double dv = 0.0;
                ducknng_csv_try_double(v, &dv);
                if (ArrowArrayAppendDouble(child, dv) != NANOARROW_OK) {
                    if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to append double to CSV Arrow array");
                    ArrowArrayRelease(out_array);
                    goto cleanup;
                }
            } else {
                struct ArrowStringView sv;
                sv.data = v;
                sv.size_bytes = (int64_t)strlen(v);
                if (ArrowArrayAppendString(child, sv) != NANOARROW_OK) {
                    if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to append string to CSV Arrow array");
                    ArrowArrayRelease(out_array);
                    goto cleanup;
                }
            }
        }
    }
    out_array->length = (int64_t)row_count;
    if (ArrowArrayFinishBuildingDefault(out_array, &aerr) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(aerr.message[0] ? aerr.message : "ducknng: failed to finish CSV Arrow array");
        ArrowArrayRelease(out_array);
        goto cleanup;
    }
    rc = 0;

cleanup:
    if (col_types) duckdb_free(col_types);
    if (headers) {
        for (col = 0; col < col_count; col++) if (headers[col]) duckdb_free(headers[col]);
        duckdb_free(headers);
    }
    if (cells) {
        idx_t total = row_count * col_count;
        idx_t i;
        for (i = 0; i < total; i++) if (cells[i]) duckdb_free(cells[i]);
        duckdb_free(cells);
    }
    if (rc != 0) {
        if (out_schema->release) ArrowSchemaRelease(out_schema);
    }
    return rc;
}

/* ---------------------------------------------------------------------------
 * Cross-platform tempfile helpers for Parquet body reader
 * --------------------------------------------------------------------------- */
/* Generate a temporary file path and set *out_path.
 * Returns 0 on success, -1 on failure.
 * The name combines the process id and a process-wide atomic counter, so it is
 * unique among concurrent callers in this process and across processes. It does
 * not create the file: the caller creates it exclusively through the DuckDB file
 * system (WRITE + CREATE + CREATE_NEW), which fails if the path already exists,
 * so a planted or stale file is never written, read, or removed.
 * On POSIX the path lives under /tmp; on Windows under the system temp dir. */
static atomic_uint_fast64_t ducknng_body_tmp_seq = 0;

static int ducknng_body_make_tempfile_path(char **out_path, char **errmsg) {
    char buf[96];
    uint64_t seq = (uint64_t)atomic_fetch_add_explicit(
        &ducknng_body_tmp_seq, 1, memory_order_relaxed);
#ifdef _WIN32
    uint64_t pid = (uint64_t)GetCurrentProcessId();
#else
    uint64_t pid = (uint64_t)getpid();
#endif
#ifdef _WIN32
    char tmp_dir[MAX_PATH];
    DWORD len = GetTempPathA(MAX_PATH, tmp_dir);
    if (len == 0 || len >= MAX_PATH) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: GetTempPathA failed");
        return -1;
    }
    snprintf(buf, sizeof(buf), "ducknng_body_%" PRIu64 "_%016" PRIx64 ".tmp", pid, seq);
    {
        size_t dir_len = strlen(tmp_dir);
        char *path = (char *)duckdb_malloc(dir_len + strlen(buf) + 1);
        if (!path) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory for temp path");
            return -1;
        }
        memcpy(path, tmp_dir, dir_len);
        memcpy(path + dir_len, buf, strlen(buf) + 1);
        *out_path = path;
    }
#else
    snprintf(buf, sizeof(buf), "/tmp/ducknng_body_%" PRIu64 "_%016" PRIx64 ".tmp", pid, seq);
    *out_path = ducknng_strdup(buf);
    if (!*out_path) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory for temp path");
        return -1;
    }
#endif
    return 0;
}

static void ducknng_body_delete_tempfile(const char *path) {
    if (!path) return;
#ifdef _WIN32
    DeleteFileA(path);
#else
    unlink(path);
#endif
}

/* ---------------------------------------------------------------------------
 * Temp-file based DuckDB reader for Parquet body codec
 * --------------------------------------------------------------------------- */
static int ducknng_body_parse_run_tempfile_reader(ducknng_sql_context *ctx,
    duckdb_client_context parent, ducknng_body_parse_bind_data *bind,
    const uint8_t *body, size_t body_len, int codec_kind, duckdb_file_system fs,
    char **errmsg) {
    ducknng_codec_scope codec_scope;
    char *path = NULL;
    char *quoted_path = NULL;
    char *sql = NULL;
    uint8_t *payload = NULL;
    size_t payload_len = 0;
    size_t sql_len;
    duckdb_file_handle fh = NULL;
    duckdb_file_open_options opts = NULL;
    int rc = -1;
    int owned = 0;
    const char *query_template = NULL;
    if (errmsg) *errmsg = NULL;
    if (!ctx || !ctx->rt || !ducknng_runtime_codec_connection(ctx->rt) || !bind || !fs) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing runtime for body reader");
        return -1;
    }
    switch (codec_kind) {
        case DUCKNNG_BODY_CODEC_PARQUET: query_template = "SELECT * FROM read_parquet(%s)"; break;
        case DUCKNNG_BODY_CODEC_CSV: query_template = "SELECT * FROM read_csv_auto(%s)"; break;
        case DUCKNNG_BODY_CODEC_TSV: query_template = "SELECT * FROM read_csv_auto(%s, delim='\t')"; break;
        default:
            if (errmsg) *errmsg = ducknng_strdup("ducknng: unsupported codec for tempfile reader");
            return -1;
    }
    if (!body || body_len == 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: body must not be empty for this codec");
        return -1;
    }
    if ((uint64_t)body_len > (uint64_t)INT64_MAX) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: body too large for temp file");
        return -1;
    }
    if (ducknng_body_make_tempfile_path(&path, errmsg) != 0) goto cleanup;
    opts = duckdb_create_file_open_options();
    if (!opts) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory creating file open options");
        goto cleanup;
    }
    duckdb_file_open_options_set_flag(opts, DUCKDB_FILE_FLAG_WRITE, true);
    duckdb_file_open_options_set_flag(opts, DUCKDB_FILE_FLAG_CREATE, true);
    duckdb_file_open_options_set_flag(opts, DUCKDB_FILE_FLAG_CREATE_NEW, true);
    if (duckdb_file_system_open(fs, path, opts, &fh) == DuckDBError || !fh) {
        if (fh) duckdb_destroy_file_handle(&fh);
        fh = NULL;
        if (errmsg && !*errmsg) {
            duckdb_error_data err = duckdb_file_system_error_data(fs);
            if (err) {
                const char *msg = duckdb_error_data_message(err);
                if (msg && msg[0]) *errmsg = ducknng_strdup(msg);
                duckdb_destroy_error_data(&err);
            }
            if (!*errmsg) *errmsg = ducknng_strdup("ducknng: failed to open temp file via DuckDB FS");
        }
        goto cleanup;
    }
    /* This call created the path exclusively, so this call removes it. */
    owned = 1;
    duckdb_destroy_file_open_options(&opts);
    opts = NULL;
    if (duckdb_file_handle_write(fh, (void *)body, (int64_t)body_len) != (int64_t)body_len) {
        if (errmsg && !*errmsg) {
            duckdb_error_data err = duckdb_file_handle_error_data(fh);
            if (err) {
                const char *msg = duckdb_error_data_message(err);
                if (msg && msg[0]) *errmsg = ducknng_strdup(msg);
                duckdb_destroy_error_data(&err);
            }
            if (!*errmsg) *errmsg = ducknng_strdup("ducknng: write to temp file failed");
        }
        duckdb_destroy_file_handle(&fh);
        fh = NULL;
        goto cleanup;
    }
    duckdb_destroy_file_handle(&fh);
    fh = NULL;
    /* Build SQL — path was generated internally so no user-controlled content.
     * Still use sql_quote_literal for correctness with any OS path chars. */
    quoted_path = ducknng_sql_quote_literal(path);
    if (!quoted_path) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory quoting temp path");
        goto cleanup;
    }
    sql_len = snprintf(NULL, 0, query_template, quoted_path) + 1;
    sql = (char *)duckdb_malloc(sql_len);
    if (!sql) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory building reader query");
        goto cleanup;
    }
    snprintf(sql, sql_len, query_template, quoted_path);
    if (ducknng_body_codec_enter(ctx, parent, &codec_scope, errmsg) != 0) goto cleanup;
    if (ducknng_query_to_ipc_stream(ducknng_runtime_codec_connection(ctx->rt),
            sql, &payload, &payload_len, errmsg) != 0) {
        ducknng_body_codec_leave(ctx, &codec_scope);
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: body reader query failed");
        goto cleanup;
    }
    ducknng_body_codec_leave(ctx, &codec_scope);
    if (ducknng_decode_ipc_table_payload(payload, payload_len, &bind->schema, &bind->array, errmsg) != 0) goto cleanup;
    bind->result_kind = DUCKNNG_BODY_RESULT_ARROW;
    bind->row_count = (idx_t)bind->array.length;
    rc = 0;
cleanup:
    if (fh) duckdb_destroy_file_handle(&fh);
    if (opts) duckdb_destroy_file_open_options(&opts);
    if (payload) duckdb_free(payload);
    if (sql) duckdb_free(sql);
    if (quoted_path) duckdb_free(quoted_path);
    if (path) {
        if (owned) ducknng_body_delete_tempfile(path);
        duckdb_free(path);
    }
    return rc;
}

/* Decode one body into an owned parse result. client_ctx is borrowed for the
 * duration of this call only and is never stored in the result. On failure
 * *out_bind is NULL and *errmsg holds an owned message when allocation allowed
 * one. No bind_info is touched: column declaration is a separate step. */
/* Select the codec for a normalized media type. User-registered codecs take
 * precedence over built-in providers so a deployment can override
 * application/json or text wildcard media types with its own scalar. On
 * DUCKNNG_BODY_CODEC_USER, *user_fn is an owned copy of the function name. */
static int ducknng_body_resolve_codec(ducknng_sql_context *ctx, const char *media_type,
    char **user_fn) {
    *user_fn = ctx && ctx->rt ? ducknng_runtime_find_user_codec(ctx->rt, media_type) : NULL;
    if (*user_fn) return DUCKNNG_BODY_CODEC_USER;
    return ducknng_body_codec_for_media_type(media_type);
}

static int ducknng_body_parse_build_codec(ducknng_sql_context *ctx,
    duckdb_client_context client_ctx, const uint8_t *body, size_t body_len,
    const char *media_type, int codec_kind, const char *user_fn,
    ducknng_body_parse_bind_data **out_bind, char **errmsg);

static int ducknng_body_parse_build(ducknng_sql_context *ctx,
    duckdb_client_context client_ctx, const uint8_t *body, size_t body_len,
    const char *content_type, ducknng_body_parse_bind_data **out_bind,
    char **errmsg) {
    char *media_type;
    char *user_fn = NULL;
    int codec_kind, rc;
    *out_bind = NULL;
    *errmsg = NULL;
    media_type = ducknng_normalize_media_type(content_type);
    if (!media_type) {
        *errmsg = ducknng_strdup("ducknng: out of memory normalizing content type");
        return -1;
    }
    codec_kind = ducknng_body_resolve_codec(ctx, media_type, &user_fn);
    rc = ducknng_body_parse_build_codec(ctx, client_ctx, body, body_len, media_type,
        codec_kind, user_fn, out_bind, errmsg);
    if (user_fn) duckdb_free(user_fn);
    duckdb_free(media_type);
    return rc;
}

/* Decode one body with an already selected codec. media_type and user_fn are
 * borrowed and copied into the result. */
static int ducknng_body_parse_build_codec(ducknng_sql_context *ctx,
    duckdb_client_context client_ctx, const uint8_t *body, size_t body_len,
    const char *media_type, int codec_kind, const char *user_fn,
    ducknng_body_parse_bind_data **out_bind, char **errmsg) {
    ducknng_body_parse_bind_data *bind;
    const char *fail_msg = "ducknng: out of memory";
    *out_bind = NULL;
    *errmsg = NULL;
    bind = (ducknng_body_parse_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) goto fail;
    memset(bind, 0, sizeof(*bind));
    bind->codec_kind = codec_kind;
    bind->media_type = ducknng_strdup(media_type ? media_type : "");
    if (!bind->media_type) goto fail;
    if (codec_kind == DUCKNNG_BODY_CODEC_USER) {
        bind->user_function_name = user_fn ? ducknng_strdup(user_fn) : NULL;
        if (!bind->user_function_name) {
            fail_msg = "ducknng: missing user codec function name";
            goto fail;
        }
    }
    bind->provider = ducknng_strdup(ducknng_body_codec_provider_name(bind->codec_kind));
    if (!bind->provider) {
        fail_msg = "ducknng: out of memory copying body codec name";
        goto fail;
    }
    switch (bind->codec_kind) {
        case DUCKNNG_BODY_CODEC_USER:
            if (ducknng_body_parse_run_user_codec(ctx, client_ctx, bind, body, body_len,
                    bind->user_function_name, errmsg) != 0) {
                fail_msg = "ducknng: user codec failed";
                goto fail;
            }
            bind->result_kind = DUCKNNG_BODY_RESULT_SINGLE;
            break;
        case DUCKNNG_BODY_CODEC_TEXT:
            if (!ducknng_sql_bytes_look_text(body, body_len)) {
                fail_msg = "ducknng: text body is not valid UTF-8 text";
                goto fail;
            }
            bind->text = ducknng_dup_bytes(body, body_len);
            if (!bind->text) {
                fail_msg = "ducknng: out of memory copying text body";
                goto fail;
            }
            bind->result_kind = DUCKNNG_BODY_RESULT_SINGLE;
            break;
        case DUCKNNG_BODY_CODEC_DUCKNNG_FRAME:
            if (ducknng_body_parse_frame_copy(body, body_len, &bind->frame) != 0) {
                fail_msg = "ducknng: failed to decode frame body";
                goto fail;
            }
            bind->result_kind = DUCKNNG_BODY_RESULT_FRAME;
            break;
        case DUCKNNG_BODY_CODEC_ARROW_IPC:
            if (ducknng_decode_ipc_table_payload(body, body_len, &bind->schema,
                    &bind->array, errmsg) != 0) {
                fail_msg = "ducknng: failed to decode Arrow IPC body";
                goto fail;
            }
            bind->result_kind = DUCKNNG_BODY_RESULT_ARROW;
            bind->row_count = (idx_t)bind->array.length;
            if (bind->schema.n_children < 0 || bind->schema.n_children != bind->array.n_children) {
                fail_msg = "ducknng: invalid Arrow IPC body schema";
                goto fail;
            }
            break;
        case DUCKNNG_BODY_CODEC_QUACK_BATCH:
            if (!body || body_len == 0) {
                fail_msg = "ducknng: ducknng_quack_batch body must not be empty";
                goto fail;
            }
            bind->raw = (uint8_t *)duckdb_malloc(body_len);
            if (!bind->raw) {
                fail_msg = "ducknng: out of memory copying ducknng_quack_batch body";
                goto fail;
            }
            memcpy(bind->raw, body, body_len);
            bind->raw_len = (idx_t)body_len;
            if (ducknng_quack_payload_parse_table(bind->raw, body_len,
                    &bind->quack_schema, &bind->row_count, errmsg) != 0) {
                fail_msg = "ducknng: failed to decode ducknng_quack_batch body";
                goto fail;
            }
            bind->result_kind = DUCKNNG_BODY_RESULT_QUACK_BATCH;
            break;
        case DUCKNNG_BODY_CODEC_JSON:
        case DUCKNNG_BODY_CODEC_NDJSON: {
            const uint8_t *json_body = body;
            size_t json_len = body_len;
            char *ndjson_arr = NULL;
            int rc;
            if (bind->codec_kind == DUCKNNG_BODY_CODEC_NDJSON) {
                ndjson_arr = ducknng_ndjson_to_json_array(body, body_len, errmsg);
                if (!ndjson_arr) {
                    fail_msg = "ducknng: failed to convert NDJSON to JSON array";
                    goto fail;
                }
                json_body = (const uint8_t *)ndjson_arr;
                json_len = strlen(ndjson_arr);
            }
            rc = ducknng_body_parse_run_duckdb_reader(ctx, client_ctx, bind, json_body, json_len,
                DUCKNNG_BODY_CODEC_JSON, errmsg);
            if (ndjson_arr) duckdb_free(ndjson_arr);
            if (rc != 0) {
                fail_msg = "ducknng: failed to parse body with DuckDB reader";
                goto fail;
            }
            if (bind->schema.n_children < 0 || bind->schema.n_children != bind->array.n_children) {
                fail_msg = "ducknng: invalid parsed body schema";
                goto fail;
            }
            break;
        }
        case DUCKNNG_BODY_CODEC_CSV:
        case DUCKNNG_BODY_CODEC_TSV: {
            char csv_delim = (bind->codec_kind == DUCKNNG_BODY_CODEC_TSV) ? '\t' : ',';
            idx_t csv_max_cols;
            if (!body || body_len == 0) {
                fail_msg = "ducknng: CSV/TSV body must not be empty";
                goto fail;
            }
            csv_max_cols = (idx_t)ducknng_sql_get_config_ubigint(
                client_ctx, "ducknng.csv_max_columns", DUCKNNG_CSV_MAX_COLS);
            if (ducknng_csv_parse_body(body, body_len, csv_delim, csv_max_cols,
                    &bind->schema, &bind->array, errmsg) != 0) {
                fail_msg = "ducknng: failed to parse CSV/TSV body";
                goto fail;
            }
            bind->result_kind = DUCKNNG_BODY_RESULT_ARROW;
            bind->row_count = (idx_t)bind->array.length;
            if (bind->schema.n_children < 0 || bind->schema.n_children != bind->array.n_children) {
                fail_msg = "ducknng: invalid CSV/TSV body schema";
                goto fail;
            }
            break;
        }
        case DUCKNNG_BODY_CODEC_PARQUET: {
            duckdb_file_system fs = NULL;
            int rc;
            if (client_ctx) fs = duckdb_client_context_get_file_system(client_ctx);
            rc = ducknng_body_parse_run_tempfile_reader(ctx, client_ctx, bind, body, body_len,
                bind->codec_kind, fs, errmsg);
            if (fs) duckdb_destroy_file_system(&fs);
            if (rc != 0) {
                fail_msg = "ducknng: failed to parse body with DuckDB file reader";
                goto fail;
            }
            if (bind->schema.n_children < 0 || bind->schema.n_children != bind->array.n_children) {
                fail_msg = "ducknng: invalid parsed body schema";
                goto fail;
            }
            break;
        }
        case DUCKNNG_BODY_CODEC_FORM:
            if (ducknng_parse_form_urlencoded(body, body_len, &bind->form_keys,
                    &bind->form_values, &bind->form_count, errmsg) != 0) {
                fail_msg = "ducknng: failed to parse form body";
                goto fail;
            }
            bind->result_kind = DUCKNNG_BODY_RESULT_FORM;
            break;
        default:
            bind->raw_len = (idx_t)body_len;
            if (body_len > 0) {
                bind->raw = (uint8_t *)duckdb_malloc(body_len);
                if (!bind->raw) {
                    fail_msg = "ducknng: out of memory copying raw body";
                    goto fail;
                }
                memcpy(bind->raw, body, body_len);
            }
            bind->result_kind = DUCKNNG_BODY_RESULT_SINGLE;
            break;
    }
    *out_bind = bind;
    return 0;
fail:
    if (bind) destroy_body_parse_bind_data(bind);
    if (!*errmsg) *errmsg = ducknng_strdup(fail_msg);
    return -1;
}

/* Column layout of a SINGLE, FRAME, or FORM result. Returns the column count,
 * or 0 for result kinds whose columns come from a decoded schema. */
static idx_t ducknng_body_parse_fixed_layout(const ducknng_body_parse_bind_data *bind,
    const char *const **names, const duckdb_type **types) {
    switch (bind->result_kind) {
        case DUCKNNG_BODY_RESULT_SINGLE:
            if (bind->codec_kind == DUCKNNG_BODY_CODEC_USER) {
                *names = ducknng_body_value_names;
                *types = ducknng_body_varchar_types;
            } else if (bind->codec_kind == DUCKNNG_BODY_CODEC_TEXT) {
                *names = ducknng_body_text_names;
                *types = ducknng_body_varchar_types;
            } else {
                *names = ducknng_body_raw_names;
                *types = ducknng_body_blob_types;
            }
            return 1;
        case DUCKNNG_BODY_RESULT_FRAME:
            *names = ducknng_body_frame_names;
            *types = ducknng_body_frame_types;
            return DUCKNNG_BODY_FRAME_NCOLS;
        case DUCKNNG_BODY_RESULT_FORM:
            *names = ducknng_body_form_names;
            *types = ducknng_body_varchar_types;
            return 2;
        default:
            return 0;
    }
}

/* Require a completed parse result to match a declared column list exactly:
 * same count, same names in order, and deep-equal DuckDB types. Runs before
 * scan writes any output vector. */
static int ducknng_body_parse_validate_declared(const ducknng_body_parse_bind_data *bind,
    const ducknng_sql_declared_schema *declared, char **errmsg) {
    const char *const *dnames = (const char *const *)declared->names;
    const char *const *names = NULL;
    const duckdb_type *types = NULL;
    idx_t ncols, i;
    char msg[256];
    *errmsg = NULL;
    if (bind->result_kind == DUCKNNG_BODY_RESULT_ARROW)
        return ducknng_sql_arrow_schema_validate_declared(&bind->schema,
            declared->ncols, dnames, declared->types, errmsg);
    if (bind->result_kind == DUCKNNG_BODY_RESULT_QUACK_BATCH)
        return ducknng_quack_schema_validate_declared(&bind->quack_schema,
            declared->ncols, dnames, declared->types, errmsg);
    ncols = ducknng_body_parse_fixed_layout(bind, &names, &types);
    if (ncols == 0) {
        *errmsg = ducknng_strdup("ducknng: invalid body parse result kind");
        return -1;
    }
    if (declared->ncols != ncols) {
        snprintf(msg, sizeof(msg),
            "ducknng: declared schema has %llu columns but the %s body has %llu",
            (unsigned long long)declared->ncols, bind->provider ? bind->provider : "decoded",
            (unsigned long long)ncols);
        *errmsg = ducknng_strdup(msg);
        return -1;
    }
    for (i = 0; i < ncols; i++) {
        duckdb_logical_type t;
        int eq;
        if (!dnames[i] || strcmp(dnames[i], names[i]) != 0) {
            snprintf(msg, sizeof(msg),
                "ducknng: declared column %llu is named '%s' but the body column is '%s'",
                (unsigned long long)(i + 1), dnames[i] ? dnames[i] : "", names[i]);
            *errmsg = ducknng_strdup(msg);
            return -1;
        }
        t = duckdb_create_logical_type(types[i]);
        if (!t) {
            *errmsg = ducknng_strdup("ducknng: out of memory validating body schema");
            return -1;
        }
        eq = ducknng_quack_logical_type_equal(t, declared->types[i]);
        duckdb_destroy_logical_type(&t);
        if (!eq) {
            snprintf(msg, sizeof(msg),
                "ducknng: declared type of column '%s' does not match the body column type",
                names[i]);
            *errmsg = ducknng_strdup(msg);
            return -1;
        }
    }
    return 0;
}

/* Declare result columns and cardinality from a completed parse result. */
static int ducknng_body_parse_bind_result(duckdb_bind_info info,
    const ducknng_body_parse_bind_data *bind, char **errmsg) {
    const char *fail_msg = "ducknng: out of memory declaring body columns";
    idx_t cardinality = 1;
    *errmsg = NULL;
    switch (bind->result_kind) {
        case DUCKNNG_BODY_RESULT_SINGLE:
        case DUCKNNG_BODY_RESULT_FRAME:
        case DUCKNNG_BODY_RESULT_FORM: {
            const char *const *names = NULL;
            const duckdb_type *types = NULL;
            idx_t ncols = ducknng_body_parse_fixed_layout(bind, &names, &types);
            if (ducknng_body_add_fixed_columns(info, ncols, names, types) != 0) goto fail;
            if (bind->result_kind == DUCKNNG_BODY_RESULT_FORM) cardinality = bind->form_count;
            break;
        }
        case DUCKNNG_BODY_RESULT_ARROW:
            if (ducknng_sql_arrow_bind_result_columns(info, &bind->schema, errmsg) != 0) {
                if (bind->codec_kind == DUCKNNG_BODY_CODEC_ARROW_IPC)
                    fail_msg = "ducknng: unsupported Arrow IPC body type";
                else if (bind->codec_kind == DUCKNNG_BODY_CODEC_CSV ||
                    bind->codec_kind == DUCKNNG_BODY_CODEC_TSV)
                    fail_msg = "ducknng: unsupported CSV/TSV body column type";
                else
                    fail_msg = "ducknng: unsupported parsed body type";
                goto fail;
            }
            cardinality = bind->row_count;
            break;
        case DUCKNNG_BODY_RESULT_QUACK_BATCH:
            if (ducknng_quack_schema_bind_columns(info, &bind->quack_schema, errmsg) != 0) {
                fail_msg = "ducknng: failed to decode ducknng_quack_batch body";
                goto fail;
            }
            cardinality = bind->row_count;
            break;
        default:
            fail_msg = "ducknng: invalid body parse result kind";
            goto fail;
    }
    duckdb_bind_set_cardinality(info, cardinality, true);
    return 0;
fail:
    if (!*errmsg) *errmsg = ducknng_strdup(fail_msg);
    return -1;
}

/* Codecs whose decoding runs DuckDB SQL or a DuckDB file reader. They never
 * run at bind; JSON, NDJSON, and Parquet also need a declared schema because
 * their columns are only known after decoding. */
static int ducknng_body_codec_is_deferred(int codec_kind) {
    return codec_kind == DUCKNNG_BODY_CODEC_USER || codec_kind == DUCKNNG_BODY_CODEC_JSON ||
        codec_kind == DUCKNNG_BODY_CODEC_NDJSON || codec_kind == DUCKNNG_BODY_CODEC_PARQUET;
}

/* Read the named schema into plan->schema and declare its columns. When the
 * argument is absent and not required, nothing is declared. Sets the bind error
 * and returns -1 on failure. */
static int ducknng_body_plan_bind_schema(duckdb_bind_info info,
    ducknng_body_plan_bind_data *plan, int required) {
    duckdb_value value = duckdb_bind_get_named_parameter(info, "schema");
    if (!value && !required) return 0;
    if (value) duckdb_destroy_value(&value);
    if (ducknng_sql_bind_declared_schema(info, &plan->schema) != 0) return -1;
    plan->has_schema = 1;
    return 0;
}

static ducknng_body_plan_bind_data *ducknng_body_plan_new(duckdb_bind_info info) {
    ducknng_body_plan_bind_data *plan =
        (ducknng_body_plan_bind_data *)duckdb_malloc(sizeof(*plan));
    if (!plan) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return NULL;
    }
    memset(plan, 0, sizeof(*plan));
    return plan;
}

/* Take ownership of the BLOB parameter at index idx. */
static void ducknng_body_plan_take_body(duckdb_bind_info info, idx_t idx,
    ducknng_body_plan_bind_data *plan) {
    duckdb_value body_val = duckdb_bind_get_parameter(info, idx);
    if (!duckdb_is_null_value(body_val)) {
        duckdb_blob blob = duckdb_get_blob(body_val);
        plan->body = (uint8_t *)blob.data;
        plan->body_len = (size_t)blob.size;
    }
    duckdb_destroy_value(&body_val);
}

static void ducknng_body_parse_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    ducknng_body_plan_bind_data *plan;
    duckdb_value content_type_val;
    char *content_type;
    char *errmsg = NULL;
    plan = ducknng_body_plan_new(info);
    if (!plan) return;
    ducknng_body_plan_take_body(info, 0, plan);
    content_type_val = duckdb_bind_get_parameter(info, 1);
    content_type = duckdb_is_null_value(content_type_val) ? NULL : duckdb_get_varchar(content_type_val);
    duckdb_destroy_value(&content_type_val);
    plan->media_type = ducknng_normalize_media_type(content_type);
    if (content_type) duckdb_free(content_type);
    if (!plan->media_type) {
        duckdb_bind_set_error(info, "ducknng: out of memory normalizing content type");
        goto fail;
    }
    plan->codec_kind = ducknng_body_resolve_codec(ctx, plan->media_type, &plan->user_function_name);
    duckdb_table_function_get_client_context(info, &plan->client_ctx);
    if (plan->codec_kind == DUCKNNG_BODY_CODEC_USER) {
        if (ducknng_body_plan_bind_schema(info, plan, 0) != 0) goto fail;
        if (!plan->has_schema && ducknng_body_add_fixed_columns(info, 1,
                ducknng_body_value_names, ducknng_body_varchar_types) != 0) {
            duckdb_bind_set_error(info, "ducknng: out of memory declaring body columns");
            goto fail;
        }
    } else if (ducknng_body_codec_is_deferred(plan->codec_kind)) {
        if (ducknng_body_plan_bind_schema(info, plan, 1) != 0) goto fail;
    } else {
        if (ducknng_body_parse_build_codec(ctx, plan->client_ctx, plan->body, plan->body_len,
                plan->media_type, plan->codec_kind, NULL, &plan->parsed, &errmsg) != 0) goto fail_msg;
        if (ducknng_body_plan_bind_schema(info, plan, 0) != 0) goto fail;
        if (plan->has_schema) {
            if (ducknng_body_parse_validate_declared(plan->parsed, &plan->schema, &errmsg) != 0)
                goto fail_msg;
        } else if (ducknng_body_parse_bind_result(info, plan->parsed, &errmsg) != 0) {
            goto fail_msg;
        }
        /* The parsed result is immutable; the copied inputs are not needed. */
        duckdb_free(plan->body);
        plan->body = NULL;
        plan->body_len = 0;
        duckdb_destroy_client_context(&plan->client_ctx);
    }
    duckdb_bind_set_bind_data(info, plan, destroy_body_plan_bind_data);
    return;
fail_msg:
    duckdb_bind_set_error(info, errmsg ? errmsg : "ducknng: failed to parse body");
    if (errmsg) duckdb_free(errmsg);
fail:
    destroy_body_plan_bind_data(plan);
}

static void ducknng_body_reader_bind(duckdb_bind_info info, int codec_kind) {
    ducknng_body_plan_bind_data *plan = ducknng_body_plan_new(info);
    if (!plan) return;
    plan->reader = 1;
    plan->codec_kind = codec_kind;
    ducknng_body_plan_take_body(info, 0, plan);
    if (!plan->body || plan->body_len == 0) {
        duckdb_bind_set_error(info, "ducknng: body must not be null or empty");
        goto fail;
    }
    if (ducknng_body_plan_bind_schema(info, plan, 1) != 0) goto fail;
    duckdb_table_function_get_client_context(info, &plan->client_ctx);
    duckdb_bind_set_bind_data(info, plan, destroy_body_plan_bind_data);
    return;
fail:
    destroy_body_plan_bind_data(plan);
}

/* Decode a deferred plan once for this execution. Returns an owned result
 * validated against the declared schema, or NULL with *errmsg set. */
static ducknng_body_parse_bind_data *ducknng_body_plan_execute(ducknng_sql_context *ctx,
    const ducknng_body_plan_bind_data *plan, char **errmsg) {
    ducknng_body_parse_bind_data *parsed = NULL;
    int rc;
    *errmsg = NULL;
    if (plan->reader) {
        duckdb_file_system fs = NULL;
        parsed = (ducknng_body_parse_bind_data *)duckdb_malloc(sizeof(*parsed));
        if (!parsed) {
            *errmsg = ducknng_strdup("ducknng: out of memory");
            return NULL;
        }
        memset(parsed, 0, sizeof(*parsed));
        parsed->codec_kind = plan->codec_kind;
        if (plan->client_ctx) fs = duckdb_client_context_get_file_system(plan->client_ctx);
        rc = ducknng_body_parse_run_tempfile_reader(ctx, plan->client_ctx, parsed,
            plan->body, plan->body_len,
            plan->codec_kind, fs, errmsg);
        if (fs) duckdb_destroy_file_system(&fs);
    } else {
        rc = ducknng_body_parse_build_codec(ctx, plan->client_ctx, plan->body, plan->body_len,
            plan->media_type, plan->codec_kind, plan->user_function_name, &parsed, errmsg);
    }
    if (rc == 0 && plan->has_schema)
        rc = ducknng_body_parse_validate_declared(parsed, &plan->schema, errmsg);
    if (rc != 0) {
        if (parsed) destroy_body_parse_bind_data(parsed);
        if (!*errmsg) *errmsg = ducknng_strdup("ducknng: failed to parse body");
        return NULL;
    }
    return parsed;
}

/* Bind copies the request arguments and the declared schema only. The request,
 * TLS and profile resolution, and decoding all run in init, once per
 * execution. */
static void ducknng_ncurl_table_bind(duckdb_bind_info info) {
    duckdb_value url_val;
    duckdb_value method_val;
    duckdb_value headers_val;
    duckdb_value body_val;
    duckdb_value timeout_val;
    duckdb_value tls_val;
    duckdb_value profile_val = NULL;
    duckdb_blob body_blob;
    ducknng_ncurl_table_bind_data *bd;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx, NULL)) return;
    bd = (ducknng_ncurl_table_bind_data *)duckdb_malloc(sizeof(*bd));
    if (!bd) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bd, 0, sizeof(*bd));
    memset(&body_blob, 0, sizeof(body_blob));
    url_val = duckdb_bind_get_parameter(info, 0);
    method_val = duckdb_bind_get_parameter(info, 1);
    headers_val = duckdb_bind_get_parameter(info, 2);
    body_val = duckdb_bind_get_parameter(info, 3);
    timeout_val = duckdb_bind_get_parameter(info, 4);
    tls_val = duckdb_bind_get_parameter(info, 5);
    if (duckdb_bind_get_parameter_count(info) > 6) profile_val = duckdb_bind_get_parameter(info, 6);
    bd->url = duckdb_is_null_value(url_val) ? NULL : duckdb_get_varchar(url_val);
    bd->method = duckdb_is_null_value(method_val) ? NULL : duckdb_get_varchar(method_val);
    bd->headers_json = duckdb_is_null_value(headers_val) ? NULL : duckdb_get_varchar(headers_val);
    if (!duckdb_is_null_value(body_val)) body_blob = duckdb_get_blob(body_val);
    bd->body = (uint8_t *)body_blob.data;
    bd->body_len = (size_t)body_blob.size;
    bd->timeout_ms = duckdb_get_int32(timeout_val);
    bd->tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    if (profile_val && !duckdb_is_null_value(profile_val)) bd->profile_id = duckdb_get_varchar(profile_val);
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&method_val);
    duckdb_destroy_value(&headers_val);
    duckdb_destroy_value(&body_val);
    duckdb_destroy_value(&timeout_val);
    duckdb_destroy_value(&tls_val);
    if (profile_val) duckdb_destroy_value(&profile_val);
    if (!bd->url || !bd->url[0]) {
        duckdb_bind_set_error(info, "ducknng: ducknng_ncurl_table URL must not be NULL or empty");
        goto fail;
    }
    if (ducknng_sql_bind_declared_schema(info, &bd->schema) != 0) goto fail;
    duckdb_table_function_get_client_context(info, &bd->client_ctx);
    duckdb_bind_set_bind_data(info, bd, destroy_ncurl_table_bind_data);
    return;
fail:
    destroy_ncurl_table_bind_data(bd);
}

/* Execution-time authorizer guard for plans that kept their binding client
 * context. A plan without one cannot prove where it runs and is rejected. */
static int ducknng_body_reject_init_inside_authorizer(duckdb_init_info info,
    ducknng_sql_context *ctx, duckdb_client_context client_ctx) {
    if (!client_ctx) {
        duckdb_init_set_error(info,
            "ducknng: missing DuckDB connection id for an authorizer-guarded function");
        return 1;
    }
    return ducknng_reject_init_inside_authorizer(info, ctx,
        (uint64_t)duckdb_client_context_get_connection_id(client_ctx));
}

static void ducknng_ncurl_table_init(duckdb_init_info info) {
    ducknng_ncurl_table_bind_data *bd = (ducknng_ncurl_table_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_init_get_extra_info(info);
    ducknng_body_parse_init_data *init;
    ducknng_body_parse_bind_data *parsed = NULL;
    const ducknng_tls_opts *tls_opts = NULL;
    char *effective_headers_json = NULL;
    char *resp_headers_json = NULL;
    char *content_type = NULL;
    char *errmsg = NULL;
    uint8_t *resp_body = NULL;
    size_t resp_body_len = 0;
    uint16_t status = 0;
    char status_msg[128];
    if (!bd) {
        duckdb_init_set_error(info, "ducknng: missing ducknng_ncurl_table bind data");
        return;
    }
    if (ducknng_body_reject_init_inside_authorizer(info, ctx, bd->client_ctx)) return;
    if (ducknng_lookup_tls_opts(ctx, bd->tls_config_id, &tls_opts, &errmsg) != 0) {
        duckdb_init_set_error(info, errmsg ? errmsg : "ducknng: tls config not found");
        goto cleanup;
    }
    if (bd->profile_id && bd->profile_id[0]) {
        uint64_t connection_id = 0;
        int has_connection_id = 0;
        if (bd->client_ctx) {
            connection_id = (uint64_t)duckdb_client_context_get_connection_id(bd->client_ctx);
            has_connection_id = 1;
        }
        if (ducknng_runtime_resolve_http_profile_headers(ctx->rt, bd->profile_id, bd->url,
                bd->method, bd->headers_json, has_connection_id, connection_id,
                &effective_headers_json, &errmsg) != 0) {
            duckdb_init_set_error(info, errmsg ? errmsg : "ducknng: failed to resolve HTTP profile");
            goto cleanup;
        }
    }
    if (ducknng_http_transact(bd->url, bd->method,
            effective_headers_json ? effective_headers_json : bd->headers_json,
            bd->body, bd->body_len, bd->timeout_ms, tls_opts,
            &status, &resp_headers_json, &resp_body, &resp_body_len, &errmsg) != 0) {
        duckdb_init_set_error(info, errmsg ? errmsg : "ducknng: HTTP request failed");
        goto cleanup;
    }
    if (status < 200 || status >= 300) {
        snprintf(status_msg, sizeof(status_msg),
            "ducknng: HTTP status %u cannot be parsed as a table", (unsigned)status);
        duckdb_init_set_error(info, status_msg);
        goto cleanup;
    }
    if (ducknng_http_headers_json_get_header(resp_headers_json, "Content-Type", 0,
            &content_type, &errmsg) < 0) {
        duckdb_init_set_error(info, errmsg ? errmsg : "ducknng: invalid response headers_json");
        goto cleanup;
    }
    if (ducknng_body_parse_build(ctx, bd->client_ctx, resp_body, resp_body_len,
            content_type, &parsed, &errmsg) != 0 ||
        ducknng_body_parse_validate_declared(parsed, &bd->schema, &errmsg) != 0) {
        duckdb_init_set_error(info, errmsg ? errmsg : "ducknng: failed to parse HTTP response body");
        goto cleanup;
    }
    init = (ducknng_body_parse_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        goto cleanup;
    }
    memset(init, 0, sizeof(*init));
    init->bind = parsed;
    init->owns_bind = 1;
    parsed = NULL;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_body_parse_init_data);
cleanup:
    if (parsed) destroy_body_parse_bind_data(parsed);
    if (effective_headers_json) duckdb_free(effective_headers_json);
    if (resp_headers_json) duckdb_free(resp_headers_json);
    if (resp_body) duckdb_free(resp_body);
    if (content_type) duckdb_free(content_type);
    if (errmsg) duckdb_free(errmsg);
}

/* Borrow the bind-time result of a pure plan, or decode a deferred plan afresh
 * for this execution and own the result in the init data. */
static void ducknng_body_parse_init(duckdb_init_info info) {
    ducknng_body_plan_bind_data *plan = (ducknng_body_plan_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_init_get_extra_info(info);
    ducknng_body_parse_bind_data *parsed;
    ducknng_body_parse_init_data *init;
    int owns = 0;
    char *errmsg = NULL;
    if (!plan) {
        duckdb_init_set_error(info, "ducknng: missing body parse bind data");
        return;
    }
    parsed = plan->parsed;
    if (!parsed) {
        parsed = ducknng_body_plan_execute(ctx, plan, &errmsg);
        if (!parsed) {
            duckdb_init_set_error(info, errmsg ? errmsg : "ducknng: failed to parse body");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        owns = 1;
    }
    init = (ducknng_body_parse_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        if (owns) destroy_body_parse_bind_data(parsed);
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(init, 0, sizeof(*init));
    init->bind = parsed;
    init->owns_bind = owns;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_body_parse_init_data);
}

static void ducknng_body_parse_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_body_parse_init_data *init = (ducknng_body_parse_init_data *)duckdb_function_get_init_data(info);
    ducknng_body_parse_bind_data *bind = init ? init->bind : NULL;
    if (!init || !bind) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    if (bind->result_kind == DUCKNNG_BODY_RESULT_SINGLE) {
        if (init->emitted) {
            duckdb_data_chunk_set_size(output, 0);
            return;
        }
        if (bind->codec_kind == DUCKNNG_BODY_CODEC_TEXT) {
            if (bind->text) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 0), 0, bind->text, (idx_t)strlen(bind->text));
            else set_null(duckdb_data_chunk_get_vector(output, 0), 0);
        } else if (bind->codec_kind == DUCKNNG_BODY_CODEC_USER) {
            if (bind->text_is_null || !bind->text) set_null(duckdb_data_chunk_get_vector(output, 0), 0);
            else duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 0), 0, bind->text, (idx_t)strlen(bind->text));
        } else {
            if (bind->raw) assign_blob(duckdb_data_chunk_get_vector(output, 0), 0, bind->raw, bind->raw_len);
            else assign_blob(duckdb_data_chunk_get_vector(output, 0), 0, (const uint8_t *)"", 0);
        }
        duckdb_data_chunk_set_size(output, 1);
        init->emitted = 1;
        return;
    }
    if (bind->result_kind == DUCKNNG_BODY_RESULT_FRAME) {
        if (init->emitted) {
            duckdb_data_chunk_set_size(output, 0);
            return;
        }
        ducknng_body_parse_emit_frame_row(output, &bind->frame);
        duckdb_data_chunk_set_size(output, 1);
        init->emitted = 1;
        return;
    }
    if (bind->result_kind == DUCKNNG_BODY_RESULT_ARROW) {
        if (init->offset >= bind->row_count) {
            duckdb_data_chunk_set_size(output, 0);
            return;
        }
        if (ducknng_sql_arrow_scan_table(output, &bind->schema, &bind->array,
                bind->row_count, &init->offset, NULL) != 0) {
            duckdb_function_set_error(info, "ducknng: failed to decode Arrow IPC body");
        }
        return;
    }
    if (bind->result_kind == DUCKNNG_BODY_RESULT_QUACK_BATCH) {
        char *errmsg = NULL;
        if (init->offset >= bind->row_count) {
            duckdb_data_chunk_set_size(output, 0);
            return;
        }
        if (ducknng_quack_payload_scan(output, bind->raw, (size_t)bind->raw_len,
                &bind->quack_schema, &init->offset, &errmsg) != 0) {
            duckdb_function_set_error(info, errmsg ? errmsg : "ducknng: failed to decode ducknng_quack_batch body");
            if (errmsg) duckdb_free(errmsg);
        }
        return;
    }
    if (bind->result_kind == DUCKNNG_BODY_RESULT_FORM) {
        idx_t chunk_cap = duckdb_vector_size();
        idx_t remaining = bind->form_count - init->offset;
        idx_t batch = remaining < chunk_cap ? remaining : chunk_cap;
        idx_t i;
        duckdb_vector name_vec = duckdb_data_chunk_get_vector(output, 0);
        duckdb_vector value_vec = duckdb_data_chunk_get_vector(output, 1);
        if (batch == 0) {
            duckdb_data_chunk_set_size(output, 0);
            return;
        }
        for (i = 0; i < batch; i++) {
            idx_t row = init->offset + i;
            const char *k = bind->form_keys[row] ? bind->form_keys[row] : "";
            const char *v = bind->form_values[row] ? bind->form_values[row] : "";
            duckdb_unsafe_vector_assign_string_element_len(name_vec, i, k, (idx_t)strlen(k));
            duckdb_unsafe_vector_assign_string_element_len(value_vec, i, v, (idx_t)strlen(v));
        }
        duckdb_data_chunk_set_size(output, batch);
        init->offset += batch;
        return;
    }
    duckdb_data_chunk_set_size(output, 0);
}

static const ducknng_codec_static_row DUCKNNG_BUILTIN_CODECS[] = {
    {"raw", "application/octet-stream, */* fallback", "body BLOB", "No decoding; bytes are returned unchanged."},
    {"text", "text/*", "body_text VARCHAR", "Valid UTF-8 text bodies are exposed as VARCHAR."},
    {"json", "application/json, application/*+json", "dynamic table", "Parsed in memory through DuckDB JSON functions."},
    {"csv", "text/csv, application/csv, text/x-csv", "dynamic table", "Parsed via direct C parser; type inference (INT64, DOUBLE, VARCHAR) with nanoarrow array encoding. Separate ducknng_parse_csv(body) uses DuckDB read_csv_auto via tempfile."},
    {"tsv", "text/tab-separated-values, application/x-tsv, text/tsv", "dynamic table", "Parsed via direct C parser with tab delimiter; type inference (INT64, DOUBLE, VARCHAR) with nanoarrow array encoding. Separate ducknng_parse_tsv(body) uses DuckDB read_csv_auto(delim='\t') via tempfile."},
    {"parquet", "application/vnd.apache.parquet, application/parquet, application/x-parquet", "dynamic table", "Parsed via DuckDB read_parquet() through a temporary file. Separate ducknng_parse_parquet(body) provides the same path."},
    {"arrow_ipc", "application/vnd.apache.arrow.stream, application/vnd.apache.arrow.ipc, application/arrow", "dynamic table", "Decoded as Arrow IPC stream bytes with nanoarrow and mapped to DuckDB vectors."},
    {"ducknng_quack_batch", "application/vnd.ducknng.quack-batch", "dynamic table", "Decoded as a standalone ducknng_quack_batch body. This is not the full upstream Quack application/vnd.duckdb message envelope."},
    {"ducknng_frame", "application/vnd.ducknng.frame", "decoded frame columns", "Decoded as one ducknng protocol frame, matching ducknng_decode_frame()."},
    {"ndjson", "application/x-ndjson, application/ndjson, application/x-jsonlines, application/jsonlines", "dynamic table", "Newline-delimited JSON lines wrapped into a JSON array and parsed in memory through DuckDB JSON functions."},
    {"form", "application/x-www-form-urlencoded", "name VARCHAR, value VARCHAR", "URL-decoded key/value pairs from a form body; one row per field."}
};

static int ducknng_codec_dyn_row_set(ducknng_codec_dyn_row *row, const char *provider,
    const char *media_types, const char *kind, const char *function_name,
    const char *output, const char *notes) {
    row->provider = provider ? ducknng_strdup(provider) : NULL;
    row->media_types = media_types ? ducknng_strdup(media_types) : NULL;
    row->kind = kind ? ducknng_strdup(kind) : NULL;
    row->function_name = function_name ? ducknng_strdup(function_name) : NULL;
    row->output = output ? ducknng_strdup(output) : NULL;
    row->notes = notes ? ducknng_strdup(notes) : NULL;
    if (provider && !row->provider) return -1;
    if (media_types && !row->media_types) return -1;
    if (kind && !row->kind) return -1;
    if (function_name && !row->function_name) return -1;
    if (output && !row->output) return -1;
    if (notes && !row->notes) return -1;
    return 0;
}

static void ducknng_codecs_bind(duckdb_bind_info info) {
    duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "provider", type);
    duckdb_bind_add_result_column(info, "media_types", type);
    duckdb_bind_add_result_column(info, "kind", type);
    duckdb_bind_add_result_column(info, "function_name", type);
    duckdb_bind_add_result_column(info, "output", type);
    duckdb_bind_add_result_column(info, "notes", type);
    duckdb_destroy_logical_type(&type);
}

/* Copy builtin rows and the current user codec registry into init. Rows are
 * zeroed before filling, and row_count counts only fully copied rows, so a
 * partial copy is released by destroy_codecs_init_data. */
static int ducknng_codecs_snapshot(ducknng_sql_context *ctx,
    ducknng_codecs_init_data *init, const char **errmsg) {
    idx_t builtin_count = (idx_t)(sizeof(DUCKNNG_BUILTIN_CODECS) / sizeof(DUCKNNG_BUILTIN_CODECS[0]));
    idx_t user_count = 0;
    idx_t total;
    idx_t i;
    int rc = -1;
    if (ctx && ctx->rt) {
        ducknng_mutex_lock(&ctx->rt->mu);
        user_count = (idx_t)ctx->rt->user_codec_count;
    }
    total = builtin_count + user_count;
    init->rows = (ducknng_codec_dyn_row *)duckdb_malloc(sizeof(*init->rows) * total);
    if (!init->rows) {
        *errmsg = "ducknng: out of memory listing codecs";
        goto out;
    }
    memset(init->rows, 0, sizeof(*init->rows) * total);
    for (i = 0; i < builtin_count; i++) {
        const ducknng_codec_static_row *src = &DUCKNNG_BUILTIN_CODECS[i];
        if (ducknng_codec_dyn_row_set(&init->rows[init->row_count], src->provider,
                src->media_types, "builtin", NULL, src->output, src->notes) != 0) {
            *errmsg = "ducknng: out of memory copying builtin codec rows";
            init->row_count++;
            goto out;
        }
        init->row_count++;
    }
    for (i = 0; i < user_count; i++) {
        const ducknng_user_codec *uc = &ctx->rt->user_codecs[i];
        if (ducknng_codec_dyn_row_set(&init->rows[init->row_count], "user", uc->content_type,
                "user", uc->function_name, "value VARCHAR",
                "User codec: <function_name>(BLOB) -> VARCHAR. Takes precedence over built-in providers.") != 0) {
            *errmsg = "ducknng: out of memory copying user codec rows";
            init->row_count++;
            goto out;
        }
        init->row_count++;
    }
    rc = 0;
out:
    if (ctx && ctx->rt) ducknng_mutex_unlock(&ctx->rt->mu);
    return rc;
}

static void ducknng_codecs_init(duckdb_init_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_init_get_extra_info(info);
    ducknng_codecs_init_data *init = (ducknng_codecs_init_data *)duckdb_malloc(sizeof(*init));
    const char *errmsg = NULL;
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(init, 0, sizeof(*init));
    if (ducknng_codecs_snapshot(ctx, init, &errmsg) != 0) {
        destroy_codecs_init_data(init);
        duckdb_init_set_error(info, errmsg);
        return;
    }
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_codecs_init_data);
}

static void ducknng_codecs_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_codecs_init_data *init = (ducknng_codecs_init_data *)duckdb_function_get_init_data(info);
    idx_t remaining;
    idx_t chunk_size;
    idx_t i;
    (void)info;
    if (!init || init->offset >= init->row_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    remaining = init->row_count - init->offset;
    chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;
    for (i = 0; i < chunk_size; i++) {
        const ducknng_codec_dyn_row *row = &init->rows[init->offset + i];
        duckdb_vector v0 = duckdb_data_chunk_get_vector(output, 0);
        duckdb_vector v1 = duckdb_data_chunk_get_vector(output, 1);
        duckdb_vector v2 = duckdb_data_chunk_get_vector(output, 2);
        duckdb_vector v3 = duckdb_data_chunk_get_vector(output, 3);
        duckdb_vector v4 = duckdb_data_chunk_get_vector(output, 4);
        duckdb_vector v5 = duckdb_data_chunk_get_vector(output, 5);
        if (row->provider) duckdb_unsafe_vector_assign_string_element_len(v0, i, row->provider, (idx_t)strlen(row->provider)); else set_null(v0, i);
        if (row->media_types) duckdb_unsafe_vector_assign_string_element_len(v1, i, row->media_types, (idx_t)strlen(row->media_types)); else set_null(v1, i);
        if (row->kind) duckdb_unsafe_vector_assign_string_element_len(v2, i, row->kind, (idx_t)strlen(row->kind)); else set_null(v2, i);
        if (row->function_name) duckdb_unsafe_vector_assign_string_element_len(v3, i, row->function_name, (idx_t)strlen(row->function_name)); else set_null(v3, i);
        if (row->output) duckdb_unsafe_vector_assign_string_element_len(v4, i, row->output, (idx_t)strlen(row->output)); else set_null(v4, i);
        if (row->notes) duckdb_unsafe_vector_assign_string_element_len(v5, i, row->notes, (idx_t)strlen(row->notes)); else set_null(v5, i);
    }
    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
}

static int register_body_parse_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_type param_types[2] = {DUCKDB_TYPE_BLOB, DUCKDB_TYPE_VARCHAR};
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE_WITH_SCHEMA(con, name, ctx, 2, param_types, ducknng_body_parse_bind,
        ducknng_body_parse_init, ducknng_body_parse_scan);
}

static int register_ncurl_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_type profile_param_types[7] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_BLOB, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_VARCHAR};
    const char *sql =
        "CREATE OR REPLACE MACRO ducknng_ncurl_table(url, method, headers_json, body, timeout_ms, tls_config_id, profile_id := NULL, schema := NULL) AS TABLE "
        "SELECT * FROM ducknng__ncurl_table(url, method, headers_json, body, timeout_ms, tls_config_id, profile_id, schema := schema)";
    (void)name;
    if (!ctx || !ctx->rt) return 0;
    if (!DUCKNNG_REGISTER_TABLE_WITH_SCHEMA(con, "ducknng__ncurl_table", ctx, 7, profile_param_types,
            ducknng_ncurl_table_bind, ducknng_ncurl_table_init, ducknng_body_parse_scan)) return 0;
    return execute_sql(con, sql);
}

static int register_codecs_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    return DUCKNNG_REGISTER_TABLE(con, name, ctx, 0, NULL, ducknng_codecs_bind,
        ducknng_codecs_init, ducknng_codecs_scan);
}

static int register_frame_decode_table_named(duckdb_connection con, const char *name) {
    duckdb_type param_types[1] = {DUCKDB_TYPE_BLOB};
    return DUCKNNG_REGISTER_TABLE(con, name, NULL, 1, param_types, ducknng_decode_frame_bind,
        ducknng_single_row_init, ducknng_decode_frame_scan);
}

static void ducknng_parse_csv_bind(duckdb_bind_info info) {
    ducknng_body_reader_bind(info, DUCKNNG_BODY_CODEC_CSV);
}

static void ducknng_parse_tsv_bind(duckdb_bind_info info) {
    ducknng_body_reader_bind(info, DUCKNNG_BODY_CODEC_TSV);
}

static void ducknng_parse_parquet_bind(duckdb_bind_info info) {
    ducknng_body_reader_bind(info, DUCKNNG_BODY_CODEC_PARQUET);
}

static int register_body_parse_format_table(duckdb_connection con, ducknng_sql_context *ctx, const char *name, duckdb_table_function_bind_t bind_fn) {
    duckdb_type param_types[1] = {DUCKDB_TYPE_BLOB};
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE_WITH_SCHEMA(con, name, ctx, 1, param_types, bind_fn,
        ducknng_body_parse_init, ducknng_body_parse_scan);
}

static void ducknng_register_codec_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    duckdb_vector ct_vec = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector fn_vec = duckdb_data_chunk_get_vector(input, 1);
    for (row = 0; row < count; row++) {
        char *content_type = arg_varchar_dup(ct_vec, row);
        char *function_name = arg_varchar_dup(fn_vec, row);
        char *errmsg = NULL;
        int ok;
        if (!ctx || !ctx->rt || !content_type || !function_name) {
            if (content_type) duckdb_free(content_type);
            if (function_name) duckdb_free(function_name);
            duckdb_scalar_function_set_error(info, "ducknng: register_codec requires non-null content_type and function_name");
            return;
        }
        if (!ducknng_is_sql_identifier(function_name)) {
            duckdb_free(content_type);
            duckdb_free(function_name);
            duckdb_scalar_function_set_error(info, "ducknng: codec function name must be a plain SQL identifier (letters, digits, underscore, dot)");
            return;
        }
        ok = ducknng_runtime_register_user_codec(ctx->rt, content_type, function_name, &errmsg);
        duckdb_free(content_type);
        duckdb_free(function_name);
        if (!ok) {
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to register codec");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (errmsg) duckdb_free(errmsg);
        out[row] = true;
    }
}

static void ducknng_unregister_codec_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    duckdb_vector ct_vec = duckdb_data_chunk_get_vector(input, 0);
    for (row = 0; row < count; row++) {
        char *content_type = arg_varchar_dup(ct_vec, row);
        if (!ctx || !ctx->rt || !content_type) {
            if (content_type) duckdb_free(content_type);
            duckdb_scalar_function_set_error(info, "ducknng: unregister_codec requires a non-null content_type");
            return;
        }
        out[row] = ducknng_runtime_unregister_user_codec(ctx->rt, content_type) ? true : false;
        duckdb_free(content_type);
    }
}

/*
 * ducknng_encode_rpc_call(method, payload) builds the version-1 call frame a
 * manifest-declared JSON method accepts. It is the encoding counterpart of
 * ducknng_decode_frame(): SQL clients send the frame with ducknng_request_raw()
 * or a socket aio instead of assembling header bytes themselves. The payload is
 * sent as JSON text; the server validates it against the method's schema.
 */
static void ducknng_encode_rpc_call_scalar(duckdb_function_info info, duckdb_data_chunk input,
    duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    duckdb_vector method_vec = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector payload_vec = duckdb_data_chunk_get_vector(input, 1);
    for (row = 0; row < count; row++) {
        ducknng_frame_parts parts;
        char *method;
        char *payload;
        uint8_t *frame;
        size_t method_len;
        size_t need = 0;
        size_t written = 0;
        if (arg_is_null(method_vec, row) || arg_is_null(payload_vec, row)) {
            set_null(output, row);
            continue;
        }
        method = arg_varchar_dup(method_vec, row);
        payload = arg_varchar_dup(payload_vec, row);
        if (!method || !payload) {
            if (method) duckdb_free(method);
            if (payload) duckdb_free(payload);
            duckdb_scalar_function_set_error(info, "ducknng: out of memory encoding RPC call");
            return;
        }
        method_len = strlen(method);
        if (method_len == 0 || method_len > DUCKNNG_MAX_METHOD_NAME_LEN) {
            duckdb_free(method);
            duckdb_free(payload);
            duckdb_scalar_function_set_error(info,
                "ducknng: RPC method name must be 1 to 128 bytes");
            return;
        }
        memset(&parts, 0, sizeof(parts));
        parts.type = DUCKNNG_RPC_CALL;
        parts.flags = DUCKNNG_RPC_FLAG_PAYLOAD_JSON;
        parts.name = (const uint8_t *)method;
        parts.name_len = method_len;
        parts.payload = (const uint8_t *)payload;
        parts.payload_len = strlen(payload);
        frame = NULL;
        if (ducknng_frame_measure(&parts, &need) == 0 && need > 0)
            frame = (uint8_t *)duckdb_malloc(need);
        if (!frame || ducknng_encode_frame_bytes(&parts, frame, need, &written) != 0 ||
            written != need) {
            if (frame) duckdb_free(frame);
            duckdb_free(method);
            duckdb_free(payload);
            duckdb_scalar_function_set_error(info, "ducknng: failed to encode RPC call frame");
            return;
        }
        assign_blob(output, row, frame, (idx_t)written);
        duckdb_free(frame);
        duckdb_free(method);
        duckdb_free(payload);
    }
}

int ducknng_register_sql_body(duckdb_connection con, ducknng_sql_context *ctx) {
    duckdb_type frame_types[1] = {DUCKDB_TYPE_BLOB};
    duckdb_type register_codec_types[2] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR};
    duckdb_type unregister_codec_types[1] = {DUCKDB_TYPE_VARCHAR};
    duckdb_type encode_call_types[2] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR};
    if (!register_body_parse_table_named(con, ctx, "ducknng_parse_body")) return 0;
    if (!register_ncurl_table_named(con, ctx, "ducknng_ncurl_table")) return 0;
    if (!register_codecs_table_named(con, ctx, "ducknng_list_codecs")) return 0;
    if (!register_frame_decode_table_named(con, "ducknng_decode_frame")) return 0;
    if (!DUCKNNG_REGISTER_SCALAR(con, "ducknng_frame_payload", 1,
            ducknng_frame_payload_scalar, ctx, frame_types, DUCKDB_TYPE_BLOB)) return 0;
    if (!DUCKNNG_REGISTER_SCALAR(con, "ducknng_frame_payload_text", 1,
            ducknng_frame_payload_text_scalar, ctx, frame_types, DUCKDB_TYPE_VARCHAR)) return 0;
    if (!DUCKNNG_REGISTER_SCALAR(con, "ducknng_frame_error_text", 1,
            ducknng_frame_error_text_scalar, ctx, frame_types, DUCKDB_TYPE_VARCHAR)) return 0;
    if (!DUCKNNG_REGISTER_SCALAR(con, "ducknng_frame_version", 1,
            ducknng_frame_version_scalar, ctx, frame_types, DUCKDB_TYPE_UTINYINT)) return 0;
    if (!DUCKNNG_REGISTER_SCALAR(con, "ducknng_frame_type", 1,
            ducknng_frame_type_scalar, ctx, frame_types, DUCKDB_TYPE_UTINYINT)) return 0;
    if (!DUCKNNG_REGISTER_SCALAR(con, "ducknng_frame_status", 1,
            ducknng_frame_status_scalar, ctx, frame_types, DUCKDB_TYPE_UTINYINT)) return 0;
    if (!DUCKNNG_REGISTER_SCALAR(con, "ducknng_frame_flags", 1,
            ducknng_frame_flags_scalar, ctx, frame_types, DUCKDB_TYPE_UINTEGER)) return 0;
    if (!DUCKNNG_REGISTER_SCALAR(con, "ducknng_frame_type_name", 1,
            ducknng_frame_type_name_scalar, ctx, frame_types, DUCKDB_TYPE_VARCHAR)) return 0;
    if (!DUCKNNG_REGISTER_SCALAR(con, "ducknng_frame_name", 1,
            ducknng_frame_name_scalar, ctx, frame_types, DUCKDB_TYPE_VARCHAR)) return 0;
    if (!DUCKNNG_REGISTER_SCALAR(con, "ducknng_frame_end_of_stream", 1,
            ducknng_frame_end_of_stream_scalar, ctx, frame_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_SCALAR(con, "ducknng_encode_rpc_call", 2,
            ducknng_encode_rpc_call_scalar, ctx, encode_call_types, DUCKDB_TYPE_BLOB)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_register_codec", 2,
            ducknng_register_codec_scalar, ctx, register_codec_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_unregister_codec", 1,
            ducknng_unregister_codec_scalar, ctx, unregister_codec_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_body_parse_format_table(con, ctx, "ducknng_parse_csv", ducknng_parse_csv_bind)) return 0;
    if (!register_body_parse_format_table(con, ctx, "ducknng_parse_tsv", ducknng_parse_tsv_bind)) return 0;
    if (!register_body_parse_format_table(con, ctx, "ducknng_parse_parquet", ducknng_parse_parquet_bind)) return 0;
    return 1;
}
