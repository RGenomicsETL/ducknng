#include "ducknng_sql_shared.h"
#include "ducknng_net_backend.h"
#include "ducknng_http_compat.h"
#include "ducknng_ipc_in.h"
#include "ducknng_ipc_out.h"
#include "ducknng_nng_compat.h"
#include "ducknng_runtime.h"
#include "ducknng_sql_arrow.h"
#include "ducknng_transport.h"
#include "ducknng_util.h"
#include "ducknng_wire.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

/* Immutable arguments of the explicit open/fetch/close/cancel helpers, or the
 * argument error that every execution reports. */
typedef struct {
    ducknng_sql_context *ctx;
    char *url;
    char *sql;
    char *method_name;
    uint64_t session_id;
    char *session_token;
    uint64_t batch_rows;
    uint64_t batch_bytes;
    uint64_t tls_config_id;
    int expect_json_only;
    char *arg_error;
    uint64_t connection_id; /* binding connection, for the init authorizer guard */
} ducknng_session_args;

/* One decoded session reply. Every field is owned. */
typedef struct {
    bool ok;
    char *error;
    uint64_t session_id;
    char *session_token;
    char *state;
    char *next_method;
    char *control_json;
    uint64_t idle_timeout_ms;
    int has_idle_timeout_ms;
    uint8_t *payload;
    idx_t payload_len;
    bool end_of_stream;
} ducknng_session_reply;

/* One execution of an explicit session helper: its reply and emit flag. */
typedef struct {
    idx_t emitted;
    ducknng_session_reply reply;
} ducknng_session_init_data;

/* Immutable ducknng_fetch_query_table arguments and declared columns resolved
 * at bind. The session itself is owned by the caller. */
typedef struct {
    ducknng_sql_context *ctx;
    char *url;
    uint64_t session_id;
    char *session_token;
    uint64_t batch_rows;
    uint64_t batch_bytes;
    uint64_t tls_config_id;
    ducknng_sql_declared_schema declared;
    uint64_t connection_id;
} ducknng_fetch_query_table_bind_data;

/* One execution: the single fetched batch and the scan offset into it. */
typedef struct {
    struct ArrowSchema schema;
    struct ArrowArray array;
    idx_t row_count;
    idx_t offset;
} ducknng_fetch_query_table_init_data;

static void ducknng_session_reply_reset(ducknng_session_reply *reply) {
    if (!reply) return;
    if (reply->error) duckdb_free(reply->error);
    if (reply->session_token) duckdb_free(reply->session_token);
    if (reply->state) duckdb_free(reply->state);
    if (reply->next_method) duckdb_free(reply->next_method);
    if (reply->control_json) duckdb_free(reply->control_json);
    if (reply->payload) duckdb_free(reply->payload);
    memset(reply, 0, sizeof(*reply));
}

static void destroy_session_args(void *ptr) {
    ducknng_session_args *data = (ducknng_session_args *)ptr;
    if (!data) return;
    if (data->url) duckdb_free(data->url);
    if (data->sql) duckdb_free(data->sql);
    if (data->method_name) duckdb_free(data->method_name);
    if (data->session_token) duckdb_free(data->session_token);
    if (data->arg_error) duckdb_free(data->arg_error);
    duckdb_free(data);
}

static void destroy_session_init_data(void *ptr) {
    ducknng_session_init_data *data = (ducknng_session_init_data *)ptr;
    if (!data) return;
    ducknng_session_reply_reset(&data->reply);
    duckdb_free(data);
}

static void destroy_fetch_query_table_bind_data(void *ptr) {
    ducknng_fetch_query_table_bind_data *data = (ducknng_fetch_query_table_bind_data *)ptr;
    if (!data) return;
    if (data->url) duckdb_free(data->url);
    if (data->session_token) duckdb_free(data->session_token);
    ducknng_sql_declared_schema_reset(&data->declared);
    duckdb_free(data);
}

static void destroy_fetch_query_table_init_data(void *ptr) {
    ducknng_fetch_query_table_init_data *data = (ducknng_fetch_query_table_init_data *)ptr;
    if (!data) return;
    if (data->array.release) ArrowArrayRelease(&data->array);
    if (data->schema.release) ArrowSchemaRelease(&data->schema);
    duckdb_free(data);
}

/* Decode the fetched batch into init and check it against the declaration.
 * On failure *errmsg is set; the batch has already been consumed remotely. */
static int ducknng_fetch_query_table_take_batch(const ducknng_fetch_query_table_bind_data *bind,
    ducknng_session_reply *resp, ducknng_fetch_query_table_init_data *init,
    char **errmsg) {
    if (!resp->ok) {
        *errmsg = resp->error ? resp->error : ducknng_strdup("ducknng: fetch_query_table request failed");
        resp->error = NULL;
        return -1;
    }
    if (!resp->payload) {
        *errmsg = ducknng_strdup("ducknng: fetch_query_table expected an Arrow row payload; "
            "use ducknng_fetch_query(...) for control-only session replies");
        return -1;
    }
    if (ducknng_decode_ipc_table_payload(resp->payload, (size_t)resp->payload_len,
            &init->schema, &init->array, errmsg) != 0) {
        if (!*errmsg) *errmsg = ducknng_strdup("ducknng: failed to decode fetch_query_table Arrow payload");
        return -1;
    }
    if (init->schema.n_children < 0 || init->schema.n_children != init->array.n_children) {
        *errmsg = ducknng_strdup("ducknng: invalid fetch_query_table Arrow schema");
        return -1;
    }
    if (ducknng_sql_arrow_schema_validate_declared(&init->schema, bind->declared.ncols,
            (const char *const *)bind->declared.names, bind->declared.types, errmsg) != 0) {
        if (!*errmsg) *errmsg = ducknng_strdup("ducknng: fetch_query_table reply does not match schema");
        return -1;
    }
    init->row_count = (idx_t)init->array.length;
    return 0;
}

static char *ducknng_frame_error_detail(const ducknng_frame *frame, const char *fallback) {
    char *detail;
    if (!frame || !frame->error || frame->error_len == 0) return ducknng_strdup(fallback);
    detail = (char *)duckdb_malloc((size_t)frame->error_len + 1);
    if (!detail) return ducknng_strdup("ducknng: out of memory decoding remote error");
    memcpy(detail, frame->error, (size_t)frame->error_len);
    detail[frame->error_len] = '\0';
    return detail;
}


static const char *ducknng_json_find_key(const char *json, const char *key) {
    char needle[128];
    const char *p;
    if (!json || !key) return NULL;
    if (snprintf(needle, sizeof(needle), "\"%s\"", key) >= (int)sizeof(needle)) return NULL;
    p = strstr(json, needle);
    if (!p) return NULL;
    p = strchr(p + strlen(needle), ':');
    if (!p) return NULL;
    p++;
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
    return p;
}

static int ducknng_json_extract_u64_value(const char *json, const char *key, uint64_t *out) {
    const char *p = ducknng_json_find_key(json, key);
    char *end = NULL;
    if (out) *out = 0;
    if (!p || !out) return -1;
    if (*p == '"') p++;
    *out = (uint64_t)strtoull(p, &end, 10);
    return end == p ? -1 : 0;
}

static char *ducknng_json_extract_string_dup(const char *json, const char *key) {
    const char *p = ducknng_json_find_key(json, key);
    const char *end;
    char *out;
    size_t len;
    if (!p || *p != '"') return NULL;
    p++;
    end = strchr(p, '"');
    if (!end) return NULL;
    len = (size_t)(end - p);
    out = (char *)duckdb_malloc(len + 1);
    if (!out) return NULL;
    if (len) memcpy(out, p, len);
    out[len] = '\0';
    return out;
}

static nng_msg *ducknng_client_method_request(const char *method_name, const void *payload,
    size_t payload_len, char **errmsg) {
    nng_msg *msg;
    msg = ducknng_build_reply(DUCKNNG_RPC_CALL, method_name, 0, NULL, payload, (uint64_t)payload_len);
    if (!msg && errmsg && !*errmsg) {
        *errmsg = ducknng_strdup("ducknng: failed to allocate RPC request message");
    }
    return msg;
}

static nng_msg *ducknng_client_roundtrip_tls(const char *url, nng_msg *req, int timeout_ms,
    const ducknng_tls_opts *tls_opts, char **errmsg);

static nng_msg *ducknng_client_method_roundtrip_tls(const char *url, const char *method_name,
    const void *payload, size_t payload_len, int timeout_ms, const ducknng_tls_opts *tls_opts,
    char **errmsg) {
    nng_msg *req = ducknng_client_method_request(method_name, payload, payload_len, errmsg);
    if (!req) return NULL;
    return ducknng_client_roundtrip_tls(url, req, timeout_ms, tls_opts, errmsg);
}

static void ducknng_session_reply_fill(ducknng_session_reply *reply,
    uint64_t fallback_session_id, nng_msg *resp_msg) {
    ducknng_frame frame;
    if (!reply) return;
    reply->session_id = fallback_session_id;
    if (!resp_msg) {
        reply->ok = false;
        reply->error = ducknng_strdup("ducknng: missing session response");
        return;
    }
    if (ducknng_decode_request(resp_msg, &frame) != 0) {
        reply->ok = false;
        reply->error = ducknng_strdup("ducknng: invalid session response envelope");
        return;
    }
    if (frame.type == DUCKNNG_RPC_ERROR) {
        reply->ok = false;
        reply->error = ducknng_frame_error_detail(&frame, "ducknng: session request failed");
        return;
    }
    if (frame.type != DUCKNNG_RPC_RESULT) {
        reply->ok = false;
        reply->error = ducknng_strdup("ducknng: session response was not an RPC result");
        return;
    }
    reply->end_of_stream = (frame.flags & DUCKNNG_RPC_FLAG_END_OF_STREAM) != 0;
    if (frame.flags & DUCKNNG_RPC_FLAG_PAYLOAD_JSON) {
        reply->control_json = (char *)duckdb_malloc((size_t)frame.payload_len + 1);
        if (!reply->control_json) {
            reply->ok = false;
            reply->error = ducknng_strdup("ducknng: out of memory copying session control payload");
            return;
        }
        if (frame.payload_len) memcpy(reply->control_json, frame.payload, (size_t)frame.payload_len);
        reply->control_json[frame.payload_len] = '\0';
        (void)ducknng_json_extract_u64_value(reply->control_json, "session_id", &reply->session_id);
        if (!reply->session_token) reply->session_token = ducknng_json_extract_string_dup(reply->control_json, "session_token");
        reply->state = ducknng_json_extract_string_dup(reply->control_json, "state");
        reply->next_method = ducknng_json_extract_string_dup(reply->control_json, "next_method");
        if (ducknng_json_extract_u64_value(reply->control_json, "idle_timeout_ms", &reply->idle_timeout_ms) == 0) {
            reply->has_idle_timeout_ms = 1;
        }
        reply->ok = true;
        return;
    }
    if ((frame.flags & DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM) &&
            (frame.flags & DUCKNNG_RPC_FLAG_RESULT_ROWS)) {
        reply->payload_len = (idx_t)frame.payload_len;
        reply->payload = (uint8_t *)duckdb_malloc((size_t)reply->payload_len);
        if (!reply->payload && reply->payload_len > 0) {
            reply->ok = false;
            reply->error = ducknng_strdup("ducknng: out of memory copying session row payload");
            return;
        }
        if (reply->payload_len) memcpy(reply->payload, frame.payload, (size_t)reply->payload_len);
        reply->ok = true;
        return;
    }
    reply->ok = false;
    reply->error = ducknng_strdup("ducknng: unexpected session response payload kind");
}


static int ducknng_client_open_req_socket_tls(const char *url, int timeout_ms, const ducknng_tls_opts *tls_opts, nng_socket *out, char **errmsg) {
    int rv;
    if (!url || !url[0] || !out) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: client URL is required");
        return -1;
    }
    if (ducknng_socket_validate_client_url(url, tls_opts, errmsg) != 0) return -1;
    rv = ducknng_req_socket_open(out);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return -1;
    }
    rv = ducknng_socket_set_timeout_ms(*out, timeout_ms, timeout_ms);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        ducknng_socket_close(*out);
        return -1;
    }
    rv = ducknng_socket_apply_tls(*out, url, tls_opts);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        ducknng_socket_close(*out);
        return -1;
    }
    rv = ducknng_socket_dial(*out, url);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        ducknng_socket_close(*out);
        return -1;
    }
    return 0;
}

static int ducknng_client_open_req_socket(const char *url, int timeout_ms, nng_socket *out, char **errmsg) {
    return ducknng_client_open_req_socket_tls(url, timeout_ms, NULL, out, errmsg);
}

static nng_msg *ducknng_client_roundtrip_tls(const char *url, nng_msg *req, int timeout_ms, const ducknng_tls_opts *tls_opts, char **errmsg) {
    ducknng_transport_url parsed;
    nng_socket sock;
    nng_msg *resp = NULL;
    uint8_t *reply_frame = NULL;
    size_t reply_frame_len = 0;
    int rv;
    memset(&sock, 0, sizeof(sock));
    if (!req) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: request message is required");
        return NULL;
    }
    if (ducknng_transport_url_parse(url, &parsed, errmsg) != 0) {
        nng_msg_free(req);
        return NULL;
    }
    if (ducknng_net_backend_carrier_scheme(parsed.scheme)) {
        if (ducknng_http_frame_transact(url, (const uint8_t *)nng_msg_body(req), nng_msg_len(req),
                timeout_ms, tls_opts, &reply_frame, &reply_frame_len, errmsg) != 0) {
            nng_msg_free(req);
            return NULL;
        }
        nng_msg_free(req);
        rv = nng_msg_alloc(&resp, reply_frame_len);
        if (rv != 0) {
            if (reply_frame) duckdb_free(reply_frame);
            if (errmsg && !*errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
            return NULL;
        }
        if (reply_frame_len) memcpy(nng_msg_body(resp), reply_frame, reply_frame_len);
        if (reply_frame) duckdb_free(reply_frame);
        return resp;
    }
    if (ducknng_client_open_req_socket_tls(url, timeout_ms, tls_opts, &sock, errmsg) != 0) {
        nng_msg_free(req);
        return NULL;
    }
    rv = ducknng_req_transact(sock, req, &resp);
    ducknng_socket_close(sock);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return NULL;
    }
    return resp;
}

static nng_msg *ducknng_client_roundtrip(const char *url, nng_msg *req, int timeout_ms, char **errmsg) {
    return ducknng_client_roundtrip_tls(url, req, timeout_ms, NULL, errmsg);
}

static nng_msg *ducknng_client_roundtrip_raw_tls(const char *url, const uint8_t *payload, size_t payload_len,
    int timeout_ms, const ducknng_tls_opts *tls_opts, char **errmsg) {
    nng_msg *req = NULL;
    int rv = nng_msg_alloc(&req, payload_len);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return NULL;
    }
    if (payload_len) memcpy(nng_msg_body(req), payload, payload_len);
    return ducknng_client_roundtrip_tls(url, req, timeout_ms, tls_opts, errmsg);
}

static nng_msg *ducknng_client_roundtrip_raw(const char *url, const uint8_t *payload, size_t payload_len,
    int timeout_ms, char **errmsg) {
    nng_msg *req = NULL;
    int rv = nng_msg_alloc(&req, payload_len);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return NULL;
    }
    if (payload_len) memcpy(nng_msg_body(req), payload, payload_len);
    return ducknng_client_roundtrip(url, req, timeout_ms, errmsg);
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

static void ducknng_session_bind_add_control_columns(duckdb_bind_info info) {
    duckdb_logical_type type;
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "ok", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "error", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "session_id", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "session_token", type);
    duckdb_bind_add_result_column(info, "state", type);
    duckdb_bind_add_result_column(info, "next_method", type);
    duckdb_bind_add_result_column(info, "control_json", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "idle_timeout_ms", type);
    duckdb_destroy_logical_type(&type);
}

static void ducknng_session_call(ducknng_session_reply *reply,
    ducknng_sql_context *ctx, const char *url, const char *method_name,
    const uint8_t *payload, size_t payload_len, uint64_t tls_config_id,
    uint64_t fallback_session_id, int expect_json_only) {
    const ducknng_tls_opts *tls_opts = NULL;
    char *errmsg = NULL;
    nng_msg *resp_msg = NULL;
    if (!reply) return;
    if (!url || !url[0]) {
        reply->ok = false;
        reply->error = ducknng_strdup("ducknng: session URL must not be NULL or empty");
        return;
    }
    if (ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, &errmsg) != 0) {
        reply->ok = false;
        reply->error = errmsg ? errmsg : ducknng_strdup("ducknng: tls config not found");
        return;
    }
    resp_msg = ducknng_client_method_roundtrip_tls(url, method_name, payload, payload_len, 5000, tls_opts, &errmsg);
    if (!resp_msg) {
        reply->ok = false;
        reply->error = errmsg ? errmsg : ducknng_strdup("ducknng: session request failed");
        return;
    }
    ducknng_session_reply_fill(reply, fallback_session_id, resp_msg);
    nng_msg_free(resp_msg);
    if (expect_json_only && reply->ok && !reply->control_json) {
        if (reply->payload) {
            duckdb_free(reply->payload);
            reply->payload = NULL;
            reply->payload_len = 0;
        }
        reply->ok = false;
        reply->error = ducknng_strdup("ducknng: session control request expected a JSON control reply");
    }
}

static void ducknng_open_query_bind(duckdb_bind_info info) {
    ducknng_session_args *bind;
    duckdb_value url_val;
    duckdb_value sql_val;
    duckdb_value batch_rows_val;
    duckdb_value batch_bytes_val;
    duckdb_value tls_val;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    uint64_t connection_id;
    if (ducknng_reject_table_inside_authorizer(info, ctx, &connection_id)) return;
    bind = (ducknng_session_args *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    bind->connection_id = connection_id;
    bind->ctx = ctx;
    url_val = duckdb_bind_get_parameter(info, 0);
    sql_val = duckdb_bind_get_parameter(info, 1);
    batch_rows_val = duckdb_bind_get_parameter(info, 2);
    batch_bytes_val = duckdb_bind_get_parameter(info, 3);
    tls_val = duckdb_bind_get_parameter(info, 4);
    bind->url = duckdb_get_varchar(url_val);
    bind->sql = duckdb_get_varchar(sql_val);
    bind->method_name = ducknng_strdup("query_open");
    bind->batch_rows = (uint64_t)duckdb_get_uint64(batch_rows_val);
    bind->batch_bytes = (uint64_t)duckdb_get_uint64(batch_bytes_val);
    bind->tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    bind->expect_json_only = 1;
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&sql_val);
    duckdb_destroy_value(&batch_rows_val);
    duckdb_destroy_value(&batch_bytes_val);
    duckdb_destroy_value(&tls_val);
    if (!bind->url || !bind->url[0] || !bind->sql || !bind->sql[0]) {
        bind->arg_error = ducknng_strdup("ducknng: open_query requires non-empty url and sql");
    } else if (!bind->method_name) {
        bind->arg_error = ducknng_strdup("ducknng: out of memory preparing open_query call");
    }
    ducknng_session_bind_add_control_columns(info);
    duckdb_bind_set_bind_data(info, bind, destroy_session_args);
    duckdb_bind_set_cardinality(info, 1, true);
}

static void ducknng_close_query_bind(duckdb_bind_info info) {
    ducknng_session_args *bind;
    duckdb_value url_val;
    duckdb_value session_val;
    duckdb_value token_val;
    duckdb_value tls_val;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    uint64_t connection_id;
    if (ducknng_reject_table_inside_authorizer(info, ctx, &connection_id)) return;
    bind = (ducknng_session_args *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    bind->connection_id = connection_id;
    bind->ctx = ctx;
    url_val = duckdb_bind_get_parameter(info, 0);
    session_val = duckdb_bind_get_parameter(info, 1);
    token_val = duckdb_bind_get_parameter(info, 2);
    tls_val = duckdb_bind_get_parameter(info, 3);
    bind->url = duckdb_get_varchar(url_val);
    bind->session_id = (uint64_t)duckdb_get_uint64(session_val);
    bind->session_token = duckdb_get_varchar(token_val);
    bind->tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    bind->method_name = ducknng_strdup("close");
    bind->expect_json_only = 1;
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&session_val);
    duckdb_destroy_value(&token_val);
    duckdb_destroy_value(&tls_val);
    if (!bind->url || !bind->url[0] || bind->session_id == 0 || !bind->session_token || !bind->session_token[0]) {
        bind->arg_error = ducknng_strdup("ducknng: close_query requires non-empty url, session_id, and session_token");
    } else if (!bind->method_name) {
        bind->arg_error = ducknng_strdup("ducknng: out of memory preparing close_query call");
    }
    ducknng_session_bind_add_control_columns(info);
    duckdb_bind_set_bind_data(info, bind, destroy_session_args);
    duckdb_bind_set_cardinality(info, 1, true);
}

static void ducknng_cancel_query_bind(duckdb_bind_info info) {
    ducknng_session_args *bind;
    duckdb_value url_val;
    duckdb_value session_val;
    duckdb_value token_val;
    duckdb_value tls_val;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    uint64_t connection_id;
    if (ducknng_reject_table_inside_authorizer(info, ctx, &connection_id)) return;
    bind = (ducknng_session_args *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    bind->connection_id = connection_id;
    bind->ctx = ctx;
    url_val = duckdb_bind_get_parameter(info, 0);
    session_val = duckdb_bind_get_parameter(info, 1);
    token_val = duckdb_bind_get_parameter(info, 2);
    tls_val = duckdb_bind_get_parameter(info, 3);
    bind->url = duckdb_get_varchar(url_val);
    bind->session_id = (uint64_t)duckdb_get_uint64(session_val);
    bind->session_token = duckdb_get_varchar(token_val);
    bind->tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    bind->method_name = ducknng_strdup("cancel");
    bind->expect_json_only = 1;
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&session_val);
    duckdb_destroy_value(&token_val);
    duckdb_destroy_value(&tls_val);
    if (!bind->url || !bind->url[0] || bind->session_id == 0 || !bind->session_token || !bind->session_token[0]) {
        bind->arg_error = ducknng_strdup("ducknng: cancel_query requires non-empty url, session_id, and session_token");
    } else if (!bind->method_name) {
        bind->arg_error = ducknng_strdup("ducknng: out of memory preparing cancel_query call");
    }
    ducknng_session_bind_add_control_columns(info);
    duckdb_bind_set_bind_data(info, bind, destroy_session_args);
    duckdb_bind_set_cardinality(info, 1, true);
}

static void ducknng_fetch_query_bind(duckdb_bind_info info) {
    ducknng_session_args *bind;
    duckdb_logical_type type;
    duckdb_value url_val;
    duckdb_value session_val;
    duckdb_value token_val;
    duckdb_value batch_rows_val;
    duckdb_value batch_bytes_val;
    duckdb_value tls_val;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    uint64_t connection_id;
    if (ducknng_reject_table_inside_authorizer(info, ctx, &connection_id)) return;
    bind = (ducknng_session_args *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    bind->connection_id = connection_id;
    bind->ctx = ctx;
    url_val = duckdb_bind_get_parameter(info, 0);
    session_val = duckdb_bind_get_parameter(info, 1);
    token_val = duckdb_bind_get_parameter(info, 2);
    batch_rows_val = duckdb_bind_get_parameter(info, 3);
    batch_bytes_val = duckdb_bind_get_parameter(info, 4);
    tls_val = duckdb_bind_get_parameter(info, 5);
    bind->url = duckdb_get_varchar(url_val);
    bind->session_id = (uint64_t)duckdb_get_uint64(session_val);
    bind->session_token = duckdb_get_varchar(token_val);
    bind->batch_rows = (uint64_t)duckdb_get_uint64(batch_rows_val);
    bind->batch_bytes = (uint64_t)duckdb_get_uint64(batch_bytes_val);
    bind->tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    bind->method_name = ducknng_strdup("fetch");
    bind->expect_json_only = 0;
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&session_val);
    duckdb_destroy_value(&token_val);
    duckdb_destroy_value(&batch_rows_val);
    duckdb_destroy_value(&batch_bytes_val);
    duckdb_destroy_value(&tls_val);
    if (!bind->url || !bind->url[0] || bind->session_id == 0 || !bind->session_token || !bind->session_token[0]) {
        bind->arg_error = ducknng_strdup("ducknng: fetch_query requires non-empty url, session_id, and session_token");
    } else if (!bind->method_name) {
        bind->arg_error = ducknng_strdup("ducknng: out of memory preparing fetch_query call");
    }
    ducknng_session_bind_add_control_columns(info);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
    duckdb_bind_add_result_column(info, "payload", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "end_of_stream", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_session_args);
    duckdb_bind_set_cardinality(info, 1, true);
}

/* Each execution of a prepared plan gets a fresh reply; the session request
 * itself runs on the first scan. */
static void ducknng_session_result_init(duckdb_init_info info) {
    const ducknng_session_args *bind =
        (const ducknng_session_args *)duckdb_init_get_bind_data(info);
    ducknng_session_init_data *init;
    if (!bind || !bind->method_name) {
        duckdb_init_set_error(info, "ducknng: missing session method arguments");
        return;
    }
    if (ducknng_reject_init_inside_authorizer(info, bind->ctx, bind->connection_id)) return;
    init = (ducknng_session_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(init, 0, sizeof(*init));
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_session_init_data);
}

/* Send one session request built from the immutable arguments and decode its
 * reply. The reply starts from the caller-supplied session identity, which a
 * decoded control reply may refine. */
static void ducknng_session_execute(const ducknng_session_args *bind,
    ducknng_session_reply *reply) {
    uint8_t *payload = NULL;
    size_t payload_len = 0;
    char *json = NULL;
    char *errmsg = NULL;
    reply->session_id = bind->session_id;
    if (bind->session_token) {
        reply->session_token = ducknng_strdup(bind->session_token);
        if (!reply->session_token) {
            reply->error = ducknng_strdup("ducknng: out of memory copying session token");
            reply->ok = false;
            return;
        }
    }
    if (bind->arg_error) {
        reply->error = ducknng_strdup(bind->arg_error);
    } else if (strcmp(bind->method_name, "query_open") == 0) {
        if (ducknng_query_open_request_to_ipc(bind->sql, bind->batch_rows, bind->batch_bytes,
                &payload, &payload_len, &errmsg) != 0) {
            reply->error = errmsg ? errmsg : ducknng_strdup("ducknng: failed to encode query_open payload");
        } else {
            ducknng_session_call(reply, bind->ctx, bind->url, bind->method_name, payload,
                payload_len, bind->tls_config_id, 0, bind->expect_json_only);
            if (reply->ok && (reply->session_id == 0 || !reply->session_token ||
                    !reply->session_token[0])) {
                reply->ok = false;
                if (!reply->error) reply->error = ducknng_strdup("ducknng: query_open reply did not include session_id and session_token");
            }
        }
    } else {
        json = ducknng_session_request_json(bind->session_id, bind->session_token,
            bind->batch_rows, bind->batch_bytes);
        if (!json) {
            reply->error = ducknng_strdup("ducknng: failed to build session request payload");
        } else {
            ducknng_session_call(reply, bind->ctx, bind->url, bind->method_name,
                (const uint8_t *)json, strlen(json), bind->tls_config_id,
                bind->session_id, bind->expect_json_only);
        }
    }
    if (payload) duckdb_free(payload);
    if (json) duckdb_free(json);
    if (reply->error) reply->ok = false;
}

static void ducknng_session_emit_control(duckdb_data_chunk output, const ducknng_session_reply *reply) {
    bool *ok_data = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0));
    uint64_t *session_ids = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 2));
    uint64_t *idle_timeout_ms = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 7));
    ok_data[0] = reply->ok;
    if (reply->error) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 1), 0, reply->error, (idx_t)strlen(reply->error));
    else set_null(duckdb_data_chunk_get_vector(output, 1), 0);
    session_ids[0] = reply->session_id;
    if (reply->session_token) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 3), 0, reply->session_token, (idx_t)strlen(reply->session_token));
    else set_null(duckdb_data_chunk_get_vector(output, 3), 0);
    if (reply->state) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 4), 0, reply->state, (idx_t)strlen(reply->state));
    else set_null(duckdb_data_chunk_get_vector(output, 4), 0);
    if (reply->next_method) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 5), 0, reply->next_method, (idx_t)strlen(reply->next_method));
    else set_null(duckdb_data_chunk_get_vector(output, 5), 0);
    if (reply->control_json) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 6), 0, reply->control_json, (idx_t)strlen(reply->control_json));
    else set_null(duckdb_data_chunk_get_vector(output, 6), 0);
    if (reply->has_idle_timeout_ms) idle_timeout_ms[0] = reply->idle_timeout_ms;
    else set_null(duckdb_data_chunk_get_vector(output, 7), 0);
}

static void ducknng_session_control_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_session_init_data *init = (ducknng_session_init_data *)duckdb_function_get_init_data(info);
    const ducknng_session_args *bind = (const ducknng_session_args *)duckdb_function_get_bind_data(info);
    if (!init || !bind || init->emitted) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    ducknng_session_execute(bind, &init->reply);
    ducknng_session_emit_control(output, &init->reply);
    duckdb_data_chunk_set_size(output, 1);
    init->emitted = 1;
}

static void ducknng_fetch_query_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_session_init_data *init = (ducknng_session_init_data *)duckdb_function_get_init_data(info);
    const ducknng_session_args *bind = (const ducknng_session_args *)duckdb_function_get_bind_data(info);
    bool *end_of_stream;
    if (!init || !bind || init->emitted) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    ducknng_session_execute(bind, &init->reply);
    ducknng_session_emit_control(output, &init->reply);
    end_of_stream = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 9));
    if (init->reply.payload) assign_blob(duckdb_data_chunk_get_vector(output, 8), 0, init->reply.payload, init->reply.payload_len);
    else set_null(duckdb_data_chunk_get_vector(output, 8), 0);
    end_of_stream[0] = init->reply.end_of_stream;
    duckdb_data_chunk_set_size(output, 1);
    init->emitted = 1;
}

static void ducknng_fetch_query_table_bind(duckdb_bind_info info) {
    ducknng_fetch_query_table_bind_data *bind;
    duckdb_value url_val;
    duckdb_value session_val;
    duckdb_value token_val;
    duckdb_value batch_rows_val;
    duckdb_value batch_bytes_val;
    duckdb_value tls_val;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    uint64_t connection_id;
    if (ducknng_reject_table_inside_authorizer(info, ctx, &connection_id)) return;
    bind = (ducknng_fetch_query_table_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    bind->connection_id = connection_id;
    bind->ctx = ctx;
    url_val = duckdb_bind_get_parameter(info, 0);
    session_val = duckdb_bind_get_parameter(info, 1);
    token_val = duckdb_bind_get_parameter(info, 2);
    batch_rows_val = duckdb_bind_get_parameter(info, 3);
    batch_bytes_val = duckdb_bind_get_parameter(info, 4);
    tls_val = duckdb_bind_get_parameter(info, 5);
    bind->url = duckdb_get_varchar(url_val);
    bind->session_id = (uint64_t)duckdb_get_uint64(session_val);
    bind->session_token = duckdb_get_varchar(token_val);
    bind->batch_rows = (uint64_t)duckdb_get_uint64(batch_rows_val);
    bind->batch_bytes = (uint64_t)duckdb_get_uint64(batch_bytes_val);
    bind->tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&session_val);
    duckdb_destroy_value(&token_val);
    duckdb_destroy_value(&batch_rows_val);
    duckdb_destroy_value(&batch_bytes_val);
    duckdb_destroy_value(&tls_val);
    if (!bind->url || !bind->url[0] || bind->session_id == 0 ||
        !bind->session_token || !bind->session_token[0]) {
        destroy_fetch_query_table_bind_data(bind);
        duckdb_bind_set_error(info, "ducknng: fetch_query_table requires non-empty url, session_id, and session_token");
        return;
    }
    if (ducknng_sql_bind_declared_schema(info, &bind->declared) != 0) {
        destroy_fetch_query_table_bind_data(bind);
        return;
    }
    duckdb_bind_set_bind_data(info, bind, destroy_fetch_query_table_bind_data);
}

/* Each execution sends exactly one fetch on the caller-owned session. */
static void ducknng_fetch_query_table_init(duckdb_init_info info) {
    ducknng_fetch_query_table_bind_data *bind =
        (ducknng_fetch_query_table_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_fetch_query_table_init_data *init;
    ducknng_session_reply resp;
    char *json;
    char *errmsg = NULL;
    if (!bind) {
        duckdb_init_set_error(info, "ducknng: missing fetch_query_table bind data");
        return;
    }
    if (ducknng_reject_init_inside_authorizer(info, bind->ctx, bind->connection_id)) return;
    init = (ducknng_fetch_query_table_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(init, 0, sizeof(*init));
    json = ducknng_session_request_json(bind->session_id, bind->session_token,
        bind->batch_rows, bind->batch_bytes);
    if (!json) {
        destroy_fetch_query_table_init_data(init);
        duckdb_init_set_error(info, "ducknng: failed to build fetch_query_table request payload");
        return;
    }
    memset(&resp, 0, sizeof(resp));
    ducknng_session_call(&resp, bind->ctx, bind->url, "fetch",
        (const uint8_t *)json, strlen(json), bind->tls_config_id, bind->session_id, 0);
    duckdb_free(json);
    if (ducknng_fetch_query_table_take_batch(bind, &resp, init, &errmsg) != 0) {
        ducknng_session_reply_reset(&resp);
        destroy_fetch_query_table_init_data(init);
        duckdb_init_set_error(info, errmsg ? errmsg : "ducknng: fetch_query_table failed");
        if (errmsg) duckdb_free(errmsg);
        return;
    }
    ducknng_session_reply_reset(&resp);
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_fetch_query_table_init_data);
}

static void ducknng_fetch_query_table_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_fetch_query_table_init_data *init =
        (ducknng_fetch_query_table_init_data *)duckdb_function_get_init_data(info);
    char *errmsg = NULL;
    if (!init || init->offset >= init->row_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    if (ducknng_sql_arrow_scan_table(output, &init->schema, &init->array,
            init->row_count, &init->offset, &errmsg) != 0) {
        duckdb_function_set_error(info, errmsg ? errmsg : "ducknng: failed to decode fetch_query_table Arrow payload");
        if (errmsg) duckdb_free(errmsg);
    }
}


static int register_open_query_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_type param_types[5] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT,
        DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT};
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE(con, name, ctx, 5, param_types, ducknng_open_query_bind,
        ducknng_session_result_init, ducknng_session_control_scan);
}

static int register_session_control_table_named(duckdb_connection con, ducknng_sql_context *ctx,
    const char *name, duckdb_table_function_bind_t bind_fn) {
    duckdb_type param_types[4] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT,
        DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT};
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE(con, name, ctx, 4, param_types, bind_fn,
        ducknng_session_result_init, ducknng_session_control_scan);
}

static int register_fetch_query_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_type param_types[6] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT};
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE(con, name, ctx, 6, param_types, ducknng_fetch_query_bind,
        ducknng_session_result_init, ducknng_fetch_query_scan);
}

static int register_fetch_query_table_rows_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_type param_types[6] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT};
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE_WITH_SCHEMA(con, name, ctx, 6, param_types,
        ducknng_fetch_query_table_bind, ducknng_fetch_query_table_init,
        ducknng_fetch_query_table_scan);
}



int ducknng_register_sql_session(duckdb_connection con, ducknng_sql_context *ctx) {
    if (!register_open_query_table_named(con, ctx, "ducknng_open_query")) return 0;
    if (!register_fetch_query_table_named(con, ctx, "ducknng_fetch_query")) return 0;
    if (!register_fetch_query_table_rows_named(con, ctx, "ducknng_fetch_query_table")) return 0;
    if (!register_session_control_table_named(con, ctx, "ducknng_close_query", ducknng_close_query_bind)) return 0;
    if (!register_session_control_table_named(con, ctx, "ducknng_cancel_query", ducknng_cancel_query_bind)) return 0;
    return 1;
}
