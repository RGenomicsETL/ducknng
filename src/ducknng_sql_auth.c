#include "ducknng_sql_shared.h"
#include "ducknng_runtime.h"
#include "ducknng_service.h"
#include "ducknng_transport.h"
#include "ducknng_util.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

/* Bind data shared by the execution-context tables: only the id of the
 * binding connection. Every execution resolves the runtime's current record
 * for that connection at init, so a reused plan never replays the context of
 * an earlier request. */
typedef struct {
    uint64_t connection_id;
} ducknng_sql_context_table_bind_data;

/* Per-execution snapshot of the authorizer callback context. row_count is 0
 * when the binding connection is not inside an authorizer callback. */
typedef struct {
    idx_t row_count;
    idx_t emitted;
    char *phase;
    char *service_name;
    char *transport_family;
    char *scheme;
    char *listen;
    char *remote_addr;
    char *remote_ip;
    int32_t remote_port;
    bool tls_verified;
    char *peer_identity;
    bool peer_allowlist_active;
    bool ip_allowlist_active;
    bool sql_authorizer_active;
    char *http_method;
    char *http_path;
    char *content_type;
    uint64_t body_bytes;
    char *rpc_method;
    char *rpc_type;
    uint64_t payload_bytes;
} ducknng_auth_context_snapshot;

static char *ducknng_dup_bytes_local(const uint8_t *data, size_t len) {
    char *out = (char *)duckdb_malloc(len + 1);
    if (!out) return NULL;
    if (len) memcpy(out, data, len);
    out[len] = '\0';
    return out;
}

/* Copies src into *out; NULL stays NULL. Returns -1 only on allocation failure. */
static int ducknng_sql_auth_dup(const char *src, char **out) {
    *out = NULL;
    if (!src) return 0;
    *out = ducknng_strdup(src);
    return *out ? 0 : -1;
}

/* Binds the fixed columns' owner: captures the binding connection id. */
static ducknng_sql_context_table_bind_data *ducknng_sql_context_table_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    ducknng_sql_context_table_bind_data *bind;
    uint64_t connection_id;
    if (!ctx || !ctx->rt) {
        duckdb_bind_set_error(info, "ducknng: missing runtime");
        return NULL;
    }
    if (ducknng_sql_table_bind_connection_id(info, &connection_id) != 0) {
        duckdb_bind_set_error(info, "ducknng: missing DuckDB connection id for an execution-context table");
        return NULL;
    }
    bind = (ducknng_sql_context_table_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return NULL;
    }
    bind->connection_id = connection_id;
    duckdb_bind_set_bind_data(info, bind, duckdb_free);
    return bind;
}

static void destroy_auth_context_snapshot(void *ptr) {
    ducknng_auth_context_snapshot *data = (ducknng_auth_context_snapshot *)ptr;
    if (!data) return;
    if (data->phase) duckdb_free(data->phase);
    if (data->service_name) duckdb_free(data->service_name);
    if (data->transport_family) duckdb_free(data->transport_family);
    if (data->scheme) duckdb_free(data->scheme);
    if (data->listen) duckdb_free(data->listen);
    if (data->remote_addr) duckdb_free(data->remote_addr);
    if (data->remote_ip) duckdb_free(data->remote_ip);
    if (data->peer_identity) duckdb_free(data->peer_identity);
    if (data->http_method) duckdb_free(data->http_method);
    if (data->http_path) duckdb_free(data->http_path);
    if (data->content_type) duckdb_free(data->content_type);
    if (data->rpc_method) duckdb_free(data->rpc_method);
    if (data->rpc_type) duckdb_free(data->rpc_type);
    duckdb_free(data);
}

static char *ducknng_sql_frame_method_dup(const ducknng_frame *frame) {
    if (!frame) return NULL;
    if (frame->type == DUCKNNG_RPC_MANIFEST) return ducknng_strdup("manifest");
    if (frame->type == DUCKNNG_RPC_CALL && frame->name && frame->name_len > 0) {
        return ducknng_dup_bytes_local(frame->name, frame->name_len);
    }
    return NULL;
}

static const char *ducknng_sql_frame_type_name(const ducknng_frame *frame) {
    if (!frame) return NULL;
    switch (frame->type) {
    case DUCKNNG_RPC_MANIFEST: return "manifest";
    case DUCKNNG_RPC_CALL: return "call";
    case DUCKNNG_RPC_RESULT: return "result";
    case DUCKNNG_RPC_ERROR: return "error";
    case DUCKNNG_RPC_EVENT: return "event";
    default: return "unknown";
    }
}

/* Copies the borrowed authorizer context into snap. Returns -1 on allocation
 * failure; the caller destroys the partial snapshot. */
static int ducknng_auth_context_snapshot_fill(ducknng_auth_context_snapshot *snap,
    const ducknng_authorizer_context *auth_ctx) {
    ducknng_transport_family family = auth_ctx->transport_family;
    ducknng_transport_scheme scheme = auth_ctx->scheme;
    const char *listen = ducknng_service_resolved_listen(auth_ctx->svc);
    const ducknng_frame *frame = auth_ctx->frame;
    int has_rpc_method;
    ducknng_transport_url parsed;
    char *parse_err = NULL;
    ducknng_transport_url_init(&parsed);
    if ((family == DUCKNNG_TRANSPORT_FAMILY_UNKNOWN || scheme == DUCKNNG_TRANSPORT_SCHEME_UNKNOWN) &&
        auth_ctx->svc->listen_url && ducknng_transport_url_parse(auth_ctx->svc->listen_url, &parsed, &parse_err) == 0) {
        if (family == DUCKNNG_TRANSPORT_FAMILY_UNKNOWN) family = parsed.family;
        if (scheme == DUCKNNG_TRANSPORT_SCHEME_UNKNOWN) scheme = parsed.scheme;
    }
    if (parse_err) duckdb_free(parse_err);
    snap->row_count = 1;
    if (ducknng_sql_auth_dup(auth_ctx->phase ? auth_ctx->phase : "rpc_request", &snap->phase) != 0 ||
        ducknng_sql_auth_dup(auth_ctx->svc->name, &snap->service_name) != 0 ||
        ducknng_sql_auth_dup(ducknng_transport_family_name(family), &snap->transport_family) != 0 ||
        ducknng_sql_auth_dup(ducknng_transport_scheme_name(scheme), &snap->scheme) != 0 ||
        ducknng_sql_auth_dup(listen, &snap->listen) != 0 ||
        ducknng_sql_auth_dup(auth_ctx->caller_identity, &snap->peer_identity) != 0 ||
        ducknng_sql_auth_dup(auth_ctx->http_method, &snap->http_method) != 0 ||
        ducknng_sql_auth_dup(auth_ctx->http_path, &snap->http_path) != 0 ||
        ducknng_sql_auth_dup(auth_ctx->content_type, &snap->content_type) != 0 ||
        ducknng_sql_auth_dup(ducknng_sql_frame_type_name(auth_ctx->frame), &snap->rpc_type) != 0)
        return -1;
    snap->remote_addr = ducknng_sql_sockaddr_addr_dup(auth_ctx->remote_addr, &snap->remote_ip, &snap->remote_port);
    has_rpc_method = frame && (frame->type == DUCKNNG_RPC_MANIFEST ||
        (frame->type == DUCKNNG_RPC_CALL && frame->name && frame->name_len > 0));
    snap->rpc_method = ducknng_sql_frame_method_dup(frame);
    if (has_rpc_method && !snap->rpc_method) return -1;
    snap->tls_verified = auth_ctx->caller_identity && auth_ctx->caller_identity[0];
    snap->peer_allowlist_active = ducknng_service_peer_allowlist_active(auth_ctx->svc) ? true : false;
    snap->ip_allowlist_active = ducknng_service_ip_allowlist_active(auth_ctx->svc) ? true : false;
    snap->sql_authorizer_active = ducknng_service_authorizer_active(auth_ctx->svc) ? true : false;
    snap->body_bytes = auth_ctx->body_bytes;
    snap->payload_bytes = auth_ctx->frame ? auth_ctx->frame->payload_len : 0;
    return 0;
}

static void ducknng_auth_context_bind(duckdb_bind_info info) {
    duckdb_logical_type type;
    if (!ducknng_sql_context_table_bind(info)) return;

    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "phase", type);
    duckdb_bind_add_result_column(info, "service_name", type);
    duckdb_bind_add_result_column(info, "transport_family", type);
    duckdb_bind_add_result_column(info, "scheme", type);
    duckdb_bind_add_result_column(info, "listen", type);
    duckdb_bind_add_result_column(info, "remote_addr", type);
    duckdb_bind_add_result_column(info, "remote_ip", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_bind_add_result_column(info, "remote_port", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "tls_verified", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "peer_identity", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "peer_allowlist_active", type);
    duckdb_bind_add_result_column(info, "ip_allowlist_active", type);
    duckdb_bind_add_result_column(info, "sql_authorizer_active", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "http_method", type);
    duckdb_bind_add_result_column(info, "http_path", type);
    duckdb_bind_add_result_column(info, "content_type", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "body_bytes", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "rpc_method", type);
    duckdb_bind_add_result_column(info, "rpc_type", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "payload_bytes", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_cardinality(info, 1, false);
}

static void ducknng_auth_context_init(duckdb_init_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_init_get_extra_info(info);
    const ducknng_sql_context_table_bind_data *bind =
        (const ducknng_sql_context_table_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_connection_context_view view;
    ducknng_auth_context_snapshot *snap;
    if (!ctx || !ctx->rt || !bind) {
        duckdb_init_set_error(info, "ducknng: missing auth context bind data");
        return;
    }
    snap = (ducknng_auth_context_snapshot *)duckdb_malloc(sizeof(*snap));
    if (!snap) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(snap, 0, sizeof(*snap));
    /* The view's auth_ctx stays valid while this connection's authorizer
     * query runs, which includes this init. */
    if (ducknng_runtime_connection_context_view_get(ctx->rt, bind->connection_id, &view) &&
        view.scope == DUCKNNG_CONNECTION_SCOPE_AUTHORIZER && view.auth_ctx && view.auth_ctx->svc &&
        ducknng_auth_context_snapshot_fill(snap, view.auth_ctx) != 0) {
        destroy_auth_context_snapshot(snap);
        duckdb_init_set_error(info, "ducknng: out of memory copying auth context");
        return;
    }
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, snap, destroy_auth_context_snapshot);
}

static void ducknng_auth_context_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_auth_context_snapshot *snap =
        (ducknng_auth_context_snapshot *)duckdb_function_get_init_data(info);
    if (!snap || snap->emitted || snap->row_count == 0) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
#define ASSIGN_AUTH_STRING(IDX, VALUE) do { \
        if ((VALUE)) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, (IDX)), 0, (VALUE), (idx_t)strlen((VALUE))); \
        else set_null(duckdb_data_chunk_get_vector(output, (IDX)), 0); \
    } while (0)
    ASSIGN_AUTH_STRING(0, snap->phase);
    ASSIGN_AUTH_STRING(1, snap->service_name);
    ASSIGN_AUTH_STRING(2, snap->transport_family);
    ASSIGN_AUTH_STRING(3, snap->scheme);
    ASSIGN_AUTH_STRING(4, snap->listen);
    ASSIGN_AUTH_STRING(5, snap->remote_addr);
    ASSIGN_AUTH_STRING(6, snap->remote_ip);
    ((int32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 7)))[0] = snap->remote_port;
    ((bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 8)))[0] = snap->tls_verified;
    ASSIGN_AUTH_STRING(9, snap->peer_identity);
    ((bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 10)))[0] = snap->peer_allowlist_active;
    ((bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 11)))[0] = snap->ip_allowlist_active;
    ((bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 12)))[0] = snap->sql_authorizer_active;
    ASSIGN_AUTH_STRING(13, snap->http_method);
    ASSIGN_AUTH_STRING(14, snap->http_path);
    ASSIGN_AUTH_STRING(15, snap->content_type);
    ((uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 16)))[0] = snap->body_bytes;
    ASSIGN_AUTH_STRING(17, snap->rpc_method);
    ASSIGN_AUTH_STRING(18, snap->rpc_type);
    ((uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 19)))[0] = snap->payload_bytes;
#undef ASSIGN_AUTH_STRING
    snap->emitted = 1;
    duckdb_data_chunk_set_size(output, 1);
}

static void ducknng_set_service_authorizer_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    for (row = 0; row < count; row++) {
        char *name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *authorizer_sql = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        ducknng_service *svc = NULL;
        char *errmsg = NULL;
        size_t i;
        if (!ctx || !ctx->rt || !name || !name[0]) {
            if (name) duckdb_free(name);
            if (authorizer_sql) duckdb_free(authorizer_sql);
            duckdb_scalar_function_set_error(info, "ducknng: service name is required");
            return;
        }
        ducknng_mutex_lock(&ctx->rt->mu);
        for (i = 0; i < ctx->rt->service_count; i++) {
            if (ctx->rt->services[i] && ctx->rt->services[i]->name && strcmp(ctx->rt->services[i]->name, name) == 0) {
                svc = ctx->rt->services[i];
                break;
            }
        }
        if (!svc) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(name);
            if (authorizer_sql) duckdb_free(authorizer_sql);
            duckdb_scalar_function_set_error(info, "ducknng: service not found");
            return;
        }
        if (ducknng_service_set_authorizer(svc, authorizer_sql, &errmsg) != 0) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(name);
            if (authorizer_sql) duckdb_free(authorizer_sql);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to set service SQL authorizer");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
        duckdb_free(name);
        if (authorizer_sql) duckdb_free(authorizer_sql);
        out[row] = true;
    }
}
/* ducknng_request_subject(): the verified caller of the request whose SQL is
 * running, as the dispatcher recorded it in the connection's execution
 * context. A session connection reports its opener. Each execution reads the
 * record for the binding connection at init; SQL that runs for no dispatched
 * caller (no record, an authorizer callback, background SQL) returns no rows,
 * while an anonymous request returns one row with authenticated = false. */
typedef struct {
    idx_t row_count;
    idx_t emitted;
    char *peer_identity;
    char *principal;
    char *subject;
    char *claims_json;
} ducknng_request_subject_snapshot;

static void destroy_request_subject_snapshot(void *ptr) {
    ducknng_request_subject_snapshot *data = (ducknng_request_subject_snapshot *)ptr;
    if (!data) return;
    if (data->peer_identity) duckdb_free(data->peer_identity);
    if (data->principal) duckdb_free(data->principal);
    if (data->subject) duckdb_free(data->subject);
    if (data->claims_json) duckdb_free(data->claims_json);
    duckdb_free(data);
}

static void ducknng_request_subject_bind(duckdb_bind_info info) {
    duckdb_logical_type type;
    if (!ducknng_sql_context_table_bind(info)) return;
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "peer_identity", type);
    duckdb_bind_add_result_column(info, "principal", type);
    duckdb_bind_add_result_column(info, "subject", type);
    duckdb_bind_add_result_column(info, "claims_json", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "authenticated", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_cardinality(info, 1, false);
}

static void ducknng_request_subject_init(duckdb_init_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_init_get_extra_info(info);
    const ducknng_sql_context_table_bind_data *bind =
        (const ducknng_sql_context_table_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_connection_context_view view;
    ducknng_request_subject_snapshot *snap;
    if (!ctx || !ctx->rt || !bind) {
        duckdb_init_set_error(info, "ducknng: missing request subject bind data");
        return;
    }
    snap = (ducknng_request_subject_snapshot *)duckdb_malloc(sizeof(*snap));
    if (!snap) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(snap, 0, sizeof(*snap));
    /* The view's strings belong to the record, which stays installed while
     * this connection's scope runs the query that includes this init. */
    if (ducknng_runtime_connection_context_view_get(ctx->rt, bind->connection_id, &view) &&
            view.has_subject) {
        snap->row_count = 1;
        if (ducknng_sql_auth_dup(view.peer_identity, &snap->peer_identity) != 0 ||
            ducknng_sql_auth_dup(view.principal, &snap->principal) != 0 ||
            ducknng_sql_auth_dup(view.subject, &snap->subject) != 0 ||
            ducknng_sql_auth_dup(view.claims_json, &snap->claims_json) != 0) {
            destroy_request_subject_snapshot(snap);
            duckdb_init_set_error(info, "ducknng: out of memory copying request subject");
            return;
        }
    }
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, snap, destroy_request_subject_snapshot);
}

static void ducknng_request_subject_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_request_subject_snapshot *snap =
        (ducknng_request_subject_snapshot *)duckdb_function_get_init_data(info);
    if (!snap || snap->emitted || snap->row_count == 0) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
#define ASSIGN_SUBJECT_STRING(IDX, VALUE) do { \
        if ((VALUE)) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, (IDX)), 0, (VALUE), (idx_t)strlen((VALUE))); \
        else set_null(duckdb_data_chunk_get_vector(output, (IDX)), 0); \
    } while (0)
    ASSIGN_SUBJECT_STRING(0, snap->peer_identity);
    ASSIGN_SUBJECT_STRING(1, snap->principal);
    ASSIGN_SUBJECT_STRING(2, snap->subject);
    ASSIGN_SUBJECT_STRING(3, snap->claims_json);
#undef ASSIGN_SUBJECT_STRING
    ((bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 4)))[0] =
        snap->peer_identity != NULL;
    snap->emitted = 1;
    duckdb_data_chunk_set_size(output, 1);
}

static int register_auth_context_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE(con, name, ctx, 0, NULL, ducknng_auth_context_bind,
        ducknng_auth_context_init, ducknng_auth_context_scan);
}

int ducknng_register_sql_auth(duckdb_connection con, ducknng_sql_context *ctx) {
    duckdb_type service_authorizer_types[2] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR};
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_set_service_authorizer", 2,
            ducknng_set_service_authorizer_scalar, ctx, service_authorizer_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_auth_context_table_named(con, ctx, "ducknng_auth_context")) return 0;
    if (!DUCKNNG_REGISTER_TABLE(con, "ducknng_request_subject", ctx, 0, NULL,
            ducknng_request_subject_bind, ducknng_request_subject_init,
            ducknng_request_subject_scan)) return 0;
    return 1;
}
