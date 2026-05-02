#include "ducknng_sql_shared.h"
#include "ducknng_nng_compat.h"
#include "ducknng_runtime.h"
#include "ducknng_transport.h"
#include "ducknng_util.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

typedef struct {
    uint64_t socket_id;
    char *protocol;
    char *url;
    bool open;
    bool connected;
    bool listening;
    int32_t send_timeout_ms;
    int32_t recv_timeout_ms;
} ducknng_socket_row;

typedef struct {
    ducknng_socket_row *rows;
    idx_t row_count;
} ducknng_sockets_bind_data;

typedef struct {
    ducknng_sockets_bind_data *bind;
    idx_t offset;
} ducknng_sockets_init_data;

static int ducknng_lookup_tls_config_copy(ducknng_sql_context *ctx, uint64_t tls_config_id,
    uint64_t *out_id, char **out_source, ducknng_tls_opts *out_opts, char **errmsg) {
    size_t i;
    ducknng_tls_config *cfg = NULL;
    if (out_id) *out_id = 0;
    if (out_source) *out_source = NULL;
    if (out_opts) memset(out_opts, 0, sizeof(*out_opts));
    if (!ctx || !ctx->rt || tls_config_id == 0) return 0;
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
    if (ducknng_lookup_tls_config_copy(ctx, tls_config_id, NULL, NULL, &tls_copy, errmsg) != 0) return -1;
    tls_copy_valid = 1;
    if (out_opts) *out_opts = &tls_copy;
    return 0;
}

static nng_msg *ducknng_client_raw_request_message(const uint8_t *payload, size_t payload_len, char **errmsg) {
    nng_msg *req = NULL;
    int rv = nng_msg_alloc(&req, payload_len);
    if (rv != 0) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return NULL;
    }
    if (payload_len) memcpy(nng_msg_body(req), payload, payload_len);
    return req;
}

static int ducknng_socket_is_active(const ducknng_client_socket *sock) {
    return sock && sock->open && (sock->connected || sock->has_listener);
}

static int ducknng_socket_is_req_protocol(const ducknng_client_socket *sock) {
    return sock && sock->protocol && strcmp(sock->protocol, "req") == 0;
}

static void destroy_sockets_bind_data(void *ptr) {
    ducknng_sockets_bind_data *data = (ducknng_sockets_bind_data *)ptr;
    idx_t i;
    if (!data) return;
    for (i = 0; i < data->row_count; i++) {
        if (data->rows[i].protocol) duckdb_free(data->rows[i].protocol);
        if (data->rows[i].url) duckdb_free(data->rows[i].url);
    }
    if (data->rows) duckdb_free(data->rows);
    duckdb_free(data);
}

static void destroy_sockets_init_data(void *ptr) {
    ducknng_sockets_init_data *data = (ducknng_sockets_init_data *)ptr;
    if (data) duckdb_free(data);
}

static void ducknng_socket_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *protocol = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        ducknng_client_socket *sock;
        char *errmsg = NULL;
        int rv;
        if (!ctx || !ctx->rt || !protocol) {
            if (protocol) duckdb_free(protocol);
            duckdb_scalar_function_set_error(info, "ducknng: socket protocol is required");
            return;
        }
        sock = (ducknng_client_socket *)duckdb_malloc(sizeof(*sock));
        if (!sock) {
            duckdb_free(protocol);
            duckdb_scalar_function_set_error(info, "ducknng: out of memory allocating client socket");
            return;
        }
        memset(sock, 0, sizeof(*sock));
        sock->protocol = protocol;
        sock->send_timeout_ms = 5000;
        sock->recv_timeout_ms = 5000;
        if (ducknng_mutex_init(&sock->mu) != 0) {
            duckdb_free(sock->protocol);
            duckdb_free(sock);
            duckdb_scalar_function_set_error(info, "ducknng: failed to initialize client socket mutex");
            return;
        }
        sock->mu_initialized = 1;
        if (ducknng_cond_init(&sock->cv) != 0) {
            ducknng_mutex_destroy(&sock->mu);
            sock->mu_initialized = 0;
            duckdb_free(sock->protocol);
            duckdb_free(sock);
            duckdb_scalar_function_set_error(info, "ducknng: failed to initialize client socket condition variable");
            return;
        }
        sock->cv_initialized = 1;
        if (ducknng_socket_open_protocol(protocol, &sock->sock, &errmsg) != 0) {
            ducknng_client_socket_destroy(sock);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to open socket protocol");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        rv = ducknng_ctx_open(&sock->ctx, sock->sock);
        if (rv == 0) {
            sock->has_ctx = 1;
        } else if (rv != NNG_ENOTSUP) {
            ducknng_socket_close(sock->sock);
            sock->open = 0;
            ducknng_client_socket_destroy(sock);
            duckdb_scalar_function_set_error(info, ducknng_nng_strerror(rv));
            return;
        }
        sock->open = 1;
        if (ducknng_runtime_add_client_socket(ctx->rt, sock, &errmsg) != 0) {
            if (sock->has_ctx) {
                ducknng_ctx_close(sock->ctx);
                sock->has_ctx = 0;
            }
            if (sock->open) {
                ducknng_socket_close(sock->sock);
                sock->open = 0;
            }
            ducknng_client_socket_destroy(sock);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to register client socket");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        out[row] = sock->socket_id;
    }
}

static void ducknng_dial_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 2), row, 5000);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 3), row, 0);
        const ducknng_tls_opts *tls_opts = NULL;
        ducknng_client_socket *sock;
        char *errmsg = NULL;
        int rv;
        out[row] = false;
        if (!ctx || !ctx->rt || socket_id == 0 || !url) {
            if (url) duckdb_free(url);
            duckdb_scalar_function_set_error(info, "ducknng: socket id and URL are required");
            return;
        }
        if (ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, &errmsg) != 0) {
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: tls config not found");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (ducknng_socket_validate_client_url(url, tls_opts, &errmsg) != 0) {
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: invalid transport URL");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        sock = ducknng_runtime_acquire_client_socket(ctx->rt, socket_id);
        if (!sock) {
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, "ducknng: client socket not found");
            return;
        }
        ducknng_mutex_lock(&sock->mu);
        if (!sock->open) {
            ducknng_mutex_unlock(&sock->mu);
            ducknng_runtime_release_client_socket(sock);
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, "ducknng: client socket not found");
            return;
        }
        if (sock->connected) {
            ducknng_mutex_unlock(&sock->mu);
            ducknng_runtime_release_client_socket(sock);
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, "ducknng: socket is already dialed");
            return;
        }
        rv = ducknng_socket_set_timeout_ms(sock->sock, timeout_ms, timeout_ms);
        if (rv == 0) rv = ducknng_socket_apply_tls(sock->sock, url, tls_opts);
        if (rv == 0) rv = ducknng_socket_dial(sock->sock, url);
        if (rv == 0) {
            if (sock->url) duckdb_free(sock->url);
            sock->url = url;
            sock->connected = 1;
            sock->send_timeout_ms = timeout_ms;
            sock->recv_timeout_ms = timeout_ms;
            out[row] = true;
            url = NULL;
        }
        ducknng_mutex_unlock(&sock->mu);
        ducknng_runtime_release_client_socket(sock);
        if (url) duckdb_free(url);
        if (rv != 0) {
            duckdb_scalar_function_set_error(info, ducknng_nng_strerror(rv));
            return;
        }
    }
}

static void ducknng_listen_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        uint64_t recv_max_bytes = arg_u64(duckdb_data_chunk_get_vector(input, 2), row, 0);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 3), row, 0);
        const ducknng_tls_opts *tls_opts = NULL;
        ducknng_client_socket *sock;
        char *errmsg = NULL;
        nng_listener lst;
        char *resolved_url = NULL;
        int rv;
        out[row] = false;
        memset(&lst, 0, sizeof(lst));
        if (!ctx || !ctx->rt || socket_id == 0 || !url) {
            if (url) duckdb_free(url);
            duckdb_scalar_function_set_error(info, "ducknng: socket id and URL are required");
            return;
        }
        if (ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, &errmsg) != 0) {
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: tls config not found");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (ducknng_listener_validate_startup_url(url, tls_opts, &errmsg) != 0) {
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: invalid listen URL");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        sock = ducknng_runtime_acquire_client_socket(ctx->rt, socket_id);
        if (!sock) {
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, "ducknng: client socket not found");
            return;
        }
        ducknng_mutex_lock(&sock->mu);
        if (!sock->open) {
            ducknng_mutex_unlock(&sock->mu);
            ducknng_runtime_release_client_socket(sock);
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, "ducknng: client socket not found");
            return;
        }
        if (sock->has_listener) {
            ducknng_mutex_unlock(&sock->mu);
            ducknng_runtime_release_client_socket(sock);
            duckdb_free(url);
            duckdb_scalar_function_set_error(info, "ducknng: socket is already listening");
            return;
        }
        rv = ducknng_listener_create(&lst, sock->sock, url);
        if (rv == 0 && recv_max_bytes > 0) rv = ducknng_listener_set_recvmaxsz(lst, (size_t)recv_max_bytes);
        if (rv == 0) rv = ducknng_listener_apply_tls(lst, tls_opts);
        if (rv == 0) rv = ducknng_listener_start(lst);
        if (rv == 0) {
            resolved_url = ducknng_listener_resolve_url(lst, url);
            if (sock->listen_url) duckdb_free(sock->listen_url);
            sock->listen_url = resolved_url ? resolved_url : url;
            if (resolved_url) duckdb_free(url);
            url = NULL;
            sock->listener = lst;
            sock->has_listener = 1;
            out[row] = true;
        }
        ducknng_mutex_unlock(&sock->mu);
        ducknng_runtime_release_client_socket(sock);
        if (url) duckdb_free(url);
        if (rv != 0) {
            duckdb_scalar_function_set_error(info, ducknng_nng_strerror(rv));
            if (errmsg) duckdb_free(errmsg);
            return;
        }
    }
}

static void ducknng_close_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        ducknng_client_socket *sock;
        if (!ctx || !ctx->rt || socket_id == 0) {
            duckdb_scalar_function_set_error(info, "ducknng: socket id is required");
            return;
        }
        sock = ducknng_runtime_remove_client_socket(ctx->rt, socket_id);
        if (!sock) {
            duckdb_scalar_function_set_error(info, "ducknng: client socket not found");
            return;
        }
        ducknng_client_socket_destroy(sock);
        out[row] = true;
    }
}

static void ducknng_send_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        idx_t payload_len = 0;
        uint8_t *payload = arg_blob_dup(duckdb_data_chunk_get_vector(input, 1), row, &payload_len);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 2), row, 5000);
        ducknng_client_socket *sock;
        nng_msg *msg = NULL;
        char *errmsg = NULL;
        int rv;
        out[row] = false;
        if (!ctx || !ctx->rt || socket_id == 0 || (!payload && payload_len > 0)) {
            if (payload) duckdb_free(payload);
            duckdb_scalar_function_set_error(info, "ducknng: socket id, payload, and timeout are required");
            return;
        }
        sock = ducknng_runtime_acquire_client_socket(ctx->rt, socket_id);
        if (!ducknng_socket_is_active(sock)) {
            if (payload) duckdb_free(payload);
            if (sock) ducknng_runtime_release_client_socket(sock);
            duckdb_scalar_function_set_error(info, "ducknng: active client socket not found");
            return;
        }
        msg = ducknng_client_raw_request_message(payload, (size_t)payload_len, &errmsg);
        if (payload) duckdb_free(payload);
        if (!msg) {
            ducknng_runtime_release_client_socket(sock);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to build socket send message");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        ducknng_mutex_lock(&sock->mu);
        rv = ducknng_socket_set_timeout_ms(sock->sock, timeout_ms, sock->recv_timeout_ms);
        if (rv == 0) rv = ducknng_socket_send(sock->sock, msg);
        if (rv == 0) {
            sock->send_timeout_ms = timeout_ms;
            out[row] = true;
        }
        ducknng_mutex_unlock(&sock->mu);
        ducknng_runtime_release_client_socket(sock);
        if (rv != 0) {
            duckdb_scalar_function_set_error(info, ducknng_nng_strerror(rv));
            return;
        }
    }
}

static void ducknng_recv_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 1), row, 5000);
        ducknng_client_socket *sock;
        nng_msg *msg = NULL;
        int rv;
        if (!ctx || !ctx->rt || socket_id == 0) {
            duckdb_scalar_function_set_error(info, "ducknng: socket id is required");
            return;
        }
        sock = ducknng_runtime_acquire_client_socket(ctx->rt, socket_id);
        if (!ducknng_socket_is_active(sock)) {
            if (sock) ducknng_runtime_release_client_socket(sock);
            duckdb_scalar_function_set_error(info, "ducknng: active client socket not found");
            return;
        }
        ducknng_mutex_lock(&sock->mu);
        rv = ducknng_socket_set_timeout_ms(sock->sock, sock->send_timeout_ms, timeout_ms);
        if (rv == 0) rv = ducknng_socket_recv(sock->sock, &msg);
        if (rv == 0) sock->recv_timeout_ms = timeout_ms;
        ducknng_mutex_unlock(&sock->mu);
        ducknng_runtime_release_client_socket(sock);
        if (rv != 0) {
            duckdb_scalar_function_set_error(info, ducknng_nng_strerror(rv));
            return;
        }
        assign_blob(output, row, (const uint8_t *)nng_msg_body(msg), (idx_t)nng_msg_len(msg));
        nng_msg_free(msg);
    }
}

static void ducknng_subscribe_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        idx_t topic_len = 0;
        uint8_t *topic = arg_blob_dup(duckdb_data_chunk_get_vector(input, 1), row, &topic_len);
        ducknng_client_socket *sock;
        int rv;
        out[row] = false;
        if (!ctx || !ctx->rt || socket_id == 0 || (!topic && topic_len > 0)) {
            if (topic) duckdb_free(topic);
            duckdb_scalar_function_set_error(info, "ducknng: subscribe_socket requires socket id and topic blob");
            return;
        }
        sock = ducknng_runtime_acquire_client_socket(ctx->rt, socket_id);
        if (!sock || !sock->open || !sock->protocol || strcmp(sock->protocol, "sub") != 0) {
            if (topic) duckdb_free(topic);
            if (sock) ducknng_runtime_release_client_socket(sock);
            duckdb_scalar_function_set_error(info, "ducknng: sub socket not found");
            return;
        }
        ducknng_mutex_lock(&sock->mu);
        rv = ducknng_socket_subscribe(sock->sock, topic, (size_t)topic_len);
        ducknng_mutex_unlock(&sock->mu);
        if (topic) duckdb_free(topic);
        ducknng_runtime_release_client_socket(sock);
        if (rv != 0) {
            duckdb_scalar_function_set_error(info, ducknng_nng_strerror(rv));
            return;
        }
        out[row] = true;
    }
}

static void ducknng_unsubscribe_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        idx_t topic_len = 0;
        uint8_t *topic = arg_blob_dup(duckdb_data_chunk_get_vector(input, 1), row, &topic_len);
        ducknng_client_socket *sock;
        int rv;
        out[row] = false;
        if (!ctx || !ctx->rt || socket_id == 0 || (!topic && topic_len > 0)) {
            if (topic) duckdb_free(topic);
            duckdb_scalar_function_set_error(info, "ducknng: unsubscribe_socket requires socket id and topic blob");
            return;
        }
        sock = ducknng_runtime_acquire_client_socket(ctx->rt, socket_id);
        if (!sock || !sock->open || !sock->protocol || strcmp(sock->protocol, "sub") != 0) {
            if (topic) duckdb_free(topic);
            if (sock) ducknng_runtime_release_client_socket(sock);
            duckdb_scalar_function_set_error(info, "ducknng: sub socket not found");
            return;
        }
        ducknng_mutex_lock(&sock->mu);
        rv = ducknng_socket_unsubscribe(sock->sock, topic, (size_t)topic_len);
        ducknng_mutex_unlock(&sock->mu);
        if (topic) duckdb_free(topic);
        ducknng_runtime_release_client_socket(sock);
        if (rv != 0) {
            duckdb_scalar_function_set_error(info, ducknng_nng_strerror(rv));
            return;
        }
        out[row] = true;
    }
}


static void ducknng_sockets_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    ducknng_sockets_bind_data *bind;
    duckdb_logical_type type;
    size_t i;
    if (!ctx || !ctx->rt) {
        duckdb_bind_set_error(info, "ducknng: missing runtime");
        return;
    }
    bind = (ducknng_sockets_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    ducknng_mutex_lock(&ctx->rt->mu);
    bind->row_count = (idx_t)ctx->rt->client_socket_count;
    if (bind->row_count > 0) {
        bind->rows = (ducknng_socket_row *)duckdb_malloc(sizeof(*bind->rows) * (size_t)bind->row_count);
        if (!bind->rows) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(bind);
            duckdb_bind_set_error(info, "ducknng: out of memory");
            return;
        }
        memset(bind->rows, 0, sizeof(*bind->rows) * (size_t)bind->row_count);
        for (i = 0; i < (size_t)bind->row_count; i++) {
            ducknng_client_socket *sock = ctx->rt->client_sockets[i];
            bind->rows[i].socket_id = sock ? sock->socket_id : 0;
            bind->rows[i].protocol = sock && sock->protocol ? ducknng_strdup(sock->protocol) : NULL;
            bind->rows[i].url = sock ? ducknng_strdup(sock->url ? sock->url : sock->listen_url) : NULL;
            bind->rows[i].open = sock ? (bool)sock->open : false;
            bind->rows[i].connected = sock ? (bool)sock->connected : false;
            bind->rows[i].listening = sock ? (bool)sock->has_listener : false;
            bind->rows[i].send_timeout_ms = sock ? sock->send_timeout_ms : 0;
            bind->rows[i].recv_timeout_ms = sock ? sock->recv_timeout_ms : 0;
        }
    }
    ducknng_mutex_unlock(&ctx->rt->mu);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "socket_id", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "protocol", type);
    duckdb_bind_add_result_column(info, "url", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "open", type);
    duckdb_bind_add_result_column(info, "connected", type);
    duckdb_bind_add_result_column(info, "listening", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_bind_add_result_column(info, "send_timeout_ms", type);
    duckdb_bind_add_result_column(info, "recv_timeout_ms", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_sockets_bind_data);
    duckdb_bind_set_cardinality(info, bind->row_count, true);
}

static void ducknng_sockets_init(duckdb_init_info info) {
    ducknng_sockets_bind_data *bind = (ducknng_sockets_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_sockets_init_data *init = (ducknng_sockets_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->bind = bind;
    init->offset = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_sockets_init_data);
}

static void ducknng_sockets_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_sockets_init_data *init = (ducknng_sockets_init_data *)duckdb_function_get_init_data(info);
    ducknng_sockets_bind_data *bind;
    idx_t remaining;
    idx_t chunk_size;
    idx_t i;
    duckdb_vector vec_socket_id;
    duckdb_vector vec_protocol;
    duckdb_vector vec_url;
    duckdb_vector vec_open;
    duckdb_vector vec_connected;
    duckdb_vector vec_listening;
    duckdb_vector vec_send_timeout_ms;
    duckdb_vector vec_recv_timeout_ms;
    uint64_t *socket_ids;
    bool *open;
    bool *connected;
    bool *listening;
    int32_t *send_timeout_ms;
    int32_t *recv_timeout_ms;
    if (!init || !init->bind || init->offset >= init->bind->row_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    bind = init->bind;
    remaining = bind->row_count - init->offset;
    chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;
    vec_socket_id = duckdb_data_chunk_get_vector(output, 0);
    vec_protocol = duckdb_data_chunk_get_vector(output, 1);
    vec_url = duckdb_data_chunk_get_vector(output, 2);
    vec_open = duckdb_data_chunk_get_vector(output, 3);
    vec_connected = duckdb_data_chunk_get_vector(output, 4);
    vec_listening = duckdb_data_chunk_get_vector(output, 5);
    vec_send_timeout_ms = duckdb_data_chunk_get_vector(output, 6);
    vec_recv_timeout_ms = duckdb_data_chunk_get_vector(output, 7);
    socket_ids = (uint64_t *)duckdb_vector_get_data(vec_socket_id);
    open = (bool *)duckdb_vector_get_data(vec_open);
    connected = (bool *)duckdb_vector_get_data(vec_connected);
    listening = (bool *)duckdb_vector_get_data(vec_listening);
    send_timeout_ms = (int32_t *)duckdb_vector_get_data(vec_send_timeout_ms);
    recv_timeout_ms = (int32_t *)duckdb_vector_get_data(vec_recv_timeout_ms);
    for (i = 0; i < chunk_size; i++) {
        ducknng_socket_row *row = &bind->rows[init->offset + i];
        socket_ids[i] = row->socket_id;
        open[i] = row->open;
        connected[i] = row->connected;
        listening[i] = row->listening;
        send_timeout_ms[i] = row->send_timeout_ms;
        recv_timeout_ms[i] = row->recv_timeout_ms;
        if (row->protocol) duckdb_vector_assign_string_element(vec_protocol, i, row->protocol);
        else set_null(vec_protocol, i);
        if (row->url) duckdb_vector_assign_string_element(vec_url, i, row->url);
        else set_null(vec_url, i);
    }
    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
}


static int register_named_sockets_table(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE(con, name, ctx, 0, NULL, ducknng_sockets_bind,
        ducknng_sockets_init, ducknng_sockets_scan);
}

int ducknng_register_sql_socket(duckdb_connection con, ducknng_sql_context *ctx) {
    duckdb_type socket_types[1] = {DUCKDB_TYPE_VARCHAR};
    duckdb_type close_types[1] = {DUCKDB_TYPE_UBIGINT};
    duckdb_type dial_types[4] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_UBIGINT};
    duckdb_type listen_types[4] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT};
    duckdb_type request_socket_types[3] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_BLOB, DUCKDB_TYPE_INTEGER};
    duckdb_type recv_socket_types[2] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_INTEGER};
    duckdb_type subscribe_types[2] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_BLOB};
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_open_socket", 1, ducknng_socket_scalar, ctx, socket_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_dial_socket", 4, ducknng_dial_scalar, ctx, dial_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_listen_socket", 4, ducknng_listen_scalar, ctx, listen_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_close_socket", 1, ducknng_close_scalar, ctx, close_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_send_socket_raw", 3, ducknng_send_scalar, ctx, request_socket_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_recv_socket_raw", 2, ducknng_recv_scalar, ctx, recv_socket_types, DUCKDB_TYPE_BLOB)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_subscribe_socket", 2, ducknng_subscribe_scalar, ctx, subscribe_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_unsubscribe_socket", 2, ducknng_unsubscribe_scalar, ctx, subscribe_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_named_sockets_table(con, ctx, "ducknng_list_sockets")) return 0;
    return 1;
}
