#include "ducknng_sql_shared.h"
#include "ducknng_service.h"
#include "ducknng_util.h"
#include <stdio.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

typedef struct {
    uint64_t service_id;
    char *service_name;
    uint64_t route_id;
    char *method;
    char *match_kind;
    char *path;
    uint64_t request_max_bytes;
    char *handler_sql;
} ducknng_http_route_row;

typedef struct {
    ducknng_http_route_row *rows;
    idx_t row_count;
    idx_t row_cap;
} ducknng_http_routes_bind_data;

typedef struct {
    ducknng_http_routes_bind_data *bind;
    idx_t offset;
} ducknng_http_routes_init_data;

typedef struct {
    idx_t row_count;
    char *service_name;
    char *listen;
    char *scheme;
    char *method;
    char *path;
    char *query_string;
    char *content_type;
    char *headers_json;
    uint64_t body_bytes;
    char *caller_identity;
    char *remote_addr;
    char *remote_ip;
    int32_t remote_port;
    uint64_t route_id;
    char *route_method;
    char *route_match_kind;
    char *route_path;
    char *path_params_json;
} ducknng_http_request_bind_data;

typedef struct {
    idx_t row_count;
    uint8_t *body;
    idx_t body_len;
    char *body_text;
} ducknng_http_request_body_bind_data;

typedef struct {
    int emitted;
} ducknng_sql_http_single_row_init_data;

static void destroy_http_routes_bind_data(void *ptr) {
    ducknng_http_routes_bind_data *data = (ducknng_http_routes_bind_data *)ptr;
    idx_t i;
    if (!data) return;
    for (i = 0; i < data->row_count; i++) {
        if (data->rows[i].service_name) duckdb_free(data->rows[i].service_name);
        if (data->rows[i].method) duckdb_free(data->rows[i].method);
        if (data->rows[i].match_kind) duckdb_free(data->rows[i].match_kind);
        if (data->rows[i].path) duckdb_free(data->rows[i].path);
        if (data->rows[i].handler_sql) duckdb_free(data->rows[i].handler_sql);
    }
    if (data->rows) duckdb_free(data->rows);
    duckdb_free(data);
}

static void destroy_http_routes_init_data(void *ptr) {
    if (ptr) duckdb_free(ptr);
}

static int ducknng_http_routes_bind_reserve(ducknng_http_routes_bind_data *bind, idx_t want) {
    ducknng_http_route_row *next;
    idx_t new_cap = bind && bind->row_cap ? bind->row_cap * 2 : 8;
    if (!bind) return -1;
    if (!bind->rows) new_cap = want > 8 ? want : 8;
    if (bind->rows && bind->row_cap >= want) return 0;
    while (new_cap < want) new_cap *= 2;
    next = (ducknng_http_route_row *)duckdb_malloc(sizeof(*next) * (size_t)new_cap);
    if (!next) return -1;
    memset(next, 0, sizeof(*next) * (size_t)new_cap);
    if (bind->rows && bind->row_count > 0) {
        memcpy(next, bind->rows, sizeof(*next) * (size_t)bind->row_count);
        duckdb_free(bind->rows);
    }
    bind->rows = next;
    bind->row_cap = new_cap;
    return 0;
}

static void destroy_http_request_bind_data(void *ptr) {
    ducknng_http_request_bind_data *data = (ducknng_http_request_bind_data *)ptr;
    if (!data) return;
    if (data->service_name) duckdb_free(data->service_name);
    if (data->listen) duckdb_free(data->listen);
    if (data->scheme) duckdb_free(data->scheme);
    if (data->method) duckdb_free(data->method);
    if (data->path) duckdb_free(data->path);
    if (data->query_string) duckdb_free(data->query_string);
    if (data->content_type) duckdb_free(data->content_type);
    if (data->headers_json) duckdb_free(data->headers_json);
    if (data->caller_identity) duckdb_free(data->caller_identity);
    if (data->remote_addr) duckdb_free(data->remote_addr);
    if (data->remote_ip) duckdb_free(data->remote_ip);
    if (data->route_method) duckdb_free(data->route_method);
    if (data->route_match_kind) duckdb_free(data->route_match_kind);
    if (data->route_path) duckdb_free(data->route_path);
    if (data->path_params_json) duckdb_free(data->path_params_json);
    duckdb_free(data);
}

static void destroy_http_request_body_bind_data(void *ptr) {
    ducknng_http_request_body_bind_data *data = (ducknng_http_request_body_bind_data *)ptr;
    if (!data) return;
    if (data->body) duckdb_free(data->body);
    if (data->body_text) duckdb_free(data->body_text);
    duckdb_free(data);
}

static void destroy_sql_http_single_row_init_data(void *ptr) {
    if (ptr) duckdb_free(ptr);
}

static int ducknng_http_sql_reject_inside_request_handler(duckdb_function_info info, ducknng_sql_context *ctx,
    const char *what) {
    if (ctx && ctx->rt && ctx->is_init_connection &&
        ducknng_runtime_current_request_service_get(ctx->rt) != NULL) {
        duckdb_scalar_function_set_error(info, what);
        return 1;
    }
    return 0;
}

static int ducknng_http_sql_reject_table_inside_request_handler(duckdb_bind_info info,
    ducknng_sql_context *ctx, const char *what) {
    if (ctx && ctx->rt && ctx->is_init_connection &&
        ducknng_runtime_current_request_service_get(ctx->rt) != NULL) {
        duckdb_bind_set_error(info, what);
        return 1;
    }
    return 0;
}

static void ducknng_register_http_route_scalar(duckdb_function_info info, duckdb_data_chunk input,
    duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t ncols = duckdb_data_chunk_get_column_count(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    if (ducknng_http_sql_reject_inside_request_handler(info, ctx,
            "ducknng: cannot register HTTP routes from a request handler")) return;
    for (row = 0; row < count; row++) {
        char *service_name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *method = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        char *path = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 2), row);
        char *handler_sql = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 3), row);
        uint64_t request_max_bytes = ncols > 4 ? arg_u64(duckdb_data_chunk_get_vector(input, 4), row, 0) : 0;
        ducknng_service *svc;
        char *errmsg = NULL;
        if (!ctx || !ctx->rt || !service_name || !method || !path || !handler_sql) {
            if (service_name) duckdb_free(service_name);
            if (method) duckdb_free(method);
            if (path) duckdb_free(path);
            if (handler_sql) duckdb_free(handler_sql);
            duckdb_scalar_function_set_error(info, "ducknng: service_name, method, path, and handler_sql are required");
            return;
        }
        svc = ducknng_runtime_find_service(ctx->rt, service_name);
        if (!svc || ducknng_service_register_http_route(svc, method, path, handler_sql,
                request_max_bytes, &errmsg) != 0) {
            if (service_name) duckdb_free(service_name);
            if (method) duckdb_free(method);
            if (path) duckdb_free(path);
            if (handler_sql) duckdb_free(handler_sql);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to register HTTP route");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        duckdb_free(service_name);
        duckdb_free(method);
        duckdb_free(path);
        duckdb_free(handler_sql);
        out[row] = true;
    }
}

static void ducknng_unregister_http_route_scalar(duckdb_function_info info, duckdb_data_chunk input,
    duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    if (ducknng_http_sql_reject_inside_request_handler(info, ctx,
            "ducknng: cannot unregister HTTP routes from a request handler")) return;
    for (row = 0; row < count; row++) {
        char *service_name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *method = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        char *path = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 2), row);
        ducknng_service *svc;
        char *errmsg = NULL;
        int removed;
        if (!ctx || !ctx->rt || !service_name || !method || !path) {
            if (service_name) duckdb_free(service_name);
            if (method) duckdb_free(method);
            if (path) duckdb_free(path);
            duckdb_scalar_function_set_error(info, "ducknng: service_name, method, and path are required");
            return;
        }
        svc = ducknng_runtime_find_service(ctx->rt, service_name);
        if (!svc) {
            duckdb_free(service_name);
            duckdb_free(method);
            duckdb_free(path);
            duckdb_scalar_function_set_error(info, "ducknng: service not found");
            return;
        }
        removed = ducknng_service_unregister_http_route(svc, method, path, &errmsg);
        duckdb_free(service_name);
        duckdb_free(method);
        duckdb_free(path);
        if (removed < 0) {
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to unregister HTTP route");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (errmsg) duckdb_free(errmsg);
        out[row] = removed ? true : false;
    }
}

static void ducknng_register_http_route_pattern_scalar(duckdb_function_info info, duckdb_data_chunk input,
    duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t ncols = duckdb_data_chunk_get_column_count(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    if (ducknng_http_sql_reject_inside_request_handler(info, ctx,
            "ducknng: cannot register HTTP routes from a request handler")) return;
    for (row = 0; row < count; row++) {
        char *service_name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *method = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        char *match_kind = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 2), row);
        char *path_pattern = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 3), row);
        char *handler_sql = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 4), row);
        uint64_t request_max_bytes = ncols > 5 ? arg_u64(duckdb_data_chunk_get_vector(input, 5), row, 0) : 0;
        ducknng_service *svc;
        char *errmsg = NULL;
        if (!ctx || !ctx->rt || !service_name || !method || !match_kind ||
            !path_pattern || !handler_sql) {
            if (service_name) duckdb_free(service_name);
            if (method) duckdb_free(method);
            if (match_kind) duckdb_free(match_kind);
            if (path_pattern) duckdb_free(path_pattern);
            if (handler_sql) duckdb_free(handler_sql);
            duckdb_scalar_function_set_error(info,
                "ducknng: service_name, method, match_kind, path_pattern, and handler_sql are required");
            return;
        }
        svc = ducknng_runtime_find_service(ctx->rt, service_name);
        if (!svc || ducknng_service_register_http_route_pattern(svc, method, match_kind,
                path_pattern, handler_sql, request_max_bytes, &errmsg) != 0) {
            if (service_name) duckdb_free(service_name);
            if (method) duckdb_free(method);
            if (match_kind) duckdb_free(match_kind);
            if (path_pattern) duckdb_free(path_pattern);
            if (handler_sql) duckdb_free(handler_sql);
            duckdb_scalar_function_set_error(info,
                errmsg ? errmsg : "ducknng: failed to register HTTP route pattern");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        duckdb_free(service_name);
        duckdb_free(method);
        duckdb_free(match_kind);
        duckdb_free(path_pattern);
        duckdb_free(handler_sql);
        out[row] = true;
    }
}

static void ducknng_unregister_http_route_pattern_scalar(duckdb_function_info info, duckdb_data_chunk input,
    duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    if (ducknng_http_sql_reject_inside_request_handler(info, ctx,
            "ducknng: cannot unregister HTTP routes from a request handler")) return;
    for (row = 0; row < count; row++) {
        char *service_name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *method = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        char *match_kind = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 2), row);
        char *path_pattern = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 3), row);
        ducknng_service *svc;
        char *errmsg = NULL;
        int removed;
        if (!ctx || !ctx->rt || !service_name || !method || !match_kind || !path_pattern) {
            if (service_name) duckdb_free(service_name);
            if (method) duckdb_free(method);
            if (match_kind) duckdb_free(match_kind);
            if (path_pattern) duckdb_free(path_pattern);
            duckdb_scalar_function_set_error(info,
                "ducknng: service_name, method, match_kind, and path_pattern are required");
            return;
        }
        svc = ducknng_runtime_find_service(ctx->rt, service_name);
        if (!svc) {
            duckdb_free(service_name);
            duckdb_free(method);
            duckdb_free(match_kind);
            duckdb_free(path_pattern);
            duckdb_scalar_function_set_error(info, "ducknng: service not found");
            return;
        }
        removed = ducknng_service_unregister_http_route_pattern(svc, method, match_kind,
            path_pattern, &errmsg);
        duckdb_free(service_name);
        duckdb_free(method);
        duckdb_free(match_kind);
        duckdb_free(path_pattern);
        if (removed < 0) {
            duckdb_scalar_function_set_error(info,
                errmsg ? errmsg : "ducknng: failed to unregister HTTP route pattern");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (errmsg) duckdb_free(errmsg);
        out[row] = removed ? true : false;
    }
}

static void ducknng_list_http_routes_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    ducknng_http_routes_bind_data *bind;
    duckdb_logical_type type;
    size_t i;
    idx_t row = 0;
    if (!ctx || !ctx->rt) {
        duckdb_bind_set_error(info, "ducknng: missing runtime");
        return;
    }
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    if (ducknng_http_sql_reject_table_inside_request_handler(info, ctx,
            "ducknng: ducknng_list_http_routes() cannot run inside a request handler")) return;
    bind = (ducknng_http_routes_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    ducknng_mutex_lock(&ctx->rt->mu);
    for (i = 0; i < ctx->rt->service_count; i++) {
        ducknng_service *svc = ctx->rt->services[i];
        ducknng_http_route *routes = NULL;
        size_t route_count = 0;
        size_t j;
        if (!svc) continue;
        if (ducknng_service_http_routes_snapshot(svc, &routes, &route_count, NULL) != 0) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            destroy_http_routes_bind_data(bind);
            duckdb_bind_set_error(info, "ducknng: failed to snapshot HTTP routes");
            return;
        }
        if (route_count > 0 && ducknng_http_routes_bind_reserve(bind, row + (idx_t)route_count) != 0) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            ducknng_service_http_routes_free(routes, route_count);
            destroy_http_routes_bind_data(bind);
            duckdb_bind_set_error(info, "ducknng: out of memory");
            return;
        }
        for (j = 0; j < route_count; j++, row++) {
            bind->rows[row].service_id = svc->service_id;
            bind->rows[row].route_id = routes[j].route_id;
            bind->rows[row].request_max_bytes = routes[j].request_max_bytes;
            bind->rows[row].service_name = svc->name ? ducknng_strdup(svc->name) : NULL;
            bind->rows[row].method = routes[j].method ? ducknng_strdup(routes[j].method) : NULL;
            bind->rows[row].match_kind = ducknng_strdup(
                ducknng_http_route_match_kind_name(routes[j].match_kind));
            bind->rows[row].path = routes[j].path ? ducknng_strdup(routes[j].path) : NULL;
            bind->rows[row].handler_sql = routes[j].handler_sql ? ducknng_strdup(routes[j].handler_sql) : NULL;
            if ((svc->name && !bind->rows[row].service_name) ||
                (routes[j].method && !bind->rows[row].method) ||
                !bind->rows[row].match_kind ||
                (routes[j].path && !bind->rows[row].path) ||
                (routes[j].handler_sql && !bind->rows[row].handler_sql)) {
                ducknng_mutex_unlock(&ctx->rt->mu);
                ducknng_service_http_routes_free(routes, route_count);
                destroy_http_routes_bind_data(bind);
                duckdb_bind_set_error(info, "ducknng: out of memory");
                return;
            }
        }
        ducknng_service_http_routes_free(routes, route_count);
    }
    bind->row_count = row;
    ducknng_mutex_unlock(&ctx->rt->mu);

    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "service_id", type);
    duckdb_bind_add_result_column(info, "route_id", type);
    duckdb_bind_add_result_column(info, "request_max_bytes", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "service_name", type);
    duckdb_bind_add_result_column(info, "method", type);
    duckdb_bind_add_result_column(info, "match_kind", type);
    duckdb_bind_add_result_column(info, "path", type);
    duckdb_bind_add_result_column(info, "handler_sql", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_http_routes_bind_data);
    duckdb_bind_set_cardinality(info, bind->row_count, true);
}

static void ducknng_list_http_routes_init(duckdb_init_info info) {
    ducknng_http_routes_bind_data *bind =
        (ducknng_http_routes_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_http_routes_init_data *init =
        (ducknng_http_routes_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->bind = bind;
    init->offset = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_http_routes_init_data);
}

static void ducknng_list_http_routes_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_http_routes_init_data *init =
        (ducknng_http_routes_init_data *)duckdb_function_get_init_data(info);
    ducknng_http_routes_bind_data *bind;
    idx_t remaining;
    idx_t chunk_size;
    idx_t i;
    uint64_t *service_ids;
    uint64_t *route_ids;
    uint64_t *request_max_bytes;
    if (!init || !init->bind || init->offset >= init->bind->row_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    bind = init->bind;
    remaining = bind->row_count - init->offset;
    chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;
    service_ids = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0));
    route_ids = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 1));
    request_max_bytes = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 2));
    for (i = 0; i < chunk_size; i++) {
        ducknng_http_route_row *row = &bind->rows[init->offset + i];
        service_ids[i] = row->service_id;
        route_ids[i] = row->route_id;
        request_max_bytes[i] = row->request_max_bytes;
        if (row->service_name) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 3), i, row->service_name);
        else set_null(duckdb_data_chunk_get_vector(output, 3), i);
        if (row->method) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 4), i, row->method);
        else set_null(duckdb_data_chunk_get_vector(output, 4), i);
        if (row->match_kind) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 5), i, row->match_kind);
        else set_null(duckdb_data_chunk_get_vector(output, 5), i);
        if (row->path) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 6), i, row->path);
        else set_null(duckdb_data_chunk_get_vector(output, 6), i);
        if (row->handler_sql) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 7), i, row->handler_sql);
        else set_null(duckdb_data_chunk_get_vector(output, 7), i);
    }
    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
}

static void ducknng_http_request_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    const ducknng_http_request_context *request_ctx = NULL;
    ducknng_http_request_bind_data *bind;
    duckdb_logical_type type;
    if (!ctx || !ctx->rt) {
        duckdb_bind_set_error(info, "ducknng: missing runtime");
        return;
    }
    bind = (ducknng_http_request_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    request_ctx = ducknng_runtime_current_thread_http_request_context_get(ctx->rt);
    if (request_ctx && request_ctx->svc) {
        bind->row_count = 1;
        bind->service_name = request_ctx->svc->name ? ducknng_strdup(request_ctx->svc->name) : NULL;
        bind->listen = ducknng_service_resolved_listen(request_ctx->svc) ?
            ducknng_strdup(ducknng_service_resolved_listen(request_ctx->svc)) : NULL;
        bind->scheme = ducknng_strdup(ducknng_transport_scheme_name(request_ctx->scheme));
        bind->method = request_ctx->method ? ducknng_strdup(request_ctx->method) : NULL;
        bind->path = request_ctx->path ? ducknng_strdup(request_ctx->path) : NULL;
        bind->query_string = request_ctx->query_string ? ducknng_strdup(request_ctx->query_string) : NULL;
        bind->content_type = request_ctx->content_type ? ducknng_strdup(request_ctx->content_type) : NULL;
        bind->headers_json = request_ctx->headers_json ? ducknng_strdup(request_ctx->headers_json) : NULL;
        bind->body_bytes = (uint64_t)request_ctx->body_len;
        bind->caller_identity = request_ctx->caller_identity ? ducknng_strdup(request_ctx->caller_identity) : NULL;
        bind->remote_addr = ducknng_sql_sockaddr_addr_dup(request_ctx->remote_addr, &bind->remote_ip, &bind->remote_port);
        bind->route_id = request_ctx->route.route_id;
        bind->route_method = request_ctx->route.method ? ducknng_strdup(request_ctx->route.method) : NULL;
        bind->route_match_kind = ducknng_strdup(
            ducknng_http_route_match_kind_name(request_ctx->route.match_kind));
        bind->route_path = request_ctx->route.path ? ducknng_strdup(request_ctx->route.path) : NULL;
        bind->path_params_json = request_ctx->path_params_json ?
            ducknng_strdup(request_ctx->path_params_json) : NULL;
    }
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "service_name", type);
    duckdb_bind_add_result_column(info, "listen", type);
    duckdb_bind_add_result_column(info, "scheme", type);
    duckdb_bind_add_result_column(info, "method", type);
    duckdb_bind_add_result_column(info, "path", type);
    duckdb_bind_add_result_column(info, "query_string", type);
    duckdb_bind_add_result_column(info, "content_type", type);
    duckdb_bind_add_result_column(info, "headers_json", type);
    duckdb_bind_add_result_column(info, "caller_identity", type);
    duckdb_bind_add_result_column(info, "remote_addr", type);
    duckdb_bind_add_result_column(info, "remote_ip", type);
    duckdb_bind_add_result_column(info, "route_method", type);
    duckdb_bind_add_result_column(info, "route_match_kind", type);
    duckdb_bind_add_result_column(info, "route_path", type);
    duckdb_bind_add_result_column(info, "path_params_json", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "body_bytes", type);
    duckdb_bind_add_result_column(info, "route_id", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_bind_add_result_column(info, "remote_port", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_http_request_bind_data);
    duckdb_bind_set_cardinality(info, bind->row_count, true);
}

static void ducknng_http_request_body_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    const ducknng_http_request_context *request_ctx = NULL;
    ducknng_http_request_body_bind_data *bind;
    duckdb_logical_type type;
    if (!ctx || !ctx->rt) {
        duckdb_bind_set_error(info, "ducknng: missing runtime");
        return;
    }
    bind = (ducknng_http_request_body_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    request_ctx = ducknng_runtime_current_thread_http_request_context_get(ctx->rt);
    if (request_ctx) {
        bind->row_count = 1;
        bind->body_len = (idx_t)request_ctx->body_len;
        if (request_ctx->body_len > 0) {
            bind->body = (uint8_t *)duckdb_malloc(request_ctx->body_len);
            if (!bind->body) {
                destroy_http_request_body_bind_data(bind);
                duckdb_bind_set_error(info, "ducknng: out of memory");
                return;
            }
            memcpy(bind->body, request_ctx->body, request_ctx->body_len);
            if (ducknng_sql_bytes_look_text(request_ctx->body, request_ctx->body_len)) {
                bind->body_text = (char *)duckdb_malloc(request_ctx->body_len + 1);
                if (!bind->body_text) {
                    destroy_http_request_body_bind_data(bind);
                    duckdb_bind_set_error(info, "ducknng: out of memory");
                    return;
                }
                memcpy(bind->body_text, request_ctx->body, request_ctx->body_len);
                bind->body_text[request_ctx->body_len] = '\0';
            }
        }
    }
    type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
    duckdb_bind_add_result_column(info, "body", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "body_text", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_http_request_body_bind_data);
    duckdb_bind_set_cardinality(info, bind->row_count, true);
}

static void ducknng_sql_http_single_row_init(duckdb_init_info info) {
    ducknng_sql_http_single_row_init_data *init =
        (ducknng_sql_http_single_row_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->emitted = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_sql_http_single_row_init_data);
}

static void ducknng_http_request_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_sql_http_single_row_init_data *init =
        (ducknng_sql_http_single_row_init_data *)duckdb_function_get_init_data(info);
    ducknng_http_request_bind_data *bind =
        (ducknng_http_request_bind_data *)duckdb_function_get_bind_data(info);
    if (!init || !bind || init->emitted || bind->row_count == 0) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
#define ASSIGN_REQ_STRING(IDX, VALUE) do { \
        if ((VALUE)) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, (IDX)), 0, (VALUE)); \
        else set_null(duckdb_data_chunk_get_vector(output, (IDX)), 0); \
    } while (0)
    ASSIGN_REQ_STRING(0, bind->service_name);
    ASSIGN_REQ_STRING(1, bind->listen);
    ASSIGN_REQ_STRING(2, bind->scheme);
    ASSIGN_REQ_STRING(3, bind->method);
    ASSIGN_REQ_STRING(4, bind->path);
    ASSIGN_REQ_STRING(5, bind->query_string);
    ASSIGN_REQ_STRING(6, bind->content_type);
    ASSIGN_REQ_STRING(7, bind->headers_json);
    ASSIGN_REQ_STRING(8, bind->caller_identity);
    ASSIGN_REQ_STRING(9, bind->remote_addr);
    ASSIGN_REQ_STRING(10, bind->remote_ip);
    ASSIGN_REQ_STRING(11, bind->route_method);
    ASSIGN_REQ_STRING(12, bind->route_match_kind);
    ASSIGN_REQ_STRING(13, bind->route_path);
    ASSIGN_REQ_STRING(14, bind->path_params_json);
    ((uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 15)))[0] = bind->body_bytes;
    ((uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 16)))[0] = bind->route_id;
    ((int32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 17)))[0] = bind->remote_port;
#undef ASSIGN_REQ_STRING
    init->emitted = 1;
    duckdb_data_chunk_set_size(output, 1);
}

static void ducknng_http_request_body_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_sql_http_single_row_init_data *init =
        (ducknng_sql_http_single_row_init_data *)duckdb_function_get_init_data(info);
    ducknng_http_request_body_bind_data *bind =
        (ducknng_http_request_body_bind_data *)duckdb_function_get_bind_data(info);
    if (!init || !bind || init->emitted || bind->row_count == 0) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    assign_blob(duckdb_data_chunk_get_vector(output, 0), 0,
        bind->body ? bind->body : (const uint8_t *)"", bind->body_len);
    if (bind->body_text) duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 1), 0, bind->body_text);
    else set_null(duckdb_data_chunk_get_vector(output, 1), 0);
    init->emitted = 1;
    duckdb_data_chunk_set_size(output, 1);
}

int ducknng_register_sql_http(duckdb_connection con, ducknng_sql_context *ctx) {
    duckdb_type route_types[4] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR};
    duckdb_type route_types_with_limit[5] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT};
    duckdb_type unregister_types[3] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR};
    duckdb_type route_pattern_types[5] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR};
    duckdb_type route_pattern_types_with_limit[6] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT};
    duckdb_type unregister_pattern_types[4] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR};
    if (!ctx || !ctx->rt) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_register_http_route", 4,
            ducknng_register_http_route_scalar, ctx, route_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_register_http_route", 5,
            ducknng_register_http_route_scalar, ctx, route_types_with_limit, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_register_http_route_pattern", 5,
            ducknng_register_http_route_pattern_scalar, ctx, route_pattern_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_register_http_route_pattern", 6,
            ducknng_register_http_route_pattern_scalar, ctx, route_pattern_types_with_limit, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_unregister_http_route", 3,
            ducknng_unregister_http_route_scalar, ctx, unregister_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_unregister_http_route_pattern", 4,
            ducknng_unregister_http_route_pattern_scalar, ctx, unregister_pattern_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_TABLE(con, "ducknng_list_http_routes", ctx, 0, NULL,
            ducknng_list_http_routes_bind, ducknng_list_http_routes_init, ducknng_list_http_routes_scan)) return 0;
    if (!DUCKNNG_REGISTER_TABLE(con, "ducknng_http_request", ctx, 0, NULL,
            ducknng_http_request_bind, ducknng_sql_http_single_row_init, ducknng_http_request_scan)) return 0;
    if (!DUCKNNG_REGISTER_TABLE(con, "ducknng_http_request_body", ctx, 0, NULL,
            ducknng_http_request_body_bind, ducknng_sql_http_single_row_init, ducknng_http_request_body_scan)) return 0;
    return 1;
}
