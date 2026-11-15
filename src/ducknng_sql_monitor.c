#include "ducknng_sql_shared.h"
#include "ducknng_service.h"
#include "ducknng_transport.h"
#include "ducknng_util.h"
#include "ducknng_runtime.h"
#include "ducknng_nng_compat.h"
#include <string.h>

DUCKDB_EXTENSION_EXTERN

static int register_monitor_table(duckdb_connection con, ducknng_sql_context *ctx);
static int register_monitor_status_table(duckdb_connection con, ducknng_sql_context *ctx);
static int register_pipes_table(duckdb_connection con, ducknng_sql_context *ctx);
static int register_log_entries_table(duckdb_connection con, ducknng_sql_context *ctx);
static int register_nng_stats_table(duckdb_connection con, ducknng_sql_context *ctx);
static int register_socket_monitor(duckdb_connection con, ducknng_sql_context *ctx);
static int register_enable_log_capture_scalar(duckdb_connection con, ducknng_sql_context *ctx);

/* Bind copies arguments only; each init takes a fresh owned snapshot. */
typedef struct {
    uint64_t seq;
    uint64_t ts_ms;
    uint64_t pipe_id;
    char *service_name;
    char *listen;
    char *transport_family;
    char *scheme;
    char *event;
    int admitted;
    char *reason;
    char *remote_addr;
    char *remote_ip;
    int32_t remote_port;
    char *peer_identity;
} ducknng_monitor_row;

typedef struct {
    char *service_name;
    uint64_t after_seq;
    uint64_t max_events;
} ducknng_monitor_bind_data;

typedef struct {
    ducknng_monitor_row *rows;
    idx_t row_count;
    idx_t offset;
} ducknng_monitor_init_data;

/* Bind data for ducknng_list_pipes and ducknng_monitor_status. */
typedef struct {
    char *service_name;
} ducknng_service_name_bind_data;

typedef struct {
    uint64_t pipe_id;
    uint64_t opened_ms;
    char *service_name;
    char *listen;
    char *transport_family;
    char *scheme;
    char *remote_addr;
    char *remote_ip;
    int32_t remote_port;
    char *peer_identity;
} ducknng_pipe_row;

typedef struct {
    ducknng_pipe_row *rows;
    idx_t row_count;
    idx_t offset;
} ducknng_pipes_init_data;

typedef struct {
    ducknng_pipe_monitor_stats stats;
    idx_t offset;
} ducknng_monitor_status_init_data;

static void ducknng_monitor_row_reset(ducknng_monitor_row *row) {
    if (!row) return;
    if (row->service_name) duckdb_free(row->service_name);
    if (row->listen) duckdb_free(row->listen);
    if (row->transport_family) duckdb_free(row->transport_family);
    if (row->scheme) duckdb_free(row->scheme);
    if (row->event) duckdb_free(row->event);
    if (row->reason) duckdb_free(row->reason);
    if (row->remote_addr) duckdb_free(row->remote_addr);
    if (row->remote_ip) duckdb_free(row->remote_ip);
    if (row->peer_identity) duckdb_free(row->peer_identity);
    memset(row, 0, sizeof(*row));
}

static void destroy_monitor_bind_data(void *ptr) {
    ducknng_monitor_bind_data *data = (ducknng_monitor_bind_data *)ptr;
    if (!data) return;
    if (data->service_name) duckdb_free(data->service_name);
    duckdb_free(data);
}

static void destroy_monitor_init_data(void *ptr) {
    ducknng_monitor_init_data *data = (ducknng_monitor_init_data *)ptr;
    idx_t i;
    if (!data) return;
    for (i = 0; i < data->row_count; i++) ducknng_monitor_row_reset(&data->rows[i]);
    if (data->rows) duckdb_free(data->rows);
    duckdb_free(data);
}

static void ducknng_pipe_row_reset(ducknng_pipe_row *row) {
    if (!row) return;
    if (row->service_name) duckdb_free(row->service_name);
    if (row->listen) duckdb_free(row->listen);
    if (row->transport_family) duckdb_free(row->transport_family);
    if (row->scheme) duckdb_free(row->scheme);
    if (row->remote_addr) duckdb_free(row->remote_addr);
    if (row->remote_ip) duckdb_free(row->remote_ip);
    if (row->peer_identity) duckdb_free(row->peer_identity);
    memset(row, 0, sizeof(*row));
}

static void destroy_pipes_init_data(void *ptr) {
    ducknng_pipes_init_data *data = (ducknng_pipes_init_data *)ptr;
    idx_t i;
    if (!data) return;
    for (i = 0; i < data->row_count; i++) ducknng_pipe_row_reset(&data->rows[i]);
    if (data->rows) duckdb_free(data->rows);
    duckdb_free(data);
}

static void destroy_service_name_bind_data(void *ptr) {
    ducknng_service_name_bind_data *data = (ducknng_service_name_bind_data *)ptr;
    if (!data) return;
    if (data->service_name) duckdb_free(data->service_name);
    duckdb_free(data);
}

static void destroy_monitor_status_init_data(void *ptr) {
    if (ptr) duckdb_free(ptr);
}

static char *ducknng_bind_varchar_parameter(duckdb_bind_info info, idx_t idx) {
    duckdb_value val = duckdb_bind_get_parameter(info, idx);
    char *out = duckdb_get_varchar(val);
    duckdb_destroy_value(&val);
    return out;
}

static uint64_t ducknng_bind_u64_parameter(duckdb_bind_info info, idx_t idx, uint64_t dflt) {
    duckdb_value val = duckdb_bind_get_parameter(info, idx);
    uint64_t out = val ? (uint64_t)duckdb_get_uint64(val) : dflt;
    duckdb_destroy_value(&val);
    return out;
}

/* Copies the service-name argument. The service itself is resolved at init. */
static char *ducknng_monitor_bind_service_name(duckdb_bind_info info, ducknng_sql_context *ctx, idx_t name_idx) {
    char *name;
    if (!ctx || !ctx->rt) {
        duckdb_bind_set_error(info, "ducknng: missing runtime");
        return NULL;
    }
    name = ducknng_bind_varchar_parameter(info, name_idx);
    if (!name || !name[0]) {
        if (name) duckdb_free(name);
        duckdb_bind_set_error(info, "ducknng: service name is required");
        return NULL;
    }
    return name;
}

static ducknng_service *ducknng_monitor_init_service(duckdb_init_info info, const char *name,
    ducknng_transport_url *out_parsed, const char **out_listen) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_init_get_extra_info(info);
    ducknng_service *svc;
    const char *listen;
    char *parse_err = NULL;
    if (out_listen) *out_listen = NULL;
    if (!ctx || !ctx->rt || !name) {
        duckdb_init_set_error(info, "ducknng: missing runtime");
        return NULL;
    }
    svc = ducknng_runtime_find_service(ctx->rt, name);
    if (!svc) {
        duckdb_init_set_error(info, "ducknng: service not found");
        return NULL;
    }
    listen = ducknng_service_resolved_listen(svc);
    if (out_parsed) {
        ducknng_transport_url_init(out_parsed);
        if (listen && ducknng_transport_url_parse(listen, out_parsed, &parse_err) != 0) {
            if (parse_err) duckdb_free(parse_err);
        }
    }
    if (out_listen) *out_listen = listen;
    return svc;
}

static void ducknng_read_monitor_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    ducknng_monitor_bind_data *bind = NULL;
    char *name;
    duckdb_logical_type type;

    if (!ctx || !ctx->rt) {
        duckdb_bind_set_error(info, "ducknng: missing runtime");
        return;
    }
    if (duckdb_bind_get_parameter_count(info) != 3) {
        duckdb_bind_set_error(info, "ducknng: ducknng_read_monitor(name, after_seq, max_events) requires exactly three parameters");
        return;
    }
    name = ducknng_monitor_bind_service_name(info, ctx, 0);
    if (!name) return;
    bind = (ducknng_monitor_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_free(name);
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    bind->service_name = name;
    bind->after_seq = ducknng_bind_u64_parameter(info, 1, 0);
    bind->max_events = ducknng_bind_u64_parameter(info, 2, 0);

    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "seq", type);
    duckdb_bind_add_result_column(info, "ts_ms", type);
    duckdb_bind_add_result_column(info, "pipe_id", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "service_name", type);
    duckdb_bind_add_result_column(info, "listen", type);
    duckdb_bind_add_result_column(info, "transport_family", type);
    duckdb_bind_add_result_column(info, "scheme", type);
    duckdb_bind_add_result_column(info, "event", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "admitted", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "reason", type);
    duckdb_bind_add_result_column(info, "remote_addr", type);
    duckdb_bind_add_result_column(info, "remote_ip", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_bind_add_result_column(info, "remote_port", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "peer_identity", type);
    duckdb_destroy_logical_type(&type);

    duckdb_bind_set_bind_data(info, bind, destroy_monitor_bind_data);
}

static void ducknng_read_monitor_init(duckdb_init_info info) {
    ducknng_monitor_bind_data *bind = (ducknng_monitor_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_monitor_init_data *init;
    ducknng_service *svc;
    ducknng_pipe_event *events = NULL;
    size_t event_count = 0;
    char *errmsg = NULL;
    ducknng_transport_url parsed;
    const char *listen = NULL;
    size_t i;

    if (!bind) {
        duckdb_init_set_error(info, "ducknng: missing bind data");
        return;
    }
    svc = ducknng_monitor_init_service(info, bind->service_name, &parsed, &listen);
    if (!svc) return;
    if (ducknng_service_pipe_events_snapshot(svc, bind->after_seq, bind->max_events,
            &events, &event_count, &errmsg) != 0) {
        duckdb_init_set_error(info, errmsg ? errmsg : "ducknng: failed to snapshot pipe monitor events");
        if (errmsg) duckdb_free(errmsg);
        return;
    }
    init = (ducknng_monitor_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        ducknng_service_pipe_events_free(events, event_count);
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(init, 0, sizeof(*init));
    if (event_count > 0) {
        init->rows = (ducknng_monitor_row *)duckdb_malloc(sizeof(*init->rows) * event_count);
        if (!init->rows) {
            ducknng_service_pipe_events_free(events, event_count);
            duckdb_free(init);
            duckdb_init_set_error(info, "ducknng: out of memory");
            return;
        }
        memset(init->rows, 0, sizeof(*init->rows) * event_count);
        init->row_count = (idx_t)event_count;
        for (i = 0; i < event_count; i++) {
            ducknng_monitor_row *row = &init->rows[i];
            row->seq = events[i].seq;
            row->ts_ms = events[i].ts_ms;
            row->pipe_id = events[i].pipe_id;
            row->service_name = ducknng_strdup(bind->service_name);
            row->listen = listen ? ducknng_strdup(listen) : NULL;
            row->transport_family = ducknng_strdup(ducknng_transport_family_name(parsed.family));
            row->scheme = ducknng_strdup(ducknng_transport_scheme_name(parsed.scheme));
            row->event = events[i].event ? ducknng_strdup(events[i].event) : NULL;
            row->admitted = events[i].admitted;
            row->reason = events[i].reason ? ducknng_strdup(events[i].reason) : NULL;
            row->remote_addr = events[i].remote_addr ? ducknng_strdup(events[i].remote_addr) : NULL;
            row->remote_ip = events[i].remote_ip ? ducknng_strdup(events[i].remote_ip) : NULL;
            row->remote_port = events[i].remote_port;
            row->peer_identity = events[i].peer_identity ? ducknng_strdup(events[i].peer_identity) : NULL;
        }
    }
    ducknng_service_pipe_events_free(events, event_count);
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_monitor_init_data);
}

static void ducknng_read_monitor_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_monitor_init_data *init = (ducknng_monitor_init_data *)duckdb_function_get_init_data(info);
    idx_t remaining;
    idx_t chunk_size;
    idx_t i;
    if (!init || init->offset >= init->row_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    remaining = init->row_count - init->offset;
    chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;
    for (i = 0; i < chunk_size; i++) {
        ducknng_monitor_row *row = &init->rows[init->offset + i];
        ((uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0)))[i] = row->seq;
        ((uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 1)))[i] = row->ts_ms;
        ((uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 2)))[i] = row->pipe_id;
        if (row->service_name) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 3), i, row->service_name, (idx_t)strlen(row->service_name)); else set_null(duckdb_data_chunk_get_vector(output, 3), i);
        if (row->listen) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 4), i, row->listen, (idx_t)strlen(row->listen)); else set_null(duckdb_data_chunk_get_vector(output, 4), i);
        if (row->transport_family) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 5), i, row->transport_family, (idx_t)strlen(row->transport_family)); else set_null(duckdb_data_chunk_get_vector(output, 5), i);
        if (row->scheme) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 6), i, row->scheme, (idx_t)strlen(row->scheme)); else set_null(duckdb_data_chunk_get_vector(output, 6), i);
        if (row->event) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 7), i, row->event, (idx_t)strlen(row->event)); else set_null(duckdb_data_chunk_get_vector(output, 7), i);
        if (row->admitted < 0) set_null(duckdb_data_chunk_get_vector(output, 8), i);
        else ((bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 8)))[i] = row->admitted ? true : false;
        if (row->reason) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 9), i, row->reason, (idx_t)strlen(row->reason)); else set_null(duckdb_data_chunk_get_vector(output, 9), i);
        if (row->remote_addr) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 10), i, row->remote_addr, (idx_t)strlen(row->remote_addr)); else set_null(duckdb_data_chunk_get_vector(output, 10), i);
        if (row->remote_ip) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 11), i, row->remote_ip, (idx_t)strlen(row->remote_ip)); else set_null(duckdb_data_chunk_get_vector(output, 11), i);
        ((int32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 12)))[i] = row->remote_port;
        if (row->peer_identity) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 13), i, row->peer_identity, (idx_t)strlen(row->peer_identity)); else set_null(duckdb_data_chunk_get_vector(output, 13), i);
    }
    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
}

static void ducknng_list_pipes_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    ducknng_service_name_bind_data *bind = NULL;
    char *name;
    duckdb_logical_type type;

    if (duckdb_bind_get_parameter_count(info) != 1) {
        duckdb_bind_set_error(info, "ducknng: ducknng_list_pipes(name) requires exactly one parameter");
        return;
    }
    name = ducknng_monitor_bind_service_name(info, ctx, 0);
    if (!name) return;
    bind = (ducknng_service_name_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_free(name);
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    bind->service_name = name;

    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "pipe_id", type);
    duckdb_bind_add_result_column(info, "opened_ms", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "service_name", type);
    duckdb_bind_add_result_column(info, "listen", type);
    duckdb_bind_add_result_column(info, "transport_family", type);
    duckdb_bind_add_result_column(info, "scheme", type);
    duckdb_bind_add_result_column(info, "remote_addr", type);
    duckdb_bind_add_result_column(info, "remote_ip", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_bind_add_result_column(info, "remote_port", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "peer_identity", type);
    duckdb_destroy_logical_type(&type);

    duckdb_bind_set_bind_data(info, bind, destroy_service_name_bind_data);
}

static void ducknng_list_pipes_init(duckdb_init_info info) {
    ducknng_service_name_bind_data *bind = (ducknng_service_name_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_pipes_init_data *init;
    ducknng_service *svc;
    ducknng_pipe_state *states = NULL;
    size_t state_count = 0;
    char *errmsg = NULL;
    ducknng_transport_url parsed;
    const char *listen = NULL;
    size_t i;

    if (!bind) {
        duckdb_init_set_error(info, "ducknng: missing bind data");
        return;
    }
    svc = ducknng_monitor_init_service(info, bind->service_name, &parsed, &listen);
    if (!svc) return;
    if (ducknng_service_pipe_states_snapshot(svc, &states, &state_count, &errmsg) != 0) {
        duckdb_init_set_error(info, errmsg ? errmsg : "ducknng: failed to snapshot active pipes");
        if (errmsg) duckdb_free(errmsg);
        return;
    }
    init = (ducknng_pipes_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        ducknng_service_pipe_states_free(states, state_count);
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(init, 0, sizeof(*init));
    if (state_count > 0) {
        init->rows = (ducknng_pipe_row *)duckdb_malloc(sizeof(*init->rows) * state_count);
        if (!init->rows) {
            ducknng_service_pipe_states_free(states, state_count);
            duckdb_free(init);
            duckdb_init_set_error(info, "ducknng: out of memory");
            return;
        }
        memset(init->rows, 0, sizeof(*init->rows) * state_count);
        init->row_count = (idx_t)state_count;
        for (i = 0; i < state_count; i++) {
            ducknng_pipe_row *row = &init->rows[i];
            row->pipe_id = states[i].pipe_id;
            row->opened_ms = states[i].opened_ms;
            row->service_name = ducknng_strdup(bind->service_name);
            row->listen = listen ? ducknng_strdup(listen) : NULL;
            row->transport_family = ducknng_strdup(ducknng_transport_family_name(parsed.family));
            row->scheme = ducknng_strdup(ducknng_transport_scheme_name(parsed.scheme));
            row->remote_addr = states[i].remote_addr ? ducknng_strdup(states[i].remote_addr) : NULL;
            row->remote_ip = states[i].remote_ip ? ducknng_strdup(states[i].remote_ip) : NULL;
            row->remote_port = states[i].remote_port;
            row->peer_identity = states[i].peer_identity ? ducknng_strdup(states[i].peer_identity) : NULL;
        }
    }
    ducknng_service_pipe_states_free(states, state_count);
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_pipes_init_data);
}

static void ducknng_list_pipes_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_pipes_init_data *init = (ducknng_pipes_init_data *)duckdb_function_get_init_data(info);
    idx_t remaining;
    idx_t chunk_size;
    idx_t i;
    if (!init || init->offset >= init->row_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    remaining = init->row_count - init->offset;
    chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;
    for (i = 0; i < chunk_size; i++) {
        ducknng_pipe_row *row = &init->rows[init->offset + i];
        ((uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0)))[i] = row->pipe_id;
        ((uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 1)))[i] = row->opened_ms;
        if (row->service_name) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 2), i, row->service_name, (idx_t)strlen(row->service_name)); else set_null(duckdb_data_chunk_get_vector(output, 2), i);
        if (row->listen) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 3), i, row->listen, (idx_t)strlen(row->listen)); else set_null(duckdb_data_chunk_get_vector(output, 3), i);
        if (row->transport_family) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 4), i, row->transport_family, (idx_t)strlen(row->transport_family)); else set_null(duckdb_data_chunk_get_vector(output, 4), i);
        if (row->scheme) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 5), i, row->scheme, (idx_t)strlen(row->scheme)); else set_null(duckdb_data_chunk_get_vector(output, 5), i);
        if (row->remote_addr) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 6), i, row->remote_addr, (idx_t)strlen(row->remote_addr)); else set_null(duckdb_data_chunk_get_vector(output, 6), i);
        if (row->remote_ip) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 7), i, row->remote_ip, (idx_t)strlen(row->remote_ip)); else set_null(duckdb_data_chunk_get_vector(output, 7), i);
        ((int32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 8)))[i] = row->remote_port;
        if (row->peer_identity) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 9), i, row->peer_identity, (idx_t)strlen(row->peer_identity)); else set_null(duckdb_data_chunk_get_vector(output, 9), i);
    }
    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
}

static void ducknng_monitor_status_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    ducknng_service_name_bind_data *bind = NULL;
    char *name;
    duckdb_logical_type type;

    if (duckdb_bind_get_parameter_count(info) != 1) {
        duckdb_bind_set_error(info, "ducknng: ducknng_monitor_status(name) requires exactly one parameter");
        return;
    }
    name = ducknng_monitor_bind_service_name(info, ctx, 0);
    if (!name) return;
    bind = (ducknng_service_name_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_free(name);
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    bind->service_name = name;

    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "service_name", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "event_capacity", type);
    duckdb_bind_add_result_column(info, "event_count", type);
    duckdb_bind_add_result_column(info, "oldest_seq", type);
    duckdb_bind_add_result_column(info, "newest_seq", type);
    duckdb_bind_add_result_column(info, "dropped_events", type);
    duckdb_bind_add_result_column(info, "active_pipes", type);
    duckdb_bind_add_result_column(info, "max_active_pipes", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_service_name_bind_data);
    duckdb_bind_set_cardinality(info, 1, true);
}

static void ducknng_monitor_status_init(duckdb_init_info info) {
    ducknng_service_name_bind_data *bind = (ducknng_service_name_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_monitor_status_init_data *init;
    ducknng_service *svc;
    ducknng_pipe_monitor_stats stats;
    char *errmsg = NULL;

    if (!bind) {
        duckdb_init_set_error(info, "ducknng: missing bind data");
        return;
    }
    svc = ducknng_monitor_init_service(info, bind->service_name, NULL, NULL);
    if (!svc) return;
    if (ducknng_service_pipe_monitor_stats(svc, &stats, &errmsg) != 0) {
        duckdb_init_set_error(info, errmsg ? errmsg : "ducknng: failed to snapshot pipe monitor status");
        if (errmsg) duckdb_free(errmsg);
        return;
    }
    init = (ducknng_monitor_status_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->stats = stats;
    init->offset = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_monitor_status_init_data);
}

static void ducknng_monitor_status_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_monitor_status_init_data *init = (ducknng_monitor_status_init_data *)duckdb_function_get_init_data(info);
    ducknng_service_name_bind_data *bind = (ducknng_service_name_bind_data *)duckdb_function_get_bind_data(info);
    uint64_t *col;
    if (!init || !bind || init->offset > 0) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    if (bind->service_name) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 0), 0, bind->service_name, (idx_t)strlen(bind->service_name));
    else set_null(duckdb_data_chunk_get_vector(output, 0), 0);
    col = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 1)); col[0] = init->stats.event_capacity;
    col = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 2)); col[0] = init->stats.event_count;
    col = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 3)); col[0] = init->stats.oldest_seq;
    col = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 4)); col[0] = init->stats.newest_seq;
    col = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 5)); col[0] = init->stats.dropped_events;
    col = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 6)); col[0] = init->stats.active_pipes;
    col = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 7)); col[0] = init->stats.max_active_pipes;
    init->offset = 1;
    duckdb_data_chunk_set_size(output, 1);
}

static int register_monitor_table(duckdb_connection con, ducknng_sql_context *ctx) {
    duckdb_type param_types[3] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT};
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE(con, "ducknng_read_monitor", ctx, 3, param_types,
        ducknng_read_monitor_bind, ducknng_read_monitor_init, ducknng_read_monitor_scan);
}

static int register_monitor_status_table(duckdb_connection con, ducknng_sql_context *ctx) {
    duckdb_type param_types[1] = {DUCKDB_TYPE_VARCHAR};
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE(con, "ducknng_monitor_status", ctx, 1, param_types,
        ducknng_monitor_status_bind, ducknng_monitor_status_init, ducknng_monitor_status_scan);
}

static int register_pipes_table(duckdb_connection con, ducknng_sql_context *ctx) {
    duckdb_type param_types[1] = {DUCKDB_TYPE_VARCHAR};
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE(con, "ducknng_list_pipes", ctx, 1, param_types,
        ducknng_list_pipes_bind, ducknng_list_pipes_init, ducknng_list_pipes_scan);
}

static int register_monitor_table(duckdb_connection con, ducknng_sql_context *ctx);
static int register_monitor_status_table(duckdb_connection con, ducknng_sql_context *ctx);
static int register_pipes_table(duckdb_connection con, ducknng_sql_context *ctx);
static int register_log_entries_table(duckdb_connection con, ducknng_sql_context *ctx);

int ducknng_register_sql_monitor(duckdb_connection con, ducknng_sql_context *ctx) {
    if (!register_monitor_table(con, ctx)) return 0;
    if (!register_monitor_status_table(con, ctx)) return 0;
    if (!register_pipes_table(con, ctx)) return 0;
    if (!register_log_entries_table(con, ctx)) return 0;
    if (!register_nng_stats_table(con, ctx)) return 0;
    if (!register_socket_monitor(con, ctx)) return 0;
    if (!register_enable_log_capture_scalar(con, ctx)) return 0;
    return 1;
}

/* ---------------------------------------------------------------------------
 * ducknng_log_entries() — snapshot of the DuckDB log ring
 * --------------------------------------------------------------------------- */

typedef struct {
    duckdb_timestamp *ts;
    char **level;
    char **log_type;
    char **message;
    idx_t row_count;
    idx_t offset;
} ducknng_log_entries_init_data;

static void destroy_log_entries_init_data(void *ptr) {
    ducknng_log_entries_init_data *data = (ducknng_log_entries_init_data *)ptr;
    idx_t i;
    if (!data) return;
    for (i = 0; i < (idx_t)DUCKNNG_LOG_RING_CAP; i++) {
        if (data->level && data->level[i]) { duckdb_free(data->level[i]); data->level[i] = NULL; }
        if (data->log_type && data->log_type[i]) { duckdb_free(data->log_type[i]); data->log_type[i] = NULL; }
        if (data->message && data->message[i]) { duckdb_free(data->message[i]); data->message[i] = NULL; }
    }
    if (data->ts) duckdb_free(data->ts);
    if (data->level) duckdb_free(data->level);
    if (data->log_type) duckdb_free(data->log_type);
    if (data->message) duckdb_free(data->message);
    duckdb_free(data);
}

static void ducknng_log_entries_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    duckdb_logical_type type;

    if (!ctx || !ctx->rt) {
        duckdb_bind_set_error(info, "ducknng: missing runtime");
        return;
    }

    type = duckdb_create_logical_type(DUCKDB_TYPE_TIMESTAMP);
    duckdb_bind_add_result_column(info, "ts", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "level", type);
    duckdb_bind_add_result_column(info, "log_type", type);
    duckdb_bind_add_result_column(info, "message", type);
    duckdb_destroy_logical_type(&type);

}

static void ducknng_log_entries_init(duckdb_init_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_init_get_extra_info(info);
    ducknng_log_entries_init_data *init;

    if (!ctx || !ctx->rt) {
        duckdb_init_set_error(info, "ducknng: missing runtime");
        return;
    }
    init = (ducknng_log_entries_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(init, 0, sizeof(*init));
    init->ts      = (duckdb_timestamp *)duckdb_malloc(sizeof(duckdb_timestamp) * DUCKNNG_LOG_RING_CAP);
    init->level   = (char **)duckdb_malloc(sizeof(char *) * DUCKNNG_LOG_RING_CAP);
    init->log_type = (char **)duckdb_malloc(sizeof(char *) * DUCKNNG_LOG_RING_CAP);
    init->message = (char **)duckdb_malloc(sizeof(char *) * DUCKNNG_LOG_RING_CAP);
    if (!init->ts || !init->level || !init->log_type || !init->message) {
        destroy_log_entries_init_data(init);
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(init->ts, 0, sizeof(duckdb_timestamp) * DUCKNNG_LOG_RING_CAP);
    memset(init->level, 0, sizeof(char *) * DUCKNNG_LOG_RING_CAP);
    memset(init->log_type, 0, sizeof(char *) * DUCKNNG_LOG_RING_CAP);
    memset(init->message, 0, sizeof(char *) * DUCKNNG_LOG_RING_CAP);
    init->row_count = (idx_t)ducknng_log_ring_snapshot(&ctx->rt->log_ring,
        init->ts, init->level, init->log_type, init->message);
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_log_entries_init_data);
}

static void ducknng_log_entries_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_log_entries_init_data *init = (ducknng_log_entries_init_data *)duckdb_function_get_init_data(info);
    idx_t remaining, chunk_size, i;
    if (!init || init->offset >= init->row_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    remaining = init->row_count - init->offset;
    chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;
    for (i = 0; i < chunk_size; i++) {
        idx_t row = init->offset + i;
        ((duckdb_timestamp *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0)))[i] = init->ts[row];
        if (init->level[row]) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 1), i, init->level[row], (idx_t)strlen(init->level[row])); else set_null(duckdb_data_chunk_get_vector(output, 1), i);
        if (init->log_type[row]) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 2), i, init->log_type[row], (idx_t)strlen(init->log_type[row])); else set_null(duckdb_data_chunk_get_vector(output, 2), i);
        if (init->message[row]) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 3), i, init->message[row], (idx_t)strlen(init->message[row])); else set_null(duckdb_data_chunk_get_vector(output, 3), i);
    }
    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
}

static int register_log_entries_table(duckdb_connection con, ducknng_sql_context *ctx) {
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE(con, "ducknng_log_entries", ctx, 0, NULL,
        ducknng_log_entries_bind, ducknng_log_entries_init, ducknng_log_entries_scan);
}

/* ---------------------------------------------------------------------------
 * ducknng_nng_stats() — flattened snapshot of NNG's native statistics tree
 * --------------------------------------------------------------------------- */

typedef struct {
    ducknng_nng_stat_row *rows;
    size_t row_count;
    idx_t offset;
} ducknng_nng_stats_init_data;

static void destroy_nng_stats_init_data(void *ptr) {
    ducknng_nng_stats_init_data *data = (ducknng_nng_stats_init_data *)ptr;
    if (!data) return;
    ducknng_nng_stats_free(data->rows, data->row_count);
    duckdb_free(data);
}

static void ducknng_nng_stats_bind(duckdb_bind_info info) {
    duckdb_logical_type type;

    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "scope", type);
    duckdb_bind_add_result_column(info, "name", type);
    duckdb_bind_add_result_column(info, "type", type);
    duckdb_bind_add_result_column(info, "unit", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "value", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "svalue", type);
    duckdb_bind_add_result_column(info, "description", type);
    duckdb_destroy_logical_type(&type);

}

static void ducknng_nng_stats_init(duckdb_init_info info) {
    ducknng_nng_stats_init_data *init;
    ducknng_nng_stat_row *rows = NULL;
    size_t count = 0;
    char *errmsg = NULL;

    if (ducknng_nng_stats_snapshot(&rows, &count, &errmsg) != 0) {
        duckdb_init_set_error(info, errmsg ? errmsg : "ducknng: failed to snapshot nng stats");
        if (errmsg) duckdb_free(errmsg);
        return;
    }
    init = (ducknng_nng_stats_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        ducknng_nng_stats_free(rows, count);
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->rows = rows;
    init->row_count = count;
    init->offset = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_nng_stats_init_data);
}

static void ducknng_nng_stats_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_nng_stats_init_data *init = (ducknng_nng_stats_init_data *)duckdb_function_get_init_data(info);
    idx_t remaining, chunk_size, i;
    if (!init || init->offset >= (idx_t)init->row_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    remaining = (idx_t)init->row_count - init->offset;
    chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;
    for (i = 0; i < chunk_size; i++) {
        ducknng_nng_stat_row *row = &init->rows[init->offset + i];
        duckdb_vector v;
        v = duckdb_data_chunk_get_vector(output, 0);
        if (row->scope) duckdb_unsafe_vector_assign_string_element_len(v, i, row->scope, (idx_t)strlen(row->scope)); else set_null(v, i);
        v = duckdb_data_chunk_get_vector(output, 1);
        if (row->name) duckdb_unsafe_vector_assign_string_element_len(v, i, row->name, (idx_t)strlen(row->name)); else set_null(v, i);
        v = duckdb_data_chunk_get_vector(output, 2);
        if (row->type) duckdb_unsafe_vector_assign_string_element_len(v, i, row->type, (idx_t)strlen(row->type)); else set_null(v, i);
        v = duckdb_data_chunk_get_vector(output, 3);
        if (row->unit) duckdb_unsafe_vector_assign_string_element_len(v, i, row->unit, (idx_t)strlen(row->unit)); else set_null(v, i);
        ((uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 4)))[i] = row->value;
        v = duckdb_data_chunk_get_vector(output, 5);
        if (row->svalue) duckdb_unsafe_vector_assign_string_element_len(v, i, row->svalue, (idx_t)strlen(row->svalue)); else set_null(v, i);
        v = duckdb_data_chunk_get_vector(output, 6);
        if (row->desc) duckdb_unsafe_vector_assign_string_element_len(v, i, row->desc, (idx_t)strlen(row->desc)); else set_null(v, i);
    }
    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
}

static int register_nng_stats_table(duckdb_connection con, ducknng_sql_context *ctx) {
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE(con, "ducknng_nng_stats", ctx, 0, NULL,
        ducknng_nng_stats_bind, ducknng_nng_stats_init, ducknng_nng_stats_scan);
}

/* ---------------------------------------------------------------------------
 * Client-socket pipe-event monitor
 * --------------------------------------------------------------------------- */

static void ducknng_monitor_socket_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    if (!ctx || !ctx->rt) {
        duckdb_scalar_function_set_error(info, "ducknng: runtime is not available");
        return;
    }
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        char *errmsg = NULL;
        if (ducknng_runtime_socket_monitor_enable(ctx->rt, socket_id, &errmsg) != 0) {
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to enable socket monitor");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        out[row] = true;
    }
}

typedef struct {
    uint64_t socket_id;
    uint64_t after_seq;
    uint64_t max_events;
} ducknng_socket_monitor_bind_data;

typedef struct {
    ducknng_socket_pipe_event *events;
    size_t row_count;
    uint64_t dropped;
    idx_t offset;
} ducknng_socket_monitor_init_data;

static void destroy_socket_monitor_bind_data(void *ptr) {
    if (ptr) duckdb_free(ptr);
}

static void destroy_socket_monitor_init_data(void *ptr) {
    ducknng_socket_monitor_init_data *data = (ducknng_socket_monitor_init_data *)ptr;
    if (!data) return;
    if (data->events) duckdb_free(data->events);
    duckdb_free(data);
}

static void ducknng_read_socket_monitor_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    ducknng_socket_monitor_bind_data *bind = NULL;
    duckdb_logical_type type;

    if (!ctx || !ctx->rt) {
        duckdb_bind_set_error(info, "ducknng: missing runtime");
        return;
    }
    if (duckdb_bind_get_parameter_count(info) != 3) {
        duckdb_bind_set_error(info, "ducknng: ducknng_read_socket_monitor(socket_id, after_seq, max_events) requires exactly three parameters");
        return;
    }
    bind = (ducknng_socket_monitor_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    bind->socket_id = ducknng_bind_u64_parameter(info, 0, 0);
    bind->after_seq = ducknng_bind_u64_parameter(info, 1, 0);
    bind->max_events = ducknng_bind_u64_parameter(info, 2, 0);

    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "seq", type);
    duckdb_bind_add_result_column(info, "ts_ms", type);
    duckdb_bind_add_result_column(info, "pipe_id", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "event", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "dropped", type);
    duckdb_destroy_logical_type(&type);

    duckdb_bind_set_bind_data(info, bind, destroy_socket_monitor_bind_data);
}

static void ducknng_read_socket_monitor_init(duckdb_init_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_init_get_extra_info(info);
    ducknng_socket_monitor_bind_data *bind = (ducknng_socket_monitor_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_socket_monitor_init_data *init;
    ducknng_socket_pipe_event *events = NULL;
    size_t event_count = 0;
    uint64_t dropped = 0;
    char *errmsg = NULL;

    if (!ctx || !ctx->rt || !bind) {
        duckdb_init_set_error(info, "ducknng: missing runtime");
        return;
    }
    if (ducknng_runtime_socket_monitor_snapshot(ctx->rt, bind->socket_id, bind->after_seq,
            bind->max_events, &events, &event_count, &dropped, &errmsg) != 0) {
        duckdb_init_set_error(info, errmsg ? errmsg : "ducknng: failed to snapshot socket monitor");
        if (errmsg) duckdb_free(errmsg);
        return;
    }
    init = (ducknng_socket_monitor_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        if (events) duckdb_free(events);
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->events = events;
    init->row_count = event_count;
    init->dropped = dropped;
    init->offset = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_socket_monitor_init_data);
}

static void ducknng_read_socket_monitor_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_socket_monitor_init_data *init = (ducknng_socket_monitor_init_data *)duckdb_function_get_init_data(info);
    idx_t remaining, chunk_size, i;
    if (!init || init->offset >= (idx_t)init->row_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    remaining = (idx_t)init->row_count - init->offset;
    chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;
    for (i = 0; i < chunk_size; i++) {
        ducknng_socket_pipe_event *e = &init->events[init->offset + i];
        const char *ev = e->added ? "add" : "remove";
        ((uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0)))[i] = e->seq;
        ((uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 1)))[i] = e->ts_ms;
        ((uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 2)))[i] = e->pipe_id;
        duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 3), i, ev, (idx_t)strlen(ev));
        ((uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 4)))[i] = init->dropped;
    }
    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
}

static void ducknng_socket_monitor_wait_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    if (!ctx || !ctx->rt) {
        duckdb_scalar_function_set_error(info, "ducknng: runtime is not available");
        return;
    }
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        uint64_t after_seq = arg_u64(duckdb_data_chunk_get_vector(input, 1), row, 0);
        uint64_t timeout_ms = arg_u64(duckdb_data_chunk_get_vector(input, 2), row, 0);
        uint64_t seq = 0;
        char *errmsg = NULL;
        if (ducknng_runtime_socket_monitor_wait(ctx->rt, socket_id, after_seq, timeout_ms, &seq, &errmsg) != 0) {
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to wait on socket monitor");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        out[row] = seq;
    }
}

static int register_socket_monitor(duckdb_connection con, ducknng_sql_context *ctx) {
    duckdb_type param_types[3] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT};
    duckdb_type mon_types[1] = {DUCKDB_TYPE_UBIGINT};
    duckdb_type wait_types[3] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT};
    if (!ctx || !ctx->rt) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_monitor_socket", 1, ducknng_monitor_socket_scalar, ctx, mon_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_TABLE(con, "ducknng_read_socket_monitor", ctx, 3, param_types,
            ducknng_read_socket_monitor_bind, ducknng_read_socket_monitor_init, ducknng_read_socket_monitor_scan)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_socket_monitor_wait", 3, ducknng_socket_monitor_wait_scalar, ctx, wait_types, DUCKDB_TYPE_UBIGINT)) return 0;
    return 1;
}

/* ---------------------------------------------------------------------------
 * ducknng_enable_log_capture() — wire the ducknng log ring into DuckDB's
 * logger subsystem at runtime, bypassing the extension-load-time crash that
 * occurs in DuckDB v1.5.2 when duckdb_register_log_storage is called from
 * the extension entry point.
 *
 * Safe to call multiple times: returns TRUE if capture is already active.
 * Returns FALSE if registration fails; DuckDB emits a warning, not an error.
 * --------------------------------------------------------------------------- */
static void ducknng_enable_log_capture_fn(duckdb_function_info info,
    duckdb_data_chunk input, duckdb_vector output) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    duckdb_log_storage ls;
    bool result;
    (void)input;
    if (!ctx || !ctx->rt) {
        duckdb_scalar_function_set_error(info, "ducknng: runtime not available");
        return;
    }
    if (ctx->rt->log_capture_enabled) {
        /* Already active — return TRUE without re-registering. */
        ((bool *)duckdb_vector_get_data(output))[0] = true;
        return;
    }
    ls = duckdb_create_log_storage();
    if (!ls) {
        ((bool *)duckdb_vector_get_data(output))[0] = false;
        return;
    }
    duckdb_log_storage_set_name(ls, "ducknng");
    duckdb_log_storage_set_extra_data(ls, ctx->rt, NULL);
    /* ducknng_log_write_entry (ducknng_runtime.c) has the exact
     * duckdb_logger_write_log_entry_t signature:
     *   void cb(void *extra, duckdb_timestamp *ts, const char *level,
     *           const char *log_type, const char *msg) */
    duckdb_log_storage_set_write_log_entry(ls,
        (duckdb_logger_write_log_entry_t)ducknng_log_write_entry);
    result = (duckdb_register_log_storage(ctx->rt->db, ls) == DuckDBSuccess);
    /* Ownership of ls transfers to DuckDB on success; destroy only on failure. */
    if (!result) {
        duckdb_destroy_log_storage(&ls);
    } else {
        ctx->rt->log_capture_enabled = 1;
    }
    ((bool *)duckdb_vector_get_data(output))[0] = result;
}

static int register_enable_log_capture_scalar(duckdb_connection con, ducknng_sql_context *ctx) {
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_enable_log_capture", 0,
        ducknng_enable_log_capture_fn, ctx, NULL, DUCKDB_TYPE_BOOLEAN);
}
