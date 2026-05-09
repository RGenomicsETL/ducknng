#include "duckdb_extension.h"
#include "ducknng_runtime.h"
#include "ducknng_sql_api.h"

static void ducknng_log_write_entry(void *extra_data, duckdb_timestamp *timestamp,
    const char *level, const char *log_type, const char *log_message) {
    ducknng_runtime *rt = (ducknng_runtime *)extra_data;
    if (!rt) return;
    ducknng_log_ring_append(&rt->log_ring, timestamp, level, log_type, log_message);
}

DUCKDB_EXTENSION_ENTRYPOINT_CUSTOM(duckdb_extension_info info, struct duckdb_extension_access *access) {
    duckdb_connection connection = NULL;
    ducknng_runtime *rt = NULL;
    duckdb_database *db = NULL;
    int created = 0;
    if (!access || !info) {
        return false;
    }
    db = access->get_database(info);
    if (!db || !*db) {
        access->set_error(info, "ducknng: failed to get database handle");
        return false;
    }
    if (duckdb_connect(*db, &connection) == DuckDBError || !connection) {
        access->set_error(info, "ducknng: failed to open init connection");
        return false;
    }
    if (!ducknng_runtime_init(connection, info, access, &rt, &created)) {
        duckdb_disconnect(&connection);
        return false;
    }
    if (!ducknng_register_sql_api(connection, rt)) {
        access->set_error(info, "ducknng: failed to register sql api");
        if (created) {
            ducknng_runtime_destroy(rt);
        } else {
            duckdb_disconnect(&connection);
        }
        return false;
    }
    /* NOTE: duckdb_register_log_storage is present in the v1.5.2 extension vtable
     * but triggers a DuckDB internal assertion failure ("Attempted to dereference
     * unique_ptr that is NULL") when called during extension load. The log ring
     * infrastructure in ducknng_runtime is in place; re-enable this block once a
     * DuckDB build where duckdb_register_log_storage is safe to call at extension
     * load time is confirmed. */
    if (!created) {
        duckdb_disconnect(&connection);
    }
    return true;
}
